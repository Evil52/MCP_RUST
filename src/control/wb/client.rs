use std::{collections::BTreeSet, fmt, future::Future, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::{
    Client, Proxy, StatusCode,
    header::{AUTHORIZATION, HeaderValue},
    redirect::Policy,
};
use serde_json::Value;
use tokio::{
    sync::Mutex,
    time::{Instant, sleep_until, timeout},
};

use super::{
    MAX_CHANGES, WbCampaignBidType, WbCreateCampaignRequest, WbGuardedWriteError,
    WbPreparedBidChange, WbWriteError,
};

const WB_PROMOTION_BASE_URL: &str = "https://advert-api.wildberries.ru";
const CHANGE_BIDS_PATH: &str = "/api/advert/v1/bids";
#[allow(
    dead_code,
    reason = "wired only after the durable campaign-create plan repository lands"
)]
const CREATE_CAMPAIGN_PATH: &str = "/adv/v2/seacat/save-ad";
const PAUSE_CAMPAIGN_PATH: &str = "/adv/v0/pause";
const START_CAMPAIGN_PATH: &str = "/adv/v0/start";
const MAX_WRITE_RESPONSE_BYTES: usize = 1_048_576;
pub(super) const MAX_ERROR_RESPONSE_BYTES: usize = 4_096;
pub(super) const MAX_REQUEST_ID_BYTES: usize = 128;
// WB currently documents a 200 ms interval for this endpoint. Keep a small
// safety margin and serialize the entire request so clones sharing one token
// cannot create a burst or overlap writes.
pub(super) const MIN_WRITE_INTERVAL: Duration = Duration::from_millis(250);
pub(super) const MIN_CREATE_INTERVAL: Duration = Duration::from_secs(12);

#[derive(Debug)]
pub(super) struct WritePacer {
    next_start: Mutex<Instant>,
    minimum_interval: Duration,
}

impl WritePacer {
    pub(super) fn new(minimum_interval: Duration) -> Self {
        Self {
            next_start: Mutex::new(Instant::now()),
            minimum_interval,
        }
    }

    #[cfg(test)]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "write serialization intentionally spans the complete request"
    )]
    pub(super) async fn run<T, F, Fut>(&self, operation: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let mut next_start = self.next_start.lock().await;
        wait_until_write_slot(*next_start).await;
        *next_start = Instant::now() + self.minimum_interval;
        // Deliberately hold the mutex for the complete request. A second apply
        // using a clone of this client cannot overlap the first write.
        operation().await
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "write serialization intentionally spans permit validation and the response"
    )]
    pub(super) async fn run_guarded<T, E, P, PermitFuture, F, Fut>(
        &self,
        permit: P,
        operation: F,
    ) -> Result<T, E>
    where
        P: FnOnce() -> PermitFuture,
        PermitFuture: Future<Output = Result<(), E>>,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut next_start = self.next_start.lock().await;
        wait_until_write_slot(*next_start).await;
        // The permit must be fresh after both the token-wide queue wait and
        // the pacing wait. Its WB preflight can itself be slow, so record the
        // next slot only after it succeeds, immediately before the PATCH.
        permit().await?;
        *next_start = Instant::now() + self.minimum_interval;
        // Hold the mutex through the response so another clone cannot overlap.
        operation().await
    }
}

async fn wait_until_write_slot(next_start: Instant) {
    if next_start > Instant::now() {
        sleep_until(next_start).await;
    }
}

#[derive(Clone)]
pub struct WbBidWriteClient {
    http: Client,
    base_url: String,
    authorization: HeaderValue,
    timeout: Duration,
    pacer: Arc<WritePacer>,
    create_pacer: Arc<WritePacer>,
}

impl fmt::Debug for WbBidWriteClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WbBidWriteClient")
            .field("base_url", &self.base_url)
            .field("authorization", &"<redacted>")
            .field("timeout", &self.timeout)
            .field("minimum_write_interval", &self.pacer.minimum_interval)
            .field(
                "minimum_create_interval",
                &self.create_pacer.minimum_interval,
            )
            .finish_non_exhaustive()
    }
}

impl WbBidWriteClient {
    pub fn new(timeout_duration: Duration, token: &str, proxy_url: &str) -> Result<Self> {
        if timeout_duration.is_zero() || timeout_duration > Duration::from_secs(30) {
            bail!("CONTROL_MCP_WB_TIMEOUT_SECONDS должен задавать 1..=30 секунд");
        }
        let proxy = Proxy::https(proxy_url).context("неверный CONTROL_MCP_WB_PROXY")?;
        let http = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .https_only(true)
            .connect_timeout(Duration::from_secs(5).min(timeout_duration))
            .timeout(timeout_duration)
            .proxy(proxy)
            .build()
            .context("не удалось создать изолированный WB write HTTP client")?;
        Self::from_parts(
            http,
            WB_PROMOTION_BASE_URL,
            token,
            timeout_duration,
            MIN_WRITE_INTERVAL,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(base_url: &str, token: &str, timeout_duration: Duration) -> Self {
        let http = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(timeout_duration)
            .build()
            .expect("test HTTP client");
        Self::from_parts(http, base_url, token, timeout_duration, Duration::ZERO)
            .expect("test write client")
    }

    pub(super) fn from_parts(
        http: Client,
        base_url: &str,
        token: &str,
        timeout_duration: Duration,
        minimum_write_interval: Duration,
    ) -> Result<Self> {
        if token.is_empty()
            || token.len() > 16_384
            || !token.is_ascii()
            || token
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            bail!("WB promotion write token имеет недопустимый формат");
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .context("WB promotion write token не помещается в Authorization header")?;
        authorization.set_sensitive(true);
        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_owned(),
            authorization,
            timeout: timeout_duration,
            pacer: Arc::new(WritePacer::new(minimum_write_interval)),
            create_pacer: Arc::new(WritePacer::new(if minimum_write_interval.is_zero() {
                Duration::ZERO
            } else {
                MIN_CREATE_INTERVAL
            })),
        })
    }

    /// Performs exactly one PATCH attempt. Retrying is intentionally the caller's
    /// responsibility only after explicit reconciliation proves the first attempt
    /// did not apply.
    #[cfg(test)]
    pub(super) async fn change_bids(
        &self,
        advert_id: u64,
        changes: &[WbPreparedBidChange],
    ) -> Result<Value, WbWriteError> {
        validate_write_request(advert_id, changes)?;
        self.pacer
            .run(|| self.change_bids_once(advert_id, changes))
            .await
    }

    /// Acquires the token-wide write slot first, then runs the caller's final
    /// authorization check immediately before constructing the HTTP request.
    /// This prevents a queued write from outliving a revoked lease/approval.
    pub(in crate::control) async fn change_bids_with_permit<E, F, Fut>(
        &self,
        advert_id: u64,
        changes: &[WbPreparedBidChange],
        permit: F,
    ) -> Result<Value, WbGuardedWriteError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        validate_write_request(advert_id, changes).map_err(WbGuardedWriteError::Write)?;
        self.pacer
            .run_guarded(
                || async { permit().await.map_err(WbGuardedWriteError::Permit) },
                || async {
                    self.change_bids_once(advert_id, changes)
                        .await
                        .map_err(WbGuardedWriteError::Write)
                },
            )
            .await
    }

    pub(in crate::control) async fn pause_campaign_with_permit<E, F, Fut>(
        &self,
        advert_id: u64,
        permit: F,
    ) -> Result<Value, WbGuardedWriteError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        self.campaign_status_with_permit(advert_id, PAUSE_CAMPAIGN_PATH, permit)
            .await
    }

    pub(in crate::control) async fn start_campaign_with_permit<E, F, Fut>(
        &self,
        advert_id: u64,
        permit: F,
    ) -> Result<Value, WbGuardedWriteError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        self.campaign_status_with_permit(advert_id, START_CAMPAIGN_PATH, permit)
            .await
    }

    /// Creates one ready-to-start campaign with exactly one HTTP attempt.
    /// The caller must persist an execute-once claim before granting `permit`.
    #[allow(
        dead_code,
        reason = "writer primitive remains unreachable until durable prepare/approve/apply wiring"
    )]
    pub(in crate::control) async fn create_campaign_with_permit<E, F, Fut>(
        &self,
        request: &WbCreateCampaignRequest,
        permit: F,
    ) -> Result<u64, WbGuardedWriteError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        validate_create_campaign_request(request).map_err(WbGuardedWriteError::Write)?;
        self.create_pacer
            .run_guarded(
                || async { permit().await.map_err(WbGuardedWriteError::Permit) },
                || async {
                    self.create_campaign_once(request)
                        .await
                        .map_err(WbGuardedWriteError::Write)
                },
            )
            .await
    }

    async fn campaign_status_with_permit<E, F, Fut>(
        &self,
        advert_id: u64,
        path: &'static str,
        permit: F,
    ) -> Result<Value, WbGuardedWriteError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        validate_advert_id(advert_id).map_err(WbGuardedWriteError::Write)?;
        self.pacer
            .run_guarded(
                || async { permit().await.map_err(WbGuardedWriteError::Permit) },
                || async {
                    self.change_campaign_status_once(advert_id, path)
                        .await
                        .map_err(WbGuardedWriteError::Write)
                },
            )
            .await
    }

    async fn change_bids_once(
        &self,
        advert_id: u64,
        changes: &[WbPreparedBidChange],
    ) -> Result<Value, WbWriteError> {
        let payload = serde_json::json!({
            "bids": [{
                "advert_id": advert_id,
                "nm_bids": changes.iter().map(|change| serde_json::json!({
                    "nm_id": change.nm_id,
                    "bid_kopecks": change.bid_kopecks,
                    "placement": change.placement.as_api_str(),
                })).collect::<Vec<_>>()
            }]
        });
        let send = self
            .http
            .patch(format!("{}{}", self.base_url, CHANGE_BIDS_PATH))
            .header(AUTHORIZATION, self.authorization.clone())
            .json(&payload)
            .send();
        let response = timeout(self.timeout, send)
            .await
            .map_err(|_| WbWriteError::Ambiguous {
                reason: "timeout",
                request_id: None,
            })?
            .map_err(|_| WbWriteError::Ambiguous {
                reason: "network_error",
                request_id: None,
            })?;
        let status = response.status();
        let request_id = response_request_id(&response);
        let limit = if status.is_success() {
            MAX_WRITE_RESPONSE_BYTES
        } else {
            MAX_ERROR_RESPONSE_BYTES
        };
        let bytes =
            read_bounded(response, limit)
                .await
                .map_err(|reason| WbWriteError::Ambiguous {
                    reason,
                    request_id: request_id.clone(),
                })?;

        if status == StatusCode::OK {
            if bytes.is_empty() {
                return Ok(Value::Null);
            }
            return serde_json::from_slice(&bytes).map_err(|_| WbWriteError::Ambiguous {
                reason: "invalid_success_json",
                request_id,
            });
        }
        Err(WbWriteError::HttpStatus { status, request_id })
    }

    #[allow(
        dead_code,
        reason = "called only by the guarded campaign-create primitive"
    )]
    async fn create_campaign_once(
        &self,
        request: &WbCreateCampaignRequest,
    ) -> Result<u64, WbWriteError> {
        let mut payload = serde_json::json!({
            "name": request.name,
            "nms": request.nm_ids,
            "bid_type": request.bid_type.as_api_str(),
            "payment_type": request.payment_type.as_api_str(),
        });
        if request.bid_type == WbCampaignBidType::Manual {
            payload["placement_types"] = Value::Array(
                request
                    .placement_types
                    .iter()
                    .map(|placement| Value::String(placement.as_api_str().to_owned()))
                    .collect(),
            );
        }
        let send = self
            .http
            .post(format!("{}{}", self.base_url, CREATE_CAMPAIGN_PATH))
            .header(AUTHORIZATION, self.authorization.clone())
            .json(&payload)
            .send();
        let response = timeout(self.timeout, send)
            .await
            .map_err(|_| WbWriteError::Ambiguous {
                reason: "timeout",
                request_id: None,
            })?
            .map_err(|_| WbWriteError::Ambiguous {
                reason: "network_error",
                request_id: None,
            })?;
        let status = response.status();
        let request_id = response_request_id(&response);
        let limit = if status.is_success() {
            MAX_WRITE_RESPONSE_BYTES
        } else {
            MAX_ERROR_RESPONSE_BYTES
        };
        let bytes =
            read_bounded(response, limit)
                .await
                .map_err(|reason| WbWriteError::Ambiguous {
                    reason,
                    request_id: request_id.clone(),
                })?;
        if status == StatusCode::OK {
            let advert_id =
                serde_json::from_slice::<u64>(&bytes).map_err(|_| WbWriteError::Ambiguous {
                    reason: "invalid_success_advert_id",
                    request_id: request_id.clone(),
                })?;
            validate_advert_id(advert_id).map_err(|_| WbWriteError::Ambiguous {
                reason: "invalid_success_advert_id",
                request_id,
            })?;
            return Ok(advert_id);
        }
        Err(WbWriteError::HttpStatus { status, request_id })
    }

    async fn change_campaign_status_once(
        &self,
        advert_id: u64,
        path: &'static str,
    ) -> Result<Value, WbWriteError> {
        let send = self
            .http
            .get(format!("{}{}", self.base_url, path))
            .header(AUTHORIZATION, self.authorization.clone())
            .query(&[("id", advert_id)])
            .send();
        let response = timeout(self.timeout, send)
            .await
            .map_err(|_| WbWriteError::Ambiguous {
                reason: "timeout",
                request_id: None,
            })?
            .map_err(|_| WbWriteError::Ambiguous {
                reason: "network_error",
                request_id: None,
            })?;
        let status = response.status();
        let request_id = response_request_id(&response);
        let limit = if status.is_success() {
            MAX_WRITE_RESPONSE_BYTES
        } else {
            MAX_ERROR_RESPONSE_BYTES
        };
        let bytes =
            read_bounded(response, limit)
                .await
                .map_err(|reason| WbWriteError::Ambiguous {
                    reason,
                    request_id: request_id.clone(),
                })?;
        if status == StatusCode::OK {
            if bytes.is_empty() {
                return Ok(Value::Null);
            }
            return serde_json::from_slice(&bytes).map_err(|_| WbWriteError::Ambiguous {
                reason: "invalid_success_json",
                request_id,
            });
        }
        Err(WbWriteError::HttpStatus { status, request_id })
    }
}

const fn validate_advert_id(advert_id: u64) -> Result<(), WbWriteError> {
    if advert_id == 0 || advert_id > i64::MAX as u64 {
        return Err(WbWriteError::InvalidRequest("advert_id"));
    }
    Ok(())
}

pub(super) fn validate_write_request(
    advert_id: u64,
    changes: &[WbPreparedBidChange],
) -> Result<(), WbWriteError> {
    validate_advert_id(advert_id)?;
    if changes.is_empty() || changes.len() > MAX_CHANGES {
        return Err(WbWriteError::InvalidRequest("changes"));
    }
    let unique = changes
        .iter()
        .map(|change| (change.nm_id, change.placement))
        .collect::<BTreeSet<_>>();
    if unique.len() != changes.len()
        || changes.iter().any(|change| {
            change.nm_id == 0
                || change.nm_id > i64::MAX as u64
                || change.before_bid_kopecks > i64::MAX as u64
                || change.bid_kopecks == 0
                || change.bid_kopecks > i64::MAX as u64
        })
    {
        return Err(WbWriteError::InvalidRequest("changes"));
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "called only by the guarded campaign-create primitive"
)]
pub(super) fn validate_create_campaign_request(
    request: &WbCreateCampaignRequest,
) -> Result<(), WbWriteError> {
    if request.name.is_empty()
        || request.name.trim() != request.name
        || request.name.len() > 100
        || request.name.chars().any(char::is_control)
    {
        return Err(WbWriteError::InvalidRequest("name"));
    }
    if request.nm_ids.is_empty() || request.nm_ids.len() > MAX_CHANGES {
        return Err(WbWriteError::InvalidRequest("nm_ids"));
    }
    let unique_nm_ids = request.nm_ids.iter().copied().collect::<BTreeSet<_>>();
    if unique_nm_ids.len() != request.nm_ids.len()
        || request
            .nm_ids
            .iter()
            .any(|nm_id| *nm_id == 0 || *nm_id > i64::MAX as u64)
    {
        return Err(WbWriteError::InvalidRequest("nm_ids"));
    }
    let unique_placements = request
        .placement_types
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let placements_valid = match request.bid_type {
        WbCampaignBidType::Manual => {
            !request.placement_types.is_empty()
                && request.placement_types.len() <= 2
                && unique_placements.len() == request.placement_types.len()
                && !unique_placements.contains(&super::WbBidPlacement::Combined)
        }
        WbCampaignBidType::Unified => request.placement_types.is_empty(),
    };
    if !placements_valid {
        return Err(WbWriteError::InvalidRequest("placement_types"));
    }
    Ok(())
}

async fn read_bounded(
    mut response: reqwest::Response,
    maximum: usize,
) -> Result<Vec<u8>, &'static str> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| "response_body_error")? {
        if body.len().saturating_add(chunk.len()) > maximum {
            return Err("response_too_large");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn response_request_id(response: &reqwest::Response) -> Option<String> {
    ["x-request-id", "request-id", "x-wb-request-id"]
        .into_iter()
        .find_map(|name| response.headers().get(name))
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= MAX_REQUEST_ID_BYTES
                && !value.chars().any(char::is_control)
        })
        .map(str::to_owned)
}
