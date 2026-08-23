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

use super::{MAX_CHANGES, WbGuardedWriteError, WbPreparedBidChange, WbWriteError};

const WB_PROMOTION_BASE_URL: &str = "https://advert-api.wildberries.ru";
const CHANGE_BIDS_PATH: &str = "/api/advert/v1/bids";
const MAX_WRITE_RESPONSE_BYTES: usize = 1_048_576;
pub(super) const MAX_ERROR_RESPONSE_BYTES: usize = 4_096;
pub(super) const MAX_REQUEST_ID_BYTES: usize = 128;
// WB currently documents a 200 ms interval for this endpoint. Keep a small
// safety margin and serialize the entire request so clones sharing one token
// cannot create a burst or overlap writes.
pub(super) const MIN_WRITE_INTERVAL: Duration = Duration::from_millis(250);

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
}

impl fmt::Debug for WbBidWriteClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WbBidWriteClient")
            .field("base_url", &self.base_url)
            .field("authorization", &"<redacted>")
            .field("timeout", &self.timeout)
            .field("minimum_write_interval", &self.pacer.minimum_interval)
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
}

pub(super) fn validate_write_request(
    advert_id: u64,
    changes: &[WbPreparedBidChange],
) -> Result<(), WbWriteError> {
    if advert_id == 0 || advert_id > i64::MAX as u64 {
        return Err(WbWriteError::InvalidRequest("advert_id"));
    }
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
