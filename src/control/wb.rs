use std::{collections::BTreeSet, fmt, future::Future, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::{
    Client, Proxy, StatusCode,
    header::{AUTHORIZATION, HeaderValue},
    redirect::Policy,
};
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    sync::Mutex,
    time::{Instant, sleep_until, timeout},
};

use crate::config::is_canonical_uuid;

use super::policy::{BidLimits, WbBidPlacement, WbPromotionBidTargetPolicy};

const WB_PROMOTION_BASE_URL: &str = "https://advert-api.wildberries.ru";
const CHANGE_BIDS_PATH: &str = "/api/advert/v1/bids";
const MAX_WRITE_RESPONSE_BYTES: usize = 1_048_576;
const MAX_ERROR_RESPONSE_BYTES: usize = 4_096;
const MAX_CHANGES: usize = 50;
const MAX_REQUEST_ID_BYTES: usize = 128;
// WB currently documents a 200 ms interval for this endpoint. Keep a small
// safety margin and serialize the entire request so clones sharing one token
// cannot create a burst or overlap writes.
const MIN_WRITE_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug)]
struct WritePacer {
    next_start: Mutex<Instant>,
    minimum_interval: Duration,
}

impl WritePacer {
    fn new(minimum_interval: Duration) -> Self {
        Self {
            next_start: Mutex::new(Instant::now()),
            minimum_interval,
        }
    }

    #[cfg(test)]
    async fn run<T, F, Fut>(&self, operation: F) -> T
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

    async fn run_guarded<T, E, P, PermitFuture, F, Fut>(
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbBidChange {
    pub nm_id: u64,
    pub placement: WbBidPlacement,
    pub bid_kopecks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbPreparedBidChange {
    pub nm_id: u64,
    pub placement: WbBidPlacement,
    pub before_bid_kopecks: u64,
    pub bid_kopecks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WbCampaignBidSnapshot {
    pub seller_sid: String,
    pub advert_id: u64,
    pub status: i32,
    pub bid_type: String,
    pub payment_type: String,
    pub bids: Vec<WbSnapshotBid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WbSnapshotBid {
    pub nm_id: u64,
    pub placement: WbBidPlacement,
    pub bid_kopecks: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WbWriteOutcomeKind {
    DefiniteFailure,
    Ambiguous,
}

#[derive(Debug)]
pub enum WbGuardedWriteError<E> {
    Permit(E),
    Write(WbWriteError),
}

#[derive(Error, Debug)]
pub enum WbWriteError {
    #[error("некорректный WB bid write request: {0}")]
    InvalidRequest(&'static str),
    #[error("WB вернул HTTP {status} после отправки bid write (request-id: {request_id:?})")]
    HttpStatus {
        status: StatusCode,
        request_id: Option<String>,
    },
    #[error("результат WB bid write неоднозначен ({reason}, request-id: {request_id:?})")]
    Ambiguous {
        reason: &'static str,
        request_id: Option<String>,
    },
}

impl WbWriteError {
    pub const fn outcome_kind(&self) -> WbWriteOutcomeKind {
        match self {
            Self::InvalidRequest(_) => WbWriteOutcomeKind::DefiniteFailure,
            // Once request bytes may have reached WB, an HTTP status alone is
            // not evidence that a batch had no partial/late effect.
            Self::HttpStatus { .. } | Self::Ambiguous { .. } => WbWriteOutcomeKind::Ambiguous,
        }
    }

    #[cfg(test)]
    fn http_status_request_id(&self) -> Option<Option<&str>> {
        match self {
            Self::HttpStatus { request_id, .. } => Some(request_id.as_deref()),
            Self::InvalidRequest(_) | Self::Ambiguous { .. } => None,
        }
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
            .finish()
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

    fn from_parts(
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
    async fn change_bids(
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
    pub async fn change_bids_with_permit<E, F, Fut>(
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

fn validate_write_request(
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

fn require_exact_advert(response: &Value, advert_id: u64) -> Result<&Value> {
    let adverts = response
        .get("adverts")
        .and_then(Value::as_array)
        .context("WB campaign details не содержит adverts")?;
    let mut matching_adverts = adverts
        .iter()
        .filter(|advert| advert.get("id").and_then(Value::as_u64) == Some(advert_id));
    let advert = matching_adverts
        .next()
        .context("WB campaign details не содержит запрошенную кампанию")?;
    if matching_adverts.next().is_some() {
        bail!("WB campaign details содержит повтор запрошенной кампании");
    }
    Ok(advert)
}

fn require_exact_nm_setting(nm_settings: &[Value], nm_id: u64) -> Result<&Value> {
    let mut matching_nm = nm_settings
        .iter()
        .filter(|item| item.get("nm_id").and_then(Value::as_u64) == Some(nm_id));
    let nm = matching_nm
        .next()
        .with_context(|| format!("nm_id {nm_id} отсутствует в WB campaign"))?;
    if matching_nm.next().is_some() {
        bail!("nm_id {nm_id} повторяется в WB campaign");
    }
    Ok(nm)
}

fn placement_bid(nm: &Value, bid_type: &str, placement: WbBidPlacement) -> Result<u64> {
    match placement {
        WbBidPlacement::Combined => {
            if bid_type != "unified" {
                bail!("placement combined разрешён только для unified bid_type");
            }
            let search = nm
                .pointer("/bids_kopecks/search")
                .and_then(Value::as_u64)
                .context("WB campaign не содержит search bid")?;
            let recommendations = nm
                .pointer("/bids_kopecks/recommendations")
                .and_then(Value::as_u64)
                .context("WB campaign не содержит recommendations bid")?;
            if search != recommendations {
                bail!("unified WB campaign вернула разные ставки размещений");
            }
            Ok(search)
        }
        WbBidPlacement::Search => {
            if bid_type != "manual" {
                bail!("placement search разрешён только для manual bid_type");
            }
            nm.pointer("/bids_kopecks/search")
                .and_then(Value::as_u64)
                .context("WB campaign не содержит search bid")
        }
        WbBidPlacement::Recommendations => {
            if bid_type != "manual" {
                bail!("placement recommendations разрешён только для manual bid_type");
            }
            nm.pointer("/bids_kopecks/recommendations")
                .and_then(Value::as_u64)
                .context("WB campaign не содержит recommendations bid")
        }
    }
}

pub fn campaign_snapshot(
    response: &Value,
    seller_sid: &str,
    advert_id: u64,
    requested: &[WbBidChange],
) -> Result<WbCampaignBidSnapshot> {
    if !is_canonical_uuid(seller_sid) {
        bail!("WB campaign snapshot имеет неверный seller_sid");
    }
    if requested.is_empty() || requested.len() > MAX_CHANGES {
        bail!("WB campaign snapshot request имеет неверный размер");
    }
    let advert = require_exact_advert(response, advert_id)?;
    let status = advert
        .get("status")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .context("WB campaign details содержит неверный status")?;
    if !matches!(status, 4 | 9 | 11) {
        bail!("WB campaign status не допускает изменение ставок");
    }
    let bid_type = advert
        .get("bid_type")
        .and_then(Value::as_str)
        .context("WB campaign details не содержит bid_type")?
        .to_owned();
    let payment_type = advert
        .pointer("/settings/payment_type")
        .and_then(Value::as_str)
        .filter(|value| matches!(*value, "cpm" | "cpc"))
        .context("WB campaign details не содержит поддерживаемый payment_type")?
        .to_owned();
    let nm_settings = advert
        .get("nm_settings")
        .and_then(Value::as_array)
        .context("WB campaign details не содержит nm_settings")?;

    let unique = requested
        .iter()
        .map(|change| (change.nm_id, change.placement))
        .collect::<BTreeSet<_>>();
    if unique.len() != requested.len() {
        bail!("WB bid changes содержат повтор nm_id/placement");
    }
    let mut bids = Vec::with_capacity(requested.len());
    for change in requested {
        let nm = require_exact_nm_setting(nm_settings, change.nm_id)?;
        let before = placement_bid(nm, &bid_type, change.placement)?;
        bids.push(WbSnapshotBid {
            nm_id: change.nm_id,
            placement: change.placement,
            bid_kopecks: before,
        });
    }
    bids.sort_by_key(|bid| (bid.nm_id, bid.placement));
    Ok(WbCampaignBidSnapshot {
        seller_sid: seller_sid.to_owned(),
        advert_id,
        status,
        bid_type,
        payment_type,
        bids,
    })
}

pub fn prepare_changes(
    target: &WbPromotionBidTargetPolicy,
    requested: &[WbBidChange],
    snapshot: &WbCampaignBidSnapshot,
) -> Result<Vec<WbPreparedBidChange>> {
    if target.seller_sid != snapshot.seller_sid
        || target.advert_id != snapshot.advert_id
        || requested.is_empty()
        || requested.len() > MAX_CHANGES
    {
        bail!("WB bid changes находятся вне policy scope");
    }
    let allowed_nm_ids = target.nm_ids.iter().copied().collect::<BTreeSet<_>>();
    let allowed_placements = target.placements.iter().copied().collect::<BTreeSet<_>>();
    let unique = requested
        .iter()
        .map(|change| (change.nm_id, change.placement))
        .collect::<BTreeSet<_>>();
    if unique.len() != requested.len()
        || requested.iter().any(|change| {
            !allowed_nm_ids.contains(&change.nm_id)
                || !allowed_placements.contains(&change.placement)
        })
    {
        bail!("WB bid changes находятся вне policy scope");
    }

    let mut prepared = Vec::with_capacity(requested.len());
    for change in requested {
        let before = snapshot
            .bids
            .iter()
            .find(|bid| bid.nm_id == change.nm_id && bid.placement == change.placement)
            .context("WB snapshot не содержит текущую ставку")?
            .bid_kopecks;
        validate_bid_delta(&target.bid_limits_kopecks, before, change.bid_kopecks)?;
        prepared.push(WbPreparedBidChange {
            nm_id: change.nm_id,
            placement: change.placement,
            before_bid_kopecks: before,
            bid_kopecks: change.bid_kopecks,
        });
    }
    prepared.sort_by_key(|change| (change.nm_id, change.placement));
    Ok(prepared)
}

fn validate_bid_delta(limits: &BidLimits, before: u64, after: u64) -> Result<()> {
    if before > i64::MAX as u64 || after > i64::MAX as u64 {
        bail!("WB bid должен помещаться в int64");
    }
    if after < limits.min_minor || after > limits.max_minor {
        bail!("WB bid выходит за серверные min/max policy limits");
    }
    if before == 0 {
        bail!("нулевую текущую WB ставку нельзя менять без ручной проверки");
    }
    if before == after {
        bail!("WB bid change не должен быть no-op");
    }
    let delta = u128::from(before.abs_diff(after));
    if delta * 100 > u128::from(before) * u128::from(limits.max_delta_percent) {
        bail!("изменение WB bid превышает max_delta_percent");
    }
    Ok(())
}

pub fn snapshot_matches_expected(
    snapshot: &WbCampaignBidSnapshot,
    changes: &[WbPreparedBidChange],
    expected_after: bool,
) -> bool {
    changes.iter().all(|change| {
        snapshot
            .bids
            .iter()
            .find(|bid| bid.nm_id == change.nm_id && bid.placement == change.placement)
            .is_some_and(|bid| {
                bid.bid_kopecks
                    == if expected_after {
                        change.bid_kopecks
                    } else {
                        change.before_bid_kopecks
                    }
            })
    })
}

/// Compares all control-relevant campaign metadata bound into the plan plus
/// the exact requested bid pairs. A queued write must not cross a campaign
/// status, bid-type or payment-type transition unnoticed.
pub fn snapshot_matches_plan_state(
    snapshot: &WbCampaignBidSnapshot,
    before: &WbCampaignBidSnapshot,
    changes: &[WbPreparedBidChange],
    expected_after: bool,
) -> bool {
    snapshot.seller_sid == before.seller_sid
        && snapshot.advert_id == before.advert_id
        && snapshot.status == before.status
        && snapshot.bid_type == before.bid_type
        && snapshot.payment_type == before.payment_type
        && snapshot.bids.len() == before.bids.len()
        && before.bids.len() == changes.len()
        && snapshot_matches_expected(snapshot, changes, expected_after)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Notify,
    };

    use super::*;
    use crate::control::policy::WbActionLimits;

    const TEST_SELLER_SID: &str = "123e4567-e89b-42d3-a456-426614174000";

    fn response(bid_type: &str) -> Value {
        serde_json::json!({
            "adverts": [{
                "id": 42,
                "status": 9,
                "bid_type": bid_type,
                "settings": {"payment_type": "cpm"},
                "nm_settings": [{
                    "nm_id": 1001,
                    "bids_kopecks": {"search": 1000, "recommendations": 1000}
                }]
            }]
        })
    }

    fn target(placement: WbBidPlacement) -> WbPromotionBidTargetPolicy {
        WbPromotionBidTargetPolicy {
            account_id: "wb_one".to_owned(),
            seller_sid: "123e4567-e89b-42d3-a456-426614174000".to_owned(),
            advert_id: 42,
            nm_ids: vec![1001],
            placements: vec![placement],
            bid_limits_kopecks: BidLimits {
                min_minor: 500,
                max_minor: 2000,
                max_delta_percent: 10,
            },
            approver_actor_ids: vec!["approver".to_owned()],
            action_limits: WbActionLimits {
                max_actions_per_hour: 4,
                max_actions_per_day: 12,
                cooldown_seconds: 900,
                max_cumulative_abs_delta_kopecks_per_day: 5000,
            },
        }
    }

    fn prepared_change() -> WbPreparedBidChange {
        WbPreparedBidChange {
            nm_id: 13_335_157,
            placement: WbBidPlacement::Recommendations,
            before_bid_kopecks: 240,
            bid_kopecks: 250,
        }
    }

    async fn response_server(
        response: Vec<u8>,
        response_delay: Duration,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener required by the network contract test");
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 2048];
            while !request.windows(4).any(|part| part == b"\r\n\r\n") {
                let read = socket.read(&mut buffer).await.unwrap();
                assert!(read > 0, "client closed before sending complete headers");
                request.extend_from_slice(&buffer[..read]);
            }
            let headers_end = request
                .windows(4)
                .position(|part| part == b"\r\n\r\n")
                .unwrap();
            let headers = String::from_utf8_lossy(&request[..headers_end + 4]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap();
            let received = request.len();
            request.resize(headers_end + 4 + content_length, 0);
            socket.read_exact(&mut request[received..]).await.unwrap();
            tokio::time::sleep(response_delay).await;
            let _ = socket.write_all(&response).await;
            String::from_utf8(request).unwrap()
        });
        (format!("http://{address}"), server)
    }

    fn http_response(status: &str, headers: &str, body: &[u8]) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n{headers}\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        response
    }

    async fn flag_permit(flag: Arc<AtomicBool>) -> Result<(), ()> {
        if flag.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(())
        }
    }

    async fn mark_operation(flag: Arc<AtomicBool>) -> Result<(), ()> {
        flag.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn run_flag_guarded(
        pacer: &WritePacer,
        permit: Arc<AtomicBool>,
        operation_ran: Arc<AtomicBool>,
    ) -> Result<(), ()> {
        pacer
            .run_guarded(|| flag_permit(permit), || mark_operation(operation_ran))
            .await
    }

    async fn guarded_write_with_decision(
        client: &WbBidWriteClient,
        advert_id: u64,
        permit_result: Result<(), &'static str>,
    ) -> Result<Value, WbGuardedWriteError<&'static str>> {
        client
            .change_bids_with_permit(advert_id, &[prepared_change()], move || async move {
                permit_result
            })
            .await
    }

    #[test]
    fn snapshot_and_policy_prepare_are_exact() {
        let requested = vec![
            WbBidChange {
                nm_id: 1001,
                placement: WbBidPlacement::Recommendations,
                bid_kopecks: 950,
            },
            WbBidChange {
                nm_id: 1001,
                placement: WbBidPlacement::Search,
                bid_kopecks: 1050,
            },
        ];
        let snapshot =
            campaign_snapshot(&response("manual"), TEST_SELLER_SID, 42, &requested).unwrap();
        let mut two_placement_target = target(WbBidPlacement::Search);
        two_placement_target
            .placements
            .push(WbBidPlacement::Recommendations);
        let prepared = prepare_changes(&two_placement_target, &requested, &snapshot).unwrap();
        assert_eq!(prepared[0].before_bid_kopecks, 1000);
        assert_eq!(prepared[0].placement, WbBidPlacement::Search);
        assert_eq!(prepared[1].placement, WbBidPlacement::Recommendations);
        assert!(snapshot_matches_expected(&snapshot, &prepared, false));
        assert!(!snapshot_matches_expected(&snapshot, &prepared, true));
        assert!(snapshot_matches_plan_state(
            &snapshot, &snapshot, &prepared, false
        ));
        let mut changed_mode = snapshot.clone();
        changed_mode.payment_type = "cpc".to_owned();
        assert!(!snapshot_matches_plan_state(
            &changed_mode,
            &snapshot,
            &prepared,
            false
        ));
        let mut changed_seller = snapshot.clone();
        changed_seller.seller_sid = "22222222-2222-4222-8222-222222222222".to_owned();
        assert!(!snapshot_matches_plan_state(
            &changed_seller,
            &snapshot,
            &prepared,
            false
        ));
        assert!(prepare_changes(&two_placement_target, &requested, &changed_seller).is_err());
        assert!(
            campaign_snapshot(
                &response("manual"),
                "00000000-0000-0000-0000-000000000000",
                42,
                &requested,
            )
            .is_err()
        );
    }

    #[test]
    fn parser_rejects_wrong_placement_status_and_missing_nm() {
        let combined = vec![WbBidChange {
            nm_id: 1001,
            placement: WbBidPlacement::Combined,
            bid_kopecks: 1000,
        }];
        assert!(campaign_snapshot(&response("manual"), TEST_SELLER_SID, 42, &combined).is_err());

        let missing = vec![WbBidChange {
            nm_id: 9999,
            placement: WbBidPlacement::Search,
            bid_kopecks: 1000,
        }];
        assert!(campaign_snapshot(&response("manual"), TEST_SELLER_SID, 42, &missing).is_err());

        let mut stopped = response("manual");
        stopped["adverts"][0]["status"] = Value::from(7);
        assert!(campaign_snapshot(&stopped, TEST_SELLER_SID, 42, &missing).is_err());
    }

    #[test]
    fn parser_rejects_ambiguous_duplicate_campaigns_and_nm_settings() {
        let requested = [WbBidChange {
            nm_id: 1001,
            placement: WbBidPlacement::Search,
            bid_kopecks: 1050,
        }];

        let mut duplicate_campaign = response("manual");
        let conflicting_advert = duplicate_campaign["adverts"][0].clone();
        duplicate_campaign["adverts"]
            .as_array_mut()
            .unwrap()
            .push(conflicting_advert);
        assert!(campaign_snapshot(&duplicate_campaign, TEST_SELLER_SID, 42, &requested,).is_err());

        let mut duplicate_nm = response("manual");
        let mut conflicting_nm = duplicate_nm["adverts"][0]["nm_settings"][0].clone();
        conflicting_nm["bids_kopecks"]["search"] = serde_json::json!(1100);
        duplicate_nm["adverts"][0]["nm_settings"]
            .as_array_mut()
            .unwrap()
            .push(conflicting_nm);
        assert!(campaign_snapshot(&duplicate_nm, TEST_SELLER_SID, 42, &requested).is_err());
    }

    #[test]
    fn parser_covers_every_exact_campaign_shape_and_placement_rule() {
        let search = WbBidChange {
            nm_id: 1001,
            placement: WbBidPlacement::Search,
            bid_kopecks: 1050,
        };
        let recommendations = WbBidChange {
            placement: WbBidPlacement::Recommendations,
            ..search.clone()
        };
        let combined = WbBidChange {
            placement: WbBidPlacement::Combined,
            ..search.clone()
        };

        let unified = campaign_snapshot(
            &response("unified"),
            TEST_SELLER_SID,
            42,
            std::slice::from_ref(&combined),
        )
        .expect("matching unified bid");
        assert_eq!(unified.bids[0].bid_kopecks, 1000);
        let manual_recommendations = campaign_snapshot(
            &response("manual"),
            TEST_SELLER_SID,
            42,
            std::slice::from_ref(&recommendations),
        )
        .expect("matching manual recommendations bid");
        assert_eq!(manual_recommendations.bids[0].bid_kopecks, 1000);

        assert!(
            campaign_snapshot(
                &response("unified"),
                TEST_SELLER_SID,
                42,
                std::slice::from_ref(&search),
            )
            .is_err()
        );
        assert!(
            campaign_snapshot(
                &response("unified"),
                TEST_SELLER_SID,
                42,
                std::slice::from_ref(&recommendations),
            )
            .is_err()
        );

        let mut unequal_unified = response("unified");
        unequal_unified["adverts"][0]["nm_settings"][0]["bids_kopecks"]["recommendations"] =
            serde_json::json!(999);
        assert!(
            campaign_snapshot(
                &unequal_unified,
                TEST_SELLER_SID,
                42,
                std::slice::from_ref(&combined),
            )
            .is_err()
        );

        for (placement, missing_key) in [
            (WbBidPlacement::Combined, "search"),
            (WbBidPlacement::Combined, "recommendations"),
            (WbBidPlacement::Search, "search"),
            (WbBidPlacement::Recommendations, "recommendations"),
        ] {
            let mut missing_bid = response(if placement == WbBidPlacement::Combined {
                "unified"
            } else {
                "manual"
            });
            missing_bid["adverts"][0]["nm_settings"][0]["bids_kopecks"]
                .as_object_mut()
                .unwrap()
                .remove(missing_key);
            let request = WbBidChange {
                placement,
                ..search.clone()
            };
            assert!(
                campaign_snapshot(
                    &missing_bid,
                    TEST_SELLER_SID,
                    42,
                    std::slice::from_ref(&request),
                )
                .is_err()
            );
        }

        assert!(campaign_snapshot(&response("manual"), TEST_SELLER_SID, 42, &[]).is_err());
        assert!(
            campaign_snapshot(
                &response("manual"),
                TEST_SELLER_SID,
                42,
                &vec![search.clone(); MAX_CHANGES + 1],
            )
            .is_err()
        );
        assert!(
            campaign_snapshot(
                &response("manual"),
                TEST_SELLER_SID,
                42,
                &[search.clone(), search.clone()],
            )
            .is_err()
        );

        let malformed_responses = [
            serde_json::json!({}),
            serde_json::json!({"adverts": []}),
            serde_json::json!({"adverts": [{"id": 42, "status": "bad"}]}),
            serde_json::json!({"adverts": [{"id": 42, "status": 9}]}),
            serde_json::json!({
                "adverts": [{"id": 42, "status": 9, "bid_type": "manual", "settings": {}}]
            }),
            serde_json::json!({
                "adverts": [{
                    "id": 42,
                    "status": 9,
                    "bid_type": "manual",
                    "settings": {"payment_type": "cpm"}
                }]
            }),
        ];
        for malformed in malformed_responses {
            assert!(
                campaign_snapshot(
                    &malformed,
                    TEST_SELLER_SID,
                    42,
                    std::slice::from_ref(&search),
                )
                .is_err()
            );
        }
    }

    #[test]
    fn prepare_rejects_scope_limit_delta_and_zero_current() {
        let requested = vec![WbBidChange {
            nm_id: 1001,
            placement: WbBidPlacement::Search,
            bid_kopecks: 1200,
        }];
        let snapshot =
            campaign_snapshot(&response("manual"), TEST_SELLER_SID, 42, &requested).unwrap();
        assert!(prepare_changes(&target(WbBidPlacement::Search), &requested, &snapshot).is_err());

        let no_op = vec![WbBidChange {
            nm_id: 1001,
            placement: WbBidPlacement::Search,
            bid_kopecks: 1000,
        }];
        let no_op_snapshot =
            campaign_snapshot(&response("manual"), TEST_SELLER_SID, 42, &no_op).unwrap();
        assert!(prepare_changes(&target(WbBidPlacement::Search), &no_op, &no_op_snapshot).is_err());

        let outside = target(WbBidPlacement::Recommendations);
        assert!(prepare_changes(&outside, &requested, &snapshot).is_err());

        let mut zero = snapshot;
        zero.bids[0].bid_kopecks = 0;
        assert!(prepare_changes(&target(WbBidPlacement::Search), &requested, &zero).is_err());

        let below_minimum = vec![WbBidChange {
            bid_kopecks: 499,
            ..requested[0].clone()
        }];
        assert!(
            prepare_changes(
                &target(WbBidPlacement::Search),
                &below_minimum,
                &no_op_snapshot,
            )
            .is_err()
        );

        let mut missing_snapshot_bid = no_op_snapshot.clone();
        missing_snapshot_bid.bids.clear();
        assert!(
            prepare_changes(
                &target(WbBidPlacement::Search),
                &requested,
                &missing_snapshot_bid,
            )
            .is_err()
        );
        assert!(
            validate_bid_delta(
                &target(WbBidPlacement::Search).bid_limits_kopecks,
                u64::MAX,
                1000
            )
            .is_err()
        );
    }

    // HTTP behavior is covered by the module's construction and request-shape
    // tests in the server integration suite; pure validation stays independently
    // exhaustive here so malformed input can never reach reqwest.
    #[test]
    fn write_request_is_bounded_and_unique() {
        let change = WbPreparedBidChange {
            nm_id: 1,
            placement: WbBidPlacement::Search,
            before_bid_kopecks: 100,
            bid_kopecks: 105,
        };
        assert!(validate_write_request(1, std::slice::from_ref(&change)).is_ok());
        assert!(validate_write_request(0, std::slice::from_ref(&change)).is_err());
        assert!(validate_write_request(u64::MAX, std::slice::from_ref(&change)).is_err());
        assert!(validate_write_request(1, &[]).is_err());
        assert!(validate_write_request(1, &vec![change.clone(); MAX_CHANGES + 1]).is_err());
        assert!(validate_write_request(1, &[change.clone(), change.clone()]).is_err());
        for invalid in [
            WbPreparedBidChange {
                nm_id: 0,
                ..change.clone()
            },
            WbPreparedBidChange {
                nm_id: u64::MAX,
                ..change.clone()
            },
            WbPreparedBidChange {
                bid_kopecks: 0,
                ..change.clone()
            },
            WbPreparedBidChange {
                before_bid_kopecks: u64::MAX,
                ..change.clone()
            },
            WbPreparedBidChange {
                bid_kopecks: u64::MAX,
                ..change.clone()
            },
        ] {
            assert!(validate_write_request(1, &[invalid]).is_err());
        }
        let client = WbBidWriteClient::new_for_test(
            "http://127.0.0.1:1",
            "test-token",
            Duration::from_secs(1),
        );
        let debug = format!("{client:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("test-token"));
        assert_eq!(
            WbWriteError::InvalidRequest("changes").outcome_kind(),
            WbWriteOutcomeKind::DefiniteFailure
        );
    }

    #[test]
    fn production_write_client_constructor_is_narrow_and_fail_closed() {
        assert!(
            WbBidWriteClient::new(
                Duration::from_secs(1),
                "test-token",
                "http://127.0.0.1:3128",
            )
            .is_ok()
        );
        assert!(
            WbBidWriteClient::new(Duration::ZERO, "test-token", "http://127.0.0.1:3128",).is_err()
        );
        assert!(
            WbBidWriteClient::new(
                Duration::from_secs(31),
                "test-token",
                "http://127.0.0.1:3128",
            )
            .is_err()
        );
        assert!(WbBidWriteClient::new(Duration::from_secs(1), "test-token", "not a URL").is_err());

        let http = Client::new();
        for invalid_token in [
            String::new(),
            "two tokens".to_owned(),
            "токен".to_owned(),
            "x".repeat(16_385),
        ] {
            assert!(
                WbBidWriteClient::from_parts(
                    http.clone(),
                    "http://127.0.0.1:1",
                    &invalid_token,
                    Duration::from_secs(1),
                    Duration::ZERO,
                )
                .is_err()
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn write_pacer_serializes_starts_with_a_safety_interval() {
        let pacer = WritePacer::new(MIN_WRITE_INTERVAL);
        let started_at = Instant::now();
        assert_eq!(pacer.run(|| async { 1_u8 }).await, 1);
        assert_eq!(pacer.run(|| async { 2_u8 }).await, 2);
        assert!(Instant::now().duration_since(started_at) >= MIN_WRITE_INTERVAL);
    }

    #[tokio::test]
    async fn queued_write_checks_permit_only_after_acquiring_the_write_slot() {
        let pacer = Arc::new(WritePacer::new(Duration::ZERO));
        let first_started = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let first = {
            let pacer = Arc::clone(&pacer);
            let first_started = Arc::clone(&first_started);
            let release_first = Arc::clone(&release_first);
            tokio::spawn(async move {
                pacer
                    .run_guarded(
                        || async { Ok::<(), ()>(()) },
                        || async move {
                            first_started.notify_one();
                            release_first.notified().await;
                            Ok(())
                        },
                    )
                    .await
                    .unwrap();
            })
        };
        first_started.notified().await;

        let permit = Arc::new(AtomicBool::new(true));
        let operation_ran = Arc::new(AtomicBool::new(false));
        run_flag_guarded(
            &WritePacer::new(Duration::ZERO),
            Arc::clone(&permit),
            Arc::clone(&operation_ran),
        )
        .await
        .unwrap();
        operation_ran.store(false, Ordering::SeqCst);
        let second = {
            let pacer = Arc::clone(&pacer);
            let permit = Arc::clone(&permit);
            let operation_ran = Arc::clone(&operation_ran);
            tokio::spawn(async move { run_flag_guarded(&pacer, permit, operation_ran).await })
        };
        tokio::task::yield_now().await;
        permit.store(false, Ordering::SeqCst);
        release_first.notify_one();

        first.await.unwrap();
        assert!(second.await.unwrap().is_err());
        assert!(!operation_ran.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn slow_permit_does_not_shift_the_actual_patch_start_interval() {
        let minimum_interval = Duration::from_millis(50);
        let pacer = Arc::new(WritePacer::new(minimum_interval));
        let permit_started = Arc::new(Notify::new());
        let release_permit = Arc::new(Notify::new());
        let starts = Arc::new(Mutex::new(Vec::new()));

        let first = {
            let pacer = Arc::clone(&pacer);
            let permit_started = Arc::clone(&permit_started);
            let release_permit = Arc::clone(&release_permit);
            let starts = Arc::clone(&starts);
            tokio::spawn(async move {
                pacer
                    .run_guarded(
                        || async move {
                            permit_started.notify_one();
                            release_permit.notified().await;
                            Ok::<(), ()>(())
                        },
                        || async move {
                            starts.lock().await.push(Instant::now());
                            Ok(())
                        },
                    )
                    .await
            })
        };
        permit_started.notified().await;

        let second = {
            let pacer = Arc::clone(&pacer);
            let starts = Arc::clone(&starts);
            tokio::spawn(async move {
                pacer
                    .run_guarded(
                        || async { Ok::<(), ()>(()) },
                        || async move {
                            starts.lock().await.push(Instant::now());
                            Ok(())
                        },
                    )
                    .await
            })
        };

        // Make the first permit slower than the write interval. Recording the
        // slot before this wait would let both operations start back-to-back.
        tokio::time::sleep(minimum_interval * 2).await;
        release_permit.notify_one();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();

        let starts = starts.lock().await;
        assert_eq!(starts.len(), 2);
        assert!(starts[1].duration_since(starts[0]) >= minimum_interval);
    }

    #[tokio::test]
    async fn guarded_write_distinguishes_validation_permit_and_write_results() {
        let offline = WbBidWriteClient::new_for_test(
            "http://127.0.0.1:1",
            "test-token",
            Duration::from_secs(1),
        );
        let invalid = guarded_write_with_decision(&offline, 0, Ok(())).await;
        assert!(matches!(
            invalid,
            Err(WbGuardedWriteError::Write(WbWriteError::InvalidRequest(
                "advert_id"
            )))
        ));

        let denied = guarded_write_with_decision(&offline, 42, Err("revoked")).await;
        assert!(matches!(
            denied,
            Err(WbGuardedWriteError::Permit("revoked"))
        ));

        let response = http_response("200 OK", "", br#"{"applied":true}"#);
        let (base_url, server) = response_server(response, Duration::ZERO).await;
        let client =
            WbBidWriteClient::new_for_test(&base_url, "test-token", Duration::from_secs(1));
        let result = guarded_write_with_decision(&client, 42, Ok(()))
            .await
            .unwrap();
        assert_eq!(result, serde_json::json!({"applied": true}));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn write_response_handling_is_bounded_and_ambiguity_preserving() {
        for (body, expected) in [
            (Vec::new(), Value::Null),
            (br#"{"ok":true}"#.to_vec(), serde_json::json!({"ok": true})),
        ] {
            let (base_url, server) =
                response_server(http_response("200 OK", "", &body), Duration::ZERO).await;
            let client =
                WbBidWriteClient::new_for_test(&base_url, "test-token", Duration::from_secs(1));
            assert_eq!(
                client.change_bids(42, &[prepared_change()]).await.unwrap(),
                expected
            );
            server.await.unwrap();
        }

        let (base_url, server) = response_server(
            http_response("200 OK", "x-request-id: invalid-json\r\n", b"not-json"),
            Duration::ZERO,
        )
        .await;
        let client =
            WbBidWriteClient::new_for_test(&base_url, "test-token", Duration::from_secs(1));
        let invalid_json = client
            .change_bids(42, &[prepared_change()])
            .await
            .unwrap_err();
        assert!(matches!(
            invalid_json,
            WbWriteError::Ambiguous {
                reason: "invalid_success_json",
                request_id: Some(ref id),
            } if id == "invalid-json"
        ));
        server.await.unwrap();

        let oversized_body = vec![b'x'; MAX_ERROR_RESPONSE_BYTES + 1];
        let (base_url, server) = response_server(
            http_response(
                "400 Bad Request",
                "x-wb-request-id: oversized\r\n",
                &oversized_body,
            ),
            Duration::ZERO,
        )
        .await;
        let client =
            WbBidWriteClient::new_for_test(&base_url, "test-token", Duration::from_secs(1));
        let oversized = client
            .change_bids(42, &[prepared_change()])
            .await
            .unwrap_err();
        assert!(matches!(
            oversized,
            WbWriteError::Ambiguous {
                reason: "response_too_large",
                request_id: Some(ref id),
            } if id == "oversized"
        ));
        server.await.unwrap();

        let (base_url, server) = response_server(
            b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\nconnection: close\r\n\r\n{".to_vec(),
            Duration::ZERO,
        )
        .await;
        let client =
            WbBidWriteClient::new_for_test(&base_url, "test-token", Duration::from_secs(1));
        let truncated = client
            .change_bids(42, &[prepared_change()])
            .await
            .unwrap_err();
        assert!(matches!(
            truncated,
            WbWriteError::Ambiguous {
                reason: "response_body_error",
                ..
            }
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn write_timeout_network_error_and_request_ids_are_sanitized() {
        let (base_url, server) = response_server(
            http_response("200 OK", "", b"{}"),
            Duration::from_millis(50),
        )
        .await;
        let http = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        let client = WbBidWriteClient::from_parts(
            http,
            &base_url,
            "test-token",
            Duration::from_millis(5),
            Duration::ZERO,
        )
        .unwrap();
        let timed_out = client
            .change_bids(42, &[prepared_change()])
            .await
            .unwrap_err();
        assert!(matches!(
            timed_out,
            WbWriteError::Ambiguous {
                reason: "timeout",
                request_id: None,
            }
        ));
        server.await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closed_address = listener.local_addr().unwrap();
        drop(listener);
        let client = WbBidWriteClient::new_for_test(
            &format!("http://{closed_address}"),
            "test-token",
            Duration::from_secs(1),
        );
        let network = client
            .change_bids(42, &[prepared_change()])
            .await
            .unwrap_err();
        assert!(matches!(
            network,
            WbWriteError::Ambiguous {
                reason: "network_error",
                request_id: None,
            }
        ));

        let request_id_cases = [
            (
                "request-id: fallback-id\r\n".to_owned(),
                Some("fallback-id"),
            ),
            ("x-request-id: \r\n".to_owned(), None),
            (
                format!("x-request-id: {}\r\n", "x".repeat(MAX_REQUEST_ID_BYTES + 1)),
                None,
            ),
            ("x-request-id: bad\tid\r\n".to_owned(), None),
        ];
        for (header, expected) in request_id_cases {
            let (base_url, server) = response_server(
                http_response("400 Bad Request", &header, b"{}"),
                Duration::ZERO,
            )
            .await;
            let client =
                WbBidWriteClient::new_for_test(&base_url, "test-token", Duration::from_secs(1));
            let error = client
                .change_bids(42, &[prepared_change()])
                .await
                .unwrap_err();
            assert_eq!(error.http_status_request_id(), Some(expected));
            server.await.unwrap();
        }
        assert_eq!(
            WbWriteError::InvalidRequest("test-only assertion").http_status_request_id(),
            None
        );
    }

    #[tokio::test]
    async fn write_client_sends_exact_patch_once_and_treats_http_4xx_as_ambiguous() {
        let (base_url, server) =
            response_server(http_response("400 Bad Request", "", b"{}"), Duration::ZERO).await;
        let client =
            WbBidWriteClient::new_for_test(&base_url, "test-token", Duration::from_secs(1));
        let error = client
            .change_bids(12345, &[prepared_change()])
            .await
            .unwrap_err();
        assert_eq!(error.outcome_kind(), WbWriteOutcomeKind::Ambiguous);
        let request = server.await.unwrap();
        assert!(request.starts_with("PATCH /api/advert/v1/bids HTTP/1.1\r\n"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer test-token\r\n")
        );
        let body = request.split_once("\r\n\r\n").unwrap().1;
        assert_eq!(
            serde_json::from_str::<Value>(body).unwrap(),
            serde_json::json!({
                "bids": [{
                    "advert_id": 12345,
                    "nm_bids": [{
                        "nm_id": 13_335_157,
                        "bid_kopecks": 250,
                        "placement": "recommendations"
                    }]
                }]
            })
        );
    }
}
