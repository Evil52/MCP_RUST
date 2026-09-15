//! Bounded response decoding, safe diagnostics and retry decisions.

use super::quota::{read_quota_error, vendor_quota_cooldown};
use super::{
    AttemptContext, AttemptOutcome, ClientPolicy, DateTime, Duration, HeaderMap, Instant,
    MAX_ERROR_BODY_BYTES, MAX_REQUEST_ID_BYTES, MAX_RESPONSE_BODY_BYTES, Method, RETRY_AFTER,
    RequestClass, Response, StatusCode, Utc, Value, WAREHOUSE_STOCKS_PATH, WbClient, WbError,
    WbErrorKind, classify_http_status, info, warn,
};
use crate::marketplace_quota::QuotaKey;

pub(super) fn classify_transport_error(
    error: reqwest::Error,
    request_id: Option<String>,
) -> WbError {
    if error.is_timeout() {
        WbError::Timeout {
            request_id,
            source: error,
        }
    } else {
        WbError::Network {
            request_id,
            source: error,
        }
    }
}

pub(super) async fn decode_response(
    response: Response,
    request_id: Option<String>,
    retry_after: Option<Duration>,
) -> Result<Value, WbError> {
    let status = response.status();
    if !status.is_success() {
        let diagnostic = read_body(response, MAX_ERROR_BODY_BYTES, request_id.as_deref())
            .await
            .unwrap_or_default();
        return Err(classify_http_status(
            status,
            request_id,
            retry_after,
            String::from_utf8_lossy(&diagnostic).into_owned(),
        ));
    }
    let body = read_body(response, MAX_RESPONSE_BODY_BYTES, request_id.as_deref()).await?;
    serde_json::from_slice(&body).map_err(|source| WbError::InvalidJson { request_id, source })
}

pub(super) async fn read_body(
    mut response: Response,
    limit: usize,
    request_id: Option<&str>,
) -> Result<Vec<u8>, WbError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(WbError::ResponseTooLarge {
            limit_bytes: limit,
            actual_bytes: response.content_length(),
            request_id: request_id.map(str::to_owned),
        });
    }
    let initial_capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default()
        .min(limit);
    let mut body = Vec::with_capacity(initial_capacity);
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|source| classify_transport_error(source, request_id.map(str::to_owned)))?
    {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(WbError::ResponseTooLarge {
                limit_bytes: limit,
                actual_bytes: None,
                request_id: request_id.map(str::to_owned),
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub(super) fn is_retriable(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

pub(super) const fn is_retriable_transport(kind: WbErrorKind) -> bool {
    matches!(kind, WbErrorKind::Timeout | WbErrorKind::Network)
}

pub(super) fn retry_plan(
    status: StatusCode,
    attempt: usize,
    retry_after: ParsedRetryDelay,
    policy: &ClientPolicy,
) -> Option<Duration> {
    if !is_retriable(status) || attempt >= policy.max_attempts {
        return None;
    }
    let server_delay = match retry_after {
        ParsedRetryDelay::Absent => None,
        ParsedRetryDelay::Valid(delay) if delay <= policy.max_retry_delay => Some(delay),
        ParsedRetryDelay::Valid(_) | ParsedRetryDelay::Invalid => return None,
    };
    Some(retry_delay(attempt, server_delay, policy))
}

pub(super) fn retry_delay(
    attempt: usize,
    server_delay: Option<Duration>,
    policy: &ClientPolicy,
) -> Duration {
    policy.retry_policy().delay(attempt, server_delay)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ParsedRetryDelay {
    Absent,
    Valid(Duration),
    Invalid,
}

impl ParsedRetryDelay {
    pub(super) const fn duration(self) -> Option<Duration> {
        match self {
            Self::Valid(duration) => Some(duration),
            Self::Absent | Self::Invalid => None,
        }
    }
}

pub(super) fn parse_retry_delay(headers: &HeaderMap, now: DateTime<Utc>) -> ParsedRetryDelay {
    for name in [
        RETRY_AFTER.as_str(),
        "x-ratelimit-retry",
        "x-ratelimit-reset",
    ] {
        let Some(value) = headers.get(name) else {
            continue;
        };
        let Ok(value) = value.to_str() else {
            return ParsedRetryDelay::Invalid;
        };
        let value = value.trim();
        if let Ok(seconds) = value.parse::<u64>() {
            return ParsedRetryDelay::Valid(Duration::from_secs(seconds));
        }
        if name == RETRY_AFTER.as_str() {
            return DateTime::parse_from_rfc2822(value).ok().map_or(
                ParsedRetryDelay::Invalid,
                |retry_at| {
                    let nonnegative_seconds = retry_at
                        .with_timezone(&Utc)
                        .signed_duration_since(now)
                        .num_seconds()
                        .max(0);
                    let seconds = u64::try_from(nonnegative_seconds)
                        .expect("nonnegative i64 always fits u64");
                    ParsedRetryDelay::Valid(Duration::from_secs(seconds))
                },
            );
        }
        return ParsedRetryDelay::Invalid;
    }
    ParsedRetryDelay::Absent
}

pub(super) fn trace_transport_failure(
    account: &str,
    endpoint: &str,
    attempt: usize,
    started: Instant,
    error: &WbError,
    will_retry: bool,
) {
    let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let error_kind = error.kind().code();
    warn!(
        account,
        endpoint, attempt, latency_ms, error_kind, will_retry, "WB API transport failed"
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn trace_response(
    account: &str,
    endpoint: &str,
    attempt: usize,
    started: Instant,
    status: StatusCode,
    request_id: Option<&str>,
    error: Option<&WbError>,
    will_retry: bool,
) {
    let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    if error.is_none() && !will_retry {
        info!(account, endpoint, attempt, %status, latency_ms, request_id, "WB API request completed");
    } else {
        let error_kind = error.map_or("retryable_http_status", |value| value.kind().code());
        warn!(
            account,
            endpoint,
            attempt,
            %status,
            latency_ms,
            request_id,
            error_kind,
            will_retry,
            "WB API request completed with an error"
        );
    }
}

/// Extracts an upstream correlation id, rejecting anything that is not a plain
/// bounded token.
///
/// The value is echoed back to the model inside tool error text, so it is held
/// to the same strict charset as [`crate::ozon`]'s: no whitespace, quotes or
/// punctuation an upstream could use to smuggle instructions into a message
/// that otherwise reads as trusted server output.
pub(super) fn extract_request_id(headers: &reqwest::header::HeaderMap) -> Option<String> {
    ["x-request-id", "x-trace-id"]
        .into_iter()
        .filter_map(|name| headers.get(name))
        .find_map(|value| {
            let value = value.to_str().ok()?.trim();
            (!value.is_empty()
                && value.len() <= MAX_REQUEST_ID_BYTES
                && value.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
                }))
            .then(|| value.to_owned())
        })
}

/// Cooldowns one response imposes on later calls with the same token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResponseCooldowns {
    /// Local gate extension honoring a vendor Retry-After.
    vendor: Option<Duration>,
    /// Local seller-inventory penalty for a 409 or a header-less 429.
    inventory: Option<Duration>,
    /// Shared-quota penalty for a seller-inventory 409.
    shared_inventory: Option<Duration>,
}

fn response_cooldowns(
    request_class: RequestClass,
    status: StatusCode,
    retry_after: ParsedRetryDelay,
    policy: &ClientPolicy,
) -> ResponseCooldowns {
    let vendor = match retry_after {
        ParsedRetryDelay::Valid(delay)
            if matches!(
                request_class,
                RequestClass::SellerInventory | RequestClass::FinanceReport
            ) && is_retriable(status) =>
        {
            // One-attempt inventory/finance reads must still honor a long
            // Retry-After for sibling callers. Cap untrusted delays at a
            // day; the generic retry budget is not this shared cooldown.
            Some(delay.min(Duration::from_hours(24)))
        }
        ParsedRetryDelay::Valid(delay)
            if is_retriable(status) && delay <= policy.max_retry_delay =>
        {
            Some(delay)
        }
        ParsedRetryDelay::Absent | ParsedRetryDelay::Invalid | ParsedRetryDelay::Valid(_) => None,
    };
    let inventory = if request_class == RequestClass::SellerInventory {
        match status {
            // WB charges ten requests for a 409 in both inventory groups.
            StatusCode::CONFLICT => Some(policy.seller_inventory_interval * 10),
            StatusCode::TOO_MANY_REQUESTS if vendor.is_none() => Some(Duration::from_secs(60)),
            _ => None,
        }
    } else {
        None
    };
    let shared_inventory = (request_class == RequestClass::SellerInventory
        && status == StatusCode::CONFLICT)
        .then_some(super::SELLER_INVENTORY_MIN_REQUEST_INTERVAL * 10);
    ResponseCooldowns {
        vendor,
        inventory,
        shared_inventory,
    }
}

/// The finance endpoint documents only 200 and 204; any other 2xx is an error.
fn is_unexpected_finance_success(request_class: RequestClass, status: StatusCode) -> bool {
    request_class == RequestClass::FinanceReport && status.is_success() && status != StatusCode::OK
}

/// Applies documented endpoint-specific response semantics after retry and
/// finance terminal-page handling. Other successful responses must be JSON.
async fn decode_endpoint_response(
    context: AttemptContext<'_>,
    response: Response,
    request_id: Option<String>,
    retry_after: Option<Duration>,
) -> Result<Value, WbError> {
    let status = response.status();
    if status == StatusCode::NO_CONTENT
        && *context.method == Method::POST
        && context.endpoint == "analytics:/api/analytics/v1/stocks-report/wb-warehouses"
    {
        // Preserve no-data evidence separately from JSON or a confirmed quantity.
        read_body(response, MAX_RESPONSE_BODY_BYTES, request_id.as_deref()).await?;
        return Ok(serde_json::json!({
            "data": {"items": []},
            "meta": {
                "upstream_status": status.as_u16(),
                "data_state": "no_data",
                "source_endpoint": WAREHOUSE_STOCKS_PATH,
                "request_id": request_id,
            },
        }));
    }
    if is_unexpected_finance_success(context.request_class, status) {
        return Err(WbError::Api {
            status,
            request_id,
            diagnostic: String::new(),
        });
    }
    decode_response(response, request_id, retry_after).await
}

impl WbClient {
    pub(super) async fn response_outcome(
        &self,
        context: AttemptContext<'_>,
        attempt: usize,
        started: Instant,
        response: Response,
        quota_key: Option<&QuotaKey>,
    ) -> Result<AttemptOutcome, WbError> {
        let status = response.status();
        let shared_cooldown = vendor_quota_cooldown(response.headers(), status);
        let request_id = extract_request_id(response.headers());
        let retry_after = parse_retry_delay(response.headers(), Utc::now());
        let planned_retry = context
            .request_class
            .allows_automatic_retry()
            .then(|| retry_plan(status, attempt, retry_after, &self.policy))
            .flatten();
        let cooldowns =
            response_cooldowns(context.request_class, status, retry_after, &self.policy);
        if let (Some(key), Some(delay)) = (
            quota_key,
            shared_cooldown
                .into_iter()
                .chain(cooldowns.shared_inventory)
                .max(),
        ) {
            self.shared_quota
                .defer(key, delay)
                .await
                .map_err(read_quota_error)?;
        }
        if let Some(delay) = planned_retry
            .into_iter()
            .chain(cooldowns.vendor)
            .chain(cooldowns.inventory)
            .max()
        {
            // A vendor-directed retry is shared by every alias using this
            // seller token and endpoint class. Extending the gate before
            // permits are released prevents sibling calls from creating a
            // same-token 429/503 retry storm during the cooldown.
            context
                .limiter
                .extend_cooldown(context.request_class, delay)
                .await;
        }

        if let Some(delay) = planned_retry {
            let diagnostic = read_body(response, MAX_ERROR_BODY_BYTES, request_id.as_deref())
                .await
                .unwrap_or_default();
            trace_response(
                context.account,
                context.endpoint,
                attempt,
                started,
                status,
                request_id.as_deref(),
                None,
                true,
            );
            return Ok(AttemptOutcome::Retry {
                delay,
                error: classify_http_status(
                    status,
                    request_id,
                    retry_after.duration(),
                    String::from_utf8_lossy(&diagnostic).into_owned(),
                ),
            });
        }

        if context.request_class == RequestClass::FinanceReport && status == StatusCode::NO_CONTENT
        {
            // Only the documented finance endpoint uses 204 as terminal proof.
            // A 200 JSON null or empty array remains a distinct response.
            read_body(response, MAX_RESPONSE_BODY_BYTES, request_id.as_deref()).await?;
            trace_response(
                context.account,
                context.endpoint,
                attempt,
                started,
                status,
                request_id.as_deref(),
                None,
                false,
            );
            return Ok(AttemptOutcome::NoContent);
        }

        let result = decode_endpoint_response(
            context,
            response,
            request_id.clone(),
            retry_after.duration(),
        )
        .await;
        let will_retry = context.request_class.allows_automatic_retry()
            && result.as_ref().is_err_and(|error| {
                is_retriable_transport(error.kind()) && attempt < self.policy.max_attempts
            });
        trace_response(
            context.account,
            context.endpoint,
            attempt,
            started,
            status,
            request_id.as_deref(),
            result.as_ref().err(),
            will_retry,
        );
        match result {
            Err(error) if will_retry => Ok(AttemptOutcome::Retry {
                delay: retry_delay(attempt, None, &self.policy),
                error,
            }),
            result => result.map(AttemptOutcome::Complete),
        }
    }
}
