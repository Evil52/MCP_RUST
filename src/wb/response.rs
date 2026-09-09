//! Bounded response decoding, safe diagnostics and retry decisions.

use super::{
    ClientPolicy, DateTime, Duration, HeaderMap, Instant, MAX_ERROR_BODY_BYTES,
    MAX_REQUEST_ID_BYTES, MAX_RESPONSE_BODY_BYTES, RETRY_AFTER, Response, StatusCode, Utc, Value,
    WbError, WbErrorKind, classify_http_status, info, warn,
};

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
