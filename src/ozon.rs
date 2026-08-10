use std::{
    collections::BTreeMap,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use reqwest::{
    Client, Response, StatusCode,
    header::{HeaderMap, RETRY_AFTER},
    redirect::Policy,
};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    sync::{Mutex, Semaphore},
    time::sleep,
};

use crate::config::{StoreCredentials, StoreId};

const MAX_RESPONSE_BODY_BYTES: usize = 8 * 1_048_576;
const MAX_ERROR_BODY_BYTES: usize = 4_096;
const MAX_ATTEMPTS: usize = 3;
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_IN_FLIGHT_REQUESTS_PER_CLIENT: usize = 16;
const MAX_GLOBAL_IN_FLIGHT_REQUESTS: usize = 32;
const MIN_REQUEST_INTERVAL: Duration = Duration::from_millis(20);
const BASE_RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);
const MAX_TOTAL_RETRY_OVERHEAD: Duration = Duration::from_secs(5);
const MAX_REQUEST_ID_BYTES: usize = 128;
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);
const HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Every Ozon Seller API path this process is allowed to reach.
///
/// This is the single source of truth for the read-only guarantee: it is
/// enforced by [`OzonClient::post`] itself, at the only place where an HTTP
/// request can leave the process, so no caller — present or future — can reach
/// a mutating Ozon endpoint even if a higher layer forgets to check.
pub const READ_ONLY_ENDPOINT_ALLOWLIST: &[&str] = &[
    "/v1/analytics/data",
    "/v1/analytics/turnover/stocks",
    "/v1/question/list",
    "/v1/rating/history",
    "/v1/rating/summary",
    "/v1/returns/list",
    "/v1/review/list",
    "/v2/posting/fbo/list",
    "/v2/returns/rfbs/list",
    "/v3/finance/transaction/list",
    "/v3/finance/transaction/totals",
    "/v3/posting/fbo/list",
    "/v3/posting/fbs/list",
    "/v4/posting/fbs/list",
    "/v4/product/info/stocks",
    "/v5/product/info/prices",
];

/// Candidate read-only finance endpoints whose exact production contract is
/// not yet treated as stable. They are denied unless the canary feature is
/// explicitly enabled on both the tool router and this egress client.
pub const PREVIEW_READ_ONLY_ENDPOINT_ALLOWLIST: &[&str] = &[
    "/v1/finance/accrual/by-day",
    "/v1/finance/accrual/postings",
    "/v1/finance/accrual/types",
];

#[must_use]
pub fn is_read_only_endpoint_allowed(endpoint: &str) -> bool {
    READ_ONLY_ENDPOINT_ALLOWLIST.contains(&endpoint)
}

fn is_preview_read_only_endpoint(endpoint: &str) -> bool {
    PREVIEW_READ_ONLY_ENDPOINT_ALLOWLIST.contains(&endpoint)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OzonErrorKind {
    EndpointNotAllowed,
    MissingCredentials,
    Unauthorized,
    Forbidden,
    NotFound,
    RateLimited,
    Server,
    Http,
    Timeout,
    Network,
    Overloaded,
    InvalidJson,
    ResponseTooLarge,
}

impl OzonErrorKind {
    pub const fn code(self) -> &'static str {
        match self {
            Self::EndpointNotAllowed => "endpoint_not_allowed",
            Self::MissingCredentials => "missing_credentials",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::NotFound => "not_found",
            Self::RateLimited => "rate_limited",
            Self::Server => "upstream_server_error",
            Self::Http => "upstream_http_error",
            Self::Timeout => "timeout",
            Self::Network => "network_error",
            Self::Overloaded => "local_overloaded",
            Self::InvalidJson => "invalid_json",
            Self::ResponseTooLarge => "response_too_large",
        }
    }
}

#[derive(Error)]
pub enum OzonError {
    #[error("endpoint {0} отсутствует в read-only allowlist Ozon Seller API")]
    EndpointNotAllowed(String),
    #[error("для магазина {0} не настроены Client-Id и Api-Key")]
    MissingCredentials(StoreId),
    #[error("Ozon API отклонил авторизацию (HTTP 401, request-id: {request_id:?})")]
    Unauthorized { request_id: Option<String> },
    #[error("доступ к ресурсу Ozon API запрещён (HTTP 403, request-id: {request_id:?})")]
    Forbidden { request_id: Option<String> },
    #[error("ресурс Ozon API не найден (HTTP 404, request-id: {request_id:?})")]
    NotFound { request_id: Option<String> },
    #[error(
        "Ozon API ограничил частоту запросов (HTTP 429, request-id: {request_id:?}, retry-after: {retry_after:?})"
    )]
    RateLimited {
        request_id: Option<String>,
        retry_after: Option<Duration>,
    },
    #[error("временная ошибка Ozon API (HTTP {status}, request-id: {request_id:?})")]
    Server {
        status: StatusCode,
        request_id: Option<String>,
        body: String,
    },
    #[error("Ozon API вернул неожиданный HTTP {status} (request-id: {request_id:?})")]
    Api {
        status: StatusCode,
        request_id: Option<String>,
        body: String,
    },
    #[error("истёк таймаут запроса к Ozon API (request-id: {request_id:?})")]
    Timeout {
        request_id: Option<String>,
        #[source]
        source: reqwest::Error,
    },
    #[error("истёк общий deadline операции Ozon API")]
    DeadlineExceeded,
    #[error("сетевая ошибка при обращении к Ozon API (request-id: {request_id:?})")]
    Network {
        request_id: Option<String>,
        #[source]
        source: reqwest::Error,
    },
    #[error("локальный лимит параллельных запросов к Ozon API исчерпан")]
    Overloaded,
    #[error("Ozon API вернул некорректный JSON (request-id: {request_id:?})")]
    InvalidJson {
        request_id: Option<String>,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "ответ Ozon API превышает лимит {limit_bytes} байт (получено: {actual_bytes:?}, request-id: {request_id:?})"
    )]
    ResponseTooLarge {
        limit_bytes: usize,
        actual_bytes: Option<u64>,
        request_id: Option<String>,
    },
}

impl fmt::Debug for OzonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OzonError")
            .field("kind", &self.kind())
            .field("message", &self.to_string())
            .finish_non_exhaustive()
    }
}

impl OzonError {
    pub fn kind(&self) -> OzonErrorKind {
        match self {
            Self::EndpointNotAllowed(_) => OzonErrorKind::EndpointNotAllowed,
            Self::MissingCredentials(_) => OzonErrorKind::MissingCredentials,
            Self::Unauthorized { .. } => OzonErrorKind::Unauthorized,
            Self::Forbidden { .. } => OzonErrorKind::Forbidden,
            Self::NotFound { .. } => OzonErrorKind::NotFound,
            Self::RateLimited { .. } => OzonErrorKind::RateLimited,
            Self::Server { .. } => OzonErrorKind::Server,
            Self::Api { .. } => OzonErrorKind::Http,
            Self::Timeout { .. } | Self::DeadlineExceeded => OzonErrorKind::Timeout,
            Self::Network { .. } => OzonErrorKind::Network,
            Self::Overloaded => OzonErrorKind::Overloaded,
            Self::InvalidJson { .. } => OzonErrorKind::InvalidJson,
            Self::ResponseTooLarge { .. } => OzonErrorKind::ResponseTooLarge,
        }
    }

    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::EndpointNotAllowed(_)
            | Self::MissingCredentials(_)
            | Self::DeadlineExceeded
            | Self::Overloaded => None,
            Self::Unauthorized { request_id }
            | Self::Forbidden { request_id }
            | Self::NotFound { request_id }
            | Self::RateLimited { request_id, .. }
            | Self::Server { request_id, .. }
            | Self::Api { request_id, .. }
            | Self::Timeout { request_id, .. }
            | Self::Network { request_id, .. }
            | Self::InvalidJson { request_id, .. }
            | Self::ResponseTooLarge { request_id, .. } => request_id.as_deref(),
        }
    }
}

#[derive(Debug)]
struct RateLimiter {
    next_allowed: Mutex<Instant>,
    in_flight: Semaphore,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            next_allowed: Mutex::new(Instant::now()),
            in_flight: Semaphore::new(MAX_IN_FLIGHT_REQUESTS_PER_CLIENT),
        }
    }

    async fn acquire(&self) {
        let mut next_allowed = self.next_allowed.lock().await;
        let wait = next_allowed.saturating_duration_since(Instant::now());
        if !wait.is_zero() {
            sleep(wait).await;
        }
        *next_allowed = Instant::now() + MIN_REQUEST_INTERVAL;
    }
}

#[derive(Debug, Clone)]
pub struct OzonClient {
    http: Client,
    base_url: String,
    request_deadline: Duration,
    finance_accruals_preview: bool,
    stores: Arc<BTreeMap<StoreId, StoreCredentials>>,
    rate_limiters: Arc<BTreeMap<StoreId, Arc<RateLimiter>>>,
    global_in_flight: Arc<Semaphore>,
}

impl OzonClient {
    pub fn new(
        base_url: String,
        timeout: Duration,
        stores: BTreeMap<StoreId, StoreCredentials>,
    ) -> Result<Self, reqwest::Error> {
        Self::new_with_user_agent(
            base_url,
            timeout,
            stores,
            concat!("mcp-ozon/", env!("CARGO_PKG_VERSION")),
        )
    }

    fn new_with_user_agent(
        base_url: String,
        timeout: Duration,
        stores: BTreeMap<StoreId, StoreCredentials>,
        user_agent: &str,
    ) -> Result<Self, reqwest::Error> {
        let http = Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(MAX_CONNECT_TIMEOUT))
            .redirect(Policy::none())
            // Marketplace credentials must never be forwarded through an
            // ambient HTTP(S)_PROXY inherited from the host/container.
            .no_proxy()
            .user_agent(user_agent)
            // Keep pooled TLS connections warm so bursts of tool calls reuse an
            // established session instead of paying a handshake each time, and
            // let HTTP/2 multiplex them over a single connection.
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .pool_max_idle_per_host(MAX_IN_FLIGHT_REQUESTS_PER_CLIENT)
            .tcp_keepalive(TCP_KEEPALIVE)
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(HTTP2_KEEP_ALIVE_INTERVAL)
            .http2_keep_alive_while_idle(true)
            .build()?;
        let mut limiters_by_client_id = BTreeMap::new();
        let rate_limiters = stores
            .iter()
            .map(|(store, credentials)| {
                let limiter = limiters_by_client_id
                    .entry(credentials.client_id.clone())
                    .or_insert_with(|| Arc::new(RateLimiter::new()));
                (store.clone(), Arc::clone(limiter))
            })
            .collect();
        Ok(Self {
            http,
            base_url,
            request_deadline: timeout.saturating_add(MAX_TOTAL_RETRY_OVERHEAD),
            finance_accruals_preview: false,
            stores: Arc::new(stores),
            rate_limiters: Arc::new(rate_limiters),
            global_in_flight: Arc::new(Semaphore::new(MAX_GLOBAL_IN_FLIGHT_REQUESTS)),
        })
    }

    pub fn is_configured(&self, store: &StoreId) -> bool {
        self.stores.contains_key(store)
    }

    pub fn with_finance_accruals_preview(mut self, enabled: bool) -> Self {
        self.finance_accruals_preview = enabled;
        self
    }

    #[must_use]
    pub fn is_endpoint_allowed(&self, endpoint: &str) -> bool {
        is_read_only_endpoint_allowed(endpoint)
            || (self.finance_accruals_preview && is_preview_read_only_endpoint(endpoint))
    }

    pub async fn post(
        &self,
        store: &StoreId,
        path: &'static str,
        payload: Value,
    ) -> Result<Value, OzonError> {
        // Enforced here, at the only point where a request can leave the
        // process, so the read-only guarantee does not depend on callers.
        if !self.is_endpoint_allowed(path) {
            return Err(OzonError::EndpointNotAllowed(path.to_owned()));
        }
        tokio::time::timeout(
            self.request_deadline,
            self.post_within_deadline(store, path, payload),
        )
        .await
        .map_err(|_| OzonError::DeadlineExceeded)?
    }

    async fn post_within_deadline(
        &self,
        store: &StoreId,
        path: &'static str,
        payload: Value,
    ) -> Result<Value, OzonError> {
        let credentials = self
            .stores
            .get(store)
            .ok_or_else(|| OzonError::MissingCredentials(store.clone()))?;
        let _global_permit = self
            .global_in_flight
            .try_acquire()
            .map_err(|_| OzonError::Overloaded)?;
        let limiter = self
            .rate_limiters
            .get(store)
            .expect("configured stores always have a rate limiter");
        let _permit = limiter
            .in_flight
            .try_acquire()
            .map_err(|_| OzonError::Overloaded)?;

        let mut attempt = 1;
        loop {
            limiter.acquire().await;
            let started_at = Instant::now();
            let request_trace = RequestTrace {
                store,
                endpoint: path,
                started_at,
                attempt,
            };
            let response = self
                .http
                .post(format!("{}{path}", self.base_url))
                .header("Client-Id", &credentials.client_id)
                .header("Api-Key", &credentials.api_key)
                .json(&payload)
                .send()
                .await;

            let mut response = match response {
                Ok(response) => response,
                Err(source) => {
                    let error = classify_transport_error(source, None);
                    let kind = error.kind();
                    let will_retry = is_retriable_transport(kind) && attempt < MAX_ATTEMPTS;
                    trace_transport_failure(&request_trace, kind, will_retry);
                    if will_retry {
                        sleep(retry_delay(attempt, None)).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(error);
                }
            };
            let status = response.status();
            let request_id = safe_request_id(response.headers());
            let retry_after = parse_retry_after(response.headers(), Utc::now());

            if let Some((delay, kind)) = retry_plan(status, attempt, retry_after) {
                trace_response(&request_trace, status, request_id.as_deref(), true, kind);
                drop(response);
                sleep(delay).await;
                attempt += 1;
                continue;
            }

            let result = decode_response(
                &mut response,
                status,
                request_id.clone(),
                retry_after.duration(),
            )
            .await;
            let kind = result
                .as_ref()
                .err()
                .map_or(OzonErrorKind::Http, OzonError::kind);
            trace_response(&request_trace, status, request_id.as_deref(), false, kind);
            return result;
        }
    }
}

fn retry_plan(
    status: StatusCode,
    attempt: usize,
    retry_after: ParsedRetryAfter,
) -> Option<(Duration, OzonErrorKind)> {
    if !is_retriable(status) || attempt >= MAX_ATTEMPTS {
        return None;
    }

    let retry_after = match retry_after {
        ParsedRetryAfter::Absent => None,
        ParsedRetryAfter::Valid(delay) if delay <= MAX_RETRY_DELAY => Some(delay),
        ParsedRetryAfter::Valid(_) | ParsedRetryAfter::Invalid => return None,
    };
    let kind = if status == StatusCode::TOO_MANY_REQUESTS {
        OzonErrorKind::RateLimited
    } else {
        OzonErrorKind::Server
    };
    Some((retry_delay(attempt, retry_after), kind))
}

async fn decode_response(
    response: &mut Response,
    status: StatusCode,
    request_id: Option<String>,
    retry_after: Option<Duration>,
) -> Result<Value, OzonError> {
    if !status.is_success() {
        let diagnostic = read_bounded_diagnostic_body(response).await;
        return Err(classify_http_error(
            status,
            request_id,
            retry_after,
            diagnostic,
        ));
    }

    let body = read_bounded_body(response, request_id.clone()).await?;
    serde_json::from_slice(&body).map_err(|source| OzonError::InvalidJson { request_id, source })
}

async fn read_bounded_diagnostic_body(response: &mut Response) -> String {
    let declared_length = response.content_length();
    let declared_truncated =
        declared_length.is_some_and(|length| length > MAX_ERROR_BODY_BYTES as u64);
    let length_is_unknown = declared_length.is_none();
    let mut body = Vec::with_capacity(
        declared_length
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or(0)
            .min(MAX_ERROR_BODY_BYTES),
    );
    let mut truncated = declared_truncated;

    while body.len() < MAX_ERROR_BODY_BYTES {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(_) => {
                truncated = true;
                break;
            }
        };
        let remaining = MAX_ERROR_BODY_BYTES - body.len();
        if chunk.len() > remaining {
            body.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }
        body.extend_from_slice(&chunk);
    }

    let mut diagnostic = String::from_utf8_lossy(&body).into_owned();
    if truncated || (length_is_unknown && body.len() == MAX_ERROR_BODY_BYTES) {
        diagnostic.push('…');
    }
    diagnostic
}

async fn read_bounded_body(
    response: &mut Response,
    request_id: Option<String>,
) -> Result<Vec<u8>, OzonError> {
    if let Some(content_length) = response.content_length()
        && content_length > MAX_RESPONSE_BODY_BYTES as u64
    {
        return Err(OzonError::ResponseTooLarge {
            limit_bytes: MAX_RESPONSE_BODY_BYTES,
            actual_bytes: Some(content_length),
            request_id,
        });
    }

    let initial_capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or(0)
        .min(MAX_RESPONSE_BODY_BYTES);
    let mut body = Vec::with_capacity(initial_capacity);
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|source| classify_transport_error(source, request_id.clone()))?
    {
        let next_length = body.len().saturating_add(chunk.len());
        if next_length > MAX_RESPONSE_BODY_BYTES {
            return Err(OzonError::ResponseTooLarge {
                limit_bytes: MAX_RESPONSE_BODY_BYTES,
                actual_bytes: Some(u64::try_from(next_length).unwrap_or(u64::MAX)),
                request_id,
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn classify_transport_error(source: reqwest::Error, request_id: Option<String>) -> OzonError {
    if source.is_timeout() {
        OzonError::Timeout { request_id, source }
    } else {
        OzonError::Network { request_id, source }
    }
}

fn classify_http_error(
    status: StatusCode,
    request_id: Option<String>,
    retry_after: Option<Duration>,
    body: String,
) -> OzonError {
    match status {
        StatusCode::UNAUTHORIZED => OzonError::Unauthorized { request_id },
        StatusCode::FORBIDDEN => OzonError::Forbidden { request_id },
        StatusCode::NOT_FOUND => OzonError::NotFound { request_id },
        StatusCode::TOO_MANY_REQUESTS => OzonError::RateLimited {
            request_id,
            retry_after,
        },
        status if status.is_server_error() => OzonError::Server {
            status,
            request_id,
            body,
        },
        status => OzonError::Api {
            status,
            request_id,
            body,
        },
    }
}

fn is_retriable(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn is_retriable_transport(kind: OzonErrorKind) -> bool {
    matches!(kind, OzonErrorKind::Timeout | OzonErrorKind::Network)
}

fn retry_delay(attempt: usize, retry_after: Option<Duration>) -> Duration {
    retry_after.unwrap_or_else(|| {
        BASE_RETRY_DELAY
            .saturating_mul(1_u32 << attempt.saturating_sub(1).min(8))
            .min(MAX_RETRY_DELAY)
    })
}

fn safe_request_id(headers: &HeaderMap) -> Option<String> {
    [
        "x-o3-trace-id",
        "x-request-id",
        "x-ozon-request-id",
        "request-id",
    ]
    .iter()
    .filter_map(|name| headers.get(*name))
    .find_map(|value| {
        let value = value.to_str().ok()?.trim();
        if value.is_empty()
            || value.len() > MAX_REQUEST_ID_BYTES
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
            })
        {
            None
        } else {
            Some(value.to_owned())
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedRetryAfter {
    Absent,
    Valid(Duration),
    Invalid,
}

impl ParsedRetryAfter {
    const fn duration(self) -> Option<Duration> {
        match self {
            Self::Valid(duration) => Some(duration),
            Self::Absent | Self::Invalid => None,
        }
    }
}

fn parse_retry_after(headers: &HeaderMap, now: DateTime<Utc>) -> ParsedRetryAfter {
    let Some(value) = headers.get(RETRY_AFTER) else {
        return ParsedRetryAfter::Absent;
    };
    let Ok(value) = value.to_str() else {
        return ParsedRetryAfter::Invalid;
    };
    let value = value.trim();
    if value.is_empty() || value.len() > MAX_REQUEST_ID_BYTES {
        return ParsedRetryAfter::Invalid;
    }

    if value.bytes().all(|byte| byte.is_ascii_digit()) {
        return value
            .parse::<u64>()
            .map(Duration::from_secs)
            .map(ParsedRetryAfter::Valid)
            .unwrap_or(ParsedRetryAfter::Invalid);
    }

    let Ok(retry_at) = DateTime::parse_from_rfc2822(value) else {
        return ParsedRetryAfter::Invalid;
    };
    ParsedRetryAfter::Valid(
        retry_at
            .with_timezone(&Utc)
            .signed_duration_since(now)
            .to_std()
            .unwrap_or(Duration::ZERO),
    )
}

struct RequestTrace<'a> {
    store: &'a StoreId,
    endpoint: &'static str,
    started_at: Instant,
    attempt: usize,
}

fn trace_transport_failure(request: &RequestTrace<'_>, kind: OzonErrorKind, will_retry: bool) {
    let latency_ms = elapsed_millis(request.started_at);
    tracing::warn!(
        store = %request.store,
        endpoint = request.endpoint,
        status = "transport_error",
        latency_ms,
        request_id = "-",
        attempt = request.attempt,
        will_retry,
        error_kind = ?kind,
        "Ozon API request failed"
    );
}

fn trace_response(
    request: &RequestTrace<'_>,
    status: StatusCode,
    request_id: Option<&str>,
    will_retry: bool,
    kind: OzonErrorKind,
) {
    let latency_ms = elapsed_millis(request.started_at);
    let request_id = request_id.unwrap_or("-");
    let status_code = status.as_u16();
    if status.is_success() {
        tracing::info!(
            store = %request.store,
            endpoint = request.endpoint,
            status = status_code,
            latency_ms,
            request_id,
            attempt = request.attempt,
            "Ozon API request completed"
        );
    } else {
        tracing::warn!(
            store = %request.store,
            endpoint = request.endpoint,
            status = status_code,
            latency_ms,
            request_id,
            attempt = request.attempt,
            will_retry,
            error_kind = ?kind,
            "Ozon API request completed with an error"
        );
    }
}

fn elapsed_millis(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::{TcpListener, TcpStream},
        sync::mpsc,
        thread,
    };

    use super::*;
    use reqwest::header::HeaderValue;

    struct MockResponse {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
        delay: Duration,
        include_content_length: bool,
    }

    impl MockResponse {
        fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
            Self {
                status,
                headers: Vec::new(),
                body: body.into(),
                delay: Duration::ZERO,
                include_content_length: true,
            }
        }

        fn header(mut self, name: &str, value: &str) -> Self {
            self.headers.push((name.to_owned(), value.to_owned()));
            self
        }

        fn delay(mut self, delay: Duration) -> Self {
            self.delay = delay;
            self
        }

        fn without_content_length(mut self) -> Self {
            self.include_content_length = false;
            self
        }
    }

    fn mock_server(responses: Vec<MockResponse>) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("mock server accepts request");
                let request = read_request(&stream);
                let _ = sender.send(request);
                thread::sleep(response.delay);

                let reason = match response.status {
                    200 => "OK",
                    302 => "Found",
                    401 => "Unauthorized",
                    403 => "Forbidden",
                    404 => "Not Found",
                    409 => "Conflict",
                    429 => "Too Many Requests",
                    500 => "Internal Server Error",
                    502 => "Bad Gateway",
                    503 => "Service Unavailable",
                    504 => "Gateway Timeout",
                    _ => "Test",
                };
                let mut head = format!(
                    "HTTP/1.1 {} {reason}\r\nContent-Type: application/json\r\nConnection: close\r\n",
                    response.status
                );
                if response.include_content_length {
                    head.push_str(&format!("Content-Length: {}\r\n", response.body.len()));
                }
                for (name, value) in response.headers {
                    head.push_str(&format!("{name}: {value}\r\n"));
                }
                head.push_str("\r\n");
                if stream.write_all(head.as_bytes()).is_ok() {
                    let _ = stream.write_all(&response.body);
                }
            }
        });

        (format!("http://{address}"), receiver)
    }

    /// Serves `count` sequential requests over HTTP keep-alive and reports how
    /// many TCP connections the client actually opened.
    ///
    /// The general-purpose mock always answers with `Connection: close`, so it
    /// cannot observe pooling at all.
    fn keep_alive_mock_server(count: usize) -> (String, mpsc::Receiver<usize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, receiver) = mpsc::channel();

        // Exactly one connection is ever accepted. If the client does not reuse
        // it, the next request never arrives on this socket and the read below
        // sees EOF, which fails the test from the mock thread.
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("mock server accepts");
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            for _ in 0..count {
                let mut content_length = 0_usize;
                loop {
                    let mut line = String::new();
                    assert!(
                        reader.read_line(&mut line).unwrap() > 0,
                        "client closed the connection instead of reusing it"
                    );
                    if let Some((name, value)) = line.split_once(':')
                        && name.eq_ignore_ascii_case("content-length")
                    {
                        content_length = value.trim().parse().unwrap_or(0);
                    }
                    if line == "\r\n" {
                        break;
                    }
                }
                let mut body = vec![0_u8; content_length];
                reader.read_exact(&mut body).unwrap();
                let payload = br#"{"result":"ok"}"#;
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                    payload.len()
                );
                stream.write_all(head.as_bytes()).unwrap();
                stream.write_all(payload).unwrap();
            }
            sender.send(count).expect("served count is reported");
        });

        (format!("http://{address}"), receiver)
    }

    #[tokio::test]
    async fn sequential_requests_reuse_one_pooled_connection() {
        // Connection pooling is what makes a burst of tool calls cheap: without
        // it every call pays a fresh TCP and TLS handshake against Ozon. This
        // fails if the idle pool is ever disabled or sized to zero.
        const REQUESTS: usize = 5;
        let (base_url, connections) = keep_alive_mock_server(REQUESTS);
        let client = OzonClient::new(base_url, Duration::from_secs(5), credentials()).unwrap();

        for _ in 0..REQUESTS {
            client
                .post(
                    &StoreId::from("ofk"),
                    "/v1/rating/summary",
                    serde_json::json!({}),
                )
                .await
                .unwrap();
        }

        assert_eq!(
            connections.recv_timeout(Duration::from_secs(10)).unwrap(),
            REQUESTS,
            "all {REQUESTS} calls must be served over the single pooled connection"
        );
    }

    fn read_request(stream: &TcpStream) -> String {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request = Vec::new();
        let mut content_length = 0;

        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).unwrap_or(0) == 0 {
                break;
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                content_length = value.trim().parse().unwrap_or(0);
            }
            request.extend_from_slice(line.as_bytes());
            if line == "\r\n" {
                break;
            }
        }

        let mut body = vec![0; content_length];
        if reader.read_exact(&mut body).is_ok() {
            request.extend_from_slice(&body);
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    fn assert_request_count(receiver: &mpsc::Receiver<String>, expected: usize) {
        for _ in 0..expected {
            receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        }
        assert!(receiver.try_recv().is_err());
    }

    fn credentials() -> BTreeMap<StoreId, StoreCredentials> {
        BTreeMap::from([(
            StoreId::from("ofk"),
            StoreCredentials {
                client_id: "test-client".to_owned(),
                api_key: "test-key".to_owned(),
            },
        )])
    }

    #[tokio::test]
    async fn non_stable_endpoints_are_refused_by_the_client_before_any_network_access() {
        // A store with credentials pointed at a port nothing listens on: if the
        // allowlist did not reject the path first, this would fail as a network
        // error instead, so the assertion proves nothing was ever sent.
        let client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
            credentials(),
        )
        .unwrap();

        for endpoint in [
            "/v1/product/import",
            "/v1/product/update",
            "/v2/posting/fbs/ship",
            "/v2/posting/fbs/cancel",
            "/v1/rating/summary/",
            "",
        ] {
            let error = client
                .post(&StoreId::from("ofk"), endpoint, serde_json::json!({}))
                .await
                .unwrap_err();
            assert_eq!(
                error.kind(),
                OzonErrorKind::EndpointNotAllowed,
                "{endpoint}"
            );
            assert_eq!(error.request_id(), None);
            assert!(error.to_string().contains(endpoint), "{endpoint}");
            assert!(format!("{error:?}").contains("EndpointNotAllowed"));
        }
        for &endpoint in PREVIEW_READ_ONLY_ENDPOINT_ALLOWLIST {
            assert_eq!(
                client
                    .post(&StoreId::from("ofk"), endpoint, serde_json::json!({}))
                    .await
                    .unwrap_err()
                    .kind(),
                OzonErrorKind::EndpointNotAllowed
            );
        }

        // The guard runs before credentials are looked up, so an unconfigured
        // store cannot be used to distinguish allowlisted from denied paths.
        assert_eq!(
            client
                .post(
                    &StoreId::from("unconfigured"),
                    "/v1/product/update",
                    serde_json::json!({})
                )
                .await
                .unwrap_err()
                .kind(),
            OzonErrorKind::EndpointNotAllowed
        );
    }

    #[tokio::test]
    async fn finance_preview_egress_requires_explicit_client_opt_in() {
        let (base_url, requests) = mock_server(vec![MockResponse::new(200, r#"{"ok":true}"#)]);
        let client = OzonClient::new(base_url, Duration::from_secs(1), credentials())
            .unwrap()
            .with_finance_accruals_preview(true);

        assert_eq!(
            client
                .post(
                    &StoreId::from("ofk"),
                    "/v1/finance/accrual/types",
                    serde_json::json!({}),
                )
                .await
                .unwrap(),
            serde_json::json!({"ok": true})
        );
        assert_request_count(&requests, 1);
    }

    #[test]
    fn the_read_only_allowlist_is_sorted_unique_and_free_of_mutating_verbs() {
        let mut sorted = READ_ONLY_ENDPOINT_ALLOWLIST.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted, READ_ONLY_ENDPOINT_ALLOWLIST);

        for endpoint in READ_ONLY_ENDPOINT_ALLOWLIST {
            assert!(endpoint.starts_with('/'), "{endpoint}");
            assert!(is_read_only_endpoint_allowed(endpoint));
            for verb in [
                "cancel", "create", "delete", "import", "set", "ship", "update", "add", "remove",
                "send", "activate", "archive",
            ] {
                assert!(!endpoint.contains(verb), "{endpoint} contains {verb}");
            }
        }
        assert!(!is_read_only_endpoint_allowed("/v1/product/import"));
    }

    #[test]
    fn error_kind_codes_are_stable() {
        for (kind, expected) in [
            (OzonErrorKind::EndpointNotAllowed, "endpoint_not_allowed"),
            (OzonErrorKind::MissingCredentials, "missing_credentials"),
            (OzonErrorKind::Unauthorized, "unauthorized"),
            (OzonErrorKind::Forbidden, "forbidden"),
            (OzonErrorKind::NotFound, "not_found"),
            (OzonErrorKind::RateLimited, "rate_limited"),
            (OzonErrorKind::Server, "upstream_server_error"),
            (OzonErrorKind::Http, "upstream_http_error"),
            (OzonErrorKind::Timeout, "timeout"),
            (OzonErrorKind::Network, "network_error"),
            (OzonErrorKind::Overloaded, "local_overloaded"),
            (OzonErrorKind::InvalidJson, "invalid_json"),
            (OzonErrorKind::ResponseTooLarge, "response_too_large"),
        ] {
            assert_eq!(kind.code(), expected);
        }
    }

    #[test]
    fn invalid_http_client_configuration_is_rejected() {
        let error = OzonClient::new_with_user_agent(
            "https://example.invalid".to_owned(),
            Duration::from_secs(1),
            BTreeMap::new(),
            "invalid\nuser-agent",
        )
        .unwrap_err();
        assert!(error.is_builder());
    }

    #[tokio::test]
    async fn post_sends_credentials_and_decodes_json() {
        let (base_url, request) =
            mock_server(vec![MockResponse::new(200, r#"{"result":{"items":[1]}}"#)]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let response = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({"limit": 5}),
            )
            .await
            .unwrap();

        assert_eq!(response["result"]["items"][0], 1);
        let request = request.recv_timeout(Duration::from_secs(3)).unwrap();
        let request_lowercase = request.to_ascii_lowercase();
        assert!(request.starts_with("POST /v1/rating/summary HTTP/1.1"));
        assert!(request_lowercase.contains("client-id: test-client"));
        assert!(request_lowercase.contains("api-key: test-key"));
        assert!(request.contains(r#"{"limit":5}"#));
    }

    #[tokio::test]
    async fn successful_json_larger_than_the_legacy_one_mib_cap_is_accepted() {
        let payload_size = 1_048_576 + 1;
        let response = serde_json::to_vec(&serde_json::json!({
            "payload": "x".repeat(payload_size)
        }))
        .unwrap();
        assert!(response.len() < MAX_RESPONSE_BODY_BYTES);
        let (base_url, requests) = mock_server(vec![MockResponse::new(200, response)]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let value = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap();

        assert_eq!(value["payload"].as_str().unwrap().len(), payload_size);
        assert_request_count(&requests, 1);
    }

    #[tokio::test]
    async fn invalid_json_is_classified_and_keeps_safe_request_id() {
        let (base_url, _) = mock_server(vec![
            MockResponse::new(200, "not-json").header("X-Request-Id", "req-safe_123"),
        ]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), OzonErrorKind::InvalidJson);
        assert_eq!(error.request_id(), Some("req-safe_123"));
    }

    #[tokio::test]
    async fn api_errors_keep_status_and_bound_bodies_without_displaying_them() {
        let (base_url, requests) = mock_server(vec![MockResponse::new(409, "x".repeat(5_000))]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), OzonErrorKind::Http);
        assert_eq!(error.request_id(), None);
        assert!(matches!(
            &error,
            OzonError::Api { status, body, .. }
                if *status == StatusCode::CONFLICT
                    && body.chars().count() == MAX_ERROR_BODY_BYTES + 1
                    && body.ends_with('…')
        ));
        assert!(!error.to_string().contains(&"x".repeat(100)));
        assert_request_count(&requests, 1);
    }

    #[tokio::test]
    async fn truncated_error_stream_keeps_classification_and_bounded_diagnostic() {
        let response = MockResponse::new(409, "short")
            .without_content_length()
            .header("Content-Length", "100");
        let (base_url, requests) = mock_server(vec![response]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind(), OzonErrorKind::Http);
        assert!(matches!(
            &error,
            OzonError::Api { body, .. } if body == "short…"
        ));
        assert_request_count(&requests, 1);
    }

    #[tokio::test]
    async fn truncated_success_stream_keeps_request_id_and_reports_network_error() {
        let response = MockResponse::new(200, "short")
            .without_content_length()
            .header("Content-Length", "100")
            .header("X-Request-Id", "truncated-success");
        let (base_url, requests) = mock_server(vec![response]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind(), OzonErrorKind::Network);
        assert_eq!(error.request_id(), Some("truncated-success"));
        assert_request_count(&requests, 1);
    }

    #[tokio::test]
    async fn debug_output_never_contains_upstream_body_or_api_key() {
        const PRIVATE_MARKER: &str = "customer-email-private@example.test";
        let (base_url, requests) = mock_server(vec![
            MockResponse::new(
                409,
                format!(r#"{{"error":"{PRIVATE_MARKER}","echo":"test-key"}}"#),
            )
            .header("X-Request-Id", "safe-debug-id"),
        ]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        let debug = format!("{error:?}");

        assert_eq!(error.kind(), OzonErrorKind::Http);
        assert!(debug.contains("Http"));
        assert!(debug.contains("safe-debug-id"));
        assert!(!debug.contains(PRIVATE_MARKER));
        assert!(!debug.contains("test-key"));
        assert_request_count(&requests, 1);
    }

    #[tokio::test]
    async fn authentication_and_not_found_statuses_have_distinct_kinds() {
        for (status, expected_kind) in [
            (401, OzonErrorKind::Unauthorized),
            (403, OzonErrorKind::Forbidden),
            (404, OzonErrorKind::NotFound),
        ] {
            let (base_url, requests) = mock_server(vec![
                MockResponse::new(status, r#"{"error":"test"}"#)
                    .header("X-Ozon-Request-Id", "ozon:req/42"),
            ]);
            let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();
            let error = client
                .post(
                    &StoreId::from("ofk"),
                    "/v1/rating/summary",
                    serde_json::json!({}),
                )
                .await
                .unwrap_err();

            assert_eq!(error.kind(), expected_kind);
            assert_eq!(error.request_id(), Some("ozon:req/42"));
            assert_request_count(&requests, 1);
        }
    }

    #[tokio::test]
    async fn oversized_error_bodies_preserve_http_classification() {
        for (status, expected_kind) in [
            (401, OzonErrorKind::Unauthorized),
            (403, OzonErrorKind::Forbidden),
            (404, OzonErrorKind::NotFound),
            (429, OzonErrorKind::RateLimited),
            (503, OzonErrorKind::Server),
        ] {
            let mut response = MockResponse::new(status, "x".repeat(MAX_ERROR_BODY_BYTES + 512))
                .header("X-O3-Trace-Id", "oversized-error");
            if matches!(status, 429 | 503) {
                response = response.header("Retry-After", "60");
            }
            let (base_url, requests) = mock_server(vec![response]);
            let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

            let error = client
                .post(
                    &StoreId::from("ofk"),
                    "/v1/rating/summary",
                    serde_json::json!({}),
                )
                .await
                .unwrap_err();

            assert_eq!(error.kind(), expected_kind, "HTTP {status}: {error:?}");
            assert_eq!(error.request_id(), Some("oversized-error"));
            match error {
                OzonError::RateLimited { retry_after, .. } => {
                    assert_eq!(retry_after, Some(Duration::from_secs(60)));
                }
                OzonError::Server { status, body, .. } => {
                    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
                    assert_eq!(body.chars().count(), MAX_ERROR_BODY_BYTES + 1);
                    assert!(body.ends_with('…'));
                }
                _ => {}
            }
            assert_request_count(&requests, 1);
        }
    }

    #[tokio::test]
    async fn rate_limit_retries_are_bounded() {
        let responses = (0..MAX_ATTEMPTS)
            .map(|_| {
                MockResponse::new(429, r#"{"error":"slow down"}"#)
                    .header("Retry-After", "0")
                    .header("X-Request-Id", "rate-42")
            })
            .collect();
        let (base_url, requests) = mock_server(responses);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), OzonErrorKind::RateLimited);
        assert_eq!(error.request_id(), Some("rate-42"));
        assert!(matches!(
            &error,
            OzonError::RateLimited {
                retry_after: Some(delay),
                ..
            } if *delay == Duration::ZERO
        ));
        assert_request_count(&requests, MAX_ATTEMPTS);
    }

    #[tokio::test]
    async fn retryable_server_error_can_recover() {
        let (base_url, requests) = mock_server(vec![
            MockResponse::new(503, r#"{"error":"temporary"}"#).header("Retry-After", "0"),
            MockResponse::new(200, r#"{"ok":true}"#),
        ]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let value = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap();
        assert_eq!(value, serde_json::json!({"ok": true}));
        assert_request_count(&requests, 2);
    }

    #[tokio::test]
    async fn server_500_is_classified_but_not_retried() {
        let (base_url, requests) =
            mock_server(vec![MockResponse::new(500, r#"{"error":"permanent"}"#)]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), OzonErrorKind::Server);
        assert!(matches!(
            &error,
            OzonError::Server { status, .. }
                if *status == StatusCode::INTERNAL_SERVER_ERROR
        ));
        assert_request_count(&requests, 1);
    }

    #[tokio::test]
    async fn redirects_are_never_followed() {
        let (base_url, requests) = mock_server(vec![
            MockResponse::new(302, "redirect")
                .header("Location", "http://127.0.0.1:9/must-not-be-called"),
        ]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), OzonErrorKind::Http);
        assert!(matches!(
            &error,
            OzonError::Api { status, .. } if *status == StatusCode::FOUND
        ));
        assert_request_count(&requests, 1);
    }

    #[tokio::test]
    async fn missing_store_credentials_fail_without_network_access() {
        let client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
            BTreeMap::new(),
        )
        .unwrap();
        assert!(!client.is_configured(&StoreId::from("ofk")));

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), OzonErrorKind::MissingCredentials);
        assert_eq!(
            error.to_string(),
            "для магазина ofk не настроены Client-Id и Api-Key"
        );
    }

    #[tokio::test]
    async fn one_logical_operation_has_a_deadline_across_attempts_and_backoff() {
        let (base_url, requests) = mock_server(vec![
            MockResponse::new(200, r#"{"ok":true}"#).delay(Duration::from_millis(100)),
        ]);
        let mut client = OzonClient::new(base_url, Duration::from_secs(1), credentials()).unwrap();
        client.request_deadline = Duration::from_millis(10);

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, OzonError::DeadlineExceeded));
        assert_eq!(error.kind(), OzonErrorKind::Timeout);
        assert_eq!(error.request_id(), None);
        assert_request_count(&requests, 1);
    }

    #[tokio::test]
    async fn timeout_retries_are_bounded_and_keep_the_error_classification() {
        let responses = (0..MAX_ATTEMPTS)
            .map(|_| MockResponse::new(200, r#"{"ok":true}"#).delay(Duration::from_millis(100)))
            .collect();
        let (base_url, requests) = mock_server(responses);
        // Keep each server delay longer than the client timeout, but shorter
        // than the following retry backoff. The single-threaded mock is then
        // ready to accept every retry before the client starts it.
        let client = OzonClient::new(base_url, Duration::from_millis(10), credentials()).unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), OzonErrorKind::Timeout);
        assert_eq!(error.request_id(), None);
        assert_request_count(&requests, MAX_ATTEMPTS);
    }

    #[tokio::test]
    async fn a_transport_timeout_can_recover_on_a_bounded_retry() {
        let (base_url, requests) = mock_server(vec![
            MockResponse::new(200, r#"{"discarded":true}"#).delay(Duration::from_millis(80)),
            MockResponse::new(200, r#"{"ok":true}"#),
        ]);
        let client = OzonClient::new(base_url, Duration::from_millis(30), credentials()).unwrap();

        let value = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap();

        assert_eq!(value, serde_json::json!({"ok": true}));
        assert_request_count(&requests, 2);
    }

    #[tokio::test]
    async fn network_retries_are_bounded_and_keep_the_error_classification() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let client = OzonClient::new(
            format!("http://{address}"),
            Duration::from_secs(1),
            credentials(),
        )
        .unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), OzonErrorKind::Network);
        assert_eq!(error.request_id(), None);
    }

    #[tokio::test]
    async fn a_compressed_response_cannot_expand_past_the_body_limit() {
        use flate2::{Compression, write::GzEncoder};

        // Transparent gzip/brotli decoding is enabled for throughput, which
        // means a compact upstream body can expand enormously. Highly
        // compressible padding stands in for a decompression bomb: ~8 MiB of
        // zeros ships as a few KiB but must still be refused after the limit.
        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        encoder
            .write_all(&vec![b'0'; MAX_RESPONSE_BODY_BYTES + 1])
            .unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(compressed.len() < 64 * 1_024, "{}", compressed.len());

        let (base_url, requests) = mock_server(vec![
            MockResponse::new(200, compressed)
                .header("Content-Encoding", "gzip")
                .header("X-Request-Id", "gzip-bomb-1"),
        ]);
        let client = OzonClient::new(base_url, Duration::from_secs(10), credentials()).unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), OzonErrorKind::ResponseTooLarge);
        assert_eq!(error.request_id(), Some("gzip-bomb-1"));
        requests.recv().unwrap();
    }

    #[tokio::test]
    async fn declared_oversized_response_is_rejected_before_reading() {
        let oversized = "x".repeat(MAX_RESPONSE_BODY_BYTES + 1);
        let (base_url, requests) = mock_server(vec![
            MockResponse::new(200, oversized).header("X-Request-Id", "oversize-1"),
        ]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), OzonErrorKind::ResponseTooLarge);
        assert_eq!(error.request_id(), Some("oversize-1"));
        assert!(matches!(
            &error,
            OzonError::ResponseTooLarge {
                limit_bytes,
                actual_bytes: Some(actual_bytes),
                ..
            } if *limit_bytes == MAX_RESPONSE_BODY_BYTES
                && *actual_bytes == (MAX_RESPONSE_BODY_BYTES + 1) as u64
        ));
        assert_request_count(&requests, 1);
    }

    #[tokio::test]
    async fn streaming_oversized_response_is_bounded_without_content_length() {
        let oversized = "x".repeat(MAX_RESPONSE_BODY_BYTES + 1);
        let (base_url, _) = mock_server(vec![
            MockResponse::new(200, oversized).without_content_length(),
        ]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), OzonErrorKind::ResponseTooLarge);
    }

    #[tokio::test]
    async fn per_store_rate_limiter_spaces_requests_at_fifty_per_second() {
        let limiter = RateLimiter::new();
        let started_at = Instant::now();
        limiter.acquire().await;
        limiter.acquire().await;
        limiter.acquire().await;

        assert!(started_at.elapsed() >= MIN_REQUEST_INTERVAL.saturating_mul(2));
    }

    #[test]
    fn stores_with_the_same_client_id_share_one_rate_limiter() {
        let stores = BTreeMap::from([
            (
                StoreId::from("first"),
                StoreCredentials {
                    client_id: "shared-client".to_owned(),
                    api_key: "first-key".to_owned(),
                },
            ),
            (
                StoreId::from("second"),
                StoreCredentials {
                    client_id: "shared-client".to_owned(),
                    api_key: "second-key".to_owned(),
                },
            ),
            (
                StoreId::from("third"),
                StoreCredentials {
                    client_id: "other-client".to_owned(),
                    api_key: "third-key".to_owned(),
                },
            ),
        ]);
        let client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
            stores,
        )
        .unwrap();

        assert!(Arc::ptr_eq(
            &client.rate_limiters[&StoreId::from("first")],
            &client.rate_limiters[&StoreId::from("second")],
        ));
        assert!(!Arc::ptr_eq(
            &client.rate_limiters[&StoreId::from("first")],
            &client.rate_limiters[&StoreId::from("third")],
        ));
    }

    #[tokio::test]
    async fn shared_per_client_concurrency_limit_fails_fast() {
        let stores = BTreeMap::from([
            (
                StoreId::from("first"),
                StoreCredentials {
                    client_id: "shared-client".to_owned(),
                    api_key: "first-key".to_owned(),
                },
            ),
            (
                StoreId::from("second"),
                StoreCredentials {
                    client_id: "shared-client".to_owned(),
                    api_key: "second-key".to_owned(),
                },
            ),
        ]);
        let client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
            stores,
        )
        .unwrap();
        let limiter = Arc::clone(&client.rate_limiters[&StoreId::from("first")]);
        assert!(Arc::ptr_eq(
            &limiter,
            &client.rate_limiters[&StoreId::from("second")]
        ));
        let mut permits = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_REQUESTS_PER_CLIENT {
            permits.push(limiter.in_flight.try_acquire().unwrap());
        }
        assert_eq!(limiter.in_flight.available_permits(), 0);

        let error = client
            .post(
                &StoreId::from("second"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind(), OzonErrorKind::Overloaded);
        assert_eq!(error.kind().code(), "local_overloaded");
        assert_eq!(error.request_id(), None);
        assert!(!error.to_string().contains("shared-client"));
        drop(permits);
    }

    #[tokio::test]
    async fn aggregate_concurrency_budget_fails_fast_before_network() {
        let client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
            credentials(),
        )
        .unwrap();
        let permits = client
            .global_in_flight
            .acquire_many(u32::try_from(MAX_GLOBAL_IN_FLIGHT_REQUESTS).unwrap())
            .await
            .unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind(), OzonErrorKind::Overloaded);
        drop(permits);
    }

    #[test]
    fn only_explicit_transient_statuses_are_retriable() {
        for status in [429, 502, 503, 504] {
            assert!(is_retriable(StatusCode::from_u16(status).unwrap()));
        }
        for status in [401, 403, 404, 408, 409, 500, 501, 505] {
            assert!(!is_retriable(StatusCode::from_u16(status).unwrap()));
        }
    }

    #[test]
    fn request_id_is_strictly_sanitized_and_bounded() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-request-id",
            HeaderValue::from_static("safe-Id_42:part/1"),
        );
        assert_eq!(
            safe_request_id(&headers).as_deref(),
            Some("safe-Id_42:part/1")
        );

        headers.insert("x-request-id", HeaderValue::from_static("unsafe value"));
        assert_eq!(safe_request_id(&headers), None);

        headers.insert(
            "x-request-id",
            HeaderValue::from_str(&"x".repeat(MAX_REQUEST_ID_BYTES + 1)).unwrap(),
        );
        assert_eq!(safe_request_id(&headers), None);
    }

    #[test]
    fn retry_after_is_parsed_without_shortening_server_delay() {
        let now = DateTime::parse_from_rfc2822("Sun, 06 Nov 1994 08:49:37 GMT")
            .unwrap()
            .with_timezone(&Utc);
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("2"));
        assert_eq!(
            parse_retry_after(&headers, now),
            ParsedRetryAfter::Valid(Duration::from_secs(2))
        );

        headers.insert(RETRY_AFTER, HeaderValue::from_static("3600"));
        assert_eq!(
            parse_retry_after(&headers, now),
            ParsedRetryAfter::Valid(Duration::from_secs(3_600))
        );

        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_static("Sun, 06 Nov 1994 08:49:40 GMT"),
        );
        assert_eq!(
            parse_retry_after(&headers, now),
            ParsedRetryAfter::Valid(Duration::from_secs(3))
        );

        headers.insert(RETRY_AFTER, HeaderValue::from_static("not-a-date"));
        assert_eq!(parse_retry_after(&headers, now), ParsedRetryAfter::Invalid);

        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_static("18446744073709551616"),
        );
        assert_eq!(parse_retry_after(&headers, now), ParsedRetryAfter::Invalid);
        assert_eq!(
            parse_retry_after(&HeaderMap::new(), now),
            ParsedRetryAfter::Absent
        );
    }

    #[test]
    fn malformed_retry_after_values_are_rejected() {
        let now = Utc::now();
        let mut headers = HeaderMap::new();

        headers.insert(RETRY_AFTER, HeaderValue::from_bytes(&[0xff]).unwrap());
        assert_eq!(parse_retry_after(&headers, now), ParsedRetryAfter::Invalid);

        headers.insert(RETRY_AFTER, HeaderValue::from_static(""));
        assert_eq!(parse_retry_after(&headers, now), ParsedRetryAfter::Invalid);

        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_str(&"x".repeat(MAX_REQUEST_ID_BYTES + 1)).unwrap(),
        );
        assert_eq!(parse_retry_after(&headers, now), ParsedRetryAfter::Invalid);
    }

    #[test]
    fn retry_backoff_is_exponential_capped_and_overridden_by_server() {
        assert_eq!(retry_delay(1, None), BASE_RETRY_DELAY);
        assert_eq!(retry_delay(2, None), BASE_RETRY_DELAY * 2);
        assert_eq!(retry_delay(usize::MAX, None), MAX_RETRY_DELAY);
        assert_eq!(
            retry_delay(usize::MAX, Some(Duration::from_secs(3))),
            Duration::from_secs(3)
        );
    }

    #[test]
    fn retry_plan_allows_only_bounded_transient_retries() {
        assert_eq!(
            retry_plan(StatusCode::SERVICE_UNAVAILABLE, 1, ParsedRetryAfter::Absent),
            Some((BASE_RETRY_DELAY, OzonErrorKind::Server))
        );
        assert_eq!(
            retry_plan(
                StatusCode::TOO_MANY_REQUESTS,
                1,
                ParsedRetryAfter::Valid(Duration::from_secs(2)),
            ),
            Some((Duration::from_secs(2), OzonErrorKind::RateLimited))
        );
        assert_eq!(
            retry_plan(
                StatusCode::SERVICE_UNAVAILABLE,
                1,
                ParsedRetryAfter::Valid(MAX_RETRY_DELAY + Duration::from_secs(1)),
            ),
            None
        );
        assert_eq!(
            retry_plan(
                StatusCode::SERVICE_UNAVAILABLE,
                1,
                ParsedRetryAfter::Invalid,
            ),
            None
        );
        assert_eq!(
            retry_plan(
                StatusCode::INTERNAL_SERVER_ERROR,
                1,
                ParsedRetryAfter::Absent,
            ),
            None
        );
        assert_eq!(
            retry_plan(
                StatusCode::SERVICE_UNAVAILABLE,
                MAX_ATTEMPTS,
                ParsedRetryAfter::Absent,
            ),
            None
        );
    }

    #[test]
    fn trace_helpers_evaluate_safe_fields_when_enabled() {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(std::io::sink)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            let store = StoreId::from("ofk");
            let request = RequestTrace {
                store: &store,
                endpoint: "/v1/rating/summary",
                started_at: Instant::now(),
                attempt: 1,
            };

            trace_transport_failure(&request, OzonErrorKind::Network, true);
            trace_response(
                &request,
                StatusCode::OK,
                Some("trace-success"),
                false,
                OzonErrorKind::Http,
            );
            trace_response(
                &request,
                StatusCode::BAD_GATEWAY,
                None,
                true,
                OzonErrorKind::Server,
            );
        });
    }

    #[test]
    fn request_reader_handles_peer_closing_before_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (stream, _) = listener.accept().unwrap();
        drop(peer);

        assert!(read_request(&stream).is_empty());
    }

    #[tokio::test]
    async fn gateway_and_unknown_statuses_are_classified_without_unsafe_retry() {
        for (status, expected_kind) in [
            (502, OzonErrorKind::Server),
            (504, OzonErrorKind::Server),
            (418, OzonErrorKind::Http),
        ] {
            let mut response = MockResponse::new(status, r#"{"error":"test"}"#);
            if matches!(status, 502 | 504) {
                response = response.header("Retry-After", "60");
            }
            let (base_url, requests) = mock_server(vec![response]);
            let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

            let error = client
                .post(
                    &StoreId::from("ofk"),
                    "/v1/rating/summary",
                    serde_json::json!({}),
                )
                .await
                .unwrap_err();

            assert_eq!(error.kind(), expected_kind);
            assert_request_count(&requests, 1);
        }
    }

    #[tokio::test]
    async fn invalid_or_overflowing_retry_after_never_retries() {
        for value in ["not-a-date", "18446744073709551616"] {
            let (base_url, requests) = mock_server(vec![
                MockResponse::new(429, r#"{"error":"slow down"}"#).header("Retry-After", value),
            ]);
            let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

            let error = client
                .post(
                    &StoreId::from("ofk"),
                    "/v1/rating/summary",
                    serde_json::json!({}),
                )
                .await
                .unwrap_err();

            assert_eq!(error.kind(), OzonErrorKind::RateLimited);
            assert!(matches!(
                &error,
                OzonError::RateLimited {
                    retry_after: None,
                    ..
                }
            ));
            assert_request_count(&requests, 1);
        }
    }

    #[tokio::test]
    async fn long_retry_after_fails_without_retrying_too_early() {
        let (base_url, requests) = mock_server(vec![
            MockResponse::new(429, r#"{"error":"slow down"}"#)
                .header("Retry-After", "60")
                .header("X-O3-Trace-Id", "o3-rate-60"),
        ]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), credentials()).unwrap();

        let error = client
            .post(
                &StoreId::from("ofk"),
                "/v1/rating/summary",
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), OzonErrorKind::RateLimited);
        assert_eq!(error.request_id(), Some("o3-rate-60"));
        assert!(matches!(
            &error,
            OzonError::RateLimited {
                retry_after: Some(delay),
                ..
            } if *delay == Duration::from_secs(60)
        ));
        assert_request_count(&requests, 1);
    }
}
