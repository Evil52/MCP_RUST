use std::{
    collections::BTreeMap,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use reqwest::{
    Client, Method, Response, StatusCode,
    header::{AUTHORIZATION, HeaderMap, HeaderValue, RETRY_AFTER},
    redirect::Policy,
};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    sync::{Mutex, Semaphore},
    time::{sleep, timeout},
};
use tracing::{info, warn};

const ANALYTICS_API_BASE_URL: &str = "https://seller-analytics-api.wildberries.ru";
const PING_PATH: &str = "/ping";
const SALES_FUNNEL_PATH: &str = "/api/analytics/v3/sales-funnel/products";
const MAX_RESPONSE_BODY_BYTES: usize = 8 * 1_048_576;
const MAX_ERROR_BODY_BYTES: usize = 4_096;
const MAX_ATTEMPTS: usize = 3;
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_IN_FLIGHT_REQUESTS_PER_TOKEN: usize = 4;
const MAX_GLOBAL_IN_FLIGHT_REQUESTS: usize = 8;
const MAX_REQUEST_ID_BYTES: usize = 128;
const PING_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(10);
const SALES_FUNNEL_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(20);
const BASE_RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
const MAX_LOGICAL_REQUEST_DURATION: Duration = Duration::from_secs(60);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);
const HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Every Wildberries request this process is allowed to make, as an exact
/// `(method, path)` pair.
///
/// Mirrors [`crate::ozon::READ_ONLY_ENDPOINT_ALLOWLIST`]: it is enforced inside
/// [`WbClient::request`], the only place a WB request can leave the process, so
/// adding a mutating call requires deliberately editing this list.
const READ_ONLY_ENDPOINT_ALLOWLIST: &[(Method, &str)] =
    &[(Method::GET, PING_PATH), (Method::POST, SALES_FUNNEL_PATH)];

#[derive(Clone)]
pub struct WbCredentials {
    pub token: String,
}

impl fmt::Debug for WbCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WbCredentials")
            .field("token", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WbErrorKind {
    EndpointNotAllowed,
    MissingCredentials,
    Unauthorized,
    Forbidden,
    RateLimited,
    Http,
    Timeout,
    Network,
    Overloaded,
    InvalidJson,
    ResponseTooLarge,
}

impl WbErrorKind {
    pub const fn code(self) -> &'static str {
        match self {
            Self::EndpointNotAllowed => "endpoint_not_allowed",
            Self::MissingCredentials => "missing_credentials",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::RateLimited => "rate_limited",
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
pub enum WbError {
    #[error("запрос {method} {path} отсутствует в read-only allowlist Wildberries API")]
    EndpointNotAllowed { method: Method, path: String },
    #[error("для кабинета WB {0} не настроен API token")]
    MissingCredentials(String),
    #[error("WB API отклонил авторизацию (HTTP 401, request-id: {request_id:?})")]
    Unauthorized { request_id: Option<String> },
    #[error("доступ к WB API запрещён (HTTP 403, request-id: {request_id:?})")]
    Forbidden { request_id: Option<String> },
    #[error(
        "WB API ограничил частоту запросов (HTTP 429, request-id: {request_id:?}, retry-after: {retry_after:?})"
    )]
    RateLimited {
        request_id: Option<String>,
        retry_after: Option<Duration>,
    },
    #[error("WB API вернул HTTP {status} (request-id: {request_id:?})")]
    Api {
        status: StatusCode,
        request_id: Option<String>,
        diagnostic: String,
    },
    #[error("истёк таймаут запроса к WB API (request-id: {request_id:?})")]
    Timeout {
        request_id: Option<String>,
        #[source]
        source: reqwest::Error,
    },
    #[error("сетевая ошибка при обращении к WB API (request-id: {request_id:?})")]
    Network {
        request_id: Option<String>,
        #[source]
        source: reqwest::Error,
    },
    #[error("исчерпан общий лимит времени read-only запроса к WB API")]
    DeadlineExceeded,
    #[error("локальный лимит параллельных запросов к WB API исчерпан")]
    Overloaded,
    #[error("WB API вернул некорректный JSON (request-id: {request_id:?})")]
    InvalidJson {
        request_id: Option<String>,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "ответ WB API превышает лимит {limit_bytes} байт (получено: {actual_bytes:?}, request-id: {request_id:?})"
    )]
    ResponseTooLarge {
        limit_bytes: usize,
        actual_bytes: Option<u64>,
        request_id: Option<String>,
    },
}

impl fmt::Debug for WbError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WbError")
            .field("kind", &self.kind())
            .field("message", &self.to_string())
            .finish_non_exhaustive()
    }
}

impl WbError {
    pub const fn kind(&self) -> WbErrorKind {
        match self {
            Self::EndpointNotAllowed { .. } => WbErrorKind::EndpointNotAllowed,
            Self::MissingCredentials(_) => WbErrorKind::MissingCredentials,
            Self::Unauthorized { .. } => WbErrorKind::Unauthorized,
            Self::Forbidden { .. } => WbErrorKind::Forbidden,
            Self::RateLimited { .. } => WbErrorKind::RateLimited,
            Self::Api { .. } => WbErrorKind::Http,
            Self::Timeout { .. } | Self::DeadlineExceeded => WbErrorKind::Timeout,
            Self::Network { .. } => WbErrorKind::Network,
            Self::Overloaded => WbErrorKind::Overloaded,
            Self::InvalidJson { .. } => WbErrorKind::InvalidJson,
            Self::ResponseTooLarge { .. } => WbErrorKind::ResponseTooLarge,
        }
    }

    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Unauthorized { request_id }
            | Self::Forbidden { request_id }
            | Self::RateLimited { request_id, .. }
            | Self::Api { request_id, .. }
            | Self::Timeout { request_id, .. }
            | Self::Network { request_id, .. }
            | Self::InvalidJson { request_id, .. }
            | Self::ResponseTooLarge { request_id, .. } => request_id.as_deref(),
            Self::EndpointNotAllowed { .. }
            | Self::MissingCredentials(_)
            | Self::DeadlineExceeded
            | Self::Overloaded => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestClass {
    AnalyticsPing,
    SalesFunnel,
}

const REQUEST_CLASS_BY_ALLOWLIST_INDEX: [RequestClass; READ_ONLY_ENDPOINT_ALLOWLIST.len()] =
    [RequestClass::AnalyticsPing, RequestClass::SalesFunnel];

impl RequestClass {
    fn for_request(method: &Method, path: &str) -> Option<Self> {
        READ_ONLY_ENDPOINT_ALLOWLIST
            .iter()
            .position(|(allowed_method, allowed_path)| {
                allowed_method == method && *allowed_path == path
            })
            .and_then(|index| REQUEST_CLASS_BY_ALLOWLIST_INDEX.get(index).copied())
    }
}

#[derive(Debug, Clone, Copy)]
struct ClientPolicy {
    ping_interval: Duration,
    sales_funnel_interval: Duration,
    max_attempts: usize,
    base_retry_delay: Duration,
    max_retry_delay: Duration,
    logical_timeout: Duration,
}

impl ClientPolicy {
    fn production(request_timeout: Duration) -> Self {
        Self {
            ping_interval: PING_MIN_REQUEST_INTERVAL,
            sales_funnel_interval: SALES_FUNNEL_MIN_REQUEST_INTERVAL,
            max_attempts: MAX_ATTEMPTS,
            base_retry_delay: BASE_RETRY_DELAY,
            max_retry_delay: MAX_RETRY_DELAY,
            logical_timeout: request_timeout
                .saturating_mul(2)
                .min(MAX_LOGICAL_REQUEST_DURATION),
        }
    }

    #[cfg(test)]
    const fn immediate_single_attempt(logical_timeout: Duration) -> Self {
        Self {
            ping_interval: Duration::ZERO,
            sales_funnel_interval: Duration::ZERO,
            max_attempts: 1,
            base_retry_delay: Duration::ZERO,
            max_retry_delay: Duration::from_secs(1),
            logical_timeout,
        }
    }

    fn interval(self, request_class: RequestClass) -> Duration {
        match request_class {
            RequestClass::AnalyticsPing => self.ping_interval,
            RequestClass::SalesFunnel => self.sales_funnel_interval,
        }
    }
}

#[derive(Debug)]
struct PacingGate {
    next_allowed: Mutex<Instant>,
}

impl PacingGate {
    fn new() -> Self {
        Self {
            next_allowed: Mutex::new(Instant::now()),
        }
    }

    /// Reserves exactly one departure slot. Holding this small lock across the
    /// sleep deliberately prevents a cancelled or delayed caller from opening
    /// an accidental burst after the interval.
    async fn pace(&self, interval: Duration) {
        let mut next_allowed = self.next_allowed.lock().await;
        let wait = next_allowed.saturating_duration_since(Instant::now());
        if !wait.is_zero() {
            sleep(wait).await;
        }
        *next_allowed = Instant::now() + interval;
    }
}

#[derive(Debug)]
struct TokenLimiter {
    in_flight: Semaphore,
    analytics_ping: PacingGate,
    sales_funnel: PacingGate,
}

impl TokenLimiter {
    fn new() -> Self {
        Self {
            in_flight: Semaphore::new(MAX_IN_FLIGHT_REQUESTS_PER_TOKEN),
            analytics_ping: PacingGate::new(),
            sales_funnel: PacingGate::new(),
        }
    }

    async fn pace(&self, request_class: RequestClass, interval: Duration) {
        match request_class {
            RequestClass::AnalyticsPing => self.analytics_ping.pace(interval).await,
            RequestClass::SalesFunnel => self.sales_funnel.pace(interval).await,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WbClient {
    http: Client,
    analytics_base_url: String,
    accounts: Arc<BTreeMap<String, WbCredentials>>,
    limiters: Arc<BTreeMap<String, Arc<TokenLimiter>>>,
    global_in_flight: Arc<Semaphore>,
    logical_timeout: Duration,
    policy: ClientPolicy,
}

impl WbClient {
    pub fn new(timeout: Duration, accounts: BTreeMap<String, WbCredentials>) -> Self {
        Self::build(
            timeout,
            accounts,
            ANALYTICS_API_BASE_URL,
            ClientPolicy::production(timeout),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        timeout: Duration,
        accounts: BTreeMap<String, WbCredentials>,
        _common_base_url: &str,
        analytics_base_url: &str,
    ) -> Self {
        Self::build(
            timeout,
            accounts,
            analytics_base_url,
            ClientPolicy::immediate_single_attempt(timeout),
        )
    }

    #[cfg(test)]
    fn new_for_test_with_policy(
        timeout: Duration,
        accounts: BTreeMap<String, WbCredentials>,
        analytics_base_url: &str,
        policy: ClientPolicy,
    ) -> Self {
        Self::build(timeout, accounts, analytics_base_url, policy)
    }

    fn build(
        timeout: Duration,
        accounts: BTreeMap<String, WbCredentials>,
        analytics_base_url: &str,
        policy: ClientPolicy,
    ) -> Self {
        let http = Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(MAX_CONNECT_TIMEOUT))
            .redirect(Policy::none())
            // Marketplace credentials must never traverse an ambient proxy.
            .no_proxy()
            .user_agent(concat!("mcp-ozon/", env!("CARGO_PKG_VERSION")))
            // Keep pooled TLS connections warm across tool calls and let
            // HTTP/2 multiplex concurrent requests over a single connection.
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .pool_max_idle_per_host(MAX_GLOBAL_IN_FLIGHT_REQUESTS)
            .tcp_keepalive(TCP_KEEPALIVE)
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(HTTP2_KEEP_ALIVE_INTERVAL)
            .http2_keep_alive_while_idle(true)
            .build()
            .expect("static WB HTTP client configuration must be valid");
        // WB enforces quotas per seller token, not per local account alias.
        // Reusing the same Arc makes aliases consume one shared quota without
        // ever logging or returning the token used as the temporary map key.
        let mut limiters_by_token = BTreeMap::new();
        let limiters = accounts
            .iter()
            .map(|(account, credentials)| {
                let limiter = limiters_by_token
                    .entry(credentials.token.clone())
                    .or_insert_with(|| Arc::new(TokenLimiter::new()));
                (account.clone(), Arc::clone(limiter))
            })
            .collect();
        Self {
            http,
            analytics_base_url: analytics_base_url.trim_end_matches('/').to_owned(),
            accounts: Arc::new(accounts),
            limiters: Arc::new(limiters),
            global_in_flight: Arc::new(Semaphore::new(MAX_GLOBAL_IN_FLIGHT_REQUESTS)),
            logical_timeout: policy.logical_timeout,
            policy,
        }
    }

    pub fn empty(timeout: Duration) -> Self {
        Self::new(timeout, BTreeMap::new())
    }

    pub fn is_configured(&self, account: &str) -> bool {
        self.accounts.contains_key(account)
    }

    pub async fn ping(&self, account: &str) -> Result<Value, WbError> {
        self.request(
            account,
            Method::GET,
            "analytics:/ping",
            &self.analytics_base_url,
            PING_PATH,
            None,
        )
        .await
    }

    pub async fn sales_funnel(&self, account: &str, payload: Value) -> Result<Value, WbError> {
        self.request(
            account,
            Method::POST,
            "analytics:/api/analytics/v3/sales-funnel/products",
            &self.analytics_base_url,
            SALES_FUNNEL_PATH,
            Some(payload),
        )
        .await
    }

    /// Drives [`Self::request`] with an arbitrary method and path so the
    /// read-only guard can be exercised. `ping` and `sales_funnel` are
    /// allowlisted by construction and cannot reach the denial branch.
    #[cfg(test)]
    pub(crate) async fn request_for_test(
        &self,
        account: &str,
        method: Method,
        path: &'static str,
    ) -> Result<Value, WbError> {
        self.request(
            account,
            method,
            "test:endpoint",
            &self.analytics_base_url,
            path,
            None,
        )
        .await
    }

    async fn request(
        &self,
        account: &str,
        method: Method,
        endpoint: &'static str,
        base_url: &str,
        path: &'static str,
        payload: Option<Value>,
    ) -> Result<Value, WbError> {
        // Enforced here, at the only point where a WB request can leave the
        // process, so the read-only guarantee does not depend on callers.
        let Some(request_class) = RequestClass::for_request(&method, path) else {
            return Err(WbError::EndpointNotAllowed {
                method,
                path: path.to_owned(),
            });
        };
        let url = format!("{base_url}{path}");
        let credentials = self
            .accounts
            .get(account)
            .ok_or_else(|| WbError::MissingCredentials(account.to_owned()))?;
        let limiter = self
            .limiters
            .get(account)
            .expect("configured WB account has a limiter");
        let _global_permit = self
            .global_in_flight
            .try_acquire()
            .map_err(|_| WbError::Overloaded)?;
        let _permit = limiter
            .in_flight
            .try_acquire()
            .map_err(|_| WbError::Overloaded)?;
        let authorization = HeaderValue::from_str(&format!("Bearer {}", credentials.token))
            .map_err(|_| WbError::Unauthorized { request_id: None })?;

        timeout(
            self.logical_timeout,
            self.request_with_retries(
                account,
                method,
                endpoint,
                request_class,
                limiter,
                url,
                authorization,
                payload,
            ),
        )
        .await
        .map_err(|_| WbError::DeadlineExceeded)?
    }

    #[allow(clippy::too_many_arguments)]
    async fn request_with_retries(
        &self,
        account: &str,
        method: Method,
        endpoint: &'static str,
        request_class: RequestClass,
        limiter: &TokenLimiter,
        url: String,
        authorization: HeaderValue,
        payload: Option<Value>,
    ) -> Result<Value, WbError> {
        let mut attempt = 1;
        loop {
            limiter
                .pace(request_class, self.policy.interval(request_class))
                .await;
            let mut request = self
                .http
                .request(method.clone(), &url)
                .header(AUTHORIZATION, authorization.clone());
            if let Some(payload) = payload.as_ref() {
                request = request.json(payload);
            }

            let started = Instant::now();
            let response = request.send().await;
            let response = match response {
                Ok(response) => response,
                Err(source) => {
                    let error = classify_transport_error(source, None);
                    let will_retry =
                        is_retriable_transport(error.kind()) && attempt < self.policy.max_attempts;
                    trace_transport_failure(
                        account, endpoint, attempt, started, &error, will_retry,
                    );
                    if will_retry {
                        sleep(retry_delay(attempt, None, self.policy)).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(error);
                }
            };
            let status = response.status();
            let request_id = extract_request_id(response.headers());
            let retry_after = parse_retry_delay(response.headers(), Utc::now());

            if let Some(delay) = retry_plan(status, attempt, retry_after, self.policy) {
                trace_response(
                    account,
                    endpoint,
                    attempt,
                    started,
                    status,
                    request_id.as_deref(),
                    None,
                    true,
                );
                drop(response);
                sleep(delay).await;
                attempt += 1;
                continue;
            }

            let result =
                decode_response(response, request_id.clone(), retry_after.duration()).await;
            let will_retry = result.as_ref().is_err_and(|error| {
                is_retriable_transport(error.kind()) && attempt < self.policy.max_attempts
            });
            trace_response(
                account,
                endpoint,
                attempt,
                started,
                status,
                request_id.as_deref(),
                result.as_ref().err(),
                will_retry,
            );
            if will_retry {
                sleep(retry_delay(attempt, None, self.policy)).await;
                attempt += 1;
                continue;
            }
            return result;
        }
    }
}

fn classify_transport_error(error: reqwest::Error, request_id: Option<String>) -> WbError {
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

async fn decode_response(
    response: Response,
    request_id: Option<String>,
    retry_after: Option<Duration>,
) -> Result<Value, WbError> {
    let status = response.status();
    if !status.is_success() {
        let diagnostic = read_body(response, MAX_ERROR_BODY_BYTES, request_id.as_deref())
            .await
            .unwrap_or_default();
        return Err(match status {
            StatusCode::UNAUTHORIZED => WbError::Unauthorized { request_id },
            StatusCode::FORBIDDEN => WbError::Forbidden { request_id },
            StatusCode::TOO_MANY_REQUESTS => WbError::RateLimited {
                request_id,
                retry_after,
            },
            _ => WbError::Api {
                status,
                request_id,
                diagnostic: String::from_utf8_lossy(&diagnostic).into_owned(),
            },
        });
    }
    let body = read_body(response, MAX_RESPONSE_BODY_BYTES, request_id.as_deref()).await?;
    serde_json::from_slice(&body).map_err(|source| WbError::InvalidJson { request_id, source })
}

async fn read_body(
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
    let mut body = Vec::new();
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

fn is_retriable(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn is_retriable_transport(kind: WbErrorKind) -> bool {
    matches!(kind, WbErrorKind::Timeout | WbErrorKind::Network)
}

fn retry_plan(
    status: StatusCode,
    attempt: usize,
    retry_after: ParsedRetryDelay,
    policy: ClientPolicy,
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

fn retry_delay(attempt: usize, server_delay: Option<Duration>, policy: ClientPolicy) -> Duration {
    server_delay.unwrap_or_else(|| {
        policy
            .base_retry_delay
            .saturating_mul(1_u32 << attempt.saturating_sub(1).min(8))
            .min(policy.max_retry_delay)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedRetryDelay {
    Absent,
    Valid(Duration),
    Invalid,
}

impl ParsedRetryDelay {
    const fn duration(self) -> Option<Duration> {
        match self {
            Self::Valid(duration) => Some(duration),
            Self::Absent | Self::Invalid => None,
        }
    }
}

fn parse_retry_delay(headers: &HeaderMap, now: DateTime<Utc>) -> ParsedRetryDelay {
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
            return DateTime::parse_from_rfc2822(value)
                .ok()
                .map(|retry_at| {
                    let seconds = retry_at
                        .with_timezone(&Utc)
                        .signed_duration_since(now)
                        .num_seconds()
                        .max(0) as u64;
                    ParsedRetryDelay::Valid(Duration::from_secs(seconds))
                })
                .unwrap_or(ParsedRetryDelay::Invalid);
        }
        return ParsedRetryDelay::Invalid;
    }
    ParsedRetryDelay::Absent
}

fn trace_transport_failure(
    account: &str,
    endpoint: &str,
    attempt: usize,
    started: Instant,
    error: &WbError,
    will_retry: bool,
) {
    let latency_ms = started.elapsed().as_millis() as u64;
    let error_kind = error.kind().code();
    warn!(
        account,
        endpoint, attempt, latency_ms, error_kind, will_retry, "WB API transport failed"
    );
}

#[allow(clippy::too_many_arguments)]
fn trace_response(
    account: &str,
    endpoint: &str,
    attempt: usize,
    started: Instant,
    status: StatusCode,
    request_id: Option<&str>,
    error: Option<&WbError>,
    will_retry: bool,
) {
    let latency_ms = started.elapsed().as_millis() as u64;
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
fn extract_request_id(headers: &reqwest::header::HeaderMap) -> Option<String> {
    ["x-request-id", "x-trace-id"]
        .into_iter()
        .find_map(|name| headers.get(name))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= MAX_REQUEST_ID_BYTES
                && value.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
                })
        })
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
    };

    use reqwest::header::{HeaderMap, HeaderValue};
    use serde_json::json;

    use super::*;
    use crate::test_support::mock_http;

    fn credentials() -> BTreeMap<String, WbCredentials> {
        BTreeMap::from([(
            "account".to_owned(),
            WbCredentials {
                token: "test-token".to_owned(),
            },
        )])
    }

    fn client(base_url: &str) -> WbClient {
        WbClient::new_for_test(Duration::from_secs(2), credentials(), base_url, base_url)
    }

    fn raw_http(
        responses: Vec<Vec<u8>>,
    ) -> (String, mpsc::Receiver<Vec<u8>>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, receiver) = mpsc::channel();
        let task = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buffer = [0_u8; 8_192];
                let count = stream.read(&mut buffer).unwrap();
                let request = buffer[..count].to_vec();
                sender.send(request).unwrap();
                stream.write_all(&response).unwrap();
            }
        });
        (format!("http://{address}"), receiver, task)
    }

    fn raw_response(status: u16, headers: &str, body: &[u8]) -> Vec<u8> {
        let reason = if status == 200 { "OK" } else { "Error" };
        let mut response = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        response
    }

    fn retrying_policy(logical_timeout: Duration) -> ClientPolicy {
        ClientPolicy {
            ping_interval: Duration::ZERO,
            sales_funnel_interval: Duration::ZERO,
            max_attempts: 3,
            base_retry_delay: Duration::ZERO,
            max_retry_delay: Duration::from_secs(1),
            logical_timeout,
        }
    }

    #[test]
    fn production_policy_is_endpoint_specific_and_has_a_total_deadline() {
        let policy = ClientPolicy::production(Duration::from_secs(5));
        assert_eq!(
            policy.interval(RequestClass::AnalyticsPing),
            Duration::from_secs(10)
        );
        assert_eq!(
            policy.interval(RequestClass::SalesFunnel),
            Duration::from_secs(20)
        );
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.logical_timeout, Duration::from_secs(10));
        assert_eq!(
            ClientPolicy::production(Duration::from_secs(300)).logical_timeout,
            MAX_LOGICAL_REQUEST_DURATION
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, PING_PATH),
            Some(RequestClass::AnalyticsPing)
        );
        assert_eq!(
            RequestClass::for_request(&Method::POST, SALES_FUNNEL_PATH),
            Some(RequestClass::SalesFunnel)
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, "/not-allowed"),
            None
        );
    }

    #[tokio::test]
    async fn exact_read_only_requests_and_success_responses() {
        let (base_url, requests) = mock_http(vec![
            (200, r#"{"Status":"OK"}"#.to_owned()),
            (200, r#"{"data":{"products":[]}}"#.to_owned()),
        ]);
        let client = client(&format!("{base_url}/"));
        assert!(client.is_configured("account"));
        assert!(!client.is_configured("missing"));

        assert_eq!(client.ping("account").await.unwrap()["Status"], "OK");
        let payload = json!({"limit": 10, "offset": 0});
        assert_eq!(
            client
                .sales_funnel("account", payload.clone())
                .await
                .unwrap()["data"]["products"],
            json!([])
        );

        let ping = requests.recv().unwrap();
        assert!(ping.starts_with("GET /ping HTTP/1.1\r\n"));
        assert!(
            ping.to_ascii_lowercase()
                .contains("authorization: bearer test-token")
        );
        let funnel = requests.recv().unwrap();
        assert!(funnel.starts_with("POST /api/analytics/v3/sales-funnel/products HTTP/1.1\r\n"));
        assert_eq!(
            serde_json::from_str::<Value>(funnel.split_once("\r\n\r\n").unwrap().1).unwrap(),
            payload
        );
    }

    #[tokio::test]
    async fn ping_targets_the_analytics_host() {
        let (analytics_base_url, requests) =
            mock_http(vec![(200, r#"{"Status":"OK"}"#.to_owned())]);
        let client = WbClient::new_for_test(
            Duration::from_secs(2),
            credentials(),
            "http://127.0.0.1:1",
            &analytics_base_url,
        );

        assert_eq!(client.ping("account").await.unwrap()["Status"], "OK");
        assert!(
            requests
                .recv()
                .unwrap()
                .starts_with("GET /ping HTTP/1.1\r\n")
        );
    }

    #[tokio::test]
    async fn same_token_aliases_share_the_exact_same_quota() {
        let accounts = BTreeMap::from([
            (
                "primary".to_owned(),
                WbCredentials {
                    token: "shared-token".to_owned(),
                },
            ),
            (
                "alias".to_owned(),
                WbCredentials {
                    token: "shared-token".to_owned(),
                },
            ),
            (
                "other".to_owned(),
                WbCredentials {
                    token: "other-token".to_owned(),
                },
            ),
        ]);
        let client = WbClient::new_for_test(
            Duration::from_secs(1),
            accounts,
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
        );
        let primary = client.limiters.get("primary").unwrap();
        let alias = client.limiters.get("alias").unwrap();
        let other = client.limiters.get("other").unwrap();
        assert!(Arc::ptr_eq(primary, alias));
        assert!(!Arc::ptr_eq(primary, other));

        let _all_shared_permits = primary
            .in_flight
            .try_acquire_many(MAX_IN_FLIGHT_REQUESTS_PER_TOKEN as u32)
            .unwrap();
        assert_eq!(
            client.ping("alias").await.unwrap_err().kind(),
            WbErrorKind::Overloaded
        );
    }

    #[tokio::test]
    async fn global_concurrency_fails_fast_across_distinct_accounts() {
        let accounts = (0..=MAX_GLOBAL_IN_FLIGHT_REQUESTS)
            .map(|index| {
                (
                    format!("account-{index}"),
                    WbCredentials {
                        token: format!("token-{index}"),
                    },
                )
            })
            .collect();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (release_sender, release_receiver) = mpsc::channel();
        let task = thread::spawn(move || {
            let mut streams = Vec::new();
            for _ in 0..MAX_GLOBAL_IN_FLIGHT_REQUESTS {
                streams.push(listener.accept().unwrap().0);
            }
            release_receiver.recv().unwrap();
            for mut stream in streams {
                let mut buffer = [0_u8; 4_096];
                let _ = stream.read(&mut buffer);
                let _ = stream.write_all(&raw_response(200, "", br#"{"Status":"OK"}"#));
            }
        });
        let client = WbClient::new_for_test(
            Duration::from_secs(3),
            accounts,
            &format!("http://{address}"),
            &format!("http://{address}"),
        );
        let mut pending = Vec::new();
        for index in 0..MAX_GLOBAL_IN_FLIGHT_REQUESTS {
            let client = client.clone();
            pending.push(tokio::spawn(async move {
                client.ping(&format!("account-{index}")).await
            }));
        }
        for _ in 0..1_000 {
            if client.global_in_flight.available_permits() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(client.global_in_flight.available_permits(), 0);
        assert_eq!(
            client
                .ping(&format!("account-{MAX_GLOBAL_IN_FLIGHT_REQUESTS}"))
                .await
                .unwrap_err()
                .kind(),
            WbErrorKind::Overloaded
        );
        release_sender.send(()).unwrap();
        for request in pending {
            assert_eq!(request.await.unwrap().unwrap()["Status"], "OK");
        }
        task.join().unwrap();
    }

    #[tokio::test]
    async fn credentials_fail_closed_before_network() {
        let empty = WbClient::new_for_test(
            Duration::from_secs(1),
            BTreeMap::new(),
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
        );
        let error = empty.ping("missing").await.unwrap_err();
        assert_eq!(error.kind(), WbErrorKind::MissingCredentials);
        assert_eq!(error.request_id(), None);

        let invalid = BTreeMap::from([(
            "account".to_owned(),
            WbCredentials {
                token: "invalid\nheader".to_owned(),
            },
        )]);
        let invalid = WbClient::new_for_test(
            Duration::from_secs(1),
            invalid,
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
        );
        assert_eq!(
            invalid.ping("account").await.unwrap_err().kind(),
            WbErrorKind::Unauthorized
        );
    }

    #[tokio::test]
    async fn upstream_statuses_are_structured_and_diagnostics_are_redacted() {
        for (status, expected) in [
            (401, WbErrorKind::Unauthorized),
            (403, WbErrorKind::Forbidden),
            (429, WbErrorKind::RateLimited),
            (500, WbErrorKind::Http),
        ] {
            let (base_url, requests, task) = raw_http(vec![raw_response(
                status,
                "x-request-id: safe-id\r\n",
                b"secret-upstream-diagnostic",
            )]);
            let error = client(&base_url).ping("account").await.unwrap_err();
            assert_eq!(error.kind(), expected);
            assert_eq!(error.request_id(), Some("safe-id"));
            assert!(!format!("{error:?}").contains("secret-upstream-diagnostic"));
            requests.recv().unwrap();
            task.join().unwrap();
        }
    }

    #[test]
    fn retry_headers_are_parsed_conservatively_and_bounded() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut headers = HeaderMap::new();
        assert_eq!(parse_retry_delay(&headers, now), ParsedRetryDelay::Absent);

        headers.insert(RETRY_AFTER, HeaderValue::from_static("7"));
        assert_eq!(
            parse_retry_delay(&headers, now),
            ParsedRetryDelay::Valid(Duration::from_secs(7))
        );
        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_static("Mon, 10 Aug 2026 00:00:05 +0000"),
        );
        assert_eq!(
            parse_retry_delay(&headers, now),
            ParsedRetryDelay::Valid(Duration::from_secs(5))
        );
        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_static("Sun, 09 Aug 2026 23:59:59 +0000"),
        );
        assert_eq!(
            parse_retry_delay(&headers, now),
            ParsedRetryDelay::Valid(Duration::ZERO)
        );
        headers.insert(RETRY_AFTER, HeaderValue::from_static("not-a-delay"));
        assert_eq!(parse_retry_delay(&headers, now), ParsedRetryDelay::Invalid);
        headers.insert(RETRY_AFTER, HeaderValue::from_bytes(&[0xff]).unwrap());
        assert_eq!(parse_retry_delay(&headers, now), ParsedRetryDelay::Invalid);

        headers.remove(RETRY_AFTER);
        headers.insert("x-ratelimit-retry", HeaderValue::from_static("8"));
        assert_eq!(
            parse_retry_delay(&headers, now),
            ParsedRetryDelay::Valid(Duration::from_secs(8))
        );
        headers.remove("x-ratelimit-retry");
        headers.insert("x-ratelimit-reset", HeaderValue::from_static("9"));
        assert_eq!(
            parse_retry_delay(&headers, now),
            ParsedRetryDelay::Valid(Duration::from_secs(9))
        );
        headers.insert("x-ratelimit-reset", HeaderValue::from_static("invalid"));
        assert_eq!(parse_retry_delay(&headers, now), ParsedRetryDelay::Invalid);

        let policy = retrying_policy(Duration::from_secs(2));
        assert_eq!(
            retry_plan(
                StatusCode::TOO_MANY_REQUESTS,
                1,
                ParsedRetryDelay::Valid(Duration::ZERO),
                policy,
            ),
            Some(Duration::ZERO)
        );
        assert_eq!(
            retry_plan(StatusCode::BAD_GATEWAY, 1, ParsedRetryDelay::Absent, policy,),
            Some(Duration::ZERO)
        );
        assert_eq!(
            retry_plan(
                StatusCode::TOO_MANY_REQUESTS,
                1,
                ParsedRetryDelay::Valid(Duration::from_secs(2)),
                policy,
            ),
            None
        );
        assert_eq!(
            retry_plan(
                StatusCode::TOO_MANY_REQUESTS,
                1,
                ParsedRetryDelay::Invalid,
                policy,
            ),
            None
        );
        assert_eq!(
            retry_plan(StatusCode::BAD_REQUEST, 1, ParsedRetryDelay::Absent, policy,),
            None
        );
        assert_eq!(
            retry_plan(
                StatusCode::SERVICE_UNAVAILABLE,
                policy.max_attempts,
                ParsedRetryDelay::Absent,
                policy,
            ),
            None
        );
        assert!(is_retriable(StatusCode::BAD_GATEWAY));
        assert!(is_retriable(StatusCode::SERVICE_UNAVAILABLE));
        assert!(is_retriable(StatusCode::GATEWAY_TIMEOUT));
        assert!(!is_retriable(StatusCode::INTERNAL_SERVER_ERROR));
    }

    #[tokio::test]
    async fn retryable_wb_statuses_recover_without_long_test_sleeps() {
        let (base_url, requests, task) = raw_http(vec![
            raw_response(
                429,
                "x-ratelimit-retry: 0\r\nx-request-id: first\r\n",
                br#"{"error":"rate"}"#,
            ),
            raw_response(
                503,
                "Retry-After: 0\r\nx-request-id: second\r\n",
                br#"{"error":"temporary"}"#,
            ),
            raw_response(200, "x-request-id: final\r\n", br#"{"Status":"OK"}"#),
        ]);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(1),
            credentials(),
            &base_url,
            retrying_policy(Duration::from_secs(2)),
        );

        assert_eq!(client.ping("account").await.unwrap()["Status"], "OK");
        for _ in 0..3 {
            requests.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        assert!(requests.try_recv().is_err());
        task.join().unwrap();
    }

    #[tokio::test]
    async fn oversized_server_retry_delay_is_never_slept_or_retried() {
        let (base_url, requests, task) = raw_http(vec![raw_response(
            429,
            "Retry-After: 2\r\nx-request-id: bounded\r\n",
            br#"{"error":"rate"}"#,
        )]);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(1),
            credentials(),
            &base_url,
            retrying_policy(Duration::from_secs(2)),
        );

        let error = client.ping("account").await.unwrap_err();
        assert!(matches!(
            error,
            WbError::RateLimited {
                retry_after: Some(delay),
                ..
            } if delay == Duration::from_secs(2)
        ));
        requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(requests.try_recv().is_err());
        task.join().unwrap();
    }

    #[tokio::test]
    async fn transport_failure_and_truncated_stream_are_retried_only_when_enabled() {
        let (base_url, requests, task) = raw_http(vec![
            Vec::new(),
            raw_response(200, "", br#"{"Status":"OK"}"#),
        ]);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(1),
            credentials(),
            &base_url,
            retrying_policy(Duration::from_secs(2)),
        );
        assert_eq!(client.ping("account").await.unwrap()["Status"], "OK");
        for _ in 0..2 {
            requests.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        task.join().unwrap();

        let truncated = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 20\r\nx-request-id: truncated-id\r\nConnection: close\r\n\r\n{}".to_vec();
        let (base_url, requests, task) = raw_http(vec![
            truncated,
            raw_response(200, "", br#"{"Status":"OK"}"#),
        ]);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(1),
            credentials(),
            &base_url,
            retrying_policy(Duration::from_secs(2)),
        );
        assert_eq!(client.ping("account").await.unwrap()["Status"], "OK");
        for _ in 0..2 {
            requests.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        task.join().unwrap();
    }

    #[tokio::test]
    async fn per_attempt_timeout_can_retry_inside_the_total_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let task = thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 4_096];
            let _ = first.read(&mut buffer);
            thread::sleep(Duration::from_millis(30));
            drop(first);
            let (mut second, _) = listener.accept().unwrap();
            let _ = second.read(&mut buffer);
            second
                .write_all(&raw_response(200, "", br#"{"Status":"OK"}"#))
                .unwrap();
        });
        let client = WbClient::new_for_test_with_policy(
            Duration::from_millis(20),
            credentials(),
            &format!("http://{address}"),
            retrying_policy(Duration::from_millis(200)),
        );
        assert_eq!(client.ping("account").await.unwrap()["Status"], "OK");
        task.join().unwrap();
    }

    #[tokio::test]
    async fn total_deadline_includes_waiting_for_the_local_quota() {
        let mut policy = ClientPolicy::immediate_single_attempt(Duration::from_millis(5));
        policy.ping_interval = Duration::from_secs(1);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(1),
            credentials(),
            "http://127.0.0.1:1",
            policy,
        );
        let limiter = client.limiters.get("account").unwrap();
        *limiter.analytics_ping.next_allowed.lock().await =
            Instant::now() + Duration::from_millis(100);

        let error = client.ping("account").await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "исчерпан общий лимит времени read-only запроса к WB API"
        );
    }

    #[test]
    fn trace_helpers_evaluate_only_safe_fields_when_enabled() {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(std::io::sink)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let transport_error = WbError::DeadlineExceeded;
            trace_transport_failure(
                "account",
                "analytics:/ping",
                1,
                Instant::now(),
                &transport_error,
                true,
            );
            trace_response(
                "account",
                "analytics:/ping",
                1,
                Instant::now(),
                StatusCode::OK,
                Some("safe-request-id"),
                None,
                false,
            );
            trace_response(
                "account",
                "analytics:/ping",
                2,
                Instant::now(),
                StatusCode::BAD_GATEWAY,
                None,
                Some(&transport_error),
                true,
            );
            trace_response(
                "account",
                "analytics:/ping",
                3,
                Instant::now(),
                StatusCode::BAD_GATEWAY,
                None,
                None,
                true,
            );
        });
    }

    #[tokio::test]
    async fn invalid_json_and_both_response_size_limits_are_enforced() {
        let declared = MAX_RESPONSE_BODY_BYTES as u64 + 1;
        let declared_oversize = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {declared}\r\nConnection: close\r\n\r\n"
        )
        .into_bytes();
        let mut streamed_oversize =
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                .to_vec();
        streamed_oversize.extend(std::iter::repeat_n(b'x', MAX_RESPONSE_BODY_BYTES + 1));
        let (base_url, requests, task) = raw_http(vec![
            raw_response(200, "x-request-id: invalid-json-id\r\n", b"not-json"),
            declared_oversize,
            streamed_oversize,
        ]);
        let client = client(&base_url);
        let invalid_json = client.ping("account").await.unwrap_err();
        assert_eq!(invalid_json.kind(), WbErrorKind::InvalidJson);
        assert_eq!(invalid_json.request_id(), Some("invalid-json-id"));
        for _ in 0..2 {
            assert_eq!(
                client.ping("account").await.unwrap_err().kind(),
                WbErrorKind::ResponseTooLarge
            );
        }
        for _ in 0..3 {
            requests.recv().unwrap();
        }
        task.join().unwrap();
    }

    #[tokio::test]
    async fn timeout_network_and_truncated_body_errors_are_distinct() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let task = thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_millis(100));
        });
        let timeout = WbClient::new_for_test(
            Duration::from_millis(10),
            credentials(),
            &format!("http://{address}"),
            &format!("http://{address}"),
        );
        assert_eq!(
            timeout.ping("account").await.unwrap_err().kind(),
            WbErrorKind::Timeout
        );
        task.join().unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let task = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 4_096];
            let _ = stream.read(&mut buffer);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 20\r\nx-request-id: body-timeout\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            thread::sleep(Duration::from_millis(50));
        });
        let body_timeout = WbClient::new_for_test_with_policy(
            Duration::from_millis(10),
            credentials(),
            &format!("http://{address}"),
            ClientPolicy::immediate_single_attempt(Duration::from_millis(100)),
        );
        let error = body_timeout.ping("account").await.unwrap_err();
        assert_eq!(error.kind(), WbErrorKind::Timeout);
        assert_eq!(error.request_id(), Some("body-timeout"));
        task.join().unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let network = WbClient::new_for_test(
            Duration::from_millis(100),
            credentials(),
            &format!("http://{address}"),
            &format!("http://{address}"),
        );
        assert_eq!(
            network.ping("account").await.unwrap_err().kind(),
            WbErrorKind::Network
        );

        let truncated = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 20\r\nx-request-id: truncated-final\r\nConnection: close\r\n\r\n{}".to_vec();
        let (base_url, requests, task) = raw_http(vec![truncated]);
        let error = client(&base_url).ping("account").await.unwrap_err();
        assert_eq!(error.kind(), WbErrorKind::Network);
        assert_eq!(error.request_id(), Some("truncated-final"));
        requests.recv().unwrap();
        task.join().unwrap();
    }

    #[tokio::test]
    async fn requests_for_one_account_overlap_instead_of_queuing_behind_each_other() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        // Accepts every connection before answering any of them, so it can only
        // reach the full count if the client really keeps requests in flight
        // together. A limiter that serialises round-trips stalls at one.
        let task = thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            let mut streams = Vec::new();
            while streams.len() < MAX_IN_FLIGHT_REQUESTS_PER_TOKEN
                && std::time::Instant::now() < deadline
            {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        streams.push(stream);
                    }
                    Err(_) => thread::sleep(Duration::from_millis(5)),
                }
            }
            let accepted = streams.len();
            for mut stream in streams {
                let mut buffer = [0_u8; 4_096];
                let _ = stream.read(&mut buffer);
                let _ = stream.write_all(&raw_response(200, "", br#"{"Status":"OK"}"#));
            }
            accepted
        });

        let client = WbClient::new_for_test(
            Duration::from_secs(5),
            credentials(),
            &format!("http://{address}"),
            &format!("http://{address}"),
        );
        let mut pending = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_REQUESTS_PER_TOKEN {
            let client = client.clone();
            pending.push(tokio::spawn(async move { client.ping("account").await }));
        }
        for request in pending {
            assert_eq!(request.await.unwrap().unwrap()["Status"], "OK");
        }
        assert_eq!(task.join().unwrap(), MAX_IN_FLIGHT_REQUESTS_PER_TOKEN);
    }

    #[tokio::test]
    async fn account_concurrency_is_bounded_and_fails_fast() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let task = thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_secs(1));
        });
        let client = WbClient::new_for_test(
            Duration::from_secs(2),
            credentials(),
            &format!("http://{address}"),
            &format!("http://{address}"),
        );
        let mut pending = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_REQUESTS_PER_TOKEN {
            let client = client.clone();
            pending.push(tokio::spawn(async move { client.ping("account").await }));
        }
        let limiter = client.limiters.get("account").unwrap();
        for _ in 0..100 {
            if limiter.in_flight.available_permits() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(limiter.in_flight.available_permits(), 0);
        assert_eq!(
            client.ping("account").await.unwrap_err().kind(),
            WbErrorKind::Overloaded
        );
        for request in pending {
            request.abort();
        }
        task.join().unwrap();
    }

    #[test]
    fn request_id_is_strictly_bounded_and_uses_safe_headers_only() {
        let mut headers = HeaderMap::new();
        headers.insert("x-trace-id", HeaderValue::from_static("trace-id"));
        assert_eq!(extract_request_id(&headers).as_deref(), Some("trace-id"));
        headers.insert("x-request-id", HeaderValue::from_static("request-id"));
        assert_eq!(extract_request_id(&headers).as_deref(), Some("request-id"));
        headers.insert("x-request-id", HeaderValue::from_static(""));
        assert_eq!(extract_request_id(&headers), None);
        headers.insert(
            "x-request-id",
            HeaderValue::from_str(&"x".repeat(MAX_REQUEST_ID_BYTES + 1)).unwrap(),
        );
        assert_eq!(extract_request_id(&headers), None);
        headers.insert("x-request-id", HeaderValue::from_bytes(&[0xff]).unwrap());
        assert_eq!(extract_request_id(&headers), None);

        // The id is echoed into tool error text, so anything an upstream could
        // use to smuggle instructions past it is dropped rather than trimmed.
        for hostile in [
            "id with spaces",
            "id\"quoted\"",
            "id;drop",
            "id\tinjected",
            "идентификатор",
            "id\\escaped",
            "id{brace}",
            "   ",
        ] {
            headers.insert("x-request-id", HeaderValue::from_str(hostile).unwrap());
            assert_eq!(extract_request_id(&headers), None, "{hostile}");
        }

        // Surrounding whitespace is stripped from an otherwise safe token.
        headers.insert("x-request-id", HeaderValue::from_static("  safe-id  "));
        assert_eq!(extract_request_id(&headers).as_deref(), Some("safe-id"));
        headers.insert(
            "x-request-id",
            HeaderValue::from_static("a1:b2/c3.d4_e5-f6"),
        );
        assert_eq!(
            extract_request_id(&headers).as_deref(),
            Some("a1:b2/c3.d4_e5-f6")
        );
    }

    #[test]
    fn credentials_and_errors_never_debug_secrets_or_diagnostics() {
        let credentials = WbCredentials {
            token: "secret-token".to_owned(),
        };
        assert_eq!(
            format!("{credentials:?}"),
            "WbCredentials { token: \"<redacted>\" }"
        );
        let error = WbError::Api {
            status: StatusCode::BAD_REQUEST,
            request_id: Some("safe-id".to_owned()),
            diagnostic: "secret-upstream-body".to_owned(),
        };
        let debug = format!("{error:?}");
        assert!(debug.contains("safe-id"));
        assert!(!debug.contains("secret-upstream-body"));
        assert_eq!(error.kind(), WbErrorKind::Http);
        assert_eq!(error.request_id(), Some("safe-id"));
    }

    #[tokio::test]
    async fn only_allowlisted_read_only_wb_requests_can_reach_the_network() {
        // Pointed at a port nothing listens on: a denied request must fail as
        // EndpointNotAllowed, never as a network error, proving nothing was sent.
        let client = WbClient::new_for_test(
            Duration::from_secs(1),
            credentials(),
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
        );

        for (method, path) in [
            // Writes, including the write counterparts of allowlisted reads.
            (Method::POST, PING_PATH),
            (Method::DELETE, PING_PATH),
            (Method::PUT, SALES_FUNNEL_PATH),
            (Method::PATCH, SALES_FUNNEL_PATH),
            // Real WB mutating endpoints.
            (Method::POST, "/api/v3/orders"),
            (Method::POST, "/content/v2/cards/update"),
            (Method::POST, "/public/api/v1/prices"),
            // Near-misses of allowlisted paths.
            (Method::GET, "/ping/"),
            (Method::GET, ""),
        ] {
            let error = client
                .request_for_test("account", method.clone(), path)
                .await
                .unwrap_err();
            assert_eq!(
                error.kind(),
                WbErrorKind::EndpointNotAllowed,
                "{method} {path}"
            );
            assert_eq!(error.request_id(), None);
            let message = error.to_string();
            assert!(message.contains(method.as_str()) && message.contains(path));
        }

        // The guard runs before credentials are looked up.
        assert_eq!(
            client
                .request_for_test("unconfigured", Method::POST, "/api/v3/orders")
                .await
                .unwrap_err()
                .kind(),
            WbErrorKind::EndpointNotAllowed
        );

        // The two allowlisted reads pass the guard and only then fail on the
        // network, which is what keeps this test honest.
        assert_eq!(
            RequestClass::for_request(&Method::GET, PING_PATH),
            Some(RequestClass::AnalyticsPing)
        );
        assert_eq!(
            RequestClass::for_request(&Method::POST, SALES_FUNNEL_PATH),
            Some(RequestClass::SalesFunnel)
        );
        assert_eq!(
            client.ping("account").await.unwrap_err().kind(),
            WbErrorKind::Network
        );
    }

    #[test]
    fn error_kind_codes_are_stable() {
        let pairs = [
            (WbErrorKind::EndpointNotAllowed, "endpoint_not_allowed"),
            (WbErrorKind::MissingCredentials, "missing_credentials"),
            (WbErrorKind::Unauthorized, "unauthorized"),
            (WbErrorKind::Forbidden, "forbidden"),
            (WbErrorKind::RateLimited, "rate_limited"),
            (WbErrorKind::Http, "upstream_http_error"),
            (WbErrorKind::Timeout, "timeout"),
            (WbErrorKind::Network, "network_error"),
            (WbErrorKind::Overloaded, "local_overloaded"),
            (WbErrorKind::InvalidJson, "invalid_json"),
            (WbErrorKind::ResponseTooLarge, "response_too_large"),
        ];
        for (kind, code) in pairs {
            assert_eq!(kind.code(), code);
        }
        for error in [
            WbError::EndpointNotAllowed {
                method: Method::POST,
                path: "/api/v3/orders".to_owned(),
            },
            WbError::Unauthorized { request_id: None },
            WbError::Forbidden { request_id: None },
            WbError::RateLimited {
                request_id: None,
                retry_after: None,
            },
            WbError::ResponseTooLarge {
                limit_bytes: 1,
                actual_bytes: None,
                request_id: None,
            },
        ] {
            assert_eq!(error.request_id(), None);
        }
    }
}
