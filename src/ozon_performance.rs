use std::{
    collections::BTreeMap,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use reqwest::{
    Client, Method, Response, StatusCode,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
    redirect::Policy,
};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, Semaphore};

use crate::config::{PerformanceCredentials, StoreId};

const PERFORMANCE_API_BASE_URL: &str = "https://api-performance.ozon.ru";
const TOKEN_PATH: &str = "/api/client/token";
pub const CAMPAIGNS_PATH: &str = "/api/client/campaign";
pub const DAILY_STATS_PATH: &str = "/api/client/statistics/daily/json";
pub const EXPENSES_PATH: &str = "/api/client/statistics/expense/json";
const MAX_RESPONSE_BODY_BYTES: usize = 2 * 1_048_576;
const MAX_TOKEN_BODY_BYTES: usize = 64 * 1_024;
const MAX_ACCESS_TOKEN_BYTES: usize = 16 * 1_024;
const MAX_TOKEN_LIFETIME: Duration = Duration::from_secs(86_400);
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TOKEN_EXPIRY_SKEW: Duration = Duration::from_secs(60);
const MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(1);
const MAX_IN_FLIGHT_PER_CLIENT: usize = 2;
const MAX_GLOBAL_IN_FLIGHT: usize = 8;
const MAX_REQUEST_ID_BYTES: usize = 128;

/// Exact business endpoints that may leave this process. The OAuth token
/// endpoint is deliberately internal and is never model-callable.
pub const READ_ONLY_ENDPOINT_ALLOWLIST: &[(Method, &str)] = &[
    (Method::GET, CAMPAIGNS_PATH),
    (Method::GET, DAILY_STATS_PATH),
    (Method::GET, EXPENSES_PATH),
];

#[must_use]
pub fn is_read_only_request_allowed(method: &Method, path: &str) -> bool {
    READ_ONLY_ENDPOINT_ALLOWLIST
        .iter()
        .any(|(allowed_method, allowed_path)| allowed_method == method && *allowed_path == path)
}

#[derive(Debug, Clone)]
pub struct CampaignsQuery {
    pub campaign_ids: Vec<u64>,
    pub adv_object_type: Option<&'static str>,
    pub state: Option<&'static str>,
    pub page: u32,
    pub page_size: u32,
}

#[derive(Debug, Clone)]
pub struct StatisticsQuery {
    pub campaign_ids: Vec<u64>,
    pub date_from: String,
    pub date_to: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PerformanceErrorKind {
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
    InvalidToken,
    ResponseTooLarge,
}

#[derive(Debug, Error)]
pub enum PerformanceClientBuildError {
    #[error("не удалось создать HTTP-клиент Ozon Performance")]
    Http(#[source] reqwest::Error),
    #[error("Performance client_id должен быть уникален для одного магазина")]
    SharedClientId,
}

impl PerformanceErrorKind {
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
            Self::InvalidToken => "invalid_token_response",
            Self::ResponseTooLarge => "response_too_large",
        }
    }
}

#[derive(Error)]
pub enum PerformanceError {
    #[error("запрос {method} {path} отсутствует в read-only allowlist Ozon Performance API")]
    EndpointNotAllowed { method: Method, path: String },
    #[error("для магазина {0} не настроены Performance Client ID и Client Secret")]
    MissingCredentials(StoreId),
    #[error("Ozon Performance API отклонил авторизацию (HTTP 401, request-id: {request_id:?})")]
    Unauthorized { request_id: Option<String> },
    #[error("доступ к Ozon Performance API запрещён (HTTP 403, request-id: {request_id:?})")]
    Forbidden { request_id: Option<String> },
    #[error(
        "Ozon Performance API ограничил частоту запросов (HTTP 429, request-id: {request_id:?})"
    )]
    RateLimited { request_id: Option<String> },
    #[error("Ozon Performance API вернул HTTP {status} (request-id: {request_id:?})")]
    Api {
        status: StatusCode,
        request_id: Option<String>,
    },
    #[error("истёк таймаут запроса к Ozon Performance API")]
    Timeout,
    #[error("сетевая ошибка при обращении к Ozon Performance API")]
    Network,
    #[error("локальный лимит параллельных запросов Ozon Performance API исчерпан")]
    Overloaded,
    #[error("Ozon Performance API вернул некорректный JSON (request-id: {request_id:?})")]
    InvalidJson {
        request_id: Option<String>,
        #[source]
        source: serde_json::Error,
    },
    #[error("Ozon Performance API вернул некорректный OAuth token response")]
    InvalidToken,
    #[error(
        "ответ Ozon Performance API превышает лимит {limit_bytes} байт (получено: {actual_bytes:?}, request-id: {request_id:?})"
    )]
    ResponseTooLarge {
        limit_bytes: usize,
        actual_bytes: Option<u64>,
        request_id: Option<String>,
    },
}

impl fmt::Debug for PerformanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PerformanceError")
            .field("kind", &self.kind())
            .field("message", &self.to_string())
            .finish_non_exhaustive()
    }
}

impl PerformanceError {
    pub const fn kind(&self) -> PerformanceErrorKind {
        match self {
            Self::EndpointNotAllowed { .. } => PerformanceErrorKind::EndpointNotAllowed,
            Self::MissingCredentials(_) => PerformanceErrorKind::MissingCredentials,
            Self::Unauthorized { .. } => PerformanceErrorKind::Unauthorized,
            Self::Forbidden { .. } => PerformanceErrorKind::Forbidden,
            Self::RateLimited { .. } => PerformanceErrorKind::RateLimited,
            Self::Api { .. } => PerformanceErrorKind::Http,
            Self::Timeout => PerformanceErrorKind::Timeout,
            Self::Network => PerformanceErrorKind::Network,
            Self::Overloaded => PerformanceErrorKind::Overloaded,
            Self::InvalidJson { .. } => PerformanceErrorKind::InvalidJson,
            Self::InvalidToken => PerformanceErrorKind::InvalidToken,
            Self::ResponseTooLarge { .. } => PerformanceErrorKind::ResponseTooLarge,
        }
    }

    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Unauthorized { request_id }
            | Self::Forbidden { request_id }
            | Self::RateLimited { request_id }
            | Self::Api { request_id, .. }
            | Self::InvalidJson { request_id, .. }
            | Self::ResponseTooLarge { request_id, .. } => request_id.as_deref(),
            Self::EndpointNotAllowed { .. }
            | Self::MissingCredentials(_)
            | Self::Timeout
            | Self::Network
            | Self::Overloaded
            | Self::InvalidToken => None,
        }
    }
}

struct CachedToken {
    value: String,
    refresh_at: Instant,
}

impl fmt::Debug for CachedToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CachedToken")
            .field("value", &"[REDACTED]")
            .field("refresh_at", &self.refresh_at)
            .finish()
    }
}

#[derive(Debug)]
struct AccountState {
    credentials: PerformanceCredentials,
    token: Mutex<Option<CachedToken>>,
    next_allowed: Mutex<Instant>,
    in_flight: Semaphore,
    statistics_in_flight: Semaphore,
}

impl AccountState {
    fn new(credentials: PerformanceCredentials) -> Self {
        Self {
            credentials,
            token: Mutex::new(None),
            next_allowed: Mutex::new(Instant::now()),
            in_flight: Semaphore::new(MAX_IN_FLIGHT_PER_CLIENT),
            statistics_in_flight: Semaphore::new(1),
        }
    }

    async fn pace(&self, interval: Duration) {
        let mut next_allowed = self.next_allowed.lock().await;
        let wait = next_allowed.saturating_duration_since(Instant::now());
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        *next_allowed = Instant::now() + interval;
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceClient {
    http: Client,
    base_url: String,
    interval: Duration,
    logical_timeout: Duration,
    accounts: Arc<BTreeMap<StoreId, Arc<AccountState>>>,
    global_in_flight: Arc<Semaphore>,
}

impl PerformanceClient {
    pub fn new(
        timeout: Duration,
        credentials: BTreeMap<StoreId, PerformanceCredentials>,
    ) -> Result<Self, PerformanceClientBuildError> {
        Self::build(
            PERFORMANCE_API_BASE_URL.to_owned(),
            timeout,
            MIN_REQUEST_INTERVAL,
            credentials,
            concat!("mcp-ozon/", env!("CARGO_PKG_VERSION")),
        )
    }

    pub fn empty(timeout: Duration) -> Self {
        Self::new(timeout, BTreeMap::new()).expect("fixed reqwest client configuration is valid")
    }

    fn build(
        base_url: String,
        timeout: Duration,
        interval: Duration,
        credentials: BTreeMap<StoreId, PerformanceCredentials>,
        user_agent: &str,
    ) -> Result<Self, PerformanceClientBuildError> {
        let http = Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(MAX_CONNECT_TIMEOUT))
            .redirect(Policy::none())
            .no_proxy()
            .user_agent(user_agent)
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(MAX_GLOBAL_IN_FLIGHT)
            .tcp_keepalive(Duration::from_secs(60))
            .http2_adaptive_window(true)
            .build()
            .map_err(PerformanceClientBuildError::Http)?;
        let mut client_ids = std::collections::BTreeSet::new();
        let mut accounts = BTreeMap::new();
        for (store, credentials) in credentials {
            if !client_ids.insert(credentials.client_id.clone()) {
                return Err(PerformanceClientBuildError::SharedClientId);
            }
            accounts.insert(store, Arc::new(AccountState::new(credentials)));
        }
        Ok(Self {
            http,
            base_url,
            interval,
            logical_timeout: timeout,
            accounts: Arc::new(accounts),
            global_in_flight: Arc::new(Semaphore::new(MAX_GLOBAL_IN_FLIGHT)),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        base_url: String,
        timeout: Duration,
        credentials: BTreeMap<StoreId, PerformanceCredentials>,
    ) -> Self {
        Self::build(
            base_url,
            timeout,
            Duration::ZERO,
            credentials,
            "mcp-ozon-test",
        )
        .unwrap()
    }

    #[must_use]
    pub fn is_configured(&self, store: &StoreId) -> bool {
        self.accounts.contains_key(store)
    }

    pub async fn campaigns(
        &self,
        store: &StoreId,
        query: CampaignsQuery,
    ) -> Result<Value, PerformanceError> {
        let mut pairs = Vec::with_capacity(query.campaign_ids.len() + 4);
        for campaign_id in query.campaign_ids {
            pairs.push(("campaignIds".to_owned(), campaign_id.to_string()));
        }
        if let Some(value) = query.adv_object_type {
            pairs.push(("advObjectType".to_owned(), value.to_owned()));
        }
        if let Some(value) = query.state {
            pairs.push(("state".to_owned(), value.to_owned()));
        }
        pairs.push(("page".to_owned(), query.page.to_string()));
        pairs.push(("pageSize".to_owned(), query.page_size.to_string()));
        self.get(store, CAMPAIGNS_PATH, pairs).await
    }

    pub async fn daily_statistics(
        &self,
        store: &StoreId,
        query: StatisticsQuery,
    ) -> Result<Value, PerformanceError> {
        self.statistics(store, DAILY_STATS_PATH, query).await
    }

    pub async fn expenses(
        &self,
        store: &StoreId,
        query: StatisticsQuery,
    ) -> Result<Value, PerformanceError> {
        self.statistics(store, EXPENSES_PATH, query).await
    }

    async fn statistics(
        &self,
        store: &StoreId,
        path: &'static str,
        query: StatisticsQuery,
    ) -> Result<Value, PerformanceError> {
        let mut pairs = Vec::with_capacity(query.campaign_ids.len() + 2);
        for campaign_id in query.campaign_ids {
            pairs.push(("campaignIds".to_owned(), campaign_id.to_string()));
        }
        pairs.push(("dateFrom".to_owned(), query.date_from));
        pairs.push(("dateTo".to_owned(), query.date_to));
        self.get(store, path, pairs).await
    }

    async fn get(
        &self,
        store: &StoreId,
        path: &'static str,
        query: Vec<(String, String)>,
    ) -> Result<Value, PerformanceError> {
        if !is_read_only_request_allowed(&Method::GET, path) {
            return Err(PerformanceError::EndpointNotAllowed {
                method: Method::GET,
                path: path.to_owned(),
            });
        }
        tokio::time::timeout(
            self.logical_timeout,
            self.get_within_deadline(store, path, query),
        )
        .await
        .map_err(|_| PerformanceError::Timeout)?
    }

    async fn get_within_deadline(
        &self,
        store: &StoreId,
        path: &'static str,
        query: Vec<(String, String)>,
    ) -> Result<Value, PerformanceError> {
        let state = self
            .accounts
            .get(store)
            .ok_or_else(|| PerformanceError::MissingCredentials(store.clone()))?;
        let _global = self
            .global_in_flight
            .try_acquire()
            .map_err(|_| PerformanceError::Overloaded)?;
        let _account = state
            .in_flight
            .try_acquire()
            .map_err(|_| PerformanceError::Overloaded)?;
        let _statistics = if matches!(path, DAILY_STATS_PATH | EXPENSES_PATH) {
            Some(
                state
                    .statistics_in_flight
                    .try_acquire()
                    .map_err(|_| PerformanceError::Overloaded)?,
            )
        } else {
            None
        };

        let token = self.access_token(state).await?;
        let response = self.send_get(state, path, &query, &token).await?;
        if response.status() != StatusCode::UNAUTHORIZED {
            return decode_json(response, MAX_RESPONSE_BODY_BYTES).await;
        }
        self.invalidate_token_if_current(state, &token).await;
        let refreshed_token = self.access_token(state).await?;
        let response = self.send_get(state, path, &query, &refreshed_token).await?;
        decode_json(response, MAX_RESPONSE_BODY_BYTES).await
    }

    async fn send_get(
        &self,
        state: &AccountState,
        path: &'static str,
        query: &[(String, String)],
        token: &str,
    ) -> Result<Response, PerformanceError> {
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| PerformanceError::InvalidToken)?;
        authorization.set_sensitive(true);
        state.pace(self.interval).await;
        self.http
            .get(format!("{}{path}", self.base_url))
            .header(AUTHORIZATION, authorization)
            .query(query)
            .send()
            .await
            .map_err(classify_transport)
    }

    async fn invalidate_token_if_current(&self, state: &AccountState, used_token: &str) {
        let mut cache = state.token.lock().await;
        if cache
            .as_ref()
            .is_some_and(|cached| cached.value == used_token)
        {
            *cache = None;
        }
    }

    async fn access_token(&self, state: &AccountState) -> Result<String, PerformanceError> {
        let mut cache = state.token.lock().await;
        if let Some(token) = cache.as_ref()
            && token.refresh_at > Instant::now()
        {
            return Ok(token.value.clone());
        }
        let response = self
            .http
            .post(format!("{}{}", self.base_url, TOKEN_PATH))
            .json(&json!({
                "client_id": state.credentials.client_id,
                "client_secret": state.credentials.client_secret,
                "grant_type": "client_credentials",
            }))
            .send()
            .await
            .map_err(classify_transport)?;
        let request_id = safe_request_id(response.headers());
        let status = response.status();
        if !status.is_success() {
            return Err(classify_status(status, request_id));
        }
        let bytes = read_body(response, MAX_TOKEN_BODY_BYTES, request_id.clone()).await?;
        let token: TokenResponse = serde_json::from_slice(&bytes)
            .map_err(|source| PerformanceError::InvalidJson { request_id, source })?;
        if token.access_token.is_empty()
            || token.access_token.len() > MAX_ACCESS_TOKEN_BYTES
            || token.expires_in == 0
            || Duration::from_secs(token.expires_in) > MAX_TOKEN_LIFETIME
            || !token.token_type.eq_ignore_ascii_case("bearer")
        {
            return Err(PerformanceError::InvalidToken);
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {}", token.access_token))
            .map_err(|_| PerformanceError::InvalidToken)?;
        authorization.set_sensitive(true);
        let lifetime = Duration::from_secs(token.expires_in);
        let refresh_after = lifetime.saturating_sub(TOKEN_EXPIRY_SKEW);
        let refresh_at = Instant::now()
            .checked_add(refresh_after)
            .ok_or(PerformanceError::InvalidToken)?;
        let value = token.access_token;
        *cache = Some(CachedToken {
            value: value.clone(),
            refresh_at,
        });
        Ok(value)
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
}

fn classify_transport(error: reqwest::Error) -> PerformanceError {
    if error.is_timeout() {
        PerformanceError::Timeout
    } else {
        PerformanceError::Network
    }
}

fn classify_status(status: StatusCode, request_id: Option<String>) -> PerformanceError {
    match status {
        StatusCode::UNAUTHORIZED => PerformanceError::Unauthorized { request_id },
        StatusCode::FORBIDDEN => PerformanceError::Forbidden { request_id },
        StatusCode::TOO_MANY_REQUESTS => PerformanceError::RateLimited { request_id },
        _ => PerformanceError::Api { status, request_id },
    }
}

async fn decode_json(response: Response, limit: usize) -> Result<Value, PerformanceError> {
    let request_id = safe_request_id(response.headers());
    let status = response.status();
    if !status.is_success() {
        return Err(classify_status(status, request_id));
    }
    let bytes = read_body(response, limit, request_id.clone()).await?;
    serde_json::from_slice(&bytes)
        .map_err(|source| PerformanceError::InvalidJson { request_id, source })
}

async fn read_body(
    mut response: Response,
    limit: usize,
    request_id: Option<String>,
) -> Result<Vec<u8>, PerformanceError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(PerformanceError::ResponseTooLarge {
            limit_bytes: limit,
            actual_bytes: response.content_length(),
            request_id,
        });
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(classify_transport)? {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(PerformanceError::ResponseTooLarge {
                limit_bytes: limit,
                actual_bytes: None,
                request_id,
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn safe_request_id(headers: &HeaderMap) -> Option<String> {
    ["x-o3-trace-id", "x-request-id"]
        .iter()
        .find_map(|name| headers.get(*name))
        .and_then(|value| value.to_str().ok())
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
    use super::*;
    use crate::test_support::mock_http;
    use std::{
        collections::BTreeMap,
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    fn credentials() -> BTreeMap<StoreId, PerformanceCredentials> {
        BTreeMap::from([(
            StoreId::from("shop"),
            PerformanceCredentials {
                client_id: "performance-client".to_owned(),
                client_secret: "performance-secret".to_owned(),
            },
        )])
    }

    fn token(expires_in: u64) -> String {
        json!({
            "access_token": "test-access-token",
            "token_type": "Bearer",
            "expires_in": expires_in,
        })
        .to_string()
    }

    fn campaigns_query() -> CampaignsQuery {
        CampaignsQuery {
            campaign_ids: vec![11, 22],
            adv_object_type: Some("SKU"),
            state: Some("CAMPAIGN_STATE_RUNNING"),
            page: 2,
            page_size: 10,
        }
    }

    fn statistics_query() -> StatisticsQuery {
        StatisticsQuery {
            campaign_ids: vec![11, 22],
            date_from: "2026-08-01".to_owned(),
            date_to: "2026-08-09".to_owned(),
        }
    }

    #[tokio::test]
    async fn exact_read_only_contracts_reuse_one_cached_token() {
        let (base_url, requests) = mock_http(vec![
            (200, token(1_800)),
            (200, json!({"list": []}).to_string()),
            (200, json!({"rows": []}).to_string()),
            (200, json!({"rows": []}).to_string()),
        ]);
        let client =
            PerformanceClient::new_for_test(base_url, Duration::from_secs(3), credentials());
        let store = StoreId::from("shop");

        assert_eq!(
            client.campaigns(&store, campaigns_query()).await.unwrap(),
            json!({"list": []})
        );
        assert_eq!(
            client
                .daily_statistics(&store, statistics_query())
                .await
                .unwrap(),
            json!({"rows": []})
        );
        assert_eq!(
            client.expenses(&store, statistics_query()).await.unwrap(),
            json!({"rows": []})
        );
        let debug = format!("{client:?}");
        assert!(!debug.contains("performance-secret"));
        assert!(!debug.contains("test-access-token"));

        let auth = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(auth.starts_with("POST /api/client/token HTTP/1.1\r\n"));
        assert!(auth.contains("\"grant_type\":\"client_credentials\""));
        assert!(auth.contains("\"client_id\":\"performance-client\""));
        assert!(auth.contains("\"client_secret\":\"performance-secret\""));

        let campaigns = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(campaigns.starts_with(
            "GET /api/client/campaign?campaignIds=11&campaignIds=22&advObjectType=SKU&state=CAMPAIGN_STATE_RUNNING&page=2&pageSize=10 HTTP/1.1\r\n"
        ));
        assert!(
            campaigns
                .to_ascii_lowercase()
                .contains("authorization: bearer test-access-token")
        );

        let daily = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(daily.starts_with(
            "GET /api/client/statistics/daily/json?campaignIds=11&campaignIds=22&dateFrom=2026-08-01&dateTo=2026-08-09 HTTP/1.1\r\n"
        ));
        let expenses = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(expenses.starts_with(
            "GET /api/client/statistics/expense/json?campaignIds=11&campaignIds=22&dateFrom=2026-08-01&dateTo=2026-08-09 HTTP/1.1\r\n"
        ));
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn data_unauthorized_refreshes_token_once_and_replays_get() {
        let (base_url, requests) = mock_http(vec![
            (200, token(1_800)),
            (401, "{}".to_owned()),
            (200, token(1_800)),
            (200, json!({"list": [1]}).to_string()),
        ]);
        let client =
            PerformanceClient::new_for_test(base_url, Duration::from_secs(3), credentials());
        let result = client
            .campaigns(&StoreId::from("shop"), campaigns_query())
            .await
            .unwrap();
        assert_eq!(result, json!({"list": [1]}));
        let captured: Vec<_> = (0..4)
            .map(|_| requests.recv_timeout(Duration::from_secs(1)).unwrap())
            .collect();
        assert!(captured[0].starts_with("POST /api/client/token"));
        assert!(captured[1].starts_with("GET /api/client/campaign?"));
        assert!(captured[2].starts_with("POST /api/client/token"));
        assert!(captured[3].starts_with("GET /api/client/campaign?"));
    }

    #[tokio::test]
    async fn expired_short_lived_token_is_not_reused() {
        let (base_url, requests) = mock_http(vec![
            (200, token(1)),
            (200, "{}".to_owned()),
            (200, token(1)),
            (200, "{}".to_owned()),
        ]);
        let client =
            PerformanceClient::new_for_test(base_url, Duration::from_secs(3), credentials());
        let store = StoreId::from("shop");
        client.campaigns(&store, campaigns_query()).await.unwrap();
        client.campaigns(&store, campaigns_query()).await.unwrap();
        let captured: Vec<_> = (0..4)
            .map(|_| requests.recv_timeout(Duration::from_secs(1)).unwrap())
            .collect();
        assert_eq!(
            captured
                .iter()
                .filter(|request| request.starts_with("POST /api/client/token"))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn missing_and_mutating_endpoints_fail_before_network() {
        let client = PerformanceClient::new_for_test(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_millis(50),
            BTreeMap::new(),
        );
        let denied = [
            TOKEN_PATH,
            "/api/client/campaign/all_sku_promo/activate",
            "/api/client/campaign/all_sku_promo/deactivate",
            "/api/client/campaign/all_sku_promo/set_bid",
            "/api/client/campaign/1/activate",
            "/api/client/campaign/1/v2/activate",
            "/api/client/campaign/1/deactivate",
            "/api/client/campaign/1/v2/deactivate",
            "/api/client/statistics",
            "/api/client/campaign/",
            "/api/client//campaign",
            "/api/client/../campaign",
            "/api/client/%63ampaign",
            "/API/client/campaign",
        ];
        for path in denied {
            let error = client
                .get(&StoreId::from("missing"), path, Vec::new())
                .await
                .expect_err("endpoint must be denied");
            assert_eq!(error.kind(), PerformanceErrorKind::EndpointNotAllowed);
        }
        let missing = client
            .campaigns(&StoreId::from("missing"), campaigns_query())
            .await
            .expect_err("credentials must be required");
        assert_eq!(missing.kind(), PerformanceErrorKind::MissingCredentials);
        assert!(!is_read_only_request_allowed(&Method::POST, CAMPAIGNS_PATH));
    }

    #[tokio::test]
    async fn oauth_and_data_errors_are_structured_and_redacted() {
        for (status, expected) in [
            (401, PerformanceErrorKind::Unauthorized),
            (403, PerformanceErrorKind::Forbidden),
            (429, PerformanceErrorKind::RateLimited),
            (500, PerformanceErrorKind::Http),
        ] {
            let (base_url, _requests) = mock_http(vec![(status, "secret-body".to_owned())]);
            let client =
                PerformanceClient::new_for_test(base_url, Duration::from_secs(3), credentials());
            let error = client
                .campaigns(&StoreId::from("shop"), campaigns_query())
                .await
                .expect_err("status must fail");
            assert_eq!(error.kind(), expected);
            assert!(!format!("{error:?}").contains("secret-body"));
            assert!(!error.to_string().contains("performance-secret"));
        }
    }

    #[tokio::test]
    async fn invalid_token_and_data_bodies_are_rejected() {
        for body in [
            "not-json".to_owned(),
            json!({"access_token":"", "token_type":"Bearer", "expires_in":1800}).to_string(),
            json!({"access_token":"token", "token_type":"Basic", "expires_in":1800}).to_string(),
            json!({"access_token":"token", "token_type":"Bearer", "expires_in":0}).to_string(),
            json!({"access_token":"token", "token_type":"Bearer", "expires_in":MAX_TOKEN_LIFETIME.as_secs() + 1}).to_string(),
            json!({"access_token":"bad\ntoken", "token_type":"Bearer", "expires_in":1800}).to_string(),
            json!({"access_token":"x".repeat(MAX_ACCESS_TOKEN_BYTES + 1), "token_type":"Bearer", "expires_in":1800}).to_string(),
        ] {
            let (base_url, _requests) = mock_http(vec![(200, body)]);
            let client = PerformanceClient::new_for_test(
                base_url,
                Duration::from_secs(3),
                credentials(),
            );
            assert!(
                client
                    .campaigns(&StoreId::from("shop"), campaigns_query())
                    .await
                    .is_err()
            );
        }

        let (base_url, _requests) =
            mock_http(vec![(200, token(1_800)), (200, "not-json".to_owned())]);
        let client =
            PerformanceClient::new_for_test(base_url, Duration::from_secs(3), credentials());
        let error = client
            .campaigns(&StoreId::from("shop"), campaigns_query())
            .await
            .expect_err("invalid data JSON must fail");
        assert_eq!(error.kind(), PerformanceErrorKind::InvalidJson);
    }

    #[tokio::test]
    async fn response_size_timeout_network_and_overload_are_bounded() {
        let oversized = "x".repeat(MAX_TOKEN_BODY_BYTES + 1);
        let (base_url, _requests) = mock_http(vec![(200, oversized)]);
        let client =
            PerformanceClient::new_for_test(base_url, Duration::from_secs(3), credentials());
        assert_eq!(
            client
                .campaigns(&StoreId::from("shop"), campaigns_query())
                .await
                .expect_err("oversized response must fail")
                .kind(),
            PerformanceErrorKind::ResponseTooLarge
        );

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 4096];
            let _ = stream.read(&mut buffer);
            let body = "x".repeat(MAX_TOKEN_BODY_BYTES + 1);
            let response = format!(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        let client = PerformanceClient::new_for_test(
            format!("http://{address}"),
            Duration::from_secs(3),
            credentials(),
        );
        assert_eq!(
            client
                .campaigns(&StoreId::from("shop"), campaigns_query())
                .await
                .expect_err("chunked oversized token response must fail")
                .kind(),
            PerformanceErrorKind::ResponseTooLarge
        );

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 4096];
            let _ = stream.read(&mut buffer);
            thread::sleep(Duration::from_millis(100));
            let _ = stream.write_all(b"");
        });
        let client = PerformanceClient::new_for_test(
            format!("http://{address}"),
            Duration::from_millis(20),
            credentials(),
        );
        assert_eq!(
            client
                .campaigns(&StoreId::from("shop"), campaigns_query())
                .await
                .expect_err("timed out response must fail")
                .kind(),
            PerformanceErrorKind::Timeout
        );

        let client = PerformanceClient::new_for_test(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_millis(30),
            credentials(),
        );
        assert_eq!(
            client
                .campaigns(&StoreId::from("shop"), campaigns_query())
                .await
                .expect_err("network failure must be classified")
                .kind(),
            PerformanceErrorKind::Network
        );

        let client = PerformanceClient::new_for_test(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_millis(30),
            credentials(),
        );
        let state = client.accounts.get(&StoreId::from("shop")).unwrap();
        let _first = state.in_flight.acquire().await.unwrap();
        let _second = state.in_flight.acquire().await.unwrap();
        assert_eq!(
            client
                .campaigns(&StoreId::from("shop"), campaigns_query())
                .await
                .expect_err("local overload must fail fast")
                .kind(),
            PerformanceErrorKind::Overloaded
        );

        drop(_first);
        drop(_second);
        let _statistics = state.statistics_in_flight.acquire().await.unwrap();
        assert_eq!(
            client
                .daily_statistics(&StoreId::from("shop"), statistics_query())
                .await
                .expect_err("a second statistics export must fail fast")
                .kind(),
            PerformanceErrorKind::Overloaded
        );
    }

    #[tokio::test]
    async fn redirects_are_not_followed_and_compressed_bodies_stay_bounded() {
        use flate2::{Compression, write::GzEncoder};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            let token_body = token(1_800);
            for response in [
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    token_body.len(),
                    token_body
                )
                .into_bytes(),
                b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/token-leak\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
            ] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buffer = [0_u8; 4096];
                let _ = stream.read(&mut buffer);
                stream.write_all(&response).unwrap();
            }
        });
        let client = PerformanceClient::new_for_test(
            format!("http://{address}"),
            Duration::from_secs(3),
            credentials(),
        );
        assert_eq!(
            client
                .campaigns(&StoreId::from("shop"), campaigns_query())
                .await
                .expect_err("redirect must be returned, not followed")
                .kind(),
            PerformanceErrorKind::Http
        );

        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        encoder
            .write_all(&vec![b'0'; MAX_RESPONSE_BODY_BYTES + 1])
            .unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(compressed.len() < MAX_TOKEN_BODY_BYTES);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            let token_body = token(1_800);
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 4096];
            let _ = stream.read(&mut buffer);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                token_body.len(),
                token_body
            )
            .unwrap();

            let (mut stream, _) = listener.accept().unwrap();
            let _ = stream.read(&mut buffer);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                compressed.len()
            )
            .unwrap();
            stream.write_all(&compressed).unwrap();
        });
        let client = PerformanceClient::new_for_test(
            format!("http://{address}"),
            Duration::from_secs(5),
            credentials(),
        );
        assert_eq!(
            client
                .campaigns(&StoreId::from("shop"), campaigns_query())
                .await
                .expect_err("decompressed body must remain bounded")
                .kind(),
            PerformanceErrorKind::ResponseTooLarge
        );
    }

    #[tokio::test]
    async fn logical_deadline_covers_pacing_and_token_invalidation_is_generation_safe() {
        let client = PerformanceClient::new_for_test(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_millis(20),
            credentials(),
        );
        let state = client.accounts.get(&StoreId::from("shop")).unwrap();
        *state.token.lock().await = Some(CachedToken {
            value: "new-token".to_owned(),
            refresh_at: Instant::now() + Duration::from_secs(60),
        });
        client.invalidate_token_if_current(state, "old-token").await;
        assert_eq!(
            state
                .token
                .lock()
                .await
                .as_ref()
                .map(|token| token.value.as_str()),
            Some("new-token")
        );

        *state.next_allowed.lock().await = Instant::now() + Duration::from_secs(1);
        assert_eq!(
            client
                .campaigns(&StoreId::from("shop"), campaigns_query())
                .await
                .expect_err("logical deadline must include pacing")
                .kind(),
            PerformanceErrorKind::Timeout
        );

        client.invalidate_token_if_current(state, "new-token").await;
        assert!(state.token.lock().await.is_none());
    }

    #[test]
    fn credentials_request_ids_allowlist_and_error_codes_are_safe() {
        let credentials = PerformanceCredentials {
            client_id: "id-secret".to_owned(),
            client_secret: "api-secret".to_owned(),
        };
        let debug = format!("{credentials:?}");
        assert!(!debug.contains("id-secret"));
        assert!(!debug.contains("api-secret"));

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("safe/id:1"));
        assert_eq!(safe_request_id(&headers).as_deref(), Some("safe/id:1"));
        headers.insert("x-o3-trace-id", HeaderValue::from_static("bad value"));
        assert_eq!(safe_request_id(&headers), None);
        headers.insert(
            "x-o3-trace-id",
            HeaderValue::from_str(&"x".repeat(MAX_REQUEST_ID_BYTES + 1)).unwrap(),
        );
        assert_eq!(safe_request_id(&headers), None);

        assert_eq!(READ_ONLY_ENDPOINT_ALLOWLIST.len(), 3);
        for (method, path) in READ_ONLY_ENDPOINT_ALLOWLIST {
            assert_eq!(*method, Method::GET);
            assert!(is_read_only_request_allowed(method, path));
        }
        for (kind, code) in [
            (
                PerformanceErrorKind::EndpointNotAllowed,
                "endpoint_not_allowed",
            ),
            (
                PerformanceErrorKind::MissingCredentials,
                "missing_credentials",
            ),
            (PerformanceErrorKind::Unauthorized, "unauthorized"),
            (PerformanceErrorKind::Forbidden, "forbidden"),
            (PerformanceErrorKind::RateLimited, "rate_limited"),
            (PerformanceErrorKind::Http, "upstream_http_error"),
            (PerformanceErrorKind::Timeout, "timeout"),
            (PerformanceErrorKind::Network, "network_error"),
            (PerformanceErrorKind::Overloaded, "local_overloaded"),
            (PerformanceErrorKind::InvalidJson, "invalid_json"),
            (PerformanceErrorKind::InvalidToken, "invalid_token_response"),
            (PerformanceErrorKind::ResponseTooLarge, "response_too_large"),
        ] {
            assert_eq!(kind.code(), code);
        }
        assert_eq!(
            PerformanceError::InvalidToken.kind(),
            PerformanceErrorKind::InvalidToken
        );

        let json_error = serde_json::from_str::<Value>("not-json").unwrap_err();
        for error in [
            PerformanceError::Unauthorized {
                request_id: Some("one".to_owned()),
            },
            PerformanceError::Forbidden {
                request_id: Some("one".to_owned()),
            },
            PerformanceError::RateLimited {
                request_id: Some("one".to_owned()),
            },
            PerformanceError::Api {
                status: StatusCode::BAD_GATEWAY,
                request_id: Some("one".to_owned()),
            },
            PerformanceError::InvalidJson {
                request_id: Some("one".to_owned()),
                source: json_error,
            },
            PerformanceError::ResponseTooLarge {
                limit_bytes: 1,
                actual_bytes: None,
                request_id: Some("one".to_owned()),
            },
        ] {
            assert_eq!(error.request_id(), Some("one"));
        }
        for error in [
            PerformanceError::EndpointNotAllowed {
                method: Method::POST,
                path: TOKEN_PATH.to_owned(),
            },
            PerformanceError::MissingCredentials(StoreId::from("missing")),
            PerformanceError::Timeout,
            PerformanceError::Network,
            PerformanceError::Overloaded,
            PerformanceError::InvalidToken,
        ] {
            assert_eq!(error.request_id(), None);
        }
    }

    #[test]
    fn shared_client_id_is_rejected_at_the_client_boundary() {
        let error = PerformanceClient::build(
            "https://example.invalid".to_owned(),
            Duration::from_secs(1),
            Duration::ZERO,
            BTreeMap::new(),
            "invalid\nuser-agent",
        )
        .expect_err("invalid HTTP client configuration must fail");
        assert!(matches!(error, PerformanceClientBuildError::Http(_)));

        let shared = PerformanceCredentials {
            client_id: "same".to_owned(),
            client_secret: "secret".to_owned(),
        };
        let error = PerformanceClient::new(
            Duration::from_secs(1),
            BTreeMap::from([
                (StoreId::from("one"), shared.clone()),
                (StoreId::from("two"), shared),
            ]),
        )
        .expect_err("one Performance principal cannot cross store ACLs");
        assert!(matches!(error, PerformanceClientBuildError::SharedClientId));

        let client = PerformanceClient::new_for_test(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
            credentials(),
        );
        assert!(client.is_configured(&StoreId::from("shop")));
        assert!(
            !PerformanceClient::empty(Duration::from_secs(1)).is_configured(&StoreId::from("one"))
        );
    }
}
