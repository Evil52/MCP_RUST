use std::{
    collections::BTreeMap,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use reqwest::{
    Client, Method, Proxy, Response, StatusCode,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, Semaphore, SemaphorePermit};

use crate::config::{PerformanceCredentials, StoreId};

const PERFORMANCE_API_BASE_URL: &str = "https://api-performance.ozon.ru";
const TOKEN_PATH: &str = "/api/client/token";
pub const CAMPAIGNS_PATH: &str = "/api/client/campaign";
pub const DAILY_STATS_PATH: &str = "/api/client/statistics/daily/json";
pub const EXPENSES_PATH: &str = "/api/client/statistics/expense/json";
pub const LIMITS_PATH: &str = "/api/client/limits/list";
pub const PRODUCT_SKU_STATS_PATH: &str = "/api/client/statistics/products/sku";
pub const CAMPAIGN_OBJECTS_PATH_TEMPLATE: &str = "/api/client/campaign/{campaignId}/objects";
pub const CAMPAIGN_PRODUCTS_PATH_TEMPLATE: &str = "/api/client/campaign/{campaignId}/v2/products";
const CAMPAIGN_RESOURCE_PREFIX: &str = "/api/client/campaign/";
const MAX_RESPONSE_BODY_BYTES: usize = 2 * 1_048_576;
const MAX_TOKEN_BODY_BYTES: usize = 64 * 1_024;
const MAX_ACCESS_TOKEN_BYTES: usize = 16 * 1_024;
const MAX_TOKEN_LIFETIME: Duration = Duration::from_hours(24);
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TOKEN_EXPIRY_SKEW: Duration = Duration::from_secs(60);
/// How long a failed token request suppresses further attempts.
///
/// The OAuth endpoint sits outside the pacing gate and both in-flight
/// semaphores, because holding scarce capacity while a single-flight refresh
/// waits would be worse. That exemption is only safe with a cooldown: without
/// one, a token endpoint returning 429 or 5xx is re-attempted once per API
/// call for as long as the failure lasts.
const TOKEN_FAILURE_COOLDOWN: Duration = Duration::from_secs(30);
const MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(1);
const MAX_IN_FLIGHT_PER_CLIENT: usize = 2;
const MAX_GLOBAL_IN_FLIGHT: usize = 8;
const MAX_REQUEST_ID_BYTES: usize = 128;

/// Exact fixed business endpoints that may leave this process. The two
/// campaign-ID routes are admitted separately by a structural matcher. The
/// OAuth token endpoint is deliberately internal and is never model-callable.
pub const READ_ONLY_ENDPOINT_ALLOWLIST: &[(Method, &str)] = &[
    (Method::GET, CAMPAIGNS_PATH),
    (Method::GET, DAILY_STATS_PATH),
    (Method::GET, EXPENSES_PATH),
    (Method::GET, LIMITS_PATH),
    (Method::POST, PRODUCT_SKU_STATS_PATH),
];

#[must_use]
pub fn is_read_only_request_allowed(method: &Method, path: &str) -> bool {
    READ_ONLY_ENDPOINT_ALLOWLIST
        .iter()
        .any(|(allowed_method, allowed_path)| allowed_method == method && *allowed_path == path)
        || (*method == Method::GET && is_allowed_campaign_resource_path(path))
}

/// Matches only the two read-only campaign routes whose identifier is dynamic.
///
/// Requiring a canonical, positive `u64` segment and an exact suffix prevents
/// path traversal, percent-encoded aliases and mutating sub-routes from being
/// smuggled through the dynamic part of the allowlist.
fn is_allowed_campaign_resource_path(path: &str) -> bool {
    let Some(remainder) = path.strip_prefix(CAMPAIGN_RESOURCE_PREFIX) else {
        return false;
    };
    let Some((campaign_id, resource)) = remainder.split_once('/') else {
        return false;
    };
    let Ok(parsed_campaign_id) = campaign_id.parse::<u64>() else {
        return false;
    };
    if parsed_campaign_id == 0 || parsed_campaign_id.to_string() != campaign_id {
        return false;
    }
    matches!(resource, "objects" | "v2/products")
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

#[derive(Debug, Clone)]
pub struct CampaignProductsQuery {
    pub page: u64,
    pub page_size: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkuStatisticsQuery {
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
    TokenUnavailable,
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
    #[must_use]
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
            Self::TokenUnavailable => "token_endpoint_cooldown",
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
        "запрос токена Ozon Performance API отложен после недавней ошибки \
         ({previous})"
    )]
    TokenUnavailable { previous: &'static str },
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
    #[must_use]
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
            Self::TokenUnavailable { .. } => PerformanceErrorKind::TokenUnavailable,
            Self::ResponseTooLarge { .. } => PerformanceErrorKind::ResponseTooLarge,
        }
    }

    #[must_use]
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
            | Self::InvalidToken
            | Self::TokenUnavailable { .. } => None,
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
/// Suppresses token requests for a while after one fails.
struct TokenCooldown {
    until: Instant,
    /// The stable code of the failure that opened the cooldown, reported to
    /// callers refused during it so the cause is never guessed at.
    previous: &'static str,
}

#[derive(Debug, Default)]
struct TokenSlot {
    cached: Option<CachedToken>,
    cooldown: Option<TokenCooldown>,
}

#[derive(Debug)]
struct AccountState {
    credentials: PerformanceCredentials,
    token: Mutex<TokenSlot>,
    next_allowed: Mutex<Instant>,
    in_flight: Semaphore,
    statistics_in_flight: Semaphore,
}

impl AccountState {
    fn new(credentials: PerformanceCredentials) -> Self {
        Self {
            credentials,
            token: Mutex::new(TokenSlot::default()),
            next_allowed: Mutex::new(Instant::now()),
            in_flight: Semaphore::new(MAX_IN_FLIGHT_PER_CLIENT),
            statistics_in_flight: Semaphore::new(1),
        }
    }

    async fn wait_until_ready(&self) {
        loop {
            let wait = {
                let next_allowed = self.next_allowed.lock().await;
                next_allowed.saturating_duration_since(Instant::now())
            };
            if wait.is_zero() {
                return;
            }
            tokio::time::sleep(wait).await;
        }
    }

    async fn try_claim_request_slot(&self, interval: Duration) -> bool {
        let mut next_allowed = self.next_allowed.lock().await;
        let now = Instant::now();
        if *next_allowed > now {
            return false;
        }
        *next_allowed = now + interval;
        true
    }
}

struct RequestPermits<'a> {
    _global: SemaphorePermit<'a>,
    _account: SemaphorePermit<'a>,
    _statistics: Option<SemaphorePermit<'a>>,
}

enum RequestAttempt {
    Complete(Value),
    Unauthorized { request_id: Option<String> },
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
            None,
        )
    }

    /// Builds a client using one deployment-owned HTTPS forward proxy.
    ///
    /// Ambient proxy variables remain disabled. This constructor is reserved
    /// for the isolated report collector whose network namespace can reach
    /// only the fixed egress gateway.
    pub fn new_with_https_proxy(
        timeout: Duration,
        credentials: BTreeMap<StoreId, PerformanceCredentials>,
        proxy_url: &str,
    ) -> Result<Self, PerformanceClientBuildError> {
        Self::build(
            PERFORMANCE_API_BASE_URL.to_owned(),
            timeout,
            MIN_REQUEST_INTERVAL,
            credentials,
            concat!("mcp-ozon/", env!("CARGO_PKG_VERSION")),
            Some(proxy_url),
        )
    }

    /// Builds an unconfigured client with the fixed production HTTP settings.
    ///
    /// # Panics
    ///
    /// Panics if `reqwest` rejects the crate's fixed HTTP client settings.
    #[must_use]
    pub fn empty(timeout: Duration) -> Self {
        Self::new(timeout, BTreeMap::new()).expect("fixed reqwest client configuration is valid")
    }

    fn build(
        base_url: String,
        timeout: Duration,
        interval: Duration,
        credentials: BTreeMap<StoreId, PerformanceCredentials>,
        user_agent: &str,
        explicit_https_proxy: Option<&str>,
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
            .http2_adaptive_window(true);
        let http = match explicit_https_proxy {
            Some(proxy_url) => {
                http.proxy(Proxy::https(proxy_url).map_err(PerformanceClientBuildError::Http)?)
            }
            None => http,
        }
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
            None,
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
            pairs.push(("campaignIds", campaign_id.to_string()));
        }
        if let Some(value) = query.adv_object_type {
            pairs.push(("advObjectType", value.to_owned()));
        }
        if let Some(value) = query.state {
            pairs.push(("state", value.to_owned()));
        }
        pairs.push(("page", query.page.to_string()));
        pairs.push(("pageSize", query.page_size.to_string()));
        self.get(store, CAMPAIGNS_PATH, pairs).await
    }

    pub async fn limits(&self, store: &StoreId) -> Result<Value, PerformanceError> {
        self.get(store, LIMITS_PATH, Vec::new()).await
    }

    pub async fn campaign_objects(
        &self,
        store: &StoreId,
        campaign_id: u64,
    ) -> Result<Value, PerformanceError> {
        let path = format!("{CAMPAIGN_RESOURCE_PREFIX}{campaign_id}/objects");
        self.get(store, &path, Vec::new()).await
    }

    pub async fn campaign_products(
        &self,
        store: &StoreId,
        campaign_id: u64,
        query: CampaignProductsQuery,
    ) -> Result<Value, PerformanceError> {
        let path = format!("{CAMPAIGN_RESOURCE_PREFIX}{campaign_id}/v2/products");
        self.get(
            store,
            &path,
            vec![
                ("page", query.page.to_string()),
                ("pageSize", query.page_size.to_string()),
            ],
        )
        .await
    }

    pub async fn sku_statistics(
        &self,
        store: &StoreId,
        query: SkuStatisticsQuery,
    ) -> Result<Value, PerformanceError> {
        self.post(store, PRODUCT_SKU_STATS_PATH, json!(query)).await
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
            pairs.push(("campaignIds", campaign_id.to_string()));
        }
        pairs.push(("dateFrom", query.date_from));
        pairs.push(("dateTo", query.date_to));
        self.get(store, path, pairs).await
    }

    async fn get(
        &self,
        store: &StoreId,
        path: &str,
        query: Vec<(&'static str, String)>,
    ) -> Result<Value, PerformanceError> {
        self.request(Method::GET, store, path, query, None).await
    }

    async fn post(
        &self,
        store: &StoreId,
        path: &str,
        body: Value,
    ) -> Result<Value, PerformanceError> {
        self.request(Method::POST, store, path, Vec::new(), Some(body))
            .await
    }

    async fn request(
        &self,
        method: Method,
        store: &StoreId,
        path: &str,
        query: Vec<(&'static str, String)>,
        body: Option<Value>,
    ) -> Result<Value, PerformanceError> {
        if !is_read_only_request_allowed(&method, path) {
            return Err(PerformanceError::EndpointNotAllowed {
                method,
                path: path.to_owned(),
            });
        }
        tokio::time::timeout(
            self.logical_timeout,
            self.request_within_deadline(method, store, path, query, body),
        )
        .await
        .map_err(|_| PerformanceError::Timeout)?
    }

    async fn request_within_deadline(
        &self,
        method: Method,
        store: &StoreId,
        path: &str,
        query: Vec<(&'static str, String)>,
        body: Option<Value>,
    ) -> Result<Value, PerformanceError> {
        let state = self
            .accounts
            .get(store)
            .ok_or_else(|| PerformanceError::MissingCredentials(store.clone()))?;

        // Preserve fail-fast overload semantics without retaining any permit
        // while OAuth single-flight or account pacing may wait.
        drop(self.try_request_permits(state, path)?);

        let token = self.access_token(state).await?;
        match self
            .request_attempt(state, method.clone(), path, &query, body.as_ref(), &token)
            .await?
        {
            RequestAttempt::Complete(value) => return Ok(value),
            RequestAttempt::Unauthorized { .. } => {}
        }

        self.invalidate_token_if_current(state, &token).await;
        let refreshed_token = self.access_token(state).await?;
        match self
            .request_attempt(state, method, path, &query, body.as_ref(), &refreshed_token)
            .await?
        {
            RequestAttempt::Complete(value) => Ok(value),
            RequestAttempt::Unauthorized { request_id } => {
                Err(PerformanceError::Unauthorized { request_id })
            }
        }
    }

    fn try_request_permits<'a>(
        &'a self,
        state: &'a AccountState,
        path: &str,
    ) -> Result<RequestPermits<'a>, PerformanceError> {
        let global = self
            .global_in_flight
            .try_acquire()
            .map_err(|_| PerformanceError::Overloaded)?;
        let account = state
            .in_flight
            .try_acquire()
            .map_err(|_| PerformanceError::Overloaded)?;
        let statistics = if matches!(
            path,
            DAILY_STATS_PATH | EXPENSES_PATH | PRODUCT_SKU_STATS_PATH
        ) {
            Some(
                state
                    .statistics_in_flight
                    .try_acquire()
                    .map_err(|_| PerformanceError::Overloaded)?,
            )
        } else {
            None
        };
        Ok(RequestPermits {
            _global: global,
            _account: account,
            _statistics: statistics,
        })
    }

    async fn request_attempt(
        &self,
        state: &AccountState,
        method: Method,
        path: &str,
        query: &[(&'static str, String)],
        body: Option<&Value>,
        token: &str,
    ) -> Result<RequestAttempt, PerformanceError> {
        loop {
            state.wait_until_ready().await;
            let permits = self.try_request_permits(state, path)?;

            // Another ready waiter may have claimed the account slot between
            // the non-blocking readiness check and permit acquisition. Never
            // sleep while retaining scarce network capacity: release and retry.
            if !state.try_claim_request_slot(self.interval).await {
                drop(permits);
                continue;
            }

            let response = self.send_request(&method, path, query, body, token).await?;
            if response.status() == StatusCode::UNAUTHORIZED {
                let request_id = safe_request_id(response.headers());
                drop(response);
                drop(permits);
                return Ok(RequestAttempt::Unauthorized { request_id });
            }

            let result = decode_json(response, MAX_RESPONSE_BODY_BYTES)
                .await
                .map(RequestAttempt::Complete);
            drop(permits);
            return result;
        }
    }

    async fn send_request(
        &self,
        method: &Method,
        path: &str,
        query: &[(&'static str, String)],
        body: Option<&Value>,
        token: &str,
    ) -> Result<Response, PerformanceError> {
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| PerformanceError::InvalidToken)?;
        authorization.set_sensitive(true);
        let mut request = self
            .http
            .request(method.clone(), format!("{}{path}", self.base_url))
            .header(AUTHORIZATION, authorization);
        if !query.is_empty() {
            request = request.query(query);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        request.send().await.map_err(classify_transport)
    }

    async fn invalidate_token_if_current(&self, state: &AccountState, used_token: &str) {
        let mut slot = state.token.lock().await;
        if slot
            .cached
            .as_ref()
            .is_some_and(|cached| cached.value == used_token)
        {
            slot.cached = None;
        }
    }

    async fn access_token(&self, state: &AccountState) -> Result<String, PerformanceError> {
        let mut slot = state.token.lock().await;
        let now = Instant::now();
        if let Some(token) = slot.cached.as_ref()
            && token.refresh_at > now
        {
            return Ok(token.value.clone());
        }
        // Holding the lock across the request is what makes this single-flight,
        // so a queued caller must not simply repeat an attempt that just failed.
        if let Some(cooldown) = slot.cooldown.as_ref() {
            if cooldown.until > now {
                return Err(PerformanceError::TokenUnavailable {
                    previous: cooldown.previous,
                });
            }
            slot.cooldown = None;
        }
        match self.request_token(state).await {
            Ok((value, refresh_at)) => {
                slot.cached = Some(CachedToken {
                    value: value.clone(),
                    refresh_at,
                });
                Ok(value)
            }
            Err(error) => {
                slot.cooldown = Some(TokenCooldown {
                    until: Instant::now() + TOKEN_FAILURE_COOLDOWN,
                    previous: error.kind().code(),
                });
                Err(error)
            }
        }
    }

    /// Performs one OAuth client-credentials exchange and validates it.
    ///
    /// Returns the token together with the instant it must be refreshed, so
    /// the caller can cache both without re-deriving the lifetime.
    async fn request_token(
        &self,
        state: &AccountState,
    ) -> Result<(String, Instant), PerformanceError> {
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
        Ok((token.access_token, refresh_at))
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
    let initial_capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default()
        .min(limit);
    let mut body = Vec::with_capacity(initial_capacity);
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
        .filter_map(|name| headers.get(*name))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::mock_http;
    use std::{
        collections::BTreeMap,
        future::Future,
        io::{Read, Write},
        net::TcpListener,
        sync::mpsc,
        task::{Context, Poll, Waker},
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

    fn credentials_for(stores: &[&str]) -> BTreeMap<StoreId, PerformanceCredentials> {
        stores
            .iter()
            .enumerate()
            .map(|(index, store)| {
                (
                    StoreId::from(*store),
                    PerformanceCredentials {
                        client_id: format!("performance-client-{index}"),
                        client_secret: format!("performance-secret-{index}"),
                    },
                )
            })
            .collect()
    }

    async fn cache_access_token(
        client: &PerformanceClient,
        store: &StoreId,
        value: &str,
    ) -> Arc<AccountState> {
        let state = Arc::clone(client.accounts.get(store).unwrap());
        state.token.lock().await.cached = Some(CachedToken {
            value: value.to_owned(),
            refresh_at: Instant::now() + Duration::from_secs(60),
        });
        state
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

    fn sku_statistics_query() -> SkuStatisticsQuery {
        SkuStatisticsQuery {
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
            (200, json!({"limits": []}).to_string()),
            (200, json!({"list": []}).to_string()),
            (200, json!({"products": []}).to_string()),
            (200, json!({"rows": []}).to_string()),
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
        assert_eq!(client.limits(&store).await.unwrap(), json!({"limits": []}));
        assert_eq!(
            client.campaign_objects(&store, 42).await.unwrap(),
            json!({"list": []})
        );
        assert_eq!(
            client
                .campaign_products(
                    &store,
                    42,
                    CampaignProductsQuery {
                        page: 3,
                        page_size: 50,
                    },
                )
                .await
                .unwrap(),
            json!({"products": []})
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
        assert_eq!(
            client
                .sku_statistics(&store, sku_statistics_query())
                .await
                .unwrap(),
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

        let limits = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(limits.starts_with("GET /api/client/limits/list HTTP/1.1\r\n"));
        let objects = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(objects.starts_with("GET /api/client/campaign/42/objects HTTP/1.1\r\n"));
        let products = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(products.starts_with(
            "GET /api/client/campaign/42/v2/products?page=3&pageSize=50 HTTP/1.1\r\n"
        ));

        let daily = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(daily.starts_with(
            "GET /api/client/statistics/daily/json?campaignIds=11&campaignIds=22&dateFrom=2026-08-01&dateTo=2026-08-09 HTTP/1.1\r\n"
        ));
        let expenses = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(expenses.starts_with(
            "GET /api/client/statistics/expense/json?campaignIds=11&campaignIds=22&dateFrom=2026-08-01&dateTo=2026-08-09 HTTP/1.1\r\n"
        ));
        let sku = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(sku.starts_with("POST /api/client/statistics/products/sku HTTP/1.1\r\n"));
        assert!(
            sku.to_ascii_lowercase()
                .contains("authorization: bearer test-access-token")
        );
        assert!(
            sku.to_ascii_lowercase()
                .contains("content-type: application/json")
        );
        assert!(
            sku.ends_with(
                r#"{"campaignIds":[11,22],"dateFrom":"2026-08-01","dateTo":"2026-08-09"}"#
            )
        );
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
    async fn data_unauthorized_refreshes_token_once_and_replays_post_body() {
        let (base_url, requests) = mock_http(vec![
            (200, token(1_800)),
            (401, "{}".to_owned()),
            (200, token(1_800)),
            (200, json!({"rows": [1]}).to_string()),
        ]);
        let client =
            PerformanceClient::new_for_test(base_url, Duration::from_secs(3), credentials());
        let result = client
            .sku_statistics(&StoreId::from("shop"), sku_statistics_query())
            .await
            .unwrap();
        assert_eq!(result, json!({"rows": [1]}));

        let captured: Vec<_> = (0..4)
            .map(|_| requests.recv_timeout(Duration::from_secs(1)).unwrap())
            .collect();
        assert!(captured[0].starts_with("POST /api/client/token "));
        assert!(captured[1].starts_with("POST /api/client/statistics/products/sku "));
        assert!(captured[2].starts_with("POST /api/client/token "));
        assert!(captured[3].starts_with("POST /api/client/statistics/products/sku "));
        let expected_body =
            r#"{"campaignIds":[11,22],"dateFrom":"2026-08-01","dateTo":"2026-08-09"}"#;
        assert!(captured[1].ends_with(expected_body));
        assert!(captured[3].ends_with(expected_body));
        assert!(requests.try_recv().is_err());
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
            PRODUCT_SKU_STATS_PATH,
            "/api/client/campaign/",
            "/api/client/campaign/1",
            "/api/client/campaign/0/objects",
            "/api/client/campaign/01/objects",
            "/api/client/campaign/+1/objects",
            "/api/client/campaign/18446744073709551616/objects",
            "/api/client/campaign/%31/objects",
            "/api/client/campaign/1/objects/",
            "/api/client/campaign/1/objects/activate",
            "/api/client/campaign/1/v2/products/",
            "/api/client/campaign/1/v2/products/activate",
            CAMPAIGN_OBJECTS_PATH_TEMPLATE,
            CAMPAIGN_PRODUCTS_PATH_TEMPLATE,
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
        for path in [
            CAMPAIGNS_PATH,
            LIMITS_PATH,
            "/api/client/campaign/1/objects",
            "/api/client/campaign/1/v2/products",
            TOKEN_PATH,
        ] {
            let error = client
                .post(&StoreId::from("missing"), path, json!({}))
                .await
                .expect_err("unlisted POST endpoint must be denied");
            assert_eq!(error.kind(), PerformanceErrorKind::EndpointNotAllowed);
        }
        let missing = client
            .campaigns(&StoreId::from("missing"), campaigns_query())
            .await
            .expect_err("credentials must be required");
        assert_eq!(missing.kind(), PerformanceErrorKind::MissingCredentials);
        assert_eq!(
            client
                .campaign_objects(&StoreId::from("missing"), 0)
                .await
                .expect_err("zero is not a canonical campaign ID")
                .kind(),
            PerformanceErrorKind::EndpointNotAllowed
        );
        assert_eq!(
            client
                .campaign_objects(&StoreId::from("missing"), 1)
                .await
                .expect_err("a valid dynamic route must reach the credential boundary")
                .kind(),
            PerformanceErrorKind::MissingCredentials
        );
        assert_eq!(
            client
                .sku_statistics(&StoreId::from("missing"), sku_statistics_query())
                .await
                .expect_err("the allowed POST must reach the credential boundary")
                .kind(),
            PerformanceErrorKind::MissingCredentials
        );
        assert!(!is_read_only_request_allowed(&Method::POST, CAMPAIGNS_PATH));
    }

    /// The OAuth endpoint is exempt from pacing and both in-flight semaphores.
    /// Without a cooldown that exemption turns a failing token endpoint into
    /// one token request per API call, for as long as the failure lasts.
    #[tokio::test]
    async fn a_failed_token_request_is_not_repeated_by_the_next_caller() {
        let (base_url, requests) = mock_http(vec![
            (500, "token-endpoint-down".to_owned()),
            (200, token(1_800)),
            (200, json!({"list": []}).to_string()),
        ]);
        let client =
            PerformanceClient::new_for_test(base_url, Duration::from_secs(3), credentials());
        let store = StoreId::from("shop");

        let first = client
            .campaigns(&store, campaigns_query())
            .await
            .expect_err("the token endpoint is failing");
        assert_eq!(first.kind(), PerformanceErrorKind::Http);

        let second = client
            .campaigns(&store, campaigns_query())
            .await
            .expect_err("the cooldown refuses the next caller");
        assert_eq!(second.kind(), PerformanceErrorKind::TokenUnavailable);
        assert_eq!(second.kind().code(), "token_endpoint_cooldown");
        // The refusal names the failure that opened the cooldown rather than
        // inventing a fresh cause.
        assert!(second.to_string().contains("upstream_http_error"));
        assert!(second.request_id().is_none());
        assert!(!format!("{second:?}").contains("performance-secret"));

        assert!(
            requests.recv().is_ok(),
            "the first caller reaches the token endpoint"
        );
        assert!(
            requests.try_recv().is_err(),
            "the cooled-down caller must not reach the token endpoint"
        );

        // Expiry reopens the single-flight token path. Mutating the private
        // clock boundary keeps this unit test deterministic and instant.
        let state = client.accounts.get(&store).unwrap();
        state.token.lock().await.cooldown.as_mut().unwrap().until = Instant::now();
        assert_eq!(
            client.campaigns(&store, campaigns_query()).await.unwrap(),
            json!({"list": []})
        );
        assert!(
            requests
                .recv()
                .unwrap()
                .starts_with("POST /api/client/token")
        );
        assert!(
            requests
                .recv()
                .unwrap()
                .starts_with("GET /api/client/campaign?")
        );
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
        let first_permit = state.in_flight.acquire().await.unwrap();
        let second_permit = state.in_flight.acquire().await.unwrap();
        assert_eq!(
            client
                .campaigns(&StoreId::from("shop"), campaigns_query())
                .await
                .expect_err("local overload must fail fast")
                .kind(),
            PerformanceErrorKind::Overloaded
        );

        drop(first_permit);
        drop(second_permit);
        let _statistics = state.statistics_in_flight.acquire().await.unwrap();
        assert_eq!(
            client
                .daily_statistics(&StoreId::from("shop"), statistics_query())
                .await
                .expect_err("a second statistics export must fail fast")
                .kind(),
            PerformanceErrorKind::Overloaded
        );
        assert_eq!(
            client
                .sku_statistics(&StoreId::from("shop"), sku_statistics_query())
                .await
                .expect_err("SKU statistics must share the statistics gate")
                .kind(),
            PerformanceErrorKind::Overloaded
        );
    }

    #[tokio::test]
    async fn reqwest_timeout_is_classified_before_the_outer_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 4096];
            let _ = stream.read(&mut buffer);
            thread::sleep(Duration::from_millis(100));
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_millis(20))
            .build()
            .unwrap();
        let error = client
            .get(format!("http://{address}"))
            .send()
            .await
            .expect_err("the mock deliberately withholds its response");

        assert!(error.is_timeout());
        assert_eq!(
            classify_transport(error).kind(),
            PerformanceErrorKind::Timeout
        );
        server.join().unwrap();
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
        state.token.lock().await.cached = Some(CachedToken {
            value: "new-token".to_owned(),
            refresh_at: Instant::now() + Duration::from_secs(60),
        });
        client.invalidate_token_if_current(state, "old-token").await;
        assert_eq!(
            state
                .token
                .lock()
                .await
                .cached
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
        assert!(state.token.lock().await.cached.is_none());
    }

    #[tokio::test]
    async fn pacing_waiters_across_accounts_do_not_hold_network_permits() {
        let noisy_stores = ["noisy-a", "noisy-b", "noisy-c", "noisy-d"];
        let client = PerformanceClient::build(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(3),
            Duration::from_secs(1),
            credentials_for(&["noisy-a", "noisy-b", "noisy-c", "noisy-d", "quiet"]),
            "mcp-ozon-test",
            None,
        )
        .unwrap();
        let noisy_states = noisy_stores
            .iter()
            .map(|store| Arc::clone(client.accounts.get(&StoreId::from(*store)).unwrap()))
            .collect::<Vec<_>>();
        let pacing_gates = [
            noisy_states[0].next_allowed.lock().await,
            noisy_states[1].next_allowed.lock().await,
            noisy_states[2].next_allowed.lock().await,
            noisy_states[3].next_allowed.lock().await,
        ];
        let query = vec![("page", "1".to_owned())];
        let mut attempts = Vec::with_capacity(MAX_GLOBAL_IN_FLIGHT);
        for state in &noisy_states {
            for _ in 0..MAX_IN_FLIGHT_PER_CLIENT {
                attempts.push(Box::pin(client.request_attempt(
                    state,
                    Method::GET,
                    CAMPAIGNS_PATH,
                    &query,
                    None,
                    "cached-token",
                )));
            }
        }
        assert_eq!(attempts.len(), MAX_GLOBAL_IN_FLIGHT);

        // Poll every request exactly to its blocked pacing lock. This avoids
        // scheduler- or wall-clock-dependent assertions about spawned tasks.
        let mut context = Context::from_waker(Waker::noop());
        for attempt in &mut attempts {
            assert!(matches!(attempt.as_mut().poll(&mut context), Poll::Pending));
        }

        assert_eq!(
            client.global_in_flight.available_permits(),
            MAX_GLOBAL_IN_FLIGHT
        );
        for state in &noisy_states {
            assert_eq!(
                state.in_flight.available_permits(),
                MAX_IN_FLIGHT_PER_CLIENT
            );
        }
        let quiet = client.accounts.get(&StoreId::from("quiet")).unwrap();
        let quiet_permits = client
            .try_request_permits(quiet, CAMPAIGNS_PATH)
            .expect("an unrelated ready account must retain network capacity");
        drop(quiet_permits);
        assert_eq!(
            client.global_in_flight.available_permits(),
            MAX_GLOBAL_IN_FLIGHT
        );

        drop(attempts);
        drop(pacing_gates);
    }

    #[tokio::test]
    async fn pacing_race_loser_releases_all_network_permits_before_waiting_again() {
        let client = PerformanceClient::new_for_test(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(3),
            credentials(),
        );
        let state = Arc::clone(client.accounts.get(&StoreId::from("shop")).unwrap());
        let query = Vec::new();

        // Hold the pacing mutex while both futures queue in a known FIFO order:
        // the request first performs its readiness check, then the competing
        // claimant takes the slot before that request can claim it itself.
        let pacing_gate = state.next_allowed.lock().await;
        let mut request = Box::pin(client.request_attempt(
            &state,
            Method::GET,
            DAILY_STATS_PATH,
            &query,
            None,
            "cached-token",
        ));
        let mut winning_claim = Box::pin(state.try_claim_request_slot(Duration::from_secs(60)));
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(request.as_mut().poll(&mut context), Poll::Pending));
        assert!(matches!(
            winning_claim.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(pacing_gate);

        // The request completes the readiness check and acquires all three
        // business permits, but its claim queues behind the competing future.
        assert!(matches!(request.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(
            client.global_in_flight.available_permits(),
            MAX_GLOBAL_IN_FLIGHT - 1
        );
        assert_eq!(
            state.in_flight.available_permits(),
            MAX_IN_FLIGHT_PER_CLIENT - 1
        );
        assert_eq!(state.statistics_in_flight.available_permits(), 0);

        assert!(matches!(
            winning_claim.as_mut().poll(&mut context),
            Poll::Ready(true)
        ));
        assert!(matches!(request.as_mut().poll(&mut context), Poll::Pending));

        // Losing the pacing race must return every permit before the request
        // loops back into its 60-second pacing wait.
        assert_eq!(
            client.global_in_flight.available_permits(),
            MAX_GLOBAL_IN_FLIGHT
        );
        assert_eq!(
            state.in_flight.available_permits(),
            MAX_IN_FLIGHT_PER_CLIENT
        );
        assert_eq!(state.statistics_in_flight.available_permits(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn oauth_refresh_after_401_releases_all_business_permits() {
        fn read_request(stream: &mut std::net::TcpStream) -> String {
            let mut buffer = [0_u8; 4096];
            let read = stream.read(&mut buffer).unwrap();
            String::from_utf8_lossy(&buffer[..read]).into_owned()
        }

        fn write_json(stream: &mut std::net::TcpStream, status: u16, body: &str) {
            write!(
                stream,
                "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (refresh_started_tx, refresh_started_rx) = tokio::sync::oneshot::channel();
        let (release_refresh_tx, release_refresh_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut first_request, _) = listener.accept().unwrap();
            assert!(
                read_request(&mut first_request)
                    .starts_with("GET /api/client/statistics/daily/json?")
            );
            write_json(&mut first_request, 401, "{}");

            let (mut refresh_request, _) = listener.accept().unwrap();
            assert!(read_request(&mut refresh_request).starts_with("POST /api/client/token "));
            refresh_started_tx.send(()).unwrap();
            let refresh_response = thread::spawn(move || {
                release_refresh_rx
                    .recv_timeout(Duration::from_secs(3))
                    .unwrap();
                write_json(&mut refresh_request, 200, &token(1_800));
            });

            let (mut quiet_request, _) = listener.accept().unwrap();
            assert!(read_request(&mut quiet_request).starts_with("GET /api/client/campaign?"));
            write_json(&mut quiet_request, 200, r#"{"list":[]}"#);

            let (mut replay_request, _) = listener.accept().unwrap();
            assert!(
                read_request(&mut replay_request)
                    .starts_with("GET /api/client/statistics/daily/json?")
            );
            write_json(&mut replay_request, 200, r#"{"rows":[]}"#);
            refresh_response.join().unwrap();
        });

        let client = PerformanceClient::new_for_test(
            format!("http://{address}"),
            Duration::from_secs(3),
            credentials_for(&["noisy", "quiet"]),
        );
        let noisy_store = StoreId::from("noisy");
        let quiet_store = StoreId::from("quiet");
        let noisy_state = cache_access_token(&client, &noisy_store, "stale-token").await;
        cache_access_token(&client, &quiet_store, "quiet-token").await;

        let noisy_client = client.clone();
        let noisy_task = tokio::spawn(async move {
            noisy_client
                .daily_statistics(&noisy_store, statistics_query())
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), refresh_started_rx)
            .await
            .expect("the refresh request must start")
            .expect("the server must signal refresh start");

        assert_eq!(
            client.global_in_flight.available_permits(),
            MAX_GLOBAL_IN_FLIGHT
        );
        assert_eq!(
            noisy_state.in_flight.available_permits(),
            MAX_IN_FLIGHT_PER_CLIENT
        );
        assert_eq!(noisy_state.statistics_in_flight.available_permits(), 1);
        let quiet_result = tokio::time::timeout(
            Duration::from_secs(1),
            client.campaigns(&quiet_store, campaigns_query()),
        )
        .await
        .expect("an unrelated account must not wait for OAuth refresh")
        .unwrap();
        assert_eq!(quiet_result, json!({"list": []}));

        release_refresh_tx.send(()).unwrap();
        let noisy_result = tokio::time::timeout(Duration::from_secs(1), noisy_task)
            .await
            .expect("the refreshed request must finish")
            .unwrap()
            .unwrap();
        assert_eq!(noisy_result, json!({"rows": []}));
        server.join().unwrap();
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
        assert_eq!(safe_request_id(&headers).as_deref(), Some("safe/id:1"));
        headers.insert(
            "x-o3-trace-id",
            HeaderValue::from_str(&"x".repeat(MAX_REQUEST_ID_BYTES + 1)).unwrap(),
        );
        assert_eq!(safe_request_id(&headers).as_deref(), Some("safe/id:1"));
        headers.insert("x-o3-trace-id", HeaderValue::from_static("  trace/id:2  "));
        assert_eq!(safe_request_id(&headers).as_deref(), Some("trace/id:2"));
        headers.insert("x-request-id", HeaderValue::from_static("also bad"));
        headers.insert("x-o3-trace-id", HeaderValue::from_static("bad value"));
        assert_eq!(safe_request_id(&headers), None);

        assert_eq!(READ_ONLY_ENDPOINT_ALLOWLIST.len(), 5);
        assert_eq!(
            READ_ONLY_ENDPOINT_ALLOWLIST,
            &[
                (Method::GET, CAMPAIGNS_PATH),
                (Method::GET, DAILY_STATS_PATH),
                (Method::GET, EXPENSES_PATH),
                (Method::GET, LIMITS_PATH),
                (Method::POST, PRODUCT_SKU_STATS_PATH),
            ]
        );
        for (method, path) in READ_ONLY_ENDPOINT_ALLOWLIST {
            assert!(is_read_only_request_allowed(method, path));
        }
        for path in [
            "/api/client/campaign/1/objects",
            "/api/client/campaign/18446744073709551615/objects",
            "/api/client/campaign/1/v2/products",
        ] {
            assert!(is_read_only_request_allowed(&Method::GET, path));
            assert!(!is_read_only_request_allowed(&Method::POST, path));
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
            None,
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

    /// The upstream answers 401, the cached token is invalidated and refreshed,
    /// and the request is replayed — but each of those three steps can fail on
    /// its own. None of them may loop, panic, or report a misleading error kind.
    #[tokio::test]
    async fn every_step_of_the_unauthorized_replay_can_fail_without_looping() {
        let store = StoreId::from("shop");

        // The very first data request never reaches the upstream: the token was
        // obtained, then the connection failed.
        let (base_url, requests) = mock_http(vec![(200, token(1_800))]);
        let client =
            PerformanceClient::new_for_test(base_url, Duration::from_secs(3), credentials());
        assert_eq!(
            client
                .campaigns(&store, campaigns_query())
                .await
                .expect_err("a failed first data request must surface")
                .kind(),
            PerformanceErrorKind::Network
        );
        assert!(
            requests.recv_timeout(Duration::from_secs(1)).is_ok(),
            "the token request must have been made"
        );

        // The refresh triggered by the 401 fails upstream. The reported error
        // must be the refresh failure, not the original 401.
        let (base_url, requests) = mock_http(vec![
            (200, token(1_800)),
            (401, "{}".to_owned()),
            (500, "{}".to_owned()),
        ]);
        let client =
            PerformanceClient::new_for_test(base_url, Duration::from_secs(3), credentials());
        let error = client
            .campaigns(&store, campaigns_query())
            .await
            .expect_err("a failed token refresh must surface");
        assert_eq!(error.kind(), PerformanceErrorKind::Http);
        for expected in [
            "POST /api/client/token",
            "GET /api/client/campaign?",
            "POST /api/client/token",
        ] {
            let request = requests.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(request.starts_with(expected), "{request}");
        }
        assert!(
            requests.try_recv().is_err(),
            "a failed refresh must not replay the data request"
        );

        // The refresh succeeds but the replayed request cannot be delivered.
        let (base_url, requests) = mock_http(vec![
            (200, token(1_800)),
            (401, "{}".to_owned()),
            (200, token(1_800)),
        ]);
        let client =
            PerformanceClient::new_for_test(base_url, Duration::from_secs(3), credentials());
        assert_eq!(
            client
                .campaigns(&store, campaigns_query())
                .await
                .expect_err("a failed replay must surface")
                .kind(),
            PerformanceErrorKind::Network
        );
        for _ in 0..3 {
            requests.recv_timeout(Duration::from_secs(1)).unwrap();
        }

        // A second 401 on the replay must be reported, not refreshed again:
        // exactly one replay per request, so a permanently unauthorized
        // principal cannot spin the token endpoint.
        let (base_url, requests) = mock_http(vec![
            (200, token(1_800)),
            (401, "{}".to_owned()),
            (200, token(1_800)),
            (401, "{}".to_owned()),
            (200, token(1_800)),
            (200, json!({"list": []}).to_string()),
        ]);
        let client =
            PerformanceClient::new_for_test(base_url, Duration::from_secs(3), credentials());
        assert_eq!(
            client
                .campaigns(&store, campaigns_query())
                .await
                .expect_err("a second 401 must be reported")
                .kind(),
            PerformanceErrorKind::Unauthorized
        );
        let mut observed = Vec::new();
        while let Ok(request) = requests.recv_timeout(Duration::from_millis(200)) {
            observed.push(request);
        }
        assert_eq!(
            observed.len(),
            4,
            "one token, one 401, one refresh, one replay — and nothing more: {observed:#?}"
        );
        assert_eq!(
            observed
                .iter()
                .filter(|request| request.starts_with("POST /api/client/token"))
                .count(),
            2,
            "the token endpoint must be called at most twice per request"
        );
    }

    /// A response that promises more bytes than it delivers must be reported as a
    /// transport failure. Accepting the prefix would hand a truncated advertising
    /// report to the model as if it were complete.
    #[tokio::test]
    async fn a_truncated_performance_response_is_reported_as_a_transport_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            let token_body = token(1_800);
            let responses = [
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{token_body}",
                    token_body.len()
                ),
                // Promises 4096 bytes of JSON, delivers a valid-JSON prefix and
                // hangs up, so only the length mismatch reveals the truncation.
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n{\"list\":[]}".to_owned(),
            ];
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buffer = [0_u8; 4096];
                let _ = stream.read(&mut buffer);
                let _ = stream.write_all(response.as_bytes());
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
                .expect_err("a truncated body must not be decoded")
                .kind(),
            PerformanceErrorKind::Network
        );
    }

    /// The process-wide budget is checked before the per-account gate, so a burst
    /// spread across accounts fails fast instead of queueing. The account gate is
    /// left completely free here, so only the global budget can explain the
    /// refusal — and releasing one permit must restore service immediately.
    #[tokio::test]
    async fn the_global_performance_budget_fails_fast_while_the_account_gate_is_free() {
        let client = PerformanceClient::new_for_test(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_millis(30),
            credentials(),
        );
        let store = StoreId::from("shop");
        let state = client.accounts.get(&store).unwrap();
        assert_eq!(
            state.in_flight.available_permits(),
            MAX_IN_FLIGHT_PER_CLIENT,
            "the account gate must be untouched for this test to mean anything"
        );

        let mut held = Vec::new();
        for _ in 0..MAX_GLOBAL_IN_FLIGHT {
            held.push(
                Arc::clone(&client.global_in_flight)
                    .acquire_owned()
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(
            client
                .campaigns(&store, campaigns_query())
                .await
                .expect_err("an exhausted global budget must fail fast")
                .kind(),
            PerformanceErrorKind::Overloaded
        );

        // Freeing one permit lets the next request through the gate; it then
        // fails on the unreachable upstream, which proves the previous refusal
        // came from the budget rather than from the network.
        held.pop();
        assert_eq!(
            client
                .campaigns(&store, campaigns_query())
                .await
                .expect_err("the upstream is unreachable in this test")
                .kind(),
            PerformanceErrorKind::Network
        );
    }
}
