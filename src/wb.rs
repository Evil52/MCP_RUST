use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, NaiveDate, Utc};
use reqwest::{
    Client, Method, Proxy, Response, StatusCode, Url,
    header::{AUTHORIZATION, HeaderMap, HeaderValue, RETRY_AFTER},
    redirect::Policy,
};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    sync::{Mutex, Semaphore, SemaphorePermit},
    time::{Instant as TokioInstant, sleep, timeout_at},
};
use tracing::{info, warn};

const ANALYTICS_API_BASE_URL: &str = "https://seller-analytics-api.wildberries.ru";
const STATISTICS_API_BASE_URL: &str = "https://statistics-api.wildberries.ru";
const CONTENT_API_BASE_URL: &str = "https://content-api.wildberries.ru";
const PRICES_API_BASE_URL: &str = "https://discounts-prices-api.wildberries.ru";
const COMMON_API_BASE_URL: &str = "https://common-api.wildberries.ru";
const PROMOTION_API_BASE_URL: &str = "https://advert-api.wildberries.ru";
const PING_PATH: &str = "/ping";
const SALES_FUNNEL_PATH: &str = "/api/analytics/v3/sales-funnel/products";
const SALES_FUNNEL_HISTORY_PATH: &str = "/api/analytics/v3/sales-funnel/products/history";
const SALES_FUNNEL_GROUPED_HISTORY_PATH: &str = "/api/analytics/v3/sales-funnel/grouped/history";
const WAREHOUSE_STOCKS_PATH: &str = "/api/analytics/v1/stocks-report/wb-warehouses";
const ORDERS_PATH: &str = "/api/v1/supplier/orders";
const SALES_PATH: &str = "/api/v1/supplier/sales";
const PRODUCT_CARDS_PATH: &str = "/content/v2/get/cards/list";
const PRODUCT_PRICES_PATH: &str = "/api/v2/list/goods/filter";
const TARIFF_COMMISSIONS_PATH: &str = "/api/v1/tariffs/commission";
const TARIFF_BOXES_PATH: &str = "/api/v1/tariffs/box";
const TARIFF_PALLETS_PATH: &str = "/api/v1/tariffs/pallet";
const TARIFF_RETURNS_PATH: &str = "/api/v1/tariffs/return";
const ACCEPTANCE_COEFFICIENTS_PATH: &str = "/api/tariffs/v1/acceptance/coefficients";
pub(crate) const PROMOTION_CAMPAIGNS_PATH: &str = "/adv/v1/promotion/count";
pub(crate) const PROMOTION_DETAILS_PATH: &str = "/api/advert/v2/adverts";
pub(crate) const PROMOTION_STATS_PATH: &str = "/adv/v3/fullstats";
pub(crate) const SEARCH_PRODUCT_QUERIES_PATH: &str = "/api/v2/search-report/product/search-texts";
pub(crate) const SEARCH_ORDERS_POSITIONS_PATH: &str = "/api/v2/search-report/product/orders";
pub(crate) const PROMOTION_MINIMUM_BIDS_PATH: &str = "/api/advert/v1/bids/min";
pub(crate) const PROMOTION_RECOMMENDATIONS_PATH: &str = "/api/advert/v0/bids/recommendations";
pub(crate) const PROMOTION_CLUSTER_BIDS_PATH: &str = "/adv/v0/normquery/get-bids";
const MAX_RESPONSE_BODY_BYTES: usize = 2 * 1_048_576;
const MAX_ERROR_BODY_BYTES: usize = 4_096;
const MAX_ATTEMPTS: usize = 3;
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_IN_FLIGHT_REQUESTS_PER_TOKEN: usize = 4;
const MAX_GLOBAL_IN_FLIGHT_REQUESTS: usize = 8;
const MAX_REQUEST_ID_BYTES: usize = 128;
const PING_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(10);
const ANALYTICS_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(20);
const STATISTICS_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(60);
const CONTENT_MIN_REQUEST_INTERVAL: Duration = Duration::from_millis(600);
const PRICES_MIN_REQUEST_INTERVAL: Duration = Duration::from_millis(600);
const COMMISSION_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(60);
const LOGISTICS_TARIFF_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(1);
const ACCEPTANCE_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(10);
const PROMOTION_CAMPAIGN_MIN_REQUEST_INTERVAL: Duration = Duration::from_millis(200);
const PROMOTION_STATS_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(20);
const SEARCH_REPORT_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(20);
const PROMOTION_MINIMUM_BIDS_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(3);
const PROMOTION_RECOMMENDATIONS_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(12);
const PROMOTION_CLUSTER_BIDS_MIN_REQUEST_INTERVAL: Duration = Duration::from_millis(200);
const MAX_PROMOTION_CAMPAIGN_IDS: usize = 50;
const MAX_PROMOTION_STATS_IDS: usize = 50;
const MAX_SEARCH_REPORT_NM_IDS: usize = 50;
const MAX_SEARCH_REPORT_TEXTS: usize = 30;
const MAX_SEARCH_REPORT_LIMIT: u32 = 30;
const MAX_SEARCH_PRODUCT_PERIOD_DAYS: i64 = 31;
const MAX_SEARCH_ORDERS_PERIOD_DAYS: i64 = 7;
const MAX_PROMOTION_BID_NM_IDS: usize = 100;
const MAX_PROMOTION_CLUSTER_BID_ITEMS: usize = 100;
const MAX_WB_SIGNED_ID: u64 = i64::MAX as u64;
const MAX_SEARCH_TEXT_BYTES: usize = 256;
const BASE_RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
const MAX_LOGICAL_REQUEST_DURATION: Duration = Duration::from_secs(60);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);
const HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Fixed Wildberries hosts. There is deliberately no host supplied by callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApiHost {
    Analytics,
    Statistics,
    Content,
    Prices,
    Common,
    Promotion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestClass {
    AnalyticsPing,
    AnalyticsReport,
    StatisticsReport,
    ContentReport,
    PricesReport,
    CommissionTariff,
    LogisticsTariff,
    AcceptanceTariff,
    PromotionCampaign,
    PromotionStats,
    SearchReport,
    PromotionMinimumBids,
    PromotionRecommendedBids,
    PromotionClusterBids,
}

/// Single source of truth for every request that may leave this process.
/// Method, exact path, fixed host, safe observability label and quota bucket
/// live in the same record so extending one dimension cannot silently drift
/// out of sync with another.
#[derive(Debug)]
struct EndpointPolicy {
    method: Method,
    path: &'static str,
    label: &'static str,
    host: ApiHost,
    request_class: RequestClass,
}

/// Every Wildberries request this process is allowed to make.
///
/// Mirrors [`crate::ozon::READ_ONLY_ENDPOINT_ALLOWLIST`]: it is enforced inside
/// [`WbClient::request`], the only place a WB request can leave the process, so
/// adding a mutating call requires deliberately editing this list.
const READ_ONLY_ENDPOINT_ALLOWLIST: &[EndpointPolicy] = &[
    EndpointPolicy {
        method: Method::GET,
        path: PING_PATH,
        label: "analytics:/ping",
        host: ApiHost::Analytics,
        request_class: RequestClass::AnalyticsPing,
    },
    EndpointPolicy {
        method: Method::POST,
        path: SALES_FUNNEL_PATH,
        label: "analytics:/api/analytics/v3/sales-funnel/products",
        host: ApiHost::Analytics,
        request_class: RequestClass::AnalyticsReport,
    },
    EndpointPolicy {
        method: Method::POST,
        path: SALES_FUNNEL_HISTORY_PATH,
        label: "analytics:/api/analytics/v3/sales-funnel/products/history",
        host: ApiHost::Analytics,
        request_class: RequestClass::AnalyticsReport,
    },
    EndpointPolicy {
        method: Method::POST,
        path: SALES_FUNNEL_GROUPED_HISTORY_PATH,
        label: "analytics:/api/analytics/v3/sales-funnel/grouped/history",
        host: ApiHost::Analytics,
        request_class: RequestClass::AnalyticsReport,
    },
    EndpointPolicy {
        method: Method::POST,
        path: WAREHOUSE_STOCKS_PATH,
        label: "analytics:/api/analytics/v1/stocks-report/wb-warehouses",
        host: ApiHost::Analytics,
        request_class: RequestClass::AnalyticsReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: ORDERS_PATH,
        label: "statistics:/api/v1/supplier/orders",
        host: ApiHost::Statistics,
        request_class: RequestClass::StatisticsReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: SALES_PATH,
        label: "statistics:/api/v1/supplier/sales",
        host: ApiHost::Statistics,
        request_class: RequestClass::StatisticsReport,
    },
    EndpointPolicy {
        method: Method::POST,
        path: PRODUCT_CARDS_PATH,
        label: "content:/content/v2/get/cards/list",
        host: ApiHost::Content,
        request_class: RequestClass::ContentReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: PRODUCT_PRICES_PATH,
        label: "prices:/api/v2/list/goods/filter",
        host: ApiHost::Prices,
        request_class: RequestClass::PricesReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: TARIFF_COMMISSIONS_PATH,
        label: "common:/api/v1/tariffs/commission",
        host: ApiHost::Common,
        request_class: RequestClass::CommissionTariff,
    },
    EndpointPolicy {
        method: Method::GET,
        path: TARIFF_BOXES_PATH,
        label: "common:/api/v1/tariffs/box",
        host: ApiHost::Common,
        request_class: RequestClass::LogisticsTariff,
    },
    EndpointPolicy {
        method: Method::GET,
        path: TARIFF_PALLETS_PATH,
        label: "common:/api/v1/tariffs/pallet",
        host: ApiHost::Common,
        request_class: RequestClass::LogisticsTariff,
    },
    EndpointPolicy {
        method: Method::GET,
        path: TARIFF_RETURNS_PATH,
        label: "common:/api/v1/tariffs/return",
        host: ApiHost::Common,
        request_class: RequestClass::LogisticsTariff,
    },
    EndpointPolicy {
        method: Method::GET,
        path: ACCEPTANCE_COEFFICIENTS_PATH,
        label: "common:/api/tariffs/v1/acceptance/coefficients",
        host: ApiHost::Common,
        request_class: RequestClass::AcceptanceTariff,
    },
    EndpointPolicy {
        method: Method::GET,
        path: PROMOTION_CAMPAIGNS_PATH,
        label: "promotion:/adv/v1/promotion/count",
        host: ApiHost::Promotion,
        request_class: RequestClass::PromotionCampaign,
    },
    EndpointPolicy {
        method: Method::GET,
        path: PROMOTION_DETAILS_PATH,
        label: "promotion:/api/advert/v2/adverts",
        host: ApiHost::Promotion,
        request_class: RequestClass::PromotionCampaign,
    },
    EndpointPolicy {
        method: Method::GET,
        path: PROMOTION_STATS_PATH,
        label: "promotion:/adv/v3/fullstats",
        host: ApiHost::Promotion,
        request_class: RequestClass::PromotionStats,
    },
    EndpointPolicy {
        method: Method::POST,
        path: SEARCH_PRODUCT_QUERIES_PATH,
        label: "analytics:/api/v2/search-report/product/search-texts",
        host: ApiHost::Analytics,
        request_class: RequestClass::SearchReport,
    },
    EndpointPolicy {
        method: Method::POST,
        path: SEARCH_ORDERS_POSITIONS_PATH,
        label: "analytics:/api/v2/search-report/product/orders",
        host: ApiHost::Analytics,
        request_class: RequestClass::SearchReport,
    },
    EndpointPolicy {
        method: Method::POST,
        path: PROMOTION_MINIMUM_BIDS_PATH,
        label: "promotion:/api/advert/v1/bids/min",
        host: ApiHost::Promotion,
        request_class: RequestClass::PromotionMinimumBids,
    },
    EndpointPolicy {
        method: Method::GET,
        path: PROMOTION_RECOMMENDATIONS_PATH,
        label: "promotion:/api/advert/v0/bids/recommendations",
        host: ApiHost::Promotion,
        request_class: RequestClass::PromotionRecommendedBids,
    },
    EndpointPolicy {
        method: Method::POST,
        path: PROMOTION_CLUSTER_BIDS_PATH,
        label: "promotion:/adv/v0/normquery/get-bids",
        host: ApiHost::Promotion,
        request_class: RequestClass::PromotionClusterBids,
    },
];

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
    InvalidArguments,
    MissingCredentials,
    Unauthorized,
    Forbidden,
    SubscriptionRequired,
    RateLimited,
    Http,
    Timeout,
    Network,
    Overloaded,
    InvalidJson,
    ResponseTooLarge,
}

impl WbErrorKind {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::EndpointNotAllowed => "endpoint_not_allowed",
            Self::InvalidArguments => "invalid_arguments",
            Self::MissingCredentials => "missing_credentials",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::SubscriptionRequired => "subscription_required",
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
    #[error("некорректные параметры read-only запроса WB API: {field}")]
    InvalidArguments { field: &'static str },
    #[error("для кабинета WB {0} не настроен API token")]
    MissingCredentials(String),
    #[error("WB API отклонил авторизацию (HTTP 401, request-id: {request_id:?})")]
    Unauthorized { request_id: Option<String> },
    #[error("доступ к WB API запрещён (HTTP 403, request-id: {request_id:?})")]
    Forbidden { request_id: Option<String> },
    #[error(
        "для этого read-only отчёта требуется платная возможность или подписка WB (HTTP 402, request-id: {request_id:?})"
    )]
    SubscriptionRequired { request_id: Option<String> },
    #[error(
        "WB API ограничил частоту запросов (HTTP 429, request-id: {request_id:?}, retry-after: {retry_after:?})"
    )]
    RateLimited {
        request_id: Option<String>,
        retry_after: Option<Duration>,
    },
    #[error(
        "локальный лимит частоты запросов WB ещё не восстановлен (retry-after: {retry_after:?})"
    )]
    LocalRateLimited { retry_after: Duration },
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
    #[must_use]
    pub const fn kind(&self) -> WbErrorKind {
        match self {
            Self::EndpointNotAllowed { .. } => WbErrorKind::EndpointNotAllowed,
            Self::InvalidArguments { .. } => WbErrorKind::InvalidArguments,
            Self::MissingCredentials(_) => WbErrorKind::MissingCredentials,
            Self::Unauthorized { .. } => WbErrorKind::Unauthorized,
            Self::Forbidden { .. } => WbErrorKind::Forbidden,
            Self::SubscriptionRequired { .. } => WbErrorKind::SubscriptionRequired,
            Self::RateLimited { .. } | Self::LocalRateLimited { .. } => WbErrorKind::RateLimited,
            Self::Api { .. } => WbErrorKind::Http,
            Self::Timeout { .. } | Self::DeadlineExceeded => WbErrorKind::Timeout,
            Self::Network { .. } => WbErrorKind::Network,
            Self::Overloaded => WbErrorKind::Overloaded,
            Self::InvalidJson { .. } => WbErrorKind::InvalidJson,
            Self::ResponseTooLarge { .. } => WbErrorKind::ResponseTooLarge,
        }
    }

    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Unauthorized { request_id }
            | Self::Forbidden { request_id }
            | Self::SubscriptionRequired { request_id }
            | Self::RateLimited { request_id, .. }
            | Self::Api { request_id, .. }
            | Self::Timeout { request_id, .. }
            | Self::Network { request_id, .. }
            | Self::InvalidJson { request_id, .. }
            | Self::ResponseTooLarge { request_id, .. } => request_id.as_deref(),
            Self::EndpointNotAllowed { .. }
            | Self::InvalidArguments { .. }
            | Self::MissingCredentials(_)
            | Self::LocalRateLimited { .. }
            | Self::DeadlineExceeded
            | Self::Overloaded => None,
        }
    }
}

impl EndpointPolicy {
    fn for_request(method: &Method, path: &str) -> Option<&'static Self> {
        READ_ONLY_ENDPOINT_ALLOWLIST
            .iter()
            .find(|policy| policy.method == *method && policy.path == path)
    }
}

impl RequestClass {
    const fn allows_automatic_retry(self) -> bool {
        !matches!(
            self,
            Self::StatisticsReport | Self::CommissionTariff | Self::SearchReport
        )
    }

    #[cfg(test)]
    fn for_request(method: &Method, path: &str) -> Option<Self> {
        EndpointPolicy::for_request(method, path).map(|policy| policy.request_class)
    }
}

#[derive(Debug, Clone, Copy)]
struct ClientPolicy {
    ping_interval: Duration,
    analytics_interval: Duration,
    statistics_interval: Duration,
    content_interval: Duration,
    prices_interval: Duration,
    commission_interval: Duration,
    logistics_tariff_interval: Duration,
    acceptance_interval: Duration,
    promotion_campaign_interval: Duration,
    promotion_stats_interval: Duration,
    search_report_interval: Duration,
    promotion_minimum_bids_interval: Duration,
    promotion_recommendations_interval: Duration,
    promotion_cluster_bids_interval: Duration,
    max_attempts: usize,
    base_retry_delay: Duration,
    max_retry_delay: Duration,
    logical_timeout: Duration,
}

impl ClientPolicy {
    fn production(request_timeout: Duration) -> Self {
        Self {
            ping_interval: PING_MIN_REQUEST_INTERVAL,
            analytics_interval: ANALYTICS_MIN_REQUEST_INTERVAL,
            statistics_interval: STATISTICS_MIN_REQUEST_INTERVAL,
            content_interval: CONTENT_MIN_REQUEST_INTERVAL,
            prices_interval: PRICES_MIN_REQUEST_INTERVAL,
            commission_interval: COMMISSION_MIN_REQUEST_INTERVAL,
            logistics_tariff_interval: LOGISTICS_TARIFF_MIN_REQUEST_INTERVAL,
            acceptance_interval: ACCEPTANCE_MIN_REQUEST_INTERVAL,
            promotion_campaign_interval: PROMOTION_CAMPAIGN_MIN_REQUEST_INTERVAL,
            promotion_stats_interval: PROMOTION_STATS_MIN_REQUEST_INTERVAL,
            search_report_interval: SEARCH_REPORT_MIN_REQUEST_INTERVAL,
            promotion_minimum_bids_interval: PROMOTION_MINIMUM_BIDS_MIN_REQUEST_INTERVAL,
            promotion_recommendations_interval: PROMOTION_RECOMMENDATIONS_MIN_REQUEST_INTERVAL,
            promotion_cluster_bids_interval: PROMOTION_CLUSTER_BIDS_MIN_REQUEST_INTERVAL,
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
            analytics_interval: Duration::ZERO,
            statistics_interval: Duration::ZERO,
            content_interval: Duration::ZERO,
            prices_interval: Duration::ZERO,
            commission_interval: Duration::ZERO,
            logistics_tariff_interval: Duration::ZERO,
            acceptance_interval: Duration::ZERO,
            promotion_campaign_interval: Duration::ZERO,
            promotion_stats_interval: Duration::ZERO,
            search_report_interval: Duration::ZERO,
            promotion_minimum_bids_interval: Duration::ZERO,
            promotion_recommendations_interval: Duration::ZERO,
            promotion_cluster_bids_interval: Duration::ZERO,
            max_attempts: 1,
            base_retry_delay: Duration::ZERO,
            max_retry_delay: Duration::from_secs(1),
            logical_timeout,
        }
    }

    fn interval(self, request_class: RequestClass) -> Duration {
        match request_class {
            RequestClass::AnalyticsPing => self.ping_interval,
            RequestClass::AnalyticsReport => self.analytics_interval,
            RequestClass::StatisticsReport => self.statistics_interval,
            RequestClass::ContentReport => self.content_interval,
            RequestClass::PricesReport => self.prices_interval,
            RequestClass::CommissionTariff => self.commission_interval,
            RequestClass::LogisticsTariff => self.logistics_tariff_interval,
            RequestClass::AcceptanceTariff => self.acceptance_interval,
            RequestClass::PromotionCampaign => self.promotion_campaign_interval,
            RequestClass::PromotionStats => self.promotion_stats_interval,
            RequestClass::SearchReport => self.search_report_interval,
            RequestClass::PromotionMinimumBids => self.promotion_minimum_bids_interval,
            RequestClass::PromotionRecommendedBids => self.promotion_recommendations_interval,
            RequestClass::PromotionClusterBids => self.promotion_cluster_bids_interval,
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

    /// Waits until a departure could be claimed, without consuming it. The
    /// caller must acquire the network permits before [`Self::try_claim`], so
    /// local overload cannot burn a marketplace quota slot.
    async fn wait_until_ready(&self) {
        loop {
            let wait = self
                .next_allowed
                .lock()
                .await
                .saturating_duration_since(Instant::now());
            if wait.is_zero() {
                return;
            }
            sleep(wait).await;
        }
    }

    /// Checks availability without consuming a minute-scale quota slot.
    async fn ensure_ready_now(&self) -> Result<(), Duration> {
        let next_allowed = self.next_allowed.lock().await;
        let wait = next_allowed.saturating_duration_since(Instant::now());
        if !wait.is_zero() {
            return Err(wait);
        }
        Ok(())
    }

    async fn ready_in(&self) -> Duration {
        self.next_allowed
            .lock()
            .await
            .saturating_duration_since(Instant::now())
    }

    /// Atomically consumes one departure only after both network permits have
    /// been acquired. A competing caller may win between readiness and this
    /// claim; the loser releases its permits and returns to readiness waiting.
    async fn try_claim(&self, interval: Duration) -> Result<(), Duration> {
        let mut next_allowed = self.next_allowed.lock().await;
        let wait = next_allowed.saturating_duration_since(Instant::now());
        if !wait.is_zero() {
            return Err(wait);
        }
        *next_allowed = Instant::now() + interval;
        Ok(())
    }

    /// Extends a shared token/class cooldown without ever shortening an
    /// existing quota or another response's longer server-directed delay.
    async fn extend_cooldown(&self, delay: Duration) {
        let cooldown_until = Instant::now() + delay;
        let mut next_allowed = self.next_allowed.lock().await;
        if *next_allowed < cooldown_until {
            *next_allowed = cooldown_until;
        }
    }
}

#[derive(Debug)]
struct TokenLimiter {
    in_flight: Semaphore,
    analytics_ping: PacingGate,
    analytics_reports: PacingGate,
    statistics_reports: PacingGate,
    content_reports: PacingGate,
    prices_reports: PacingGate,
    commission_tariffs: PacingGate,
    logistics_tariffs: PacingGate,
    acceptance_tariffs: PacingGate,
    promotion_campaigns: PacingGate,
    promotion_stats: PacingGate,
    search_reports: PacingGate,
    promotion_minimum_bids: PacingGate,
    promotion_recommendations: PacingGate,
    promotion_cluster_bids: PacingGate,
}

impl TokenLimiter {
    fn new() -> Self {
        Self {
            in_flight: Semaphore::new(MAX_IN_FLIGHT_REQUESTS_PER_TOKEN),
            analytics_ping: PacingGate::new(),
            analytics_reports: PacingGate::new(),
            statistics_reports: PacingGate::new(),
            content_reports: PacingGate::new(),
            prices_reports: PacingGate::new(),
            commission_tariffs: PacingGate::new(),
            logistics_tariffs: PacingGate::new(),
            acceptance_tariffs: PacingGate::new(),
            promotion_campaigns: PacingGate::new(),
            promotion_stats: PacingGate::new(),
            search_reports: PacingGate::new(),
            promotion_minimum_bids: PacingGate::new(),
            promotion_recommendations: PacingGate::new(),
            promotion_cluster_bids: PacingGate::new(),
        }
    }

    fn gate(&self, request_class: RequestClass) -> &PacingGate {
        match request_class {
            RequestClass::AnalyticsPing => &self.analytics_ping,
            RequestClass::AnalyticsReport => &self.analytics_reports,
            RequestClass::StatisticsReport => &self.statistics_reports,
            RequestClass::ContentReport => &self.content_reports,
            RequestClass::PricesReport => &self.prices_reports,
            RequestClass::CommissionTariff => &self.commission_tariffs,
            RequestClass::LogisticsTariff => &self.logistics_tariffs,
            RequestClass::AcceptanceTariff => &self.acceptance_tariffs,
            RequestClass::PromotionCampaign => &self.promotion_campaigns,
            RequestClass::PromotionStats => &self.promotion_stats,
            RequestClass::SearchReport => &self.search_reports,
            RequestClass::PromotionMinimumBids => &self.promotion_minimum_bids,
            RequestClass::PromotionRecommendedBids => &self.promotion_recommendations,
            RequestClass::PromotionClusterBids => &self.promotion_cluster_bids,
        }
    }

    async fn wait_until_ready(
        &self,
        request_class: RequestClass,
        retry: bool,
        deadline: TokioInstant,
    ) -> Result<(), WbError> {
        let gate = self.gate(request_class);
        // Classes whose quota slot is minute-scale are never queued for: a
        // caller that missed the slot is told when to come back instead of
        // parking on it. `StatisticsReport` paces at a full 60s — the same as
        // `CommissionTariff` — but was absent here, so its callers queued for
        // an entire interval only to expire against the 60s logical timeout.
        if !retry
            && matches!(
                request_class,
                RequestClass::CommissionTariff
                    | RequestClass::StatisticsReport
                    | RequestClass::PromotionStats
                    | RequestClass::SearchReport
            )
        {
            return gate
                .ensure_ready_now()
                .await
                .map_err(|retry_after| WbError::LocalRateLimited { retry_after });
        }
        // A wait that cannot end before the caller's deadline is not a wait,
        // it is a timeout dressed as one — and an expensive one, because the
        // MCP request slot and the HTTP connection stay held for its whole
        // duration before failing. `StatisticsReport` paces at exactly the
        // logical timeout, so a second concurrent caller was guaranteed to
        // spend a full minute reaching `Timeout`. Refuse now instead, naming
        // the instant a retry could actually succeed.
        let ready_in = gate.ready_in().await;
        if TokioInstant::now() + ready_in >= deadline {
            return Err(WbError::LocalRateLimited {
                retry_after: ready_in,
            });
        }
        gate.wait_until_ready().await;
        Ok(())
    }

    async fn try_claim(
        &self,
        request_class: RequestClass,
        interval: Duration,
    ) -> Result<(), Duration> {
        self.gate(request_class).try_claim(interval).await
    }

    async fn extend_cooldown(&self, request_class: RequestClass, delay: Duration) {
        self.gate(request_class).extend_cooldown(delay).await;
    }

    async fn ready_in(&self, request_class: RequestClass) -> Duration {
        self.gate(request_class).ready_in().await
    }
}

#[derive(Debug, Clone)]
struct BaseUrls {
    analytics: String,
    statistics: String,
    content: String,
    prices: String,
    common: String,
    promotion: String,
}

impl BaseUrls {
    fn production() -> Self {
        Self {
            analytics: ANALYTICS_API_BASE_URL.to_owned(),
            statistics: STATISTICS_API_BASE_URL.to_owned(),
            content: CONTENT_API_BASE_URL.to_owned(),
            prices: PRICES_API_BASE_URL.to_owned(),
            common: COMMON_API_BASE_URL.to_owned(),
            promotion: PROMOTION_API_BASE_URL.to_owned(),
        }
    }

    #[cfg(test)]
    fn for_test(common_base_url: &str, analytics_base_url: &str) -> Self {
        let common = common_base_url.trim_end_matches('/').to_owned();
        Self {
            analytics: analytics_base_url.trim_end_matches('/').to_owned(),
            statistics: common.clone(),
            content: common.clone(),
            prices: common.clone(),
            common: common.clone(),
            promotion: common,
        }
    }

    fn base_url(&self, host: ApiHost) -> &str {
        match host {
            ApiHost::Analytics => &self.analytics,
            ApiHost::Statistics => &self.statistics,
            ApiHost::Content => &self.content,
            ApiHost::Prices => &self.prices,
            ApiHost::Common => &self.common,
            ApiHost::Promotion => &self.promotion,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WbClient {
    http: Client,
    base_urls: BaseUrls,
    accounts: Arc<BTreeMap<String, WbCredentials>>,
    limiters: Arc<BTreeMap<String, Arc<TokenLimiter>>>,
    global_in_flight: Arc<Semaphore>,
    logical_timeout: Duration,
    policy: ClientPolicy,
}

#[derive(Clone, Copy)]
struct AttemptContext<'a> {
    account: &'a str,
    /// The logical deadline for the whole call, used to refuse a pacing wait
    /// that could never finish inside it.
    deadline: TokioInstant,
    method: &'a Method,
    endpoint: &'static str,
    request_class: RequestClass,
    limiter: &'a TokenLimiter,
    url: &'a str,
    authorization: &'a HeaderValue,
    payload: Option<&'a Value>,
}

enum AttemptOutcome {
    Complete(Value),
    Retry { delay: Duration, error: WbError },
}

impl WbClient {
    #[must_use]
    pub fn new(timeout: Duration, accounts: BTreeMap<String, WbCredentials>) -> Self {
        Self::build(
            timeout,
            accounts,
            BaseUrls::production(),
            ClientPolicy::production(timeout),
        )
    }

    /// Builds a client which uses one deployment-owned HTTPS forward proxy.
    ///
    /// Ambient proxy variables remain disabled. This constructor is reserved
    /// for the isolated report collector; the proxy itself pins the exact WB
    /// read-only API hosts that the central endpoint allowlist can reach.
    pub fn new_with_https_proxy(
        timeout: Duration,
        accounts: BTreeMap<String, WbCredentials>,
        proxy_url: &str,
    ) -> Result<Self, reqwest::Error> {
        Self::try_build(
            timeout,
            accounts,
            BaseUrls::production(),
            ClientPolicy::production(timeout),
            Some(proxy_url),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        timeout: Duration,
        accounts: BTreeMap<String, WbCredentials>,
        common_base_url: &str,
        analytics_base_url: &str,
    ) -> Self {
        Self::build(
            timeout,
            accounts,
            BaseUrls::for_test(common_base_url, analytics_base_url),
            ClientPolicy::immediate_single_attempt(timeout),
        )
    }

    #[cfg(test)]
    #[allow(clippy::large_types_passed_by_value)]
    fn new_for_test_with_policy(
        timeout: Duration,
        accounts: BTreeMap<String, WbCredentials>,
        base_url: &str,
        policy: ClientPolicy,
    ) -> Self {
        Self::build(
            timeout,
            accounts,
            BaseUrls::for_test(base_url, base_url),
            policy,
        )
    }

    #[allow(clippy::large_types_passed_by_value)]
    fn build(
        timeout: Duration,
        accounts: BTreeMap<String, WbCredentials>,
        base_urls: BaseUrls,
        policy: ClientPolicy,
    ) -> Self {
        Self::try_build(timeout, accounts, base_urls, policy, None)
            .expect("static WB HTTP client configuration must be valid")
    }

    #[allow(clippy::large_types_passed_by_value)]
    fn try_build(
        timeout: Duration,
        accounts: BTreeMap<String, WbCredentials>,
        base_urls: BaseUrls,
        policy: ClientPolicy,
        explicit_https_proxy: Option<&str>,
    ) -> Result<Self, reqwest::Error> {
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
            .no_proxy();
        let http = match explicit_https_proxy {
            Some(proxy_url) => http.proxy(Proxy::https(proxy_url)?),
            None => http,
        }
        .build()?;
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
        Ok(Self {
            http,
            base_urls,
            accounts: Arc::new(accounts),
            limiters: Arc::new(limiters),
            global_in_flight: Arc::new(Semaphore::new(MAX_GLOBAL_IN_FLIGHT_REQUESTS)),
            logical_timeout: policy.logical_timeout,
            policy,
        })
    }

    #[must_use]
    pub fn empty(timeout: Duration) -> Self {
        Self::new(timeout, BTreeMap::new())
    }

    #[must_use]
    pub fn is_configured(&self, account: &str) -> bool {
        self.accounts.contains_key(account)
    }

    pub async fn ping(&self, account: &str) -> Result<Value, WbError> {
        self.request(account, Method::GET, PING_PATH, None, None)
            .await
    }

    pub async fn sales_funnel(&self, account: &str, payload: Value) -> Result<Value, WbError> {
        self.request(
            account,
            Method::POST,
            SALES_FUNNEL_PATH,
            None,
            Some(payload),
        )
        .await
    }

    pub async fn sales_funnel_history(
        &self,
        account: &str,
        payload: Value,
    ) -> Result<Value, WbError> {
        self.request(
            account,
            Method::POST,
            SALES_FUNNEL_HISTORY_PATH,
            None,
            Some(payload),
        )
        .await
    }

    pub async fn sales_funnel_grouped_history(
        &self,
        account: &str,
        payload: Value,
    ) -> Result<Value, WbError> {
        self.request(
            account,
            Method::POST,
            SALES_FUNNEL_GROUPED_HISTORY_PATH,
            None,
            Some(payload),
        )
        .await
    }

    pub async fn warehouse_stocks(&self, account: &str, payload: Value) -> Result<Value, WbError> {
        self.request(
            account,
            Method::POST,
            WAREHOUSE_STOCKS_PATH,
            None,
            Some(payload),
        )
        .await
    }

    pub async fn orders(
        &self,
        account: &str,
        date_from: String,
        flag: u8,
    ) -> Result<Value, WbError> {
        self.statistics_report(account, ORDERS_PATH, date_from, flag)
            .await
    }

    pub async fn sales(
        &self,
        account: &str,
        date_from: String,
        flag: u8,
    ) -> Result<Value, WbError> {
        self.statistics_report(account, SALES_PATH, date_from, flag)
            .await
    }

    pub async fn product_cards(
        &self,
        account: &str,
        locale: Option<String>,
        payload: Value,
    ) -> Result<Value, WbError> {
        self.request(
            account,
            Method::POST,
            PRODUCT_CARDS_PATH,
            locale.map(|locale| vec![("locale", locale)]),
            Some(payload),
        )
        .await
    }

    pub async fn product_prices(
        &self,
        account: &str,
        nm_id: Option<u64>,
        limit: u32,
        offset: u32,
    ) -> Result<Value, WbError> {
        let mut query = vec![("limit", limit.to_string()), ("offset", offset.to_string())];
        if let Some(nm_id) = nm_id {
            query.push(("filterNmID", nm_id.to_string()));
        }
        self.request(account, Method::GET, PRODUCT_PRICES_PATH, Some(query), None)
            .await
    }

    pub async fn tariff_commissions(
        &self,
        account: &str,
        locale: Option<String>,
    ) -> Result<Value, WbError> {
        self.request(
            account,
            Method::GET,
            TARIFF_COMMISSIONS_PATH,
            locale.map(|locale| vec![("locale", locale)]),
            None,
        )
        .await
    }

    pub async fn tariff_boxes(&self, account: &str, date: String) -> Result<Value, WbError> {
        self.dated_tariff(account, TARIFF_BOXES_PATH, date).await
    }

    pub async fn tariff_pallets(&self, account: &str, date: String) -> Result<Value, WbError> {
        self.dated_tariff(account, TARIFF_PALLETS_PATH, date).await
    }

    pub async fn tariff_returns(&self, account: &str, date: String) -> Result<Value, WbError> {
        self.dated_tariff(account, TARIFF_RETURNS_PATH, date).await
    }

    pub async fn acceptance_coefficients(
        &self,
        account: &str,
        warehouse_ids: Vec<u64>,
    ) -> Result<Value, WbError> {
        let query = (!warehouse_ids.is_empty()).then(|| {
            vec![(
                "warehouseIDs",
                warehouse_ids
                    .into_iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            )]
        });
        self.request(
            account,
            Method::GET,
            ACCEPTANCE_COEFFICIENTS_PATH,
            query,
            None,
        )
        .await
    }

    /// Lists all promotion campaigns grouped by type and status.
    ///
    /// Although some WB write operations also use `GET`, this method can only
    /// reach anything except the exact read-only path enforced by the client allowlist.
    pub async fn promotion_campaigns(&self, account: &str) -> Result<Value, WbError> {
        self.request(account, Method::GET, PROMOTION_CAMPAIGNS_PATH, None, None)
            .await
    }

    /// Returns details for promotion campaigns using only documented filters.
    pub async fn promotion_campaign_details(
        &self,
        account: &str,
        ids: Vec<u64>,
        statuses: Vec<i32>,
        payment_type: Option<String>,
    ) -> Result<Value, WbError> {
        // Local safety cap: callers must select a bounded campaign set rather
        // than accidentally requesting every campaign in a large account.
        validate_promotion_ids(&ids, MAX_PROMOTION_CAMPAIGN_IDS)?;
        validate_promotion_statuses(&statuses)?;
        validate_payment_type(payment_type.as_deref())?;

        let mut query = vec![("ids", comma_separated(ids))];
        if !statuses.is_empty() {
            query.push(("statuses", comma_separated(statuses)));
        }
        if let Some(payment_type) = payment_type {
            query.push(("payment_type", payment_type));
        }
        self.request(
            account,
            Method::GET,
            PROMOTION_DETAILS_PATH,
            Some(query),
            None,
        )
        .await
    }

    /// Returns campaign statistics for an inclusive period of at most 31 days.
    pub async fn promotion_stats(
        &self,
        account: &str,
        ids: Vec<u64>,
        begin_date: String,
        end_date: String,
    ) -> Result<Value, WbError> {
        validate_promotion_ids(&ids, MAX_PROMOTION_STATS_IDS)?;
        validate_promotion_period(&begin_date, &end_date)?;
        self.request(
            account,
            Method::GET,
            PROMOTION_STATS_PATH,
            Some(vec![
                ("ids", comma_separated(ids)),
                ("beginDate", begin_date),
                ("endDate", end_date),
            ]),
            None,
        )
        .await
    }

    /// Returns the bounded top search texts for selected products.
    ///
    /// This deployment intentionally enables the Analytics/Jam report only for
    /// Personal tokens. WB also documents other token classes with different
    /// quotas; production configuration rejects those unsupported policies,
    /// while this client keeps the selected report quota isolated and fail-fast.
    #[allow(clippy::too_many_arguments)]
    pub async fn search_product_queries(
        &self,
        account: &str,
        current_start: String,
        current_end: String,
        past_period: Option<(String, String)>,
        nm_ids: Vec<u64>,
        top_order_by: String,
        limit: u32,
    ) -> Result<Value, WbError> {
        let current_days = validate_search_period(
            &current_start,
            &current_end,
            "current_start",
            "current_end",
            MAX_SEARCH_PRODUCT_PERIOD_DAYS,
        )?;
        validate_positive_unique_ids(&nm_ids, MAX_SEARCH_REPORT_NM_IDS, "nm_ids", None)?;
        validate_top_order_by(&top_order_by)?;
        if !(1..=MAX_SEARCH_REPORT_LIMIT).contains(&limit) {
            return Err(WbError::InvalidArguments { field: "limit" });
        }

        let mut payload = serde_json::json!({
            "currentPeriod": {"start": current_start, "end": current_end},
            "nmIds": nm_ids,
            "topOrderBy": top_order_by,
            "includeSubstitutedSKUs": true,
            "includeSearchTexts": true,
            "orderBy": {"field": "avgPosition", "mode": "asc"},
            "limit": limit,
        });
        if let Some((past_start, past_end)) = past_period {
            let past_days = validate_search_period(
                &past_start,
                &past_end,
                "past_start",
                "past_end",
                MAX_SEARCH_PRODUCT_PERIOD_DAYS,
            )?;
            if past_days > current_days {
                return Err(WbError::InvalidArguments {
                    field: "past_period",
                });
            }
            payload
                .as_object_mut()
                .expect("fixed search payload is an object")
                .insert(
                    "pastPeriod".to_owned(),
                    serde_json::json!({"start": past_start, "end": past_end}),
                );
        }

        self.request(
            account,
            Method::POST,
            SEARCH_PRODUCT_QUERIES_PATH,
            None,
            Some(payload),
        )
        .await
    }

    /// Returns orders and average positions for an explicit bounded set of
    /// search texts. This deployment requires its Personal Analytics/Jam policy.
    pub async fn search_orders_positions(
        &self,
        account: &str,
        start: String,
        end: String,
        nm_id: u64,
        search_texts: Vec<String>,
    ) -> Result<Value, WbError> {
        validate_search_period(&start, &end, "start", "end", MAX_SEARCH_ORDERS_PERIOD_DAYS)?;
        validate_unsigned_id(nm_id, "nm_id", None)?;
        validate_search_texts(&search_texts)?;
        self.request(
            account,
            Method::POST,
            SEARCH_ORDERS_POSITIONS_PATH,
            None,
            Some(serde_json::json!({
                "period": {"start": start, "end": end},
                "nmId": nm_id,
                "searchTexts": search_texts,
            })),
        )
        .await
    }

    /// Returns the minimum read-only campaign bids for the requested products
    /// and placements. It cannot reach the neighboring bid mutation route.
    pub async fn promotion_minimum_bids(
        &self,
        account: &str,
        advert_id: u64,
        nm_ids: Vec<u64>,
        payment_type: String,
        placement_types: Vec<String>,
    ) -> Result<Value, WbError> {
        validate_unsigned_id(advert_id, "advert_id", Some(MAX_WB_SIGNED_ID))?;
        validate_positive_unique_ids(
            &nm_ids,
            MAX_PROMOTION_BID_NM_IDS,
            "nm_ids",
            Some(MAX_WB_SIGNED_ID),
        )?;
        validate_payment_type(Some(&payment_type))?;
        validate_placement_types(&placement_types)?;
        self.request(
            account,
            Method::POST,
            PROMOTION_MINIMUM_BIDS_PATH,
            None,
            Some(serde_json::json!({
                "advert_id": advert_id,
                "nm_ids": nm_ids,
                "payment_type": payment_type,
                "placement_types": placement_types,
            })),
        )
        .await
    }

    /// Returns recommended bids for one campaign/product pair.
    pub async fn promotion_recommended_bids(
        &self,
        account: &str,
        advert_id: u64,
        nm_id: u64,
    ) -> Result<Value, WbError> {
        validate_unsigned_id(advert_id, "advert_id", Some(MAX_WB_SIGNED_ID))?;
        validate_unsigned_id(nm_id, "nm_id", Some(MAX_WB_SIGNED_ID))?;
        self.request(
            account,
            Method::GET,
            PROMOTION_RECOMMENDATIONS_PATH,
            Some(vec![
                ("nmId", nm_id.to_string()),
                ("advertId", advert_id.to_string()),
            ]),
            None,
        )
        .await
    }

    /// Returns existing bids for a bounded set of campaign/product pairs.
    pub async fn promotion_search_cluster_bids(
        &self,
        account: &str,
        items: Vec<(u64, u64)>,
    ) -> Result<Value, WbError> {
        validate_bid_items(&items)?;
        let items = items
            .into_iter()
            .map(|(advert_id, nm_id)| {
                serde_json::json!({
                    "advert_id": advert_id,
                    "nm_id": nm_id,
                })
            })
            .collect::<Vec<_>>();
        self.request(
            account,
            Method::POST,
            PROMOTION_CLUSTER_BIDS_PATH,
            None,
            Some(serde_json::json!({"items": items})),
        )
        .await
    }

    async fn dated_tariff(
        &self,
        account: &str,
        path: &'static str,
        date: String,
    ) -> Result<Value, WbError> {
        self.request(account, Method::GET, path, Some(vec![("date", date)]), None)
            .await
    }

    async fn statistics_report(
        &self,
        account: &str,
        path: &'static str,
        date_from: String,
        flag: u8,
    ) -> Result<Value, WbError> {
        self.request(
            account,
            Method::GET,
            path,
            Some(vec![("dateFrom", date_from), ("flag", flag.to_string())]),
            None,
        )
        .await
    }

    /// Drives [`Self::request`] with an arbitrary method and path so the
    /// read-only guard can be exercised. Public methods above are allowlisted
    /// allowlisted by construction and cannot reach the denial branch.
    #[cfg(test)]
    pub(crate) async fn request_for_test(
        &self,
        account: &str,
        method: Method,
        path: &'static str,
    ) -> Result<Value, WbError> {
        self.request(account, method, path, None, None).await
    }

    async fn request(
        &self,
        account: &str,
        method: Method,
        path: &'static str,
        query: Option<Vec<(&'static str, String)>>,
        payload: Option<Value>,
    ) -> Result<Value, WbError> {
        // Enforced here, at the only point where a WB request can leave the
        // process, so the read-only guarantee does not depend on callers.
        let Some(endpoint_policy) = EndpointPolicy::for_request(&method, path) else {
            return Err(WbError::EndpointNotAllowed {
                method,
                path: path.to_owned(),
            });
        };
        let endpoint = endpoint_policy.label;
        let request_class = endpoint_policy.request_class;
        let base_url = self.base_urls.base_url(endpoint_policy.host);
        let mut url = Url::parse(&format!("{base_url}{path}"))
            .expect("static production or validated test WB base URL");
        if let Some(query) = query {
            url.query_pairs_mut().extend_pairs(query);
        }
        let url = url.to_string();
        let credentials = self
            .accounts
            .get(account)
            .ok_or_else(|| WbError::MissingCredentials(account.to_owned()))?;
        let limiter = self
            .limiters
            .get(account)
            .expect("configured WB account has a limiter");
        let authorization = bearer_authorization(&credentials.token)?;

        let deadline = TokioInstant::now() + self.logical_timeout;
        self.request_with_retries(
            account,
            method,
            endpoint,
            request_class,
            limiter,
            url,
            authorization,
            payload,
            deadline,
        )
        .await
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
        deadline: TokioInstant,
    ) -> Result<Value, WbError> {
        let context = AttemptContext {
            account,
            deadline,
            method: &method,
            endpoint,
            request_class,
            limiter,
            url: &url,
            authorization: &authorization,
            payload: payload.as_ref(),
        };
        let mut attempt = 1;
        let mut preceding_retry_error = None;
        loop {
            let retry = preceding_retry_error.is_some();
            let outcome =
                match timeout_at(deadline, self.request_attempt(context, attempt, retry)).await {
                    Err(_) => {
                        return Err(preceding_retry_error.unwrap_or(WbError::DeadlineExceeded));
                    }
                    Ok(Err(error)) if is_local_admission_error(&error) => {
                        return Err(preceding_retry_error.take().unwrap_or(error));
                    }
                    Ok(Err(error)) => return Err(error),
                    Ok(Ok(outcome)) => outcome,
                };
            match outcome {
                AttemptOutcome::Complete(value) => return Ok(value),
                AttemptOutcome::Retry { delay, error } => {
                    // A timed-out quota-state read means no retry can fit. Map
                    // it to an impossible wait so the shared fit check below
                    // preserves the causal upstream error through one path.
                    let ready_in =
                        timeout_at(deadline, context.limiter.ready_in(context.request_class))
                            .await
                            .unwrap_or(Duration::MAX);
                    let remaining = deadline.saturating_duration_since(TokioInstant::now());
                    if !retry_wait_fits_deadline(remaining, delay, ready_in) {
                        return Err(error);
                    }
                    preceding_retry_error = Some(error);
                    // The strict fit check proves this sleep's deadline is
                    // earlier than the logical deadline. Tokio polls the inner
                    // sleep first even if scheduling wakes both timers late,
                    // so wrapping it in `timeout_at` had an unreachable error
                    // branch. The next attempt remains bounded by the outer
                    // `timeout_at(deadline, request_attempt(...))` above.
                    sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }

    async fn acquire_request_permits<'a>(
        &'a self,
        limiter: &'a TokenLimiter,
        request_class: RequestClass,
        retry: bool,
        deadline: TokioInstant,
    ) -> Result<(SemaphorePermit<'a>, SemaphorePermit<'a>), WbError> {
        let interval = self.policy.interval(request_class);
        loop {
            // Readiness does not consume quota. Queued callers therefore hold
            // no network capacity, and a fail-fast permit rejection cannot
            // postpone the next real WB request by 20 or 60s.
            limiter
                .wait_until_ready(request_class, retry, deadline)
                .await?;
            let global_permit = self
                .global_in_flight
                .try_acquire()
                .map_err(|_| WbError::Overloaded)?;
            let Ok(token_permit) = limiter.in_flight.try_acquire() else {
                drop(global_permit);
                return Err(WbError::Overloaded);
            };
            if limiter.try_claim(request_class, interval).await.is_ok() {
                return Ok((global_permit, token_permit));
            }

            // Another ready caller claimed the departure between our readiness
            // check and permit acquisition. Never queue while reserving scarce
            // HTTP capacity; retry the readiness phase.
            drop(token_permit);
            drop(global_permit);
        }
    }

    async fn request_attempt(
        &self,
        context: AttemptContext<'_>,
        attempt: usize,
        retry: bool,
    ) -> Result<AttemptOutcome, WbError> {
        // Both permits are released when this helper returns, before the retry
        // loop performs any backoff sleep.
        let (_global_permit, _token_permit) = self
            .acquire_request_permits(
                context.limiter,
                context.request_class,
                retry,
                context.deadline,
            )
            .await?;
        let mut request = self
            .http
            .request(context.method.clone(), context.url)
            .header(AUTHORIZATION, context.authorization.clone());
        if let Some(payload) = context.payload {
            request = request.json(payload);
        }

        let started = Instant::now();
        match request.send().await {
            Ok(response) => {
                self.response_outcome(context, attempt, started, response)
                    .await
            }
            Err(source) => {
                transport_failure_outcome(context, attempt, started, source, &self.policy)
            }
        }
    }

    async fn response_outcome(
        &self,
        context: AttemptContext<'_>,
        attempt: usize,
        started: Instant,
        response: Response,
    ) -> Result<AttemptOutcome, WbError> {
        let status = response.status();
        let request_id = extract_request_id(response.headers());
        let retry_after = parse_retry_delay(response.headers(), Utc::now());
        let planned_retry = context
            .request_class
            .allows_automatic_retry()
            .then(|| retry_plan(status, attempt, retry_after, &self.policy))
            .flatten();
        let vendor_cooldown = match retry_after {
            ParsedRetryDelay::Valid(delay)
                if is_retriable(status) && delay <= self.policy.max_retry_delay =>
            {
                Some(delay)
            }
            ParsedRetryDelay::Absent | ParsedRetryDelay::Invalid | ParsedRetryDelay::Valid(_) => {
                None
            }
        };
        if let Some(delay) = planned_retry.into_iter().chain(vendor_cooldown).max() {
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

        let result = decode_response(response, request_id.clone(), retry_after.duration()).await;
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

fn retry_wait_fits_deadline(
    remaining: Duration,
    retry_delay: Duration,
    quota_wait: Duration,
) -> bool {
    retry_delay.max(quota_wait) < remaining
}

fn is_local_admission_error(error: &WbError) -> bool {
    matches!(
        error,
        WbError::LocalRateLimited { .. } | WbError::Overloaded
    )
}

fn classify_http_status(
    status: StatusCode,
    request_id: Option<String>,
    retry_after: Option<Duration>,
    diagnostic: String,
) -> WbError {
    match status {
        StatusCode::UNAUTHORIZED => WbError::Unauthorized { request_id },
        StatusCode::PAYMENT_REQUIRED => WbError::SubscriptionRequired { request_id },
        StatusCode::FORBIDDEN => WbError::Forbidden { request_id },
        StatusCode::TOO_MANY_REQUESTS => WbError::RateLimited {
            request_id,
            retry_after,
        },
        _ => WbError::Api {
            status,
            request_id,
            diagnostic,
        },
    }
}

fn transport_failure_outcome(
    context: AttemptContext<'_>,
    attempt: usize,
    started: Instant,
    source: reqwest::Error,
    policy: &ClientPolicy,
) -> Result<AttemptOutcome, WbError> {
    let error = classify_transport_error(source, None);
    let will_retry = context.request_class.allows_automatic_retry()
        && is_retriable_transport(error.kind())
        && attempt < policy.max_attempts;
    trace_transport_failure(
        context.account,
        context.endpoint,
        attempt,
        started,
        &error,
        will_retry,
    );
    if will_retry {
        Ok(AttemptOutcome::Retry {
            delay: retry_delay(attempt, None, policy),
            error,
        })
    } else {
        Err(error)
    }
}

fn comma_separated<T: ToString>(values: impl IntoIterator<Item = T>) -> String {
    values
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn bearer_authorization(token: &str) -> Result<HeaderValue, WbError> {
    let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| WbError::Unauthorized { request_id: None })?;
    // Prevent accidental disclosure if reqwest headers are ever formatted by
    // future middleware or debug instrumentation.
    authorization.set_sensitive(true);
    Ok(authorization)
}

fn validate_promotion_ids(ids: &[u64], maximum: usize) -> Result<(), WbError> {
    let unique = ids.iter().collect::<BTreeSet<_>>().len();
    if ids.is_empty() || ids.len() > maximum || ids.contains(&0) || unique != ids.len() {
        return Err(WbError::InvalidArguments { field: "ids" });
    }
    Ok(())
}

fn validate_promotion_statuses(statuses: &[i32]) -> Result<(), WbError> {
    let unique = statuses.iter().collect::<BTreeSet<_>>().len();
    if statuses.len() > 6
        || unique != statuses.len()
        || statuses
            .iter()
            .any(|status| !matches!(status, -1 | 4 | 7 | 8 | 9 | 11))
    {
        return Err(WbError::InvalidArguments { field: "statuses" });
    }
    Ok(())
}

fn validate_payment_type(payment_type: Option<&str>) -> Result<(), WbError> {
    if payment_type.is_some_and(|value| !matches!(value, "cpm" | "cpc")) {
        return Err(WbError::InvalidArguments {
            field: "payment_type",
        });
    }
    Ok(())
}

fn validate_unsigned_id(
    value: u64,
    field: &'static str,
    maximum: Option<u64>,
) -> Result<(), WbError> {
    if value == 0 || maximum.is_some_and(|maximum| value > maximum) {
        return Err(WbError::InvalidArguments { field });
    }
    Ok(())
}

fn validate_positive_unique_ids(
    values: &[u64],
    maximum_count: usize,
    field: &'static str,
    maximum_value: Option<u64>,
) -> Result<(), WbError> {
    let unique = values.iter().collect::<BTreeSet<_>>().len();
    if values.is_empty()
        || values.len() > maximum_count
        || unique != values.len()
        || values
            .iter()
            .any(|value| validate_unsigned_id(*value, field, maximum_value).is_err())
    {
        return Err(WbError::InvalidArguments { field });
    }
    Ok(())
}

fn validate_top_order_by(value: &str) -> Result<(), WbError> {
    if !matches!(
        value,
        "openCard" | "addToCart" | "openToCart" | "orders" | "cartToOrder"
    ) {
        return Err(WbError::InvalidArguments {
            field: "top_order_by",
        });
    }
    Ok(())
}

fn validate_search_texts(values: &[String]) -> Result<(), WbError> {
    let unique = values.iter().collect::<BTreeSet<_>>().len();
    if values.is_empty()
        || values.len() > MAX_SEARCH_REPORT_TEXTS
        || unique != values.len()
        || values.iter().any(|value| {
            value.is_empty()
                || value.len() > MAX_SEARCH_TEXT_BYTES
                || value.trim() != value
                || value.chars().any(char::is_control)
        })
    {
        return Err(WbError::InvalidArguments {
            field: "search_texts",
        });
    }
    Ok(())
}

fn validate_placement_types(values: &[String]) -> Result<(), WbError> {
    let unique = values.iter().collect::<BTreeSet<_>>().len();
    if values.is_empty()
        || values.len() > 3
        || unique != values.len()
        || values
            .iter()
            .any(|value| !matches!(value.as_str(), "combined" | "search" | "recommendation"))
    {
        return Err(WbError::InvalidArguments {
            field: "placement_types",
        });
    }
    Ok(())
}

fn validate_bid_items(items: &[(u64, u64)]) -> Result<(), WbError> {
    let unique = items.iter().collect::<BTreeSet<_>>().len();
    if items.is_empty()
        || items.len() > MAX_PROMOTION_CLUSTER_BID_ITEMS
        || unique != items.len()
        || items.iter().any(|(advert_id, nm_id)| {
            validate_unsigned_id(*advert_id, "items", Some(MAX_WB_SIGNED_ID)).is_err()
                || validate_unsigned_id(*nm_id, "items", Some(MAX_WB_SIGNED_ID)).is_err()
        })
    {
        return Err(WbError::InvalidArguments { field: "items" });
    }
    Ok(())
}

fn parse_strict_date(value: &str, field: &'static str) -> Result<NaiveDate, WbError> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| WbError::InvalidArguments { field })?;
    if date.format("%Y-%m-%d").to_string() != value {
        return Err(WbError::InvalidArguments { field });
    }
    Ok(date)
}

fn validate_search_period(
    start: &str,
    end: &str,
    start_field: &'static str,
    end_field: &'static str,
    maximum_days: i64,
) -> Result<i64, WbError> {
    let start = parse_strict_date(start, start_field)?;
    let end = parse_strict_date(end, end_field)?;
    let span = end.signed_duration_since(start).num_days();
    if !(0..maximum_days).contains(&span) {
        return Err(WbError::InvalidArguments {
            field: "date_range",
        });
    }
    Ok(span + 1)
}

fn validate_promotion_period(begin_date: &str, end_date: &str) -> Result<(), WbError> {
    let begin = parse_strict_date(begin_date, "begin_date")?;
    let end = parse_strict_date(end_date, "end_date")?;
    let days = end.signed_duration_since(begin).num_days();
    if !(0..=30).contains(&days) {
        return Err(WbError::InvalidArguments {
            field: "date_range",
        });
    }
    Ok(())
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

fn retry_delay(attempt: usize, server_delay: Option<Duration>, policy: &ClientPolicy) -> Duration {
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

fn trace_transport_failure(
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
fn extract_request_id(headers: &reqwest::header::HeaderMap) -> Option<String> {
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
            max_attempts: 3,
            base_retry_delay: Duration::ZERO,
            max_retry_delay: Duration::from_secs(1),
            ..ClientPolicy::immediate_single_attempt(logical_timeout)
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
            policy.interval(RequestClass::AnalyticsReport),
            Duration::from_secs(20)
        );
        assert_eq!(
            policy.interval(RequestClass::StatisticsReport),
            Duration::from_secs(60)
        );
        assert_eq!(
            policy.interval(RequestClass::ContentReport),
            Duration::from_millis(600)
        );
        assert_eq!(
            policy.interval(RequestClass::PricesReport),
            Duration::from_millis(600)
        );
        assert_eq!(
            policy.interval(RequestClass::CommissionTariff),
            Duration::from_secs(60)
        );
        assert_eq!(
            policy.interval(RequestClass::LogisticsTariff),
            Duration::from_secs(1)
        );
        assert_eq!(
            policy.interval(RequestClass::AcceptanceTariff),
            Duration::from_secs(10)
        );
        assert_eq!(
            policy.interval(RequestClass::PromotionCampaign),
            Duration::from_millis(200)
        );
        assert_eq!(
            policy.interval(RequestClass::PromotionStats),
            Duration::from_secs(20)
        );
        assert_eq!(
            policy.interval(RequestClass::SearchReport),
            Duration::from_secs(20)
        );
        assert_eq!(
            policy.interval(RequestClass::PromotionMinimumBids),
            Duration::from_secs(3)
        );
        assert_eq!(
            policy.interval(RequestClass::PromotionRecommendedBids),
            Duration::from_secs(12)
        );
        assert_eq!(
            policy.interval(RequestClass::PromotionClusterBids),
            Duration::from_millis(200)
        );
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.logical_timeout, Duration::from_secs(10));
        for request_class in [
            RequestClass::AnalyticsPing,
            RequestClass::AnalyticsReport,
            RequestClass::ContentReport,
            RequestClass::PricesReport,
            RequestClass::LogisticsTariff,
            RequestClass::AcceptanceTariff,
            RequestClass::PromotionCampaign,
            RequestClass::PromotionStats,
            RequestClass::PromotionMinimumBids,
            RequestClass::PromotionRecommendedBids,
            RequestClass::PromotionClusterBids,
        ] {
            assert!(request_class.allows_automatic_retry());
        }
        for request_class in [
            RequestClass::StatisticsReport,
            RequestClass::CommissionTariff,
            RequestClass::SearchReport,
        ] {
            assert!(!request_class.allows_automatic_retry());
        }
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
            Some(RequestClass::AnalyticsReport)
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, ORDERS_PATH),
            Some(RequestClass::StatisticsReport)
        );
        assert_eq!(
            RequestClass::for_request(&Method::POST, PRODUCT_CARDS_PATH),
            Some(RequestClass::ContentReport)
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, PRODUCT_PRICES_PATH),
            Some(RequestClass::PricesReport)
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, TARIFF_COMMISSIONS_PATH),
            Some(RequestClass::CommissionTariff)
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, TARIFF_BOXES_PATH),
            Some(RequestClass::LogisticsTariff)
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, ACCEPTANCE_COEFFICIENTS_PATH),
            Some(RequestClass::AcceptanceTariff)
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, PROMOTION_CAMPAIGNS_PATH),
            Some(RequestClass::PromotionCampaign)
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, PROMOTION_STATS_PATH),
            Some(RequestClass::PromotionStats)
        );
        for path in [SEARCH_PRODUCT_QUERIES_PATH, SEARCH_ORDERS_POSITIONS_PATH] {
            assert_eq!(
                RequestClass::for_request(&Method::POST, path),
                Some(RequestClass::SearchReport)
            );
        }
        assert_eq!(
            RequestClass::for_request(&Method::POST, PROMOTION_MINIMUM_BIDS_PATH),
            Some(RequestClass::PromotionMinimumBids)
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, PROMOTION_RECOMMENDATIONS_PATH),
            Some(RequestClass::PromotionRecommendedBids)
        );
        assert_eq!(
            RequestClass::for_request(&Method::POST, PROMOTION_CLUSTER_BIDS_PATH),
            Some(RequestClass::PromotionClusterBids)
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, "/not-allowed"),
            None
        );

        let urls = BaseUrls::production();
        assert_eq!(urls.base_url(ApiHost::Analytics), ANALYTICS_API_BASE_URL);
        assert_eq!(urls.base_url(ApiHost::Statistics), STATISTICS_API_BASE_URL);
        assert_eq!(urls.base_url(ApiHost::Content), CONTENT_API_BASE_URL);
        assert_eq!(urls.base_url(ApiHost::Prices), PRICES_API_BASE_URL);
        assert_eq!(urls.base_url(ApiHost::Common), COMMON_API_BASE_URL);
        assert_eq!(urls.base_url(ApiHost::Promotion), PROMOTION_API_BASE_URL);
    }

    #[test]
    fn production_promotion_stats_retry_wait_fits_the_sixty_second_deadline() {
        let policy = ClientPolicy::production(Duration::from_secs(30));
        assert_eq!(policy.logical_timeout, Duration::from_secs(60));
        assert_eq!(
            policy.interval(RequestClass::PromotionStats),
            Duration::from_secs(20)
        );
        assert!(retry_wait_fits_deadline(
            policy.logical_timeout,
            policy.base_retry_delay,
            policy.interval(RequestClass::PromotionStats),
        ));

        // Equality is deliberately rejected: a retry needs non-zero time for
        // permit acquisition, transport and response decoding after the gate.
        assert!(!retry_wait_fits_deadline(
            Duration::from_secs(20),
            policy.base_retry_delay,
            Duration::from_secs(20),
        ));
        assert!(!retry_wait_fits_deadline(
            Duration::from_secs(19),
            policy.base_retry_delay,
            Duration::from_secs(20),
        ));
    }

    #[test]
    fn endpoint_policy_table_matches_the_immutable_security_snapshot() {
        let expected = [
            (
                Method::GET,
                PING_PATH,
                "analytics:/ping",
                ApiHost::Analytics,
                RequestClass::AnalyticsPing,
            ),
            (
                Method::POST,
                SALES_FUNNEL_PATH,
                "analytics:/api/analytics/v3/sales-funnel/products",
                ApiHost::Analytics,
                RequestClass::AnalyticsReport,
            ),
            (
                Method::POST,
                SALES_FUNNEL_HISTORY_PATH,
                "analytics:/api/analytics/v3/sales-funnel/products/history",
                ApiHost::Analytics,
                RequestClass::AnalyticsReport,
            ),
            (
                Method::POST,
                SALES_FUNNEL_GROUPED_HISTORY_PATH,
                "analytics:/api/analytics/v3/sales-funnel/grouped/history",
                ApiHost::Analytics,
                RequestClass::AnalyticsReport,
            ),
            (
                Method::POST,
                WAREHOUSE_STOCKS_PATH,
                "analytics:/api/analytics/v1/stocks-report/wb-warehouses",
                ApiHost::Analytics,
                RequestClass::AnalyticsReport,
            ),
            (
                Method::GET,
                ORDERS_PATH,
                "statistics:/api/v1/supplier/orders",
                ApiHost::Statistics,
                RequestClass::StatisticsReport,
            ),
            (
                Method::GET,
                SALES_PATH,
                "statistics:/api/v1/supplier/sales",
                ApiHost::Statistics,
                RequestClass::StatisticsReport,
            ),
            (
                Method::POST,
                PRODUCT_CARDS_PATH,
                "content:/content/v2/get/cards/list",
                ApiHost::Content,
                RequestClass::ContentReport,
            ),
            (
                Method::GET,
                PRODUCT_PRICES_PATH,
                "prices:/api/v2/list/goods/filter",
                ApiHost::Prices,
                RequestClass::PricesReport,
            ),
            (
                Method::GET,
                TARIFF_COMMISSIONS_PATH,
                "common:/api/v1/tariffs/commission",
                ApiHost::Common,
                RequestClass::CommissionTariff,
            ),
            (
                Method::GET,
                TARIFF_BOXES_PATH,
                "common:/api/v1/tariffs/box",
                ApiHost::Common,
                RequestClass::LogisticsTariff,
            ),
            (
                Method::GET,
                TARIFF_PALLETS_PATH,
                "common:/api/v1/tariffs/pallet",
                ApiHost::Common,
                RequestClass::LogisticsTariff,
            ),
            (
                Method::GET,
                TARIFF_RETURNS_PATH,
                "common:/api/v1/tariffs/return",
                ApiHost::Common,
                RequestClass::LogisticsTariff,
            ),
            (
                Method::GET,
                ACCEPTANCE_COEFFICIENTS_PATH,
                "common:/api/tariffs/v1/acceptance/coefficients",
                ApiHost::Common,
                RequestClass::AcceptanceTariff,
            ),
            (
                Method::GET,
                PROMOTION_CAMPAIGNS_PATH,
                "promotion:/adv/v1/promotion/count",
                ApiHost::Promotion,
                RequestClass::PromotionCampaign,
            ),
            (
                Method::GET,
                PROMOTION_DETAILS_PATH,
                "promotion:/api/advert/v2/adverts",
                ApiHost::Promotion,
                RequestClass::PromotionCampaign,
            ),
            (
                Method::GET,
                PROMOTION_STATS_PATH,
                "promotion:/adv/v3/fullstats",
                ApiHost::Promotion,
                RequestClass::PromotionStats,
            ),
            (
                Method::POST,
                SEARCH_PRODUCT_QUERIES_PATH,
                "analytics:/api/v2/search-report/product/search-texts",
                ApiHost::Analytics,
                RequestClass::SearchReport,
            ),
            (
                Method::POST,
                SEARCH_ORDERS_POSITIONS_PATH,
                "analytics:/api/v2/search-report/product/orders",
                ApiHost::Analytics,
                RequestClass::SearchReport,
            ),
            (
                Method::POST,
                PROMOTION_MINIMUM_BIDS_PATH,
                "promotion:/api/advert/v1/bids/min",
                ApiHost::Promotion,
                RequestClass::PromotionMinimumBids,
            ),
            (
                Method::GET,
                PROMOTION_RECOMMENDATIONS_PATH,
                "promotion:/api/advert/v0/bids/recommendations",
                ApiHost::Promotion,
                RequestClass::PromotionRecommendedBids,
            ),
            (
                Method::POST,
                PROMOTION_CLUSTER_BIDS_PATH,
                "promotion:/adv/v0/normquery/get-bids",
                ApiHost::Promotion,
                RequestClass::PromotionClusterBids,
            ),
        ];
        assert_eq!(READ_ONLY_ENDPOINT_ALLOWLIST.len(), expected.len());
        for (policy, (method, path, label, host, request_class)) in
            READ_ONLY_ENDPOINT_ALLOWLIST.iter().zip(expected)
        {
            assert_eq!(policy.method, method, "method drift for {path}");
            assert_eq!(policy.path, path, "path drift for {path}");
            assert_eq!(policy.label, label, "observability label drift for {path}");
            assert_eq!(policy.host, host, "host drift for {path}");
            assert_eq!(
                policy.request_class, request_class,
                "quota class drift for {path}"
            );
        }

        let mut pairs = READ_ONLY_ENDPOINT_ALLOWLIST
            .iter()
            .map(|policy| (policy.method.as_str(), policy.path))
            .collect::<Vec<_>>();
        let original_len = pairs.len();
        pairs.sort_unstable();
        pairs.dedup();
        assert_eq!(pairs.len(), original_len);

        for policy in READ_ONLY_ENDPOINT_ALLOWLIST {
            assert!(policy.path.starts_with('/'));
            assert!(!policy.path.contains("//"));
            assert_eq!(
                EndpointPolicy::for_request(&policy.method, policy.path)
                    .map(|found| (found.host, found.request_class)),
                Some((policy.host, policy.request_class))
            );
        }
    }

    #[tokio::test]
    async fn exact_read_only_requests_and_success_responses() {
        let (base_url, requests) = mock_http(vec![
            (200, r#"{"Status":"OK"}"#.to_owned()),
            (200, r#"{"data":{"products":[]}}"#.to_owned()),
            (200, "[]".to_owned()),
            (200, r#"{"data":[]}"#.to_owned()),
            (200, r#"{"items":[]}"#.to_owned()),
            (200, "[]".to_owned()),
            (200, "[]".to_owned()),
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
        assert_eq!(
            client
                .sales_funnel_history("account", payload.clone())
                .await
                .unwrap(),
            json!([])
        );
        assert_eq!(
            client
                .sales_funnel_grouped_history("account", payload.clone())
                .await
                .unwrap()["data"],
            json!([])
        );
        assert_eq!(
            client
                .warehouse_stocks("account", payload.clone())
                .await
                .unwrap()["items"],
            json!([])
        );
        assert_eq!(
            client
                .orders("account", "2026-08-01T00:00:00Z".to_owned(), 0)
                .await
                .unwrap(),
            json!([])
        );
        assert_eq!(
            client
                .sales("account", "2026-08-01T00:00:00Z".to_owned(), 1)
                .await
                .unwrap(),
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
            payload.clone()
        );
        for expected_path in [
            SALES_FUNNEL_HISTORY_PATH,
            SALES_FUNNEL_GROUPED_HISTORY_PATH,
            WAREHOUSE_STOCKS_PATH,
        ] {
            let request = requests.recv().unwrap();
            assert!(
                request.starts_with(&format!("POST {expected_path} HTTP/1.1\r\n")),
                "{request}"
            );
            assert_eq!(
                serde_json::from_str::<Value>(request.split_once("\r\n\r\n").unwrap().1).unwrap(),
                payload
            );
        }
        let orders = requests.recv().unwrap();
        assert!(orders.starts_with(
            "GET /api/v1/supplier/orders?dateFrom=2026-08-01T00%3A00%3A00Z&flag=0 HTTP/1.1\r\n"
        ));
        let sales = requests.recv().unwrap();
        assert!(sales.starts_with(
            "GET /api/v1/supplier/sales?dateFrom=2026-08-01T00%3A00%3A00Z&flag=1 HTTP/1.1\r\n"
        ));
    }

    #[tokio::test]
    async fn search_and_bid_reads_have_exact_wire_contracts() {
        let (base_url, requests) = mock_http(vec![
            (200, r#"{"data":{"items":[]}}"#.to_owned()),
            (200, r#"{"data":{"items":[]}}"#.to_owned()),
            (200, r#"{"data":[]}"#.to_owned()),
            (200, r#"{"bids":[]}"#.to_owned()),
            (200, r#"{"normQueries":[]}"#.to_owned()),
            (200, r#"{"bids":[]}"#.to_owned()),
        ]);
        let client = client(&base_url);

        assert!(
            client
                .search_product_queries(
                    "account",
                    "2026-08-10".to_owned(),
                    "2026-08-12".to_owned(),
                    Some(("2026-08-01".to_owned(), "2026-08-03".to_owned())),
                    vec![162_579_635, 166_699_779],
                    "openToCart".to_owned(),
                    30,
                )
                .await
                .is_ok()
        );
        assert!(
            client
                .search_orders_positions(
                    "account",
                    "2026-08-06".to_owned(),
                    "2026-08-12".to_owned(),
                    162_579_635,
                    vec!["костюм".to_owned(), "костюм мужской".to_owned()],
                )
                .await
                .is_ok()
        );
        assert!(
            client
                .search_product_queries(
                    "account",
                    "2026-08-01".to_owned(),
                    "2026-08-31".to_owned(),
                    None,
                    vec![162_579_635],
                    "orders".to_owned(),
                    1,
                )
                .await
                .is_ok()
        );
        assert!(
            client
                .promotion_minimum_bids(
                    "account",
                    98_765_432,
                    vec![12_345_678, 87_654_321],
                    "cpm".to_owned(),
                    vec![
                        "combined".to_owned(),
                        "search".to_owned(),
                        "recommendation".to_owned(),
                    ],
                )
                .await
                .is_ok()
        );
        assert!(
            client
                .promotion_recommended_bids("account", 987_654_321, 123_456_789)
                .await
                .is_ok()
        );
        assert!(
            client
                .promotion_search_cluster_bids(
                    "account",
                    vec![(1_825_035, 983_512_347), (1_825_036, 983_512_348)],
                )
                .await
                .is_ok()
        );

        let expected = [
            (
                format!("POST {SEARCH_PRODUCT_QUERIES_PATH} HTTP/1.1\r\n"),
                Some(json!({
                    "currentPeriod": {"start": "2026-08-10", "end": "2026-08-12"},
                    "pastPeriod": {"start": "2026-08-01", "end": "2026-08-03"},
                    "nmIds": [162_579_635, 166_699_779],
                    "topOrderBy": "openToCart",
                    "includeSubstitutedSKUs": true,
                    "includeSearchTexts": true,
                    "orderBy": {"field": "avgPosition", "mode": "asc"},
                    "limit": 30,
                })),
            ),
            (
                format!("POST {SEARCH_ORDERS_POSITIONS_PATH} HTTP/1.1\r\n"),
                Some(json!({
                    "period": {"start": "2026-08-06", "end": "2026-08-12"},
                    "nmId": 162_579_635,
                    "searchTexts": ["костюм", "костюм мужской"],
                })),
            ),
            (
                format!("POST {SEARCH_PRODUCT_QUERIES_PATH} HTTP/1.1\r\n"),
                Some(json!({
                    "currentPeriod": {"start": "2026-08-01", "end": "2026-08-31"},
                    "nmIds": [162_579_635],
                    "topOrderBy": "orders",
                    "includeSubstitutedSKUs": true,
                    "includeSearchTexts": true,
                    "orderBy": {"field": "avgPosition", "mode": "asc"},
                    "limit": 1,
                })),
            ),
            (
                format!("POST {PROMOTION_MINIMUM_BIDS_PATH} HTTP/1.1\r\n"),
                Some(json!({
                    "advert_id": 98_765_432,
                    "nm_ids": [12_345_678, 87_654_321],
                    "payment_type": "cpm",
                    "placement_types": ["combined", "search", "recommendation"],
                })),
            ),
            (
                format!(
                    "GET {PROMOTION_RECOMMENDATIONS_PATH}?nmId=123456789&advertId=987654321 HTTP/1.1\r\n"
                ),
                None,
            ),
            (
                format!("POST {PROMOTION_CLUSTER_BIDS_PATH} HTTP/1.1\r\n"),
                Some(json!({
                    "items": [
                        {"advert_id": 1_825_035, "nm_id": 983_512_347},
                        {"advert_id": 1_825_036, "nm_id": 983_512_348},
                    ],
                })),
            ),
        ];

        for (request_line, body) in expected {
            let request = requests.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(request.starts_with(&request_line), "{request}");
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("authorization: bearer test-token")
            );
            if let Some(expected) = body {
                assert_eq!(
                    serde_json::from_str::<Value>(request.split_once("\r\n\r\n").unwrap().1)
                        .unwrap(),
                    expected
                );
            } else {
                assert!(request.ends_with("\r\n\r\n"));
                assert!(!request.to_ascii_lowercase().contains("content-type:"));
            }
        }
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn catalog_price_and_tariff_requests_have_exact_contracts() {
        let (base_url, requests) = mock_http(vec![
            (200, r#"{"cards":[]}"#.to_owned()),
            (200, r#"{"data":{"listGoods":[]}}"#.to_owned()),
            (200, r#"{"report":[]}"#.to_owned()),
            (200, r#"{"response":{"data":{}}}"#.to_owned()),
            (200, r#"{"response":{"data":{}}}"#.to_owned()),
            (200, r#"{"response":{"data":{}}}"#.to_owned()),
            (200, "[]".to_owned()),
            (200, "[]".to_owned()),
        ]);
        let client = client(&base_url);
        let cards_payload = json!({
            "settings": {
                "cursor": {"limit": 100},
                "filter": {"withPhoto": -1}
            }
        });

        assert_eq!(
            client
                .product_cards("account", Some("ru".to_owned()), cards_payload.clone())
                .await
                .unwrap()["cards"],
            json!([])
        );
        assert_eq!(
            client
                .product_prices("account", Some(123_456), 100, 25)
                .await
                .unwrap()["data"]["listGoods"],
            json!([])
        );
        assert_eq!(
            client
                .tariff_commissions("account", Some("en".to_owned()))
                .await
                .unwrap()["report"],
            json!([])
        );
        assert!(
            client
                .tariff_boxes("account", "2026-08-11".to_owned())
                .await
                .is_ok()
        );
        assert!(
            client
                .tariff_pallets("account", "2026-08-12".to_owned())
                .await
                .is_ok()
        );
        assert!(
            client
                .tariff_returns("account", "2026-08-13".to_owned())
                .await
                .is_ok()
        );
        assert_eq!(
            client
                .acceptance_coefficients("account", vec![507, 117_501])
                .await
                .unwrap(),
            json!([])
        );
        assert_eq!(
            client
                .acceptance_coefficients("account", Vec::new())
                .await
                .unwrap(),
            json!([])
        );

        let cards = requests.recv().unwrap();
        assert!(cards.starts_with("POST /content/v2/get/cards/list?locale=ru HTTP/1.1\r\n"));
        assert_eq!(
            serde_json::from_str::<Value>(cards.split_once("\r\n\r\n").unwrap().1).unwrap(),
            cards_payload
        );
        for expected in [
            "GET /api/v2/list/goods/filter?limit=100&offset=25&filterNmID=123456 HTTP/1.1\r\n",
            "GET /api/v1/tariffs/commission?locale=en HTTP/1.1\r\n",
            "GET /api/v1/tariffs/box?date=2026-08-11 HTTP/1.1\r\n",
            "GET /api/v1/tariffs/pallet?date=2026-08-12 HTTP/1.1\r\n",
            "GET /api/v1/tariffs/return?date=2026-08-13 HTTP/1.1\r\n",
            "GET /api/tariffs/v1/acceptance/coefficients?warehouseIDs=507%2C117501 HTTP/1.1\r\n",
            "GET /api/tariffs/v1/acceptance/coefficients HTTP/1.1\r\n",
        ] {
            let request = requests.recv().unwrap();
            assert!(request.starts_with(expected), "{request}");
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("authorization: bearer test-token")
            );
        }
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
    async fn every_request_class_targets_its_dedicated_fixed_host() {
        let (analytics, analytics_requests) = mock_http(vec![
            (200, r#"{"Status":"OK"}"#.to_owned()),
            (200, r#"{"data":{"products":[]}}"#.to_owned()),
        ]);
        let (statistics, statistics_requests) = mock_http(vec![(200, "[]".to_owned())]);
        let (content, content_requests) = mock_http(vec![(200, r#"{"cards":[]}"#.to_owned())]);
        let (prices, prices_requests) =
            mock_http(vec![(200, r#"{"data":{"listGoods":[]}}"#.to_owned())]);
        let (common, common_requests) = mock_http(vec![
            (200, r#"{"report":[]}"#.to_owned()),
            (200, r#"{"response":{"data":{}}}"#.to_owned()),
            (200, "[]".to_owned()),
        ]);
        let (promotion, promotion_requests) = mock_http(vec![
            (200, r#"{"adverts":[],"all":0}"#.to_owned()),
            (200, r#"{"adverts":[]}"#.to_owned()),
            (200, r#"{"adverts":[]}"#.to_owned()),
            (200, "[]".to_owned()),
        ]);
        let client = WbClient::build(
            Duration::from_secs(2),
            credentials(),
            BaseUrls {
                analytics,
                statistics,
                content,
                prices,
                common,
                promotion,
            },
            ClientPolicy::immediate_single_attempt(Duration::from_secs(2)),
        );

        client.ping("account").await.unwrap();
        client
            .sales_funnel("account", json!({"limit": 1}))
            .await
            .unwrap();
        client
            .orders("account", "2026-08-01T00:00:00Z".to_owned(), 0)
            .await
            .unwrap();
        client
            .product_cards("account", None, json!({"settings":{"cursor":{"limit":1}}}))
            .await
            .unwrap();
        client.product_prices("account", None, 1, 0).await.unwrap();
        client.tariff_commissions("account", None).await.unwrap();
        client
            .tariff_boxes("account", "2026-08-10".to_owned())
            .await
            .unwrap();
        client
            .acceptance_coefficients("account", Vec::new())
            .await
            .unwrap();
        client.promotion_campaigns("account").await.unwrap();
        client
            .promotion_campaign_details(
                "account",
                vec![12, 34],
                vec![9, 11],
                Some("cpm".to_owned()),
            )
            .await
            .unwrap();
        client
            .promotion_campaign_details("account", vec![56], Vec::new(), None)
            .await
            .unwrap();
        client
            .promotion_stats(
                "account",
                vec![12, 34],
                "2026-08-01".to_owned(),
                "2026-08-11".to_owned(),
            )
            .await
            .unwrap();

        assert!(
            analytics_requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with("GET /ping HTTP/1.1\r\n")
        );
        assert!(
            analytics_requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with("POST /api/analytics/v3/sales-funnel/products HTTP/1.1\r\n")
        );
        assert!(analytics_requests.try_recv().is_err());
        assert!(
            statistics_requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with("GET /api/v1/supplier/orders?dateFrom=")
        );
        assert!(statistics_requests.try_recv().is_err());
        assert!(
            content_requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with("POST /content/v2/get/cards/list HTTP/1.1\r\n")
        );
        assert!(content_requests.try_recv().is_err());
        assert!(
            prices_requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with("GET /api/v2/list/goods/filter?limit=1&offset=0 HTTP/1.1\r\n")
        );
        assert!(prices_requests.try_recv().is_err());
        for expected in [
            "GET /api/v1/tariffs/commission HTTP/1.1\r\n",
            "GET /api/v1/tariffs/box?date=2026-08-10 HTTP/1.1\r\n",
            "GET /api/tariffs/v1/acceptance/coefficients HTTP/1.1\r\n",
        ] {
            assert!(
                common_requests
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .starts_with(expected)
            );
        }
        assert!(common_requests.try_recv().is_err());
        for expected in [
            "GET /adv/v1/promotion/count HTTP/1.1\r\n",
            "GET /api/advert/v2/adverts?ids=12%2C34&statuses=9%2C11&payment_type=cpm HTTP/1.1\r\n",
            "GET /api/advert/v2/adverts?ids=56 HTTP/1.1\r\n",
            "GET /adv/v3/fullstats?ids=12%2C34&beginDate=2026-08-01&endDate=2026-08-11 HTTP/1.1\r\n",
        ] {
            let request = promotion_requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap();
            assert!(request.starts_with(expected), "{request}");
            assert!(request.ends_with("\r\n\r\n"), "GET must have no body");
            assert!(!request.to_ascii_lowercase().contains("content-type:"));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("authorization: bearer test-token")
            );
        }
        assert!(promotion_requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn search_reports_and_bid_reads_use_only_their_fixed_hosts() {
        let (analytics, analytics_requests) = mock_http(vec![
            (200, r#"{"data":{"items":[]}}"#.to_owned()),
            (200, r#"{"data":[]}"#.to_owned()),
        ]);
        let (promotion, promotion_requests) = mock_http(vec![
            (200, r#"{"bids":[]}"#.to_owned()),
            (200, r#"{"normQueries":[]}"#.to_owned()),
            (200, r#"{"bids":[]}"#.to_owned()),
        ]);
        let unreachable = "http://127.0.0.1:1".to_owned();
        let client = WbClient::build(
            Duration::from_secs(2),
            credentials(),
            BaseUrls {
                analytics,
                statistics: unreachable.clone(),
                content: unreachable.clone(),
                prices: unreachable.clone(),
                common: unreachable,
                promotion,
            },
            ClientPolicy::immediate_single_attempt(Duration::from_secs(2)),
        );

        client
            .search_product_queries(
                "account",
                "2026-08-10".to_owned(),
                "2026-08-10".to_owned(),
                None,
                vec![1],
                "orders".to_owned(),
                1,
            )
            .await
            .unwrap();
        client
            .search_orders_positions(
                "account",
                "2026-08-10".to_owned(),
                "2026-08-10".to_owned(),
                1,
                vec!["ручка".to_owned()],
            )
            .await
            .unwrap();
        client
            .promotion_minimum_bids(
                "account",
                1,
                vec![2],
                "cpc".to_owned(),
                vec!["search".to_owned()],
            )
            .await
            .unwrap();
        client
            .promotion_recommended_bids("account", 1, 2)
            .await
            .unwrap();
        client
            .promotion_search_cluster_bids("account", vec![(1, 2)])
            .await
            .unwrap();

        for expected in [
            format!("POST {SEARCH_PRODUCT_QUERIES_PATH} HTTP/1.1\r\n"),
            format!("POST {SEARCH_ORDERS_POSITIONS_PATH} HTTP/1.1\r\n"),
        ] {
            let request = analytics_requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap();
            assert!(request.starts_with(&expected), "{request}");
        }
        assert!(analytics_requests.try_recv().is_err());
        for expected in [
            format!("POST {PROMOTION_MINIMUM_BIDS_PATH} HTTP/1.1\r\n"),
            format!("GET {PROMOTION_RECOMMENDATIONS_PATH}?nmId=2&advertId=1 HTTP/1.1\r\n"),
            format!("POST {PROMOTION_CLUSTER_BIDS_PATH} HTTP/1.1\r\n"),
        ] {
            let request = promotion_requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap();
            assert!(request.starts_with(&expected), "{request}");
        }
        assert!(promotion_requests.try_recv().is_err());
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
            .try_acquire_many(
                u32::try_from(MAX_IN_FLIGHT_REQUESTS_PER_TOKEN)
                    .expect("per-token request limit fits u32"),
            )
            .unwrap();
        assert_eq!(
            client.ping("alias").await.unwrap_err().kind(),
            WbErrorKind::Overloaded
        );
    }

    #[tokio::test]
    async fn new_endpoint_quota_gates_are_per_token_shared_and_per_class_separate() {
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
        let classes = [
            RequestClass::SearchReport,
            RequestClass::PromotionMinimumBids,
            RequestClass::PromotionRecommendedBids,
            RequestClass::PromotionClusterBids,
        ];

        for (index, selected) in classes.iter().copied().enumerate() {
            for request_class in classes {
                *primary.gate(request_class).next_allowed.lock().await = Instant::now();
            }
            *primary.gate(selected).next_allowed.lock().await =
                Instant::now() + Duration::from_secs(2);

            assert!(std::ptr::eq(primary.gate(selected), alias.gate(selected)));
            assert!(!std::ptr::eq(primary.gate(selected), other.gate(selected)));
            assert!(alias.ready_in(selected).await > Duration::from_secs(1));
            assert!(other.ready_in(selected).await.is_zero());
            for (other_index, request_class) in classes.iter().copied().enumerate() {
                if other_index != index {
                    assert!(
                        alias.ready_in(request_class).await.is_zero(),
                        "{selected:?} must not consume the {request_class:?} gate"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn global_overload_does_not_burn_a_ready_departure_slot() {
        let (base_url, requests) = mock_http(vec![(200, r#"{"Status":"OK"}"#.to_owned())]);
        let mut policy = ClientPolicy::immediate_single_attempt(Duration::from_millis(250));
        policy.ping_interval = Duration::from_secs(20);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_millis(200),
            credentials(),
            &base_url,
            policy,
        );

        let all_global_permits = client
            .global_in_flight
            .try_acquire_many(
                u32::try_from(MAX_GLOBAL_IN_FLIGHT_REQUESTS)
                    .expect("global request limit fits u32"),
            )
            .unwrap();
        assert_eq!(
            client.ping("account").await.unwrap_err().kind(),
            WbErrorKind::Overloaded
        );
        assert!(requests.try_recv().is_err(), "overload must not reach WB");
        drop(all_global_permits);

        let response = tokio::time::timeout(Duration::from_secs(1), client.ping("account"))
            .await
            .expect("the unconsumed departure must remain immediately available")
            .unwrap();
        assert_eq!(response["Status"], "OK");
        assert!(
            requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with("GET /ping HTTP/1.1\r\n")
        );
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn token_overload_does_not_burn_a_minute_quota_slot() {
        let (base_url, requests) = mock_http(vec![(200, r#"{"report":[]}"#.to_owned())]);
        let mut policy = ClientPolicy::immediate_single_attempt(Duration::from_millis(250));
        policy.commission_interval = Duration::from_secs(60);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(1),
            credentials(),
            &base_url,
            policy,
        );
        let limiter = client.limiters.get("account").unwrap();

        let all_token_permits = limiter
            .in_flight
            .try_acquire_many(
                u32::try_from(MAX_IN_FLIGHT_REQUESTS_PER_TOKEN)
                    .expect("per-token request limit fits u32"),
            )
            .unwrap();
        assert_eq!(
            client
                .tariff_commissions("account", None)
                .await
                .unwrap_err()
                .kind(),
            WbErrorKind::Overloaded
        );
        assert_eq!(
            client.global_in_flight.available_permits(),
            MAX_GLOBAL_IN_FLIGHT_REQUESTS,
            "a rejected token permit must release the tentative global permit"
        );
        assert!(requests.try_recv().is_err(), "overload must not reach WB");
        drop(all_token_permits);

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            client.tariff_commissions("account", None),
        )
        .await
        .expect("the unconsumed minute quota must remain immediately available")
        .unwrap();
        assert_eq!(response["report"], json!([]));
        assert!(
            requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with("GET /api/v1/tariffs/commission HTTP/1.1\r\n")
        );
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn competing_ready_call_releases_permits_before_waiting_for_the_next_slot() {
        let (base_url, requests) = mock_http(vec![
            (200, r#"{"Status":"OK"}"#.to_owned()),
            (200, r#"{"Status":"OK"}"#.to_owned()),
        ]);
        let mut policy = ClientPolicy::immediate_single_attempt(Duration::from_secs(2));
        policy.ping_interval = Duration::from_millis(500);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(2),
            credentials(),
            &base_url,
            policy,
        );
        let limiter = Arc::clone(client.limiters.get("account").unwrap());

        // Queue both callers behind the same gate. Tokio's FIFO mutex then
        // lets both observe readiness before either can reacquire the gate to
        // claim the departure: the first claim wins and the loser must release
        // both network permits before waiting for the following slot.
        let gate = limiter.analytics_ping.next_allowed.lock().await;
        let mut calls = tokio::task::JoinSet::new();
        {
            let client = client.clone();
            calls.spawn(async move { client.ping("account").await });
        }
        {
            let client = client.clone();
            calls.spawn(async move { client.ping("account").await });
        }
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        drop(gate);

        let first = tokio::time::timeout(Duration::from_secs(1), calls.join_next())
            .await
            .expect("the winning departure must complete promptly")
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first["Status"], "OK");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), calls.join_next())
                .await
                .is_err(),
            "the competing call must wait for the next paced departure"
        );
        assert_eq!(
            client.global_in_flight.available_permits(),
            MAX_GLOBAL_IN_FLIGHT_REQUESTS
        );
        assert_eq!(
            limiter.in_flight.available_permits(),
            MAX_IN_FLIGHT_REQUESTS_PER_TOKEN
        );
        let second = tokio::time::timeout(Duration::from_secs(1), calls.join_next())
            .await
            .expect("the competing call must resume at the next slot")
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(second["Status"], "OK");
        assert!(calls.is_empty());
        for _ in 0..2 {
            assert!(
                requests
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .starts_with("GET /ping HTTP/1.1\r\n")
            );
        }
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn paced_waiters_from_noisy_tokens_do_not_starve_an_unrelated_account() {
        let accounts = BTreeMap::from([
            (
                "noisy-a".to_owned(),
                WbCredentials {
                    token: "noisy-token-a".to_owned(),
                },
            ),
            (
                "noisy-b".to_owned(),
                WbCredentials {
                    token: "noisy-token-b".to_owned(),
                },
            ),
            (
                "quiet".to_owned(),
                WbCredentials {
                    token: "quiet-token".to_owned(),
                },
            ),
        ]);
        let responses =
            vec![(200, r#"{"Status":"OK"}"#.to_owned()); 2 * MAX_IN_FLIGHT_REQUESTS_PER_TOKEN + 1];
        let (base_url, requests) = mock_http(responses);
        let client = WbClient::new_for_test(Duration::from_secs(2), accounts, &base_url, &base_url);
        let first_noisy = Arc::clone(client.limiters.get("noisy-a").unwrap());
        let second_noisy = Arc::clone(client.limiters.get("noisy-b").unwrap());

        // Holding both departure gates deterministically leaves all noisy
        // requests waiting in pacing. Four calls per token used to acquire all
        // eight global permits before reaching these locks.
        let first_gate = first_noisy.analytics_ping.next_allowed.lock().await;
        let second_gate = second_noisy.analytics_ping.next_allowed.lock().await;
        let barrier = Arc::new(tokio::sync::Barrier::new(
            2 * MAX_IN_FLIGHT_REQUESTS_PER_TOKEN + 1,
        ));
        let mut noisy_requests = Vec::new();
        for account in ["noisy-a", "noisy-b"] {
            for _ in 0..MAX_IN_FLIGHT_REQUESTS_PER_TOKEN {
                let client = client.clone();
                let barrier = Arc::clone(&barrier);
                noisy_requests.push(tokio::spawn(async move {
                    barrier.wait().await;
                    client.ping(account).await
                }));
            }
        }
        barrier.wait().await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            client.global_in_flight.available_permits(),
            MAX_GLOBAL_IN_FLIGHT_REQUESTS,
            "pacing waiters must not reserve the shared network budget"
        );
        assert_eq!(
            first_noisy.in_flight.available_permits(),
            MAX_IN_FLIGHT_REQUESTS_PER_TOKEN
        );
        assert_eq!(
            second_noisy.in_flight.available_permits(),
            MAX_IN_FLIGHT_REQUESTS_PER_TOKEN
        );
        let quiet = tokio::time::timeout(Duration::from_secs(1), client.ping("quiet"))
            .await
            .expect("an unrelated token must not wait behind noisy pacing queues")
            .expect("the unrelated request must have a network permit");
        assert_eq!(quiet["Status"], "OK");
        assert!(
            requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with("GET /ping HTTP/1.1\r\n")
        );

        drop((first_gate, second_gate));
        for request in noisy_requests {
            assert_eq!(request.await.unwrap().unwrap()["Status"], "OK");
            assert!(
                requests
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .starts_with("GET /ping HTTP/1.1\r\n")
            );
        }
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn retry_backoff_releases_global_and_token_network_permits() {
        let (base_url, requests, task) = raw_http(vec![
            raw_response(
                503,
                "x-request-id: retrying\r\n",
                br#"{"error":"temporary"}"#,
            ),
            raw_response(200, "", br#"{"Status":"QUIET"}"#),
            raw_response(200, "", br#"{"Status":"RETRIED"}"#),
        ]);
        let accounts = BTreeMap::from([
            (
                "noisy".to_owned(),
                WbCredentials {
                    token: "noisy-token".to_owned(),
                },
            ),
            (
                "quiet".to_owned(),
                WbCredentials {
                    token: "quiet-token".to_owned(),
                },
            ),
        ]);
        let mut policy = retrying_policy(Duration::from_secs(2));
        policy.base_retry_delay = Duration::from_millis(400);
        let client =
            WbClient::new_for_test_with_policy(Duration::from_secs(1), accounts, &base_url, policy);
        let noisy_limiter = Arc::clone(client.limiters.get("noisy").unwrap());
        let noisy_client = client.clone();
        let noisy = tokio::spawn(async move { noisy_client.ping("noisy").await });

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if requests.try_recv().is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first retryable request must reach the mock");
        tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                if client.global_in_flight.available_permits() == MAX_GLOBAL_IN_FLIGHT_REQUESTS
                    && noisy_limiter.in_flight.available_permits()
                        == MAX_IN_FLIGHT_REQUESTS_PER_TOKEN
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retry backoff must not retain network permits");

        assert_eq!(client.ping("quiet").await.unwrap()["Status"], "QUIET");
        assert_eq!(noisy.await.unwrap().unwrap()["Status"], "RETRIED");
        for _ in 0..2 {
            requests.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        assert!(requests.try_recv().is_err());
        task.join().unwrap();
    }

    #[tokio::test]
    async fn vendor_retry_after_extends_only_the_shared_token_class_gate() {
        let (base_url, requests, task) = raw_http(vec![
            raw_response(503, "Retry-After: 1\r\n", br#"{"error":"temporary"}"#),
            raw_response(200, "", br#"{"Status":"QUIET"}"#),
            raw_response(200, "", br#"{"Status":"SAME"}"#),
            raw_response(200, "", br#"{"Status":"SAME"}"#),
        ]);
        let accounts = BTreeMap::from([
            (
                "same".to_owned(),
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
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(1),
            accounts,
            &base_url,
            retrying_policy(Duration::from_secs(3)),
        );
        let same_limiter = Arc::clone(client.limiters.get("same").unwrap());
        let first_client = client.clone();
        let first = tokio::spawn(async move { first_client.ping("same").await });

        let first_request = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(request) = requests.try_recv() {
                    break request;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first same-token request must reach the mock");
        assert!(first_request.starts_with(b"GET /ping HTTP/1.1\r\n"));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let remaining = same_limiter
                    .analytics_ping
                    .next_allowed
                    .lock()
                    .await
                    .saturating_duration_since(Instant::now());
                if remaining > Duration::from_millis(500) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the Retry-After cooldown must reach the shared gate");

        let sibling_client = client.clone();
        let mut sibling = tokio::spawn(async move { sibling_client.ping("same").await });
        let quiet = tokio::time::timeout(Duration::from_millis(300), client.ping("other"))
            .await
            .expect("an unrelated token must not inherit the cooldown")
            .unwrap();
        assert_eq!(quiet["Status"], "QUIET");
        assert!(
            requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with(b"GET /ping HTTP/1.1\r\n")
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut sibling)
                .await
                .is_err(),
            "a same-token sibling must remain behind Retry-After"
        );
        assert!(requests.try_recv().is_err());

        assert_eq!(first.await.unwrap().unwrap()["Status"], "SAME");
        assert_eq!(sibling.await.unwrap().unwrap()["Status"], "SAME");
        for _ in 0..2 {
            assert!(
                requests
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .starts_with(b"GET /ping HTTP/1.1\r\n")
            );
        }
        assert!(requests.try_recv().is_err());
        task.join().unwrap();
    }

    #[tokio::test]
    async fn final_attempt_retry_after_still_extends_the_shared_token_gate() {
        let (base_url, requests, task) = raw_http(vec![
            raw_response(429, "Retry-After: 1\r\n", br#"{"error":"limited"}"#),
            raw_response(200, "", br#"{"Status":"QUIET"}"#),
            raw_response(200, "", br#"{"Status":"SAME"}"#),
        ]);
        let accounts = BTreeMap::from([
            (
                "same".to_owned(),
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
        let mut policy = retrying_policy(Duration::from_secs(3));
        policy.max_attempts = 1;
        let client =
            WbClient::new_for_test_with_policy(Duration::from_secs(1), accounts, &base_url, policy);

        assert!(matches!(
            client.ping("same").await,
            Err(WbError::RateLimited {
                retry_after: Some(delay),
                ..
            }) if delay == Duration::from_secs(1)
        ));
        assert!(
            requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with(b"GET /ping HTTP/1.1\r\n")
        );

        let sibling_client = client.clone();
        let mut sibling = tokio::spawn(async move { sibling_client.ping("same").await });
        let quiet = tokio::time::timeout(Duration::from_millis(300), client.ping("other"))
            .await
            .expect("an unrelated token must not inherit the final-attempt cooldown")
            .unwrap();
        assert_eq!(quiet["Status"], "QUIET");
        assert!(
            requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with(b"GET /ping HTTP/1.1\r\n")
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut sibling)
                .await
                .is_err(),
            "a same-token sibling must honor Retry-After after the final attempt"
        );

        assert_eq!(sibling.await.unwrap().unwrap()["Status"], "SAME");
        assert!(
            requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with(b"GET /ping HTTP/1.1\r\n")
        );
        assert!(requests.try_recv().is_err());
        task.join().unwrap();
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
                &policy,
            ),
            Some(Duration::ZERO)
        );
        assert_eq!(
            retry_plan(
                StatusCode::BAD_GATEWAY,
                1,
                ParsedRetryDelay::Absent,
                &policy,
            ),
            Some(Duration::ZERO)
        );
        assert_eq!(
            retry_plan(
                StatusCode::TOO_MANY_REQUESTS,
                1,
                ParsedRetryDelay::Valid(Duration::from_secs(2)),
                &policy,
            ),
            None
        );
        assert_eq!(
            retry_plan(
                StatusCode::TOO_MANY_REQUESTS,
                1,
                ParsedRetryDelay::Invalid,
                &policy,
            ),
            None
        );
        assert_eq!(
            retry_plan(
                StatusCode::BAD_REQUEST,
                1,
                ParsedRetryDelay::Absent,
                &policy,
            ),
            None
        );
        assert_eq!(
            retry_plan(
                StatusCode::SERVICE_UNAVAILABLE,
                policy.max_attempts,
                ParsedRetryDelay::Absent,
                &policy,
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
    async fn statistics_and_commission_classes_remain_non_retryable() {
        for path in [ORDERS_PATH, TARIFF_COMMISSIONS_PATH] {
            let (base_url, requests, task) = raw_http(vec![raw_response(
                503,
                "x-request-id: slow-original\r\n",
                br#"{"error":"temporary"}"#,
            )]);
            let mut policy = retrying_policy(Duration::from_millis(50));
            policy.statistics_interval = Duration::from_secs(1);
            policy.commission_interval = Duration::from_secs(1);
            policy.promotion_stats_interval = Duration::from_secs(1);
            let client = WbClient::new_for_test_with_policy(
                Duration::from_secs(1),
                credentials(),
                &base_url,
                policy,
            );

            let error = client
                .request("account", Method::GET, path, None, None)
                .await
                .expect_err("these minute-scale classes must make one attempt");
            assert!(matches!(
                error,
                WbError::Api {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    ref request_id,
                    ..
                } if request_id.as_deref() == Some("slow-original")
            ));
            requests
                .recv_timeout(Duration::from_secs(1))
                .expect("exactly one request must reach WB");
            assert!(requests.try_recv().is_err());
            task.join().unwrap();
        }
    }

    #[tokio::test]
    async fn promotion_stats_returns_the_upstream_error_when_quota_wait_cannot_fit() {
        let (base_url, requests, task) = raw_http(vec![raw_response(
            503,
            "x-request-id: promotion-original\r\n",
            br#"{"error":"temporary"}"#,
        )]);
        let mut policy = retrying_policy(Duration::from_millis(50));
        policy.promotion_stats_interval = Duration::from_secs(1);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(1),
            credentials(),
            &base_url,
            policy,
        );

        let error = client
            .promotion_stats(
                "account",
                vec![1],
                "2026-08-01".to_owned(),
                "2026-08-02".to_owned(),
            )
            .await
            .expect_err("the upstream 503 must remain causal when a retry cannot fit");
        assert!(matches!(
            error,
            WbError::Api {
                status: StatusCode::SERVICE_UNAVAILABLE,
                ref request_id,
                ref diagnostic,
            } if request_id.as_deref() == Some("promotion-original")
                && diagnostic == r#"{"error":"temporary"}"#
        ));
        requests
            .recv_timeout(Duration::from_secs(1))
            .expect("exactly one request must reach WB");
        assert!(requests.try_recv().is_err());
        task.join().unwrap();
    }

    #[tokio::test]
    async fn oversized_retryable_error_body_is_bounded_without_losing_status_causality() {
        let declared = MAX_ERROR_BODY_BYTES as u64 + 1;
        let response = format!(
            "HTTP/1.1 503 Error\r\nContent-Type: application/json\r\nContent-Length: {declared}\r\nx-request-id: oversized-retry\r\nConnection: close\r\n\r\n"
        )
        .into_bytes();
        let (base_url, requests, task) = raw_http(vec![response]);
        let mut policy = retrying_policy(Duration::from_millis(50));
        policy.promotion_stats_interval = Duration::from_secs(1);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(1),
            credentials(),
            &base_url,
            policy,
        );

        let error = client
            .promotion_stats(
                "account",
                vec![1],
                "2026-08-01".to_owned(),
                "2026-08-02".to_owned(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            WbError::Api {
                status: StatusCode::SERVICE_UNAVAILABLE,
                ref request_id,
                ref diagnostic,
            } if request_id.as_deref() == Some("oversized-retry") && diagnostic.is_empty()
        ));
        requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(requests.try_recv().is_err());
        task.join().unwrap();
    }

    #[tokio::test]
    async fn promotion_stats_internal_retry_waits_for_its_gate_and_recovers() {
        let (base_url, requests, task) = raw_http(vec![
            raw_response(
                503,
                "x-request-id: promotion-retry\r\n",
                br#"{"error":"temporary"}"#,
            ),
            raw_response(200, "", br#"{"status":"ok"}"#),
        ]);
        let mut policy = retrying_policy(Duration::from_secs(1));
        policy.promotion_stats_interval = Duration::from_millis(25);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(1),
            credentials(),
            &base_url,
            policy,
        );

        let response = client
            .promotion_stats(
                "account",
                vec![1],
                "2026-08-01".to_owned(),
                "2026-08-02".to_owned(),
            )
            .await
            .expect("the internal retry must wait instead of failing fast");
        assert_eq!(response["status"], "ok");
        for _ in 0..2 {
            requests
                .recv_timeout(Duration::from_secs(1))
                .expect("both promotion attempts must reach WB");
        }
        assert!(requests.try_recv().is_err());
        task.join().unwrap();
    }

    #[tokio::test]
    async fn retry_admission_failure_preserves_the_preceding_upstream_error() {
        for status in [429, 503] {
            let (base_url, requests, task) = raw_http(vec![raw_response(
                status,
                "x-request-id: causal-upstream\r\n",
                br#"{"error":"temporary"}"#,
            )]);
            let mut policy = retrying_policy(Duration::from_secs(1));
            policy.base_retry_delay = Duration::from_millis(100);
            let client = WbClient::new_for_test_with_policy(
                Duration::from_secs(1),
                credentials(),
                &base_url,
                policy,
            );
            let limiter = Arc::clone(client.limiters.get("account").unwrap());
            let held_permits = limiter
                .in_flight
                .acquire_many(
                    u32::try_from(MAX_IN_FLIGHT_REQUESTS_PER_TOKEN - 1)
                        .expect("per-token request limit fits u32"),
                )
                .await
                .unwrap();

            let request_client = client.clone();
            let pending = tokio::spawn(async move {
                request_client
                    .promotion_stats(
                        "account",
                        vec![1],
                        "2026-08-01".to_owned(),
                        "2026-08-02".to_owned(),
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if requests.try_recv().is_ok() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the first request must reach WB");
            let final_permit = limiter.in_flight.acquire().await.unwrap();

            let error = pending
                .await
                .unwrap()
                .expect_err("retry admission must fail while all token permits are held");
            if status == 429 {
                assert!(matches!(
                    error,
                    WbError::RateLimited {
                        ref request_id,
                        ..
                    } if request_id.as_deref() == Some("causal-upstream")
                ));
            } else {
                assert_eq!(status, 503);
                assert!(matches!(
                    error,
                    WbError::Api {
                        status: StatusCode::SERVICE_UNAVAILABLE,
                        ref request_id,
                        ref diagnostic,
                    } if request_id.as_deref() == Some("causal-upstream")
                        && diagnostic == r#"{"error":"temporary"}"#
                ));
            }
            assert!(requests.try_recv().is_err());
            drop(final_permit);
            drop(held_permits);
            task.join().unwrap();
        }
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
    async fn logical_deadline_cancels_an_in_flight_attempt() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let task = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0_u8; 4_096];
            let _ = stream.read(&mut buffer);
            thread::sleep(Duration::from_millis(100));
        });
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(1),
            credentials(),
            &format!("http://{address}"),
            ClientPolicy::immediate_single_attempt(Duration::from_millis(20)),
        );

        assert!(matches!(
            client.ping("account").await.unwrap_err(),
            WbError::DeadlineExceeded
        ));
        task.join().unwrap();
    }

    /// The production case this rule exists for.
    ///
    /// `StatisticsReport` paces at 60s and the logical timeout caps at 60s, so
    /// a second concurrent caller could never win its slot in time: it was
    /// guaranteed to sleep a full minute — holding an MCP request slot and an
    /// HTTP connection throughout — and then fail with a bare `Timeout`.
    #[tokio::test]
    async fn a_statistics_caller_behind_a_full_interval_is_refused_not_queued() {
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(30),
            credentials(),
            "http://127.0.0.1:1",
            ClientPolicy::production(Duration::from_secs(30)),
        );
        let limiter = client.limiters.get("account").unwrap();
        // Exactly the state a sibling call leaves behind after claiming.
        *limiter.statistics_reports.next_allowed.lock().await =
            Instant::now() + STATISTICS_MIN_REQUEST_INTERVAL;

        let started = Instant::now();
        let error = client
            .orders("account", "2026-08-01T00:00:00".to_owned(), 0)
            .await
            .unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the caller must be refused, not parked for the whole interval"
        );
        assert!(matches!(
            error,
            WbError::LocalRateLimited { retry_after }
                if retry_after > Duration::from_secs(59)
                    && retry_after <= STATISTICS_MIN_REQUEST_INTERVAL
        ));
    }

    /// The logical deadline still governs a local quota wait — but it now
    /// decides the wait up front instead of being consumed by it.
    ///
    /// Entering a wait that cannot end in time produced `DeadlineExceeded`
    /// only once it expired, holding the caller's request slot and its HTTP
    /// connection for the whole duration and saying nothing about when a
    /// retry could succeed.
    #[tokio::test]
    async fn a_quota_wait_that_cannot_fit_the_deadline_is_refused_immediately() {
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

        let started = Instant::now();
        let error = client.ping("account").await.unwrap_err();
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "the refusal must not spend the wait it declined to make"
        );
        assert!(matches!(
            error,
            WbError::LocalRateLimited { retry_after }
                if retry_after > Duration::from_millis(50)
                    && retry_after <= Duration::from_millis(100)
        ));
    }

    #[tokio::test]
    async fn minute_scale_commission_quota_fails_fast_with_retry_after() {
        let (base_url, requests) = mock_http(vec![(200, r#"{"report":[]}"#.to_owned())]);
        let mut policy = ClientPolicy::immediate_single_attempt(Duration::from_secs(2));
        policy.commission_interval = Duration::from_secs(60);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_secs(1),
            credentials(),
            &base_url,
            policy,
        );

        client.tariff_commissions("account", None).await.unwrap();
        let started = Instant::now();
        let error = client
            .tariff_commissions("account", None)
            .await
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_millis(100));
        assert_eq!(error.kind(), WbErrorKind::RateLimited);
        assert_eq!(error.request_id(), None);
        assert!(matches!(
            error,
            WbError::LocalRateLimited { retry_after }
                if retry_after > Duration::from_secs(59)
                    && retry_after <= Duration::from_secs(60)
        ));
        requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(requests.try_recv().is_err());
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
        assert_eq!(extract_request_id(&headers).as_deref(), Some("trace-id"));
        headers.insert(
            "x-request-id",
            HeaderValue::from_str(&"x".repeat(MAX_REQUEST_ID_BYTES + 1)).unwrap(),
        );
        assert_eq!(extract_request_id(&headers).as_deref(), Some("trace-id"));
        headers.insert("x-request-id", HeaderValue::from_bytes(&[0xff]).unwrap());
        assert_eq!(extract_request_id(&headers).as_deref(), Some("trace-id"));
        headers.remove("x-trace-id");

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

    #[test]
    fn bearer_authorization_is_always_marked_sensitive() {
        let authorization = bearer_authorization("secret-token").unwrap();
        assert_eq!(authorization.as_bytes(), b"Bearer secret-token");
        assert!(authorization.is_sensitive());

        let error = bearer_authorization("invalid\nheader").unwrap_err();
        assert_eq!(error.kind(), WbErrorKind::Unauthorized);
        assert!(!format!("{error:?}").contains("invalid"));
    }

    #[tokio::test]
    async fn search_report_arguments_fail_closed_before_network() {
        let client = WbClient::new_for_test(
            Duration::from_millis(50),
            credentials(),
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
        );

        macro_rules! assert_invalid {
            ($future:expr, $field:literal) => {{
                let error = $future.await.unwrap_err();
                assert!(
                    matches!(error, WbError::InvalidArguments { field } if field == $field),
                    "expected invalid {}, got {error:?}",
                    $field
                );
            }};
        }

        assert_invalid!(
            client.search_product_queries(
                "account",
                "2026-8-10".to_owned(),
                "2026-08-10".to_owned(),
                None,
                vec![1],
                "orders".to_owned(),
                1,
            ),
            "current_start"
        );
        assert_invalid!(
            client.search_product_queries(
                "account",
                "2026-08-10".to_owned(),
                "bad".to_owned(),
                None,
                vec![1],
                "orders".to_owned(),
                1,
            ),
            "current_end"
        );
        assert_invalid!(
            client.search_product_queries(
                "account",
                "2026-08-01".to_owned(),
                "2026-09-01".to_owned(),
                None,
                vec![1],
                "orders".to_owned(),
                1,
            ),
            "date_range"
        );
        assert_invalid!(
            client.search_product_queries(
                "account",
                "2026-08-10".to_owned(),
                "2026-08-10".to_owned(),
                Some(("bad".to_owned(), "2026-08-01".to_owned())),
                vec![1],
                "orders".to_owned(),
                1,
            ),
            "past_start"
        );
        assert_invalid!(
            client.search_product_queries(
                "account",
                "2026-08-10".to_owned(),
                "2026-08-10".to_owned(),
                Some(("2026-08-01".to_owned(), "2026-08-02".to_owned())),
                vec![1],
                "orders".to_owned(),
                1,
            ),
            "past_period"
        );
        for nm_ids in [Vec::new(), vec![0], vec![1, 1], (1..=51).collect()] {
            assert_invalid!(
                client.search_product_queries(
                    "account",
                    "2026-08-10".to_owned(),
                    "2026-08-10".to_owned(),
                    None,
                    nm_ids,
                    "orders".to_owned(),
                    1,
                ),
                "nm_ids"
            );
        }
        assert_invalid!(
            client.search_product_queries(
                "account",
                "2026-08-10".to_owned(),
                "2026-08-10".to_owned(),
                None,
                vec![1],
                "Orders".to_owned(),
                1,
            ),
            "top_order_by"
        );
        for limit in [0, MAX_SEARCH_REPORT_LIMIT + 1] {
            assert_invalid!(
                client.search_product_queries(
                    "account",
                    "2026-08-10".to_owned(),
                    "2026-08-10".to_owned(),
                    None,
                    vec![1],
                    "orders".to_owned(),
                    limit,
                ),
                "limit"
            );
        }

        assert_invalid!(
            client.search_orders_positions(
                "account",
                "2026-08-10".to_owned(),
                "2026-08-09".to_owned(),
                1,
                vec!["ручка".to_owned()],
            ),
            "date_range"
        );
        assert_invalid!(
            client.search_orders_positions(
                "account",
                "2026-08-01".to_owned(),
                "2026-08-08".to_owned(),
                1,
                vec!["ручка".to_owned()],
            ),
            "date_range"
        );
        assert_invalid!(
            client.search_orders_positions(
                "account",
                "2026-08-10".to_owned(),
                "2026-08-10".to_owned(),
                0,
                vec!["ручка".to_owned()],
            ),
            "nm_id"
        );
        for search_texts in [
            Vec::new(),
            vec!["ручка".to_owned(), "ручка".to_owned()],
            vec![" ручка".to_owned()],
            vec!["ручка\nкнопка".to_owned()],
            vec!["я".repeat(129)],
            (1..=31).map(|index| format!("ручка {index}")).collect(),
        ] {
            assert_invalid!(
                client.search_orders_positions(
                    "account",
                    "2026-08-10".to_owned(),
                    "2026-08-10".to_owned(),
                    1,
                    search_texts,
                ),
                "search_texts"
            );
        }

        assert_eq!(
            validate_search_period(
                "2026-08-01",
                "2026-08-31",
                "start",
                "end",
                MAX_SEARCH_PRODUCT_PERIOD_DAYS,
            )
            .unwrap(),
            31
        );
        assert!(matches!(
            validate_search_period(
                "2026-08-01",
                "2026-09-01",
                "start",
                "end",
                MAX_SEARCH_PRODUCT_PERIOD_DAYS,
            ),
            Err(WbError::InvalidArguments {
                field: "date_range"
            })
        ));
        assert_eq!(
            validate_search_period(
                "2026-08-01",
                "2026-08-07",
                "start",
                "end",
                MAX_SEARCH_ORDERS_PERIOD_DAYS,
            )
            .unwrap(),
            7
        );
        assert!(matches!(
            validate_search_period(
                "2026-08-01",
                "2026-08-08",
                "start",
                "end",
                MAX_SEARCH_ORDERS_PERIOD_DAYS,
            ),
            Err(WbError::InvalidArguments {
                field: "date_range"
            })
        ));
        assert!(
            validate_positive_unique_ids(&(1..=50).collect::<Vec<_>>(), 50, "nm_ids", None).is_ok()
        );
        for value in [
            "openCard",
            "addToCart",
            "openToCart",
            "orders",
            "cartToOrder",
        ] {
            assert!(validate_top_order_by(value).is_ok(), "{value}");
        }
        assert!(validate_search_texts(&vec!["я".repeat(128); 1]).is_ok());
        assert!(
            validate_search_texts(
                &(1..=MAX_SEARCH_REPORT_TEXTS)
                    .map(|index| format!("phrase-{index}"))
                    .collect::<Vec<_>>()
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn bid_read_arguments_fail_closed_before_network() {
        let client = WbClient::new_for_test(
            Duration::from_millis(50),
            credentials(),
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
        );

        macro_rules! assert_invalid {
            ($future:expr, $field:literal) => {{
                let error = $future.await.unwrap_err();
                assert!(
                    matches!(error, WbError::InvalidArguments { field } if field == $field),
                    "expected invalid {}, got {error:?}",
                    $field
                );
            }};
        }

        for advert_id in [0, MAX_WB_SIGNED_ID + 1] {
            assert_invalid!(
                client.promotion_minimum_bids(
                    "account",
                    advert_id,
                    vec![1],
                    "cpm".to_owned(),
                    vec!["search".to_owned()],
                ),
                "advert_id"
            );
        }
        for nm_ids in [
            Vec::new(),
            vec![0],
            vec![MAX_WB_SIGNED_ID + 1],
            vec![1, 1],
            (1..=101).collect(),
        ] {
            assert_invalid!(
                client.promotion_minimum_bids(
                    "account",
                    1,
                    nm_ids,
                    "cpm".to_owned(),
                    vec!["search".to_owned()],
                ),
                "nm_ids"
            );
        }
        assert_invalid!(
            client.promotion_minimum_bids(
                "account",
                1,
                vec![1],
                "CPM".to_owned(),
                vec!["search".to_owned()],
            ),
            "payment_type"
        );
        for placements in [
            Vec::new(),
            vec!["search".to_owned(), "search".to_owned()],
            vec!["recommendations".to_owned()],
            vec![
                "combined".to_owned(),
                "search".to_owned(),
                "recommendation".to_owned(),
                "extra".to_owned(),
            ],
        ] {
            assert_invalid!(
                client.promotion_minimum_bids("account", 1, vec![1], "cpm".to_owned(), placements,),
                "placement_types"
            );
        }

        for (advert_id, nm_id, field) in [
            (0, 1, "advert_id"),
            (MAX_WB_SIGNED_ID + 1, 1, "advert_id"),
            (1, 0, "nm_id"),
            (1, MAX_WB_SIGNED_ID + 1, "nm_id"),
        ] {
            let error = client
                .promotion_recommended_bids("account", advert_id, nm_id)
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                WbError::InvalidArguments { field: actual } if actual == field
            ));
        }

        for items in [
            Vec::new(),
            vec![(0, 1)],
            vec![(1, 0)],
            vec![(MAX_WB_SIGNED_ID + 1, 1)],
            vec![(1, MAX_WB_SIGNED_ID + 1)],
            vec![(1, 2), (1, 2)],
            (1..=101).map(|value| (value, value)).collect(),
        ] {
            assert_invalid!(
                client.promotion_search_cluster_bids("account", items),
                "items"
            );
        }

        assert!(
            validate_positive_unique_ids(
                &(1..=100).collect::<Vec<_>>(),
                100,
                "nm_ids",
                Some(MAX_WB_SIGNED_ID),
            )
            .is_ok()
        );
        assert!(
            validate_placement_types(&[
                "combined".to_owned(),
                "search".to_owned(),
                "recommendation".to_owned(),
            ])
            .is_ok()
        );
        assert!(
            validate_bid_items(
                &(1..=MAX_PROMOTION_CLUSTER_BID_ITEMS as u64)
                    .map(|value| (value, value))
                    .collect::<Vec<_>>()
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn promotion_arguments_fail_closed_before_any_network_attempt() {
        let client = WbClient::new_for_test(
            Duration::from_millis(100),
            credentials(),
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
        );

        for ids in [Vec::new(), vec![0], vec![1, 1], (1..=51).collect()] {
            let error = client
                .promotion_campaign_details("account", ids, Vec::new(), None)
                .await
                .unwrap_err();
            assert!(matches!(error, WbError::InvalidArguments { field: "ids" }));
        }
        for statuses in [vec![0], vec![4, 4], vec![-1, 4, 7, 8, 9, 11, 11]] {
            let error = client
                .promotion_campaign_details("account", vec![1], statuses, None)
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                WbError::InvalidArguments { field: "statuses" }
            ));
        }
        let error = client
            .promotion_campaign_details("account", vec![1], Vec::new(), Some("CPM".to_owned()))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            WbError::InvalidArguments {
                field: "payment_type"
            }
        ));

        for ids in [Vec::new(), vec![0], vec![1, 1], (1..=51).collect()] {
            let error = client
                .promotion_stats(
                    "account",
                    ids,
                    "2026-08-01".to_owned(),
                    "2026-08-02".to_owned(),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, WbError::InvalidArguments { field: "ids" }));
        }
        for (begin_date, end_date, field) in [
            ("not-a-date", "2026-08-02", "begin_date"),
            ("2026-8-01", "2026-08-02", "begin_date"),
            ("2026-08-01", "not-a-date", "end_date"),
            ("2026-08-02", "2026-08-01", "date_range"),
            ("2026-08-01", "2026-09-01", "date_range"),
        ] {
            let error = client
                .promotion_stats(
                    "account",
                    vec![1],
                    begin_date.to_owned(),
                    end_date.to_owned(),
                )
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                WbError::InvalidArguments { field: actual } if actual == field
            ));
        }

        assert_eq!(
            WbError::InvalidArguments { field: "ids" }.kind(),
            WbErrorKind::InvalidArguments
        );
    }

    #[tokio::test]
    async fn search_reports_are_single_attempt_and_quota_fails_fast() {
        let (base_url, requests, task) = raw_http(vec![raw_response(
            503,
            "X-Request-Id: search-unavailable\r\n",
            b"{}",
        )]);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_millis(200),
            credentials(),
            &base_url,
            retrying_policy(Duration::from_secs(1)),
        );
        let error = client
            .search_product_queries(
                "account",
                "2026-08-10".to_owned(),
                "2026-08-10".to_owned(),
                None,
                vec![1],
                "orders".to_owned(),
                1,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            WbError::Api {
                status: StatusCode::SERVICE_UNAVAILABLE,
                ..
            }
        ));
        let request = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        let expected = format!("POST {SEARCH_PRODUCT_QUERIES_PATH} HTTP/1.1\r\n");
        assert!(request.starts_with(expected.as_bytes()));
        assert!(requests.try_recv().is_err(), "search report must not retry");
        task.join().unwrap();

        let mut policy = ClientPolicy::immediate_single_attempt(Duration::from_millis(20));
        policy.search_report_interval = SEARCH_REPORT_MIN_REQUEST_INTERVAL;
        let client = WbClient::new_for_test_with_policy(
            Duration::from_millis(100),
            credentials(),
            "http://127.0.0.1:1",
            policy,
        );
        let limiter = client.limiters.get("account").unwrap();
        *limiter.search_reports.next_allowed.lock().await =
            Instant::now() + SEARCH_REPORT_MIN_REQUEST_INTERVAL;
        let started = Instant::now();
        let error = client
            .search_orders_positions(
                "account",
                "2026-08-10".to_owned(),
                "2026-08-10".to_owned(),
                1,
                vec!["ручка".to_owned()],
            )
            .await
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_millis(100));
        assert!(matches!(
            error,
            WbError::LocalRateLimited { retry_after }
                if retry_after > Duration::from_secs(19)
                    && retry_after <= SEARCH_REPORT_MIN_REQUEST_INTERVAL
        ));
    }

    #[tokio::test]
    async fn search_report_http_402_is_a_non_retryable_subscription_error() {
        let (base_url, requests, task) = raw_http(vec![raw_response(
            402,
            "X-Request-Id: jam-required\r\n",
            b"{}",
        )]);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_millis(200),
            credentials(),
            &base_url,
            retrying_policy(Duration::from_secs(1)),
        );
        let error = client
            .search_orders_positions(
                "account",
                "2026-08-10".to_owned(),
                "2026-08-10".to_owned(),
                1,
                vec!["ручка".to_owned()],
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), WbErrorKind::SubscriptionRequired);
        assert_eq!(error.request_id(), Some("jam-required"));
        assert!(error.to_string().contains("подписка WB"));
        let request = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        let expected = format!("POST {SEARCH_ORDERS_POSITIONS_PATH} HTTP/1.1\r\n");
        assert!(request.starts_with(expected.as_bytes()));
        assert!(requests.try_recv().is_err(), "HTTP 402 must not retry");
        task.join().unwrap();
    }

    #[tokio::test]
    async fn promotion_stats_quota_fails_fast_and_campaign_routes_share_one_gate() {
        let mut policy = ClientPolicy::immediate_single_attempt(Duration::from_secs(2));
        policy.promotion_stats_interval = PROMOTION_STATS_MIN_REQUEST_INTERVAL;
        policy.promotion_campaign_interval = PROMOTION_CAMPAIGN_MIN_REQUEST_INTERVAL;
        policy.logical_timeout = Duration::from_millis(20);
        let client = WbClient::new_for_test_with_policy(
            Duration::from_millis(100),
            credentials(),
            "http://127.0.0.1:1",
            policy,
        );
        let limiter = client.limiters.get("account").unwrap();

        *limiter.promotion_stats.next_allowed.lock().await =
            Instant::now() + PROMOTION_STATS_MIN_REQUEST_INTERVAL;
        let started = Instant::now();
        let error = client
            .promotion_stats(
                "account",
                vec![1],
                "2026-08-01".to_owned(),
                "2026-08-31".to_owned(),
            )
            .await
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_millis(100));
        assert!(matches!(
            error,
            WbError::LocalRateLimited { retry_after }
                if retry_after > Duration::from_secs(19)
                    && retry_after <= PROMOTION_STATS_MIN_REQUEST_INTERVAL
        ));

        // Both campaign list and details use this exact gate. A pending slot
        // therefore blocks details before any connection can be attempted.
        // The 200ms slot cannot fit the 20ms deadline, so it is refused with
        // the retry instant rather than waited out into a timeout.
        *limiter.promotion_campaigns.next_allowed.lock().await =
            Instant::now() + Duration::from_millis(200);
        let started = Instant::now();
        let error = client
            .promotion_campaign_details("account", vec![1], Vec::new(), Some("cpc".to_owned()))
            .await
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_millis(100));
        assert!(matches!(
            error,
            WbError::LocalRateLimited { retry_after }
                if retry_after > Duration::from_millis(100)
                    && retry_after <= Duration::from_millis(200)
        ));
    }

    /// `SECURITY.md` invariant 1 disables redirects on every marketplace client,
    /// and the WB client had no test for it while Ozon and Ozon Performance did.
    ///
    /// Following a redirect would defeat the read-only allowlist outright: the
    /// allowlist is checked against the URL this process constructs, so a `307`
    /// pointing at a mutating path would carry the original method and body to a
    /// destination that was never allowlisted, and the `Authorization` token
    /// along with it. The 3xx must therefore surface as a plain HTTP error.
    #[tokio::test]
    async fn wb_redirects_are_never_followed_so_the_allowlist_cannot_be_escaped() {
        for status in [301_u16, 302, 303, 307, 308] {
            let (base_url, requests, _task) = raw_http(vec![raw_response(
                status,
                // Port 9 (discard) is never served: reaching it would hang or
                // fail as a network error rather than the HTTP error asserted.
                "Location: http://127.0.0.1:9/api/v2/cards/update\r\n",
                b"{}",
            )]);
            let client = client(&base_url);

            let error = client
                .ping("account")
                .await
                .expect_err("a 3xx must not be followed");

            assert_eq!(
                error.kind(),
                WbErrorKind::Http,
                "HTTP {status} must surface as an upstream HTTP error, got {error:?}"
            );
            assert!(
                matches!(&error, WbError::Api { status: got, .. } if got.as_u16() == status),
                "HTTP {status} must be reported verbatim, got {error:?}"
            );

            // Exactly one request left the process: the redirect target was
            // never contacted.
            let sent = requests
                .recv_timeout(Duration::from_secs(2))
                .expect("the initial request must have been sent");
            assert!(
                String::from_utf8_lossy(&sent).starts_with("GET /ping "),
                "only the allowlisted request may be sent"
            );
            assert!(
                requests.try_recv().is_err(),
                "a redirect must not produce a second request"
            );
        }
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
            (Method::DELETE, PRODUCT_CARDS_PATH),
            (Method::POST, PRODUCT_PRICES_PATH),
            (Method::POST, TARIFF_COMMISSIONS_PATH),
            (Method::POST, ACCEPTANCE_COEFFICIENTS_PATH),
            // WB Promotion has mutating GET routes, so allowing every GET is
            // unsafe. These exact operations must remain unreachable.
            (Method::GET, "/adv/v0/delete"),
            (Method::GET, "/adv/v0/start"),
            (Method::GET, "/adv/v0/pause"),
            (Method::GET, "/adv/v0/stop"),
            (Method::POST, "/adv/v0/rename"),
            (Method::POST, "/adv/v2/seacat/save-ad"),
            (Method::POST, "/adv/v1/budget/deposit"),
            (Method::POST, "/adv/v0/normquery/set-minus"),
            (Method::POST, "/adv/v0/normquery/bids"),
            (Method::DELETE, "/adv/v0/normquery/bids"),
            (Method::PATCH, "/api/advert/v1/bids"),
            (Method::PATCH, "/adv/v0/auction/nms"),
            (Method::PUT, "/adv/v0/auction/placements"),
            (Method::POST, "/adv/v0/all_sku_promo/activate"),
            (Method::POST, "/adv/v0/all_sku_promo/deactivate"),
            (Method::POST, "/adv/v0/all_sku_promo/set_bid"),
            // Wrong methods for the three newly allowlisted read endpoints.
            (Method::POST, PROMOTION_CAMPAIGNS_PATH),
            (Method::POST, PROMOTION_DETAILS_PATH),
            (Method::POST, PROMOTION_STATS_PATH),
            (Method::GET, SEARCH_PRODUCT_QUERIES_PATH),
            (Method::GET, SEARCH_ORDERS_POSITIONS_PATH),
            (Method::GET, PROMOTION_MINIMUM_BIDS_PATH),
            (Method::POST, PROMOTION_RECOMMENDATIONS_PATH),
            (Method::GET, PROMOTION_CLUSTER_BIDS_PATH),
            // Near-misses of allowlisted paths.
            (Method::GET, "/ping/"),
            (Method::GET, "/api/v1/tariffs/commission/"),
            (Method::GET, "/adv/v1/promotion/count/"),
            (Method::GET, "/api/advert/v2/adverts/"),
            (Method::GET, "/adv/v3/fullstats/"),
            (Method::POST, "/api/v2/search-report/product/search-texts/"),
            (Method::POST, "/api/v2/search-report/product/orders/"),
            (Method::POST, "/api/advert/v1/bids/min/"),
            (Method::GET, "/api/advert/v0/bids/recommendations/"),
            (Method::POST, "/adv/v0/normquery/get-bids/"),
            // Deprecated and neighboring read paths are not implicitly trusted.
            (Method::POST, "/adv/v0/bids/min"),
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

        // Every allowlisted read maps to an explicit quota class. The ping
        // call below passes the guard and only then fails on the
        // network, which is what keeps this test honest.
        assert_eq!(
            RequestClass::for_request(&Method::GET, PING_PATH),
            Some(RequestClass::AnalyticsPing)
        );
        assert_eq!(
            RequestClass::for_request(&Method::POST, SALES_FUNNEL_PATH),
            Some(RequestClass::AnalyticsReport)
        );
        for path in [
            SALES_FUNNEL_HISTORY_PATH,
            SALES_FUNNEL_GROUPED_HISTORY_PATH,
            WAREHOUSE_STOCKS_PATH,
        ] {
            assert_eq!(
                RequestClass::for_request(&Method::POST, path),
                Some(RequestClass::AnalyticsReport)
            );
        }
        for path in [ORDERS_PATH, SALES_PATH] {
            assert_eq!(
                RequestClass::for_request(&Method::GET, path),
                Some(RequestClass::StatisticsReport)
            );
        }
        assert_eq!(
            RequestClass::for_request(&Method::POST, PRODUCT_CARDS_PATH),
            Some(RequestClass::ContentReport)
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, PRODUCT_PRICES_PATH),
            Some(RequestClass::PricesReport)
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, TARIFF_COMMISSIONS_PATH),
            Some(RequestClass::CommissionTariff)
        );
        for path in [TARIFF_BOXES_PATH, TARIFF_PALLETS_PATH, TARIFF_RETURNS_PATH] {
            assert_eq!(
                RequestClass::for_request(&Method::GET, path),
                Some(RequestClass::LogisticsTariff)
            );
        }
        assert_eq!(
            RequestClass::for_request(&Method::GET, ACCEPTANCE_COEFFICIENTS_PATH),
            Some(RequestClass::AcceptanceTariff)
        );
        for path in [PROMOTION_CAMPAIGNS_PATH, PROMOTION_DETAILS_PATH] {
            assert_eq!(
                RequestClass::for_request(&Method::GET, path),
                Some(RequestClass::PromotionCampaign)
            );
        }
        assert_eq!(
            RequestClass::for_request(&Method::GET, PROMOTION_STATS_PATH),
            Some(RequestClass::PromotionStats)
        );
        for path in [SEARCH_PRODUCT_QUERIES_PATH, SEARCH_ORDERS_POSITIONS_PATH] {
            assert_eq!(
                RequestClass::for_request(&Method::POST, path),
                Some(RequestClass::SearchReport)
            );
        }
        assert_eq!(
            RequestClass::for_request(&Method::POST, PROMOTION_MINIMUM_BIDS_PATH),
            Some(RequestClass::PromotionMinimumBids)
        );
        assert_eq!(
            RequestClass::for_request(&Method::GET, PROMOTION_RECOMMENDATIONS_PATH),
            Some(RequestClass::PromotionRecommendedBids)
        );
        assert_eq!(
            RequestClass::for_request(&Method::POST, PROMOTION_CLUSTER_BIDS_PATH),
            Some(RequestClass::PromotionClusterBids)
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
            (WbErrorKind::InvalidArguments, "invalid_arguments"),
            (WbErrorKind::MissingCredentials, "missing_credentials"),
            (WbErrorKind::Unauthorized, "unauthorized"),
            (WbErrorKind::Forbidden, "forbidden"),
            (WbErrorKind::SubscriptionRequired, "subscription_required"),
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
            WbError::InvalidArguments { field: "ids" },
            WbError::Unauthorized { request_id: None },
            WbError::Forbidden { request_id: None },
            WbError::SubscriptionRequired { request_id: None },
            WbError::RateLimited {
                request_id: None,
                retry_after: None,
            },
            WbError::LocalRateLimited {
                retry_after: Duration::from_secs(1),
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
