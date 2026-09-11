//! Read-only endpoint policy and fixed hosts, never supplied by callers.

use super::{
    ACCEPTANCE_COEFFICIENTS_PATH, ACCEPTANCE_MIN_REQUEST_INTERVAL, ANALYTICS_MIN_REQUEST_INTERVAL,
    BASE_RETRY_DELAY, COMMISSION_MIN_REQUEST_INTERVAL, CONTENT_MIN_REQUEST_INTERVAL, Duration,
    LOGISTICS_TARIFF_MIN_REQUEST_INTERVAL, MAX_ATTEMPTS, MAX_LOGICAL_REQUEST_DURATION,
    MAX_RETRY_DELAY, Method, ORDERS_PATH, PING_MIN_REQUEST_INTERVAL, PING_PATH,
    PRICES_MIN_REQUEST_INTERVAL, PRODUCT_CARDS_PATH, PRODUCT_PRICES_PATH, PROMOTION_BALANCE_PATH,
    PROMOTION_BUDGET_PATH, PROMOTION_CAMPAIGN_MIN_REQUEST_INTERVAL, PROMOTION_CAMPAIGNS_PATH,
    PROMOTION_CLUSTER_BIDS_MIN_REQUEST_INTERVAL, PROMOTION_CLUSTER_BIDS_PATH,
    PROMOTION_DETAILS_PATH, PROMOTION_MINIMUM_BIDS_MIN_REQUEST_INTERVAL,
    PROMOTION_MINIMUM_BIDS_PATH, PROMOTION_RECOMMENDATIONS_MIN_REQUEST_INTERVAL,
    PROMOTION_RECOMMENDATIONS_PATH, PROMOTION_STATS_MIN_REQUEST_INTERVAL, PROMOTION_STATS_PATH,
    RetryPolicy, SALES_FUNNEL_GROUPED_HISTORY_PATH, SALES_FUNNEL_HISTORY_PATH, SALES_FUNNEL_PATH,
    SALES_PATH, SEARCH_ORDERS_POSITIONS_PATH, SEARCH_PRODUCT_QUERIES_PATH,
    SEARCH_REPORT_MIN_REQUEST_INTERVAL, SELLER_INVENTORY_MIN_REQUEST_INTERVAL, SELLER_STOCKS_PATH,
    SELLER_WAREHOUSES_PATH, STATISTICS_MIN_REQUEST_INTERVAL, TARIFF_BOXES_PATH,
    TARIFF_COMMISSIONS_PATH, TARIFF_PALLETS_PATH, TARIFF_RETURNS_PATH, WAREHOUSE_STOCKS_PATH,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ApiHost {
    Analytics,
    Statistics,
    Content,
    Prices,
    Common,
    Promotion,
    Marketplace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RequestClass {
    AnalyticsPing,
    AnalyticsReport,
    StatisticsReport,
    ContentReport,
    PricesReport,
    CommissionTariff,
    LogisticsTariff,
    AcceptanceTariff,
    PromotionCampaign,
    PromotionBalance,
    PromotionStats,
    SearchReport,
    PromotionMinimumBids,
    PromotionRecommendedBids,
    PromotionClusterBids,
    SellerInventory,
}

/// Single source of truth for every request that may leave this process.
/// Method, exact path, fixed host, safe observability label and quota bucket
/// live in the same record so extending one dimension cannot silently drift
/// out of sync with another.
#[derive(Debug)]
pub(super) struct EndpointPolicy {
    pub(super) method: Method,
    pub(super) path: &'static str,
    pub(super) label: &'static str,
    pub(super) host: ApiHost,
    pub(super) request_class: RequestClass,
}

/// Every Wildberries request this process is allowed to make.
///
/// Mirrors [`crate::ozon::READ_ONLY_ENDPOINT_ALLOWLIST`]: it is enforced inside
/// [`WbClient::request`], the only place a WB request can leave the process, so
/// adding a mutating call requires deliberately editing this list.
pub(super) const READ_ONLY_ENDPOINT_ALLOWLIST: &[EndpointPolicy] = &[
    EndpointPolicy {
        method: Method::GET,
        path: SELLER_WAREHOUSES_PATH,
        label: "marketplace:/api/v3/warehouses",
        host: ApiHost::Marketplace,
        request_class: RequestClass::SellerInventory,
    },
    EndpointPolicy {
        method: Method::POST,
        path: SELLER_STOCKS_PATH,
        label: "marketplace:/api/v3/stocks/{warehouseId}",
        host: ApiHost::Marketplace,
        request_class: RequestClass::SellerInventory,
    },
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
        path: PROMOTION_BUDGET_PATH,
        label: "promotion:/adv/v1/budget",
        host: ApiHost::Promotion,
        request_class: RequestClass::PromotionCampaign,
    },
    EndpointPolicy {
        method: Method::GET,
        path: PROMOTION_BALANCE_PATH,
        label: "promotion:/adv/v1/balance",
        host: ApiHost::Promotion,
        request_class: RequestClass::PromotionBalance,
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

impl EndpointPolicy {
    pub(super) fn for_request(method: &Method, path: &str) -> Option<&'static Self> {
        READ_ONLY_ENDPOINT_ALLOWLIST.iter().find(|policy| {
            policy.method == *method
                && if policy.path == SELLER_STOCKS_PATH {
                    is_seller_stock_read_path(path)
                } else {
                    policy.path == path
                }
        })
    }
}

/// Admit only one canonical positive int64 segment. Never admit a prefix,
/// encoded path, query, or the neighboring PUT/DELETE inventory operations.
pub(super) fn is_seller_stock_read_path(path: &str) -> bool {
    let Some(id) = path.strip_prefix("/api/v3/stocks/") else {
        return false;
    };
    !id.starts_with('0')
        && id.bytes().all(|byte| byte.is_ascii_digit())
        && id.parse::<i64>().is_ok_and(|id| id > 0)
}

impl RequestClass {
    pub(super) const fn allows_automatic_retry(self) -> bool {
        !matches!(
            self,
            Self::StatisticsReport
                | Self::CommissionTariff
                | Self::SearchReport
                | Self::SellerInventory
        )
    }

    #[cfg(test)]
    pub(super) fn for_request(method: &Method, path: &str) -> Option<Self> {
        EndpointPolicy::for_request(method, path).map(|policy| policy.request_class)
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ClientPolicy {
    pub(super) ping_interval: Duration,
    pub(super) analytics_interval: Duration,
    pub(super) statistics_interval: Duration,
    pub(super) content_interval: Duration,
    pub(super) prices_interval: Duration,
    pub(super) commission_interval: Duration,
    pub(super) logistics_tariff_interval: Duration,
    pub(super) acceptance_interval: Duration,
    pub(super) promotion_campaign_interval: Duration,
    pub(super) promotion_stats_interval: Duration,
    pub(super) search_report_interval: Duration,
    pub(super) promotion_minimum_bids_interval: Duration,
    pub(super) promotion_recommendations_interval: Duration,
    pub(super) promotion_cluster_bids_interval: Duration,
    pub(super) seller_inventory_interval: Duration,
    pub(super) max_attempts: usize,
    pub(super) base_retry_delay: Duration,
    pub(super) max_retry_delay: Duration,
    pub(super) logical_timeout: Duration,
}

impl ClientPolicy {
    /// The generic slice of this policy, used for the backoff arithmetic that
    /// is shared with the Ozon client. The Wildberries ceiling is deliberately
    /// far above Ozon's: retries here run inside a sixty-second logical
    /// request deadline rather than a five-second overhead budget.
    pub(super) const fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::new(
            self.max_attempts,
            self.base_retry_delay,
            self.max_retry_delay,
        )
    }

    pub(super) fn production(request_timeout: Duration) -> Self {
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
            seller_inventory_interval: SELLER_INVENTORY_MIN_REQUEST_INTERVAL,
            max_attempts: MAX_ATTEMPTS,
            base_retry_delay: BASE_RETRY_DELAY,
            max_retry_delay: MAX_RETRY_DELAY,
            logical_timeout: request_timeout
                .saturating_mul(2)
                .min(MAX_LOGICAL_REQUEST_DURATION),
        }
    }

    #[cfg(test)]
    pub(super) const fn immediate_single_attempt(logical_timeout: Duration) -> Self {
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
            seller_inventory_interval: Duration::ZERO,
            max_attempts: 1,
            base_retry_delay: Duration::ZERO,
            max_retry_delay: Duration::from_secs(1),
            logical_timeout,
        }
    }

    pub(super) const fn interval(self, request_class: RequestClass) -> Duration {
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
            RequestClass::PromotionBalance => Duration::from_secs(1),
            RequestClass::PromotionStats => self.promotion_stats_interval,
            RequestClass::SearchReport => self.search_report_interval,
            RequestClass::PromotionMinimumBids => self.promotion_minimum_bids_interval,
            RequestClass::PromotionRecommendedBids => self.promotion_recommendations_interval,
            RequestClass::PromotionClusterBids => self.promotion_cluster_bids_interval,
            RequestClass::SellerInventory => self.seller_inventory_interval,
        }
    }
}
