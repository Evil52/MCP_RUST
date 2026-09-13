//! Fixed production hosts; caller-selected URLs are test-only.

use super::{
    ANALYTICS_API_BASE_URL, ApiHost, COMMON_API_BASE_URL, CONTENT_API_BASE_URL,
    FINANCE_API_BASE_URL, MARKETPLACE_API_BASE_URL, PRICES_API_BASE_URL, PROMOTION_API_BASE_URL,
    STATISTICS_API_BASE_URL,
};

#[derive(Debug, Clone)]
pub(super) struct BaseUrls {
    pub(super) analytics: String,
    pub(super) statistics: String,
    pub(super) content: String,
    pub(super) prices: String,
    pub(super) common: String,
    pub(super) promotion: String,
    pub(super) marketplace: String,
    pub(super) finance: String,
}

impl BaseUrls {
    pub(super) fn production() -> Self {
        Self {
            analytics: ANALYTICS_API_BASE_URL.to_owned(),
            statistics: STATISTICS_API_BASE_URL.to_owned(),
            content: CONTENT_API_BASE_URL.to_owned(),
            prices: PRICES_API_BASE_URL.to_owned(),
            common: COMMON_API_BASE_URL.to_owned(),
            promotion: PROMOTION_API_BASE_URL.to_owned(),
            marketplace: MARKETPLACE_API_BASE_URL.to_owned(),
            finance: FINANCE_API_BASE_URL.to_owned(),
        }
    }

    #[cfg(test)]
    pub(super) fn for_test(common_base_url: &str, analytics_base_url: &str) -> Self {
        let common = common_base_url.trim_end_matches('/').to_owned();
        Self {
            analytics: analytics_base_url.trim_end_matches('/').to_owned(),
            statistics: common.clone(),
            content: common.clone(),
            prices: common.clone(),
            common: common.clone(),
            promotion: common.clone(),
            marketplace: common.clone(),
            finance: common,
        }
    }

    pub(super) fn base_url(&self, host: ApiHost) -> &str {
        match host {
            ApiHost::Analytics => &self.analytics,
            ApiHost::Statistics => &self.statistics,
            ApiHost::Content => &self.content,
            ApiHost::Prices => &self.prices,
            ApiHost::Common => &self.common,
            ApiHost::Promotion => &self.promotion,
            ApiHost::Marketplace => &self.marketplace,
            ApiHost::Finance => &self.finance,
        }
    }
}
