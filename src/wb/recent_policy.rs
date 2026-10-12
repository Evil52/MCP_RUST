//! Reviewed October 2026 reads. Each long quota has its own fail-fast gate.
use super::policy::{ApiHost, ClientPolicy, EndpointPolicy, RequestClass};
use jsonwebtoken::dangerous::insecure_decode;
use reqwest::Method;
use serde::Deserialize;
use std::time::Duration;

pub const ORDER_FEED: &str = "/api/analytics/v1/order-feed";
pub const BUDGETS: &str = "/api/advert/v2/budget";
pub const DAILY_LIMITS: &str = "/api/advert/v0/daily-limits";
pub const DISCREPANCIES: &str = "/api/supplies/v1/discrepancies/{supplyId}";
pub const ITEM_RETURNS: &str = "/api/analytics/v1/item-returns";
pub const DOWNLOADS: &str = "/api/v2/nm-report/downloads";
pub const DOWNLOAD_FILE: &str = "/api/v2/nm-report/downloads/file/{downloadId}";
pub const DOWNLOAD_LABEL: &str = "analytics:/api/v2/nm-report/downloads/file/{downloadId}";

pub(super) const ENDPOINTS: &[EndpointPolicy] = &[
    EndpointPolicy {
        method: Method::POST,
        path: ORDER_FEED,
        label: "analytics:/api/analytics/v1/order-feed",
        host: ApiHost::Analytics,
        request_class: RequestClass::OrderFeed,
    },
    EndpointPolicy {
        method: Method::GET,
        path: DAILY_LIMITS,
        label: "promotion:/api/advert/v0/daily-limits",
        host: ApiHost::Promotion,
        request_class: RequestClass::PromotionDailyLimits,
    },
    EndpointPolicy {
        method: Method::GET,
        path: DISCREPANCIES,
        label: "supplies:/api/supplies/v1/discrepancies/{supplyId}",
        host: ApiHost::Supplies,
        request_class: RequestClass::SupplyReport,
    },
    EndpointPolicy {
        method: Method::GET,
        path: ITEM_RETURNS,
        label: "analytics:/api/analytics/v1/item-returns",
        host: ApiHost::Analytics,
        request_class: RequestClass::ItemReturns,
    },
    EndpointPolicy {
        method: Method::POST,
        path: DOWNLOADS,
        label: "analytics:/api/v2/nm-report/downloads",
        host: ApiHost::Analytics,
        request_class: RequestClass::AnalyticsDownloads,
    },
    EndpointPolicy {
        method: Method::GET,
        path: DOWNLOADS,
        label: "analytics:/api/v2/nm-report/downloads",
        host: ApiHost::Analytics,
        request_class: RequestClass::AnalyticsDownloadStatus,
    },
    EndpointPolicy {
        method: Method::GET,
        path: DOWNLOAD_FILE,
        label: DOWNLOAD_LABEL,
        host: ApiHost::Analytics,
        request_class: RequestClass::AnalyticsDownloadFile,
    },
];

pub(super) fn matches_dynamic(template: &str, path: &str) -> Option<bool> {
    if template == DISCREPANCIES {
        return Some(super::policy::canonical_positive_id_segment(
            path,
            "/api/supplies/v1/discrepancies/",
        ));
    }
    (template == DOWNLOAD_FILE).then(|| {
        path.strip_prefix("/api/v2/nm-report/downloads/file/")
            .is_some_and(valid_uuid)
    })
}

pub(super) fn valid_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
}

#[derive(Deserialize)]
struct CapabilityHints {
    acc: u8,
    #[serde(rename = "for")]
    audience: String,
    t: bool,
    s: u64,
    exp: i64,
}

impl ClientPolicy {
    /// Local JWT hints select documented pacing, never authorize a request.
    /// Unknown, expired, test and Base tokens keep conservative intervals.
    pub(super) fn recent_interval(&self, class: RequestClass, token: &str) -> Duration {
        let conservative = self.interval(class);
        let (category, fast) = match class {
            RequestClass::PromotionBudgets => (6, Duration::from_secs(3)),
            RequestClass::OrderFeed => (2, Duration::from_mins(1)),
            RequestClass::AnalyticsDownloads
            | RequestClass::AnalyticsDownloadStatus
            | RequestClass::AnalyticsDownloadFile => (2, Duration::from_secs(20)),
            _ => return conservative,
        };
        if token.len() > 16_384 {
            return conservative;
        }
        let Ok(decoded) = insecure_decode::<CapabilityHints>(token) else {
            return conservative;
        };
        let hints = decoded.claims;
        // Service tokens require an additional secret/binding that this
        // client does not configure. Config currently admits Personal only.
        let recognized = hints.acc == 3 && hints.audience == "self";
        if recognized
            && !hints.t
            && hints.exp > chrono::Utc::now().timestamp()
            && hints.s & (1 << category) != 0
        {
            conservative.min(fast)
        } else {
            conservative
        }
    }
}
