/// Every Ozon Seller API path this process is allowed to reach.
///
/// This is the single source of truth for the read-only guarantee: it is
/// enforced by [`super::OzonClient::post`] itself, at the only place where an HTTP
/// request can leave the process, so no caller — present or future — can reach
/// a mutating Ozon endpoint even if a higher layer forgets to check.
pub const ANALYTICS_DATA_PATH: &str = "/v1/analytics/data";

pub const READ_ONLY_ENDPOINT_ALLOWLIST: &[&str] = &[
    ANALYTICS_DATA_PATH,
    "/v1/analytics/product-queries",
    "/v1/analytics/product-queries/details",
    "/v1/analytics/turnover/stocks",
    "/v1/finance/accrual/by-day",
    "/v1/finance/accrual/postings",
    "/v1/finance/accrual/types",
    "/v1/finance/cash-flow-statement/list",
    "/v1/finance/mutual-settlement",
    "/v1/finance/realization/by-day",
    "/v1/posting/fbo/cancel-reason/list",
    "/v1/product/info/stocks-by-warehouse/fbo",
    "/v1/product/info/warehouse/stocks",
    "/v1/question/list",
    "/v1/rating/history",
    "/v1/rating/summary",
    "/v1/returns/list",
    "/v2/posting/fbo/get",
    "/v2/posting/fbs/cancel-reason/list",
    "/v2/product/info/stocks-by-warehouse/fbs",
    "/v2/product/pictures/info",
    "/v2/returns/rfbs/list",
    "/v2/review/list",
    "/v2/warehouse/list",
    "/v3/finance/transaction/list",
    "/v3/finance/transaction/totals",
    "/v3/posting/fbo/list",
    "/v3/posting/fbs/get",
    "/v3/product/info/list",
    "/v3/product/list",
    "/v3/supply-order/get",
    "/v3/supply-order/list",
    "/v4/posting/fbs/list",
    "/v4/posting/fbs/unfulfilled/list",
    "/v4/product/info/attributes",
    "/v4/product/info/stocks",
    "/v5/product/info/prices",
];

/// Reserved for future canary-only read endpoints.
///
/// The finance accrual
/// contracts have completed their canary period and now live in the stable
/// allowlist above. The empty constant keeps the feature-flag API compatible
/// while callers migrate away from the old preview switch.
pub const PREVIEW_READ_ONLY_ENDPOINT_ALLOWLIST: &[&str] = &[];

#[must_use]
pub fn is_read_only_endpoint_allowed(endpoint: &str) -> bool {
    READ_ONLY_ENDPOINT_ALLOWLIST.contains(&endpoint)
}

pub(super) fn is_search_path(path: &str) -> bool {
    matches!(
        path,
        "/v1/analytics/product-queries" | "/v1/analytics/product-queries/details"
    )
}

#[cfg(test)]
mod tests {
    use super::super::{
        Duration, MIN_REQUEST_INTERVAL, OzonClient, OzonError, RateLimiter, StoreCredentials,
        StoreId,
    };
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn search_analytics_has_its_own_gate_and_shares_it_between_both_methods() {
        let limiter = RateLimiter::new();
        limiter
            .try_claim_for("/v1/analytics/product-queries")
            .await
            .unwrap();
        assert!(
            limiter
                .ready_in_for("/v1/analytics/product-queries/details")
                .await
                > Duration::from_secs(60)
        );
        assert!(limiter.ready_in_for(ANALYTICS_DATA_PATH).await <= MIN_REQUEST_INTERVAL);
        tokio::time::sleep(MIN_REQUEST_INTERVAL).await;
        limiter.try_claim_for(ANALYTICS_DATA_PATH).await.unwrap();
    }

    #[tokio::test]
    async fn search_errors_are_terminal_and_do_not_replay() {
        for status in [401, 402, 403, 429, 500] {
            let (url, requests) = crate::test_support::mock_http(vec![(status, "{}".into())]);
            let store = StoreId::from("synthetic");
            let client = OzonClient::new(
                url,
                Duration::from_secs(2),
                BTreeMap::from([(
                    store.clone(),
                    StoreCredentials {
                        client_id: "synthetic-client".into(),
                        api_key: "synthetic-key".into(),
                    },
                )]),
            )
            .unwrap();
            let error = client
                .post(&store, "/v1/analytics/product-queries", json!({}))
                .await
                .unwrap_err();
            assert!(!matches!(error, OzonError::DeadlineExceeded), "{error}");
            assert!(
                requests
                    .recv()
                    .unwrap()
                    .starts_with("POST /v1/analytics/product-queries ")
            );
            assert!(requests.try_recv().is_err());
        }
    }

    #[test]
    fn only_exact_search_read_paths_are_admitted() {
        for path in [
            "/v1/analytics/product-queries",
            "/v1/analytics/product-queries/details",
        ] {
            assert!(is_search_path(path));
            assert!(is_read_only_endpoint_allowed(path));
            for suffix in ["/", "?url=http://127.0.0.1", "/../import", "#fragment"] {
                assert!(!is_read_only_endpoint_allowed(&format!("{path}{suffix}")));
            }
        }
    }
}
