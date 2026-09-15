//! Shared quota names and vendor cooldowns used by WB reads and guarded writes.

use std::time::Duration;

use chrono::Utc;
use reqwest::{StatusCode, header::HeaderMap};

use super::response::{is_retriable, parse_retry_delay};
use super::{AttemptContext, WbError, policy::RequestClass};
use crate::marketplace_quota::{QuotaError, QuotaKey};

const PROMOTION_CAMPAIGN_BUCKET: &str = "promotion_campaign";
const PROMOTION_BALANCE_BUCKET: &str = "promotion_balance";

impl RequestClass {
    pub(super) const fn shared_quota_bucket(self) -> &'static str {
        match self {
            Self::AnalyticsPing => "analytics_ping",
            Self::AnalyticsReport => "analytics_report",
            Self::StatisticsReport => "statistics_report",
            Self::FinanceReport => "finance_report",
            Self::FeedbackReport => "feedback_report",
            Self::ReturnClaims => "return_claims",
            Self::SupplyReport => "supply_report",
            Self::CardErrors => "card_errors",
            Self::ContentReport => "content_report",
            Self::PricesReport => "prices_report",
            Self::CommissionTariff => "commission_tariff",
            Self::LogisticsTariff => "logistics_tariff",
            Self::AcceptanceTariff => "acceptance_tariff",
            Self::PromotionCampaign => PROMOTION_CAMPAIGN_BUCKET,
            Self::PromotionBalance => PROMOTION_BALANCE_BUCKET,
            Self::PromotionStats => "promotion_stats",
            Self::SearchReport => "search_report",
            Self::PromotionMinimumBids => "promotion_minimum_bids",
            Self::PromotionRecommendedBids => "promotion_recommended_bids",
            Self::PromotionClusterBids => "promotion_cluster_bids",
            Self::SellerInventory => "seller_inventory",
            Self::FbsOrders => "marketplace_fbs_orders",
            Self::PromotionCosts => "promotion_costs",
            Self::PromotionPayments => "promotion_payments",
        }
    }
}

pub(super) const fn read_quota_error(error: QuotaError) -> WbError {
    match error {
        QuotaError::Limited { retry_after } => WbError::LocalRateLimited { retry_after },
        QuotaError::Unavailable => WbError::SharedQuota {
            reason: "unavailable",
        },
        QuotaError::InvalidIdentity => WbError::SharedQuota {
            reason: "invalid_identity",
        },
    }
}

pub fn vendor_quota_cooldown(headers: &HeaderMap, status: StatusCode) -> Option<Duration> {
    let exhausted = headers
        .get("x-ratelimit-remaining")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim() == "0");
    if !is_retriable(status) && !exhausted {
        return None;
    }
    parse_retry_delay(headers, Utc::now())
        .duration()
        .or_else(|| (status == StatusCode::TOO_MANY_REQUESTS).then_some(Duration::from_secs(60)))
}

impl super::WbClient {
    pub(super) fn shared_quota_key(
        &self,
        context: AttemptContext<'_>,
    ) -> Result<Option<QuotaKey>, WbError> {
        if !self.shared_quota.is_enabled() {
            return Ok(None);
        }
        let credentials = self
            .accounts
            .get(context.account)
            .ok_or_else(|| WbError::MissingCredentials(context.account.to_owned()))?;
        QuotaKey::wb(
            &credentials.token,
            context.request_class.shared_quota_bucket(),
        )
        .map(Some)
        .map_err(read_quota_error)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use reqwest::Method;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::timeout,
    };

    use super::*;
    use crate::{
        marketplace_quota::SharedQuota,
        wb::{WbClient, WbCredentials},
    };

    fn token() -> String {
        format!(
            "e30.{}.signature",
            URL_SAFE_NO_PAD.encode(br#"{"sid":"123e4567-e89b-42d3-a456-426614174000"}"#)
        )
    }

    fn database_quota() -> SharedQuota {
        SharedQuota::from_database_url(
            &std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL")
                .expect("requires isolated PostgreSQL collector URL"),
        )
    }

    fn database_client(base: &str, sid: &str, signature: &str) -> WbClient {
        let payload =
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&serde_json::json!({"sid": sid})).unwrap());
        let token = format!("e30.{payload}.{signature}");
        let accounts = BTreeMap::from([("store".to_owned(), WbCredentials { token })]);
        WbClient::new_for_test(Duration::from_secs(2), accounts, base, base)
            .with_shared_quota(database_quota())
    }

    async fn serve_one(listener: TcpListener, status: &str, headers: &str) -> TcpListener {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let mut buffer = [0; 1024];
            let read = socket.read(&mut buffer).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&buffer[..read]);
        }
        let body = if status.starts_with("204 ") { "" } else { "{}" };
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        listener
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL"]
    async fn independent_wb_clients_with_rotated_tokens_share_one_departure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let first = database_client(&base, "d1000000-0000-4000-8000-000000000001", "first");
        let second = database_client(&base, "d1000000-0000-4000-8000-000000000001", "rotated");
        let server = tokio::spawn(serve_one(listener, "200 OK", ""));
        let (first, second) = tokio::join!(first.ping("store"), second.ping("store"));
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        let error = first.err().or_else(|| second.err()).unwrap();
        assert!(matches!(error, WbError::LocalRateLimited { .. }));
        let listener = server.await.unwrap();
        assert!(
            timeout(Duration::from_millis(25), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL"]
    async fn terminal_finance_read_still_reserves_shared_seller_quota() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let first = database_client(&base, "d1000000-0000-4000-8000-000000000004", "first");
        let second = database_client(&base, "d1000000-0000-4000-8000-000000000004", "rotated");
        let server = tokio::spawn(serve_one(listener, "204 No Content", ""));
        assert!(
            first
                .financial_report_by_id_page("store", 42, 1, 0)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            second.financial_report_by_id_page("store", 42, 1, 0).await,
            Err(WbError::LocalRateLimited { .. })
        ));
        let listener = server.await.unwrap();
        assert!(
            timeout(Duration::from_millis(25), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL"]
    async fn final_attempt_vendor_cooldown_reaches_independent_wb_client() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let first = database_client(&base, "d1000000-0000-4000-8000-000000000002", "first");
        let second = database_client(&base, "d1000000-0000-4000-8000-000000000002", "second");
        let server = tokio::spawn(serve_one(
            listener,
            "429 Too Many Requests",
            "X-Ratelimit-Retry: 120\r\n",
        ));
        assert!(matches!(
            first.ping("store").await,
            Err(WbError::RateLimited { .. })
        ));
        assert!(
            matches!(second.ping("store").await, Err(WbError::LocalRateLimited { retry_after }) if retry_after > Duration::from_secs(100))
        );
        let listener = server.await.unwrap();
        assert!(
            timeout(Duration::from_millis(25), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL"]
    async fn wb_retry_rechecks_shared_departure_even_when_local_gate_is_ready() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let mut client = database_client(&base, "d1000000-0000-4000-8000-000000000003", "first");
        client.policy.max_attempts = 3;
        client.policy.base_retry_delay = Duration::from_millis(1);
        let server = tokio::spawn(serve_one(listener, "503 Service Unavailable", ""));
        assert!(matches!(
            client.ping("store").await,
            Err(WbError::Api {
                status: StatusCode::SERVICE_UNAVAILABLE,
                ..
            })
        ));
        let listener = server.await.unwrap();
        assert!(
            timeout(Duration::from_millis(25), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn configured_unavailable_quota_blocks_wb_before_any_wire_departure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let accounts = BTreeMap::from([("store".to_owned(), WbCredentials { token: token() })]);
        let client = WbClient::new_for_test(Duration::from_secs(1), accounts, &base, &base)
            .with_shared_quota(SharedQuota::from_database_url(
                "invalid quota configuration",
            ));
        for (method, path) in [
            (Method::POST, super::super::SALES_FUNNEL_PATH),
            (Method::GET, super::super::PROMOTION_CAMPAIGNS_PATH),
            (Method::GET, super::super::SELLER_WAREHOUSES_PATH),
            (Method::POST, super::super::FINANCE_DETAILS_PATH),
        ] {
            let error = client
                .request("store", method, path, None, None)
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                WbError::SharedQuota {
                    reason: "unavailable"
                }
            ));
        }
        assert!(
            timeout(Duration::from_millis(25), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn shared_quota_rejects_missing_seller_identity_without_sending_token() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let accounts = BTreeMap::from([(
            "store".to_owned(),
            WbCredentials {
                token: "opaque-token".to_owned(),
            },
        )]);
        let client = WbClient::new_for_test(Duration::from_secs(1), accounts, &base, &base)
            .with_shared_quota(SharedQuota::from_database_url(
                "invalid quota configuration",
            ));
        assert!(matches!(
            client.ping("store").await,
            Err(WbError::SharedQuota {
                reason: "invalid_identity"
            })
        ));
        assert!(
            timeout(Duration::from_millis(25), listener.accept())
                .await
                .is_err()
        );
    }

    #[test]
    fn vendor_cooldown_handles_wb_headers_and_terminal_attempts() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-retry", "120".parse().unwrap());
        assert_eq!(
            vendor_quota_cooldown(&headers, StatusCode::TOO_MANY_REQUESTS),
            Some(Duration::from_secs(120))
        );
        assert_eq!(vendor_quota_cooldown(&headers, StatusCode::OK), None);
        headers.insert("x-ratelimit-remaining", "0".parse().unwrap());
        assert_eq!(
            vendor_quota_cooldown(&headers, StatusCode::OK),
            Some(Duration::from_secs(120))
        );
        headers.clear();
        assert_eq!(
            vendor_quota_cooldown(&headers, StatusCode::TOO_MANY_REQUESTS),
            Some(Duration::from_secs(60))
        );
    }

    #[test]
    fn shared_vendor_cooldown_never_shortens_long_or_zero_retry_after() {
        for seconds in [0, 172_800, u64::MAX] {
            let mut headers = HeaderMap::new();
            headers.insert("x-ratelimit-retry", seconds.to_string().parse().unwrap());
            assert_eq!(
                vendor_quota_cooldown(&headers, StatusCode::TOO_MANY_REQUESTS),
                Some(Duration::from_secs(seconds))
            );
        }
    }
}
