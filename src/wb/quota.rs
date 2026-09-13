//! Shared quota names and vendor cooldowns used by WB reads and guarded writes.

use std::time::{Duration, Instant};

use chrono::Utc;
use reqwest::{Response, StatusCode, header::HeaderMap};

use super::{
    AttemptContext, AttemptOutcome, MAX_ERROR_BODY_BYTES, ParsedRetryDelay, WbError,
    classify_http_status, decode_response, extract_request_id, is_retriable,
    is_retriable_transport, parse_retry_delay, policy::RequestClass, read_body, retry_delay,
    retry_plan, trace_response,
};
use crate::marketplace_quota::{QuotaError, QuotaKey};

const PROMOTION_CAMPAIGN_BUCKET: &str = "promotion_campaign";
const PROMOTION_BALANCE_BUCKET: &str = "promotion_balance";

impl RequestClass {
    pub(super) const fn shared_quota_bucket(self) -> &'static str {
        match self {
            Self::AnalyticsPing => "analytics_ping",
            Self::AnalyticsReport => "analytics_report",
            Self::StatisticsReport => "statistics_report",
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

    pub(super) async fn response_outcome(
        &self,
        context: AttemptContext<'_>,
        attempt: usize,
        started: Instant,
        response: Response,
        quota_key: Option<&QuotaKey>,
    ) -> Result<AttemptOutcome, WbError> {
        let status = response.status();
        let shared_cooldown = vendor_quota_cooldown(response.headers(), status);
        let request_id = extract_request_id(response.headers());
        let retry_after = parse_retry_delay(response.headers(), Utc::now());
        let planned_retry = context
            .request_class
            .allows_automatic_retry()
            .then(|| retry_plan(status, attempt, retry_after, &self.policy))
            .flatten();
        let vendor_cooldown = match retry_after {
            ParsedRetryDelay::Valid(delay)
                if context.request_class == RequestClass::SellerInventory
                    && is_retriable(status) =>
            {
                // The one-attempt inventory reader must still honor a long
                // Retry-After for sibling callers. Cap untrusted delays at a
                // day; the generic retry budget is not this shared cooldown.
                Some(delay.min(Duration::from_hours(24)))
            }
            ParsedRetryDelay::Valid(delay)
                if is_retriable(status) && delay <= self.policy.max_retry_delay =>
            {
                Some(delay)
            }
            ParsedRetryDelay::Absent | ParsedRetryDelay::Invalid | ParsedRetryDelay::Valid(_) => {
                None
            }
        };
        let inventory_cooldown = if context.request_class == RequestClass::SellerInventory {
            match status {
                // WB charges ten requests for a 409 in both inventory groups.
                StatusCode::CONFLICT => Some(self.policy.seller_inventory_interval * 10),
                StatusCode::TOO_MANY_REQUESTS if vendor_cooldown.is_none() => {
                    Some(Duration::from_secs(60))
                }
                _ => None,
            }
        } else {
            None
        };
        let shared_inventory_cooldown = (context.request_class == RequestClass::SellerInventory
            && status == StatusCode::CONFLICT)
            .then_some(super::SELLER_INVENTORY_MIN_REQUEST_INTERVAL * 10);
        if let (Some(key), Some(delay)) = (
            quota_key,
            shared_cooldown
                .into_iter()
                .chain(shared_inventory_cooldown)
                .max(),
        ) {
            self.shared_quota
                .defer(key, delay)
                .await
                .map_err(read_quota_error)?;
        }
        if let Some(delay) = planned_retry
            .into_iter()
            .chain(vendor_cooldown)
            .chain(inventory_cooldown)
            .max()
        {
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
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Length: 2\r\nConnection: close\r\n{headers}\r\n{{}}"
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
