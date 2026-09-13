use std::collections::BTreeMap;

use jsonwebtoken::{EncodingKey, Header, encode};
use serde_json::json;

use super::*;
use crate::{
    test_support::mock_http,
    wb::{BaseUrls, ClientPolicy, RequestClass, WbCredentials},
};

fn token(acc: u8, s: u64, exp: i64) -> String {
    encode(
        &Header::default(),
        &json!({"acc":acc,"s":s,"exp":exp}),
        &EncodingKey::from_secret(b"synthetic-test-secret"),
    )
    .unwrap()
}

fn personal_token() -> String {
    token(
        3,
        (1 << 30) | (1 << 13),
        chrono::Utc::now().timestamp() + 3600,
    )
}

fn client(base: &str, token: &str) -> WbClient {
    let credentials = WbCredentials {
        token: token.to_owned(),
    };
    WbClient::build(
        Duration::from_secs(2),
        BTreeMap::from([
            ("account".to_owned(), credentials.clone()),
            ("alias".to_owned(), credentials),
        ]),
        BaseUrls::for_test(base, base),
        ClientPolicy::production(Duration::from_secs(2)),
    )
}

#[tokio::test]
async fn successful_personal_read_proves_one_minute_quota_and_shares_alias_gate() {
    let (base, requests) = mock_http(vec![(204, String::new())]);
    let client = client(&base, &personal_token());
    assert_eq!(
        client
            .financial_report_min_interval("account")
            .await
            .unwrap(),
        Duration::from_hours(12)
    );
    assert!(
        client
            .financial_report_by_id_page("account", 1, 1, 0)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        client.financial_report_min_interval("alias").await.unwrap(),
        PERSONAL_INTERVAL
    );
    let error = client
        .financial_report_by_id_page("alias", 2, 1, 0)
        .await
        .unwrap_err();
    assert!(
        matches!(error, WbError::LocalRateLimited {retry_after} if retry_after > Duration::from_secs(55) && retry_after <= PERSONAL_INTERVAL)
    );
    assert!(
        requests
            .recv()
            .unwrap()
            .starts_with("POST /api/finance/v1/sales-reports/detailed/1 ")
    );
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn local_personal_claims_and_failed_finance_read_never_promote_quota() {
    for status in [201, 202, 401, 403, 429, 503] {
        let (base, requests) = mock_http(vec![(status, "{}".to_owned())]);
        let client = client(&base, &personal_token());
        assert!(
            client
                .financial_report_by_id_page("account", 1, 1, 0)
                .await
                .is_err()
        );
        assert_eq!(
            client
                .financial_report_min_interval("account")
                .await
                .unwrap(),
            Duration::from_hours(12)
        );
        assert!(requests.recv().is_ok());
        assert!(requests.try_recv().is_err());
    }
}

#[tokio::test]
async fn unknown_base_expired_and_write_enabled_tokens_keep_conservative_interval() {
    let future = chrono::Utc::now().timestamp() + 3600;
    let both = (1 << 30) | (1 << 13);
    for key in [
        "unknown-token".to_owned(),
        token(1, both, future),
        token(3, both, 1),
        token(3, 1 << 13, future),
        token(3, 1 << 30, future),
    ] {
        let (base, _) = mock_http(vec![(204, String::new())]);
        let client = client(&base, &key);
        client
            .financial_report_by_id_page("account", 1, 1, 0)
            .await
            .unwrap();
        assert_eq!(
            client
                .financial_report_min_interval("account")
                .await
                .unwrap(),
            Duration::from_hours(12)
        );
    }
}

#[tokio::test]
async fn proof_promotion_preserves_existing_vendor_retry_after_and_never_repromotes() {
    let limiter = crate::wb::TokenLimiter::new();
    limiter
        .try_claim(RequestClass::FinanceReport, Duration::from_hours(12))
        .await
        .unwrap();
    // A server delay shorter than the old conservative interval must survive
    // promotion too, rather than being hidden behind the initial reservation.
    limiter
        .extend_cooldown(RequestClass::FinanceReport, Duration::from_hours(2))
        .await;
    limiter
        .finance_access
        .confirm_read(
            &personal_token(),
            &limiter.finance_reports,
            Duration::from_hours(12),
        )
        .await;
    assert!(limiter.ready_in(RequestClass::FinanceReport).await > Duration::from_secs(7195));
    limiter
        .extend_cooldown(RequestClass::FinanceReport, Duration::from_hours(24))
        .await;
    limiter
        .finance_access
        .confirm_read(
            &personal_token(),
            &limiter.finance_reports,
            Duration::from_hours(12),
        )
        .await;
    assert!(limiter.ready_in(RequestClass::FinanceReport).await > Duration::from_secs(86395));
}
