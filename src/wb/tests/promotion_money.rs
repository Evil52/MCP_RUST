use super::*;
use crate::{
    test_support::mock_http,
    wb::{WbCredentials, WbErrorKind, policy::RequestClass},
};
use std::{collections::BTreeMap, time::Duration};

fn client(base: &str) -> WbClient {
    WbClient::new_for_test(
        Duration::from_secs(2),
        BTreeMap::from([(
            "account".into(),
            WbCredentials {
                token: "synthetic".into(),
            },
        )]),
        base,
        base,
    )
}

#[tokio::test]
async fn histories_preserve_values_and_payments_204_is_empty_only_for_that_endpoint() {
    let (url, requests) = mock_http(vec![
        (200, r#"[{"updSum":24,"advertId":5}]"#.into()),
        (204, String::new()),
        (200, r#"[{"sum":600,"statusId":1}]"#.into()),
        (204, String::new()),
        (200, String::new()),
        (200, "{}".into()),
    ]);
    let client = client(&url);
    assert_eq!(
        client
            .promotion_costs("account", "2026-08-01", "2026-08-31")
            .await
            .unwrap()[0]["updSum"],
        24
    );
    assert_eq!(
        client
            .promotion_payments("account", "2026-08-01", "2026-08-31")
            .await
            .unwrap(),
        serde_json::json!([])
    );
    assert_eq!(
        client
            .promotion_payments("account", "2026-08-01", "2026-08-31")
            .await
            .unwrap()[0]["sum"],
        600
    );
    for payments in [false, true, true] {
        let error = if payments {
            client
                .promotion_payments("account", "2026-08-01", "2026-08-31")
                .await
        } else {
            client
                .promotion_costs("account", "2026-08-01", "2026-08-31")
                .await
        }
        .unwrap_err();
        assert_eq!(error.kind(), WbErrorKind::InvalidJson);
    }
    for path in [
        COSTS_PATH,
        PAYMENTS_PATH,
        PAYMENTS_PATH,
        COSTS_PATH,
        PAYMENTS_PATH,
        PAYMENTS_PATH,
    ] {
        assert!(
            requests
                .recv()
                .unwrap()
                .starts_with(&format!("GET {path}?from=2026-08-01&to=2026-08-31 "))
        );
    }
}

#[test]
fn history_rejects_unbounded_dates() {
    for (from, to) in [
        ("2026-08-01", "2026-09-01"),
        ("2026-09-01", "2026-08-01"),
        ("2026-02-30", "2026-03-01"),
        ("2026-8-01", "2026-08-02"),
        ("", "2026-08-01"),
    ] {
        assert!(validate_period(from, to).is_err());
    }
    assert!(validate_period("2026-08-01", "2026-08-01").is_ok());
}

#[tokio::test]
async fn history_cooldown_fails_promptly_and_does_not_block_other_sources() {
    let (url, requests) = mock_http(vec![(200, "[]".into()), (200, "[]".into())]);
    let mut client = client(&url);
    client.policy.promotion_history_interval = Duration::from_hours(1);
    client
        .promotion_costs("account", "2026-08-01", "2026-08-01")
        .await
        .unwrap();
    let error = tokio::time::timeout(
        Duration::from_millis(200),
        client.promotion_costs("account", "2026-08-01", "2026-08-01"),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(error, WbError::LocalRateLimited { .. }));
    client
        .promotion_payments("account", "2026-08-01", "2026-08-01")
        .await
        .unwrap();
    requests.recv().unwrap();
    requests.recv().unwrap();
    assert!(requests.try_recv().is_err());
    assert!(
        client.limiters["account"]
            .ready_in(RequestClass::FbsOrders)
            .await
            .is_zero()
    );
}
