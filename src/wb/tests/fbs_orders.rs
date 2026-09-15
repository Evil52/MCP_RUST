use super::*;
use crate::test_support::mock_http;
use crate::wb::{
    WbCredentials, WbErrorKind,
    policy::{ClientPolicy, EndpointPolicy, RequestClass},
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
async fn fbs_wire_and_empty_page_preserve_cursor_and_period() {
    let (url, requests) = mock_http(vec![
        (200, r#"{"orders":[{"id":1,"deliveryType":"fbs"}]}"#.into()),
        (200, r#"{"next":5,"orders":[{"id":5}]}"#.into()),
        (200, r#"{"next":5,"orders":[]}"#.into()),
        (
            200,
            r#"{"orders":[{"id":5,"supplierStatus":"new","wbStatus":"waiting"}]}"#.into(),
        ),
    ]);
    let client = client(&url);
    assert_eq!(
        client.fbs_new_orders("account").await.unwrap()["orders"][0]["id"],
        1
    );
    assert_eq!(
        client
            .fbs_orders_page("account", 1, 0, 100, 200)
            .await
            .unwrap()["next"],
        5
    );
    assert_eq!(
        client
            .fbs_orders_page("account", 1, 5, 100, 200)
            .await
            .unwrap()["orders"],
        json!([])
    );
    client.fbs_order_statuses("account", &[5, 6]).await.unwrap();
    assert!(
        requests
            .recv()
            .unwrap()
            .starts_with("GET /api/v3/orders/new ")
    );
    for cursor in [0, 5] {
        let request = requests.recv().unwrap();
        assert!(request.starts_with(&format!(
            "GET /api/v3/orders?limit=1&next={cursor}&dateFrom=100&dateTo=200 "
        )));
    }
    let request = requests.recv().unwrap();
    assert!(request.starts_with("POST /api/v3/orders/status "));
    assert_eq!(
        serde_json::from_str::<Value>(request.split("\r\n\r\n").nth(1).unwrap()).unwrap(),
        json!({"orders":[5,6]})
    );
}

#[tokio::test]
async fn fbs_invalid_inputs_never_reach_wire() {
    let client = client("http://127.0.0.1:1");
    for (limit, next, from, to) in [
        (0, 0, 0, 1),
        (1001, 0, 0, 1),
        (1, u64::MAX, 0, 1),
        (1, 0, -1, 1),
        (1, 0, 10, 1),
        (1, 0, 0, 30 * 86_400 + 1),
    ] {
        assert_eq!(
            client
                .fbs_orders_page("account", limit, next, from, to)
                .await
                .unwrap_err()
                .kind(),
            WbErrorKind::InvalidArguments
        );
    }
    for ids in [
        vec![],
        vec![0],
        vec![1, 1],
        vec![u64::MAX],
        (1..=1001).collect(),
    ] {
        assert_eq!(
            client
                .fbs_order_statuses("account", &ids)
                .await
                .unwrap_err()
                .kind(),
            WbErrorKind::InvalidArguments
        );
    }
}

#[tokio::test]
async fn fbs_cursor_is_opaque_and_must_not_restart_a_nonempty_page() {
    for (cursor, accepted) in [(5, true), (0, false), (10, false)] {
        let (url, _) = mock_http(vec![(
            200,
            json!({"next":cursor,"orders":[{"id":12}]}).to_string(),
        )]);
        let result = client(&url).fbs_orders_page("account", 100, 10, 0, 1).await;
        assert_eq!(result.is_ok(), accepted);
    }
}

#[tokio::test]
async fn fbs_rejects_malformed_pages_repeated_cursors_and_foreign_statuses() {
    for body in [
        json!({}),
        json!({"orders":[],"next":null}),
        json!({"orders":[{"id":1}],"next":1}),
        json!({"orders":[{"id":2},{"id":2}],"next":2}),
        json!({"orders":[{"id":0}],"next":2}),
    ] {
        let (url, _) = mock_http(vec![(200, body.to_string())]);
        assert_eq!(
            client(&url)
                .fbs_orders_page("account", 10, 1, 0, 1)
                .await
                .unwrap_err()
                .kind(),
            WbErrorKind::InvalidJson
        );
    }
    for body in [
        json!({"orders":[{"id":2,"supplierStatus":"new","wbStatus":"waiting"}]}),
        json!({"orders":[{"id":1,"supplierStatus":null,"wbStatus":"waiting"}]}),
    ] {
        let (url, _) = mock_http(vec![(200, body.to_string())]);
        assert_eq!(
            client(&url)
                .fbs_order_statuses("account", &[1])
                .await
                .unwrap_err()
                .kind(),
            WbErrorKind::InvalidJson
        );
    }
}

#[test]
fn operational_policy_has_exact_paths_and_separate_conservative_quotas() {
    let policy = ClientPolicy::production(Duration::from_secs(10));
    for (method, path, class, interval) in [
        (
            Method::GET,
            NEW_PATH,
            RequestClass::FbsOrders,
            Duration::from_secs(2),
        ),
        (
            Method::GET,
            LIST_PATH,
            RequestClass::FbsOrders,
            Duration::from_secs(2),
        ),
        (
            Method::POST,
            STATUS_PATH,
            RequestClass::FbsOrders,
            Duration::from_secs(2),
        ),
        (
            Method::GET,
            "/adv/v1/upd",
            RequestClass::PromotionCosts,
            Duration::from_hours(1),
        ),
        (
            Method::GET,
            "/adv/v1/payments",
            RequestClass::PromotionPayments,
            Duration::from_hours(1),
        ),
    ] {
        assert_eq!(
            EndpointPolicy::for_request(&method, path)
                .unwrap()
                .request_class,
            class
        );
        assert_eq!(policy.interval(class), interval);
        assert!(!class.allows_automatic_retry());
        for suffix in ["/", "?x=1", "/../cancel", "%2Fcancel"] {
            assert!(EndpointPolicy::for_request(&method, &format!("{path}{suffix}")).is_none());
        }
        for wrong in [Method::PUT, Method::DELETE, Method::PATCH] {
            assert!(EndpointPolicy::for_request(&wrong, path).is_none());
        }
    }
    for (method, path) in [
        (Method::PATCH, "/api/v3/orders/1/cancel"),
        (Method::POST, "/adv/v1/budget/deposit"),
        (Method::POST, "/adv/v1/payments"),
        (Method::POST, LIST_PATH),
        (Method::GET, STATUS_PATH),
    ] {
        assert!(EndpointPolicy::for_request(&method, path).is_none());
    }
    assert_ne!(
        RequestClass::PromotionCosts.shared_quota_bucket(),
        RequestClass::PromotionPayments.shared_quota_bucket()
    );
    assert_ne!(
        RequestClass::FbsOrders.shared_quota_bucket(),
        RequestClass::SellerInventory.shared_quota_bucket()
    );
}
