use super::*;
use crate::server::inputs_replenishment::{
    ClusterListInput, ClusterStockAnalyticsInput, SupplyBundleInput,
};

#[tokio::test]
async fn replenishment_reads_preserve_fixed_paths_and_bounded_payloads() {
    let (server, requests) = mock_server(3);
    server
        .clusters(
            RequestIdentity::dev(),
            Parameters(ClusterListInput {
                store: Some(StoreId::from("store_a")),
                cluster_ids: vec!["4007".into()],
            }),
        )
        .await
        .unwrap();
    server
        .cluster_stock_analytics(
            RequestIdentity::dev(),
            Parameters(ClusterStockAnalyticsInput {
                store: Some(StoreId::from("store_a")),
                skus: vec!["11".into()],
                warehouse_ids: vec!["-1051784".into()],
                cluster_ids: vec!["4007".into()],
            }),
        )
        .await
        .unwrap();
    server
        .supply_bundle(
            RequestIdentity::dev(),
            Parameters(SupplyBundleInput {
                store: Some(StoreId::from("store_a")),
                bundle_ids: vec!["01a13993-e904-7bc4-8756-bbbe0d9e5f72".into()],
                limit: 100,
                query: String::new(),
                last_id: Some("123".to_owned()),
            }),
        )
        .await
        .unwrap();
    for (path, payload) in [
        (
            "/v1/cluster/list",
            json!({"cluster_ids":["4007"],"cluster_type":"CLUSTER_TYPE_OZON"}),
        ),
        (
            "/v1/analytics/stocks",
            json!({"skus":["11"],"warehouse_ids":["-1051784"],"cluster_ids":["4007"]}),
        ),
        (
            "/v1/supply-order/bundle",
            json!({"bundle_ids":["01a13993-e904-7bc4-8756-bbbe0d9e5f72"],"limit":100,"last_id":"123","is_asc":true,"query":"","sort_field":"UNSPECIFIED"}),
        ),
    ] {
        let request = requests.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(request.starts_with(&format!("POST {path} ")));
        assert_eq!(
            serde_json::from_str::<Value>(request.split_once("\r\n\r\n").unwrap().1).unwrap(),
            payload
        );
    }
}

#[tokio::test]
async fn warehouse_stock_filter_errors_are_local_and_replenishment_reads_keep_account_boundaries() {
    let (server, requests) = mock_server(0);
    assert!(
        server
            .fbo_stocks_by_warehouse(
                RequestIdentity::dev(),
                Parameters(WarehouseStockListInput {
                    store: Some(StoreId::from("store_a")),
                    offer_ids: vec![],
                    skus: vec![],
                    limit: 100,
                    cursor: String::new()
                })
            )
            .await
            .err()
            .expect("empty warehouse filter must be rejected")
            .contains("require offer_ids or skus")
    );
    let denied = manager_server("analyst_denied")
        .clusters(
            RequestIdentity::dev(),
            Parameters(ClusterListInput {
                store: Some(StoreId::from("store_a")),
                cluster_ids: vec![],
            }),
        )
        .await;
    assert!(denied.is_err());
    assert!(requests.try_recv().is_err());
    assert!(
        serde_json::from_value::<ClusterStockAnalyticsInput>(
            json!({"skus":["11"],"endpoint":"/v2/products/stocks"})
        )
        .is_err()
    );
}

#[tokio::test]
async fn warehouse_filter_rejects_zero_noncanonical_overflowing_and_duplicate_ids_locally() {
    let (server, requests) = mock_server(0);
    for warehouse_ids in [
        vec!["0"],
        vec!["-0"],
        vec!["+1"],
        vec!["01"],
        vec!["-01"],
        vec!["9223372036854775808"],
        vec!["-1", "-1"],
    ] {
        let result = server
            .cluster_stock_analytics(
                RequestIdentity::dev(),
                Parameters(ClusterStockAnalyticsInput {
                    store: Some(StoreId::from("store_a")),
                    skus: vec!["11".into()],
                    warehouse_ids: warehouse_ids.into_iter().map(str::to_owned).collect(),
                    cluster_ids: vec![],
                }),
            )
            .await;
        assert!(result.is_err());
    }
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn optional_date_filters_are_forwarded_exactly() {
    let (server, requests) = mock_server(2);

    server
        .fbs_unfulfilled(
            RequestIdentity::dev(),
            Parameters(FbsUnfulfilledInput {
                store: Some(StoreId::from("store_a")),
                date_from: "2026-08-01".to_owned(),
                date_to: "2026-08-02".to_owned(),
                cursor: String::new(),
                limit: 100,
                direction: SortDirection::Asc,
                statuses: Vec::new(),
                warehouse_ids: Vec::new(),
                provider_ids: Vec::new(),
                delivery_method_ids: Vec::new(),
                cutoff_from: None,
                cutoff_to: None,
                delivering_date_from: Some("2026-08-03".to_owned()),
                delivering_date_to: Some("2026-08-04".to_owned()),
            }),
        )
        .await
        .unwrap();

    server
        .reviews(
            RequestIdentity::dev(),
            Parameters(ReviewsInput {
                store: Some(StoreId::from("store_a")),
                limit: 100,
                last_id: String::new(),
                status: "ALL".to_owned(),
                skus: Vec::new(),
                order_status: "ALL".to_owned(),
                published_from: Some("2026-08-05".to_owned()),
                published_to: Some("2026-08-06".to_owned()),
                direction: SortDirection::Asc,
            }),
        )
        .await
        .unwrap();

    let request = requests.recv_timeout(Duration::from_secs(3)).unwrap();
    let (path, body) = request_path_and_body(&request);
    assert_eq!(path, "/v4/posting/fbs/unfulfilled/list");
    assert_eq!(
        body["filter"]["delivering_date_from"],
        json!("2026-08-03T00:00:00.000Z")
    );
    assert_eq!(
        body["filter"]["delivering_date_to"],
        json!("2026-08-04T23:59:59.999Z")
    );
    assert!(body["filter"].get("cutoff_from").is_none());

    let request = requests.recv_timeout(Duration::from_secs(3)).unwrap();
    let (path, body) = request_path_and_body(&request);
    assert_eq!(path, "/v2/review/list");
    assert_eq!(
        body["filters"]["published_from"],
        json!("2026-08-05T00:00:00.000Z")
    );
    assert_eq!(
        body["filters"]["published_to"],
        json!("2026-08-06T23:59:59.999Z")
    );
    assert!(requests.try_recv().is_err());
}
