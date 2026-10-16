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
                warehouse_ids: vec!["99".into()],
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
            json!({"skus":["11"],"warehouse_ids":["99"],"cluster_ids":["4007"]}),
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
