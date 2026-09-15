use super::*;

#[tokio::test]
async fn seller_report_tool_preserves_identity_privacy_and_page_scope() {
    let (server, requests) = mock_wb_server_with_responses("admin", vec![
        (200, r#"{"data":{"items":[{"nmId":1,"chrtId":2,"warehouseId":3,"quantity":0}]},"buyer_name":"private"}"#.to_owned()),
        (200, r#"{"data":{"items":[]}}"#.to_owned()),
    ]);
    let input = |offset| WbWarehouseStocksInput {
        account: Some("account_wb".to_owned()),
        nm_ids: vec![],
        chrt_ids: vec![],
        limit: 1,
        offset,
    };
    let page = server
        .wb_seller_warehouses_stock_report(RequestIdentity::dev(), Parameters(input(0)))
        .await
        .unwrap()
        .0;
    assert_eq!(page.source.account_id, "account_wb");
    assert_eq!(
        page.source.data_classification,
        UNTRUSTED_DATA_CLASSIFICATION
    );
    assert_eq!(page.source.data["buyer_name"], REDACTED_VALUE);
    assert_eq!(page.source.data["data"]["items"][0]["quantity"], 0);
    assert_eq!(page.inventory_scope, "seller");
    assert_eq!(page.observation_kind, "current");
    assert_eq!(page.upstream_refresh_interval_seconds, 1_800);
    assert_eq!(page.next_offset, Some(1));
    assert!(!page.page_is_last);
    assert!(!page.missing_rows_mean_zero);
    let end = server
        .wb_seller_warehouses_stock_report(RequestIdentity::dev(), Parameters(input(1)))
        .await
        .unwrap()
        .0;
    assert_eq!(end.returned_rows, 0);
    assert_eq!(end.next_offset, None);
    assert!(end.page_is_last);
    assert!(!end.missing_rows_mean_zero);
    for _ in 0..2 {
        requests.recv_timeout(Duration::from_secs(1)).unwrap();
    }
}

#[tokio::test]
async fn seller_report_denies_foreign_account_before_network() {
    let input = WbWarehouseStocksInput {
        account: Some("account_wb".to_owned()),
        nm_ids: vec![],
        chrt_ids: vec![],
        limit: 1,
        offset: 0,
    };
    let (server, requests) = mock_wb_server_for("manager", 0);
    let error = server
        .wb_seller_warehouses_stock_report(RequestIdentity::dev(), Parameters(input))
        .await
        .err()
        .expect("foreign account must be denied");
    assert!(error.contains("ACCESS_DENIED"), "{error}");
    assert!(requests.try_recv().is_err());
}

#[test]
fn seller_report_schema_exposes_bounded_read_without_historical_or_warehouse_filters() {
    let tools = server().tool_router.list_all();
    let tool = tools
        .iter()
        .find(|tool| tool.name == "wb_seller_warehouses_stock_report")
        .unwrap();
    assert_eq!(
        tool.annotations.as_ref().unwrap().read_only_hint,
        Some(true)
    );
    let schema = serde_json::to_value(&tool.input_schema).unwrap();
    assert_eq!(schema["properties"]["limit"]["maximum"], 1_000);
    assert_eq!(schema["additionalProperties"], false);
    for field in ["date_from", "date", "warehouse_id", "skus"] {
        assert!(schema["properties"].get(field).is_none());
    }
}
