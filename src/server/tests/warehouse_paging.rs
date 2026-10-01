use super::*;

#[tokio::test]
async fn warehouse_page_limit_rejects_oversized_calls_before_marketplace_io() {
    let (server, requests) = mock_server(0);
    for limit in [0, 101, 1000] {
        let input = WarehouseListInput {
            store: Some(StoreId::from("store_a")),
            limit,
            cursor: None,
            warehouse_ids: Vec::new(),
        };
        assert_validation_error(
            server
                .warehouses(RequestIdentity::dev(), Parameters(input))
                .await,
            "limit",
        );
    }
    assert!(requests.try_recv().is_err());
    let tool = server
        .tool_router
        .list_all()
        .into_iter()
        .find(|tool| tool.name == "ozon_warehouses")
        .unwrap();
    assert_eq!(tool.input_schema["properties"]["limit"]["maximum"], 100);
}

#[tokio::test]
async fn warehouse_default_page_preserves_cursor_and_filters() {
    let (server, requests) = mock_server(1);
    let input: WarehouseListInput = serde_json::from_value(json!({
        "store":"store_a", "cursor":"next-warehouse-page", "warehouse_ids":[101]
    }))
    .unwrap();
    assert_eq!(input.limit, 100);
    server
        .warehouses(RequestIdentity::dev(), Parameters(input))
        .await
        .unwrap();
    let request = requests.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(request.starts_with("POST /v2/warehouse/list "));
    let (_, body) = request.split_once("\r\n\r\n").unwrap();
    let payload: Value = serde_json::from_str(body).unwrap();
    assert_eq!(
        payload,
        json!({"limit":100,"cursor":"next-warehouse-page","warehouse_ids":[101]})
    );
    assert!(requests.try_recv().is_err());
}
