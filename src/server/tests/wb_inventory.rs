use super::*;

fn inventory_input(warehouse_id: u64, chrt_ids: Vec<u64>) -> WbSellerWarehouseStocksInput {
    WbSellerWarehouseStocksInput {
        account: Some("account_wb".to_owned()),
        warehouse_id,
        chrt_ids,
    }
}

#[test]
fn wb_inventory_schema_separates_fbw_seller_stock_and_historical_dates() {
    let tools = server().tool_router.list_all();
    for name in [
        "wb_warehouse_stocks",
        "wb_seller_warehouses",
        "wb_seller_warehouse_stocks",
    ] {
        let tool = tools.iter().find(|tool| tool.name == name).unwrap();
        assert_eq!(
            tool.annotations.as_ref().unwrap().read_only_hint,
            Some(true)
        );
        assert!(tool.description.as_deref().unwrap().contains("FBS"));
    }
    let fbw = tools
        .iter()
        .find(|tool| tool.name == "wb_warehouse_stocks")
        .unwrap();
    assert!(fbw.description.as_deref().unwrap().contains("FBW"));
    let seller = tools
        .iter()
        .find(|tool| tool.name == "wb_seller_warehouse_stocks")
        .unwrap();
    let input = serde_json::to_value(&seller.input_schema).unwrap();
    assert_eq!(input["additionalProperties"], false);
    assert_eq!(input["properties"]["chrt_ids"]["minItems"], 1);
    assert_eq!(input["properties"]["chrt_ids"]["maxItems"], 1_000);
    assert_eq!(input["properties"]["warehouse_id"]["minimum"], 1);
    assert!(input["properties"].get("date_from").is_none());
    assert!(input["properties"].get("offset").is_none());
    for extra in ["date_from", "date", "as_of", "offset", "skus", "nm_ids"] {
        let mut value = json!({"warehouse_id":1,"chrt_ids":[1]});
        value[extra] = json!("2026-09-07");
        assert!(
            serde_json::from_value::<WbSellerWarehouseStocksInput>(value).is_err(),
            "{extra}"
        );
    }
}

#[tokio::test]
async fn wb_seller_stock_preserves_real_zeroes_and_marks_missing_ids() {
    let (server, requests) = mock_wb_server_with_responses("admin", vec![
        (200, r#"[{"id":12,"deliveryType":1},{"id":13,"deliveryType":2}]"#.to_owned()),
        (200, r#"{"stocks":[{"chrtId":101,"amount":0},{"chrtId":102,"amount":9}],"account_id":"forged","buyer_name":"private"}"#.to_owned()),
        (200, r#"{"stocks":[]}"#.to_owned()),
        (200, r#"{"stocks":[{"chrtId":101,"amount":5}]}"#.to_owned()),
    ]);
    let warehouses = server
        .wb_seller_warehouses(
            RequestIdentity::dev(),
            Parameters(WbAccountInput { account: None }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(warehouses.endpoint, "marketplace:/api/v3/warehouses");
    assert_eq!(warehouses.data[1]["deliveryType"], 2);
    let stocks = server
        .wb_seller_warehouse_stocks(
            RequestIdentity::dev(),
            Parameters(inventory_input(12, vec![101, 102, 103])),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(stocks.source.account_id, "account_wb");
    assert_eq!(stocks.warehouse_id, 12);
    assert_eq!(stocks.inventory_scope, "seller_warehouse");
    assert_eq!(stocks.observation_kind, "current");
    assert_eq!(
        stocks.source.data_classification,
        UNTRUSTED_DATA_CLASSIFICATION
    );
    assert!(chrono::DateTime::parse_from_rfc3339(&stocks.source.fetched_at).is_ok());
    assert_eq!(stocks.requested_chrt_ids, [101, 102, 103]);
    assert_eq!(stocks.missing_chrt_ids, [103]);
    assert!(!stocks.complete_for_requested_ids);
    assert_eq!(stocks.source.data["stocks"][0]["amount"], 0);
    assert_eq!(stocks.source.data["stocks"].as_array().unwrap().len(), 2);
    assert_eq!(stocks.source.data["buyer_name"], REDACTED_VALUE);
    let serialized = serde_json::to_value(&stocks).unwrap();
    assert_eq!(serialized["account_id"], "account_wb");
    assert!(serialized.get("fetched_at").is_some());
    let empty = server
        .wb_seller_warehouse_stocks(
            RequestIdentity::dev(),
            Parameters(inventory_input(13, vec![101])),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(empty.missing_chrt_ids, [101]);
    assert!(!empty.complete_for_requested_ids);
    assert_eq!(empty.source.data["stocks"], json!([]));
    let full = server
        .wb_seller_warehouse_stocks(
            RequestIdentity::dev(),
            Parameters(inventory_input(12, vec![101])),
        )
        .await
        .unwrap()
        .0;
    assert!(full.complete_for_requested_ids);
    assert!(full.missing_chrt_ids.is_empty());
    for path in [
        "GET /api/v3/warehouses ",
        "POST /api/v3/stocks/12 ",
        "POST /api/v3/stocks/13 ",
        "POST /api/v3/stocks/12 ",
    ] {
        assert!(
            requests
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .starts_with(path)
        );
    }
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn wb_seller_stock_handlers_reject_bad_input_and_inaccessible_accounts_before_network() {
    let (server, requests) = mock_wb_server_for("admin", 0);
    for (warehouse, ids) in [
        (0, vec![1]),
        (u64::MAX, vec![1]),
        (1, vec![]),
        (1, vec![0]),
        (1, vec![u64::MAX]),
        (1, vec![1, 1]),
        (1, (1..=1_001).collect()),
    ] {
        let error = server
            .wb_seller_warehouse_stocks(
                RequestIdentity::dev(),
                Parameters(inventory_input(warehouse, ids)),
            )
            .await
            .err()
            .expect("request must fail");
        assert!(
            !error.contains(WB_TOOL_FAILURE),
            "invalid input must fail locally"
        );
    }
    assert!(requests.try_recv().is_err());
    for actor in ["manager", "admin"] {
        let (server, requests) = mock_wb_server_for(actor, 0);
        let account = if actor == "manager" {
            "account_wb"
        } else {
            "unknown-wb"
        };
        let expected = if actor == "manager" {
            ACCESS_DENIED
        } else {
            "UNKNOWN_WB_ACCOUNT"
        };
        let warehouses = server
            .wb_seller_warehouses(
                RequestIdentity::dev(),
                Parameters(WbAccountInput {
                    account: Some(account.to_owned()),
                }),
            )
            .await
            .err()
            .expect("request must fail");
        assert!(warehouses.contains(expected));
        let mut input = inventory_input(1, vec![1]);
        input.account = Some(account.to_owned());
        let stocks = server
            .wb_seller_warehouse_stocks(RequestIdentity::dev(), Parameters(input))
            .await
            .err()
            .expect("request must fail");
        assert!(stocks.contains(expected));
        assert!(requests.try_recv().is_err());
    }
}

#[tokio::test]
async fn wb_seller_inventory_is_callable_over_mcp_and_propagates_upstream_failures() {
    let (server, requests) = mock_wb_server_with_responses(
        "admin",
        vec![
            (200, "[]".to_owned()),
            (200, r#"{"stocks":[{"chrtId":1,"amount":2}]}"#.to_owned()),
            (403, "{}".to_owned()),
            (429, "{}".to_owned()),
        ],
    );
    for (tool, args) in [
        ("wb_seller_warehouses", json!({"account":"account_wb"})),
        (
            "wb_seller_warehouse_stocks",
            json!({"account":"account_wb","warehouse_id":12,"chrt_ids":[1]}),
        ),
    ] {
        let text = call_tool_over_http(server.clone(), tool, args).await;
        let envelope: Value = serde_json::from_str(&text).unwrap();
        assert_ne!(envelope["result"]["isError"], true);
        let content: Value =
            serde_json::from_str(envelope["result"]["content"][0]["text"].as_str().unwrap())
                .unwrap();
        assert_eq!(content["account_id"], "account_wb");
    }
    let forbidden = server
        .wb_seller_warehouses(
            RequestIdentity::dev(),
            Parameters(WbAccountInput { account: None }),
        )
        .await
        .err()
        .expect("request must fail");
    assert!(forbidden.contains("kind=forbidden"));
    let limited = server
        .wb_seller_warehouse_stocks(
            RequestIdentity::dev(),
            Parameters(inventory_input(12, vec![1])),
        )
        .await
        .err()
        .expect("request must fail");
    assert!(limited.contains("kind=rate_limited"));
    for _ in 0..4 {
        requests.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn wb_seller_stock_rejects_malformed_and_unrequested_rows() {
    for body in [
        json!({}),
        json!({"stocks":null}),
        json!({"stocks":[{}]}),
        json!({"stocks":[{"chrtId":1}]}),
        json!({"stocks":[{"chrtId":1,"amount":-1}]}),
        json!({"stocks":[{"chrtId":1,"amount":"0"}]}),
        json!({"stocks":[{"chrtId":1,"amount":0.5}]}),
        json!({"stocks":[{"chrtId":2,"amount":7}]}),
        json!({"stocks":[{"chrtId":1,"amount":7},{"chrtId":1,"amount":7}]}),
    ] {
        let (server, requests) =
            mock_wb_server_with_responses("admin", vec![(200, body.to_string())]);
        let error = server
            .wb_seller_warehouse_stocks(
                RequestIdentity::dev(),
                Parameters(inventory_input(1, vec![1])),
            )
            .await
            .err()
            .expect("request must fail");
        assert!(error.contains("WB_STOCKS_INVALID_RESPONSE"), "{body}");
        requests.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(requests.try_recv().is_err());
    }
}
