use super::*;

fn tools() -> Vec<(&'static str, Value)> {
    vec![
        ("wb_fbs_new_orders", json!({})),
        ("wb_fbs_orders", json!({"date_from":100,"date_to":200})),
        ("wb_fbs_order_statuses", json!({"orders":[1,2]})),
        ("wb_promotion_balance", json!({})),
        ("wb_promotion_budget", json!({"advert_id":1})),
        (
            "wb_promotion_costs",
            json!({"from":"2026-08-01","to":"2026-08-02"}),
        ),
        (
            "wb_promotion_payments",
            json!({"from":"2026-08-01","to":"2026-08-02"}),
        ),
    ]
}

#[tokio::test]
async fn new_operational_tools_enforce_rbac_and_reject_unknown_arguments() {
    let (server, requests) = mock_wb_server_for("manager", 0);
    for (name, mut args) in tools() {
        args["account"] = json!("account_wb");
        assert!(
            call_tool_over_http(server.clone(), name, args)
                .await
                .contains(ACCESS_DENIED)
        );
    }
    assert!(requests.try_recv().is_err());
    let (server, requests) = mock_wb_server_for("admin", 0);
    for (name, mut args) in tools() {
        args["account"] = json!("account_wb");
        args["url"] = json!("https://example.invalid");
        let result: Value =
            serde_json::from_str(&call_tool_over_http(server.clone(), name, args).await).unwrap();
        assert!(
            result.get("error").is_some() || result["result"]["isError"] == true,
            "{name}: {result}"
        );
    }
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn fbs_status_tool_exposes_missing_ids_and_redacts_customer_data() {
    let (server, requests) = mock_wb_server_with_responses("admin", vec![
        (200,r#"{"orders":[{"id":1,"supplierStatus":"new","wbStatus":"waiting","address":{"fullAddress":"private-address"},"buyer_name":"private-name"}]}"#.into())]);
    let raw = call_tool_over_http(
        server,
        "wb_fbs_order_statuses",
        json!({"account":"account_wb","orders":[1,2]}),
    )
    .await;
    let result: Value = serde_json::from_str(&raw).unwrap();
    let data = &result["result"]["structuredContent"];
    assert_eq!(data["missing_order_ids"], json!([2]), "{raw}");
    assert_eq!(data["complete_for_requested_ids"], false);
    assert_eq!(data["account_id"], "account_wb");
    assert!(!raw.contains("private-address") && !raw.contains("private-name"));
    requests.recv().unwrap();
}

#[tokio::test]
async fn ozon_key_roles_are_read_only_scoped_and_use_empty_body() {
    let (seed, requests) = mock_server(0);
    let server = OzonMcp::new(seed.client, "manager".into(), seed.registry);
    assert!(
        call_tool_over_http(server, "ozon_api_key_roles", json!({"store":"store_a"}))
            .await
            .contains(ACCESS_DENIED)
    );
    assert!(requests.try_recv().is_err());
    let (server, requests) = mock_server_with_responses(vec![(
        200,
        r#"{"roles":[{"name":"ReadOnly","methods":["/v1/roles"]}]}"#.into(),
    )]);
    let raw = call_tool_over_http(server, "ozon_api_key_roles", json!({"store":"store_a"})).await;
    let result: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        result["result"]["structuredContent"]["endpoint"], "/v1/roles",
        "{raw}"
    );
    let request = requests.recv().unwrap();
    assert!(request.starts_with("POST /v1/roles "));
    assert_eq!(
        serde_json::from_str::<Value>(request.split("\r\n\r\n").nth(1).unwrap()).unwrap(),
        json!({})
    );
}

const ENDPOINT_LABELS: [(&str, &str); 7] = [
    ("wb_fbs_new_orders", "marketplace:/api/v3/orders/new"),
    ("wb_fbs_orders", "marketplace:/api/v3/orders"),
    ("wb_fbs_order_statuses", "marketplace:/api/v3/orders/status"),
    ("wb_promotion_balance", "promotion:/adv/v1/balance"),
    ("wb_promotion_budget", "promotion:/api/advert/v2/budget"),
    ("wb_promotion_costs", "promotion:/adv/v1/upd"),
    ("wb_promotion_payments", "promotion:/adv/v1/payments"),
];

fn structured(text: &str) -> Value {
    let envelope: Value = serde_json::from_str(text).unwrap();
    assert_ne!(envelope["result"]["isError"], true, "{text}");
    envelope["result"]["structuredContent"].clone()
}

#[tokio::test]
async fn operational_upstream_failures_name_the_wb_endpoint_that_failed() {
    for (name, mut args) in tools() {
        let (_, label) = ENDPOINT_LABELS
            .iter()
            .find(|(tool, _)| *tool == name)
            .unwrap();
        let (server, requests) =
            mock_wb_server_with_responses("admin", vec![(500, "{}".to_owned())]);
        args["account"] = json!("account_wb");
        let text = call_tool_over_http(server, name, args).await;
        let envelope: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(envelope["result"]["isError"], true, "{name}: {text}");
        assert!(text.contains(WB_TOOL_FAILURE), "{name}: {text}");
        assert!(
            text.contains(&format!("endpoint={label}")),
            "{name}: {text}"
        );
        requests.recv_timeout(Duration::from_secs(2)).unwrap();
    }
}

#[tokio::test]
async fn operational_reads_return_labelled_untrusted_wb_payloads() {
    let period = json!({"from":"2026-08-01","to":"2026-08-02"});
    let cases = [
        (
            "wb_fbs_new_orders",
            json!({}),
            json!({"orders":[{"id":5,"deliveryType":"fbs"}]}),
            "GET /api/v3/orders/new ",
        ),
        (
            "wb_promotion_balance",
            json!({}),
            json!({"balance":100,"net":5,"bonus":0}),
            "GET /adv/v1/balance ",
        ),
        (
            "wb_promotion_costs",
            period.clone(),
            json!([{"updNum":1,"updSum":10}]),
            "GET /adv/v1/upd?",
        ),
        (
            "wb_promotion_payments",
            period,
            json!([{"id":3,"sum":500,"type":0}]),
            "GET /adv/v1/payments?",
        ),
    ];
    for (name, mut args, body, request_line) in cases {
        let (_, label) = ENDPOINT_LABELS
            .iter()
            .find(|(tool, _)| *tool == name)
            .unwrap();
        let (server, requests) =
            mock_wb_server_with_responses("admin", vec![(200, body.to_string())]);
        args["account"] = json!("account_wb");
        let content = structured(&call_tool_over_http(server, name, args).await);
        assert_eq!(content["account_id"], "account_wb", "{name}");
        assert_eq!(content["endpoint"], *label, "{name}");
        assert_eq!(
            content["data_classification"],
            UNTRUSTED_DATA_CLASSIFICATION
        );
        assert_eq!(content["data"], body, "{name}");
        let request = requests.recv().unwrap();
        assert!(request.starts_with(request_line), "{name}: {request}");
    }
}

#[tokio::test]
async fn single_campaign_budget_returns_only_the_requested_ruble_row() {
    let row = json!({"advertId":7,"currency":"RUB","total":500});
    let (server, requests) =
        mock_wb_server_with_responses("admin", vec![(200, json!({"adverts":[row]}).to_string())]);
    let content = structured(
        &call_tool_over_http(
            server,
            "wb_promotion_budget",
            json!({"account":"account_wb","advert_id":7}),
        )
        .await,
    );
    assert_eq!(content["data"], row);
    let request = requests.recv().unwrap();
    assert!(request.starts_with("POST /api/advert/v2/budget "));
    let body: Value = serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(body, json!({"advertIds":[7]}));
}

#[tokio::test]
async fn fbs_order_pages_expose_the_upstream_cursor_until_an_empty_page() {
    let (server, requests) = mock_wb_server_with_responses(
        "admin",
        vec![(200, json!({"orders":[{"id":7}],"next":42}).to_string())],
    );
    let page = structured(
        &call_tool_over_http(
            server,
            "wb_fbs_orders",
            json!({"account":"account_wb","limit":1,"date_from":100,"date_to":200}),
        )
        .await,
    );
    assert_eq!(page["next_cursor"], 42);
    assert_eq!(page["page_is_last"], false);
    assert_eq!(
        (page["date_from"].clone(), page["date_to"].clone()),
        (json!(100), json!(200))
    );
    assert_eq!(page["endpoint"], "marketplace:/api/v3/orders");
    let request = requests.recv().unwrap();
    assert!(
        request.starts_with("GET /api/v3/orders?limit=1&next=0&dateFrom=100&dateTo=200 "),
        "{request}"
    );

    let (server, requests) = mock_wb_server_with_responses(
        "admin",
        vec![(200, json!({"orders":[],"next":42}).to_string())],
    );
    let last = structured(
        &call_tool_over_http(
            server,
            "wb_fbs_orders",
            json!({"account":"account_wb","next":42,"date_from":100,"date_to":200}),
        )
        .await,
    );
    assert_eq!(last["next_cursor"], Value::Null);
    assert_eq!(last["page_is_last"], true);
    assert!(requests.recv().unwrap().contains("next=42"));
}
