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
