use super::*;

#[tokio::test]
async fn control_http_wire_lists_exact_inventory_and_propagates_request_identity() {
    let fixtures = Fixtures::new(true);
    let registry = RegistrySource::new(&fixtures.registry_path).unwrap();
    let snapshot = registry.load().unwrap();
    let policy = ControlPolicy::load(&fixtures.policy_path, &snapshot).unwrap();
    let server = ControlMcp::new_disabled("revoked".to_owned(), registry, policy);
    let router = control_router(server)
        .layer(Extension(AuthenticatedActor {
            actor_id: "admin".to_owned(),
        }))
        .layer(Extension(snapshot));
    let session_id = initialize(&router).await;

    let (status, headers, body) = rpc(
        &router,
        Some(&session_id),
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let response = rpc_json(&headers, &body);
    let mut names = response
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "ozon_ads_control_scope",
            "ozon_ads_control_status",
            "ozon_performance_apply_campaign_launch",
            "ozon_performance_approve_campaign_launch",
            "ozon_performance_prepare_campaign_launch",
            "ozon_performance_preview_campaign_launch",
            "ozon_performance_reconcile_campaign_launch",
            "wb_promotion_apply_bid_plan",
            "wb_promotion_approve_bid_plan",
            "wb_promotion_bid_plan_status",
            "wb_promotion_campaign_preflight",
            "wb_promotion_create_campaign",
            "wb_promotion_export_campaign_robot",
            "wb_promotion_find_campaign",
            "wb_promotion_fund_campaign",
            "wb_promotion_prepare_bid_update",
            "wb_promotion_prepare_campaign",
            "wb_promotion_reconcile_bid_plan",
            "wb_promotion_reconcile_campaign",
            "wb_promotion_set_initial_campaign_bids",
            "wb_promotion_start_campaign",
        ]
    );

    let ozon_spec = json!({
        "account_id": "ozon_one",
        "title": "Wire contract test",
        "from_date": "2026-09-02",
        "to_date": "2026-09-08",
        "skus": [1001],
        "weekly_budget_microrubles": 2_000_000_000_u64,
        "per_sku_spend_cap_microrubles": 2_000_000_000_u64,
        "initial_cpc_bid_microrubles": 7_000_000_u64,
        "max_cpc_bid_microrubles": 12_000_000_u64,
        "target_drr_percent": 15,
        "target_position": 30
    });
    let digest = "a".repeat(64);
    for (id, name, arguments, expect_error) in [
        (10, "ozon_ads_control_scope", json!({}), false),
        (
            11,
            "ozon_performance_preview_campaign_launch",
            json!({"spec": ozon_spec.clone()}),
            true,
        ),
        (
            12,
            "ozon_performance_prepare_campaign_launch",
            json!({"spec": ozon_spec}),
            true,
        ),
        (
            13,
            "ozon_performance_approve_campaign_launch",
            json!({
                "plan_id": digest,
                "plan_digest": digest,
                "approval_reference": "wire_test"
            }),
            true,
        ),
        (
            14,
            "ozon_performance_apply_campaign_launch",
            json!({"plan_id": digest, "plan_digest": digest}),
            true,
        ),
        (
            15,
            "ozon_performance_reconcile_campaign_launch",
            json!({"plan_id": digest}),
            true,
        ),
        (
            16,
            "wb_promotion_prepare_bid_update",
            json!({"account_id": "wb_one", "advert_id": 77, "changes": []}),
            true,
        ),
        (
            17,
            "wb_promotion_approve_bid_plan",
            json!({
                "plan_id": digest,
                "plan_digest": digest,
                "approval_reference": "wire_test"
            }),
            true,
        ),
        (
            18,
            "wb_promotion_apply_bid_plan",
            json!({"plan_id": digest, "plan_digest": digest}),
            true,
        ),
        (
            19,
            "wb_promotion_bid_plan_status",
            json!({"plan_id": digest}),
            true,
        ),
        (
            20,
            "wb_promotion_reconcile_bid_plan",
            json!({"plan_id": digest}),
            true,
        ),
        (
            21,
            "wb_promotion_prepare_campaign",
            json!({"account_id":"wb_one","campaign_name":"Wire test",
                "bids_kopecks":{"1001":700},"budget_rubles":0,
                "authorization_reference":"wire_test",
                "expires_at":"2026-09-26T00:00:00Z",
                "robot_authorization_expires_at":"2026-09-27T00:00:00Z"}),
            true,
        ),
        (
            22,
            "wb_promotion_create_campaign",
            json!({"account_id":"wb_one","campaign_handle":"a".repeat(64)}),
            true,
        ),
    ] {
        let (status, headers, body) = rpc(
            &router,
            Some(&session_id),
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{name}: {body}");
        let response = rpc_json(&headers, &body);
        assert_eq!(
            response.pointer("/result/isError").and_then(Value::as_bool),
            Some(expect_error),
            "{name}: {response}"
        );
    }

    // Prove that the exact registry snapshot attached to this HTTP request
    // reaches the tool context; a fallback reload can no longer succeed.
    fs::remove_file(&fixtures.registry_path).unwrap();
    let (status, headers, body) = rpc(
        &router,
        Some(&session_id),
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "ozon_ads_control_status", "arguments": {}}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let response = rpc_json(&headers, &body);
    let text = response
        .pointer("/result/content/0/text")
        .and_then(Value::as_str)
        .unwrap();
    let result: Value = serde_json::from_str(text).unwrap();
    assert_eq!(result["actor_id"], "admin");
    assert_eq!(result["write_executor_configured"], false);
    assert_eq!(result["runtime_gates_required"], true);
}
