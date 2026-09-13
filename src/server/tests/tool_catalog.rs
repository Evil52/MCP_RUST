//! Catalog invariants shared by direct and prepared-data modes.

use super::*;

#[test]
fn all_tools_have_truthful_annotations_and_descriptions() {
    const INTERNAL_REPORTING_TOOLS: &[&str] = &[
        "ofk_collection_status",
        "ofk_wb_financial_ledger",
        "ofk_wb_report_reconciliation",
        "ofk_source_snapshot",
        "ofk_data_completeness",
        "ofk_manager_actions",
        "ofk_marketplace_sales_refresh_status",
        "ofk_metrics_history",
        "ofk_ozon_sales_analytics",
        "ofk_ozon_sales_refresh_status",
        "ofk_request_marketplace_sales_refresh",
        "ofk_request_ozon_sales_refresh",
        "ofk_reports",
        "ofk_tool_call_log",
        "ofk_weekly_marketplace_ranking",
    ];
    let tools = server()
        .with_preview_features(false, true)
        .tool_router
        .list_all();
    assert!(tools.len() >= 10);
    for tool in tools {
        assert!(!tool.description.as_deref().unwrap_or_default().is_empty());
        assert_eq!(
            tool.annotations
                .as_ref()
                .and_then(|annotations| annotations.read_only_hint),
            Some(!REPORT_REFRESH_WRITE_TOOLS.contains(&tool.name.as_ref())),
            "{} has an incorrect read-only annotation",
            tool.name
        );
        let annotations = tool.annotations.as_ref().unwrap();
        assert_eq!(annotations.destructive_hint, Some(false), "{}", tool.name);
        assert_eq!(annotations.idempotent_hint, Some(true), "{}", tool.name);
        assert_eq!(
            annotations.open_world_hint,
            Some(!INTERNAL_REPORTING_TOOLS.contains(&tool.name.as_ref())),
            "{}",
            tool.name
        );
        assert_eq!(
            tool.input_schema.get("additionalProperties"),
            Some(&Value::Bool(false)),
            "{} must reject unknown input fields",
            tool.name
        );
    }
}

#[test]
fn planned_read_tools_are_stable_and_legacy_finance_flag_is_a_noop() {
    const STABLE_TOOL_NAMES: &[&str] = &[
        "wb_reviews",
        "wb_review",
        "wb_questions",
        "wb_question",
        "wb_reviews_archive",
        "wb_return_claims",
        "wb_product_card_errors",
        "wb_product_card_limits",
        "wb_product_cards_trash",
        "wb_product_content_diagnostics",
        "wb_subject_characteristics",
        "wb_supplies",
        "wb_supply",
        "wb_supply_goods",
        "wb_supply_packages",
        "ozon_search_product_queries",
        "ozon_search_product_query_details",
        "wb_seller_warehouses",
        "wb_seller_warehouse_stocks",
        "ozon_stores_status",
        "marketplace_accounts",
        "list_members",
        "ofk_collection_status",
        "ofk_wb_financial_ledger",
        "ofk_wb_report_reconciliation",
        "ofk_source_snapshot",
        "ofk_data_completeness",
        "ofk_marketplace_sales_refresh_status",
        "ofk_metrics_history",
        "ofk_manager_actions",
        "ofk_ozon_sales_analytics",
        "ofk_ozon_sales_refresh_status",
        "ofk_request_marketplace_sales_refresh",
        "ofk_request_ozon_sales_refresh",
        "ofk_reports",
        "ofk_tool_call_log",
        "ofk_weekly_marketplace_ranking",
        "wb_stores_status",
        "wb_ping",
        "wb_sales_funnel",
        "wb_sales_funnel_history",
        "wb_sales_funnel_grouped_history",
        "wb_warehouse_stocks",
        "wb_orders",
        "wb_sales",
        "wb_product_cards",
        "wb_product_prices",
        "wb_tariff_commissions",
        "wb_tariff_boxes",
        "wb_tariff_pallets",
        "wb_tariff_returns",
        "wb_acceptance_coefficients",
        "wb_promotion_campaigns",
        "wb_promotion_campaign_details",
        "wb_promotion_stats",
        "wb_search_product_queries",
        "wb_search_orders_positions",
        "wb_promotion_minimum_bids",
        "wb_promotion_recommended_bids",
        "wb_promotion_search_cluster_bids",
        "ozon_analytics",
        "ozon_product_stocks",
        "ozon_warehouse_stocks",
        "ozon_fbo_stocks_by_warehouse",
        "ozon_fbs_stocks_by_warehouse",
        "ozon_warehouses",
        "ozon_product_prices",
        "ozon_live_buyer_prices",
        "ozon_products",
        "ozon_product_info",
        "ozon_product_pictures_info",
        "ozon_product_content_diagnostics",
        "ozon_product_attributes",
        "ozon_stock_turnover",
        "ozon_supply_order_list",
        "ozon_supply_order_get",
        "ozon_fbs_postings",
        "ozon_fbo_postings",
        "ozon_posting_sales_fallback",
        "ozon_fbs_unfulfilled",
        "ozon_fbo_posting",
        "ozon_fbs_posting",
        "ozon_fbo_cancel_reasons",
        "ozon_fbs_cancel_reasons",
        "ozon_returns",
        "ozon_rfbs_returns",
        "ozon_finance_transactions",
        "ozon_finance_totals",
        "ozon_finance_accrual_postings",
        "ozon_finance_accrual_types",
        "ozon_finance_accrual_by_day",
        "ozon_finance_realization_by_day",
        "ozon_finance_cash_flow",
        "ozon_finance_mutual_settlement",
        "ozon_performance_campaigns",
        "ozon_performance_limits",
        "ozon_performance_campaign_objects",
        "ozon_performance_campaign_products",
        "ozon_performance_daily",
        "ozon_performance_sku_statistics",
        "ozon_performance_expenses",
        "ozon_seller_rating",
        "ozon_seller_rating_history",
        "ozon_reviews",
        "ozon_questions",
    ];

    let names = |server: &OzonMcp| {
        server
            .tool_router
            .list_all()
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect::<BTreeSet<_>>()
    };
    let default_names = names(&server());
    let expected_names = STABLE_TOOL_NAMES
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(default_names, expected_names);

    let seed = server();
    let authenticator = jwt_authenticator(&seed.registry);
    let authenticated = OzonMcp::new_authenticated(seed.client, seed.registry, authenticator);
    assert_eq!(names(&authenticated), default_names);

    let legacy_flags = server()
        .with_preview_features(false, true)
        .with_preview_features(false, false);
    assert_eq!(names(&legacy_flags), default_names);
}

#[test]
fn every_tool_advertises_exact_security_policy_and_compatibility_mirror() {
    fn assert_policy(tools: Vec<rmcp::model::Tool>, expected: &Value) {
        for tool in tools {
            let serialized = serde_json::to_value(&tool).unwrap();
            assert_eq!(
                serialized.get("securitySchemes"),
                Some(expected),
                "{} canonical security policy differs",
                tool.name
            );
            assert_eq!(
                serialized.pointer("/_meta/securitySchemes"),
                Some(expected),
                "{} compatibility mirror differs",
                tool.name
            );
            assert!(serialized.get("security_schemes").is_none());
        }
    }

    let dev_tools = server().tool_router.list_all();
    // The release checklist in `SECURITY.md` states this count verbatim.
    // Changing it here without updating that gate leaves the gate
    // describing a router that no longer exists.
    assert_eq!(dev_tools.len(), 105);
    assert_policy(dev_tools, &json!([{"type": "noauth"}]));

    let seed = server();
    let authenticator = jwt_authenticator(&seed.registry);
    let authenticated = OzonMcp::new_authenticated(seed.client, seed.registry, authenticator);
    let metadata = authenticated.protected_resource_metadata().unwrap();
    assert_eq!(metadata.resource, "http://localhost:8788/mcp");
    assert_eq!(metadata.scopes_supported, vec!["mcp:tools"]);

    let jwt_tools = authenticated.tool_router.list_all();
    assert_eq!(jwt_tools.len(), 105);
    assert_policy(
        jwt_tools,
        &json!([{"type": "oauth2", "scopes": ["mcp:tools"]}]),
    );

    let seed = server();
    let authenticator = jwt_authenticator(&seed.registry);
    let legacy_flag_tools = OzonMcp::new_authenticated(seed.client, seed.registry, authenticator)
        .with_preview_features(false, true)
        .tool_router
        .list_all();
    assert_eq!(legacy_flag_tools.len(), 105);
    assert_policy(
        legacy_flag_tools,
        &json!([{"type": "oauth2", "scopes": ["mcp:tools"]}]),
    );
}

#[test]
fn ozon_network_endpoints_are_confined_to_explicit_read_only_allowlist() {
    const EXPECTED: &[&str] = &[
        "/v1/analytics/data",
        "/v1/analytics/product-queries",
        "/v1/analytics/product-queries/details",
        "/v1/analytics/turnover/stocks",
        "/v1/finance/accrual/by-day",
        "/v1/finance/accrual/postings",
        "/v1/finance/accrual/types",
        "/v1/finance/cash-flow-statement/list",
        "/v1/finance/mutual-settlement",
        "/v1/finance/realization/by-day",
        "/v1/posting/fbo/cancel-reason/list",
        "/v1/product/info/stocks-by-warehouse/fbo",
        "/v1/product/info/warehouse/stocks",
        "/v1/question/list",
        "/v1/rating/history",
        "/v1/rating/summary",
        "/v1/returns/list",
        "/v2/posting/fbo/get",
        "/v2/posting/fbs/cancel-reason/list",
        "/v2/product/info/stocks-by-warehouse/fbs",
        "/v2/product/pictures/info",
        "/v2/returns/rfbs/list",
        "/v2/review/list",
        "/v2/warehouse/list",
        "/v3/finance/transaction/list",
        "/v3/finance/transaction/totals",
        "/v3/posting/fbo/list",
        "/v3/posting/fbs/get",
        "/v3/product/info/list",
        "/v3/product/list",
        "/v3/supply-order/get",
        "/v3/supply-order/list",
        "/v4/posting/fbs/list",
        "/v4/posting/fbs/unfulfilled/list",
        "/v4/product/info/attributes",
        "/v4/product/info/stocks",
        "/v5/product/info/prices",
    ];
    assert_eq!(READ_ONLY_ENDPOINT_ALLOWLIST, EXPECTED);
    for endpoint in READ_ONLY_ENDPOINT_ALLOWLIST {
        for forbidden in ["/create", "/delete", "/import", "/set", "/ship", "/update"] {
            assert!(
                !endpoint.contains(forbidden),
                "{endpoint} contains {forbidden}"
            );
        }
        assert!(is_read_only_endpoint_allowed(endpoint));
    }
    assert!(PREVIEW_READ_ONLY_ENDPOINT_ALLOWLIST.is_empty());
    for endpoint in [
        "/v1/product/update",
        "/v1/order/create",
        "/v2/posting/fbs/ship",
        "/v2/posting/fbs/cancel",
    ] {
        assert!(!is_read_only_endpoint_allowed(endpoint));
    }
}
