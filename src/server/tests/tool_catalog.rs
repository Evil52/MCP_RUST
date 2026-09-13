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
