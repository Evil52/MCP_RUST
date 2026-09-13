//! Catalog invariants shared by direct and prepared-data modes.

use super::*;

#[test]
fn all_tools_have_truthful_annotations_and_descriptions() {
    const INTERNAL_REPORTING_TOOLS: &[&str] = &[
        "ofk_collection_status",
        "ofk_wb_financial_ledger",
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
