use std::collections::BTreeMap;

use rmcp::{
    ErrorData,
    handler::server::{tool::IntoCallToolResult, wrapper::Json as RmcpJson},
    schemars::JsonSchema,
};
use serde_json::json;

use super::*;
use crate::{
    config::StoreId,
    reporting::{
        mcp_read::DataQuality,
        snapshot::{Marketplace, SnapshotSource},
    },
};

fn catalog(items: u64) -> Value {
    json!({
        "items": (0..items)
            .map(|sku| json!({"offer_id": format!("OЛ{sku:09}"), "price": "1214.00",
                "stocks": {"present": sku % 7, "reserved": 0}, "customer_phone": "+7999"}))
            .collect::<Vec<_>>(),
        "total": items,
    })
}

fn ozon(data: Value) -> OzonResult {
    OzonResult {
        store: StoreId::from("store_a"),
        endpoint: "/v3/product/info/list",
        fetched_at: "2026-09-27T10:00:00+00:00".to_owned(),
        data_classification: "untrusted_external_marketplace_data",
        data,
    }
}

fn wb(data: Value) -> WbResult {
    WbResult {
        account_id: "wb_a".to_owned(),
        endpoint: "/api/v3/stocks",
        fetched_at: "2026-09-27T10:00:00+00:00".to_owned(),
        data_classification: "untrusted_external_marketplace_data",
        data,
    }
}

fn wb_stocks(data: Value) -> WbSellerWarehouseStocksResult {
    WbSellerWarehouseStocksResult {
        source: wb(data),
        warehouse_id: 7,
        inventory_scope: "seller",
        observation_kind: "current",
        requested_chrt_ids: vec![1, 2],
        missing_chrt_ids: vec![2],
        complete_for_requested_ids: false,
    }
}

fn snapshot(latest_collection: Option<Value>) -> SourceSnapshotResult {
    SourceSnapshotResult {
        account_id: "ozon_a".to_owned(),
        marketplace: Marketplace::Ozon.into(),
        source: SnapshotSource::Stocks,
        storage: "postgres".to_owned(),
        state: "published".to_owned(),
        data_state: "complete".to_owned(),
        catalog_scope: None,
        quality: Some(DataQuality::Complete),
        inventory_scope: Some("fbo".to_owned()),
        pagination_complete: Some(true),
        coverage: None,
        snapshot_id: Some("42".to_owned()),
        cutoff_at: None,
        source_as_of: Some("2026-09-27T09:00:00Z".to_owned()),
        observed_from: None,
        period_start: None,
        period_end: None,
        total_rows: 3,
        rows: (0..3)
            .map(|sku| json!({"sku": sku, "units": sku * 2}))
            .collect(),
        next_offset: None,
        latest_collection,
    }
}

#[derive(Serialize, JsonSchema)]
struct Extra {
    flattened: &'static str,
}

/// Exercises the copy path with the serde features the real results use.
#[derive(Serialize, JsonSchema)]
struct Typed {
    name: String,
    price: f64,
    missing: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    skipped: Option<u64>,
    by_sku: BTreeMap<u64, u32>,
    #[serde(flatten)]
    extra: Extra,
    tags: Vec<&'static str>,
}

fn typed() -> Typed {
    Typed {
        name: "Футболка \"оверсайз\"".to_owned(),
        price: 1214.1,
        missing: None,
        skipped: None,
        by_sku: BTreeMap::from([(10, 1), (2, 3)]),
        extra: Extra { flattened: "yes" },
        tags: vec!["a", "b"],
    }
}

fn complete(response: Result<CallToolResponse, ErrorData>) -> CallToolResult {
    match response {
        Ok(CallToolResponse::Complete(result)) => result,
        other => panic!("expected a complete tool result, got {other:?}"),
    }
}

fn ours<T: Serialize + 'static>(value: T) -> CallToolResult {
    complete(Json(value).into_call_tool_result())
}

fn theirs<T: Serialize + JsonSchema + 'static>(value: T) -> CallToolResult {
    complete(RmcpJson(value).into_call_tool_result())
}

fn text(result: &CallToolResult) -> &str {
    let [ContentBlock::Text(text)] = result.content.as_slice() else {
        panic!("a structured result carries exactly one text block");
    };
    &text.text
}

#[test]
fn mode_parses_only_the_documented_values() {
    assert_eq!(
        "json".parse::<ToolTextContent>().unwrap(),
        ToolTextContent::Json
    );
    assert_eq!(
        "summary".parse::<ToolTextContent>().unwrap(),
        ToolTextContent::Summary
    );
    for rejected in ["", "JSON", "none", " summary"] {
        assert!(rejected.parse::<ToolTextContent>().is_err(), "{rejected:?}");
    }
    assert_eq!(ToolTextContent::default(), ToolTextContent::Json);
    assert_eq!(ToolTextContent::current(), ToolTextContent::Json);
}

#[test]
fn json_mode_matches_rmcp_json_for_moved_and_copied_results() {
    assert_eq!(ours(ozon(catalog(40))), theirs(ozon(catalog(40))));
    assert_eq!(ours(wb(catalog(40))), theirs(wb(catalog(40))));
    assert_eq!(ours(wb_stocks(catalog(40))), theirs(wb_stocks(catalog(40))));
    for latest_collection in [None, Some(json!({"state": "running"}))] {
        assert_eq!(
            ours(snapshot(latest_collection.clone())),
            theirs(snapshot(latest_collection))
        );
    }
    assert_eq!(ours(typed()), theirs(typed()));
    assert_eq!(ours(json!({"ok": true})), theirs(json!({"ok": true})));
}

#[tokio::test]
async fn summary_mode_sends_a_pointer_with_the_same_structured_content() {
    let result = ToolTextContent::Summary
        .scope(async { ours(ozon(catalog(40))) })
        .await;
    let mut expected = theirs(ozon(catalog(40)));
    assert!(text(&expected).len() > 10 * STRUCTURED_CONTENT_POINTER.len());
    expected.content = vec![ContentBlock::text(STRUCTURED_CONTENT_POINTER)];
    assert_eq!(result, expected);

    let outside = ours(ozon(catalog(40)));
    assert_ne!(text(&outside), STRUCTURED_CONTENT_POINTER);
}

#[tokio::test]
async fn summary_mode_keeps_a_mirror_shorter_than_the_pointer() {
    let result = ToolTextContent::Summary
        .scope(async { ours(json!({"ok": true})) })
        .await;
    assert_eq!(text(&result), r#"{"ok":true}"#);
}

#[tokio::test]
async fn limits_match_rmcp_json() {
    fn outcome(response: Result<CallToolResponse, ErrorData>) -> Result<(), ErrorData> {
        response.map(|_| ())
    }
    // The structured cap accepts a payload exactly at the limit.
    for length in [
        MAX_STRUCTURED_CONTENT_BYTES - 2,
        MAX_STRUCTURED_CONTENT_BYTES - 1,
    ] {
        let payload = "a".repeat(length);
        assert_eq!(
            outcome(Json(payload.clone()).into_call_tool_result()),
            outcome(RmcpJson(payload).into_call_tool_result()),
            "length={length}"
        );
    }
    // Escaped quotes keep the structured content under its cap while the
    // doubly escaped mirror pushes the whole result past its own.
    let quotes = "\"".repeat((MAX_STRUCTURED_CONTENT_BYTES - 2) / 2);
    let expected = outcome(RmcpJson(quotes.clone()).into_call_tool_result());
    assert_eq!(
        expected,
        Err(ErrorData::internal_error(
            "Structured tool result exceeds the response size limit",
            None
        ))
    );
    assert_eq!(
        outcome(Json(quotes.clone()).into_call_tool_result()),
        expected
    );
    // Without the mirror the same payload fits.
    let summary = ToolTextContent::Summary
        .scope(async { outcome(Json(quotes).into_call_tool_result()) })
        .await;
    assert_eq!(summary, Ok(()));
}
