//! `MCP_TOOL_TEXT_CONTENT` through the production HTTP router.
//!
//! Drives a real `tools/call` over the streamable HTTP transport so the test
//! observes the bytes a client receives, not only the handler's return value.

use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header::CONTENT_TYPE},
};
use mcp_ozon::{
    config::RegistrySource,
    http::build_router,
    ozon::OzonClient,
    server::{OzonMcp, ToolTextContent},
};
use serde_json::{Value, json};
use tower::ServiceExt;

const PROTOCOL_VERSION: &str = "2025-06-18";
const BODY_LIMIT_BYTES: usize = 1_048_576;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn router(mode: ToolTextContent) -> Router {
    let id = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "mcp-ozon-tool-text-{}-{id}.json",
        std::process::id()
    ));
    std::fs::write(
        &path,
        json!({
            "version": 1,
            "actors": [{"id": "admin", "name": "Administrator", "role": "admin"}],
            "accounts": [],
        })
        .to_string(),
    )
    .expect("registry fixture is written");
    let client = OzonClient::new(
        "http://127.0.0.1:1".to_owned(),
        Duration::from_millis(100),
        BTreeMap::new(),
    )
    .expect("test Ozon client configuration must be valid");
    let registry = RegistrySource::new(&path).expect("registry fixture is valid");
    let server = OzonMcp::new(client, "admin".to_owned(), registry).with_tool_text_content(mode);
    build_router(server, NonZeroUsize::MIN)
}

fn post(session_id: Option<&str>, body: &Value) -> Request<Body> {
    let mut request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header(CONTENT_TYPE, "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .header("host", "localhost");
    if let Some(session_id) = session_id {
        request = request.header("mcp-session-id", session_id);
    }
    request
        .body(Body::from(body.to_string()))
        .expect("request builds")
}

fn sse_data(raw: &str) -> Value {
    let data = raw
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim_start)
        .find(|data| !data.is_empty())
        .unwrap_or_else(|| panic!("SSE response carries no data event: {raw}"));
    serde_json::from_str(data).expect("SSE data is JSON-RPC")
}

/// Returns the raw response body size and the JSON-RPC `result` of one call.
async fn call_stores_status(mode: ToolTextContent) -> (usize, Value) {
    let router = router(mode);
    let initialized = router
        .clone()
        .oneshot(post(
            None,
            &json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "tool-text-content-test", "version": "0"}
                }
            }),
        ))
        .await
        .expect("router responds");
    assert_eq!(initialized.status(), StatusCode::OK);
    let session_id = initialized
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .expect("initialize establishes a session")
        .to_owned();
    to_bytes(initialized.into_body(), BODY_LIMIT_BYTES)
        .await
        .expect("initialize stream completes");

    let notification = router
        .clone()
        .oneshot(post(
            Some(&session_id),
            &json!({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}),
        ))
        .await
        .expect("router responds");
    assert_eq!(notification.status(), StatusCode::ACCEPTED);

    let response = router
        .oneshot(post(
            Some(&session_id),
            &json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "ozon_stores_status", "arguments": {}}
            }),
        ))
        .await
        .expect("router responds");
    assert_eq!(response.status(), StatusCode::OK);
    let raw = tokio::time::timeout(
        Duration::from_secs(5),
        to_bytes(response.into_body(), BODY_LIMIT_BYTES),
    )
    .await
    .expect("tool response completes")
    .expect("tool response is readable");
    let raw = String::from_utf8(raw.to_vec()).expect("tool response is UTF-8");
    let mut message = sse_data(&raw);
    (raw.len(), message["result"].take())
}

fn text_block(result: &Value) -> &str {
    let content = result["content"].as_array().expect("content is an array");
    assert_eq!(content.len(), 1, "{result}");
    assert_eq!(content[0]["type"], "text");
    content[0]["text"]
        .as_str()
        .expect("text block carries text")
}

#[tokio::test]
async fn summary_mode_sends_structured_content_once() {
    let (json_bytes, json_result) = call_stores_status(ToolTextContent::Json).await;
    let (summary_bytes, summary_result) = call_stores_status(ToolTextContent::Summary).await;

    let structured = &json_result["structuredContent"];
    assert_eq!(structured["actor"]["id"], "admin", "{json_result}");
    let mirror = text_block(&json_result);
    assert_eq!(
        serde_json::from_str::<Value>(mirror).expect("mirror is JSON"),
        *structured
    );

    assert_eq!(summary_result["structuredContent"], *structured);
    let pointer = text_block(&summary_result);
    assert!(pointer.contains("structuredContent"), "{pointer}");
    assert!(pointer.len() < mirror.len());
    // The mirror is a JSON string inside JSON, so the wire saves at least its
    // raw length minus the pointer.
    assert!(
        json_bytes - summary_bytes >= mirror.len() - pointer.len(),
        "json={json_bytes} summary={summary_bytes} mirror={}",
        mirror.len()
    );
}
