//! Exercise the vendored schema cache through the public HTTP service.

use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{CallToolRequestParams, CallToolResponse, CallToolResult, JsonObject, Tool},
    service::RequestContext,
    transport::{
        StreamableHttpServerConfig, StreamableHttpService,
        streamable_http_server::session::local::LocalSessionManager,
    },
};
use serde_json::{Value, json};
use tower::ServiceExt;

#[derive(Default)]
struct Probe {
    lookups: AtomicUsize,
    calls: AtomicUsize,
    schemas: Mutex<Vec<Weak<JsonObject>>>,
}

impl Probe {
    fn retained_schemas(&self) -> usize {
        self.schemas
            .lock()
            .unwrap()
            .iter()
            .filter(|schema| schema.strong_count() > 0)
            .count()
    }
}

#[derive(Clone)]
struct Handler(Arc<Probe>);

impl ServerHandler for Handler {
    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.0.lookups.fetch_add(1, Ordering::Relaxed);
        if name.starts_with("unknown") {
            return None;
        }
        let mut schema = json!({
            "type": "object",
            "properties": {
                "account": {"type": "string", "x-mcp-header": "account"}
            }
        })
        .as_object()
        .unwrap()
        .clone();
        if name == "oversized_schema" {
            schema.insert("description".to_owned(), Value::String("a".repeat(65_536)));
        }
        let schema = Arc::new(schema);
        self.0.schemas.lock().unwrap().push(Arc::downgrade(&schema));
        Some(Tool::new(name.to_owned(), "cache probe", schema))
    }

    fn call_tool(
        &self,
        _request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CallToolResponse, ErrorData>> + Send + '_ {
        self.0.calls.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Ok(CallToolResult::success(Vec::new()).into()))
    }
}

type TestService = StreamableHttpService<Handler, LocalSessionManager>;

fn service() -> (TestService, Arc<Probe>) {
    let probe = Arc::new(Probe::default());
    let handler = Handler(Arc::clone(&probe));
    let service = StreamableHttpService::new(
        move || Ok(handler.clone()),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .with_json_response(true),
    );
    (service, probe)
}

async fn call(
    service: &TestService,
    name: &str,
    account_header: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", "2026-07-28");
    if let Some(account) = account_header {
        request = request
            .header("mcp-method", "tools/call")
            .header("mcp-name", name)
            .header("mcp-param-account", account);
    }
    let request = request
        .body(Body::from(
            json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {
                    "name": name,
                    "arguments": {"account": "allowed"},
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                        "io.modelcontextprotocol/clientCapabilities": {}
                    }
                }
            })
            .to_string(),
        ))
        .unwrap();
    let response = service.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(Body::new(response.into_body()), 4096)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn assert_param_mismatch(service: &TestService, name: &str) {
    let (status, body) = call(service, name, Some("different")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("account")
    );
}

#[tokio::test]
async fn rejected_unknown_names_are_not_retained_and_known_tools_remain_cached() {
    let (service, probe) = service();
    assert_param_mismatch(&service, "known").await;
    assert_eq!(probe.lookups.load(Ordering::Relaxed), 1);

    // Each batch supplied over 6 MB of distinct, request-controlled tool names
    // to the previous negative cache, even though every request was rejected.
    for _ in 0..2 {
        for index in 0..32 {
            let name = format!("unknown_{index}_{}", "a".repeat(200_000));
            let (status, _) = call(&service, &name, None).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
        }
    }
    assert_eq!(
        probe.lookups.load(Ordering::Relaxed),
        65,
        "repeated unknown names must still be looked up, never negatively cached"
    );
    assert_eq!(probe.retained_schemas(), 1);
    assert_eq!(
        call(&service, "known", Some("allowed")).await.0,
        StatusCode::OK
    );
    assert_eq!(probe.lookups.load(Ordering::Relaxed), 65);
    assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_dynamic_tool_definitions_cannot_exceed_the_cache_capacity() {
    let (service, probe) = service();
    assert_param_mismatch(&service, "known").await;
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..300 {
        let service = service.clone();
        tasks.spawn(async move {
            assert_param_mismatch(&service, &format!("dynamic_{index}")).await;
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    assert_eq!(probe.retained_schemas(), 256);
    for _ in 0..2 {
        assert_param_mismatch(&service, "after_capacity").await;
    }
    assert_eq!(probe.retained_schemas(), 256);
    assert_eq!(probe.lookups.load(Ordering::Relaxed), 303);
    assert_eq!(
        call(&service, "known", Some("allowed")).await.0,
        StatusCode::OK
    );
    assert_eq!(probe.lookups.load(Ordering::Relaxed), 303);
    assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn oversized_names_and_schemas_bypass_retention_but_still_validate_headers() {
    let (service, probe) = service();
    for name in ["a".repeat(129), "oversized_schema".to_owned()] {
        assert_param_mismatch(&service, &name).await;
        assert_eq!(probe.retained_schemas(), 0);
        assert_eq!(
            call(&service, &name, Some("allowed")).await.0,
            StatusCode::OK
        );
        assert_eq!(probe.retained_schemas(), 0);
    }
    assert_eq!(probe.lookups.load(Ordering::Relaxed), 4);
    assert_eq!(probe.calls.load(Ordering::Relaxed), 2);
}
