//! Integration tests for the production HTTP router.
//!
//! These drive `mcp_ozon::http::build_router` — the same function `main.rs`
//! calls — rather than a re-implementation, so the routes that exist only in
//! the binary cannot silently disappear or drift from what is served.

use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header::CONTENT_TYPE},
};
use mcp_ozon::{
    auth::JwtAuthenticator,
    config::{JwtConfig, RegistrySource},
    http::build_router,
    ozon::OzonClient,
    server::OzonMcp,
};
use serde_json::{Value, json};
use tower::ServiceExt;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

const RESOURCE_URL: &str = "http://localhost:8788/mcp";
const RESOURCE_METADATA_URL: &str = "http://localhost:8788/.well-known/oauth-protected-resource";
const ISSUER: &str = "http://issuer.test/realms/ofk";

fn registry() -> RegistrySource {
    let id = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("mcp-ozon-router-{}-{id}.json", std::process::id()));
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&json!({
            "version": 1,
            "actors": [{
                "id": "admin",
                "name": "Administrator",
                "role": "admin",
                "oidc": {"username": "admin"}
            }],
            "accounts": [],
        }))
        .expect("registry fixture serializes"),
    )
    .expect("registry fixture is written");
    RegistrySource::new(path).expect("registry fixture is valid")
}

fn ozon_client() -> OzonClient {
    OzonClient::new(
        "http://127.0.0.1:1".to_owned(),
        Duration::from_millis(100),
        BTreeMap::new(),
    )
    .expect("test Ozon client configuration must be valid")
}

fn jwt_config() -> JwtConfig {
    JwtConfig {
        issuer: ISSUER.to_owned(),
        audience: RESOURCE_URL.to_owned(),
        jwks_url: "http://127.0.0.1:1/certs".to_owned(),
        resource_url: RESOURCE_URL.to_owned(),
        resource_metadata_url: RESOURCE_METADATA_URL.to_owned(),
        required_scopes: vec!["mcp:tools".to_owned()],
        jwks_cache_ttl: Duration::from_secs(300),
    }
}

fn dev_router() -> Router {
    let server = OzonMcp::new(ozon_client(), "admin".to_owned(), registry());
    build_router(server, NonZeroUsize::new(4).expect("non-zero"))
}

fn jwt_router() -> Router {
    let registry = registry();
    let authenticator = JwtAuthenticator::new(jwt_config(), registry.clone())
        .expect("test authenticator configuration must be valid");
    let server = OzonMcp::new_authenticated(ozon_client(), registry, authenticator);
    build_router(server, NonZeroUsize::new(4).expect("non-zero"))
}

async fn get(router: Router, uri: &str) -> (StatusCode, Option<String>, String) {
    let response = router
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("router responds");
    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = axum::body::to_bytes(response.into_body(), 1_048_576)
        .await
        .expect("response body is readable");
    (
        status,
        content_type,
        String::from_utf8_lossy(&body).into_owned(),
    )
}

#[tokio::test]
async fn health_probe_is_served_without_authentication_in_both_modes() {
    // The container healthcheck depends on this route, and it exists only in
    // the router, so nothing else in the suite would notice it disappearing.
    for router in [dev_router(), jwt_router()] {
        let (status, _, body) = get(router, "/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ok");
    }
}

#[tokio::test]
async fn oauth_resource_metadata_is_published_only_when_the_server_authenticates() {
    // Dev mode must not advertise an OAuth resource: doing so would tell a
    // client to expect token verification that is not actually happening.
    let (status, _, _) = get(dev_router(), "/.well-known/oauth-protected-resource").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, content_type, body) =
        get(jwt_router(), "/.well-known/oauth-protected-resource").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type.as_deref(), Some("application/json"));
    let metadata: Value = serde_json::from_str(&body).expect("metadata is JSON");
    assert_eq!(
        metadata,
        json!({
            "resource": RESOURCE_URL,
            "authorization_servers": [ISSUER],
            "bearer_methods_supported": ["header"],
            "scopes_supported": ["mcp:tools"],
        })
    );
}

#[tokio::test]
async fn the_router_exposes_mcp_and_nothing_else() {
    // Pins the served surface. A new public route has to be added here
    // deliberately instead of appearing unnoticed.
    for uri in [
        "/",
        "/metrics",
        "/config",
        "/.env",
        "/.well-known/oauth-authorization-server",
    ] {
        let (status, _, _) = get(dev_router(), uri).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri} must not be served");
    }

    // Traversal is refused before routing (400 rather than 404), so it can
    // never resolve to a real handler.
    let (status, _, body) = get(dev_router(), "/mcp/../health").await;
    assert!(status.is_client_error(), "{status}");
    assert_ne!(body, "ok");

    // /mcp is mounted: it rejects a bare GET rather than 404-ing.
    let (status, _, _) = get(dev_router(), "/mcp").await;
    assert_ne!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_client_can_initialize_a_session_against_the_production_router() {
    // Exercises the session factory the router installs, which nothing else in
    // the coverage-instrumented suite reaches, and pins the handshake a real
    // MCP client performs before any tool call.
    let response = dev_router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(CONTENT_TYPE, "application/json")
                .header("accept", "application/json, text/event-stream")
                .header("mcp-protocol-version", "2025-06-18")
                .header("host", "localhost")
                .body(Body::from(
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2025-06-18",
                            "capabilities": {},
                            "clientInfo": {"name": "router-test", "version": "0"}
                        }
                    })
                    .to_string(),
                ))
                .expect("request builds"),
        )
        .await
        .expect("router responds");

    let status = response.status();
    let session_id = response
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = axum::body::to_bytes(response.into_body(), 1_048_576)
        .await
        .expect("response body is readable");
    let body = String::from_utf8_lossy(&body).into_owned();

    // The handshake payload itself travels on the session's SSE stream, which
    // `oauth_wire.rs` asserts over a real socket. What matters here is that the
    // router's session factory ran and handed out exactly one session.
    assert_eq!(status, StatusCode::OK, "{status}: {body}");
    let session_id = session_id.expect("initialize must open a session");
    assert!(!session_id.is_empty());
    assert!(
        session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "session id must be an opaque safe token: {session_id}"
    );
}

#[tokio::test]
async fn the_mcp_endpoint_only_accepts_loopback_host_headers() {
    // DNS-rebinding protection: a browser page on an attacker's domain that
    // resolves to 127.0.0.1 still sends that domain as Host, so a foreign or
    // absent Host must be refused before any MCP handling.
    //
    // This also pins a deployment constraint. Behind a reverse proxy that
    // forwards the original Host, every request fails with 400; the proxy must
    // rewrite Host to localhost.
    let initialize = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "router-test", "version": "0"}
        }
    })
    .to_string();
    let post = |host: Option<&'static str>| {
        let initialize = initialize.clone();
        async move {
            let mut request = Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(CONTENT_TYPE, "application/json")
                .header("accept", "application/json, text/event-stream")
                .header("mcp-protocol-version", "2025-06-18");
            if let Some(host) = host {
                request = request.header("host", host);
            }
            let response = dev_router()
                .oneshot(
                    request
                        .body(Body::from(initialize))
                        .expect("request builds"),
                )
                .await
                .expect("router responds");
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), 1_048_576)
                .await
                .expect("response body is readable");
            (status, String::from_utf8_lossy(&body).into_owned())
        }
    };

    let (status, body) = post(None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    for host in [
        "evil.example",
        "mcp.example.com:443",
        "127.0.0.1.evil.example",
    ] {
        let (status, body) = post(Some(host)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{host} -> {body}");
        assert!(!body.contains("serverInfo"), "{host} reached MCP: {body}");
    }

    for host in ["localhost", "127.0.0.1", "localhost:8787"] {
        let (status, _) = post(Some(host)).await;
        assert_eq!(status, StatusCode::OK, "{host} must be accepted");
    }
}

#[tokio::test]
async fn mcp_rejects_unauthenticated_initialize_in_jwt_mode() {
    let response = jwt_router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(CONTENT_TYPE, "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {"name": "ozon_stores_status", "arguments": {}}
                    })
                    .to_string(),
                ))
                .expect("request builds"),
        )
        .await
        .expect("router responds");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1_048_576)
        .await
        .expect("response body is readable");
    let body = String::from_utf8_lossy(&body);

    // Either the transport refuses the session outright or the tool layer
    // returns an auth failure — but an unauthenticated caller must never get a
    // successful tool result back.
    assert!(
        !body.contains("\"stores\""),
        "unauthenticated caller received tool data: {status} {body}"
    );
}
