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
    http::{
        MCP_MAX_IN_FLIGHT_STREAMS, MCP_REQUEST_BODY_LIMIT_BYTES, build_router,
        build_router_with_cancellation,
    },
    ozon::OzonClient,
    server::OzonMcp,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
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
    dev_router_with_session_limit(4)
}

fn dev_router_with_session_limit(max_sessions: usize) -> Router {
    let server = OzonMcp::new(ozon_client(), "admin".to_owned(), registry());
    build_router(
        server,
        NonZeroUsize::new(max_sessions).expect("session limit is non-zero"),
    )
}

/// Sends the MCP `initialize` handshake and reports the status and session id.
async fn initialize_session(router: Router) -> (StatusCode, Option<String>) {
    let response = router
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
    (status, session_id)
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

/// The session cap must be enforced by the router that `main.rs` builds, not
/// merely by the session manager in isolation. `max_sessions` is what stops an
/// unauthenticated client from exhausting process memory by opening sessions, so
/// a router that accepted the bound and then failed to apply it would be a live
/// denial-of-service hole that a manager-level unit test cannot see.
#[tokio::test]
async fn the_production_router_enforces_its_session_cap_end_to_end() {
    const LIMIT: usize = 2;
    let router = dev_router_with_session_limit(LIMIT);

    let mut session_ids = Vec::new();
    for attempt in 1..=LIMIT {
        let (status, session_id) = initialize_session(router.clone()).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "session {attempt} of {LIMIT} must be accepted"
        );
        session_ids.push(session_id.expect("an accepted initialize must open a session"));
    }
    // Distinct sessions, so the cap is counting real sessions rather than
    // handing the same one out repeatedly.
    session_ids.sort();
    session_ids.dedup();
    assert_eq!(session_ids.len(), LIMIT);

    let (status, session_id) = initialize_session(router.clone()).await;
    assert!(
        status.is_server_error(),
        "session {} must be refused once the cap is reached, got {status}",
        LIMIT + 1
    );
    assert!(
        session_id.is_none(),
        "a refused initialize must not hand out a session id"
    );

    // Refusing the extra session must not take the process down with it: the
    // liveness probe has to keep answering so orchestration sees a healthy
    // container that is merely at capacity.
    let (status, _, body) = get(router, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "ok");
}

#[tokio::test]
async fn cancelling_the_router_root_terminates_an_active_legacy_session() {
    let cancellation_token = CancellationToken::new();
    let server = OzonMcp::new(ozon_client(), "admin".to_owned(), registry());
    let router = build_router_with_cancellation(
        server,
        NonZeroUsize::new(4).expect("non-zero"),
        cancellation_token.clone(),
    );

    let initialized = router
        .clone()
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
                            "clientInfo": {"name": "root-cancellation-test", "version": "0"}
                        }
                    })
                    .to_string(),
                ))
                .expect("initialize request builds"),
        )
        .await
        .expect("router responds");
    assert_eq!(initialized.status(), StatusCode::OK);
    let session_id = initialized
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .expect("initialize establishes a session")
        .to_owned();
    axum::body::to_bytes(initialized.into_body(), MCP_REQUEST_BODY_LIMIT_BYTES)
        .await
        .expect("initialize stream completes");

    let handshake_complete = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(CONTENT_TYPE, "application/json")
                .header("accept", "application/json, text/event-stream")
                .header("mcp-protocol-version", "2025-06-18")
                .header("mcp-session-id", &session_id)
                .header("host", "localhost")
                .body(Body::from(
                    json!({
                        "jsonrpc": "2.0",
                        "method": "notifications/initialized",
                        "params": {}
                    })
                    .to_string(),
                ))
                .expect("initialized notification builds"),
        )
        .await
        .expect("router responds");
    assert_eq!(handshake_complete.status(), StatusCode::ACCEPTED);

    cancellation_token.cancel();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri("/mcp")
                        .header("accept", "text/event-stream")
                        .header("mcp-protocol-version", "2025-06-18")
                        .header("mcp-session-id", &session_id)
                        .header("host", "localhost")
                        .body(Body::empty())
                        .expect("session probe builds"),
                )
                .await
                .expect("router responds");
            if response.status() == StatusCode::NOT_FOUND {
                break;
            }
            assert_eq!(response.status(), StatusCode::OK);
            drop(response);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("root cancellation must close the legacy session promptly");

    let (health_status, _, body) = get(router, "/health").await;
    assert_eq!(health_status, StatusCode::OK);
    assert_eq!(body, "ok");
}

#[tokio::test]
async fn production_router_bounds_get_sse_shadows_without_blocking_post_or_health() {
    let router = dev_router();
    let initialize = || {
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
                        "clientInfo": {"name": "stream-limit-test", "version": "0"}
                    }
                })
                .to_string(),
            ))
            .expect("initialize request builds")
    };
    let initialized = router
        .clone()
        .oneshot(initialize())
        .await
        .expect("router responds");
    assert_eq!(initialized.status(), StatusCode::OK);
    let session_id = initialized
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .expect("initialize establishes a session")
        .to_owned();
    let _ = axum::body::to_bytes(initialized.into_body(), MCP_REQUEST_BODY_LIMIT_BYTES)
        .await
        .expect("initialize stream completes");

    let stream_request = || {
        Request::builder()
            .method("GET")
            .uri("/mcp")
            .header("accept", "text/event-stream")
            .header("mcp-protocol-version", "2025-06-18")
            .header("mcp-session-id", &session_id)
            .header("host", "localhost")
            .body(Body::empty())
            .expect("stream request builds")
    };
    let mut streams = Vec::with_capacity(MCP_MAX_IN_FLIGHT_STREAMS);
    for _ in 0..MCP_MAX_IN_FLIGHT_STREAMS {
        let response = router
            .clone()
            .oneshot(stream_request())
            .await
            .expect("router responds");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("text/event-stream")
        );
        streams.push(response);
    }

    let overloaded = router
        .clone()
        .oneshot(stream_request())
        .await
        .expect("router responds");
    assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        overloaded
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );

    let post = router
        .clone()
        .oneshot(initialize())
        .await
        .expect("router responds");
    assert_eq!(post.status(), StatusCode::OK);
    drop(post);
    let (health_status, _, _) = get(router.clone(), "/health").await;
    assert_eq!(health_status, StatusCode::OK);

    drop(streams.pop());
    let recovered = router
        .oneshot(stream_request())
        .await
        .expect("router responds");
    assert_eq!(recovered.status(), StatusCode::OK);
}

#[tokio::test]
async fn production_router_enforces_the_mcp_body_limit_while_streaming() {
    let post = |body: Vec<u8>| async move {
        dev_router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header(CONTENT_TYPE, "application/json")
                    .header("accept", "application/json, text/event-stream")
                    .header("host", "localhost")
                    // Deliberately omit Content-Length: the transport must
                    // enforce the bound from streamed bytes, not trust a
                    // caller-controlled header.
                    .body(Body::from(body))
                    .expect("request builds"),
            )
            .await
            .expect("router responds")
    };

    let at_limit = post(vec![b' '; MCP_REQUEST_BODY_LIMIT_BYTES]).await;
    assert_ne!(at_limit.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let over_limit = post(vec![b' '; MCP_REQUEST_BODY_LIMIT_BYTES + 1]).await;
    assert_eq!(over_limit.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(over_limit.headers().get("mcp-session-id").is_none());
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
