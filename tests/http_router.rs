//! Integration tests for the production HTTP router.
//!
//! These drive `mcp_ozon::http::build_router` — the same function `main.rs`
//! calls — rather than a re-implementation, so the routes that exist only in
//! the binary cannot silently disappear or drift from what is served.

use std::{
    collections::BTreeMap,
    convert::Infallible,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes, HttpBody},
    http::{
        Request, StatusCode,
        header::{CONTENT_TYPE, WWW_AUTHENTICATE},
    },
};
use http_body::Frame;
use mcp_ozon::{
    auth::JwtAuthenticator,
    config::{JwtConfig, RegistrySource},
    http::{
        MCP_MAX_IN_FLIGHT_STREAMS, MCP_REQUEST_BODY_LIMIT_BYTES, build_router,
        build_router_with_cancellation, build_router_with_session_idle_timeout,
    },
    ozon::OzonClient,
    runtime::{
        HTTP_CANCELLED_DRAIN_TIMEOUT, HTTP_HEADER_READ_TIMEOUT, HTTP_NATURAL_DRAIN_TIMEOUT,
        run_http_until_bounded_shutdown, serve_hardened_http,
    },
    server::OzonMcp,
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Semaphore,
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct PendingBody;

impl HttpBody for PendingBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Pending
    }
}

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

fn initialize_request() -> Request<Body> {
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
        .expect("request builds")
}

/// Sends the MCP `initialize` handshake and reports the status and session id.
async fn initialize_session(router: Router) -> (StatusCode, Option<String>) {
    let response = router
        .oneshot(initialize_request())
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

async fn initialize_session_with_origin(
    router: Router,
    origin: Option<&str>,
) -> (StatusCode, Option<String>, String) {
    initialize_session_with_origin_and_host(router, origin, "localhost").await
}

async fn initialize_session_with_origin_and_host(
    router: Router,
    origin: Option<&str>,
    host: &str,
) -> (StatusCode, Option<String>, String) {
    let mut request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header(CONTENT_TYPE, "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", "2025-06-18")
        .header("host", host);
    if let Some(origin) = origin {
        request = request.header("origin", origin);
    }
    let response = router
        .oneshot(
            request
                .body(Body::from(
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2025-06-18",
                            "capabilities": {},
                            "clientInfo": {"name": "origin-test", "version": "0"}
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
    (
        status,
        session_id,
        String::from_utf8_lossy(&body).into_owned(),
    )
}

fn jwt_router() -> Router {
    jwt_router_for_resource(RESOURCE_URL)
}

fn jwt_router_for_resource(resource_url: &str) -> Router {
    let registry = registry();
    let mut config = jwt_config();
    config.audience = resource_url.to_owned();
    config.resource_url = resource_url.to_owned();
    let authenticator = JwtAuthenticator::new(config, registry.clone())
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

    let response = router
        .clone()
        .oneshot(initialize_request())
        .await
        .expect("router responds");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );
    assert!(
        response.headers().get("mcp-session-id").is_none(),
        "a refused initialize must not hand out a session id"
    );
    let body = axum::body::to_bytes(response.into_body(), 1_048_576)
        .await
        .expect("capacity response body is readable");
    assert_eq!(
        body.as_ref(),
        b"Service Unavailable: session capacity exhausted"
    );

    // Refusing the extra session must not take the process down with it: the
    // liveness probe has to keep answering so orchestration sees a healthy
    // container that is merely at capacity.
    let (status, _, body) = get(router, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "ok");
}

#[tokio::test(start_paused = true)]
async fn dropping_an_initialize_response_cannot_pin_the_only_session_slot() {
    let server = OzonMcp::new(ozon_client(), "admin".to_owned(), registry());
    let idle_timeout = Duration::from_secs(90);
    let router = build_router_with_session_idle_timeout(
        server,
        NonZeroUsize::new(1).expect("non-zero"),
        idle_timeout,
    );

    let response = router
        .clone()
        .oneshot(initialize_request())
        .await
        .expect("router responds");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("mcp-session-id").is_some());
    // Deliberately abandon the SSE body before consuming the initialize
    // response. The session manager, not body EOF, owns eventual reclamation.
    drop(response);
    tokio::task::yield_now().await;

    tokio::time::advance(idle_timeout).await;
    tokio::task::yield_now().await;

    let replacement = router
        .oneshot(initialize_request())
        .await
        .expect("router responds after idle reclamation");
    assert_eq!(replacement.status(), StatusCode::OK);
    assert!(replacement.headers().get("mcp-session-id").is_some());
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
async fn mcp_distinguishes_invalid_json_rpc_from_unsupported_media_type() {
    async fn post(content_type: &'static str, body: &'static str) -> (StatusCode, String) {
        let response = dev_router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header(CONTENT_TYPE, content_type)
                    .header("accept", "application/json, text/event-stream")
                    .header("mcp-protocol-version", "2025-06-18")
                    .header("host", "localhost")
                    .body(Body::from(body))
                    .expect("request builds"),
            )
            .await
            .expect("router responds");
        let status = response.status();
        assert!(response.headers().get("mcp-session-id").is_none());
        let body = axum::body::to_bytes(response.into_body(), 1_048_576)
            .await
            .expect("response body is readable");
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    for invalid in ["not valid json", r#"{"jsonrpc":"2.0","id":1}"#] {
        let (status, body) = post("application/json", invalid).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "Bad Request: invalid JSON-RPC body");
    }

    let (status, body) = post("text/plain", "not valid json").await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(
        body,
        "Unsupported Media Type: Content-Type must be application/json"
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
async fn dev_mode_accepts_only_loopback_browser_origins_on_any_port() {
    for origin in [
        None,
        Some("http://localhost:49152"),
        Some("http://127.0.0.1:3000"),
        Some("https://[::1]:9443"),
    ] {
        let (status, session_id, body) = initialize_session_with_origin(dev_router(), origin).await;
        assert_eq!(status, StatusCode::OK, "origin={origin:?}: {body}");
        assert!(session_id.is_some(), "origin={origin:?}: {body}");
    }

    for origin in [
        "https://evil.example",
        "http://localhost.evil.example:49152",
        "null",
    ] {
        let (status, session_id, body) =
            initialize_session_with_origin(dev_router(), Some(origin)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "origin={origin}: {body}");
        assert!(session_id.is_none(), "origin={origin}: {body}");
        assert!(!body.contains("serverInfo"), "origin={origin}: {body}");
    }

    let (status, session_id, body) =
        initialize_session_with_origin(dev_router(), Some("not-an-origin")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(session_id.is_none(), "{body}");

    for malformed in [
        "http://localhost:49152/path",
        "http://localhost:49152?query",
    ] {
        let (status, session_id, body) =
            initialize_session_with_origin(dev_router(), Some(malformed)).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "origin={malformed}: {body}"
        );
        assert!(session_id.is_none(), "origin={malformed}: {body}");
    }
}

#[tokio::test]
async fn jwt_mode_checks_the_exact_protected_resource_origin_before_authentication() {
    for origin in [None, Some("http://localhost:8788")] {
        let (status, session_id, body) = initialize_session_with_origin(jwt_router(), origin).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "origin={origin:?}: {body}"
        );
        assert!(session_id.is_none(), "origin={origin:?}: {body}");
    }

    for origin in [
        "http://localhost:8789",
        "https://localhost:8788",
        "http://127.0.0.1:8788",
        "https://evil.example",
    ] {
        let (status, session_id, body) =
            initialize_session_with_origin(jwt_router(), Some(origin)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "origin={origin}: {body}");
        assert!(session_id.is_none(), "origin={origin}: {body}");
        assert!(!body.contains("serverInfo"), "origin={origin}: {body}");
    }

    for origin in [
        None,
        Some("https://mcp.example"),
        Some("https://mcp.example:443"),
    ] {
        let (status, session_id, body) = initialize_session_with_origin_and_host(
            jwt_router_for_resource("https://mcp.example/mcp"),
            origin,
            "mcp.example",
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "origin={origin:?}: {body}"
        );
        assert!(session_id.is_none(), "origin={origin:?}: {body}");
    }

    for origin in ["https://mcp.example:4443", "http://mcp.example"] {
        let (status, session_id, body) = initialize_session_with_origin_and_host(
            jwt_router_for_resource("https://mcp.example/mcp"),
            Some(origin),
            "mcp.example",
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "origin={origin}: {body}");
        assert!(session_id.is_none(), "origin={origin}: {body}");
    }
}

#[tokio::test]
async fn jwt_mode_checks_the_protected_resource_hostname_before_authentication() {
    let resource = "https://mcp.example/mcp";
    let (status, session_id, body) = initialize_session_with_origin_and_host(
        jwt_router_for_resource(resource),
        None,
        "mcp.example",
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert!(session_id.is_none(), "{body}");

    for host in ["localhost", "127.0.0.1", "evil.example"] {
        let (status, session_id, body) =
            initialize_session_with_origin_and_host(jwt_router_for_resource(resource), None, host)
                .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "host={host}: {body}");
        assert!(session_id.is_none(), "host={host}: {body}");
        assert!(!body.contains("serverInfo"), "host={host}: {body}");
    }
}

#[tokio::test]
async fn mcp_rejects_unauthenticated_initialize_in_jwt_mode() {
    let response = tokio::time::timeout(
        Duration::from_millis(250),
        jwt_router().oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(CONTENT_TYPE, "application/json")
                .header("accept", "application/json, text/event-stream")
                .header("host", "localhost")
                .body(Body::new(PendingBody))
                .expect("request builds"),
        ),
    )
    .await
    .expect("missing authorization must be rejected before polling the pending body")
    .expect("router responds");
    let status = response.status();
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(response.headers().get("mcp-session-id").is_none());
    let challenge = response
        .headers()
        .get(WWW_AUTHENTICATE)
        .and_then(|value| value.to_str().ok())
        .expect("401 must include a Bearer challenge");
    assert!(challenge.starts_with("Bearer "), "{challenge}");
    assert!(challenge.contains("resource_metadata=\""), "{challenge}");
    assert!(!challenge.contains("error="), "{challenge}");
    let body = axum::body::to_bytes(response.into_body(), 1_048_576)
        .await
        .expect("response body is readable");
    let body = String::from_utf8_lossy(&body);
    assert_eq!(
        body, "Требуется авторизация: access token не передан.",
        "{status}"
    );
}

#[tokio::test]
async fn hardened_runtime_serves_and_boundedly_drains_the_real_tcp_path() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("runtime integration listener binds");
    let address = listener.local_addr().expect("listener has an address");
    let router = Router::new().route("/health", axum::routing::get(|| async { "ok" }));
    let (graceful_tx, graceful_rx) = tokio::sync::oneshot::channel();
    let serve = serve_hardened_http(
        listener,
        router,
        graceful_rx,
        Arc::new(Semaphore::new(2)),
        HTTP_HEADER_READ_TIMEOUT,
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let runtime = tokio::spawn(run_http_until_bounded_shutdown(
        Box::pin(serve),
        Box::pin(async move {
            let _ = shutdown_rx.await;
        }),
        graceful_tx,
        CancellationToken::new(),
        HTTP_NATURAL_DRAIN_TIMEOUT,
        HTTP_CANCELLED_DRAIN_TIMEOUT,
    ));

    let mut rejected = TcpStream::connect(address)
        .await
        .expect("HTTP/2 probe connects through the real accept loop");
    rejected
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .expect("HTTP/2 prior-knowledge probe writes");
    let mut rejected_response = Vec::new();
    rejected
        .read_to_end(&mut rejected_response)
        .await
        .expect("rejected protocol response closes");
    assert!(!String::from_utf8_lossy(&rejected_response).contains("200 OK"));

    let mut client = TcpStream::connect(address)
        .await
        .expect("client connects through the real accept loop");
    client
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("request writes");
    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .await
        .expect("response reads");
    assert!(
        response.windows(6).any(|window| window == b"200 OK"),
        "health response must traverse the hardened TCP runtime"
    );

    shutdown_tx.send(()).expect("shutdown signal sends");
    let result = tokio::time::timeout(Duration::from_secs(2), runtime)
        .await
        .expect("runtime drains before the test deadline")
        .expect("runtime task joins");
    assert!(matches!(result, Some(Ok(()))));
}
