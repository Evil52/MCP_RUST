// The normal all-targets test suite always runs this end-to-end wire contract.
// During cargo-llvm-cov the library is already fully instrumented by its unit-test
// binary; compiling it again for this integration binary creates duplicate async
// regions for the same source lines and distorts LLVM's line denominator.
#![cfg(not(coverage))]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{Json, Router, routing::get};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use mcp_ozon::{
    auth::JwtAuthenticator,
    config::{JwtConfig, RegistrySource},
    http::build_router,
    ozon::OzonClient,
    server::OzonMcp,
};
use reqwest::{
    Client, StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HOST, HeaderMap, WWW_AUTHENTICATE},
};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use serde::Serialize;
use serde_json::{Value, json};

const PROTOCOL_VERSION: &str = "2025-06-18";
const SESSION_HEADER: &str = "mcp-session-id";
const PROTOCOL_HEADER: &str = "mcp-protocol-version";
const KID: &str = "oauth-wire-test-key";
const ISSUER: &str = "http://issuer.test/realms/ofk";
const AUDIENCE: &str = "ozonofk-mcp";
const RESOURCE_METADATA_URL: &str = "http://localhost:8788/.well-known/oauth-protected-resource";

static REGISTRY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static TEST_KEY: OnceLock<TestKey> = OnceLock::new();

struct TestKey {
    encoding: EncodingKey,
    modulus: String,
    exponent: String,
}

struct TempRegistry {
    path: PathBuf,
    source: RegistrySource,
}

impl Drop for TempRegistry {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

struct RunningServer {
    base_url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct WireResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Option<Value>,
    raw_body: String,
}

fn openssl(args: &[&str], input: &[u8]) -> Vec<u8> {
    let mut child = Command::new("openssl")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("openssl is required to generate an ephemeral JWT test key");
    child
        .stdin
        .take()
        .expect("openssl stdin must be piped")
        .write_all(input)
        .expect("test key input must be writable");
    let output = child
        .wait_with_output()
        .expect("openssl test-key process must finish");
    assert!(
        output.status.success(),
        "openssl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn test_key() -> &'static TestKey {
    TEST_KEY.get_or_init(|| {
        let private_pem = openssl(
            &[
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
                "-pkeyopt",
                "rsa_keygen_pubexp:65537",
            ],
            &[],
        );
        let modulus_output = openssl(&["rsa", "-noout", "-modulus"], &private_pem);
        let modulus_hex = std::str::from_utf8(&modulus_output)
            .expect("openssl modulus must be UTF-8")
            .trim()
            .strip_prefix("Modulus=")
            .expect("openssl modulus output must have its standard prefix");
        let (modulus_pairs, remainder) = modulus_hex.as_bytes().as_chunks::<2>();
        assert!(remainder.is_empty());
        let modulus = modulus_pairs
            .iter()
            .map(|pair| {
                u8::from_str_radix(
                    std::str::from_utf8(pair).expect("hex modulus must be ASCII"),
                    16,
                )
                .expect("openssl modulus must be hexadecimal")
            })
            .collect::<Vec<_>>();
        TestKey {
            encoding: EncodingKey::from_rsa_pem(&private_pem)
                .expect("generated RSA private key must be valid"),
            modulus: URL_SAFE_NO_PAD.encode(modulus),
            exponent: URL_SAFE_NO_PAD.encode([1, 0, 1]),
        }
    })
}

#[derive(Serialize)]
struct TestClaims<'a> {
    iss: &'a str,
    aud: &'a str,
    sub: &'a str,
    scope: &'a str,
    preferred_username: &'a str,
    exp: i64,
    nbf: i64,
}

fn token(audience: &str, scope: &str) -> String {
    token_for_username(audience, scope, "admin")
}

fn token_for_username(audience: &str, scope: &str, username: &str) -> String {
    let now = chrono::Utc::now().timestamp();
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(KID.to_owned());
    encode(
        &header,
        &TestClaims {
            iss: ISSUER,
            aud: audience,
            sub: "wire-test-subject",
            scope,
            preferred_username: username,
            exp: now + 3_600,
            nbf: now - 1,
        },
        &test_key().encoding,
    )
    .expect("ephemeral test token must be signed")
}

fn registry() -> TempRegistry {
    let sequence = REGISTRY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "mcp-ozon-oauth-wire-{}-{sequence}.json",
        std::process::id()
    ));
    fs::write(
        &path,
        serde_json::to_vec_pretty(&json!({
            "version": 1,
            "actors": [{
                "id": "admin",
                "name": "OAuth Wire Administrator",
                "role": "admin",
                "oidc": {"username": "admin"}
            }],
            "accounts": []
        }))
        .expect("test registry must serialize"),
    )
    .expect("test registry must be writable");
    let source = RegistrySource::new(path.clone()).expect("test registry must be valid");
    TempRegistry { path, source }
}

async fn start_jwks_server() -> RunningServer {
    let key = test_key();
    let jwks = json!({
        "keys": [{
            "kty": "RSA",
            "kid": KID,
            "use": "sig",
            "alg": "RS256",
            "n": key.modulus,
            "e": key.exponent
        }]
    });
    let router = Router::new().route(
        "/jwks",
        get(move || {
            let jwks = jwks.clone();
            async move { Json(jwks) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("JWKS listener must bind");
    let address = listener.local_addr().expect("JWKS listener has an address");
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("JWKS server must run");
    });
    RunningServer {
        base_url: format!("http://{address}"),
        task,
    }
}

async fn start_mcp_server(registry: &RegistrySource, jwks_url: String) -> RunningServer {
    let client = OzonClient::new(
        "http://127.0.0.1:1".to_owned(),
        Duration::from_millis(100),
        BTreeMap::new(),
    )
    .expect("test Ozon client configuration must be valid");
    let authenticator = JwtAuthenticator::new(
        JwtConfig {
            issuer: ISSUER.to_owned(),
            audience: AUDIENCE.to_owned(),
            jwks_url,
            resource_url: "http://localhost:8788/mcp".to_owned(),
            resource_metadata_url: RESOURCE_METADATA_URL.to_owned(),
            required_scopes: vec!["mcp:tools".to_owned()],
            jwks_cache_ttl: Duration::from_secs(300),
        },
        registry.clone(),
    )
    .expect("test JWT authenticator configuration must be valid");
    // Drive the production router rather than a replica, so the OAuth wire
    // contract is asserted against the wiring `main.rs` actually serves.
    let router = build_router(
        OzonMcp::new_authenticated(client, registry.clone(), authenticator),
        LocalSessionManager::DEFAULT_MAX_SESSIONS,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("MCP listener must bind");
    let address = listener.local_addr().expect("MCP listener has an address");
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("MCP server must run");
    });
    RunningServer {
        base_url: format!("http://{address}"),
        task,
    }
}

fn parse_json_or_sse(headers: &HeaderMap, raw_body: &str) -> Option<Value> {
    if raw_body.trim().is_empty() {
        return None;
    }
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if content_type.starts_with("application/json") {
        return Some(serde_json::from_str(raw_body).expect("JSON response must be valid"));
    }
    if !content_type.starts_with("text/event-stream") {
        return None;
    }

    let mut event_data = String::new();
    for line in raw_body.lines().chain(std::iter::once("")) {
        if let Some(data) = line.strip_prefix("data:") {
            if !event_data.is_empty() {
                event_data.push('\n');
            }
            event_data.push_str(data.strip_prefix(' ').unwrap_or(data));
        } else if line.trim().is_empty() && !event_data.is_empty() {
            if let Ok(value) = serde_json::from_str(&event_data) {
                return Some(value);
            }
            event_data.clear();
        }
    }
    panic!("SSE response did not contain a JSON-RPC data event: {raw_body}");
}

async fn post_rpc(
    client: &Client,
    endpoint: &str,
    session_id: Option<&str>,
    bearer: Option<&str>,
    message: Value,
) -> WireResponse {
    let mut request = client
        .post(endpoint)
        // The socket uses an ephemeral loopback address, while this server's
        // configured public protected resource is localhost:8788. Model the
        // reverse proxy contract by preserving that public Host at the app.
        .header(HOST, "localhost:8788")
        .header(ACCEPT, "application/json, text/event-stream")
        .header(PROTOCOL_HEADER, PROTOCOL_VERSION)
        .json(&message);
    if let Some(session_id) = session_id {
        request = request.header(SESSION_HEADER, session_id);
    }
    if let Some(bearer) = bearer {
        request = request.header(AUTHORIZATION, format!("Bearer {bearer}"));
    }
    let response = request.send().await.expect("MCP POST must complete");
    finite_wire_response(response).await
}

async fn finite_wire_response(response: reqwest::Response) -> WireResponse {
    let status = response.status();
    let headers = response.headers().clone();
    let raw_body = response.text().await.expect("MCP body must be readable");
    let body = parse_json_or_sse(&headers, &raw_body);
    WireResponse {
        status,
        headers,
        body,
        raw_body,
    }
}

fn rpc_body(response: &WireResponse) -> &Value {
    response.body.as_ref().unwrap_or_else(|| {
        panic!(
            "expected JSON-RPC response, got HTTP {} with body {:?}",
            response.status, response.raw_body
        )
    })
}

fn assert_transport_auth_failure(
    response: &WireResponse,
    status: StatusCode,
    oauth_error: Option<&str>,
    public_message: &str,
) {
    assert_eq!(response.status, status, "{}", response.raw_body);
    assert_eq!(response.raw_body, public_message);
    assert!(response.body.is_none());
    assert!(response.headers.get(SESSION_HEADER).is_none());
    let challenge = response
        .headers
        .get(WWW_AUTHENTICATE)
        .and_then(|value| value.to_str().ok());
    let Some(oauth_error) = oauth_error else {
        if status == StatusCode::UNAUTHORIZED {
            let challenge = challenge.expect("401 must carry a Bearer challenge");
            assert!(challenge.starts_with("Bearer "), "{challenge}");
            assert!(
                challenge.contains(&format!("resource_metadata=\"{RESOURCE_METADATA_URL}\"")),
                "{challenge}"
            );
            assert!(challenge.contains("scope=\"mcp:tools\""), "{challenge}");
            assert!(!challenge.contains("error="), "{challenge}");
            assert!(!challenge.contains("error_description="), "{challenge}");
        } else {
            assert!(challenge.is_none(), "unexpected challenge: {challenge:?}");
        }
        return;
    };
    let challenge = challenge.expect("OAuth failure must carry WWW-Authenticate");
    assert!(challenge.starts_with("Bearer "), "{challenge}");
    assert!(
        challenge.contains(&format!("resource_metadata=\"{RESOURCE_METADATA_URL}\"")),
        "{challenge}"
    );
    assert!(challenge.contains("scope=\"mcp:tools\""), "{challenge}");
    assert!(
        challenge.contains(&format!("error=\"{oauth_error}\"")),
        "{challenge}"
    );
    assert!(challenge.contains("error_description=\""), "{challenge}");
}

#[tokio::test]
async fn chatgpt_oauth_contract_is_request_scoped_on_the_mcp_wire() {
    let registry = registry();
    let jwks = start_jwks_server().await;
    let mcp = start_mcp_server(&registry.source, format!("{}/jwks", jwks.base_url)).await;
    let endpoint = format!("{}/mcp", mcp.base_url);
    let client = Client::new();

    let missing_initialize = post_rpc(
        &client,
        &endpoint,
        None,
        None,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "oauth-wire-test", "version": "1.0.0"}
            }
        }),
    )
    .await;
    assert_transport_auth_failure(
        &missing_initialize,
        StatusCode::UNAUTHORIZED,
        None,
        "Требуется авторизация: access token не передан.",
    );

    let valid_token = token(AUDIENCE, "openid profile mcp:tools");
    let initialize = post_rpc(
        &client,
        &endpoint,
        None,
        Some(&valid_token),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "oauth-wire-test", "version": "1.0.0"}
            }
        }),
    )
    .await;
    assert_eq!(initialize.status, StatusCode::OK, "{}", initialize.raw_body);
    assert_eq!(
        rpc_body(&initialize)
            .pointer("/result/protocolVersion")
            .and_then(Value::as_str),
        Some(PROTOCOL_VERSION)
    );
    let session_id = initialize
        .headers
        .get(SESSION_HEADER)
        .and_then(|value| value.to_str().ok())
        .expect("initialize response must establish an MCP session")
        .to_owned();

    let missing_notification = post_rpc(
        &client,
        &endpoint,
        Some(&session_id),
        None,
        json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        }),
    )
    .await;
    assert_transport_auth_failure(
        &missing_notification,
        StatusCode::UNAUTHORIZED,
        None,
        "Требуется авторизация: access token не передан.",
    );

    let initialized = post_rpc(
        &client,
        &endpoint,
        Some(&session_id),
        Some(&valid_token),
        json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        }),
    )
    .await;
    assert_eq!(
        initialized.status,
        StatusCode::ACCEPTED,
        "{}",
        initialized.raw_body
    );
    assert!(initialized.body.is_none());

    let listed = post_rpc(
        &client,
        &endpoint,
        Some(&session_id),
        Some(&valid_token),
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
    )
    .await;
    assert_eq!(listed.status, StatusCode::OK, "{}", listed.raw_body);
    let tools = rpc_body(&listed)
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("tools/list did not return tools: {}", listed.raw_body));
    let expected_names = BTreeSet::from([
        "list_members",
        "marketplace_accounts",
        "ofk_collection_status",
        "ofk_data_completeness",
        "ofk_manager_actions",
        "ofk_metrics_history",
        "ofk_ozon_sales_analytics",
        "ofk_ozon_sales_refresh_status",
        "ofk_reports",
        "ofk_request_ozon_sales_refresh",
        "ozon_analytics",
        "ozon_fbo_cancel_reasons",
        "ozon_fbo_posting",
        "ozon_fbo_postings",
        "ozon_fbo_stocks_by_warehouse",
        "ozon_fbs_cancel_reasons",
        "ozon_fbs_posting",
        "ozon_fbs_postings",
        "ozon_posting_sales_fallback",
        "ozon_fbs_stocks_by_warehouse",
        "ozon_fbs_unfulfilled",
        "ozon_finance_accrual_by_day",
        "ozon_finance_accrual_postings",
        "ozon_finance_accrual_types",
        "ozon_finance_cash_flow",
        "ozon_finance_mutual_settlement",
        "ozon_finance_realization_by_day",
        "ozon_finance_totals",
        "ozon_finance_transactions",
        "ozon_performance_campaign_objects",
        "ozon_performance_campaign_products",
        "ozon_performance_campaigns",
        "ozon_performance_daily",
        "ozon_performance_expenses",
        "ozon_performance_limits",
        "ozon_performance_sku_statistics",
        "ozon_product_attributes",
        "ozon_product_info",
        "ozon_live_buyer_prices",
        "ozon_product_prices",
        "ozon_product_stocks",
        "ozon_products",
        "ozon_product_pictures_info",
        "ozon_product_content_diagnostics",
        "ozon_questions",
        "ozon_returns",
        "ozon_reviews",
        "ozon_rfbs_returns",
        "ozon_seller_rating",
        "ozon_seller_rating_history",
        "ozon_stock_turnover",
        "ozon_stores_status",
        "ozon_supply_order_get",
        "ozon_supply_order_list",
        "ozon_warehouse_stocks",
        "ozon_warehouses",
        "wb_ping",
        "wb_acceptance_coefficients",
        "wb_product_cards",
        "wb_product_prices",
        "wb_promotion_minimum_bids",
        "wb_promotion_recommended_bids",
        "wb_promotion_campaigns",
        "wb_promotion_campaign_details",
        "wb_promotion_search_cluster_bids",
        "wb_promotion_stats",
        "wb_search_orders_positions",
        "wb_search_product_queries",
        "wb_sales_funnel",
        "wb_sales_funnel_history",
        "wb_sales_funnel_grouped_history",
        "wb_tariff_boxes",
        "wb_tariff_commissions",
        "wb_tariff_pallets",
        "wb_tariff_returns",
        "wb_warehouse_stocks",
        "wb_orders",
        "wb_sales",
        "wb_stores_status",
    ]);
    let actual_names = tools
        .iter()
        .map(|tool| {
            tool.get("name")
                .and_then(Value::as_str)
                .expect("every tool must have a name")
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_names, expected_names);
    let expected_security = json!([{"type": "oauth2", "scopes": ["mcp:tools"]}]);
    for tool in tools {
        assert_eq!(
            tool.get("securitySchemes"),
            Some(&expected_security),
            "canonical policy missing from {}",
            tool["name"]
        );
        assert_eq!(
            tool.pointer("/_meta/securitySchemes"),
            Some(&expected_security),
            "compatibility mirror differs for {}",
            tool["name"]
        );
    }

    let listed_with_stale_token = post_rpc(
        &client,
        &endpoint,
        Some(&session_id),
        Some("not-a-jwt"),
        json!({"jsonrpc": "2.0", "id": 20, "method": "tools/list", "params": {}}),
    )
    .await;
    assert_transport_auth_failure(
        &listed_with_stale_token,
        StatusCode::UNAUTHORIZED,
        Some("invalid_token"),
        "Требуется повторная авторизация: access token недействителен.",
    );

    let missing_before_validation = post_rpc(
        &client,
        &endpoint,
        Some(&session_id),
        None,
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "ozon_analytics", "arguments": {}}
        }),
    )
    .await;
    assert_transport_auth_failure(
        &missing_before_validation,
        StatusCode::UNAUTHORIZED,
        None,
        "Требуется авторизация: access token не передан.",
    );

    let valid = post_rpc(
        &client,
        &endpoint,
        Some(&session_id),
        Some(&valid_token),
        json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {"name": "list_members", "arguments": {}}
        }),
    )
    .await;
    assert_eq!(valid.status, StatusCode::OK, "{}", valid.raw_body);
    assert_eq!(
        rpc_body(&valid)
            .pointer("/result/isError")
            .and_then(Value::as_bool),
        Some(false),
        "{}",
        valid.raw_body
    );
    assert_eq!(
        rpc_body(&valid)
            .pointer("/result/structuredContent/members/0/id")
            .and_then(Value::as_str),
        Some("admin"),
        "{}",
        valid.raw_body
    );

    let missing_after_valid = post_rpc(
        &client,
        &endpoint,
        Some(&session_id),
        None,
        json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {"name": "list_members", "arguments": {}}
        }),
    )
    .await;
    assert_transport_auth_failure(
        &missing_after_valid,
        StatusCode::UNAUTHORIZED,
        None,
        "Требуется авторизация: access token не передан.",
    );

    let insufficient_scope_token = token(AUDIENCE, "openid profile");
    let insufficient_scope = post_rpc(
        &client,
        &endpoint,
        Some(&session_id),
        Some(&insufficient_scope_token),
        json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {"name": "list_members", "arguments": {}}
        }),
    )
    .await;
    assert_transport_auth_failure(
        &insufficient_scope,
        StatusCode::FORBIDDEN,
        Some("insufficient_scope"),
        "Требуется повторная авторизация с необходимыми разрешениями.",
    );

    let wrong_audience_token = token("another-resource", "mcp:tools");
    let wrong_audience = post_rpc(
        &client,
        &endpoint,
        Some(&session_id),
        Some(&wrong_audience_token),
        json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {"name": "list_members", "arguments": {}}
        }),
    )
    .await;
    assert_transport_auth_failure(
        &wrong_audience,
        StatusCode::UNAUTHORIZED,
        Some("invalid_token"),
        "Требуется повторная авторизация: access token выпущен для другого ресурса.",
    );

    let unknown_actor_token = token_for_username(AUDIENCE, "mcp:tools", "not-provisioned");
    let unknown_actor = post_rpc(
        &client,
        &endpoint,
        Some(&session_id),
        Some(&unknown_actor_token),
        json!({
            "jsonrpc": "2.0",
            "id": 8,
            "method": "tools/call",
            "params": {"name": "list_members", "arguments": {}}
        }),
    )
    .await;
    assert_transport_auth_failure(
        &unknown_actor,
        StatusCode::FORBIDDEN,
        None,
        "Доступ для подтверждённой учётной записи не разрешён.",
    );

    let missing_get = finite_wire_response(
        client
            .get(&endpoint)
            .header(HOST, "localhost:8788")
            .header(ACCEPT, "text/event-stream")
            .header(PROTOCOL_HEADER, PROTOCOL_VERSION)
            .header(SESSION_HEADER, &session_id)
            .send()
            .await
            .expect("unauthenticated session GET must complete"),
    )
    .await;
    assert_transport_auth_failure(
        &missing_get,
        StatusCode::UNAUTHORIZED,
        None,
        "Требуется авторизация: access token не передан.",
    );

    let valid_get = client
        .get(&endpoint)
        .header(HOST, "localhost:8788")
        .header(ACCEPT, "text/event-stream")
        .header(PROTOCOL_HEADER, PROTOCOL_VERSION)
        .header(SESSION_HEADER, &session_id)
        .bearer_auth(&valid_token)
        .send()
        .await
        .expect("authenticated session GET must connect");
    assert_eq!(valid_get.status(), StatusCode::OK);
    assert_eq!(
        valid_get
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );
    drop(valid_get);

    let missing_delete = finite_wire_response(
        client
            .delete(&endpoint)
            .header(HOST, "localhost:8788")
            .header(PROTOCOL_HEADER, PROTOCOL_VERSION)
            .header(SESSION_HEADER, &session_id)
            .send()
            .await
            .expect("unauthenticated session DELETE must complete"),
    )
    .await;
    assert_transport_auth_failure(
        &missing_delete,
        StatusCode::UNAUTHORIZED,
        None,
        "Требуется авторизация: access token не передан.",
    );

    let deleted = client
        .delete(&endpoint)
        .header(HOST, "localhost:8788")
        .header(PROTOCOL_HEADER, PROTOCOL_VERSION)
        .header(SESSION_HEADER, &session_id)
        .bearer_auth(&valid_token)
        .send()
        .await
        .expect("authenticated session DELETE must complete");
    assert_eq!(deleted.status(), StatusCode::ACCEPTED);
}
