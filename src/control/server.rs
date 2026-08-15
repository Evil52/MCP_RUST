use std::sync::Arc;

use rmcp::{
    Json, ServerHandler,
    handler::server::{
        common::{AsRequestContext, FromContextPart},
        router::tool::ToolRouter,
        wrapper::Parameters,
    },
    model::{Implementation, JsonObject, MetaObject, ServerCapabilities, ServerInfo},
    schemars::JsonSchema,
    tool, tool_handler, tool_router,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    auth::{AuthenticatedActor, JwtAuthenticator, ProtectedResourceMetadata},
    config::{AccessRegistry, Actor, RegistrySource},
    control::policy::{ControlMode, ControlPolicy},
    http::HttpMcpServer,
};

const ACCESS_DENIED: &str = "CONTROL_ACCESS_DENIED";

#[derive(Debug, Clone)]
pub struct ControlMcp {
    policy: Arc<ControlPolicy>,
    registry: RegistrySource,
    default_actor_id: Option<String>,
    authenticator: Option<JwtAuthenticator>,
    tool_router: ToolRouter<Self>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EmptyInput {}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ControlStatusResult {
    pub actor_id: String,
    pub policy_version: u32,
    pub mode: ControlMode,
    pub explicit_policy_binding: bool,
    pub marketplace_writes_enabled: bool,
    pub credentials_loaded: bool,
    pub marketplace_egress_enabled: bool,
    pub persistence_configured: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ControlScopeResult {
    pub actor_id: String,
    pub policy_version: u32,
    pub mode: ControlMode,
    pub targets: Vec<ControlTargetResult>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ControlTargetResult {
    pub account_id: String,
    pub campaign_id: u64,
    pub skus: Vec<u64>,
    pub bid_limits: BidLimitsResult,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct BidLimitsResult {
    pub min_minor: u64,
    pub max_minor: u64,
    pub max_delta_percent: u8,
}

#[derive(Debug, Clone, Default)]
struct ControlIdentity {
    actor_id: Option<String>,
    registry: Option<Arc<AccessRegistry>>,
}

impl<C> FromContextPart<C> for ControlIdentity
where
    C: AsRequestContext,
{
    fn from_context_part(context: &mut C) -> Result<Self, rmcp::ErrorData> {
        let context = context.as_request_context();
        let actor_id = context
            .extensions
            .get::<AuthenticatedActor>()
            .or_else(|| {
                context
                    .extensions
                    .get::<axum::http::request::Parts>()
                    .and_then(|parts| parts.extensions.get::<AuthenticatedActor>())
            })
            .map(|actor| actor.actor_id.clone());
        let registry = context
            .extensions
            .get::<Arc<AccessRegistry>>()
            .cloned()
            .or_else(|| {
                context
                    .extensions
                    .get::<axum::http::request::Parts>()
                    .and_then(|parts| parts.extensions.get::<Arc<AccessRegistry>>())
                    .cloned()
            });
        Ok(Self { actor_id, registry })
    }
}

impl ControlMcp {
    pub fn new_disabled(actor_id: String, registry: RegistrySource, policy: ControlPolicy) -> Self {
        Self {
            policy: Arc::new(policy),
            registry,
            default_actor_id: Some(actor_id),
            authenticator: None,
            tool_router: Self::configured_tool_router(None),
        }
    }

    pub fn new_authenticated_disabled(
        registry: RegistrySource,
        policy: ControlPolicy,
        authenticator: JwtAuthenticator,
    ) -> Self {
        let tool_router = Self::configured_tool_router(Some(&authenticator));
        Self {
            policy: Arc::new(policy),
            registry,
            default_actor_id: None,
            authenticator: Some(authenticator),
            tool_router,
        }
    }

    fn configured_tool_router(authenticator: Option<&JwtAuthenticator>) -> ToolRouter<Self> {
        let mut router = Self::tool_router();
        let mut security_scheme = JsonObject::new();
        match authenticator {
            Some(authenticator) => {
                security_scheme.insert("type".to_owned(), Value::String("oauth2".to_owned()));
                security_scheme.insert(
                    "scopes".to_owned(),
                    Value::Array(
                        authenticator
                            .required_scopes()
                            .iter()
                            .cloned()
                            .map(Value::String)
                            .collect(),
                    ),
                );
            }
            None => {
                security_scheme.insert("type".to_owned(), Value::String("noauth".to_owned()));
            }
        }
        let schemes = vec![security_scheme];
        let schemes_value = Value::Array(schemes.iter().cloned().map(Value::Object).collect());
        for route in router.map.values_mut() {
            route.attr.security_schemes = Some(schemes.clone());
            route
                .attr
                .meta
                .get_or_insert_with(MetaObject::new)
                .0
                .insert("securitySchemes".to_owned(), schemes_value.clone());
        }
        router
    }

    fn access_context(
        &self,
        identity: &ControlIdentity,
    ) -> Result<(Arc<AccessRegistry>, Actor), String> {
        let registry = match &identity.registry {
            Some(registry) => Arc::clone(registry),
            None => self
                .registry
                .load()
                .map_err(|error| format!("CONTROL_POLICY_ERROR: {error}"))?,
        };
        let actor_id = identity
            .actor_id
            .as_deref()
            .or(self.default_actor_id.as_deref())
            .ok_or_else(|| format!("{ACCESS_DENIED}: отсутствует проверенная идентичность"))?;
        let actor = registry
            .actor(actor_id)
            .map_err(|_| format!("{ACCESS_DENIED}: actor отсутствует в access registry"))?
            .clone();
        Ok((registry, actor))
    }

    pub fn protected_resource_metadata(&self) -> Option<ProtectedResourceMetadata> {
        self.authenticator
            .as_ref()
            .map(JwtAuthenticator::protected_resource_metadata)
    }

    pub fn transport_authenticator(&self) -> Option<&JwtAuthenticator> {
        self.authenticator.as_ref()
    }
}

#[tool_router]
impl ControlMcp {
    /// Показывает локальное состояние scaffold. Всегда подтверждает, что ключи, egress и marketplace writes выключены.
    #[tool(
        name = "ozon_ads_control_status",
        annotations(
            title = "Статус Control MCP",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn control_status(
        &self,
        identity: ControlIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<ControlStatusResult>, String> {
        let (_registry, actor) = self.access_context(&identity)?;
        let actor_id = actor.id;
        Ok(Json(ControlStatusResult {
            explicit_policy_binding: self.policy.actor_policy(&actor_id).is_some(),
            actor_id,
            policy_version: self.policy.version,
            mode: self.policy.mode,
            marketplace_writes_enabled: false,
            credentials_loaded: false,
            marketplace_egress_enabled: false,
            persistence_configured: false,
        }))
    }

    /// Возвращает только явно перечисленные в локальной policy кампании, SKU и лимиты текущего actor. Сетевых запросов нет.
    #[tool(
        name = "ozon_ads_control_scope",
        annotations(
            title = "Разрешённый scope рекламы",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn control_scope(
        &self,
        identity: ControlIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<ControlScopeResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        let actor_id = actor.id.clone();
        let targets = self
            .policy
            .actor_policy(&actor_id)
            .into_iter()
            .flat_map(|policy| &policy.targets)
            .filter(|target| {
                registry
                    .accounts
                    .iter()
                    .find(|account| account.id == target.account_id)
                    .is_some_and(|account| actor.can_access_account(account))
            })
            .map(|target| ControlTargetResult {
                account_id: target.account_id.clone(),
                campaign_id: target.campaign_id,
                skus: target.skus.clone(),
                bid_limits: BidLimitsResult {
                    min_minor: target.bid_limits.min_minor,
                    max_minor: target.bid_limits.max_minor,
                    max_delta_percent: target.bid_limits.max_delta_percent,
                },
            })
            .collect();
        Ok(Json(ControlScopeResult {
            actor_id,
            policy_version: self.policy.version,
            mode: self.policy.mode,
            targets,
        }))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for ControlMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("mcp-ozon-control", env!("CARGO_PKG_VERSION"))
                    .with_title("OzonOFK Advertising Control — Disabled Scaffold"),
            )
            .with_instructions(
                "Отдельный fail-closed scaffold управления рекламой. В этой версии marketplace credentials не загружаются, marketplace egress отсутствует (в JWT-режиме возможен только настроенный JWKS), планы не создаются и реклама не изменяется. Доступны только локальные read-only status/scope. Не заявляйте, что изменение применено, и не просите API-ключи через чат. Любая будущая write-функция должна появиться отдельным проверенным инструментом с явным approval, серверным allowlist и аудитом.",
            )
    }
}

impl HttpMcpServer for ControlMcp {
    fn protected_resource_metadata(&self) -> Option<ProtectedResourceMetadata> {
        self.protected_resource_metadata()
    }

    fn transport_authenticator(&self) -> Option<&JwtAuthenticator> {
        self.transport_authenticator()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        num::NonZeroUsize,
        path::PathBuf,
        sync::Arc,
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };

    use axum::{
        Extension, Router,
        body::{Body, to_bytes},
        http::{HeaderMap, Request, StatusCode, header::CONTENT_TYPE},
    };
    use serde_json::json;
    use tokio_util::sync::CancellationToken;
    use tower::ServiceExt;

    use super::*;
    use crate::{
        config::JwtConfig, http::build_router_for_server_with_cancellation_and_session_idle_timeout,
    };

    static FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct Fixtures {
        registry_path: PathBuf,
        policy_path: PathBuf,
    }

    impl Fixtures {
        fn new(policy_actor: bool) -> Self {
            let id = FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir();
            let registry_path = root.join(format!("control-server-registry-{id}.json"));
            let policy_path = root.join(format!("control-server-policy-{id}.json"));
            fs::write(
                &registry_path,
                serde_json::to_vec(&serde_json::json!({
                    "version": 1,
                    "actors": [{
                        "id": "admin",
                        "name": "Administrator",
                        "role": "admin",
                        "oidc": { "username": "admin" }
                    }],
                    "accounts": [{
                        "id": "ozon_one",
                        "organization": "Example",
                        "marketplace": "ozon",
                        "seller_client_id": "seller",
                        "manager_id": "admin",
                        "ozon": {
                            "store_id": "store_one",
                            "client_id_env": "UNUSED_CLIENT_ID",
                            "api_key_env": "UNUSED_API_KEY",
                            "performance": {
                                "client_id_env": "UNUSED_PERF_ID",
                                "client_secret_env": "UNUSED_PERF_SECRET"
                            }
                        }
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
            let actors = if policy_actor {
                serde_json::json!([{
                    "actor_id": "admin",
                    "targets": [{
                        "account_id": "ozon_one",
                        "campaign_id": 42,
                        "skus": [1001],
                        "bid_limits": {
                            "min_minor": 100,
                            "max_minor": 5000,
                            "max_delta_percent": 5
                        }
                    }]
                }])
            } else {
                serde_json::json!([])
            };
            fs::write(
                &policy_path,
                serde_json::to_vec(&serde_json::json!({
                    "version": 1,
                    "mode": "disabled",
                    "actors": actors
                }))
                .unwrap(),
            )
            .unwrap();
            Self {
                registry_path,
                policy_path,
            }
        }

        fn server(&self) -> ControlMcp {
            let registry = RegistrySource::new(&self.registry_path).unwrap();
            let snapshot = registry.load().unwrap();
            let policy = ControlPolicy::load(&self.policy_path, &snapshot).unwrap();
            ControlMcp::new_disabled("admin".to_owned(), registry, policy)
        }

        fn authenticated_server(&self) -> ControlMcp {
            let registry = RegistrySource::new(&self.registry_path).unwrap();
            let snapshot = registry.load().unwrap();
            let policy = ControlPolicy::load(&self.policy_path, &snapshot).unwrap();
            let authenticator = JwtAuthenticator::new(
                JwtConfig {
                    issuer: "https://issuer.example/realms/ofk".to_owned(),
                    audience: "http://localhost:8790/mcp".to_owned(),
                    jwks_url: "http://127.0.0.1:1/jwks".to_owned(),
                    resource_url: "http://localhost:8790/mcp".to_owned(),
                    resource_metadata_url:
                        "http://localhost:8790/.well-known/oauth-protected-resource".to_owned(),
                    required_scopes: vec!["mcp:ads-control".to_owned()],
                    jwks_cache_ttl: Duration::from_secs(300),
                },
                registry.clone(),
            )
            .unwrap();
            ControlMcp::new_authenticated_disabled(registry, policy, authenticator)
        }
    }

    impl Drop for Fixtures {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.registry_path);
            let _ = fs::remove_file(&self.policy_path);
        }
    }

    fn control_router(server: ControlMcp) -> Router {
        build_router_for_server_with_cancellation_and_session_idle_timeout(
            server,
            NonZeroUsize::new(4).unwrap(),
            Duration::from_secs(120),
            CancellationToken::new(),
        )
    }

    async fn rpc(
        router: &Router,
        session_id: Option<&str>,
        message: Value,
    ) -> (StatusCode, HeaderMap, String) {
        let mut request = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header(CONTENT_TYPE, "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-06-18")
            .header("host", "localhost");
        if let Some(session_id) = session_id {
            request = request.header("mcp-session-id", session_id);
        }
        let response = router
            .clone()
            .oneshot(request.body(Body::from(message.to_string())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let body = to_bytes(response.into_body(), 1_048_576).await.unwrap();
        (status, headers, String::from_utf8_lossy(&body).into_owned())
    }

    fn rpc_json(headers: &HeaderMap, body: &str) -> Value {
        if headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
        {
            return serde_json::from_str(body).unwrap();
        }
        let mut event_data = String::new();
        for line in body.lines().chain(std::iter::once("")) {
            if let Some(data) = line.strip_prefix("data:") {
                if !event_data.is_empty() {
                    event_data.push('\n');
                }
                event_data.push_str(data.strip_prefix(' ').unwrap_or(data));
            } else if line.trim().is_empty() && !event_data.is_empty() {
                if let Ok(value) = serde_json::from_str(&event_data) {
                    return value;
                }
                event_data.clear();
            }
        }
        panic!("missing JSON-RPC response in {body:?}")
    }

    #[test]
    fn wire_response_parser_covers_json_multiline_and_invalid_sse_events() {
        let mut json_headers = HeaderMap::new();
        json_headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        assert_eq!(rpc_json(&json_headers, r#"{"ok":true}"#)["ok"], true);

        let sse_headers = HeaderMap::new();
        assert_eq!(
            rpc_json(&sse_headers, "data: {\"value\":\ndata: 7}\n\n")["value"],
            7
        );
        assert_eq!(
            rpc_json(
                &sse_headers,
                "data: not-json\n\ndata: {\"recovered\":true}\n\n"
            )["recovered"],
            true
        );
        assert!(std::panic::catch_unwind(|| rpc_json(&sse_headers, "event: ping\n\n")).is_err());
    }

    async fn initialize(router: &Router) -> String {
        let (status, headers, body) = rpc(
            router,
            None,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "control-server-test", "version": "1"}
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            rpc_json(&headers, &body)
                .pointer("/result/serverInfo/name")
                .and_then(Value::as_str),
            Some("mcp-ozon-control")
        );
        let session_id = headers
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .unwrap()
            .to_owned();
        let (status, _, body) = rpc(
            router,
            Some(&session_id),
            json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        session_id
    }

    #[tokio::test]
    async fn disabled_status_and_explicit_scope_are_truthful() {
        let fixtures = Fixtures::new(true);
        let server = fixtures.server();
        let status = server
            .control_status(
                ControlIdentity::default(),
                Parameters(EmptyInput::default()),
            )
            .await
            .unwrap()
            .0;
        assert!(status.explicit_policy_binding);
        assert!(!status.marketplace_writes_enabled);
        assert!(!status.credentials_loaded);
        assert!(!status.marketplace_egress_enabled);
        assert!(!status.persistence_configured);

        let scope = server
            .control_scope(
                ControlIdentity::default(),
                Parameters(EmptyInput::default()),
            )
            .await
            .unwrap()
            .0;
        assert_eq!(scope.targets.len(), 1);
        assert_eq!(scope.targets[0].campaign_id, 42);
        assert_eq!(scope.targets[0].skus, [1001]);
    }

    #[tokio::test]
    async fn admin_has_no_implicit_control_scope() {
        let fixtures = Fixtures::new(false);
        let server = fixtures.server();
        let status = server
            .control_status(
                ControlIdentity::default(),
                Parameters(EmptyInput::default()),
            )
            .await
            .unwrap()
            .0;
        assert!(!status.explicit_policy_binding);
        let scope = server
            .control_scope(
                ControlIdentity::default(),
                Parameters(EmptyInput::default()),
            )
            .await
            .unwrap()
            .0;
        assert!(scope.targets.is_empty());
    }

    #[test]
    fn inventory_contains_only_two_read_only_local_tools() {
        let fixtures = Fixtures::new(false);
        let server = fixtures.server();
        assert_eq!(server.tool_router.map.len(), 2);
        for route in server.tool_router.map.values() {
            let annotations = route.attr.annotations.as_ref().unwrap();
            assert_eq!(annotations.read_only_hint, Some(true));
            assert_eq!(annotations.destructive_hint, Some(false));
            assert_eq!(annotations.idempotent_hint, Some(true));
            assert_eq!(annotations.open_world_hint, Some(false));
        }
        let info = server.get_info();
        assert!(
            info.instructions
                .unwrap()
                .contains("credentials не загружаются")
        );
    }

    #[test]
    fn authenticated_constructor_advertises_exact_control_oauth_policy() {
        let fixtures = Fixtures::new(false);
        let server = fixtures.authenticated_server();

        let metadata = server.protected_resource_metadata().unwrap();
        assert_eq!(metadata.resource, "http://localhost:8790/mcp");
        assert_eq!(metadata.scopes_supported, ["mcp:ads-control"]);
        assert!(server.transport_authenticator().is_some());

        let trait_metadata =
            <ControlMcp as HttpMcpServer>::protected_resource_metadata(&server).unwrap();
        assert_eq!(trait_metadata.resource, metadata.resource);
        assert_eq!(
            trait_metadata.authorization_servers,
            metadata.authorization_servers
        );
        assert_eq!(trait_metadata.scopes_supported, metadata.scopes_supported);
        assert!(
            <ControlMcp as HttpMcpServer>::transport_authenticator(&server).is_some(),
            "the generic HTTP router must receive the Control authenticator"
        );

        let expected = json!([{"type": "oauth2", "scopes": ["mcp:ads-control"]}]);
        for tool in server.tool_router.list_all() {
            let serialized = serde_json::to_value(&tool).unwrap();
            assert_eq!(serialized.get("securitySchemes"), Some(&expected));
            assert_eq!(
                serialized.pointer("/_meta/securitySchemes"),
                Some(&expected)
            );
        }
    }

    #[tokio::test]
    async fn access_context_uses_request_snapshot_and_fails_closed() {
        let fixtures = Fixtures::new(true);
        let server = fixtures.server();
        let snapshot = server.registry.load().unwrap();

        let status = server
            .control_status(
                ControlIdentity {
                    actor_id: Some("admin".to_owned()),
                    registry: Some(Arc::clone(&snapshot)),
                },
                Parameters(EmptyInput::default()),
            )
            .await
            .unwrap()
            .0;
        assert_eq!(status.actor_id, "admin");

        let missing_identity = fixtures.authenticated_server();
        let denied = missing_identity
            .control_status(
                ControlIdentity::default(),
                Parameters(EmptyInput::default()),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(
            denied,
            "CONTROL_ACCESS_DENIED: отсутствует проверенная идентичность"
        );

        let revoked = server
            .control_scope(
                ControlIdentity {
                    actor_id: Some("revoked".to_owned()),
                    registry: Some(snapshot),
                },
                Parameters(EmptyInput::default()),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(
            revoked,
            "CONTROL_ACCESS_DENIED: actor отсутствует в access registry"
        );

        fs::remove_file(&fixtures.registry_path).unwrap();
        let registry_error = server
            .control_status(
                ControlIdentity::default(),
                Parameters(EmptyInput::default()),
            )
            .await
            .err()
            .unwrap();
        assert!(registry_error.starts_with("CONTROL_POLICY_ERROR:"));
    }

    #[tokio::test]
    async fn control_http_wire_lists_exact_inventory_and_propagates_request_identity() {
        let fixtures = Fixtures::new(true);
        let registry = RegistrySource::new(&fixtures.registry_path).unwrap();
        let snapshot = registry.load().unwrap();
        let policy = ControlPolicy::load(&fixtures.policy_path, &snapshot).unwrap();
        let server = ControlMcp::new_disabled("revoked".to_owned(), registry, policy);
        let router = control_router(server)
            .layer(Extension(AuthenticatedActor {
                actor_id: "admin".to_owned(),
            }))
            .layer(Extension(snapshot));
        let session_id = initialize(&router).await;

        let (status, headers, body) = rpc(
            &router,
            Some(&session_id),
            json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let response = rpc_json(&headers, &body);
        let mut names = response
            .pointer("/result/tools")
            .and_then(Value::as_array)
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(names, ["ozon_ads_control_scope", "ozon_ads_control_status"]);

        // Prove that the exact registry snapshot attached to this HTTP request
        // reaches the tool context; a fallback reload can no longer succeed.
        fs::remove_file(&fixtures.registry_path).unwrap();
        let (status, headers, body) = rpc(
            &router,
            Some(&session_id),
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "ozon_ads_control_status", "arguments": {}}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let response = rpc_json(&headers, &body);
        let text = response
            .pointer("/result/content/0/text")
            .and_then(Value::as_str)
            .unwrap();
        let result: Value = serde_json::from_str(text).unwrap();
        assert_eq!(result["actor_id"], "admin");
        assert_eq!(result["marketplace_writes_enabled"], false);
    }
}
