use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, Write},
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
use chrono::{Duration as ChronoDuration, Utc};
use rmcp::handler::server::wrapper::Parameters;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use super::*;
use crate::{
    auth::AuthenticatedActor,
    config::JwtConfig,
    control::{
        plan::{
            CONTROL_DB_TEST_LOCK, PlanStoreError, WbActionQuota, WbApplyContext, WbControlPlan,
            WbPlanApproval, WbPlanRepository, WbPlanStatus,
        },
        policy::{ControlMode, WbBidPlacement},
        wb::{
            WbBidChange, WbCampaignBidSnapshot, WbPreparedBidChange, WbSnapshotBid, WbWriteError,
        },
    },
    http::build_router_for_server_with_cancellation_and_session_idle_timeout,
    test_support::mock_http,
    wb::WbCredentials,
};

use super::{
    authorization::{
        allowed_plan_target, authorize_plan_account_access, authorize_plan_apply,
        authorize_plan_approval, plan_target,
    },
    contract::{
        ApplyWbBidPlanInput, ApproveWbBidPlanInput, EmptyInput, PrepareWbBidPlanInput, WbPlanInput,
        WbPlanResult,
    },
    presentation::{
        WritePermitFailure, guarded_write_permit_error_class, plan_result, plan_store_error,
        write_failure_finish,
    },
    tools::read_plan_snapshot,
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
                "revision": 1,
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

    fn new_wb(mode: ControlMode) -> Self {
        Self::new_wb_with_adverts(mode, &[77])
    }

    fn new_wb_with_adverts(mode: ControlMode, advert_ids: &[u64]) -> Self {
        let id = FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir();
        let registry_path = root.join(format!("control-server-wb-registry-{id}.json"));
        let policy_path = root.join(format!("control-server-wb-policy-{id}.json"));
        fs::write(
            &registry_path,
            serde_json::to_vec(&json!({
                "version": 1,
                "actors": [
                    {
                        "id": "manager",
                        "name": "Manager",
                        "role": "manager",
                        "oidc": { "username": "manager" }
                    },
                    {
                        "id": "approver",
                        "name": "Approver",
                        "role": "finance",
                        "account_ids": ["wb_one"],
                        "oidc": { "username": "approver" }
                    },
                    {
                        "id": "observer",
                        "name": "Observer",
                        "role": "analyst",
                        "account_ids": ["wb_one"],
                        "oidc": { "username": "observer" }
                    }
                ],
                "accounts": [{
                    "id": "wb_one",
                    "organization": "WB Example",
                    "marketplace": "wildberries",
                    "seller_client_id": "wb-seller",
                    "manager_id": "manager",
                    "wildberries": {
                        "api_token_env": "UNUSED_WB_TOKEN",
                        "seller_sid": "123e4567-e89b-42d3-a456-426614174000"
                    }
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        let wb_targets = advert_ids
            .iter()
            .map(|advert_id| {
                json!({
                    "account_id": "wb_one",
                    "seller_sid": "123e4567-e89b-42d3-a456-426614174000",
                    "advert_id": advert_id,
                    "nm_ids": [1001],
                    "placements": ["search"],
                    "bid_limits_kopecks": {
                        "min_minor": 500,
                        "max_minor": 5000,
                        "max_delta_percent": 10
                    },
                    "approver_actor_ids": ["approver"],
                    "action_limits": {
                        "max_actions_per_hour": 4,
                        "max_actions_per_day": 12,
                        "cooldown_seconds": 30,
                        "max_cumulative_abs_delta_kopecks_per_day": 5000
                    }
                })
            })
            .collect::<Vec<_>>();
        fs::write(
            &policy_path,
            serde_json::to_vec(&json!({
                "version": 1,
                "revision": 1,
                "mode": mode,
                "actors": [{
                    "actor_id": "manager",
                    "wb_promotion_bid_targets": wb_targets
                }]
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
                resource_metadata_url: "http://localhost:8790/.well-known/oauth-protected-resource"
                    .to_owned(),
                required_scopes: vec!["mcp:ads-control".to_owned()],
                jwks_cache_ttl: Duration::from_secs(300),
            },
            registry.clone(),
        )
        .unwrap();
        ControlMcp::new_authenticated_disabled(registry, policy, authenticator)
    }

    fn identity(&self, actor_id: &str) -> ControlIdentity {
        let registry = RegistrySource::new(&self.registry_path).unwrap();
        ControlIdentity {
            actor_id: Some(actor_id.to_owned()),
            registry: Some(registry.load().unwrap()),
        }
    }
}

impl Drop for Fixtures {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.registry_path);
        let _ = fs::remove_file(&self.policy_path);
    }
}

fn wb_requested(bid_kopecks: u64) -> Vec<WbBidChange> {
    vec![WbBidChange {
        nm_id: 1001,
        placement: WbBidPlacement::Search,
        bid_kopecks,
    }]
}

fn wb_snapshot(advert_id: u64, bid_kopecks: u64) -> WbCampaignBidSnapshot {
    WbCampaignBidSnapshot {
        seller_sid: "123e4567-e89b-42d3-a456-426614174000".to_owned(),
        advert_id,
        status: 9,
        bid_type: "manual".to_owned(),
        payment_type: "cpm".to_owned(),
        bids: vec![WbSnapshotBid {
            nm_id: 1001,
            placement: WbBidPlacement::Search,
            bid_kopecks,
        }],
    }
}

fn wb_details(advert_id: u64, bid_kopecks: u64) -> String {
    json!({
        "adverts": [{
            "id": advert_id,
            "status": 9,
            "bid_type": "manual",
            "settings": {"payment_type": "cpm"},
            "nm_settings": [{
                "nm_id": 1001,
                "bids_kopecks": {
                    "search": bid_kopecks,
                    "recommendations": bid_kopecks
                }
            }]
        }]
    })
    .to_string()
}

fn paused_http(
    body: String,
) -> (
    String,
    std::sync::mpsc::Receiver<String>,
    std::sync::mpsc::Sender<()>,
) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (request_sender, request_receiver) = std::sync::mpsc::channel();
    let (release_sender, release_receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            request.push_str(&line);
            if line == "\r\n" {
                break;
            }
        }
        request_sender.send(request).unwrap();
        release_receiver.recv().unwrap();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
    });
    (
        format!("http://{address}"),
        request_receiver,
        release_sender,
    )
}

fn sample_plan(server: &ControlMcp) -> WbControlPlan {
    let now = Utc::now();
    WbControlPlan {
        plan_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        plan_digest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        prepare_reservation_id: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            .to_owned(),
        actor_id: "manager".to_owned(),
        account_id: "wb_one".to_owned(),
        advert_id: 77,
        schema_version: server.policy.version,
        policy_revision: server.policy.revision,
        policy_digest: server.policy.digest().to_owned(),
        action_quota: WbActionQuota {
            max_actions_per_hour: 4,
            max_actions_per_day: 12,
            cooldown_seconds: 30,
            max_cumulative_abs_delta_kopecks_per_day: 5000,
        },
        status: WbPlanStatus::Prepared,
        approval: None,
        requested: wb_requested(1050),
        changes: vec![WbPreparedBidChange {
            nm_id: 1001,
            placement: WbBidPlacement::Search,
            before_bid_kopecks: 1000,
            bid_kopecks: 1050,
        }],
        before: wb_snapshot(77, 1000),
        created_at: now,
        expires_at: now + ChronoDuration::minutes(5),
        apply_started_at: None,
        last_error_class: None,
        write_response: None,
        readback: None,
    }
}

fn test_wb_services(
    plans: Arc<WbPlanRepository>,
    read_responses: Vec<(u16, String)>,
    write_responses: Option<Vec<(u16, String)>>,
) -> (
    WbControlServices,
    std::sync::mpsc::Receiver<String>,
    Option<std::sync::mpsc::Receiver<String>>,
) {
    let (read_base_url, read_requests) = mock_http(read_responses);
    let reader = WbClient::new_for_test(
        Duration::from_secs(1),
        BTreeMap::from([(
            "wb_one".to_owned(),
            WbCredentials {
                token: "test-read-token".to_owned(),
            },
        )]),
        &read_base_url,
        &read_base_url,
    );
    let (writer, write_requests) = write_responses.map_or_else(
        || (None, None),
        |responses| {
            let (write_base_url, requests) = mock_http(responses);
            (
                Some(Arc::new(WbBidWriteClient::new_for_test(
                    &write_base_url,
                    "test-write-token",
                    Duration::from_secs(1),
                ))),
                Some(requests),
            )
        },
    );
    (
        WbControlServices {
            account_id: "wb_one".to_owned(),
            seller_sid: "123e4567-e89b-42d3-a456-426614174000".to_owned(),
            reader: Arc::new(reader),
            writer,
            plans,
        },
        read_requests,
        write_requests,
    )
}

async fn clean_control_tables(admin: &tokio_postgres::Client) {
    admin
        .batch_execute(
            "TRUNCATE TABLE control.wb_audit_events, control.wb_action_reservations, \
             control.wb_plan_approvals, control.wb_plans, \
             control.wb_prepare_reservations, control.wb_runtime_gates, \
             control.wb_policy_revisions RESTART IDENTITY CASCADE",
        )
        .await
        .unwrap();
}

async fn enable_test_gates(admin: &tokio_postgres::Client, account_id: &str, advert_id: u64) {
    let now = Utc::now();
    let lease_expires_at = now + ChronoDuration::minutes(10);
    for (gate_key, scope_kind, gate_account_id, gate_advert_id) in [
        ("global".to_owned(), "global", None, None),
        (
            format!("account/{account_id}"),
            "account",
            Some(account_id),
            None,
        ),
        (
            format!("campaign/{account_id}/{advert_id}"),
            "campaign",
            Some(account_id),
            Some(i64::try_from(advert_id).unwrap()),
        ),
    ] {
        admin
            .execute(
                "INSERT INTO control.wb_runtime_gates \
                    (gate_key, scope_kind, account_id, advert_id, enabled, \
                     lease_expires_at, disabled_until, revision, reason, updated_by, updated_at) \
                 VALUES ($1,$2,$3,$4,true,$5,NULL,1,'server_test','server_test',$6) \
                 ON CONFLICT (gate_key) DO UPDATE SET \
                    enabled=true, lease_expires_at=EXCLUDED.lease_expires_at, \
                    disabled_until=NULL, revision=control.wb_runtime_gates.revision+1, \
                    reason=EXCLUDED.reason, updated_by=EXCLUDED.updated_by, \
                    updated_at=EXCLUDED.updated_at",
                &[
                    &gate_key,
                    &scope_kind,
                    &gate_account_id,
                    &gate_advert_id,
                    &lease_expires_at,
                    &now,
                ],
            )
            .await
            .unwrap();
    }
}

async fn prepare_approved_plan(
    fixtures: &Fixtures,
    base_server: &ControlMcp,
    plans: Arc<WbPlanRepository>,
    advert_id: u64,
) -> WbPlanResult {
    let (services, requests, _) =
        test_wb_services(plans, vec![(200, wb_details(advert_id, 1000))], None);
    let server = base_server.clone().with_wb_control_services(services);
    let prepared = server
        .prepare_wb_bid_update(
            fixtures.identity("manager"),
            Parameters(PrepareWbBidPlanInput {
                account_id: "wb_one".to_owned(),
                advert_id,
                changes: wb_requested(1050),
            }),
        )
        .await
        .unwrap()
        .0;
    assert!(
        requests
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .starts_with(&format!(
                "GET /api/advert/v2/adverts?ids={advert_id} HTTP/1.1\r\n"
            ))
    );
    server
        .approve_wb_bid_plan(
            fixtures.identity("approver"),
            Parameters(ApproveWbBidPlanInput {
                plan_id: prepared.plan_id,
                plan_digest: prepared.plan_digest,
                approval_reference: format!("server-test/approval-{advert_id}"),
            }),
        )
        .await
        .unwrap()
        .0
}

async fn apply_test_plan(
    fixtures: &Fixtures,
    base_server: &ControlMcp,
    plans: Arc<WbPlanRepository>,
    admin: &tokio_postgres::Client,
    advert_id: u64,
    read_responses: Vec<(u16, String)>,
    write_response: (u16, String),
) -> WbPlanResult {
    let approved =
        prepare_approved_plan(fixtures, base_server, Arc::clone(&plans), advert_id).await;
    enable_test_gates(admin, "wb_one", advert_id).await;
    let (services, _, _) = test_wb_services(plans, read_responses, Some(vec![write_response]));
    base_server
        .clone()
        .with_wb_control_services(services)
        .apply_wb_bid_plan(
            fixtures.identity("manager"),
            Parameters(ApplyWbBidPlanInput {
                plan_id: approved.plan_id,
                plan_digest: approved.plan_digest,
            }),
        )
        .await
        .unwrap()
        .0
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
    assert!(!status.write_executor_configured);
    assert!(status.runtime_gates_required);
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

#[tokio::test]
async fn wb_scope_and_write_tools_fail_closed_before_runtime_access() {
    let disabled_fixtures = Fixtures::new_wb(ControlMode::Disabled);
    let disabled = disabled_fixtures.authenticated_server();
    let manager = disabled_fixtures.identity("manager");
    let scope = disabled
        .control_scope(manager.clone(), Parameters(EmptyInput::default()))
        .await
        .unwrap()
        .0;
    assert_eq!(scope.wb_promotion_bid_targets.len(), 1);
    let target = &scope.wb_promotion_bid_targets[0];
    assert_eq!(target.account_id, "wb_one");
    assert_eq!(target.seller_sid, "123e4567-e89b-42d3-a456-426614174000");
    assert_eq!(target.advert_id, 77);
    assert_eq!(target.nm_ids, [1001]);
    assert_eq!(target.placements, [WbBidPlacement::Search]);
    assert_eq!(target.bid_limits_kopecks.min_minor, 500);
    assert_eq!(target.bid_limits_kopecks.max_minor, 5000);
    assert_eq!(target.bid_limits_kopecks.max_delta_percent, 10);
    assert_eq!(target.approver_actor_ids, ["approver"]);
    assert_eq!(target.action_limits.max_actions_per_hour, 4);

    let prepare = PrepareWbBidPlanInput {
        account_id: "wb_one".to_owned(),
        advert_id: 77,
        changes: wb_requested(1050),
    };
    assert_eq!(
        disabled
            .prepare_wb_bid_update(manager.clone(), Parameters(prepare))
            .await
            .err()
            .unwrap(),
        "CONTROL_DISABLED: создание планов выключено policy"
    );
    assert_eq!(
        disabled
            .approve_wb_bid_plan(
                disabled_fixtures.identity("approver"),
                Parameters(ApproveWbBidPlanInput {
                    plan_id: "a".repeat(64),
                    plan_digest: "b".repeat(64),
                    approval_reference: "test/reference".to_owned(),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_DISABLED: approval планов выключен policy"
    );

    let plan_only_fixtures = Fixtures::new_wb(ControlMode::PlanOnly);
    let plan_only = plan_only_fixtures.authenticated_server();
    let manager = plan_only_fixtures.identity("manager");
    assert_eq!(
        plan_only
            .prepare_wb_bid_update(
                manager.clone(),
                Parameters(PrepareWbBidPlanInput {
                    account_id: "wb_one".to_owned(),
                    advert_id: 77,
                    changes: wb_requested(1050),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_DISABLED: WB runtime не настроен"
    );
    assert_eq!(
        plan_only
            .prepare_wb_bid_update(
                plan_only_fixtures.identity("observer"),
                Parameters(PrepareWbBidPlanInput {
                    account_id: "wb_one".to_owned(),
                    advert_id: 77,
                    changes: wb_requested(1050),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_ACCESS_DENIED: отсутствует явная control policy binding"
    );
    assert_eq!(
        plan_only
            .prepare_wb_bid_update(
                manager.clone(),
                Parameters(PrepareWbBidPlanInput {
                    account_id: "wb_one".to_owned(),
                    advert_id: 999,
                    changes: wb_requested(1050),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_ACCESS_DENIED: WB campaign отсутствует в control policy"
    );

    let mut missing_account = manager.clone();
    let mut missing_account_registry = (**missing_account.registry.as_ref().unwrap()).clone();
    missing_account_registry.accounts.clear();
    missing_account.registry = Some(Arc::new(missing_account_registry));
    assert_eq!(
        plan_only
            .prepare_wb_bid_update(
                missing_account,
                Parameters(PrepareWbBidPlanInput {
                    account_id: "wb_one".to_owned(),
                    advert_id: 77,
                    changes: wb_requested(1050),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_ACCESS_DENIED: WB account отсутствует в registry"
    );

    let mut revoked = manager.clone();
    let mut revoked_registry = (**revoked.registry.as_ref().unwrap()).clone();
    revoked_registry.accounts[0].manager_id = "revoked".to_owned();
    revoked_registry.actors[0].account_ids.clear();
    revoked.registry = Some(Arc::new(revoked_registry));
    assert_eq!(
        plan_only
            .prepare_wb_bid_update(
                revoked,
                Parameters(PrepareWbBidPlanInput {
                    account_id: "wb_one".to_owned(),
                    advert_id: 77,
                    changes: wb_requested(1050),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_ACCESS_DENIED: actor не имеет доступа к WB account"
    );

    assert_eq!(
        plan_only
            .apply_wb_bid_plan(
                manager.clone(),
                Parameters(ApplyWbBidPlanInput {
                    plan_id: "a".repeat(64),
                    plan_digest: "b".repeat(64),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_DISABLED: применение планов выключено policy"
    );
    assert_eq!(
        plan_only
            .wb_bid_plan_status(
                manager.clone(),
                Parameters(WbPlanInput {
                    plan_id: "a".repeat(64),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_DISABLED: WB plan store не настроен"
    );
    assert_eq!(
        plan_only
            .reconcile_wb_bid_plan(
                manager,
                Parameters(WbPlanInput {
                    plan_id: "a".repeat(64),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_DISABLED: WB runtime не настроен"
    );
}

#[test]
fn plan_projection_authorization_and_error_classes_are_exhaustive() {
    let fixtures = Fixtures::new_wb(ControlMode::Enabled);
    let server = fixtures.authenticated_server();
    let registry = server.registry.load().unwrap();
    let manager = registry.actor("manager").unwrap();
    let approver = registry.actor("approver").unwrap();
    let observer = registry.actor("observer").unwrap();
    let mut plan = sample_plan(&server);

    assert_eq!(plan_target(&server.policy, &plan).unwrap().advert_id, 77);
    assert!(allowed_plan_target(&server.policy, &plan).is_some());
    authorize_plan_approval(&server.policy, &registry, approver, &plan).unwrap();
    assert!(authorize_plan_approval(&server.policy, &registry, manager, &plan).is_err());
    assert!(authorize_plan_approval(&server.policy, &registry, observer, &plan).is_err());

    let mut changed = plan.clone();
    changed.policy_digest = "d".repeat(64);
    assert!(authorize_plan_approval(&server.policy, &registry, approver, &changed).is_err());
    let mut missing_target = plan.clone();
    missing_target.advert_id = 78;
    missing_target.before.advert_id = 78;
    assert!(allowed_plan_target(&server.policy, &missing_target).is_none());
    let mut missing_account = (*registry).clone();
    missing_account.accounts.clear();
    assert!(authorize_plan_approval(&server.policy, &missing_account, approver, &plan).is_err());
    let mut missing_actor = (*registry).clone();
    missing_actor.actors.retain(|actor| actor.id != "manager");
    assert!(authorize_plan_approval(&server.policy, &missing_actor, approver, &plan).is_err());
    let mut revoked = (*registry).clone();
    revoked.accounts[0].manager_id = "revoked".to_owned();
    revoked.actors[0].account_ids.clear();
    assert!(
        authorize_plan_approval(
            &server.policy,
            &revoked,
            revoked.actor("approver").unwrap(),
            &plan,
        )
        .is_err()
    );

    let now = Utc::now();
    plan.status = WbPlanStatus::Ambiguous;
    plan.last_error_class = Some("wb_write_ambiguous".to_owned());
    plan.approval = Some(WbPlanApproval {
        approval_id: "approval-id".to_owned(),
        approver_id: "approver".to_owned(),
        reason: "test/reference".to_owned(),
        approved_at: now,
        expires_at: now + ChronoDuration::minutes(2),
    });
    let result = plan_result(&plan);
    assert_eq!(result.plan_id, plan.plan_id);
    assert_eq!(result.plan_digest, plan.plan_digest);
    assert_eq!(result.actor_id, "manager");
    assert_eq!(result.account_id, "wb_one");
    assert_eq!(result.seller_sid, plan.before.seller_sid);
    assert_eq!(result.advert_id, 77);
    assert_eq!(result.policy_schema_version, 1);
    assert_eq!(result.policy_revision, 1);
    assert_eq!(result.policy_digest, server.policy.digest());
    assert_eq!(result.action_quota, plan.action_quota);
    assert_eq!(result.status, WbPlanStatus::Ambiguous);
    assert_eq!(result.approval.unwrap().approver_id, "approver");
    assert_eq!(result.changes, plan.changes);
    assert!(result.created_at.ends_with('Z'));
    assert!(result.expires_at.ends_with('Z'));
    assert_eq!(
        result.last_error_class.as_deref(),
        Some("wb_write_ambiguous")
    );
    assert!(result.requires_reconciliation);

    let store_cases = [
        (PlanStoreError::NotFound, "CONTROL_PLAN_NOT_FOUND"),
        (PlanStoreError::InvalidState, "CONTROL_PLAN_ALREADY_USED"),
        (PlanStoreError::Expired, "CONTROL_PLAN_EXPIRED"),
        (
            PlanStoreError::ApprovalRequired,
            "CONTROL_PLAN_APPROVAL_REQUIRED",
        ),
        (
            PlanStoreError::ApprovalExpired,
            "CONTROL_PLAN_APPROVAL_EXPIRED",
        ),
        (PlanStoreError::PlanChanged, "CONTROL_PLAN_CHANGED"),
        (PlanStoreError::PolicyChanged, "CONTROL_POLICY_CHANGED"),
        (
            PlanStoreError::CampaignLocked,
            "CONTROL_CAMPAIGN_INCIDENT_LOCKED",
        ),
        (PlanStoreError::RuntimeDisabled, "CONTROL_RUNTIME_DISABLED"),
        (
            PlanStoreError::QuotaExceeded,
            "CONTROL_ACTION_LIMIT_REACHED",
        ),
        (
            PlanStoreError::PrepareLimitExceeded,
            "CONTROL_PREPARE_LIMIT_REACHED",
        ),
        (PlanStoreError::Busy, "CONTROL_CAMPAIGN_BUSY"),
        (
            PlanStoreError::ApplyInProgress,
            "CONTROL_PLAN_APPLY_IN_PROGRESS",
        ),
        (PlanStoreError::InvalidPlan, "CONTROL_PLAN_INVALID"),
        (
            PlanStoreError::Unavailable,
            "CONTROL_PERSISTENCE_UNAVAILABLE",
        ),
    ];
    for (error, expected) in store_cases {
        assert_eq!(plan_store_error(error), expected);
    }

    assert_eq!(
        guarded_write_permit_error_class(&WritePermitFailure::Authorization),
        "access_revoked"
    );
    assert_eq!(
        guarded_write_permit_error_class(&WritePermitFailure::PreflightRead),
        "preflight_read_failed"
    );
    assert_eq!(
        guarded_write_permit_error_class(&WritePermitFailure::PreconditionChanged(Box::new(
            wb_snapshot(77, 999),
        ))),
        "precondition_changed"
    );
    for (error, expected) in [
        (PlanStoreError::ApprovalRequired, "approval_revoked"),
        (PlanStoreError::ApprovalExpired, "approval_revoked"),
        (PlanStoreError::PlanChanged, "policy_changed"),
        (PlanStoreError::PolicyChanged, "policy_changed"),
        (PlanStoreError::CampaignLocked, "incident_lock"),
        (PlanStoreError::RuntimeDisabled, "runtime_gate_revoked"),
        (PlanStoreError::QuotaExceeded, "quota_revoked"),
        (PlanStoreError::NotFound, "write_permit_unavailable"),
        (PlanStoreError::InvalidState, "write_permit_unavailable"),
        (PlanStoreError::Expired, "write_permit_unavailable"),
        (
            PlanStoreError::PrepareLimitExceeded,
            "write_permit_unavailable",
        ),
        (PlanStoreError::Busy, "write_permit_unavailable"),
        (PlanStoreError::ApplyInProgress, "write_permit_unavailable"),
        (PlanStoreError::InvalidPlan, "write_permit_unavailable"),
        (PlanStoreError::Unavailable, "write_permit_unavailable"),
    ] {
        assert_eq!(
            guarded_write_permit_error_class(&WritePermitFailure::Store(error)),
            expected
        );
    }
    assert_eq!(
        write_failure_finish(&WbWriteError::InvalidRequest("changes")),
        (WbPlanStatus::Failed, "wb_write_rejected")
    );
    assert_eq!(
        write_failure_finish(&WbWriteError::Ambiguous {
            reason: "timeout",
            request_id: None,
        }),
        (WbPlanStatus::Ambiguous, "wb_write_ambiguous")
    );
}

async fn run_wb_runtime_happy_path(
    database_url: Result<String, std::env::VarError>,
    admin_url: Result<String, std::env::VarError>,
) {
    enum Revocation {
        MissingFile,
        MissingActor,
        RevokedAccess,
    }

    let (Ok(database_url), Ok(admin_url)) = (database_url, admin_url) else {
        return;
    };
    let _database_guard = CONTROL_DB_TEST_LOCK.lock().await;
    let config = crate::control::plan::validate_control_database_url(&database_url).unwrap();
    let plans = Arc::new(WbPlanRepository::connect(&config).await.unwrap());
    let (admin, admin_connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let admin_connection_task = tokio::spawn(admin_connection);
    clean_control_tables(&admin).await;

    let advert_ids = (77..=95).collect::<Vec<_>>();
    let fixtures = Fixtures::new_wb_with_adverts(ControlMode::Enabled, &advert_ids);
    let base_server = fixtures.authenticated_server();
    plans
        .register_policy(
            base_server.policy.version,
            base_server.policy.revision,
            base_server.policy.digest(),
            Utc::now(),
        )
        .await
        .unwrap();

    let (prepare_services, prepare_requests, no_write_requests) =
        test_wb_services(Arc::clone(&plans), vec![(200, wb_details(77, 1000))], None);
    assert!(no_write_requests.is_none());
    let debug = format!("{prepare_services:?}");
    assert!(debug.contains("WbControlServices"));
    assert!(debug.contains("wb_one"));
    assert!(debug.contains("<configured>"));
    assert!(debug.contains("writer_configured: false"));

    let refused = fixtures
        .server()
        .with_wb_control_services(prepare_services.clone());
    assert!(refused.wb.is_none());
    let prepare_server = base_server
        .clone()
        .with_wb_control_services(prepare_services);
    assert!(prepare_server.wb_services("wb_one").is_ok());
    assert_eq!(
        prepare_server.wb_services("other").unwrap_err(),
        "CONTROL_ACCESS_DENIED: WB account находится вне runtime scope"
    );

    let prepared = prepare_server
        .prepare_wb_bid_update(
            fixtures.identity("manager"),
            Parameters(PrepareWbBidPlanInput {
                account_id: "wb_one".to_owned(),
                advert_id: 77,
                changes: wb_requested(1050),
            }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(prepared.status, WbPlanStatus::Prepared);
    assert_eq!(prepared.changes[0].before_bid_kopecks, 1000);
    assert_eq!(prepared.changes[0].bid_kopecks, 1050);
    assert!(
        prepare_requests
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .starts_with("GET /api/advert/v2/adverts?ids=77 HTTP/1.1\r\n")
    );

    let (mut foreign_services, _, _) = test_wb_services(Arc::clone(&plans), Vec::new(), None);
    foreign_services.account_id = "other".to_owned();
    let foreign_server = base_server
        .clone()
        .with_wb_control_services(foreign_services);
    assert_eq!(
        foreign_server
            .approve_wb_bid_plan(
                fixtures.identity("approver"),
                Parameters(ApproveWbBidPlanInput {
                    plan_id: prepared.plan_id.clone(),
                    plan_digest: prepared.plan_digest.clone(),
                    approval_reference: "server-test/foreign-runtime".to_owned(),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_ACCESS_DENIED: WB account находится вне runtime scope"
    );

    assert_eq!(
        prepare_server
            .approve_wb_bid_plan(
                fixtures.identity("approver"),
                Parameters(ApproveWbBidPlanInput {
                    plan_id: prepared.plan_id.clone(),
                    plan_digest: "d".repeat(64),
                    approval_reference: "server-test/wrong-digest".to_owned(),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_PLAN_CHANGED"
    );
    assert!(
        prepare_server
            .approve_wb_bid_plan(
                fixtures.identity("observer"),
                Parameters(ApproveWbBidPlanInput {
                    plan_id: prepared.plan_id.clone(),
                    plan_digest: prepared.plan_digest.clone(),
                    approval_reference: "server-test/observer".to_owned(),
                }),
            )
            .await
            .err()
            .unwrap()
            .starts_with(ACCESS_DENIED)
    );
    assert!(
        prepare_server
            .approve_wb_bid_plan(
                fixtures.identity("manager"),
                Parameters(ApproveWbBidPlanInput {
                    plan_id: prepared.plan_id.clone(),
                    plan_digest: prepared.plan_digest.clone(),
                    approval_reference: "server-test/self".to_owned(),
                }),
            )
            .await
            .err()
            .unwrap()
            .starts_with(ACCESS_DENIED)
    );
    let approved = prepare_server
        .approve_wb_bid_plan(
            fixtures.identity("approver"),
            Parameters(ApproveWbBidPlanInput {
                plan_id: prepared.plan_id.clone(),
                plan_digest: prepared.plan_digest.clone(),
                approval_reference: "server-test/approval".to_owned(),
            }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(approved.status, WbPlanStatus::Approved);
    assert_eq!(approved.approval.unwrap().approver_id, "approver");

    let durable = prepare_server
        .wb_bid_plan_status(
            fixtures.identity("manager"),
            Parameters(WbPlanInput {
                plan_id: prepared.plan_id.clone(),
            }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(durable.status, WbPlanStatus::Approved);
    enable_test_gates(&admin, "wb_one", 77).await;

    let (apply_services, read_requests, write_requests) = test_wb_services(
        Arc::clone(&plans),
        vec![(200, wb_details(77, 1000)), (200, wb_details(77, 1050))],
        Some(vec![(200, "{}".to_owned())]),
    );
    let apply_server = base_server.clone().with_wb_control_services(apply_services);
    let status = apply_server
        .control_status(
            fixtures.identity("manager"),
            Parameters(EmptyInput::default()),
        )
        .await
        .unwrap()
        .0;
    assert!(status.write_executor_configured);
    assert!(status.credentials_loaded);
    assert!(status.marketplace_egress_enabled);
    assert!(status.persistence_configured);

    let applied = apply_server
        .apply_wb_bid_plan(
            fixtures.identity("manager"),
            Parameters(ApplyWbBidPlanInput {
                plan_id: prepared.plan_id.clone(),
                plan_digest: prepared.plan_digest.clone(),
            }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(applied.status, WbPlanStatus::Applied);
    assert!(!applied.requires_reconciliation);
    for expected_bid in [1000, 1050] {
        let request = read_requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(request.starts_with("GET /api/advert/v2/adverts?ids=77 HTTP/1.1\r\n"));
        assert!(wb_details(77, expected_bid).contains(&expected_bid.to_string()));
    }
    let write_request = write_requests
        .unwrap()
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(write_request.starts_with("PATCH /api/advert/v1/bids HTTP/1.1\r\n"));
    assert!(write_request.contains("test-write-token"));
    assert!(write_request.contains("\"bid_kopecks\":1050"));

    let reconciled = apply_server
        .reconcile_wb_bid_plan(
            fixtures.identity("manager"),
            Parameters(WbPlanInput {
                plan_id: prepared.plan_id,
            }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(reconciled.status, WbPlanStatus::Applied);

    assert_eq!(
        base_server
            .approve_wb_bid_plan(
                fixtures.identity("approver"),
                Parameters(ApproveWbBidPlanInput {
                    plan_id: "a".repeat(64),
                    plan_digest: "b".repeat(64),
                    approval_reference: "server-test/no-runtime".to_owned(),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_DISABLED: WB plan store не настроен"
    );
    assert_eq!(
        base_server
            .apply_wb_bid_plan(
                fixtures.identity("manager"),
                Parameters(ApplyWbBidPlanInput {
                    plan_id: "a".repeat(64),
                    plan_digest: "b".repeat(64),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_DISABLED: WB runtime не настроен"
    );
    assert_eq!(
        prepare_server
            .apply_wb_bid_plan(
                fixtures.identity("manager"),
                Parameters(ApplyWbBidPlanInput {
                    plan_id: "a".repeat(64),
                    plan_digest: "b".repeat(64),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_DISABLED: WB write executor не настроен"
    );
    assert_eq!(
        apply_server
            .apply_wb_bid_plan(
                fixtures.identity("manager"),
                Parameters(ApplyWbBidPlanInput {
                    plan_id: reconciled.plan_id.clone(),
                    plan_digest: "e".repeat(64),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_PLAN_CHANGED"
    );
    assert_eq!(
        apply_server
            .wb_bid_plan_status(
                fixtures.identity("manager"),
                Parameters(WbPlanInput {
                    plan_id: "f".repeat(64),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_PLAN_NOT_FOUND"
    );

    let (wrong_seller_services, _, _) = test_wb_services(Arc::clone(&plans), Vec::new(), None);
    let mut wrong_seller_services = wrong_seller_services;
    wrong_seller_services.seller_sid = "22222222-2222-4222-8222-222222222222".to_owned();
    let wrong_seller_server = base_server
        .clone()
        .with_wb_control_services(wrong_seller_services);
    assert_eq!(
        wrong_seller_server
            .prepare_wb_bid_update(
                fixtures.identity("manager"),
                Parameters(PrepareWbBidPlanInput {
                    account_id: "wb_one".to_owned(),
                    advert_id: 78,
                    changes: wb_requested(1050),
                }),
            )
            .await
            .err()
            .unwrap(),
        "CONTROL_ACCESS_DENIED: WB seller sid находится вне runtime scope"
    );

    for (advert_id, response, requested_bid, expected) in [
        (
            78,
            (500, "{}".to_owned()),
            1050,
            "CONTROL_PREFLIGHT_FAILED:",
        ),
        (
            79,
            (200, "{}".to_owned()),
            1050,
            "CONTROL_PREFLIGHT_FAILED:",
        ),
        (
            80,
            (200, wb_details(80, 1000)),
            1200,
            "CONTROL_POLICY_DENIED:",
        ),
    ] {
        let (services, requests, _) = test_wb_services(Arc::clone(&plans), vec![response], None);
        let server = base_server.clone().with_wb_control_services(services);
        let error = server
            .prepare_wb_bid_update(
                fixtures.identity("manager"),
                Parameters(PrepareWbBidPlanInput {
                    account_id: "wb_one".to_owned(),
                    advert_id,
                    changes: wb_requested(requested_bid),
                }),
            )
            .await
            .err()
            .unwrap();
        assert!(error.starts_with(expected), "{error}");
        assert!(
            requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with(&format!(
                    "GET /api/advert/v2/adverts?ids={advert_id} HTTP/1.1\r\n"
                ))
        );
    }

    let mismatch = apply_test_plan(
        &fixtures,
        &base_server,
        Arc::clone(&plans),
        &admin,
        81,
        vec![(200, wb_details(81, 1000)), (200, wb_details(81, 1000))],
        (200, "{}".to_owned()),
    )
    .await;
    assert_eq!(mismatch.status, WbPlanStatus::ReconciliationRequired);
    assert_eq!(
        mismatch.last_error_class.as_deref(),
        Some("readback_mismatch")
    );
    assert!(mismatch.requires_reconciliation);

    let (mismatch_services, _, _) =
        test_wb_services(Arc::clone(&plans), vec![(200, wb_details(81, 1000))], None);
    let mismatch_server = base_server
        .clone()
        .with_wb_control_services(mismatch_services);
    let still_mismatched = mismatch_server
        .reconcile_wb_bid_plan(
            fixtures.identity("manager"),
            Parameters(WbPlanInput {
                plan_id: mismatch.plan_id.clone(),
            }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(
        still_mismatched.status,
        WbPlanStatus::ReconciliationRequired
    );

    let (failed_reconcile_services, _, _) =
        test_wb_services(Arc::clone(&plans), vec![(500, "{}".to_owned())], None);
    let failed_reconcile = base_server
        .clone()
        .with_wb_control_services(failed_reconcile_services)
        .reconcile_wb_bid_plan(
            fixtures.identity("manager"),
            Parameters(WbPlanInput {
                plan_id: mismatch.plan_id.clone(),
            }),
        )
        .await
        .err()
        .unwrap();
    assert!(
        failed_reconcile.starts_with("CONTROL_RECONCILIATION_FAILED:"),
        "{failed_reconcile}"
    );

    let (matching_reconcile_services, _, _) =
        test_wb_services(Arc::clone(&plans), vec![(200, wb_details(81, 1050))], None);
    let matched = base_server
        .clone()
        .with_wb_control_services(matching_reconcile_services)
        .reconcile_wb_bid_plan(
            fixtures.identity("manager"),
            Parameters(WbPlanInput {
                plan_id: mismatch.plan_id,
            }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(matched.status, WbPlanStatus::Applied);

    let unavailable = apply_test_plan(
        &fixtures,
        &base_server,
        Arc::clone(&plans),
        &admin,
        82,
        vec![(200, wb_details(82, 1000)), (500, "{}".to_owned())],
        (200, "{}".to_owned()),
    )
    .await;
    assert_eq!(unavailable.status, WbPlanStatus::ReconciliationRequired);
    assert_eq!(
        unavailable.last_error_class.as_deref(),
        Some("readback_unavailable")
    );

    let ambiguous = apply_test_plan(
        &fixtures,
        &base_server,
        Arc::clone(&plans),
        &admin,
        83,
        vec![(200, wb_details(83, 1000))],
        (400, "{}".to_owned()),
    )
    .await;
    assert_eq!(ambiguous.status, WbPlanStatus::Ambiguous);
    assert_eq!(
        ambiguous.last_error_class.as_deref(),
        Some("wb_write_ambiguous")
    );

    let preflight_rejected = apply_test_plan(
        &fixtures,
        &base_server,
        Arc::clone(&plans),
        &admin,
        84,
        vec![(500, "{}".to_owned())],
        (200, "{}".to_owned()),
    )
    .await;
    assert_eq!(preflight_rejected.status, WbPlanStatus::Rejected);
    assert_eq!(
        preflight_rejected.last_error_class.as_deref(),
        Some("preflight_read_failed")
    );
    let (rejected_services, _, _) = test_wb_services(Arc::clone(&plans), Vec::new(), None);
    let rejected_error = base_server
        .clone()
        .with_wb_control_services(rejected_services)
        .reconcile_wb_bid_plan(
            fixtures.identity("manager"),
            Parameters(WbPlanInput {
                plan_id: preflight_rejected.plan_id,
            }),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(
        rejected_error,
        "CONTROL_PLAN_STATE: план не требует reconciliation"
    );

    let precondition_rejected = apply_test_plan(
        &fixtures,
        &base_server,
        Arc::clone(&plans),
        &admin,
        85,
        vec![(200, wb_details(85, 900))],
        (200, "{}".to_owned()),
    )
    .await;
    assert_eq!(precondition_rejected.status, WbPlanStatus::Rejected);
    assert_eq!(
        precondition_rejected.last_error_class.as_deref(),
        Some("precondition_changed")
    );

    let original_registry = fs::read(&fixtures.registry_path).unwrap();
    for (advert_id, revocation) in [
        (86, Revocation::MissingFile),
        (87, Revocation::MissingActor),
        (88, Revocation::RevokedAccess),
    ] {
        let approved =
            prepare_approved_plan(&fixtures, &base_server, Arc::clone(&plans), advert_id).await;
        enable_test_gates(&admin, "wb_one", advert_id).await;
        let (services, _, _) = test_wb_services(
            Arc::clone(&plans),
            vec![(200, wb_details(advert_id, 1000))],
            Some(vec![(200, "{}".to_owned())]),
        );
        let server = base_server.clone().with_wb_control_services(services);
        let identity = fixtures.identity("manager");
        match revocation {
            Revocation::MissingFile => fs::remove_file(&fixtures.registry_path).unwrap(),
            Revocation::MissingActor => {
                let mut registry: Value = serde_json::from_slice(&original_registry).unwrap();
                registry["actors"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|actor| actor["id"] != "manager");
                registry["accounts"][0]["manager_id"] = json!("observer");
                fs::write(
                    &fixtures.registry_path,
                    serde_json::to_vec(&registry).unwrap(),
                )
                .unwrap();
            }
            Revocation::RevokedAccess => {
                let mut registry: Value = serde_json::from_slice(&original_registry).unwrap();
                registry["accounts"][0]["manager_id"] = json!("observer");
                fs::write(
                    &fixtures.registry_path,
                    serde_json::to_vec(&registry).unwrap(),
                )
                .unwrap();
            }
        }
        let rejected = server
            .apply_wb_bid_plan(
                identity,
                Parameters(ApplyWbBidPlanInput {
                    plan_id: approved.plan_id,
                    plan_digest: approved.plan_digest,
                }),
            )
            .await
            .unwrap()
            .0;
        fs::write(&fixtures.registry_path, &original_registry).unwrap();
        assert_eq!(rejected.status, WbPlanStatus::Rejected);
        assert_eq!(rejected.last_error_class.as_deref(), Some("access_revoked"));
    }

    let gate_revoked_plan =
        prepare_approved_plan(&fixtures, &base_server, Arc::clone(&plans), 89).await;
    enable_test_gates(&admin, "wb_one", 89).await;
    let (paused_base_url, paused_request, release_response) = paused_http(wb_details(89, 1000));
    let paused_reader = WbClient::new_for_test(
        Duration::from_secs(2),
        BTreeMap::from([(
            "wb_one".to_owned(),
            WbCredentials {
                token: "test-read-token".to_owned(),
            },
        )]),
        &paused_base_url,
        &paused_base_url,
    );
    let (write_base_url, _) = mock_http(vec![(200, "{}".to_owned())]);
    let gate_revoked_services = WbControlServices {
        account_id: "wb_one".to_owned(),
        seller_sid: "123e4567-e89b-42d3-a456-426614174000".to_owned(),
        reader: Arc::new(paused_reader),
        writer: Some(Arc::new(WbBidWriteClient::new_for_test(
            &write_base_url,
            "test-write-token",
            Duration::from_secs(1),
        ))),
        plans: Arc::clone(&plans),
    };
    let gate_revoked_server = base_server
        .clone()
        .with_wb_control_services(gate_revoked_services);
    let gate_revoked_identity = fixtures.identity("manager");
    let gate_revoked_apply = tokio::spawn(async move {
        gate_revoked_server
            .apply_wb_bid_plan(
                gate_revoked_identity,
                Parameters(ApplyWbBidPlanInput {
                    plan_id: gate_revoked_plan.plan_id,
                    plan_digest: gate_revoked_plan.plan_digest,
                }),
            )
            .await
    });
    let request = tokio::task::spawn_blocking(move || {
        paused_request.recv_timeout(Duration::from_secs(2)).unwrap()
    })
    .await
    .unwrap();
    assert!(request.starts_with("GET /api/advert/v2/adverts?ids=89 HTTP/1.1\r\n"));
    admin
        .execute(
            "UPDATE control.wb_runtime_gates \
             SET enabled=false, revision=revision+1, reason='server_test_revoked', \
                 updated_by='server_test' WHERE gate_key='global'",
            &[],
        )
        .await
        .unwrap();
    release_response.send(()).unwrap();
    let gate_rejected = gate_revoked_apply.await.unwrap().unwrap().0;
    assert_eq!(gate_rejected.status, WbPlanStatus::Rejected);
    assert_eq!(
        gate_rejected.last_error_class.as_deref(),
        Some("runtime_gate_revoked")
    );

    let stale_plan = prepare_approved_plan(&fixtures, &base_server, Arc::clone(&plans), 90).await;
    enable_test_gates(&admin, "wb_one", 90).await;
    plans
        .claim_for_apply(WbApplyContext {
            plan_id: &stale_plan.plan_id,
            actor_id: "manager",
            expected_plan_digest: &stale_plan.plan_digest,
            expected_schema_version: base_server.policy.version,
            expected_policy_revision: base_server.policy.revision,
            expected_policy_digest: base_server.policy.digest(),
            now: Utc::now(),
        })
        .await
        .unwrap();
    // Simulate three minutes of process downtime without sleeping. The
    // resulting row is a valid Applying plan whose lease-age alone is old.
    admin
        .batch_execute("SET session_replication_role = replica")
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE control.wb_plans \
             SET apply_started_at=clock_timestamp()-interval '4 minutes' \
             WHERE plan_id=$1",
            &[&stale_plan.plan_id],
        )
        .await
        .unwrap();
    admin
        .batch_execute("SET session_replication_role = origin")
        .await
        .unwrap();
    let (stale_services, _, _) =
        test_wb_services(Arc::clone(&plans), vec![(200, wb_details(90, 1050))], None);
    let stale_reconciled = base_server
        .clone()
        .with_wb_control_services(stale_services)
        .reconcile_wb_bid_plan(
            fixtures.identity("manager"),
            Parameters(WbPlanInput {
                plan_id: stale_plan.plan_id,
            }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(stale_reconciled.status, WbPlanStatus::Applied);

    let services = apply_server.wb.as_ref().unwrap();
    let registry = base_server.registry.load().unwrap();
    let manager = registry.actor("manager").unwrap();
    let stored_plan = plans
        .load_for_actor(&reconciled.plan_id, "manager")
        .await
        .unwrap();
    let (invalid_snapshot_services, _, _) =
        test_wb_services(Arc::clone(&plans), vec![(200, "{}".to_owned())], None);
    assert!(
        read_plan_snapshot(&invalid_snapshot_services, &stored_plan)
            .await
            .is_err()
    );
    authorize_plan_account_access(&registry, manager, services, &stored_plan).unwrap();
    authorize_plan_apply(
        &base_server.policy,
        &registry,
        manager,
        services,
        &stored_plan,
    )
    .unwrap();

    let mut wrong_actor_plan = stored_plan.clone();
    wrong_actor_plan.actor_id = "observer".to_owned();
    assert!(
        authorize_plan_account_access(&registry, manager, services, &wrong_actor_plan).is_err()
    );
    let mut wrong_account_plan = stored_plan.clone();
    wrong_account_plan.account_id = "other".to_owned();
    assert!(
        authorize_plan_account_access(&registry, manager, services, &wrong_account_plan).is_err()
    );
    let mut wrong_runtime_seller = stored_plan.clone();
    wrong_runtime_seller.before.seller_sid = "22222222-2222-4222-8222-222222222222".to_owned();
    assert!(
        authorize_plan_account_access(&registry, manager, services, &wrong_runtime_seller).is_err()
    );
    let mut no_account_registry = (*registry).clone();
    no_account_registry.accounts.clear();
    assert!(
        authorize_plan_account_access(
            &no_account_registry,
            no_account_registry.actor("manager").unwrap(),
            services,
            &stored_plan,
        )
        .is_err()
    );
    let mut revoked_registry = (*registry).clone();
    revoked_registry.accounts[0].manager_id = "revoked".to_owned();
    revoked_registry.actors[0].account_ids.clear();
    assert!(
        authorize_plan_account_access(
            &revoked_registry,
            revoked_registry.actor("manager").unwrap(),
            services,
            &stored_plan,
        )
        .is_err()
    );
    let mut rebound_registry = (*registry).clone();
    rebound_registry.accounts[0]
        .wildberries
        .as_mut()
        .unwrap()
        .seller_sid = Some("22222222-2222-4222-8222-222222222222".to_owned());
    assert!(
        authorize_plan_account_access(
            &rebound_registry,
            rebound_registry.actor("manager").unwrap(),
            services,
            &stored_plan,
        )
        .is_err()
    );

    let mut changed_policy_plan = stored_plan.clone();
    changed_policy_plan.policy_digest = "d".repeat(64);
    assert!(
        authorize_plan_apply(
            &base_server.policy,
            &registry,
            manager,
            services,
            &changed_policy_plan,
        )
        .is_err()
    );
    let mut unapproved_plan = stored_plan.clone();
    unapproved_plan.approval = None;
    assert!(
        authorize_plan_apply(
            &base_server.policy,
            &registry,
            manager,
            services,
            &unapproved_plan,
        )
        .is_err()
    );
    let mut missing_approver_registry = (*registry).clone();
    missing_approver_registry
        .actors
        .retain(|actor| actor.id != "approver");
    assert!(
        authorize_plan_apply(
            &base_server.policy,
            &missing_approver_registry,
            missing_approver_registry.actor("manager").unwrap(),
            services,
            &stored_plan,
        )
        .is_err()
    );
    let mut revoked_approver_registry = (*registry).clone();
    revoked_approver_registry
        .actors
        .iter_mut()
        .find(|actor| actor.id == "approver")
        .unwrap()
        .account_ids
        .clear();
    assert!(
        authorize_plan_apply(
            &base_server.policy,
            &revoked_approver_registry,
            revoked_approver_registry.actor("manager").unwrap(),
            services,
            &stored_plan,
        )
        .is_err()
    );

    let (readiness_services, _, _) = test_wb_services(Arc::clone(&plans), Vec::new(), None);
    let readiness_server = base_server
        .clone()
        .with_wb_control_services(readiness_services);
    assert!(readiness_server.readiness().await.is_ok());

    admin
        .batch_execute("ALTER ROLE control_writer NOLOGIN")
        .await
        .unwrap();
    admin
        .execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE usename = 'control_writer' AND pid <> pg_backend_pid()",
            &[],
        )
        .await
        .unwrap();
    let unavailable = readiness_server.readiness().await;
    admin
        .batch_execute("ALTER ROLE control_writer LOGIN")
        .await
        .unwrap();
    assert!(unavailable.is_err());

    clean_control_tables(&admin).await;
    drop(admin);
    admin_connection_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn wb_runtime_happy_path_is_durable_and_uses_exact_http_calls() {
    Box::pin(run_wb_runtime_happy_path(
        Err(std::env::VarError::NotPresent),
        Err(std::env::VarError::NotPresent),
    ))
    .await;
    Box::pin(run_wb_runtime_happy_path(
        std::env::var("WB_CONTROL_TEST_DATABASE_URL"),
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
    ))
    .await;
}

#[test]
fn inventory_exposes_local_inspection_and_guarded_wb_workflow() {
    let fixtures = Fixtures::new(false);
    let server = fixtures.server();
    assert_eq!(server.tool_router.map.len(), 12);
    let status = server
        .tool_router
        .map
        .get("ozon_ads_control_status")
        .unwrap();
    assert_eq!(
        status.attr.annotations.as_ref().unwrap().read_only_hint,
        Some(true)
    );
    let apply = server
        .tool_router
        .map
        .get("wb_promotion_apply_bid_plan")
        .unwrap();
    let annotations = apply.attr.annotations.as_ref().unwrap();
    assert_eq!(annotations.read_only_hint, Some(false));
    assert_eq!(annotations.destructive_hint, Some(true));
    assert_eq!(annotations.idempotent_hint, Some(true));
    assert_eq!(annotations.open_world_hint, Some(true));
    let info = server.get_info();
    assert!(
        info.instructions
            .unwrap()
            .contains("reconciliation_required")
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
async fn control_readiness_tracks_the_hot_reloaded_registry() {
    let fixtures = Fixtures::new(false);
    let server = fixtures.authenticated_server();
    assert!(
        <ControlMcp as HttpMcpServer>::readiness(&server)
            .await
            .is_ok()
    );

    fs::write(&fixtures.registry_path, b"{").unwrap();
    assert!(
        <ControlMcp as HttpMcpServer>::readiness(&server)
            .await
            .is_err()
    );
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
    assert_eq!(
        names,
        [
            "ozon_ads_control_scope",
            "ozon_ads_control_status",
            "ozon_performance_apply_campaign_launch",
            "ozon_performance_approve_campaign_launch",
            "ozon_performance_prepare_campaign_launch",
            "ozon_performance_preview_campaign_launch",
            "ozon_performance_reconcile_campaign_launch",
            "wb_promotion_apply_bid_plan",
            "wb_promotion_approve_bid_plan",
            "wb_promotion_bid_plan_status",
            "wb_promotion_prepare_bid_update",
            "wb_promotion_reconcile_bid_plan",
        ]
    );

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
    assert_eq!(result["write_executor_configured"], false);
    assert_eq!(result["runtime_gates_required"], true);
}
