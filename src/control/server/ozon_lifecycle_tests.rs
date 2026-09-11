use super::super::{
    authorization::{authorize_ozon_plan_apply, authorize_ozon_plan_approval, ozon_plan_target},
    contract::OzonCampaignPlanInput,
    presentation::ozon_plan_result,
};
use super::*;
use crate::control::ozon::{OzonCampaignPlan, OzonLaunchStatus};

#[tokio::test]
async fn ozon_preview_returns_the_exact_policy_bound_manifest_without_a_runtime() {
    let fixtures = Fixtures::new_ozon(ControlMode::PlanOnly);
    let server = fixtures.authenticated_server();
    let preview = server
        .preview_ozon_campaign_launch(
            fixtures.identity("launcher"),
            Parameters(PreviewOzonCampaignLaunchInput {
                spec: ozon_launch_spec(),
            }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(preview.spec, ozon_launch_spec());
    assert_eq!(preview.actor_id, "launcher");
    assert_eq!(preview.policy_digest, server.policy.digest());
    assert_eq!(preview.manifest_digest.len(), 64);
    assert!(server.ozon.is_none());
}

async fn clean_ozon_tables(admin: &tokio_postgres::Client) {
    admin.batch_execute("TRUNCATE TABLE control.ozon_static_guard_audit_events, control.ozon_campaign_audit_events, control.ozon_campaign_guards, control.ozon_campaign_action_reservations, control.ozon_campaign_plan_approvals, control.ozon_campaign_launch_workflows, control.ozon_campaign_plans, control.ozon_runtime_gates, control.ozon_policy_revisions RESTART IDENTITY CASCADE").await.unwrap();
}

fn assert_ozon_authorization_boundaries(
    server: &ControlMcp,
    registry: &AccessRegistry,
    plan: &OzonCampaignPlan,
) {
    let policy = &server.policy;
    let launcher = registry.actor("launcher").unwrap();
    let approver = registry.actor("approver").unwrap();
    assert!(ozon_plan_target(policy, plan).is_some());
    assert!(authorize_ozon_plan_approval(policy, registry, approver, plan).is_ok());
    assert!(authorize_ozon_plan_apply(policy, registry, launcher, "ozon_one", plan).is_ok());
    assert!(authorize_ozon_plan_apply(policy, registry, approver, "ozon_one", plan).is_err());
    assert!(authorize_ozon_plan_apply(policy, registry, launcher, "other", plan).is_err());
    assert!(authorize_ozon_plan_approval(policy, registry, launcher, plan).is_err());

    let mut changed = plan.clone();
    changed.policy_revision += 1;
    assert!(
        authorize_ozon_plan_approval(policy, registry, approver, &changed)
            .unwrap_err()
            .starts_with("CONTROL_POLICY_CHANGED")
    );
    let mut changed = plan.clone();
    changed.sku += 1;
    assert!(
        authorize_ozon_plan_approval(policy, registry, approver, &changed)
            .unwrap_err()
            .contains("target отсутствует")
    );
    let mut changed = plan.clone();
    changed.approval = None;
    assert_eq!(
        authorize_ozon_plan_apply(policy, registry, launcher, "ozon_one", &changed).unwrap_err(),
        "CONTROL_PLAN_APPROVAL_REQUIRED"
    );

    let mut changed = registry.clone();
    changed.accounts.clear();
    assert!(
        authorize_ozon_plan_approval(policy, &changed, approver, plan)
            .unwrap_err()
            .contains("account отсутствует")
    );
    assert!(
        authorize_ozon_plan_apply(policy, &changed, launcher, "ozon_one", plan)
            .unwrap_err()
            .contains("account отсутствует")
    );
    let mut changed = registry.clone();
    changed.actors.retain(|actor| actor.id != "launcher");
    assert!(
        authorize_ozon_plan_approval(policy, &changed, approver, plan)
            .unwrap_err()
            .contains("plan actor отсутствует")
    );
    let mut changed = registry.clone();
    changed.actors.retain(|actor| actor.id != "approver");
    assert!(
        authorize_ozon_plan_apply(policy, &changed, launcher, "ozon_one", plan)
            .unwrap_err()
            .contains("approver отсутствует")
    );
    let mut changed = registry.clone();
    changed.accounts[0].manager_id = "another_manager".to_owned();
    assert!(
        authorize_ozon_plan_apply(policy, &changed, launcher, "ozon_one", plan)
            .unwrap_err()
            .contains("доступ к Ozon account отозван")
    );
    assert!(authorize_ozon_plan_approval(policy, &changed, approver, plan).is_err());
}

async fn run_ozon_server_lifecycle(planner_url: &str, admin_url: &str) {
    let _database_guard = CONTROL_DB_TEST_LOCK.lock().await;
    let (admin, connection) = tokio_postgres::connect(admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let admin_task = tokio::spawn(connection);
    clean_ozon_tables(&admin).await;
    let plans = Arc::new(
        OzonPlanRepository::connect(&planner_url.parse().unwrap())
            .await
            .unwrap(),
    );
    let fixtures = Fixtures::new_ozon(ControlMode::Enabled);
    let base = fixtures.authenticated_server();
    plans
        .register_policy(
            base.policy.version,
            base.policy.revision,
            base.policy.digest(),
        )
        .await
        .unwrap();
    let services = OzonControlServices {
        account_id: "ozon_one".to_owned(),
        plans: Arc::clone(&plans),
    };
    let debug = format!("{services:?}");
    assert!(debug.contains("ozon_one"));
    assert!(debug.contains("<configured>"));
    let dev = ControlMcp::new_disabled(
        "launcher".to_owned(),
        base.registry.clone(),
        (*base.policy).clone(),
    )
    .with_ozon_control_services(services.clone());
    assert!(dev.ozon.is_none());
    let server = base.with_ozon_control_services(services);
    assert!(server.ozon_services("ozon_one").is_ok());
    assert!(
        server
            .ozon_services("other")
            .unwrap_err()
            .contains("runtime scope")
    );
    assert!(server.readiness().await.is_ok());
    let scope = server
        .control_scope(
            fixtures.identity("launcher"),
            Parameters(EmptyInput::default()),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(scope.ozon_campaign_launch_targets.len(), 1);
    assert_eq!(scope.ozon_campaign_launch_targets[0].skus, [1001]);

    let mut invalid_spec = ozon_launch_spec();
    invalid_spec.title.clear();
    let error = server
        .prepare_ozon_campaign_launch(
            fixtures.identity("launcher"),
            Parameters(PrepareOzonCampaignLaunchInput { spec: invalid_spec }),
        )
        .await
        .err()
        .expect("operation must be rejected");
    assert!(error.starts_with("CONTROL_POLICY_DENIED"));
    let prepared = server
        .prepare_ozon_campaign_launch(
            fixtures.identity("launcher"),
            Parameters(PrepareOzonCampaignLaunchInput {
                spec: ozon_launch_spec(),
            }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(prepared.status, OzonLaunchStatus::Prepared);
    assert_eq!(prepared.provider_identity_version, "plan_id_v1");
    assert!(prepared.approval.is_none());
    assert!(prepared.execution_requested_at.is_none());
    let mut legacy = plans.load(&prepared.plan_id).await.unwrap();
    legacy.manifest.create_request.title = "legacy-title".to_owned();
    assert_eq!(
        ozon_plan_result(&legacy).provider_identity_version,
        "legacy_title_v0"
    );

    let approved = server
        .approve_ozon_campaign_launch(
            fixtures.identity("approver"),
            Parameters(ApproveOzonCampaignLaunchInput {
                plan_id: prepared.plan_id.clone(),
                plan_digest: prepared.plan_digest.clone(),
                approval_reference: "test/ozon-control".to_owned(),
            }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(approved.status, OzonLaunchStatus::Approved);
    assert_eq!(approved.approval.unwrap().approver_id, "approver");
    let stored = plans.load(&prepared.plan_id).await.unwrap();
    let registry = server.registry.load().unwrap();
    assert_ozon_authorization_boundaries(&server, &registry, &stored);
    let digest_error = server
        .apply_ozon_campaign_launch(
            fixtures.identity("launcher"),
            Parameters(ApplyOzonCampaignLaunchInput {
                plan_id: prepared.plan_id.clone(),
                plan_digest: "0".repeat(64),
            }),
        )
        .await
        .err()
        .expect("operation must be rejected");
    assert_eq!(digest_error, "CONTROL_PLAN_CHANGED");
    for (gate_key, scope_kind, account_id, sku) in [
        ("global", "global", None, None),
        ("account/ozon_one", "account", Some("ozon_one"), None),
        ("sku/ozon_one/1001", "sku", Some("ozon_one"), Some(1001_i64)),
    ] {
        admin.execute("INSERT INTO control.ozon_runtime_gates(gate_key,scope_kind,account_id,sku,enabled,lease_expires_at,disabled_until,revision,reason,updated_by,updated_at) VALUES($1,$2,$3,$4,true,clock_timestamp()+interval '5 minutes',NULL,1,'test','test',clock_timestamp())", &[&gate_key, &scope_kind, &account_id, &sku]).await.unwrap();
    }
    let queued = server
        .apply_ozon_campaign_launch(
            fixtures.identity("launcher"),
            Parameters(ApplyOzonCampaignLaunchInput {
                plan_id: prepared.plan_id.clone(),
                plan_digest: prepared.plan_digest.clone(),
            }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(queued.status, OzonLaunchStatus::Approved);
    assert!(queued.execution_requested_at.is_some());
    assert_eq!(queued.campaign_id, None);
    assert_eq!(queued.workflow_generation, 0);
    let reconciled = server
        .reconcile_ozon_campaign_launch(
            fixtures.identity("launcher"),
            Parameters(OzonCampaignPlanInput {
                plan_id: prepared.plan_id.clone(),
            }),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(
        reconciled.execution_requested_at,
        queued.execution_requested_at
    );
    assert!(!reconciled.requires_reconciliation);
    let mut identity = fixtures.identity("launcher");
    let mut registry = (*identity.registry.take().unwrap()).clone();
    registry.accounts.clear();
    identity.registry = Some(Arc::new(registry));
    let error = server
        .reconcile_ozon_campaign_launch(
            identity,
            Parameters(OzonCampaignPlanInput {
                plan_id: prepared.plan_id.clone(),
            }),
        )
        .await
        .err()
        .expect("operation must be rejected");
    assert!(error.contains("account отсутствует"));
    let error = server
        .reconcile_ozon_campaign_launch(
            fixtures.identity("approver"),
            Parameters(OzonCampaignPlanInput {
                plan_id: prepared.plan_id,
            }),
        )
        .await
        .err()
        .expect("operation must be rejected");
    assert!(error.contains("actor scope"));

    admin
        .batch_execute("ALTER ROLE ozon_control_planner NOLOGIN")
        .await
        .unwrap();
    admin.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='ozon_control_planner' AND pid<>pg_backend_pid()", &[]).await.unwrap();
    let readiness = server.readiness().await;
    admin
        .batch_execute("ALTER ROLE ozon_control_planner LOGIN")
        .await
        .unwrap();
    assert!(readiness.is_err());
    clean_ozon_tables(&admin).await;
    drop(admin);
    admin_task.await.unwrap().unwrap();
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run through scripts/with-position-test-db.sh"]
async fn ozon_control_prepares_approves_and_queues_without_marketplace_credentials() {
    let planner_url = std::env::var("OZON_CONTROL_TEST_DATABASE_URL")
        .expect("disposable fixture provides the Ozon planner URL");
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL")
        .expect("disposable fixture provides the admin URL");
    Box::pin(run_ozon_server_lifecycle(&planner_url, &admin_url)).await;
}
