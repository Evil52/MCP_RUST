use super::*;
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{AuthorizationFixture, Database},
    plan::CONTROL_DB_TEST_LOCK,
};

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run through scripts/with-position-test-db.sh"]
async fn approving_a_naturally_expired_plan_persists_expiry_without_an_approval() {
    let _serial = CONTROL_DB_TEST_LOCK.lock().await;
    let mut database = Database::connect()
        .await
        .expect("disposable fixture URLs are required");
    let authorization = AuthorizationFixture::new();
    database.prepare_plan(&authorization).await;
    database
        .admin
        .batch_execute("TRUNCATE control.ozon_campaign_plans CASCADE")
        .await
        .unwrap();
    let (plan_id, plan_digest) = seed_expired_plan(&mut database.admin, &authorization).await;
    let before = database.planner.load(&plan_id).await.unwrap();
    assert_eq!(before.status, OzonLaunchStatus::Prepared);
    assert!(before.expires_at < Utc::now());
    assert_eq!(
        database
            .planner
            .approve(&plan_id, "approver", &plan_digest, "boundary/expired")
            .await
            .err(),
        Some(OzonPlanStoreError::Expired)
    );
    let after = database.planner.load(&plan_id).await.unwrap();
    assert_eq!(after.status, OzonLaunchStatus::Expired);
    assert!(after.finished_at.is_some());
    assert!(after.approval.is_none());
    assert_eq!(
        database
            .admin
            .query_one(
                "SELECT count(*) FROM control.ozon_campaign_plan_approvals WHERE plan_id=$1",
                &[&plan_id]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
}

async fn seed_expired_plan(
    admin: &mut tokio_postgres::Client,
    authorization: &AuthorizationFixture,
) -> (String, String) {
    let mut manifest = authorization.manifest();
    let created_at = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap()
        - Duration::minutes(16);
    let expires_at = created_at + PLAN_TTL;
    let plan_digest = digest_fields(&[
        b"mcp-ozon/ozon-plan/v1",
        manifest.manifest_digest.as_bytes(),
        &created_at.timestamp_micros().to_be_bytes(),
        &expires_at.timestamp_micros().to_be_bytes(),
    ]);
    let plan_id = digest_fields(&[b"mcp-ozon/ozon-plan-id/v1", plan_digest.as_bytes()]);
    manifest.create_request.title = provider_title_for_plan_id(&plan_id);
    assert!(manifest.has_exact_persisted_integrity(&plan_id));
    let manifest_json = serde_json::to_string(&manifest).unwrap();
    let tx = admin.transaction().await.unwrap();
    // Only setup bypasses the insert-time freshness check. The historical
    // row keeps the exact digest, ID and 15-minute lifetime it would have
    // acquired sixteen minutes ago; every transition guard remains enabled.
    tx.batch_execute(
        "ALTER TABLE control.ozon_campaign_plans DISABLE TRIGGER ozon_plans_validate_insert",
    )
    .await
    .unwrap();
    tx.execute(
        "INSERT INTO control.ozon_campaign_plans(plan_id,plan_digest,actor_id,account_id,sku,schema_version,policy_revision,policy_digest,manifest_json,status,created_at,expires_at) VALUES($1,$2,'actor','account',1001,1,7,$3,$4,'prepared',$5,$6)",
        &[&plan_id, &plan_digest, &authorization.policy.digest(), &manifest_json, &created_at, &expires_at],
    ).await.unwrap();
    tx.batch_execute(
        "ALTER TABLE control.ozon_campaign_plans ENABLE TRIGGER ozon_plans_validate_insert",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    (plan_id, plan_digest)
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run through scripts/with-position-test-db.sh"]
async fn denied_plan_insert_rolls_back_and_database_uniqueness_is_classified_separately() {
    let _serial = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("disposable fixture URLs are required");
    let authorization = AuthorizationFixture::new();
    database.prepare_plan(&authorization).await;
    database.admin.batch_execute("TRUNCATE control.ozon_campaign_plans CASCADE; REVOKE INSERT ON control.ozon_campaign_plans FROM ozon_control_planner").await.unwrap();
    let denied = database.planner.create(&authorization.manifest()).await;
    database
        .admin
        .batch_execute("GRANT INSERT ON control.ozon_campaign_plans TO ozon_control_planner")
        .await
        .unwrap();
    assert_eq!(denied.err(), Some(OzonPlanStoreError::Unavailable));
    assert_eq!(
        database
            .admin
            .query_one("SELECT count(*) FROM control.ozon_campaign_plans", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    let recovered = database
        .planner
        .create(&authorization.manifest())
        .await
        .unwrap();
    assert_eq!(recovered.status, OzonLaunchStatus::Prepared);

    // Obtain a real PostgreSQL unique-violation value at an isolated primary
    // key boundary; the mapper must retain its domain classification while
    // permission failures remain unavailable and their diagnostics stay local.
    database.admin.batch_execute("CREATE TEMP TABLE plan_insert_error_boundary(id bigint PRIMARY KEY); INSERT INTO plan_insert_error_boundary VALUES(1)").await.unwrap();
    let unique = database
        .admin
        .execute("INSERT INTO plan_insert_error_boundary VALUES(1)", &[])
        .await
        .unwrap_err();
    assert_eq!(map_plan_insert(&unique), OzonPlanStoreError::SkuLocked);
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run through scripts/with-position-test-db.sh"]
async fn legacy_human_title_remains_auditable_but_cannot_enqueue_a_create() {
    let _serial = CONTROL_DB_TEST_LOCK.lock().await;
    let mut database = Database::connect()
        .await
        .expect("disposable fixture URLs are required");
    let authorization = AuthorizationFixture::new();
    database.prepare_plan(&authorization).await;
    database
        .admin
        .batch_execute("TRUNCATE control.ozon_campaign_plans CASCADE")
        .await
        .unwrap();
    let plan_id = seed_legacy_plan(&mut database.admin, &authorization).await;
    let legacy = database.planner.load(&plan_id).await.unwrap();
    assert_eq!(legacy.status, OzonLaunchStatus::Prepared);
    assert_eq!(
        legacy.manifest.create_request.title,
        authorization.manifest().create_request.title
    );
    let approved = database
        .planner
        .approve(&plan_id, "approver", &legacy.plan_digest, "boundary/legacy")
        .await
        .unwrap();
    assert_eq!(
        database
            .planner
            .enqueue_launch(&plan_id, "actor", &legacy.plan_digest)
            .await
            .err(),
        Some(OzonPlanStoreError::InvalidPlan)
    );
    let after = database.planner.load(&plan_id).await.unwrap();
    assert_eq!(after.status, OzonLaunchStatus::Approved);
    assert_eq!(after.approval, approved.approval);
    assert!(after.execution_requested_at.is_none());
    assert_eq!(after.workflow_generation, 0);
    assert!(
        database
            .executor
            .claim_next_launch_action("account", "worker")
            .await
            .unwrap()
            .is_none()
    );
}

async fn seed_legacy_plan(
    admin: &mut tokio_postgres::Client,
    authorization: &AuthorizationFixture,
) -> String {
    let manifest = authorization.manifest();
    let created_at = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    let expires_at = created_at + PLAN_TTL;
    let plan_digest = digest_fields(&[
        b"mcp-ozon/ozon-plan/v1",
        manifest.manifest_digest.as_bytes(),
        &created_at.timestamp_micros().to_be_bytes(),
        &expires_at.timestamp_micros().to_be_bytes(),
    ]);
    let plan_id = digest_fields(&[b"mcp-ozon/ozon-plan-id/v1", plan_digest.as_bytes()]);
    assert!(manifest.has_exact_persisted_integrity(&plan_id));
    let manifest_json = serde_json::to_string(&manifest).unwrap();
    let tx = admin.transaction().await.unwrap();
    // Migration 024 permitted this exact human title. Only fixture insertion
    // bypasses migration 025's new-title requirement; the plan and workflow
    // transition guards stay enabled for every repository operation below.
    tx.batch_execute(
        "ALTER TABLE control.ozon_campaign_plans DISABLE TRIGGER ozon_plans_validate_insert",
    )
    .await
    .unwrap();
    tx.execute(
        "INSERT INTO control.ozon_campaign_plans(plan_id,plan_digest,actor_id,account_id,sku,schema_version,policy_revision,policy_digest,manifest_json,status,created_at,expires_at) VALUES($1,$2,'actor','account',1001,1,7,$3,$4,'prepared',$5,$6)",
        &[&plan_id, &plan_digest, &authorization.policy.digest(), &manifest_json, &created_at, &expires_at],
    ).await.unwrap();
    tx.batch_execute(
        "ALTER TABLE control.ozon_campaign_plans ENABLE TRIGGER ozon_plans_validate_insert",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    plan_id
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run through scripts/with-position-test-db.sh"]
async fn row_decoder_rejects_corrupt_manifest_digest_and_identity_without_changing_storage() {
    let _serial = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("disposable fixture URLs are required");
    let authorization = AuthorizationFixture::new();
    let plan = database.prepare_plan(&authorization).await;
    let mut changed_manifest = plan.manifest.clone();
    changed_manifest.create_request.title = "unrelated campaign".to_owned();
    // Project corrupt wire values over a real row. Persistent rows, grants,
    // triggers and constraints remain untouched by these decoder checks.
    for (column, value) in [
        (
            "p.manifest_json",
            serde_json::to_string(&changed_manifest).unwrap(),
        ),
        ("p.plan_digest", "a".repeat(64)),
        ("p.plan_id", "b".repeat(64)),
    ] {
        let query = format!(
            "{} WHERE p.plan_id=$1",
            PLAN_SELECT.replacen(column, "$2::text", 1)
        );
        let row = database
            .admin
            .query_one(&query, &[&plan.plan_id, &value])
            .await
            .unwrap();
        assert_eq!(
            plan_from_row(&row).err(),
            Some(OzonPlanStoreError::Unavailable),
            "{column}",
        );
    }
    let after = database.planner.load(&plan.plan_id).await.unwrap();
    assert_eq!(after.plan_digest, plan.plan_digest);
    assert_eq!(
        after.manifest.create_request.title,
        plan.manifest.create_request.title
    );
    assert_eq!(after.status, OzonLaunchStatus::Prepared);
}
