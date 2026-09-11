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
