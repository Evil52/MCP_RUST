use super::*;
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{AuthorizationFixture, Database},
    plan::CONTROL_DB_TEST_LOCK,
};

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run through scripts/with-position-test-db.sh"]
async fn policy_registration_is_monotonic_idempotent_and_rolls_back_failed_inserts() {
    let _serial = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("disposable fixture URLs are required");
    let authorization = AuthorizationFixture::new();
    let plan = database.prepare_plan(&authorization).await;
    let digest = authorization.policy.digest();
    for (schema, revision) in [(0, 7), (1, 0)] {
        assert_eq!(
            database
                .planner
                .register_policy(schema, revision, digest)
                .await,
            Err(OzonPlanStoreError::InvalidPlan)
        );
    }
    assert_eq!(database.planner.register_policy(1, 7, digest).await, Ok(()));
    for (schema, revision, other_digest) in [
        (1, 6, digest.to_owned()),
        (2, 7, digest.to_owned()),
        (1, 7, "b".repeat(64)),
        (1, 8, digest.to_owned()),
    ] {
        assert_eq!(
            database
                .planner
                .register_policy(schema, revision, &other_digest)
                .await,
            Err(OzonPlanStoreError::PolicyChanged)
        );
    }
    database
        .admin
        .batch_execute("REVOKE INSERT ON control.ozon_policy_revisions FROM ozon_control_planner")
        .await
        .unwrap();
    let unavailable = database
        .planner
        .register_policy(1, 8, &"b".repeat(64))
        .await;
    database
        .admin
        .batch_execute("GRANT INSERT ON control.ozon_policy_revisions TO ozon_control_planner")
        .await
        .unwrap();
    assert_eq!(unavailable, Err(OzonPlanStoreError::Unavailable));
    assert_eq!(
        database
            .admin
            .query_one("SELECT count(*) FROM control.ozon_policy_revisions", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    database
        .planner
        .register_policy(1, 8, &"b".repeat(64))
        .await
        .unwrap();
    assert_eq!(
        database
            .planner
            .approve(
                &plan.plan_id,
                "approver",
                &plan.plan_digest,
                "test/policy-drift"
            )
            .await
            .err(),
        Some(OzonPlanStoreError::PolicyChanged)
    );
    assert_eq!(
        database.planner.load(&plan.plan_id).await.unwrap().status,
        OzonLaunchStatus::Prepared
    );
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run through scripts/with-position-test-db.sh"]
async fn approval_replays_preserve_the_original_authorization_and_expiry() {
    let _serial = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("disposable fixture URLs are required");
    let authorization = AuthorizationFixture::new();
    let plan = database.prepare_plan(&authorization).await;
    assert_eq!(
        database
            .planner
            .create(&authorization.manifest())
            .await
            .err(),
        Some(OzonPlanStoreError::SkuLocked)
    );
    assert_eq!(
        database
            .planner
            .enqueue_launch(&plan.plan_id, "actor", &plan.plan_digest)
            .await
            .err(),
        Some(OzonPlanStoreError::InvalidState)
    );
    assert_eq!(
        database
            .planner
            .approve(&plan.plan_id, "approver", &"a".repeat(64), "test/replay")
            .await
            .err(),
        Some(OzonPlanStoreError::PlanChanged)
    );
    assert_eq!(
        database
            .planner
            .approve(&plan.plan_id, "actor", &plan.plan_digest, "test/replay")
            .await
            .err(),
        Some(OzonPlanStoreError::InvalidState)
    );
    let approved = database
        .planner
        .approve(&plan.plan_id, "approver", &plan.plan_digest, "test/replay")
        .await
        .unwrap();
    let replay = database
        .planner
        .approve(&plan.plan_id, "approver", &plan.plan_digest, "test/replay")
        .await
        .unwrap();
    assert_eq!(replay.approval, approved.approval);
    for (approver, reference) in [
        ("another_approver", "test/replay"),
        ("approver", "test/different"),
    ] {
        assert_eq!(
            database
                .planner
                .approve(&plan.plan_id, approver, &plan.plan_digest, reference)
                .await
                .err(),
            Some(OzonPlanStoreError::InvalidState)
        );
    }
    assert_eq!(
        database
            .admin
            .query_one(
                "SELECT count(*) FROM control.ozon_campaign_plan_approvals WHERE plan_id=$1",
                &[&plan.plan_id]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    // Only the disposable administrator advances the persisted authorization
    // timestamps; the application still evaluates the real database clock.
    database.admin.batch_execute("ALTER TABLE control.ozon_campaign_plan_approvals DISABLE TRIGGER ozon_approvals_append_only").await.unwrap();
    database.admin.execute("UPDATE control.ozon_campaign_plan_approvals SET approved_at=clock_timestamp()-interval '10 minutes',expires_at=clock_timestamp()-interval '8 minutes' WHERE plan_id=$1", &[&plan.plan_id]).await.unwrap();
    database.admin.batch_execute("ALTER TABLE control.ozon_campaign_plan_approvals ENABLE TRIGGER ozon_approvals_append_only").await.unwrap();
    assert_eq!(
        database
            .planner
            .approve(&plan.plan_id, "approver", &plan.plan_digest, "test/replay")
            .await
            .err(),
        Some(OzonPlanStoreError::ApprovalExpired)
    );
    let replacement = database
        .planner
        .create(&authorization.manifest())
        .await
        .unwrap();
    assert_ne!(replacement.plan_id, plan.plan_id);
    assert_eq!(
        database.planner.load(&plan.plan_id).await.unwrap().status,
        OzonLaunchStatus::Expired
    );
    assert_eq!(database.admin.query_one(
        "SELECT count(*) FROM control.ozon_campaign_audit_events WHERE plan_id=$1 AND event_type='stale_plan_expired'", &[&plan.plan_id]
    ).await.unwrap().get::<_, i64>(0), 1);
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run through scripts/with-position-test-db.sh"]
async fn queued_launch_with_superseded_policy_expires_before_any_worker_claim() {
    let _serial = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("disposable fixture URLs are required");
    let authorization = AuthorizationFixture::new();
    let plan = database.prepare_plan(&authorization).await;
    database
        .planner
        .approve(&plan.plan_id, "approver", &plan.plan_digest, "test/queued")
        .await
        .unwrap();
    database
        .planner
        .enqueue_launch(&plan.plan_id, "actor", &plan.plan_digest)
        .await
        .unwrap();
    database
        .planner
        .register_policy(1, 8, &"b".repeat(64))
        .await
        .unwrap();
    assert!(
        database
            .executor
            .claim_next_launch_action("account", "worker")
            .await
            .unwrap()
            .is_none()
    );
    let expired = database.planner.load(&plan.plan_id).await.unwrap();
    assert_eq!(expired.status, OzonLaunchStatus::Expired);
    assert_eq!(expired.workflow_generation, 0);
    assert!(expired.operation_started_at.is_none());
    assert!(expired.workflow_write_started_at.is_none());
    assert_eq!(database.admin.query_one(
        "SELECT count(*) FROM control.ozon_campaign_audit_events WHERE plan_id=$1 AND event_type='workflow_initial_authorization_expired'", &[&plan.plan_id]
    ).await.unwrap().get::<_, i64>(0), 1);
    assert!(
        database
            .executor
            .claim_next_launch_action("account", "second_worker")
            .await
            .unwrap()
            .is_none()
    );
}
