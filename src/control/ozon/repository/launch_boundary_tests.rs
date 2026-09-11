use super::*;
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{AuthorizationFixture, Database},
    plan::CONTROL_DB_TEST_LOCK,
};
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run through scripts/with-position-test-db.sh"]
async fn launch_markers_reject_forged_scope_and_results_cannot_skip_the_write_boundary() {
    let _serial = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("disposable fixture URLs are required");
    let authorization = AuthorizationFixture::new();
    let lease = database.prepare(&authorization).await;
    let commit_attempted = AtomicBool::new(false);
    let identity = create_identity_preflight_digest_for(&lease.plan);
    let mut recovery = lease.clone();
    recovery.mode = OzonLaunchClaimMode::Reconcile;
    let mut another_actor = lease.clone();
    another_actor.plan.actor_id = "different_actor".to_owned();
    let mut changed_digest = lease.clone();
    changed_digest.plan.plan_digest = "b".repeat(64);
    for malformed in [recovery, another_actor, changed_digest] {
        assert_eq!(
            database
                .executor
                .start_launch_write(&malformed, Some(&identity), || {
                    commit_attempted.store(true, Ordering::Relaxed);
                })
                .await,
            Err(OzonPlanStoreError::InvalidState)
        );
    }
    for proof in [None, Some("unrelated_preflight")] {
        assert_eq!(
            database
                .executor
                .start_launch_write(&lease, proof, || {
                    commit_attempted.store(true, Ordering::Relaxed);
                })
                .await,
            Err(OzonPlanStoreError::InvalidPlan)
        );
    }
    let readback = stage_readback(&lease);
    assert_eq!(
        database
            .executor
            .complete_launch_action(&lease, Some(42), Some(&readback))
            .await
            .err(),
        Some(OzonPlanStoreError::InvalidPlan)
    );
    assert_eq!(
        database
            .executor
            .mark_launch_ambiguous(&lease, "readback_unavailable", None, None)
            .await
            .err(),
        Some(OzonPlanStoreError::InvalidState)
    );
    assert_eq!(
        database
            .executor
            .confirm_launch_applied(&lease, 43, &readback)
            .await
            .err(),
        Some(OzonPlanStoreError::InvalidPlan)
    );
    assert!(!commit_attempted.load(Ordering::Relaxed));
    let stored = database.planner.load(&lease.plan.plan_id).await.unwrap();
    assert_eq!(stored.status, OzonLaunchStatus::Approved);
    assert!(stored.operation_started_at.is_none());
    assert!(stored.finished_at.is_none());
    let row = database
        .admin
        .query_one(
            "SELECT write_started_at FROM control.ozon_campaign_launch_workflows WHERE plan_id=$1",
            &[&lease.plan.plan_id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<DateTime<Utc>>>(0), None);
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run through scripts/with-position-test-db.sh"]
async fn each_prewrite_conflict_is_bound_to_its_stage_and_existing_campaign() {
    let _serial = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("disposable fixture URLs are required");
    let authorization = AuthorizationFixture::new();
    for (action, reason) in [
        (
            OzonLaunchAction::CreateCampaign,
            "ozon_create_precondition_conflict",
        ),
        (
            OzonLaunchAction::AddProducts,
            "ozon_products_precondition_conflict",
        ),
        (
            OzonLaunchAction::ActivateCampaign,
            "ozon_activate_precondition_conflict",
        ),
    ] {
        let lease = claim_stage(&database, &authorization, action).await;
        assert_eq!(
            database
                .executor
                .fail_launch_action(&lease, "wrong_stage_conflict", lease.plan.campaign_id)
                .await
                .err(),
            Some(OzonPlanStoreError::InvalidState)
        );
        assert_eq!(
            database
                .executor
                .fail_launch_action(&lease, reason, Some(43))
                .await
                .err(),
            Some(OzonPlanStoreError::InvalidState)
        );
        if action != OzonLaunchAction::CreateCampaign {
            assert_eq!(
                database
                    .executor
                    .complete_launch_action(&lease, Some(42), Some(&stage_readback(&lease)))
                    .await
                    .err(),
                Some(OzonPlanStoreError::InvalidState)
            );
            let called = AtomicBool::new(false);
            assert_eq!(
                database
                    .executor
                    .start_launch_write(&lease, Some("create_only_proof"), || {
                        called.store(true, Ordering::Relaxed);
                    })
                    .await,
                Err(OzonPlanStoreError::InvalidPlan)
            );
            assert!(!called.load(Ordering::Relaxed));
        }
        let failed = database
            .executor
            .fail_launch_action(&lease, reason, lease.plan.campaign_id)
            .await
            .unwrap();
        assert_eq!(failed.status, OzonLaunchStatus::Failed);
        assert_eq!(failed.campaign_id, lease.plan.campaign_id);
        assert_eq!(failed.last_error_class.as_deref(), Some(reason));
        assert!(
            database
                .executor
                .claim_next_launch_action("account", "next_worker")
                .await
                .unwrap()
                .is_none()
        );
    }
}

async fn claim_stage(
    database: &Database,
    authorization: &AuthorizationFixture,
    action: OzonLaunchAction,
) -> OzonLaunchLease {
    let mut lease = database.prepare(authorization).await;
    while lease.action != action {
        let proof = (lease.action == OzonLaunchAction::CreateCampaign)
            .then(|| create_identity_preflight_digest_for(&lease.plan));
        database
            .executor
            .start_launch_write(&lease, proof.as_deref(), || {})
            .await
            .unwrap();
        database
            .executor
            .complete_launch_action(&lease, Some(42), Some(&stage_readback(&lease)))
            .await
            .unwrap();
        lease = database
            .executor
            .claim_next_launch_action("account", "worker")
            .await
            .unwrap()
            .unwrap();
    }
    lease
}

fn stage_readback(lease: &OzonLaunchLease) -> Value {
    let state = if lease.action == OzonLaunchAction::ActivateCampaign {
        "CAMPAIGN_STATE_RUNNING"
    } else {
        "CAMPAIGN_STATE_INACTIVE"
    };
    serde_json::json!({
        "campaign_id":42,"action":lease.action.as_db(),"verified":true,
        "title":lease.plan.manifest.create_request.title,"state":state,
        "sku":lease.plan.sku,"bid_microrubles":lease.plan.manifest.spec.initial_cpc_bid_microrubles,
    })
}
