use super::*;
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{AuthorizationFixture, Database},
    plan::CONTROL_DB_TEST_LOCK,
};

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run through scripts/with-position-test-db.sh"]
async fn stop_claims_require_the_exact_active_guard_and_completion_replays_are_idempotent() {
    let _serial = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("disposable fixture URLs are required");
    let authorization = AuthorizationFixture::new();
    let guard = database.applied_guard(&authorization).await;
    let mut stopped = guard.clone();
    stopped.status = OzonCampaignGuardStatus::Stopped;
    let mut incident = guard.clone();
    incident.incident_error_class = Some("unconfirmed".to_owned());
    for malformed in [stopped, incident] {
        assert_eq!(
            database
                .executor
                .claim_guard_stop_leased(
                    &malformed,
                    "spend_cap_reached",
                    Some(100),
                    Some(0),
                    "worker"
                )
                .await
                .err(),
            Some(OzonPlanStoreError::InvalidPlan)
        );
    }
    let mut changed = guard.clone();
    changed.target_drr_percent = 16;
    assert_eq!(
        database
            .executor
            .claim_guard_stop_leased(&changed, "spend_cap_reached", Some(100), Some(0), "worker")
            .await
            .err(),
        Some(OzonPlanStoreError::InvalidState)
    );
    assert_eq!(
        database
            .executor
            .record_guard_observation(&changed, 100, 0)
            .await,
        Err(OzonPlanStoreError::InvalidState),
        "a stale reviewed binding must not overwrite current guard metrics"
    );
    database
        .executor
        .record_guard_observation(&guard, 100, 0)
        .await
        .unwrap();
    let lease = database
        .executor
        .claim_guard_stop_leased(&guard, "spend_cap_reached", Some(100), Some(0), "worker")
        .await
        .unwrap();
    let mut malformed = lease.clone();
    malformed.guard.stop_reason = None;
    assert_eq!(
        database
            .executor
            .start_guard_stop_write(&malformed)
            .await
            .err(),
        Some(OzonPlanStoreError::InvalidPlan)
    );
    let mut stale = lease.clone();
    stale.generation += 1;
    assert_eq!(
        database.executor.start_guard_stop_write(&stale).await,
        Err(OzonPlanStoreError::LeaseLost)
    );
    let mut marked = lease.clone();
    marked.write_started_at = Some(Utc::now());
    assert_eq!(
        database.executor.start_guard_stop_write(&marked).await,
        Err(OzonPlanStoreError::InvalidState)
    );
    assert_eq!(
        database
            .executor
            .finish_guard_leased(&lease, Some(101), Some(0))
            .await,
        Err(OzonPlanStoreError::InvalidState)
    );
    database
        .executor
        .start_guard_stop_write(&lease)
        .await
        .unwrap();
    database
        .executor
        .record_guard_stop_readback(&lease, OzonGuardStopReadback::Stopped)
        .await
        .unwrap();
    database
        .executor
        .finish_guard_leased(&lease, Some(100), Some(0))
        .await
        .unwrap();
    database
        .executor
        .finish_guard_leased(&lease, Some(100), Some(0))
        .await
        .unwrap();
    let stored = database.admin.query_one("SELECT status,stop_generation,last_spend_minor,last_revenue_minor FROM control.ozon_campaign_guards WHERE plan_id=$1", &[&guard.plan_id]).await.unwrap();
    assert_eq!(stored.get::<_, &str>(0), "stopped");
    assert_eq!(stored.get::<_, i64>(1), 1);
    assert_eq!(stored.get::<_, Option<i64>>(2), Some(100));
    assert_eq!(stored.get::<_, Option<i64>>(3), Some(0));
    let count = database.admin.query_one("SELECT count(*) FROM control.ozon_campaign_audit_events WHERE plan_id=$1 AND event_type='guard_stop_stopped'", &[&guard.plan_id]).await.unwrap().get::<_, i64>(0);
    assert_eq!(
        count, 1,
        "an exact replay must not append a second completion event"
    );
}
