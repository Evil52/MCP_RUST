use super::*;
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{AuthorizationFixture, Database},
    plan::CONTROL_DB_TEST_LOCK,
};

#[tokio::test]
async fn released_launch_leases_preserve_backoff_fences_and_reconciliation_mode() {
    let Some(database) = Database::connect().await else {
        return;
    };
    let _serial = CONTROL_DB_TEST_LOCK.lock().await;
    let authorization = AuthorizationFixture::new();
    for write_started in [false, true] {
        let lease = database.prepare(&authorization).await;
        if write_started {
            database
                .executor
                .start_launch_write(
                    &lease,
                    Some(&create_identity_preflight_digest_for(&lease.plan)),
                    || {},
                )
                .await
                .unwrap();
        }
        let mut overflow = lease.clone();
        overflow.generation = u64::MAX;
        assert_eq!(
            database
                .executor
                .release_launch_lease(&overflow, "ozon_create_not_started")
                .await,
            Err(OzonPlanStoreError::InvalidPlan)
        );
        assert_eq!(
            database
                .executor
                .release_launch_lease(&lease, "Invalid reason")
                .await,
            Err(OzonPlanStoreError::InvalidPlan)
        );
        let released = database
            .executor
            .release_launch_lease(&lease, "ozon_create_not_started")
            .await;
        assert_eq!(
            released,
            if write_started {
                Err(OzonPlanStoreError::Unavailable)
            } else {
                Ok(())
            }
        );
        let row = database.admin.query_one(
            "SELECT lease_owner_id,lease_token,write_started_at,last_error_class,available_at>clock_timestamp(),available_at<=clock_timestamp()+interval '2 minutes' FROM control.ozon_campaign_launch_workflows WHERE plan_id=$1",
            &[&lease.plan.plan_id],
        ).await.unwrap();
        if write_started {
            assert_eq!(row.get::<_, &str>(0), lease.owner_id);
            assert_eq!(row.get::<_, &str>(1), lease.lease_token);
            assert!(row.get::<_, Option<DateTime<Utc>>>(2).is_some());
        } else {
            assert_eq!(row.get::<_, Option<String>>(0), None);
            assert_eq!(row.get::<_, Option<String>>(1), None);
            assert_eq!(row.get::<_, Option<DateTime<Utc>>>(2), None);
            assert_eq!(row.get::<_, &str>(3), "ozon_create_not_started");
            assert!(row.get::<_, bool>(4));
            assert!(row.get::<_, bool>(5));
        }
        assert!(
            database
                .executor
                .claim_next_launch_action("account", "waiting_worker")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            database
                .executor
                .claim_launch_recovery("account", "waiting_worker")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            database
                .executor
                .release_launch_lease(&lease, "ozon_create_not_started")
                .await,
            Err(if write_started {
                OzonPlanStoreError::Unavailable
            } else {
                OzonPlanStoreError::InvalidState
            })
        );
        // Advance scheduling/lease timestamps as the disposable DB admin.
        // Preserve the durable plan status and mutation marker so recovery
        // must still use readback rather than authorize another write.
        database.admin.batch_execute("ALTER TABLE control.ozon_campaign_launch_workflows DISABLE TRIGGER ozon_launch_workflow_update_guard").await.unwrap();
        database.admin.execute("UPDATE control.ozon_campaign_launch_workflows SET available_at=clock_timestamp()-interval '1 second',lease_expires_at=CASE WHEN $2 THEN clock_timestamp() ELSE lease_expires_at END WHERE plan_id=$1", &[&lease.plan.plan_id, &write_started]).await.unwrap();
        database.admin.batch_execute("ALTER TABLE control.ozon_campaign_launch_workflows ENABLE TRIGGER ozon_launch_workflow_update_guard").await.unwrap();
        let next = if write_started {
            assert!(
                database
                    .executor
                    .claim_next_launch_action("account", "new_worker")
                    .await
                    .unwrap()
                    .is_none()
            );
            database
                .executor
                .claim_launch_recovery("account", "new_worker")
                .await
                .unwrap()
                .unwrap()
        } else {
            database
                .executor
                .claim_next_launch_action("account", "new_worker")
                .await
                .unwrap()
                .unwrap()
        };
        assert!(next.generation > lease.generation);
        assert_ne!(next.lease_token, lease.lease_token);
        assert_eq!(
            next.mode,
            if write_started {
                OzonLaunchClaimMode::Reconcile
            } else {
                OzonLaunchClaimMode::Execute
            }
        );
        assert_eq!(
            next.plan.status,
            if write_started {
                OzonLaunchStatus::Creating
            } else {
                OzonLaunchStatus::Approved
            }
        );
        assert_eq!(
            database
                .executor
                .release_launch_lease(&lease, "stale_worker")
                .await,
            Err(OzonPlanStoreError::InvalidState)
        );
        assert_eq!(
            database
                .executor
                .load(&next.plan.plan_id)
                .await
                .unwrap()
                .workflow_generation,
            next.generation
        );
    }
}

#[tokio::test]
async fn persistence_failures_remain_opaque_and_cannot_release_a_live_lease() {
    let Some(database) = Database::connect().await else {
        return;
    };
    let _serial = CONTROL_DB_TEST_LOCK.lock().await;
    let authorization = AuthorizationFixture::new();
    let lease = database.prepare(&authorization).await;
    // An SQL permission outage must roll back the transaction and leave the
    // lease intact. The public result never exposes the server's SQL detail.
    database
        .admin
        .batch_execute(
            "REVOKE UPDATE(lease_token) ON control.ozon_campaign_launch_workflows FROM ozon_control_executor",
        )
        .await
        .unwrap();
    let failure = database
        .executor
        .release_launch_lease(&lease, "ozon_create_not_started")
        .await;
    database
        .admin
        .batch_execute(
            "GRANT UPDATE(lease_token) ON control.ozon_campaign_launch_workflows TO ozon_control_executor",
        )
        .await
        .unwrap();
    assert_eq!(failure, Err(OzonPlanStoreError::Unavailable));
    let row = database.admin.query_one("SELECT lease_owner_id,lease_token FROM control.ozon_campaign_launch_workflows WHERE plan_id=$1", &[&lease.plan.plan_id]).await.unwrap();
    assert_eq!(row.get::<_, &str>(0), lease.owner_id);
    assert_eq!(row.get::<_, &str>(1), lease.lease_token);

    database
        .admin
        .batch_execute("ALTER ROLE ozon_control_executor NOLOGIN")
        .await
        .unwrap();
    database.admin.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='ozon_control_executor' AND pid<>pg_backend_pid()", &[]).await.unwrap();
    let failure = database
        .executor
        .release_launch_lease(&lease, "ozon_create_not_started")
        .await;
    database
        .admin
        .batch_execute("ALTER ROLE ozon_control_executor LOGIN")
        .await
        .unwrap();
    assert_eq!(failure, Err(OzonPlanStoreError::Unavailable));
    let row = database
        .admin
        .query_one(
            "SELECT lease_token FROM control.ozon_campaign_launch_workflows WHERE plan_id=$1",
            &[&lease.plan.plan_id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), lease.lease_token);
}
