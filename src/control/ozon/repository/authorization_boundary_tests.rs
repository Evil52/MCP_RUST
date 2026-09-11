use super::*;
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{AuthorizationFixture, Database},
    plan::CONTROL_DB_TEST_LOCK,
};
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run through scripts/with-position-test-db.sh"]
async fn malformed_static_authorization_never_calls_the_local_marker_or_advances_the_cursor() {
    let _serial = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("disposable fixture URLs are required");
    let authorization = AuthorizationFixture::new();
    database.prepare_plan(&authorization).await;
    let intent = OzonStaticGuardWriteIntent {
        account_id: "account".to_owned(),
        sku: 1001,
        campaign_id: 42,
        mutation: OzonStaticGuardMutation::Deactivate,
        target_bid_microrubles: None,
        config_digest: "a".repeat(64),
    };
    let called = AtomicBool::new(false);
    for (mutation, target_bid) in [
        (OzonStaticGuardMutation::SetBid, None),
        (OzonStaticGuardMutation::SetBid, Some(0)),
        (OzonStaticGuardMutation::Activate, Some(7_000_000)),
        (OzonStaticGuardMutation::Deactivate, Some(7_000_000)),
    ] {
        let malformed = OzonStaticGuardWriteIntent {
            mutation,
            target_bid_microrubles: target_bid,
            ..intent.clone()
        };
        assert_eq!(
            database
                .executor
                .authorize_static_guard_write(
                    1,
                    7,
                    authorization.policy.digest(),
                    &malformed,
                    "worker",
                    None,
                    |_| async {
                        called.store(true, Ordering::Relaxed);
                        Ok(())
                    },
                )
                .await,
            Err(OzonPlanStoreError::InvalidPlan)
        );
    }
    for (schema, revision, sku, campaign) in [
        (0, 7, 1001, 42),
        (1, 0, 1001, 42),
        (1, 7, 0, 42),
        (1, 7, 1001, 0),
    ] {
        let malformed = OzonStaticGuardWriteIntent {
            sku,
            campaign_id: campaign,
            ..intent.clone()
        };
        assert_eq!(
            database
                .executor
                .authorize_static_guard_write(
                    schema,
                    revision,
                    authorization.policy.digest(),
                    &malformed,
                    "worker",
                    None,
                    |_| async {
                        called.store(true, Ordering::Relaxed);
                        Ok(())
                    },
                )
                .await,
            Err(OzonPlanStoreError::InvalidPlan)
        );
    }
    for (schema, revision) in [(0, 7), (1, 0)] {
        assert_eq!(
            database
                .executor
                .initialize_static_guard_state(
                    schema,
                    revision,
                    authorization.policy.digest(),
                    "account",
                    &intent.config_digest,
                    "worker",
                    None,
                    |_| async {
                        called.store(true, Ordering::Relaxed);
                        Ok(())
                    },
                )
                .await,
            Err(OzonPlanStoreError::InvalidPlan)
        );
    }
    assert_eq!(
        database
            .executor
            .authorize_static_guard_write(
                1,
                7,
                authorization.policy.digest(),
                &intent,
                "worker",
                None,
                |_| async {
                    called.store(true, Ordering::Relaxed);
                    Ok(())
                },
            )
            .await,
        Err(OzonPlanStoreError::InvalidState)
    );
    assert!(!called.load(Ordering::Relaxed));
    assert_eq!(
        database
            .executor
            .latest_static_guard_audit_event_id("account")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        database
            .admin
            .query_one(
                "SELECT count(*) FROM control.ozon_static_guard_audit_events",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
}
