use super::*;
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{
        AuthorizationFixture, Database, TOKEN, mock_writer,
    },
    plan::CONTROL_DB_TEST_LOCK,
};

async fn expire_stop_lease(database: &Database) {
    // Advance only the disposable fixture's stored clock. One SQL transaction
    // ensures failure also rolls back the temporary trigger change.
    database
        .admin
        .batch_execute(
            "BEGIN; \
         ALTER TABLE control.ozon_campaign_guards DISABLE TRIGGER ozon_guards_transition_guard; \
         UPDATE control.ozon_campaign_guards \
         SET stop_lease_claimed_at=stop_write_started_at-interval '1 second', \
             stop_lease_expires_at=stop_write_started_at+interval '1 microsecond' \
         WHERE account_id='account' AND campaign_id=42 AND status='stopping'; \
         ALTER TABLE control.ozon_campaign_guards ENABLE TRIGGER ozon_guards_transition_guard; \
         COMMIT;",
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_guard_ports_recover_one_deactivation_and_preserve_complete_evidence() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let authorization = AuthorizationFixture::new();
        let guard = database.applied_guard(&authorization).await;
        let repository = database.executor.as_ref();
        assert!(
            OzonGuardRepositoryPort::claim_stop_recovery(repository, "account", "worker")
                .await
                .unwrap()
                .is_none()
        );
        let metrics = OzonGuardMetrics {
            spend_minor: 20_000,
            attributed_revenue_minor: 100_000,
        };
        OzonGuardRepositoryPort::record_observation(repository, &guard, metrics)
            .await
            .unwrap();
        let lease = OzonGuardRepositoryPort::claim_stop(
            repository,
            &guard,
            "drr_cap_exceeded",
            Some(metrics),
            "worker",
        )
        .await
        .unwrap();
        let (client, requests) = mock_writer(vec![(200, TOKEN.to_owned()), (200, "{}".to_owned())]);
        let writer = PerformanceGuardWriter {
            client: &client,
            repository,
        };
        writer.deactivate_with_final_permit(&lease).await.unwrap();
        let requests = requests.try_iter().collect::<Vec<_>>();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with("POST /api/client/campaign/42/deactivate "));
        expire_stop_lease(&database).await;
        let recovered =
            OzonGuardRepositoryPort::claim_stop_recovery(repository, "account", "recovery")
                .await
                .unwrap()
                .unwrap();
        assert!(recovered.write_started_at.is_some());
        assert_eq!(recovered.generation, lease.generation + 1);
        assert_eq!(recovered.spend_minor, Some(metrics.spend_minor));
        assert_eq!(
            recovered.revenue_minor,
            Some(metrics.attributed_revenue_minor)
        );
        OzonGuardRepositoryPort::record_readback(
            repository,
            &recovered,
            OzonGuardStopReadback::Unavailable,
        )
        .await
        .unwrap();
        OzonGuardRepositoryPort::record_readback(
            repository,
            &recovered,
            OzonGuardStopReadback::Stopped,
        )
        .await
        .unwrap();
        OzonGuardRepositoryPort::finish_stop(repository, &recovered, Some(metrics))
            .await
            .unwrap();
        assert!(
            OzonGuardRepositoryPort::active_guards(repository, "account")
                .await
                .unwrap()
                .is_empty()
        );
        let row = database.admin.query_one("SELECT status,last_spend_minor,last_revenue_minor FROM control.ozon_campaign_guards WHERE plan_id=$1", &[&guard.plan_id]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "stopped");
        assert_eq!(row.get::<_, i64>(1), 20_000);
        assert_eq!(row.get::<_, i64>(2), 100_000);
        let events: Vec<String> = database.admin.query_one("SELECT array_agg(event_type ORDER BY event_id) FROM control.ozon_campaign_audit_events WHERE plan_id=$1 AND event_type LIKE 'guard_stop_%'", &[&guard.plan_id]).await.unwrap().get(0);
        assert_eq!(
            events,
            [
                "guard_stop_claimed",
                "guard_stop_write_started",
                "guard_stop_reclaimed",
                "guard_stop_readback_unavailable",
                "guard_stop_readback_stopped",
                "guard_stop_stopped"
            ]
        );
    }
}

#[tokio::test]
async fn postgres_guard_writer_distinguishes_oauth_final_marker_and_provider_failures() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let authorization = AuthorizationFixture::new();
        for (case, expected) in [
            ("oauth", OzonGuardWriteFailure::Permit),
            ("gate_revoked", OzonGuardWriteFailure::MarkerUncertain),
            ("provider", OzonGuardWriteFailure::Ambiguous),
        ] {
            let guard = database.applied_guard(&authorization).await;
            let repository = database.executor.as_ref();
            let lease = OzonGuardRepositoryPort::claim_stop(
                repository,
                &guard,
                "telemetry_unavailable",
                None,
                "worker",
            )
            .await
            .unwrap();
            if case == "gate_revoked" {
                database.admin.execute("UPDATE control.ozon_runtime_gates SET enabled=false WHERE gate_key='global'", &[]).await.unwrap();
            }
            let responses = match case {
                "oauth" => vec![(401, "{}".to_owned())],
                "gate_revoked" => vec![(200, TOKEN.to_owned())],
                _ => vec![(200, TOKEN.to_owned()), (400, "{}".to_owned())],
            };
            let (client, requests) = mock_writer(responses);
            let writer = PerformanceGuardWriter {
                client: &client,
                repository,
            };
            assert_eq!(
                writer.deactivate_with_final_permit(&lease).await,
                Err(expected)
            );
            assert_eq!(
                requests.try_iter().count(),
                if case == "provider" { 2 } else { 1 }
            );
            let marked: bool = database.admin.query_one("SELECT stop_write_started_at IS NOT NULL FROM control.ozon_campaign_guards WHERE plan_id=$1", &[&guard.plan_id]).await.unwrap().get(0);
            assert_eq!(marked, case == "provider");
            if marked {
                OzonGuardRepositoryPort::record_readback(
                    repository,
                    &lease,
                    OzonGuardStopReadback::Running,
                )
                .await
                .unwrap();
                OzonGuardRepositoryPort::mark_incident(
                    repository,
                    &lease,
                    "campaign_still_running",
                    None,
                )
                .await
                .unwrap();
                let status: String = database
                    .admin
                    .query_one(
                        "SELECT status FROM control.ozon_campaign_guards WHERE plan_id=$1",
                        &[&guard.plan_id],
                    )
                    .await
                    .unwrap()
                    .get(0);
                assert_eq!(status, "incident");
            }
        }
    }
}
