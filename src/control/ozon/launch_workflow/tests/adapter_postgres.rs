use super::{adapter_fixture::*, *};
use crate::control::plan::CONTROL_DB_TEST_LOCK;

#[tokio::test]
async fn postgres_adapter_executes_three_stages_and_persists_guard_after_exact_running_readback() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let authorization = AuthorizationFixture::new();
        let lease = database.prepare(&authorization).await;
        let (reader, reads) = mock_reader(vec![
            (200, EMPTY_CAMPAIGNS.to_owned()),
            (200, EMPTY_CAMPAIGNS.to_owned()),
            (200, campaign(&lease.plan, "CAMPAIGN_STATE_INACTIVE")),
            (200, campaign(&lease.plan, "CAMPAIGN_STATE_INACTIVE")),
            (200, EMPTY_PRODUCTS.to_owned()),
            (200, campaign(&lease.plan, "CAMPAIGN_STATE_INACTIVE")),
            (200, products()),
            (200, campaign(&lease.plan, "CAMPAIGN_STATE_INACTIVE")),
            (200, products()),
            (200, campaign(&lease.plan, "CAMPAIGN_STATE_RUNNING")),
            (200, products()),
        ]);
        let (writer, requests) = mock_writer(vec![
            (200, TOKEN.to_owned()),
            (200, r#"{"campaignId":"42"}"#.to_owned()),
            (200, "{}".to_owned()),
            (200, "{}".to_owned()),
        ]);
        let io = database.io(&authorization, reader, writer);
        let outcome = execute(
            database.executor.as_ref(),
            &io,
            &NoOzonLaunchFailpoints,
            &lease,
        )
        .await
        .unwrap();
        assert!(matches!(
            outcome,
            OzonLaunchDrainOutcome::Executed {
                status: OzonLaunchStatus::Created,
                ..
            }
        ));
        let batch = drain_ozon_launch_workflow_batch(
            database.executor.as_ref(),
            &io,
            &NoOzonLaunchFailpoints,
            "account",
            "worker",
        )
        .await
        .unwrap();
        assert_eq!(batch.processed, 2);
        assert_eq!(batch.persisted_failures, 0);
        assert!(!batch.saturated);
        let plan = database.planner.load(&lease.plan.plan_id).await.unwrap();
        assert_eq!(plan.status, OzonLaunchStatus::Applied);
        assert_eq!(plan.readback.as_ref().unwrap()["campaign_id"], 42);
        assert_eq!(
            database
                .executor
                .active_guards_for_account("account")
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(reads.try_iter().count(), 12);
        let requests = requests.try_iter().collect::<Vec<_>>();
        assert_eq!(requests.len(), 4);
        assert!(requests[1].starts_with("POST /api/client/campaign/cpc/v2/product "));
        assert!(requests[2].starts_with("POST /api/client/campaign/42/products "));
        assert!(requests[3].starts_with("POST /api/client/campaign/42/activate "));
    }
}

#[tokio::test]
async fn postgres_adapter_prewrite_failures_release_or_fail_without_mutations() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let authorization = AuthorizationFixture::new();
        for case in [
            "conflict",
            "read_failure",
            "oauth",
            "preflight_timeout",
            "final_timeout",
            "registry",
            "binding",
            "gate",
        ] {
            assert_prewrite_case(&database, &authorization, case).await;
        }
    }
}

async fn assert_prewrite_case(
    database: &Database,
    authorization: &AuthorizationFixture,
    case: &str,
) {
    let lease = database.prepare(authorization).await;
    let responses = match case {
        "conflict" => vec![
            (200, campaign(&lease.plan, "CAMPAIGN_STATE_RUNNING")),
            (200, products()),
        ],
        "read_failure" => vec![(200, "{}".to_owned())],
        "preflight_timeout" => vec![],
        _ => vec![
            (200, EMPTY_CAMPAIGNS.to_owned()),
            (200, EMPTY_CAMPAIGNS.to_owned()),
        ],
    };
    let (reader, _) = mock_reader(responses);
    let token_needed = !matches!(case, "conflict" | "read_failure" | "preflight_timeout");
    let (writer, requests) = mock_writer(if token_needed {
        vec![(if case == "oauth" { 403 } else { 200 }, TOKEN.to_owned())]
    } else {
        vec![]
    });
    let mut io = database.io(authorization, reader, writer);
    match case {
        "preflight_timeout" => io.clock.timeout_at = Some(0),
        "final_timeout" => io.clock.timeout_at = Some(1),
        "registry" => std::fs::rename(
            authorization.path.join("registry.json"),
            authorization.path.join("registry.hidden"),
        )
        .unwrap(),
        "binding" => io.account_id = "other".to_owned(),
        "gate" => {
            database
                .admin
                .execute(
                    "UPDATE control.ozon_runtime_gates SET enabled=false WHERE gate_key='global'",
                    &[],
                )
                .await
                .unwrap();
        }
        _ => {}
    }
    let result = execute(
        database.executor.as_ref(),
        &io,
        &NoOzonLaunchFailpoints,
        &lease,
    )
    .await;
    if case == "registry" {
        std::fs::rename(
            authorization.path.join("registry.hidden"),
            authorization.path.join("registry.json"),
        )
        .unwrap();
    }
    let plan = database.planner.load(&lease.plan.plan_id).await.unwrap();
    if case == "conflict" {
        assert!(matches!(result, Err(OzonLaunchWorkflowError::Write(_))));
        assert_eq!(plan.status, OzonLaunchStatus::Failed);
    } else {
        assert!(
            matches!(result, Err(OzonLaunchWorkflowError::WriteNotStarted(_))),
            "case {case}"
        );
        assert_eq!(plan.status, OzonLaunchStatus::Approved);
    }
    assert!(plan.workflow_write_started_at.is_none());
    assert_eq!(requests.try_iter().count(), usize::from(token_needed));
}

#[tokio::test]
async fn postgres_adapter_ambiguous_boundary_recovers_running_campaign_without_second_post() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let authorization = AuthorizationFixture::new();
        for point in [
            None,
            Some(OzonLaunchFailpoint::AfterWriteStarted),
            Some(OzonLaunchFailpoint::AfterWrite),
        ] {
            let lease = database.prepare(&authorization).await;
            let (reader, _) = mock_reader(vec![
                (200, EMPTY_CAMPAIGNS.to_owned()),
                (200, EMPTY_CAMPAIGNS.to_owned()),
            ]);
            let mut responses = vec![(200, TOKEN.to_owned())];
            if point != Some(OzonLaunchFailpoint::AfterWriteStarted) {
                responses.push((
                    if point.is_none() { 400 } else { 200 },
                    r#"{"campaignId":"42"}"#.to_owned(),
                ));
            }
            let (writer, requests) = mock_writer(responses);
            let io = database.io(&authorization, reader, writer);
            let failpoints = OneShotFailpoint {
                selected: point,
                fired: AtomicBool::new(false),
            };
            assert!(matches!(
                execute(database.executor.as_ref(), &io, &failpoints, &lease).await,
                Err(OzonLaunchWorkflowError::Write(_))
            ));
            assert_eq!(
                database
                    .planner
                    .load(&lease.plan.plan_id)
                    .await
                    .unwrap()
                    .status,
                OzonLaunchStatus::Ambiguous
            );
            // Advance only the disposable fixture's recovery clock. Re-enable
            // the transition guard before exercising the production claim path.
            database.admin.batch_execute("ALTER TABLE control.ozon_campaign_launch_workflows DISABLE TRIGGER ozon_launch_workflow_update_guard").await.unwrap();
            let advanced = database.admin.execute("UPDATE control.ozon_campaign_launch_workflows SET available_at=clock_timestamp()-interval '1 second' WHERE plan_id=$1", &[&lease.plan.plan_id]).await;
            database.admin.batch_execute("ALTER TABLE control.ozon_campaign_launch_workflows ENABLE TRIGGER ozon_launch_workflow_update_guard").await.unwrap();
            advanced.unwrap();
            let recovery = OzonLaunchRepositoryPort::claim_recovery(
                database.executor.as_ref(),
                "account",
                "worker",
            )
            .await
            .unwrap()
            .unwrap();
            let bypass = database.admin.execute("UPDATE control.ozon_campaign_launch_workflows SET action='activate_campaign' WHERE plan_id=$1", &[&lease.plan.plan_id]).await.unwrap_err();
            assert_eq!(
                bypass.as_db_error().unwrap().message(),
                "unclaimed Ozon launch workflow cannot change"
            );
            let (reader, reads) = mock_reader(vec![
                (200, campaign(&lease.plan, "CAMPAIGN_STATE_RUNNING")),
                (200, campaign(&lease.plan, "CAMPAIGN_STATE_RUNNING")),
                (200, products()),
            ]);
            let (writer, recovery_writes) = mock_writer(vec![]);
            let recovery_io = database.io(&authorization, reader, writer);
            assert!(matches!(
                reconcile(
                    database.executor.as_ref(),
                    &recovery_io,
                    &NoOzonLaunchFailpoints,
                    &recovery,
                )
                .await
                .unwrap(),
                OzonLaunchDrainOutcome::Reconciled {
                    status: OzonLaunchStatus::Applied,
                    ..
                }
            ));
            assert_eq!(reads.try_iter().count(), 4);
            assert_eq!(recovery_writes.try_iter().count(), 0);
            assert_eq!(
                requests.try_iter().count(),
                if point == Some(OzonLaunchFailpoint::AfterWriteStarted) {
                    1
                } else {
                    2
                }
            );
        }
    }
}

#[tokio::test]
async fn postgres_adapter_readback_handles_applied_products_missing_identity_and_deadline() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let authorization = AuthorizationFixture::new();
        for action in [
            OzonLaunchAction::CreateCampaign,
            OzonLaunchAction::AddProducts,
            OzonLaunchAction::ActivateCampaign,
        ] {
            let mut lease = lease(
                action,
                OzonLaunchClaimMode::Reconcile,
                OzonLaunchStatus::Ambiguous,
            );
            lease.plan.policy_digest = authorization.policy.digest().to_owned();
            let (reader, _) = mock_reader(if action == OzonLaunchAction::CreateCampaign {
                vec![
                    (200, campaign(&lease.plan, "CAMPAIGN_STATE_RUNNING")),
                    (200, campaign(&lease.plan, "CAMPAIGN_STATE_RUNNING")),
                    (200, products()),
                ]
            } else {
                vec![
                    (200, campaign(&lease.plan, "CAMPAIGN_STATE_RUNNING")),
                    (200, products()),
                ]
            });
            let (writer, requests) = mock_writer(vec![]);
            let mut io = database.io(&authorization, reader, writer);
            assert!(matches!(
                io.readback(&lease).await.unwrap(),
                OzonLaunchObservation::Applied {
                    campaign_id: 42,
                    ..
                }
            ));
            assert_eq!(requests.try_iter().count(), 0);
            io.clock.timeout_at = Some(1);
            assert_eq!(
                io.readback(&lease).await.unwrap_err(),
                "launch readback deadline exceeded"
            );
            if action != OzonLaunchAction::CreateCampaign {
                lease.plan.campaign_id = None;
                assert_eq!(
                    io.readback(&lease).await.unwrap_err(),
                    "campaign id missing"
                );
                assert!(matches!(
                    io.execute(&lease, &NoOzonLaunchFailpoints).await,
                    Err(OzonLaunchWriteFailure::Definite(_))
                ));
            }
        }
    }
}
