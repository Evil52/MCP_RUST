use super::{adapter_fixture::*, *};

#[tokio::test]
async fn batch_saturation_preserves_unclaimed_work_for_the_next_poll() {
    let queued = (0..=MAX_OZON_LAUNCH_ACTIONS_PER_CYCLE).map(|index| {
        let mut lease = lease(
            OzonLaunchAction::AddProducts,
            OzonLaunchClaimMode::Execute,
            OzonLaunchStatus::Created,
        );
        lease.plan.plan_id = format!("{index:064x}");
        lease
    });
    let repository = TestRepository::with_leases([], queued);
    let io = TestIo::new(
        (0..MAX_OZON_LAUNCH_ACTIONS_PER_CYCLE).map(|_| {
            Err(OzonLaunchWriteFailure::NotStarted(
                "temporary preflight failure".to_owned(),
            ))
        }),
        [],
    );
    let outcome = drain_ozon_launch_workflow_batch(
        &repository,
        &io,
        &NoOzonLaunchFailpoints,
        "account",
        "worker",
    )
    .await
    .unwrap();
    assert_eq!(
        outcome,
        OzonLaunchBatchOutcome {
            processed: MAX_OZON_LAUNCH_ACTIONS_PER_CYCLE,
            persisted_failures: MAX_OZON_LAUNCH_ACTIONS_PER_CYCLE,
            saturated: true,
        }
    );
    assert_eq!(repository.state.lock().unwrap().executions.len(), 1);
    assert_eq!(
        io.execute_count.load(Ordering::Acquire),
        MAX_OZON_LAUNCH_ACTIONS_PER_CYCLE
    );
    assert_eq!(
        repository
            .events()
            .iter()
            .filter(|event| event.as_str() == "release:add_products:ozon_products_not_started")
            .count(),
        MAX_OZON_LAUNCH_ACTIONS_PER_CYCLE
    );
}

#[tokio::test]
async fn running_readback_finishes_create_or_products_without_another_activation() {
    for action in [
        OzonLaunchAction::CreateCampaign,
        OzonLaunchAction::AddProducts,
    ] {
        let lease = lease(action, OzonLaunchClaimMode::Execute, action.stable_status());
        let repository = TestRepository::default();
        let receipt = match action {
            OzonLaunchAction::CreateCampaign => OzonLaunchWriteReceipt::Created(42),
            _ => OzonLaunchWriteReceipt::Mutated(42),
        };
        let io = TestIo::new([Ok(receipt)], [Ok(applied(42))]);
        assert!(matches!(
            execute(&repository, &io, &NoOzonLaunchFailpoints, &lease)
                .await
                .unwrap(),
            OzonLaunchDrainOutcome::Executed {
                status: OzonLaunchStatus::Applied,
                ..
            }
        ));
        assert_eq!(
            repository.events(),
            [format!("confirm:{}:42", action.as_db())]
        );
        assert_eq!(io.execute_count.load(Ordering::Acquire), 1);
    }
}

#[tokio::test]
async fn create_readback_outage_keeps_response_identity_and_prewrite_failures_release_exact_stage()
{
    let lease = lease(
        OzonLaunchAction::CreateCampaign,
        OzonLaunchClaimMode::Execute,
        OzonLaunchStatus::Approved,
    );
    let repository = TestRepository::default();
    let io = TestIo::new(
        [Ok(OzonLaunchWriteReceipt::Created(42))],
        [Err("connection lost".to_owned())],
    );
    assert_eq!(
        execute(&repository, &io, &NoOzonLaunchFailpoints, &lease).await,
        Err(OzonLaunchWorkflowError::Readback(
            "connection lost".to_owned()
        ))
    );
    assert_eq!(
        repository.events(),
        ["ambiguous:create_campaign:ozon_create_readback_unavailable:42:false"]
    );
    let lease = super::lease(
        OzonLaunchAction::ActivateCampaign,
        OzonLaunchClaimMode::Execute,
        OzonLaunchStatus::ProductsAdded,
    );
    let repository = TestRepository::default();
    let io = TestIo::new(
        [Err(OzonLaunchWriteFailure::NotStarted(
            "revoked".to_owned(),
        ))],
        [],
    );
    assert_eq!(
        execute(&repository, &io, &NoOzonLaunchFailpoints, &lease).await,
        Err(OzonLaunchWorkflowError::WriteNotStarted(
            "revoked".to_owned()
        ))
    );
    assert_eq!(
        repository.events(),
        ["release:activate_campaign:ozon_activate_not_started"]
    );
}

#[tokio::test]
async fn exact_readback_rejects_stopped_wrong_identity_and_wrong_metadata() {
    let store = StoreId::from("store");
    for (id, title, state, expected) in [
        (
            42,
            "wanted",
            "CAMPAIGN_STATE_STOPPED",
            "campaign is not running",
        ),
        (
            43,
            "wanted",
            "CAMPAIGN_STATE_RUNNING",
            "campaign readback id mismatch",
        ),
        (
            42,
            "unrelated",
            "CAMPAIGN_STATE_RUNNING",
            "campaign readback metadata is unsupported",
        ),
    ] {
        let body = serde_json::json!({"list":[{"id":id,"title":title,"state":state}]}).to_string();
        let (reader, requests) = mock_reader(vec![(200, body)]);
        assert_eq!(
            exact_ozon_launch_readback(&reader, &store, 42, 1001, "wanted", 7_000_000)
                .await
                .unwrap_err(),
            expected
        );
        assert_eq!(requests.try_iter().count(), 2);
    }
}

#[tokio::test]
async fn sku_preflight_rejects_nonrunning_campaign_or_invalid_product_identity() {
    let store = StoreId::from("store");
    let campaign = serde_json::json!({"id":42,"title":"other","state":"CAMPAIGN_STATE_STOPPED"});
    let (reader, requests) = mock_reader(vec![]);
    assert!(
        matches!(ensure_running_campaign_does_not_serve_sku(&reader, &store, &campaign, 1001).await,
        Err(OzonFinalPermitError::Transient(message)) if message == "SKU preflight campaign state is not running")
    );
    assert_eq!(requests.try_iter().count(), 0);
    let mut campaign = campaign;
    campaign["state"] = serde_json::json!("CAMPAIGN_STATE_RUNNING");
    let (reader, requests) = mock_reader(vec![(200, r#"{"products":[{"sku":0}]}"#.to_owned())]);
    assert!(
        matches!(ensure_running_campaign_does_not_serve_sku(&reader, &store, &campaign, 1001).await,
        Err(OzonFinalPermitError::Transient(message)) if message == "SKU preflight product SKU is invalid")
    );
    assert_eq!(requests.try_iter().count(), 2);
}
