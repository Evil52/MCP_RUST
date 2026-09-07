use super::*;

#[tokio::test]
async fn create_readback_identity_mismatch_preserves_evidence_and_recovers_without_second_post() {
    let repository = TestRepository::with_leases(
        [],
        [lease(
            OzonLaunchAction::CreateCampaign,
            OzonLaunchClaimMode::Execute,
            OzonLaunchStatus::Approved,
        )],
    );
    let io = TestIo::new(
        [Ok(OzonLaunchWriteReceipt::Created(42))],
        [Ok(create_stage(43)), Ok(create_stage(42))],
    );

    let result = drain_ozon_launch_workflow_once(
        &repository,
        &io,
        &NoOzonLaunchFailpoints,
        "account",
        "worker",
    )
    .await;
    assert!(matches!(result, Err(OzonLaunchWorkflowError::Readback(_))));
    // Keep the POST identity and the conflicting provider evidence. Neither
    // identity may be silently accepted as a completed create operation.
    assert_eq!(
        repository.events(),
        [
            "claim_recovery",
            "claim_execution",
            "ambiguous:create_campaign:ozon_create_readback_mismatch:42:true",
        ]
    );
    assert_eq!(
        repository.state.lock().unwrap().ambiguous_readbacks[0]
            .as_ref()
            .unwrap()["campaign_id"],
        43
    );

    let mut recovery = lease(
        OzonLaunchAction::CreateCampaign,
        OzonLaunchClaimMode::Reconcile,
        OzonLaunchStatus::Ambiguous,
    );
    recovery.plan.campaign_id = Some(42);
    repository.push_recovery(recovery);
    let outcome = drain_ozon_launch_workflow_once(
        &repository,
        &io,
        &NoOzonLaunchFailpoints,
        "account",
        "worker",
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        OzonLaunchDrainOutcome::Reconciled {
            status: OzonLaunchStatus::Created,
            ..
        }
    ));
    assert_eq!(io.execute_count.load(Ordering::Acquire), 1);
    assert_eq!(
        repository.events().last().map(String::as_str),
        Some("complete:create_campaign:42:true")
    );
}

#[tokio::test]
async fn inactive_activation_readback_stays_ambiguous_until_running_is_confirmed_without_rewrite() {
    let repository = TestRepository::with_leases(
        [],
        [lease(
            OzonLaunchAction::ActivateCampaign,
            OzonLaunchClaimMode::Execute,
            OzonLaunchStatus::ProductsAdded,
        )],
    );
    let io = TestIo::new(
        [Ok(OzonLaunchWriteReceipt::Mutated(42))],
        [
            Ok(product_stage(42)),
            Err("provider readback outage".to_owned()),
            Ok(applied(42)),
        ],
    );

    for recovery_attempt in [false, true] {
        if recovery_attempt {
            repository.push_recovery(lease(
                OzonLaunchAction::ActivateCampaign,
                OzonLaunchClaimMode::Reconcile,
                OzonLaunchStatus::Ambiguous,
            ));
        }
        let result = drain_ozon_launch_workflow_once(
            &repository,
            &io,
            &NoOzonLaunchFailpoints,
            "account",
            "worker",
        )
        .await;
        assert!(matches!(result, Err(OzonLaunchWorkflowError::Readback(_))));
        assert_eq!(io.execute_count.load(Ordering::Acquire), 1);
    }
    assert_eq!(
        repository.events(),
        [
            "claim_recovery",
            "claim_execution",
            "ambiguous:activate_campaign:ozon_activate_readback_mismatch:42:false",
            "claim_recovery",
            "ambiguous:activate_campaign:ozon_activate_readback_unavailable:42:false",
        ]
    );

    repository.push_recovery(lease(
        OzonLaunchAction::ActivateCampaign,
        OzonLaunchClaimMode::Reconcile,
        OzonLaunchStatus::Ambiguous,
    ));
    let outcome = drain_ozon_launch_workflow_once(
        &repository,
        &io,
        &NoOzonLaunchFailpoints,
        "account",
        "worker",
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        OzonLaunchDrainOutcome::Reconciled {
            status: OzonLaunchStatus::Applied,
            ..
        }
    ));
    assert_eq!(io.execute_count.load(Ordering::Acquire), 1);
    assert_eq!(
        repository.events().last().map(String::as_str),
        Some("confirm:activate_campaign:42")
    );
}

#[tokio::test]
async fn postwrite_lease_loss_stops_the_batch_and_recovers_before_another_mutation() {
    let first = lease(
        OzonLaunchAction::CreateCampaign,
        OzonLaunchClaimMode::Execute,
        OzonLaunchStatus::Approved,
    );
    let mut next = first.clone();
    next.plan.plan_id = "f".repeat(64);
    let repository = TestRepository::with_leases([], [first, next]);
    repository.state.lock().unwrap().fail_complete = Some(OzonPlanStoreError::LeaseLost);
    let io = TestIo::new(
        [
            Ok(OzonLaunchWriteReceipt::Created(42)),
            Ok(OzonLaunchWriteReceipt::Created(43)),
        ],
        [Ok(create_stage(42)), Ok(create_stage(42))],
    );

    assert_eq!(
        drain_ozon_launch_workflow_batch(
            &repository,
            &io,
            &NoOzonLaunchFailpoints,
            "account",
            "worker",
        )
        .await,
        Err(OzonLaunchWorkflowError::Repository(
            OzonPlanStoreError::LeaseLost
        ))
    );
    // Provider success is insufficient after fencing is lost. The batch
    // must stop, leaving the next row and its write untouched.
    assert_eq!(io.execute_count.load(Ordering::Acquire), 1);
    assert_eq!(repository.state.lock().unwrap().executions.len(), 1);
    assert_eq!(
        repository.events(),
        [
            "claim_recovery",
            "claim_execution",
            "complete:create_campaign:42:true"
        ]
    );

    let mut recovery = lease(
        OzonLaunchAction::CreateCampaign,
        OzonLaunchClaimMode::Reconcile,
        OzonLaunchStatus::Creating,
    );
    recovery.generation = 2;
    recovery.plan.workflow_generation = 2;
    repository.push_recovery(recovery);
    let outcome = drain_ozon_launch_workflow_once(
        &repository,
        &io,
        &NoOzonLaunchFailpoints,
        "account",
        "worker",
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        OzonLaunchDrainOutcome::Reconciled {
            status: OzonLaunchStatus::Created,
            ..
        }
    ));
    assert_eq!(io.execute_count.load(Ordering::Acquire), 1);
    assert_eq!(repository.state.lock().unwrap().executions.len(), 1);
    assert_eq!(io.writes.lock().unwrap().len(), 1);
}
