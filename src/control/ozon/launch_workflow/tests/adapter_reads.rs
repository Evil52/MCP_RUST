use super::{adapter_fixture::*, *};

#[test]
fn final_authorization_requires_current_policy_identity_access_and_exact_delegation() {
    let fixture = AuthorizationFixture::new();
    let plan = fixture.plan();
    let registry = fixture.registry.load().unwrap();
    assert!(authorize_launch_plan(&fixture.policy, &registry, "account", &plan).is_ok());
    assert_eq!(
        authorize_launch_plan(&fixture.policy, &registry, "other", &plan).unwrap_err(),
        "policy/runtime binding changed"
    );
    let mut changed = plan.clone();
    changed.actor_id = "missing".to_owned();
    assert_eq!(
        authorize_launch_plan(&fixture.policy, &registry, "account", &changed).unwrap_err(),
        "plan actor missing"
    );
    let mut changed_registry = registry.as_ref().clone();
    changed_registry.accounts.clear();
    assert_eq!(
        authorize_launch_plan(&fixture.policy, &changed_registry, "account", &plan).unwrap_err(),
        "runtime account missing"
    );
    let mut changed_registry = registry.as_ref().clone();
    changed_registry.accounts[0].manager_id = "approver".to_owned();
    assert_eq!(
        authorize_launch_plan(&fixture.policy, &changed_registry, "account", &plan).unwrap_err(),
        "plan actor account access revoked"
    );
    let mut changed = plan.clone();
    changed.approval = None;
    assert_eq!(
        authorize_launch_plan(&fixture.policy, &registry, "account", &changed).unwrap_err(),
        "approval missing"
    );
    let mut changed = plan.clone();
    changed.approval.as_mut().unwrap().approver_id = "missing".to_owned();
    assert_eq!(
        authorize_launch_plan(&fixture.policy, &registry, "account", &changed).unwrap_err(),
        "approver missing"
    );
    let mut changed_registry = registry.as_ref().clone();
    changed_registry.actors[1].account_ids.clear();
    assert_eq!(
        authorize_launch_plan(&fixture.policy, &changed_registry, "account", &plan).unwrap_err(),
        "approver account access revoked"
    );
    let mut changed = plan.clone();
    changed.approval.as_mut().unwrap().approver_id = "actor".to_owned();
    assert_eq!(
        authorize_launch_plan(&fixture.policy, &registry, "account", &changed).unwrap_err(),
        "approver account access revoked"
    );
    let mut changed = plan;
    changed.sku = 1002;
    assert_eq!(
        authorize_launch_plan(&fixture.policy, &registry, "account", &changed).unwrap_err(),
        "launch delegation changed"
    );
    assert_eq!(
        OzonFinalPermitError::Conflict("conflict").to_string(),
        "conflict"
    );
    assert_eq!(
        OzonFinalPermitError::Transient("transient".to_owned()).to_string(),
        "transient"
    );
}

#[tokio::test]
async fn tokio_launch_clock_enforces_deadline_and_completes_ready_work() {
    let clock = TokioOzonLaunchClock;
    assert_eq!(
        clock
            .timeout(Duration::from_secs(1), std::future::ready(42))
            .await,
        Ok(42)
    );
    assert_eq!(
        clock
            .timeout(Duration::ZERO, std::future::pending::<()>())
            .await,
        Err(OzonLaunchDeadlineExceeded)
    );
    clock.sleep(Duration::ZERO).await;
}

#[tokio::test]
async fn title_preflight_requires_complete_valid_unique_identity_and_bounds_listing() {
    let store = StoreId::from("store");
    for (status, body, expected) in [
        (400, "{}".to_owned(), "title preflight failed"),
        (
            200,
            "{}".to_owned(),
            "title preflight campaign list is invalid",
        ),
        (
            200,
            r#"{"list":[{}]}"#.to_owned(),
            "title preflight campaign id is invalid",
        ),
    ] {
        let (reader, _) = mock_reader(vec![(status, body)]);
        let error = ensure_ozon_campaign_title_absent(&reader, &store, "wanted")
            .await
            .unwrap_err();
        assert!(
            matches!(error, OzonFinalPermitError::Transient(ref message) if message.starts_with(expected))
        );
    }
    let plan = AuthorizationFixture::new().plan();
    let (client, _) = mock_reader(vec![(200, campaign(&plan, "CAMPAIGN_STATE_INACTIVE"))]);
    assert!(matches!(
        ensure_ozon_campaign_title_absent(&client, &store, &plan.manifest.create_request.title)
            .await,
        Err(OzonFinalPermitError::Conflict(
            "ozon_create_precondition_conflict"
        ))
    ));
    let page = serde_json::json!({"list": vec![serde_json::json!({"id":42,"title":"other","state":"CAMPAIGN_STATE_INACTIVE"});100]}).to_string();
    let (client, requests) =
        mock_reader(vec![(200, page.clone()), (200, EMPTY_CAMPAIGNS.to_owned())]);
    ensure_ozon_campaign_title_absent(&client, &store, "wanted")
        .await
        .unwrap();
    assert_eq!(requests.try_iter().count(), 3);
    let (client, requests) = mock_reader(vec![(200, page); 100]);
    assert!(
        matches!(ensure_ozon_campaign_title_absent(&client, &store, "wanted").await,
        Err(OzonFinalPermitError::Transient(message)) if message == "title preflight campaign listing bound exceeded")
    );
    assert_eq!(requests.try_iter().count(), 101);
}

#[tokio::test]
async fn add_and_activate_preconditions_reject_incomplete_or_conflicting_provider_state() {
    let mut plan = AuthorizationFixture::new().plan();
    plan.campaign_id = Some(42);
    let store = StoreId::from("store");
    for action in [
        OzonLaunchAction::AddProducts,
        OzonLaunchAction::ActivateCampaign,
    ] {
        for (campaign_state, product_status, product_body, accepted) in [
            (
                "CAMPAIGN_STATE_STOPPED",
                200,
                EMPTY_PRODUCTS.to_owned(),
                action == OzonLaunchAction::AddProducts,
            ),
            (
                "CAMPAIGN_STATE_INACTIVE",
                200,
                products(),
                action == OzonLaunchAction::ActivateCampaign,
            ),
            ("CAMPAIGN_STATE_RUNNING", 200, products(), false),
            ("CAMPAIGN_STATE_PLANNED", 400, "{}".to_owned(), false),
            ("CAMPAIGN_STATE_PLANNED", 200, "{}".to_owned(), false),
        ] {
            let (reader, requests) = mock_reader(vec![
                (200, campaign(&plan, campaign_state)),
                (product_status, product_body),
            ]);
            let result = if action == OzonLaunchAction::AddProducts {
                ensure_add_products_precondition(&reader, &store, &plan).await
            } else {
                ensure_activate_precondition(&reader, &store, &plan).await
            };
            assert_eq!(result.is_ok(), accepted);
            assert_eq!(requests.try_iter().count(), 3);
        }
        let (reader, _) = mock_reader(vec![(200, EMPTY_CAMPAIGNS.to_owned())]);
        let result = if action == OzonLaunchAction::AddProducts {
            ensure_add_products_precondition(&reader, &store, &plan).await
        } else {
            ensure_activate_precondition(&reader, &store, &plan).await
        };
        assert!(matches!(result, Err(OzonFinalPermitError::Transient(_))));
    }
    plan.campaign_id = None;
    let (reader, requests) = mock_reader(vec![]);
    assert!(matches!(
        ensure_add_products_precondition(&reader, &store, &plan).await,
        Err(OzonFinalPermitError::Conflict(_))
    ));
    assert!(matches!(
        ensure_activate_precondition(&reader, &store, &plan).await,
        Err(OzonFinalPermitError::Conflict(_))
    ));
    assert_eq!(requests.try_iter().count(), 0);
}

#[tokio::test]
async fn product_readback_distinguishes_mutable_attached_running_and_archived_states() {
    let plan = AuthorizationFixture::new().plan();
    let store = StoreId::from("store");
    for state in [
        "CAMPAIGN_STATE_STOPPED",
        "CAMPAIGN_STATE_RUNNING",
        "CAMPAIGN_STATE_ARCHIVED",
    ] {
        let (reader, _) = mock_reader(vec![(200, campaign(&plan, state)), (200, products())]);
        let result = exact_ozon_products_stage_readback(
            &reader,
            &store,
            42,
            plan.sku,
            &plan.manifest.create_request.title,
            7_000_000,
        )
        .await;
        match state {
            "CAMPAIGN_STATE_STOPPED" => assert!(matches!(
                result,
                Ok(OzonLaunchObservation::Stage {
                    campaign_id: 42,
                    ..
                })
            )),
            "CAMPAIGN_STATE_RUNNING" => assert!(matches!(
                result,
                Ok(OzonLaunchObservation::Applied {
                    campaign_id: 42,
                    ..
                })
            )),
            _ => assert_eq!(
                result.unwrap_err(),
                "campaign state is not mutable for product attachment"
            ),
        }
    }
    let (reader, _) = mock_reader(vec![(
        200,
        r#"{"list":[{"id":42,"title":"expected","state":"UNSUPPORTED"}]}"#.to_owned(),
    )]);
    assert_eq!(
        exact_campaign(&reader, &store, 42, "expected")
            .await
            .unwrap_err(),
        "campaign state is unsupported"
    );
    let mut malformed =
        serde_json::json!({"id":42,"title":"expected","state":"CAMPAIGN_STATE_INACTIVE"});
    malformed.as_object_mut().unwrap().remove("title");
    assert_eq!(
        campaign_identity(&malformed).unwrap_err(),
        "campaign title is invalid"
    );
    malformed["title"] = serde_json::json!("expected");
    malformed.as_object_mut().unwrap().remove("state");
    assert_eq!(
        campaign_identity(&malformed).unwrap_err(),
        "campaign state is invalid"
    );
}

#[tokio::test]
async fn malformed_workflow_claims_and_receipts_never_advance_the_repository() {
    let io = TestIo::new([], []);
    let repository = TestRepository::with_leases(
        [lease(
            OzonLaunchAction::CreateCampaign,
            OzonLaunchClaimMode::Execute,
            OzonLaunchStatus::Approved,
        )],
        [],
    );
    assert_eq!(
        drain_ozon_launch_workflow_once(
            &repository,
            &io,
            &NoOzonLaunchFailpoints,
            "account",
            "worker"
        )
        .await
        .unwrap_err(),
        OzonLaunchWorkflowError::Repository(OzonPlanStoreError::InvalidState)
    );
    let io = TestIo::new([Ok(OzonLaunchWriteReceipt::Mutated(42))], []);
    let repository = TestRepository::with_leases(
        [],
        [lease(
            OzonLaunchAction::CreateCampaign,
            OzonLaunchClaimMode::Execute,
            OzonLaunchStatus::Approved,
        )],
    );
    assert_eq!(
        drain_ozon_launch_workflow_once(
            &repository,
            &io,
            &NoOzonLaunchFailpoints,
            "account",
            "worker"
        )
        .await
        .unwrap_err(),
        OzonLaunchWorkflowError::Repository(OzonPlanStoreError::InvalidState)
    );
    let repository = TestRepository::with_leases(
        [],
        [lease(
            OzonLaunchAction::CreateCampaign,
            OzonLaunchClaimMode::Execute,
            OzonLaunchStatus::Approved,
        )],
    );
    assert_eq!(
        drain_ozon_launch_workflow_batch(
            &repository,
            &io,
            &OneShotFailpoint::new(OzonLaunchFailpoint::AfterClaim),
            "account",
            "worker"
        )
        .await
        .unwrap_err(),
        OzonLaunchWorkflowError::Failpoint(OzonLaunchFailpoint::AfterClaim)
    );
    let repository = TestRepository::with_leases([], []);
    let batch = drain_ozon_launch_workflow_batch(
        &repository,
        &io,
        &NoOzonLaunchFailpoints,
        "account",
        "worker",
    )
    .await
    .unwrap();
    assert_eq!(batch.processed, 0);
    assert!(!batch.saturated);
}
