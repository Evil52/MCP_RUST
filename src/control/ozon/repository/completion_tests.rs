use super::*;
use crate::control::ozon::launch_workflow::tests::adapter_fixture::AuthorizationFixture;

#[test]
fn workflow_claim_modes_follow_the_exact_stage_transition_table() {
    use OzonLaunchClaimMode::{Execute, Reconcile};
    use OzonLaunchStatus::{
        Activating, AddingProducts, Ambiguous, Applied, Approved, Created, Creating, Expired,
        Failed, Prepared, ProductsAdded,
    };
    let actions = [
        OzonLaunchAction::CreateCampaign,
        OzonLaunchAction::AddProducts,
        OzonLaunchAction::ActivateCampaign,
    ];
    for (status, expected) in [
        (Prepared, [None, None, None]),
        (Approved, [Some(Execute), None, None]),
        (Creating, [Some(Reconcile), None, None]),
        (Created, [None, Some(Execute), None]),
        (AddingProducts, [None, Some(Reconcile), None]),
        (ProductsAdded, [None, None, Some(Execute)]),
        (Activating, [None, None, Some(Reconcile)]),
        (Ambiguous, [Some(Reconcile); 3]),
        (Applied, [None, None, None]),
        (Failed, [None, None, None]),
        (Expired, [None, None, None]),
    ] {
        for (action, mode) in actions.into_iter().zip(expected) {
            assert_eq!(
                workflow_claim_mode(status, action),
                mode.ok_or(OzonPlanStoreError::Unavailable),
                "{status:?}/{action:?}",
            );
        }
    }
}

#[test]
fn completion_requires_correlated_campaign_and_readback_evidence() {
    let authorization = AuthorizationFixture::new();
    let mut lease = OzonLaunchLease {
        plan: authorization.plan(),
        action: OzonLaunchAction::CreateCampaign,
        mode: OzonLaunchClaimMode::Execute,
        generation: 1,
        owner_id: "worker".to_owned(),
        lease_token: "a".repeat(64),
    };
    let readback = serde_json::json!({
        "campaign_id":42,
        "action":"create_campaign",
        "verified":true,
        "title":lease.plan.manifest.create_request.title,
        "state":"CAMPAIGN_STATE_INACTIVE"
    });
    assert_eq!(
        completion::validate_completion(&lease, Some(42), Some(&readback), false),
        Ok((Some(42), OzonLaunchStatus::Created))
    );
    assert_eq!(
        completion::validate_completion(&lease, None, Some(&readback), false),
        Err(OzonPlanStoreError::InvalidPlan)
    );
    lease.mode = OzonLaunchClaimMode::Reconcile;
    assert_eq!(
        completion::validate_completion(&lease, Some(42), None, false),
        Err(OzonPlanStoreError::InvalidPlan)
    );
    lease.mode = OzonLaunchClaimMode::Execute;
    lease.plan.campaign_id = Some(42);
    assert_eq!(
        completion::validate_completion(&lease, Some(43), Some(&readback), false),
        Err(OzonPlanStoreError::InvalidPlan)
    );
    for (field, value) in [
        ("campaign_id", serde_json::json!(43)),
        ("action", serde_json::json!("activate_campaign")),
        ("verified", serde_json::json!(false)),
        ("title", serde_json::json!("another campaign")),
        ("state", serde_json::json!("CAMPAIGN_STATE_RUNNING")),
    ] {
        let mut invalid = readback.clone();
        invalid[field] = value;
        assert_eq!(
            completion::validate_completion(&lease, Some(42), Some(&invalid), false),
            Err(OzonPlanStoreError::InvalidPlan)
        );
    }
    lease.generation = 0;
    assert_eq!(
        completion::validate_completion(&lease, Some(42), Some(&readback), false),
        Err(OzonPlanStoreError::InvalidPlan)
    );
}
