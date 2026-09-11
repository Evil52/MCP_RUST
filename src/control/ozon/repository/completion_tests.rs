use super::*;
use crate::control::ozon::launch_workflow::tests::adapter_fixture::AuthorizationFixture;

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
