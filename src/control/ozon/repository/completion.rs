//! Pure completion validation, performed before acquiring a database session.

use super::{
    OzonCampaignPlan, OzonLaunchAction, OzonLaunchClaimMode, OzonLaunchLease, OzonLaunchStatus,
    OzonPlanStoreError, Value, exact_running_readback, json_u64, validate_launch_lease,
};

pub(super) fn validate_completion(
    lease: &OzonLaunchLease,
    campaign_id: Option<u64>,
    readback: Option<&Value>,
    force_applied: bool,
) -> Result<(Option<u64>, OzonLaunchStatus), OzonPlanStoreError> {
    validate_launch_lease(lease)?;
    if lease.mode == OzonLaunchClaimMode::Reconcile && readback.is_none() {
        return Err(OzonPlanStoreError::InvalidPlan);
    }
    let effective_campaign_id = campaign_id.or(lease.plan.campaign_id);
    if lease.action == OzonLaunchAction::CreateCampaign && effective_campaign_id.is_none() {
        return Err(OzonPlanStoreError::InvalidPlan);
    }
    if let (Some(expected), Some(actual)) = (lease.plan.campaign_id, campaign_id)
        && expected != actual
    {
        return Err(OzonPlanStoreError::InvalidPlan);
    }
    if !force_applied {
        let campaign_id = effective_campaign_id.ok_or(OzonPlanStoreError::InvalidPlan)?;
        let exact = completion_readback_is_exact(
            readback.ok_or(OzonPlanStoreError::InvalidPlan)?,
            lease.action,
            &lease.plan,
            campaign_id,
        );
        if !exact {
            return Err(OzonPlanStoreError::InvalidPlan);
        }
    }
    let target = if force_applied {
        OzonLaunchStatus::Applied
    } else {
        lease.action.completed_status()
    };
    Ok((effective_campaign_id, target))
}

fn completion_readback_is_exact(
    readback: &Value,
    action: OzonLaunchAction,
    plan: &OzonCampaignPlan,
    campaign_id: u64,
) -> bool {
    match action {
        OzonLaunchAction::ActivateCampaign => exact_running_readback(readback, campaign_id, plan),
        OzonLaunchAction::CreateCampaign | OzonLaunchAction::AddProducts => {
            if json_u64(readback.get("campaign_id")) != Some(campaign_id)
                || readback.get("action").and_then(Value::as_str) != Some(action.as_db())
                || readback.get("verified").and_then(Value::as_bool) != Some(true)
                || readback.get("title").and_then(Value::as_str)
                    != Some(plan.manifest.create_request.title.as_str())
            {
                return false;
            }
            if action == OzonLaunchAction::CreateCampaign {
                readback
                    .get("state")
                    .and_then(Value::as_str)
                    .is_some_and(is_supported_non_running_state)
            } else {
                json_u64(readback.get("sku")) == Some(plan.sku)
                    && json_u64(readback.get("bid_microrubles"))
                        == Some(plan.manifest.spec.initial_cpc_bid_microrubles)
            }
        }
    }
}

fn is_supported_non_running_state(state: &str) -> bool {
    matches!(
        state,
        "CAMPAIGN_STATE_STOPPED" | "CAMPAIGN_STATE_INACTIVE" | "CAMPAIGN_STATE_PLANNED"
    )
}
