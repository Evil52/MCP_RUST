//! Pure completion validation, performed before acquiring a database session.

use super::{
    OzonLaunchAction, OzonLaunchClaimMode, OzonLaunchLease, OzonLaunchStatus, OzonPlanStoreError,
    Value, exact_running_readback, stage_readback_is_exact, validate_launch_lease,
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
        let exact = match lease.action {
            OzonLaunchAction::CreateCampaign | OzonLaunchAction::AddProducts => {
                stage_readback_is_exact(
                    readback.ok_or(OzonPlanStoreError::InvalidPlan)?,
                    lease.action,
                    &lease.plan,
                    campaign_id,
                )
            }
            OzonLaunchAction::ActivateCampaign => exact_running_readback(
                readback.ok_or(OzonPlanStoreError::InvalidPlan)?,
                campaign_id,
                &lease.plan,
            ),
        };
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
