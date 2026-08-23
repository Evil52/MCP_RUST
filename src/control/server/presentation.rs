use chrono::{DateTime, Utc};

use crate::control::{
    plan::{PlanStoreError, WbControlPlan, WbPlanStatus},
    wb::{WbCampaignBidSnapshot, WbWriteError, WbWriteOutcomeKind},
};

use super::{WbPlanApprovalResult, WbPlanResult};

#[derive(Debug)]
pub(super) enum WritePermitFailure {
    Authorization,
    PreflightRead,
    PreconditionChanged(Box<WbCampaignBidSnapshot>),
    Store(PlanStoreError),
}

pub(super) fn plan_result(plan: &WbControlPlan) -> WbPlanResult {
    WbPlanResult {
        plan_id: plan.plan_id.clone(),
        plan_digest: plan.plan_digest.clone(),
        actor_id: plan.actor_id.clone(),
        account_id: plan.account_id.clone(),
        seller_sid: plan.before.seller_sid.clone(),
        advert_id: plan.advert_id,
        policy_schema_version: plan.schema_version,
        policy_revision: plan.policy_revision,
        policy_digest: plan.policy_digest.clone(),
        action_quota: plan.action_quota,
        status: plan.status,
        approval: plan.approval.as_ref().map(|approval| WbPlanApprovalResult {
            approval_id: approval.approval_id.clone(),
            approver_id: approval.approver_id.clone(),
            approved_at: format_timestamp(approval.approved_at),
            expires_at: format_timestamp(approval.expires_at),
        }),
        changes: plan.changes.clone(),
        created_at: format_timestamp(plan.created_at),
        expires_at: format_timestamp(plan.expires_at),
        last_error_class: plan.last_error_class.clone(),
        requires_reconciliation: matches!(
            plan.status,
            WbPlanStatus::ReconciliationRequired | WbPlanStatus::Ambiguous
        ),
    }
}

fn format_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

pub(super) fn plan_store_error(error: PlanStoreError) -> String {
    match error {
        PlanStoreError::NotFound => "CONTROL_PLAN_NOT_FOUND".to_owned(),
        PlanStoreError::InvalidState => "CONTROL_PLAN_ALREADY_USED".to_owned(),
        PlanStoreError::Expired => "CONTROL_PLAN_EXPIRED".to_owned(),
        PlanStoreError::ApprovalRequired => "CONTROL_PLAN_APPROVAL_REQUIRED".to_owned(),
        PlanStoreError::ApprovalExpired => "CONTROL_PLAN_APPROVAL_EXPIRED".to_owned(),
        PlanStoreError::PlanChanged => "CONTROL_PLAN_CHANGED".to_owned(),
        PlanStoreError::PolicyChanged => "CONTROL_POLICY_CHANGED".to_owned(),
        PlanStoreError::CampaignLocked => "CONTROL_CAMPAIGN_INCIDENT_LOCKED".to_owned(),
        PlanStoreError::RuntimeDisabled => "CONTROL_RUNTIME_DISABLED".to_owned(),
        PlanStoreError::QuotaExceeded => "CONTROL_ACTION_LIMIT_REACHED".to_owned(),
        PlanStoreError::PrepareLimitExceeded => "CONTROL_PREPARE_LIMIT_REACHED".to_owned(),
        PlanStoreError::Busy => "CONTROL_CAMPAIGN_BUSY".to_owned(),
        PlanStoreError::ApplyInProgress => "CONTROL_PLAN_APPLY_IN_PROGRESS".to_owned(),
        PlanStoreError::InvalidPlan => "CONTROL_PLAN_INVALID".to_owned(),
        PlanStoreError::Unavailable => "CONTROL_PERSISTENCE_UNAVAILABLE".to_owned(),
    }
}

pub(super) const fn guarded_write_permit_error_class(error: &WritePermitFailure) -> &'static str {
    match error {
        WritePermitFailure::Authorization => "access_revoked",
        WritePermitFailure::PreflightRead => "preflight_read_failed",
        WritePermitFailure::PreconditionChanged(_) => "precondition_changed",
        WritePermitFailure::Store(error) => match error {
            PlanStoreError::ApprovalRequired | PlanStoreError::ApprovalExpired => {
                "approval_revoked"
            }
            PlanStoreError::PlanChanged | PlanStoreError::PolicyChanged => "policy_changed",
            PlanStoreError::CampaignLocked => "incident_lock",
            PlanStoreError::RuntimeDisabled => "runtime_gate_revoked",
            PlanStoreError::QuotaExceeded => "quota_revoked",
            PlanStoreError::NotFound
            | PlanStoreError::InvalidState
            | PlanStoreError::Expired
            | PlanStoreError::PrepareLimitExceeded
            | PlanStoreError::Busy
            | PlanStoreError::ApplyInProgress
            | PlanStoreError::InvalidPlan
            | PlanStoreError::Unavailable => "write_permit_unavailable",
        },
    }
}

pub(super) const fn write_failure_finish(error: &WbWriteError) -> (WbPlanStatus, &'static str) {
    match error.outcome_kind() {
        WbWriteOutcomeKind::DefiniteFailure => (WbPlanStatus::Failed, "wb_write_rejected"),
        WbWriteOutcomeKind::Ambiguous => (WbPlanStatus::Ambiguous, "wb_write_ambiguous"),
    }
}
