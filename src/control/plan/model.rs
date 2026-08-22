use chrono::{DateTime, Utc};
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::control::wb::{WbBidChange, WbCampaignBidSnapshot, WbPreparedBidChange};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WbPlanStatus {
    Prepared,
    Approved,
    Applying,
    Applied,
    ReconciliationRequired,
    Ambiguous,
    Rejected,
    Failed,
    Expired,
}

impl WbPlanStatus {
    pub(super) const fn as_db(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Approved => "approved",
            Self::Applying => "applying",
            Self::Applied => "applied",
            Self::ReconciliationRequired => "reconciliation_required",
            Self::Ambiguous => "ambiguous",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
            Self::Expired => "expired",
        }
    }

    pub(super) fn from_db(value: &str) -> Result<Self, PlanStoreError> {
        match value {
            "prepared" => Ok(Self::Prepared),
            "approved" => Ok(Self::Approved),
            "applying" => Ok(Self::Applying),
            "applied" => Ok(Self::Applied),
            "reconciliation_required" => Ok(Self::ReconciliationRequired),
            "ambiguous" => Ok(Self::Ambiguous),
            "rejected" => Ok(Self::Rejected),
            "failed" => Ok(Self::Failed),
            "expired" => Ok(Self::Expired),
            _ => Err(PlanStoreError::Unavailable),
        }
    }
}

/// Immutable rolling limits copied from the policy into the plan digest.
///
/// A reservation is consumed as soon as apply is claimed, including definite
/// failures and ambiguous outcomes, so retries cannot bypass these limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbActionQuota {
    pub max_actions_per_hour: u32,
    pub max_actions_per_day: u32,
    pub cooldown_seconds: u64,
    pub max_cumulative_abs_delta_kopecks_per_day: u64,
}

impl WbActionQuota {
    pub(super) fn validate(self) -> Result<(), PlanStoreError> {
        if self.max_actions_per_hour == 0
            || self.max_actions_per_hour > 60
            || self.max_actions_per_day < self.max_actions_per_hour
            || self.max_actions_per_day > 500
            || !(30..=86_400).contains(&self.cooldown_seconds)
            || self.max_cumulative_abs_delta_kopecks_per_day == 0
            || i32::try_from(self.max_actions_per_hour).is_err()
            || i32::try_from(self.max_actions_per_day).is_err()
            || i32::try_from(self.cooldown_seconds).is_err()
            || i64::try_from(self.max_cumulative_abs_delta_kopecks_per_day).is_err()
        {
            return Err(PlanStoreError::InvalidPlan);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub(in crate::control) struct WbPlanApproval {
    pub(in crate::control) approval_id: String,
    pub(in crate::control) approver_id: String,
    pub(in crate::control) reason: String,
    pub(in crate::control) approved_at: DateTime<Utc>,
    pub(in crate::control) expires_at: DateTime<Utc>,
}

/// Short-lived, append-only authorization to perform the WB read needed to
/// prepare one control plan. It must be reserved before calling WB and can be
/// consumed by exactly one matching plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub(in crate::control) struct WbPrepareReservation {
    pub(in crate::control) reservation_id: String,
    pub(in crate::control) actor_id: String,
    pub(in crate::control) account_id: String,
    pub(in crate::control) advert_id: u64,
    pub(in crate::control) schema_version: u32,
    pub(in crate::control) policy_revision: u64,
    pub(in crate::control) policy_digest: String,
    pub(in crate::control) action_quota: WbActionQuota,
    pub(in crate::control) reserved_at: DateTime<Utc>,
    pub(in crate::control) expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::control) struct WbControlPlan {
    pub(in crate::control) plan_id: String,
    pub(in crate::control) plan_digest: String,
    pub(in crate::control) prepare_reservation_id: String,
    pub(in crate::control) actor_id: String,
    pub(in crate::control) account_id: String,
    pub(in crate::control) advert_id: u64,
    pub(in crate::control) schema_version: u32,
    pub(in crate::control) policy_revision: u64,
    pub(in crate::control) policy_digest: String,
    pub(in crate::control) action_quota: WbActionQuota,
    pub(in crate::control) status: WbPlanStatus,
    pub(in crate::control) approval: Option<WbPlanApproval>,
    pub(in crate::control) requested: Vec<WbBidChange>,
    pub(in crate::control) changes: Vec<WbPreparedBidChange>,
    pub(in crate::control) before: WbCampaignBidSnapshot,
    pub(in crate::control) created_at: DateTime<Utc>,
    pub(in crate::control) expires_at: DateTime<Utc>,
    pub(in crate::control) apply_started_at: Option<DateTime<Utc>>,
    pub(in crate::control) last_error_class: Option<String>,
    pub(in crate::control) write_response: Option<Value>,
    pub(in crate::control) readback: Option<WbCampaignBidSnapshot>,
}

pub(in crate::control) struct WbPlanFinish<'a> {
    pub(in crate::control) status: WbPlanStatus,
    pub(in crate::control) error_class: Option<&'a str>,
    pub(in crate::control) write_response: Option<&'a Value>,
    pub(in crate::control) readback: Option<&'a WbCampaignBidSnapshot>,
    pub(in crate::control) now: DateTime<Utc>,
}

#[derive(Debug)]
pub(in crate::control) struct WbApplyContext<'a> {
    pub(in crate::control) plan_id: &'a str,
    pub(in crate::control) actor_id: &'a str,
    pub(in crate::control) expected_plan_digest: &'a str,
    pub(in crate::control) expected_schema_version: u32,
    pub(in crate::control) expected_policy_revision: u64,
    pub(in crate::control) expected_policy_digest: &'a str,
    pub(in crate::control) now: DateTime<Utc>,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum PlanStoreError {
    #[error("WB control plan store недоступен")]
    Unavailable,
    #[error("WB control plan не найден")]
    NotFound,
    #[error("WB control plan уже использован или имеет неверное состояние")]
    InvalidState,
    #[error("WB control plan истёк")]
    Expired,
    #[error("WB control plan требует отдельного server-side approval")]
    ApprovalRequired,
    #[error("WB control plan approval истёк")]
    ApprovalExpired,
    #[error("WB control plan digest не совпадает с подтверждённым")]
    PlanChanged,
    #[error("WB control policy digest изменился после подготовки плана")]
    PolicyChanged,
    #[error("WB campaign заблокирована незакрытым incident")]
    CampaignLocked,
    #[error("WB runtime gate выключен, отсутствует или lease истекла")]
    RuntimeDisabled,
    #[error("WB action quota или cooldown исчерпаны")]
    QuotaExceeded,
    #[error("лимит попыток подготовки WB control plan исчерпан")]
    PrepareLimitExceeded,
    #[error("другая операция для этой WB campaign уже выполняется")]
    Busy,
    #[error("WB control plan всё ещё может выполняться")]
    ApplyInProgress,
    #[error("WB control plan имеет недопустимые данные")]
    InvalidPlan,
}
