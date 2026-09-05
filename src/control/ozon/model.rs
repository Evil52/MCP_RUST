use chrono::{DateTime, Utc};
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::OzonCampaignLaunchManifest;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OzonLaunchStatus {
    Prepared,
    Approved,
    Creating,
    Created,
    AddingProducts,
    ProductsAdded,
    Activating,
    Applied,
    Ambiguous,
    Failed,
    Expired,
}

/// One externally visible mutation in the durable Ozon launch workflow.
///
/// The action is persisted separately from the plan status.  In particular an
/// `ambiguous` plan keeps the exact action whose outcome must be read back, so
/// a recovery worker never guesses which mutation may have reached Ozon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::control) enum OzonLaunchAction {
    CreateCampaign,
    AddProducts,
    ActivateCampaign,
}

impl OzonLaunchAction {
    pub(in crate::control) const fn as_db(self) -> &'static str {
        match self {
            Self::CreateCampaign => "create_campaign",
            Self::AddProducts => "add_products",
            Self::ActivateCampaign => "activate_campaign",
        }
    }

    pub(super) fn from_db(value: &str) -> Result<Self, OzonPlanStoreError> {
        match value {
            "create_campaign" => Ok(Self::CreateCampaign),
            "add_products" => Ok(Self::AddProducts),
            "activate_campaign" => Ok(Self::ActivateCampaign),
            _ => Err(OzonPlanStoreError::Unavailable),
        }
    }

    pub(super) const fn stable_status(self) -> OzonLaunchStatus {
        match self {
            Self::CreateCampaign => OzonLaunchStatus::Approved,
            Self::AddProducts => OzonLaunchStatus::Created,
            Self::ActivateCampaign => OzonLaunchStatus::ProductsAdded,
        }
    }

    pub(super) const fn in_progress_status(self) -> OzonLaunchStatus {
        match self {
            Self::CreateCampaign => OzonLaunchStatus::Creating,
            Self::AddProducts => OzonLaunchStatus::AddingProducts,
            Self::ActivateCampaign => OzonLaunchStatus::Activating,
        }
    }

    pub(super) const fn completed_status(self) -> OzonLaunchStatus {
        match self {
            Self::CreateCampaign => OzonLaunchStatus::Created,
            Self::AddProducts => OzonLaunchStatus::ProductsAdded,
            Self::ActivateCampaign => OzonLaunchStatus::Applied,
        }
    }

    pub(super) const fn next(self) -> Option<Self> {
        match self {
            Self::CreateCampaign => Some(Self::AddProducts),
            Self::AddProducts => Some(Self::ActivateCampaign),
            Self::ActivateCampaign => None,
        }
    }
}

/// Whether a lease may execute a new mutation or may only inspect Ozon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control) enum OzonLaunchClaimMode {
    Execute,
    Reconcile,
}

impl OzonLaunchStatus {
    pub(super) const fn as_db(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Approved => "approved",
            Self::Creating => "creating",
            Self::Created => "created",
            Self::AddingProducts => "adding_products",
            Self::ProductsAdded => "products_added",
            Self::Activating => "activating",
            Self::Applied => "applied",
            Self::Ambiguous => "ambiguous",
            Self::Failed => "failed",
            Self::Expired => "expired",
        }
    }

    pub(super) fn from_db(value: &str) -> Result<Self, OzonPlanStoreError> {
        match value {
            "prepared" => Ok(Self::Prepared),
            "approved" => Ok(Self::Approved),
            "creating" => Ok(Self::Creating),
            "created" => Ok(Self::Created),
            "adding_products" => Ok(Self::AddingProducts),
            "products_added" => Ok(Self::ProductsAdded),
            "activating" => Ok(Self::Activating),
            "applied" => Ok(Self::Applied),
            "ambiguous" => Ok(Self::Ambiguous),
            "failed" => Ok(Self::Failed),
            "expired" => Ok(Self::Expired),
            _ => Err(OzonPlanStoreError::Unavailable),
        }
    }

    #[must_use]
    pub const fn requires_reconciliation(self) -> bool {
        matches!(
            self,
            Self::Creating | Self::AddingProducts | Self::Activating | Self::Ambiguous
        )
    }

    #[must_use]
    pub const fn is_durable_workflow_pending(self) -> bool {
        matches!(
            self,
            Self::Approved
                | Self::Creating
                | Self::Created
                | Self::AddingProducts
                | Self::ProductsAdded
                | Self::Activating
                | Self::Ambiguous
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::control) struct OzonPlanApproval {
    pub(in crate::control) approval_id: String,
    pub(in crate::control) approver_id: String,
    pub(in crate::control) reference: String,
    pub(in crate::control) approved_at: DateTime<Utc>,
    pub(in crate::control) expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::control) struct OzonCampaignPlan {
    pub(in crate::control) plan_id: String,
    pub(in crate::control) plan_digest: String,
    pub(in crate::control) actor_id: String,
    pub(in crate::control) account_id: String,
    pub(in crate::control) sku: u64,
    pub(in crate::control) schema_version: u32,
    pub(in crate::control) policy_revision: u64,
    pub(in crate::control) policy_digest: String,
    pub(in crate::control) manifest: OzonCampaignLaunchManifest,
    pub(in crate::control) status: OzonLaunchStatus,
    pub(in crate::control) approval: Option<OzonPlanApproval>,
    pub(in crate::control) campaign_id: Option<u64>,
    pub(in crate::control) created_at: DateTime<Utc>,
    pub(in crate::control) expires_at: DateTime<Utc>,
    pub(in crate::control) operation_started_at: Option<DateTime<Utc>>,
    pub(in crate::control) finished_at: Option<DateTime<Utc>>,
    pub(in crate::control) last_error_class: Option<String>,
    pub(in crate::control) readback: Option<Value>,
    pub(in crate::control) execution_requested_at: Option<DateTime<Utc>>,
    pub(in crate::control) current_action: OzonLaunchAction,
    pub(in crate::control) workflow_generation: u64,
    pub(in crate::control) workflow_lease_expires_at: Option<DateTime<Utc>>,
    pub(in crate::control) workflow_write_started_at: Option<DateTime<Utc>>,
}

/// Fencing capability for exactly one durable launch-workflow action.
///
/// Callers must present the full `(plan_id, generation, owner_id, token)` tuple
/// to every state-changing repository method.  A reclaimed lease increments
/// `generation`, permanently fencing a delayed worker from committing.
#[derive(Debug, Clone)]
pub(in crate::control) struct OzonLaunchLease {
    pub(in crate::control) plan: OzonCampaignPlan,
    pub(in crate::control) action: OzonLaunchAction,
    pub(in crate::control) mode: OzonLaunchClaimMode,
    pub(in crate::control) generation: u64,
    pub(in crate::control) owner_id: String,
    pub(in crate::control) lease_token: String,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonPlanStoreError {
    #[error("Ozon control plan store недоступен")]
    Unavailable,
    #[error("Ozon control plan не найден")]
    NotFound,
    #[error("Ozon control plan уже использован или имеет неверное состояние")]
    InvalidState,
    #[error("Ozon control plan истёк")]
    Expired,
    #[error("Ozon control plan approval истёк")]
    ApprovalExpired,
    #[error("Ozon control plan digest изменился")]
    PlanChanged,
    #[error("Ozon control policy изменилась")]
    PolicyChanged,
    #[error("Ozon runtime gate выключен или истёк")]
    RuntimeDisabled,
    #[error("Ozon SKU заблокирован незакрытым incident")]
    SkuLocked,
    #[error("Ozon control plan имеет недопустимые данные")]
    InvalidPlan,
    #[error("Ozon durable-workflow lease утрачен")]
    LeaseLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OzonCampaignGuardStatus {
    Active,
    Stopping,
    Stopped,
    Incident,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OzonCampaignGuard {
    pub plan_id: String,
    pub account_id: String,
    pub sku: u64,
    pub campaign_id: u64,
    pub date_from: String,
    pub spend_cap_microrubles: u64,
    pub target_drr_percent: u8,
    pub status: OzonCampaignGuardStatus,
    pub stop_reason: Option<String>,
    pub incident_error_class: Option<String>,
}

/// Fencing capability for a stop that may already have reached Ozon.
#[derive(Debug, Clone)]
pub struct OzonGuardStopLease {
    pub guard: OzonCampaignGuard,
    pub stop_reason: String,
    /// Last complete metrics snapshot persisted with the stop intent. Recovery
    /// must reuse it instead of silently replacing the evidence with zeroes.
    pub spend_minor: Option<u64>,
    pub revenue_minor: Option<u64>,
    pub generation: u64,
    pub owner_id: String,
    pub lease_token: String,
    pub lease_expires_at: DateTime<Utc>,
    /// Durable mutation boundary. Once present, recovery is readback-only and
    /// must never send another deactivate request.
    pub write_started_at: Option<DateTime<Utc>>,
}

/// Bounded provider observation persisted in the append-only guard audit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OzonGuardStopReadback {
    Stopped,
    Running,
    Unavailable,
}

impl OzonGuardStopReadback {
    pub(super) const fn event_type(self) -> &'static str {
        match self {
            Self::Stopped => "guard_stop_readback_stopped",
            Self::Running => "guard_stop_readback_running",
            Self::Unavailable => "guard_stop_readback_unavailable",
        }
    }
}

/// Exact marketplace mutation authorized for one static guard entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OzonStaticGuardMutation {
    Activate,
    Deactivate,
    SetBid,
}

impl OzonStaticGuardMutation {
    pub(super) const fn as_db(self) -> &'static str {
        match self {
            Self::Activate => "activate",
            Self::Deactivate => "deactivate",
            Self::SetBid => "set_bid",
        }
    }
}

/// Immutable identity recorded at the static guard's local write marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OzonStaticGuardWriteIntent {
    pub account_id: String,
    pub sku: u64,
    pub campaign_id: u64,
    pub mutation: OzonStaticGuardMutation,
    pub target_bid_microrubles: Option<u64>,
    /// SHA-256 of the exact validated static guard configuration bytes.
    pub config_digest: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_launch_status_has_an_exact_database_and_reconciliation_contract() {
        for (status, database) in [
            (OzonLaunchStatus::Prepared, "prepared"),
            (OzonLaunchStatus::Approved, "approved"),
            (OzonLaunchStatus::Creating, "creating"),
            (OzonLaunchStatus::Created, "created"),
            (OzonLaunchStatus::AddingProducts, "adding_products"),
            (OzonLaunchStatus::ProductsAdded, "products_added"),
            (OzonLaunchStatus::Activating, "activating"),
            (OzonLaunchStatus::Applied, "applied"),
            (OzonLaunchStatus::Ambiguous, "ambiguous"),
            (OzonLaunchStatus::Failed, "failed"),
            (OzonLaunchStatus::Expired, "expired"),
        ] {
            assert_eq!(status.as_db(), database);
            assert_eq!(OzonLaunchStatus::from_db(database), Ok(status));
        }
        assert_eq!(
            OzonLaunchStatus::from_db("unknown"),
            Err(OzonPlanStoreError::Unavailable)
        );
        for status in [
            OzonLaunchStatus::Creating,
            OzonLaunchStatus::AddingProducts,
            OzonLaunchStatus::Activating,
            OzonLaunchStatus::Ambiguous,
        ] {
            assert!(status.requires_reconciliation());
        }
        assert!(!OzonLaunchStatus::Applied.requires_reconciliation());
        for status in [
            OzonLaunchStatus::Approved,
            OzonLaunchStatus::Creating,
            OzonLaunchStatus::Created,
            OzonLaunchStatus::AddingProducts,
            OzonLaunchStatus::ProductsAdded,
            OzonLaunchStatus::Activating,
            OzonLaunchStatus::Ambiguous,
        ] {
            assert!(status.is_durable_workflow_pending());
        }
        assert!(!OzonLaunchStatus::Applied.is_durable_workflow_pending());
    }

    #[test]
    fn launch_actions_define_exact_state_boundaries() {
        for (action, database, stable, in_progress, completed, next) in [
            (
                OzonLaunchAction::CreateCampaign,
                "create_campaign",
                OzonLaunchStatus::Approved,
                OzonLaunchStatus::Creating,
                OzonLaunchStatus::Created,
                Some(OzonLaunchAction::AddProducts),
            ),
            (
                OzonLaunchAction::AddProducts,
                "add_products",
                OzonLaunchStatus::Created,
                OzonLaunchStatus::AddingProducts,
                OzonLaunchStatus::ProductsAdded,
                Some(OzonLaunchAction::ActivateCampaign),
            ),
            (
                OzonLaunchAction::ActivateCampaign,
                "activate_campaign",
                OzonLaunchStatus::ProductsAdded,
                OzonLaunchStatus::Activating,
                OzonLaunchStatus::Applied,
                None,
            ),
        ] {
            assert_eq!(action.as_db(), database);
            assert_eq!(OzonLaunchAction::from_db(database), Ok(action));
            assert_eq!(action.stable_status(), stable);
            assert_eq!(action.in_progress_status(), in_progress);
            assert_eq!(action.completed_status(), completed);
            assert_eq!(action.next(), next);
        }
        assert_eq!(
            OzonLaunchAction::from_db("unknown"),
            Err(OzonPlanStoreError::Unavailable)
        );
    }
}
