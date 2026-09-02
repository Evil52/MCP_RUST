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
}
