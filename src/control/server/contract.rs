use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::control::{
    plan::{WbActionQuota, WbPlanStatus},
    policy::{ControlMode, WbActionLimits, WbBidPlacement},
    wb::{WbBidChange, WbPreparedBidChange},
};

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EmptyInput {}

#[derive(Debug, Serialize, JsonSchema)]
// These are independent, externally visible capability facts rather than one
// state machine; collapsing them would make the status contract less precise.
#[allow(clippy::struct_excessive_bools)]
pub struct ControlStatusResult {
    pub actor_id: String,
    pub policy_schema_version: u32,
    pub policy_revision: u64,
    pub policy_digest: String,
    pub mode: ControlMode,
    pub explicit_policy_binding: bool,
    pub write_executor_configured: bool,
    pub runtime_gates_required: bool,
    pub credentials_loaded: bool,
    pub marketplace_egress_enabled: bool,
    pub persistence_configured: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ControlScopeResult {
    pub actor_id: String,
    pub policy_schema_version: u32,
    pub policy_revision: u64,
    pub policy_digest: String,
    pub mode: ControlMode,
    pub targets: Vec<ControlTargetResult>,
    pub wb_promotion_bid_targets: Vec<WbPromotionBidTargetResult>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ControlTargetResult {
    pub account_id: String,
    pub campaign_id: u64,
    pub skus: Vec<u64>,
    pub bid_limits: BidLimitsResult,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct BidLimitsResult {
    pub min_minor: u64,
    pub max_minor: u64,
    pub max_delta_percent: u8,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WbPromotionBidTargetResult {
    pub account_id: String,
    pub seller_sid: String,
    pub advert_id: u64,
    pub nm_ids: Vec<u64>,
    pub placements: Vec<WbBidPlacement>,
    pub bid_limits_kopecks: BidLimitsResult,
    pub approver_actor_ids: Vec<String>,
    pub action_limits: WbActionLimits,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PrepareWbBidPlanInput {
    pub account_id: String,
    pub advert_id: u64,
    pub changes: Vec<WbBidChange>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbPlanInput {
    pub plan_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApproveWbBidPlanInput {
    pub plan_id: String,
    pub plan_digest: String,
    pub approval_reference: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplyWbBidPlanInput {
    pub plan_id: String,
    pub plan_digest: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WbPlanResult {
    pub plan_id: String,
    pub plan_digest: String,
    pub actor_id: String,
    pub account_id: String,
    pub seller_sid: String,
    pub advert_id: u64,
    pub policy_schema_version: u32,
    pub policy_revision: u64,
    pub policy_digest: String,
    pub action_quota: WbActionQuota,
    pub status: WbPlanStatus,
    pub approval: Option<WbPlanApprovalResult>,
    pub changes: Vec<WbPreparedBidChange>,
    pub created_at: String,
    pub expires_at: String,
    pub last_error_class: Option<String>,
    pub requires_reconciliation: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WbPlanApprovalResult {
    pub approval_id: String,
    pub approver_id: String,
    pub approved_at: String,
    pub expires_at: String,
}
