//! Fail-closed advertising control MCP.
//!
//! Marketplace writes live here rather than in the analytics server. The WB
//! flow is a short-lived durable plan followed by a one-time apply and explicit
//! reconciliation; absent runtime gates keep the original local-only behavior.

mod config;
mod plan;
mod policy;
mod server;
mod wb;

pub use config::{
    ControlAppConfig, ControlAuthConfig, ControlPolicyDatabaseConfig, ControlWbRuntimeConfig,
};
pub use plan::{
    PlanStoreError, WbActionQuota, WbApplyContext, WbControlPlan, WbPlanApproval, WbPlanRepository,
    WbPlanStatus, WbPrepareReservation,
};
pub use policy::{
    ControlMode, ControlPolicy, WbActionLimits, WbBidPlacement, WbPromotionBidTargetPolicy,
};
pub use server::{ControlMcp, WbControlServices};
pub use wb::{WbBidChange, WbBidWriteClient, WbGuardedWriteError, WbPreparedBidChange};
