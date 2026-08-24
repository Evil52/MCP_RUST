mod automation;
mod automation_executor;
mod automation_observer;
mod config;
mod plan;
mod policy;
mod server;
mod wb;

pub use automation::{
    WbAutomationAction, WbAutomationBidChange, WbAutomationBidReason, WbAutomationDecision,
    WbAutomationDecisionError, WbAutomationDisableReason, WbAutomationHoldReason,
    WbAutomationObservation, WbAutomationPolicy, WbAutomationSkuObservation,
    evaluate_wb_automation, validate_wb_automation_policy,
};
pub use automation_executor::{
    WbAutomationExecutionOutcome, WbAutomationExecutionReceipt, WbAutomationExecutor,
};
pub use automation_observer::{
    WbAutomationObserver, WbAutomationSnapshot, WbAutomationStateView,
    persist_wb_automation_snapshot,
};
pub use config::{
    ControlAppConfig, ControlAuthConfig, ControlPolicyDatabaseConfig, ControlWbRuntimeConfig,
};
pub use plan::{PlanStoreError, WbActionQuota, WbPlanRepository, WbPlanStatus};
pub use policy::{ControlMode, ControlPolicy, WbActionLimits, WbBidPlacement};
pub use server::{ControlMcp, WbControlServices};
pub use wb::{WbBidChange, WbBidWriteClient, WbPreparedBidChange};
