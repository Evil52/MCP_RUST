mod automation;
mod automation_executor;
mod automation_observer;
mod automation_postgres;
mod config;
mod plan;
mod policy;
mod server;
mod wb;

pub use automation::{
    WbAutomationAction, WbAutomationBidChange, WbAutomationBidReason, WbAutomationCampaignMetrics,
    WbAutomationDecision, WbAutomationDecisionError, WbAutomationDisableReason,
    WbAutomationHoldReason, WbAutomationObservation, WbAutomationPolicy,
    WbAutomationSkuObservation, evaluate_wb_automation, validate_wb_automation_policy,
    wb_automation_business_date,
};
pub use automation_executor::{
    WbAutomationExecutionOutcome, WbAutomationExecutionReceipt, WbAutomationExecutor,
};
pub use automation_observer::{
    WbAutomationObserver, WbAutomationSnapshot, WbAutomationStateView,
    persist_wb_automation_snapshot,
};
#[cfg(coverage)]
#[doc(hidden)]
pub use automation_postgres::exercise_coverage_only_database_mappings;
pub use automation_postgres::{
    WbAutomationActionReservation, WbAutomationCampaignLease, WbAutomationDatabaseState,
    WbAutomationDurableAction, WbAutomationDurableActionKind, WbAutomationDurableActionStatus,
    WbAutomationLegacyStateSeed, WbAutomationPostgresError, WbAutomationPostgresStore,
    WbAutomationReservationReceipt, WbAutomationStateTransitionReceipt,
};
pub use config::{
    ControlAppConfig, ControlAuthConfig, ControlPolicyDatabaseConfig, ControlWbRuntimeConfig,
};
pub use plan::{PlanStoreError, WbActionQuota, WbPlanRepository, WbPlanStatus};
pub use policy::{ControlMode, ControlPolicy, WbActionLimits, WbBidPlacement};
pub use server::{ControlMcp, WbControlServices};
pub use wb::{WbBidChange, WbBidWriteClient, WbPreparedBidChange};
