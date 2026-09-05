mod automation;
mod automation_executor;
mod automation_observer;
mod automation_postgres;
mod config;
mod ozon;
mod plan;
mod policy;
mod server;
mod wb;

pub use automation::{
    WbAutomationAction, WbAutomationBidChange, WbAutomationBidReason, WbAutomationCampaignMetrics,
    WbAutomationDecision, WbAutomationDecisionError, WbAutomationDisableReason,
    WbAutomationHoldReason, WbAutomationObservation, WbAutomationPacingMode, WbAutomationPolicy,
    WbAutomationSkuObservation, evaluate_wb_automation, validate_wb_automation_policy,
    wb_automation_business_date,
};
pub use automation_executor::{
    WbAutomationExecutionOutcome, WbAutomationExecutionReceipt, WbAutomationExecutor,
    WbAutomationPostgresExecutionReceipt,
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
    ControlAppConfig, ControlAuthConfig, ControlOzonRuntimeConfig, ControlPolicyDatabaseConfig,
    ControlWbRuntimeConfig,
};
pub use ozon::{
    DEFAULT_OZON_STATIC_MAX_CPC_BID_MICROROUBLES, DEFAULT_OZON_STATIC_MIN_CPC_BID_MICROROUBLES,
    MAX_OZON_STATIC_GUARD_FILE_BYTES, MAX_OZON_STATIC_GUARD_STATE_BYTES, MAX_OZON_STATIC_GUARDS,
    OzonAdsWriteClient, OzonBidPacingAction, OzonBidPacingError, OzonBidPacingHoldReason,
    OzonBidPacingObservation, OzonBidPacingPauseReason, OzonBidPacingPolicy,
    OzonBidPositionReadError, OzonBidPositionReader, OzonCampaignCreateRequest, OzonCampaignGuard,
    OzonCampaignLaunchManifest, OzonCampaignLaunchSpec, OzonCampaignProduct,
    OzonCampaignProductsRequest, OzonCampaignStrategy, OzonGuardEvaluationError, OzonGuardEvidence,
    OzonGuardMetricRow, OzonGuardMetrics, OzonGuardStopReadback, OzonGuardStopReason,
    OzonGuardTelemetryError, OzonGuardedWriteError, OzonLaunchPlanError, OzonLaunchStatus,
    OzonPlacement, OzonPlanRepository, OzonPlanStoreError, OzonPositionSignal,
    OzonProductGuardError, OzonStaticCampaignGuard, OzonStaticCampaignMutationKind,
    OzonStaticDynamicBidControl, OzonStaticGuardConfig, OzonStaticGuardError,
    OzonStaticGuardFirstStep, OzonStaticGuardIncident, OzonStaticGuardState,
    OzonStaticGuardStateError, OzonStaticGuardStateLease, OzonStaticPendingBidChange,
    OzonStaticPendingCampaignMutation, OzonWriteError, OzonWriteErrorKind,
    aggregate_complete_guard_metrics, evaluate_ozon_bid_pacing, evaluate_ozon_campaign_guard,
    group_static_guard_metric_windows, load_ozon_static_guard_state,
    parse_complete_running_campaigns, parse_ozon_campaign_product, parse_ozon_static_guard_config,
    parse_ozon_static_guards, persist_ozon_static_guard_state, plan_static_guard_first_step,
    prepare_campaign_launch_manifest, run_ozon_campaign_guard,
    validate_ozon_campaign_product_guard, validate_ozon_static_guard_policy,
    validate_ozon_static_guard_state_scope,
};
pub use plan::{PlanStoreError, WbActionQuota, WbPlanRepository, WbPlanStatus};
pub use policy::{ControlMode, ControlPolicy, WbActionLimits, WbBidPlacement};
pub use server::{ControlMcp, OzonControlServices, WbControlServices};
pub use wb::{
    WbBidChange, WbBidWriteClient, WbCampaignBidType, WbCampaignPaymentType,
    WbCreateCampaignRequest, WbPreparedBidChange,
};
