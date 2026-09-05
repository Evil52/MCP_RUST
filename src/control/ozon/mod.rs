mod client;
mod executor_lease;
mod guard;
mod guard_runtime;
mod guard_workflow;
mod launch_workflow;
mod model;
mod pacing;
mod plan;
mod position;
mod repository;
mod static_guard;
mod static_state;

pub use client::{
    OzonAdsWriteClient, OzonCampaignCreateRequest, OzonCampaignProduct,
    OzonCampaignProductsRequest, OzonCampaignStrategy, OzonGuardedWriteError, OzonPlacement,
    OzonWriteError, OzonWriteErrorKind,
};
pub(super) use executor_lease::OzonExecutorLease;
pub use guard::{
    OzonGuardEvaluationError, OzonGuardStopReason, OzonProductGuardError,
    evaluate_ozon_campaign_guard, parse_ozon_campaign_product,
    validate_ozon_campaign_product_guard,
};
pub use guard_runtime::run_ozon_campaign_guard;
pub use guard_workflow::{
    OzonGuardEvidence, OzonGuardMetricRow, OzonGuardMetrics, OzonGuardTelemetryError,
    OzonStaticGuardFirstStep, aggregate_complete_guard_metrics, group_static_guard_metric_windows,
    parse_complete_running_campaigns, plan_static_guard_first_step,
};
#[cfg(test)]
pub(in crate::control) use launch_workflow::{
    ensure_ozon_sku_not_running, exact_ozon_launch_readback, find_ozon_campaign_by_title,
    positive_json_u64,
};
pub use model::{
    OzonCampaignGuard, OzonGuardStopReadback, OzonLaunchStatus, OzonPlanStoreError,
    OzonStaticGuardMutation, OzonStaticGuardWriteIntent,
};
pub use pacing::{
    OzonBidPacingAction, OzonBidPacingError, OzonBidPacingHoldReason, OzonBidPacingObservation,
    OzonBidPacingPauseReason, OzonBidPacingPolicy, OzonPositionSignal, evaluate_ozon_bid_pacing,
};
pub use plan::{
    OzonCampaignLaunchManifest, OzonCampaignLaunchSpec, OzonLaunchPlanError,
    prepare_campaign_launch_manifest,
};
pub use position::{OzonBidPositionReadError, OzonBidPositionReader};
pub use repository::OzonPlanRepository;
pub use static_guard::{
    DEFAULT_OZON_STATIC_MAX_CPC_BID_MICROROUBLES, DEFAULT_OZON_STATIC_MIN_CPC_BID_MICROROUBLES,
    MAX_OZON_STATIC_GUARD_FILE_BYTES, MAX_OZON_STATIC_GUARDS, OzonStaticCampaignGuard,
    OzonStaticDynamicBidControl, OzonStaticGuardConfig, OzonStaticGuardError,
    parse_ozon_static_guard_config, parse_ozon_static_guards, validate_ozon_static_guard_policy,
};
pub use static_state::{
    MAX_OZON_STATIC_GUARD_STATE_BYTES, OzonStaticCampaignMutationKind, OzonStaticGuardIncident,
    OzonStaticGuardState, OzonStaticGuardStateError, OzonStaticGuardStateLease,
    OzonStaticPendingBidChange, OzonStaticPendingCampaignMutation, load_ozon_static_guard_state,
    persist_ozon_static_guard_state, validate_ozon_static_guard_state_scope,
};

pub(in crate::control) use model::OzonCampaignPlan;

#[cfg(test)]
mod tests;
