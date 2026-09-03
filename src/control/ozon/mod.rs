mod client;
mod guard;
mod model;
mod pacing;
mod plan;
mod position;
mod repository;
mod static_guard;

pub use client::{
    OzonAdsWriteClient, OzonCampaignCreateRequest, OzonCampaignProduct,
    OzonCampaignProductsRequest, OzonCampaignStrategy, OzonGuardedWriteError, OzonPlacement,
    OzonWriteError, OzonWriteErrorKind,
};
pub use guard::{
    OzonGuardEvaluationError, OzonGuardStopReason, OzonProductGuardError,
    evaluate_ozon_campaign_guard, parse_ozon_campaign_product,
    validate_ozon_campaign_product_guard,
};
pub use model::{OzonCampaignGuard, OzonLaunchStatus, OzonPlanStoreError};
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
    parse_ozon_static_guard_config, parse_ozon_static_guards,
};

pub(in crate::control) use model::OzonCampaignPlan;

#[cfg(test)]
mod tests;
