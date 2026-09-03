mod client;
mod guard;
mod model;
mod pacing;
mod plan;
mod position;
mod repository;

pub use client::{
    OzonAdsWriteClient, OzonCampaignCreateRequest, OzonCampaignProduct,
    OzonCampaignProductsRequest, OzonCampaignStrategy, OzonGuardedWriteError, OzonPlacement,
    OzonWriteError, OzonWriteErrorKind,
};
pub use guard::{
    OzonGuardEvaluationError, OzonGuardStopReason, OzonProductGuardError,
    evaluate_ozon_campaign_guard, validate_ozon_campaign_product_guard,
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

pub(in crate::control) use model::OzonCampaignPlan;

#[cfg(test)]
mod tests;
