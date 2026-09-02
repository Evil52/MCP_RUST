mod client;
mod guard;
mod model;
mod plan;
mod repository;

pub use client::{
    OzonAdsWriteClient, OzonCampaignCreateRequest, OzonCampaignProduct,
    OzonCampaignProductsRequest, OzonCampaignStrategy, OzonGuardedWriteError, OzonPlacement,
    OzonWriteError, OzonWriteErrorKind,
};
pub use guard::{OzonGuardEvaluationError, OzonGuardStopReason, evaluate_ozon_campaign_guard};
pub use model::{OzonCampaignGuard, OzonLaunchStatus, OzonPlanStoreError};
pub use plan::{
    OzonCampaignLaunchManifest, OzonCampaignLaunchSpec, OzonLaunchPlanError,
    prepare_campaign_launch_manifest,
};
pub use repository::OzonPlanRepository;

pub(in crate::control) use model::OzonCampaignPlan;

#[cfg(test)]
mod tests;
