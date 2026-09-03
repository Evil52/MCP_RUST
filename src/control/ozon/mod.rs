mod client;
mod guard;
mod model;
mod plan;
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
pub use plan::{
    OzonCampaignLaunchManifest, OzonCampaignLaunchSpec, OzonLaunchPlanError,
    prepare_campaign_launch_manifest,
};
pub use repository::OzonPlanRepository;
pub use static_guard::{
    DEFAULT_OZON_STATIC_MAX_CPC_BID_MICROROUBLES, DEFAULT_OZON_STATIC_MIN_CPC_BID_MICROROUBLES,
    MAX_OZON_STATIC_GUARD_FILE_BYTES, MAX_OZON_STATIC_GUARDS, OzonStaticCampaignGuard,
    OzonStaticGuardError, parse_ozon_static_guards,
};

pub(in crate::control) use model::OzonCampaignPlan;

#[cfg(test)]
mod tests;
