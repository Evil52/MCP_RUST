use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WbReviewInput {
    pub version: u32,
    pub account_id: String,
    pub as_of: DateTime<Utc>,
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
    pub coverage: Vec<DayCoverage>,
    pub advertising: Vec<AdvertisingDay>,
    pub products: Vec<ProductEvidence>,
    pub composition: CampaignComposition,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DayCoverage {
    pub date: NaiveDate,
    pub snapshot_id: u64,
    pub complete: bool,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdvertisingDay {
    pub date: NaiveDate,
    pub campaign_id: u64,
    /// Campaign-level expense without SKU is retained separately, never allocated.
    pub sku: Option<u64>,
    pub spend_minor: u64,
    pub clicks: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProductEvidence {
    pub sku: u64,
    pub ordered_units: Option<u64>,
    pub fbw: Option<StockObservation>,
    /// Only deliveryType=1 seller warehouses, or an explicitly captured FBS total.
    pub fbs: Option<StockObservation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StockQuality {
    Complete,
    Partial,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StockObservation {
    /// None is an unobserved value, never a fabricated zero.
    pub units: Option<u64>,
    pub quality: StockQuality,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignComposition {
    pub observed_at: DateTime<Utc>,
    /// Complete scope means all active AND paused campaigns, including all SKUs.
    pub active_and_paused_complete: bool,
    pub campaigns: Vec<CurrentCampaign>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignStatus {
    Active,
    Paused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingModel {
    Cpc,
    Cpm,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CurrentCampaign {
    pub campaign_id: u64,
    pub status: CampaignStatus,
    pub pricing_model: PricingModel,
    pub sku_ids: Vec<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PromotionState {
    Active,
    Paused,
    AbsentFromActiveAndPaused,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InventorySignal {
    StockPresent,
    FbwZeroCheckFbs,
    BothChannelsZero,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewAction {
    Observe,
    ReviewActiveProduct,
    ReviewBeforeResuming,
    ReviewHistoricalSpend,
    VerifyCurrentComposition,
    RestoreAdvertisingCoverage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CampaignLink {
    pub campaign_id: u64,
    pub status: CampaignStatus,
    pub pricing_model: PricingModel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductReview {
    pub sku: u64,
    pub observed_ad_dates: Vec<NaiveDate>,
    pub missing_ad_dates: Vec<NaiveDate>,
    pub historical_campaign_ids: Vec<u64>,
    pub current_campaigns: Vec<CampaignLink>,
    pub promotion_state: PromotionState,
    pub spend_minor: u64,
    pub clicks: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
    pub ordered_units: Option<u64>,
    pub spend_without_observed_ad_orders: bool,
    pub fbw: Option<StockObservation>,
    pub fbs: Option<StockObservation>,
    pub fbw_has_stock: Option<bool>,
    pub fbs_has_stock: Option<bool>,
    pub inventory_signal: InventorySignal,
    pub action: ReviewAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent coverage, freshness, attribution and execution guarantees remain explicit"
)]
pub struct WbReviewReport {
    pub version: u32,
    pub mode: &'static str,
    pub marketplace: &'static str,
    pub currency: &'static str,
    pub account_id: String,
    pub as_of: DateTime<Utc>,
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
    pub source_sha256: String,
    pub advertising_coverage_complete: bool,
    pub composition_complete_and_fresh: bool,
    pub composition_observed_at: DateTime<Utc>,
    pub attribution_maturity_verified: bool,
    pub auto_apply_allowed: bool,
    pub campaign_rows_without_sku: Vec<AdvertisingDay>,
    pub products: Vec<ProductReview>,
}
