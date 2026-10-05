use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::reporting::ads_optimizer::wb_review::{AdvertisingDay, DayCoverage};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WbBaselineInput {
    pub version: u32,
    pub account_id: String,
    pub as_of: DateTime<Utc>,
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
    pub coverage: Vec<DayCoverage>,
    pub advertising: Vec<AdvertisingDay>,
    /// Totals only after complete export validation; never inferred from ads.
    #[serde(default)]
    pub sales: Vec<StoreSalesDay>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSalesDay {
    pub date: NaiveDate,
    pub snapshot_id: u64,
    pub observed_at: DateTime<Utc>,
    pub complete: bool,
    pub ordered_units: u64,
    pub operational_gmv_minor: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct BaselineMetrics {
    pub spend_minor: u64,
    pub clicks: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
    pub drr_bps: Option<u64>,
    pub cpc_minor: Option<u64>,
    pub cpo_minor: Option<u64>,
    pub ad_conversion_bps: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
pub struct StoreSalesTotals {
    pub ordered_units: u64,
    pub operational_gmv_minor: u64,
}

#[derive(Debug, Serialize)]
pub struct ProductBaseline {
    pub sku: u64,
    pub missing_ad_dates: Vec<NaiveDate>,
    pub metrics: BaselineMetrics,
}

#[derive(Debug, Serialize)]
pub struct CampaignBaseline {
    pub campaign_id: u64,
    pub missing_ad_dates: Vec<NaiveDate>,
    pub sku_metrics: Option<BaselineMetrics>,
    pub unallocated_campaign_metrics: Option<BaselineMetrics>,
}

#[derive(Debug, Serialize)]
pub struct DayBaseline {
    pub date: NaiveDate,
    pub snapshot_id: Option<u64>,
    pub observed_at: Option<DateTime<Utc>>,
    pub publication_complete: bool,
    pub advertising_rows_observed: bool,
    pub sku_metrics: Option<BaselineMetrics>,
    pub unallocated_campaign_metrics: Option<BaselineMetrics>,
    pub sales: Option<StoreSalesDay>,
}

#[derive(Debug, Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent coverage and evidence boundaries remain explicit"
)]
pub struct WbBaselineReport {
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
    pub missing_ad_dates: Vec<NaiveDate>,
    pub sales_coverage_complete: bool,
    pub missing_sales_dates: Vec<NaiveDate>,
    pub observed_store_sales: Option<StoreSalesTotals>,
    pub observed_sku_metrics: Option<BaselineMetrics>,
    pub unallocated_campaign_metrics: Option<BaselineMetrics>,
    pub daily: Vec<DayBaseline>,
    pub campaigns: Vec<CampaignBaseline>,
    pub products: Vec<ProductBaseline>,
    pub attribution_maturity_verified: bool,
    pub causal_effect_verified: bool,
    pub current_composition_verified: bool,
    pub auto_apply_allowed: bool,
}
