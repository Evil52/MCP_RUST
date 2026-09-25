use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

pub const MAX_INPUT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_PRODUCTS: usize = 1_000;
pub const MAX_WINDOW_DAYS: i64 = 90;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowInput {
    pub version: u32,
    pub marketplace: SupportedMarketplace,
    pub pricing_model: PricingModel,
    pub currency: Currency,
    pub source_refs: Vec<String>,
    pub account_id: String,
    pub as_of: DateTime<Utc>,
    /// When the advertising data was actually re-observed upstream.
    pub observed_at: DateTime<Utc>,
    pub window_start: NaiveDate,
    pub window_end: NaiveDate,
    pub coverage_complete: bool,
    pub policy: OptimizerPolicy,
    pub products: Vec<ProductEvidence>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportedMarketplace {
    Ozon,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingModel {
    Cpc,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum Currency {
    RUB,
}

/// Explicit operator choices, not claims about Ozon's attribution window.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizerPolicy {
    pub total_daily_budget_minor: u64,
    pub attribution_lag_days: u16,
    pub min_mature_days: u16,
    pub min_clicks: u64,
    pub min_orders: u64,
    pub max_data_age_hours: u16,
    pub max_window_end_age_days: u16,
    pub max_stock_age_hours: u16,
    pub safety_discount_bps: u16,
    pub max_budget_increase_bps: u16,
    pub budget_decrease_bps: u16,
    pub zero_order_spend_allowances: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProductEvidence {
    pub sku: u64,
    pub current_daily_budget_minor: u64,
    pub max_daily_budget_minor: u64,
    pub budget_constraint: Option<BudgetConstraintEvidence>,
    /// One row per date, summed across all CPC campaigns for this SKU.
    /// Missing dates are unknown; zero requires confirmed no activity.
    pub daily: Vec<AdvertisingDay>,
    pub stock: Option<StockEvidence>,
    pub economics: Option<OrderEconomics>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdvertisingDay {
    pub date: NaiveDate,
    pub clicks: u64,
    pub spend_minor: u64,
    pub direct_orders: u64,
    pub direct_revenue_minor: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetConstraintEvidence {
    pub limited: bool,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StockEvidence {
    pub sellable_units: u64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EconomicsBasis {
    ExpectedPerDirectAttributedOrder,
}

/// Reviewed expectations per direct attributed order, in RUB kopecks.
///
/// Uses the same denominator as `direct_orders`, including buyout/return risk
/// and a consistent tax basis. These expectations are
/// not inferred from ordered GMV or an isolated 1C cost row.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OrderEconomics {
    pub basis: EconomicsBasis,
    pub source_ref: String,
    pub valid_from: NaiveDate,
    pub valid_to: NaiveDate,
    pub reviewed_at: DateTime<Utc>,
    pub expected_revenue_minor: u64,
    pub expected_cost_of_goods_minor: u64,
    /// Commissions, acquiring, logistics, taxes and allocated operating costs.
    pub expected_other_costs_minor: u64,
    /// Additional cancellation/return costs not already in the fields above.
    pub return_reserve_minor: u64,
    pub target_profit_minor: u64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationAction {
    Hold,
    ReviewReduction,
    ReviewPause,
    TestBudgetIncrease,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationReason {
    IncompleteCoverage,
    InconsistentAdvertisingEvidence,
    MissingBudgetConstraintEvidence,
    StaleBudgetConstraintEvidence,
    BudgetNotLimited,
    MissingDates,
    StaleAdvertising,
    StalePerformanceWindow,
    MissingEconomics,
    EconomicsOutsidePeriod,
    MissingStock,
    StaleStock,
    OutOfStock,
    NoAdvertisingAllowance,
    InsufficientMatureDays,
    InsufficientClicks,
    InsufficientOrders,
    MatureSpendWithoutOrders,
    CpcAboveEconomicCeiling,
    WithinEconomicCeiling,
    NoCurrentBudget,
    ProductBudgetCap,
    PortfolioBudgetCap,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct EvidenceMetrics {
    pub all_period_spend_minor: u64,
    pub mature_days: u16,
    pub mature_clicks: u64,
    pub mature_orders: u64,
    pub mature_spend_minor: u64,
    pub mature_direct_revenue_minor: u64,
    pub excluded_recent_days: u16,
    pub advertising_allowance_per_order_minor: Option<u64>,
    /// Historical rate with a policy haircut; not an auction bid or forecast.
    pub economic_average_cpc_ceiling_minor: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProductRecommendation {
    pub sku: u64,
    pub action: RecommendationAction,
    pub reasons: Vec<RecommendationReason>,
    pub current_daily_budget_minor: u64,
    pub suggested_daily_budget_minor: u64,
    pub metrics: EvidenceMetrics,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ShadowReport {
    pub version: u32,
    pub mode: String,
    pub currency: String,
    pub account_id: String,
    pub as_of: DateTime<Utc>,
    pub input_sha256: String,
    pub total_daily_budget_minor: u64,
    pub allocated_daily_budget_minor: u64,
    pub unallocated_daily_budget_minor: u64,
    pub recommendations: Vec<ProductRecommendation>,
}
