//! Public reporting read-model contracts.

use super::{
    Deserialize, JsonSchema, KpiSummary, Marketplace, NaiveDate, PriorityProblem, ProblemKind,
    Serialize, Severity, SnapshotQuality, SnapshotSource, SnapshotStatus,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReportingMarketplace {
    Ozon,
    Wildberries,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReportingSource {
    Sales,
    Advertising,
    Finance,
    Stocks,
    Prices,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CollectionState {
    Running,
    Succeeded,
    Partial,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DataQuality {
    Complete,
    Partial,
    Stale,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
pub enum DataState {
    #[serde(rename = "COMPLETE")]
    Complete,
    #[serde(rename = "PARTIAL")]
    Partial,
    #[serde(rename = "N/D")]
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct PublishedCheckpoint {
    pub cutoff_at: String,
    pub source_as_of: String,
    pub status: CollectionState,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct CollectionStatusItem {
    pub snapshot_id: String,
    pub source: ReportingSource,
    pub cutoff_at: String,
    pub source_as_of: String,
    pub status: CollectionState,
    pub pagination_complete: bool,
    pub row_count: u64,
    pub collector_version: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub error_class: Option<String>,
    pub http_status: Option<u16>,
    pub last_published: Option<PublishedCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct CollectionStatusResult {
    pub account_id: String,
    pub marketplace: ReportingMarketplace,
    pub items: Vec<CollectionStatusItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct SourceCompleteness {
    pub source: ReportingSource,
    pub available: bool,
    pub status: Option<CollectionState>,
    pub quality: Option<DataQuality>,
    pub pagination_complete: Option<bool>,
    pub row_count: Option<u64>,
    pub source_as_of: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct DataCompletenessResult {
    pub account_id: String,
    pub marketplace: ReportingMarketplace,
    pub cutoff_at: Option<String>,
    pub state: DataState,
    pub recommendations_allowed: bool,
    pub sources: Vec<SourceCompleteness>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct KpiValues {
    pub ordered_units: u64,
    pub realized_units: Option<u64>,
    pub operational_gmv_minor: u64,
    pub cancelled_units: Option<u64>,
    pub returned_units: Option<u64>,
    pub ad_impressions: u64,
    pub ad_clicks: u64,
    pub ad_spend_minor: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
    pub ctr_bps: Option<u64>,
    pub cpc_minor: Option<u64>,
    pub ad_conversion_bps: Option<u64>,
    pub cpo_minor: Option<u64>,
    pub drr_bps: Option<u64>,
    pub buyout_rate_bps: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct MetricsHistoryPoint {
    pub cutoff_at: String,
    pub state: DataState,
    pub kpis: Option<KpiValues>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct MetricsHistoryResult {
    pub account_id: String,
    pub marketplace: ReportingMarketplace,
    pub date_from: String,
    pub date_to: String,
    pub points: Vec<MetricsHistoryPoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SalesAnalyticsGroup {
    Day,
    Sku,
    DaySku,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SalesAnalyticsSort {
    Dimension,
    OrderedUnits,
    OperationalGmv,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SalesAnalyticsDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SalesAnalyticsQuery {
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
    pub group_by: SalesAnalyticsGroup,
    pub sort_by: SalesAnalyticsSort,
    pub direction: SalesAnalyticsDirection,
    pub limit: u16,
    pub offset: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SalesDateCoverageState {
    Complete,
    Preliminary,
    Partial,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct SalesDateCoverage {
    pub business_date: String,
    pub state: SalesDateCoverageState,
    pub served: bool,
    pub cutoff_at: Option<String>,
    pub source_as_of: Option<String>,
    pub period_end: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct SalesAnalyticsRow {
    pub business_date: Option<String>,
    pub sku: Option<String>,
    pub ordered_units: u64,
    pub operational_gmv_minor: u64,
    pub currency: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct SalesAnalyticsResult {
    pub account_id: String,
    pub marketplace: ReportingMarketplace,
    pub date_from: String,
    pub date_to: String,
    pub state: DataState,
    pub source: String,
    pub group_by: SalesAnalyticsGroup,
    pub sort_by: SalesAnalyticsSort,
    pub direction: SalesAnalyticsDirection,
    pub limit: u16,
    pub offset: u32,
    pub total_rows: u64,
    pub rows: Vec<SalesAnalyticsRow>,
    pub coverage: Vec<SalesDateCoverage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct WeeklyRankingCoverageGap {
    pub account_id: String,
    pub marketplace: ReportingMarketplace,
    pub dates: Vec<SalesDateCoverage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct WeeklyRankingEntry {
    pub rank: u16,
    pub account_id: String,
    pub marketplace: ReportingMarketplace,
    pub ordered_units: u64,
    pub operational_gmv_minor: u64,
    pub currency: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct WeeklyMarketplaceRankingResult {
    pub date_from: String,
    pub date_to: String,
    pub state: DataState,
    pub source: String,
    pub expected_accounts: u16,
    pub complete_accounts: u16,
    pub missing: Vec<WeeklyRankingCoverageGap>,
    pub ranking: Vec<WeeklyRankingEntry>,
    pub leader: Option<WeeklyRankingEntry>,
    pub outsider: Option<WeeklyRankingEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActionSeverity {
    Yellow,
    Red,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ManagerActionKind {
    AdvertisedWithoutStock,
    Stockout,
    LowStockCover,
    SpendWithoutOrders,
    HighDrr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct ManagerAction {
    pub sku: String,
    pub kind: ManagerActionKind,
    pub severity: ActionSeverity,
    pub observed: u64,
    pub threshold: u64,
    pub impact_minor: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct ManagerActionsResult {
    pub account_id: String,
    pub marketplace: ReportingMarketplace,
    pub cutoff_at: Option<String>,
    pub state: DataState,
    pub recommendations_allowed: bool,
    pub actions: Vec<ManagerAction>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReadyReportKind {
    Morning,
    Evening,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReadyReportState {
    Ready,
    Sent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct ReadyReportItem {
    pub batch_id: String,
    pub report_version: u32,
    pub local_date: String,
    pub kind: ReadyReportKind,
    pub state: ReadyReportState,
    pub artifact_ready: bool,
    pub sent: bool,
    pub delayed: bool,
    pub scheduled_for: String,
    pub deadline_at: String,
    pub state_changed_at: String,
    pub sent_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct ReadyReportsResult {
    pub reports: Vec<ReadyReportItem>,
}

impl From<Marketplace> for ReportingMarketplace {
    fn from(value: Marketplace) -> Self {
        match value {
            Marketplace::Ozon => Self::Ozon,
            Marketplace::Wildberries => Self::Wildberries,
        }
    }
}

impl From<SnapshotSource> for ReportingSource {
    fn from(value: SnapshotSource) -> Self {
        match value {
            SnapshotSource::Sales => Self::Sales,
            SnapshotSource::Advertising => Self::Advertising,
            SnapshotSource::Finance => Self::Finance,
            SnapshotSource::Stocks => Self::Stocks,
            SnapshotSource::Prices => Self::Prices,
        }
    }
}

impl From<SnapshotStatus> for CollectionState {
    fn from(value: SnapshotStatus) -> Self {
        match value {
            SnapshotStatus::Succeeded => Self::Succeeded,
            SnapshotStatus::Partial => Self::Partial,
        }
    }
}

impl From<SnapshotQuality> for DataQuality {
    fn from(value: SnapshotQuality) -> Self {
        match value {
            SnapshotQuality::Complete => Self::Complete,
            SnapshotQuality::Partial => Self::Partial,
            SnapshotQuality::Stale => Self::Stale,
            SnapshotQuality::Critical => Self::Critical,
        }
    }
}

impl From<KpiSummary> for KpiValues {
    fn from(value: KpiSummary) -> Self {
        Self {
            ordered_units: value.ordered_units,
            realized_units: value.realized_units,
            operational_gmv_minor: value.operational_gmv_minor,
            cancelled_units: value.cancelled_units,
            returned_units: value.returned_units,
            ad_impressions: value.ad_impressions,
            ad_clicks: value.ad_clicks,
            ad_spend_minor: value.ad_spend_minor,
            attributed_orders: value.attributed_orders,
            attributed_revenue_minor: value.attributed_revenue_minor,
            ctr_bps: value.ctr.map(|amount| amount.0),
            cpc_minor: value.cpc_minor,
            ad_conversion_bps: value.ad_conversion.map(|amount| amount.0),
            cpo_minor: value.cpo_minor,
            drr_bps: value.drr.map(|amount| amount.0),
            buyout_rate_bps: value.buyout_rate.map(|amount| amount.0),
        }
    }
}

impl From<PriorityProblem> for ManagerAction {
    fn from(value: PriorityProblem) -> Self {
        Self {
            sku: value.sku.to_string(),
            kind: match value.kind {
                ProblemKind::AdvertisedWithoutStock => ManagerActionKind::AdvertisedWithoutStock,
                ProblemKind::Stockout => ManagerActionKind::Stockout,
                ProblemKind::LowStockCover => ManagerActionKind::LowStockCover,
                ProblemKind::SpendWithoutOrders => ManagerActionKind::SpendWithoutOrders,
                ProblemKind::HighDrr => ManagerActionKind::HighDrr,
            },
            severity: match value.severity {
                Severity::Yellow => ActionSeverity::Yellow,
                Severity::Red => ActionSeverity::Red,
            },
            observed: value.observed,
            threshold: value.threshold,
            impact_minor: value.impact_minor,
        }
    }
}
