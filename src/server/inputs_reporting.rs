//! Published reporting and internal refresh input contracts.

use super::{
    Deserialize, JsonSchema, SalesAnalyticsDirection, SalesAnalyticsGroup, SalesAnalyticsSort,
};

pub(super) const fn default_reporting_status_limit() -> u16 {
    20
}

pub(super) const fn default_reporting_history_limit() -> u16 {
    14
}

pub(super) const fn default_reporting_reports_limit() -> u16 {
    20
}

pub(super) const fn default_tool_call_log_limit() -> u16 {
    50
}

pub(super) const fn default_sales_analytics_limit() -> u16 {
    100
}

pub(super) const fn default_sales_analytics_group() -> SalesAnalyticsGroup {
    SalesAnalyticsGroup::Day
}

pub(super) const fn default_sales_analytics_sort() -> SalesAnalyticsSort {
    SalesAnalyticsSort::Dimension
}

pub(super) const fn default_sales_analytics_direction() -> SalesAnalyticsDirection {
    SalesAnalyticsDirection::Asc
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportingCollectionStatusInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[serde(default = "default_reporting_status_limit")]
    #[schemars(range(min = 1, max = 50))]
    pub limit: u16,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportingCompletenessInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[serde(default)]
    #[schemars(
        description = "Точный cutoff опубликованного набора в RFC 3339; без значения выбирается последний наблюдаемый cutoff",
        length(min = 1, max = 64)
    )]
    pub cutoff_at: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportingMetricsHistoryInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[serde(default)]
    #[schemars(description = "Начало периода YYYY-MM-DD", length(equal = 10))]
    pub date_from: Option<String>,
    #[serde(default)]
    #[schemars(description = "Конец периода YYYY-MM-DD", length(equal = 10))]
    pub date_to: Option<String>,
    #[serde(default = "default_reporting_history_limit")]
    #[schemars(range(min = 1, max = 100))]
    pub limit: u16,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportingWeeklyMarketplaceRankingInput {
    #[serde(default)]
    #[schemars(
        description = "Начало одной завершённой семидневной недели YYYY-MM-DD; без обеих дат выбирается предыдущая календарная неделя",
        length(equal = 10)
    )]
    pub date_from: Option<String>,
    #[serde(default)]
    #[schemars(
        description = "Конец той же семидневной недели YYYY-MM-DD; без обеих дат выбирается предыдущая календарная неделя",
        length(equal = 10)
    )]
    pub date_to: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportingSourceSnapshotInput {
    pub account: Option<String>,
    pub source: crate::reporting::snapshot::SnapshotSource,
    /// For subsequent pages, repeat the snapshot ID from the first response.
    pub snapshot_id: Option<i64>,
    #[serde(default = "default_source_snapshot_limit")]
    pub limit: u16,
    #[serde(default)]
    pub offset: u32,
}
pub(super) const fn default_source_snapshot_limit() -> u16 {
    100
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportingOzonSalesAnalyticsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический Ozon account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[schemars(description = "Начало периода YYYY-MM-DD", length(equal = 10))]
    pub date_from: String,
    #[schemars(description = "Конец периода YYYY-MM-DD", length(equal = 10))]
    pub date_to: String,
    #[serde(default = "default_sales_analytics_group")]
    pub group_by: SalesAnalyticsGroup,
    #[serde(default = "default_sales_analytics_sort")]
    pub sort_by: SalesAnalyticsSort,
    #[serde(default = "default_sales_analytics_direction")]
    pub direction: SalesAnalyticsDirection,
    #[serde(default = "default_sales_analytics_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u16,
    #[serde(default)]
    #[schemars(range(max = 100_000))]
    pub offset: u32,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportingOzonSalesRefreshInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический Ozon account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportingManagerActionsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[serde(default)]
    #[schemars(
        description = "Точный cutoff опубликованного набора в RFC 3339; без значения выбирается последний наблюдаемый cutoff",
        length(min = 1, max = 64)
    )]
    pub cutoff_at: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportingReadyReportsInput {
    #[serde(default = "default_reporting_reports_limit")]
    #[schemars(range(min = 1, max = 100))]
    pub limit: u16,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolCallLogInput {
    #[serde(default = "default_tool_call_log_limit")]
    #[schemars(range(min = 1, max = 200))]
    pub limit: u16,
}
