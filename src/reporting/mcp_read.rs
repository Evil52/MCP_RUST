#![expect(
    clippy::significant_drop_tightening,
    reason = "query rows borrow data associated with the supervised PostgreSQL session"
)]

//! Least-privilege read projection for deterministic daily-report data.
//!
//! The main MCP process may connect only as `position_reader`. It reads the
//! deliberately curated `daily_reporting.mcp_*` views and never receives a
//! table, outbox, recipient, provider or artifact capability. All calculated
//! values are rebuilt from immutable published facts with the same Rust KPI
//! and rule code used by server-generated reports.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::Arc,
};

use anyhow::{Result as AnyResult, anyhow};
use chrono::{DateTime, Duration, NaiveDate, NaiveTime, TimeZone, Utc};
use schemars::JsonSchema;
use serde::Serialize;
use tokio_postgres::{Client, Config, Row, config::Host, types::FromSql};

use crate::postgres::SupervisedClient;

use super::{
    dataset::ReportDataset,
    kpi::{AdvertisingMetricInput, KpiSummary, SalesMetricInput, calculate_kpis},
    postgres_collector::FinanceCategory,
    postgres_snapshot::{
        PublishedAdvertisingExpenseFact, PublishedAdvertisingFact, PublishedFinanceFact,
        PublishedPriceFact, PublishedReportFacts, PublishedSalesFact, PublishedStockFact,
    },
    preview::rule_inputs,
    rules::{PriorityProblem, ProblemKind, Severity, priority_problems},
    snapshot::{
        AccountScope, FrozenSnapshotManifest, Marketplace, SnapshotDescriptor, SnapshotQuality,
        SnapshotSource, SnapshotStatus,
    },
};

const READER_COMPONENT: &str = "mcp-ozon-reporting-reader";
const MAX_STATUS_ROWS: u16 = 50;
const MAX_HISTORY_POINTS: u16 = 100;
const MAX_READY_REPORTS: u16 = 100;
const MAX_HISTORY_DAYS: i64 = 366;
const MAX_FACT_ROWS: usize = 25_000;
const UTC_03_00: NaiveTime = NaiveTime::from_hms_opt(3, 0, 0).unwrap();
const UTC_09_00: NaiveTime = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
const UTC_12_00: NaiveTime = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
const UTC_18_00: NaiveTime = NaiveTime::from_hms_opt(18, 0, 0).unwrap();

pub type ReportingReadFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ReportingReadError>> + Send + 'a>>;

/// Sanitized runtime failures safe to expose through an MCP tool result.
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum ReportingReadError {
    #[error("reporting history is disabled")]
    Disabled,
    #[error("reporting read request is invalid")]
    InvalidRequest,
    #[error("reporting history is temporarily unavailable")]
    Unavailable,
    #[error("published reporting data failed validation")]
    InvalidPublishedData,
}

/// Injectable repository boundary used by the MCP router's RBAC tests.
pub trait ReportingReadRepository: Send + Sync {
    fn enabled(&self) -> bool;

    fn collection_status<'a>(
        &'a self,
        account: &'a AccountScope,
        limit: u16,
    ) -> ReportingReadFuture<'a, CollectionStatusResult>;

    fn data_completeness<'a>(
        &'a self,
        account: &'a AccountScope,
        cutoff: Option<DateTime<Utc>>,
    ) -> ReportingReadFuture<'a, DataCompletenessResult>;

    fn metrics_history<'a>(
        &'a self,
        account: &'a AccountScope,
        from: Option<NaiveDate>,
        to: Option<NaiveDate>,
        limit: u16,
    ) -> ReportingReadFuture<'a, MetricsHistoryResult>;

    fn manager_actions<'a>(
        &'a self,
        account: &'a AccountScope,
        cutoff: Option<DateTime<Utc>>,
    ) -> ReportingReadFuture<'a, ManagerActionsResult>;

    fn ready_reports(&self, limit: u16) -> ReportingReadFuture<'_, ReadyReportsResult>;
}

/// Cloneable service handle installed in every MCP server instance.
#[derive(Clone)]
pub struct ReportingReader {
    repository: Arc<dyn ReportingReadRepository>,
}

impl fmt::Debug for ReportingReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReportingReader")
            .field("enabled", &self.repository.enabled())
            .finish_non_exhaustive()
    }
}

impl ReportingReader {
    /// Constructs the shipped no-database mode.
    #[must_use]
    pub fn disabled() -> Self {
        Self::from_repository(Arc::new(DisabledReportingRepository))
    }

    /// Connects only when an explicit restricted-reader URL is configured.
    ///
    /// An absent URL is the intentional disabled mode. A present but malformed,
    /// over-privileged, unreachable or outdated database fails startup.
    pub async fn connect_optional(database_url: Option<&str>) -> AnyResult<Self> {
        let Some(database_url) = database_url else {
            return Ok(Self::disabled());
        };
        let mut config = Config::from_str(database_url)
            .map_err(|_| anyhow!("reporting reader database configuration is invalid"))?;
        validate_reader_database(&config)
            .map_err(|_| anyhow!("reporting reader database configuration is invalid"))?;
        crate::postgres::harden(&mut config, READER_COMPONENT);
        let repository = PostgresReportingRepository::connect(&config)
            .await
            .map_err(|_| anyhow!("reporting reader database contract is unavailable"))?;
        Ok(Self::from_repository(Arc::new(repository)))
    }

    /// Injects a fake repository without weakening the production constructor.
    pub fn from_repository(repository: Arc<dyn ReportingReadRepository>) -> Self {
        Self { repository }
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.repository.enabled()
    }

    pub async fn collection_status(
        &self,
        account: &AccountScope,
        limit: u16,
    ) -> Result<CollectionStatusResult, ReportingReadError> {
        validate_limit(limit, MAX_STATUS_ROWS)?;
        self.repository.collection_status(account, limit).await
    }

    pub async fn data_completeness(
        &self,
        account: &AccountScope,
        cutoff: Option<DateTime<Utc>>,
    ) -> Result<DataCompletenessResult, ReportingReadError> {
        self.repository.data_completeness(account, cutoff).await
    }

    pub async fn metrics_history(
        &self,
        account: &AccountScope,
        from: Option<NaiveDate>,
        to: Option<NaiveDate>,
        limit: u16,
    ) -> Result<MetricsHistoryResult, ReportingReadError> {
        validate_limit(limit, MAX_HISTORY_POINTS)?;
        self.repository
            .metrics_history(account, from, to, limit)
            .await
    }

    pub async fn manager_actions(
        &self,
        account: &AccountScope,
        cutoff: Option<DateTime<Utc>>,
    ) -> Result<ManagerActionsResult, ReportingReadError> {
        self.repository.manager_actions(account, cutoff).await
    }

    pub async fn ready_reports(
        &self,
        limit: u16,
    ) -> Result<ReadyReportsResult, ReportingReadError> {
        validate_limit(limit, MAX_READY_REPORTS)?;
        self.repository.ready_reports(limit).await
    }
}

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

#[derive(Debug)]
struct DisabledReportingRepository;

impl ReportingReadRepository for DisabledReportingRepository {
    fn enabled(&self) -> bool {
        false
    }

    fn collection_status<'a>(
        &'a self,
        _account: &'a AccountScope,
        _limit: u16,
    ) -> ReportingReadFuture<'a, CollectionStatusResult> {
        disabled()
    }

    fn data_completeness<'a>(
        &'a self,
        _account: &'a AccountScope,
        _cutoff: Option<DateTime<Utc>>,
    ) -> ReportingReadFuture<'a, DataCompletenessResult> {
        disabled()
    }

    fn metrics_history<'a>(
        &'a self,
        _account: &'a AccountScope,
        _from: Option<NaiveDate>,
        _to: Option<NaiveDate>,
        _limit: u16,
    ) -> ReportingReadFuture<'a, MetricsHistoryResult> {
        disabled()
    }

    fn manager_actions<'a>(
        &'a self,
        _account: &'a AccountScope,
        _cutoff: Option<DateTime<Utc>>,
    ) -> ReportingReadFuture<'a, ManagerActionsResult> {
        disabled()
    }

    fn ready_reports(&self, _limit: u16) -> ReportingReadFuture<'_, ReadyReportsResult> {
        disabled()
    }
}

fn disabled<T>() -> ReportingReadFuture<'static, T> {
    Box::pin(async { Err(ReportingReadError::Disabled) })
}

fn validate_limit(limit: u16, maximum: u16) -> Result<(), ReportingReadError> {
    (limit > 0 && limit <= maximum)
        .then_some(())
        .ok_or(ReportingReadError::InvalidRequest)
}

fn validate_runtime_contract(valid: bool) -> Result<(), ReportingReadError> {
    valid.then_some(()).ok_or(ReportingReadError::Unavailable)
}

struct PostgresReportingRepository {
    client: SupervisedClient,
}

impl PostgresReportingRepository {
    async fn connect(config: &Config) -> Result<Self, ReportingReadError> {
        let client = SupervisedClient::connect(config, READER_COMPONENT)
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let repository = Self { client };
        repository.verify_runtime_contract().await?;
        Ok(repository)
    }

    #[cfg(test)]
    fn from_client(client: tokio_postgres::Client) -> Self {
        Self {
            client: SupervisedClient::preconnected(client, READER_COMPONENT),
        }
    }

    async fn verify_runtime_contract(&self) -> Result<(), ReportingReadError> {
        self.client
            .verify_session_bounds()
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let row = client
            .query_one(
                "SELECT current_user = 'position_reader' \
                    AND current_setting('transaction_read_only')::boolean \
                    AND has_schema_privilege(current_user, 'daily_reporting', 'USAGE') \
                    AND NOT has_schema_privilege(current_user, 'daily_reporting', 'CREATE') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.mcp_collection_status', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.mcp_published_source_snapshots', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.mcp_sales_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.mcp_advertising_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.mcp_advertising_expense_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.mcp_finance_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.mcp_stock_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.mcp_price_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.mcp_ready_reports', 'SELECT') \
                    AND NOT has_table_privilege(current_user, \
                        'daily_reporting.source_snapshots', 'SELECT') \
                    AND NOT has_table_privilege(current_user, \
                        'daily_reporting.delivery_batches', 'SELECT')",
                &[],
            )
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        validate_runtime_contract(
            row.try_get::<_, bool>(0)
                .map_err(|_| ReportingReadError::Unavailable)?,
        )?;
        for query in CONTRACT_PROBES {
            client
                .prepare(query)
                .await
                .map_err(|_| ReportingReadError::Unavailable)?;
        }
        Ok(())
    }

    async fn collection_status_impl(
        &self,
        account: &AccountScope,
        limit: u16,
    ) -> Result<CollectionStatusResult, ReportingReadError> {
        validate_limit(limit, MAX_STATUS_ROWS)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let marketplace = marketplace_str(account.marketplace());
        let rows = client
            .query(
                COLLECTION_STATUS_QUERY,
                &[&account.account_id(), &marketplace, &i64::from(limit)],
            )
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let items = rows
            .into_iter()
            .map(|row| collection_status_item(&row, account))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(CollectionStatusResult {
            account_id: account.account_id().to_owned(),
            marketplace: account.marketplace().into(),
            items,
        })
    }

    async fn data_completeness_impl(
        &self,
        account: &AccountScope,
        requested_cutoff: Option<DateTime<Utc>>,
    ) -> Result<DataCompletenessResult, ReportingReadError> {
        let Some(cutoff) = self.resolve_cutoff(account, requested_cutoff).await? else {
            return Ok(unavailable_completeness(account));
        };
        let descriptors = self.load_descriptors(account, cutoff).await?;
        completeness_from_descriptors(account, cutoff, &descriptors)
    }

    async fn resolve_cutoff(
        &self,
        account: &AccountScope,
        requested: Option<DateTime<Utc>>,
    ) -> Result<Option<DateTime<Utc>>, ReportingReadError> {
        if requested.is_some() {
            return Ok(requested);
        }
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let marketplace = marketplace_str(account.marketplace());
        let row = client
            .query_one(
                "SELECT max(cutoff_at) \
                 FROM daily_reporting.mcp_published_source_snapshots \
                 WHERE account_id = $1 AND marketplace = $2",
                &[&account.account_id(), &marketplace],
            )
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        row.try_get(0)
            .map_err(|_| ReportingReadError::InvalidPublishedData)
    }

    async fn load_descriptors(
        &self,
        account: &AccountScope,
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<SnapshotDescriptor>, ReportingReadError> {
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let marketplace = marketplace_str(account.marketplace());
        let rows = client
            .query(
                PUBLISHED_SNAPSHOTS_QUERY,
                &[&account.account_id(), &marketplace, &cutoff],
            )
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        rows.iter()
            .map(|row| published_descriptor(row, account, cutoff))
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
struct HistoryRange {
    from: NaiveDate,
    to: NaiveDate,
    utc_start: DateTime<Utc>,
    utc_end: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
struct CollectionLifecycle<'a> {
    status: CollectionState,
    pagination_complete: bool,
    cutoff: DateTime<Utc>,
    source_as_of: DateTime<Utc>,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    error_class: Option<&'a str>,
    http_status: Option<u16>,
}

fn validate_collection_lifecycle(
    lifecycle: CollectionLifecycle<'_>,
) -> Result<(), ReportingReadError> {
    if lifecycle.source_as_of > lifecycle.cutoff + Duration::hours(24)
        || lifecycle.started_at > lifecycle.cutoff + Duration::hours(24)
        || lifecycle
            .finished_at
            .is_some_and(|finished| finished < lifecycle.started_at)
    {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    let state_valid = match lifecycle.status {
        CollectionState::Running => {
            lifecycle.finished_at.is_none()
                && lifecycle.error_class.is_none()
                && lifecycle.http_status.is_none()
        }
        CollectionState::Succeeded => {
            lifecycle.finished_at.is_some()
                && lifecycle.error_class.is_none()
                && lifecycle.http_status.is_none()
                && lifecycle.pagination_complete
        }
        CollectionState::Partial => {
            lifecycle.finished_at.is_some()
                && lifecycle.error_class.is_none()
                && lifecycle.http_status.is_none()
        }
        CollectionState::Failed => {
            lifecycle.finished_at.is_some() && lifecycle.error_class.is_some()
        }
    };
    if state_valid && lifecycle.error_class.is_none_or(valid_error_class) {
        Ok(())
    } else {
        Err(ReportingReadError::InvalidPublishedData)
    }
}

#[derive(Debug, Clone, Copy)]
struct PublishedCheckpointParts<'a> {
    current_cutoff: DateTime<Utc>,
    cutoff: Option<DateTime<Utc>>,
    source_as_of: Option<DateTime<Utc>>,
    status: Option<&'a str>,
    row_count: Option<i32>,
}

fn published_checkpoint(
    parts: PublishedCheckpointParts<'_>,
) -> Result<Option<PublishedCheckpoint>, ReportingReadError> {
    match (
        parts.cutoff,
        parts.source_as_of,
        parts.status,
        parts.row_count,
    ) {
        (None, None, None, None) => Ok(None),
        (Some(cutoff), Some(source_as_of), Some(status), Some(row_count)) => {
            if cutoff > parts.current_cutoff || source_as_of > cutoff + Duration::hours(24) {
                return Err(ReportingReadError::InvalidPublishedData);
            }
            Ok(Some(PublishedCheckpoint {
                cutoff_at: timestamp_string(cutoff),
                source_as_of: timestamp_string(source_as_of),
                status: parse_published_collection_status(status)?,
                row_count: nonnegative_i32(row_count)?,
            }))
        }
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

fn history_range(
    requested_from: Option<NaiveDate>,
    requested_to: Option<NaiveDate>,
) -> Result<HistoryRange, ReportingReadError> {
    let offset = super::yekaterinburg_offset();
    let today = Utc::now().with_timezone(&offset).date_naive();
    let to = requested_to.unwrap_or(today);
    let from = requested_from.unwrap_or_else(|| to - Duration::days(29));
    let inclusive_days = to.signed_duration_since(from).num_days() + 1;
    if !(1..=MAX_HISTORY_DAYS).contains(&inclusive_days) {
        return Err(ReportingReadError::InvalidRequest);
    }
    let next_day = to.succ_opt().ok_or(ReportingReadError::InvalidRequest)?;
    let local_start = from
        .and_hms_opt(0, 0, 0)
        .ok_or(ReportingReadError::InvalidRequest)?;
    let local_end = next_day
        .and_hms_opt(0, 0, 0)
        .ok_or(ReportingReadError::InvalidRequest)?;
    let utc_start = offset
        .from_local_datetime(&local_start)
        .single()
        .ok_or(ReportingReadError::InvalidRequest)?
        .with_timezone(&Utc);
    let utc_end = offset
        .from_local_datetime(&local_end)
        .single()
        .ok_or(ReportingReadError::InvalidRequest)?
        .with_timezone(&Utc);
    Ok(HistoryRange {
        from,
        to,
        utc_start,
        utc_end,
    })
}

fn collection_status_item(
    row: &Row,
    account: &AccountScope,
) -> Result<CollectionStatusItem, ReportingReadError> {
    let snapshot_id = positive_i64(column(row, 0)?)?;
    validate_scope_columns(row, account, 1, 2)?;
    let source = parse_source(&column::<String>(row, 3)?)?;
    let cutoff: DateTime<Utc> = column(row, 4)?;
    let source_as_of: DateTime<Utc> = column(row, 5)?;
    let status = parse_collection_status(&column::<String>(row, 6)?)?;
    let pagination_complete: bool = column(row, 7)?;
    let row_count = nonnegative_i32(column(row, 8)?)?;
    let collector_version: String = column(row, 9)?;
    let started_at: DateTime<Utc> = column(row, 10)?;
    let finished_at: Option<DateTime<Utc>> = column(row, 11)?;
    let error_class: Option<String> = column(row, 12)?;
    let http_status = column::<Option<i16>>(row, 13)?
        .map(valid_http_status)
        .transpose()?;
    validate_collector_version(&collector_version)?;
    validate_collection_lifecycle(CollectionLifecycle {
        status,
        pagination_complete,
        cutoff,
        source_as_of,
        started_at,
        finished_at,
        error_class: error_class.as_deref(),
        http_status,
    })?;

    let last_cutoff: Option<DateTime<Utc>> = column(row, 14)?;
    let last_as_of: Option<DateTime<Utc>> = column(row, 15)?;
    let last_status: Option<String> = column(row, 16)?;
    let last_count: Option<i32> = column(row, 17)?;
    let last_published = published_checkpoint(PublishedCheckpointParts {
        current_cutoff: cutoff,
        cutoff: last_cutoff,
        source_as_of: last_as_of,
        status: last_status.as_deref(),
        row_count: last_count,
    })?;
    Ok(CollectionStatusItem {
        snapshot_id: format!("cs_{snapshot_id:016x}"),
        source: source.into(),
        cutoff_at: timestamp_string(cutoff),
        source_as_of: timestamp_string(source_as_of),
        status,
        pagination_complete,
        row_count,
        collector_version,
        started_at: timestamp_string(started_at),
        finished_at: finished_at.map(timestamp_string),
        error_class,
        http_status,
        last_published,
    })
}

fn published_descriptor(
    row: &Row,
    account: &AccountScope,
    expected_cutoff: DateTime<Utc>,
) -> Result<SnapshotDescriptor, ReportingReadError> {
    let snapshot_id: i64 = column(row, 0)?;
    validate_scope_columns(row, account, 1, 2)?;
    let source = parse_source(&column::<String>(row, 3)?)?;
    let cutoff: DateTime<Utc> = column(row, 4)?;
    let source_as_of: DateTime<Utc> = column(row, 5)?;
    let period_start: DateTime<Utc> = column(row, 6)?;
    let period_end: DateTime<Utc> = column(row, 7)?;
    let status = parse_snapshot_status(&column::<String>(row, 8)?)?;
    let pagination_complete: bool = column(row, 9)?;
    let row_count = u32::try_from(nonnegative_i32(column(row, 10)?)?)
        .map_err(|_| ReportingReadError::InvalidPublishedData)?;
    validate_descriptor_cutoff(cutoff, expected_cutoff)?;
    SnapshotDescriptor::new(
        snapshot_id,
        account.account_id().to_owned(),
        account.marketplace(),
        source,
        cutoff,
        source_as_of,
        period_start,
        period_end,
        row_count,
        pagination_complete,
        status,
    )
    .map_err(|_| ReportingReadError::InvalidPublishedData)
}

fn validate_descriptor_cutoff(
    actual: DateTime<Utc>,
    expected: DateTime<Utc>,
) -> Result<(), ReportingReadError> {
    (actual == expected)
        .then_some(())
        .ok_or(ReportingReadError::InvalidPublishedData)
}

fn completeness_from_descriptors(
    account: &AccountScope,
    cutoff: DateTime<Utc>,
    descriptors: &[SnapshotDescriptor],
) -> Result<DataCompletenessResult, ReportingReadError> {
    if descriptors.is_empty() {
        let mut result = unavailable_completeness(account);
        result.cutoff_at = Some(timestamp_string(cutoff));
        return Ok(result);
    }
    let required = SnapshotSource::required_for(account.marketplace());
    let mut by_source = BTreeMap::new();
    for descriptor in descriptors {
        if descriptor.account_id() != account.account_id()
            || descriptor.marketplace() != account.marketplace()
            || descriptor.cutoff_at() != cutoff
            || !required.contains(&descriptor.source())
            || by_source.insert(descriptor.source(), descriptor).is_some()
        {
            return Err(ReportingReadError::InvalidPublishedData);
        }
    }
    let manifest = if by_source.len() == required.len() {
        Some(
            FrozenSnapshotManifest::new(cutoff, vec![account.clone()], descriptors.to_vec())
                .map_err(|_| ReportingReadError::InvalidPublishedData)?,
        )
    } else {
        None
    };
    let state = manifest.as_ref().map_or(DataState::Partial, |value| {
        state_from_quality(value.quality())
    });
    let recommendations_allowed = manifest
        .as_ref()
        .is_some_and(FrozenSnapshotManifest::recommendations_allowed);
    let sources = required
        .iter()
        .map(|source| {
            let descriptor = by_source.get(source).copied();
            SourceCompleteness {
                source: (*source).into(),
                available: descriptor.is_some(),
                status: descriptor.map(|value| value.status().into()),
                quality: descriptor.map(|value| value.quality().into()),
                pagination_complete: descriptor.map(SnapshotDescriptor::pagination_complete),
                row_count: descriptor.map(|value| u64::from(value.row_count())),
                source_as_of: descriptor.map(|value| timestamp_string(value.source_as_of())),
            }
        })
        .collect();
    Ok(DataCompletenessResult {
        account_id: account.account_id().to_owned(),
        marketplace: account.marketplace().into(),
        cutoff_at: Some(timestamp_string(cutoff)),
        state,
        recommendations_allowed,
        sources,
    })
}

fn unavailable_completeness(account: &AccountScope) -> DataCompletenessResult {
    DataCompletenessResult {
        account_id: account.account_id().to_owned(),
        marketplace: account.marketplace().into(),
        cutoff_at: None,
        state: DataState::Unavailable,
        recommendations_allowed: false,
        sources: SnapshotSource::required_for(account.marketplace())
            .iter()
            .map(|source| SourceCompleteness {
                source: (*source).into(),
                available: false,
                status: None,
                quality: None,
                pagination_complete: None,
                row_count: None,
                source_as_of: None,
            })
            .collect(),
    }
}

fn unavailable_actions(account: &AccountScope) -> ManagerActionsResult {
    ManagerActionsResult {
        account_id: account.account_id().to_owned(),
        marketplace: account.marketplace().into(),
        cutoff_at: None,
        state: DataState::Unavailable,
        recommendations_allowed: false,
        actions: Vec::new(),
    }
}

fn state_for_descriptors(
    account: &AccountScope,
    descriptors: &[SnapshotDescriptor],
) -> Result<DataState, ReportingReadError> {
    if descriptors.is_empty() {
        return Ok(DataState::Unavailable);
    }
    let required = SnapshotSource::required_for(account.marketplace());
    let seen = descriptors
        .iter()
        .map(SnapshotDescriptor::source)
        .collect::<BTreeSet<_>>();
    if descriptors.iter().any(|descriptor| {
        descriptor.account_id() != account.account_id()
            || descriptor.marketplace() != account.marketplace()
            || !required.contains(&descriptor.source())
    }) || seen.len() != descriptors.len()
    {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    if seen.len() != required.len() {
        return Ok(DataState::Partial);
    }
    let quality = descriptors
        .iter()
        .map(SnapshotDescriptor::quality)
        .max()
        .unwrap_or(SnapshotQuality::Critical);
    Ok(state_from_quality(quality))
}

fn state_from_quality(quality: SnapshotQuality) -> DataState {
    if quality == SnapshotQuality::Complete {
        DataState::Complete
    } else {
        DataState::Partial
    }
}

fn validate_expected_fact_rows(rows: usize) -> Result<(), ReportingReadError> {
    (rows <= MAX_FACT_ROWS)
        .then_some(())
        .ok_or(ReportingReadError::InvalidRequest)
}

const fn metric_fact_count(
    source: SnapshotSource,
    sales_count: usize,
    advertising_count: usize,
) -> Result<usize, ReportingReadError> {
    match source {
        SnapshotSource::Sales => Ok(sales_count),
        SnapshotSource::Advertising => Ok(advertising_count),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

fn validate_snapshot_fact_count(actual: usize, expected: u32) -> Result<(), ReportingReadError> {
    (actual == expected as usize)
        .then_some(())
        .ok_or(ReportingReadError::InvalidPublishedData)
}

type MetricSourceIds = BTreeMap<DateTime<Utc>, (Option<i64>, Option<i64>)>;

fn insert_metric_source_id(
    source_ids: &mut MetricSourceIds,
    descriptor: &SnapshotDescriptor,
) -> Result<(), ReportingReadError> {
    let entry = source_ids.entry(descriptor.cutoff_at()).or_default();
    let slot = match descriptor.source() {
        SnapshotSource::Sales => &mut entry.0,
        SnapshotSource::Advertising => &mut entry.1,
        _ => return Err(ReportingReadError::InvalidPublishedData),
    };
    if slot.replace(descriptor.snapshot_id()).is_some() {
        Err(ReportingReadError::InvalidPublishedData)
    } else {
        Ok(())
    }
}

fn complete_metric_pair(pair: (Option<i64>, Option<i64>)) -> Option<(i64, i64)> {
    pair.0.zip(pair.1)
}

async fn load_history_kpis(
    client: &Client,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<BTreeMap<DateTime<Utc>, KpiSummary>, ReportingReadError> {
    let metric_descriptors = expected
        .values()
        .filter(|descriptor| {
            matches!(
                descriptor.source(),
                SnapshotSource::Sales | SnapshotSource::Advertising
            )
        })
        .collect::<Vec<_>>();
    let expected_rows = metric_descriptors
        .iter()
        .try_fold(0usize, |total, descriptor| {
            total.checked_add(descriptor.row_count() as usize)
        })
        .ok_or(ReportingReadError::InvalidPublishedData)?;
    validate_expected_fact_rows(expected_rows)?;
    let snapshot_ids = metric_descriptors
        .iter()
        .map(|descriptor| descriptor.snapshot_id())
        .collect::<Vec<_>>();
    let sales_rows = client
        .query(SALES_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    let advertising_rows = client
        .query(ADVERTISING_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(sales_rows.len())?;
    validate_fact_query_size(advertising_rows.len())?;

    let mut sales = BTreeMap::<i64, Vec<SalesMetricInput>>::new();
    for row in sales_rows {
        let (snapshot_id, fact) = sales_fact(&row, expected)?;
        sales
            .entry(snapshot_id)
            .or_default()
            .push(SalesMetricInput {
                ordered_units: fact.ordered_units,
                operational_gmv_minor: fact.operational_gmv_minor,
                cancelled_units: fact.cancelled_units,
                returned_units: fact.returned_units,
            });
    }
    let mut advertising = BTreeMap::<i64, Vec<AdvertisingMetricInput>>::new();
    for row in advertising_rows {
        let (snapshot_id, fact) = advertising_fact(&row, expected)?;
        advertising
            .entry(snapshot_id)
            .or_default()
            .push(AdvertisingMetricInput {
                impressions: fact.impressions,
                clicks: fact.clicks,
                spend_minor: fact.spend_minor,
                attributed_orders: fact.attributed_orders,
                attributed_revenue_minor: fact.attributed_revenue_minor,
            });
    }
    for descriptor in &metric_descriptors {
        let sales_count = sales.get(&descriptor.snapshot_id()).map_or(0, Vec::len);
        let advertising_count = advertising
            .get(&descriptor.snapshot_id())
            .map_or(0, Vec::len);
        let actual = metric_fact_count(descriptor.source(), sales_count, advertising_count)?;
        validate_snapshot_fact_count(actual, descriptor.row_count())?;
    }

    let mut source_ids = MetricSourceIds::new();
    for descriptor in metric_descriptors {
        insert_metric_source_id(&mut source_ids, descriptor)?;
    }
    source_ids
        .into_iter()
        .filter_map(|(cutoff, pair)| {
            let (sales_id, advertising_id) = complete_metric_pair(pair)?;
            let sales = sales.remove(&sales_id).unwrap_or_default();
            let advertising = advertising.remove(&advertising_id).unwrap_or_default();
            Some(
                calculate_kpis(&sales, &advertising)
                    .map(|summary| (cutoff, summary))
                    .map_err(|_| ReportingReadError::InvalidPublishedData),
            )
        })
        .collect()
}

async fn query_sales_facts(
    client: &Client,
    snapshot_ids: &[i64],
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<Vec<PublishedSalesFact>, ReportingReadError> {
    let rows = client
        .query(SALES_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(rows.len())?;
    rows.iter()
        .map(|row| sales_fact(row, expected).map(|(_, fact)| fact))
        .collect()
}

async fn query_advertising_facts(
    client: &Client,
    snapshot_ids: &[i64],
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<Vec<PublishedAdvertisingFact>, ReportingReadError> {
    let rows = client
        .query(ADVERTISING_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(rows.len())?;
    rows.iter()
        .map(|row| advertising_fact(row, expected).map(|(_, fact)| fact))
        .collect()
}

async fn query_advertising_expense_facts(
    client: &Client,
    snapshot_ids: &[i64],
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<Vec<PublishedAdvertisingExpenseFact>, ReportingReadError> {
    let rows = client
        .query(ADVERTISING_EXPENSE_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(rows.len())?;
    rows.iter()
        .map(|row| advertising_expense_fact(row, expected).map(|(_, fact)| fact))
        .collect()
}

async fn query_finance_facts(
    client: &Client,
    snapshot_ids: &[i64],
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<Vec<PublishedFinanceFact>, ReportingReadError> {
    let rows = client
        .query(FINANCE_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(rows.len())?;
    rows.iter()
        .map(|row| finance_fact(row, expected).map(|(_, fact)| fact))
        .collect()
}

async fn query_stock_facts(
    client: &Client,
    snapshot_ids: &[i64],
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<Vec<PublishedStockFact>, ReportingReadError> {
    let rows = client
        .query(STOCK_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(rows.len())?;
    rows.iter()
        .map(|row| stock_fact(row, expected).map(|(_, fact)| fact))
        .collect()
}

async fn query_price_facts(
    client: &Client,
    snapshot_ids: &[i64],
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<Vec<PublishedPriceFact>, ReportingReadError> {
    let rows = client
        .query(PRICE_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(rows.len())?;
    rows.iter()
        .map(|row| price_fact(row, expected).map(|(_, fact)| fact))
        .collect()
}

const SALES_FACTS_QUERY: &str = "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, sku, ordered_units, \
            operational_gmv_minor, cancelled_units, returned_units, currency \
     FROM daily_reporting.mcp_sales_facts \
     WHERE snapshot_id = ANY($1::bigint[]) \
     ORDER BY snapshot_id, business_date, sku \
     LIMIT 25001";

const ADVERTISING_FACTS_QUERY: &str = "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, campaign_id, sku, \
            impressions, clicks, spend_minor, attributed_orders, attributed_revenue_minor, \
            currency, basket_additions, model_attributed_orders, \
            model_attributed_revenue_minor, product_price_minor, average_cpc_minor, \
            cpm_minor, cpl_minor \
     FROM daily_reporting.mcp_advertising_facts \
     WHERE snapshot_id = ANY($1::bigint[]) \
     ORDER BY snapshot_id, business_date, campaign_id, sku \
     LIMIT 25001";

const ADVERTISING_EXPENSE_FACTS_QUERY: &str = "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, campaign_id, \
            money_spent_minor, bonus_spent_minor, prepayment_spent_minor, currency \
     FROM daily_reporting.mcp_advertising_expense_facts \
     WHERE snapshot_id = ANY($1::bigint[]) \
     ORDER BY snapshot_id, business_date, campaign_id \
     LIMIT 25001";

const FINANCE_FACTS_QUERY: &str = "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, sku, sku_key, category, \
            amount_minor, line_count, unknown_type_count \
     FROM daily_reporting.mcp_finance_facts \
     WHERE snapshot_id = ANY($1::bigint[]) \
     ORDER BY snapshot_id, business_date, sku NULLS FIRST, category \
     LIMIT 25001";

const STOCK_FACTS_QUERY: &str = "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, sku, warehouse_id, sellable_units \
     FROM daily_reporting.mcp_stock_facts \
     WHERE snapshot_id = ANY($1::bigint[]) \
     ORDER BY snapshot_id, sku, warehouse_id \
     LIMIT 25001";

const PRICE_FACTS_QUERY: &str = "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, sku, price_minor, old_price_minor, currency \
     FROM daily_reporting.mcp_price_facts \
     WHERE snapshot_id = ANY($1::bigint[]) \
     ORDER BY snapshot_id, sku \
     LIMIT 25001";

fn validate_fact_query_size(rows: usize) -> Result<(), ReportingReadError> {
    (rows <= MAX_FACT_ROWS)
        .then_some(())
        .ok_or(ReportingReadError::InvalidPublishedData)
}

fn validate_advertising_counts(impressions: u64, clicks: u64) -> Result<(), ReportingReadError> {
    (clicks <= impressions)
        .then_some(())
        .ok_or(ReportingReadError::InvalidPublishedData)
}

fn validate_finance_identity(
    sku: Option<u64>,
    sku_key: i64,
    line_count: u64,
    unknown_type_count: u64,
) -> Result<(), ReportingReadError> {
    let expected_sku_key = match sku {
        None => 0,
        Some(0) => return Err(ReportingReadError::InvalidPublishedData),
        Some(sku) => i64::try_from(sku).map_err(|_| ReportingReadError::InvalidPublishedData)?,
    };
    if sku_key == expected_sku_key && unknown_type_count <= line_count {
        Ok(())
    } else {
        Err(ReportingReadError::InvalidPublishedData)
    }
}

fn validate_warehouse_id(value: &str) -> Result<(), ReportingReadError> {
    valid_warehouse_id(value)
        .then_some(())
        .ok_or(ReportingReadError::InvalidPublishedData)
}

fn validate_price_relation(
    price_minor: u64,
    old_price_minor: Option<u64>,
) -> Result<(), ReportingReadError> {
    old_price_minor
        .is_none_or(|old| old >= price_minor)
        .then_some(())
        .ok_or(ReportingReadError::InvalidPublishedData)
}

fn sales_fact(
    row: &Row,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<(i64, PublishedSalesFact), ReportingReadError> {
    let (snapshot_id, descriptor) = fact_descriptor(row, expected, SnapshotSource::Sales)?;
    validate_currency(&column::<String>(row, 14)?)?;
    Ok((
        snapshot_id,
        PublishedSalesFact {
            account_id: descriptor.account_id().to_owned(),
            business_date: column(row, 8)?,
            sku: positive_i64(column(row, 9)?)?,
            ordered_units: nonnegative_i32(column(row, 10)?)?,
            operational_gmv_minor: nonnegative_i64(column(row, 11)?)?,
            cancelled_units: nonnegative_optional_i32(column(row, 12)?)?,
            returned_units: nonnegative_optional_i32(column(row, 13)?)?,
        },
    ))
}

fn advertising_fact(
    row: &Row,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<(i64, PublishedAdvertisingFact), ReportingReadError> {
    let (snapshot_id, descriptor) = fact_descriptor(row, expected, SnapshotSource::Advertising)?;
    validate_currency(&column::<String>(row, 16)?)?;
    let impressions = nonnegative_i64(column(row, 11)?)?;
    let clicks = nonnegative_i64(column(row, 12)?)?;
    validate_advertising_counts(impressions, clicks)?;
    Ok((
        snapshot_id,
        PublishedAdvertisingFact {
            account_id: descriptor.account_id().to_owned(),
            business_date: column(row, 8)?,
            campaign_id: positive_i64(column(row, 9)?)?,
            sku: nonnegative_i64(column(row, 10)?)?,
            impressions,
            clicks,
            spend_minor: nonnegative_i64(column(row, 13)?)?,
            attributed_orders: nonnegative_i32(column(row, 14)?)?,
            attributed_revenue_minor: nonnegative_i64(column(row, 15)?)?,
            basket_additions: nonnegative_i32(column(row, 17)?)?,
            model_attributed_orders: nonnegative_i32(column(row, 18)?)?,
            model_attributed_revenue_minor: nonnegative_i64(column(row, 19)?)?,
            product_price_minor: nonnegative_i64(column(row, 20)?)?,
            average_cpc_minor: nonnegative_optional_i64(column(row, 21)?)?,
            cpm_minor: nonnegative_optional_i64(column(row, 22)?)?,
            cpl_minor: nonnegative_optional_i64(column(row, 23)?)?,
        },
    ))
}

fn advertising_expense_fact(
    row: &Row,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<(i64, PublishedAdvertisingExpenseFact), ReportingReadError> {
    let (snapshot_id, descriptor) = fact_descriptor(row, expected, SnapshotSource::Advertising)?;
    validate_currency(&column::<String>(row, 13)?)?;
    Ok((
        snapshot_id,
        PublishedAdvertisingExpenseFact {
            account_id: descriptor.account_id().to_owned(),
            business_date: column(row, 8)?,
            campaign_id: positive_i64(column(row, 9)?)?,
            money_spent_minor: nonnegative_i64(column(row, 10)?)?,
            bonus_spent_minor: nonnegative_i64(column(row, 11)?)?,
            prepayment_spent_minor: nonnegative_i64(column(row, 12)?)?,
        },
    ))
}

fn finance_fact(
    row: &Row,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<(i64, PublishedFinanceFact), ReportingReadError> {
    let (snapshot_id, descriptor) = fact_descriptor(row, expected, SnapshotSource::Finance)?;
    let sku = nonnegative_optional_i64(column(row, 9)?)?;
    let sku_key: i64 = column(row, 10)?;
    let line_count = positive_i32(column(row, 13)?)?;
    let unknown_type_count = nonnegative_i32(column(row, 14)?)?;
    validate_finance_identity(sku, sku_key, line_count, unknown_type_count)?;
    Ok((
        snapshot_id,
        PublishedFinanceFact {
            account_id: descriptor.account_id().to_owned(),
            business_date: column(row, 8)?,
            sku,
            category: parse_finance_category(&column::<String>(row, 11)?)?,
            amount_minor: column(row, 12)?,
            line_count,
            unknown_type_count,
        },
    ))
}

fn stock_fact(
    row: &Row,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<(i64, PublishedStockFact), ReportingReadError> {
    let (snapshot_id, descriptor) = fact_descriptor(row, expected, SnapshotSource::Stocks)?;
    let warehouse_id: String = column(row, 9)?;
    validate_warehouse_id(&warehouse_id)?;
    Ok((
        snapshot_id,
        PublishedStockFact {
            account_id: descriptor.account_id().to_owned(),
            sku: positive_i64(column(row, 8)?)?,
            warehouse_id,
            sellable_units: nonnegative_i32(column(row, 10)?)?,
            observed_at: descriptor.source_as_of(),
        },
    ))
}

fn price_fact(
    row: &Row,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<(i64, PublishedPriceFact), ReportingReadError> {
    let (snapshot_id, descriptor) = fact_descriptor(row, expected, SnapshotSource::Prices)?;
    validate_currency(&column::<String>(row, 11)?)?;
    let price_minor = nonnegative_i64(column(row, 9)?)?;
    let old_price_minor = nonnegative_optional_i64(column(row, 10)?)?;
    validate_price_relation(price_minor, old_price_minor)?;
    Ok((
        snapshot_id,
        PublishedPriceFact {
            account_id: descriptor.account_id().to_owned(),
            sku: positive_i64(column(row, 8)?)?,
            price_minor,
            old_price_minor,
            observed_at: descriptor.source_as_of(),
        },
    ))
}

#[derive(Debug, Clone, Copy)]
struct FactProvenance<'a> {
    snapshot_id: i64,
    source: SnapshotSource,
    account_id: &'a str,
    marketplace: Marketplace,
    cutoff_at: DateTime<Utc>,
    source_as_of: DateTime<Utc>,
    status: SnapshotStatus,
    pagination_complete: bool,
}

fn validate_fact_provenance(
    actual: FactProvenance<'_>,
    descriptor: &SnapshotDescriptor,
    expected_source: SnapshotSource,
) -> Result<(), ReportingReadError> {
    if actual.snapshot_id > 0
        && actual.source == expected_source
        && descriptor.source() == expected_source
        && descriptor.account_id() == actual.account_id
        && descriptor.marketplace() == actual.marketplace
        && descriptor.cutoff_at() == actual.cutoff_at
        && descriptor.source_as_of() == actual.source_as_of
        && descriptor.status() == actual.status
        && descriptor.pagination_complete() == actual.pagination_complete
    {
        Ok(())
    } else {
        Err(ReportingReadError::InvalidPublishedData)
    }
}

fn fact_descriptor<'a>(
    row: &Row,
    expected: &'a BTreeMap<i64, SnapshotDescriptor>,
    expected_source: SnapshotSource,
) -> Result<(i64, &'a SnapshotDescriptor), ReportingReadError> {
    let account_id: String = column(row, 0)?;
    let marketplace = parse_marketplace(&column::<String>(row, 1)?)?;
    let cutoff_at: DateTime<Utc> = column(row, 2)?;
    let source_as_of: DateTime<Utc> = column(row, 3)?;
    let status = parse_snapshot_status(&column::<String>(row, 4)?)?;
    let pagination_complete: bool = column(row, 5)?;
    let snapshot_id: i64 = column(row, 6)?;
    let source = parse_source(&column::<String>(row, 7)?)?;
    let descriptor = expected
        .get(&snapshot_id)
        .ok_or(ReportingReadError::InvalidPublishedData)?;
    let provenance = FactProvenance {
        snapshot_id,
        source,
        account_id: &account_id,
        marketplace,
        cutoff_at,
        source_as_of,
        status,
        pagination_complete,
    };
    validate_fact_provenance(provenance, descriptor, expected_source)?;
    Ok((snapshot_id, descriptor))
}

fn ready_report_item(row: &Row) -> Result<ReadyReportItem, ReportingReadError> {
    let batch_id = positive_i64(column(row, 0)?)?;
    let recipient_id: String = column(row, 1)?;
    validate_recipient_id(&recipient_id)?;
    let report_version = positive_i32(column(row, 2)?)?;
    let report_version =
        u32::try_from(report_version).map_err(|_| ReportingReadError::InvalidPublishedData)?;
    let local_date: NaiveDate = column(row, 3)?;
    let kind = parse_report_kind(&column::<String>(row, 4)?)?;
    let scheduled_for: DateTime<Utc> = column(row, 5)?;
    let deadline_at: DateTime<Utc> = column(row, 6)?;
    let state = parse_report_state(&column::<String>(row, 7)?)?;
    let delayed: bool = column(row, 8)?;
    let created_at: DateTime<Utc> = column(row, 9)?;
    let updated_at: DateTime<Utc> = column(row, 10)?;
    let sent_at: Option<DateTime<Utc>> = column(row, 11)?;
    validate_ready_report_contract(ReadyReportContract {
        local_date,
        kind,
        scheduled_for,
        deadline_at,
        state,
        created_at,
        updated_at,
        sent_at,
    })?;
    let state_changed_at = sent_at.unwrap_or(updated_at);
    Ok(ReadyReportItem {
        batch_id: format!("rb_{batch_id:016x}"),
        report_version,
        local_date: local_date.to_string(),
        kind,
        state,
        artifact_ready: true,
        sent: state == ReadyReportState::Sent,
        delayed,
        scheduled_for: timestamp_string(scheduled_for),
        deadline_at: timestamp_string(deadline_at),
        state_changed_at: timestamp_string(state_changed_at),
        sent_at: sent_at.map(timestamp_string),
    })
}

fn validate_recipient_id(value: &str) -> Result<(), ReportingReadError> {
    valid_identifier(value)
        .then_some(())
        .ok_or(ReportingReadError::InvalidPublishedData)
}

#[derive(Debug, Clone, Copy)]
struct ReadyReportContract {
    local_date: NaiveDate,
    kind: ReadyReportKind,
    scheduled_for: DateTime<Utc>,
    deadline_at: DateTime<Utc>,
    state: ReadyReportState,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    sent_at: Option<DateTime<Utc>>,
}

fn validate_ready_report_contract(contract: ReadyReportContract) -> Result<(), ReportingReadError> {
    let date = contract.local_date;
    let kind = contract.kind;
    let start = contract.scheduled_for;
    let end = contract.deadline_at;
    let schedule_matches = report_schedule_matches(date, kind, start, end);

    if contract.updated_at >= contract.created_at
        && contract.deadline_at >= contract.scheduled_for
        && schedule_matches
        && (contract.state != ReadyReportState::Ready || contract.sent_at.is_none())
        && (contract.state != ReadyReportState::Sent || contract.sent_at.is_some())
    {
        Ok(())
    } else {
        Err(ReportingReadError::InvalidPublishedData)
    }
}

fn report_schedule_matches(
    local_date: NaiveDate,
    kind: ReadyReportKind,
    scheduled_for: DateTime<Utc>,
    deadline_at: DateTime<Utc>,
) -> bool {
    // Yekaterinburg is a fixed UTC+5 zone with no daylight-saving transitions.
    // These UTC times are the exact 08:00-14:00 and 17:00-23:00 local windows.
    let matches_hours = |scheduled_time, deadline_time| {
        let scheduled = local_date.and_time(scheduled_time).and_utc();
        let deadline = local_date.and_time(deadline_time).and_utc();
        scheduled == scheduled_for && deadline == deadline_at
    };

    match kind {
        ReadyReportKind::Morning => {
            matches_hours(UTC_03_00, UTC_09_00) || matches_hours(UTC_12_00, UTC_18_00)
        }
        ReadyReportKind::Evening => matches_hours(UTC_12_00, UTC_18_00),
    }
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

fn column<'row, T>(row: &'row Row, index: usize) -> Result<T, ReportingReadError>
where
    T: FromSql<'row>,
{
    row.try_get(index)
        .map_err(|_| ReportingReadError::InvalidPublishedData)
}

fn validate_scope_columns(
    row: &Row,
    account: &AccountScope,
    account_index: usize,
    marketplace_index: usize,
) -> Result<(), ReportingReadError> {
    let account_id: String = column(row, account_index)?;
    let marketplace = parse_marketplace(&column::<String>(row, marketplace_index)?)?;
    validate_scope(&account_id, marketplace, account)
}

fn validate_scope(
    account_id: &str,
    marketplace: Marketplace,
    account: &AccountScope,
) -> Result<(), ReportingReadError> {
    if account_id == account.account_id() && marketplace == account.marketplace() {
        Ok(())
    } else {
        Err(ReportingReadError::InvalidPublishedData)
    }
}

fn parse_marketplace(value: &str) -> Result<Marketplace, ReportingReadError> {
    match value {
        "ozon" => Ok(Marketplace::Ozon),
        "wildberries" => Ok(Marketplace::Wildberries),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

const fn marketplace_str(value: Marketplace) -> &'static str {
    match value {
        Marketplace::Ozon => "ozon",
        Marketplace::Wildberries => "wildberries",
    }
}

fn parse_source(value: &str) -> Result<SnapshotSource, ReportingReadError> {
    match value {
        "sales" => Ok(SnapshotSource::Sales),
        "advertising" => Ok(SnapshotSource::Advertising),
        "finance" => Ok(SnapshotSource::Finance),
        "stocks" => Ok(SnapshotSource::Stocks),
        "prices" => Ok(SnapshotSource::Prices),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

fn parse_collection_status(value: &str) -> Result<CollectionState, ReportingReadError> {
    match value {
        "running" => Ok(CollectionState::Running),
        "succeeded" => Ok(CollectionState::Succeeded),
        "partial" => Ok(CollectionState::Partial),
        "failed" => Ok(CollectionState::Failed),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

fn parse_published_collection_status(value: &str) -> Result<CollectionState, ReportingReadError> {
    match parse_collection_status(value)? {
        status @ (CollectionState::Succeeded | CollectionState::Partial) => Ok(status),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

fn parse_snapshot_status(value: &str) -> Result<SnapshotStatus, ReportingReadError> {
    match value {
        "succeeded" => Ok(SnapshotStatus::Succeeded),
        "partial" => Ok(SnapshotStatus::Partial),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

fn parse_finance_category(value: &str) -> Result<FinanceCategory, ReportingReadError> {
    match value {
        "sale" => Ok(FinanceCategory::Sale),
        "commission" => Ok(FinanceCategory::Commission),
        "acquiring" => Ok(FinanceCategory::Acquiring),
        "logistics" => Ok(FinanceCategory::Logistics),
        "storage" => Ok(FinanceCategory::Storage),
        "paid_acceptance" => Ok(FinanceCategory::PaidAcceptance),
        "compensation" => Ok(FinanceCategory::Compensation),
        "marketplace_discount" => Ok(FinanceCategory::MarketplaceDiscount),
        "advertising" => Ok(FinanceCategory::Advertising),
        "other" => Ok(FinanceCategory::Other),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

fn parse_report_kind(value: &str) -> Result<ReadyReportKind, ReportingReadError> {
    match value {
        "morning" => Ok(ReadyReportKind::Morning),
        "evening" => Ok(ReadyReportKind::Evening),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

fn parse_report_state(value: &str) -> Result<ReadyReportState, ReportingReadError> {
    match value {
        "ready" => Ok(ReadyReportState::Ready),
        "sent" => Ok(ReadyReportState::Sent),
        _ => Err(ReportingReadError::InvalidPublishedData),
    }
}

fn nonnegative_i64(value: i64) -> Result<u64, ReportingReadError> {
    u64::try_from(value).map_err(|_| ReportingReadError::InvalidPublishedData)
}

fn positive_i64(value: i64) -> Result<u64, ReportingReadError> {
    let value = nonnegative_i64(value)?;
    (value > 0)
        .then_some(value)
        .ok_or(ReportingReadError::InvalidPublishedData)
}

fn nonnegative_optional_i64(value: Option<i64>) -> Result<Option<u64>, ReportingReadError> {
    value.map(nonnegative_i64).transpose()
}

fn nonnegative_i32(value: i32) -> Result<u64, ReportingReadError> {
    u64::try_from(value).map_err(|_| ReportingReadError::InvalidPublishedData)
}

fn positive_i32(value: i32) -> Result<u64, ReportingReadError> {
    let value = nonnegative_i32(value)?;
    (value > 0)
        .then_some(value)
        .ok_or(ReportingReadError::InvalidPublishedData)
}

fn nonnegative_optional_i32(value: Option<i32>) -> Result<Option<u64>, ReportingReadError> {
    value.map(nonnegative_i32).transpose()
}

fn valid_http_status(value: i16) -> Result<u16, ReportingReadError> {
    if (400..=599).contains(&value) {
        u16::try_from(value).map_err(|_| ReportingReadError::InvalidPublishedData)
    } else {
        Err(ReportingReadError::InvalidPublishedData)
    }
}

fn validate_currency(value: &str) -> Result<(), ReportingReadError> {
    (value == "RUB")
        .then_some(())
        .ok_or(ReportingReadError::InvalidPublishedData)
}

fn validate_collector_version(value: &str) -> Result<(), ReportingReadError> {
    if !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        Ok(())
    } else {
        Err(ReportingReadError::InvalidPublishedData)
    }
}

fn valid_error_class(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn valid_warehouse_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

fn timestamp_string(value: DateTime<Utc>) -> String {
    super::business_timestamp(value)
}

impl ReportingReadRepository for PostgresReportingRepository {
    fn enabled(&self) -> bool {
        true
    }

    fn collection_status<'a>(
        &'a self,
        account: &'a AccountScope,
        limit: u16,
    ) -> ReportingReadFuture<'a, CollectionStatusResult> {
        Box::pin(async move { self.collection_status_impl(account, limit).await })
    }

    fn data_completeness<'a>(
        &'a self,
        account: &'a AccountScope,
        cutoff: Option<DateTime<Utc>>,
    ) -> ReportingReadFuture<'a, DataCompletenessResult> {
        Box::pin(async move { self.data_completeness_impl(account, cutoff).await })
    }

    fn metrics_history<'a>(
        &'a self,
        account: &'a AccountScope,
        from: Option<NaiveDate>,
        to: Option<NaiveDate>,
        limit: u16,
    ) -> ReportingReadFuture<'a, MetricsHistoryResult> {
        Box::pin(async move { self.metrics_history_impl(account, from, to, limit).await })
    }

    fn manager_actions<'a>(
        &'a self,
        account: &'a AccountScope,
        cutoff: Option<DateTime<Utc>>,
    ) -> ReportingReadFuture<'a, ManagerActionsResult> {
        Box::pin(async move { self.manager_actions_impl(account, cutoff).await })
    }

    fn ready_reports(&self, limit: u16) -> ReportingReadFuture<'_, ReadyReportsResult> {
        Box::pin(async move { self.ready_reports_impl(limit).await })
    }
}

const COLLECTION_STATUS_QUERY: &str = "SELECT snapshot_id, account_id, marketplace, source, cutoff_at, source_as_of, \
            status, pagination_complete, row_count, collector_version, started_at, \
            finished_at, error_class, http_status, last_published_cutoff_at, \
            last_published_source_as_of, last_published_status, last_published_row_count \
     FROM daily_reporting.mcp_collection_status \
     WHERE account_id = $1 AND marketplace = $2 \
     ORDER BY cutoff_at DESC, source \
     LIMIT $3";

const PUBLISHED_SNAPSHOTS_QUERY: &str = "SELECT snapshot_id, account_id, marketplace, source, cutoff_at, source_as_of, \
            period_start, period_end, status, pagination_complete, row_count \
     FROM daily_reporting.mcp_published_source_snapshots \
     WHERE account_id = $1 AND marketplace = $2 AND cutoff_at = $3 \
     ORDER BY source";

const CONTRACT_PROBES: &[&str] = &[
    "SELECT snapshot_id, account_id, marketplace, source, cutoff_at, source_as_of, \
            period_start, period_end, status, pagination_complete, row_count, \
            collector_version, started_at, finished_at, error_class, http_status, \
            last_published_cutoff_at, last_published_source_as_of, \
            last_published_status, last_published_row_count \
     FROM daily_reporting.mcp_collection_status LIMIT 0",
    "SELECT snapshot_id, account_id, marketplace, source, cutoff_at, source_as_of, \
            period_start, period_end, status, pagination_complete, row_count, \
            collector_version, finished_at \
     FROM daily_reporting.mcp_published_source_snapshots LIMIT 0",
    "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, sku, \
            ordered_units, operational_gmv_minor, cancelled_units, returned_units, currency \
     FROM daily_reporting.mcp_sales_facts LIMIT 0",
    "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, campaign_id, sku, \
            impressions, clicks, spend_minor, attributed_orders, attributed_revenue_minor, \
            currency, basket_additions, model_attributed_orders, \
            model_attributed_revenue_minor, product_price_minor, average_cpc_minor, \
            cpm_minor, cpl_minor FROM daily_reporting.mcp_advertising_facts LIMIT 0",
    "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, campaign_id, \
            money_spent_minor, bonus_spent_minor, prepayment_spent_minor, currency \
     FROM daily_reporting.mcp_advertising_expense_facts LIMIT 0",
    "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, sku, sku_key, category, \
            amount_minor, line_count, unknown_type_count \
     FROM daily_reporting.mcp_finance_facts LIMIT 0",
    "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, sku, warehouse_id, sellable_units \
     FROM daily_reporting.mcp_stock_facts LIMIT 0",
    "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, sku, price_minor, old_price_minor, currency \
     FROM daily_reporting.mcp_price_facts LIMIT 0",
    "SELECT batch_id, recipient_id, report_version, local_date, report_kind, \
            scheduled_for, deadline_at, status, delayed, created_at, updated_at, sent_at \
     FROM daily_reporting.mcp_ready_reports LIMIT 0",
];

impl PostgresReportingRepository {
    async fn metrics_history_impl(
        &self,
        account: &AccountScope,
        from: Option<NaiveDate>,
        to: Option<NaiveDate>,
        limit: u16,
    ) -> Result<MetricsHistoryResult, ReportingReadError> {
        validate_limit(limit, MAX_HISTORY_POINTS)?;
        let range = history_range(from, to)?;
        let marketplace = marketplace_str(account.marketplace());
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let cutoff_rows = client
            .query(
                "SELECT cutoff_at \
                 FROM daily_reporting.mcp_published_source_snapshots \
                 WHERE account_id = $1 AND marketplace = $2 \
                   AND cutoff_at >= $3 AND cutoff_at < $4 \
                 GROUP BY cutoff_at \
                 ORDER BY cutoff_at DESC \
                 LIMIT $5",
                &[
                    &account.account_id(),
                    &marketplace,
                    &range.utc_start,
                    &range.utc_end,
                    &i64::from(limit),
                ],
            )
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let cutoffs = cutoff_rows
            .iter()
            .map(|row| column(row, 0))
            .collect::<Result<Vec<DateTime<Utc>>, _>>()?;
        if cutoffs.is_empty() {
            return Ok(MetricsHistoryResult {
                account_id: account.account_id().to_owned(),
                marketplace: account.marketplace().into(),
                date_from: range.from.to_string(),
                date_to: range.to.to_string(),
                points: Vec::new(),
            });
        }

        let snapshot_rows = client
            .query(
                "SELECT snapshot_id, account_id, marketplace, source, cutoff_at, source_as_of, \
                        period_start, period_end, status, pagination_complete, row_count \
                 FROM daily_reporting.mcp_published_source_snapshots \
                 WHERE account_id = $1 AND marketplace = $2 \
                   AND cutoff_at = ANY($3::timestamptz[]) \
                 ORDER BY cutoff_at, source",
                &[&account.account_id(), &marketplace, &cutoffs],
            )
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let mut by_cutoff = BTreeMap::<DateTime<Utc>, Vec<SnapshotDescriptor>>::new();
        for row in snapshot_rows {
            let cutoff: DateTime<Utc> = column(&row, 4)?;
            let descriptor = published_descriptor(&row, account, cutoff)?;
            by_cutoff.entry(cutoff).or_default().push(descriptor);
        }
        let expected = by_cutoff
            .values()
            .flatten()
            .map(|descriptor| (descriptor.snapshot_id(), descriptor.clone()))
            .collect::<BTreeMap<_, _>>();
        let kpis = load_history_kpis(&client, &expected).await?;
        std::mem::drop(client);

        let mut points = Vec::with_capacity(cutoffs.len());
        for cutoff in cutoffs.into_iter().rev() {
            let descriptors = by_cutoff.remove(&cutoff).unwrap_or_default();
            let state = state_for_descriptors(account, &descriptors)?;
            let summary = kpis.get(&cutoff).cloned();
            points.push(MetricsHistoryPoint {
                cutoff_at: timestamp_string(cutoff),
                state,
                kpis: summary.map(Into::into),
            });
        }
        Ok(MetricsHistoryResult {
            account_id: account.account_id().to_owned(),
            marketplace: account.marketplace().into(),
            date_from: range.from.to_string(),
            date_to: range.to.to_string(),
            points,
        })
    }

    async fn manager_actions_impl(
        &self,
        account: &AccountScope,
        requested_cutoff: Option<DateTime<Utc>>,
    ) -> Result<ManagerActionsResult, ReportingReadError> {
        let Some(cutoff) = self.resolve_cutoff(account, requested_cutoff).await? else {
            return Ok(unavailable_actions(account));
        };
        let descriptors = self.load_descriptors(account, cutoff).await?;
        if descriptors.is_empty() {
            return Ok(ManagerActionsResult {
                account_id: account.account_id().to_owned(),
                marketplace: account.marketplace().into(),
                cutoff_at: Some(timestamp_string(cutoff)),
                state: DataState::Unavailable,
                recommendations_allowed: false,
                actions: Vec::new(),
            });
        }
        let manifest = FrozenSnapshotManifest::new(cutoff, vec![account.clone()], descriptors)
            .map_err(|_| ReportingReadError::InvalidPublishedData)?;
        let state = state_from_quality(manifest.quality());
        if !manifest.recommendations_allowed() {
            return Ok(ManagerActionsResult {
                account_id: account.account_id().to_owned(),
                marketplace: account.marketplace().into(),
                cutoff_at: Some(timestamp_string(cutoff)),
                state,
                recommendations_allowed: false,
                actions: Vec::new(),
            });
        }
        let facts = self.load_report_facts(&manifest).await?;
        let dataset = ReportDataset::from_published(&manifest, facts)
            .map_err(|_| ReportingReadError::InvalidPublishedData)?;
        let inputs = rule_inputs(&dataset).map_err(|_| ReportingReadError::InvalidPublishedData)?;
        let actions = priority_problems(&inputs, true, u64::MAX)
            .map_err(|_| ReportingReadError::InvalidPublishedData)?
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(ManagerActionsResult {
            account_id: account.account_id().to_owned(),
            marketplace: account.marketplace().into(),
            cutoff_at: Some(timestamp_string(cutoff)),
            state,
            recommendations_allowed: true,
            actions,
        })
    }

    async fn ready_reports_impl(
        &self,
        limit: u16,
    ) -> Result<ReadyReportsResult, ReportingReadError> {
        validate_limit(limit, MAX_READY_REPORTS)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let rows = client
            .query(
                "SELECT batch_id, recipient_id, report_version, local_date, report_kind, \
                        scheduled_for, deadline_at, status, delayed, created_at, updated_at, sent_at \
                 FROM daily_reporting.mcp_ready_reports \
                 ORDER BY local_date DESC, scheduled_for DESC, batch_id DESC \
                 LIMIT $1",
                &[&i64::from(limit)],
            )
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let reports = rows
            .iter()
            .map(ready_report_item)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ReadyReportsResult { reports })
    }

    async fn load_report_facts(
        &self,
        manifest: &FrozenSnapshotManifest,
    ) -> Result<PublishedReportFacts, ReportingReadError> {
        let expected_rows = manifest
            .snapshots()
            .iter()
            .try_fold(0usize, |total, snapshot| {
                total.checked_add(snapshot.row_count() as usize)
            })
            .ok_or(ReportingReadError::InvalidPublishedData)?;
        validate_expected_fact_rows(expected_rows)?;
        let expected = manifest
            .snapshots()
            .iter()
            .map(|descriptor| (descriptor.snapshot_id(), descriptor.clone()))
            .collect::<BTreeMap<_, _>>();
        let snapshot_ids = expected.keys().copied().collect::<Vec<_>>();
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let sales = query_sales_facts(&client, &snapshot_ids, &expected).await?;
        let advertising = query_advertising_facts(&client, &snapshot_ids, &expected).await?;
        let advertising_expenses =
            query_advertising_expense_facts(&client, &snapshot_ids, &expected).await?;
        let finance = query_finance_facts(&client, &snapshot_ids, &expected).await?;
        let stocks = query_stock_facts(&client, &snapshot_ids, &expected).await?;
        let prices = query_price_facts(&client, &snapshot_ids, &expected).await?;
        Ok(PublishedReportFacts {
            sales,
            advertising,
            advertising_expenses,
            finance,
            stocks,
            prices,
        })
    }
}

fn validate_reader_database(config: &Config) -> Result<(), ReportingReadError> {
    if config.get_user() == Some("position_reader")
        && config
            .get_password()
            .is_some_and(|password| !password.is_empty())
        && config.get_dbname().is_some_and(|value| !value.is_empty())
        && config.get_hosts().len() == 1
        && matches!(config.get_hosts(), [Host::Tcp(host)] if !host.trim().is_empty())
        && config.get_options().is_none()
    {
        Ok(())
    } else {
        Err(ReportingReadError::InvalidRequest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account() -> AccountScope {
        AccountScope::new("store_1".to_owned(), Marketplace::Ozon).unwrap()
    }

    #[derive(Debug)]
    struct FakeReportingRepository;

    impl ReportingReadRepository for FakeReportingRepository {
        fn enabled(&self) -> bool {
            true
        }

        fn collection_status<'a>(
            &'a self,
            account: &'a AccountScope,
            _limit: u16,
        ) -> ReportingReadFuture<'a, CollectionStatusResult> {
            Box::pin(async move {
                Ok(CollectionStatusResult {
                    account_id: account.account_id().to_owned(),
                    marketplace: account.marketplace().into(),
                    items: Vec::new(),
                })
            })
        }

        fn data_completeness<'a>(
            &'a self,
            account: &'a AccountScope,
            _cutoff: Option<DateTime<Utc>>,
        ) -> ReportingReadFuture<'a, DataCompletenessResult> {
            Box::pin(async move { Ok(unavailable_completeness(account)) })
        }

        fn metrics_history<'a>(
            &'a self,
            account: &'a AccountScope,
            from: Option<NaiveDate>,
            to: Option<NaiveDate>,
            _limit: u16,
        ) -> ReportingReadFuture<'a, MetricsHistoryResult> {
            Box::pin(async move {
                let date = NaiveDate::from_ymd_opt(2026, 8, 20).unwrap();
                Ok(MetricsHistoryResult {
                    account_id: account.account_id().to_owned(),
                    marketplace: account.marketplace().into(),
                    date_from: from.unwrap_or(date).to_string(),
                    date_to: to.unwrap_or(date).to_string(),
                    points: Vec::new(),
                })
            })
        }

        fn manager_actions<'a>(
            &'a self,
            account: &'a AccountScope,
            _cutoff: Option<DateTime<Utc>>,
        ) -> ReportingReadFuture<'a, ManagerActionsResult> {
            Box::pin(async move { Ok(unavailable_actions(account)) })
        }

        fn ready_reports(&self, _limit: u16) -> ReportingReadFuture<'_, ReadyReportsResult> {
            Box::pin(async {
                Ok(ReadyReportsResult {
                    reports: Vec::new(),
                })
            })
        }
    }

    #[tokio::test]
    async fn disabled_mode_fails_closed_with_stable_errors() {
        let reader = ReportingReader::disabled();
        let account = account();
        let date = NaiveDate::from_ymd_opt(2026, 8, 20).unwrap();
        assert!(!reader.is_enabled());
        assert_eq!(
            reader.collection_status(&account, 1).await,
            Err(ReportingReadError::Disabled)
        );
        assert_eq!(
            reader.data_completeness(&account, None).await,
            Err(ReportingReadError::Disabled)
        );
        assert_eq!(
            reader
                .metrics_history(&account, Some(date), Some(date), 1)
                .await,
            Err(ReportingReadError::Disabled)
        );
        assert_eq!(
            reader.manager_actions(&account, None).await,
            Err(ReportingReadError::Disabled)
        );
        assert_eq!(
            reader.ready_reports(1).await,
            Err(ReportingReadError::Disabled)
        );
        assert_eq!(
            ReportingReadError::Unavailable.to_string(),
            "reporting history is temporarily unavailable"
        );
    }

    #[tokio::test]
    async fn public_bounds_are_checked_before_repository_dispatch() {
        let reader = ReportingReader::from_repository(Arc::new(FakeReportingRepository));
        let account = account();
        assert!(reader.is_enabled());
        assert_eq!(
            reader.collection_status(&account, 0).await,
            Err(ReportingReadError::InvalidRequest)
        );
        assert_eq!(
            reader
                .collection_status(&account, MAX_STATUS_ROWS + 1)
                .await,
            Err(ReportingReadError::InvalidRequest)
        );
        assert_eq!(
            reader.metrics_history(&account, None, None, 0).await,
            Err(ReportingReadError::InvalidRequest)
        );
        assert_eq!(
            reader.ready_reports(MAX_READY_REPORTS + 1).await,
            Err(ReportingReadError::InvalidRequest)
        );

        let result = reader.collection_status(&account, 1).await.unwrap();
        assert_eq!(result.account_id, account.account_id());
        assert_eq!(result.marketplace, ReportingMarketplace::Ozon);
        assert_eq!(
            reader
                .data_completeness(&account, None)
                .await
                .unwrap()
                .state,
            DataState::Unavailable
        );
        assert!(
            reader
                .metrics_history(&account, None, None, 1)
                .await
                .unwrap()
                .points
                .is_empty()
        );
        assert_eq!(
            reader.manager_actions(&account, None).await.unwrap().state,
            DataState::Unavailable
        );
        assert!(reader.ready_reports(1).await.unwrap().reports.is_empty());
    }

    #[tokio::test]
    async fn absent_configuration_is_the_explicit_disabled_mode() {
        let reader = ReportingReader::connect_optional(None).await.unwrap();
        assert!(!reader.is_enabled());
        assert_eq!(
            format!("{reader:?}"),
            "ReportingReader { enabled: false, .. }"
        );
    }

    /// The reader is the MCP surface a manager queries directly, so a database
    /// that disappears mid-session must surface as `Unavailable` on every tool
    /// rather than as an empty result set that reads like "there is no data".
    /// Aborting the connection task reproduces a severed socket or a backend
    /// restart while the client handle is still alive.
    #[tokio::test]
    async fn every_read_reports_unavailable_when_the_database_is_gone() {
        verify_every_read_reports_unavailable_when_the_database_is_gone(None).await;
        verify_every_read_reports_unavailable_when_the_database_is_gone(
            std::env::var("POSITION_REPOSITORY_TEST_READER_URL").ok(),
        )
        .await;
    }

    async fn verify_every_read_reports_unavailable_when_the_database_is_gone(
        reader_url: Option<String>,
    ) {
        let Some(reader_url) = reader_url else {
            return;
        };
        let (client, connection) = Config::from_str(&reader_url)
            .unwrap()
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        let connection_task = tokio::spawn(connection);
        let repository = PostgresReportingRepository::from_client(client);
        // Prove the repository is genuinely healthy first, so the assertions
        // below cannot pass because the fixture was broken all along.
        repository.verify_runtime_contract().await.unwrap();

        connection_task.abort();
        let _ = connection_task.await;

        let account = account();
        assert_eq!(
            repository.verify_runtime_contract().await,
            Err(ReportingReadError::Unavailable)
        );
        assert_eq!(
            repository.collection_status(&account, 10).await.err(),
            Some(ReportingReadError::Unavailable)
        );
        assert_eq!(
            repository.data_completeness(&account, None).await.err(),
            Some(ReportingReadError::Unavailable)
        );
        assert_eq!(
            repository
                .metrics_history(&account, None, None, 10)
                .await
                .err(),
            Some(ReportingReadError::Unavailable)
        );
        assert_eq!(
            repository.manager_actions(&account, None).await.err(),
            Some(ReportingReadError::Unavailable)
        );
        assert_eq!(
            repository.ready_reports(10).await.err(),
            Some(ReportingReadError::Unavailable)
        );
    }

    #[tokio::test]
    async fn postgres_row_decoders_propagate_published_contract_failures() {
        verify_postgres_row_decoders_propagate_published_contract_failures(None).await;
        verify_postgres_row_decoders_propagate_published_contract_failures(
            std::env::var("POSITION_REPOSITORY_TEST_READER_URL").ok(),
        )
        .await;
    }

    async fn verify_postgres_row_decoders_propagate_published_contract_failures(
        reader_url: Option<String>,
    ) {
        let Some(reader_url) = reader_url else {
            return;
        };
        let (client, connection) = Config::from_str(&reader_url)
            .unwrap()
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        let connection_task = tokio::spawn(connection);
        let account = account();

        let invalid_lifecycle = client
            .query_one(
                "SELECT 1::bigint, 'store_1'::text, 'ozon'::text, 'sales'::text, \
                        '2026-08-20 03:00:00Z'::timestamptz, \
                        '2026-08-20 02:30:00Z'::timestamptz, \
                        'succeeded'::text, true, 1::integer, 'collector-1'::text, \
                        '2026-08-20 02:58:00Z'::timestamptz, NULL::timestamptz, \
                        NULL::text, NULL::smallint, NULL::timestamptz, \
                        NULL::timestamptz, NULL::text, NULL::integer",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            collection_status_item(&invalid_lifecycle, &account),
            Err(ReportingReadError::InvalidPublishedData)
        );

        let invalid_checkpoint = client
            .query_one(
                "SELECT 2::bigint, 'store_1'::text, 'ozon'::text, 'sales'::text, \
                        '2026-08-20 03:00:00Z'::timestamptz, \
                        '2026-08-20 02:30:00Z'::timestamptz, \
                        'succeeded'::text, true, 1::integer, 'collector-1'::text, \
                        '2026-08-20 02:58:00Z'::timestamptz, \
                        '2026-08-20 02:59:00Z'::timestamptz, NULL::text, \
                        NULL::smallint, '2026-08-20 02:00:00Z'::timestamptz, \
                        NULL::timestamptz, NULL::text, NULL::integer",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            collection_status_item(&invalid_checkpoint, &account),
            Err(ReportingReadError::InvalidPublishedData)
        );

        let invalid_report_schedule = client
            .query_one(
                "SELECT 1::bigint, 'manager_1'::text, 1::integer, \
                        '2026-08-20'::date, 'morning'::text, \
                        '2026-08-20 03:01:00Z'::timestamptz, \
                        '2026-08-20 09:00:00Z'::timestamptz, 'ready'::text, \
                        false, '2026-08-20 03:00:00Z'::timestamptz, \
                        '2026-08-20 03:02:00Z'::timestamptz, NULL::timestamptz",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            ready_report_item(&invalid_report_schedule),
            Err(ReportingReadError::InvalidPublishedData)
        );
        drop(client);
        connection_task
            .await
            .expect("reader connection task must join")
            .expect("reader connection must close cleanly");
    }

    #[test]
    fn history_range_uses_exact_yekaterinburg_day_boundaries() {
        let from = NaiveDate::from_ymd_opt(2026, 8, 20).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 8, 22).unwrap();
        let range = history_range(Some(from), Some(to)).unwrap();
        assert_eq!(range.from, from);
        assert_eq!(range.to, to);
        assert_eq!(
            range.utc_start,
            Utc.with_ymd_and_hms(2026, 8, 19, 19, 0, 0).unwrap()
        );
        assert_eq!(
            range.utc_end,
            Utc.with_ymd_and_hms(2026, 8, 22, 19, 0, 0).unwrap()
        );
        assert!(matches!(
            history_range(Some(to), Some(from)),
            Err(ReportingReadError::InvalidRequest)
        ));
        assert!(matches!(
            history_range(
                Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
                Some(NaiveDate::from_ymd_opt(2026, 1, 2).unwrap())
            ),
            Err(ReportingReadError::InvalidRequest)
        ));
        assert!(matches!(
            history_range(Some(NaiveDate::MAX), Some(NaiveDate::MAX)),
            Err(ReportingReadError::InvalidRequest)
        ));
        let defaults = history_range(None, None).unwrap();
        assert_eq!(
            defaults.to.signed_duration_since(defaults.from).num_days(),
            29
        );
    }

    #[test]
    fn only_the_restricted_tcp_reader_configuration_is_accepted() {
        let valid: Config = "postgresql://position_reader:secret@127.0.0.1/ofk"
            .parse()
            .unwrap();
        assert_eq!(validate_reader_database(&valid), Ok(()));

        for invalid in [
            "postgresql://report_worker:secret@127.0.0.1/ofk",
            "postgresql://position_reader@127.0.0.1/ofk",
            "host=/tmp user=position_reader password=secret dbname=ofk",
        ] {
            let config: Config = invalid.parse().unwrap();
            assert_eq!(
                validate_reader_database(&config),
                Err(ReportingReadError::InvalidRequest)
            );
        }
    }

    #[test]
    fn ready_report_dto_cannot_serialize_routing_or_provider_secrets() {
        let report = ReadyReportsResult {
            reports: vec![ReadyReportItem {
                batch_id: "rb_0000000000000001".to_owned(),
                report_version: 1,
                local_date: "2026-08-20".to_owned(),
                kind: ReadyReportKind::Morning,
                state: ReadyReportState::Ready,
                artifact_ready: true,
                sent: false,
                delayed: false,
                scheduled_for: "2026-08-20T03:00:00.000000Z".to_owned(),
                deadline_at: "2026-08-20T09:00:00.000000Z".to_owned(),
                state_changed_at: "2026-08-20T03:00:01.000000Z".to_owned(),
                sent_at: None,
            }],
        };
        let json = serde_json::to_string(&report).unwrap();
        for forbidden in [
            "recipient",
            "email",
            "provider",
            "object_path",
            "hash",
            "error",
        ] {
            assert!(!json.contains(forbidden));
        }
    }

    #[test]
    fn ready_report_schedule_accepts_normal_and_recovered_morning_windows() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 20).unwrap();
        let utc = |hour| Utc.with_ymd_and_hms(2026, 8, 20, hour, 0, 0).unwrap();

        assert!(report_schedule_matches(
            date,
            ReadyReportKind::Morning,
            utc(3),
            utc(9)
        ));
        assert!(report_schedule_matches(
            date,
            ReadyReportKind::Morning,
            utc(12),
            utc(18)
        ));
        assert!(report_schedule_matches(
            date,
            ReadyReportKind::Evening,
            utc(12),
            utc(18)
        ));
        assert!(!report_schedule_matches(
            date,
            ReadyReportKind::Evening,
            utc(3),
            utc(9)
        ));
    }

    fn descriptor(
        account: &AccountScope,
        snapshot_id: i64,
        source: SnapshotSource,
        status: SnapshotStatus,
        age: Duration,
    ) -> SnapshotDescriptor {
        let cutoff = Utc.with_ymd_and_hms(2026, 8, 20, 3, 0, 0).unwrap();
        let source_as_of = cutoff - age;
        let (period_start, period_end) = if matches!(
            source,
            SnapshotSource::Sales | SnapshotSource::Advertising | SnapshotSource::Finance
        ) {
            (cutoff - Duration::days(1), cutoff)
        } else {
            (source_as_of, source_as_of)
        };
        SnapshotDescriptor::new(
            snapshot_id,
            account.account_id().to_owned(),
            account.marketplace(),
            source,
            cutoff,
            source_as_of,
            period_start,
            period_end,
            1,
            status == SnapshotStatus::Succeeded,
            status,
        )
        .unwrap()
    }

    #[test]
    fn pure_contract_parsers_reject_unknown_or_impossible_values() {
        assert_eq!(parse_marketplace("ozon"), Ok(Marketplace::Ozon));
        assert_eq!(
            parse_marketplace("wildberries"),
            Ok(Marketplace::Wildberries)
        );
        assert_eq!(
            parse_marketplace("other"),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(marketplace_str(Marketplace::Ozon), "ozon");
        assert_eq!(marketplace_str(Marketplace::Wildberries), "wildberries");

        for (raw, source) in [
            ("sales", SnapshotSource::Sales),
            ("advertising", SnapshotSource::Advertising),
            ("finance", SnapshotSource::Finance),
            ("stocks", SnapshotSource::Stocks),
            ("prices", SnapshotSource::Prices),
        ] {
            assert_eq!(parse_source(raw), Ok(source));
        }
        assert_eq!(
            parse_source("other"),
            Err(ReportingReadError::InvalidPublishedData)
        );

        for (raw, state) in [
            ("running", CollectionState::Running),
            ("succeeded", CollectionState::Succeeded),
            ("partial", CollectionState::Partial),
            ("failed", CollectionState::Failed),
        ] {
            assert_eq!(parse_collection_status(raw), Ok(state));
        }
        assert_eq!(
            parse_collection_status("unknown"),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(
            parse_published_collection_status("succeeded"),
            Ok(CollectionState::Succeeded)
        );
        assert_eq!(
            parse_published_collection_status("partial"),
            Ok(CollectionState::Partial)
        );
        assert_eq!(
            parse_published_collection_status("running"),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(
            parse_published_collection_status("unknown"),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(
            parse_snapshot_status("succeeded"),
            Ok(SnapshotStatus::Succeeded)
        );
        assert_eq!(
            parse_snapshot_status("partial"),
            Ok(SnapshotStatus::Partial)
        );
        assert_eq!(
            parse_snapshot_status("failed"),
            Err(ReportingReadError::InvalidPublishedData)
        );

        for (raw, category) in [
            ("sale", FinanceCategory::Sale),
            ("commission", FinanceCategory::Commission),
            ("acquiring", FinanceCategory::Acquiring),
            ("logistics", FinanceCategory::Logistics),
            ("storage", FinanceCategory::Storage),
            ("paid_acceptance", FinanceCategory::PaidAcceptance),
            ("compensation", FinanceCategory::Compensation),
            ("marketplace_discount", FinanceCategory::MarketplaceDiscount),
            ("advertising", FinanceCategory::Advertising),
            ("other", FinanceCategory::Other),
        ] {
            assert_eq!(parse_finance_category(raw), Ok(category));
        }
        assert_eq!(
            parse_finance_category("unknown"),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(parse_report_kind("morning"), Ok(ReadyReportKind::Morning));
        assert_eq!(parse_report_kind("evening"), Ok(ReadyReportKind::Evening));
        assert_eq!(
            parse_report_kind("weekly"),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(parse_report_state("ready"), Ok(ReadyReportState::Ready));
        assert_eq!(parse_report_state("sent"), Ok(ReadyReportState::Sent));
        assert_eq!(
            parse_report_state("sending"),
            Err(ReportingReadError::InvalidPublishedData)
        );
    }

    #[test]
    fn numeric_and_identifier_contracts_are_bounded() {
        assert_eq!(nonnegative_i64(0), Ok(0));
        assert_eq!(
            nonnegative_i64(-1),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(positive_i64(1), Ok(1));
        assert_eq!(
            positive_i64(0),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(nonnegative_optional_i64(None), Ok(None));
        assert_eq!(nonnegative_optional_i64(Some(2)), Ok(Some(2)));
        assert_eq!(
            nonnegative_optional_i64(Some(-1)),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(nonnegative_i32(0), Ok(0));
        assert_eq!(
            nonnegative_i32(-1),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(positive_i32(1), Ok(1));
        assert_eq!(
            positive_i32(0),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(nonnegative_optional_i32(None), Ok(None));
        assert_eq!(nonnegative_optional_i32(Some(2)), Ok(Some(2)));
        assert_eq!(
            nonnegative_optional_i32(Some(-1)),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(valid_http_status(400), Ok(400));
        assert_eq!(valid_http_status(599), Ok(599));
        assert_eq!(
            valid_http_status(399),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(
            valid_http_status(600),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(validate_currency("RUB"), Ok(()));
        assert_eq!(
            validate_currency("USD"),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(validate_collector_version("collector-1.2_test"), Ok(()));
        for invalid in ["", "bad/version", &"x".repeat(65)] {
            assert_eq!(
                validate_collector_version(invalid),
                Err(ReportingReadError::InvalidPublishedData)
            );
        }
        assert!(valid_error_class("http_429"));
        assert!(!valid_error_class(""));
        assert!(!valid_error_class("HTTP_429"));
        assert!(!valid_error_class("bad-class"));
        assert!(!valid_error_class(&"x".repeat(65)));
        assert!(valid_identifier("store_1-test"));
        assert!(!valid_identifier(""));
        assert!(!valid_identifier("bad/id"));
        assert!(!valid_identifier(&"x".repeat(129)));
        assert!(valid_warehouse_id("fbo.msk:1-test"));
        assert!(!valid_warehouse_id(""));
        assert!(!valid_warehouse_id("bad/id"));
        assert!(!valid_warehouse_id(&"x".repeat(129)));
        assert_eq!(validate_fact_query_size(MAX_FACT_ROWS), Ok(()));
        assert_eq!(
            validate_fact_query_size(MAX_FACT_ROWS + 1),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(
            timestamp_string(Utc.with_ymd_and_hms(2026, 8, 20, 3, 0, 0).unwrap()),
            "2026-08-20T08:00:00.000000+05:00"
        );
    }

    #[test]
    fn typed_collection_contracts_cover_every_lifecycle_and_checkpoint_shape() {
        let cutoff = Utc.with_ymd_and_hms(2026, 8, 20, 3, 0, 0).unwrap();
        let started_at = cutoff - Duration::minutes(2);
        let finished_at = cutoff - Duration::minutes(1);
        let base = CollectionLifecycle {
            status: CollectionState::Succeeded,
            pagination_complete: true,
            cutoff,
            source_as_of: cutoff - Duration::minutes(30),
            started_at,
            finished_at: Some(finished_at),
            error_class: None,
            http_status: None,
        };
        assert_eq!(validate_collection_lifecycle(base), Ok(()));
        assert_eq!(
            validate_collection_lifecycle(CollectionLifecycle {
                status: CollectionState::Running,
                pagination_complete: false,
                finished_at: None,
                ..base
            }),
            Ok(())
        );
        assert_eq!(
            validate_collection_lifecycle(CollectionLifecycle {
                status: CollectionState::Partial,
                pagination_complete: false,
                ..base
            }),
            Ok(())
        );
        assert_eq!(
            validate_collection_lifecycle(CollectionLifecycle {
                status: CollectionState::Failed,
                pagination_complete: false,
                error_class: Some("http_429"),
                http_status: Some(429),
                ..base
            }),
            Ok(())
        );
        for invalid in [
            CollectionLifecycle {
                source_as_of: cutoff + Duration::hours(25),
                ..base
            },
            CollectionLifecycle {
                pagination_complete: false,
                ..base
            },
            CollectionLifecycle {
                status: CollectionState::Failed,
                error_class: Some("INVALID"),
                ..base
            },
        ] {
            assert_eq!(
                validate_collection_lifecycle(invalid),
                Err(ReportingReadError::InvalidPublishedData)
            );
        }

        let empty = PublishedCheckpointParts {
            current_cutoff: cutoff,
            cutoff: None,
            source_as_of: None,
            status: None,
            row_count: None,
        };
        assert_eq!(published_checkpoint(empty), Ok(None));
        let valid = PublishedCheckpointParts {
            current_cutoff: cutoff,
            cutoff: Some(cutoff - Duration::hours(1)),
            source_as_of: Some(cutoff - Duration::hours(2)),
            status: Some("succeeded"),
            row_count: Some(10),
        };
        assert_eq!(published_checkpoint(valid).unwrap().unwrap().row_count, 10);
        assert_eq!(
            published_checkpoint(PublishedCheckpointParts {
                cutoff: Some(cutoff + Duration::seconds(1)),
                ..valid
            }),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(
            published_checkpoint(PublishedCheckpointParts {
                cutoff: None,
                ..valid
            }),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(validate_runtime_contract(true), Ok(()));
        assert_eq!(
            validate_runtime_contract(false),
            Err(ReportingReadError::Unavailable)
        );
        assert_eq!(validate_descriptor_cutoff(cutoff, cutoff), Ok(()));
        assert_eq!(
            validate_descriptor_cutoff(cutoff, cutoff + Duration::seconds(1)),
            Err(ReportingReadError::InvalidPublishedData)
        );

        let account = account();
        assert_eq!(
            validate_scope(account.account_id(), Marketplace::Ozon, &account),
            Ok(())
        );
        assert_eq!(
            validate_scope("another_store", Marketplace::Ozon, &account),
            Err(ReportingReadError::InvalidPublishedData)
        );
    }

    #[test]
    fn typed_metric_and_fact_contracts_reject_corrupt_published_rows() {
        assert_eq!(validate_expected_fact_rows(MAX_FACT_ROWS), Ok(()));
        assert_eq!(
            validate_expected_fact_rows(MAX_FACT_ROWS + 1),
            Err(ReportingReadError::InvalidRequest)
        );
        assert_eq!(metric_fact_count(SnapshotSource::Sales, 2, 3), Ok(2));
        assert_eq!(metric_fact_count(SnapshotSource::Advertising, 2, 3), Ok(3));
        for source in [
            SnapshotSource::Finance,
            SnapshotSource::Stocks,
            SnapshotSource::Prices,
        ] {
            assert_eq!(
                metric_fact_count(source, 2, 3),
                Err(ReportingReadError::InvalidPublishedData)
            );
        }
        assert_eq!(validate_snapshot_fact_count(1, 1), Ok(()));
        assert_eq!(
            validate_snapshot_fact_count(0, 1),
            Err(ReportingReadError::InvalidPublishedData)
        );

        let account = account();
        let sales = descriptor(
            &account,
            1,
            SnapshotSource::Sales,
            SnapshotStatus::Succeeded,
            Duration::minutes(30),
        );
        let advertising = descriptor(
            &account,
            2,
            SnapshotSource::Advertising,
            SnapshotStatus::Succeeded,
            Duration::minutes(30),
        );
        let duplicate_sales = descriptor(
            &account,
            3,
            SnapshotSource::Sales,
            SnapshotStatus::Succeeded,
            Duration::minutes(30),
        );
        let finance = descriptor(
            &account,
            4,
            SnapshotSource::Finance,
            SnapshotStatus::Succeeded,
            Duration::minutes(30),
        );
        let mut ids = MetricSourceIds::new();
        assert_eq!(insert_metric_source_id(&mut ids, &sales), Ok(()));
        assert_eq!(insert_metric_source_id(&mut ids, &advertising), Ok(()));
        assert_eq!(complete_metric_pair(ids[&sales.cutoff_at()]), Some((1, 2)));
        assert_eq!(
            insert_metric_source_id(&mut ids, &duplicate_sales),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(
            insert_metric_source_id(&mut MetricSourceIds::new(), &finance),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(complete_metric_pair((Some(1), None)), None);

        assert_eq!(validate_advertising_counts(10, 10), Ok(()));
        assert_eq!(
            validate_advertising_counts(10, 11),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(validate_finance_identity(Some(7), 7, 2, 1), Ok(()));
        assert_eq!(validate_finance_identity(None, 0, 1, 0), Ok(()));
        for invalid in [
            validate_finance_identity(Some(0), 0, 1, 0),
            validate_finance_identity(Some(7), 8, 1, 0),
            validate_finance_identity(Some(7), 7, 1, 2),
            validate_finance_identity(Some(u64::MAX), 1, 1, 0),
        ] {
            assert_eq!(invalid, Err(ReportingReadError::InvalidPublishedData));
        }
        assert_eq!(validate_warehouse_id("fbo-msk:1"), Ok(()));
        assert_eq!(
            validate_warehouse_id("bad/warehouse"),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(validate_price_relation(100, None), Ok(()));
        assert_eq!(validate_price_relation(100, Some(120)), Ok(()));
        assert_eq!(
            validate_price_relation(100, Some(99)),
            Err(ReportingReadError::InvalidPublishedData)
        );

        let provenance = FactProvenance {
            snapshot_id: sales.snapshot_id(),
            source: sales.source(),
            account_id: sales.account_id(),
            marketplace: sales.marketplace(),
            cutoff_at: sales.cutoff_at(),
            source_as_of: sales.source_as_of(),
            status: sales.status(),
            pagination_complete: sales.pagination_complete(),
        };
        assert_eq!(
            validate_fact_provenance(provenance, &sales, SnapshotSource::Sales),
            Ok(())
        );
        assert_eq!(
            validate_fact_provenance(
                FactProvenance {
                    pagination_complete: false,
                    ..provenance
                },
                &sales,
                SnapshotSource::Sales,
            ),
            Err(ReportingReadError::InvalidPublishedData)
        );
    }

    #[test]
    fn typed_ready_report_contract_rejects_invalid_identity_timing_and_state() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 20).unwrap();
        let scheduled_for = Utc.with_ymd_and_hms(2026, 8, 20, 3, 0, 0).unwrap();
        let deadline_at = Utc.with_ymd_and_hms(2026, 8, 20, 9, 0, 0).unwrap();
        let created_at = scheduled_for - Duration::minutes(1);
        let updated_at = scheduled_for + Duration::minutes(1);
        let ready = ReadyReportContract {
            local_date: date,
            kind: ReadyReportKind::Morning,
            scheduled_for,
            deadline_at,
            state: ReadyReportState::Ready,
            created_at,
            updated_at,
            sent_at: None,
        };
        assert_eq!(validate_recipient_id("manager_1"), Ok(()));
        assert_eq!(
            validate_recipient_id("manager/1"),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(validate_ready_report_contract(ready), Ok(()));
        assert_eq!(
            validate_ready_report_contract(ReadyReportContract {
                state: ReadyReportState::Sent,
                sent_at: Some(updated_at),
                ..ready
            }),
            Ok(())
        );
        for invalid in [
            ReadyReportContract {
                updated_at: created_at - Duration::seconds(1),
                ..ready
            },
            ReadyReportContract {
                deadline_at: scheduled_for - Duration::seconds(1),
                ..ready
            },
            ReadyReportContract {
                scheduled_for: scheduled_for + Duration::minutes(1),
                ..ready
            },
            ReadyReportContract {
                sent_at: Some(updated_at),
                ..ready
            },
            ReadyReportContract {
                state: ReadyReportState::Sent,
                ..ready
            },
            ReadyReportContract {
                local_date: NaiveDate::MIN,
                ..ready
            },
        ] {
            assert_eq!(
                validate_ready_report_contract(invalid),
                Err(ReportingReadError::InvalidPublishedData)
            );
        }
    }

    #[test]
    fn descriptor_completeness_is_account_scoped_and_duplicate_safe() {
        let account = account();
        let cutoff = Utc.with_ymd_and_hms(2026, 8, 20, 3, 0, 0).unwrap();
        let descriptors = SnapshotSource::required_for(Marketplace::Ozon)
            .iter()
            .enumerate()
            .map(|(index, source)| {
                descriptor(
                    &account,
                    i64::try_from(index + 1).unwrap(),
                    *source,
                    SnapshotStatus::Succeeded,
                    Duration::minutes(30),
                )
            })
            .collect::<Vec<_>>();
        let complete = completeness_from_descriptors(&account, cutoff, &descriptors).unwrap();
        assert_eq!(complete.state, DataState::Complete);
        assert!(complete.recommendations_allowed);
        assert!(complete.sources.iter().all(|source| source.available));
        assert_eq!(
            state_for_descriptors(&account, &descriptors),
            Ok(DataState::Complete)
        );

        let partial = completeness_from_descriptors(&account, cutoff, &descriptors[..2]).unwrap();
        assert_eq!(partial.state, DataState::Partial);
        assert!(!partial.recommendations_allowed);
        assert!(partial.sources.iter().any(|source| !source.available));
        assert_eq!(
            state_for_descriptors(&account, &descriptors[..2]),
            Ok(DataState::Partial)
        );
        assert_eq!(
            state_for_descriptors(&account, &[]),
            Ok(DataState::Unavailable)
        );

        let unavailable = completeness_from_descriptors(&account, cutoff, &[]).unwrap();
        assert_eq!(unavailable.state, DataState::Unavailable);
        assert!(unavailable.cutoff_at.is_some());

        let mut duplicate = descriptors.clone();
        duplicate.push(descriptors[0].clone());
        assert_eq!(
            completeness_from_descriptors(&account, cutoff, &duplicate),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(
            state_for_descriptors(&account, &duplicate),
            Err(ReportingReadError::InvalidPublishedData)
        );

        let other = AccountScope::new("store_2".to_owned(), Marketplace::Ozon).unwrap();
        let wrong_scope = vec![descriptor(
            &other,
            99,
            SnapshotSource::Sales,
            SnapshotStatus::Succeeded,
            Duration::minutes(30),
        )];
        assert_eq!(
            completeness_from_descriptors(&account, cutoff, &wrong_scope),
            Err(ReportingReadError::InvalidPublishedData)
        );
        assert_eq!(
            state_for_descriptors(&account, &wrong_scope),
            Err(ReportingReadError::InvalidPublishedData)
        );

        let mut degraded = descriptors;
        degraded[0] = descriptor(
            &account,
            1,
            SnapshotSource::Sales,
            SnapshotStatus::Partial,
            Duration::minutes(30),
        );
        let degraded = completeness_from_descriptors(&account, cutoff, &degraded).unwrap();
        assert_eq!(degraded.state, DataState::Partial);
        assert!(!degraded.recommendations_allowed);
        assert_eq!(
            state_from_quality(SnapshotQuality::Complete),
            DataState::Complete
        );
        for quality in [
            SnapshotQuality::Partial,
            SnapshotQuality::Stale,
            SnapshotQuality::Critical,
        ] {
            assert_eq!(state_from_quality(quality), DataState::Partial);
        }
    }

    #[test]
    fn public_projection_conversions_cover_every_supported_variant() {
        assert_eq!(
            ReportingMarketplace::from(Marketplace::Ozon),
            ReportingMarketplace::Ozon
        );
        assert_eq!(
            ReportingMarketplace::from(Marketplace::Wildberries),
            ReportingMarketplace::Wildberries
        );
        for source in SnapshotSource::required_for(Marketplace::Ozon) {
            let _ = ReportingSource::from(*source);
        }
        assert_eq!(
            CollectionState::from(SnapshotStatus::Succeeded),
            CollectionState::Succeeded
        );
        assert_eq!(
            CollectionState::from(SnapshotStatus::Partial),
            CollectionState::Partial
        );
        for quality in [
            SnapshotQuality::Complete,
            SnapshotQuality::Partial,
            SnapshotQuality::Stale,
            SnapshotQuality::Critical,
        ] {
            let _ = DataQuality::from(quality);
        }
        let summary = calculate_kpis(
            &[SalesMetricInput {
                ordered_units: 3,
                operational_gmv_minor: 30_000,
                cancelled_units: Some(1),
                returned_units: Some(0),
            }],
            &[AdvertisingMetricInput {
                impressions: 100,
                clicks: 10,
                spend_minor: 1_000,
                attributed_orders: 2,
                attributed_revenue_minor: 10_000,
            }],
        )
        .unwrap();
        let projected = KpiValues::from(summary);
        assert_eq!(projected.ordered_units, 3);
        assert_eq!(projected.ctr_bps, Some(1_000));

        for (index, kind) in [
            ProblemKind::AdvertisedWithoutStock,
            ProblemKind::Stockout,
            ProblemKind::LowStockCover,
            ProblemKind::SpendWithoutOrders,
            ProblemKind::HighDrr,
        ]
        .into_iter()
        .enumerate()
        {
            for severity in [Severity::Yellow, Severity::Red] {
                let action = ManagerAction::from(PriorityProblem {
                    account_id: "store_1".to_owned(),
                    sku: u64::try_from(index + 1).unwrap(),
                    kind,
                    severity,
                    observed: 10,
                    threshold: 5,
                    impact_minor: 1_000,
                });
                assert_eq!(
                    action.severity,
                    if severity == Severity::Yellow {
                        ActionSeverity::Yellow
                    } else {
                        ActionSeverity::Red
                    }
                );
            }
        }
    }
}
