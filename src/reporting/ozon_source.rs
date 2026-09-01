//! Bounded read-only Ozon report source.
//!
//! This module keeps collection transport-agnostic for tests and delegates the
//! explicit canary runtime to `OzonClient`. Every request is first built by the
//! exact contract in `ozon_adapter` and every response is normalized before it
//! can reach report persistence.

use std::{collections::BTreeSet, future::Future, pin::Pin};

use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde_json::Value;
use thiserror::Error;

use crate::{
    config::StoreId,
    ozon::{OzonClient, OzonErrorKind},
};

use super::{
    ozon_adapter::{
        OzonReportParseError, OzonReportRequest, next_warehouse_stock_cursor, parse_price_page,
        parse_sales_page, parse_stock_page, parse_warehouse_stock_page, product_page_request,
        sales_request, warehouse_stock_page_request,
    },
    ozon_finance_source::collect_finance_facts,
    postgres_collector::{
        CollectedAdvertisingExpenseFact, CollectedAdvertisingFact, CollectedFacts,
        CollectedFinanceFact, CollectedPriceFact, CollectedSalesFact, CollectedSnapshot,
        CollectedStockFact, PostgresCollectorError,
    },
    snapshot::{Marketplace, SnapshotStatus},
};

/// How many times a locally-refused page request is re-offered.
///
/// `Overloaded` is the one Ozon error that never reached the marketplace:
/// `OzonClient` returns it from permit acquisition, before anything is sent,
/// and suppresses it in favour of the causal error once an upstream attempt
/// has been made. Retrying it therefore cannot duplicate a request.
///
/// Without this, one transient burst of local contention aborted an entire
/// account's collection and discarded every page already gathered — the whole
/// run has to be re-done because snapshots are published atomically.
const OVERLOAD_RETRY_ATTEMPTS: usize = 4;
const OVERLOAD_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(250);

// `/v1/analytics/data` uses the shared guarded 65-second queue. Ten bounded
// pages cover up to 9,999 rows and keep a complete collection inside the
// operator dry-run deadline; a tenth full page fails closed instead of
// starting an unbounded multi-hour backfill. On 429 the client installs an
// adaptive cooldown and allows this collector at most two queued retries
// inside a ten-minute retry budget.
const MAX_SALES_PAGES: usize = 10;
// At 100 products/page this still accommodates 10,000 products, while the
// manual dry-run's absolute deadline bounds the total request time.
const MAX_PRODUCT_PAGES: usize = 100;

/// Collects the three Seller sources and atomically publishes them together
/// with a separately verified Performance SKU snapshot.
///
/// The observation clock is invoked only after every Seller request has
/// completed. This prevents callers from accidentally labelling later facts
/// with a timestamp captured before collection finished.
#[allow(clippy::too_many_arguments)]
pub async fn collect_complete_snapshots<F>(
    transport: &dyn OzonReportTransport,
    advertising: Vec<CollectedAdvertisingFact>,
    account_id: String,
    cutoff_at: DateTime<Utc>,
    completed_at: F,
    sales_period_start: DateTime<Utc>,
    sales_period_end: DateTime<Utc>,
    collector_version: String,
) -> Result<Vec<CollectedSnapshot>, OzonReportSourceError>
where
    F: FnOnce() -> DateTime<Utc>,
{
    collect_complete_snapshots_extended(
        transport,
        advertising,
        Vec::new(),
        account_id,
        cutoff_at,
        completed_at,
        sales_period_start,
        sales_period_end,
        collector_version,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn collect_complete_snapshots_extended<F>(
    transport: &dyn OzonReportTransport,
    advertising: Vec<CollectedAdvertisingFact>,
    advertising_expenses: Vec<CollectedAdvertisingExpenseFact>,
    account_id: String,
    cutoff_at: DateTime<Utc>,
    completed_at: F,
    sales_period_start: DateTime<Utc>,
    sales_period_end: DateTime<Utc>,
    collector_version: String,
) -> Result<Vec<CollectedSnapshot>, OzonReportSourceError>
where
    F: FnOnce() -> DateTime<Utc>,
{
    let source = OzonReportSource::new(transport);
    let (date_from, date_to) = report_business_dates(sales_period_start, sales_period_end)?;
    let facts = source
        .collect_required_seller_facts(date_from, date_to)
        .await?;
    let source_as_of = completed_at();
    facts
        .into_complete_snapshots_extended(
            advertising,
            advertising_expenses,
            account_id,
            cutoff_at,
            source_as_of,
            sales_period_start,
            sales_period_end,
            collector_version,
        )
        .map_err(|_| OzonReportSourceError::InvalidSnapshotInput)
}

fn report_business_dates(
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
) -> Result<(NaiveDate, NaiveDate), OzonReportSourceError> {
    if period_end <= period_start {
        return Err(OzonReportSourceError::InvalidSnapshotInput);
    }
    let offset = super::yekaterinburg_offset();
    // `period_end > period_start` proves that `period_end` is not the minimum
    // representable instant, so moving the exclusive bound back by 1 ns is safe.
    let inclusive_end = period_end - Duration::nanoseconds(1);
    Ok((
        period_start.with_timezone(&offset).date_naive(),
        inclusive_end.with_timezone(&offset).date_naive(),
    ))
}

pub trait OzonReportTransport: Send + Sync {
    fn post<'a>(
        &'a self,
        request: OzonReportRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>>;
}

impl<T: OzonReportTransport + ?Sized> OzonReportTransport for &T {
    fn post<'a>(
        &'a self,
        request: OzonReportRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>> {
        (**self).post(request)
    }
}

/// Production transport adapter. It deliberately owns no credential material:
/// `OzonClient` owns the policy-scoped store map and enforces the central
/// Seller API read-only egress allowlist at request time.
#[derive(Clone)]
pub struct OzonClientReportTransport {
    client: OzonClient,
    store: StoreId,
}

impl OzonClientReportTransport {
    #[must_use]
    pub const fn new(client: OzonClient, store: StoreId) -> Self {
        Self { client, store }
    }
}

impl OzonReportTransport for OzonClientReportTransport {
    fn post<'a>(
        &'a self,
        request: OzonReportRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>> {
        let path = request.path;
        Box::pin(async move {
            retry_local_overload(path, || async {
                self.client
                    .post_queued(&self.store, path, request.payload.clone())
                    .await
                    // Keep only the stable, non-sensitive classification. In
                    // particular, never retain Ozon's error body in report
                    // collection diagnostics.
                    .map_err(|error| error.kind())
            })
            .await
        })
    }
}

/// Re-offers a page request that local admission control refused.
///
/// Extracted from the transport so the policy is testable without saturating
/// a real client's semaphores, which are private to `OzonClient` for good
/// reason.
async fn retry_local_overload<F, Fut>(
    path: &'static str,
    mut attempt_once: F,
) -> Result<Value, OzonReportSourceError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Value, OzonErrorKind>>,
{
    let mut attempt = 1;
    loop {
        match attempt_once().await {
            Ok(value) => return Ok(value),
            Err(OzonErrorKind::Overloaded) if attempt < OVERLOAD_RETRY_ATTEMPTS => {
                tracing::debug!(
                    endpoint = path,
                    attempt,
                    "report collection deferred by local admission control"
                );
                attempt += 1;
                tokio::time::sleep(OVERLOAD_RETRY_DELAY).await;
            }
            Err(kind) => return Err(report_upstream_failure(path, kind)),
        }
    }
}

fn report_upstream_failure(path: &'static str, kind: OzonErrorKind) -> OzonReportSourceError {
    let error_code = kind.code();
    tracing::warn!(
        endpoint = path,
        error_code,
        "Ozon daily-report request failed"
    );
    OzonReportSourceError::Upstream(kind)
}

pub struct OzonReportSource<T> {
    transport: T,
}

/// Complete in-memory Ozon Seller input for one report cutoff.
///
/// It is only
/// returned after all requested sources have succeeded, so callers can pass
/// it to the transactional PostgreSQL writer without mixing partial data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OzonCollectedFacts {
    pub sales: Vec<CollectedSalesFact>,
    pub finance: Vec<CollectedFinanceFact>,
    pub stocks: Vec<CollectedStockFact>,
    pub prices: Vec<CollectedPriceFact>,
}

impl OzonCollectedFacts {
    #[allow(clippy::too_many_arguments)]
    pub fn into_snapshots(
        self,
        account_id: String,
        cutoff_at: DateTime<Utc>,
        source_as_of: DateTime<Utc>,
        sales_period_start: DateTime<Utc>,
        sales_period_end: DateTime<Utc>,
        collector_version: String,
    ) -> Result<Vec<CollectedSnapshot>, PostgresCollectorError> {
        let sales = CollectedSnapshot::new(
            account_id.clone(),
            Marketplace::Ozon,
            cutoff_at,
            source_as_of,
            sales_period_start,
            sales_period_end,
            SnapshotStatus::Succeeded,
            true,
            collector_version.clone(),
            CollectedFacts::Sales(self.sales),
        )
        .map_err(|error| snapshot_validation_error("sales", error))?;
        let stocks = CollectedSnapshot::new(
            account_id.clone(),
            Marketplace::Ozon,
            cutoff_at,
            source_as_of,
            source_as_of,
            source_as_of,
            SnapshotStatus::Succeeded,
            true,
            collector_version.clone(),
            CollectedFacts::Stocks(self.stocks),
        )
        .map_err(|error| snapshot_validation_error("stocks", error))?;
        let finance = CollectedSnapshot::new(
            account_id.clone(),
            Marketplace::Ozon,
            cutoff_at,
            source_as_of,
            sales_period_start,
            sales_period_end,
            SnapshotStatus::Succeeded,
            true,
            collector_version.clone(),
            CollectedFacts::Finance(self.finance),
        )
        .map_err(|error| snapshot_validation_error("finance", error))?;
        let prices = CollectedSnapshot::new(
            account_id,
            Marketplace::Ozon,
            cutoff_at,
            source_as_of,
            source_as_of,
            source_as_of,
            SnapshotStatus::Succeeded,
            true,
            collector_version,
            CollectedFacts::Prices(self.prices),
        )
        .map_err(|error| snapshot_validation_error("prices", error))?;
        Ok(vec![sales, finance, stocks, prices])
    }

    #[allow(clippy::too_many_arguments)]
    pub fn into_complete_snapshots(
        self,
        advertising: Vec<CollectedAdvertisingFact>,
        account_id: String,
        cutoff_at: DateTime<Utc>,
        source_as_of: DateTime<Utc>,
        sales_period_start: DateTime<Utc>,
        sales_period_end: DateTime<Utc>,
        collector_version: String,
    ) -> Result<Vec<CollectedSnapshot>, PostgresCollectorError> {
        self.into_complete_snapshots_extended(
            advertising,
            Vec::new(),
            account_id,
            cutoff_at,
            source_as_of,
            sales_period_start,
            sales_period_end,
            collector_version,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn into_complete_snapshots_extended(
        self,
        advertising: Vec<CollectedAdvertisingFact>,
        advertising_expenses: Vec<CollectedAdvertisingExpenseFact>,
        account_id: String,
        cutoff_at: DateTime<Utc>,
        source_as_of: DateTime<Utc>,
        sales_period_start: DateTime<Utc>,
        sales_period_end: DateTime<Utc>,
        collector_version: String,
    ) -> Result<Vec<CollectedSnapshot>, PostgresCollectorError> {
        let advertising = CollectedSnapshot::new(
            account_id.clone(),
            Marketplace::Ozon,
            cutoff_at,
            source_as_of,
            sales_period_start,
            sales_period_end,
            SnapshotStatus::Succeeded,
            true,
            collector_version.clone(),
            CollectedFacts::Advertising(advertising),
        )?
        .with_advertising_expenses(advertising_expenses)?;
        let mut snapshots = self.into_snapshots(
            account_id,
            cutoff_at,
            source_as_of,
            sales_period_start,
            sales_period_end,
            collector_version,
        )?;
        snapshots.insert(1, advertising);
        Ok(snapshots)
    }
}

fn snapshot_validation_error(
    source: &'static str,
    error: PostgresCollectorError,
) -> PostgresCollectorError {
    tracing::warn!(source, "Ozon daily-report snapshot facts failed validation");
    error
}

impl<T> OzonReportSource<T> {
    pub const fn new(transport: T) -> Self {
        Self { transport }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OzonReportSourceError {
    #[error("Ozon daily-report source request failed")]
    Upstream(OzonErrorKind),
    #[error("Ozon daily-report source request failed")]
    Transport,
    #[error("Ozon daily-report source response is invalid")]
    InvalidResponse,
    #[error("Ozon daily-report sales response is invalid")]
    InvalidSalesResponse { shape: String },
    #[error("Ozon daily-report stocks response is invalid")]
    InvalidStocksResponse,
    #[error("Ozon daily-report prices response is invalid")]
    InvalidPricesResponse,
    #[error("Ozon daily-report finance response is invalid")]
    InvalidFinanceResponse,
    #[error("Ozon daily-report snapshot input is invalid")]
    InvalidSnapshotInput,
    #[error("Ozon daily-report source pagination exceeded its fixed bound")]
    PaginationLimit,
}

impl OzonReportSourceError {
    /// A stable, non-sensitive diagnostic code suitable for operator logs.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Upstream(kind) => kind.code(),
            Self::Transport => "transport_error",
            Self::InvalidResponse => "invalid_response",
            Self::InvalidSalesResponse { .. } => "invalid_sales_response",
            Self::InvalidStocksResponse => "invalid_stocks_response",
            Self::InvalidPricesResponse => "invalid_prices_response",
            Self::InvalidFinanceResponse => "invalid_finance_response",
            Self::InvalidSnapshotInput => "invalid_snapshot_input",
            Self::PaginationLimit => "pagination_limit",
        }
    }

    /// A value-free, bounded structural fingerprint for the one sales parse
    /// failure that needs operator investigation. It never includes an Ozon
    /// response value, identifier, amount, name, or credential.
    #[must_use]
    pub fn diagnostic(&self) -> Option<&str> {
        match self {
            Self::InvalidSalesResponse { shape } => Some(shape),
            _ => None,
        }
    }
}

impl<T: OzonReportTransport> OzonReportSource<T> {
    pub async fn collect_required_seller_facts(
        &self,
        date_from: NaiveDate,
        date_to: NaiveDate,
    ) -> Result<OzonCollectedFacts, OzonReportSourceError> {
        tracing::info!(source = "sales", "collecting Ozon daily-report source");
        let sales = self.collect_sales_pages(date_from, date_to).await?;
        tracing::info!(source = "finance", "collecting Ozon daily-report source");
        let finance = collect_finance_facts(&self.transport, date_from, date_to).await?;
        tracing::info!(source = "stocks", "collecting Ozon daily-report source");
        let stocks = self.collect_stock_pages().await?;
        tracing::info!(source = "prices", "collecting Ozon daily-report source");
        let prices = self.collect_price_pages().await?;
        Ok(OzonCollectedFacts {
            sales,
            finance,
            stocks,
            prices,
        })
    }

    pub async fn sales_page(
        &self,
        date_from: NaiveDate,
        date_to: NaiveDate,
        offset: u32,
    ) -> Result<Vec<CollectedSalesFact>, OzonReportSourceError> {
        let request = sales_request(date_from, date_to, offset)
            .map_err(|_| OzonReportSourceError::InvalidResponse)?;
        let response = self.transport.post(request).await?;
        parse_sales_page(&response).map_err(|_| {
            let shape = sales_response_shape(&response);
            tracing::warn!(shape, "Ozon Seller analytics response shape was rejected");
            OzonReportSourceError::InvalidSalesResponse { shape }
        })
    }

    /// Collects offset-paginated sales rows under the client's one-request-per-
    /// minute Analytics gate. A full-size final page is not accepted as
    /// complete because the upstream response has no trustworthy total-row
    /// contract.
    pub async fn collect_sales_pages(
        &self,
        date_from: NaiveDate,
        date_to: NaiveDate,
    ) -> Result<Vec<CollectedSalesFact>, OzonReportSourceError> {
        let mut facts = Vec::new();
        for page in 0..MAX_SALES_PAGES {
            let offset = u32::try_from(page)
                .ok()
                .and_then(|page| page.checked_mul(1_000))
                .ok_or(OzonReportSourceError::PaginationLimit)?;
            let rows = self.sales_page(date_from, date_to, offset).await?;
            let complete = rows.len() < 1_000;
            facts.extend(rows);
            if complete {
                return Ok(facts);
            }
        }
        Err(OzonReportSourceError::PaginationLimit)
    }

    /// Collects real warehouse-granular FBO and FBS stock pages when the
    /// legacy warehouse endpoints remain available.
    ///
    /// Ozon has retired the legacy FBO route for some accounts. Only an HTTP
    /// rejection or an explicit not-found response from that first route may
    /// fall back to the already allowlisted `/v4/product/info/stocks` source.
    /// The normalized fallback exposes fulfillment-level, not physical-
    /// warehouse-level, inventory; its stable `fbo`/`fbs` identifiers retain
    /// that provenance. Authentication, quota, server, and transport failures
    /// remain fail-closed and are never hidden by the fallback.
    pub async fn collect_stock_pages(
        &self,
    ) -> Result<Vec<CollectedStockFact>, OzonReportSourceError> {
        let fbo = self
            .collect_warehouse_stock_pages("/v1/product/info/stocks-by-warehouse/fbo", "fbo")
            .await;
        let mut facts = match fbo {
            Ok(facts) => facts,
            Err(OzonReportSourceError::Upstream(OzonErrorKind::Http | OzonErrorKind::NotFound)) => {
                tracing::warn!(
                    endpoint = "/v1/product/info/stocks-by-warehouse/fbo",
                    fallback = "/v4/product/info/stocks",
                    "legacy Ozon stock endpoint was rejected; using fulfillment-level fallback"
                );
                return self
                    .collect_product_pages(
                        "/v4/product/info/stocks",
                        parse_stock_page,
                        OzonReportSourceError::InvalidStocksResponse,
                    )
                    .await;
            }
            Err(error) => return Err(error),
        };
        facts.extend(
            self.collect_warehouse_stock_pages("/v2/product/info/stocks-by-warehouse/fbs", "fbs")
                .await?,
        );
        Ok(facts)
    }

    async fn collect_warehouse_stock_pages(
        &self,
        path: &'static str,
        scheme: &'static str,
    ) -> Result<Vec<CollectedStockFact>, OzonReportSourceError> {
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut facts = Vec::new();
        for _ in 0..MAX_PRODUCT_PAGES {
            let request = warehouse_stock_page_request(path, cursor.as_deref())
                .map_err(|_| OzonReportSourceError::InvalidResponse)?;
            let response = self.transport.post(request).await?;
            facts.extend(
                parse_warehouse_stock_page(&response, scheme)
                    .map_err(|_| OzonReportSourceError::InvalidStocksResponse)?,
            );
            cursor = next_warehouse_stock_cursor(&response)
                .map_err(|_| OzonReportSourceError::InvalidStocksResponse)?;
            if cursor
                .as_ref()
                .is_some_and(|cursor| !seen_cursors.insert(cursor.clone()))
            {
                return Err(OzonReportSourceError::InvalidStocksResponse);
            }
            if cursor.is_none() {
                return Ok(facts);
            }
        }
        Err(OzonReportSourceError::PaginationLimit)
    }

    /// Collects cursor-paginated price pages with a fixed upper bound.
    pub async fn collect_price_pages(
        &self,
    ) -> Result<Vec<CollectedPriceFact>, OzonReportSourceError> {
        self.collect_product_pages(
            "/v5/product/info/prices",
            parse_price_page,
            OzonReportSourceError::InvalidPricesResponse,
        )
        .await
    }

    async fn collect_product_pages<F, Fact>(
        &self,
        path: &'static str,
        parse: F,
        invalid_response: OzonReportSourceError,
    ) -> Result<Vec<Fact>, OzonReportSourceError>
    where
        F: Fn(&Value) -> Result<Vec<Fact>, OzonReportParseError> + Copy,
    {
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut facts = Vec::new();
        for _ in 0..MAX_PRODUCT_PAGES {
            let request = product_page_request(path, cursor.as_deref())
                .map_err(|_| OzonReportSourceError::InvalidResponse)?;
            let response = self.transport.post(request).await?;
            facts.extend(parse(&response).map_err(|_| invalid_response.clone())?);
            cursor = next_cursor(&response, invalid_response.clone())?;
            if cursor
                .as_ref()
                .is_some_and(|cursor| !seen_cursors.insert(cursor.clone()))
            {
                return Err(invalid_response);
            }
            if cursor.is_none() {
                return Ok(facts);
            }
        }
        Err(OzonReportSourceError::PaginationLimit)
    }
}

fn next_cursor(
    response: &Value,
    invalid_response: OzonReportSourceError,
) -> Result<Option<String>, OzonReportSourceError> {
    let cursor = response
        .get("cursor")
        .and_then(Value::as_str)
        .ok_or(invalid_response)?;
    if cursor.is_empty() {
        return Ok(None);
    }
    product_page_request("/v4/product/info/stocks", Some(cursor))
        .map_err(|_| OzonReportSourceError::InvalidResponse)?;
    Ok(Some(cursor.to_owned()))
}

/// Produces a bounded, value-free structural fingerprint for a rejected sales
/// response. It intentionally emits neither identifiers nor metric values.
fn sales_response_shape(response: &Value) -> String {
    let result = response.get("result").and_then(Value::as_object);
    let data = result
        .and_then(|result| result.get("data"))
        .and_then(Value::as_array);
    let Some(first) = data
        .and_then(|rows| rows.first())
        .and_then(Value::as_object)
    else {
        return format!(
            "root={},result={},data={}",
            json_kind(response),
            result.is_some(),
            data.map_or_else(
                || "not_array".to_owned(),
                |rows| format!("array:{}", rows.len())
            )
        );
    };
    let dimensions = first.get("dimensions").and_then(Value::as_array);
    let metrics = first.get("metrics").and_then(Value::as_array);
    let dimension_kinds = dimensions.map_or_else(
        || "not_array".to_owned(),
        |values| {
            values
                .iter()
                .take(4)
                .map(|value| json_kind(value).to_owned())
                .collect::<Vec<_>>()
                .join(",")
        },
    );
    let metric_kinds = metrics.map_or_else(
        || "not_array".to_owned(),
        |values| {
            values
                .iter()
                .take(6)
                .map(|value| json_kind(value).to_owned())
                .collect::<Vec<_>>()
                .join(",")
        },
    );
    format!(
        "root={},result={},data=array:{},dimensions={}:[{}],metrics={}:[{}]",
        json_kind(response),
        result.is_some(),
        data.map_or(0, Vec::len),
        dimensions.map_or(0, Vec::len),
        dimension_kinds,
        metrics.map_or(0, Vec::len),
        metric_kinds,
    )
}

const fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::Mutex,
    };

    use chrono::{NaiveDate, TimeZone, Utc};
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::config::StoreCredentials;
    use crate::reporting::postgres_collector::FinanceCategory;

    /// A local admission refusal never reached Ozon, so re-offering the page
    /// cannot duplicate a marketplace request. Before this, one transient
    /// burst of contention aborted the whole account run and discarded every
    /// page already collected, because snapshots publish atomically.
    #[tokio::test(start_paused = true)]
    async fn a_transient_local_overload_does_not_discard_the_run() {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_writer(std::io::sink)
            .finish();
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);
        let calls = std::cell::Cell::new(0_usize);
        let forced_failure = std::cell::Cell::new(None);
        let mut attempt_once = || async {
            calls.set(calls.get() + 1);
            forced_failure.get().map_or_else(
                || {
                    if calls.get() < 3 {
                        Err(OzonErrorKind::Overloaded)
                    } else {
                        Ok(serde_json::json!({"result": "ok"}))
                    }
                },
                Err,
            )
        };
        let value = retry_local_overload("/v3/posting/fbo/list", &mut attempt_once)
            .await
            .expect("a transient local refusal must not fail the page");
        assert_eq!(value["result"], "ok");
        assert_eq!(calls.get(), 3);

        forced_failure.set(Some(OzonErrorKind::Overloaded));
        let error = retry_local_overload("/v3/posting/fbo/list", &mut attempt_once)
            .await
            .expect_err("the same request closure must honor the bounded retry budget");
        assert_eq!(
            error,
            OzonReportSourceError::Upstream(OzonErrorKind::Overloaded)
        );
        assert_eq!(calls.get(), 3 + OVERLOAD_RETRY_ATTEMPTS);

        // Only the local refusal is retried. An upstream failure has already
        // reached Ozon, so repeating it would be a second marketplace request.
        let calls_before_upstream_failure = calls.get();
        forced_failure.set(Some(OzonErrorKind::RateLimited));
        let error = retry_local_overload("/v3/posting/fbo/list", &mut attempt_once)
            .await
            .expect_err("an upstream failure is surfaced");
        assert_eq!(
            error,
            OzonReportSourceError::Upstream(OzonErrorKind::RateLimited)
        );
        assert_eq!(calls.get(), calls_before_upstream_failure + 1);
    }

    struct FixtureTransport(Mutex<VecDeque<Result<Value, OzonReportSourceError>>>);

    impl OzonReportTransport for FixtureTransport {
        fn post<'a>(
            &'a self,
            _request: OzonReportRequest,
        ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>>
        {
            Box::pin(async move { self.0.lock().unwrap().pop_front().unwrap() })
        }
    }

    struct RecordingTransport {
        responses: Mutex<VecDeque<Result<Value, OzonReportSourceError>>>,
        paths: Mutex<Vec<&'static str>>,
    }

    impl OzonReportTransport for RecordingTransport {
        fn post<'a>(
            &'a self,
            request: OzonReportRequest,
        ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>>
        {
            Box::pin(async move {
                self.paths.lock().unwrap().push(request.path);
                self.responses.lock().unwrap().pop_front().unwrap()
            })
        }
    }

    #[tokio::test]
    async fn stock_collection_reads_both_warehouse_schemes_in_order() {
        let transport = RecordingTransport {
            responses: Mutex::new(VecDeque::from([
                Ok(json!({"products":[],"cursor":"","has_next":false})),
                Ok(json!({"products":[],"cursor":"","has_next":false})),
            ])),
            paths: Mutex::new(Vec::new()),
        };
        let source = OzonReportSource::new(&transport);

        assert!(source.collect_stock_pages().await.unwrap().is_empty());
        assert_eq!(
            transport.paths.lock().unwrap().as_slice(),
            [
                "/v1/product/info/stocks-by-warehouse/fbo",
                "/v2/product/info/stocks-by-warehouse/fbs",
            ]
        );
    }

    #[tokio::test]
    async fn stock_collection_propagates_a_non_fallback_fbo_failure() {
        let transport = RecordingTransport {
            responses: Mutex::new(VecDeque::from([Err(OzonReportSourceError::Upstream(
                OzonErrorKind::RateLimited,
            ))])),
            paths: Mutex::new(Vec::new()),
        };
        let source = OzonReportSource::new(&transport);

        assert_eq!(
            source.collect_stock_pages().await,
            Err(OzonReportSourceError::Upstream(OzonErrorKind::RateLimited))
        );
        assert_eq!(
            transport.paths.lock().unwrap().as_slice(),
            ["/v1/product/info/stocks-by-warehouse/fbo"]
        );
    }

    #[tokio::test]
    async fn retired_fbo_route_uses_the_bounded_fulfillment_fallback() {
        let transport = RecordingTransport {
            responses: Mutex::new(VecDeque::from([
                Err(OzonReportSourceError::Upstream(OzonErrorKind::Http)),
                Ok(json!({
                    "items":[{
                        "product_id":1,
                        "stocks":[
                            {"type":"fbo","present":4,"reserved":0},
                            {"type":"fbs","present":2,"reserved":0}
                        ]
                    }],
                    "cursor":""
                })),
            ])),
            paths: Mutex::new(Vec::new()),
        };
        let source = OzonReportSource::new(&transport);

        let facts = source.collect_stock_pages().await.unwrap();

        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].warehouse_id, "FBO");
        assert_eq!(facts[1].warehouse_id, "FBS");
        assert_eq!(
            transport.paths.lock().unwrap().as_slice(),
            [
                "/v1/product/info/stocks-by-warehouse/fbo",
                "/v4/product/info/stocks",
            ]
        );
    }

    #[tokio::test]
    async fn retired_fbo_route_rejects_an_invalid_fulfillment_fallback_page() {
        let transport = RecordingTransport {
            responses: Mutex::new(VecDeque::from([
                Err(OzonReportSourceError::Upstream(OzonErrorKind::Http)),
                Ok(json!({"items": "invalid", "cursor": ""})),
            ])),
            paths: Mutex::new(Vec::new()),
        };
        let source = OzonReportSource::new(&transport);

        assert_eq!(
            source.collect_stock_pages().await,
            Err(OzonReportSourceError::InvalidStocksResponse)
        );
        assert_eq!(
            transport.paths.lock().unwrap().as_slice(),
            [
                "/v1/product/info/stocks-by-warehouse/fbo",
                "/v4/product/info/stocks",
            ]
        );
    }

    #[tokio::test]
    async fn stock_collection_propagates_an_fbs_failure_after_fbo_completed() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"products": [], "cursor": "", "has_next": false})),
            Err(OzonReportSourceError::Transport),
        ]))));

        assert_eq!(
            source.collect_stock_pages().await,
            Err(OzonReportSourceError::Transport)
        );
    }

    #[tokio::test]
    async fn source_normalizes_only_valid_read_only_pages() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"result":{"data":[{
                "dimensions":[{"id":"1"},{"id":"2026-08-16"}],
                "metrics":["1.00", 2]
            }]}})),
            Ok(json!({
                "products":[{"sku":1,"warehouse_id":11,"present":4,"reserved":1}],
                "cursor":"",
                "has_next":false
            })),
            Ok(json!({
                "products":[{
                    "sku":1,"warehouse_id":22,"present":5,"reserved":1,"free_stock":2
                }],
                "cursor":"",
                "has_next":false
            })),
            Ok(
                json!({"items":[{"product_id":1,"price":{"currency_code":"RUB","price":"2","old_price":"0"}}],"cursor":""}),
            ),
        ]))));
        let day = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        let sales = source.sales_page(day, day, 0).await.unwrap();
        assert_eq!(sales[0].ordered_units, 2);
        assert_eq!(sales[0].returned_units, None);
        assert_eq!(sales[0].cancelled_units, None);
        let stocks = source.collect_stock_pages().await.unwrap();
        assert_eq!(stocks.len(), 2);
        assert_eq!(stocks[0].warehouse_id, "fbo:11");
        assert_eq!(stocks[0].sellable_units, 3);
        assert_eq!(stocks[1].warehouse_id, "fbs:22");
        assert_eq!(stocks[1].sellable_units, 2);
        assert_eq!(
            source.collect_price_pages().await.unwrap()[0].price_minor,
            200
        );
    }

    #[tokio::test]
    async fn source_hides_transport_and_parse_details() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Err(OzonReportSourceError::Transport),
            Ok(json!({"items":"invalid"})),
            Ok(json!({"result":{"data":[{"dimensions":[],"metrics":[]}]}})),
        ]))));
        let day = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        assert_eq!(
            source.sales_page(day, day, 0).await,
            Err(OzonReportSourceError::Transport)
        );
        assert_eq!(
            source.collect_stock_pages().await,
            Err(OzonReportSourceError::InvalidStocksResponse)
        );
        assert_eq!(
            source.sales_page(day, day, 0).await.unwrap_err().code(),
            "invalid_sales_response"
        );
    }

    #[tokio::test]
    async fn concrete_transport_classifies_an_invalid_price_response() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([Ok(
            json!({"items": "invalid", "cursor": ""}),
        )]))));

        assert_eq!(
            source.collect_price_pages().await,
            Err(OzonReportSourceError::InvalidPricesResponse)
        );
    }

    #[tokio::test]
    async fn dynamic_transport_classifies_invalid_price_and_sales_responses() {
        let invalid_price = FixtureTransport(Mutex::new(VecDeque::from([Ok(json!({
            "items": "invalid",
            "cursor": ""
        }))])));
        let transport: &dyn OzonReportTransport = &invalid_price;
        let source = OzonReportSource::new(transport);
        assert_eq!(
            source.collect_price_pages().await,
            Err(OzonReportSourceError::InvalidPricesResponse)
        );

        let invalid_sales = FixtureTransport(Mutex::new(VecDeque::from([Ok(json!({
            "result": {"data": [{"dimensions": [], "metrics": []}]}
        }))])));
        let transport: &dyn OzonReportTransport = &invalid_sales;
        let source = OzonReportSource::new(transport);
        let day = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        assert_eq!(
            source.sales_page(day, day, 0).await.unwrap_err().code(),
            "invalid_sales_response"
        );
    }

    #[test]
    fn sales_shape_fingerprint_never_includes_response_values() {
        let shape = sales_response_shape(&json!({
            "result": {"data": [{
                "dimensions": [{"id": "secret-sku"}, {"id": "2026-08-16"}],
                "metrics": [123.45, null, 7]
            }]}
        }));
        assert_eq!(
            shape,
            "root=object,result=true,data=array:1,dimensions=2:[object,object],metrics=3:[number,null,number]"
        );
        assert!(!shape.contains("secret"));
        assert!(!shape.contains("123.45"));
        assert_eq!(
            sales_response_shape(&json!(true)),
            "root=bool,result=false,data=not_array"
        );
        assert_eq!(
            sales_response_shape(&json!({"result":{"data":[{"dimensions":true}]}})),
            "root=object,result=true,data=array:1,dimensions=0:[not_array],metrics=0:[not_array]"
        );
        assert_eq!(
            sales_response_shape(&json!({"result":{"data":[]}})),
            "root=object,result=true,data=array:0"
        );
        assert_eq!(json_kind(&json!("value")), "string");
        assert_eq!(json_kind(&json!([])), "array");
    }

    #[tokio::test]
    async fn warehouse_collection_follows_cursor_and_requires_a_valid_terminal_cursor() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"products":[],"cursor":"next","has_next":true})),
            Ok(json!({"products":[],"cursor":"","has_next":false})),
            Ok(json!({"products":[],"cursor":"","has_next":false})),
        ]))));
        assert_eq!(source.collect_stock_pages().await.unwrap().len(), 0);

        let invalid = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([Ok(
            json!({"products":[],"cursor":42,"has_next":true}),
        )]))));
        assert_eq!(
            invalid.collect_stock_pages().await,
            Err(OzonReportSourceError::InvalidStocksResponse)
        );

        let repeated = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"products":[],"cursor":"same","has_next":true})),
            Ok(json!({"products":[],"cursor":"same","has_next":true})),
        ]))));
        assert_eq!(
            repeated.collect_stock_pages().await,
            Err(OzonReportSourceError::InvalidStocksResponse)
        );
    }

    #[tokio::test]
    async fn price_collection_follows_valid_cursors_and_fails_closed_at_the_page_bound() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"items": [], "cursor": "next"})),
            Ok(json!({"items": [], "cursor": ""})),
        ]))));
        assert!(source.collect_price_pages().await.unwrap().is_empty());

        assert_eq!(
            next_cursor(
                &json!({"cursor": "next"}),
                OzonReportSourceError::InvalidPricesResponse
            ),
            Ok(Some("next".to_owned()))
        );
        assert_eq!(
            next_cursor(
                &json!({"cursor": "unsafe\n"}),
                OzonReportSourceError::InvalidPricesResponse
            ),
            Err(OzonReportSourceError::InvalidResponse)
        );
        assert_eq!(
            next_cursor(&json!({}), OzonReportSourceError::InvalidPricesResponse),
            Err(OzonReportSourceError::InvalidPricesResponse)
        );

        let missing_cursor =
            OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([Ok(
                json!({"items": []}),
            )]))));
        assert_eq!(
            missing_cursor.collect_price_pages().await,
            Err(OzonReportSourceError::InvalidPricesResponse)
        );

        let repeated = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"items": [], "cursor": "same"})),
            Ok(json!({"items": [], "cursor": "same"})),
        ]))));
        assert_eq!(
            repeated.collect_price_pages().await,
            Err(OzonReportSourceError::InvalidPricesResponse)
        );

        let bounded = OzonReportSource::new(FixtureTransport(Mutex::new(
            (0..MAX_PRODUCT_PAGES)
                .map(|page| Ok(json!({"items": [], "cursor": format!("next-{page}")})))
                .collect(),
        )));
        assert_eq!(
            bounded.collect_price_pages().await,
            Err(OzonReportSourceError::PaginationLimit)
        );
    }

    #[tokio::test]
    async fn required_facts_are_returned_only_after_all_sources_succeed() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"result":{"data":[]}})),
            Ok(json!({"accrual_types":[]})),
            Ok(json!({"accruals":[],"last_id":""})),
            Ok(json!({"products":[],"cursor":"","has_next":false})),
            Ok(json!({"products":[],"cursor":"","has_next":false})),
            Ok(json!({"items":[],"cursor":""})),
        ]))));
        let day = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        assert_eq!(
            source
                .collect_required_seller_facts(day, day)
                .await
                .unwrap(),
            OzonCollectedFacts {
                sales: vec![],
                finance: vec![],
                stocks: vec![],
                prices: vec![]
            }
        );
    }

    #[tokio::test]
    async fn complete_collection_builds_five_validated_snapshots_without_database_io() {
        let transport = FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"result":{"data":[]}})),
            Ok(json!({"accrual_types":[]})),
            Ok(json!({"accruals":[],"last_id":""})),
            Ok(json!({"products":[],"cursor":"","has_next":false})),
            Ok(json!({"products":[],"cursor":"","has_next":false})),
            Ok(json!({"items":[],"cursor":""})),
        ])));
        let as_of = Utc.with_ymd_and_hms(2026, 8, 16, 19, 0, 0).unwrap();
        let period_start = Utc.with_ymd_and_hms(2026, 8, 15, 19, 0, 0).unwrap();
        let snapshots = collect_complete_snapshots(
            &transport,
            vec![CollectedAdvertisingFact {
                business_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
                campaign_id: 7,
                sku: 1,
                impressions: 10,
                clicks: 1,
                spend_minor: 100,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
                basket_additions: 0,
                model_attributed_orders: 0,
                model_attributed_revenue_minor: 0,
                product_price_minor: 0,
                average_cpc_minor: None,
                cpm_minor: None,
                cpl_minor: None,
            }],
            "ozon".to_owned(),
            as_of,
            || {
                assert!(transport.0.lock().unwrap().is_empty());
                as_of
            },
            period_start,
            as_of,
            "test-1".to_owned(),
        )
        .await
        .unwrap();
        assert_eq!(snapshots.len(), 5);
    }

    #[tokio::test]
    async fn complete_collection_accepts_the_production_clock_after_sources_finish() {
        let transport = FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"result":{"data":[]}})),
            Ok(json!({"accrual_types":[]})),
            Ok(json!({"accruals":[],"last_id":""})),
            Ok(json!({"products":[],"cursor":"","has_next":false})),
            Ok(json!({"products":[],"cursor":"","has_next":false})),
            Ok(json!({"items":[],"cursor":""})),
        ])));
        let cutoff_at = Utc::now();
        let offset = crate::reporting::yekaterinburg_offset();
        let local_midnight = cutoff_at
            .with_timezone(&offset)
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let period_end = offset
            .from_local_datetime(&local_midnight)
            .single()
            .unwrap()
            .with_timezone(&Utc);
        let period_start = period_end - Duration::days(1);

        let snapshots = collect_complete_snapshots(
            &transport,
            Vec::new(),
            "ozon".to_owned(),
            cutoff_at,
            Utc::now,
            period_start,
            period_end,
            "test-1".to_owned(),
        )
        .await
        .unwrap();

        assert_eq!(snapshots.len(), 5);
        assert!(transport.0.lock().unwrap().is_empty());

        assert_eq!(
            collect_complete_snapshots(
                &transport,
                Vec::new(),
                "ozon".to_owned(),
                cutoff_at,
                Utc::now,
                period_end,
                period_end,
                "test-1".to_owned(),
            )
            .await,
            Err(OzonReportSourceError::InvalidSnapshotInput)
        );
        assert!(transport.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn complete_collection_propagates_a_required_seller_source_failure() {
        let transport = FixtureTransport(Mutex::new(VecDeque::from([Err(
            OzonReportSourceError::Transport,
        )])));
        let as_of = Utc.with_ymd_and_hms(2026, 8, 16, 19, 0, 0).unwrap();
        let period_start = Utc.with_ymd_and_hms(2026, 8, 15, 19, 0, 0).unwrap();

        assert_eq!(
            collect_complete_snapshots(
                &transport,
                Vec::new(),
                "ozon".to_owned(),
                as_of,
                Utc::now,
                period_start,
                as_of,
                "test-1".to_owned(),
            )
            .await,
            Err(OzonReportSourceError::Transport)
        );
    }

    #[tokio::test]
    async fn complete_collection_refuses_an_invalid_performance_fact_atomically() {
        let transport = FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"result":{"data":[]}})),
            Ok(json!({"accrual_types":[]})),
            Ok(json!({"accruals":[],"last_id":""})),
            Ok(json!({"products":[],"cursor":"","has_next":false})),
            Ok(json!({"products":[],"cursor":"","has_next":false})),
            Ok(json!({"items":[],"cursor":""})),
        ])));
        let as_of = Utc.with_ymd_and_hms(2026, 8, 16, 19, 0, 0).unwrap();
        let period_start = Utc.with_ymd_and_hms(2026, 8, 15, 19, 0, 0).unwrap();
        assert_eq!(
            collect_complete_snapshots(
                &transport,
                vec![CollectedAdvertisingFact {
                    business_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
                    campaign_id: 0,
                    sku: 1,
                    impressions: 0,
                    clicks: 0,
                    spend_minor: 0,
                    attributed_orders: 0,
                    attributed_revenue_minor: 0,
                    basket_additions: 0,
                    model_attributed_orders: 0,
                    model_attributed_revenue_minor: 0,
                    product_price_minor: 0,
                    average_cpc_minor: None,
                    cpm_minor: None,
                    cpl_minor: None,
                }],
                "ozon".to_owned(),
                as_of,
                || as_of,
                period_start,
                as_of,
                "test-1".to_owned(),
            )
            .await,
            Err(OzonReportSourceError::InvalidSnapshotInput)
        );
    }

    #[test]
    fn complete_facts_become_atomic_seller_and_performance_snapshots() {
        let as_of = Utc.with_ymd_and_hms(2026, 8, 16, 19, 0, 0).unwrap();
        let start = Utc.with_ymd_and_hms(2026, 8, 15, 19, 0, 0).unwrap();
        let facts = OzonCollectedFacts {
            sales: vec![CollectedSalesFact {
                business_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
                sku: 1,
                ordered_units: 1,
                operational_gmv_minor: 100,
                cancelled_units: Some(0),
                returned_units: Some(0),
            }],
            finance: vec![],
            stocks: vec![CollectedStockFact {
                sku: 1,
                warehouse_id: "fbo".to_owned(),
                sellable_units: 1,
            }],
            prices: vec![CollectedPriceFact {
                sku: 1,
                price_minor: 100,
                old_price_minor: None,
            }],
        };
        let seller_only = facts
            .clone()
            .into_snapshots(
                "ozon".to_owned(),
                as_of,
                as_of,
                start,
                as_of,
                "test-1".to_owned(),
            )
            .unwrap();
        assert_eq!(seller_only.len(), 4);
        assert_eq!(
            facts.clone().into_complete_snapshots_extended(
                Vec::new(),
                vec![CollectedAdvertisingExpenseFact {
                    business_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
                    campaign_id: 0,
                    money_spent_minor: 100,
                    bonus_spent_minor: 0,
                    prepayment_spent_minor: 100,
                }],
                "ozon".to_owned(),
                as_of,
                as_of,
                start,
                as_of,
                "test-1".to_owned(),
            ),
            Err(PostgresCollectorError::InvalidInput)
        );
        let complete = facts
            .into_complete_snapshots(
                vec![CollectedAdvertisingFact {
                    business_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
                    campaign_id: 7,
                    sku: 1,
                    impressions: 10,
                    clicks: 1,
                    spend_minor: 100,
                    attributed_orders: 0,
                    attributed_revenue_minor: 0,
                    basket_additions: 0,
                    model_attributed_orders: 0,
                    model_attributed_revenue_minor: 0,
                    product_price_minor: 0,
                    average_cpc_minor: None,
                    cpm_minor: None,
                    cpl_minor: None,
                }],
                "ozon".to_owned(),
                as_of,
                as_of,
                start,
                as_of,
                "test-1".to_owned(),
            )
            .unwrap();
        assert_eq!(complete.len(), 5);
    }

    #[test]
    fn utc_period_is_mapped_to_the_same_yekaterinburg_business_date() {
        let start = Utc.with_ymd_and_hms(2026, 8, 16, 19, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 8, 17, 19, 0, 0).unwrap();
        assert_eq!(
            report_business_dates(start, end).unwrap(),
            (
                NaiveDate::from_ymd_opt(2026, 8, 17).unwrap(),
                NaiveDate::from_ymd_opt(2026, 8, 17).unwrap()
            )
        );
        assert_eq!(
            report_business_dates(end, start),
            Err(OzonReportSourceError::InvalidSnapshotInput)
        );
    }

    #[test]
    fn invalid_fact_in_each_source_refuses_the_entire_snapshot_set() {
        let as_of = Utc.with_ymd_and_hms(2026, 8, 16, 19, 0, 0).unwrap();
        let start = Utc.with_ymd_and_hms(2026, 8, 15, 19, 0, 0).unwrap();
        let valid_sales = || CollectedSalesFact {
            business_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
            sku: 1,
            ordered_units: 1,
            operational_gmv_minor: 100,
            cancelled_units: Some(0),
            returned_units: Some(0),
        };
        let valid_stock = || CollectedStockFact {
            sku: 1,
            warehouse_id: "fbo".to_owned(),
            sellable_units: 1,
        };
        let invalid_sets = [
            OzonCollectedFacts {
                sales: vec![CollectedSalesFact {
                    sku: 0,
                    ..valid_sales()
                }],
                finance: vec![],
                stocks: vec![valid_stock()],
                prices: Vec::new(),
            },
            OzonCollectedFacts {
                sales: vec![valid_sales()],
                finance: vec![CollectedFinanceFact {
                    business_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
                    sku: Some(1),
                    category: FinanceCategory::Other,
                    amount_minor: 1,
                    line_count: 0,
                    unknown_type_count: 0,
                }],
                stocks: vec![valid_stock()],
                prices: Vec::new(),
            },
            OzonCollectedFacts {
                sales: vec![valid_sales()],
                finance: vec![],
                stocks: vec![CollectedStockFact {
                    warehouse_id: String::new(),
                    ..valid_stock()
                }],
                prices: Vec::new(),
            },
            OzonCollectedFacts {
                sales: vec![valid_sales()],
                finance: vec![],
                stocks: vec![valid_stock()],
                prices: vec![CollectedPriceFact {
                    sku: 0,
                    price_minor: 100,
                    old_price_minor: None,
                }],
            },
        ];
        for facts in invalid_sets {
            assert!(
                facts
                    .into_snapshots(
                        "ozon".to_owned(),
                        as_of,
                        as_of,
                        start,
                        as_of,
                        "test-1".to_owned(),
                    )
                    .is_err()
            );
        }
        assert_eq!(
            OzonReportSourceError::InvalidFinanceResponse.code(),
            "invalid_finance_response"
        );

        let valid_advertising = || CollectedAdvertisingFact {
            business_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
            campaign_id: 7,
            sku: 1,
            impressions: 10,
            clicks: 1,
            spend_minor: 100,
            attributed_orders: 0,
            attributed_revenue_minor: 0,
            basket_additions: 0,
            model_attributed_orders: 0,
            model_attributed_revenue_minor: 0,
            product_price_minor: 0,
            average_cpc_minor: None,
            cpm_minor: None,
            cpl_minor: None,
        };
        assert!(
            OzonCollectedFacts {
                sales: vec![valid_sales()],
                finance: vec![],
                stocks: vec![valid_stock()],
                prices: Vec::new(),
            }
            .into_complete_snapshots(
                vec![CollectedAdvertisingFact {
                    campaign_id: 0,
                    ..valid_advertising()
                }],
                "ozon".to_owned(),
                as_of,
                as_of,
                start,
                as_of,
                "test-1".to_owned(),
            )
            .is_err()
        );
        assert!(
            OzonCollectedFacts {
                sales: vec![CollectedSalesFact {
                    sku: 0,
                    ..valid_sales()
                }],
                finance: vec![],
                stocks: vec![valid_stock()],
                prices: Vec::new(),
            }
            .into_complete_snapshots(
                vec![valid_advertising()],
                "ozon".to_owned(),
                as_of,
                as_of,
                start,
                as_of,
                "test-1".to_owned(),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn pagination_limits_fail_closed_for_sales_and_cursor_sources() {
        let full_sales_page = || {
            json!({"result":{"data":(0..1_000).map(|index| json!({
                "dimensions":[{"id":index.to_string()},{"id":"2026-08-16"}],
                "metrics":["1", 1]
            })).collect::<Vec<_>>()}})
        };
        let sales = OzonReportSource::new(FixtureTransport(Mutex::new(
            std::iter::repeat_with(|| Ok(full_sales_page()))
                .take(MAX_SALES_PAGES)
                .collect(),
        )));
        let day = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        assert_eq!(
            sales.collect_sales_pages(day, day).await,
            Err(OzonReportSourceError::PaginationLimit)
        );

        let cursor = OzonReportSource::new(FixtureTransport(Mutex::new(
            (0..MAX_PRODUCT_PAGES)
                .map(|page| {
                    Ok(json!({
                        "products":[],
                        "cursor":format!("next-{page}"),
                        "has_next":true
                    }))
                })
                .collect(),
        )));
        assert_eq!(
            cursor.collect_stock_pages().await,
            Err(OzonReportSourceError::PaginationLimit)
        );
    }

    #[tokio::test]
    async fn sales_collection_stops_at_a_short_page() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([Ok(
            json!({"result":{"data":[]}}),
        )]))));
        let day = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        assert!(
            source
                .collect_sales_pages(day, day)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn client_transport_uses_the_hardened_client_and_exposes_only_safe_error_kind() {
        let client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            std::time::Duration::from_millis(1),
            BTreeMap::default(),
        )
        .unwrap();
        let transport = OzonClientReportTransport::new(client, StoreId::from("missing"));
        let error = transport
            .post(
                warehouse_stock_page_request("/v1/product/info/stocks-by-warehouse/fbo", None)
                    .unwrap(),
            )
            .await
            .expect_err("an unknown store must fail before network access");
        assert_eq!(error.code(), "missing_credentials");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 4_096];
            let _ = stream.read(&mut request).await.unwrap();
            let body = br#"{"result":"ok"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });
        let store = StoreId::from("configured");
        let client = OzonClient::new(
            base_url,
            std::time::Duration::from_secs(1),
            BTreeMap::from([(
                store.clone(),
                StoreCredentials {
                    client_id: "test-client".to_owned(),
                    api_key: "test-key".to_owned(),
                },
            )]),
        )
        .unwrap();
        let transport = OzonClientReportTransport::new(client, store);
        let value = transport
            .post(
                warehouse_stock_page_request("/v1/product/info/stocks-by-warehouse/fbo", None)
                    .unwrap(),
            )
            .await
            .expect("the hardened transport must pass through a bounded success response");
        assert_eq!(value["result"], "ok");
        server.await.unwrap();

        assert_eq!(
            OzonReportSourceError::Upstream(OzonErrorKind::Forbidden).code(),
            "forbidden"
        );
        for (error, code) in [
            (OzonReportSourceError::Transport, "transport_error"),
            (OzonReportSourceError::InvalidResponse, "invalid_response"),
            (
                OzonReportSourceError::InvalidSalesResponse {
                    shape: "bounded".to_owned(),
                },
                "invalid_sales_response",
            ),
            (
                OzonReportSourceError::InvalidStocksResponse,
                "invalid_stocks_response",
            ),
            (
                OzonReportSourceError::InvalidPricesResponse,
                "invalid_prices_response",
            ),
            (
                OzonReportSourceError::InvalidSnapshotInput,
                "invalid_snapshot_input",
            ),
            (OzonReportSourceError::PaginationLimit, "pagination_limit"),
        ] {
            assert_eq!(error.code(), code);
            assert_eq!(
                error.diagnostic(),
                (code == "invalid_sales_response").then_some("bounded")
            );
        }
    }
}
