//! Bounded read-only Ozon report source.
//!
//! This module keeps collection transport-agnostic for tests and delegates the
//! explicit canary runtime to `OzonClient`. Every request is first built by the
//! exact contract in `ozon_adapter` and every response is normalized before it
//! can reach report persistence.

use std::{future::Future, pin::Pin};

use chrono::{DateTime, Duration, FixedOffset, NaiveDate, Utc};
use serde_json::Value;
use thiserror::Error;

use crate::{
    config::StoreId,
    ozon::{OzonClient, OzonErrorKind},
};

use super::{
    ozon_adapter::{
        OzonReportParseError, OzonReportRequest, next_warehouse_stock_cursor, parse_price_page,
        parse_sales_page, parse_warehouse_stock_page, product_page_request, sales_request,
        warehouse_stock_page_request,
    },
    postgres_collector::{
        CollectedAdvertisingFact, CollectedFacts, CollectedPriceFact, CollectedSalesFact,
        CollectedSnapshot, CollectedStockFact, PostgresCollectorError,
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

// `/v1/analytics/data` is limited by Ozon to one request per minute. Ten
// bounded pages cover up to 9,999 rows and keep a complete collection inside
// the operator dry-run deadline; a tenth full page fails closed instead of
// starting an unbounded multi-hour backfill.
const MAX_SALES_PAGES: usize = 10;
// At 100 products/page this still accommodates 10,000 products, while the
// manual dry-run's absolute deadline bounds the total request time.
const MAX_PRODUCT_PAGES: usize = 100;

/// Collects the three Seller sources and atomically publishes them together
/// with a separately verified Performance SKU snapshot.
#[allow(clippy::too_many_arguments)]
pub async fn collect_complete_snapshots(
    transport: &dyn OzonReportTransport,
    advertising: Vec<CollectedAdvertisingFact>,
    account_id: String,
    cutoff_at: DateTime<Utc>,
    source_as_of: DateTime<Utc>,
    sales_period_start: DateTime<Utc>,
    sales_period_end: DateTime<Utc>,
    collector_version: String,
) -> Result<Vec<CollectedSnapshot>, OzonReportSourceError> {
    let source = OzonReportSource::new(transport);
    let (date_from, date_to) = report_business_dates(sales_period_start, sales_period_end)?;
    let facts = source
        .collect_required_seller_facts(date_from, date_to)
        .await?;
    let snapshots = facts
        .into_complete_snapshots(
            advertising,
            account_id,
            cutoff_at,
            source_as_of,
            sales_period_start,
            sales_period_end,
            collector_version,
        )
        .map_err(|_| OzonReportSourceError::InvalidSnapshotInput)?;
    Ok(snapshots)
}

fn report_business_dates(
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
) -> Result<(NaiveDate, NaiveDate), OzonReportSourceError> {
    if period_end <= period_start {
        return Err(OzonReportSourceError::InvalidSnapshotInput);
    }
    let offset =
        FixedOffset::east_opt(5 * 60 * 60).expect("the fixed Yekaterinburg UTC offset is valid");
    let inclusive_end = period_end
        .checked_sub_signed(Duration::nanoseconds(1))
        .ok_or(OzonReportSourceError::InvalidSnapshotInput)?;
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
    pub fn new(client: OzonClient, store: StoreId) -> Self {
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
                    .post(&self.store, path, request.payload.clone())
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
            Err(kind) => return Err(OzonReportSourceError::Upstream(kind)),
        }
    }
}

pub struct OzonReportSource<T> {
    transport: T,
}

/// Complete in-memory Ozon Seller input for one report cutoff. It is only
/// returned after all requested sources have succeeded, so callers can pass
/// it to the transactional PostgreSQL writer without mixing partial data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OzonCollectedFacts {
    pub sales: Vec<CollectedSalesFact>,
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
        )?;
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
        )?;
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
        )?;
        Ok(vec![sales, stocks, prices])
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
        )?;
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

impl<T> OzonReportSource<T> {
    pub fn new(transport: T) -> Self {
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
    #[error("Ozon daily-report snapshot input is invalid")]
    InvalidSnapshotInput,
    #[error("Ozon daily-report source pagination exceeded its fixed bound")]
    PaginationLimit,
}

impl OzonReportSourceError {
    /// A stable, non-sensitive diagnostic code suitable for operator logs.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Upstream(kind) => kind.code(),
            Self::Transport => "transport_error",
            Self::InvalidResponse => "invalid_response",
            Self::InvalidSalesResponse { .. } => "invalid_sales_response",
            Self::InvalidStocksResponse => "invalid_stocks_response",
            Self::InvalidPricesResponse => "invalid_prices_response",
            Self::InvalidSnapshotInput => "invalid_snapshot_input",
            Self::PaginationLimit => "pagination_limit",
        }
    }

    /// A value-free, bounded structural fingerprint for the one sales parse
    /// failure that needs operator investigation. It never includes an Ozon
    /// response value, identifier, amount, name, or credential.
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
        let sales = self.collect_sales_pages(date_from, date_to).await?;
        let stocks = self.collect_stock_pages().await?;
        let prices = self.collect_price_pages().await?;
        Ok(OzonCollectedFacts {
            sales,
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
        parse_sales_page(&response).map_err(|_| OzonReportSourceError::InvalidSalesResponse {
            shape: sales_response_shape(&response),
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

    /// Collects real warehouse-granular FBO and FBS stock pages.
    ///
    /// Both sources must complete. Falling back to `/v4/product/info/stocks`
    /// would silently collapse all physical warehouses into two fulfillment
    /// labels and make OOS/DaysCover analytics misleading.
    pub async fn collect_stock_pages(
        &self,
    ) -> Result<Vec<CollectedStockFact>, OzonReportSourceError> {
        let mut facts = self
            .collect_warehouse_stock_pages("/v1/product/info/stocks-by-warehouse/fbo", "fbo")
            .await?;
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
        let mut facts = Vec::new();
        for _ in 0..MAX_PRODUCT_PAGES {
            let request = product_page_request(path, cursor.as_deref())
                .map_err(|_| OzonReportSourceError::InvalidResponse)?;
            let response = self.transport.post(request).await?;
            facts.extend(parse(&response).map_err(|_| invalid_response.clone())?);
            cursor = next_cursor(&response, invalid_response.clone())?;
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
            data.map_or("not_array".to_owned(), |rows| format!(
                "array:{}",
                rows.len()
            ))
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

fn json_kind(value: &Value) -> &'static str {
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

    /// A local admission refusal never reached Ozon, so re-offering the page
    /// cannot duplicate a marketplace request. Before this, one transient
    /// burst of contention aborted the whole account run and discarded every
    /// page already collected, because snapshots publish atomically.
    #[tokio::test(start_paused = true)]
    async fn a_transient_local_overload_does_not_discard_the_run() {
        let calls = std::cell::Cell::new(0_usize);
        let value = retry_local_overload("/v3/posting/fbo/list", || async {
            calls.set(calls.get() + 1);
            if calls.get() < 3 {
                Err(OzonErrorKind::Overloaded)
            } else {
                Ok(serde_json::json!({"result": "ok"}))
            }
        })
        .await
        .expect("a transient local refusal must not fail the page");
        assert_eq!(value["result"], "ok");
        assert_eq!(calls.get(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_sustained_local_overload_still_fails_the_page() {
        let calls = std::cell::Cell::new(0_usize);
        let error = retry_local_overload("/v3/posting/fbo/list", || async {
            calls.set(calls.get() + 1);
            Err(OzonErrorKind::Overloaded)
        })
        .await
        .expect_err("the retry budget is bounded");
        assert!(matches!(
            error,
            OzonReportSourceError::Upstream(OzonErrorKind::Overloaded)
        ));
        assert_eq!(calls.get(), OVERLOAD_RETRY_ATTEMPTS);
    }

    /// Only the local refusal is retried. An upstream failure has already
    /// reached Ozon, so repeating it would be a second marketplace request.
    #[tokio::test(start_paused = true)]
    async fn an_upstream_failure_is_never_retried() {
        let calls = std::cell::Cell::new(0_usize);
        let error = retry_local_overload("/v3/posting/fbo/list", || async {
            calls.set(calls.get() + 1);
            Err(OzonErrorKind::RateLimited)
        })
        .await
        .expect_err("an upstream failure is surfaced");
        assert!(matches!(
            error,
            OzonReportSourceError::Upstream(OzonErrorKind::RateLimited)
        ));
        assert_eq!(calls.get(), 1);
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
                "metrics":["1.00", 2, 1, 1]
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
        assert_eq!(sales[0].returned_units, Some(1));
        assert_eq!(sales[0].cancelled_units, Some(1));
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
        assert!(matches!(
            source.sales_page(day, day, 0).await,
            Err(OzonReportSourceError::InvalidSalesResponse { .. })
        ));
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
        assert!(matches!(
            source.sales_page(day, day, 0).await,
            Err(OzonReportSourceError::InvalidSalesResponse { .. })
        ));
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

        let bounded = OzonReportSource::new(FixtureTransport(Mutex::new(
            std::iter::repeat_with(|| Ok(json!({"items": [], "cursor": "next"})))
                .take(MAX_PRODUCT_PAGES)
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
                stocks: vec![],
                prices: vec![]
            }
        );
    }

    #[tokio::test]
    async fn complete_collection_builds_four_validated_snapshots_without_database_io() {
        let transport = FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"result":{"data":[]}})),
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
            }],
            "ozon".to_owned(),
            as_of,
            as_of,
            period_start,
            as_of,
            "test-1".to_owned(),
        )
        .await
        .unwrap();
        assert_eq!(snapshots.len(), 4);
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
                as_of,
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
                }],
                "ozon".to_owned(),
                as_of,
                as_of,
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
        assert_eq!(seller_only.len(), 3);
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
                }],
                "ozon".to_owned(),
                as_of,
                as_of,
                start,
                as_of,
                "test-1".to_owned(),
            )
            .unwrap();
        assert_eq!(complete.len(), 4);
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
                stocks: vec![valid_stock()],
                prices: Vec::new(),
            },
            OzonCollectedFacts {
                sales: vec![valid_sales()],
                stocks: vec![CollectedStockFact {
                    warehouse_id: String::new(),
                    ..valid_stock()
                }],
                prices: Vec::new(),
            },
            OzonCollectedFacts {
                sales: vec![valid_sales()],
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

        let valid_advertising = || CollectedAdvertisingFact {
            business_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
            campaign_id: 7,
            sku: 1,
            impressions: 10,
            clicks: 1,
            spend_minor: 100,
            attributed_orders: 0,
            attributed_revenue_minor: 0,
        };
        assert!(
            OzonCollectedFacts {
                sales: vec![valid_sales()],
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
                "metrics":["1", 1, 0, 0]
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
            std::iter::repeat_with(|| Ok(json!({"products":[],"cursor":"next","has_next":true})))
                .take(MAX_PRODUCT_PAGES)
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
            Default::default(),
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
