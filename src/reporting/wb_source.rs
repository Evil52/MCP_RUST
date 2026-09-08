//! Bounded read-only Wildberries source for daily reports.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::time::{Instant, sleep};

use crate::wb::{WbClient, WbError, WbErrorKind};

use super::{
    checkpoint::{CheckpointError, Checkpoints, checkpointed},
    postgres_collector::{
        CollectedAdvertisingFact, CollectedFacts, CollectedPriceFact, CollectedSalesFact,
        CollectedSnapshot, CollectedStockFact, PostgresCollectorError,
    },
    snapshot::{Marketplace, SnapshotStatus},
    wb_adapter::{
        WbReportParseError, parse_campaign_ids, parse_price_page, parse_promotion_stats,
        parse_sales_page, parse_stock_page,
    },
};

const PAGE_SIZE: usize = 1_000;
const PAGE_SIZE_U32: u32 = 1_000;
const MAX_PAGES: usize = 25;
const CAMPAIGNS_PER_REQUEST: usize = 50;
const PROMOTION_STATS_ADMISSION_BUDGET: Duration = Duration::from_secs(60);

pub trait WbReportTransport: Send + Sync {
    fn sales_page<'a>(
        &'a self,
        start: NaiveDate,
        end: NaiveDate,
        limit: u32,
        offset: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>>;

    fn stock_page<'a>(
        &'a self,
        limit: u32,
        offset: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>>;

    fn price_page<'a>(
        &'a self,
        limit: u32,
        offset: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>>;

    fn campaigns<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>>;

    fn promotion_stats<'a>(
        &'a self,
        ids: Vec<u64>,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>>;
}

#[derive(Clone)]
pub struct WbClientReportTransport {
    client: WbClient,
    account_id: String,
}

impl WbClientReportTransport {
    #[must_use]
    pub const fn new(client: WbClient, account_id: String) -> Self {
        Self { client, account_id }
    }
}

/// Only background fullstats collection queues a rejected local departure.
/// The admitted HTTP request retains `WbClient`'s own bounded retry policy;
/// this helper never repeats a vendor failure or holds a network permit.
/// The collector additionally bounds the entire account run to twelve minutes.
async fn wait_for_local_admission<F, Request>(
    deadline: Instant,
    mut request: F,
) -> Result<Value, WbError>
where
    F: FnMut() -> Request,
    Request: Future<Output = Result<Value, WbError>>,
{
    loop {
        if Instant::now() >= deadline {
            return Err(WbError::DeadlineExceeded);
        }
        match request().await {
            Err(error @ WbError::LocalRateLimited { retry_after }) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if retry_after.is_zero() || retry_after >= remaining {
                    return Err(error);
                }
                tracing::info!(
                    source = "advertising",
                    admission = "local_wait",
                    retry_after_ms = ?retry_after.as_millis(),
                    "WB background report waiting for local quota"
                );
                sleep(retry_after).await;
                // Scheduling may wake us after the strict admission deadline.
                // Preserve the local refusal and never send a late request.
                if Instant::now() >= deadline {
                    return Err(error);
                }
            }
            outcome => return outcome,
        }
    }
}

fn wb_source_failure(error: &WbError) -> WbReportSourceError {
    match error {
        WbError::RateLimited {
            retry_after: Some(delay),
            ..
        }
        | WbError::LocalRateLimited { retry_after: delay } => WbReportSourceError::RetryAfter {
            seconds: super::checkpoint::delay_seconds(*delay),
        },
        _ => WbReportSourceError::Upstream(error.kind()),
    }
}

fn promotion_stats_failure(error: &WbError) -> WbReportSourceError {
    let failure = match error {
        WbError::LocalRateLimited { .. } => "local_admission_exhausted",
        WbError::RateLimited { .. } => "vendor_rate_limited",
        _ => "upstream_failure",
    };
    tracing::warn!(
        source = "advertising",
        failure,
        error_code = error.kind().code(),
        "WB background report source failed"
    );
    wb_source_failure(error)
}

impl WbReportTransport for WbClientReportTransport {
    fn sales_page<'a>(
        &'a self,
        start: NaiveDate,
        end: NaiveDate,
        limit: u32,
        offset: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .sales_funnel(
                    &self.account_id,
                    json!({
                        "selectedPeriod": {
                            "start": start.format("%Y-%m-%d").to_string(),
                            "end": end.format("%Y-%m-%d").to_string(),
                        },
                        "nmIds": [],
                        "brandNames": [],
                        "subjectIds": [],
                        "tagIds": [],
                        "skipDeletedNm": false,
                        "limit": limit,
                        "offset": offset,
                    }),
                )
                .await
                .map_err(|error| wb_source_failure(&error))
        })
    }

    fn stock_page<'a>(
        &'a self,
        limit: u32,
        offset: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .warehouse_stocks(
                    &self.account_id,
                    json!({"nmIds": [], "chrtIds": [], "limit": limit, "offset": offset}),
                )
                .await
                .map_err(|error| wb_source_failure(&error))
        })
    }

    fn price_page<'a>(
        &'a self,
        limit: u32,
        offset: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .product_prices(&self.account_id, None, limit, offset)
                .await
                .map_err(|error| wb_source_failure(&error))
        })
    }

    fn campaigns<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .promotion_campaigns(&self.account_id)
                .await
                .map_err(|error| wb_source_failure(&error))
        })
    }

    fn promotion_stats<'a>(
        &'a self,
        ids: Vec<u64>,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            let start = start.format("%Y-%m-%d").to_string();
            let end = end.format("%Y-%m-%d").to_string();
            wait_for_local_admission(Instant::now() + PROMOTION_STATS_ADMISSION_BUDGET, || {
                self.client.promotion_stats(
                    &self.account_id,
                    ids.clone(),
                    start.clone(),
                    end.clone(),
                )
            })
            .await
            .map_err(|error| promotion_stats_failure(&error))
        })
    }
}

pub struct WbReportSource {
    checkpoints: Checkpoints,
    transport: Arc<dyn WbReportTransport>,
}

impl WbReportSource {
    pub fn new(transport: impl WbReportTransport + 'static) -> Self {
        Self {
            transport: Arc::new(transport),
            checkpoints: None,
        }
    }
    #[must_use]
    pub fn with_checkpoints(mut self, checkpoints: Checkpoints) -> Self {
        self.checkpoints = checkpoints;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WbCollectedFacts {
    pub sales: Vec<CollectedSalesFact>,
    pub advertising: Vec<CollectedAdvertisingFact>,
    pub stocks: Vec<CollectedStockFact>,
    pub prices: Vec<CollectedPriceFact>,
}

impl WbCollectedFacts {
    #[allow(clippy::too_many_arguments)]
    pub fn into_snapshots(
        self,
        account_id: &str,
        cutoff_at: DateTime<Utc>,
        source_as_of: DateTime<Utc>,
        period_start: DateTime<Utc>,
        period_end: DateTime<Utc>,
        collector_version: &str,
    ) -> Result<Vec<CollectedSnapshot>, PostgresCollectorError> {
        let period = |facts| {
            CollectedSnapshot::new(
                account_id.to_owned(),
                Marketplace::Wildberries,
                cutoff_at,
                source_as_of,
                period_start,
                period_end,
                SnapshotStatus::Succeeded,
                true,
                collector_version.to_owned(),
                facts,
            )
        };
        let point = |facts| {
            CollectedSnapshot::new(
                account_id.to_owned(),
                Marketplace::Wildberries,
                cutoff_at,
                source_as_of,
                source_as_of,
                source_as_of,
                SnapshotStatus::Succeeded,
                true,
                collector_version.to_owned(),
                facts,
            )
        };
        Ok(vec![
            period(CollectedFacts::Sales(self.sales))?,
            period(CollectedFacts::Advertising(self.advertising))?,
            point(CollectedFacts::Stocks(self.stocks))?,
            point(CollectedFacts::Prices(self.prices))?,
        ])
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum WbReportSourceError {
    #[error("collection quota requires a pause")]
    RetryAfter { seconds: u64 },
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error("Wildberries daily-report source request failed")]
    Upstream(WbErrorKind),
    #[error("Wildberries daily-report source response is invalid")]
    InvalidResponse,
    #[error("Wildberries sales response is invalid")]
    InvalidSalesResponse,
    #[error("Wildberries stock response is invalid")]
    InvalidStockResponse,
    #[error("Wildberries price response is invalid")]
    InvalidPriceResponse,
    #[error("Wildberries campaign response is invalid")]
    InvalidCampaignResponse,
    #[error("Wildberries campaign inventory exceeds the bounded collection capacity")]
    CampaignInventoryLimit,
    #[error("Wildberries promotion statistics response is invalid")]
    InvalidPromotionResponse,
    #[error("Wildberries daily-report source pagination exceeded its fixed bound")]
    PaginationLimit,
    #[error("Wildberries daily-report snapshot input is invalid")]
    InvalidSnapshotInput,
}

impl WbReportSourceError {
    #[must_use]
    pub const fn failure(&self) -> super::source_collection::SourceFailure {
        super::source_collection::SourceFailure {
            code: self.code(),
            retry_after: match self {
                Self::RetryAfter { seconds } => Some(*seconds),
                _ => None,
            },
        }
    }
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::RetryAfter { .. } => "rate_limited",
            Self::Checkpoint(error) => error.code(),
            Self::Upstream(kind) => kind.code(),
            Self::InvalidResponse => "invalid_response",
            Self::InvalidSalesResponse => "invalid_sales_response",
            Self::InvalidStockResponse => "invalid_stock_response",
            Self::InvalidPriceResponse => "invalid_price_response",
            Self::InvalidCampaignResponse => "invalid_campaign_response",
            Self::CampaignInventoryLimit => "campaign_inventory_limit",
            Self::InvalidPromotionResponse => "invalid_promotion_response",
            Self::PaginationLimit => "pagination_limit",
            Self::InvalidSnapshotInput => "invalid_snapshot_input",
        }
    }
}

impl From<WbReportParseError> for WbReportSourceError {
    fn from(_: WbReportParseError) -> Self {
        Self::InvalidResponse
    }
}

fn log_source_completed(source: &'static str, facts: usize) {
    tracing::info!(source, facts, "WB source completed");
}

impl WbReportSource {
    pub async fn collect(&self, date: NaiveDate) -> Result<WbCollectedFacts, WbReportSourceError> {
        let advertising = self.collect_advertising(date).await?;
        tracing::info!(source = "sales", "collecting WB daily-report source");
        let sales = self.collect_sales_pages(date).await?;
        log_source_completed("sales", sales.len());
        tracing::info!(source = "stocks", "collecting WB daily-report source");
        let stocks = self.collect_stock_pages().await?;
        log_source_completed("stocks", stocks.len());
        tracing::info!(source = "prices", "collecting WB daily-report source");
        let prices = self.collect_price_pages().await?;
        log_source_completed("prices", prices.len());
        Ok(WbCollectedFacts {
            sales,
            advertising,
            stocks,
            prices,
        })
    }

    pub async fn collect_advertising(
        &self,
        date: NaiveDate,
    ) -> Result<Vec<CollectedAdvertisingFact>, WbReportSourceError> {
        tracing::info!(source = "campaigns", "collecting WB daily-report source");
        let ids = checkpointed(&self.checkpoints, json!(["wb_campaigns", date]), || async {
            let response = self.transport.campaigns().await?;
            let parsed = if self.checkpoints.is_some() {
                super::wb_adapter::parse_campaign_ids_for_checkpointed_collection(&response)
            } else {
                parse_campaign_ids(&response)
            };
            parsed.map_err(|error| {
                tracing::warn!(
                    source = "campaigns",
                    parse_error = ?error,
                    "WB campaign inventory could not be collected"
                );
                if error == WbReportParseError::TooManyRows {
                    WbReportSourceError::CampaignInventoryLimit
                } else {
                    WbReportSourceError::InvalidCampaignResponse
                }
            })
        })
        .await?;
        log_source_completed("campaigns", ids.len());
        let mut advertising = Vec::new();
        for chunk in ids.chunks(CAMPAIGNS_PER_REQUEST) {
            let rows: Vec<CollectedAdvertisingFact> = checkpointed(
                &self.checkpoints,
                json!(["wb_stats", date, chunk]),
                || async {
                    parse_promotion_stats(
                        &self
                            .transport
                            .promotion_stats(chunk.to_vec(), date, date)
                            .await?,
                    )
                    .map_err(|_| WbReportSourceError::InvalidPromotionResponse)
                },
            )
            .await?;
            if rows
                .iter()
                .any(|row| row.business_date != date || !chunk.contains(&row.campaign_id))
            {
                return Err(WbReportSourceError::InvalidPromotionResponse);
            }
            advertising.extend(rows);
            if advertising.len() > 25_000 {
                return Err(WbReportSourceError::PaginationLimit);
            }
        }
        log_source_completed("advertising", advertising.len());
        Ok(advertising)
    }

    pub async fn collect_sales_pages(
        &self,
        date: NaiveDate,
    ) -> Result<Vec<CollectedSalesFact>, WbReportSourceError> {
        self.collect_sales_pages_with_limit(date, MAX_PAGES).await
    }

    async fn collect_sales_pages_with_limit(
        &self,
        date: NaiveDate,
        max_pages: usize,
    ) -> Result<Vec<CollectedSalesFact>, WbReportSourceError> {
        let mut facts = Vec::new();
        for page in 0..max_pages {
            let offset = page_offset(page)?;
            let (rows, source_rows) = checkpointed(
                &self.checkpoints,
                json!(["wb_sales", date, offset]),
                || async {
                    parse_sales_page(
                        &self
                            .transport
                            .sales_page(date, date, PAGE_SIZE_U32, offset)
                            .await?,
                    )
                    .map_err(|_| WbReportSourceError::InvalidSalesResponse)
                },
            )
            .await?;
            if rows.iter().any(|row| row.business_date != date) {
                return Err(WbReportSourceError::InvalidSalesResponse);
            }
            facts.extend(rows);
            if source_rows < PAGE_SIZE {
                return Ok(facts);
            }
        }
        Err(WbReportSourceError::PaginationLimit)
    }

    pub async fn collect_stock_pages(
        &self,
    ) -> Result<Vec<CollectedStockFact>, WbReportSourceError> {
        self.collect_stock_pages_with_limit(MAX_PAGES).await
    }

    async fn collect_stock_pages_with_limit(
        &self,
        max_pages: usize,
    ) -> Result<Vec<CollectedStockFact>, WbReportSourceError> {
        let mut facts = Vec::new();
        for page in 0..max_pages {
            let offset = page_offset(page)?;
            let (rows, source_rows) =
                checkpointed(&self.checkpoints, json!(["wb_stock", offset]), || async {
                    parse_stock_page(&self.transport.stock_page(PAGE_SIZE_U32, offset).await?)
                        .map_err(|_| WbReportSourceError::InvalidStockResponse)
                })
                .await?;
            // Multiple chrt rows can normalize into one SKU/warehouse fact.
            // Only the raw response count proves that the page was short.
            let complete = source_rows < PAGE_SIZE;
            facts.extend(rows);
            if complete {
                return Ok(facts);
            }
        }
        Err(WbReportSourceError::PaginationLimit)
    }

    pub async fn collect_price_pages(
        &self,
    ) -> Result<Vec<CollectedPriceFact>, WbReportSourceError> {
        self.collect_price_pages_with_limit(MAX_PAGES).await
    }

    async fn collect_price_pages_with_limit(
        &self,
        max_pages: usize,
    ) -> Result<Vec<CollectedPriceFact>, WbReportSourceError> {
        let mut facts = Vec::new();
        for page in 0..max_pages {
            let offset = page_offset(page)?;
            let (rows, source_rows) =
                checkpointed(&self.checkpoints, json!(["wb_price", offset]), || async {
                    parse_price_page(&self.transport.price_page(PAGE_SIZE_U32, offset).await?)
                        .map_err(|_| WbReportSourceError::InvalidPriceResponse)
                })
                .await?;
            let complete = source_rows < PAGE_SIZE;
            facts.extend(rows);
            if complete {
                return Ok(facts);
            }
        }
        Err(WbReportSourceError::PaginationLimit)
    }
}

fn page_offset(page: usize) -> Result<u32, WbReportSourceError> {
    u32::try_from(page)
        .ok()
        .and_then(|page| page.checked_mul(PAGE_SIZE_U32))
        .ok_or(WbReportSourceError::PaginationLimit)
}

#[cfg(test)]
mod tests {
    mod admission;

    use std::{
        collections::BTreeMap,
        collections::VecDeque,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::{test_support::mock_http, wb::WbCredentials};

    #[derive(Clone)]
    struct FixtureTransport {
        stocks: Arc<Mutex<VecDeque<Value>>>,
        prices: Arc<Mutex<VecDeque<Value>>>,
        campaign_ids: Value,
        stats: Arc<Mutex<VecDeque<Result<Value, WbReportSourceError>>>>,
        requested_stats: Arc<Mutex<Vec<Vec<u64>>>>,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    #[tokio::test]
    async fn large_ad_inventory_resumes_one_chunk_without_repeating_campaigns() {
        use crate::reporting::checkpoint::tests::{MemoryPages, journal};
        let mut fixture = FixtureTransport::complete();
        fixture.campaign_ids = json!({"adverts":[{"status":9,"advert_list":(1..=2282).map(|id|json!({"advertId":id})).collect::<Vec<_>>()}]});
        fixture.stats.lock().unwrap().clear();
        fixture
            .stats
            .lock()
            .unwrap()
            .extend((0..46).map(|_| Ok(Value::Null)));
        let pages = MemoryPages::default();
        let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        for quantum in 0..47 {
            let source = WbReportSource::new(fixture.clone()).with_checkpoints(journal(&pages));
            let result = source.collect_advertising(date).await;
            if quantum < 46 {
                assert_eq!(
                    result,
                    Err(WbReportSourceError::Checkpoint(CheckpointError::Deferred))
                );
            } else {
                assert!(result.unwrap().is_empty());
            }
        }
        assert_eq!(fixture.requested_stats.lock().unwrap().len(), 46);
        let ids: Vec<_> = fixture
            .requested_stats
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .copied()
            .collect();
        assert_eq!(ids, (1..=2282).collect::<Vec<_>>());
        assert_eq!(
            fixture
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|&&name| name == "campaigns")
                .count(),
            1
        );
    }

    struct SalesFixtureTransport {
        pages: Mutex<VecDeque<Value>>,
    }

    impl WbReportTransport for SalesFixtureTransport {
        fn sales_page<'a>(
            &'a self,
            _start: NaiveDate,
            _end: NaiveDate,
            _limit: u32,
            _offset: u32,
        ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
            Box::pin(async {
                self.pages
                    .lock()
                    .unwrap()
                    .pop_front()
                    .ok_or(WbReportSourceError::InvalidResponse)
            })
        }

        fn stock_page<'a>(
            &'a self,
            _limit: u32,
            _offset: u32,
        ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
            Box::pin(async { Err(WbReportSourceError::InvalidResponse) })
        }

        fn price_page<'a>(
            &'a self,
            _limit: u32,
            _offset: u32,
        ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
            Box::pin(async { Err(WbReportSourceError::InvalidResponse) })
        }

        fn campaigns<'a>(
            &'a self,
        ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
            Box::pin(async { Err(WbReportSourceError::InvalidResponse) })
        }

        fn promotion_stats<'a>(
            &'a self,
            _ids: Vec<u64>,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
            Box::pin(async { Err(WbReportSourceError::InvalidResponse) })
        }
    }

    impl FixtureTransport {
        fn complete() -> Self {
            Self {
                stocks: Arc::new(Mutex::new(VecDeque::from([json!({"data":{"items":[
                    {"nmId":1,"warehouseId":2,"quantity":3}
                ]}})]))),
                prices: Arc::new(Mutex::new(VecDeque::from([json!({"data":{"listGoods":[{
                    "nmID":1,"currencyIsoCode4217":"RUB",
                    "sizes":[{"price":100,"discountedPrice":90}]
                }]}})]))),
                campaign_ids: json!({"adverts":[{"status":9,"advert_list":[{"advertId":4}]}]}),
                stats: Arc::new(Mutex::new(VecDeque::from([Ok(
                    json!([{"advertId":4,"stats":[{
                        "date":"2026-08-17","nm_id":1,"views":10,"clicks":1,"sum":2,
                        "orders":1,"sum_price":90
                    }]}]),
                )]))),
                requested_stats: Arc::new(Mutex::new(Vec::new())),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl WbReportTransport for FixtureTransport {
        fn sales_page<'a>(
            &'a self,
            start: NaiveDate,
            end: NaiveDate,
            _limit: u32,
            _offset: u32,
        ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
            Box::pin(async move {
                self.calls.lock().unwrap().push("sales");
                Ok(json!({"data":{"currency":"RUB","products":[{
                    "product":{"nmId":1},"statistic":{"selected":{
                        "period":{
                            "start":start.format("%Y-%m-%d").to_string(),
                            "end":end.format("%Y-%m-%d").to_string()
                        },
                        "orderCount":2,"orderSum":180,"cancelCount":0
                    }}
                }]}}))
            })
        }

        fn stock_page<'a>(
            &'a self,
            _limit: u32,
            _offset: u32,
        ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
            Box::pin(async {
                self.calls.lock().unwrap().push("stocks");
                self.stocks
                    .lock()
                    .unwrap()
                    .pop_front()
                    .ok_or(WbReportSourceError::InvalidResponse)
            })
        }

        fn price_page<'a>(
            &'a self,
            _limit: u32,
            _offset: u32,
        ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
            Box::pin(async {
                self.calls.lock().unwrap().push("prices");
                self.prices
                    .lock()
                    .unwrap()
                    .pop_front()
                    .ok_or(WbReportSourceError::InvalidResponse)
            })
        }

        fn campaigns<'a>(
            &'a self,
        ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
            Box::pin(async {
                self.calls.lock().unwrap().push("campaigns");
                Ok(self.campaign_ids.clone())
            })
        }

        fn promotion_stats<'a>(
            &'a self,
            ids: Vec<u64>,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
            Box::pin(async {
                self.calls.lock().unwrap().push("advertising");
                self.requested_stats.lock().unwrap().push(ids);
                self.stats
                    .lock()
                    .unwrap()
                    .pop_front()
                    .ok_or(WbReportSourceError::InvalidResponse)?
            })
        }
    }

    #[tokio::test]
    async fn complete_fixture_becomes_exact_four_source_snapshot_set() {
        let source = WbReportSource::new(FixtureTransport::complete());
        let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        let facts = source.collect(date).await.unwrap();
        assert_eq!(
            (
                facts.sales.len(),
                facts.advertising.len(),
                facts.stocks.len(),
                facts.prices.len()
            ),
            (1, 1, 1, 1)
        );
        let cutoff = Utc.with_ymd_and_hms(2026, 8, 18, 3, 0, 0).unwrap();
        let snapshots = facts
            .into_snapshots(
                "wb_account",
                cutoff,
                cutoff,
                Utc.with_ymd_and_hms(2026, 8, 16, 19, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 8, 17, 19, 0, 0).unwrap(),
                "test-1",
            )
            .unwrap();
        assert_eq!(snapshots.len(), 4);
    }

    #[tokio::test]
    async fn no_eligible_campaigns_is_a_complete_empty_advertising_source() {
        let mut fixture = FixtureTransport::complete();
        fixture.campaign_ids = json!({"adverts":[]});
        fixture.stats = Arc::new(Mutex::new(VecDeque::new()));
        assert!(
            WbReportSource::new(fixture)
                .collect(NaiveDate::from_ymd_opt(2026, 8, 17).unwrap())
                .await
                .unwrap()
                .advertising
                .is_empty()
        );
    }

    #[tokio::test]
    async fn advertising_rate_limit_fails_before_other_wb_sources_are_called() {
        let mut fixture = FixtureTransport::complete();
        fixture.stats = Arc::new(Mutex::new(VecDeque::from([Err(
            WbReportSourceError::Upstream(WbErrorKind::RateLimited),
        )])));
        let calls = Arc::clone(&fixture.calls);
        assert_eq!(
            WbReportSource::new(fixture)
                .collect(NaiveDate::from_ymd_opt(2026, 8, 17).unwrap())
                .await,
            Err(WbReportSourceError::Upstream(WbErrorKind::RateLimited))
        );
        assert_eq!(&*calls.lock().unwrap(), &["campaigns", "advertising"]);
    }

    #[tokio::test]
    async fn stock_pagination_uses_raw_rows_after_normalization() {
        let mut fixture = FixtureTransport::complete();
        fixture.stocks = Arc::new(Mutex::new(VecDeque::from([
            json!({"data":{"items": vec![
                json!({"nmId":1,"warehouseId":2,"quantity":1}); PAGE_SIZE
            ]}}),
            json!({"data":{"items":[]}}),
        ])));
        let stocks = Arc::clone(&fixture.stocks);
        let source = WbReportSource::new(fixture);
        let facts = source.collect_stock_pages().await.unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].sellable_units, PAGE_SIZE as u64);
        assert!(stocks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn sales_pagination_rejects_foreign_dates_and_requires_a_terminal_page() {
        fn sales_page(date: &str, count: usize) -> Value {
            json!({"data":{"currency":"RUB","products": (1..=count).map(|sku| json!({
                "product":{"nmId":sku},
                "statistic":{"selected":{
                    "period":{"start":date,"end":date},
                    "orderCount":1,"orderSum":90,"cancelCount":0
                }}
            })).collect::<Vec<_>>()}})
        }

        let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        let wrong_date = WbReportSource::new(SalesFixtureTransport {
            pages: Mutex::new(VecDeque::from([sales_page("2026-08-16", 1)])),
        });
        assert_eq!(
            wrong_date.collect_sales_pages(date).await,
            Err(WbReportSourceError::InvalidSalesResponse)
        );

        let unterminated = WbReportSource::new(SalesFixtureTransport {
            pages: Mutex::new(VecDeque::from([sales_page("2026-08-17", PAGE_SIZE)])),
        });
        assert_eq!(
            unterminated.collect_sales_pages_with_limit(date, 1).await,
            Err(WbReportSourceError::PaginationLimit)
        );

        // The sales-only fixture deliberately refuses every unrelated route;
        // exercising those refusals also keeps the all-target line gate exact.
        let unrelated = SalesFixtureTransport {
            pages: Mutex::new(VecDeque::new()),
        };
        assert_eq!(
            unrelated.stock_page(1, 0).await,
            Err(WbReportSourceError::InvalidResponse)
        );
        assert_eq!(
            unrelated.price_page(1, 0).await,
            Err(WbReportSourceError::InvalidResponse)
        );
        assert_eq!(
            unrelated.campaigns().await,
            Err(WbReportSourceError::InvalidResponse)
        );
        assert_eq!(
            unrelated.promotion_stats(vec![1], date, date).await,
            Err(WbReportSourceError::InvalidResponse)
        );
    }

    #[tokio::test]
    async fn campaigns_are_split_into_documented_fifty_id_requests() {
        let mut fixture = FixtureTransport::complete();
        fixture.campaign_ids = json!({"adverts":[{
            "status":9,
            "advert_list": (1_u64..=51).map(|advert_id| json!({"advertId":advert_id})).collect::<Vec<_>>()
        }]});
        fixture.stats = Arc::new(Mutex::new(VecDeque::from([Ok(json!([])), Ok(json!([]))])));
        let requested_stats = Arc::clone(&fixture.requested_stats);
        let source = WbReportSource::new(fixture);
        let facts = source
            .collect(NaiveDate::from_ymd_opt(2026, 8, 17).unwrap())
            .await
            .unwrap();
        assert!(facts.advertising.is_empty());
        let requests = requested_stats.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], (1_u64..=50).collect::<Vec<_>>());
        assert_eq!(requests[1], vec![51]);
    }

    #[tokio::test]
    async fn client_transport_uses_only_the_hardened_wb_methods() {
        let (analytics, analytics_requests) = mock_http(vec![
            (200, "[]".to_owned()),
            (200, r#"{"data":{"items":[]}}"#.to_owned()),
        ]);
        let (other, other_requests) = mock_http(vec![
            (200, r#"{"data":{"listGoods":[]}}"#.to_owned()),
            (200, r#"{"adverts":[]}"#.to_owned()),
            (200, "[]".to_owned()),
        ]);
        let client = WbClient::new_for_test(
            Duration::from_secs(2),
            BTreeMap::from([(
                "account".to_owned(),
                WbCredentials {
                    token: "test-token".to_owned(),
                },
            )]),
            &other,
            &analytics,
        );
        let transport = WbClientReportTransport::new(client, "account".to_owned());
        let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();

        assert_eq!(
            transport.sales_page(date, date, 1000, 0).await.unwrap(),
            json!([])
        );
        assert_eq!(
            transport.stock_page(1000, 0).await.unwrap(),
            json!({"data":{"items":[]}})
        );
        assert_eq!(
            transport.price_page(1000, 0).await.unwrap(),
            json!({"data":{"listGoods":[]}})
        );
        assert_eq!(transport.campaigns().await.unwrap(), json!({"adverts":[]}));
        assert_eq!(
            transport
                .promotion_stats(vec![7], date, date)
                .await
                .unwrap(),
            json!([])
        );

        let sales = analytics_requests.recv().unwrap();
        assert!(sales.starts_with("POST /api/analytics/v3/sales-funnel/products HTTP/1.1"));
        assert!(sales.contains(r#""limit":1000"#));
        assert!(sales.contains(r#""offset":0"#));
        let stock = analytics_requests.recv().unwrap();
        assert!(stock.starts_with("POST /api/analytics/v1/stocks-report/wb-warehouses HTTP/1.1"));
        assert!(stock.contains(r#""limit":1000"#));
        assert!(
            other_requests
                .recv()
                .unwrap()
                .starts_with("GET /api/v2/list/goods/filter?limit=1000&offset=0 HTTP/1.1")
        );
        assert!(
            other_requests
                .recv()
                .unwrap()
                .starts_with("GET /adv/v1/promotion/count HTTP/1.1")
        );
        let stats = other_requests.recv().unwrap();
        assert!(stats.starts_with("GET /adv/v3/fullstats?"));
        assert!(stats.contains("ids=7"));
        assert!(stats.contains("beginDate=2026-08-17"));
        assert!(stats.contains("endDate=2026-08-17"));
    }

    #[tokio::test]
    async fn client_transport_preserves_each_upstream_error_class() {
        let (analytics, _analytics_requests) =
            mock_http(vec![(401, String::new()), (401, String::new())]);
        let (other, _other_requests) = mock_http(vec![
            (401, String::new()),
            (401, String::new()),
            (401, String::new()),
        ]);
        let client = WbClient::new_for_test(
            Duration::from_secs(2),
            BTreeMap::from([(
                "account".to_owned(),
                WbCredentials {
                    token: "test-token".to_owned(),
                },
            )]),
            &other,
            &analytics,
        );
        let transport = WbClientReportTransport::new(client, "account".to_owned());
        let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        let expected = Err(WbReportSourceError::Upstream(WbErrorKind::Unauthorized));

        assert_eq!(transport.sales_page(date, date, 1000, 0).await, expected);
        assert_eq!(transport.stock_page(1_000, 0).await, expected);
        assert_eq!(transport.price_page(1_000, 0).await, expected);
        assert_eq!(transport.campaigns().await, expected);
        assert_eq!(
            transport.promotion_stats(vec![7], date, date).await,
            expected
        );
    }

    #[tokio::test]
    async fn oversized_valid_campaign_inventory_reports_capacity_without_truncating() {
        // Production accounts can exceed the bounded 500-campaign planner.
        // This is a capacity failure, not a malformed vendor response. Never
        // silently truncate it or spend quota collecting an incomplete set.
        for count in [519, 2_282] {
            let mut fixture = FixtureTransport::complete();
            fixture.campaign_ids = json!({"adverts":[{"status":7,"advert_list":
                (1..=count).map(|id| json!({"advertId":id})).collect::<Vec<_>>()
            }]});
            let requests = fixture.requested_stats.clone();
            assert_eq!(
                WbReportSource::new(fixture)
                    .collect(NaiveDate::from_ymd_opt(2026, 9, 7).unwrap())
                    .await,
                Err(WbReportSourceError::CampaignInventoryLimit)
            );
            assert!(requests.lock().unwrap().is_empty());
        }
        assert_eq!(
            WbReportSourceError::CampaignInventoryLimit.code(),
            "campaign_inventory_limit"
        );
    }

    #[tokio::test]
    async fn invalid_stats_and_full_page_limits_fail_closed() {
        let mut malformed = FixtureTransport::complete();
        malformed.stats = Arc::new(Mutex::new(VecDeque::from([Ok(json!({}))])));
        assert_eq!(
            WbReportSource::new(malformed)
                .collect(NaiveDate::from_ymd_opt(2026, 8, 17).unwrap())
                .await,
            Err(WbReportSourceError::InvalidPromotionResponse)
        );

        let mut full_stock = FixtureTransport::complete();
        full_stock.stocks = Arc::new(Mutex::new(VecDeque::from([json!({"data":{"items": vec![
            json!({"nmId":1,"warehouseId":2,"quantity":1}); PAGE_SIZE
        ]}})])));
        assert_eq!(
            WbReportSource::new(full_stock)
                .collect_stock_pages_with_limit(1)
                .await,
            Err(WbReportSourceError::PaginationLimit)
        );

        let goods = (1_u64..=PAGE_SIZE as u64)
            .map(|sku| {
                json!({
                    "nmID":sku,"currencyIsoCode4217":"RUB",
                    "sizes":[{"price":100,"discountedPrice":90}]
                })
            })
            .collect::<Vec<_>>();
        let mut full_prices = FixtureTransport::complete();
        full_prices.prices = Arc::new(Mutex::new(VecDeque::from([json!({
            "data":{"listGoods":goods}
        })])));
        assert_eq!(
            WbReportSource::new(full_prices)
                .collect_price_pages_with_limit(1)
                .await,
            Err(WbReportSourceError::PaginationLimit)
        );
    }

    #[test]
    fn error_codes_and_page_offset_are_stable() {
        assert_eq!(
            WbReportSourceError::Upstream(WbErrorKind::Unauthorized).code(),
            "unauthorized"
        );
        assert_eq!(
            WbReportSourceError::InvalidResponse.code(),
            "invalid_response"
        );
        assert_eq!(
            WbReportSourceError::InvalidSalesResponse.code(),
            "invalid_sales_response"
        );
        assert_eq!(
            WbReportSourceError::InvalidStockResponse.code(),
            "invalid_stock_response"
        );
        assert_eq!(
            WbReportSourceError::InvalidPriceResponse.code(),
            "invalid_price_response"
        );
        assert_eq!(
            WbReportSourceError::InvalidCampaignResponse.code(),
            "invalid_campaign_response"
        );
        assert_eq!(
            WbReportSourceError::InvalidPromotionResponse.code(),
            "invalid_promotion_response"
        );
        assert_eq!(
            WbReportSourceError::PaginationLimit.code(),
            "pagination_limit"
        );
        assert_eq!(
            WbReportSourceError::InvalidSnapshotInput.code(),
            "invalid_snapshot_input"
        );
        assert_eq!(
            WbReportSourceError::from(WbReportParseError::Shape),
            WbReportSourceError::InvalidResponse
        );
        assert_eq!(page_offset(2).unwrap(), 2_000);
    }
}
