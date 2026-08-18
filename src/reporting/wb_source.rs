//! Bounded read-only Wildberries source for daily reports.

use std::{future::Future, pin::Pin, sync::Arc};

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{Value, json};
use thiserror::Error;

use crate::wb::{WbClient, WbErrorKind};

use super::{
    postgres_collector::{
        CollectedAdvertisingFact, CollectedFacts, CollectedPriceFact, CollectedSalesFact,
        CollectedSnapshot, CollectedStockFact, PostgresCollectorError,
    },
    snapshot::{Marketplace, SnapshotStatus},
    wb_adapter::{
        WbReportParseError, parse_campaign_ids, parse_price_page, parse_promotion_stats,
        parse_sales_history, parse_stock_page,
    },
};

const PAGE_SIZE: usize = 1_000;
const MAX_PAGES: usize = 25;
const CAMPAIGNS_PER_REQUEST: usize = 50;

pub trait WbReportTransport: Send + Sync {
    fn sales_history<'a>(
        &'a self,
        start: NaiveDate,
        end: NaiveDate,
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
    pub fn new(client: WbClient, account_id: String) -> Self {
        Self { client, account_id }
    }
}

impl WbReportTransport for WbClientReportTransport {
    fn sales_history<'a>(
        &'a self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .sales_funnel_history(
                    &self.account_id,
                    json!({
                        "selectedPeriod": {
                            "start": start.format("%Y-%m-%d").to_string(),
                            "end": end.format("%Y-%m-%d").to_string(),
                        },
                        "nmIds": [],
                        "skipDeletedNm": false,
                        "aggregationLevel": "day",
                    }),
                )
                .await
                .map_err(|error| WbReportSourceError::Upstream(error.kind()))
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
                .map_err(|error| WbReportSourceError::Upstream(error.kind()))
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
                .map_err(|error| WbReportSourceError::Upstream(error.kind()))
        })
    }

    fn campaigns<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .promotion_campaigns(&self.account_id)
                .await
                .map_err(|error| WbReportSourceError::Upstream(error.kind()))
        })
    }

    fn promotion_stats<'a>(
        &'a self,
        ids: Vec<u64>,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .promotion_stats(
                    &self.account_id,
                    ids,
                    start.format("%Y-%m-%d").to_string(),
                    end.format("%Y-%m-%d").to_string(),
                )
                .await
                .map_err(|error| WbReportSourceError::Upstream(error.kind()))
        })
    }
}

pub struct WbReportSource {
    transport: Arc<dyn WbReportTransport>,
}

impl WbReportSource {
    pub fn new(transport: impl WbReportTransport + 'static) -> Self {
        Self {
            transport: Arc::new(transport),
        }
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
        account_id: String,
        cutoff_at: DateTime<Utc>,
        source_as_of: DateTime<Utc>,
        period_start: DateTime<Utc>,
        period_end: DateTime<Utc>,
        collector_version: String,
    ) -> Result<Vec<CollectedSnapshot>, PostgresCollectorError> {
        let period = |facts| {
            CollectedSnapshot::new(
                account_id.clone(),
                Marketplace::Wildberries,
                cutoff_at,
                source_as_of,
                period_start,
                period_end,
                SnapshotStatus::Succeeded,
                true,
                collector_version.clone(),
                facts,
            )
        };
        let point = |facts| {
            CollectedSnapshot::new(
                account_id.clone(),
                Marketplace::Wildberries,
                cutoff_at,
                source_as_of,
                source_as_of,
                source_as_of,
                SnapshotStatus::Succeeded,
                true,
                collector_version.clone(),
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
    #[error("Wildberries daily-report source request failed")]
    Upstream(WbErrorKind),
    #[error("Wildberries daily-report source response is invalid")]
    InvalidResponse,
    #[error("Wildberries daily-report source pagination exceeded its fixed bound")]
    PaginationLimit,
    #[error("Wildberries daily-report snapshot input is invalid")]
    InvalidSnapshotInput,
}

impl WbReportSourceError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Upstream(kind) => kind.code(),
            Self::InvalidResponse => "invalid_response",
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

impl WbReportSource {
    pub async fn collect(&self, date: NaiveDate) -> Result<WbCollectedFacts, WbReportSourceError> {
        let sales = parse_sales_history(&self.transport.sales_history(date, date).await?)?;
        let stocks = self.collect_stock_pages().await?;
        let prices = self.collect_price_pages().await?;
        let ids = parse_campaign_ids(&self.transport.campaigns().await?)?;
        let mut advertising = Vec::new();
        for chunk in ids.chunks(CAMPAIGNS_PER_REQUEST) {
            advertising.extend(parse_promotion_stats(
                &self
                    .transport
                    .promotion_stats(chunk.to_vec(), date, date)
                    .await?,
            )?);
        }
        Ok(WbCollectedFacts {
            sales,
            advertising,
            stocks,
            prices,
        })
    }

    async fn collect_stock_pages(&self) -> Result<Vec<CollectedStockFact>, WbReportSourceError> {
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
                parse_stock_page(&self.transport.stock_page(PAGE_SIZE as u32, offset).await?)?;
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

    async fn collect_price_pages(&self) -> Result<Vec<CollectedPriceFact>, WbReportSourceError> {
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
                parse_price_page(&self.transport.price_page(PAGE_SIZE as u32, offset).await?)?;
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
        .and_then(|page| page.checked_mul(PAGE_SIZE as u32))
        .ok_or(WbReportSourceError::PaginationLimit)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        collections::VecDeque,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::{test_support::mock_http, wb::WbCredentials};

    struct FixtureTransport {
        stocks: Arc<Mutex<VecDeque<Value>>>,
        prices: Arc<Mutex<VecDeque<Value>>>,
        campaign_ids: Value,
        stats: Arc<Mutex<VecDeque<Value>>>,
        requested_stats: Arc<Mutex<Vec<Vec<u64>>>>,
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
                stats: Arc::new(Mutex::new(VecDeque::from([
                    json!([{"advertId":4,"stats":[{
                        "date":"2026-08-17","nm_id":1,"views":10,"clicks":1,"sum":2,
                        "orders":1,"sum_price":90
                    }]}]),
                ]))),
                requested_stats: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl WbReportTransport for FixtureTransport {
        fn sales_history<'a>(
            &'a self,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
            Box::pin(async {
                Ok(json!([{"product":{"nmId":1},"currency":"RUB","history":[{
                    "date":"2026-08-17","orderCount":2,"orderSum":180
                }]}]))
            })
        }

        fn stock_page<'a>(
            &'a self,
            _limit: u32,
            _offset: u32,
        ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
            Box::pin(async {
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
            Box::pin(async { Ok(self.campaign_ids.clone()) })
        }

        fn promotion_stats<'a>(
            &'a self,
            ids: Vec<u64>,
            _start: NaiveDate,
            _end: NaiveDate,
        ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
            Box::pin(async {
                self.requested_stats.lock().unwrap().push(ids);
                self.stats
                    .lock()
                    .unwrap()
                    .pop_front()
                    .ok_or(WbReportSourceError::InvalidResponse)
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
                "wb_account".to_owned(),
                cutoff,
                cutoff,
                Utc.with_ymd_and_hms(2026, 8, 16, 19, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 8, 17, 19, 0, 0).unwrap(),
                "test-1".to_owned(),
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
    async fn campaigns_are_split_into_documented_fifty_id_requests() {
        let mut fixture = FixtureTransport::complete();
        fixture.campaign_ids = json!({"adverts":[{
            "status":9,
            "advert_list": (1_u64..=51).map(|advert_id| json!({"advertId":advert_id})).collect::<Vec<_>>()
        }]});
        fixture.stats = Arc::new(Mutex::new(VecDeque::from([json!([]), json!([])])));
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
            transport.sales_history(date, date).await.unwrap(),
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
        assert!(sales.starts_with("POST /api/analytics/v3/sales-funnel/products/history HTTP/1.1"));
        assert!(sales.contains(r#""aggregationLevel":"day""#));
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

        assert_eq!(transport.sales_history(date, date).await, expected);
        assert_eq!(transport.stock_page(1_000, 0).await, expected);
        assert_eq!(transport.price_page(1_000, 0).await, expected);
        assert_eq!(transport.campaigns().await, expected);
        assert_eq!(
            transport.promotion_stats(vec![7], date, date).await,
            expected
        );
    }

    #[tokio::test]
    async fn invalid_stats_and_full_page_limits_fail_closed() {
        let mut malformed = FixtureTransport::complete();
        malformed.stats = Arc::new(Mutex::new(VecDeque::from([json!({})])));
        assert_eq!(
            WbReportSource::new(malformed)
                .collect(NaiveDate::from_ymd_opt(2026, 8, 17).unwrap())
                .await,
            Err(WbReportSourceError::InvalidResponse)
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
