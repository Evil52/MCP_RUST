//! Bounded read-only Ozon report source.
//!
//! This module is deliberately transport-agnostic. A future runtime adapter
//! may delegate to `OzonClient`, but every request is first built by the exact
//! request contract in `ozon_adapter` and every response is normalized before
//! it can reach report persistence.

use std::{future::Future, pin::Pin};

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use thiserror::Error;

use crate::{
    config::StoreId,
    ozon::{OzonClient, OzonErrorKind},
};

use super::{
    ozon_adapter::{
        OzonReportParseError, OzonReportRequest, parse_price_page, parse_sales_page,
        parse_stock_page, product_page_request, sales_request,
    },
    postgres_collector::{
        CollectedFacts, CollectedPriceFact, CollectedSalesFact, CollectedSnapshot,
        CollectedStockFact, PostgresCollectorError, PostgresSnapshotWriter,
    },
    snapshot::{Marketplace, SnapshotStatus},
};

const MAX_PAGES_PER_SOURCE: usize = 25;

#[allow(clippy::too_many_arguments)]
pub async fn collect_and_persist<T: OzonReportTransport>(
    source: &OzonReportSource<T>,
    writer: &PostgresSnapshotWriter,
    account_id: String,
    cutoff_at: DateTime<Utc>,
    source_as_of: DateTime<Utc>,
    sales_period_start: DateTime<Utc>,
    sales_period_end: DateTime<Utc>,
    collector_version: String,
) -> Result<Vec<i64>, OzonReportSourceError> {
    let facts = source
        .collect_required_seller_facts(
            sales_period_start.date_naive(),
            sales_period_end.date_naive(),
        )
        .await?;
    let snapshots = facts
        .into_snapshots(
            account_id,
            cutoff_at,
            source_as_of,
            sales_period_start,
            sales_period_end,
            collector_version,
        )
        .map_err(|_| OzonReportSourceError::InvalidResponse)?;
    writer
        .persist_batch(&snapshots)
        .await
        .map_err(|_| OzonReportSourceError::Transport)
}

pub trait OzonReportTransport: Send + Sync {
    fn post<'a>(
        &'a self,
        request: OzonReportRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>>;
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
        Box::pin(async move {
            self.client
                .post(&self.store, request.path, request.payload)
                .await
                // Keep only the stable, non-sensitive classification. In
                // particular, never retain Ozon's error body in report
                // collection diagnostics.
                .map_err(|error| OzonReportSourceError::Upstream(error.kind()))
        })
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
            Self::PaginationLimit => "pagination_limit",
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
        parse_sales_page(&response).map_err(parse_error)
    }

    /// Collects offset-paginated sales rows with the same hard bound used for
    /// the cursor sources. A full-size final page is not accepted as complete
    /// because the upstream response has no trustworthy total-row contract.
    pub async fn collect_sales_pages(
        &self,
        date_from: NaiveDate,
        date_to: NaiveDate,
    ) -> Result<Vec<CollectedSalesFact>, OzonReportSourceError> {
        let mut facts = Vec::new();
        for page in 0..MAX_PAGES_PER_SOURCE {
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

    pub async fn stock_page(
        &self,
        cursor: Option<&str>,
    ) -> Result<Vec<CollectedStockFact>, OzonReportSourceError> {
        self.product_page("/v4/product/info/stocks", cursor, parse_stock_page)
            .await
    }

    /// Collects cursor-paginated stock pages with a fixed upper bound.
    pub async fn collect_stock_pages(
        &self,
    ) -> Result<Vec<CollectedStockFact>, OzonReportSourceError> {
        self.collect_product_pages("/v4/product/info/stocks", parse_stock_page)
            .await
    }

    pub async fn price_page(
        &self,
        cursor: Option<&str>,
    ) -> Result<Vec<CollectedPriceFact>, OzonReportSourceError> {
        self.product_page("/v5/product/info/prices", cursor, parse_price_page)
            .await
    }

    /// Collects cursor-paginated price pages with a fixed upper bound.
    pub async fn collect_price_pages(
        &self,
    ) -> Result<Vec<CollectedPriceFact>, OzonReportSourceError> {
        self.collect_product_pages("/v5/product/info/prices", parse_price_page)
            .await
    }

    async fn product_page<F, Fact>(
        &self,
        path: &'static str,
        cursor: Option<&str>,
        parse: F,
    ) -> Result<Vec<Fact>, OzonReportSourceError>
    where
        F: Fn(&Value) -> Result<Vec<Fact>, OzonReportParseError>,
    {
        let request = product_page_request(path, cursor)
            .map_err(|_| OzonReportSourceError::InvalidResponse)?;
        let response = self.transport.post(request).await?;
        parse(&response).map_err(parse_error)
    }

    async fn collect_product_pages<F, Fact>(
        &self,
        path: &'static str,
        parse: F,
    ) -> Result<Vec<Fact>, OzonReportSourceError>
    where
        F: Fn(&Value) -> Result<Vec<Fact>, OzonReportParseError> + Copy,
    {
        let mut cursor = None;
        let mut facts = Vec::new();
        for _ in 0..MAX_PAGES_PER_SOURCE {
            let request = product_page_request(path, cursor.as_deref())
                .map_err(|_| OzonReportSourceError::InvalidResponse)?;
            let response = self.transport.post(request).await?;
            facts.extend(parse(&response).map_err(parse_error)?);
            cursor = next_cursor(&response)?;
            if cursor.is_none() {
                return Ok(facts);
            }
        }
        Err(OzonReportSourceError::PaginationLimit)
    }
}

fn parse_error(_: OzonReportParseError) -> OzonReportSourceError {
    OzonReportSourceError::InvalidResponse
}

fn next_cursor(response: &Value) -> Result<Option<String>, OzonReportSourceError> {
    let cursor = response
        .get("cursor")
        .and_then(Value::as_str)
        .ok_or(OzonReportSourceError::InvalidResponse)?;
    if cursor.is_empty() {
        return Ok(None);
    }
    product_page_request("/v4/product/info/stocks", Some(cursor))
        .map_err(|_| OzonReportSourceError::InvalidResponse)?;
    Ok(Some(cursor.to_owned()))
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex};

    use chrono::{NaiveDate, TimeZone, Utc};
    use serde_json::json;

    use super::*;

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

    #[tokio::test]
    async fn source_normalizes_only_valid_read_only_pages() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"result":{"data":[{
                "dimensions":[{"id":"1"},{"id":"2026-08-16"}],
                "metrics":["1.00", 2, 0, 0]
            }]}})),
            Ok(json!({"items":[{"product_id":1,"stocks":[{"type":"FBO","present":3}]}]})),
            Ok(
                json!({"items":[{"product_id":1,"price":{"currency_code":"RUB","price":"2","old_price":"0"}}]}),
            ),
        ]))));
        let day = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        assert_eq!(
            source.sales_page(day, day, 0).await.unwrap()[0].ordered_units,
            2
        );
        assert_eq!(source.stock_page(None).await.unwrap()[0].sellable_units, 3);
        assert_eq!(source.price_page(None).await.unwrap()[0].price_minor, 200);
    }

    #[tokio::test]
    async fn source_hides_transport_and_parse_details() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Err(OzonReportSourceError::Transport),
            Ok(json!({"items":"invalid"})),
        ]))));
        let day = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        assert_eq!(
            source.sales_page(day, day, 0).await,
            Err(OzonReportSourceError::Transport)
        );
        assert_eq!(
            source.stock_page(None).await,
            Err(OzonReportSourceError::InvalidResponse)
        );
    }

    #[tokio::test]
    async fn product_collection_follows_cursor_and_requires_a_valid_terminal_cursor() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"items":[{"product_id":1,"stocks":[]}],"cursor":"next"})),
            Ok(json!({"items":[{"product_id":2,"stocks":[]}],"cursor":""})),
        ]))));
        assert_eq!(source.collect_stock_pages().await.unwrap().len(), 0);

        let invalid = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([Ok(
            json!({"items":[],"cursor":42}),
        )]))));
        assert_eq!(
            invalid.collect_stock_pages().await,
            Err(OzonReportSourceError::InvalidResponse)
        );
    }

    #[tokio::test]
    async fn required_facts_are_returned_only_after_all_sources_succeed() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"result":{"data":[]}})),
            Ok(json!({"items":[],"cursor":""})),
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

    #[test]
    fn complete_facts_become_three_complete_source_snapshots() {
        let as_of = Utc.with_ymd_and_hms(2026, 8, 16, 19, 0, 0).unwrap();
        let start = Utc.with_ymd_and_hms(2026, 8, 15, 19, 0, 0).unwrap();
        let facts = OzonCollectedFacts {
            sales: vec![CollectedSalesFact {
                business_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
                sku: 1,
                ordered_units: 1,
                operational_gmv_minor: 100,
                cancelled_units: 0,
                returned_units: 0,
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
        assert_eq!(
            facts
                .into_snapshots(
                    "ozon".to_owned(),
                    as_of,
                    as_of,
                    start,
                    as_of,
                    "test-1".to_owned(),
                )
                .unwrap()
                .len(),
            3
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
            cancelled_units: 0,
            returned_units: 0,
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
                .take(MAX_PAGES_PER_SOURCE)
                .collect(),
        )));
        let day = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        assert_eq!(
            sales.collect_sales_pages(day, day).await,
            Err(OzonReportSourceError::PaginationLimit)
        );

        let cursor = OzonReportSource::new(FixtureTransport(Mutex::new(
            std::iter::repeat_with(|| Ok(json!({"items":[],"cursor":"next"})))
                .take(MAX_PAGES_PER_SOURCE)
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
        assert!(matches!(
            transport
                .post(product_page_request("/v4/product/info/stocks", None).unwrap())
                .await,
            Err(OzonReportSourceError::Upstream(
                OzonErrorKind::MissingCredentials
            ))
        ));
        assert_eq!(
            OzonReportSourceError::Upstream(OzonErrorKind::Forbidden).code(),
            "forbidden"
        );
    }
}
