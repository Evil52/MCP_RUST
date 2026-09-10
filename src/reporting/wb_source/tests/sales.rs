//! Small funnel pages retain the HTTP cap, catalogue bound and durable replay.

use super::*;
use crate::reporting::checkpoint::tests::{MemoryPages, journal};

const DATE: &str = "2026-08-17";
const HTTP_BODY_LIMIT: usize = 2 * 1_048_576;

#[derive(Clone)]
struct SalesFixtureTransport {
    pages: Arc<Mutex<VecDeque<Value>>>,
    requested: Arc<Mutex<Vec<(u32, u32)>>>,
}

impl SalesFixtureTransport {
    fn new(pages: Vec<Value>) -> Self {
        Self {
            pages: Arc::new(Mutex::new(pages.into())),
            requested: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl WbReportTransport for SalesFixtureTransport {
    fn sales_page<'a>(
        &'a self,
        _start: NaiveDate,
        _end: NaiveDate,
        limit: u32,
        offset: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.requested.lock().unwrap().push((limit, offset));
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

fn sales_page(date: &str, offset: u32, count: u32, metadata_bytes: usize) -> Value {
    json!({"data":{"currency":"RUB","products": (offset..offset + count).map(|sku| json!({
        "product":{"nmId":sku + 1, "title":"x".repeat(metadata_bytes)},
        "statistic":{"selected":{
            "period":{"start":date,"end":date},
            "orderCount":1,"orderSum":90,"cancelCount":0
        }}
    })).collect::<Vec<_>>()}})
}

#[tokio::test]
async fn rich_funnel_pages_fit_unchanged_http_budget_without_losing_products() {
    // Synthetic rich metadata reproduces a 1,000-product body above 2 MiB.
    let old_page = sales_page(DATE, 0, 1_000, 2_300).to_string();
    assert!(old_page.len() > HTTP_BODY_LIMIT);
    drop(old_page);
    let mut responses = Vec::new();
    for offset in [0, 250, 500, 750] {
        let body = sales_page(DATE, offset, 250, 2_300).to_string();
        assert!(body.len() < HTTP_BODY_LIMIT);
        responses.push((200, body));
    }
    responses.push((200, sales_page(DATE, 1_000, 0, 0).to_string()));
    let (url, requests) = mock_http(responses);
    let client = WbClient::new_for_test(
        Duration::from_secs(2),
        BTreeMap::from([(
            "account".to_owned(),
            WbCredentials {
                token: "test-token".to_owned(),
            },
        )]),
        &url,
        &url,
    );
    let source = WbReportSource::new(WbClientReportTransport::new(client, "account".to_owned()));
    let date = NaiveDate::parse_from_str(DATE, "%Y-%m-%d").unwrap();
    let facts = source.collect_sales_pages(date).await.unwrap();
    assert_eq!(facts.len(), 1_000);
    assert_eq!(
        facts.iter().map(|fact| fact.sku).collect::<Vec<_>>(),
        (1..=1_000).collect::<Vec<_>>()
    );
    for offset in [0, 250, 500, 750, 1_000] {
        let request = requests.recv().unwrap();
        assert!(request.starts_with("POST /api/analytics/v3/sales-funnel/products HTTP/1.1"));
        let (_, body) = request.split_once("\r\n\r\n").unwrap();
        let payload: Value = serde_json::from_str(body).unwrap();
        assert_eq!(payload["limit"], 250);
        assert_eq!(payload["offset"], offset);
        assert_eq!(payload["selectedPeriod"], json!({"start":DATE,"end":DATE}));
    }
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn sales_resume_skips_saved_pages_but_never_reuses_old_page_boundaries() {
    let date = NaiveDate::parse_from_str(DATE, "%Y-%m-%d").unwrap();
    let pages = MemoryPages::default();
    // The old request could return a short page with a different catalogue.
    // Neither its contents nor its completion marker belong to the new request.
    checkpointed(&journal(&pages), json!(["wb_sales", date, 0]), || async {
        parse_sales_page(&sales_page(DATE, 99_999, 100, 0))
            .map_err(|_| WbReportSourceError::InvalidSalesResponse)
    })
    .await
    .unwrap();
    let fixture = SalesFixtureTransport::new(vec![
        sales_page(DATE, 0, 250, 0),
        sales_page(DATE, 250, 250, 0),
        sales_page(DATE, 500, 1, 0),
    ]);
    for expected_requests in 1..=2 {
        let source = WbReportSource::new(fixture.clone()).with_checkpoints(journal(&pages));
        assert_eq!(
            source.collect_sales_pages(date).await,
            Err(WbReportSourceError::Checkpoint(CheckpointError::Deferred))
        );
        assert_eq!(fixture.requested.lock().unwrap().len(), expected_requests);
    }
    let source = WbReportSource::new(fixture.clone()).with_checkpoints(journal(&pages));
    let facts = source.collect_sales_pages(date).await.unwrap();
    assert_eq!(facts.len(), 501);
    assert_eq!(
        facts.iter().map(|fact| fact.sku).collect::<Vec<_>>(),
        (1..=501).collect::<Vec<_>>()
    );
    assert_eq!(
        *fixture.requested.lock().unwrap(),
        vec![(250, 0), (250, 250), (250, 500)]
    );
    assert_eq!(pages.lock().unwrap().len(), 4);
    // Replaying the completed source requires no further marketplace call.
    let source = WbReportSource::new(fixture.clone()).with_checkpoints(journal(&pages));
    assert_eq!(source.collect_sales_pages(date).await.unwrap(), facts);
    assert_eq!(fixture.requested.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn sales_pagination_rejects_wrong_dates_oversized_and_unterminated_pages() {
    let date = NaiveDate::parse_from_str(DATE, "%Y-%m-%d").unwrap();
    for page in [
        sales_page("2026-08-16", 0, 1, 0),
        sales_page(DATE, 0, 251, 0),
    ] {
        let source = WbReportSource::new(SalesFixtureTransport::new(vec![page]));
        assert_eq!(
            source.collect_sales_pages(date).await,
            Err(WbReportSourceError::InvalidSalesResponse)
        );
    }
    let unterminated = WbReportSource::new(SalesFixtureTransport::new(vec![sales_page(
        DATE, 0, 250, 0,
    )]));
    assert_eq!(
        unterminated.collect_sales_pages_with_limit(date, 1).await,
        Err(WbReportSourceError::PaginationLimit)
    );
    // The sales fixture explicitly refuses all unrelated sources and EOF.
    let unrelated = SalesFixtureTransport::new(Vec::new());
    assert_eq!(
        unrelated.sales_page(date, date, 250, 0).await,
        Err(WbReportSourceError::InvalidResponse)
    );
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
async fn smaller_sales_pages_preserve_total_catalogue_limit_and_require_terminal_page() {
    let date = NaiveDate::parse_from_str(DATE, "%Y-%m-%d").unwrap();
    let fixture = SalesFixtureTransport::new(
        (0..100)
            .map(|page| sales_page(DATE, page * 250, 250, 0))
            .collect(),
    );
    let source = WbReportSource::new(fixture.clone());
    assert_eq!(
        source.collect_sales_pages(date).await,
        Err(WbReportSourceError::PaginationLimit)
    );
    assert_eq!(fixture.requested.lock().unwrap().len(), 100);
    assert_eq!(
        fixture.requested.lock().unwrap().last(),
        Some(&(250, 24_750))
    );
    assert_eq!(
        page_offset(usize::MAX, 250),
        Err(WbReportSourceError::PaginationLimit)
    );
    assert_eq!(
        page_offset(u32::MAX as usize, 250),
        Err(WbReportSourceError::PaginationLimit)
    );
}
