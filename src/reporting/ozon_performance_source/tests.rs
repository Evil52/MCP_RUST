mod retry_after;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
    time::Duration,
};

use chrono::NaiveDate;
use serde_json::{Value, json};

use super::*;
use crate::{config::PerformanceCredentials, test_support::mock_http};

struct FixtureTransport {
    campaign_pages: Mutex<VecDeque<Result<Value, OzonPerformanceReportSourceError>>>,
    statistics: Mutex<VecDeque<Result<Value, OzonPerformanceReportSourceError>>>,
    expenses: Mutex<VecDeque<Result<Value, OzonPerformanceReportSourceError>>>,
    calls: Mutex<Vec<Vec<u64>>>,
}

impl FixtureTransport {
    fn new(campaign_pages: Vec<Value>, statistics: Vec<Value>) -> Self {
        Self {
            campaign_pages: Mutex::new(campaign_pages.into_iter().map(Ok).collect::<VecDeque<_>>()),
            statistics: Mutex::new(statistics.into_iter().map(Ok).collect::<VecDeque<_>>()),
            expenses: Mutex::new(VecDeque::new()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn with_expenses(self, expenses: Vec<Value>) -> Self {
        *self.expenses.lock().unwrap() = expenses.into_iter().map(Ok).collect();
        self
    }
}

impl OzonPerformanceReportTransport for FixtureTransport {
    fn campaigns(
        &self,
        _page: u32,
        _page_size: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonPerformanceReportSourceError>> + Send + '_>>
    {
        Box::pin(async {
            self.campaign_pages
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Err(OzonPerformanceReportSourceError::PaginationLimit))
        })
    }

    fn sku_statistics(
        &self,
        campaign_ids: Vec<u64>,
        _date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonPerformanceReportSourceError>> + Send + '_>>
    {
        self.calls.lock().unwrap().push(campaign_ids);
        Box::pin(async {
            self.statistics
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Err(OzonPerformanceReportSourceError::InvalidResponse))
        })
    }

    fn expenses(
        &self,
        campaign_ids: Vec<u64>,
        _date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonPerformanceReportSourceError>> + Send + '_>>
    {
        self.calls.lock().unwrap().push(campaign_ids);
        Box::pin(async {
            self.expenses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Err(OzonPerformanceReportSourceError::InvalidResponse))
        })
    }
}

fn campaign_page(ids: &[u64], total: usize) -> Value {
    json!({
        "list": ids.iter().map(|id| json!({
            "id": id,
            "fromDate": "2026-08-01",
            "toDate": "2026-08-31"
        })).collect::<Vec<_>>(),
        "total": total.to_string(),
    })
}

fn statistics(campaign_id: u64, sku: u64, date: &str) -> Value {
    json!({"rows": [{
        "campaignId": campaign_id.to_string(), "sku": sku.to_string(), "date": date,
        "views": "10", "clicks": "2", "expense": "3.00", "orders": "1",
        "sales": "9.00", "toCart": "3", "modelOrders": "1",
        "modelSales": "9.00", "price": "9.00", "avgCpc": "1.50"
    }]})
}

fn credentials() -> BTreeMap<StoreId, PerformanceCredentials> {
    BTreeMap::from([(
        StoreId::from("shop"),
        PerformanceCredentials {
            client_id: "performance-client".to_owned(),
            client_secret: "performance-secret".to_owned(),
        },
    )])
}

fn token() -> String {
    json!({
        "access_token": "test-access-token",
        "token_type": "Bearer",
        "expires_in": 1_800,
    })
    .to_string()
}

#[tokio::test]
async fn client_transport_uses_exact_hardened_performance_contracts() {
    let (base_url, requests) = mock_http(vec![
        (200, token()),
        (200, json!({"list": [], "total": "0"}).to_string()),
        (200, json!({"rows": []}).to_string()),
        (200, json!({"rows": []}).to_string()),
    ]);
    let client = PerformanceClient::new_for_test(base_url, Duration::from_secs(3), credentials());
    let transport = PerformanceClientReportTransport::new(client, StoreId::from("shop"));
    let date = NaiveDate::from_ymd_opt(2026, 8, 18).unwrap();

    assert_eq!(
        transport.campaigns(2, 100).await.unwrap(),
        json!({"list": [], "total": "0"})
    );
    assert_eq!(
        transport.sku_statistics(vec![11, 22], date).await.unwrap(),
        json!({"rows": []})
    );
    assert_eq!(
        transport.expenses(vec![11, 22], date).await.unwrap(),
        json!({"rows": []})
    );

    let auth = requests.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(auth.starts_with("POST /api/client/token HTTP/1.1"));
    let campaigns = requests.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(
        campaigns
            .starts_with("GET /api/client/campaign?advObjectType=SKU&page=2&pageSize=100 HTTP/1.1")
    );
    let statistics = requests.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(statistics.starts_with("POST /api/client/statistics/products/sku HTTP/1.1"));
    assert!(statistics.contains(r#""campaignIds":[11,22]"#));
    assert!(statistics.contains(r#""dateFrom":"2026-08-18""#));
    assert!(statistics.contains(r#""dateTo":"2026-08-18""#));
    let expenses = requests.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(expenses.starts_with(
        "GET /api/client/statistics/expense/json?campaignIds=11&campaignIds=22&dateFrom=2026-08-18&dateTo=2026-08-18 HTTP/1.1"
    ));
}

#[tokio::test]
async fn client_transport_preserves_upstream_error_classes() {
    let (campaign_url, _campaign_requests) = mock_http(vec![(200, token()), (403, String::new())]);
    let campaign_transport = PerformanceClientReportTransport::new(
        PerformanceClient::new_for_test(campaign_url, Duration::from_secs(3), credentials()),
        StoreId::from("shop"),
    );
    assert_eq!(
        campaign_transport.campaigns(1, 100).await,
        Err(OzonPerformanceReportSourceError::Upstream(
            PerformanceErrorKind::Forbidden
        ))
    );

    let (statistics_url, _statistics_requests) =
        mock_http(vec![(200, token()), (429, String::new())]);
    let statistics_transport = PerformanceClientReportTransport::new(
        PerformanceClient::new_for_test(statistics_url, Duration::from_secs(3), credentials()),
        StoreId::from("shop"),
    );
    assert_eq!(
        statistics_transport
            .sku_statistics(vec![1], NaiveDate::from_ymd_opt(2026, 8, 18).unwrap())
            .await,
        Err(OzonPerformanceReportSourceError::Upstream(
            PerformanceErrorKind::RateLimited
        ))
    );
}

#[tokio::test]
async fn enumerates_campaigns_and_batches_statistics() {
    let first = (1..=100).collect::<Vec<_>>();
    let source = OzonPerformanceReportSource::new(FixtureTransport::new(
        vec![campaign_page(&first, 101), campaign_page(&[101], 101)],
        (0..11)
            .map(|index| statistics(index * 10 + 1, index + 1, "2026-08-18"))
            .collect(),
    ));
    let facts = source
        .collect(NaiveDate::from_ymd_opt(2026, 8, 18).unwrap())
        .await
        .unwrap();
    assert_eq!(facts.len(), 11);
    assert_eq!(facts[0].campaign_id, 1);
    assert_eq!(facts[10].campaign_id, 101);
}

#[tokio::test]
async fn empty_campaign_inventory_is_a_valid_empty_snapshot() {
    let source = OzonPerformanceReportSource::new(FixtureTransport::new(
        vec![campaign_page(&[], 0)],
        Vec::new(),
    ));
    assert!(
        source
            .collect(NaiveDate::from_ymd_opt(2026, 8, 18).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn default_expense_contract_fails_closed() {
    struct WithoutExpenses;

    impl OzonPerformanceReportTransport for WithoutExpenses {
        fn campaigns(
            &self,
            _page: u32,
            _page_size: u32,
        ) -> Pin<
            Box<dyn Future<Output = Result<Value, OzonPerformanceReportSourceError>> + Send + '_>,
        > {
            Box::pin(async { Ok(campaign_page(&[], 0)) })
        }

        fn sku_statistics(
            &self,
            _campaign_ids: Vec<u64>,
            _date: NaiveDate,
        ) -> Pin<
            Box<dyn Future<Output = Result<Value, OzonPerformanceReportSourceError>> + Send + '_>,
        > {
            Box::pin(async { Ok(json!({"rows":[]})) })
        }
    }

    assert_eq!(
        WithoutExpenses.campaigns(1, 100).await,
        Ok(campaign_page(&[], 0))
    );
    assert_eq!(
        WithoutExpenses
            .sku_statistics(vec![1], NaiveDate::from_ymd_opt(2026, 8, 18).unwrap())
            .await,
        Ok(json!({"rows":[]}))
    );
    assert_eq!(
        WithoutExpenses
            .expenses(vec![1], NaiveDate::from_ymd_opt(2026, 8, 18).unwrap())
            .await,
        Err(OzonPerformanceReportSourceError::InvalidResponse)
    );
}

#[tokio::test]
async fn extended_collection_preserves_rubles_bonuses_and_prepayment() {
    let source = OzonPerformanceReportSource::new(
        FixtureTransport::new(
            vec![campaign_page(&[7], 1)],
            vec![statistics(7, 123, "2026-08-18")],
        )
        .with_expenses(vec![json!({"rows": [{
            "id": "7", "date": "2026-08-18", "title": "campaign",
            "moneySpent": "12,34", "bonusSpent": "2,00",
            "prepaymentSpent": "9,50"
        }]})]),
    );
    let facts = source
        .collect_extended(NaiveDate::from_ymd_opt(2026, 8, 18).unwrap())
        .await
        .unwrap();
    assert_eq!(facts.advertising.len(), 1);
    assert_eq!(facts.expenses.len(), 1);
    assert_eq!(facts.expenses[0].money_spent_minor, 1_234);
    assert_eq!(facts.expenses[0].bonus_spent_minor, 200);
    assert_eq!(facts.expenses[0].prepayment_spent_minor, 950);
}

#[tokio::test]
async fn extended_collection_rejects_malformed_statistics_and_expenses() {
    let report_date = NaiveDate::from_ymd_opt(2026, 8, 18).unwrap();
    let malformed_statistics = OzonPerformanceReportSource::new(FixtureTransport::new(
        vec![campaign_page(&[7], 1)],
        vec![json!({"rows": [{"campaignId": "7"}]})],
    ));
    assert_eq!(
        malformed_statistics.collect_extended(report_date).await,
        Err(OzonPerformanceReportSourceError::InvalidResponse)
    );

    let malformed_expenses = OzonPerformanceReportSource::new(
        FixtureTransport::new(
            vec![campaign_page(&[7], 1)],
            vec![statistics(7, 123, "2026-08-18")],
        )
        .with_expenses(vec![json!({"rows": [{"id": "7"}]})]),
    );
    assert_eq!(
        malformed_expenses.collect_extended(report_date).await,
        Err(OzonPerformanceReportSourceError::InvalidResponse)
    );
}

#[tokio::test]
async fn extended_collection_rejects_foreign_duplicates_and_oversized_sets() {
    let report_date = NaiveDate::from_ymd_opt(2026, 8, 18).unwrap();
    for statistics_response in [
        statistics(8, 123, "2026-08-18"),
        json!({"rows":[
            statistics(7, 123, "2026-08-18")["rows"][0].clone(),
            statistics(7, 123, "2026-08-18")["rows"][0].clone()
        ]}),
    ] {
        let source = OzonPerformanceReportSource::new(FixtureTransport::new(
            vec![campaign_page(&[7], 1)],
            vec![statistics_response],
        ));
        assert_eq!(
            source.collect_extended(report_date).await,
            Err(OzonPerformanceReportSourceError::InvalidResponse)
        );
    }

    for expense_rows in [
        vec![json!({
            "id":"8", "date":"2026-08-18", "moneySpent":"1",
            "bonusSpent":"0", "prepaymentSpent":"0"
        })],
        vec![
            json!({"id":"7", "date":"2026-08-18", "moneySpent":"1", "bonusSpent":"0", "prepaymentSpent":"0"}),
            json!({"id":"7", "date":"2026-08-18", "moneySpent":"2", "bonusSpent":"0", "prepaymentSpent":"0"}),
        ],
    ] {
        let source = OzonPerformanceReportSource::new(
            FixtureTransport::new(
                vec![campaign_page(&[7], 1)],
                vec![statistics(7, 123, "2026-08-18")],
            )
            .with_expenses(vec![json!({"rows":expense_rows})]),
        );
        assert_eq!(
            source.collect_extended(report_date).await,
            Err(OzonPerformanceReportSourceError::InvalidResponse)
        );
    }

    let rows = |campaign_id: u64, count: usize| {
        json!({"rows":(1..=count).map(|sku| json!({
            "campaignId":campaign_id.to_string(), "sku":sku.to_string(), "date":"2026-08-18",
            "views":"1", "clicks":"0", "expense":"0", "orders":"0", "sales":"0",
            "toCart":"0", "modelOrders":"0", "modelSales":"0", "price":"0", "avgCpc":"0"
        })).collect::<Vec<_>>()})
    };
    let source = OzonPerformanceReportSource::new(
        FixtureTransport::new(
            vec![campaign_page(&(1..=11).collect::<Vec<_>>(), 11)],
            vec![rows(1, 20_000), rows(11, 5_001)],
        )
        .with_expenses(vec![json!({"rows":[]}), json!({"rows":[]})]),
    );
    assert_eq!(
        source.collect_extended(report_date).await,
        Err(OzonPerformanceReportSourceError::TooManyFacts)
    );
}

#[tokio::test]
async fn requests_statistics_only_for_campaigns_active_on_the_report_date() {
    let source = OzonPerformanceReportSource::new(FixtureTransport::new(
        vec![json!({
            "list": [
                {"id": 1, "fromDate": "2026-08-01", "toDate": "2026-08-17"},
                {"id": 2, "fromDate": "2026-08-18T00:00:00Z", "toDate": null},
                {"id": 3, "fromDate": "2026-08-19", "toDate": "2026-08-31"}
            ],
            "total": "3"
        })],
        vec![statistics(2, 9, "2026-08-18")],
    ));
    let facts = source
        .collect(NaiveDate::from_ymd_opt(2026, 8, 18).unwrap())
        .await
        .unwrap();
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].campaign_id, 2);
}

#[tokio::test]
async fn rejects_inconsistent_campaign_pages_and_statistic_provenance() {
    let duplicate = OzonPerformanceReportSource::new(FixtureTransport::new(
        vec![campaign_page(&[1], 2), campaign_page(&[1], 2)],
        Vec::new(),
    ));
    assert_eq!(
        duplicate
            .collect(NaiveDate::from_ymd_opt(2026, 8, 18).unwrap())
            .await,
        Err(OzonPerformanceReportSourceError::InconsistentPagination)
    );

    let wrong_campaign = OzonPerformanceReportSource::new(FixtureTransport::new(
        vec![campaign_page(&[1], 1)],
        vec![statistics(2, 7, "2026-08-18")],
    ));
    assert_eq!(
        wrong_campaign
            .collect(NaiveDate::from_ymd_opt(2026, 8, 18).unwrap())
            .await,
        Err(OzonPerformanceReportSourceError::InvalidResponse)
    );

    let duplicate_facts = OzonPerformanceReportSource::new(FixtureTransport::new(
        vec![campaign_page(&[1], 1)],
        vec![json!({"rows": [
            statistics(1, 7, "2026-08-18")["rows"][0].clone(),
            statistics(1, 7, "2026-08-18")["rows"][0].clone()
        ]})],
    ));
    assert_eq!(
        duplicate_facts
            .collect(NaiveDate::from_ymd_opt(2026, 8, 18).unwrap())
            .await,
        Err(OzonPerformanceReportSourceError::InvalidResponse)
    );
}

#[tokio::test]
async fn pagination_totals_and_duplicates_fail_closed() {
    let first = (1..=100).collect::<Vec<_>>();
    let changed_total = OzonPerformanceReportSource::new(FixtureTransport::new(
        vec![campaign_page(&first, 101), campaign_page(&[101], 102)],
        Vec::new(),
    ));
    assert_eq!(
        changed_total
            .collect(NaiveDate::from_ymd_opt(2026, 8, 18).unwrap())
            .await,
        Err(OzonPerformanceReportSourceError::InconsistentPagination)
    );

    let duplicate_second_page = OzonPerformanceReportSource::new(FixtureTransport::new(
        vec![campaign_page(&first, 101), campaign_page(&[100], 101)],
        Vec::new(),
    ));
    assert_eq!(
        duplicate_second_page
            .collect(NaiveDate::from_ymd_opt(2026, 8, 18).unwrap())
            .await,
        Err(OzonPerformanceReportSourceError::InconsistentPagination)
    );
}

#[tokio::test]
async fn aggregate_fact_limit_is_enforced_across_statistics_chunks() {
    fn rows(campaign_id: u64, count: usize) -> Value {
        json!({"rows": (1..=count).map(|sku| json!({
            "campaignId": campaign_id.to_string(),
            "sku": sku.to_string(),
            "date": "2026-08-18",
            "views": "10",
            "clicks": "2",
            "expense": "3.00",
            "orders": "1",
            "sales": "9.00",
            "toCart": "3",
            "modelOrders": "1",
            "modelSales": "9.00",
            "price": "9.00",
            "avgCpc": "1.50"
        })).collect::<Vec<_>>()})
    }

    let source = OzonPerformanceReportSource::new(FixtureTransport::new(
        vec![campaign_page(&(1..=11).collect::<Vec<_>>(), 11)],
        vec![rows(1, 20_000), rows(11, 5_001)],
    ));
    assert_eq!(
        source
            .collect(NaiveDate::from_ymd_opt(2026, 8, 18).unwrap())
            .await,
        Err(OzonPerformanceReportSourceError::TooManyFacts)
    );
}

#[test]
fn campaign_page_and_error_codes_are_fail_closed() {
    assert_eq!(
        parse_campaign_page(&json!({"list": [{
            "id": 0,
            "fromDate": "2026-08-01",
            "toDate": "2026-08-31"
        }], "total": 1})),
        Err(OzonPerformanceReportSourceError::InvalidResponse)
    );
    assert_eq!(
        parse_campaign_page(&json!({"list": [], "total": 10_001})),
        Err(OzonPerformanceReportSourceError::PaginationLimit)
    );
    assert_eq!(
        OzonPerformanceReportSourceError::Upstream(PerformanceErrorKind::Forbidden).code(),
        "forbidden"
    );
    assert_eq!(
        OzonPerformanceReportSourceError::TooManyFacts.code(),
        "too_many_facts"
    );
    assert_eq!(
        OzonPerformanceReportSourceError::InvalidResponse.code(),
        "invalid_response"
    );
    assert_eq!(
        OzonPerformanceReportSourceError::InconsistentPagination.code(),
        "inconsistent_pagination"
    );
    assert_eq!(
        OzonPerformanceReportSourceError::PaginationLimit.code(),
        "pagination_limit"
    );
    assert_eq!(
        OzonPerformanceReportSourceError::from(OzonReportParseError::Shape),
        OzonPerformanceReportSourceError::InvalidResponse
    );
}

#[test]
fn campaign_page_parser_covers_every_bounded_wire_shape() {
    let oversized = vec![json!({"id": 1}); CAMPAIGN_PAGE_SIZE as usize + 1];
    let invalid = [
        Value::Null,
        json!({"list": []}),
        json!({"list": [], "total": false}),
        json!({"list": [null], "total": 1}),
        json!({"list": [{"id": 1, "fromDate": false}], "total": 1}),
        json!({"list": [{"id": 1, "fromDate": "bad-date"}], "total": 1}),
        json!({"list": [{"id": 1, "fromDate": "2026-08-20", "toDate": "2026-08-19"}], "total": 1}),
        json!({"list": [{"id": "01"}], "total": 1}),
        json!({"list": [{"id": -1}], "total": 1}),
        json!({"list": oversized, "total": 101}),
    ];
    for response in invalid {
        assert_eq!(
            parse_campaign_page(&response),
            Err(OzonPerformanceReportSourceError::InvalidResponse)
        );
    }

    assert_eq!(
        parse_campaign_page(&json!({
            "list": [
                {"id": "7", "fromDate": "", "toDate": null},
                {"id": 8}
            ],
            "total": 2
        }))
        .unwrap(),
        (
            vec![
                CampaignWindow {
                    id: 7,
                    from_date: None,
                    to_date: None
                },
                CampaignWindow {
                    id: 8,
                    from_date: None,
                    to_date: None
                }
            ],
            2
        )
    );
}
