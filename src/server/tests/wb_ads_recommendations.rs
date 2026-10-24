use super::*;
use crate::reporting::mcp_read::{SourceSnapshotQuery, SourceSnapshotResult};
use crate::reporting::snapshot::SnapshotSource;
use crate::server::tools::wb_ads_recommendations::WbAdsRecommendationsInput;

fn input() -> WbAdsRecommendationsInput {
    WbAdsRecommendationsInput {
        account: Some("account_wb".into()),
        campaign_ids: vec![11],
        days: 14,
        target_drr_bps: Some(1500),
        limit: 50,
    }
}

#[tokio::test]
async fn recommendations_deny_foreign_accounts_and_revoked_identity_before_io() {
    let reads = Arc::new(FakeReportingRepository::succeeding());
    let (server, requests) = mock_wb_server_for("manager", 0);
    let server = server.with_reporting_reader(ReportingReader::from_repository(reads.clone()));
    assert!(
        server
            .wb_ads_recommendations(RequestIdentity::dev(), Parameters(input()))
            .await
            .is_err()
    );
    assert!(
        server
            .wb_ads_recommendations(RequestIdentity::authenticated("ghost"), Parameters(input()))
            .await
            .is_err()
    );
    assert_eq!(reads.calls(), 0);
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn recommendations_validate_scope_before_database_or_marketplace_io() {
    let reads = Arc::new(FakeReportingRepository::succeeding());
    let (server, requests) = mock_wb_server_for("admin", 0);
    let server = server.with_reporting_reader(ReportingReader::from_repository(reads.clone()));
    let mut bad_window = input();
    bad_window.days = 31;
    let mut bad_target = input();
    bad_target.target_drr_bps = Some(0);
    let mut duplicate = input();
    duplicate.campaign_ids = vec![11, 11];
    let mut too_many = input();
    too_many.campaign_ids = (1..=11).collect();
    let mut bad_page = input();
    bad_page.limit = 101;
    for invalid in [bad_window, bad_target, duplicate, too_many, bad_page] {
        assert!(
            server
                .wb_ads_recommendations(RequestIdentity::dev(), Parameters(invalid))
                .await
                .is_err()
        );
    }
    assert_eq!(reads.calls(), 0);
    assert!(requests.try_recv().is_err());
}

/// Campaign composition and statistics from the example, inside the closed window.
fn live_responses() -> Vec<(u16, String)> {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../config/wb-ads-recommendations.example.json"
    ))
    .unwrap();
    let mut statistics = fixture["statistics"].clone();
    let closed = crate::reporting::advertising_history::last_closed_day();
    for (index, day) in statistics[0]["days"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .enumerate()
    {
        day["date"] = json!(format!(
            "{}T00:00:00Z",
            closed - chrono::Duration::days(i64::try_from(index).unwrap() + 4)
        ));
    }
    vec![
        (200, fixture["composition"].to_string()),
        (200, statistics.to_string()),
    ]
}

#[tokio::test]
async fn recommendations_read_live_scope_once_and_keep_missing_fbw_unknown() {
    let (server, requests) = mock_wb_server_with_responses("admin", live_responses());
    let reads = Arc::new(FakeReportingRepository::succeeding());
    let server = server.with_reporting_reader(ReportingReader::from_repository(reads.clone()));
    let report = server
        .wb_ads_recommendations(RequestIdentity::dev(), Parameters(input()))
        .await
        .unwrap()
        .0;
    assert_eq!(report.account_id, "account_wb");
    assert_eq!(report.recommendations[0].fbw_units, None);
    assert_eq!(
        report.recommendations[0].windows[1].metrics.spend_minor,
        15000
    );
    assert_eq!(
        report.recommendations[0].action,
        crate::reporting::ads_optimizer::wb_recommendations::Action::Hold
    );
    assert!(!report.auto_apply_allowed);
    assert_eq!(reads.calls(), 2);
    let details = requests.recv_timeout(Duration::from_secs(3)).unwrap();
    let history = requests.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(details.starts_with("GET /api/advert/v2/adverts?"));
    assert!(history.starts_with("GET /adv/v3/fullstats?"));
    assert!(requests.try_recv().is_err());
}

#[test]
fn recommendations_are_live_read_only_and_absent_in_credentialless_reporting_mode() {
    let tool = server()
        .tool_router
        .list_all()
        .into_iter()
        .find(|tool| tool.name == "wb_ads_recommendations")
        .unwrap();
    assert_eq!(tool.annotations.unwrap().read_only_hint, Some(true));
    assert_eq!(tool.input_schema["additionalProperties"], json!(false));
    assert!(
        !server()
            .into_reporting_only()
            .unwrap()
            .tool_router
            .list_all()
            .iter()
            .any(|tool| tool.name == "wb_ads_recommendations")
    );
}

/// The tool's error text; its success type intentionally has no `Debug`.
async fn rejection(server: &OzonMcp, input: WbAdsRecommendationsInput) -> String {
    match server
        .wb_ads_recommendations(RequestIdentity::dev(), Parameters(input))
        .await
    {
        Ok(_) => panic!("the recommendation request must be rejected"),
        Err(error) => error,
    }
}

/// Serves stock snapshot pages by offset; every other source is missing.
struct PagedStocks {
    pages: Vec<(u32, SourceSnapshotResult)>,
    queries: Mutex<Vec<SourceSnapshotQuery>>,
}

impl PagedStocks {
    fn new(pages: Vec<(u32, SourceSnapshotResult)>) -> Arc<Self> {
        Arc::new(Self {
            pages,
            queries: Mutex::new(Vec::new()),
        })
    }

    fn queries(&self) -> Vec<(SnapshotSource, Option<i64>, u32)> {
        self.queries
            .lock()
            .unwrap()
            .iter()
            .map(|query| (query.source, query.snapshot_id, query.offset))
            .collect()
    }
}

fn disabled<'a, T: Send + 'a>() -> ReportingReadFuture<'a, T> {
    Box::pin(async { Err(ReportingReadError::Disabled) })
}

impl ReportingReadRepository for PagedStocks {
    fn enabled(&self) -> bool {
        true
    }

    fn source_snapshot<'a>(
        &'a self,
        account: &'a AccountScope,
        query: SourceSnapshotQuery,
    ) -> ReportingReadFuture<'a, SourceSnapshotResult> {
        self.queries.lock().unwrap().push(query);
        let page = if query.source == SnapshotSource::Stocks {
            self.pages
                .iter()
                .find(|(offset, _)| *offset == query.offset)
                .map(|(_, page)| page.clone())
                .expect("the tool requested an unserved stock page")
        } else {
            let mut missing = stock_page(Vec::new(), 0, None);
            missing.account_id = account.account_id().to_owned();
            missing.source = query.source;
            missing.quality = None;
            missing.snapshot_id = None;
            missing
        };
        Box::pin(async move { Ok(page) })
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

    fn sales_analytics<'a>(
        &'a self,
        _account: &'a AccountScope,
        _query: SalesAnalyticsQuery,
    ) -> ReportingReadFuture<'a, SalesAnalyticsResult> {
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

fn stock_row(warehouse_id: &str, sellable_units: u64) -> Value {
    json!({"account_id":"account_wb","marketplace":"wildberries","snapshot_id":101,
        "sku":99,"warehouse_id":warehouse_id,"sellable_units":sellable_units})
}

/// A complete, fresh FBW stock page of published snapshot 101.
fn stock_page(rows: Vec<Value>, total_rows: u64, next_offset: Option<u32>) -> SourceSnapshotResult {
    SourceSnapshotResult {
        account_id: "account_wb".to_owned(),
        marketplace: ReadMarketplace::Wildberries,
        source: SnapshotSource::Stocks,
        storage: "published_postgresql_snapshots".to_owned(),
        state: "published".to_owned(),
        data_state: "complete".to_owned(),
        catalog_scope: None,
        quality: Some(DataQuality::Complete),
        inventory_scope: Some("fbw".to_owned()),
        pagination_complete: Some(true),
        coverage: None,
        snapshot_id: Some("101".to_owned()),
        cutoff_at: None,
        source_as_of: Some("2026-10-18T08:00:00Z".to_owned()),
        observed_from: None,
        period_start: None,
        period_end: None,
        total_rows,
        rows,
        next_offset,
        latest_collection: None,
    }
}

/// Two consistent pages; the stock is only fresh relative to `as_of`.
fn two_stock_pages(as_of: &str) -> (SourceSnapshotResult, SourceSnapshotResult) {
    let mut first = stock_page(vec![stock_row("wb:123", 20)], 2, Some(1));
    first.source_as_of = Some(as_of.to_owned());
    let mut second = first.clone();
    second.rows = vec![stock_row("wb:456", 5)];
    second.next_offset = None;
    (first, second)
}

#[tokio::test]
async fn recommendations_merge_every_page_of_one_pinned_stock_snapshot() {
    let as_of = (Utc::now() - chrono::Duration::minutes(5)).to_rfc3339();
    let (first, second) = two_stock_pages(&as_of);
    let reads = PagedStocks::new(vec![(0, first), (1, second)]);
    let (server, requests) = mock_wb_server_with_responses("admin", live_responses());
    let server = server.with_reporting_reader(ReportingReader::from_repository(reads.clone()));
    let report = server
        .wb_ads_recommendations(RequestIdentity::dev(), Parameters(input()))
        .await
        .unwrap()
        .0;
    assert_eq!(report.recommendations[0].fbw_units, Some(25));
    assert_eq!(
        reads.queries(),
        vec![
            (SnapshotSource::Stocks, None, 0),
            (SnapshotSource::Stocks, Some(101), 1),
            (SnapshotSource::Sales, None, 0),
        ]
    );
    requests.recv_timeout(Duration::from_secs(3)).unwrap();
    requests.recv_timeout(Duration::from_secs(3)).unwrap();
}

#[tokio::test]
async fn recommendations_reject_unpinned_inconsistent_or_oversized_snapshots_before_wb_io() {
    let (first, second) = two_stock_pages("2026-10-18T08:00:00Z");
    let mut oversized = first.clone();
    oversized.total_rows = 25_001;
    let mut unparsable = first.clone();
    unparsable.snapshot_id = Some("latest".to_owned());
    let mut refreshed = second.clone();
    refreshed.snapshot_id = Some("102".to_owned());
    let mut repeated = second.clone();
    repeated.next_offset = Some(1);
    let mut empty = second;
    empty.rows.clear();
    let truncated = stock_page(vec![stock_row("wb:123", 20)], 2, None);
    let cases = [
        ("WB_RECOMMENDATIONS_LIMIT", vec![(0, oversized)]),
        ("WB_RECOMMENDATIONS_INVALID_SNAPSHOT", vec![(0, unparsable)]),
        (
            "WB_RECOMMENDATIONS_INVALID_SNAPSHOT",
            vec![(0, first.clone()), (1, refreshed)],
        ),
        (
            "WB_RECOMMENDATIONS_INVALID_SNAPSHOT",
            vec![(0, first.clone()), (1, repeated)],
        ),
        (
            "WB_RECOMMENDATIONS_INVALID_SNAPSHOT",
            vec![(0, first), (1, empty)],
        ),
        ("WB_RECOMMENDATIONS_INVALID_SNAPSHOT", vec![(0, truncated)]),
    ];
    for (code, pages) in cases {
        let (server, requests) = mock_wb_server_for("admin", 0);
        let server =
            server.with_reporting_reader(ReportingReader::from_repository(PagedStocks::new(pages)));
        let error = rejection(&server, input()).await;
        assert!(error.starts_with(code), "{code}: {error}");
        assert!(requests.try_recv().is_err());
    }
}

#[tokio::test]
async fn recommendations_require_a_wildberries_cabinet_before_any_read() {
    let reads = Arc::new(FakeReportingRepository::succeeding());
    let (server, requests) = mock_wb_server_for("admin", 0);
    let server = server.with_reporting_reader(ReportingReader::from_repository(reads.clone()));
    let mut ozon = input();
    ozon.account = Some("store_a".into());
    let error = rejection(&server, ozon).await;
    assert!(error.starts_with(REPORTING_INVALID_REQUEST), "{error}");
    assert_eq!(reads.calls(), 0);
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn recommendations_name_the_promotion_endpoint_that_failed() {
    let mut responses = live_responses();
    let composition = responses.remove(0);
    for (upstream, label) in [
        (
            vec![(500, "{}".to_owned())],
            "promotion:/api/advert/v2/adverts",
        ),
        (
            vec![composition, (500, "{}".to_owned())],
            "promotion:/adv/v3/fullstats",
        ),
    ] {
        let (server, _requests) = mock_wb_server_with_responses("admin", upstream);
        let server = server.with_reporting_reader(ReportingReader::from_repository(Arc::new(
            FakeReportingRepository::succeeding(),
        )));
        let error = rejection(&server, input()).await;
        assert!(error.starts_with(WB_TOOL_FAILURE), "{error}");
        assert!(error.contains(&format!("endpoint={label}")), "{error}");
    }
}

#[tokio::test]
async fn recommendations_reject_a_stock_snapshot_from_another_cabinet() {
    let mut foreign = stock_page(vec![stock_row("wb:123", 20)], 1, None);
    foreign.account_id = "account_other".to_owned();
    foreign.source_as_of = Some(Utc::now().to_rfc3339());
    let (server, _requests) = mock_wb_server_with_responses("admin", live_responses());
    let server = server.with_reporting_reader(ReportingReader::from_repository(PagedStocks::new(
        vec![(0, foreign)],
    )));
    let error = rejection(&server, input()).await;
    assert!(
        error.starts_with("WB_RECOMMENDATIONS_INVALID_EVIDENCE"),
        "{error}"
    );
}
