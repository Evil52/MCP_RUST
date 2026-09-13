use std::{collections::VecDeque, sync::Mutex};

use super::*;
use crate::reporting::checkpoint::{
    CheckpointError,
    tests::{MemoryPages, journal},
};

const ACCOUNT: &str = "ofk_region_wb";
const REPORT_ID: u64 = 9_007_199_254_740_993;

#[derive(Debug, PartialEq, Eq)]
enum Request {
    List(NaiveDate, NaiveDate, WbFinanceReportPeriod, u32, u32),
    Details(u64, u32, u64),
}

struct FixtureTransport {
    responses: Mutex<VecDeque<Result<Option<Value>, WbReportSourceError>>>,
    requests: Mutex<Vec<Request>>,
}

impl FixtureTransport {
    fn new(responses: Vec<Option<Value>>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().map(Ok).collect()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn reply(&self, request: Request) -> WbOfficialReportFuture<'_> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(request);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Err(WbReportSourceError::InvalidResponse))
        })
    }
}

impl WbOfficialReportTransport for FixtureTransport {
    fn account_id(&self) -> &str {
        ACCOUNT
    }

    fn list_reports(
        &self,
        from: NaiveDate,
        to: NaiveDate,
        period: WbFinanceReportPeriod,
        limit: u32,
        offset: u32,
    ) -> WbOfficialReportFuture<'_> {
        self.reply(Request::List(from, to, period, limit, offset))
    }

    fn report_details(
        &self,
        report_id: u64,
        limit: u32,
        rrd_id: u64,
    ) -> WbOfficialReportFuture<'_> {
        self.reply(Request::Details(report_id, limit, rrd_id))
    }
}

fn date(day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, day).unwrap()
}

fn summary(report_id: u64) -> Value {
    json!({
        "reportId": report_id,
        "dateFrom": "2026-09-01", "dateTo": "2026-09-07", "createDate": "2026-09-08",
        "currency": "RUB", "reportType": 1,
        "retailAmountSum": "9007199254740993.01", "forPaySum": "101.349",
        "deliveryServiceSum": "1.349", "paidStorageSum": null,
        "sellerFinanceName": "private seller data",
        "other": {"http": "http://127.0.0.1:9000/secret"}
    })
}

fn detail(rrd_id: u64) -> Value {
    json!({
        "rrdId": rrd_id, "reportId": REPORT_ID, "rrDate": "2026-08-31",
        "currency": "RUB", "nmId": 11, "docTypeName": "Продажа",
        "sellerOperName": "Корректировка продаж", "quantity": 1,
        "retailAmount": "100.01", "forPay": "10.01",
        "ppvzSupplierName": "private third-party name"
    })
}

async fn collect_list(
    transport: &dyn WbOfficialReportTransport,
    checkpoints: &Checkpoints,
) -> Result<WbReportList, WbReportSourceError> {
    collect_report_list_checkpointed(
        transport,
        ACCOUNT,
        date(1),
        date(8),
        WbFinanceReportPeriod::Weekly,
        checkpoints,
    )
    .await
}

async fn selected() -> WbSelectedReport {
    let transport = FixtureTransport::new(vec![Some(json!([summary(REPORT_ID)])), None]);
    collect_list(&transport, &None)
        .await
        .unwrap()
        .select_closed(REPORT_ID, "RUB", date(13))
        .unwrap()
}

#[tokio::test]
async fn list_offsets_require_terminal_and_normalize_exactly_without_pii() {
    let transport = FixtureTransport::new(vec![
        Some(json!([summary(REPORT_ID)])),
        Some(json!([summary(18)])),
        None,
    ]);
    let list = collect_list(&transport, &None).await.unwrap();
    assert_eq!(list.reports().len(), 2);
    assert_eq!(list.reports()[1].scope.report_id, REPORT_ID);
    let selected = list.select_closed(REPORT_ID, "RUB", date(13)).unwrap();
    assert_eq!(
        selected.summary().amounts["retailAmountSum"].units,
        900_719_925_474_099_301
    );
    assert_eq!(selected.summary().amounts["deliveryServiceSum"].scale, 3);
    assert!(!selected.summary().amounts.contains_key("paidStorageSum"));
    let normalized = serde_json::to_string(&selected).unwrap();
    for forbidden in ["private", "sellerFinanceName", "other", "127.0.0.1"] {
        assert!(!normalized.contains(forbidden));
    }
    assert_eq!(
        *transport.requests.lock().unwrap(),
        vec![
            Request::List(date(1), date(8), WbFinanceReportPeriod::Weekly, 1_000, 0),
            Request::List(date(1), date(8), WbFinanceReportPeriod::Weekly, 1_000, 1),
            Request::List(date(1), date(8), WbFinanceReportPeriod::Weekly, 1_000, 2),
        ]
    );
}

#[tokio::test]
async fn by_id_keeps_historical_corrections_and_has_separate_provenance() {
    let selected = selected().await;
    let transport = FixtureTransport::new(vec![
        Some(json!([detail(4)])),
        Some(json!([detail(9)])),
        None,
    ]);
    let report = WbCompleteReportDetails::collect(&transport, &selected, &None)
        .await
        .unwrap();
    assert_eq!(report.rows().len(), 2);
    assert!(report.rows()[0].business_date < report.evidence().scope.date_from);
    assert!(report.evidence().terminal_observed && report.evidence().covers_entire_report);
    assert_eq!(report.evidence().scope, selected.evidence().scope);
    assert_ne!(
        report.evidence().source_sha256,
        selected.evidence().source_sha256
    );
    assert_ne!(
        report.evidence().observation_id,
        selected.evidence().observation_id
    );
    assert_eq!(
        selected.baseline().kind,
        WbFinanceBaselineKind::OfficialReportSummary
    );
    assert_eq!(selected.baseline().totals["forPaySum"].units, 101_349);
    assert!(!serde_json::to_string(&report).unwrap().contains("private"));
    assert_eq!(
        *transport.requests.lock().unwrap(),
        vec![
            Request::Details(REPORT_ID, 1_000, 0),
            Request::Details(REPORT_ID, 1_000, 4),
            Request::Details(REPORT_ID, 1_000, 9),
        ]
    );
}

#[tokio::test]
async fn unclosed_wrong_id_currency_or_not_yet_created_report_cannot_be_selected() {
    let transport = FixtureTransport::new(vec![Some(json!([summary(REPORT_ID)])), None]);
    let list = collect_list(&transport, &None).await.unwrap();
    for (id, currency, as_of) in [
        (REPORT_ID, "RUB", date(7)),
        (REPORT_ID, "USD", date(13)),
        (10, "RUB", date(13)),
        (0, "RUB", date(13)),
        (REPORT_ID, "rub", date(13)),
    ] {
        assert!(list.select_closed(id, currency, as_of).is_err());
    }
    let mut future_created = summary(REPORT_ID);
    future_created["createDate"] = json!("2026-09-14");
    let transport = FixtureTransport::new(vec![Some(json!([future_created])), None]);
    assert!(
        collect_list(&transport, &None)
            .await
            .unwrap()
            .select_closed(REPORT_ID, "RUB", date(13))
            .is_err()
    );
}

#[tokio::test]
async fn malformed_empty_or_duplicate_lists_never_become_completed_evidence() {
    for response in [
        Value::Null,
        json!([]),
        json!({"data": []}),
        json!([summary(REPORT_ID), summary(REPORT_ID)]),
    ] {
        let transport = FixtureTransport::new(vec![Some(response), None]);
        assert!(matches!(
            collect_list(&transport, &None).await,
            Err(WbReportSourceError::InvalidResponse)
        ));
    }
    let transport = FixtureTransport::new(vec![
        Some(json!([summary(REPORT_ID)])),
        Some(json!([summary(REPORT_ID)])),
        None,
    ]);
    assert!(matches!(
        collect_list(&transport, &None).await,
        Err(WbReportSourceError::InvalidResponse)
    ));
    assert!(
        collect_list(&FixtureTransport::new(vec![None]), &None)
            .await
            .unwrap()
            .reports()
            .is_empty()
    );
}

#[tokio::test]
async fn mismatched_account_is_rejected_before_any_departure() {
    let transport = FixtureTransport::new(vec![]);
    assert!(matches!(
        collect_report_list_checkpointed(
            &transport,
            "another_account",
            date(1),
            date(8),
            WbFinanceReportPeriod::Weekly,
            &None,
        )
        .await,
        Err(WbReportSourceError::InvalidSnapshotInput)
    ));
    let mut selected = selected().await;
    selected.summary.scope.account_id = "another_account".into();
    assert!(matches!(
        WbCompleteReportDetails::collect(&transport, &selected, &None).await,
        Err(WbReportSourceError::InvalidSnapshotInput)
    ));
    assert!(transport.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn discovery_rejects_unbounded_requests_and_unrelated_report_intervals() {
    let transport = FixtureTransport::new(vec![]);
    for (from, to) in [
        (date(8), date(1)),
        (date(1), NaiveDate::from_ymd_opt(2026, 10, 2).unwrap()),
        (NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(), date(1)),
    ] {
        assert!(
            collect_report_list_checkpointed(
                &transport,
                ACCOUNT,
                from,
                to,
                WbFinanceReportPeriod::Weekly,
                &None,
            )
            .await
            .is_err()
        );
    }
    assert!(transport.requests.lock().unwrap().is_empty());
    let transport = FixtureTransport::new(vec![Some(json!([summary(REPORT_ID)])), None]);
    assert!(matches!(
        collect_report_list_checkpointed(
            &transport,
            ACCOUNT,
            date(9),
            date(10),
            WbFinanceReportPeriod::Weekly,
            &None,
        )
        .await,
        Err(WbReportSourceError::InvalidResponse)
    ));
    // Partial overlap is normal: the API's weekly example straddles a query.
    let transport = FixtureTransport::new(vec![Some(json!([summary(REPORT_ID)])), None]);
    assert_eq!(
        collect_report_list_checkpointed(
            &transport,
            ACCOUNT,
            date(3),
            date(4),
            WbFinanceReportPeriod::Weekly,
            &None,
        )
        .await
        .unwrap()
        .reports()
        .len(),
        1
    );
}

#[test]
fn malformed_ids_dates_types_and_float_amounts_are_rejected() {
    for (field, value) in [
        ("reportId", json!(0)),
        ("reportId", json!(u64::MAX)),
        ("reportId", json!("9007199254740993")),
        ("reportId", json!(1.5)),
        ("retailAmountSum", json!(0.1)),
        ("retailAmountSum", json!("1e2")),
        (
            "retailAmountSum",
            json!("9999999999999999999999999999999999999999"),
        ),
        ("dateFrom", json!("2024-12-31")),
        ("dateFrom", json!("2026-9-01")),
        ("dateTo", json!("2026-10-02")),
        ("dateTo", json!("2026-08-31")),
        ("reportType", json!(-1)),
        ("reportType", json!(1.1)),
        ("currency", json!("rub")),
        ("createDate", json!("2026-09-08T00:00:00Z")),
    ] {
        let mut malformed = summary(REPORT_ID);
        malformed[field] = value;
        assert!(
            parse_summary(&malformed, ACCOUNT, WbFinanceReportPeriod::Weekly).is_err(),
            "{field}"
        );
    }
}

#[tokio::test]
async fn details_reject_wrong_scope_bad_cursors_and_empty_200() {
    let selected = selected().await;
    for (field, value) in [("reportId", json!(10)), ("currency", json!("USD"))] {
        let mut wrong = detail(1);
        wrong[field] = value;
        let transport = FixtureTransport::new(vec![Some(json!([wrong])), None]);
        assert!(
            WbCompleteReportDetails::collect(&transport, &selected, &None)
                .await
                .is_err()
        );
    }
    for responses in [
        vec![Some(json!([detail(1), detail(1)])), None],
        vec![Some(json!([detail(2), detail(1)])), None],
        vec![Some(json!([detail(1)])), Some(json!([detail(1)])), None],
        vec![Some(json!([])), None],
        vec![Some(Value::Null), None],
    ] {
        assert!(
            WbCompleteReportDetails::collect(&FixtureTransport::new(responses), &selected, &None)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn checkpoint_resume_preserves_normalization_and_explicit_terminal_proof() {
    let pages = MemoryPages::default();
    let transport = FixtureTransport::new(vec![Some(json!([summary(REPORT_ID)])), None]);
    assert!(matches!(
        collect_list(&transport, &journal(&pages)).await,
        Err(WbReportSourceError::Checkpoint(CheckpointError::Deferred))
    ));
    assert_eq!(transport.requests.lock().unwrap().len(), 1);
    let saved = serde_json::to_string(&*pages.lock().unwrap()).unwrap();
    assert!(!saved.contains("private") && !saved.contains("127.0.0.1"));
    let list = collect_list(&transport, &journal(&pages)).await.unwrap();
    let selected = list.select_closed(REPORT_ID, "RUB", date(13)).unwrap();
    let details = FixtureTransport::new(vec![Some(json!([detail(1)])), None]);
    assert!(matches!(
        WbCompleteReportDetails::collect(&details, &selected, &journal(&pages)).await,
        Err(WbReportSourceError::Checkpoint(CheckpointError::Deferred))
    ));
    let completed = WbCompleteReportDetails::collect(&details, &selected, &journal(&pages))
        .await
        .unwrap();
    assert_eq!(completed.rows().len(), 1);
    assert_eq!(details.requests.lock().unwrap().len(), 2);
    assert_eq!(pages.lock().unwrap().len(), 4);
    let resumed = WbCompleteReportDetails::collect(&details, &selected, &journal(&pages))
        .await
        .unwrap();
    assert_eq!(resumed.evidence(), completed.evidence());
    assert_eq!(details.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn poisoned_normalized_checkpoint_is_revalidated_before_completion() {
    let pages = MemoryPages::default();
    let transport = FixtureTransport::new(vec![Some(json!([summary(REPORT_ID)])), None]);
    assert!(collect_list(&transport, &journal(&pages)).await.is_err());
    {
        let mut pages = pages.lock().unwrap();
        pages.values_mut().next().unwrap()[0]["scope"]["account_id"] = json!("other_account");
    }
    assert!(matches!(
        collect_list(&transport, &journal(&pages)).await,
        Err(WbReportSourceError::InvalidResponse)
    ));
    assert_eq!(transport.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn rows_exceeding_capacity_never_produce_complete_report() {
    let selected = selected().await;
    let responses = (0..26)
        .map(|page| {
            Some(json!(
                (1..=1_000)
                    .map(|row| detail(page * 1_000 + row))
                    .collect::<Vec<_>>()
            ))
        })
        .chain(std::iter::once(None))
        .collect();
    let transport = FixtureTransport::new(responses);
    assert!(matches!(
        WbCompleteReportDetails::collect(&transport, &selected, &None).await,
        Err(WbReportSourceError::PaginationLimit)
    ));
    assert_eq!(transport.requests.lock().unwrap().len(), 26);
}

#[tokio::test]
async fn revised_summary_cannot_replay_old_detail_checkpoints() {
    let selected = selected().await;
    let pages = MemoryPages::default();
    let first = FixtureTransport::new(vec![Some(json!([detail(1)])), None]);
    assert!(
        WbCompleteReportDetails::collect(&first, &selected, &journal(&pages))
            .await
            .is_err()
    );
    WbCompleteReportDetails::collect(&first, &selected, &journal(&pages))
        .await
        .unwrap();
    let mut changed_summary = summary(REPORT_ID);
    changed_summary["retailAmountSum"] = json!("100.01");
    let list_transport = FixtureTransport::new(vec![Some(json!([changed_summary])), None]);
    let revised = collect_list(&list_transport, &None)
        .await
        .unwrap()
        .select_closed(REPORT_ID, "RUB", date(13))
        .unwrap();
    let new_details = FixtureTransport::new(vec![None]);
    let complete = WbCompleteReportDetails::collect(&new_details, &revised, &journal(&pages))
        .await
        .unwrap();
    assert!(complete.rows().is_empty());
    assert_eq!(new_details.requests.lock().unwrap().len(), 1);
}
