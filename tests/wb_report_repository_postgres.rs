use std::{collections::VecDeque, fmt::Write, sync::Mutex};

use chrono::NaiveDate;
use mcp_ozon::reporting::{
    finance_reconciliation::{ExactFinanceTotal, WbFinanceReportPeriod},
    wb_official_reconciliation::{
        WbOfficialComparisonStatus, WbOfficialUnavailable, reconcile_wb_official_report,
    },
    wb_report_repository::{
        PostgresWbReportReader, PostgresWbReportRepository, WbReportRepositoryError,
    },
    wb_report_source::{
        WbCompleteReportDetails, WbOfficialReportFuture, WbOfficialReportTransport,
        WbSelectedReport, collect_report_list_checkpointed,
    },
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Config, NoTls, error::SqlState};

// The real collector role has a deliberately small connection limit. Serialize
// independent fixtures; the atomic-publication fixture still races two writers.
static POSTGRES_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const REPORT_ID: u64 = 9_007_199_254_740_993;
const INVOKE: &str = "SELECT * FROM daily_reporting.publish_wb_official_report($1,$2,$3)";

struct Pages {
    account: String,
    responses: Mutex<VecDeque<Option<Value>>>,
}

impl Pages {
    fn reply(&self) -> WbOfficialReportFuture<'_> {
        Box::pin(async {
            Ok(self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected request"))
        })
    }
}

impl WbOfficialReportTransport for Pages {
    fn account_id(&self) -> &str {
        &self.account
    }
    fn list_reports(
        &self,
        _: NaiveDate,
        _: NaiveDate,
        _: WbFinanceReportPeriod,
        _: u32,
        _: u32,
    ) -> WbOfficialReportFuture<'_> {
        self.reply()
    }
    fn report_details(&self, _: u64, _: u32, _: u64) -> WbOfficialReportFuture<'_> {
        self.reply()
    }
}

const fn date(day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2025, 9, day).unwrap()
}

fn account(label: &str) -> String {
    format!(
        "official-{}-{label}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_micros()
    )
}

fn summary() -> Value {
    json!({"reportId":REPORT_ID,"dateFrom":"2025-09-01","dateTo":"2025-09-07",
        "createDate":"2025-09-08","reportType":1,"currency":"RUB",
        "retailAmountSum":"89.50000","forPaySum":"71.600","bankPaymentSum":"1.11",
        "sellerFinanceName":"must not survive projection"})
}

fn raw_rows() -> Value {
    json!([
        {"rrdId":11,"reportId":REPORT_ID,"rrDate":"2025-09-02","currency":"RUB",
         "docTypeName":"Продажа","sellerOperName":"Продажа","nmId":7,"retailAmount":"100","forPay":"80"},
        {"rrdId":12,"reportId":REPORT_ID,"rrDate":"2025-09-03","currency":"RUB",
         "docTypeName":"Возврат","sellerOperName":"Возврат","nmId":7,"retailAmount":"10","forPay":"8"},
        {"rrdId":13,"reportId":REPORT_ID,"rrDate":"2024-09-04","currency":"RUB",
         "docTypeName":"Продажа","sellerOperName":"Коррекция продаж","nmId":7,"retailAmount":"-1","forPay":"-0.8"},
        {"rrdId":14,"reportId":REPORT_ID,"rrDate":"2025-09-05","currency":"RUB",
         "docTypeName":"Возврат","sellerOperName":"Коррекция продаж","nmId":7,"retailAmount":"-0.5","forPay":"-0.4"},
        {"rrdId":15,"reportId":REPORT_ID,"rrDate":"2025-09-06","currency":"RUB",
         "docTypeName":"","sellerOperName":"Логистика","nmId":0,"retailAmount":"0","forPay":"0",
         "deliveryService":"-0.000000001","buyer_phone":"must not survive projection"}
    ])
}

async fn collected(
    cabinet: &str,
    summary: Value,
    rows: Option<Value>,
    period: WbFinanceReportPeriod,
) -> (WbSelectedReport, WbCompleteReportDetails) {
    let mut responses = VecDeque::from([Some(json!([summary])), None]);
    if let Some(rows) = rows {
        responses.push_back(Some(rows));
    }
    responses.push_back(None);
    let pages = Pages {
        account: cabinet.into(),
        responses: Mutex::new(responses),
    };
    let list = collect_report_list_checkpointed(&pages, cabinet, date(1), date(7), period, &None)
        .await
        .unwrap();
    let selected = list.select_closed(REPORT_ID, "RUB", date(9)).unwrap();
    let details = WbCompleteReportDetails::collect(&pages, &selected, &None)
        .await
        .unwrap();
    (selected, details)
}

fn urls() -> Option<(String, String)> {
    Some((
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL").ok()?,
        std::env::var("POSITION_REPOSITORY_TEST_READER_URL").ok()?,
    ))
}

async fn connect(url: &str) -> Client {
    let (client, connection) = url.parse::<Config>().unwrap().connect(NoTls).await.unwrap();
    std::mem::drop(tokio::spawn(async move {
        let _ = connection.await;
    }));
    client
}

const fn canonical(value: ExactFinanceTotal) -> i128 {
    value.units * 10_i128.pow(18 - value.scale)
}

#[tokio::test]
async fn official_report_preserves_sixteen_decimal_digits_and_coefficients_above_i64() {
    let Some((writer_url, reader_url)) = urls() else {
        return;
    };
    let _test_lock = POSTGRES_TEST_LOCK.lock().await;
    let writer = PostgresWbReportRepository::connect(&writer_url.parse().unwrap())
        .await
        .unwrap();
    let reader = PostgresWbReportReader::connect(&reader_url.parse().unwrap())
        .await
        .unwrap();
    let cabinet = account("precision");
    let mut summary = summary();
    summary["retailAmountSum"] = json!("1234.1234567890123456");
    summary["forPaySum"] = json!("1123.9876543210123456");
    let rows = json!([{"rrdId":11,"reportId":REPORT_ID,"rrDate":"2025-09-02","currency":"RUB",
        "docTypeName":"Продажа","retailAmount":"1234.1234567890123456",
        "forPay":"1123.9876543210123456","vw":"-9999.123456789012345678"}]);
    let (selected, details) =
        collected(&cabinet, summary, Some(rows), WbFinanceReportPeriod::Weekly).await;
    writer
        .publish(&selected, &details, "employee")
        .await
        .unwrap();
    let stored = reader.read(&cabinet, REPORT_ID).await.unwrap().unwrap();
    assert_eq!(
        stored.comparison.status,
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    );
    assert_eq!(
        stored.summary.amounts["retailAmountSum"].units,
        12_341_234_567_890_123_456_i128
    );
    assert_eq!(stored.comparison.columns[0].detail_total.unwrap().scale, 18);
    assert_eq!(
        stored.comparison.columns[0].detail_total.unwrap().units,
        1_234_123_456_789_012_345_600_i128
    );
    let stored_rows = reader
        .read_rows(&cabinet, REPORT_ID, 0, 1000)
        .await
        .unwrap();
    assert_eq!(stored_rows, details.rows());
    assert_eq!(
        stored_rows[0].amounts["vw"].units,
        -9_999_123_456_789_012_345_678_i128
    );
    assert_eq!(stored_rows[0].amounts["vw"].scale, 18);
    let json = serde_json::to_value(&stored_rows).unwrap();
    assert_eq!(json[0]["amounts"]["vw"]["units"], "-9999123456789012345678");
}

#[tokio::test]
async fn official_report_atomic_roundtrip_replay_revision_and_cabinet_scope() {
    let Some((writer_url, reader_url)) = urls() else {
        return;
    };
    let _test_lock = POSTGRES_TEST_LOCK.lock().await;
    let writer = PostgresWbReportRepository::connect(&writer_url.parse().unwrap())
        .await
        .unwrap();
    let second_writer = PostgresWbReportRepository::connect(&writer_url.parse().unwrap())
        .await
        .unwrap();
    let reader = PostgresWbReportReader::connect(&reader_url.parse().unwrap())
        .await
        .unwrap();
    let cabinet = account("roundtrip");
    let (selected, details) = collected(
        &cabinet,
        summary(),
        Some(raw_rows()),
        WbFinanceReportPeriod::Weekly,
    )
    .await;
    let (a, b) = tokio::join!(
        writer.publish(&selected, &details, "employee-one"),
        second_writer.publish(&selected, &details, "employee-two")
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.snapshot_id, b.snapshot_id);
    assert_ne!(a.already_present, b.already_present);
    let stored = reader.read(&cabinet, REPORT_ID).await.unwrap().unwrap();
    assert_eq!(stored.summary, selected.summary().clone());
    assert_eq!(stored.summary_evidence, selected.evidence().clone());
    assert_eq!(stored.details_evidence, details.evidence().clone());
    assert_eq!(stored.row_count, 5);
    assert!(matches!(
        stored.actor_id.as_str(),
        "employee-one" | "employee-two"
    ));
    assert_eq!(
        stored.comparison.status,
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    );
    let pure = reconcile_wb_official_report(
        details.rows(),
        details.evidence(),
        Some(&selected.baseline()),
    )
    .unwrap();
    for (sql, rust) in stored.comparison.columns.iter().zip(&pure.columns) {
        assert_eq!(sql.status, rust.status);
        assert_eq!(
            sql.detail_total.map(canonical),
            rust.detail_total.map(canonical)
        );
        assert_eq!(
            sql.summary_total.map(canonical),
            rust.summary_total.map(canonical)
        );
        assert_eq!(
            sql.difference.map(canonical),
            rust.difference.map(canonical)
        );
    }
    assert_eq!(
        reader
            .read_rows(&cabinet, REPORT_ID, 0, 1000)
            .await
            .unwrap(),
        details.rows()
    );
    assert_eq!(
        reader.read_rows(&cabinet, REPORT_ID, 12, 1).await.unwrap()[0].rrd_id,
        13
    );
    assert!(
        reader
            .read("another-account", REPORT_ID)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        reader
            .read_rows("another-account", REPORT_ID, 0, 1000)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reader
            .read_rows(&cabinet, REPORT_ID, 0, 1001)
            .await
            .unwrap_err(),
        WbReportRepositoryError::InvalidInput
    );
    let mut changed = raw_rows();
    changed[0]["forPay"] = json!("81");
    let (revised, revised_details) = collected(
        &cabinet,
        summary(),
        Some(changed),
        WbFinanceReportPeriod::Weekly,
    )
    .await;
    assert_eq!(
        writer
            .publish(&revised, &revised_details, "employee-one")
            .await,
        Err(WbReportRepositoryError::RevisionConflict)
    );
    assert_eq!(
        reader
            .read(&cabinet, REPORT_ID)
            .await
            .unwrap()
            .unwrap()
            .content_sha256,
        stored.content_sha256
    );
    let serialized = serde_json::to_string(&stored).unwrap();
    assert!(!serialized.contains("must not survive"));
    assert!(!serialized.contains("buyer_phone"));
}

#[tokio::test]
async fn official_report_sql_and_rust_preserve_unavailable_and_mismatch_semantics() {
    let Some((writer_url, reader_url)) = urls() else {
        return;
    };
    let _test_lock = POSTGRES_TEST_LOCK.lock().await;
    let writer = PostgresWbReportRepository::connect(&writer_url.parse().unwrap())
        .await
        .unwrap();
    let reader = PostgresWbReportReader::connect(&reader_url.parse().unwrap())
        .await
        .unwrap();
    let mut missing = raw_rows();
    missing[0]["retailAmount"] = Value::Null;
    let mut unknown = raw_rows();
    unknown[0]["docTypeName"] = json!("продажа");
    let mut mismatch = summary();
    mismatch["forPaySum"] = json!("71.61");
    let mut zero = summary();
    zero["forPaySum"] = json!("0");
    zero["retailAmountSum"] = json!("0");
    let mut missing_summary = summary();
    missing_summary.as_object_mut().unwrap().remove("forPaySum");
    for (label, summary, rows, period, reason) in [
        (
            "missing",
            summary(),
            Some(missing),
            WbFinanceReportPeriod::Weekly,
            Some(WbOfficialUnavailable::MissingDetailAmount),
        ),
        (
            "unknown",
            summary(),
            Some(unknown),
            WbFinanceReportPeriod::Weekly,
            Some(WbOfficialUnavailable::UnsupportedDocumentType),
        ),
        (
            "mismatch",
            mismatch,
            Some(raw_rows()),
            WbFinanceReportPeriod::Weekly,
            None,
        ),
        (
            "daily",
            summary(),
            Some(raw_rows()),
            WbFinanceReportPeriod::Daily,
            Some(WbOfficialUnavailable::UnsupportedPeriod),
        ),
        (
            "empty",
            zero,
            None,
            WbFinanceReportPeriod::Weekly,
            Some(WbOfficialUnavailable::EmptyDetails),
        ),
        (
            "missing-summary",
            missing_summary,
            Some(raw_rows()),
            WbFinanceReportPeriod::Weekly,
            Some(WbOfficialUnavailable::MissingSummaryAmount),
        ),
    ] {
        let cabinet = account(label);
        let (selected, details) = collected(&cabinet, summary, rows, period).await;
        writer
            .publish(&selected, &details, "employee")
            .await
            .unwrap();
        let stored = reader.read(&cabinet, REPORT_ID).await.unwrap().unwrap();
        let pure = reconcile_wb_official_report(
            details.rows(),
            details.evidence(),
            Some(&selected.baseline()),
        )
        .unwrap();
        assert_eq!(stored.comparison.status, pure.status);
        assert_eq!(stored.comparison.unavailable_reason, reason);
        assert_eq!(stored.comparison.columns.len(), pure.columns.len());
        for (sql, rust) in stored.comparison.columns.iter().zip(&pure.columns) {
            assert_eq!(sql.status, rust.status);
            assert_eq!(sql.unavailable_reason, rust.unavailable_reason);
            assert_eq!(
                sql.detail_total.map(canonical),
                rust.detail_total.map(canonical)
            );
            assert_eq!(
                sql.summary_total.map(canonical),
                rust.summary_total.map(canonical)
            );
            assert_eq!(
                sql.difference.map(canonical),
                rust.difference.map(canonical)
            );
        }
    }
}

fn envelope(selected: &WbSelectedReport, details: &WbCompleteReportDetails) -> Value {
    json!({"scope_json":serde_json::to_string(&selected.summary().scope).unwrap(),
        "summary_json":serde_json::to_string(selected.summary()).unwrap(),
        "rows_json":serde_json::to_string(details.rows()).unwrap(),
        "summary_evidence":selected.evidence(),"details_evidence":details.evidence()})
}

fn hash(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn rehash_evidence(envelope: &mut Value) {
    for (source, document, evidence) in [
        ("wb_report_summary_v1", "summary_json", "summary_evidence"),
        ("wb_report_id_details_v1", "rows_json", "details_evidence"),
    ] {
        let source_text = format!(
            "[\"{source}\",{},{}]",
            envelope["scope_json"].as_str().unwrap(),
            envelope[document].as_str().unwrap()
        );
        let digest = hash(source_text.as_bytes());
        envelope[evidence]["source_sha256"] = json!(digest);
        envelope[evidence]["observation_id"] = json!(format!("{source}_{digest}"));
    }
}

#[tokio::test]
async fn official_report_database_rejects_forged_partial_and_unknown_projection_atomically() {
    let Some((writer_url, reader_url)) = urls() else {
        return;
    };
    let _test_lock = POSTGRES_TEST_LOCK.lock().await;
    let writer = connect(&writer_url).await;
    let reader = PostgresWbReportReader::connect(&reader_url.parse().unwrap())
        .await
        .unwrap();
    let cabinet = account("boundary");
    let (selected, details) = collected(
        &cabinet,
        summary(),
        Some(raw_rows()),
        WbFinanceReportPeriod::Weekly,
    )
    .await;
    let good = envelope(&selected, &details);
    let mut partial = good.clone();
    partial["details_evidence"]["terminal_observed"] = json!(false);
    let mut subset = good.clone();
    subset["summary_evidence"]["covers_entire_report"] = json!(false);
    let mut forged_hash = good.clone();
    forged_hash["details_evidence"]["source_sha256"] = json!("00".repeat(32));
    let mut forged_scope = good.clone();
    forged_scope["summary_evidence"]["scope"]["account_id"] = json!("other-account");
    let mut injected = good.clone();
    injected["comparison_status"] = json!("primary_totals_match");
    let mut unknown = good.clone();
    let mut rows: Value = serde_json::from_str(unknown["rows_json"].as_str().unwrap()).unwrap();
    rows[1]["buyer_phone"] = json!("not allowed");
    unknown["rows_json"] = json!(serde_json::to_string(&rows).unwrap());
    rehash_evidence(&mut unknown);
    let mut duplicate = good.clone();
    rows = serde_json::from_str(duplicate["rows_json"].as_str().unwrap()).unwrap();
    rows[1]["rrd_id"] = rows[0]["rrd_id"].clone();
    duplicate["rows_json"] = json!(serde_json::to_string(&rows).unwrap());
    rehash_evidence(&mut duplicate);
    for bad in [
        partial,
        subset,
        forged_hash,
        forged_scope,
        injected,
        unknown,
        duplicate,
    ] {
        let payload = serde_json::to_string(&bad).unwrap();
        let digest = hash(payload.as_bytes());
        let error = writer
            .query(INVOKE, &[&"employee", &payload, &digest])
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::INVALID_PARAMETER_VALUE));
        assert!(reader.read(&cabinet, REPORT_ID).await.unwrap().is_none());
    }
    let payload = serde_json::to_string(&good).unwrap();
    let error = writer
        .query(INVOKE, &[&"employee", &payload, &"00".repeat(32)])
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::INVALID_PARAMETER_VALUE));
    let digest = hash(payload.as_bytes());
    writer
        .query(INVOKE, &[&"employee", &payload, &digest])
        .await
        .unwrap();
    assert_eq!(
        reader
            .read(&cabinet, REPORT_ID)
            .await
            .unwrap()
            .unwrap()
            .comparison
            .status,
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    );
}

#[tokio::test]
async fn official_report_roles_cannot_mutate_base_tables_or_publish_as_reader() {
    let Some((writer_url, reader_url)) = urls() else {
        return;
    };
    let _test_lock = POSTGRES_TEST_LOCK.lock().await;
    let writer = connect(&writer_url).await;
    let reader = connect(&reader_url).await;
    for client in [&writer, &reader] {
        for relation in [
            "wb_official_reports",
            "wb_official_report_rows",
            "wb_official_report_amounts",
            "wb_official_report_summary_amounts",
            "wb_official_report_comparisons",
        ] {
            for operation in ["SELECT * FROM", "DELETE FROM", "TRUNCATE"] {
                let error = client
                    .batch_execute(&format!("{operation} daily_reporting.{relation}"))
                    .await
                    .unwrap_err();
                assert!(matches!(
                    error.code(),
                    Some(&SqlState::INSUFFICIENT_PRIVILEGE | &SqlState::READ_ONLY_SQL_TRANSACTION)
                ));
            }
        }
    }
    let error = reader
        .query(INVOKE, &[&"employee", &"{}", &"00".repeat(32)])
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));
    assert!(matches!(
        PostgresWbReportRepository::connect(&reader_url.parse().unwrap()).await,
        Err(WbReportRepositoryError::Unavailable)
    ));
    assert!(matches!(
        PostgresWbReportReader::connect(&writer_url.parse().unwrap()).await,
        Err(WbReportRepositoryError::Unavailable)
    ));
}
