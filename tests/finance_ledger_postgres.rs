use std::{collections::VecDeque, fmt::Write, future::Future, pin::Pin, sync::Mutex};

use chrono::NaiveDate;
use mcp_ozon::reporting::{
    finance_ledger::{
        FinanceLedgerError, PostgresFinanceLedger, PostgresFinanceLedgerReader, WbFinanceBatch,
    },
    wb_finance_source::WbFinanceTransport,
    wb_source::WbReportSourceError,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Config, NoTls, error::SqlState};

struct Pages(Mutex<VecDeque<Result<Option<Value>, WbReportSourceError>>>);

impl WbFinanceTransport for Pages {
    fn finance_page<'a>(
        &'a self,
        _: NaiveDate,
        _: NaiveDate,
        _: u32,
        _: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, WbReportSourceError>> + Send + 'a>> {
        Box::pin(async { self.0.lock().unwrap().pop_front().expect("unexpected page") })
    }
}

const fn date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, 10).unwrap()
}

fn raw_rows() -> Value {
    json!([
        {"rrdId": 11, "reportId": 21, "rrDate": "2026-09-10", "nmId": 31,
         "currency": "RUB", "docTypeName": "Продажа", "quantity": 1,
         "forPay": "112.345", "retailAmount": "200.00", "deliveryService": "-0.000000001",
         "vw": "-9999.123456789012345678"},
        {"rrdId": 12, "reportId": 21, "rrDate": "2026-09-10", "nmId": 0,
         "currency": "RUB", "docTypeName": "Возврат", "quantity": -1,
         "forPay": "-25.1", "retailAmount": null}
    ])
}

async fn batch(account: String, rows: Value, from: NaiveDate, to: NaiveDate) -> WbFinanceBatch {
    let pages = Pages(Mutex::new(VecDeque::from([Ok(Some(rows)), Ok(None)])));
    WbFinanceBatch::collect(&pages, account, from, to, &None)
        .await
        .unwrap()
}

async fn connect(url: &str) -> Client {
    let (client, connection) = url.parse::<Config>().unwrap().connect(NoTls).await.unwrap();
    std::mem::drop(tokio::spawn(async move {
        let _ = connection.await;
    }));
    client
}

fn account(suffix: &str) -> String {
    format!(
        "ledger-{}-{suffix}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_micros()
    )
}

#[tokio::test]
async fn financial_ledger_preserves_decimals_rejects_revisions_and_isolates_accounts() {
    let (Ok(writer_url), Ok(reader_url)) = (
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
        std::env::var("POSITION_REPOSITORY_TEST_READER_URL"),
    ) else {
        return;
    };
    let writer = PostgresFinanceLedger::connect(&writer_url.parse().unwrap())
        .await
        .unwrap();
    let reader = PostgresFinanceLedgerReader::connect(&reader_url.parse().unwrap())
        .await
        .unwrap();
    let cabinet = account("roundtrip");
    let original = batch(cabinet.clone(), raw_rows(), date(), date()).await;
    let publication = writer.publish_wb(&original).await.unwrap();
    assert!(!publication.already_present);
    assert_eq!(publication.content_sha256.len(), 64);
    let replay = batch(cabinet.clone(), raw_rows(), date(), date()).await;
    let repeated = writer.publish_wb(&replay).await.unwrap();
    assert!(repeated.already_present);
    assert_eq!(repeated.batch_id, publication.batch_id);
    assert_eq!(repeated.content_sha256, publication.content_sha256);
    let stored = reader
        .read_wb_rows(&cabinet, publication.batch_id, 0, 1000)
        .await
        .unwrap();
    assert_eq!(stored, original.rows());
    assert!(!stored[1].amounts.contains_key("retailAmount"));
    assert_eq!(stored[0].amounts["deliveryService"].units, -1);
    assert_eq!(stored[0].amounts["deliveryService"].scale, 9);
    assert_eq!(
        stored[0].amounts["vw"].units,
        -9_999_123_456_789_012_345_678_i128
    );
    assert_eq!(stored[0].amounts["vw"].scale, 18);
    let next = reader
        .read_wb_rows(&cabinet, publication.batch_id, 11, 1)
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].rrd_id, 12);
    assert!(
        reader
            .read_wb_rows("another-account", publication.batch_id, 0, 1000)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reader
            .read_wb_rows(&cabinet, publication.batch_id, 0, 1001)
            .await
            .unwrap_err(),
        FinanceLedgerError::InvalidInput
    );
    let mut changed_rows = raw_rows();
    changed_rows[0]["forPay"] = json!("111.00");
    let revision = batch(cabinet.clone(), changed_rows, date(), date()).await;
    assert_eq!(
        writer.publish_wb(&revision).await,
        Err(FinanceLedgerError::RevisionConflict)
    );
    let overlap = batch(
        cabinet.clone(),
        raw_rows(),
        date().pred_opt().unwrap(),
        date(),
    )
    .await;
    assert_eq!(
        writer.publish_wb(&overlap).await,
        Err(FinanceLedgerError::RevisionConflict)
    );
    assert_eq!(
        reader.list_wb_batches(&cabinet, 100).await.unwrap().len(),
        1
    );
    let independent = batch(account("independent"), raw_rows(), date(), date()).await;
    assert!(writer.publish_wb(&independent).await.is_ok());
    let empty_transport = Pages(Mutex::new(VecDeque::from([Ok(None)])));
    let empty = WbFinanceBatch::collect(&empty_transport, account("empty"), date(), date(), &None)
        .await
        .unwrap();
    let empty_publication = writer.publish_wb(&empty).await.unwrap();
    assert!(
        reader
            .read_wb_rows(empty.account_id(), empty_publication.batch_id, 0, 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reader.list_wb_batches(empty.account_id(), 1).await.unwrap()[0].row_count,
        0
    );
}

#[tokio::test]
async fn financial_ledger_database_boundary_rejects_partial_unknown_and_direct_writes() {
    let (Ok(writer_url), Ok(reader_url)) = (
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
        std::env::var("POSITION_REPOSITORY_TEST_READER_URL"),
    ) else {
        return;
    };
    let writer = connect(&writer_url).await;
    let reader = connect(&reader_url).await;
    for client in [&writer, &reader] {
        for relation in [
            "financial_ledger_batches",
            "financial_ledger_rows",
            "financial_ledger_amounts",
        ] {
            let error = client
                .batch_execute(&format!("SELECT * FROM daily_reporting.{relation}"))
                .await
                .unwrap_err();
            assert_eq!(error.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));
            for sql in [
                format!("DELETE FROM daily_reporting.{relation}"),
                format!("INSERT INTO daily_reporting.{relation} DEFAULT VALUES"),
                format!("TRUNCATE daily_reporting.{relation}"),
            ] {
                let error = client.batch_execute(&sql).await.unwrap_err();
                assert!(matches!(
                    error.code(),
                    Some(&SqlState::INSUFFICIENT_PRIVILEGE | &SqlState::READ_ONLY_SQL_TRANSACTION)
                ));
            }
        }
    }
    let original = batch(account("boundary"), raw_rows(), date(), date()).await;
    let good_payload = serde_json::to_string(original.rows()).unwrap();
    let invoke = "SELECT * FROM daily_reporting.publish_wb_financial_ledger($1,$2,$3,$4,$5,$6)";
    let hash = sha256(good_payload.as_bytes());
    let error = reader
        .query(
            invoke,
            &[
                &original.account_id(),
                &date(),
                &date(),
                &good_payload,
                &hash,
                &204_i32,
            ],
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));
    let error = writer
        .query(
            invoke,
            &[
                &original.account_id(),
                &date(),
                &date(),
                &good_payload,
                &hash,
                &200_i32,
            ],
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::INVALID_PARAMETER_VALUE));
    let mut unknown = serde_json::to_value(original.rows()).unwrap();
    unknown[1]["buyer_phone"] = json!("not-permitted");
    let invalid_payload = unknown.to_string();
    let invalid_hash = sha256(invalid_payload.as_bytes());
    assert!(
        writer
            .query(
                invoke,
                &[
                    &original.account_id(),
                    &date(),
                    &date(),
                    &invalid_payload,
                    &invalid_hash,
                    &204_i32
                ]
            )
            .await
            .is_err()
    );
    let visible: i64 = reader
        .query_one(
            "SELECT count(*) FROM daily_reporting.mcp_financial_ledger_batches WHERE account_id=$1",
            &[&original.account_id()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        visible, 0,
        "failed second row must roll back first row and manifest"
    );
    let restricted_writer = PostgresFinanceLedger::from_client(writer);
    restricted_writer.publish_wb(&original).await.unwrap();
    let restricted_reader = PostgresFinanceLedgerReader::from_client(reader);
    assert_eq!(
        restricted_reader
            .read_wb_rows(
                original.account_id(),
                restricted_reader
                    .list_wb_batches(original.account_id(), 1)
                    .await
                    .unwrap()[0]
                    .batch_id,
                0,
                100
            )
            .await
            .unwrap(),
        original.rows()
    );
}

#[tokio::test]
async fn terminal_proof_and_date_scope_are_required_before_publication() {
    let partial = Pages(Mutex::new(VecDeque::from([
        Ok(Some(raw_rows())),
        Err(WbReportSourceError::InvalidResponse),
    ])));
    assert!(
        WbFinanceBatch::collect(&partial, "pilot".to_owned(), date(), date(), &None)
            .await
            .is_err()
    );
    let outside = Pages(Mutex::new(VecDeque::from([Ok(Some(raw_rows())), Ok(None)])));
    assert_eq!(
        WbFinanceBatch::collect(
            &outside,
            "pilot".to_owned(),
            date().pred_opt().unwrap(),
            date().pred_opt().unwrap(),
            &None
        )
        .await
        .unwrap_err(),
        FinanceLedgerError::InvalidInput
    );
    let no_request = Pages(Mutex::new(VecDeque::new()));
    assert_eq!(
        WbFinanceBatch::collect(&no_request, "bad account".to_owned(), date(), date(), &None)
            .await
            .unwrap_err(),
        FinanceLedgerError::InvalidInput
    );
}

fn sha256(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        let _ = write!(result, "{byte:02x}");
    }
    result
}
