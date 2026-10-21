use super::*;
use crate::reporting::wb_finance_source::WbFinanceDecimal;
use chrono::{NaiveDate, TimeZone, Utc};

fn account() -> AccountScope {
    AccountScope::new("account-wb".into(), Marketplace::Wildberries).unwrap()
}

fn query() -> WbFinancialLedgerQuery {
    WbFinancialLedgerQuery {
        batch_id: None,
        after_rrd_id: 0,
        limit: 1,
    }
}

fn date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, 10).unwrap()
}

fn batch(row_count: i32) -> FinanceLedgerBatch {
    FinanceLedgerBatch {
        batch_id: 42,
        account_id: "account-wb".into(),
        marketplace: "wildberries".into(),
        source: "wb_sales_reports_detailed_v1".into(),
        date_from: date(),
        date_to: date(),
        row_count,
        terminal_http_status: 204,
        published_at: Utc.with_ymd_and_hms(2026, 9, 11, 0, 0, 0).unwrap(),
    }
}

fn row(id: u64) -> WbFinanceDetailRow {
    WbFinanceDetailRow {
        rrd_id: id,
        report_id: 9_007_199_254_740_993,
        business_date: date(),
        sku: None,
        currency: "RUB".into(),
        document_type: Some("Возврат".into()),
        operation_type: Some("Недоверенные данные".into()),
        quantity: None,
        amounts: BTreeMap::from([(
            "forPay".into(),
            WbFinanceDecimal {
                units: 9_007_199_254_740_993,
                scale: 3,
            },
        )]),
    }
}

#[test]
fn missing_batch_is_unknown_but_terminal_empty_batch_is_complete_empty() {
    let missing = page_result(&account(), query(), None).unwrap();
    assert_eq!(missing.state, DataState::Unavailable);
    assert!(missing.batch.is_none());
    let empty = page_result(&account(), query(), Some((batch(0), vec![], false))).unwrap();
    assert_eq!(empty.state, DataState::Complete);
    assert_eq!(empty.batch.unwrap().row_count, 0);
    assert_eq!(empty.reconciliation_state, DataState::Unavailable);
}

#[test]
fn page_uses_exact_string_ids_coefficients_and_pins_continuation_to_batch() {
    let result = page_result(
        &account(),
        query(),
        Some((batch(2), vec![row(9_007_199_254_740_993)], true)),
    )
    .unwrap();
    assert_eq!(
        result.next_after_rrd_id.as_deref(),
        Some("9007199254740993")
    );
    assert_eq!(result.batch.unwrap().batch_id, "42");
    let encoded = serde_json::to_value(&result.rows).unwrap();
    assert_eq!(encoded[0]["rrd_id"], "9007199254740993");
    assert_eq!(encoded[0]["report_id"], "9007199254740993");
    assert_eq!(encoded[0]["amounts"]["forPay"]["units"], "9007199254740993");
    assert_eq!(encoded[0]["amounts"]["forPay"]["scale"], 3);
    assert!(encoded[0]["amounts"].get("paidStorage").is_none());
    assert_eq!(result.reconciliation_state, DataState::Unavailable);
}

#[test]
fn foreign_or_invalid_metadata_and_incomplete_first_pages_fail_closed() {
    let mut foreign = batch(0);
    foreign.account_id = "another-wb".into();
    let mut nonterminal = batch(0);
    nonterminal.terminal_http_status = 200;
    for batch in [foreign, nonterminal, batch(-1), batch(1)] {
        assert_eq!(
            page_result(&account(), query(), Some((batch, vec![], false))),
            Err(ReportingReadError::InvalidPublishedData)
        );
    }
    let pinned = WbFinancialLedgerQuery {
        batch_id: Some(999),
        ..query()
    };
    assert_eq!(
        page_result(&account(), pinned, Some((batch(0), vec![], false))),
        Err(ReportingReadError::InvalidPublishedData)
    );
}

#[test]
fn invalid_rows_and_nonadvancing_cursors_never_produce_page_success() {
    let next = WbFinancialLedgerQuery {
        batch_id: Some(42),
        after_rrd_id: 3,
        ..query()
    };
    assert_eq!(
        page_result(&account(), next, Some((batch(2), vec![row(3)], false))),
        Err(ReportingReadError::InvalidPublishedData)
    );
    let mut wrong_date = row(4);
    wrong_date.business_date = date().succ_opt().unwrap();
    assert_eq!(
        page_result(&account(), next, Some((batch(2), vec![wrong_date], false))),
        Err(ReportingReadError::InvalidPublishedData)
    );
    assert_eq!(
        page_result(&account(), next, Some((batch(2), vec![], true))),
        Err(ReportingReadError::InvalidPublishedData)
    );
}

#[test]
fn query_requires_wb_and_bounded_stable_pagination() {
    let ozon = AccountScope::new("ozon-account".into(), Marketplace::Ozon).unwrap();
    assert_eq!(
        validate_query(&ozon, query()),
        Err(ReportingReadError::InvalidRequest)
    );
    for invalid in [
        WbFinancialLedgerQuery {
            limit: 0,
            ..query()
        },
        WbFinancialLedgerQuery {
            limit: 1001,
            ..query()
        },
        WbFinancialLedgerQuery {
            after_rrd_id: 1,
            ..query()
        },
        WbFinancialLedgerQuery {
            batch_id: Some(-1),
            ..query()
        },
    ] {
        assert_eq!(
            validate_query(&account(), invalid),
            Err(ReportingReadError::InvalidRequest)
        );
    }
}

#[test]
fn ledger_preserves_large_sixteen_decimal_financial_coefficients_as_strings() {
    let mut value = row(1);
    value.amounts.insert(
        "vw".into(),
        WbFinanceDecimal {
            units: 19_223_372_036_854_775_808_i128,
            scale: 16,
        },
    );
    let result = page_result(&account(), query(), Some((batch(1), vec![value], false))).unwrap();
    let encoded = serde_json::to_value(result).unwrap();
    assert_eq!(
        encoded["rows"][0]["amounts"]["vw"]["units"],
        "19223372036854775808"
    );
    assert_eq!(encoded["rows"][0]["amounts"]["vw"]["scale"], 16);
}

/// One complete WB detail page followed by the terminal HTTP 204.
struct OnePage(std::sync::Mutex<std::collections::VecDeque<Option<serde_json::Value>>>);

impl crate::reporting::wb_finance_source::WbFinanceTransport for OnePage {
    fn finance_page<'a>(
        &'a self,
        _: NaiveDate,
        _: NaiveDate,
        _: u32,
        _: u64,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        Option<serde_json::Value>,
                        crate::reporting::wb_source::WbReportSourceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async { Ok(self.0.lock().unwrap().pop_front().flatten()) })
    }
}

fn database_url(name: &str) -> String {
    std::env::var(name)
        .expect("run through scripts/with-position-test-db.sh with --include-ignored")
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn published_ledger_pages_through_the_reader_within_one_pinned_batch() {
    use super::super::ReportingReadRepository;
    use crate::reporting::finance_ledger::{PostgresFinanceLedger, WbFinanceBatch};

    let cabinet = format!(
        "ledger-mcp-{}-{}",
        std::process::id(),
        Utc::now().timestamp_micros()
    );
    let rows = serde_json::json!([
        {"rrdId": 11, "reportId": 21, "rrDate": "2026-09-10", "nmId": 31, "currency": "RUB",
         "docTypeName": "Продажа", "quantity": 1, "forPay": "112.345"},
        {"rrdId": 12, "reportId": 21, "rrDate": "2026-09-10", "nmId": 0, "currency": "RUB",
         "docTypeName": "Возврат", "quantity": -1, "forPay": "-25.1"}
    ]);
    let pages = OnePage(std::sync::Mutex::new([Some(rows), None].into()));
    let batch = WbFinanceBatch::collect(&pages, cabinet.clone(), date(), date(), &None)
        .await
        .unwrap();
    let writer_url = database_url("REPORT_SNAPSHOT_TEST_COLLECTOR_URL");
    let published = PostgresFinanceLedger::connect(&writer_url.parse().unwrap())
        .await
        .unwrap()
        .publish_wb(&batch)
        .await
        .unwrap();
    let (client, connection) = database_url("POSITION_REPOSITORY_TEST_READER_URL")
        .parse::<tokio_postgres::Config>()
        .unwrap()
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    let repository = PostgresReportingRepository::from_client(client);
    let scope = AccountScope::new(cabinet.clone(), Marketplace::Wildberries).unwrap();

    let first = repository
        .wb_financial_ledger(&scope, query())
        .await
        .unwrap();
    assert_eq!(
        first.batch.as_ref().unwrap().batch_id,
        published.batch_id.to_string()
    );
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].rrd_id, "11");
    assert_eq!(first.next_after_rrd_id.as_deref(), Some("11"));
    let pinned = WbFinancialLedgerQuery {
        batch_id: Some(published.batch_id),
        after_rrd_id: 11,
        limit: 1,
    };
    let second = repository
        .wb_financial_ledger(&scope, pinned)
        .await
        .unwrap();
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].rrd_id, "12");
    assert_eq!(second.next_after_rrd_id, None);

    let absent = AccountScope::new(format!("{cabinet}-none"), Marketplace::Wildberries).unwrap();
    let missing = repository
        .wb_financial_ledger(&absent, query())
        .await
        .unwrap();
    assert!(missing.batch.is_none() && missing.rows.is_empty());
    // A cursor is meaningful only inside one pinned batch, on both layers.
    let unpinned = WbFinancialLedgerQuery {
        batch_id: None,
        ..pinned
    };
    assert_eq!(
        repository.wb_financial_ledger(&scope, unpinned).await,
        Err(ReportingReadError::InvalidRequest)
    );
    assert!(matches!(
        read_wb_ledger_page(&repository.client, &cabinet, None, 11, 1).await,
        Err(FinanceLedgerError::InvalidInput)
    ));
}
