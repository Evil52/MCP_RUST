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
