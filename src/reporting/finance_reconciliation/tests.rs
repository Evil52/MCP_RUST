use serde_json::json;

use super::*;

fn date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, 10).unwrap()
}

fn row(id: u64, document: Option<&str>, units: i64, scale: u32) -> WbFinanceDetailRow {
    WbFinanceDetailRow {
        rrd_id: id,
        report_id: 9_007_199_254_740_993,
        business_date: date(),
        sku: Some(111),
        currency: "RUB".into(),
        document_type: document.map(str::to_owned),
        operation_type: Some("Продажа".into()),
        quantity: Some(1),
        amounts: BTreeMap::from([
            ("forPay".into(), WbFinanceDecimal { units, scale }),
            (
                "retailAmount".into(),
                WbFinanceDecimal {
                    units: 100,
                    scale: 0,
                },
            ),
        ]),
    }
}

fn total(units: i128, scale: u32) -> ExactFinanceTotal {
    ExactFinanceTotal { units, scale }
}

fn report() -> WbFinanceReportTotals {
    aggregate_wb_finance_rows(&[row(1, Some("Продажа"), 1234, 2)])
        .unwrap()
        .remove(0)
}

fn evidence() -> WbFinanceComparisonEvidence {
    WbFinanceComparisonEvidence {
        scope: WbFinanceReportScope {
            account_id: "wb-shop-1".into(),
            report_id: 9_007_199_254_740_993,
            currency: "RUB".into(),
            period: WbFinanceReportPeriod::Daily,
            date_from: date(),
            date_to: date(),
        },
        observation_id: "detail-job-1".into(),
        source_sha256: "ab".repeat(32),
        terminal_observed: true,
        covers_entire_report: true,
    }
}

fn baseline() -> WbFinanceBaseline {
    WbFinanceBaseline {
        kind: WbFinanceBaselineKind::IndependentRawColumns,
        evidence: WbFinanceComparisonEvidence {
            observation_id: "report-export-1".into(),
            source_sha256: "cd".repeat(32),
            ..evidence()
        },
        totals: BTreeMap::from([("forPay".into(), total(12340, 3))]),
    }
}

#[test]
fn report_and_currency_are_separate_even_when_ids_exceed_javascript_precision() {
    let a = row(1, Some("Продажа"), 101, 1);
    let b = WbFinanceDetailRow {
        report_id: a.report_id + 1,
        ..row(2, Some("Продажа"), 222, 2)
    };
    let c = WbFinanceDetailRow {
        currency: "CNY".into(),
        ..row(3, Some("Продажа"), 333, 3)
    };
    let reports = aggregate_wb_finance_rows(&[a, b, c]).unwrap();
    assert_eq!(reports.len(), 3);
    assert_eq!(reports[0].currency, "CNY");
    assert_eq!(reports[1].report_id, 9_007_199_254_740_993);
    assert_eq!(reports[0].columns["forPay"].total, Some(total(333, 3)));
    assert_eq!(reports[1].columns["forPay"].total, Some(total(101, 1)));
    assert_eq!(reports[2].columns["forPay"].total, Some(total(222, 2)));
}

#[test]
fn raw_refund_and_correction_values_keep_their_signs_and_never_overlap_columns() {
    let rows = [
        row(1, Some("Продажа"), 12345, 2),
        row(2, Some("Возврат"), 349, 3),
        row(3, Some("Неизвестная коррекция"), -1000, 2),
    ];
    let report = aggregate_wb_finance_rows(&rows).unwrap().remove(0);
    // Raw return remains positive. This is intentionally not seller net pay.
    assert_eq!(report.columns["forPay"].total, Some(total(113_799, 3)));
    assert_eq!(report.columns["retailAmount"].total, Some(total(300, 0)));
    assert_eq!(report.document_type_counts[&WbFinanceDocumentKind::Sale], 1);
    assert_eq!(
        report.document_type_counts[&WbFinanceDocumentKind::Return],
        1
    );
    assert_eq!(
        report.document_type_counts[&WbFinanceDocumentKind::Unknown],
        1
    );
}

#[test]
fn missing_amount_invalidates_column_total_in_any_row_order() {
    for absent_id in [1, 2, 3] {
        let mut rows: Vec<_> = (1..=3).map(|id| row(id, Some("Продажа"), 100, 2)).collect();
        rows[absent_id - 1].amounts.remove("forPay");
        let report = aggregate_wb_finance_rows(&rows).unwrap().remove(0);
        assert_eq!(
            report.columns["forPay"],
            FinanceColumnTotal {
                total: None,
                present_rows: 2,
                missing_rows: 1,
            }
        );
        assert_eq!(report.columns["retailAmount"].total, Some(total(300, 0)));
        assert_eq!(report.columns["paidStorage"].missing_rows, 3);
        assert_eq!(report.columns["paidStorage"].total, None);
    }
}

#[test]
fn large_signed_decimal_sums_and_json_roundtrip_remain_exact() {
    let reports = aggregate_wb_finance_rows(&[
        row(1, Some("Продажа"), i64::MAX, 0),
        row(2, Some("Продажа"), i64::MAX, 0),
        row(3, None, -1, 9),
    ])
    .unwrap();
    let amount = reports[0].columns["forPay"].total.unwrap();
    assert_eq!(amount, total(18_446_744_073_709_551_613_999_999_999, 9));
    let encoded = serde_json::to_value(&reports).unwrap();
    assert_eq!(
        encoded[0]["columns"]["forPay"]["total"]["units"],
        "18446744073709551613999999999"
    );
    let decoded: Vec<WbFinanceReportTotals> = serde_json::from_value(encoded).unwrap();
    assert_eq!(reports, decoded);
}

#[test]
fn deserialization_rejects_floating_numeric_and_noncanonical_coefficients() {
    for value in [
        json!(1),
        json!(1.5),
        json!("+1"),
        json!("01"),
        json!("-0"),
        json!("1e2"),
    ] {
        assert!(
            serde_json::from_value::<ExactFinanceTotal>(json!({"units": value, "scale": 0}))
                .is_err()
        );
    }
}

#[test]
fn duplicate_invalid_and_excess_rows_fail_closed_and_empty_rows_are_not_zero() {
    assert!(aggregate_wb_finance_rows(&[]).unwrap().is_empty());
    let a = row(1, Some("Продажа"), 100, 2);
    let mut b = a.clone();
    b.report_id += 1;
    assert_eq!(
        aggregate_wb_finance_rows(&[a.clone(), b]),
        Err(FinanceReconciliationError::DuplicateRow)
    );
    let mut invalid = a.clone();
    invalid.amounts.insert(
        "arbitraryProfit".into(),
        WbFinanceDecimal { units: 1, scale: 0 },
    );
    assert_eq!(
        aggregate_wb_finance_rows(&[invalid]),
        Err(FinanceReconciliationError::InvalidRows)
    );
    assert_eq!(
        aggregate_wb_finance_rows(&vec![a; WB_FINANCE_MAX_ROWS + 1]),
        Err(FinanceReconciliationError::InvalidRows)
    );
}

#[test]
fn document_labels_only_recognize_exact_vendor_values() {
    assert_eq!(
        wb_finance_document_kind(Some("Возврат")),
        WbFinanceDocumentKind::Return
    );
    for unknown in ["возврат", "Return", "Коррекция", "Продажа "] {
        assert_eq!(
            wb_finance_document_kind(Some(unknown)),
            WbFinanceDocumentKind::Unknown
        );
    }
    assert_eq!(
        wb_finance_document_kind(Some("")),
        WbFinanceDocumentKind::Unspecified
    );
    assert_eq!(
        wb_finance_document_kind(None),
        WbFinanceDocumentKind::Unspecified
    );
}

#[test]
fn equal_exact_values_with_different_scales_match_only_the_specified_raw_column() {
    let comparison =
        reconcile_wb_finance_report(&report(), &evidence(), Some(&baseline())).unwrap();
    assert_eq!(comparison.status, FinanceComparisonStatus::RawColumnsMatch);
    assert_eq!(comparison.unavailable_reason, None);
    assert_eq!(comparison.columns.len(), 1);
    assert_eq!(comparison.columns[0].column, "forPay");
    assert_eq!(comparison.columns[0].detail_total, Some(total(1234, 2)));
    assert_eq!(comparison.columns[0].baseline_total, total(12340, 3));
}

#[test]
fn a_one_billionth_difference_is_a_mismatch_without_tolerance_rounding() {
    let mut baseline = baseline();
    baseline
        .totals
        .insert("forPay".into(), total(12_340_000_001, 9));
    let comparison = reconcile_wb_finance_report(&report(), &evidence(), Some(&baseline)).unwrap();
    assert_eq!(comparison.status, FinanceComparisonStatus::Mismatch);
    assert_eq!(
        comparison.columns[0].status,
        FinanceComparisonStatus::Mismatch
    );
}

#[test]
fn missing_column_is_unavailable_even_when_another_column_mismatches() {
    let mut baseline = baseline();
    baseline.totals.insert("forPay".into(), total(0, 0));
    baseline.totals.insert("paidStorage".into(), total(0, 0));
    let comparison = reconcile_wb_finance_report(&report(), &evidence(), Some(&baseline)).unwrap();
    assert_eq!(comparison.status, FinanceComparisonStatus::Unavailable);
    assert_eq!(
        comparison.unavailable_reason,
        Some(FinanceComparisonUnavailable::MissingColumnAmount)
    );
    assert_eq!(
        comparison.columns[0].status,
        FinanceComparisonStatus::Mismatch
    );
    assert_eq!(comparison.columns[1].detail_total, None);
}

#[test]
fn missing_baseline_or_incomplete_report_cannot_be_reconciled() {
    let missing = reconcile_wb_finance_report(&report(), &evidence(), None).unwrap();
    assert_eq!(
        missing.unavailable_reason,
        Some(FinanceComparisonUnavailable::MissingBaseline)
    );
    for (terminal_observed, covers_entire_report) in [(false, true), (true, false), (false, false)]
    {
        let partial = WbFinanceComparisonEvidence {
            terminal_observed,
            covers_entire_report,
            ..evidence()
        };
        let result = reconcile_wb_finance_report(&report(), &partial, Some(&baseline())).unwrap();
        assert_eq!(
            result.unavailable_reason,
            Some(FinanceComparisonUnavailable::IncompleteDetails)
        );
        let mut baseline = baseline();
        baseline.evidence.terminal_observed = terminal_observed;
        baseline.evidence.covers_entire_report = covers_entire_report;
        let result = reconcile_wb_finance_report(&report(), &evidence(), Some(&baseline)).unwrap();
        assert_eq!(
            result.unavailable_reason,
            Some(FinanceComparisonUnavailable::IncompleteBaseline)
        );
    }
}

#[test]
fn cross_account_report_currency_period_or_date_baselines_are_not_compared() {
    let scope = evidence().scope;
    let scopes = [
        WbFinanceReportScope {
            account_id: "other-shop".into(),
            ..scope.clone()
        },
        WbFinanceReportScope {
            report_id: 123,
            ..scope.clone()
        },
        WbFinanceReportScope {
            currency: "CNY".into(),
            ..scope.clone()
        },
        WbFinanceReportScope {
            period: WbFinanceReportPeriod::Weekly,
            ..scope.clone()
        },
        WbFinanceReportScope {
            date_to: date().succ_opt().unwrap(),
            ..scope
        },
    ];
    for scope in scopes {
        let mut baseline = baseline();
        baseline.evidence.scope = scope;
        let result = reconcile_wb_finance_report(&report(), &evidence(), Some(&baseline)).unwrap();
        assert_eq!(
            result.unavailable_reason,
            Some(FinanceComparisonUnavailable::ScopeMismatch)
        );
        assert!(result.columns.is_empty());
    }
}

#[test]
fn same_observation_or_same_evidence_hash_cannot_supply_independent_baseline() {
    for same_hash in [false, true] {
        let mut baseline = baseline();
        if same_hash {
            baseline.evidence.source_sha256 = evidence().source_sha256;
        } else {
            baseline.evidence.observation_id = evidence().observation_id;
        }
        let result = reconcile_wb_finance_report(&report(), &evidence(), Some(&baseline)).unwrap();
        assert_eq!(
            result.unavailable_reason,
            Some(FinanceComparisonUnavailable::NonIndependentBaseline)
        );
    }
}

#[test]
fn official_summary_fields_cannot_be_guessed_from_raw_amounts_even_if_equal() {
    let mut baseline = baseline();
    baseline.kind = WbFinanceBaselineKind::OfficialReportSummary;
    baseline.totals = BTreeMap::from([("forPaySum".into(), total(1234, 2))]);
    let result = reconcile_wb_finance_report(&report(), &evidence(), Some(&baseline)).unwrap();
    assert_eq!(
        result.unavailable_reason,
        Some(FinanceComparisonUnavailable::UnverifiedOfficialSummaryMapping)
    );
    assert!(result.columns.is_empty());
}

#[test]
fn malformed_receipts_and_tampered_derived_totals_fail_validation() {
    let mut receipt = evidence();
    receipt.source_sha256 = "invalid".into();
    assert_eq!(
        reconcile_wb_finance_report(&report(), &receipt, None),
        Err(FinanceReconciliationError::InvalidEvidence)
    );
    receipt = evidence();
    receipt.scope.report_id += 1;
    assert_eq!(
        reconcile_wb_finance_report(&report(), &receipt, None),
        Err(FinanceReconciliationError::InvalidEvidence)
    );
    let mut totals = report();
    totals.columns.get_mut("forPay").unwrap().missing_rows = 1;
    assert_eq!(
        reconcile_wb_finance_report(&totals, &evidence(), None),
        Err(FinanceReconciliationError::InvalidRows)
    );
}

#[test]
fn empty_unknown_and_out_of_range_baselines_do_not_pass() {
    let mut empty = baseline();
    empty.totals.clear();
    let result = reconcile_wb_finance_report(&report(), &evidence(), Some(&empty)).unwrap();
    assert_eq!(
        result.unavailable_reason,
        Some(FinanceComparisonUnavailable::EmptyBaseline)
    );
    for (column, amount) in [
        ("profit", total(1, 0)),
        ("forPay", total(1, 10)),
        ("forPay", total(i128::MAX, 0)),
    ] {
        let mut invalid = baseline();
        invalid.totals = BTreeMap::from([(column.into(), amount)]);
        assert_eq!(
            reconcile_wb_finance_report(&report(), &evidence(), Some(&invalid)),
            Err(FinanceReconciliationError::InvalidAmount)
        );
    }
}

#[test]
fn allowed_raw_columns_match_the_source_projection() {
    for column in WB_RAW_AMOUNT_COLUMNS {
        assert!(super::super::wb_finance_source::WB_FINANCE_FIELDS.contains(column));
        let mut record = row(1, None, 0, 0);
        record.amounts =
            BTreeMap::from([((*column).into(), WbFinanceDecimal { units: 0, scale: 0 })]);
        record.validate().unwrap();
    }
}
