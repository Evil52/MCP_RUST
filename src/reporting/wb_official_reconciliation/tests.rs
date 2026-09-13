use std::collections::BTreeMap;

use chrono::NaiveDate;

use super::*;
use crate::reporting::finance_reconciliation::WbFinanceReportScope;

fn date(day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, day).unwrap()
}

fn amount(units: i64, scale: u32) -> WbFinanceDecimal {
    WbFinanceDecimal {
        units: i128::from(units),
        scale,
    }
}

fn exact(units: i128, scale: u32) -> ExactFinanceTotal {
    ExactFinanceTotal { units, scale }
}

fn row(id: u64, document: Option<&str>, retail: i64, payout: i64) -> WbFinanceDetailRow {
    WbFinanceDetailRow {
        rrd_id: id,
        report_id: 9_007_199_254_740_993,
        business_date: date(4),
        sku: Some(111),
        currency: "RUB".into(),
        document_type: document.map(str::to_owned),
        operation_type: Some("Продажа".into()),
        quantity: Some(1),
        amounts: BTreeMap::from([
            ("retailAmount".into(), amount(retail, 2)),
            ("forPay".into(), amount(payout, 2)),
        ]),
    }
}

fn evidence() -> WbFinanceComparisonEvidence {
    WbFinanceComparisonEvidence {
        scope: WbFinanceReportScope {
            account_id: "wb-shop-1".into(),
            report_id: 9_007_199_254_740_993,
            currency: "RUB".into(),
            period: WbFinanceReportPeriod::Weekly,
            date_from: date(1),
            date_to: date(6),
        },
        observation_id: "details-by-id-1".into(),
        source_sha256: "ab".repeat(32),
        terminal_observed: true,
        covers_entire_report: true,
    }
}

fn baseline(retail: i128, payout: i128) -> WbFinanceBaseline {
    WbFinanceBaseline {
        kind: WbFinanceBaselineKind::OfficialReportSummary,
        evidence: WbFinanceComparisonEvidence {
            observation_id: "summary-list-1".into(),
            source_sha256: "cd".repeat(32),
            ..evidence()
        },
        totals: BTreeMap::from([
            ("retailAmountSum".into(), exact(retail, 2)),
            ("forPaySum".into(), exact(payout, 2)),
        ]),
    }
}

fn compare(rows: &[WbFinanceDetailRow], baseline: &WbFinanceBaseline) -> WbOfficialComparison {
    reconcile_wb_official_report(rows, &evidence(), Some(baseline)).unwrap()
}

#[test]
fn sale_minus_return_matches_both_official_totals() {
    let rows = [
        row(1, Some("Продажа"), 10_000, 8_000),
        row(2, Some("Продажа"), 7_000, 5_800),
        row(3, Some("Возврат"), 2_500, 2_000),
    ];
    let result = compare(&rows, &baseline(14_500, 11_800));
    assert_eq!(
        result.status,
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    );
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0].detail_total, Some(exact(14_500, 2)));
    assert_eq!(result.columns[1].detail_total, Some(exact(11_800, 2)));
    assert_eq!(result.columns[0].difference, Some(exact(0, 2)));
}

#[test]
fn correction_values_keep_their_algebraic_sign_before_document_subtraction() {
    let mut rows = [
        row(1, Some("Продажа"), 10_000, 8_000),
        row(2, Some("Продажа"), -100, -90),
        row(3, Some("Возврат"), 2_500, 2_000),
        row(4, Some("Возврат"), -500, -400),
    ];
    rows[1].operation_type = Some("Коррекция продаж".into());
    rows[3].operation_type = Some("Коррекция эквайринга".into());
    assert_eq!(
        compare(&rows, &baseline(7_900, 6_310)).status,
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    );
}

#[test]
fn operation_name_and_quantity_never_replace_the_document_sign() {
    let mut sale = row(1, Some("Продажа"), 10_000, 8_000);
    sale.operation_type = Some("Добровольная компенсация при возврате".into());
    sale.quantity = Some(-9);
    let mut refund = row(2, Some("Возврат"), 2_500, 2_000);
    refund.operation_type = Some("Новая операция".into());
    refund.quantity = None;
    assert_eq!(
        compare(&[sale, refund], &baseline(7_500, 6_000)).status,
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    );
}

#[test]
fn blank_document_for_a_fee_can_only_contribute_explicit_zero() {
    for document in [None, Some("")] {
        let mut fee = row(2, document, 0, 0);
        fee.operation_type = Some("Логистика".into());
        fee.amounts
            .insert("deliveryService".into(), amount(1_349, 3));
        assert_eq!(
            compare(
                &[row(1, Some("Продажа"), 10_000, 8_000), fee],
                &baseline(10_000, 8_000)
            )
            .status,
            WbOfficialComparisonStatus::PrimaryTotalsMatch
        );
    }
}

#[test]
fn unknown_labels_and_variants_are_not_guessed_from_operation_names() {
    for document in [
        None,
        Some(""),
        Some("продажа"),
        Some("Продажа "),
        Some("RETURN"),
        Some("Возврат товара"),
        Some("Коррекция продаж"),
    ] {
        let result = compare(&[row(1, document, 100, 80)], &baseline(100, 80));
        assert_eq!(result.status, WbOfficialComparisonStatus::Unavailable);
        assert_eq!(
            result.unavailable_reason,
            Some(WbOfficialUnavailable::UnsupportedDocumentType)
        );
        assert!(
            result
                .columns
                .iter()
                .all(|column| column.detail_total.is_none())
        );
    }
}

#[test]
fn unknown_document_with_explicit_zero_is_mathematically_harmless() {
    let result = compare(
        &[
            row(1, Some("Продажа"), 100, 80),
            row(2, Some("New fee kind"), 0, 0),
        ],
        &baseline(100, 80),
    );
    assert_eq!(
        result.status,
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    );
}

#[test]
fn missing_detail_field_blocks_only_its_metric_and_does_not_become_zero() {
    let mut sale = row(1, Some("Продажа"), 0, 80);
    sale.amounts.remove("retailAmount");
    let result = compare(&[sale], &baseline(0, 80));
    assert_eq!(result.status, WbOfficialComparisonStatus::Unavailable);
    assert_eq!(
        result.columns[0].unavailable_reason,
        Some(WbOfficialUnavailable::MissingDetailAmount)
    );
    assert_eq!(result.columns[0].detail_total, None);
    assert_eq!(
        result.columns[1].status,
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    );
}

#[test]
fn missing_zero_amount_on_blank_document_is_still_unavailable() {
    let mut fee = row(1, None, 0, 0);
    fee.amounts.remove("forPay");
    let result = compare(&[fee], &baseline(0, 0));
    assert_eq!(
        result.columns[1].unavailable_reason,
        Some(WbOfficialUnavailable::MissingDetailAmount)
    );
}

#[test]
fn missing_summary_metric_does_not_claim_success_for_both_totals() {
    let mut summary = baseline(100, 80);
    summary.totals.remove("forPaySum");
    let result = compare(&[row(1, Some("Продажа"), 100, 80)], &summary);
    assert_eq!(result.status, WbOfficialComparisonStatus::Unavailable);
    assert_eq!(
        result.columns[1].unavailable_reason,
        Some(WbOfficialUnavailable::MissingSummaryAmount)
    );
    assert_eq!(result.columns[1].summary_total, None);
    assert_eq!(result.columns[1].detail_total, Some(exact(80, 2)));
}

#[test]
fn mismatch_reports_exact_signed_difference_without_rounding() {
    let mut sale = row(1, Some("Продажа"), 100, 80);
    sale.amounts.insert("retailAmount".into(), amount(1_001, 3));
    let result = compare(&[sale], &baseline(100, 90));
    assert_eq!(result.status, WbOfficialComparisonStatus::Mismatch);
    assert_eq!(result.columns[0].difference, Some(exact(1, 3)));
    assert_eq!(result.columns[1].difference, Some(exact(-10, 2)));
}

#[test]
fn mathematically_equal_different_decimal_scales_match() {
    let mut summary = baseline(100, 80);
    summary.totals.insert("retailAmountSum".into(), exact(1, 0));
    summary.totals.insert("forPaySum".into(), exact(800, 3));
    assert_eq!(
        compare(&[row(1, Some("Продажа"), 100, 80)], &summary).status,
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    );
}

#[test]
fn aggregate_exceeds_i64_and_handles_i64_minimum_return_without_overflow() {
    let rows = [
        row(1, Some("Продажа"), i64::MAX, i64::MAX),
        row(2, Some("Возврат"), i64::MIN, i64::MIN),
    ];
    let expected = i128::from(i64::MAX) - i128::from(i64::MIN);
    let result = compare(&rows, &baseline(expected, expected));
    assert_eq!(
        result.status,
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    );
    let encoded = serde_json::to_value(&result).unwrap();
    assert_eq!(
        encoded["columns"][0]["detail_total"]["units"],
        expected.to_string()
    );
}

#[test]
fn malformed_and_overflowing_summary_amounts_fail_without_panicking() {
    for malformed in [exact(1, 19), exact(i128::MAX, 0), exact(i128::MIN, 2)] {
        let mut summary = baseline(100, 80);
        summary.totals.insert("retailAmountSum".into(), malformed);
        assert_eq!(
            reconcile_wb_official_report(
                &[row(1, Some("Продажа"), 100, 80)],
                &evidence(),
                Some(&summary)
            ),
            Err(FinanceReconciliationError::InvalidAmount)
        );
    }
}

#[test]
fn minimum_i128_return_fails_and_eighteen_places_reconcile_exactly() {
    let mut minimum = row(1, Some("Возврат"), 1, 1);
    minimum.amounts.insert(
        "retailAmount".into(),
        WbFinanceDecimal {
            units: i128::MIN,
            scale: 0,
        },
    );
    assert_eq!(
        reconcile_wb_official_report(&[minimum], &evidence(), Some(&baseline(0, 0))),
        Err(FinanceReconciliationError::InvalidAmount)
    );
    let mut precise = row(1, Some("Продажа"), 1, 1);
    precise.amounts.insert(
        "retailAmount".into(),
        WbFinanceDecimal {
            units: 123_456_789_012_345_678,
            scale: 18,
        },
    );
    let mut summary = baseline(0, 1);
    summary
        .totals
        .insert("retailAmountSum".into(), exact(123_456_789_012_345_678, 18));
    // The payout helper uses scale two, so retain its independently supplied
    // baseline while checking all 18 retail fractional places exactly.
    summary.totals.insert("forPaySum".into(), exact(1, 2));
    assert_eq!(
        compare(&[precise], &summary).status,
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    );
}

#[test]
fn duplicate_rows_and_other_report_or_currency_are_rejected() {
    let original = row(1, Some("Продажа"), 100, 80);
    assert_eq!(
        reconcile_wb_official_report(
            &[original.clone(), original.clone()],
            &evidence(),
            Some(&baseline(200, 160))
        ),
        Err(FinanceReconciliationError::DuplicateRow)
    );
    for other in [
        WbFinanceDetailRow {
            report_id: 2,
            ..original.clone()
        },
        WbFinanceDetailRow {
            currency: "CNY".into(),
            ..original
        },
    ] {
        assert_eq!(
            reconcile_wb_official_report(&[other], &evidence(), Some(&baseline(100, 80))),
            Err(FinanceReconciliationError::InvalidEvidence)
        );
    }
}

#[test]
fn adjustment_business_date_does_not_remove_a_row_from_its_report() {
    let mut adjustment = row(1, Some("Продажа"), -100, -80);
    adjustment.business_date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    assert_eq!(
        compare(&[adjustment], &baseline(-100, -80)).status,
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    );
}

#[test]
fn terminal_and_whole_report_evidence_are_both_required() {
    for incomplete in [
        WbFinanceComparisonEvidence {
            terminal_observed: false,
            ..evidence()
        },
        WbFinanceComparisonEvidence {
            covers_entire_report: false,
            ..evidence()
        },
    ] {
        let result = reconcile_wb_official_report(
            &[row(1, Some("Продажа"), 100, 80)],
            &incomplete,
            Some(&baseline(100, 80)),
        )
        .unwrap();
        assert_eq!(
            result.unavailable_reason,
            Some(WbOfficialUnavailable::IncompleteDetails)
        );
        let mut summary = baseline(100, 80);
        summary.evidence.terminal_observed = incomplete.terminal_observed;
        summary.evidence.covers_entire_report = incomplete.covers_entire_report;
        assert_eq!(
            compare(&[row(1, Some("Продажа"), 100, 80)], &summary).unavailable_reason,
            Some(WbOfficialUnavailable::IncompleteBaseline)
        );
    }
}

#[test]
fn summary_scope_and_independent_observation_are_required() {
    let rows = [row(1, Some("Продажа"), 100, 80)];
    let mut wrong_scope = baseline(100, 80);
    wrong_scope.evidence.scope.account_id = "other-shop".into();
    assert_eq!(
        compare(&rows, &wrong_scope).unavailable_reason,
        Some(WbOfficialUnavailable::ScopeMismatch)
    );
    for same in [
        WbFinanceComparisonEvidence {
            observation_id: evidence().observation_id,
            ..baseline(100, 80).evidence
        },
        WbFinanceComparisonEvidence {
            source_sha256: evidence().source_sha256,
            ..baseline(100, 80).evidence
        },
    ] {
        let mut summary = baseline(100, 80);
        summary.evidence = same;
        assert_eq!(
            compare(&rows, &summary).unavailable_reason,
            Some(WbOfficialUnavailable::NonIndependentBaseline)
        );
    }
}

#[test]
fn daily_scope_is_not_claimed_covered_by_weekly_guide() {
    let mut daily = evidence();
    daily.scope.period = WbFinanceReportPeriod::Daily;
    let mut summary = baseline(100, 80);
    summary.evidence.scope = daily.scope.clone();
    let result =
        reconcile_wb_official_report(&[row(1, Some("Продажа"), 100, 80)], &daily, Some(&summary))
            .unwrap();
    assert_eq!(
        result.unavailable_reason,
        Some(WbOfficialUnavailable::UnsupportedPeriod)
    );
}

#[test]
fn missing_summary_wrong_baseline_and_empty_details_are_unavailable() {
    let rows = [row(1, Some("Продажа"), 100, 80)];
    assert_eq!(
        reconcile_wb_official_report(&rows, &evidence(), None)
            .unwrap()
            .unavailable_reason,
        Some(WbOfficialUnavailable::MissingBaseline)
    );
    let mut raw = baseline(100, 80);
    raw.kind = WbFinanceBaselineKind::IndependentRawColumns;
    assert_eq!(
        compare(&rows, &raw).unavailable_reason,
        Some(WbOfficialUnavailable::NotOfficialSummary)
    );
    assert_eq!(
        compare(&[], &baseline(0, 0)).unavailable_reason,
        Some(WbOfficialUnavailable::EmptyDetails)
    );
}

#[test]
fn extra_summary_fields_are_never_invented_as_verified_payout() {
    let mut summary = baseline(100, 80);
    summary
        .totals
        .insert("bankPaymentSum".into(), exact(999_999, 2));
    let result = compare(&[row(1, Some("Продажа"), 100, 80)], &summary);
    assert_eq!(
        result.status,
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    );
    assert_eq!(result.columns.len(), 2);
    assert!(
        result
            .columns
            .iter()
            .all(|column| column.summary_column != "bankPaymentSum")
    );
}

#[test]
fn malformed_evidence_is_rejected_before_any_success() {
    let mut malformed = evidence();
    malformed.source_sha256 = "not-a-sha256".into();
    assert_eq!(
        reconcile_wb_official_report(
            &[row(1, Some("Продажа"), 100, 80)],
            &malformed,
            Some(&baseline(100, 80))
        ),
        Err(FinanceReconciliationError::InvalidEvidence)
    );
}
