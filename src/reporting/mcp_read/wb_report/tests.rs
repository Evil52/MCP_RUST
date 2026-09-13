use super::*;
use crate::reporting::{
    finance_reconciliation::{WbFinanceComparisonEvidence, WbFinanceReportScope},
    wb_finance_source::WbFinanceDecimal,
    wb_report_source::WbOfficialReportSummary,
};
use chrono::{NaiveDate, TimeZone, Utc};

const REPORT: u64 = 9_007_199_254_740_993;

fn account() -> AccountScope {
    AccountScope::new("account-wb".into(), Marketplace::Wildberries).unwrap()
}

fn query() -> WbReportReconciliationQuery {
    WbReportReconciliationQuery {
        report_id: REPORT,
        after_rrd_id: 0,
        limit: 1,
    }
}

fn fixture() -> (StoredWbOfficialReport, Vec<WbFinanceDetailRow>) {
    let scope = WbFinanceReportScope {
        account_id: "account-wb".into(),
        report_id: REPORT,
        currency: "RUB".into(),
        period: WbFinanceReportPeriod::Weekly,
        date_from: NaiveDate::from_ymd_opt(2026, 9, 7).unwrap(),
        date_to: NaiveDate::from_ymd_opt(2026, 9, 13).unwrap(),
    };
    let evidence = |name: &str, digest: &str| WbFinanceComparisonEvidence {
        scope: scope.clone(),
        observation_id: name.into(),
        source_sha256: digest.repeat(64),
        terminal_observed: true,
        covers_entire_report: true,
    };
    let summary = WbOfficialReportSummary {
        scope: scope.clone(),
        created_date: scope.date_to.succ_opt().unwrap(),
        report_type: 1,
        amounts: BTreeMap::from([
            (
                "retailAmountSum".into(),
                WbFinanceDecimal {
                    units: 200,
                    scale: 0,
                },
            ),
            (
                "forPaySum".into(),
                WbFinanceDecimal {
                    units: 180,
                    scale: 0,
                },
            ),
        ]),
    };
    let rows = [REPORT + 1, REPORT + 2]
        .into_iter()
        .map(|id| WbFinanceDetailRow {
            rrd_id: id,
            report_id: REPORT,
            business_date: scope.date_from.pred_opt().unwrap(),
            sku: Some(REPORT),
            currency: "RUB".into(),
            document_type: Some("Продажа".into()),
            operation_type: Some("Недоверенный текст".into()),
            quantity: Some(1),
            amounts: BTreeMap::from([
                (
                    "retailAmount".into(),
                    WbFinanceDecimal {
                        units: 100,
                        scale: 0,
                    },
                ),
                (
                    "forPay".into(),
                    WbFinanceDecimal {
                        units: 90,
                        scale: 0,
                    },
                ),
            ]),
        })
        .collect::<Vec<_>>();
    let summary_evidence = evidence("summary-1", "a");
    let details_evidence = evidence("details-1", "b");
    let baseline = WbFinanceBaseline {
        kind: WbFinanceBaselineKind::OfficialReportSummary,
        evidence: summary_evidence.clone(),
        totals: summary
            .amounts
            .iter()
            .map(|(name, amount)| {
                (
                    name.clone(),
                    ExactFinanceTotal {
                        units: amount.units,
                        scale: amount.scale,
                    },
                )
            })
            .collect(),
    };
    let comparison =
        reconcile_wb_official_report(&rows, &details_evidence, Some(&baseline)).unwrap();
    (
        StoredWbOfficialReport {
            snapshot_id: 7,
            summary,
            summary_evidence,
            details_evidence,
            comparison,
            row_count: 2,
            actor_id: "finance".into(),
            content_sha256: "c".repeat(64),
            published_at: Utc.with_ymd_and_hms(2026, 9, 14, 10, 0, 0).unwrap(),
        },
        rows,
    )
}

fn recompute(stored: &mut StoredWbOfficialReport, rows: &[WbFinanceDetailRow]) {
    let baseline = WbFinanceBaseline {
        kind: WbFinanceBaselineKind::OfficialReportSummary,
        evidence: stored.summary_evidence.clone(),
        totals: stored
            .summary
            .amounts
            .iter()
            .map(|(name, amount)| {
                (
                    name.clone(),
                    ExactFinanceTotal {
                        units: amount.units,
                        scale: amount.scale,
                    },
                )
            })
            .collect(),
    };
    stored.comparison =
        reconcile_wb_official_report(rows, &stored.details_evidence, Some(&baseline)).unwrap();
}

#[test]
fn missing_official_report_is_unavailable_without_zero_or_comparison() {
    let result = page_result(&account(), query(), None).unwrap();
    assert_eq!(result.state, DataState::Unavailable);
    assert!(result.report.is_none());
    assert!(result.comparison.is_none());
    assert!(result.rows.is_empty());
    assert!(result.next_after_rrd_id.is_none());
}

#[test]
fn published_primary_totals_are_stable_across_exact_cursor_pages() {
    let (stored, rows) = fixture();
    let first = page_result(
        &account(),
        query(),
        Some((stored.clone(), rows[..1].to_vec(), true)),
    )
    .unwrap();
    let second = page_result(
        &account(),
        WbReportReconciliationQuery {
            after_rrd_id: rows[0].rrd_id,
            ..query()
        },
        Some((stored, rows[1..].to_vec(), false)),
    )
    .unwrap();
    assert_eq!(first.state, DataState::Complete);
    assert_eq!(first.next_after_rrd_id, Some((REPORT + 1).to_string()));
    assert_eq!(second.next_after_rrd_id, None);
    assert_eq!(first.report, second.report);
    assert_eq!(first.comparison, second.comparison);
    let comparison = first.comparison.unwrap();
    assert_eq!(
        comparison.status,
        WbReportComparisonStatus::PrimaryTotalsMatch
    );
    assert_eq!(
        comparison.columns[0].detail_total.as_ref().unwrap().units,
        "200"
    );
    assert_eq!(first.rows[0].report_id, REPORT.to_string());
    assert_eq!(first.rows[0].rrd_id, (REPORT + 1).to_string());
}

#[test]
fn native_mismatch_and_unavailable_statuses_survive_public_projection() {
    let (mut stored, mut rows) = fixture();
    stored.summary.amounts.get_mut("forPaySum").unwrap().units = 179;
    recompute(&mut stored, &rows);
    let result = page_result(
        &account(),
        WbReportReconciliationQuery {
            limit: 10,
            ..query()
        },
        Some((stored.clone(), rows.clone(), false)),
    )
    .unwrap();
    assert_eq!(
        result.comparison.unwrap().status,
        WbReportComparisonStatus::Mismatch
    );
    rows[0].amounts.remove("forPay");
    recompute(&mut stored, &rows);
    let result = page_result(
        &account(),
        WbReportReconciliationQuery {
            limit: 10,
            ..query()
        },
        Some((stored, rows, false)),
    )
    .unwrap();
    let comparison = result.comparison.unwrap();
    assert_eq!(result.state, DataState::Complete);
    assert_eq!(comparison.status, WbReportComparisonStatus::Unavailable);
    assert_eq!(
        comparison.unavailable_reason.as_deref(),
        Some("missing_detail_amount")
    );
    assert_eq!(comparison.columns[1].detail_total, None);
}

#[test]
fn sql_fixed_scale_totals_compare_exactly_with_source_scale_without_rounding() {
    let (mut stored, mut rows) = fixture();
    for amount in rows.iter_mut().flat_map(|row| row.amounts.values_mut()) {
        amount.units *= 100;
        amount.scale = 2;
    }
    for amount in stored.summary.amounts.values_mut() {
        amount.units *= 100;
        amount.scale = 2;
    }
    recompute(&mut stored, &rows);
    for column in &mut stored.comparison.columns {
        for value in [
            &mut column.detail_total,
            &mut column.summary_total,
            &mut column.difference,
        ]
        .into_iter()
        .flatten()
        {
            value.units *= 10_i128.pow(18 - value.scale);
            value.scale = 18;
        }
    }
    let full_query = WbReportReconciliationQuery {
        limit: 10,
        ..query()
    };
    let result = page_result(
        &account(),
        full_query,
        Some((stored.clone(), rows.clone(), false)),
    )
    .unwrap();
    assert_eq!(
        result.comparison.unwrap().status,
        WbReportComparisonStatus::PrimaryTotalsMatch
    );
    stored.comparison.columns[0]
        .detail_total
        .as_mut()
        .unwrap()
        .units += 1;
    assert!(page_result(&account(), full_query, Some((stored, rows, false))).is_err());
}

#[test]
fn published_sixteen_decimal_commissions_and_large_coefficients_remain_exact() {
    let (stored, mut rows) = fixture();
    let coefficient = 19_223_372_036_854_775_808_i128;
    rows[0].amounts.insert(
        "vw".into(),
        WbFinanceDecimal {
            units: coefficient,
            scale: 16,
        },
    );
    let result = page_result(
        &account(),
        WbReportReconciliationQuery {
            limit: 10,
            ..query()
        },
        Some((stored, rows, false)),
    )
    .unwrap();
    assert_eq!(result.rows[0].amounts["vw"].units, coefficient.to_string());
    assert_eq!(result.rows[0].amounts["vw"].scale, 16);
    let encoded = serde_json::to_value(&result).unwrap();
    assert_eq!(
        encoded["rows"][0]["amounts"]["vw"]["units"],
        "19223372036854775808"
    );
    assert_eq!(encoded["comparison"]["status"], "primary_totals_match");
}

#[test]
fn foreign_scope_incomplete_evidence_wrong_totals_and_bad_cursor_pages_fail_closed() {
    let (stored, rows) = fixture();
    let mut corrupt = stored.clone();
    corrupt.summary.scope.account_id = "foreign".into();
    assert!(
        page_result(
            &account(),
            query(),
            Some((corrupt, rows[..1].to_vec(), true))
        )
        .is_err()
    );
    let mut corrupt = stored.clone();
    corrupt.details_evidence.terminal_observed = false;
    assert!(
        page_result(
            &account(),
            query(),
            Some((corrupt, rows[..1].to_vec(), true))
        )
        .is_err()
    );
    let mut corrupt = stored.clone();
    corrupt.comparison.columns[0].difference = Some(ExactFinanceTotal { units: 1, scale: 0 });
    assert!(
        page_result(
            &account(),
            query(),
            Some((corrupt, rows[..1].to_vec(), true))
        )
        .is_err()
    );
    let mut corrupt = stored.clone();
    corrupt.comparison.columns[0].detail_total = Some(ExactFinanceTotal {
        units: 300,
        scale: 0,
    });
    assert!(
        page_result(
            &account(),
            WbReportReconciliationQuery {
                limit: 10,
                ..query()
            },
            Some((corrupt, rows.clone(), false))
        )
        .is_err()
    );
    assert!(
        page_result(
            &account(),
            query(),
            Some((stored.clone(), rows.clone(), true))
        )
        .is_err()
    );
    assert!(page_result(&account(), query(), Some((stored.clone(), vec![], true))).is_err());
    assert!(
        page_result(
            &account(),
            query(),
            Some((stored.clone(), rows[..1].to_vec(), false))
        )
        .is_err()
    );
    assert!(
        page_result(
            &account(),
            WbReportReconciliationQuery {
                after_rrd_id: rows[0].rrd_id,
                ..query()
            },
            Some((stored, rows[..1].to_vec(), false))
        )
        .is_err()
    );
}

#[test]
fn query_validation_rejects_cross_market_and_nonpositive_or_oversized_ids() {
    for query in [
        WbReportReconciliationQuery {
            report_id: 0,
            ..query()
        },
        WbReportReconciliationQuery {
            report_id: u64::MAX,
            ..query()
        },
        WbReportReconciliationQuery {
            after_rrd_id: u64::MAX,
            ..query()
        },
        WbReportReconciliationQuery {
            limit: 0,
            ..query()
        },
        WbReportReconciliationQuery {
            limit: 1001,
            ..query()
        },
    ] {
        assert_eq!(
            validate_query(&account(), query),
            Err(ReportingReadError::InvalidRequest)
        );
    }
    let other = AccountScope::new("account-ozon".into(), Marketplace::Ozon).unwrap();
    assert_eq!(
        validate_query(&other, query()),
        Err(ReportingReadError::InvalidRequest)
    );
}

#[test]
fn corrupt_extreme_scales_and_coefficient_overflow_fail_before_decimal_work() {
    let (mut stored, rows) = fixture();
    stored.comparison.columns[0].difference = Some(ExactFinanceTotal {
        units: 0,
        scale: u32::MAX,
    });
    assert!(
        page_result(
            &account(),
            query(),
            Some((stored, rows[..1].to_vec(), true))
        )
        .is_err()
    );
    let (mut stored, rows) = fixture();
    stored.comparison.columns[0].detail_total = Some(ExactFinanceTotal {
        units: i128::MAX,
        scale: 0,
    });
    stored.comparison.columns[0].summary_total = Some(ExactFinanceTotal {
        units: 1,
        scale: 18,
    });
    assert!(
        page_result(
            &account(),
            query(),
            Some((stored, rows[..1].to_vec(), true))
        )
        .is_err()
    );
}
