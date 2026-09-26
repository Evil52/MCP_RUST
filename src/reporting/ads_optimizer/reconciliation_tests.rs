use chrono::{Duration, TimeZone};
use serde_json::{Value, json};

use super::*;

fn day(day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, day).unwrap()
}

fn fixture() -> ReconciliationInput {
    let observed_at = Utc.with_ymd_and_hms(2026, 9, 25, 8, 0, 0).unwrap();
    ReconciliationInput {
        version: 1,
        account_id: "synthetic_test_store".to_owned(),
        as_of: observed_at + Duration::hours(1),
        window_start: day(23),
        window_end: day(24),
        sku_source: SkuReconciliationSource {
            account_id: "synthetic_test_store".to_owned(),
            source_ref: "published:advertising:100".to_owned(),
            observed_at,
            coverage_complete: true,
            rows: vec![ReconciliationSkuRow {
                date: day(24),
                campaign_id: 11,
                sku: 101,
                clicks: 20,
                spend_minor: 300,
                direct_orders: 2,
                direct_revenue_minor: 10_000,
                model_orders: 1,
                model_revenue_minor: 5_000,
            }],
        },
        campaign_source: CampaignReconciliationSource {
            account_id: "synthetic_test_store".to_owned(),
            source_ref: "performance:daily:2026-09-24".to_owned(),
            observed_at,
            coverage_complete: true,
            rows: vec![ReconciliationCampaignRow {
                date: day(24),
                campaign_id: 11,
                clicks: 20,
                spend_minor: 300,
                orders: 3,
                revenue_minor: 15_000,
            }],
        },
    }
}

#[test]
fn arithmetic_match_never_promotes_model_revenue_to_verified_optimizer_evidence() {
    let report = reconcile(fixture()).unwrap();
    assert_eq!(report.mode, ReconciliationMode::DiagnosticOnly);
    assert!(!report.semantic_equivalence_verified);
    assert!(report.reasons.is_empty());
    let row = &report.rows[0];
    let direct = row.daily_minus_direct.as_ref().unwrap();
    assert_eq!((direct.orders, direct.revenue_minor), (1, 5_000));
    let arithmetic = row.direct_plus_model_arithmetic.as_ref().unwrap();
    assert!(arithmetic.arithmetic_only);
    assert!(arithmetic.orders_match && arithmetic.revenue_match);
    assert_eq!(
        (
            arithmetic.combined_orders,
            arithmetic.combined_revenue_minor
        ),
        (3, 15_000)
    );
    assert_eq!(
        row.sku_totals.as_ref().unwrap().direct_revenue_minor,
        10_000
    );
    assert_eq!(row.sku_totals.as_ref().unwrap().model_revenue_minor, 5_000);
    assert_eq!(
        row.reasons,
        [
            ReconciliationReason::DirectOrdersDiffer,
            ReconciliationReason::DirectRevenueDiffers
        ]
    );
    let encoded = serde_json::to_value(report).unwrap();
    assert_eq!(encoded["mode"], "diagnostic_only");
    assert!(encoded.get("recommendations").is_none());
    assert!(encoded.get("products").is_none());
}

#[test]
fn grouping_sums_skus_without_merging_campaigns_or_dates() {
    let mut input = fixture();
    let mut second_sku = input.sku_source.rows[0].clone();
    second_sku.sku = 102;
    input.sku_source.rows.push(second_sku);
    let mut second_campaign = input.sku_source.rows[0].clone();
    second_campaign.campaign_id = 12;
    input.sku_source.rows.push(second_campaign);
    let mut previous_day = input.sku_source.rows[0].clone();
    previous_day.date = day(23);
    input.sku_source.rows.push(previous_day);
    let report = reconcile(input).unwrap();
    assert_eq!(
        report
            .rows
            .iter()
            .map(|row| (row.date, row.campaign_id))
            .collect::<Vec<_>>(),
        [(day(23), 11), (day(24), 11), (day(24), 12)]
    );
    let sum = report.rows[1].sku_totals.as_ref().unwrap();
    assert_eq!((sum.sku_count, sum.clicks, sum.spend_minor), (2, 40, 600));
    assert_eq!(
        (
            sum.direct_orders,
            sum.direct_revenue_minor,
            sum.model_orders,
            sum.model_revenue_minor
        ),
        (4, 20_000, 2, 10_000)
    );
    assert_eq!(report.rows[0].sku_totals.as_ref().unwrap().sku_count, 1);
    assert_eq!(report.rows[2].sku_totals.as_ref().unwrap().sku_count, 1);
}

#[test]
fn independent_order_revenue_and_spend_differences_preserve_sign_and_one_kopeck() {
    let mut input = fixture();
    input.campaign_source.rows[0].clicks = 19;
    input.campaign_source.rows[0].spend_minor = 301;
    input.campaign_source.rows[0].orders = 4;
    input.campaign_source.rows[0].revenue_minor = 9_999;
    let report = reconcile(input).unwrap();
    let row = &report.rows[0];
    let delta = row.daily_minus_direct.as_ref().unwrap();
    assert_eq!(
        (
            delta.clicks,
            delta.spend_minor,
            delta.orders,
            delta.revenue_minor
        ),
        (-1, 1, 2, -1)
    );
    let arithmetic = row.direct_plus_model_arithmetic.as_ref().unwrap();
    assert!(!arithmetic.orders_match && !arithmetic.revenue_match);
    assert_eq!(
        (
            arithmetic.daily_minus_combined_orders,
            arithmetic.daily_minus_combined_revenue_minor
        ),
        (1, -5_001)
    );
    for reason in [
        ReconciliationReason::ClicksDiffer,
        ReconciliationReason::SpendDiffers,
        ReconciliationReason::DirectOrdersDiffer,
        ReconciliationReason::DirectRevenueDiffers,
        ReconciliationReason::DirectPlusModelOrdersDiffer,
        ReconciliationReason::DirectPlusModelRevenueDiffers,
    ] {
        assert!(row.reasons.contains(&reason));
    }
}

#[test]
fn missing_rows_remain_unknown_even_when_sources_claim_complete_coverage() {
    let mut input = fixture();
    input.campaign_source.rows[0].campaign_id = 12;
    let report = reconcile(input).unwrap();
    assert!(
        report
            .reasons
            .contains(&ReconciliationReason::NoComparableRows)
    );
    assert_eq!(report.rows.len(), 2);
    let sku_only = &report.rows[0];
    assert!(sku_only.sku_totals.is_some() && sku_only.campaign_totals.is_none());
    assert!(
        sku_only
            .reasons
            .contains(&ReconciliationReason::MissingCampaignRow)
    );
    let daily_only = &report.rows[1];
    assert!(daily_only.sku_totals.is_none() && daily_only.campaign_totals.is_some());
    assert!(
        daily_only
            .reasons
            .contains(&ReconciliationReason::MissingSkuRow)
    );
    for row in &report.rows {
        assert!(row.daily_minus_direct.is_none());
        assert!(row.direct_plus_model_arithmetic.is_none());
    }
    let encoded = serde_json::to_value(report).unwrap();
    assert!(encoded["rows"][0]["campaign_totals"].is_null());
    assert!(encoded["rows"][0]["daily_minus_direct"].is_null());
}

#[test]
fn empty_exports_never_synthesize_zero_days() {
    let mut input = fixture();
    input.sku_source.rows.clear();
    input.campaign_source.rows.clear();
    let report = reconcile(input).unwrap();
    assert!(report.rows.is_empty());
    assert_eq!(report.reasons, [ReconciliationReason::NoComparableRows]);
}

#[test]
fn incomplete_coverage_and_distinct_observation_times_remain_visible_on_matches() {
    let mut input = fixture();
    input.sku_source.coverage_complete = false;
    input.campaign_source.coverage_complete = false;
    input.campaign_source.observed_at += Duration::minutes(10);
    let report = reconcile(input.clone()).unwrap();
    assert_eq!(report.sku_source.account_id, input.account_id);
    assert_eq!(report.sku_source.source_ref, input.sku_source.source_ref);
    assert_eq!(
        report.campaign_source.source_ref,
        input.campaign_source.source_ref
    );
    assert_eq!(report.sku_source.observed_at, input.sku_source.observed_at);
    assert_eq!(
        report.campaign_source.observed_at,
        input.campaign_source.observed_at
    );
    assert_eq!(
        (
            report.sku_source.row_count,
            report.campaign_source.row_count
        ),
        (1, 1)
    );
    for reason in [
        ReconciliationReason::SkuCoverageIncomplete,
        ReconciliationReason::CampaignCoverageIncomplete,
        ReconciliationReason::SourceObservationsDiffer,
    ] {
        assert!(report.reasons.contains(&reason));
        assert!(report.rows[0].reasons.contains(&reason));
    }
    assert!(
        report.rows[0]
            .direct_plus_model_arithmetic
            .as_ref()
            .unwrap()
            .revenue_match
    );
    assert!(!report.semantic_equivalence_verified);
}

#[test]
fn observations_earlier_than_requested_window_end_are_explicit() {
    let mut input = fixture();
    input.sku_source.observed_at -= Duration::days(2);
    input.campaign_source.observed_at -= Duration::days(2);
    let report = reconcile(input).unwrap();
    assert!(
        report
            .reasons
            .contains(&ReconciliationReason::SkuObservationBeforeWindowEnd)
    );
    assert!(
        report
            .reasons
            .contains(&ReconciliationReason::CampaignObservationBeforeWindowEnd)
    );
}

#[test]
fn canonical_report_and_digest_ignore_row_order_but_include_provenance_and_values() {
    let mut input = fixture();
    let mut sku = input.sku_source.rows[0].clone();
    sku.campaign_id = 12;
    let mut campaign = input.campaign_source.rows[0].clone();
    campaign.campaign_id = 12;
    input.sku_source.rows.push(sku);
    input.campaign_source.rows.push(campaign);
    let expected = reconcile(input.clone()).unwrap();
    input.sku_source.rows.reverse();
    input.campaign_source.rows.reverse();
    assert_eq!(reconcile(input.clone()).unwrap(), expected);
    assert_eq!(expected.input_sha256.len(), 64);
    assert!(
        expected
            .input_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    );
    let text = serde_json::to_vec_pretty(&input).unwrap();
    assert_eq!(
        reconcile(parse_reconciliation_input(&text).unwrap()).unwrap(),
        expected
    );
    for field in 0..4 {
        let mut changed = input.clone();
        match field {
            0 => changed.sku_source.source_ref.push_str("-revised"),
            1 => changed.campaign_source.observed_at += Duration::seconds(1),
            2 => changed.sku_source.coverage_complete = false,
            _ => changed.sku_source.rows[0].spend_minor += 1,
        }
        assert_ne!(
            reconcile(changed).unwrap().input_sha256,
            expected.input_sha256
        );
    }
}

#[test]
fn both_duplicate_key_contracts_fail_closed() {
    let mut duplicate_sku = fixture();
    let row = duplicate_sku.sku_source.rows[0].clone();
    duplicate_sku.sku_source.rows.push(row);
    assert_eq!(
        reconcile(duplicate_sku).unwrap_err(),
        OptimizerError::DuplicateEvidence
    );
    let mut duplicate_campaign = fixture();
    let row = duplicate_campaign.campaign_source.rows[0].clone();
    duplicate_campaign.campaign_source.rows.push(row);
    assert_eq!(
        reconcile(duplicate_campaign).unwrap_err(),
        OptimizerError::DuplicateEvidence
    );
}

#[test]
fn foreign_accounts_dates_and_invalid_identifiers_are_rejected() {
    for variant in 0..12 {
        let mut input = fixture();
        match variant {
            0 => input.sku_source.account_id = "other_store".to_owned(),
            1 => input.campaign_source.account_id = "other_store".to_owned(),
            2 => input.sku_source.rows[0].date = day(22),
            3 => input.campaign_source.rows[0].date = day(25),
            4 => input.sku_source.rows[0].campaign_id = 0,
            5 => input.campaign_source.rows[0].campaign_id = 0,
            6 => input.sku_source.rows[0].sku = 0,
            7 => input.sku_source.rows[0].sku = u64::MAX,
            8 => input.campaign_source.rows[0].campaign_id = u64::MAX,
            9 => input.account_id = "invalid/account".to_owned(),
            10 => input.sku_source.source_ref = "\ninvalid-reference".to_owned(),
            _ => input.campaign_source.source_ref = " ".to_owned(),
        }
        assert_eq!(
            reconcile(input).unwrap_err(),
            OptimizerError::InvalidInput,
            "case {variant}"
        );
    }
}

#[test]
fn version_time_order_and_ninety_day_window_are_enforced() {
    for variant in 0..7 {
        let mut input = fixture();
        match variant {
            0 => input.version = 2,
            1 => input.sku_source.observed_at = input.as_of + Duration::seconds(1),
            2 => input.campaign_source.observed_at = input.as_of + Duration::seconds(1),
            3 => input.window_start = day(25),
            4 => input.window_end = day(26),
            5 => input.sku_source.observed_at = Utc.with_ymd_and_hms(1999, 1, 1, 0, 0, 0).unwrap(),
            _ => input.window_start = NaiveDate::from_ymd_opt(1999, 1, 1).unwrap(),
        }
        assert_eq!(
            reconcile(input).unwrap_err(),
            OptimizerError::InvalidInput,
            "case {variant}"
        );
    }
    let mut input = fixture();
    input.window_start = input.window_end - Duration::days(89);
    assert!(reconcile(input.clone()).is_ok());
    input.window_start -= Duration::days(1);
    assert_eq!(reconcile(input).unwrap_err(), OptimizerError::LimitExceeded);
}

#[test]
fn serde_contract_rejects_unknown_missing_fractional_negative_and_duplicate_fields() {
    let baseline = serde_json::to_value(fixture()).unwrap();
    for (pointer, value) in [
        ("/extra", json!(true)),
        ("/sku_source/extra", json!(true)),
        ("/campaign_source/extra", json!(true)),
        ("/sku_source/rows/0/extra", json!(true)),
        ("/campaign_source/rows/0/extra", json!(true)),
        ("/sku_source/rows/0/spend_minor", json!(-1)),
        ("/campaign_source/rows/0/orders", json!(1.5)),
        (
            "/sku_source/rows/0/campaign_id",
            Value::String("11".to_owned()),
        ),
    ] {
        let mut encoded = baseline.clone();
        let (parent, key) = pointer.rsplit_once('/').unwrap();
        let parent = if parent.is_empty() {
            &mut encoded
        } else {
            encoded.pointer_mut(parent).unwrap()
        };
        parent
            .as_object_mut()
            .unwrap()
            .insert(key.to_owned(), value);
        assert_eq!(
            parse_reconciliation_input(&serde_json::to_vec(&encoded).unwrap()).unwrap_err(),
            OptimizerError::InvalidInput,
            "{pointer}"
        );
    }
    let mut missing = baseline;
    missing["sku_source"]["rows"][0]
        .as_object_mut()
        .unwrap()
        .remove("model_orders");
    assert_eq!(
        parse_reconciliation_input(&serde_json::to_vec(&missing).unwrap()).unwrap_err(),
        OptimizerError::InvalidInput
    );
    let duplicate = serde_json::to_string(&fixture()).unwrap().replacen(
        "\"version\":1",
        "\"version\":1,\"version\":1",
        1,
    );
    assert_eq!(
        parse_reconciliation_input(duplicate.as_bytes()).unwrap_err(),
        OptimizerError::InvalidInput
    );
}

#[test]
fn signed_extremes_and_values_above_float_precision_serialize_exactly() {
    for reverse in [false, true] {
        let mut input = fixture();
        let sku = &mut input.sku_source.rows[0];
        sku.clicks = if reverse { 0 } else { u64::MAX };
        sku.spend_minor = sku.clicks;
        sku.direct_orders = sku.clicks;
        sku.direct_revenue_minor = sku.clicks;
        sku.model_orders = 0;
        sku.model_revenue_minor = 0;
        let campaign = &mut input.campaign_source.rows[0];
        campaign.clicks = if reverse { u64::MAX } else { 0 };
        campaign.spend_minor = campaign.clicks;
        campaign.orders = campaign.clicks;
        campaign.revenue_minor = campaign.clicks;
        let serialized = serde_json::to_vec(&input).unwrap();
        let report = reconcile(parse_reconciliation_input(&serialized).unwrap()).unwrap();
        let expected = if reverse {
            i128::from(u64::MAX)
        } else {
            -i128::from(u64::MAX)
        };
        assert_eq!(
            report.rows[0]
                .daily_minus_direct
                .as_ref()
                .unwrap()
                .revenue_minor,
            expected
        );
        let encoded = serde_json::to_value(&report).unwrap();
        assert_eq!(encoded["delta_encoding"], "signed_decimal_string");
        assert_eq!(
            encoded["rows"][0]["daily_minus_direct"]["revenue_minor"],
            expected.to_string()
        );
        assert_eq!(
            encoded["rows"][0]["direct_plus_model_arithmetic"]["daily_minus_combined_orders"],
            expected.to_string()
        );
        assert_eq!(
            encoded["rows"][0]["sku_totals"]["direct_revenue_minor"].as_u64(),
            Some(if reverse { 0 } else { u64::MAX })
        );
    }
}

#[test]
fn every_aggregate_and_direct_plus_model_addition_fails_closed_on_overflow() {
    for field in 0..6 {
        let mut input = fixture();
        let mut second = input.sku_source.rows[0].clone();
        second.sku = 102;
        let first = &mut input.sku_source.rows[0];
        match field {
            0 => first.clicks = u64::MAX,
            1 => first.spend_minor = u64::MAX,
            2 => first.direct_orders = u64::MAX,
            3 => first.direct_revenue_minor = u64::MAX,
            4 => first.model_orders = u64::MAX,
            _ => first.model_revenue_minor = u64::MAX,
        }
        input.sku_source.rows.push(second);
        assert_eq!(
            reconcile(input).unwrap_err(),
            OptimizerError::Overflow,
            "aggregate {field}"
        );
    }
    for revenue in [false, true] {
        let mut input = fixture();
        if revenue {
            input.sku_source.rows[0].direct_revenue_minor = u64::MAX;
        } else {
            input.sku_source.rows[0].direct_orders = u64::MAX;
        }
        assert_eq!(reconcile(input).unwrap_err(), OptimizerError::Overflow);
    }
}

#[test]
fn combined_row_limit_and_raw_byte_limit_are_enforced_at_boundaries() {
    let mut input = fixture();
    let sku = input.sku_source.rows[0].clone();
    let campaign = input.campaign_source.rows[0].clone();
    input.sku_source.rows = (1..=12_500)
        .map(|id| ReconciliationSkuRow {
            campaign_id: id,
            ..sku.clone()
        })
        .collect();
    input.campaign_source.rows = (1..=12_500)
        .map(|id| ReconciliationCampaignRow {
            campaign_id: id,
            ..campaign.clone()
        })
        .collect();
    assert_eq!(validate(&input), Ok(()));
    let raw = serde_json::to_vec(&input).unwrap();
    assert!(raw.len() <= MAX_INPUT_BYTES);
    let report = reconcile(parse_reconciliation_input(&raw).unwrap()).unwrap();
    assert_eq!(report.rows.len(), 12_500);
    input.campaign_source.rows.push(ReconciliationCampaignRow {
        campaign_id: 12_501,
        ..campaign
    });
    assert_eq!(reconcile(input).unwrap_err(), OptimizerError::LimitExceeded);
    assert_eq!(
        parse_reconciliation_input(&vec![b' '; MAX_INPUT_BYTES + 1]).unwrap_err(),
        OptimizerError::LimitExceeded
    );
}

#[test]
fn direct_api_also_enforces_canonical_byte_limit() {
    let mut input = fixture();
    let row = input.sku_source.rows[0].clone();
    input.campaign_source.rows.clear();
    input.sku_source.rows = (1..=25_000)
        .map(|sku| ReconciliationSkuRow { sku, ..row.clone() })
        .collect();
    assert_eq!(validate(&input), Ok(()));
    assert!(serde_json::to_vec(&input).unwrap().len() > MAX_INPUT_BYTES);
    assert_eq!(reconcile(input).unwrap_err(), OptimizerError::LimitExceeded);
}
