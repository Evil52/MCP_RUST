use chrono::{Duration, TimeZone};
use serde_json::json;

use super::*;

fn day(value: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, value).unwrap()
}

fn fixture() -> CampaignHistoryInput {
    let observed_at = Utc.with_ymd_and_hms(2026, 9, 26, 13, 0, 0).unwrap();
    CampaignHistoryInput {
        version: 1,
        account_id: "synthetic_store".to_owned(),
        source_ref: "performance:daily:synthetic".to_owned(),
        observed_at,
        as_of: observed_at + Duration::minutes(1),
        window_start: day(15),
        window_end: day(17),
        source_coverage_complete: true,
        rows: vec![
            ReconciliationCampaignRow {
                date: day(15),
                campaign_id: 11,
                clicks: 10,
                spend_minor: 1_000,
                orders: 0,
                revenue_minor: 0,
            },
            ReconciliationCampaignRow {
                date: day(17),
                campaign_id: 11,
                clicks: 20,
                spend_minor: 2_000,
                orders: 2,
                revenue_minor: 10_000,
            },
            ReconciliationCampaignRow {
                date: day(16),
                campaign_id: 12,
                clicks: 0,
                spend_minor: 0,
                orders: 0,
                revenue_minor: 0,
            },
        ],
    }
}

#[test]
fn campaign_totals_leave_gaps_unknown_and_attribution_unverified() {
    let report = analyze_campaign_history(fixture()).unwrap();
    assert_eq!(report.mode, CampaignHistoryMode::DiagnosticOnly);
    assert!(!report.direct_sku_attribution_verified);
    let first = &report.campaigns[0];
    assert_eq!(first.campaign_id, 11);
    assert_eq!((first.observed_days, first.expected_days), (2, 3));
    assert_eq!(first.missing_dates, [day(16)]);
    assert!(!first.calendar_complete);
    assert_eq!((first.clicks, first.spend_minor), (30, 3_000));
    assert_eq!((first.orders, first.revenue_minor), (2, 10_000));
    assert_eq!(first.reported_drr_bps, Some(3_000));
    assert_eq!(first.average_cpc_minor, Some(100));
    let second = &report.campaigns[1];
    assert_eq!(second.reported_drr_bps, None);
    assert_eq!(second.average_cpc_minor, None);
    let encoded = serde_json::to_value(&report).unwrap();
    assert_eq!(encoded["mode"], "diagnostic_only");
    assert!(encoded.get("recommendations").is_none());
    assert!(encoded.get("products").is_none());
}

#[test]
fn row_order_does_not_change_digest_or_totals() {
    let input = fixture();
    let expected = analyze_campaign_history(input.clone()).unwrap();
    let mut reversed = input;
    reversed.rows.reverse();
    assert_eq!(analyze_campaign_history(reversed).unwrap(), expected);
}

#[test]
fn duplicates_out_of_window_and_future_observation_are_rejected() {
    let mut duplicate = fixture();
    duplicate.rows.push(duplicate.rows[0].clone());
    assert_eq!(
        analyze_campaign_history(duplicate).unwrap_err(),
        OptimizerError::DuplicateEvidence
    );
    let mut outside = fixture();
    outside.rows[0].date = day(14);
    assert_eq!(
        analyze_campaign_history(outside).unwrap_err(),
        OptimizerError::InvalidInput
    );
    let mut future = fixture();
    future.observed_at = future.as_of + Duration::seconds(1);
    assert_eq!(
        analyze_campaign_history(future).unwrap_err(),
        OptimizerError::InvalidInput
    );
}

#[test]
fn malformed_input_and_arithmetic_overflow_fail_closed() {
    let mut raw = serde_json::to_value(fixture()).unwrap();
    raw["rows"][0]["unexpected"] = json!(true);
    assert_eq!(
        parse_campaign_history_input(&serde_json::to_vec(&raw).unwrap()).unwrap_err(),
        OptimizerError::InvalidInput
    );
    let mut large = fixture();
    large.rows[0].spend_minor = u64::MAX;
    assert_eq!(
        analyze_campaign_history(large).unwrap_err(),
        OptimizerError::Overflow
    );
}
