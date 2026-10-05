use super::*;

fn example() -> WbBaselineInput {
    serde_json::from_str(include_str!(
        "../../../../config/wb-ads-baseline.example.json"
    ))
    .unwrap()
}

#[test]
fn baseline_uses_weighted_ratios_and_separates_store_orders() {
    let report = analyze(example()).unwrap();
    assert_eq!(
        report.observed_sku_metrics.as_ref().unwrap().spend_minor,
        1000
    );
    assert_eq!(
        report
            .observed_sku_metrics
            .as_ref()
            .unwrap()
            .attributed_orders,
        10
    );
    assert_eq!(
        report.observed_sku_metrics.as_ref().unwrap().drr_bps,
        Some(1000)
    );
    assert_eq!(
        report.observed_sku_metrics.as_ref().unwrap().cpc_minor,
        Some(10)
    );
    assert_eq!(
        report.observed_sku_metrics.as_ref().unwrap().cpo_minor,
        Some(100)
    );
    assert_eq!(report.observed_store_sales.unwrap().ordered_units, 15);
    assert!(report.advertising_coverage_complete);
    assert!(report.sales_coverage_complete);
    assert!(!report.auto_apply_allowed);
    assert!(!report.attribution_maturity_verified);
    assert!(!report.causal_effect_verified);
    assert!(!report.current_composition_verified);
}

#[test]
fn missing_and_partial_publications_do_not_become_zero_days() {
    let mut input = example();
    input.coverage.remove(1);
    input.advertising.remove(1);
    input.sales[0].complete = false;
    let report = analyze(input).unwrap();
    assert!(!report.advertising_coverage_complete);
    assert_eq!(report.missing_ad_dates, vec![example().date_to]);
    assert!(!report.sales_coverage_complete);
    assert_eq!(report.missing_sales_dates, vec![example().date_from]);
    assert_eq!(
        report.campaigns[0].missing_ad_dates,
        vec![example().date_to]
    );
    assert_eq!(
        report.observed_sku_metrics.as_ref().unwrap().spend_minor,
        100
    );
    assert!(report.daily[1].sku_metrics.is_none());
    assert!(report.daily[1].snapshot_id.is_none());
    assert!(!report.daily[1].advertising_rows_observed);
}

#[test]
fn absent_sales_and_zero_denominators_remain_unknown() {
    let mut input = example();
    input.sales.clear();
    for row in &mut input.advertising {
        row.attributed_orders = 0;
        row.attributed_revenue_minor = 0;
    }
    let report = analyze(input).unwrap();
    assert!(report.observed_store_sales.is_none());
    assert!(!report.sales_coverage_complete);
    assert_eq!(report.observed_sku_metrics.as_ref().unwrap().drr_bps, None);
    assert_eq!(
        report.observed_sku_metrics.as_ref().unwrap().cpo_minor,
        None
    );
    assert_eq!(
        report.observed_sku_metrics.as_ref().unwrap().spend_minor,
        1000
    );
}

#[test]
fn empty_publications_and_explicit_zero_rows_are_distinct() {
    let mut input = example();
    input.advertising.clear();
    let report = analyze(input).unwrap();
    assert!(report.advertising_coverage_complete);
    assert!(report.observed_sku_metrics.is_none());
    assert!(report.unallocated_campaign_metrics.is_none());
    assert!(report.daily[0].sku_metrics.is_none());
    let mut input = example();
    for row in &mut input.advertising {
        row.spend_minor = 0;
        row.clicks = 0;
        row.attributed_orders = 0;
        row.attributed_revenue_minor = 0;
    }
    let report = analyze(input).unwrap();
    assert_eq!(report.observed_sku_metrics.unwrap().spend_minor, 0);
    assert!(report.daily[0].sku_metrics.is_some());
    assert!(report.daily[0].advertising_rows_observed);
}

#[test]
fn campaign_rows_without_sku_are_kept_out_of_sku_metrics() {
    let mut input = example();
    let mut row = input.advertising[0].clone();
    row.sku = None;
    row.spend_minor = 5000;
    input.advertising.push(row);
    let report = analyze(input).unwrap();
    assert_eq!(
        report.observed_sku_metrics.as_ref().unwrap().spend_minor,
        1000
    );
    assert_eq!(
        report
            .unallocated_campaign_metrics
            .as_ref()
            .unwrap()
            .spend_minor,
        5000
    );
    assert_eq!(
        report.campaigns[0]
            .sku_metrics
            .as_ref()
            .unwrap()
            .spend_minor,
        1000
    );
    assert_eq!(
        report.campaigns[0]
            .unallocated_campaign_metrics
            .as_ref()
            .unwrap()
            .spend_minor,
        5000
    );
}

#[test]
fn repeated_day_row_and_snapshot_are_rejected() {
    let mut input = example();
    input.coverage[1].date = input.coverage[0].date;
    assert_eq!(
        analyze(input).unwrap_err(),
        OptimizerError::DuplicateEvidence
    );
    let mut input = example();
    input.advertising.push(input.advertising[0].clone());
    assert_eq!(
        analyze(input).unwrap_err(),
        OptimizerError::DuplicateEvidence
    );
    let mut input = example();
    input.sales[1].snapshot_id = input.coverage[0].snapshot_id;
    assert_eq!(
        analyze(input).unwrap_err(),
        OptimizerError::DuplicateEvidence
    );
    let mut input = example();
    input.sales[1].date = input.sales[0].date;
    assert_eq!(
        analyze(input).unwrap_err(),
        OptimizerError::DuplicateEvidence
    );
}

#[test]
fn unfinished_days_future_observations_and_foreign_dates_are_rejected() {
    let mut input = example();
    input.coverage[0].observed_at = "2026-10-01T12:00:00Z".parse().unwrap();
    assert_eq!(analyze(input).unwrap_err(), OptimizerError::InvalidInput);
    let mut input = example();
    input.sales[0].observed_at = input.as_of + chrono::Duration::hours(1);
    assert_eq!(analyze(input).unwrap_err(), OptimizerError::InvalidInput);
    let mut input = example();
    input.advertising[0].date = input.date_from.pred_opt().unwrap();
    assert_eq!(analyze(input).unwrap_err(), OptimizerError::InvalidInput);
}

#[test]
fn overflow_is_an_error_and_input_order_does_not_change_digest_or_report() {
    let mut input = example();
    input.advertising[0].spend_minor = u64::MAX;
    assert_eq!(analyze(input).unwrap_err(), OptimizerError::Overflow);
    let original = serde_json::to_vec(&analyze(example()).unwrap()).unwrap();
    let mut input = example();
    input.coverage.reverse();
    input.advertising.reverse();
    input.sales.reverse();
    assert_eq!(
        original,
        serde_json::to_vec(&analyze(input).unwrap()).unwrap()
    );
}

#[test]
fn unknown_fields_and_oversized_files_are_rejected() {
    let mut input = serde_json::to_value(example()).unwrap();
    input["budget"] = 1000.into();
    assert_eq!(
        analyze_wb_baseline_export(&serde_json::to_vec(&input).unwrap()).unwrap_err(),
        OptimizerError::InvalidInput
    );
    assert_eq!(
        analyze_wb_baseline_export(&vec![b' '; MAX_INPUT_BYTES + 1]).unwrap_err(),
        OptimizerError::LimitExceeded
    );
}
