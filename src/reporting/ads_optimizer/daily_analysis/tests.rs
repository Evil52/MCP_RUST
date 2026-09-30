use super::*;
use serde_json::json;

fn scope() -> DailyAnalysisScope {
    DailyAnalysisScope {
        store_id: "store_a".to_owned(),
        campaign_ids: vec![11, 22],
        date_from: NaiveDate::from_ymd_opt(2026, 9, 1).unwrap(),
        date_to: NaiveDate::from_ymd_opt(2026, 9, 2).unwrap(),
        observed_at: "2026-09-03T12:00:00Z".parse().unwrap(),
        target_drr_bps: Some(1500),
    }
}

fn row() -> Value {
    json!({"id":"11", "date":"2026-09-01", "title":"external title",
        "views":"100", "clicks":"10", "moneySpent":"15,01",
        "orders":"2", "ordersMoney":"100.00"})
}

#[test]
fn money_is_exact_and_unknown_days_and_campaigns_remain_visible() {
    let report = analyze_daily_response(scope(), &json!({"rows":[row()]})).unwrap();
    assert_eq!(report.missing_campaign_ids, vec![22]);
    assert_eq!(report.campaigns.len(), 1);
    let campaign = &report.campaigns[0];
    assert_eq!(
        (campaign.spend_minor, campaign.revenue_minor),
        (1501, 10000)
    );
    assert_eq!(campaign.average_cpc_minor, Some(150));
    assert_eq!(campaign.missing_dates, vec![scope().date_to]);
    assert!(report.signals.contains(&DailyAnalysisSignal {
        campaign_id: 11,
        kind: DailyAnalysisSignalKind::AboveTargetDrr
    }));
    assert!(!report.auto_apply_allowed);
    assert!(!report.attribution_maturity_verified);
    assert!(!report.direct_sku_attribution_verified);
    assert!(!report.source_coverage_complete);
    assert!(
        !serde_json::to_string(&report)
            .unwrap()
            .contains("external title")
    );
}

#[test]
fn empty_response_is_unknown_rather_than_zero_performance() {
    let report = analyze_daily_response(scope(), &json!({"rows":[]})).unwrap();
    assert_eq!(report.missing_campaign_ids, vec![11, 22]);
    assert!(report.campaigns.is_empty());
    assert!(
        report
            .signals
            .iter()
            .all(|s| s.kind == DailyAnalysisSignalKind::NoRows)
    );
}

#[test]
fn duplicates_out_of_scope_campaigns_and_dates_are_rejected() {
    assert_eq!(
        analyze_daily_response(scope(), &json!({"rows":[row(),row()]})).unwrap_err(),
        OptimizerError::DuplicateEvidence
    );
    for (field, value) in [
        ("id", json!(33)),
        ("date", json!("2026-08-31")),
        ("moneySpent", json!("-1")),
    ] {
        let mut data = row();
        data[field] = value;
        assert_eq!(
            analyze_daily_response(scope(), &json!({"rows":[data]})).unwrap_err(),
            OptimizerError::InvalidInput
        );
    }
}

#[test]
fn exact_ratio_detects_a_breach_hidden_by_display_rounding() {
    let mut data = row();
    data["moneySpent"] = json!("150.01");
    data["ordersMoney"] = json!("1000.00");
    let report = analyze_daily_response(scope(), &json!({"rows":[data]})).unwrap();
    assert_eq!(report.campaigns[0].reported_drr_bps, Some(1500));
    assert!(
        report
            .signals
            .iter()
            .any(|s| s.kind == DailyAnalysisSignalKind::AboveTargetDrr)
    );
}

#[test]
fn scope_limits_are_checked_even_for_empty_results() {
    let mut invalid = Vec::new();
    let mut s = scope();
    s.campaign_ids.clear();
    invalid.push(s);
    let mut s = scope();
    s.campaign_ids = vec![11, 11];
    invalid.push(s);
    let mut s = scope();
    s.campaign_ids = (1..=11).collect();
    invalid.push(s);
    let mut s = scope();
    s.target_drr_bps = Some(0);
    invalid.push(s);
    let mut s = scope();
    s.date_to = s.date_from - chrono::Duration::days(1);
    invalid.push(s);
    let mut s = scope();
    s.date_to = s.date_from + chrono::Duration::days(31);
    invalid.push(s);
    let mut s = scope();
    s.observed_at = "2026-08-31T12:00:00Z".parse().unwrap();
    invalid.push(s);
    for s in invalid {
        assert_eq!(
            analyze_daily_response(s, &json!({"rows":[]})).unwrap_err(),
            OptimizerError::InvalidInput
        );
    }
}

#[test]
fn absent_target_does_not_invent_an_efficiency_threshold() {
    let mut s = scope();
    s.target_drr_bps = None;
    let report = analyze_daily_response(s, &json!({"rows":[row()]})).unwrap();
    assert!(!report.signals.iter().any(|s| matches!(
        s.kind,
        DailyAnalysisSignalKind::AboveTargetDrr | DailyAnalysisSignalKind::WithinTargetDrr
    )));
}

#[test]
fn zero_revenue_and_orders_do_not_produce_division_or_budget_advice() {
    let mut data = row();
    data["orders"] = json!("0");
    data["ordersMoney"] = json!("0,00");
    let report = analyze_daily_response(scope(), &json!({"rows":[data]})).unwrap();
    assert_eq!(report.campaigns[0].reported_drr_bps, None);
    assert!(
        report
            .signals
            .iter()
            .any(|s| s.kind == DailyAnalysisSignalKind::SpendWithoutReportedOrders)
    );
    assert!(!report.auto_apply_allowed);
}

#[test]
fn observed_zero_spend_and_missing_revenue_have_distinct_signals() {
    for (spend, revenue, expected) in [
        ("0,00", "100,00", DailyAnalysisSignalKind::NoObservedSpend),
        ("15,00", "100,00", DailyAnalysisSignalKind::WithinTargetDrr),
        ("15,00", "0,00", DailyAnalysisSignalKind::RevenueUnavailable),
    ] {
        let mut data = row();
        data["moneySpent"] = json!(spend);
        data["ordersMoney"] = json!(revenue);
        let report = analyze_daily_response(scope(), &json!({"rows":[data]})).unwrap();
        assert!(report.signals.iter().any(|s| s.kind == expected));
    }
}

#[test]
fn raw_export_rejects_unknown_envelope_fields_and_oversized_payloads() {
    let export = json!({"scope":scope(),"response":{"rows":[]},"write":true});
    assert_eq!(
        analyze_daily_export(&serde_json::to_vec(&export).unwrap()).unwrap_err(),
        OptimizerError::InvalidInput
    );
    assert_eq!(
        analyze_daily_export(&vec![b' '; MAX_INPUT_BYTES + 1]).unwrap_err(),
        OptimizerError::LimitExceeded
    );
    let response = json!({"rows":[],"unexpected": "x".repeat(MAX_INPUT_BYTES)});
    assert_eq!(
        analyze_daily_response(scope(), &response).unwrap_err(),
        OptimizerError::LimitExceeded
    );
}
