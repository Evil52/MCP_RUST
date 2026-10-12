use super::*;
use serde_json::json;

fn evidence() -> RecommendationEvidence {
    serde_json::from_str(include_str!(
        "../../../../config/wb-ads-recommendations.example.json"
    ))
    .unwrap()
}

fn change_performance(
    input: &mut RecommendationEvidence,
    spend: u64,
    clicks: u64,
    orders: u64,
    revenue: u64,
) {
    for day in input.statistics[0]["days"].as_array_mut().unwrap() {
        for (key, value) in [
            ("sum", spend),
            ("clicks", clicks),
            ("orders", orders),
            ("sum_price", revenue),
        ] {
            day[key] = json!(value);
            day["apps"][0]["nms"][0][key] = json!(value);
        }
    }
}

#[test]
fn mature_search_history_has_weighted_metrics_and_bounded_review_proposal() {
    let report = recommend(evidence()).unwrap();
    let row = &report.recommendations[0];
    assert_eq!(row.action, Action::TestIncrease);
    assert_eq!(row.suggested_step_bps, Some(500));
    assert_eq!(row.current_search_bid_minor, Some(700));
    let metrics = &row.windows[1].metrics;
    assert_eq!(metrics.spend_minor, 15_000);
    assert_eq!(metrics.revenue_minor, 300_000);
    assert_eq!(metrics.drr_bps, Some(500));
    assert_eq!(metrics.cpo_minor, Some(2500));
    assert_eq!(metrics.observed_days, 3);
    assert_eq!(metrics.excluded_recent_days, 3);
    assert_eq!(row.recent_store_ordered_units, Some(2));
    assert_eq!(
        report.sales_period_start.as_deref(),
        Some("2026-10-10T19:00:00Z")
    );
    assert!(!report.auto_apply_allowed);
    assert!(!report.attribution_finality_verified);
    assert!(!report.causal_effect_verified);
}

#[test]
fn target_is_required_and_a_high_drr_proposes_a_reviewed_decrease() {
    let mut input = evidence();
    input.target_drr_bps = None;
    let report = recommend(input).unwrap();
    assert_eq!(report.recommendations[0].action, Action::Hold);
    assert!(
        report.recommendations[0]
            .reasons
            .contains(&Reason::TargetDrrRequired)
    );
    let mut input = evidence();
    change_performance(&mut input, 200, 15, 2, 1000);
    let report = recommend(input).unwrap();
    assert_eq!(report.recommendations[0].action, Action::ReviewDecrease);
    assert_eq!(report.recommendations[0].suggested_step_bps, Some(-500));
}

#[test]
fn zero_orders_are_a_funnel_review_with_no_numeric_bid_or_pause_command() {
    let mut input = evidence();
    change_performance(&mut input, 200, 40, 0, 0);
    let report = recommend(input).unwrap();
    let row = &report.recommendations[0];
    assert_eq!(row.action, Action::ReviewDecrease);
    assert_eq!(row.reasons, vec![Reason::SpendWithoutOrders]);
    assert_eq!(row.suggested_step_bps, None);
    assert_eq!(row.windows[1].metrics.drr_bps, None);
    assert_eq!(row.recent_store_ordered_units, Some(2));
    assert!(!report.auto_apply_allowed);
}

#[test]
fn replaying_old_statistics_does_not_create_late_attribution() {
    let mut input = evidence();
    input.statistics_observed_at = "2026-10-08T09:00:00Z".parse().unwrap();
    let report = recommend(input).unwrap();
    let row = &report.recommendations[0];
    assert_eq!(row.action, Action::Hold);
    assert!(row.reasons.contains(&Reason::StaleHistory));
    assert_eq!(row.windows[1].metrics.spend_minor, 0);
    assert_eq!(row.windows[1].metrics.observed_days, 0);
    assert_eq!(row.windows[1].metrics.drr_bps, None);
}

#[test]
fn gaps_and_unreconciled_sku_days_block_only_the_affected_campaign() {
    let mut input = evidence();
    input.campaign_ids.push(22);
    let mut other = input.composition["adverts"][0].clone();
    other["id"] = json!(22);
    input.composition["adverts"]
        .as_array_mut()
        .unwrap()
        .push(other);
    let report = recommend(input).unwrap();
    assert_eq!(report.recommendations[0].action, Action::TestIncrease);
    assert!(
        report.recommendations[1]
            .reasons
            .contains(&Reason::MissingHistory)
    );
    let mut input = evidence();
    input.statistics[0]["days"][0]["apps"][0]["nms"][0]["sum"] = json!(49);
    let report = recommend(input).unwrap();
    assert_eq!(report.recommendations[0].action, Action::Hold);
    assert!(
        report.recommendations[0]
            .reasons
            .contains(&Reason::UnallocatedSkuHistory)
    );
}

#[test]
fn recent_clicks_do_not_pass_sample_threshold_and_stock_unknown_is_not_zero() {
    let mut input = evidence();
    input.statistics[0]["days"][0]["date"] = json!("2026-10-11T00:00:00Z");
    let report = recommend(input).unwrap();
    assert!(
        report.recommendations[0]
            .reasons
            .contains(&Reason::InsufficientMatureObservations)
    );
    let mut input = evidence();
    input.stocks["source_as_of"] = json!("2026-10-12T07:59:59Z");
    let report = recommend(input).unwrap();
    assert_eq!(report.recommendations[0].fbw_units, None);
    assert!(
        report.recommendations[0]
            .reasons
            .contains(&Reason::UnknownOrStaleFbwStock)
    );
    let mut input = evidence();
    input.stocks["rows"][0]["sellable_units"] = json!(0);
    let report = recommend(input).unwrap();
    assert!(
        report.recommendations[0]
            .reasons
            .contains(&Reason::FbwZeroCheckFbs)
    );
}

#[test]
fn mixed_placements_cpm_paused_or_removed_products_never_get_bid_proposals() {
    for path in [
        "/settings/placements/recommendations",
        "/settings/payment_type",
        "/status",
        "/nm_settings/0/nm_id",
    ] {
        let mut input = evidence();
        *input.composition["adverts"][0].pointer_mut(path).unwrap() = match path {
            "/status" => json!(11),
            "/nm_settings/0/nm_id" => json!(123),
            "/settings/payment_type" => json!("cpm"),
            _ => json!(true),
        };
        assert!(
            recommend(input)
                .unwrap()
                .recommendations
                .iter()
                .all(|row| row.action == Action::Hold && row.suggested_step_bps.is_none())
        );
    }
}

#[test]
fn foreign_rows_partial_pages_and_duplicate_warehouses_are_rejected() {
    for case in 0..4 {
        let mut input = evidence();
        match case {
            0 => input.stocks["rows"][0]["account_id"] = json!("foreign"),
            1 => input.stocks["next_offset"] = json!(1000),
            2 => input.sales["rows"][0]["snapshot_id"] = json!(103),
            _ => {
                let duplicate = input.stocks["rows"][0].clone();
                input.stocks["rows"].as_array_mut().unwrap().push(duplicate);
                input.stocks["total_rows"] = json!(2);
            }
        }
        assert!(recommend(input).is_err());
    }
}

#[test]
fn moscow_open_day_and_exact_target_band_boundaries_are_respected() {
    let mut input = evidence();
    input.as_of = "2026-10-11T21:00:00Z".parse().unwrap();
    input.statistics_observed_at = input.as_of;
    input.composition_observed_at = input.as_of;
    assert!(recommend(input).is_ok());
    let mut input = evidence();
    input.as_of = "2026-10-11T20:59:59Z".parse().unwrap();
    input.statistics_observed_at = input.as_of;
    input.composition_observed_at = input.as_of;
    assert!(recommend(input).is_err());
    for spend in [135, 165] {
        let mut input = evidence();
        change_performance(&mut input, spend, 15, 2, 1000);
        let report = recommend(input).unwrap();
        assert_eq!(report.recommendations[0].action, Action::Hold);
        assert_eq!(
            report.recommendations[0].reasons,
            vec![Reason::WithinTargetBand]
        );
    }
}

#[test]
fn no_observations_serialize_as_unknown_and_low_fbw_blocks_growth() {
    let mut input = evidence();
    input.statistics = json!([]);
    let report = serde_json::to_value(recommend(input).unwrap()).unwrap();
    assert!(report["recommendations"][0]["windows"][1]["metrics"]["spend_minor"].is_null());
    assert!(report["recommendations"][0]["windows"][1]["metrics"]["orders"].is_null());
    let mut input = evidence();
    input.stocks["rows"][0]["sellable_units"] = json!(5);
    let report = recommend(input).unwrap();
    assert_eq!(report.recommendations[0].action, Action::Hold);
    assert!(
        report.recommendations[0]
            .reasons
            .contains(&Reason::LowFbwBuffer)
    );
    let mut input = evidence();
    input.sales["quality"] = json!("stale");
    let report = recommend(input).unwrap();
    assert_eq!(report.recommendations[0].action, Action::Hold);
    assert!(
        report.recommendations[0]
            .reasons
            .contains(&Reason::UnknownSalesForIncrease)
    );
}

#[test]
fn mixed_placements_allow_efficiency_review_but_never_a_search_bid_step() {
    let mut input = evidence();
    input.composition["adverts"][0]["settings"]["placements"]["recommendations"] = json!(true);
    change_performance(&mut input, 200, 15, 2, 1000);
    let row = recommend(input).unwrap().recommendations.remove(0);
    assert_eq!(row.action, Action::ReviewDecrease);
    assert_eq!(row.suggested_step_bps, None);
    assert!(row.reasons.contains(&Reason::AboveTargetDrr));
    assert!(row.reasons.contains(&Reason::UnsupportedPlacementOrPricing));
    let mut input = evidence();
    input.target_drr_bps = None;
    change_performance(&mut input, 200, 40, 0, 0);
    let row = recommend(input).unwrap().recommendations.remove(0);
    assert_eq!(row.action, Action::ReviewDecrease);
    assert_eq!(row.suggested_step_bps, None);
    assert!(row.reasons.contains(&Reason::SpendWithoutOrders));
}

#[test]
fn rounded_drr_cannot_hide_an_exact_threshold_breach() {
    let mut input = evidence();
    change_performance(&mut input, 165, 15, 2, 1000);
    input.statistics[0]["days"][0]["sum"] = json!("165.01");
    input.statistics[0]["days"][0]["apps"][0]["nms"][0]["sum"] = json!("165.01");
    let row = recommend(input).unwrap().recommendations.remove(0);
    assert_eq!(row.windows[1].metrics.drr_bps, Some(1650));
    assert_eq!(row.action, Action::ReviewDecrease);
    assert_eq!(row.suggested_step_bps, Some(-500));
}

#[test]
fn associated_orders_are_kept_in_campaign_decisions_without_being_assigned_to_advertised_sku() {
    let mut input = evidence();
    change_performance(&mut input, 200, 15, 2, 1000);
    for day in input.statistics[0]["days"].as_array_mut().unwrap() {
        day["sum_price"] = json!(5000);
        day["orders"] = json!(6);
        day["apps"][0]["nms"].as_array_mut().unwrap().push(json!({
            "nmId": 123, "sum": 0, "sum_price": 4000, "orders": 4, "views": 0, "clicks": 0
        }));
    }
    let report = recommend(input).unwrap();
    let row = report
        .recommendations
        .iter()
        .find(|row| row.sku == 99)
        .unwrap();
    assert_eq!(row.windows[1].metrics.drr_bps, Some(2000));
    assert_eq!(row.campaign_windows[1].metrics.drr_bps, Some(400));
    assert_eq!(row.action, Action::Hold);
    assert!(row.reasons.contains(&Reason::CampaignLevelAttribution));
    assert!(!row.reasons.contains(&Reason::AboveTargetDrr));
    assert_eq!(row.suggested_step_bps, None);
    let associated = report
        .recommendations
        .iter()
        .find(|row| row.sku == 123)
        .unwrap();
    assert_eq!(associated.action, Action::Hold);
    assert!(
        associated
            .reasons
            .contains(&Reason::OutsideCurrentComposition)
    );
}
