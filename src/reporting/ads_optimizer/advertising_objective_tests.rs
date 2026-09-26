use chrono::{Duration, NaiveDate, TimeZone, Utc};

use super::{RecommendationAction as Action, RecommendationReason as Reason, *};

fn fixture() -> ShadowInput {
    let observed_at = Utc.with_ymd_and_hms(2026, 9, 25, 12, 0, 0).unwrap();
    ShadowInput {
        version: 1,
        marketplace: SupportedMarketplace::Ozon,
        pricing_model: PricingModel::Cpc,
        currency: Currency::RUB,
        source_refs: vec![
            "advertising-observation".to_owned(),
            "stock-observation".to_owned(),
        ],
        account_id: "synthetic_drr_store".to_owned(),
        as_of: observed_at,
        observed_at,
        window_start: date(10),
        window_end: date(16),
        coverage_complete: true,
        objective: OptimizationObjective::TargetAdvertisingDrr { max_drr_bps: 1_000 },
        policy: OptimizerPolicy {
            total_daily_budget_minor: 110_000,
            attribution_lag_days: 7,
            min_mature_days: 7,
            min_clicks: 100,
            min_orders: 5,
            max_data_age_hours: 48,
            max_window_end_age_days: 14,
            max_stock_age_hours: 24,
            safety_discount_bps: 2_000,
            max_budget_increase_bps: 1_000,
            budget_decrease_bps: 2_000,
            zero_order_spend_allowances: 2,
        },
        products: vec![ProductEvidence {
            sku: 1,
            current_daily_budget_minor: 100_000,
            max_daily_budget_minor: 150_000,
            budget_constraint: Some(BudgetConstraintEvidence {
                limited: true,
                observed_at,
            }),
            daily: (10..=16)
                .map(|day| AdvertisingDay {
                    observed_at: None,
                    date: date(day),
                    clicks: 100,
                    spend_minor: 10_000,
                    direct_orders: 5,
                    direct_revenue_minor: 500_000,
                })
                .collect(),
            stock: Some(StockEvidence {
                sellable_units: 1_000,
                observed_at,
            }),
            economics: None,
        }],
    }
}

fn date(day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, day).unwrap()
}

fn one(input: ShadowInput) -> ProductRecommendation {
    recommend(input).unwrap().recommendations.remove(0)
}

#[test]
fn drr_mode_without_economics_supports_bounded_growth_and_reduction() {
    let input = fixture();
    let row = one(input.clone());
    assert_eq!(row.action, Action::TestBudgetIncrease);
    assert_eq!(row.suggested_daily_budget_minor, 110_000);
    assert_eq!(row.reasons, [Reason::WithinAdvertisingDrrCeiling]);
    assert_eq!(
        row.metrics.advertising_allowance_per_order_minor,
        Some(10_000)
    );
    assert_eq!(row.metrics.average_cpc_ceiling_minor, Some(400));
    assert_eq!(row.metrics.economic_average_cpc_ceiling_minor, None);
    assert_eq!(
        row.metrics.cpc_ceiling_basis,
        Some(CpcCeilingBasis::TargetAdvertisingDrr)
    );

    let mut above = input;
    for day in &mut above.products[0].daily {
        day.spend_minor = 50_000;
    }
    let row = one(above);
    assert_eq!(row.action, Action::ReviewReduction);
    assert_eq!(row.suggested_daily_budget_minor, 80_000);
    assert_eq!(row.reasons, [Reason::CpcAboveAdvertisingDrrCeiling]);
}

#[test]
fn drr_output_carries_the_operator_objective_and_avoids_an_economics_claim() {
    let report = recommend(fixture()).unwrap();
    assert_eq!(
        report.objective,
        OptimizationObjective::TargetAdvertisingDrr { max_drr_bps: 1_000 }
    );
    let json = serde_json::to_value(report).unwrap();
    assert_eq!(json["objective"]["kind"], "target_advertising_drr");
    assert_eq!(json["objective"]["max_drr_bps"], 1_000);
    let metrics = &json["recommendations"][0]["metrics"];
    assert_eq!(metrics["cpc_ceiling_basis"], "target_advertising_drr");
    assert!(metrics["economic_average_cpc_ceiling_minor"].is_null());
    assert_eq!(metrics["average_cpc_ceiling_minor"], 400);
}

#[test]
fn drr_without_orders_holds_without_an_invented_order_allowance() {
    let mut input = fixture();
    for day in &mut input.products[0].daily {
        day.direct_orders = 0;
        day.direct_revenue_minor = 0;
        day.spend_minor = 1_000_000;
    }
    let row = one(input);
    assert_eq!(row.action, Action::Hold);
    assert_eq!(row.reasons, [Reason::NoMatureOrdersForDrrAllowance]);
    assert_eq!(row.metrics.advertising_allowance_per_order_minor, None);
    assert_eq!(row.metrics.average_cpc_ceiling_minor, None);
    assert_eq!(row.suggested_daily_budget_minor, 100_000);
}

#[test]
fn drr_orders_without_revenue_are_unknown_not_a_zero_allowance() {
    let mut input = fixture();
    for day in &mut input.products[0].daily {
        day.direct_revenue_minor = 0;
    }
    let row = one(input);
    assert_eq!(row.action, Action::Hold);
    assert_eq!(row.reasons, [Reason::InconsistentAdvertisingEvidence]);
    assert_eq!(row.metrics.advertising_allowance_per_order_minor, None);
    assert_eq!(row.metrics.average_cpc_ceiling_minor, None);
}

#[test]
fn drr_rounds_only_display_metrics_and_compares_exact_allowed_spend() {
    for clicks in [3, 4] {
        let mut input = fixture();
        input.window_start = date(16);
        input.products[0].daily = vec![AdvertisingDay {
            observed_at: None,
            date: date(16),
            clicks,
            spend_minor: 30,
            direct_orders: 2,
            direct_revenue_minor: 101,
        }];
        input.objective = OptimizationObjective::TargetAdvertisingDrr { max_drr_bps: 3_333 };
        input.policy.safety_discount_bps = 1_000;
        input.policy.min_mature_days = 1;
        input.policy.min_clicks = 1;
        input.policy.min_orders = 1;
        let row = one(input.clone());
        // Total allowance is 101 * .3333 * .9 = 30.29697 kopecks.
        // Floor allowance/order = 16; using it would lose precision.
        assert_eq!(row.metrics.advertising_allowance_per_order_minor, Some(16));
        assert_eq!(
            row.metrics.average_cpc_ceiling_minor,
            Some(if clicks == 3 { 10 } else { 7 })
        );
        assert_eq!(row.action, Action::TestBudgetIncrease);
        input.products[0].daily[0].spend_minor = 31;
        assert_eq!(one(input).action, Action::ReviewReduction);
    }
}

#[test]
fn drr_exact_equality_does_not_reduce_after_display_rounding() {
    let mut input = fixture();
    input.window_start = date(16);
    input.products[0].daily = vec![AdvertisingDay {
        observed_at: None,
        date: date(16),
        clicks: 7,
        spend_minor: 30,
        direct_orders: 1,
        direct_revenue_minor: 300,
    }];
    input.policy.safety_discount_bps = 0;
    input.policy.min_mature_days = 1;
    input.policy.min_clicks = 1;
    input.policy.min_orders = 1;
    let row = one(input);
    assert_eq!(row.metrics.average_cpc_ceiling_minor, Some(4));
    assert_eq!(row.action, Action::TestBudgetIncrease);
}

#[test]
fn drr_keeps_coverage_freshness_stock_and_sample_gates() {
    for reason in [
        Reason::IncompleteCoverage,
        Reason::MissingDates,
        Reason::StaleAdvertising,
        Reason::StalePerformanceWindow,
        Reason::MissingStock,
        Reason::StaleStock,
        Reason::InsufficientMatureDays,
        Reason::InsufficientClicks,
        Reason::InsufficientOrders,
    ] {
        let mut input = fixture();
        for day in &mut input.products[0].daily {
            day.spend_minor = 1_000_000;
        }
        match reason {
            Reason::IncompleteCoverage => input.coverage_complete = false,
            Reason::MissingDates => {
                input.products[0].daily.remove(0);
            }
            Reason::StaleAdvertising => input.observed_at -= Duration::hours(49),
            Reason::StalePerformanceWindow => input.policy.max_window_end_age_days = 1,
            Reason::MissingStock => input.products[0].stock = None,
            Reason::StaleStock => {
                input.products[0].stock.as_mut().unwrap().observed_at -= Duration::hours(25);
            }
            Reason::InsufficientMatureDays => input.policy.min_mature_days = 8,
            Reason::InsufficientClicks => input.policy.min_clicks = 701,
            Reason::InsufficientOrders => input.policy.min_orders = 36,
            _ => unreachable!(),
        }
        let row = one(input);
        assert_eq!(row.action, Action::Hold, "{reason:?}");
        assert!(row.reasons.contains(&reason), "{reason:?}");
        assert!(!row.reasons.contains(&Reason::MissingEconomics));
        assert_eq!(row.metrics.average_cpc_ceiling_minor, None);
    }
}

#[test]
fn drr_growth_requires_recent_evidence_of_a_binding_budget() {
    for reason in [
        Reason::MissingBudgetConstraintEvidence,
        Reason::StaleBudgetConstraintEvidence,
        Reason::BudgetNotLimited,
    ] {
        let mut input = fixture();
        match reason {
            Reason::MissingBudgetConstraintEvidence => input.products[0].budget_constraint = None,
            Reason::StaleBudgetConstraintEvidence => {
                input.products[0]
                    .budget_constraint
                    .as_mut()
                    .unwrap()
                    .observed_at -= Duration::hours(49);
            }
            Reason::BudgetNotLimited => {
                input.products[0]
                    .budget_constraint
                    .as_mut()
                    .unwrap()
                    .limited = false;
            }
            _ => unreachable!(),
        }
        let row = one(input);
        assert_eq!(row.action, Action::Hold);
        assert!(row.reasons.contains(&reason));
        assert!(row.reasons.contains(&Reason::WithinAdvertisingDrrCeiling));
    }
}

#[test]
fn drr_uses_actual_observation_for_maturity_and_keeps_budget_caps() {
    let mut input = fixture();
    input.observed_at = Utc.with_ymd_and_hms(2026, 9, 23, 12, 0, 0).unwrap();
    input.policy.min_mature_days = 6;
    let last = input.products[0].daily.last_mut().unwrap();
    last.spend_minor = 1_000_000;
    last.direct_orders = 0;
    last.direct_revenue_minor = 0;
    input.products[0].max_daily_budget_minor = 105_000;
    input.policy.total_daily_budget_minor = 103_000;
    let row = one(input);
    assert_eq!(row.metrics.mature_days, 6);
    assert_eq!(row.metrics.excluded_recent_days, 1);
    assert_eq!(row.metrics.average_cpc_ceiling_minor, Some(400));
    assert_eq!(row.suggested_daily_budget_minor, 103_000);
    assert!(row.reasons.contains(&Reason::ProductBudgetCap));
    assert!(row.reasons.contains(&Reason::PortfolioBudgetCap));
}

#[test]
fn drr_objective_is_explicit_and_its_range_is_validated() {
    for value in [0, 100_001, u32::MAX] {
        let mut input = fixture();
        input.objective = OptimizationObjective::TargetAdvertisingDrr { max_drr_bps: value };
        assert_eq!(recommend(input).unwrap_err(), OptimizerError::InvalidInput);
    }
    for value in [1, 10_001, 100_000] {
        let mut input = fixture();
        input.objective = OptimizationObjective::TargetAdvertisingDrr { max_drr_bps: value };
        assert!(recommend(input).is_ok());
    }
    let mut json = serde_json::to_value(fixture()).unwrap();
    json.as_object_mut().unwrap().remove("objective");
    let legacy = parse_input(&serde_json::to_vec(&json).unwrap()).unwrap();
    assert_eq!(legacy.objective, OptimizationObjective::ExpectedEconomics);
    assert!(one(legacy).reasons.contains(&Reason::MissingEconomics));
    json["objective"] = serde_json::json!({"kind": "target_advertising_drr"});
    assert!(parse_input(&serde_json::to_vec(&json).unwrap()).is_err());
    json["objective"] =
        serde_json::json!({"kind": "target_advertising_drr", "max_drr_bps": 1_000, "profit": true});
    assert!(parse_input(&serde_json::to_vec(&json).unwrap()).is_err());
}
