use chrono::{Duration, NaiveDate, TimeZone, Utc};
use serde_json::json;

use super::{RecommendationAction as Action, RecommendationReason as Reason, *};

fn fixture() -> ShadowInput {
    let observed_at = Utc.with_ymd_and_hms(2026, 9, 25, 12, 0, 0).unwrap();
    ShadowInput {
        version: 1,
        objective: OptimizationObjective::ExpectedEconomics,
        marketplace: SupportedMarketplace::Ozon,
        pricing_model: PricingModel::Cpc,
        currency: Currency::RUB,
        source_refs: vec!["advertising-v1".to_owned(), "stock-v1".to_owned()],
        account_id: "synthetic_test_store".to_owned(),
        as_of: observed_at,
        observed_at,
        window_start: date(1),
        window_end: date(14),
        coverage_complete: true,
        policy: OptimizerPolicy {
            total_daily_budget_minor: 110_000,
            attribution_lag_days: 7,
            min_mature_days: 7,
            min_clicks: 100,
            min_orders: 5,
            max_data_age_hours: 48,
            max_window_end_age_days: 21,
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
            daily: (1..=14)
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
            budget_constraint: Some(BudgetConstraintEvidence {
                limited: true,
                observed_at,
            }),
            economics: Some(OrderEconomics {
                basis: EconomicsBasis::ExpectedPerDirectAttributedOrder,
                source_ref: "reviewed-economics-v1".to_owned(),
                valid_from: date(1),
                valid_to: NaiveDate::from_ymd_opt(2026, 10, 31).unwrap(),
                reviewed_at: observed_at,
                expected_revenue_minor: 100_000,
                expected_cost_of_goods_minor: 50_000,
                expected_other_costs_minor: 25_000,
                return_reserve_minor: 5_000,
                target_profit_minor: 5_000,
            }),
        }],
    }
}

fn date(day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, day).unwrap()
}

fn one(input: ShadowInput) -> ProductRecommendation {
    let mut report = recommend(input).unwrap();
    assert_eq!(report.recommendations.len(), 1);
    report.recommendations.remove(0)
}

#[test]
fn complete_evidence_yields_bounded_trial_and_explained_integer_metrics() {
    let report = recommend(fixture()).unwrap();
    assert_eq!(report.mode, "shadow");
    assert_eq!(report.currency, "RUB");
    assert_eq!(report.allocated_daily_budget_minor, 110_000);
    assert_eq!(report.unallocated_daily_budget_minor, 0);
    assert_eq!(report.input_sha256.len(), 64);
    let row = &report.recommendations[0];
    assert_eq!(row.action, Action::TestBudgetIncrease);
    assert_eq!(row.reasons, [Reason::WithinEconomicCeiling]);
    assert_eq!(row.suggested_daily_budget_minor, 110_000);
    assert_eq!(
        row.metrics.advertising_allowance_per_order_minor,
        Some(15_000)
    );
    assert_eq!(row.metrics.economic_average_cpc_ceiling_minor, Some(600));
    assert_eq!(row.metrics.mature_days, 14);
    assert_eq!(row.metrics.mature_clicks, 1_400);
    assert_eq!(row.metrics.mature_orders, 70);
    assert_eq!(row.metrics.mature_spend_minor, 140_000);
    assert_eq!(row.metrics.mature_direct_revenue_minor, 7_000_000);
}

#[test]
fn rounds_fractional_budget_changes_and_final_cpc_to_whole_kopecks() {
    let mut input = fixture();
    input.products[0].current_daily_budget_minor = 101;
    let row = one(input.clone());
    assert_eq!(row.suggested_daily_budget_minor, 111);
    for day in &mut input.products[0].daily {
        day.spend_minor = 70_000;
    }
    let row = one(input);
    assert_eq!(row.action, Action::ReviewReduction);
    assert_eq!(row.suggested_daily_budget_minor, 80);

    let mut exact = fixture();
    exact.window_end = date(1);
    exact.products[0].daily.truncate(1);
    exact.products[0].daily[0].clicks = 3;
    exact.products[0].daily[0].direct_orders = 1;
    exact.products[0].daily[0].spend_minor = 9;
    exact.policy.min_mature_days = 1;
    exact.policy.max_window_end_age_days = 30;
    exact.policy.min_clicks = 1;
    exact.policy.min_orders = 1;
    exact.policy.safety_discount_bps = 1_000;
    let economics = exact.products[0].economics.as_mut().unwrap();
    economics.expected_revenue_minor = 10;
    economics.expected_cost_of_goods_minor = 0;
    economics.expected_other_costs_minor = 0;
    economics.return_reserve_minor = 0;
    economics.target_profit_minor = 0;
    let row = one(exact);
    // floor(10 * 1 * 0.9 / 3) = 3; flooring before the haircut gives 2.
    assert_eq!(row.metrics.economic_average_cpc_ceiling_minor, Some(3));
    assert_eq!(row.action, Action::TestBudgetIncrease);
}

#[test]
fn product_cap_that_still_allows_an_increase_keeps_the_increase_action() {
    let mut input = fixture();
    input.products[0].max_daily_budget_minor = 105_000;
    let row = one(input);
    assert_eq!(row.suggested_daily_budget_minor, 105_000);
    assert_eq!(row.action, Action::TestBudgetIncrease);
    assert!(row.reasons.contains(&Reason::ProductBudgetCap));
}

#[test]
fn maturity_uses_actual_source_observation_instead_of_report_clock() {
    let mut input = fixture();
    input.observed_at = Utc.with_ymd_and_hms(2026, 9, 20, 12, 0, 0).unwrap();
    input.policy.max_data_age_hours = 168;
    // These two days are after September 12, the last fully mature date at
    // the source observation with a seven-day lag. Waiting until September
    // 25 without re-observation must not make their counters mature.
    for day in &mut input.products[0].daily[12..] {
        day.spend_minor = 1_000_000;
        day.direct_orders = 0;
    }
    let row = one(input);
    assert_eq!(row.action, Action::TestBudgetIncrease);
    assert_eq!(row.metrics.mature_days, 12);
    assert_eq!(row.metrics.excluded_recent_days, 2);
    assert_eq!(row.metrics.mature_spend_minor, 120_000);
    assert_eq!(row.metrics.all_period_spend_minor, 2_120_000);
    assert_eq!(row.metrics.economic_average_cpc_ceiling_minor, Some(600));
}

#[test]
fn missing_or_stale_evidence_holds_without_performance_reductions() {
    let cases = [
        Reason::IncompleteCoverage,
        Reason::MissingDates,
        Reason::MissingEconomics,
        Reason::MissingStock,
        Reason::StaleAdvertising,
        Reason::StaleStock,
        Reason::EconomicsOutsidePeriod,
    ];
    for reason in cases {
        let mut input = fixture();
        for day in &mut input.products[0].daily {
            day.spend_minor = 1_000_000;
        }
        match reason {
            Reason::IncompleteCoverage => input.coverage_complete = false,
            Reason::MissingDates => {
                input.products[0].daily.remove(3);
            }
            Reason::MissingEconomics => input.products[0].economics = None,
            Reason::MissingStock => input.products[0].stock = None,
            Reason::StaleAdvertising => input.observed_at -= Duration::hours(49),
            Reason::StaleStock => {
                input.products[0].stock.as_mut().unwrap().observed_at -= Duration::hours(25);
            }
            Reason::EconomicsOutsidePeriod => {
                input.products[0].economics.as_mut().unwrap().valid_to = date(24);
            }
            _ => unreachable!(),
        }
        let row = one(input);
        assert_eq!(row.action, Action::Hold, "{reason:?}");
        assert_eq!(row.suggested_daily_budget_minor, 100_000, "{reason:?}");
        assert!(row.reasons.contains(&reason));
        assert_eq!(row.metrics.economic_average_cpc_ceiling_minor, None);
    }
}

#[test]
fn stale_zero_stock_is_unknown_but_fresh_zero_stock_justifies_review_pause() {
    let mut input = fixture();
    let stock = input.products[0].stock.as_mut().unwrap();
    stock.sellable_units = 0;
    stock.observed_at -= Duration::hours(25);
    let stale = one(input.clone());
    assert_eq!(stale.action, Action::Hold);
    assert_eq!(stale.reasons, [Reason::StaleStock]);
    input.products[0].stock.as_mut().unwrap().observed_at = input.as_of;
    input.coverage_complete = false;
    let fresh = one(input);
    assert_eq!(fresh.action, Action::ReviewPause);
    assert_eq!(fresh.suggested_daily_budget_minor, 0);
    assert!(fresh.reasons.contains(&Reason::OutOfStock));
    assert!(fresh.reasons.contains(&Reason::IncompleteCoverage));
}

#[test]
fn nonpositive_advertising_allowance_never_becomes_unsigned_profit() {
    for cost in [65_000, 80_000] {
        let mut input = fixture();
        input.products[0]
            .economics
            .as_mut()
            .unwrap()
            .expected_cost_of_goods_minor = cost;
        let row = one(input);
        assert_eq!(row.metrics.advertising_allowance_per_order_minor, Some(0));
        assert_eq!(row.action, Action::ReviewPause);
        assert_eq!(row.suggested_daily_budget_minor, 0);
        assert!(row.reasons.contains(&Reason::NoAdvertisingAllowance));
    }
}

#[test]
fn insufficient_mature_days_clicks_or_orders_produce_specific_hold_reasons() {
    for reason in [
        Reason::InsufficientMatureDays,
        Reason::InsufficientClicks,
        Reason::InsufficientOrders,
    ] {
        let mut input = fixture();
        match reason {
            Reason::InsufficientMatureDays => input.policy.min_mature_days = 15,
            Reason::InsufficientClicks => input.policy.min_clicks = 1_401,
            Reason::InsufficientOrders => input.policy.min_orders = 71,
            _ => unreachable!(),
        }
        let row = one(input);
        assert_eq!(row.action, Action::Hold);
        assert_eq!(row.suggested_daily_budget_minor, 100_000);
        assert_eq!(row.reasons, [reason]);
    }
}

#[test]
fn zero_order_reduction_begins_at_exact_allowance_multiple() {
    let mut input = fixture();
    for day in &mut input.products[0].daily {
        day.direct_orders = 0;
        day.direct_revenue_minor = 0;
        day.spend_minor = 0;
    }
    input.products[0].daily[0].spend_minor = 29_999;
    let below = one(input.clone());
    assert_eq!(below.action, Action::Hold);
    assert_eq!(below.reasons, [Reason::InsufficientOrders]);
    input.products[0].daily[0].spend_minor = 30_000;
    let at_threshold = one(input);
    assert_eq!(at_threshold.action, Action::ReviewReduction);
    assert_eq!(at_threshold.suggested_daily_budget_minor, 80_000);
    assert_eq!(at_threshold.reasons, [Reason::MatureSpendWithoutOrders]);
}

#[test]
fn compares_exact_spend_even_when_rounded_average_cpc_equals_ceiling() {
    let mut input = fixture();
    for day in &mut input.products[0].daily {
        day.spend_minor = 60_000;
    }
    let equal = one(input.clone());
    assert_eq!(equal.action, Action::TestBudgetIncrease);
    input.products[0].daily[0].spend_minor += 1;
    let above = one(input);
    assert_eq!(
        above.metrics.mature_spend_minor / above.metrics.mature_clicks,
        600
    );
    assert_eq!(above.metrics.economic_average_cpc_ceiling_minor, Some(600));
    assert_eq!(above.action, Action::ReviewReduction);
    assert_eq!(above.suggested_daily_budget_minor, 80_000);
    assert_eq!(above.reasons, [Reason::CpcAboveEconomicCeiling]);
}

#[test]
fn portfolio_baselines_share_tight_budget_with_stable_largest_remainders() {
    let mut input = fixture();
    let mut product = input.products.remove(0);
    product.current_daily_budget_minor = 100;
    product.max_daily_budget_minor = 100;
    product.economics = None;
    input.products = (1..=3)
        .rev()
        .map(|sku| {
            let mut row = product.clone();
            row.sku = sku;
            row
        })
        .collect();
    input.policy.total_daily_budget_minor = 101;
    let report = recommend(input).unwrap();
    assert_eq!(report.allocated_daily_budget_minor, 101);
    assert_eq!(report.unallocated_daily_budget_minor, 0);
    let allocations = report
        .recommendations
        .iter()
        .map(|row| {
            assert_eq!(row.action, Action::ReviewReduction);
            assert!(row.reasons.contains(&Reason::MissingEconomics));
            assert!(row.reasons.contains(&Reason::PortfolioBudgetCap));
            (row.sku, row.suggested_daily_budget_minor)
        })
        .collect::<Vec<_>>();
    assert_eq!(allocations, [(1, 34), (2, 34), (3, 33)]);
}

#[test]
fn portfolio_preserves_baselines_and_shares_only_increment_headroom() {
    let mut input = fixture();
    let mut second = input.products[0].clone();
    second.sku = 2;
    second.current_daily_budget_minor = 200_000;
    second.max_daily_budget_minor = 300_000;
    input.products.push(second);
    input.policy.total_daily_budget_minor = 315_001;
    let report = recommend(input).unwrap();
    assert_eq!(report.allocated_daily_budget_minor, 315_001);
    assert_eq!(
        report.recommendations[0].suggested_daily_budget_minor,
        105_000
    );
    assert_eq!(
        report.recommendations[1].suggested_daily_budget_minor,
        210_001
    );
    assert!(report.recommendations.iter().all(|row| {
        row.action == Action::TestBudgetIncrease
            && row.reasons.contains(&Reason::PortfolioBudgetCap)
    }));
}

#[test]
fn portfolio_never_restores_budget_to_a_product_recommended_for_pause() {
    let mut input = fixture();
    let mut second = input.products[0].clone();
    second.sku = 2;
    input.products[0].stock.as_mut().unwrap().sellable_units = 0;
    input.products.push(second);
    input.policy.total_daily_budget_minor = 200_000;
    let report = recommend(input).unwrap();
    assert_eq!(report.recommendations[0].action, Action::ReviewPause);
    assert_eq!(report.recommendations[0].suggested_daily_budget_minor, 0);
    assert_eq!(
        report.recommendations[1].suggested_daily_budget_minor,
        110_000
    );
    assert_eq!(report.unallocated_daily_budget_minor, 90_000);
}

#[test]
fn canonical_report_and_hash_ignore_permutations_but_detect_evidence_changes() {
    let mut input = fixture();
    let mut second = input.products[0].clone();
    second.sku = 2;
    input.products.push(second);
    input.policy.total_daily_budget_minor = 210_001;
    let expected = recommend(input.clone()).unwrap();
    input.source_refs.reverse();
    input.products.reverse();
    for product in &mut input.products {
        product.daily.reverse();
    }
    assert_eq!(recommend(input.clone()).unwrap(), expected);
    input.products[0].daily[0].direct_revenue_minor += 1;
    assert_ne!(
        recommend(input).unwrap().input_sha256,
        expected.input_sha256
    );
}

#[test]
fn parser_rejects_unknown_fields_models_and_duplicate_json_members() {
    let value = serde_json::to_value(fixture()).unwrap();
    let mut root = value.clone();
    root["execute"] = json!(true);
    let mut nested = value.clone();
    nested["products"][0]["economics"]["guessed_cost"] = json!(0);
    let mut unsupported_model = value.clone();
    unsupported_model["pricing_model"] = json!("cpm");
    let mut unsupported_marketplace = value;
    unsupported_marketplace["marketplace"] = json!("wildberries");
    for invalid in [root, nested, unsupported_model, unsupported_marketplace] {
        assert!(matches!(
            parse_input(&serde_json::to_vec(&invalid).unwrap()),
            Err(OptimizerError::InvalidInput)
        ));
    }
    let serialized = serde_json::to_string(&fixture()).unwrap();
    let duplicate = serialized.replacen("\"version\":1", "\"version\":1,\"version\":1", 1);
    assert_ne!(duplicate, serialized);
    assert!(matches!(
        parse_input(duplicate.as_bytes()),
        Err(OptimizerError::InvalidInput)
    ));
}

#[test]
fn rejects_duplicate_products_dates_and_provenance_refs() {
    for duplicate_kind in 0..3 {
        let mut input = fixture();
        match duplicate_kind {
            0 => input.products.push(input.products[0].clone()),
            1 => {
                let duplicate = input.products[0].daily[0].clone();
                input.products[0].daily.push(duplicate);
            }
            _ => input.source_refs.push(input.source_refs[0].clone()),
        }
        assert!(matches!(
            recommend(input),
            Err(OptimizerError::DuplicateEvidence)
        ));
    }
}

#[test]
fn rejects_future_observations_outside_dates_and_unsafe_policy_values() {
    for invalid_kind in 0..7 {
        let mut input = fixture();
        match invalid_kind {
            0 => input.observed_at = input.as_of + Duration::seconds(1),
            1 => {
                input.products[0].stock.as_mut().unwrap().observed_at =
                    input.as_of + Duration::seconds(1);
            }
            2 => {
                input.products[0].economics.as_mut().unwrap().reviewed_at =
                    input.as_of + Duration::seconds(1);
            }
            3 => input.products[0].daily[0].date = date(15),
            4 => input.policy.safety_discount_bps = 10_000,
            5 => input.policy.min_clicks = 0,
            _ => input.policy.total_daily_budget_minor = 0,
        }
        assert!(
            matches!(recommend(input), Err(OptimizerError::InvalidInput)),
            "case {invalid_kind}"
        );
    }
}

#[test]
fn arithmetic_overflow_is_an_error_in_metrics_economics_and_budget_growth() {
    for overflow_kind in 0..4 {
        let mut input = fixture();
        match overflow_kind {
            0 => input.products[0].daily[0].spend_minor = u64::MAX,
            1 => input.products[0].daily[0].direct_orders = u64::MAX,
            2 => {
                input.products[0]
                    .economics
                    .as_mut()
                    .unwrap()
                    .expected_cost_of_goods_minor = u64::MAX;
            }
            _ => {
                input.products[0].current_daily_budget_minor = u64::MAX;
                input.products[0].max_daily_budget_minor = u64::MAX;
                input.policy.total_daily_budget_minor = u64::MAX;
            }
        }
        assert!(
            matches!(recommend(input), Err(OptimizerError::Overflow)),
            "case {overflow_kind}"
        );
    }
}

#[test]
fn bounds_input_bytes_product_count_and_history_length() {
    assert!(matches!(
        parse_input(&vec![b' '; MAX_INPUT_BYTES + 1]),
        Err(OptimizerError::LimitExceeded)
    ));
    let mut products = fixture();
    products.products = vec![products.products[0].clone(); MAX_PRODUCTS + 1];
    assert!(matches!(
        recommend(products),
        Err(OptimizerError::LimitExceeded)
    ));
    let mut window = fixture();
    window.window_start = window.window_end - Duration::days(MAX_WINDOW_DAYS);
    assert!(matches!(
        recommend(window),
        Err(OptimizerError::LimitExceeded)
    ));
}

#[test]
fn does_not_bootstrap_zero_current_budget_and_respects_explicit_zero_product_cap() {
    let mut input = fixture();
    input.products[0].current_daily_budget_minor = 0;
    let no_budget = one(input);
    assert_eq!(no_budget.action, Action::Hold);
    assert_eq!(no_budget.suggested_daily_budget_minor, 0);
    assert_eq!(no_budget.reasons, [Reason::NoCurrentBudget]);
    let mut capped = fixture();
    capped.products[0].max_daily_budget_minor = 0;
    let zero_cap = one(capped);
    assert_eq!(zero_cap.action, Action::ReviewPause);
    assert_eq!(zero_cap.suggested_daily_budget_minor, 0);
    assert!(zero_cap.reasons.contains(&Reason::ProductBudgetCap));
}

#[test]
fn budget_increases_require_fresh_evidence_that_existing_budget_limits_traffic() {
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
        let row = one(input.clone());
        assert_eq!(row.action, Action::Hold);
        assert_eq!(row.suggested_daily_budget_minor, 100_000);
        assert!(row.reasons.contains(&Reason::WithinEconomicCeiling));
        assert!(row.reasons.contains(&reason));
        // A budget bottleneck is not necessary to recommend reducing an
        // expensive campaign whose mature performance already exceeds limits.
        for day in &mut input.products[0].daily {
            day.spend_minor = 100_000;
        }
        let expensive = one(input);
        assert_eq!(expensive.action, Action::ReviewReduction);
        assert!(expensive.reasons.contains(&Reason::CpcAboveEconomicCeiling));
    }
    let mut future = fixture();
    future.products[0]
        .budget_constraint
        .as_mut()
        .unwrap()
        .observed_at = future.as_of + Duration::seconds(1);
    assert!(matches!(
        recommend(future),
        Err(OptimizerError::InvalidInput)
    ));
}

#[test]
fn contradictory_zero_spend_or_revenue_cannot_justify_growth() {
    for missing_spend in [true, false] {
        let mut input = fixture();
        for day in &mut input.products[0].daily {
            if missing_spend {
                day.spend_minor = 0;
            } else {
                day.direct_revenue_minor = 0;
            }
        }
        let row = one(input);
        assert_eq!(row.action, Action::Hold);
        assert_eq!(row.suggested_daily_budget_minor, 100_000);
        assert_eq!(row.reasons, [Reason::InconsistentAdvertisingEvidence]);
        assert_eq!(row.metrics.economic_average_cpc_ceiling_minor, None);
    }
}

#[test]
fn reobserving_an_old_performance_window_does_not_make_its_conversion_current() {
    let mut input = fixture();
    input.policy.max_window_end_age_days = 11;
    assert_eq!(one(input.clone()).action, Action::TestBudgetIncrease);
    input.policy.max_window_end_age_days = 10;
    let row = one(input);
    assert_eq!(row.action, Action::Hold);
    assert_eq!(row.reasons, [Reason::StalePerformanceWindow]);
    assert_eq!(row.metrics.economic_average_cpc_ceiling_minor, None);
}
