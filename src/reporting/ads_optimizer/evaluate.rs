use chrono::Duration;

use super::{
    CpcCeilingBasis, EvidenceMetrics, OptimizationObjective, OptimizerError, ProductEvidence,
    ProductRecommendation, RecommendationAction as Action, RecommendationReason as Reason,
    ShadowInput, scaled,
};

pub(super) fn evaluate(
    input: &ShadowInput,
    product: &ProductEvidence,
) -> Result<ProductRecommendation, OptimizerError> {
    let policy = &input.policy;
    let mut row = ProductRecommendation {
        sku: product.sku,
        action: Action::Hold,
        reasons: Vec::new(),
        current_daily_budget_minor: product.current_daily_budget_minor,
        suggested_daily_budget_minor: product.current_daily_budget_minor,
        metrics: metrics(input, product)?,
    };
    if !input.coverage_complete {
        row.reasons.push(Reason::IncompleteCoverage);
    }
    let expected_days = (input.window_end - input.window_start).num_days() + 1;
    if i64::try_from(product.daily.len()).map_err(|_| OptimizerError::Overflow)? != expected_days {
        row.reasons.push(Reason::MissingDates);
    }
    if input.as_of - input.observed_at > Duration::hours(i64::from(policy.max_data_age_hours))
        || product.daily.iter().any(|day| {
            input.as_of - day.observed_at.unwrap_or(input.observed_at)
                > Duration::hours(i64::from(policy.max_data_age_hours))
        })
    {
        row.reasons.push(Reason::StaleAdvertising);
    }
    if (input.as_of.date_naive() - input.window_end).num_days()
        > i64::from(policy.max_window_end_age_days)
    {
        row.reasons.push(Reason::StalePerformanceWindow);
    }
    match &product.stock {
        None => row.reasons.push(Reason::MissingStock),
        Some(stock)
            if input.as_of - stock.observed_at
                > Duration::hours(i64::from(policy.max_stock_age_hours)) =>
        {
            row.reasons.push(Reason::StaleStock);
        }
        Some(stock) if stock.sellable_units == 0 => {
            row.reasons.push(Reason::OutOfStock);
            row.action = Action::ReviewPause;
            row.suggested_daily_budget_minor = 0;
        }
        Some(_) => {}
    }
    let allowance = match input.objective {
        OptimizationObjective::ExpectedEconomics => economic_allowance(input, product, &mut row)?,
        OptimizationObjective::TargetAdvertisingDrr { .. } => None,
    };
    if row.metrics.mature_days < policy.min_mature_days {
        row.reasons.push(Reason::InsufficientMatureDays);
    }
    if row.metrics.mature_clicks < policy.min_clicks {
        row.reasons.push(Reason::InsufficientClicks);
    }
    // Unknown data cannot be used to justify either performance-based increases
    // or reductions. Fresh zero stock and a valid nonpositive allowance stand
    // on their own and retain their pause recommendation.
    if row.reasons.is_empty() {
        performance(input, product, &mut row, allowance)?;
    }
    if row.suggested_daily_budget_minor > product.max_daily_budget_minor {
        row.suggested_daily_budget_minor = product.max_daily_budget_minor;
        row.reasons.push(Reason::ProductBudgetCap);
        row.action = if product.max_daily_budget_minor == 0 {
            Action::ReviewPause
        } else if product.max_daily_budget_minor < product.current_daily_budget_minor {
            Action::ReviewReduction
        } else if product.max_daily_budget_minor == product.current_daily_budget_minor {
            Action::Hold
        } else {
            Action::TestBudgetIncrease
        };
    }
    Ok(row)
}

fn economic_allowance(
    input: &ShadowInput,
    product: &ProductEvidence,
    row: &mut ProductRecommendation,
) -> Result<Option<u64>, OptimizerError> {
    match &product.economics {
        None => {
            row.reasons.push(Reason::MissingEconomics);
            Ok(None)
        }
        Some(economics)
            if economics.valid_from > input.window_start
                || economics.valid_to < input.as_of.date_naive() =>
        {
            row.reasons.push(Reason::EconomicsOutsidePeriod);
            Ok(None)
        }
        Some(economics) => {
            let costs = [
                economics.expected_cost_of_goods_minor,
                economics.expected_other_costs_minor,
                economics.return_reserve_minor,
                economics.target_profit_minor,
            ]
            .into_iter()
            .try_fold(0u64, |sum, amount| {
                sum.checked_add(amount).ok_or(OptimizerError::Overflow)
            })?;
            let allowance = economics.expected_revenue_minor.saturating_sub(costs);
            row.metrics.advertising_allowance_per_order_minor = Some(allowance);
            if allowance == 0 {
                row.reasons.push(Reason::NoAdvertisingAllowance);
                row.action = Action::ReviewPause;
                row.suggested_daily_budget_minor = 0;
            }
            Ok(Some(allowance))
        }
    }
}

fn performance(
    input: &ShadowInput,
    product: &ProductEvidence,
    row: &mut ProductRecommendation,
    allowance: Option<u64>,
) -> Result<(), OptimizerError> {
    let policy = &input.policy;
    if (row.metrics.mature_clicks > 0 && row.metrics.mature_spend_minor == 0)
        || (row.metrics.mature_orders > 0 && row.metrics.mature_direct_revenue_minor == 0)
    {
        row.reasons.push(Reason::InconsistentAdvertisingEvidence);
        return Ok(());
    }
    if row.metrics.mature_orders == 0 {
        if let OptimizationObjective::TargetAdvertisingDrr { .. } = input.objective {
            // Zero attributed orders cannot establish an allowance per order.
            // Do not invent economics or a generic spend threshold.
            row.reasons.push(Reason::NoMatureOrdersForDrrAllowance);
            return Ok(());
        }
        if let Some(allowance) = allowance
            && u128::from(row.metrics.mature_spend_minor)
                >= u128::from(allowance) * u128::from(policy.zero_order_spend_allowances)
        {
            row.reasons.push(Reason::MatureSpendWithoutOrders);
            return reduce(input, row);
        }
    }
    if row.metrics.mature_orders < policy.min_orders {
        row.reasons.push(Reason::InsufficientOrders);
        return Ok(());
    }
    let (above_ceiling, above_reason, within_reason) = match input.objective {
        OptimizationObjective::ExpectedEconomics => {
            let allowance = allowance.ok_or(OptimizerError::InvalidInput)?;
            let numerator = u128::from(allowance)
                .checked_mul(u128::from(row.metrics.mature_orders))
                .and_then(|value| {
                    value.checked_mul(10_000 - u128::from(policy.safety_discount_bps))
                })
                .ok_or(OptimizerError::Overflow)?;
            let denominator = u128::from(row.metrics.mature_clicks) * 10_000;
            let ceiling =
                u64::try_from(numerator / denominator).map_err(|_| OptimizerError::Overflow)?;
            row.metrics.average_cpc_ceiling_minor = Some(ceiling);
            row.metrics.economic_average_cpc_ceiling_minor = Some(ceiling);
            row.metrics.cpc_ceiling_basis = Some(CpcCeilingBasis::ExpectedEconomics);
            (
                u128::from(row.metrics.mature_spend_minor)
                    > u128::from(ceiling) * u128::from(row.metrics.mature_clicks),
                Reason::CpcAboveEconomicCeiling,
                Reason::WithinEconomicCeiling,
            )
        }
        OptimizationObjective::TargetAdvertisingDrr { max_drr_bps } => {
            let target_spend_numerator = u128::from(row.metrics.mature_direct_revenue_minor)
                .checked_mul(u128::from(max_drr_bps))
                .ok_or(OptimizerError::Overflow)?;
            let allowance =
                target_spend_numerator / (u128::from(row.metrics.mature_orders) * 10_000);
            row.metrics.advertising_allowance_per_order_minor =
                Some(u64::try_from(allowance).map_err(|_| OptimizerError::Overflow)?);
            // Retain full precision through the final CPC division. In
            // particular, do not multiply a floored per-order allowance back
            // by the number of orders.
            let numerator = target_spend_numerator
                .checked_mul(10_000 - u128::from(policy.safety_discount_bps))
                .ok_or(OptimizerError::Overflow)?;
            let denominator = u128::from(row.metrics.mature_clicks) * 100_000_000;
            let ceiling =
                u64::try_from(numerator / denominator).map_err(|_| OptimizerError::Overflow)?;
            row.metrics.average_cpc_ceiling_minor = Some(ceiling);
            row.metrics.cpc_ceiling_basis = Some(CpcCeilingBasis::TargetAdvertisingDrr);
            // Decide on the exact total allowance; rounding a displayed CPC
            // must not turn an acceptable DRR into a reduction.
            (
                u128::from(row.metrics.mature_spend_minor) * 100_000_000 > numerator,
                Reason::CpcAboveAdvertisingDrrCeiling,
                Reason::WithinAdvertisingDrrCeiling,
            )
        }
    };
    if above_ceiling {
        row.reasons.push(above_reason);
        reduce(input, row)?;
    } else if row.current_daily_budget_minor == 0 {
        row.reasons.push(Reason::NoCurrentBudget);
    } else if let Some(reason) = budget_blocker(input, product) {
        row.reasons.push(within_reason);
        row.reasons.push(reason);
    } else {
        row.reasons.push(within_reason);
        let increment = scaled(
            row.current_daily_budget_minor,
            u64::from(policy.max_budget_increase_bps),
            10_000,
        )?;
        row.suggested_daily_budget_minor = row
            .current_daily_budget_minor
            .checked_add(increment)
            .ok_or(OptimizerError::Overflow)?;
        if increment > 0 {
            row.action = Action::TestBudgetIncrease;
        }
    }
    Ok(())
}

fn budget_blocker(input: &ShadowInput, product: &ProductEvidence) -> Option<Reason> {
    match &product.budget_constraint {
        None => Some(Reason::MissingBudgetConstraintEvidence),
        Some(evidence)
            if input.as_of - evidence.observed_at
                > Duration::hours(i64::from(input.policy.max_data_age_hours)) =>
        {
            Some(Reason::StaleBudgetConstraintEvidence)
        }
        Some(evidence) if !evidence.limited => Some(Reason::BudgetNotLimited),
        Some(_) => None,
    }
}

fn reduce(input: &ShadowInput, row: &mut ProductRecommendation) -> Result<(), OptimizerError> {
    row.suggested_daily_budget_minor = scaled(
        row.current_daily_budget_minor,
        10_000 - u64::from(input.policy.budget_decrease_bps),
        10_000,
    )?;
    row.action = Action::ReviewReduction;
    Ok(())
}

fn metrics(
    input: &ShadowInput,
    product: &ProductEvidence,
) -> Result<EvidenceMetrics, OptimizerError> {
    let lag = Duration::days(i64::from(input.policy.attribution_lag_days) + 1);
    let mut metrics = EvidenceMetrics::default();
    for day in &product.daily {
        let mature_through = day
            .observed_at
            .unwrap_or(input.observed_at)
            .date_naive()
            .checked_sub_signed(lag)
            .ok_or(OptimizerError::InvalidInput)?;
        add(&mut metrics.all_period_spend_minor, day.spend_minor)?;
        if day.date > mature_through {
            metrics.excluded_recent_days += 1;
            continue;
        }
        metrics.mature_days += 1;
        add(&mut metrics.mature_clicks, day.clicks)?;
        add(&mut metrics.mature_orders, day.direct_orders)?;
        add(&mut metrics.mature_spend_minor, day.spend_minor)?;
        add(
            &mut metrics.mature_direct_revenue_minor,
            day.direct_revenue_minor,
        )?;
    }
    Ok(metrics)
}

fn add(total: &mut u64, value: u64) -> Result<(), OptimizerError> {
    *total = total.checked_add(value).ok_or(OptimizerError::Overflow)?;
    Ok(())
}
