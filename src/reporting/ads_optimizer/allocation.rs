use super::{
    OptimizerError, OptimizerPolicy, ProductRecommendation, RecommendationAction as Action,
    RecommendationReason as Reason,
};

/// Retain existing eligible baselines first, then share available headroom
/// proportionally to bounded test increments. No causal profit forecast is
/// implied by this deterministic allocation.
pub(super) fn allocate(
    policy: &OptimizerPolicy,
    rows: &mut [ProductRecommendation],
) -> Result<(), OptimizerError> {
    let bases = rows
        .iter()
        .map(|row| {
            row.current_daily_budget_minor
                .min(row.suggested_daily_budget_minor)
        })
        .collect::<Vec<_>>();
    let base_total: u128 = bases.iter().map(|value| u128::from(*value)).sum();
    let assigned = if base_total > u128::from(policy.total_daily_budget_minor) {
        proportional(&bases, policy.total_daily_budget_minor)?
    } else {
        let available = policy.total_daily_budget_minor
            - u64::try_from(base_total).map_err(|_| OptimizerError::Overflow)?;
        let increments = rows
            .iter()
            .zip(&bases)
            .map(|(row, base)| row.suggested_daily_budget_minor - base)
            .collect::<Vec<_>>();
        let desired: u128 = increments.iter().map(|value| u128::from(*value)).sum();
        let additions = if desired > u128::from(available) {
            proportional(&increments, available)?
        } else {
            increments
        };
        bases
            .into_iter()
            .zip(additions)
            .map(|(base, extra)| base.checked_add(extra).ok_or(OptimizerError::Overflow))
            .collect::<Result<Vec<_>, _>>()?
    };
    for (row, amount) in rows.iter_mut().zip(assigned) {
        if amount < row.suggested_daily_budget_minor {
            row.reasons.push(Reason::PortfolioBudgetCap);
            row.suggested_daily_budget_minor = amount;
            row.action = match amount.cmp(&row.current_daily_budget_minor) {
                std::cmp::Ordering::Less => Action::ReviewReduction,
                std::cmp::Ordering::Equal => Action::Hold,
                std::cmp::Ordering::Greater => Action::TestBudgetIncrease,
            };
        }
    }
    Ok(())
}

/// Largest-remainder allocation; input rows are sorted by SKU, so index is a
/// stable tie-breaker. Never allocates more than an individual requested weight.
fn proportional(weights: &[u64], budget: u64) -> Result<Vec<u64>, OptimizerError> {
    let total: u128 = weights.iter().map(|value| u128::from(*value)).sum();
    if total == 0 {
        return Ok(vec![0; weights.len()]);
    }
    let mut amounts = Vec::with_capacity(weights.len());
    let mut remainders = Vec::with_capacity(weights.len());
    let mut assigned = 0u64;
    for (index, weight) in weights.iter().enumerate() {
        let numerator = u128::from(*weight) * u128::from(budget);
        let amount = u64::try_from(numerator / total).map_err(|_| OptimizerError::Overflow)?;
        assigned = assigned
            .checked_add(amount)
            .ok_or(OptimizerError::Overflow)?;
        amounts.push(amount);
        remainders.push((index, numerator % total));
    }
    remainders.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let remaining = usize::try_from(budget - assigned).map_err(|_| OptimizerError::Overflow)?;
    for (index, _) in remainders.into_iter().take(remaining) {
        amounts[index] = amounts[index]
            .checked_add(1)
            .ok_or(OptimizerError::Overflow)?;
    }
    Ok(amounts)
}
