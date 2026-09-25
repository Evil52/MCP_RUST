use std::collections::BTreeSet;

use chrono::Datelike;

use super::{MAX_PRODUCTS, MAX_WINDOW_DAYS, OptimizerError, ShadowInput};

pub(super) fn validate(input: &ShadowInput) -> Result<(), OptimizerError> {
    let policy = &input.policy;
    let days = (input.window_end - input.window_start).num_days() + 1;
    if input.version != 1
        || input.account_id.is_empty()
        || input.account_id.len() > 128
        || !input
            .account_id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
        || !(2000..=2200).contains(&input.as_of.year())
        || !(2000..=2200).contains(&input.window_start.year())
        || days <= 0
        || input.window_end >= input.observed_at.date_naive()
        || input.observed_at > input.as_of
        || input.source_refs.is_empty()
        || input.source_refs.len() > 256
        || input.source_refs.iter().any(|value| !valid_ref(value))
        || policy.total_daily_budget_minor == 0
        || !(1..=60).contains(&policy.attribution_lag_days)
        || !(1..=90).contains(&policy.min_mature_days)
        || policy.min_clicks == 0
        || policy.min_orders == 0
        || !(1..=168).contains(&policy.max_data_age_hours)
        || !(1..=90).contains(&policy.max_window_end_age_days)
        || !(1..=168).contains(&policy.max_stock_age_hours)
        || policy.safety_discount_bps >= 10_000
        || policy.max_budget_increase_bps > 10_000
        || !(1..=10_000).contains(&policy.budget_decrease_bps)
        || !(1..=100).contains(&policy.zero_order_spend_allowances)
    {
        return Err(OptimizerError::InvalidInput);
    }
    if input.products.is_empty() || input.products.len() > MAX_PRODUCTS || days > MAX_WINDOW_DAYS {
        return Err(OptimizerError::LimitExceeded);
    }
    let mut refs = BTreeSet::new();
    if input.source_refs.iter().any(|value| !refs.insert(value)) {
        return Err(OptimizerError::DuplicateEvidence);
    }
    let mut skus = BTreeSet::new();
    for product in &input.products {
        if product.sku == 0 || i64::try_from(product.sku).is_err() {
            return Err(OptimizerError::InvalidInput);
        }
        if !skus.insert(product.sku) {
            return Err(OptimizerError::DuplicateEvidence);
        }
        if product.daily.len() > usize::try_from(MAX_WINDOW_DAYS).expect("positive constant") {
            return Err(OptimizerError::LimitExceeded);
        }
        let mut dates = BTreeSet::new();
        for day in &product.daily {
            if day.date < input.window_start || day.date > input.window_end {
                return Err(OptimizerError::InvalidInput);
            }
            if !dates.insert(day.date) {
                return Err(OptimizerError::DuplicateEvidence);
            }
        }
        if product
            .budget_constraint
            .as_ref()
            .is_some_and(|evidence| evidence.observed_at > input.as_of)
        {
            return Err(OptimizerError::InvalidInput);
        }
        if product
            .stock
            .as_ref()
            .is_some_and(|stock| stock.observed_at > input.as_of)
        {
            return Err(OptimizerError::InvalidInput);
        }
        if let Some(economics) = &product.economics
            && (!valid_ref(&economics.source_ref)
                || economics.valid_from > economics.valid_to
                || economics.reviewed_at > input.as_of
                || economics.expected_revenue_minor == 0)
        {
            return Err(OptimizerError::InvalidInput);
        }
    }
    Ok(())
}

fn valid_ref(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}
