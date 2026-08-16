use std::{cmp::Reverse, collections::BTreeSet};

const MAX_RULE_INPUTS: usize = 10_000;
const MAX_ACTIONS: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Yellow,
    Red,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProblemKind {
    AdvertisedWithoutStock,
    Stockout,
    LowStockCover,
    SpendWithoutOrders,
    HighDrr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleInput {
    pub account_id: String,
    pub sku: u64,
    pub sellable_stock: u64,
    pub sold_units: u64,
    pub sales_window_days: u8,
    pub sales_gmv_minor: u64,
    pub lead_time_days: Option<u16>,
    pub ad_clicks: u64,
    pub ad_spend_minor: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
    pub target_cpo_minor: Option<u64>,
    pub target_drr_bps: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriorityProblem {
    pub account_id: String,
    pub sku: u64,
    pub kind: ProblemKind,
    pub severity: Severity,
    pub observed: u64,
    pub threshold: u64,
    pub impact_minor: u64,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum RuleError {
    #[error("daily report rule input is invalid")]
    InvalidInput,
    #[error("daily report rule input contains a duplicate account/SKU pair")]
    DuplicateSku,
}

pub fn priority_problems(
    inputs: &[RuleInput],
    recommendations_allowed: bool,
    store_min_ad_spend_minor: u64,
) -> Result<Vec<PriorityProblem>, RuleError> {
    if inputs.len() > MAX_RULE_INPUTS {
        return Err(RuleError::InvalidInput);
    }
    if !recommendations_allowed {
        return Ok(Vec::new());
    }
    let mut seen = BTreeSet::new();
    let mut problems = Vec::new();
    for input in inputs {
        validate(input)?;
        if !seen.insert((input.account_id.as_str(), input.sku)) {
            return Err(RuleError::DuplicateSku);
        }
        stock_problem(input)
            .into_iter()
            .for_each(|item| problems.push(item));
        if let Some(item) = spend_without_orders(input, store_min_ad_spend_minor) {
            problems.push(item);
        }
        if let Some(item) = high_drr(input) {
            problems.push(item);
        }
    }
    problems.sort_by_key(|problem| {
        (
            Reverse(problem.severity),
            Reverse(problem.impact_minor),
            problem.account_id.clone(),
            problem.sku,
            problem.kind,
        )
    });
    problems.truncate(MAX_ACTIONS);
    Ok(problems)
}

fn validate(input: &RuleInput) -> Result<(), RuleError> {
    if !valid_account_id(&input.account_id)
        || input.sku == 0
        || !(1..=31).contains(&input.sales_window_days)
        || input.attributed_orders > input.ad_clicks
        || input.lead_time_days == Some(0)
        || input.target_cpo_minor == Some(0)
        || input.target_drr_bps == Some(0)
    {
        Err(RuleError::InvalidInput)
    } else {
        Ok(())
    }
}

fn valid_account_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn stock_problem(input: &RuleInput) -> Option<PriorityProblem> {
    if input.sellable_stock == 0 && input.sold_units >= 3 {
        let kind = if input.ad_spend_minor > 0 {
            ProblemKind::AdvertisedWithoutStock
        } else {
            ProblemKind::Stockout
        };
        return Some(PriorityProblem {
            account_id: input.account_id.clone(),
            sku: input.sku,
            kind,
            severity: Severity::Red,
            observed: 0,
            threshold: 1,
            impact_minor: input.sales_gmv_minor.max(input.ad_spend_minor),
        });
    }
    if input.sellable_stock == 0 || input.sold_units == 0 {
        return None;
    }
    let lead_time = u128::from(input.lead_time_days.unwrap_or(3));
    let cover_left = u128::from(input.sellable_stock) * u128::from(input.sales_window_days);
    let red_limit = u128::from(input.sold_units) * lead_time;
    let yellow_limit = u128::from(input.sold_units) * (lead_time + 3);
    let severity = if cover_left <= red_limit {
        Severity::Red
    } else if cover_left <= yellow_limit {
        Severity::Yellow
    } else {
        return None;
    };
    Some(PriorityProblem {
        account_id: input.account_id.clone(),
        sku: input.sku,
        kind: ProblemKind::LowStockCover,
        severity,
        observed: ((cover_left * 10) / u128::from(input.sold_units)) as u64,
        threshold: u64::from(input.lead_time_days.unwrap_or(3)) * 10,
        impact_minor: input.sales_gmv_minor,
    })
}

fn spend_without_orders(input: &RuleInput, minimum: u64) -> Option<PriorityProblem> {
    if input.attributed_orders != 0 {
        return None;
    }
    let red_spend = input
        .target_cpo_minor
        .and_then(|target| target.checked_mul(2))
        .unwrap_or(u64::MAX)
        .max(minimum);
    let severity = if input.ad_clicks >= 20 && input.ad_spend_minor >= red_spend {
        Severity::Red
    } else if input.ad_clicks >= 10 && input.ad_spend_minor >= minimum {
        Severity::Yellow
    } else {
        return None;
    };
    Some(PriorityProblem {
        account_id: input.account_id.clone(),
        sku: input.sku,
        kind: ProblemKind::SpendWithoutOrders,
        severity,
        observed: input.ad_spend_minor,
        threshold: if severity == Severity::Red {
            red_spend
        } else {
            minimum
        },
        impact_minor: input.ad_spend_minor,
    })
}

fn high_drr(input: &RuleInput) -> Option<PriorityProblem> {
    let target = input.target_drr_bps?;
    if input.attributed_revenue_minor == 0 {
        return None;
    }
    let current = (u128::from(input.ad_spend_minor) * 10_000
        / u128::from(input.attributed_revenue_minor))
    .min(u128::from(u64::MAX)) as u64;
    let severity = if u128::from(current) * 2 > u128::from(target) * 3
        && current >= target.saturating_add(500)
    {
        Severity::Red
    } else if u128::from(current) * 5 > u128::from(target) * 6
        && current >= target.saturating_add(300)
    {
        Severity::Yellow
    } else {
        return None;
    };
    Some(PriorityProblem {
        account_id: input.account_id.clone(),
        sku: input.sku,
        kind: ProblemKind::HighDrr,
        severity,
        observed: current,
        threshold: target,
        impact_minor: input.ad_spend_minor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(sku: u64) -> RuleInput {
        RuleInput {
            account_id: "ozon_store".to_owned(),
            sku,
            sellable_stock: 100,
            sold_units: 10,
            sales_window_days: 14,
            sales_gmv_minor: 100_000,
            lead_time_days: Some(3),
            ad_clicks: 0,
            ad_spend_minor: 0,
            attributed_orders: 0,
            attributed_revenue_minor: 0,
            target_cpo_minor: Some(1_000),
            target_drr_bps: Some(1_000),
        }
    }

    #[test]
    fn rules_prioritize_at_most_five_actions_and_keep_numeric_evidence() {
        let mut values = (1..=7).map(input).collect::<Vec<_>>();
        for (index, value) in values.iter_mut().enumerate() {
            value.sellable_stock = 0;
            value.sold_units = 3;
            value.sales_gmv_minor = 0;
            value.ad_spend_minor = (index as u64 + 1) * 1_000;
        }
        let problems = priority_problems(&values, true, 500).unwrap();
        assert_eq!(problems.len(), 5);
        assert_eq!(problems[0].sku, 7);
        assert_eq!(problems[0].account_id, "ozon_store");
        assert_eq!(problems[0].kind, ProblemKind::AdvertisedWithoutStock);
        assert_eq!(problems[0].severity, Severity::Red);
        assert_eq!(problems[0].observed, 0);
        assert_eq!(problems[0].threshold, 1);
    }

    #[test]
    fn stock_advertising_and_drr_boundaries_are_exact() {
        let mut stock = input(1);
        stock.sellable_stock = 3;
        stock.sold_units = 14;
        let mut ads = input(2);
        ads.ad_clicks = 20;
        ads.ad_spend_minor = 2_000;
        let mut drr = input(3);
        drr.ad_spend_minor = 1_600;
        drr.attributed_orders = 1;
        drr.ad_clicks = 1;
        drr.attributed_revenue_minor = 10_000;
        let problems = priority_problems(&[stock, ads, drr], true, 500).unwrap();
        assert!(
            problems
                .iter()
                .any(|p| p.kind == ProblemKind::LowStockCover && p.severity == Severity::Red)
        );
        assert!(
            problems
                .iter()
                .any(|p| p.kind == ProblemKind::SpendWithoutOrders && p.severity == Severity::Red)
        );
        assert!(
            problems
                .iter()
                .any(|p| p.kind == ProblemKind::HighDrr && p.severity == Severity::Red)
        );

        let mut yellow = input(4);
        yellow.sellable_stock = 5;
        yellow.sold_units = 14;
        yellow.ad_clicks = 10;
        yellow.ad_spend_minor = 500;
        yellow.attributed_revenue_minor = 5_000;
        let yellow_problems = priority_problems(&[yellow], true, 500).unwrap();
        assert!(
            yellow_problems
                .iter()
                .any(|p| p.severity == Severity::Yellow)
        );

        let mut plain_stockout = input(5);
        plain_stockout.sellable_stock = 0;
        plain_stockout.sold_units = 3;
        let mut yellow_drr = input(6);
        yellow_drr.ad_clicks = 1;
        yellow_drr.ad_spend_minor = 1_301;
        yellow_drr.attributed_orders = 1;
        yellow_drr.attributed_revenue_minor = 10_000;
        let boundary_problems =
            priority_problems(&[plain_stockout, yellow_drr], true, 500).unwrap();
        assert!(boundary_problems.iter().any(|problem| {
            problem.kind == ProblemKind::Stockout && problem.severity == Severity::Red
        }));
        assert!(boundary_problems.iter().any(|problem| {
            problem.kind == ProblemKind::HighDrr && problem.severity == Severity::Yellow
        }));
    }

    #[test]
    fn incomplete_data_and_invalid_or_duplicate_inputs_fail_closed() {
        assert!(priority_problems(&[input(1)], false, 1).unwrap().is_empty());
        assert_eq!(
            priority_problems(&[input(1), input(1)], true, 1),
            Err(RuleError::DuplicateSku)
        );
        let same_sku_different_account = RuleInput {
            account_id: "second_store".to_owned(),
            ..input(1)
        };
        assert_eq!(
            priority_problems(&[input(1), same_sku_different_account], true, 1)
                .unwrap()
                .len(),
            0
        );
        for invalid in [
            RuleInput {
                account_id: "bad account".to_owned(),
                ..input(1)
            },
            RuleInput { sku: 0, ..input(1) },
            RuleInput {
                sales_window_days: 0,
                ..input(1)
            },
            RuleInput {
                ad_clicks: 1,
                attributed_orders: 2,
                ..input(1)
            },
            RuleInput {
                lead_time_days: Some(0),
                ..input(1)
            },
            RuleInput {
                target_cpo_minor: Some(0),
                ..input(1)
            },
            RuleInput {
                target_drr_bps: Some(0),
                ..input(1)
            },
        ] {
            assert_eq!(
                priority_problems(&[invalid], true, 1),
                Err(RuleError::InvalidInput)
            );
        }
        assert_eq!(
            priority_problems(&vec![input(1); MAX_RULE_INPUTS + 1], true, 1),
            Err(RuleError::InvalidInput)
        );
    }

    #[test]
    fn quiet_products_do_not_generate_actions() {
        let mut quiet = input(1);
        quiet.sold_units = 0;
        quiet.sellable_stock = 0;
        quiet.target_cpo_minor = None;
        quiet.target_drr_bps = None;
        assert!(priority_problems(&[quiet], true, 500).unwrap().is_empty());
    }
}
