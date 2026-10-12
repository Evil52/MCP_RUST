//! Daily, read-only WB proposals from freshly reread history and pinned snapshots.
//! CPC search proposals are separate from the offline Ozon budget allocator.
mod context;
mod model;
pub use model::*;

use std::collections::BTreeSet;

use chrono::{Datelike as _, Duration, NaiveDate};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use super::{MAX_INPUT_BYTES, OptimizerError};
use crate::reporting::advertising_history::normalize_statistics;
use context::{campaigns, fresh, number, snapshot_totals};

pub fn recommend_export(bytes: &[u8]) -> Result<RecommendationReport, OptimizerError> {
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(OptimizerError::LimitExceeded);
    }
    recommend(serde_json::from_slice(bytes).map_err(|_| OptimizerError::InvalidInput)?)
}

pub fn recommend(input: RecommendationEvidence) -> Result<RecommendationReport, OptimizerError> {
    validate(&input)?;
    let bytes = serde_json::to_vec(&input).map_err(|_| OptimizerError::InvalidInput)?;
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(OptimizerError::LimitExceeded);
    }
    let days = normalize_statistics(
        &input.statistics,
        &input.campaign_ids,
        input.date_from,
        input.date_to,
    )
    .map_err(|_| OptimizerError::InvalidInput)?;
    let rows = days.as_array().ok_or(OptimizerError::InvalidInput)?;
    let stocks = snapshot_totals(&input, "stocks")?;
    let sales = snapshot_totals(&input, "sales")?;
    let mut recommendations = Vec::new();
    for campaign in campaigns(&input)? {
        let campaign_days: Vec<_> = rows
            .iter()
            .filter(|row| row["campaign_id"].as_u64() == Some(campaign.id))
            .collect();
        let campaign_windows = [7, 14, 30]
            .into_iter()
            .filter(|&n| i64::from(n) <= (input.date_to - input.date_from).num_days() + 1)
            .map(|n| window(&input, &campaign_days, None, n))
            .collect::<Result<Vec<_>, _>>()?;
        let decision_metrics = &campaign_windows
            .iter()
            .find(|window| window.days == 14)
            .or_else(|| campaign_windows.last())
            .ok_or(OptimizerError::InvalidInput)?
            .metrics;
        let mut skus: BTreeSet<_> = campaign.bids.keys().copied().collect();
        for day in &campaign_days {
            for product in day["products"]
                .as_array()
                .ok_or(OptimizerError::InvalidInput)?
            {
                skus.insert(number(product, "sku")?);
            }
        }
        for sku in skus {
            let windows = [7, 14, 30]
                .into_iter()
                .filter(|&n| i64::from(n) <= (input.date_to - input.date_from).num_days() + 1)
                .map(|n| window(&input, &campaign_days, Some(sku), n))
                .collect::<Result<Vec<_>, _>>()?;
            let selected = windows
                .iter()
                .find(|w| w.days == 14)
                .or_else(|| windows.last())
                .ok_or(OptimizerError::InvalidInput)?;
            let fbw_units = stocks.get(&sku).copied();
            let mut reasons = Vec::new();
            if selected.metrics.missing_days > 0 {
                reasons.push(Reason::MissingHistory);
            }
            if selected.metrics.unallocated_days > 0 {
                reasons.push(Reason::UnallocatedSkuHistory);
            }
            if !fresh(input.statistics_observed_at, input.as_of, 60) {
                reasons.push(Reason::StaleHistory);
            }
            if !fresh(input.composition_observed_at, input.as_of, 30) {
                reasons.push(Reason::StaleComposition);
            }
            if !campaign.active {
                reasons.push(Reason::NotActive);
            }
            if !campaign.bids.contains_key(&sku) {
                reasons.push(Reason::OutsideCurrentComposition);
            }
            if !campaign.supported
                || campaign
                    .bids
                    .get(&sku)
                    .copied()
                    .flatten()
                    .is_none_or(|bid| bid == 0)
            {
                reasons.push(Reason::UnsupportedPlacementOrPricing);
            }
            match fbw_units {
                None => reasons.push(Reason::UnknownOrStaleFbwStock),
                Some(0) => reasons.push(Reason::FbwZeroCheckFbs),
                Some(_) => {}
            }
            if input.target_drr_bps.is_none() {
                reasons.push(Reason::TargetDrrRequired);
            }
            if campaign.bids.len() != 1
                || selected.metrics.spend_minor != decision_metrics.spend_minor
                || selected.metrics.orders != decision_metrics.orders
                || selected.metrics.revenue_minor != decision_metrics.revenue_minor
                || selected.metrics.clicks != decision_metrics.clicks
                || selected.metrics.observed_days != decision_metrics.observed_days
            {
                reasons.push(Reason::CampaignLevelAttribution);
            }
            let core_blocked = reasons.iter().any(|reason| {
                matches!(
                    reason,
                    Reason::MissingHistory
                        | Reason::UnallocatedSkuHistory
                        | Reason::StaleHistory
                        | Reason::StaleComposition
                        | Reason::NotActive
                        | Reason::OutsideCurrentComposition
                )
            });
            let bid_context_blocked = reasons.iter().any(|reason| {
                matches!(
                    reason,
                    Reason::UnsupportedPlacementOrPricing
                        | Reason::CampaignLevelAttribution
                        | Reason::UnknownOrStaleFbwStock
                        | Reason::FbwZeroCheckFbs
                )
            });
            let (mut action, mut step) = if core_blocked {
                (Action::Hold, None)
            } else {
                decide(
                    decision_metrics,
                    input.target_drr_bps.unwrap_or(0),
                    &mut reasons,
                )
            };
            // Aggregate inefficiency can justify review, never a mixed-placement search bid.
            if bid_context_blocked {
                step = None;
                if action == Action::TestIncrease {
                    action = Action::Hold;
                }
            }
            if action == Action::TestIncrease {
                let daily_sales = context::timestamp(&input.sales["period_start"])
                    .zip(context::timestamp(&input.sales["period_end"]))
                    .filter(|(start, end)| {
                        *end - *start == Duration::days(1)
                            && *end <= input.as_of
                            && input.as_of - *end <= Duration::hours(36)
                    })
                    .and_then(|_| sales.get(&sku).copied());
                let blocked = daily_sales.map_or(Some(Reason::UnknownSalesForIncrease), |units| {
                    (u128::from(fbw_units.unwrap_or(0)) < u128::from(units) * 3)
                        .then_some(Reason::LowFbwBuffer)
                });
                if let Some(reason) = blocked {
                    reasons.push(reason);
                    action = Action::Hold;
                    step = None;
                }
            }
            recommendations.push(ProductRecommendation {
                campaign_id: campaign.id,
                campaign_name: campaign.name.to_owned(),
                sku,
                current_search_bid_minor: campaign.bids.get(&sku).copied().flatten(),
                suggested_step_bps: step,
                action,
                explanations: reasons.iter().map(|reason| reason.explanation()).collect(),
                reasons,
                fbw_units,
                recent_store_ordered_units: sales.get(&sku).copied(),
                windows,
                campaign_windows: campaign_windows.clone(),
            });
        }
    }
    if recommendations.len() > 10_000 {
        return Err(OptimizerError::LimitExceeded);
    }
    recommendations.sort_by_key(|row| {
        (
            match row.action {
                Action::ReviewDecrease => 0,
                Action::TestIncrease => 1,
                Action::Hold => 2,
            },
            std::cmp::Reverse(
                row.windows
                    .iter()
                    .find(|window| window.days == 14)
                    .or_else(|| row.windows.last())
                    .map_or(0, |window| window.metrics.spend_minor),
            ),
            row.campaign_id,
            row.sku,
        )
    });
    Ok(RecommendationReport {
        version: 1,
        mode: "review_only",
        account_id: input.account_id,
        as_of: input.as_of,
        calendar: "Europe/Moscow",
        currency: "RUB",
        revenue_basis: "attributed_orders_including_associated_conversions",
        decision_basis: "campaign_totals",
        target_drr_bps: input.target_drr_bps,
        attribution_lag_days: 3,
        attribution_finality_verified: false,
        auto_apply_allowed: false,
        causal_effect_verified: false,
        source_sha256: Sha256::digest(bytes).iter().fold(
            String::with_capacity(64),
            |mut hash, byte| {
                use std::fmt::Write as _;
                write!(hash, "{byte:02x}").expect("writing to String cannot fail");
                hash
            },
        ),
        data_classification: "untrusted_external_marketplace_data",
        total_recommendations: recommendations.len(),
        truncated: false,
        stock_snapshot_id: input.stocks["snapshot_id"].as_str().map(str::to_owned),
        sales_snapshot_id: input.sales["snapshot_id"].as_str().map(str::to_owned),
        sales_period_start: input.sales["period_start"].as_str().map(str::to_owned),
        sales_period_end: input.sales["period_end"].as_str().map(str::to_owned),
        recommendations,
    })
}

fn validate(input: &RecommendationEvidence) -> Result<(), OptimizerError> {
    if !(2010..=2200).contains(&input.as_of.year()) {
        return Err(OptimizerError::InvalidInput);
    }
    let ids: BTreeSet<_> = input.campaign_ids.iter().collect();
    let closed = (input.as_of + Duration::hours(3)).date_naive() - Duration::days(1);
    if input.version != 1
        || !crate::identifiers::is_account_id(&input.account_id)
        || input.campaign_ids.is_empty()
        || input.campaign_ids.len() > 10
        || ids.len() != input.campaign_ids.len()
        || input.campaign_ids.contains(&0)
        || input.campaign_ids.iter().any(|id| *id > i64::MAX as u64)
        || ![7, 14, 30].contains(&((input.date_to - input.date_from).num_days() + 1))
        || input.date_to != closed
        || input.date_from
            < NaiveDate::from_ymd_opt(2010, 1, 1).ok_or(OptimizerError::InvalidInput)?
        || input.statistics_observed_at > input.as_of
        || input.composition_observed_at > input.as_of
        || input
            .target_drr_bps
            .is_some_and(|target| target == 0 || target > 10_000)
    {
        return Err(OptimizerError::InvalidInput);
    }
    Ok(())
}

fn window(
    input: &RecommendationEvidence,
    days: &[&Value],
    sku: Option<u64>,
    n: u32,
) -> Result<Window, OptimizerError> {
    let from = input.date_to - Duration::days(i64::from(n) - 1);
    let mut metrics = Metrics::default();
    for day in days {
        let date = day["business_date"]
            .as_str()
            .ok_or(OptimizerError::InvalidInput)?
            .parse::<NaiveDate>()
            .map_err(|_| OptimizerError::InvalidInput)?;
        if date < from {
            continue;
        }
        // Use the actual time of rereading WB, not the time an old file is replayed.
        let mature_at = (date + Duration::days(4))
            .and_hms_opt(0, 0, 0)
            .ok_or(OptimizerError::InvalidInput)?
            .and_utc()
            - Duration::hours(3);
        if input.statistics_observed_at < mature_at {
            metrics.excluded_recent_days += 1;
            continue;
        }
        match day["state"].as_str() {
            Some("missing") => metrics.missing_days += 1,
            Some("no_data") => metrics.no_data_days += 1,
            Some("observed") if sku.is_some() && day["sku_reconciled"].as_bool() != Some(true) => {
                metrics.unallocated_days += 1;
            }
            Some("observed") => {
                let m = if let Some(sku) = sku {
                    day["products"]
                        .as_array()
                        .ok_or(OptimizerError::InvalidInput)?
                        .iter()
                        .find(|row| row["sku"].as_u64() == Some(sku))
                        .map(|product| &product["metrics"])
                } else {
                    Some(&day["metrics"])
                };
                if let Some(m) = m {
                    metrics.observed_days += 1;
                    metrics.spend_minor = add(metrics.spend_minor, number(m, "spend_minor")?)?;
                    metrics.clicks = add(metrics.clicks, number(m, "clicks")?)?;
                    metrics.orders = add(metrics.orders, number(m, "orders")?)?;
                    metrics.revenue_minor =
                        add(metrics.revenue_minor, number(m, "revenue_minor")?)?;
                }
            }
            _ => return Err(OptimizerError::InvalidInput),
        }
    }
    metrics.drr_bps = if metrics.revenue_minor == 0 {
        None
    } else {
        Some(
            u64::try_from(
                u128::from(metrics.spend_minor) * 10_000 / u128::from(metrics.revenue_minor),
            )
            .map_err(|_| OptimizerError::Overflow)?,
        )
    };
    metrics.cpo_minor = (metrics.orders > 0).then(|| metrics.spend_minor / metrics.orders);
    Ok(Window {
        days: n,
        date_from: from,
        date_to: input.date_to,
        metrics,
    })
}

fn add(a: u64, b: u64) -> Result<u64, OptimizerError> {
    a.checked_add(b).ok_or(OptimizerError::Overflow)
}

fn decide(m: &Metrics, target: u32, reasons: &mut Vec<Reason>) -> (Action, Option<i32>) {
    if m.observed_days < 3
        || m.clicks < 30
        || (m.orders < 3 && !(m.orders == 0 && m.clicks >= 100 && m.spend_minor > 0))
    {
        reasons.push(Reason::InsufficientMatureObservations);
        return (Action::Hold, None);
    }
    if m.orders == 0 {
        reasons.push(Reason::SpendWithoutOrders);
        return (Action::ReviewDecrease, None);
    }
    if m.revenue_minor == 0 {
        reasons.push(Reason::InsufficientMatureObservations);
        return (Action::Hold, None);
    }
    if target == 0 {
        return (Action::Hold, None);
    }
    let spend = u128::from(m.spend_minor) * 10_000 * 10;
    let revenue_target = u128::from(m.revenue_minor) * u128::from(target);
    if spend > revenue_target * 11 {
        reasons.push(Reason::AboveTargetDrr);
        (Action::ReviewDecrease, Some(-500))
    } else if m.spend_minor > 0 && spend < revenue_target * 9 {
        reasons.push(Reason::BelowTargetDrr);
        (Action::TestIncrease, Some(500))
    } else {
        reasons.push(Reason::WithinTargetBand);
        (Action::Hold, None)
    }
}

#[cfg(test)]
mod tests;
