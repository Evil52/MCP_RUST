//! Campaign feedback and exposure-pacing calculations.

use super::{
    ChronoDuration, DateTime, Result, Utc, WbAutomationAction, WbAutomationBidChange,
    WbAutomationDecision, WbAutomationSnapshot, ensure, wb_automation_business_date,
};
use anyhow::Context;

pub(super) const PACING_LOWER_BASIS_POINTS: u128 = 8_000;
pub(super) const PACING_UPPER_BASIS_POINTS: u128 = 12_000;
pub(super) const BASIS_POINTS: u128 = 10_000;
pub(super) const SECONDS_PER_DAY: u64 = 86_400;
pub(super) const MOSCOW_OFFSET_SECONDS: i64 = 3 * 60 * 60;

pub(super) struct TrafficFrontierFeedback {
    pub(super) metrics: super::super::automation::WbAutomationCampaignMetrics,
    pub(super) zero_cost_probe: bool,
}

/// Maintains a CPC campaign around a measured traffic frontier without
/// repeatedly acting on the same delayed WB statistics. The initial jump is
/// explicit policy. V2 retains cumulative feedback semantics, while V3 waits
/// for a statistically useful post-action delta and judges the marginal
/// orders and DRR produced by that last bid change. V4 additionally permits a
/// zero-cost probe after the cooldown when CPC delivery produced no clicks,
/// spend, orders or revenue. Any paid feedback immediately restores the full
/// marginal-sample and economic guards.
pub(super) fn traffic_frontier_pacing_decision(
    policy: &super::super::automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
    last_applied_snapshot: Option<&WbAutomationSnapshot>,
) -> Result<Option<WbAutomationDecision>> {
    use super::super::automation::WbAutomationHoldReason;

    if !matches!(
        snapshot.decision.action,
        WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::AttributionIncomplete
        }
    ) {
        return Ok(None);
    }
    let Some(metrics) = snapshot.observation.current_campaign_metrics.as_ref() else {
        return Ok(Some(traffic_frontier_hold(
            policy,
            snapshot,
            WbAutomationHoldReason::TrafficFeedbackPending,
        )));
    };
    ensure!(
        metrics.clicks <= metrics.impressions
            && metrics.attributed_orders <= metrics.clicks
            && (metrics.attributed_orders > 0 || metrics.attributed_revenue_minor == 0),
        "WB traffic-frontier current campaign metrics are inconsistent"
    );

    let Some(feedback) =
        traffic_frontier_feedback(policy, snapshot, metrics, last_applied_snapshot)?
    else {
        return Ok(Some(traffic_frontier_hold(
            policy,
            snapshot,
            WbAutomationHoldReason::TrafficFeedbackPending,
        )));
    };
    let drr = campaign_drr_basis_points(&feedback.metrics)?;
    let expected_spend = paced_value(
        policy.daily_pause_threshold_minor,
        snapshot.observation.observed_at,
    );
    let expected_impressions = paced_value(
        policy.target_impressions_per_day,
        snapshot.observation.observed_at,
    );
    let spend_over_pace = u128::from(metrics.spend_minor) * BASIS_POINTS
        > u128::from(expected_spend) * PACING_UPPER_BASIS_POINTS;
    let spend_under_pace = u128::from(metrics.spend_minor) * BASIS_POINTS
        < u128::from(expected_spend) * PACING_LOWER_BASIS_POINTS;
    let delivery_under_pace = metrics.impressions < expected_impressions;

    if let Some(reason) =
        traffic_frontier_decrease_reason(policy, &feedback.metrics, drr, spend_over_pace)?
    {
        return Ok(traffic_frontier_decrease(policy, snapshot, reason));
    }
    if policy.autonomous_pacing.uses_marginal_feedback()
        && metrics.attributed_orders >= policy.target_orders_per_day
    {
        return Ok(None);
    }
    if !spend_under_pace || !delivery_under_pace {
        return Ok(None);
    }
    // V3, and V4 after any paid feedback, never buy more traffic merely
    // because delivery is behind its time curve. A regular bid increase
    // requires at least one incremental attributed order and revenue at or
    // below the target marginal DRR. The only exception is V4's bounded
    // zero-cost probe established above.
    if policy.autonomous_pacing.uses_marginal_feedback()
        && !feedback.zero_cost_probe
        && (feedback.metrics.attributed_orders == 0
            || feedback.metrics.attributed_revenue_minor == 0
            || drr.is_none_or(|value| value > policy.target_drr_basis_points))
    {
        return Ok(Some(traffic_frontier_hold(
            policy,
            snapshot,
            WbAutomationHoldReason::TrafficFeedbackPending,
        )));
    }

    traffic_frontier_increase_decision(policy, snapshot, &feedback.metrics)
}

pub(super) fn traffic_frontier_feedback(
    policy: &super::super::automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
    metrics: &super::super::automation::WbAutomationCampaignMetrics,
    last_applied_snapshot: Option<&WbAutomationSnapshot>,
) -> Result<Option<TrafficFrontierFeedback>> {
    if !policy.autonomous_pacing.uses_marginal_feedback() {
        let actionable = traffic_frontier_v2_feedback_is_actionable(
            policy,
            snapshot,
            metrics,
            last_applied_snapshot,
        )?;
        return Ok(actionable.then(|| TrafficFrontierFeedback {
            metrics: metrics.clone(),
            zero_cost_probe: false,
        }));
    }

    let Some(delta) = traffic_feedback_delta(snapshot, metrics, last_applied_snapshot) else {
        return Ok(None);
    };
    let min_impressions = policy
        .traffic_frontier_min_feedback_impressions
        .context("WB traffic-frontier v3 minimum feedback impressions are unavailable")?;
    let min_clicks = policy
        .traffic_frontier_min_feedback_clicks
        .context("WB traffic-frontier v3 minimum feedback clicks are unavailable")?;
    let zero_cost_probe = policy.autonomous_pacing.allows_zero_cost_probe()
        && traffic_frontier_zero_cost_probe_is_actionable(policy, snapshot, &delta)?;
    if delta.impressions < min_impressions && delta.clicks < min_clicks && !zero_cost_probe {
        return Ok(None);
    }
    Ok(Some(TrafficFrontierFeedback {
        metrics: delta,
        zero_cost_probe,
    }))
}

pub(super) fn traffic_frontier_decrease_reason(
    policy: &super::super::automation::WbAutomationPolicy,
    feedback: &super::super::automation::WbAutomationCampaignMetrics,
    drr: Option<u32>,
    spend_over_pace: bool,
) -> Result<Option<super::super::automation::WbAutomationBidReason>> {
    use super::super::automation::WbAutomationBidReason;

    let no_order_reduce_clicks = if policy.autonomous_pacing.uses_marginal_feedback() {
        policy
            .traffic_frontier_min_feedback_clicks
            .context("WB traffic-frontier v3 minimum feedback clicks are unavailable")?
    } else {
        policy.no_order_reduce_clicks
    };
    if feedback.attributed_orders == 0 && feedback.clicks >= no_order_reduce_clicks {
        return Ok(Some(WbAutomationBidReason::NoOrdersAfterClicks));
    }
    if let Some(value) = drr.filter(|value| *value > policy.target_drr_basis_points) {
        return Ok(Some(if value > policy.hard_drr_basis_points {
            WbAutomationBidReason::HardDrrExceeded
        } else {
            WbAutomationBidReason::TargetDrrExceeded
        }));
    }
    Ok(spend_over_pace.then_some(WbAutomationBidReason::TrafficFrontierRetentionDecrease))
}

pub(super) fn traffic_frontier_increase_decision(
    policy: &super::super::automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
    feedback: &super::super::automation::WbAutomationCampaignMetrics,
) -> Result<Option<WbAutomationDecision>> {
    use super::super::automation::WbAutomationBidReason;

    let frontier = policy
        .traffic_frontier_bid_kopecks
        .context("WB traffic-frontier entry bid is unavailable")?;
    let dynamic_cap = traffic_frontier_dynamic_cap(policy, snapshot, feedback)?;
    if dynamic_cap < frontier {
        return Ok(None);
    }
    let Some(sku) = snapshot
        .observation
        .skus
        .iter()
        .filter(|sku| {
            sku.sellable_stock > policy.min_sellable_stock
                && sku.current_bid_kopecks < dynamic_cap
                && super::super::automation::effective_minimum_bid(policy, sku) <= dynamic_cap
        })
        .min_by_key(|sku| (sku.current_bid_kopecks, sku.nm_id))
    else {
        return Ok(None);
    };
    let minimum = super::super::automation::effective_minimum_bid(policy, sku);
    let (to_bid_kopecks, reason) = if sku.current_bid_kopecks < frontier {
        (
            frontier.max(minimum).min(dynamic_cap),
            WbAutomationBidReason::TrafficFrontierBootstrap,
        )
    } else {
        (
            super::super::automation::increase_bid(policy, sku.current_bid_kopecks, minimum)?
                .min(dynamic_cap),
            WbAutomationBidReason::TrafficFrontierIncrease,
        )
    };
    Ok(Some(traffic_frontier_change(
        policy,
        snapshot,
        sku.nm_id,
        sku.current_bid_kopecks,
        to_bid_kopecks,
        reason,
    )))
}

pub(super) fn traffic_frontier_zero_cost_probe_is_actionable(
    policy: &super::super::automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
    delta: &super::super::automation::WbAutomationCampaignMetrics,
) -> Result<bool> {
    if delta.clicks != 0
        || delta.spend_minor != 0
        || delta.attributed_orders != 0
        || delta.attributed_revenue_minor != 0
    {
        return Ok(false);
    }
    let Some(last_action_at) = snapshot.observation.last_action_at else {
        return Ok(true);
    };
    let timeout = policy
        .traffic_frontier_feedback_timeout_seconds
        .context("WB traffic-frontier v4 probe timeout is unavailable")?;
    Ok(snapshot.observation.observed_at
        >= last_action_at + ChronoDuration::seconds(i64::from(timeout)))
}

pub(super) fn traffic_frontier_v2_feedback_is_actionable(
    policy: &super::super::automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
    current: &super::super::automation::WbAutomationCampaignMetrics,
    last_applied_snapshot: Option<&WbAutomationSnapshot>,
) -> Result<bool> {
    let Some(last_action_at) = snapshot.observation.last_action_at else {
        return Ok(true);
    };
    let feedback_timeout = policy
        .traffic_frontier_feedback_timeout_seconds
        .context("WB traffic-frontier feedback timeout is unavailable")?;
    if snapshot.observation.observed_at
        >= last_action_at + ChronoDuration::seconds(i64::from(feedback_timeout))
    {
        return Ok(true);
    }
    let Some(previous) = last_applied_snapshot else {
        return Ok(false);
    };
    if wb_automation_business_date(previous.observation.observed_at)
        != wb_automation_business_date(snapshot.observation.observed_at)
    {
        return Ok(true);
    }
    let Some(baseline) = previous.observation.current_campaign_metrics.as_ref() else {
        return Ok(false);
    };
    let monotonic = current.impressions >= baseline.impressions
        && current.clicks >= baseline.clicks
        && current.spend_minor >= baseline.spend_minor
        && current.attributed_orders >= baseline.attributed_orders
        && current.attributed_revenue_minor >= baseline.attributed_revenue_minor;
    if !monotonic {
        return Ok(false);
    }
    Ok(current != baseline)
}

pub(super) fn traffic_feedback_delta(
    snapshot: &WbAutomationSnapshot,
    current: &super::super::automation::WbAutomationCampaignMetrics,
    last_applied_snapshot: Option<&WbAutomationSnapshot>,
) -> Option<super::super::automation::WbAutomationCampaignMetrics> {
    let Some(previous) = last_applied_snapshot else {
        return Some(current.clone());
    };
    if wb_automation_business_date(previous.observation.observed_at)
        != wb_automation_business_date(snapshot.observation.observed_at)
    {
        return Some(current.clone());
    }
    let baseline = previous.observation.current_campaign_metrics.as_ref()?;
    if current.impressions < baseline.impressions
        || current.clicks < baseline.clicks
        || current.spend_minor < baseline.spend_minor
        || current.attributed_orders < baseline.attributed_orders
        || current.attributed_revenue_minor < baseline.attributed_revenue_minor
    {
        return None;
    }
    Some(super::super::automation::WbAutomationCampaignMetrics {
        impressions: current.impressions - baseline.impressions,
        clicks: current.clicks - baseline.clicks,
        spend_minor: current.spend_minor - baseline.spend_minor,
        attributed_orders: current.attributed_orders - baseline.attributed_orders,
        attributed_revenue_minor: current.attributed_revenue_minor
            - baseline.attributed_revenue_minor,
    })
}

pub(super) fn traffic_frontier_dynamic_cap(
    policy: &super::super::automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
    metrics: &super::super::automation::WbAutomationCampaignMetrics,
) -> Result<u64> {
    let remaining_to_pause = policy
        .daily_pause_threshold_minor
        .saturating_sub(snapshot.observation.daily_spend_minor);
    let economic_cap = if metrics.attributed_orders == 0 {
        policy
            .daily_spend_cap_minor
            .checked_div(policy.no_order_reduce_clicks)
            .context("WB traffic-frontier exploration click budget is invalid")?
    } else {
        let allowed_spend = u128::from(metrics.attributed_revenue_minor)
            .checked_mul(u128::from(policy.target_drr_basis_points))
            .context("WB traffic-frontier economic cap overflow")?
            / BASIS_POINTS;
        u64::try_from(allowed_spend / u128::from(metrics.clicks))
            .context("WB traffic-frontier economic cap is out of range")?
    };
    Ok(policy
        .max_bid_kopecks
        .min(remaining_to_pause)
        .min(economic_cap))
}

pub(super) fn campaign_drr_basis_points(
    metrics: &super::super::automation::WbAutomationCampaignMetrics,
) -> Result<Option<u32>> {
    if metrics.attributed_revenue_minor == 0 {
        return Ok(None);
    }
    let value = u128::from(metrics.spend_minor)
        .checked_mul(BASIS_POINTS)
        .context("WB traffic-frontier DRR overflow")?
        / u128::from(metrics.attributed_revenue_minor);
    Ok(Some(
        u32::try_from(value).context("WB traffic-frontier DRR is out of range")?,
    ))
}

pub(super) fn paced_value(total: u64, observed_at: DateTime<Utc>) -> u64 {
    let seconds = (observed_at.timestamp() + MOSCOW_OFFSET_SECONDS)
        .rem_euclid(i64::try_from(SECONDS_PER_DAY).expect("seconds per day fit i64"));
    let elapsed = u64::try_from(seconds)
        .expect("Moscow seconds-of-day are non-negative and fit u64")
        .max(300);
    let value = u128::from(total) * u128::from(elapsed) / u128::from(SECONDS_PER_DAY);
    u64::try_from(value.max(1)).expect("paced value cannot exceed its u64 total")
}

pub(super) fn traffic_frontier_decrease(
    policy: &super::super::automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
    reason: super::super::automation::WbAutomationBidReason,
) -> Option<WbAutomationDecision> {
    let sku = snapshot
        .observation
        .skus
        .iter()
        .filter(|sku| {
            sku.current_bid_kopecks > super::super::automation::effective_minimum_bid(policy, sku)
        })
        .max_by_key(|sku| (sku.current_bid_kopecks, std::cmp::Reverse(sku.nm_id)))?;
    let to_bid_kopecks = decrease_frontier_bid(
        policy,
        sku.current_bid_kopecks,
        super::super::automation::effective_minimum_bid(policy, sku),
    );
    (to_bid_kopecks < sku.current_bid_kopecks).then(|| {
        traffic_frontier_change(
            policy,
            snapshot,
            sku.nm_id,
            sku.current_bid_kopecks,
            to_bid_kopecks,
            reason,
        )
    })
}

pub(super) fn decrease_frontier_bid(
    policy: &super::super::automation::WbAutomationPolicy,
    current: u64,
    minimum: u64,
) -> u64 {
    let delta = current.saturating_mul(u64::from(policy.bid_step_percent)) / 100;
    current.saturating_sub(delta.max(1)).max(minimum)
}

pub(super) fn traffic_frontier_change(
    policy: &super::super::automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
    nm_id: u64,
    from_bid_kopecks: u64,
    to_bid_kopecks: u64,
    reason: super::super::automation::WbAutomationBidReason,
) -> WbAutomationDecision {
    WbAutomationDecision {
        account_id: policy.account_id.clone(),
        campaign_id: policy.campaign_id,
        observed_at: snapshot.observation.observed_at,
        action: WbAutomationAction::ChangeBids {
            changes: vec![WbAutomationBidChange {
                nm_id,
                from_bid_kopecks,
                to_bid_kopecks,
                reason,
            }],
        },
        unresolved_stops: Vec::new(),
    }
}

pub(super) fn traffic_frontier_hold(
    policy: &super::super::automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
    reason: super::super::automation::WbAutomationHoldReason,
) -> WbAutomationDecision {
    WbAutomationDecision {
        account_id: policy.account_id.clone(),
        campaign_id: policy.campaign_id,
        observed_at: snapshot.observation.observed_at,
        action: WbAutomationAction::Hold { reason },
        unresolved_stops: Vec::new(),
    }
}

/// Turns an otherwise fail-closed campaign-level attribution hold into one
/// bounded pacing step. Aggregate metrics may authorize more campaign
/// exposure, but never pretend to identify SKU economics: the least-bid safe
/// in-stock SKU is selected deterministically, one at a time.
pub(super) fn autonomous_exposure_pacing_decision(
    policy: &super::super::automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
) -> Result<Option<WbAutomationDecision>> {
    if !matches!(
        snapshot.decision.action,
        WbAutomationAction::Hold {
            reason: super::super::automation::WbAutomationHoldReason::AttributionIncomplete
        }
    ) {
        return Ok(None);
    }
    let Some(metrics) = snapshot.observation.campaign_level_metrics.as_ref() else {
        return Ok(None);
    };
    let delivery_below_target = metrics.impressions < policy.target_impressions_per_day;
    let no_order_signal_is_safe = if metrics.attributed_orders == 0 {
        metrics.attributed_revenue_minor == 0 && metrics.clicks < policy.no_order_reduce_clicks
    } else {
        metrics.attributed_revenue_minor > 0
            && u128::from(metrics.spend_minor) * 10_000
                <= u128::from(metrics.attributed_revenue_minor)
                    * u128::from(policy.target_drr_basis_points)
    };
    if !delivery_below_target || !no_order_signal_is_safe {
        return Ok(None);
    }
    let Some(sku) = policy
        .nm_ids
        .iter()
        .filter_map(|nm_id| {
            snapshot
                .observation
                .skus
                .iter()
                .find(|sku| sku.nm_id == *nm_id)
        })
        .filter(|sku| {
            sku.sellable_stock > policy.min_sellable_stock
                && sku.current_bid_kopecks < policy.max_bid_kopecks
        })
        .min_by_key(|sku| sku.current_bid_kopecks)
    else {
        return Ok(None);
    };
    let to_bid_kopecks = super::super::automation::increase_bid(
        policy,
        sku.current_bid_kopecks,
        super::super::automation::effective_minimum_bid(policy, sku),
    )
    .context("WB autonomous exposure pacing bid increase is invalid")?;
    ensure!(
        to_bid_kopecks > sku.current_bid_kopecks,
        "WB autonomous exposure pacing bid cannot increase"
    );
    Ok(Some(WbAutomationDecision {
        account_id: policy.account_id.clone(),
        campaign_id: policy.campaign_id,
        observed_at: snapshot.observation.observed_at,
        action: WbAutomationAction::ChangeBids {
            changes: vec![WbAutomationBidChange {
                nm_id: sku.nm_id,
                from_bid_kopecks: sku.current_bid_kopecks,
                to_bid_kopecks,
                reason: super::super::automation::WbAutomationBidReason::AutonomousExposurePacing,
            }],
        },
        unresolved_stops: Vec::new(),
    }))
}
