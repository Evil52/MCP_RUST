//! Decisions that require a separate explicit maintenance intent.
use super::{
    WbAutomationAction, WbAutomationBidChange, WbAutomationDecision, WbAutomationSnapshot,
    traffic_frontier_pacing_decision,
};
use crate::control::automation;
use anyhow::{Context as _, Result, ensure};

pub(super) fn explicit_bid_cap_repair_decision(
    policy: &automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
) -> Result<WbAutomationDecision> {
    use automation::{WbAutomationBidReason, WbAutomationHoldReason};

    let observation = &snapshot.observation;
    let observed_at = observation.observed_at;
    let authorization_active = policy.authorized_at <= observed_at
        && policy.observe_until <= observed_at
        && observed_at < policy.authorization_expires_at;
    ensure!(
        policy.write_enabled && policy.bid_writes_enabled && authorization_active,
        "WB bid cap repair requires active explicit write authorization"
    );
    ensure!(
        matches!(observation.campaign_status, 9 | 11),
        "WB bid cap repair requires an active or paused campaign"
    );
    ensure!(
        observation.daily_spend_complete,
        "WB bid cap repair requires complete daily spend"
    );
    ensure!(
        observation.campaign_status == 11
            || observation.daily_spend_minor < policy.daily_pause_threshold_minor,
        "WB bid cap repair requires protective pause before repairing an overspent active campaign"
    );
    ensure!(
        observation.actions_today < policy.max_actions_per_day,
        "WB bid cap repair preserves the daily action quota"
    );
    let action = policy
        .nm_ids
        .iter()
        .filter_map(|nm_id| observation.skus.iter().find(|sku| sku.nm_id == *nm_id))
        .find(|sku| sku.current_bid_kopecks > policy.max_bid_kopecks)
        .map_or(
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::NoMaterialChange,
            },
            |sku| WbAutomationAction::ChangeBids {
                changes: vec![WbAutomationBidChange {
                    nm_id: sku.nm_id,
                    from_bid_kopecks: sku.current_bid_kopecks,
                    to_bid_kopecks: policy.max_bid_kopecks,
                    reason: WbAutomationBidReason::PolicyMaximumExceeded,
                }],
            },
        );
    Ok(WbAutomationDecision {
        account_id: policy.account_id.clone(),
        campaign_id: policy.campaign_id,
        observed_at: observation.observed_at,
        action,
        unresolved_stops: Vec::new(),
    })
}

pub(super) fn explicit_quota_override_decision(
    policy: &automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
    last_applied_snapshot: Option<&WbAutomationSnapshot>,
) -> Result<Option<WbAutomationDecision>> {
    use automation::WbAutomationHoldReason;

    ensure!(
        policy.autonomous_pacing.uses_marginal_feedback(),
        "WB explicit quota override requires traffic frontier v3"
    );
    ensure!(
        matches!(
            snapshot.decision.action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::ActionQuotaExhausted
            }
        ) && snapshot.observation.actions_today >= policy.max_actions_per_day,
        "WB explicit quota override requires an exhausted daily action quota"
    );
    ensure!(
        snapshot
            .observation
            .last_action_at
            .is_none_or(|last_action| {
                snapshot.observation.observed_at
                    >= last_action + chrono::Duration::seconds(i64::from(policy.cooldown_seconds))
            }),
        "WB explicit quota override preserves the cooldown guard"
    );

    let mut eligible = snapshot.clone();
    eligible.decision.action = WbAutomationAction::Hold {
        reason: WbAutomationHoldReason::AttributionIncomplete,
    };
    traffic_frontier_pacing_decision(policy, &eligible, last_applied_snapshot)
}

pub(super) fn explicit_resume_after_daily_cap_decision(
    policy: &automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
) -> Result<WbAutomationDecision> {
    use automation::WbAutomationHoldReason;

    ensure!(
        policy.write_enabled && policy.bid_writes_enabled,
        "WB explicit resume requires active write and bid-write policy"
    );
    ensure!(
        matches!(
            snapshot.decision.action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::ProtectivePauseRequiresApproval
                    | WbAutomationHoldReason::SpendDataIncomplete
            }
        ),
        "WB explicit resume is blocked by a stronger decision guard"
    );
    ensure!(
        snapshot.observation.campaign_status == 11 && snapshot.observation.paused_by_automation,
        "WB explicit resume requires the automation-owned protective pause"
    );
    ensure!(
        snapshot.observation.budget_remaining_minor > 0,
        "WB explicit resume requires a positive campaign budget"
    );
    ensure!(
        snapshot.observation.daily_spend_minor < policy.daily_pause_threshold_minor
            && snapshot.observation.daily_spend_minor < policy.daily_spend_cap_minor,
        "WB explicit resume is blocked by the current daily spend guard"
    );
    ensure!(
        snapshot
            .observation
            .skus
            .iter()
            .any(|sku| sku.sellable_stock > policy.min_sellable_stock),
        "WB explicit resume requires at least one sellable SKU"
    );
    ensure!(
        snapshot.observation.skus.iter().all(|sku| {
            sku.current_bid_kopecks >= automation::effective_minimum_bid(policy, sku)
                && sku.current_bid_kopecks <= policy.max_bid_kopecks
        }),
        "WB explicit resume requires every current bid inside the reviewed corridor"
    );
    Ok(WbAutomationDecision {
        account_id: policy.account_id.clone(),
        campaign_id: policy.campaign_id,
        observed_at: snapshot.observation.observed_at,
        action: WbAutomationAction::ResumeCampaignAfterDailyCap,
        unresolved_stops: Vec::new(),
    })
}

pub(super) fn explicit_exposure_increase_decision(
    policy: &automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
    target_impressions: u64,
) -> Result<WbAutomationDecision> {
    ensure!(
        target_impressions == policy.target_impressions_per_day,
        "WB explicit exposure target does not match policy"
    );
    ensure!(
        matches!(
            snapshot.decision.action,
            WbAutomationAction::Hold {
                reason: automation::WbAutomationHoldReason::AttributionIncomplete
            }
        ),
        "WB explicit exposure increase is blocked by a stronger decision guard"
    );
    let metrics = snapshot
        .observation
        .campaign_level_metrics
        .as_ref()
        .context("WB explicit exposure increase requires campaign delivery metrics")?;
    ensure!(
        metrics.impressions < target_impressions
            && metrics.impressions < policy.low_exposure_max_impressions
            && metrics.clicks < policy.low_exposure_max_clicks,
        "WB explicit exposure increase requires verified low exposure"
    );
    let sku = policy
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
        .context("WB explicit exposure increase has no safe SKU candidate")?;
    let to_bid_kopecks = automation::increase_bid(
        policy,
        sku.current_bid_kopecks,
        automation::effective_minimum_bid(policy, sku),
    )
    .context("WB explicit exposure bid increase is invalid")?;
    ensure!(
        to_bid_kopecks > sku.current_bid_kopecks,
        "WB explicit exposure bid cannot increase"
    );
    Ok(WbAutomationDecision {
        account_id: policy.account_id.clone(),
        campaign_id: policy.campaign_id,
        observed_at: snapshot.observation.observed_at,
        action: WbAutomationAction::ChangeBids {
            changes: vec![WbAutomationBidChange {
                nm_id: sku.nm_id,
                from_bid_kopecks: sku.current_bid_kopecks,
                to_bid_kopecks,
                reason: automation::WbAutomationBidReason::ExplicitExposureTarget,
            }],
        },
        unresolved_stops: Vec::new(),
    })
}
