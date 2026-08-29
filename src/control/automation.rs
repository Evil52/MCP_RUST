use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, FixedOffset, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const ACTIVE_CAMPAIGN_STATUS: i32 = 9;
const PAUSED_CAMPAIGN_STATUS: i32 = 11;
const BASIS_POINTS: u128 = 10_000;
const MOSCOW_OFFSET_SECONDS: i32 = 3 * 60 * 60;

#[must_use]
/// Returns the Moscow advertising business date for a UTC instant.
///
/// # Panics
///
/// Panics only if the compile-time UTC+3 offset cannot be constructed.
pub fn wb_automation_business_date(now: DateTime<Utc>) -> NaiveDate {
    now.with_timezone(
        &FixedOffset::east_opt(MOSCOW_OFFSET_SECONDS)
            .expect("the fixed Moscow UTC offset is valid"),
    )
    .date_naive()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbAutomationPolicy {
    pub policy_version: String,
    pub write_enabled: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub bid_writes_enabled: bool,
    pub account_id: String,
    pub campaign_id: u64,
    pub campaign_name: String,
    pub payment_type: String,
    pub placement: String,
    pub timezone: String,
    pub authorized_by_actor_id: String,
    pub authorization_reference: String,
    pub authorized_at: DateTime<Utc>,
    pub authorization_expires_at: DateTime<Utc>,
    pub nm_ids: Vec<u64>,
    pub target_drr_basis_points: u32,
    pub hard_drr_basis_points: u32,
    pub target_impressions_per_day: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub target_orders_per_day: u64,
    #[serde(default, skip_serializing_if = "is_pacing_disabled")]
    pub autonomous_pacing: WbAutomationPacingMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic_frontier_bid_kopecks: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic_frontier_feedback_timeout_seconds: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic_frontier_min_feedback_impressions: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic_frontier_min_feedback_clicks: Option<u64>,
    pub min_bid_kopecks: u64,
    pub max_bid_kopecks: u64,
    pub bid_step_percent: u8,
    pub daily_spend_cap_minor: u64,
    pub daily_pause_threshold_minor: u64,
    pub max_actions_per_day: u32,
    pub cooldown_seconds: u32,
    pub min_sellable_stock: u64,
    pub no_order_reduce_clicks: u64,
    pub no_order_disable_clicks: u64,
    pub no_order_disable_spend_minor: u64,
    pub efficient_min_orders: u64,
    pub efficient_min_conversion_basis_points: u32,
    pub low_exposure_max_impressions: u64,
    pub low_exposure_max_clicks: u64,
    pub observe_until: DateTime<Utc>,
    pub allow_budget_top_up: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WbAutomationPacingMode {
    #[default]
    Disabled,
    Enabled,
    TrafficFrontierV2,
    TrafficFrontierV3,
    TrafficFrontierV4,
}

impl WbAutomationPacingMode {
    #[must_use]
    pub const fn is_enabled(self) -> bool {
        matches!(
            self,
            Self::Enabled
                | Self::TrafficFrontierV2
                | Self::TrafficFrontierV3
                | Self::TrafficFrontierV4
        )
    }

    #[must_use]
    pub const fn uses_traffic_frontier(self) -> bool {
        matches!(
            self,
            Self::TrafficFrontierV2 | Self::TrafficFrontierV3 | Self::TrafficFrontierV4
        )
    }

    #[must_use]
    pub const fn uses_marginal_feedback(self) -> bool {
        matches!(self, Self::TrafficFrontierV3 | Self::TrafficFrontierV4)
    }

    #[must_use]
    pub const fn allows_zero_cost_probe(self) -> bool {
        matches!(self, Self::TrafficFrontierV4)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbAutomationObservation {
    pub observed_at: DateTime<Utc>,
    pub campaign_status: i32,
    pub paused_by_automation: bool,
    pub budget_remaining_minor: u64,
    pub daily_spend_minor: u64,
    pub daily_spend_complete: bool,
    pub actions_today: u32,
    pub last_action_at: Option<DateTime<Utc>>,
    pub attribution_complete: bool,
    /// Previous-business-day totals when WB exposes delivery only at campaign
    /// level. These totals are never copied into individual SKU rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign_level_metrics: Option<WbAutomationCampaignMetrics>,
    /// Current-business-day campaign totals used only for budget/traffic
    /// pacing. They remain aggregate evidence and are never attributed to an
    /// individual SKU.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_campaign_metrics: Option<WbAutomationCampaignMetrics>,
    pub skus: Vec<WbAutomationSkuObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbAutomationCampaignMetrics {
    pub impressions: u64,
    pub clicks: u64,
    pub spend_minor: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbAutomationSkuObservation {
    pub nm_id: u64,
    /// Current WB-enforced floor for this SKU, payment type and placement.
    /// Zero exists only for backward-compatible deserialization of old
    /// snapshots; every fresh observation rejects it.
    #[serde(default)]
    pub minimum_bid_kopecks: u64,
    pub current_bid_kopecks: u64,
    pub sellable_stock: u64,
    pub impressions: u64,
    pub clicks: u64,
    pub spend_minor: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WbAutomationHoldReason {
    AuthorizationNotActive,
    AuthorizationExpired,
    PolicyShadowOnly,
    BidWritesDisabled,
    ObserveOnly,
    AttributionIncomplete,
    CampaignNotActive,
    ProtectivePauseRequiresApproval,
    SpendDataIncomplete,
    BudgetExhausted,
    ActionQuotaExhausted,
    CooldownActive,
    TrafficFeedbackPending,
    NoMaterialChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WbAutomationBidReason {
    PolicyMaximumExceeded,
    TargetDrrExceeded,
    NoOrdersAfterClicks,
    EfficientSales,
    LowExposureExploration,
    ExplicitExposureTarget,
    AutonomousExposurePacing,
    TrafficFrontierBootstrap,
    TrafficFrontierIncrease,
    TrafficFrontierRetentionDecrease,
    LowStockGuard,
    NoOrdersHardStop,
    HardDrrExceeded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbAutomationBidChange {
    pub nm_id: u64,
    pub from_bid_kopecks: u64,
    pub to_bid_kopecks: u64,
    pub reason: WbAutomationBidReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WbAutomationDisableReason {
    LowStock,
    NoOrdersHardStop,
    HardDrrExceeded,
}

/// A SKU that still qualifies for a hard stop while its bid already sits at
/// `min_bid_kopecks`.
///
/// Lowering the bid is the strongest per-SKU lever the WB promotion API
/// exposes — there is no per-SKU disable endpoint — so such a SKU cannot be
/// stopped any further by this automation. Recording it keeps that unresolved
/// state visible in the snapshot instead of leaving an operator to infer it
/// from a run that reports no action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbAutomationSkuStop {
    pub nm_id: u64,
    pub reason: WbAutomationDisableReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WbAutomationAction {
    Hold {
        reason: WbAutomationHoldReason,
    },
    ChangeBids {
        changes: Vec<WbAutomationBidChange>,
    },
    DisableSku {
        nm_id: u64,
        reason: WbAutomationDisableReason,
    },
    PauseCampaignForDailyCap,
    ResumeCampaignAfterDailyCap,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbAutomationDecision {
    pub account_id: String,
    pub campaign_id: u64,
    pub observed_at: DateTime<Utc>,
    pub action: WbAutomationAction,
    /// SKUs under a hard stop that are already floored, so `action` cannot
    /// address them. Empty whenever a campaign-wide guard held before any
    /// per-SKU reasoning ran.
    pub unresolved_stops: Vec<WbAutomationSkuStop>,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum WbAutomationDecisionError {
    #[error("WB automation policy имеет недопустимые границы")]
    InvalidPolicy,
    #[error("WB automation observation не соответствует policy")]
    InvalidObservation,
    #[error("WB automation arithmetic overflow")]
    Overflow,
}

pub fn evaluate_wb_automation(
    policy: &WbAutomationPolicy,
    observation: &WbAutomationObservation,
) -> Result<WbAutomationDecision, WbAutomationDecisionError> {
    validate_policy(policy)?;
    let observations = validate_observation(policy, observation)?;
    let decision = |action, unresolved_stops| WbAutomationDecision {
        account_id: policy.account_id.clone(),
        campaign_id: policy.campaign_id,
        observed_at: observation.observed_at,
        action,
        unresolved_stops,
    };

    if let Some(action) = campaign_gate(policy, observation) {
        return Ok(decision(action, Vec::new()));
    }

    // The first PostgreSQL live rollout is deliberately protective-only. It
    // may pause the whole campaign at the approved spend threshold, but SKU
    // bid changes stay unavailable until their additional data gates and
    // minimum-bid read-before-write contract are enabled in typed policy.
    if !policy.bid_writes_enabled {
        return Ok(decision(
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::BidWritesDisabled,
            },
            Vec::new(),
        ));
    }

    if let Some(sku) = policy
        .nm_ids
        .iter()
        .map(|nm_id| observations[nm_id])
        .find(|sku| sku.current_bid_kopecks > policy.max_bid_kopecks)
    {
        return Ok(decision(
            WbAutomationAction::ChangeBids {
                changes: vec![WbAutomationBidChange {
                    nm_id: sku.nm_id,
                    from_bid_kopecks: sku.current_bid_kopecks,
                    to_bid_kopecks: policy.max_bid_kopecks,
                    reason: WbAutomationBidReason::PolicyMaximumExceeded,
                }],
            },
            Vec::new(),
        ));
    }

    if !observation.attribution_complete {
        let (action, unresolved_stops) = campaign_level_action(policy, observation, &observations);
        return Ok(decision(action, unresolved_stops));
    }

    let stops = sku_stops(policy, &observations)?;
    // A stopped SKU whose bid is already at the floor has had every available
    // lever applied. It must not abort the run: doing so would freeze bid
    // management for every healthy SKU in the campaign for as long as the stop
    // lasts, which for a sold-out SKU can be indefinitely.
    let unresolved = policy
        .nm_ids
        .iter()
        .filter_map(|nm_id| {
            stops
                .get(nm_id)
                .filter(|_| {
                    observations[nm_id].current_bid_kopecks
                        <= effective_minimum_bid(policy, observations[nm_id])
                })
                .map(|reason| WbAutomationSkuStop {
                    nm_id: *nm_id,
                    reason: *reason,
                })
        })
        .collect::<Vec<_>>();

    if let Some((nm_id, reason)) = policy.nm_ids.iter().find_map(|nm_id| {
        stops
            .get(nm_id)
            .filter(|_| {
                observations[nm_id].current_bid_kopecks
                    > effective_minimum_bid(policy, observations[nm_id])
            })
            .map(|reason| (*nm_id, *reason))
    }) {
        return Ok(decision(
            WbAutomationAction::DisableSku { nm_id, reason },
            unresolved,
        ));
    }

    let mut changes = Vec::new();
    for nm_id in &policy.nm_ids {
        // Never bid a stopped SKU back up. Without this a sold-out SKU parked
        // at the floor would match the low-exposure rule on its own suppressed
        // traffic and be raised straight off the floor.
        if stops.contains_key(nm_id) {
            continue;
        }
        if let Some(change) = bid_change(policy, observations[nm_id])? {
            changes.push(change);
        }
    }
    // A cycle may execute at most one minimal SKU action. The policy order is
    // stable, while the six-hour cooldown forces the next choice to use a
    // fresh observation instead of batching speculative changes.
    changes.truncate(1);
    Ok(decision(
        if changes.is_empty() {
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::NoMaterialChange,
            }
        } else {
            WbAutomationAction::ChangeBids { changes }
        },
        unresolved,
    ))
}

/// Campaign-wide guards, evaluated in fixed priority order before any per-SKU
/// reasoning. Returns the action to take, or `None` when the campaign is clear
/// to be judged SKU by SKU.
///
/// The emergency daily-cap pause precedes quota and cooldown checks: honouring
/// the spend cap must never be blocked by rate limits for ordinary bid changes.
fn campaign_gate(
    policy: &WbAutomationPolicy,
    observation: &WbAutomationObservation,
) -> Option<WbAutomationAction> {
    let hold = |reason| Some(WbAutomationAction::Hold { reason });
    if observation.observed_at < policy.authorized_at {
        return hold(WbAutomationHoldReason::AuthorizationNotActive);
    }
    if observation.observed_at >= policy.authorization_expires_at {
        return hold(WbAutomationHoldReason::AuthorizationExpired);
    }
    if !policy.write_enabled {
        return hold(WbAutomationHoldReason::PolicyShadowOnly);
    }
    if observation.observed_at < policy.observe_until {
        return hold(WbAutomationHoldReason::ObserveOnly);
    }
    if observation.budget_remaining_minor == 0 {
        return hold(WbAutomationHoldReason::BudgetExhausted);
    }
    if !observation.daily_spend_complete {
        return hold(WbAutomationHoldReason::SpendDataIncomplete);
    }
    if observation.campaign_status == ACTIVE_CAMPAIGN_STATUS
        && observation.daily_spend_minor >= policy.daily_pause_threshold_minor
    {
        return Some(WbAutomationAction::PauseCampaignForDailyCap);
    }
    // WB ADS ROBOT v1 never resumes a campaign automatically after a
    // protective pause. A new explicit authorization or a separately reviewed
    // resume rule is required, even after the business date changes.
    if observation.campaign_status == PAUSED_CAMPAIGN_STATUS && observation.paused_by_automation {
        return hold(WbAutomationHoldReason::ProtectivePauseRequiresApproval);
    }
    if observation.campaign_status != ACTIVE_CAMPAIGN_STATUS {
        return hold(WbAutomationHoldReason::CampaignNotActive);
    }
    if observation.actions_today >= policy.max_actions_per_day {
        return hold(WbAutomationHoldReason::ActionQuotaExhausted);
    }
    if observation.last_action_at.is_some_and(|last_action| {
        observation.observed_at
            < last_action + Duration::seconds(i64::from(policy.cooldown_seconds))
    }) {
        return hold(WbAutomationHoldReason::CooldownActive);
    }
    None
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if requires a predicate that borrows the field"
)]
const fn is_false(value: &bool) -> bool {
    !*value
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if requires a predicate that borrows the field"
)]
const fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if requires a predicate that borrows the field"
)]
const fn is_pacing_disabled(value: &WbAutomationPacingMode) -> bool {
    matches!(value, WbAutomationPacingMode::Disabled)
}

/// Campaign totals are useful for campaign-wide reporting, but cannot identify
/// which SKU has the best probability of an economical order. WB ADS ROBOT v1
/// permits at most one SKU change per cycle, so aggregate delivery must never
/// be expanded into fabricated SKU decisions. Per-SKU low-stock protection is
/// retained because stock remains genuinely attributable by SKU.
fn campaign_level_action(
    policy: &WbAutomationPolicy,
    observation: &WbAutomationObservation,
    observations: &BTreeMap<u64, &WbAutomationSkuObservation>,
) -> (WbAutomationAction, Vec<WbAutomationSkuStop>) {
    let Some(_metrics) = observation.campaign_level_metrics.as_ref() else {
        return (
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AttributionIncomplete,
            },
            Vec::new(),
        );
    };

    let unresolved_stops = policy
        .nm_ids
        .iter()
        .filter_map(|nm_id| {
            let sku = observations[nm_id];
            (sku.sellable_stock <= policy.min_sellable_stock
                && sku.current_bid_kopecks <= effective_minimum_bid(policy, sku))
            .then_some(WbAutomationSkuStop {
                nm_id: *nm_id,
                reason: WbAutomationDisableReason::LowStock,
            })
        })
        .collect::<Vec<_>>();
    if let Some(nm_id) = policy.nm_ids.iter().find(|nm_id| {
        let sku = observations[nm_id];
        sku.sellable_stock <= policy.min_sellable_stock
            && sku.current_bid_kopecks > effective_minimum_bid(policy, sku)
    }) {
        return (
            WbAutomationAction::DisableSku {
                nm_id: *nm_id,
                reason: WbAutomationDisableReason::LowStock,
            },
            unresolved_stops,
        );
    }
    if !unresolved_stops.is_empty() {
        return (
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::NoMaterialChange,
            },
            unresolved_stops,
        );
    }

    (
        WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::AttributionIncomplete,
        },
        Vec::new(),
    )
}

/// Every SKU that currently qualifies for a hard stop, keyed by SKU.
fn sku_stops(
    policy: &WbAutomationPolicy,
    observations: &BTreeMap<u64, &WbAutomationSkuObservation>,
) -> Result<BTreeMap<u64, WbAutomationDisableReason>, WbAutomationDecisionError> {
    let mut stops = BTreeMap::new();
    for nm_id in &policy.nm_ids {
        let sku = observations[nm_id];
        let reason = if sku.sellable_stock <= policy.min_sellable_stock {
            Some(WbAutomationDisableReason::LowStock)
        } else if sku.attributed_orders == 0
            && (sku.clicks >= policy.no_order_disable_clicks
                || sku.spend_minor >= policy.no_order_disable_spend_minor)
        {
            Some(WbAutomationDisableReason::NoOrdersHardStop)
        } else if drr_basis_points(sku)?.is_some_and(|drr| drr > policy.hard_drr_basis_points) {
            Some(WbAutomationDisableReason::HardDrrExceeded)
        } else {
            None
        };
        if let Some(reason) = reason {
            stops.insert(*nm_id, reason);
        }
    }
    Ok(stops)
}

pub fn validate_wb_automation_policy(
    policy: &WbAutomationPolicy,
) -> Result<(), WbAutomationDecisionError> {
    validate_policy(policy)
}

fn validate_policy(policy: &WbAutomationPolicy) -> Result<(), WbAutomationDecisionError> {
    let nm_ids = policy.nm_ids.iter().copied().collect::<BTreeSet<_>>();
    if ![
        valid_policy_identity(policy),
        valid_policy_authorization(policy),
        valid_policy_targets(policy, &nm_ids),
        valid_policy_pacing(policy),
        valid_policy_action_limits(policy),
    ]
    .into_iter()
    .all(|valid| valid)
    {
        return Err(WbAutomationDecisionError::InvalidPolicy);
    }
    Ok(())
}

fn valid_policy_identity(policy: &WbAutomationPolicy) -> bool {
    let account_id_valid = !policy.account_id.is_empty()
        && policy.account_id.len() <= 128
        && policy
            .account_id
            .bytes()
            .all(|byte| matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'));
    let campaign_name_valid = !policy.campaign_name.is_empty()
        && policy.campaign_name.len() <= 128
        && policy.campaign_name.trim() == policy.campaign_name
        && !policy.campaign_name.chars().any(char::is_control);
    policy.policy_version == "wb_ads_robot.v1"
        && policy.payment_type == "cpc"
        && policy.placement == "search"
        && policy.timezone == "Europe/Moscow"
        && account_id_valid
        && campaign_name_valid
        && policy.campaign_id != 0
}

fn valid_policy_authorization(policy: &WbAutomationPolicy) -> bool {
    valid_identifier(&policy.authorized_by_actor_id)
        && !policy.authorization_reference.is_empty()
        && policy.authorization_reference.len() <= 128
        && policy
            .authorization_reference
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'/' | b'.'))
        && policy.authorized_at < policy.observe_until
        && policy.observe_until < policy.authorization_expires_at
}

fn valid_policy_targets(policy: &WbAutomationPolicy, nm_ids: &BTreeSet<u64>) -> bool {
    !policy.nm_ids.is_empty()
        && policy.nm_ids.len() <= 50
        && nm_ids.len() == policy.nm_ids.len()
        && !nm_ids.contains(&0)
}

fn valid_policy_pacing(policy: &WbAutomationPolicy) -> bool {
    let drr_valid = policy.target_drr_basis_points > 0
        && policy.hard_drr_basis_points >= policy.target_drr_basis_points
        && (policy.hard_drr_basis_points != policy.target_drr_basis_points
            || policy.autonomous_pacing.allows_zero_cost_probe())
        && policy.hard_drr_basis_points <= 10_000;
    let frontier_fields_absent = policy.traffic_frontier_bid_kopecks.is_none()
        && policy.traffic_frontier_feedback_timeout_seconds.is_none()
        && policy.traffic_frontier_min_feedback_impressions.is_none()
        && policy.traffic_frontier_min_feedback_clicks.is_none();
    let frontier_valid = if policy.autonomous_pacing.uses_traffic_frontier() {
        valid_traffic_frontier_policy(policy)
    } else {
        frontier_fields_absent
    };
    let marginal_valid = if policy.autonomous_pacing.uses_marginal_feedback() {
        valid_marginal_feedback_policy(policy)
    } else {
        policy.target_orders_per_day == 0
    };
    let v2_valid = policy.autonomous_pacing != WbAutomationPacingMode::TrafficFrontierV2
        || (policy.traffic_frontier_min_feedback_impressions.is_none()
            && policy.traffic_frontier_min_feedback_clicks.is_none());
    drr_valid
        && policy.target_impressions_per_day > 0
        && frontier_valid
        && marginal_valid
        && v2_valid
}

fn valid_policy_action_limits(policy: &WbAutomationPolicy) -> bool {
    policy.min_bid_kopecks > 0
        && policy.max_bid_kopecks >= policy.min_bid_kopecks
        && (1..=25).contains(&policy.bid_step_percent)
        && policy.daily_spend_cap_minor > 0
        && policy.daily_pause_threshold_minor > 0
        && policy.daily_pause_threshold_minor <= policy.daily_spend_cap_minor
        && (1..=50).contains(&policy.max_actions_per_day)
        && (300..=86_400).contains(&policy.cooldown_seconds)
        && policy.no_order_reduce_clicks > 0
        && policy.no_order_disable_clicks > policy.no_order_reduce_clicks
        && policy.no_order_disable_spend_minor > 0
        && policy.efficient_min_orders > 0
        && (1..=10_000).contains(&policy.efficient_min_conversion_basis_points)
        && policy.low_exposure_max_impressions > 0
        && policy.low_exposure_max_clicks > 0
        && !policy.allow_budget_top_up
        && (!policy.bid_writes_enabled || policy.write_enabled)
}

const fn valid_marginal_feedback_policy(policy: &WbAutomationPolicy) -> bool {
    let Some(min_impressions) = policy.traffic_frontier_min_feedback_impressions else {
        return false;
    };
    let Some(min_clicks) = policy.traffic_frontier_min_feedback_clicks else {
        return false;
    };
    min_impressions >= 100
        && min_impressions <= 10_000
        && min_clicks >= 5
        && min_clicks <= 100
        && min_clicks <= min_impressions
        && policy.target_impressions_per_day >= 1_300
        && policy.target_impressions_per_day <= 1_600
        && policy.target_orders_per_day >= 3
        && policy.target_orders_per_day <= 4
        && policy.max_actions_per_day >= 9
        && policy.max_actions_per_day <= 48
        && policy.cooldown_seconds >= 1_800
        && policy.cooldown_seconds <= 3_600
}

const fn valid_traffic_frontier_policy(policy: &WbAutomationPolicy) -> bool {
    let Some(frontier) = policy.traffic_frontier_bid_kopecks else {
        return false;
    };
    let Some(feedback_timeout) = policy.traffic_frontier_feedback_timeout_seconds else {
        return false;
    };
    let emergency_headroom = policy
        .daily_spend_cap_minor
        .saturating_sub(policy.daily_pause_threshold_minor);
    frontier > policy.min_bid_kopecks
        && frontier <= policy.max_bid_kopecks
        && policy.max_bid_kopecks <= emergency_headroom
        && feedback_timeout >= policy.cooldown_seconds
        && feedback_timeout <= 86_400
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn validate_observation<'a>(
    policy: &WbAutomationPolicy,
    observation: &'a WbAutomationObservation,
) -> Result<BTreeMap<u64, &'a WbAutomationSkuObservation>, WbAutomationDecisionError> {
    if observation
        .last_action_at
        .is_some_and(|last_action| last_action > observation.observed_at)
        || observation.attribution_complete && observation.campaign_level_metrics.is_some()
        || [
            observation.campaign_level_metrics.as_ref(),
            observation.current_campaign_metrics.as_ref(),
        ]
        .into_iter()
        .flatten()
        .any(|metrics| metrics.clicks > metrics.impressions)
    {
        return Err(WbAutomationDecisionError::InvalidObservation);
    }
    let mut observations = BTreeMap::new();
    for sku in &observation.skus {
        let vendor_minimum_valid = (1..=policy.max_bid_kopecks).contains(&sku.minimum_bid_kopecks);
        if sku.nm_id == 0
            || !vendor_minimum_valid
            || sku.current_bid_kopecks < policy.min_bid_kopecks
            || sku.clicks > sku.impressions
            || observations.insert(sku.nm_id, sku).is_some()
        {
            return Err(WbAutomationDecisionError::InvalidObservation);
        }
    }
    let expected = policy.nm_ids.iter().copied().collect::<BTreeSet<_>>();
    let actual = observations.keys().copied().collect::<BTreeSet<_>>();
    if expected != actual {
        return Err(WbAutomationDecisionError::InvalidObservation);
    }
    Ok(observations)
}

fn bid_change(
    policy: &WbAutomationPolicy,
    sku: &WbAutomationSkuObservation,
) -> Result<Option<WbAutomationBidChange>, WbAutomationDecisionError> {
    let drr = drr_basis_points(sku)?;
    let reason_and_direction =
        if sku.attributed_orders == 0 && sku.clicks >= policy.no_order_reduce_clicks {
            Some((WbAutomationBidReason::NoOrdersAfterClicks, false))
        } else if drr.is_some_and(|value| value > policy.target_drr_basis_points) {
            Some((WbAutomationBidReason::TargetDrrExceeded, false))
        } else if sku.attributed_orders >= policy.efficient_min_orders
            && conversion_basis_points(sku)? >= policy.efficient_min_conversion_basis_points
            && drr.is_some_and(|value| value <= policy.target_drr_basis_points)
        {
            Some((WbAutomationBidReason::EfficientSales, true))
        } else if sku.impressions < policy.low_exposure_max_impressions
            && sku.clicks < policy.low_exposure_max_clicks
        {
            Some((WbAutomationBidReason::LowExposureExploration, true))
        } else {
            None
        };
    let Some((reason, increase)) = reason_and_direction else {
        return Ok(None);
    };
    let minimum = effective_minimum_bid(policy, sku);
    let to_bid_kopecks = if increase {
        increase_bid(policy, sku.current_bid_kopecks, minimum)?
    } else {
        decrease_bid(policy, sku.current_bid_kopecks, minimum)
    };
    Ok(
        (to_bid_kopecks != sku.current_bid_kopecks).then_some(WbAutomationBidChange {
            nm_id: sku.nm_id,
            from_bid_kopecks: sku.current_bid_kopecks,
            to_bid_kopecks,
            reason,
        }),
    )
}

fn drr_basis_points(
    sku: &WbAutomationSkuObservation,
) -> Result<Option<u32>, WbAutomationDecisionError> {
    drr_basis_points_from(sku.spend_minor, sku.attributed_revenue_minor)
}

fn drr_basis_points_from(
    spend_minor: u64,
    attributed_revenue_minor: u64,
) -> Result<Option<u32>, WbAutomationDecisionError> {
    if attributed_revenue_minor == 0 {
        return Ok(None);
    }
    ratio_basis_points(spend_minor, attributed_revenue_minor).map(Some)
}

fn conversion_basis_points(
    sku: &WbAutomationSkuObservation,
) -> Result<u32, WbAutomationDecisionError> {
    conversion_basis_points_from(sku.attributed_orders, sku.clicks)
}

fn conversion_basis_points_from(
    attributed_orders: u64,
    clicks: u64,
) -> Result<u32, WbAutomationDecisionError> {
    if clicks == 0 {
        return Ok(0);
    }
    ratio_basis_points(attributed_orders, clicks)
}

fn ratio_basis_points(numerator: u64, denominator: u64) -> Result<u32, WbAutomationDecisionError> {
    let value = u128::from(numerator)
        .checked_mul(BASIS_POINTS)
        .ok_or(WbAutomationDecisionError::Overflow)?
        / u128::from(denominator);
    u32::try_from(value).map_err(|_| WbAutomationDecisionError::Overflow)
}

pub(super) fn increase_bid(
    policy: &WbAutomationPolicy,
    current: u64,
    minimum: u64,
) -> Result<u64, WbAutomationDecisionError> {
    let delta = current
        .checked_mul(u64::from(policy.bid_step_percent))
        .ok_or(WbAutomationDecisionError::Overflow)?
        / 100;
    current
        .checked_add(delta.max(1))
        .map(|value| value.max(minimum).min(policy.max_bid_kopecks))
        .ok_or(WbAutomationDecisionError::Overflow)
}

fn decrease_bid(policy: &WbAutomationPolicy, current: u64, minimum: u64) -> u64 {
    let delta = current.saturating_mul(u64::from(policy.bid_step_percent)) / 100;
    current.saturating_sub(delta.max(1)).max(minimum)
}

pub(super) fn effective_minimum_bid(
    policy: &WbAutomationPolicy,
    sku: &WbAutomationSkuObservation,
) -> u64 {
    policy.min_bid_kopecks.max(sku.minimum_bid_kopecks)
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, 0).unwrap()
    }

    fn policy() -> WbAutomationPolicy {
        WbAutomationPolicy {
            policy_version: "wb_ads_robot.v1".to_owned(),
            write_enabled: true,
            bid_writes_enabled: true,
            account_id: "ip_domnyshev_wb".to_owned(),
            campaign_id: 39_682_633,
            campaign_name: "Робот".to_owned(),
            payment_type: "cpc".to_owned(),
            placement: "search".to_owned(),
            timezone: "Europe/Moscow".to_owned(),
            authorized_by_actor_id: "rustam_magasumov".to_owned(),
            authorization_reference: "chat/2026-08-24/safe-auto-robot".to_owned(),
            authorized_at: Utc.with_ymd_and_hms(2026, 8, 24, 7, 0, 0).unwrap(),
            authorization_expires_at: Utc.with_ymd_and_hms(2026, 9, 23, 7, 0, 0).unwrap(),
            nm_ids: vec![449_627_598, 449_627_015, 497_424_314],
            target_drr_basis_points: 1_500,
            hard_drr_basis_points: 2_500,
            target_impressions_per_day: 5_000,
            target_orders_per_day: 0,
            autonomous_pacing: WbAutomationPacingMode::Enabled,
            traffic_frontier_bid_kopecks: None,
            traffic_frontier_feedback_timeout_seconds: None,
            traffic_frontier_min_feedback_impressions: None,
            traffic_frontier_min_feedback_clicks: None,
            min_bid_kopecks: 102,
            max_bid_kopecks: 600,
            bid_step_percent: 15,
            daily_spend_cap_minor: 30_000,
            daily_pause_threshold_minor: 25_000,
            max_actions_per_day: 2,
            cooldown_seconds: 21_600,
            min_sellable_stock: 3,
            no_order_reduce_clicks: 30,
            no_order_disable_clicks: 50,
            no_order_disable_spend_minor: 25_000,
            efficient_min_orders: 2,
            efficient_min_conversion_basis_points: 200,
            low_exposure_max_impressions: 200,
            low_exposure_max_clicks: 10,
            observe_until: now() - Duration::hours(1),
            allow_budget_top_up: false,
        }
    }

    fn sku(nm_id: u64) -> WbAutomationSkuObservation {
        WbAutomationSkuObservation {
            nm_id,
            minimum_bid_kopecks: 102,
            current_bid_kopecks: 200,
            sellable_stock: 10,
            impressions: 500,
            clicks: 20,
            spend_minor: 2_000,
            attributed_orders: 2,
            attributed_revenue_minor: 20_000,
        }
    }

    fn observation() -> WbAutomationObservation {
        WbAutomationObservation {
            observed_at: now(),
            campaign_status: ACTIVE_CAMPAIGN_STATUS,
            paused_by_automation: false,
            budget_remaining_minor: 100_000,
            daily_spend_minor: 10_000,
            daily_spend_complete: true,
            actions_today: 0,
            last_action_at: None,
            attribution_complete: true,
            campaign_level_metrics: None,
            current_campaign_metrics: None,
            skus: policy().nm_ids.into_iter().map(sku).collect(),
        }
    }

    fn campaign_level_observation(metrics: WbAutomationCampaignMetrics) -> WbAutomationObservation {
        let mut input = observation();
        input.attribution_complete = false;
        input.campaign_level_metrics = Some(metrics);
        input.current_campaign_metrics = None;
        for sku in &mut input.skus {
            sku.impressions = 0;
            sku.clicks = 0;
            sku.spend_minor = 0;
            sku.attributed_orders = 0;
            sku.attributed_revenue_minor = 0;
        }
        input
    }

    /// A SKU whose bid already sits at the floor cannot be stopped any further,
    /// because the WB promotion API exposes no per-SKU disable. It must not
    /// abort the run: returning its stop as the decision would freeze bid
    /// management for every healthy SKU in the campaign for as long as the stop
    /// lasts, which for a sold-out SKU is indefinite.
    #[test]
    fn a_floored_stopped_sku_does_not_freeze_the_rest_of_the_campaign() {
        let mut input = observation();
        input.skus[0].sellable_stock = 0;
        input.skus[0].current_bid_kopecks = 102;

        let decision = evaluate_wb_automation(&policy(), &input).unwrap();

        assert_eq!(
            decision.unresolved_stops,
            vec![WbAutomationSkuStop {
                nm_id: 449_627_598,
                reason: WbAutomationDisableReason::LowStock,
            }],
            "the stop that no action can address stays visible in the decision"
        );
        assert_eq!(
            decision.action,
            WbAutomationAction::ChangeBids {
                changes: vec![WbAutomationBidChange {
                    nm_id: 449_627_015,
                    from_bid_kopecks: 200,
                    to_bid_kopecks: 230,
                    reason: WbAutomationBidReason::EfficientSales,
                }],
            },
            "one healthy SKU is managed and the floored SKU is absent"
        );
    }

    /// The same SKU above the floor still yields a stop, and that stop keeps
    /// priority over any bid change, so one action per run remains the rule.
    #[test]
    fn a_stopped_sku_above_the_floor_is_still_lowered_first() {
        let mut input = observation();
        input.skus[0].sellable_stock = 0;

        let decision = evaluate_wb_automation(&policy(), &input).unwrap();

        assert_eq!(
            decision.action,
            WbAutomationAction::DisableSku {
                nm_id: 449_627_598,
                reason: WbAutomationDisableReason::LowStock,
            }
        );
        assert!(
            decision.unresolved_stops.is_empty(),
            "a stop the action addresses is not unresolved"
        );
    }

    /// With every SKU stopped and floored there is nothing left to change, and
    /// the run must say so explicitly rather than silently reporting no action.
    #[test]
    fn every_sku_stopped_at_the_floor_holds_with_all_stops_recorded() {
        let mut input = observation();
        for sku in &mut input.skus {
            sku.sellable_stock = 0;
            sku.current_bid_kopecks = 102;
        }

        let decision = evaluate_wb_automation(&policy(), &input).unwrap();

        assert_eq!(
            decision.action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::NoMaterialChange
            }
        );
        assert_eq!(
            decision
                .unresolved_stops
                .iter()
                .map(|stop| stop.nm_id)
                .collect::<Vec<_>>(),
            vec![449_627_598, 449_627_015, 497_424_314]
        );
    }

    #[test]
    fn observe_only_budget_and_campaign_guards_hold_without_writes() {
        let mut observe_policy = policy();
        observe_policy.observe_until = now() + Duration::hours(1);
        assert_eq!(
            evaluate_wb_automation(&observe_policy, &observation())
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::ObserveOnly
            }
        );

        let mut exhausted = observation();
        exhausted.budget_remaining_minor = 0;
        assert_eq!(
            evaluate_wb_automation(&policy(), &exhausted)
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::BudgetExhausted
            }
        );

        let mut manual_pause = observation();
        manual_pause.campaign_status = PAUSED_CAMPAIGN_STATUS;
        assert_eq!(
            evaluate_wb_automation(&policy(), &manual_pause)
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::CampaignNotActive
            }
        );
    }

    #[test]
    fn daily_cap_pause_has_priority_and_never_auto_resumes() {
        let mut capped = observation();
        capped.daily_spend_minor = 25_000;
        capped.actions_today = policy().max_actions_per_day;
        assert_eq!(
            evaluate_wb_automation(&policy(), &capped).unwrap().action,
            WbAutomationAction::PauseCampaignForDailyCap
        );

        let mut resumable = observation();
        resumable.campaign_status = PAUSED_CAMPAIGN_STATUS;
        resumable.paused_by_automation = true;
        resumable.daily_spend_minor = 0;
        assert_eq!(
            evaluate_wb_automation(&policy(), &resumable)
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::ProtectivePauseRequiresApproval
            }
        );

        let mut protective_only = policy();
        protective_only.bid_writes_enabled = false;
        assert_eq!(
            evaluate_wb_automation(&protective_only, &observation())
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::BidWritesDisabled
            }
        );
        assert_eq!(
            evaluate_wb_automation(&protective_only, &capped)
                .unwrap()
                .action,
            WbAutomationAction::PauseCampaignForDailyCap
        );
    }

    #[test]
    fn low_stock_no_orders_and_hard_drr_disable_one_sku() {
        let mut low_stock = observation();
        low_stock.skus[0].sellable_stock = 3;
        assert_eq!(
            evaluate_wb_automation(&policy(), &low_stock)
                .unwrap()
                .action,
            WbAutomationAction::DisableSku {
                nm_id: 449_627_598,
                reason: WbAutomationDisableReason::LowStock
            }
        );

        let mut no_orders = observation();
        no_orders.skus[0].attributed_orders = 0;
        no_orders.skus[0].attributed_revenue_minor = 0;
        no_orders.skus[0].clicks = 50;
        assert_eq!(
            evaluate_wb_automation(&policy(), &no_orders)
                .unwrap()
                .action,
            WbAutomationAction::DisableSku {
                nm_id: 449_627_598,
                reason: WbAutomationDisableReason::NoOrdersHardStop
            }
        );

        let mut expensive = observation();
        expensive.skus[0].spend_minor = 3_000;
        expensive.skus[0].attributed_revenue_minor = 10_000;
        assert_eq!(
            evaluate_wb_automation(&policy(), &expensive)
                .unwrap()
                .action,
            WbAutomationAction::DisableSku {
                nm_id: 449_627_598,
                reason: WbAutomationDisableReason::HardDrrExceeded
            }
        );
    }

    #[test]
    fn bid_changes_are_bounded_and_keep_per_sku_reasons() {
        let mut input = observation();
        input.skus[0].spend_minor = 4_000;
        input.skus[0].attributed_revenue_minor = 20_000;
        input.skus[1].impressions = 100;
        input.skus[1].clicks = 5;
        input.skus[1].attributed_orders = 0;
        input.skus[1].attributed_revenue_minor = 0;
        input.skus[2].clicks = 30;
        input.skus[2].attributed_orders = 0;
        input.skus[2].attributed_revenue_minor = 0;

        assert_eq!(
            evaluate_wb_automation(&policy(), &input).unwrap().action,
            WbAutomationAction::ChangeBids {
                changes: vec![WbAutomationBidChange {
                    nm_id: 449_627_598,
                    from_bid_kopecks: 200,
                    to_bid_kopecks: 170,
                    reason: WbAutomationBidReason::TargetDrrExceeded,
                }],
            }
        );
    }

    #[test]
    fn vendor_minimum_is_a_dynamic_floor_for_every_bid_direction() {
        let mut decrease = observation();
        decrease.skus[0].minimum_bid_kopecks = 190;
        decrease.skus[0].spend_minor = 4_000;
        decrease.skus[0].attributed_revenue_minor = 20_000;
        assert!(matches!(
            evaluate_wb_automation(&policy(), &decrease).unwrap().action,
            WbAutomationAction::ChangeBids { changes }
                if changes[0].to_bid_kopecks == 190
        ));

        let mut increase = observation();
        increase.skus[0].minimum_bid_kopecks = 250;
        assert!(matches!(
            evaluate_wb_automation(&policy(), &increase).unwrap().action,
            WbAutomationAction::ChangeBids { changes }
                if changes[0].to_bid_kopecks == 250
        ));

        let mut stopped = observation();
        stopped.skus[0].minimum_bid_kopecks = 190;
        stopped.skus[0].current_bid_kopecks = 190;
        stopped.skus[0].sellable_stock = 0;
        let decision = evaluate_wb_automation(&policy(), &stopped).unwrap();
        assert!(matches!(
            decision.action,
            WbAutomationAction::ChangeBids { .. }
        ));
        assert_eq!(decision.unresolved_stops[0].nm_id, 449_627_598);
    }

    #[test]
    fn a_bid_above_a_new_policy_ceiling_is_reduced_before_attribution_logic() {
        let mut tightened = policy();
        tightened.max_bid_kopecks = 300;
        let mut input = campaign_level_observation(WbAutomationCampaignMetrics {
            impressions: 100,
            clicks: 3,
            spend_minor: 300,
            attributed_orders: 0,
            attributed_revenue_minor: 0,
        });
        input.skus[0].current_bid_kopecks = 1_274;

        assert_eq!(
            evaluate_wb_automation(&tightened, &input).unwrap().action,
            WbAutomationAction::ChangeBids {
                changes: vec![WbAutomationBidChange {
                    nm_id: 449_627_598,
                    from_bid_kopecks: 1_274,
                    to_bid_kopecks: 300,
                    reason: WbAutomationBidReason::PolicyMaximumExceeded,
                }],
            }
        );
    }

    #[test]
    fn efficient_sales_increase_and_limits_prevent_extra_actions() {
        let decision = evaluate_wb_automation(&policy(), &observation()).unwrap();
        assert!(matches!(
            decision.action,
            WbAutomationAction::ChangeBids { ref changes }
                if changes.len() == 1 && changes[0].to_bid_kopecks == 230
        ));

        let mut quota = observation();
        quota.actions_today = 2;
        assert_eq!(
            evaluate_wb_automation(&policy(), &quota).unwrap().action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::ActionQuotaExhausted
            }
        );

        let mut cooldown = observation();
        cooldown.last_action_at = Some(now() - Duration::hours(1));
        assert_eq!(
            evaluate_wb_automation(&policy(), &cooldown).unwrap().action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::CooldownActive
            }
        );
    }

    #[test]
    fn campaign_totals_never_select_a_sku_for_exploration() {
        let input = campaign_level_observation(WbAutomationCampaignMetrics {
            impressions: 30,
            clicks: 3,
            spend_minor: 306,
            attributed_orders: 0,
            attributed_revenue_minor: 0,
        });

        assert_eq!(
            evaluate_wb_automation(&policy(), &input).unwrap().action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AttributionIncomplete,
            }
        );
        assert!(input.skus.iter().all(|sku| {
            sku.impressions == 0
                && sku.clicks == 0
                && sku.spend_minor == 0
                && sku.attributed_orders == 0
                && sku.attributed_revenue_minor == 0
        }));
    }

    #[test]
    fn campaign_totals_never_drive_sku_performance_changes() {
        for metrics in [
            WbAutomationCampaignMetrics {
                impressions: 500,
                clicks: 30,
                spend_minor: 2_000,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            },
            WbAutomationCampaignMetrics {
                impressions: 500,
                clicks: 20,
                spend_minor: 3_000,
                attributed_orders: 2,
                attributed_revenue_minor: 10_000,
            },
            WbAutomationCampaignMetrics {
                impressions: 500,
                clicks: 20,
                spend_minor: 2_000,
                attributed_orders: 2,
                attributed_revenue_minor: 20_000,
            },
        ] {
            assert_eq!(
                evaluate_wb_automation(&policy(), &campaign_level_observation(metrics))
                    .unwrap()
                    .action,
                WbAutomationAction::Hold {
                    reason: WbAutomationHoldReason::AttributionIncomplete,
                }
            );
        }
    }

    #[test]
    fn campaign_fallback_holds_on_mixed_bids_and_keeps_stock_guard() {
        let metrics = WbAutomationCampaignMetrics {
            impressions: 30,
            clicks: 3,
            spend_minor: 306,
            attributed_orders: 0,
            attributed_revenue_minor: 0,
        };
        let mut mixed = campaign_level_observation(metrics.clone());
        mixed.skus[0].current_bid_kopecks = 201;
        assert_eq!(
            evaluate_wb_automation(&policy(), &mixed).unwrap().action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AttributionIncomplete
            }
        );

        let mut low_stock = campaign_level_observation(metrics);
        low_stock.skus[0].sellable_stock = policy().min_sellable_stock;
        assert_eq!(
            evaluate_wb_automation(&policy(), &low_stock)
                .unwrap()
                .action,
            WbAutomationAction::DisableSku {
                nm_id: 449_627_598,
                reason: WbAutomationDisableReason::LowStock,
            }
        );

        low_stock.skus[0].current_bid_kopecks = policy().min_bid_kopecks;
        let decision = evaluate_wb_automation(&policy(), &low_stock).unwrap();
        assert_eq!(
            decision.action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::NoMaterialChange,
            }
        );
        assert_eq!(
            decision.unresolved_stops,
            vec![WbAutomationSkuStop {
                nm_id: 449_627_598,
                reason: WbAutomationDisableReason::LowStock,
            }]
        );
    }

    #[test]
    fn campaign_fallback_holds_across_performance_boundaries() {
        let no_orders_hard_stop = campaign_level_observation(WbAutomationCampaignMetrics {
            impressions: 500,
            clicks: 50,
            spend_minor: 5_000,
            attributed_orders: 0,
            attributed_revenue_minor: 0,
        });
        assert_eq!(
            evaluate_wb_automation(&policy(), &no_orders_hard_stop)
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AttributionIncomplete,
            }
        );

        let target_drr = campaign_level_observation(WbAutomationCampaignMetrics {
            impressions: 500,
            clicks: 20,
            spend_minor: 2_000,
            attributed_orders: 2,
            attributed_revenue_minor: 10_000,
        });
        assert_eq!(
            evaluate_wb_automation(&policy(), &target_drr)
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AttributionIncomplete,
            }
        );

        let no_signal = campaign_level_observation(WbAutomationCampaignMetrics {
            impressions: 500,
            clicks: 20,
            spend_minor: 1_000,
            attributed_orders: 1,
            attributed_revenue_minor: 10_000,
        });
        assert_eq!(
            evaluate_wb_automation(&policy(), &no_signal)
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AttributionIncomplete,
            }
        );

        let mut already_floored = no_orders_hard_stop;
        for sku in &mut already_floored.skus {
            sku.current_bid_kopecks = policy().min_bid_kopecks;
        }
        assert_eq!(
            evaluate_wb_automation(&policy(), &already_floored)
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AttributionIncomplete,
            }
        );
    }

    #[test]
    fn malformed_policy_or_observation_fails_closed() {
        let mut invalid_policy = policy();
        invalid_policy.allow_budget_top_up = true;
        assert_eq!(
            evaluate_wb_automation(&invalid_policy, &observation()),
            Err(WbAutomationDecisionError::InvalidPolicy)
        );

        let mut unsafe_pause_threshold = policy();
        unsafe_pause_threshold.daily_pause_threshold_minor = 30_001;
        assert_eq!(
            evaluate_wb_automation(&unsafe_pause_threshold, &observation()),
            Err(WbAutomationDecisionError::InvalidPolicy)
        );

        let mut missing_sku = observation();
        missing_sku.skus.pop();
        assert_eq!(
            evaluate_wb_automation(&policy(), &missing_sku),
            Err(WbAutomationDecisionError::InvalidObservation)
        );

        let mut impossible_clicks = observation();
        impossible_clicks.skus[0].clicks = impossible_clicks.skus[0].impressions + 1;
        assert_eq!(
            evaluate_wb_automation(&policy(), &impossible_clicks),
            Err(WbAutomationDecisionError::InvalidObservation)
        );

        let mut contradictory_scope = campaign_level_observation(WbAutomationCampaignMetrics {
            impressions: 30,
            clicks: 3,
            spend_minor: 306,
            attributed_orders: 0,
            attributed_revenue_minor: 0,
        });
        contradictory_scope.attribution_complete = true;
        assert_eq!(
            evaluate_wb_automation(&policy(), &contradictory_scope),
            Err(WbAutomationDecisionError::InvalidObservation)
        );

        let mut impossible_campaign_clicks = contradictory_scope;
        impossible_campaign_clicks.attribution_complete = false;
        impossible_campaign_clicks
            .campaign_level_metrics
            .as_mut()
            .unwrap()
            .clicks = 31;
        assert_eq!(
            evaluate_wb_automation(&policy(), &impossible_campaign_clicks),
            Err(WbAutomationDecisionError::InvalidObservation)
        );
    }

    #[test]
    fn authorization_attribution_and_no_change_holds_are_explicit() {
        let mut not_active = observation();
        not_active.observed_at = policy().authorized_at - Duration::seconds(1);
        assert_eq!(
            evaluate_wb_automation(&policy(), &not_active)
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AuthorizationNotActive
            }
        );

        let mut expired = observation();
        expired.observed_at = policy().authorization_expires_at;
        assert_eq!(
            evaluate_wb_automation(&policy(), &expired).unwrap().action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AuthorizationExpired
            }
        );

        let mut incomplete = observation();
        incomplete.attribution_complete = false;
        assert_eq!(
            evaluate_wb_automation(&policy(), &incomplete)
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AttributionIncomplete
            }
        );

        let mut unchanged = observation();
        for sku in &mut unchanged.skus {
            sku.attributed_orders = 1;
        }
        assert_eq!(
            evaluate_wb_automation(&policy(), &unchanged)
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::NoMaterialChange
            }
        );
        assert_eq!(validate_wb_automation_policy(&policy()), Ok(()));
    }

    #[test]
    fn future_actions_and_arithmetic_overflow_fail_closed() {
        let mut future_action = observation();
        future_action.last_action_at = Some(now() + Duration::seconds(1));
        assert_eq!(
            evaluate_wb_automation(&policy(), &future_action),
            Err(WbAutomationDecisionError::InvalidObservation)
        );

        let mut zero_click_conversion = observation();
        zero_click_conversion.skus[0].clicks = 0;
        zero_click_conversion.skus[0].impressions = 0;
        assert!(matches!(
            evaluate_wb_automation(&policy(), &zero_click_conversion)
                .unwrap()
                .action,
            WbAutomationAction::ChangeBids { .. }
        ));

        let mut overflow_policy = policy();
        overflow_policy.max_bid_kopecks = u64::MAX;
        let mut overflow = observation();
        overflow.skus[0].current_bid_kopecks = u64::MAX;
        overflow.skus[0].impressions = 0;
        overflow.skus[0].clicks = 0;
        overflow.skus[0].attributed_orders = 0;
        overflow.skus[0].attributed_revenue_minor = 0;
        assert_eq!(
            evaluate_wb_automation(&overflow_policy, &overflow),
            Err(WbAutomationDecisionError::Overflow)
        );

        assert_eq!(
            ratio_basis_points(u64::MAX, 1),
            Err(WbAutomationDecisionError::Overflow)
        );
    }

    #[test]
    fn moscow_business_date_changes_at_21_00_utc() {
        let before = Utc.with_ymd_and_hms(2026, 8, 25, 20, 59, 59).unwrap();
        let after = Utc.with_ymd_and_hms(2026, 8, 25, 21, 0, 0).unwrap();
        assert_eq!(
            wb_automation_business_date(before),
            NaiveDate::from_ymd_opt(2026, 8, 25).unwrap()
        );
        assert_eq!(
            wb_automation_business_date(after),
            NaiveDate::from_ymd_opt(2026, 8, 26).unwrap()
        );
    }

    #[test]
    fn shadow_policy_and_missing_today_spend_block_actions() {
        let mut shadow = policy();
        shadow.write_enabled = false;
        shadow.bid_writes_enabled = false;
        assert_eq!(
            evaluate_wb_automation(&shadow, &observation())
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::PolicyShadowOnly,
            }
        );

        let mut incomplete = observation();
        incomplete.daily_spend_complete = false;
        assert_eq!(
            evaluate_wb_automation(&policy(), &incomplete)
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::SpendDataIncomplete,
            }
        );
    }

    #[test]
    fn robot_v1_policy_is_valid_and_starts_in_shadow_mode() {
        let robot_policy = serde_json::from_str::<WbAutomationPolicy>(include_str!(
            "../../config/wb-automation-robot.json"
        ))
        .unwrap();
        let observed_at = robot_policy.observe_until + Duration::minutes(5);
        let live_skus = [(449_627_598, 10), (449_627_015, 12), (497_424_314, 10)]
            .into_iter()
            .map(|(nm_id, sellable_stock)| WbAutomationSkuObservation {
                nm_id,
                minimum_bid_kopecks: 102,
                current_bid_kopecks: 102,
                sellable_stock,
                impressions: 0,
                clicks: 0,
                spend_minor: 0,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            })
            .collect();
        let live_observation = WbAutomationObservation {
            observed_at,
            campaign_status: ACTIVE_CAMPAIGN_STATUS,
            paused_by_automation: false,
            budget_remaining_minor: 100_000,
            daily_spend_minor: 0,
            daily_spend_complete: true,
            actions_today: 0,
            last_action_at: None,
            attribution_complete: false,
            campaign_level_metrics: None,
            current_campaign_metrics: None,
            skus: live_skus,
        };

        assert_eq!(
            evaluate_wb_automation(&robot_policy, &live_observation)
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::PolicyShadowOnly
            }
        );
        assert!(!robot_policy.write_enabled);
        assert!(!robot_policy.allow_budget_top_up);
    }

    #[test]
    fn traffic_frontier_policy_is_bounded_by_feedback_and_budget_headroom() {
        let traffic = serde_json::from_str::<WbAutomationPolicy>(include_str!(
            "../../config/wb-automation-robot.bid-live.json"
        ))
        .unwrap();
        validate_wb_automation_policy(&traffic).unwrap();

        let mut missing_frontier = traffic.clone();
        missing_frontier.traffic_frontier_bid_kopecks = None;
        assert_eq!(
            validate_wb_automation_policy(&missing_frontier),
            Err(WbAutomationDecisionError::InvalidPolicy)
        );

        let mut missing_timeout = traffic.clone();
        missing_timeout.traffic_frontier_feedback_timeout_seconds = None;
        assert_eq!(
            validate_wb_automation_policy(&missing_timeout),
            Err(WbAutomationDecisionError::InvalidPolicy)
        );

        let mut missing_min_impressions = traffic.clone();
        missing_min_impressions.traffic_frontier_min_feedback_impressions = None;
        assert_eq!(
            validate_wb_automation_policy(&missing_min_impressions),
            Err(WbAutomationDecisionError::InvalidPolicy)
        );

        let mut missing_min_clicks = traffic.clone();
        missing_min_clicks.traffic_frontier_min_feedback_clicks = None;
        assert_eq!(
            validate_wb_automation_policy(&missing_min_clicks),
            Err(WbAutomationDecisionError::InvalidPolicy)
        );

        let mut no_click_headroom = traffic.clone();
        no_click_headroom.max_bid_kopecks = 5_001;
        assert_eq!(
            validate_wb_automation_policy(&no_click_headroom),
            Err(WbAutomationDecisionError::InvalidPolicy)
        );

        let mut stale_feedback = traffic.clone();
        stale_feedback.traffic_frontier_feedback_timeout_seconds = Some(299);
        assert_eq!(
            validate_wb_automation_policy(&stale_feedback),
            Err(WbAutomationDecisionError::InvalidPolicy)
        );

        let mut too_many_actions = traffic;
        too_many_actions.max_actions_per_day = 49;
        assert_eq!(
            validate_wb_automation_policy(&too_many_actions),
            Err(WbAutomationDecisionError::InvalidPolicy)
        );
    }

    #[test]
    fn only_traffic_frontier_v4_accepts_a_single_fifteen_percent_drr_limit() {
        let mut v4 = policy();
        v4.autonomous_pacing = WbAutomationPacingMode::TrafficFrontierV4;
        v4.target_drr_basis_points = 1_500;
        v4.hard_drr_basis_points = 1_500;
        v4.target_impressions_per_day = 1_500;
        v4.target_orders_per_day = 3;
        v4.traffic_frontier_bid_kopecks = Some(700);
        v4.traffic_frontier_feedback_timeout_seconds = Some(1_800);
        v4.traffic_frontier_min_feedback_impressions = Some(200);
        v4.traffic_frontier_min_feedback_clicks = Some(10);
        v4.min_bid_kopecks = 500;
        v4.max_bid_kopecks = 1_050;
        v4.daily_spend_cap_minor = 50_000;
        v4.daily_pause_threshold_minor = 45_000;
        v4.max_actions_per_day = 48;
        v4.cooldown_seconds = 1_800;
        assert!(validate_wb_automation_policy(&v4).is_ok());

        v4.autonomous_pacing = WbAutomationPacingMode::TrafficFrontierV3;
        assert_eq!(
            validate_wb_automation_policy(&v4),
            Err(WbAutomationDecisionError::InvalidPolicy)
        );
    }

    #[test]
    fn oduvanchik_policy_accepts_the_reviewed_half_hour_action_ceiling() {
        let policy = serde_json::from_str::<WbAutomationPolicy>(include_str!(
            "../../config/wb-automation-oduvanchik.bid-live.json"
        ))
        .unwrap();

        validate_wb_automation_policy(&policy).unwrap();
        assert_eq!(policy.max_actions_per_day, 48);
        assert_eq!(policy.cooldown_seconds, 1_800);
        assert_eq!(policy.min_bid_kopecks, 500);
        assert_eq!(policy.max_bid_kopecks, 1_050);
        assert!(!policy.allow_budget_top_up);
    }
}
