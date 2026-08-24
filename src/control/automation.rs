use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const ACTIVE_CAMPAIGN_STATUS: i32 = 9;
const PAUSED_CAMPAIGN_STATUS: i32 = 11;
const BASIS_POINTS: u128 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbAutomationPolicy {
    pub account_id: String,
    pub campaign_id: u64,
    pub campaign_name: String,
    pub authorized_by_actor_id: String,
    pub authorization_reference: String,
    pub authorized_at: DateTime<Utc>,
    pub authorization_expires_at: DateTime<Utc>,
    pub nm_ids: Vec<u64>,
    pub target_drr_basis_points: u32,
    pub hard_drr_basis_points: u32,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbAutomationObservation {
    pub observed_at: DateTime<Utc>,
    pub campaign_status: i32,
    pub paused_by_automation: bool,
    pub budget_remaining_minor: u64,
    pub daily_spend_minor: u64,
    pub actions_today: u32,
    pub last_action_at: Option<DateTime<Utc>>,
    pub attribution_complete: bool,
    pub skus: Vec<WbAutomationSkuObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbAutomationSkuObservation {
    pub nm_id: u64,
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
    ObserveOnly,
    AttributionIncomplete,
    CampaignNotActive,
    BudgetExhausted,
    ActionQuotaExhausted,
    CooldownActive,
    NoMaterialChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WbAutomationBidReason {
    TargetDrrExceeded,
    NoOrdersAfterClicks,
    EfficientSales,
    LowExposureExploration,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WbAutomationDisableReason {
    LowStock,
    NoOrdersHardStop,
    HardDrrExceeded,
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
    let decision = |action| WbAutomationDecision {
        account_id: policy.account_id.clone(),
        campaign_id: policy.campaign_id,
        observed_at: observation.observed_at,
        action,
    };

    if observation.observed_at < policy.authorized_at {
        return Ok(decision(WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::AuthorizationNotActive,
        }));
    }
    if observation.observed_at >= policy.authorization_expires_at {
        return Ok(decision(WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::AuthorizationExpired,
        }));
    }
    if observation.observed_at < policy.observe_until {
        return Ok(decision(WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::ObserveOnly,
        }));
    }
    if observation.budget_remaining_minor == 0 {
        return Ok(decision(WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::BudgetExhausted,
        }));
    }
    if observation.campaign_status == ACTIVE_CAMPAIGN_STATUS
        && observation.daily_spend_minor >= policy.daily_pause_threshold_minor
    {
        return Ok(decision(WbAutomationAction::PauseCampaignForDailyCap));
    }
    if observation.campaign_status == PAUSED_CAMPAIGN_STATUS
        && observation.paused_by_automation
        && observation.daily_spend_minor < policy.daily_pause_threshold_minor
    {
        return Ok(decision(WbAutomationAction::ResumeCampaignAfterDailyCap));
    }
    if observation.campaign_status != ACTIVE_CAMPAIGN_STATUS {
        return Ok(decision(WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::CampaignNotActive,
        }));
    }
    if observation.actions_today >= policy.max_actions_per_day {
        return Ok(decision(WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::ActionQuotaExhausted,
        }));
    }
    if observation.last_action_at.is_some_and(|last_action| {
        observation.observed_at
            < last_action + Duration::seconds(i64::from(policy.cooldown_seconds))
    }) {
        return Ok(decision(WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::CooldownActive,
        }));
    }
    if !observation.attribution_complete {
        return Ok(decision(WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::AttributionIncomplete,
        }));
    }

    for nm_id in &policy.nm_ids {
        let sku = observations[nm_id];
        if sku.sellable_stock <= policy.min_sellable_stock {
            return Ok(decision(WbAutomationAction::DisableSku {
                nm_id: *nm_id,
                reason: WbAutomationDisableReason::LowStock,
            }));
        }
        if sku.attributed_orders == 0
            && (sku.clicks >= policy.no_order_disable_clicks
                || sku.spend_minor >= policy.no_order_disable_spend_minor)
        {
            return Ok(decision(WbAutomationAction::DisableSku {
                nm_id: *nm_id,
                reason: WbAutomationDisableReason::NoOrdersHardStop,
            }));
        }
        if drr_basis_points(sku)?.is_some_and(|drr| drr > policy.hard_drr_basis_points) {
            return Ok(decision(WbAutomationAction::DisableSku {
                nm_id: *nm_id,
                reason: WbAutomationDisableReason::HardDrrExceeded,
            }));
        }
    }

    let mut changes = Vec::new();
    for nm_id in &policy.nm_ids {
        let sku = observations[nm_id];
        if let Some(change) = bid_change(policy, sku)? {
            changes.push(change);
        }
    }
    if changes.is_empty() {
        Ok(decision(WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::NoMaterialChange,
        }))
    } else {
        Ok(decision(WbAutomationAction::ChangeBids { changes }))
    }
}

pub fn validate_wb_automation_policy(
    policy: &WbAutomationPolicy,
) -> Result<(), WbAutomationDecisionError> {
    validate_policy(policy)
}

fn validate_policy(policy: &WbAutomationPolicy) -> Result<(), WbAutomationDecisionError> {
    let identifiers_valid = !policy.account_id.is_empty()
        && policy.account_id.len() <= 128
        && policy
            .account_id
            .bytes()
            .all(|byte| matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'));
    let campaign_name_valid = !policy.campaign_name.is_empty()
        && policy.campaign_name.len() <= 128
        && policy.campaign_name.trim() == policy.campaign_name
        && !policy.campaign_name.chars().any(char::is_control);
    let authorization_valid = valid_identifier(&policy.authorized_by_actor_id)
        && !policy.authorization_reference.is_empty()
        && policy.authorization_reference.len() <= 128
        && policy
            .authorization_reference
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'/' | b'.'))
        && policy.authorized_at < policy.observe_until
        && policy.observe_until < policy.authorization_expires_at;
    let nm_ids = policy.nm_ids.iter().copied().collect::<BTreeSet<_>>();
    if !identifiers_valid
        || !campaign_name_valid
        || !authorization_valid
        || policy.campaign_id == 0
        || policy.nm_ids.is_empty()
        || policy.nm_ids.len() > 50
        || nm_ids.len() != policy.nm_ids.len()
        || nm_ids.contains(&0)
        || policy.target_drr_basis_points == 0
        || policy.hard_drr_basis_points <= policy.target_drr_basis_points
        || policy.hard_drr_basis_points > 10_000
        || policy.min_bid_kopecks == 0
        || policy.max_bid_kopecks < policy.min_bid_kopecks
        || !(1..=25).contains(&policy.bid_step_percent)
        || policy.daily_spend_cap_minor == 0
        || policy.daily_pause_threshold_minor == 0
        || policy.daily_pause_threshold_minor > policy.daily_spend_cap_minor
        || !(1..=24).contains(&policy.max_actions_per_day)
        || !(300..=86_400).contains(&policy.cooldown_seconds)
        || policy.no_order_reduce_clicks == 0
        || policy.no_order_disable_clicks <= policy.no_order_reduce_clicks
        || policy.no_order_disable_spend_minor == 0
        || policy.efficient_min_orders == 0
        || policy.efficient_min_conversion_basis_points == 0
        || policy.efficient_min_conversion_basis_points > 10_000
        || policy.low_exposure_max_impressions == 0
        || policy.low_exposure_max_clicks == 0
        || policy.allow_budget_top_up
    {
        return Err(WbAutomationDecisionError::InvalidPolicy);
    }
    Ok(())
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
    {
        return Err(WbAutomationDecisionError::InvalidObservation);
    }
    let mut observations = BTreeMap::new();
    for sku in &observation.skus {
        if sku.nm_id == 0
            || !(policy.min_bid_kopecks..=policy.max_bid_kopecks).contains(&sku.current_bid_kopecks)
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
    let to_bid_kopecks = if increase {
        increase_bid(policy, sku.current_bid_kopecks)?
    } else {
        decrease_bid(policy, sku.current_bid_kopecks)
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
    if sku.attributed_revenue_minor == 0 {
        return Ok(None);
    }
    ratio_basis_points(sku.spend_minor, sku.attributed_revenue_minor).map(Some)
}

fn conversion_basis_points(
    sku: &WbAutomationSkuObservation,
) -> Result<u32, WbAutomationDecisionError> {
    if sku.clicks == 0 {
        return Ok(0);
    }
    ratio_basis_points(sku.attributed_orders, sku.clicks)
}

fn ratio_basis_points(numerator: u64, denominator: u64) -> Result<u32, WbAutomationDecisionError> {
    let value = u128::from(numerator)
        .checked_mul(BASIS_POINTS)
        .ok_or(WbAutomationDecisionError::Overflow)?
        / u128::from(denominator);
    u32::try_from(value).map_err(|_| WbAutomationDecisionError::Overflow)
}

fn increase_bid(
    policy: &WbAutomationPolicy,
    current: u64,
) -> Result<u64, WbAutomationDecisionError> {
    let delta = current
        .checked_mul(u64::from(policy.bid_step_percent))
        .ok_or(WbAutomationDecisionError::Overflow)?
        / 100;
    current
        .checked_add(delta.max(1))
        .map(|value| value.min(policy.max_bid_kopecks))
        .ok_or(WbAutomationDecisionError::Overflow)
}

fn decrease_bid(policy: &WbAutomationPolicy, current: u64) -> u64 {
    let delta = current.saturating_mul(u64::from(policy.bid_step_percent)) / 100;
    current
        .saturating_sub(delta.max(1))
        .max(policy.min_bid_kopecks)
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
            account_id: "ip_domnyshev_wb".to_owned(),
            campaign_id: 39_682_633,
            campaign_name: "Робот".to_owned(),
            authorized_by_actor_id: "rustam_magasumov".to_owned(),
            authorization_reference: "chat/2026-08-24/safe-auto-robot".to_owned(),
            authorized_at: Utc.with_ymd_and_hms(2026, 8, 24, 7, 0, 0).unwrap(),
            authorization_expires_at: Utc.with_ymd_and_hms(2026, 9, 23, 7, 0, 0).unwrap(),
            nm_ids: vec![449_627_598, 449_627_015, 497_424_314],
            target_drr_basis_points: 1_500,
            hard_drr_basis_points: 2_500,
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
            no_order_disable_spend_minor: 15_000,
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
            actions_today: 0,
            last_action_at: None,
            attribution_complete: true,
            skus: policy().nm_ids.into_iter().map(sku).collect(),
        }
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
    fn daily_cap_pause_and_next_day_resume_have_priority() {
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
            WbAutomationAction::ResumeCampaignAfterDailyCap
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

        let WbAutomationAction::ChangeBids { changes } =
            evaluate_wb_automation(&policy(), &input).unwrap().action
        else {
            panic!("expected bid changes");
        };
        assert_eq!(changes.len(), 3);
        assert_eq!(
            changes
                .iter()
                .map(|change| (
                    change.nm_id,
                    change.from_bid_kopecks,
                    change.to_bid_kopecks,
                    change.reason.clone()
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    449_627_598,
                    200,
                    170,
                    WbAutomationBidReason::TargetDrrExceeded
                ),
                (
                    449_627_015,
                    200,
                    230,
                    WbAutomationBidReason::LowExposureExploration
                ),
                (
                    497_424_314,
                    200,
                    170,
                    WbAutomationBidReason::NoOrdersAfterClicks
                ),
            ]
        );
    }

    #[test]
    fn efficient_sales_increase_and_limits_prevent_extra_actions() {
        let WbAutomationAction::ChangeBids { changes } =
            evaluate_wb_automation(&policy(), &observation())
                .unwrap()
                .action
        else {
            panic!("expected efficient increases");
        };
        assert!(changes.iter().all(|change| change.to_bid_kopecks == 230));

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
    }

    #[test]
    fn robot_policy_is_valid_and_starts_in_observe_only_mode() {
        let robot_policy = serde_json::from_str::<WbAutomationPolicy>(include_str!(
            "../../config/wb-automation-robot.json"
        ))
        .unwrap();
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 24, 7, 3, 27).unwrap();
        let live_skus = [(449_627_598, 10), (449_627_015, 12), (497_424_314, 10)]
            .into_iter()
            .map(|(nm_id, sellable_stock)| WbAutomationSkuObservation {
                nm_id,
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
            actions_today: 0,
            last_action_at: None,
            attribution_complete: false,
            skus: live_skus,
        };

        assert_eq!(
            evaluate_wb_automation(&robot_policy, &live_observation)
                .unwrap()
                .action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::ObserveOnly
            }
        );
        assert!(!robot_policy.allow_budget_top_up);
    }
}
