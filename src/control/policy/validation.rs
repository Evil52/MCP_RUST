use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};

use crate::config::{AccessRegistry, Actor, Marketplace, MarketplaceAccount, is_canonical_uuid};

use super::{
    ActorControlPolicy, ControlPolicy, ControlTargetPolicy, MAX_ACTIONS_PER_DAY,
    MAX_ACTIONS_PER_HOUR, MAX_ACTORS, MAX_APPROVERS_PER_TARGET,
    MAX_CUMULATIVE_ABS_DELTA_KOPECKS_PER_DAY, MAX_IDENTIFIER_BYTES,
    MAX_OZON_LAUNCH_SKUS_PER_TARGET, MAX_OZON_WEEKLY_BUDGET_MICRORUBLES, MAX_SKUS_PER_TARGET,
    MAX_TARGETS_PER_ACTOR, MAX_WB_NM_IDS_PER_TARGET, MAX_WB_SIGNED_ID,
    OzonCampaignLaunchTargetPolicy, WbActionLimits, WbPromotionBidTargetPolicy,
};

pub(super) fn validate_policy(policy: &ControlPolicy, registry: &AccessRegistry) -> Result<()> {
    if policy.version != 1 {
        bail!("control policy version должна быть равна 1");
    }
    if policy.revision == 0 {
        bail!("control policy revision должна быть положительной");
    }
    if policy.actors.len() > MAX_ACTORS {
        bail!("control policy содержит слишком много actor bindings");
    }

    let mut actor_ids = BTreeSet::new();
    for actor_policy in &policy.actors {
        validate_identifier("actor_id", &actor_policy.actor_id)?;
        if !actor_ids.insert(actor_policy.actor_id.as_str()) {
            bail!("control policy содержит повтор actor_id");
        }
        validate_actor_policy(actor_policy, registry)?;
    }
    Ok(())
}

fn validate_actor_policy(
    actor_policy: &ActorControlPolicy,
    registry: &AccessRegistry,
) -> Result<()> {
    let actor = registry.actor(&actor_policy.actor_id)?;
    if actor_policy.targets.len() > MAX_TARGETS_PER_ACTOR {
        bail!("control policy содержит слишком много targets для actor");
    }
    if actor_policy.wb_promotion_bid_targets.len() > MAX_TARGETS_PER_ACTOR {
        bail!("control policy содержит слишком много WB promotion targets для actor");
    }
    if actor_policy.ozon_campaign_launch_targets.len() > MAX_TARGETS_PER_ACTOR {
        bail!("control policy содержит слишком много Ozon campaign launch targets для actor");
    }

    validate_ozon_actor_targets(actor_policy, actor, registry)?;
    validate_ozon_campaign_launch_targets(actor_policy, actor, registry)?;
    validate_wb_actor_targets(actor_policy, actor, registry)
}

fn validate_ozon_campaign_launch_targets(
    actor_policy: &ActorControlPolicy,
    actor: &Actor,
    registry: &AccessRegistry,
) -> Result<()> {
    let mut targets = BTreeSet::new();
    for target in &actor_policy.ozon_campaign_launch_targets {
        validate_identifier("account_id", &target.account_id)?;
        let account = registry
            .accounts
            .iter()
            .find(|account| account.id == target.account_id)
            .with_context(|| {
                format!(
                    "Ozon campaign launch target ссылается на неизвестный account_id {}",
                    target.account_id
                )
            })?;
        if !matches!(account.marketplace, Marketplace::Ozon)
            || account
                .ozon
                .as_ref()
                .and_then(|ozon| ozon.performance.as_ref())
                .is_none()
        {
            bail!("Ozon campaign launch target требует Ozon Performance binding");
        }
        if !actor.can_access_account(account) {
            bail!("actor не имеет базового доступа к Ozon campaign launch account");
        }
        validate_ozon_campaign_launch_target(target, &actor_policy.actor_id, account, registry)?;
        let identity = (target.account_id.as_str(), target.skus.as_slice());
        if !targets.insert(identity) {
            bail!("control policy содержит повтор Ozon campaign launch target");
        }
    }
    Ok(())
}

fn validate_ozon_campaign_launch_target(
    target: &OzonCampaignLaunchTargetPolicy,
    plan_actor_id: &str,
    account: &MarketplaceAccount,
    registry: &AccessRegistry,
) -> Result<()> {
    if target.skus.is_empty() || target.skus.len() > MAX_OZON_LAUNCH_SKUS_PER_TARGET {
        bail!(
            "Ozon campaign launch target должен содержать от 1 до {MAX_OZON_LAUNCH_SKUS_PER_TARGET} SKU"
        );
    }
    let mut skus = BTreeSet::new();
    if target
        .skus
        .iter()
        .any(|sku| *sku == 0 || !skus.insert(*sku))
    {
        bail!("Ozon campaign launch SKU должны быть положительными и уникальными");
    }
    if target.weekly_budget_microrubles == 0
        || target.weekly_budget_microrubles > MAX_OZON_WEEKLY_BUDGET_MICRORUBLES
        || target.per_sku_spend_cap_microrubles == 0
        || target
            .per_sku_spend_cap_microrubles
            .checked_mul(target.skus.len() as u64)
            != Some(target.weekly_budget_microrubles)
    {
        bail!("Ozon campaign budget должен быть положительным и точно делиться по SKU");
    }
    if !(10..=100).contains(&target.target_drr_percent) {
        bail!("Ozon target_drr_percent должен быть от 10 до 100");
    }
    if target.initial_cpc_bid_microrubles == 0
        || target.initial_cpc_bid_microrubles > target.max_cpc_bid_microrubles
        || target.max_cpc_bid_microrubles > 1_000_000_000
    {
        bail!("Ozon CPC bid range должен быть положительным и не выше 1 000 RUB");
    }
    if !(1..=30).contains(&target.target_position) {
        bail!("Ozon target_position должен быть от 1 до 30");
    }
    validate_ozon_approvers(target, plan_actor_id, account, registry)
}

fn validate_ozon_approvers(
    target: &OzonCampaignLaunchTargetPolicy,
    plan_actor_id: &str,
    account: &MarketplaceAccount,
    registry: &AccessRegistry,
) -> Result<()> {
    if target.approver_actor_ids.is_empty()
        || target.approver_actor_ids.len() > MAX_APPROVERS_PER_TARGET
    {
        bail!("Ozon approver_actor_ids должны содержать от 1 до {MAX_APPROVERS_PER_TARGET} actor");
    }
    let mut approvers = BTreeSet::new();
    for approver_id in &target.approver_actor_ids {
        validate_identifier("approver_actor_id", approver_id)?;
        if approver_id == plan_actor_id {
            bail!("Ozon plan actor не может approve собственный план");
        }
        if !approvers.insert(approver_id.as_str()) {
            bail!("Ozon approver_actor_ids должны быть уникальными");
        }
        let approver = registry
            .actor(approver_id)
            .with_context(|| format!("неизвестный Ozon approver actor {approver_id}"))?;
        if !approver.can_access_account(account) {
            bail!("Ozon approver actor не имеет базового доступа к account");
        }
    }
    Ok(())
}

fn validate_ozon_actor_targets(
    actor_policy: &ActorControlPolicy,
    actor: &Actor,
    registry: &AccessRegistry,
) -> Result<()> {
    let mut targets = BTreeSet::new();
    for target in &actor_policy.targets {
        validate_identifier("account_id", &target.account_id)?;
        if target.campaign_id == 0 {
            bail!("campaign_id должен быть положительным");
        }
        if !targets.insert((target.account_id.as_str(), target.campaign_id)) {
            bail!("control policy содержит повтор account_id/campaign_id");
        }
        let account = registry
            .accounts
            .iter()
            .find(|account| account.id == target.account_id)
            .with_context(|| {
                format!(
                    "control policy ссылается на неизвестный account_id {}",
                    target.account_id
                )
            })?;
        if !matches!(account.marketplace, Marketplace::Ozon)
            || account
                .ozon
                .as_ref()
                .and_then(|ozon| ozon.performance.as_ref())
                .is_none()
        {
            bail!("control policy target должен ссылаться на Ozon account с Performance binding");
        }
        if !actor.can_access_account(account) {
            bail!("actor не имеет базового доступа к account из control policy");
        }
        validate_target(target)?;
    }
    Ok(())
}

fn validate_wb_actor_targets(
    actor_policy: &ActorControlPolicy,
    actor: &Actor,
    registry: &AccessRegistry,
) -> Result<()> {
    let mut wb_targets = BTreeSet::new();
    for target in &actor_policy.wb_promotion_bid_targets {
        validate_identifier("account_id", &target.account_id)?;
        if target.advert_id == 0 || target.advert_id > MAX_WB_SIGNED_ID {
            bail!("advert_id WB должен быть положительным int64");
        }
        if !wb_targets.insert((target.account_id.as_str(), target.advert_id)) {
            bail!("control policy содержит повтор WB account_id/advert_id");
        }
        let account = registry
            .accounts
            .iter()
            .find(|account| account.id == target.account_id)
            .with_context(|| {
                format!(
                    "control policy ссылается на неизвестный WB account_id {}",
                    target.account_id
                )
            })?;
        validate_wb_account_binding(target, account)?;
        if !actor.can_access_account(account) {
            bail!("actor не имеет базового доступа к WB account из control policy");
        }
        validate_wb_target(target, &actor_policy.actor_id, account, registry)?;
    }
    Ok(())
}

fn validate_wb_account_binding(
    target: &WbPromotionBidTargetPolicy,
    account: &MarketplaceAccount,
) -> Result<()> {
    if !matches!(account.marketplace, Marketplace::Wildberries) || account.wildberries.is_none() {
        bail!("WB promotion target должен ссылаться на Wildberries account с binding");
    }
    let registry_seller_sid = account
        .wildberries
        .as_ref()
        .and_then(|wildberries| wildberries.seller_sid.as_deref())
        .context("WB promotion target требует reviewed seller_sid в access registry")?;
    if !is_canonical_uuid(&target.seller_sid) || target.seller_sid != registry_seller_sid {
        bail!("WB promotion target seller_sid не совпадает с access registry");
    }
    Ok(())
}

fn validate_identifier(field: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_IDENTIFIER_BYTES
        || value.bytes().any(
            |byte| !matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.'),
        )
    {
        bail!("{field} имеет недопустимый формат");
    }
    Ok(())
}

fn validate_target(target: &ControlTargetPolicy) -> Result<()> {
    if target.skus.is_empty() || target.skus.len() > MAX_SKUS_PER_TARGET {
        bail!("target должен содержать от 1 до {MAX_SKUS_PER_TARGET} SKU");
    }
    let mut skus = BTreeSet::new();
    if target
        .skus
        .iter()
        .any(|sku| *sku == 0 || !skus.insert(*sku))
    {
        bail!("SKU должны быть положительными и уникальными");
    }
    if target.bid_limits.min_minor == 0 || target.bid_limits.max_minor < target.bid_limits.min_minor
    {
        bail!("bid_limits должны задавать положительный диапазон min_minor..=max_minor");
    }
    if !(1..=100).contains(&target.bid_limits.max_delta_percent) {
        bail!("max_delta_percent должен быть от 1 до 100");
    }
    Ok(())
}

fn validate_wb_target(
    target: &WbPromotionBidTargetPolicy,
    plan_actor_id: &str,
    account: &MarketplaceAccount,
    registry: &AccessRegistry,
) -> Result<()> {
    validate_wb_nm_ids(target)?;
    validate_wb_placements(target)?;
    validate_wb_bid_limits(target)?;
    validate_wb_approvers(target, plan_actor_id, account, registry)?;
    validate_wb_action_limits(target.action_limits)
}

fn validate_wb_nm_ids(target: &WbPromotionBidTargetPolicy) -> Result<()> {
    if target.nm_ids.is_empty() || target.nm_ids.len() > MAX_WB_NM_IDS_PER_TARGET {
        bail!("WB promotion target должен содержать от 1 до {MAX_WB_NM_IDS_PER_TARGET} nm_id");
    }
    let mut nm_ids = BTreeSet::new();
    if target
        .nm_ids
        .iter()
        .any(|nm_id| *nm_id == 0 || *nm_id > MAX_WB_SIGNED_ID || !nm_ids.insert(*nm_id))
    {
        bail!("WB nm_ids должны быть положительными уникальными int64");
    }
    Ok(())
}

fn validate_wb_placements(target: &WbPromotionBidTargetPolicy) -> Result<()> {
    if target.placements.is_empty() || target.placements.len() > 3 {
        bail!("WB placements должны содержать от 1 до 3 значений");
    }
    let placements = target.placements.iter().copied().collect::<BTreeSet<_>>();
    if placements.len() != target.placements.len() {
        bail!("WB placements должны быть уникальными");
    }
    Ok(())
}

fn validate_wb_bid_limits(target: &WbPromotionBidTargetPolicy) -> Result<()> {
    if target.bid_limits_kopecks.min_minor == 0
        || target.bid_limits_kopecks.max_minor < target.bid_limits_kopecks.min_minor
        || target.bid_limits_kopecks.max_minor > MAX_WB_SIGNED_ID
    {
        bail!("WB bid_limits_kopecks должны задавать положительный диапазон int64");
    }
    if !(1..=100).contains(&target.bid_limits_kopecks.max_delta_percent) {
        bail!("WB max_delta_percent должен быть от 1 до 100");
    }
    Ok(())
}

fn validate_wb_approvers(
    target: &WbPromotionBidTargetPolicy,
    plan_actor_id: &str,
    account: &MarketplaceAccount,
    registry: &AccessRegistry,
) -> Result<()> {
    if target.approver_actor_ids.is_empty()
        || target.approver_actor_ids.len() > MAX_APPROVERS_PER_TARGET
    {
        bail!("WB approver_actor_ids должны содержать от 1 до {MAX_APPROVERS_PER_TARGET} actor");
    }
    let mut approvers = BTreeSet::new();
    for approver_id in &target.approver_actor_ids {
        validate_identifier("approver_actor_id", approver_id)?;
        if approver_id == plan_actor_id {
            bail!("WB plan actor не может approve собственный план");
        }
        if !approvers.insert(approver_id.as_str()) {
            bail!("WB approver_actor_ids должны быть уникальными");
        }
        let approver = registry
            .actor(approver_id)
            .with_context(|| format!("неизвестный WB approver actor {approver_id}"))?;
        if !approver.can_access_account(account) {
            bail!("WB approver actor не имеет базового доступа к account");
        }
    }
    Ok(())
}

fn validate_wb_action_limits(limits: WbActionLimits) -> Result<()> {
    if !(1..=MAX_ACTIONS_PER_HOUR).contains(&limits.max_actions_per_hour) {
        bail!("WB max_actions_per_hour должен быть от 1 до {MAX_ACTIONS_PER_HOUR}");
    }
    if limits.max_actions_per_day < limits.max_actions_per_hour
        || limits.max_actions_per_day > MAX_ACTIONS_PER_DAY
    {
        bail!(
            "WB max_actions_per_day должен быть не меньше hourly и не больше {MAX_ACTIONS_PER_DAY}"
        );
    }
    if !(30..=86_400).contains(&limits.cooldown_seconds) {
        bail!("WB cooldown_seconds должен быть от 30 до 86400");
    }
    if limits.max_cumulative_abs_delta_kopecks_per_day == 0
        || limits.max_cumulative_abs_delta_kopecks_per_day
            > MAX_CUMULATIVE_ABS_DELTA_KOPECKS_PER_DAY
    {
        bail!("WB cumulative bid delta limit должен быть положительным int64");
    }
    Ok(())
}
