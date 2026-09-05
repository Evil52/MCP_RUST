use std::sync::Arc;

use rmcp::handler::server::common::{AsRequestContext, FromContextPart};

use crate::{
    auth::AuthenticatedActor,
    config::{AccessRegistry, Actor, MarketplaceAccount},
    control::{
        ozon::OzonCampaignPlan,
        plan::{WbActionQuota, WbControlPlan},
        policy::{ControlPolicy, OzonCampaignLaunchTargetPolicy, WbPromotionBidTargetPolicy},
        wb::prepare_changes,
    },
};

use super::{ACCESS_DENIED, WbControlServices};

pub(super) fn ozon_plan_target<'a>(
    policy: &'a ControlPolicy,
    plan: &OzonCampaignPlan,
) -> Option<&'a OzonCampaignLaunchTargetPolicy> {
    policy
        .actor_policy(&plan.actor_id)
        .into_iter()
        .flat_map(|actor| &actor.ozon_campaign_launch_targets)
        .find(|target| {
            target.account_id == plan.account_id
                && target.skus.as_slice() == [plan.sku]
                && target.weekly_budget_microrubles == plan.manifest.spec.weekly_budget_microrubles
                && target.per_sku_spend_cap_microrubles
                    == plan.manifest.spec.per_sku_spend_cap_microrubles
                && target.initial_cpc_bid_microrubles
                    == plan.manifest.spec.initial_cpc_bid_microrubles
                && target.max_cpc_bid_microrubles == plan.manifest.spec.max_cpc_bid_microrubles
                && target.target_drr_percent == plan.manifest.spec.target_drr_percent
                && target.target_position == plan.manifest.spec.target_position
        })
}

pub(super) fn authorize_ozon_plan_approval(
    policy: &ControlPolicy,
    registry: &AccessRegistry,
    approver: &Actor,
    plan: &OzonCampaignPlan,
) -> Result<(), String> {
    if plan.schema_version != policy.version
        || plan.policy_revision != policy.revision
        || plan.policy_digest != policy.digest()
    {
        return Err("CONTROL_POLICY_CHANGED: Ozon plan больше не соответствует policy".to_owned());
    }
    let Some(target) = ozon_plan_target(policy, plan) else {
        return Err("CONTROL_POLICY_CHANGED: Ozon target отсутствует".to_owned());
    };
    let Some(account) = registry
        .accounts
        .iter()
        .find(|account| account.id == plan.account_id)
    else {
        return Err(format!(
            "{ACCESS_DENIED}: Ozon account отсутствует в registry"
        ));
    };
    let Ok(plan_actor) = registry.actor(&plan.actor_id) else {
        return Err(format!(
            "{ACCESS_DENIED}: plan actor отсутствует в registry"
        ));
    };
    if approver.id == plan.actor_id
        || !plan_actor.can_access_account(account)
        || !approver.can_access_account(account)
        || !target
            .approver_actor_ids
            .iter()
            .any(|id| id == &approver.id)
    {
        return Err(format!(
            "{ACCESS_DENIED}: actor не делегирован как отдельный Ozon approver"
        ));
    }
    Ok(())
}

pub(super) fn authorize_ozon_plan_apply(
    policy: &ControlPolicy,
    registry: &AccessRegistry,
    actor: &Actor,
    runtime_account_id: &str,
    plan: &OzonCampaignPlan,
) -> Result<(), String> {
    if actor.id != plan.actor_id || runtime_account_id != plan.account_id {
        return Err(format!(
            "{ACCESS_DENIED}: Ozon plan вне actor/runtime scope"
        ));
    }
    let account = registry
        .accounts
        .iter()
        .find(|account| account.id == plan.account_id)
        .ok_or_else(|| format!("{ACCESS_DENIED}: Ozon account отсутствует в registry"))?;
    if !actor.can_access_account(account) {
        return Err(format!("{ACCESS_DENIED}: доступ к Ozon account отозван"));
    }
    let approval = plan
        .approval
        .as_ref()
        .ok_or_else(|| "CONTROL_PLAN_APPROVAL_REQUIRED".to_owned())?;
    let approver = registry
        .actor(&approval.approver_id)
        .map_err(|_| format!("{ACCESS_DENIED}: Ozon approver отсутствует в registry"))?;
    authorize_ozon_plan_approval(policy, registry, approver, plan)
}

#[derive(Debug, Clone, Default)]
pub(super) struct ControlIdentity {
    pub(super) actor_id: Option<String>,
    pub(super) registry: Option<Arc<AccessRegistry>>,
}

impl<C> FromContextPart<C> for ControlIdentity
where
    C: AsRequestContext,
{
    fn from_context_part(context: &mut C) -> Result<Self, rmcp::ErrorData> {
        let context = context.as_request_context();
        let actor_id = context
            .extensions
            .get::<AuthenticatedActor>()
            .or_else(|| {
                context
                    .extensions
                    .get::<axum::http::request::Parts>()
                    .and_then(|parts| parts.extensions.get::<AuthenticatedActor>())
            })
            .map(|actor| actor.actor_id.clone());
        let registry = context
            .extensions
            .get::<Arc<AccessRegistry>>()
            .cloned()
            .or_else(|| {
                context
                    .extensions
                    .get::<axum::http::request::Parts>()
                    .and_then(|parts| parts.extensions.get::<Arc<AccessRegistry>>())
                    .cloned()
            });
        Ok(Self { actor_id, registry })
    }
}

pub(super) fn plan_target<'a>(
    policy: &'a ControlPolicy,
    plan: &WbControlPlan,
) -> Option<&'a WbPromotionBidTargetPolicy> {
    policy
        .actor_policy(&plan.actor_id)
        .into_iter()
        .flat_map(|actor| &actor.wb_promotion_bid_targets)
        .find(|target| target.account_id == plan.account_id && target.advert_id == plan.advert_id)
}

pub(super) fn allowed_plan_target<'a>(
    policy: &'a ControlPolicy,
    plan: &WbControlPlan,
) -> Option<&'a WbPromotionBidTargetPolicy> {
    if plan.schema_version != policy.version
        || plan.policy_revision != policy.revision
        || plan.policy_digest != policy.digest()
    {
        return None;
    }
    let target = plan_target(policy, plan)?;
    let expected_quota = WbActionQuota {
        max_actions_per_hour: target.action_limits.max_actions_per_hour,
        max_actions_per_day: target.action_limits.max_actions_per_day,
        cooldown_seconds: u64::from(target.action_limits.cooldown_seconds),
        max_cumulative_abs_delta_kopecks_per_day: target
            .action_limits
            .max_cumulative_abs_delta_kopecks_per_day,
    };
    (prepare_changes(target, &plan.requested, &plan.before)
        .is_ok_and(|changes| changes == plan.changes)
        && plan.action_quota == expected_quota)
        .then_some(target)
}

pub(super) fn authorize_plan_approval(
    policy: &ControlPolicy,
    registry: &AccessRegistry,
    approver: &Actor,
    plan: &WbControlPlan,
) -> Result<(), String> {
    let target = allowed_plan_target(policy, plan)
        .ok_or_else(|| "CONTROL_POLICY_CHANGED: plan больше не соответствует policy".to_owned())?;
    let account = registry
        .accounts
        .iter()
        .find(|account| account.id == plan.account_id)
        .ok_or_else(|| format!("{ACCESS_DENIED}: WB account отсутствует в registry"))?;
    let plan_actor = registry
        .actor(&plan.actor_id)
        .map_err(|_| format!("{ACCESS_DENIED}: plan actor отсутствует в registry"))?;
    if !plan_actor.can_access_account(account) || !approver.can_access_account(account) {
        return Err(format!(
            "{ACCESS_DENIED}: актуальный доступ к WB account отозван"
        ));
    }
    if approver.id == plan.actor_id
        || !target
            .approver_actor_ids
            .iter()
            .any(|actor_id| actor_id == &approver.id)
    {
        return Err(format!(
            "{ACCESS_DENIED}: actor не делегирован как отдельный approver"
        ));
    }
    Ok(())
}

#[expect(
    clippy::suspicious_operation_groupings,
    reason = "all comparisons independently bind the plan to its actor and runtime scope"
)]
pub(super) fn authorize_plan_account_access<'a>(
    registry: &'a AccessRegistry,
    actor: &Actor,
    services: &WbControlServices,
    plan: &WbControlPlan,
) -> Result<&'a MarketplaceAccount, String> {
    if actor.id != plan.actor_id
        || plan.account_id != services.account_id
        || plan.before.seller_sid != services.seller_sid
    {
        return Err(format!(
            "{ACCESS_DENIED}: plan находится вне runtime/actor scope"
        ));
    }
    let account = registry
        .accounts
        .iter()
        .find(|account| account.id == plan.account_id)
        .ok_or_else(|| format!("{ACCESS_DENIED}: WB account отсутствует в registry"))?;
    if !actor.can_access_account(account) {
        return Err(format!(
            "{ACCESS_DENIED}: актуальный доступ к WB account отозван"
        ));
    }
    if account
        .wildberries
        .as_ref()
        .and_then(|wildberries| wildberries.seller_sid.as_deref())
        != Some(plan.before.seller_sid.as_str())
    {
        return Err(format!(
            "{ACCESS_DENIED}: WB cabinet binding изменился после создания plan"
        ));
    }
    Ok(account)
}

pub(super) fn authorize_plan_apply(
    policy: &ControlPolicy,
    registry: &AccessRegistry,
    actor: &Actor,
    services: &WbControlServices,
    plan: &WbControlPlan,
) -> Result<(), String> {
    let account = authorize_plan_account_access(registry, actor, services, plan)?;
    let target = allowed_plan_target(policy, plan)
        .ok_or_else(|| "CONTROL_POLICY_CHANGED: plan больше не соответствует policy".to_owned())?;
    let approval = plan
        .approval
        .as_ref()
        .ok_or_else(|| "CONTROL_PLAN_APPROVAL_REQUIRED".to_owned())?;
    let approver = registry
        .actor(&approval.approver_id)
        .map_err(|_| format!("{ACCESS_DENIED}: approver отсутствует в registry"))?;
    if approval.approver_id == plan.actor_id
        || !approver.can_access_account(account)
        || !target
            .approver_actor_ids
            .iter()
            .any(|actor_id| actor_id == &approval.approver_id)
    {
        return Err(format!(
            "{ACCESS_DENIED}: актуальная approval delegation отозвана"
        ));
    }
    Ok(())
}
