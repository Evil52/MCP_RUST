use crate::control::policy::ControlMode;

use super::{
    ControlMcp,
    authorization::ControlIdentity,
    contract::{
        BidLimitsResult, ControlScopeResult, ControlStatusResult, ControlTargetResult,
        OzonCampaignLaunchTargetResult, WbPromotionBidTargetResult,
    },
};

impl ControlMcp {
    pub(super) fn status_result(
        &self,
        identity: &ControlIdentity,
    ) -> Result<ControlStatusResult, String> {
        let (_registry, actor) = self.access_context(identity)?;
        let actor_id = actor.id;
        // Ozon writes belong to the separate durable guard. Only local WB
        // executors are reported by this Control process.
        let writer_ready = self
            .wb
            .as_ref()
            .is_some_and(|services| services.writer.is_some());
        let campaign_ready = self
            .wb_campaign
            .as_ref()
            .is_some_and(|runtime| runtime.writes_enabled);
        Ok(ControlStatusResult {
            explicit_policy_binding: self.policy.actor_policy(&actor_id).is_some(),
            actor_id,
            policy_schema_version: self.policy.version,
            policy_revision: self.policy.revision,
            policy_digest: self.policy.digest().to_owned(),
            mode: self.policy.mode,
            write_executor_configured: (writer_ready || campaign_ready)
                && self.policy.mode == ControlMode::Enabled,
            wb_campaign_creation_configured: campaign_ready,
            runtime_gates_required: true,
            credentials_loaded: self.wb.is_some() || self.wb_campaign.is_some(),
            marketplace_egress_enabled: self.wb.is_some() || self.wb_campaign.is_some(),
            persistence_configured: self.wb.is_some()
                || self.wb_campaign.is_some()
                || self.ozon.is_some(),
        })
    }

    pub(super) fn scope_result(
        &self,
        identity: &ControlIdentity,
    ) -> Result<ControlScopeResult, String> {
        let (registry, actor) = self.access_context(identity)?;
        let actor_id = actor.id.clone();
        let targets = self
            .policy
            .actor_policy(&actor_id)
            .into_iter()
            .flat_map(|policy| &policy.targets)
            .filter(|target| {
                registry
                    .accounts
                    .iter()
                    .find(|account| account.id == target.account_id)
                    .is_some_and(|account| actor.can_access_account(account))
            })
            .map(|target| ControlTargetResult {
                account_id: target.account_id.clone(),
                campaign_id: target.campaign_id,
                skus: target.skus.clone(),
                bid_limits: BidLimitsResult {
                    min_minor: target.bid_limits.min_minor,
                    max_minor: target.bid_limits.max_minor,
                    max_delta_percent: target.bid_limits.max_delta_percent,
                },
            })
            .collect();
        let wb_promotion_bid_targets = self
            .policy
            .actor_policy(&actor_id)
            .into_iter()
            .flat_map(|policy| &policy.wb_promotion_bid_targets)
            .filter(|target| {
                registry
                    .accounts
                    .iter()
                    .find(|account| account.id == target.account_id)
                    .is_some_and(|account| actor.can_access_account(account))
            })
            .map(|target| WbPromotionBidTargetResult {
                account_id: target.account_id.clone(),
                seller_sid: target.seller_sid.clone(),
                advert_id: target.advert_id,
                nm_ids: target.nm_ids.clone(),
                placements: target.placements.clone(),
                bid_limits_kopecks: BidLimitsResult {
                    min_minor: target.bid_limits_kopecks.min_minor,
                    max_minor: target.bid_limits_kopecks.max_minor,
                    max_delta_percent: target.bid_limits_kopecks.max_delta_percent,
                },
                approver_actor_ids: target.approver_actor_ids.clone(),
                action_limits: target.action_limits,
            })
            .collect();
        let ozon_campaign_launch_targets = self
            .policy
            .actor_policy(&actor_id)
            .into_iter()
            .flat_map(|policy| &policy.ozon_campaign_launch_targets)
            .filter(|target| {
                registry
                    .accounts
                    .iter()
                    .find(|account| account.id == target.account_id)
                    .is_some_and(|account| actor.can_access_account(account))
            })
            .map(|target| OzonCampaignLaunchTargetResult {
                account_id: target.account_id.clone(),
                skus: target.skus.clone(),
                weekly_budget_microrubles: target.weekly_budget_microrubles,
                per_sku_spend_cap_microrubles: target.per_sku_spend_cap_microrubles,
                initial_cpc_bid_microrubles: target.initial_cpc_bid_microrubles,
                max_cpc_bid_microrubles: target.max_cpc_bid_microrubles,
                target_drr_percent: target.target_drr_percent,
                target_position: target.target_position,
                approver_actor_ids: target.approver_actor_ids.clone(),
            })
            .collect();
        let wb_campaign_account_id = self.wb_campaign.as_ref().and_then(|runtime| {
            (runtime.actor_id == actor_id
                && registry.accounts.iter().any(|account| {
                    account.id == runtime.account_id && actor.can_access_account(account)
                }))
            .then(|| runtime.account_id.clone())
        });
        Ok(ControlScopeResult {
            actor_id,
            policy_schema_version: self.policy.version,
            policy_revision: self.policy.revision,
            policy_digest: self.policy.digest().to_owned(),
            mode: self.policy.mode,
            targets,
            ozon_campaign_launch_targets,
            wb_promotion_bid_targets,
            wb_campaign_account_id,
        })
    }
}
