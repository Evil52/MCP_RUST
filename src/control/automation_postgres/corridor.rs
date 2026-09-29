use super::{
    DateTime, PolicyTransition, Sha256, Utc, WbAutomationCampaignLease, WbAutomationPostgresError,
    WbAutomationStateTransitionReceipt,
};
use crate::control::{WbAutomationPolicy, validate_wb_automation_corridor_update};
use sha2::Digest;
use std::fmt::Write as _;

impl WbAutomationCampaignLease<'_> {
    /// Preserve all durable guards and audit the new explicit authorization.
    pub async fn authorize_corridor_policy(
        &mut self,
        source: &WbAutomationPolicy,
        target: &WbAutomationPolicy,
        now: DateTime<Utc>,
    ) -> Result<WbAutomationStateTransitionReceipt, WbAutomationPostgresError> {
        validate_wb_automation_corridor_update(source, target, now)
            .map_err(|_| WbAutomationPostgresError::InvalidInput)?;
        if source.account_id != self.account_id
            || i64::try_from(source.campaign_id).ok() != Some(self.campaign_id)
        {
            return Err(WbAutomationPostgresError::InvalidInput);
        }
        let digest = |policy: &WbAutomationPolicy| {
            serde_json::to_vec(policy)
                .map(|bytes| {
                    let mut value = String::with_capacity(64);
                    for byte in Sha256::digest(bytes) {
                        write!(&mut value, "{byte:02x}").expect("writing a digest to String");
                    }
                    value
                })
                .map_err(|_| WbAutomationPostgresError::InvalidInput)
        };
        self.activate_policy_transition_with_authorization(
            &digest(source)?,
            &digest(target)?,
            PolicyTransition::AuthorizedCorridorAdjusted {
                from_min_bid_kopecks: source.min_bid_kopecks,
                to_min_bid_kopecks: target.min_bid_kopecks,
                from_max_bid_kopecks: source.max_bid_kopecks,
                to_max_bid_kopecks: target.max_bid_kopecks,
            },
            Some(target),
        )
        .await
    }
}
