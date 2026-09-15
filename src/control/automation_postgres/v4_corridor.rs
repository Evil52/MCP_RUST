use super::{
    PolicyTransition, WbAutomationCampaignLease, WbAutomationPostgresError,
    WbAutomationStateTransitionReceipt,
};

impl WbAutomationCampaignLease<'_> {
    /// Records the narrow v4 7--12 RUB adjustment without resetting any
    /// protective runtime state. Caller-side policy validation binds it to the
    /// two reviewed campaigns and leaves every other setting untouched.
    pub async fn activate_traffic_frontier_v4_corridor_policy(
        &mut self,
        source_policy_digest: &str,
        target_policy_digest: &str,
        from_min_bid_kopecks: u64,
        to_min_bid_kopecks: u64,
        from_max_bid_kopecks: u64,
        to_max_bid_kopecks: u64,
    ) -> Result<WbAutomationStateTransitionReceipt, WbAutomationPostgresError> {
        if !(102..=700).contains(&from_min_bid_kopecks)
            || to_min_bid_kopecks != 700
            || from_max_bid_kopecks != 1_050
            || to_max_bid_kopecks != 1_200
        {
            return Err(WbAutomationPostgresError::InvalidInput);
        }
        self.activate_policy_transition(
            source_policy_digest,
            target_policy_digest,
            PolicyTransition::TrafficFrontierV4CorridorAdjusted {
                from_min_bid_kopecks,
                to_min_bid_kopecks,
                from_max_bid_kopecks,
                to_max_bid_kopecks,
            },
        )
        .await
    }
}
