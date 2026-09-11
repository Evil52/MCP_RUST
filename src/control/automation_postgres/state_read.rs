use super::{
    WbAutomationCampaignLease, WbAutomationDatabaseState, WbAutomationPostgresError, parse_state,
};

impl WbAutomationCampaignLease<'_> {
    pub async fn load_state(
        &self,
    ) -> Result<Option<WbAutomationDatabaseState>, WbAutomationPostgresError> {
        let client = self
            .client
            .as_ref()
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        let row = client
            .query_opt(
                "SELECT account_id, advert_id, policy_digest, business_date, \
                        actions_today, last_action_at, paused_for_daily_cap_on, \
                        pending_idempotency_key, incident_class, revision, \
                        imported_legacy_digest \
                 FROM wb_automation.execution_state \
                 WHERE account_id=$1 AND advert_id=$2",
                &[&self.account_id, &self.campaign_id],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        row.as_ref().map(parse_state).transpose()
    }
}
