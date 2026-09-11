use super::{
    WbAutomationCampaignLease, WbAutomationPostgresError, WbAutomationPostgresStore, to_i64,
    validate_account,
};

impl WbAutomationPostgresStore {
    /// Acquires the same session-level campaign lock used by manual Control
    /// transactions. `None` means another runtime or operator owns the exact
    /// account/campaign boundary and this cycle must safely do nothing.
    pub async fn try_acquire_campaign(
        &self,
        account_id: &str,
        campaign_id: u64,
    ) -> Result<Option<WbAutomationCampaignLease<'_>>, WbAutomationPostgresError> {
        validate_account(account_id)?;
        let campaign_id = to_i64(campaign_id)?;
        let lock_key = format!("wb/{account_id}/{campaign_id}");
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        // Arm Drop before sending SQL: cancellation can arrive after PostgreSQL
        // grants the lock but before this future receives its result.
        let mut lease = WbAutomationCampaignLease {
            client: Some(client),
            account_id: account_id.to_owned(),
            campaign_id,
            lock_key,
        };
        let locked = lease
            .client
            .as_ref()
            .ok_or(WbAutomationPostgresError::Unavailable)?
            .query_one(
                "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                &[&lease.lock_key],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?
            .get::<_, bool>(0);
        if !locked {
            // A confirmed miss owns no lock and can safely reuse the session.
            lease.client.take();
            return Ok(None);
        }
        Ok(Some(lease))
    }
}
