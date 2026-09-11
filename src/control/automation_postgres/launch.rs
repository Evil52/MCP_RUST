use super::{WbAutomationCampaignLease, WbAutomationPostgresError};
use chrono::{DateTime, Utc};

impl WbAutomationCampaignLease<'_> {
    /// Startup is permitted only after two recent observations by the already
    /// registered robot, under the exact target policy and account lease.
    /// A healthy container or an operator-supplied boolean is not evidence.
    pub(in crate::control) async fn verify_launch_cycles(
        &self,
        digest: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, WbAutomationPostgresError> {
        let client = self
            .client
            .as_ref()
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        let rows = client
            .query(
                "SELECT policy_digest, observed_at FROM wb_automation.cycles \
             WHERE account_id=$1 AND advert_id=$2 ORDER BY observed_at DESC LIMIT 2",
                &[&self.account_id, &self.campaign_id],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        if rows.len() != 2 || rows.iter().any(|row| row.get::<_, &str>(0) != digest) {
            return Ok(false);
        }
        let latest: DateTime<Utc> = rows[0].get(1);
        let preceding: DateTime<Utc> = rows[1].get(1);
        Ok((0..=90).contains(&(now - latest).num_seconds())
            && (240..=420).contains(&(latest - preceding).num_seconds()))
    }
}
