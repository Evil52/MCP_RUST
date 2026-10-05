use super::{
    BTreeMap, Context, REPORT_EGRESS_PROXY, REPORT_REQUEST_TIMEOUT, ReportCollectorConfig,
    ReportCollectorMode, Result, Utc, WbClient, WbCredentials, ensure, required_secret,
    validate_wb_token_type,
};
use crate::reporting::{
    advertising_history::worker::HistoryClaim,
    snapshot::{Marketplace, SnapshotSource},
};

impl ReportCollectorConfig {
    /// Reads only the account bound to an already acquired archive lease.
    pub fn resolve_wb_history(&self, claim: &HistoryClaim) -> Result<WbClient> {
        ensure!(
            self.mode == ReportCollectorMode::Scheduled && self.policy.enabled,
            "history requires enabled collection"
        );
        ensure!(claim.lease_until() > Utc::now(), "history lease expired");
        ensure!(
            self.collection_plan
                .iter()
                .any(|t| t.account_id == claim.account_id()
                    && t.marketplace == Marketplace::Wildberries
                    && t.sources.contains(&SnapshotSource::Advertising)),
            "history account outside collection policy"
        );
        let account = self
            .registry
            .accounts
            .iter()
            .find(|a| a.id == claim.account_id())
            .context("history account unavailable")?;
        let binding = account
            .wildberries
            .as_ref()
            .context("history WB binding unavailable")?;
        let directory = self
            .credential_directory
            .as_ref()
            .context("history credential directory unavailable")?;
        let token = required_secret(
            &mut |name| directory.read(name),
            &binding.api_token_env,
            "WB advertising history",
        )?;
        validate_wb_token_type(&token, &binding.api_token_env)?;
        WbClient::new_with_https_proxy(
            REPORT_REQUEST_TIMEOUT,
            BTreeMap::from([(account.id.clone(), WbCredentials { token })]),
            REPORT_EGRESS_PROXY,
        )
        .context("history WB read client unavailable")
    }
}
