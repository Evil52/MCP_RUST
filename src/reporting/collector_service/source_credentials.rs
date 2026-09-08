use super::{
    BTreeMap, Context, CredentialDirectory, OZON_SELLER_API_BASE_URL, OzonClient,
    PerformanceClient, PerformanceCredentials, REPORT_EGRESS_PROXY, REPORT_REQUEST_TIMEOUT,
    ReportCollectorConfig, ReportCollectorMode, Result, StoreCredentials, StoreId, Utc,
    credential_sha256, ensure, required_secret,
};
use crate::config::MarketplaceAccount;
use crate::reporting::{postgres_collector::SourceJobClaim, snapshot::SnapshotSource};

impl ReportCollectorConfig {
    fn source_account(
        &self,
        claim: &SourceJobClaim,
    ) -> Result<(&MarketplaceAccount, &CredentialDirectory)> {
        ensure!(
            self.mode == ReportCollectorMode::Scheduled && self.policy.enabled,
            "independent collection requires an enabled scheduled policy"
        );
        ensure!(
            claim.credential_claim().lease_until() > Utc::now(),
            "source collection lease expired"
        );
        ensure!(
            self.collection_plan
                .iter()
                .any(|t| t.account_id == claim.account_id()
                    && t.marketplace == claim.marketplace()
                    && t.sources.contains(&claim.source)),
            "source is outside collection policy"
        );
        let account = self
            .registry
            .accounts
            .iter()
            .find(|a| a.id == claim.account_id())
            .context("source account is unavailable")?;
        Ok((
            account,
            self.credential_directory
                .as_ref()
                .context("source credential directory unavailable")?,
        ))
    }

    pub fn source_seller(&self, claim: &SourceJobClaim) -> Result<(OzonClient, StoreId)> {
        ensure!(
            claim.source != SnapshotSource::Advertising,
            "advertising uses a separate credential binding"
        );
        let (account, directory) = self.source_account(claim)?;
        let binding = account
            .ozon
            .as_ref()
            .context("Ozon Seller binding unavailable")?;
        let mut lookup = |name: &str| directory.read(name);
        let credentials = StoreCredentials {
            client_id: required_secret(&mut lookup, &binding.client_id_env, "Seller source")?,
            api_key: required_secret(&mut lookup, &binding.api_key_env, "Seller source")?,
        };
        let client = OzonClient::new_with_https_proxy(
            OZON_SELLER_API_BASE_URL.to_owned(),
            REPORT_REQUEST_TIMEOUT,
            BTreeMap::from([(binding.store_id.clone(), credentials)]),
            REPORT_EGRESS_PROXY,
        )?;
        Ok((client, binding.store_id.clone()))
    }

    pub fn source_performance(
        &self,
        claim: &SourceJobClaim,
    ) -> Result<(PerformanceClient, StoreId)> {
        ensure!(
            claim.source == SnapshotSource::Advertising,
            "Performance is restricted to advertising jobs"
        );
        let (account, directory) = self.source_account(claim)?;
        // Scope/lease validation precedes even cache access. Keep the client's
        // shared OAuth cache and pacing state across page quanta; there are at
        // most 64 policy accounts. Credential rotation requires a restart.
        let mut clients = self
            .performance_source_clients
            .lock()
            .map_err(|_| anyhow::anyhow!("Performance source cache unavailable"))?;
        if let Some(client) = clients.get(claim.account_id()) {
            return Ok(client.clone());
        }
        let ozon = account.ozon.as_ref().context("Ozon binding unavailable")?;
        let binding = ozon
            .performance
            .as_ref()
            .context("Performance binding unavailable")?;
        let mut lookup = |name: &str| directory.read(name);
        let client_id = required_secret(&mut lookup, &binding.client_id_env, "Performance source")?;
        ensure!(
            !self
                .registry
                .has_control_performance_client_id_fingerprint(&credential_sha256(&client_id)),
            "dedicated Control credential cannot be used by reporting"
        );
        let credentials = PerformanceCredentials {
            client_id,
            client_secret: required_secret(
                &mut lookup,
                &binding.client_secret_env,
                "Performance source",
            )?,
        };
        let client = PerformanceClient::new_with_https_proxy(
            REPORT_REQUEST_TIMEOUT,
            BTreeMap::from([(ozon.store_id.clone(), credentials)]),
            REPORT_EGRESS_PROXY,
        )?;
        let result = (client, ozon.store_id.clone());
        clients.insert(claim.account_id().to_owned(), result.clone());
        drop(clients);
        Ok(result)
    }
}
