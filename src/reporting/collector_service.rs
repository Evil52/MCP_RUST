use std::{collections::BTreeMap, fs, path::Path, str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::{Config, config::Host};

use crate::{
    config::{
        AccessRegistry, Marketplace, PerformanceCredentials, RegistrySource, StoreCredentials,
        StoreId,
    },
    ozon::OzonClient,
    ozon_performance::PerformanceClient,
};

use super::{
    collector_plan::{CollectionPlanError, CollectionTarget, build_collection_plan},
    policy::DailyReportPolicy,
    postgres_collector::PostgresSnapshotWriter,
};

const DATABASE_URL_ENV: &str = "REPORT_COLLECTOR_DATABASE_URL";
const POLICY_PATH_ENV: &str = "DAILY_REPORT_POLICY";
const ACCESS_CONFIG_ENV: &str = "MCP_ACCESS_CONFIG";
const MODE_ENV: &str = "REPORT_COLLECTOR_MODE";
const MAX_POLICY_BYTES: u64 = 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const OZON_DRY_RUN_TIMEOUT: Duration = Duration::from_secs(20);
const OZON_SELLER_API_BASE_URL: &str = "https://api-seller.ozon.ru";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportCollectorMode {
    Disabled,
    /// Explicit preparation mode for the Ozon Seller dry-run adapter.
    ///
    /// The binary still refuses to execute collection until its separate
    /// source/persistence implementation is enabled. This mode only permits
    /// constructing a narrowly scoped read-only client for that future step.
    OzonDryRun,
}

/// Credential-free configuration for the initial report collector runtime.
///
/// Marketplace credentials and network adapters are deliberately absent from
/// this phase. The process can validate its exact pilot scope and its
/// least-privilege PostgreSQL writer, but cannot call Ozon or Wildberries.
pub struct ReportCollectorConfig {
    database: Config,
    mode: ReportCollectorMode,
    policy: DailyReportPolicy,
    registry: Arc<AccessRegistry>,
    ozon_dry_run_stores: BTreeMap<StoreId, StoreCredentials>,
    ozon_performance_dry_run_stores: BTreeMap<StoreId, PerformanceCredentials>,
}

impl ReportCollectorConfig {
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let mode = match lookup(MODE_ENV).as_deref().unwrap_or("disabled") {
            "disabled" => ReportCollectorMode::Disabled,
            "ozon_dry_run" => ReportCollectorMode::OzonDryRun,
            _ => bail!("report-collector mode is unsupported"),
        };
        let raw_database =
            lookup(DATABASE_URL_ENV).context("REPORT_COLLECTOR_DATABASE_URL is required")?;
        let mut database = Config::from_str(&raw_database)
            .context("REPORT_COLLECTOR_DATABASE_URL must be a PostgreSQL URL")?;
        validate_database(&database)?;
        database.connect_timeout(CONNECT_TIMEOUT);
        database.application_name("mcp-ozon-report-collector");

        let registry_path = lookup(ACCESS_CONFIG_ENV).context("MCP_ACCESS_CONFIG is required")?;
        let registry = RegistrySource::new(registry_path)
            .context("MCP_ACCESS_CONFIG must contain a valid access registry")?;
        let policy_path = lookup(POLICY_PATH_ENV).context("DAILY_REPORT_POLICY is required")?;
        let policy_bytes = read_bounded_file(Path::new(&policy_path), MAX_POLICY_BYTES)
            .context("DAILY_REPORT_POLICY cannot be read")?;
        let registry = registry
            .load()
            .context("MCP_ACCESS_CONFIG cannot be loaded")?;
        let policy = DailyReportPolicy::from_slice(&policy_bytes, &registry)
            .context("DAILY_REPORT_POLICY is invalid")?;
        let (ozon_dry_run_stores, ozon_performance_dry_run_stores) = match mode {
            ReportCollectorMode::Disabled => (BTreeMap::new(), BTreeMap::new()),
            ReportCollectorMode::OzonDryRun => {
                resolve_ozon_dry_run_stores(&registry, &policy, &mut lookup)?
            }
        };
        Ok(Self {
            database,
            mode,
            policy,
            registry,
            ozon_dry_run_stores,
            ozon_performance_dry_run_stores,
        })
    }

    pub fn mode(&self) -> ReportCollectorMode {
        self.mode
    }

    pub fn policy(&self) -> &DailyReportPolicy {
        &self.policy
    }

    pub fn collection_plan(&self) -> Result<Vec<CollectionTarget>, CollectionPlanError> {
        build_collection_plan(&self.policy, &self.registry)
    }

    pub async fn connect_writer(&self) -> Result<PostgresSnapshotWriter> {
        PostgresSnapshotWriter::connect(&self.database)
            .await
            .context("daily report snapshot writer is unavailable")
    }

    /// Builds a read-only Ozon client containing only accounts selected by
    /// the report policy. It is unavailable in normal disabled mode.
    pub fn ozon_dry_run_client(&self) -> Result<OzonClient> {
        ensure!(
            self.mode == ReportCollectorMode::OzonDryRun && !self.ozon_dry_run_stores.is_empty(),
            "Ozon dry-run credentials are unavailable in disabled mode"
        );
        // These are fixed, previously validated client-builder inputs. A
        // failure here can only be an unrecoverable local reqwest/TLS runtime
        // construction failure; no marketplace request or snapshot write has
        // begun at this point.
        Ok(OzonClient::new(
            OZON_SELLER_API_BASE_URL.to_owned(),
            OZON_DRY_RUN_TIMEOUT,
            self.ozon_dry_run_stores.clone(),
        )
        .expect("fixed Ozon dry-run client configuration is valid"))
    }

    /// Resolves one policy-selected Ozon account to its opaque store identity.
    ///
    /// This is intentionally available only for the explicit manual dry-run
    /// mode. Callers cannot select an arbitrary registry account or borrow a
    /// store binding from a different marketplace.
    pub fn ozon_dry_run_store(&self, account_id: &str) -> Result<StoreId> {
        ensure!(
            self.mode == ReportCollectorMode::OzonDryRun,
            "Ozon dry-run store is unavailable in disabled mode"
        );
        let target = self
            .collection_plan()
            .map_err(|error| anyhow::anyhow!("daily report collection plan is invalid: {error}"))?
            .into_iter()
            .find(|target| {
                target.account_id == account_id
                    && target.marketplace == super::snapshot::Marketplace::Ozon
            })
            .context("Ozon report account is not selected by the policy")?;
        let account = self
            .registry
            .accounts
            .iter()
            .find(|account| {
                account.id == target.account_id && account.marketplace == Marketplace::Ozon
            })
            .context("Ozon report account is unavailable")?;
        let store_id = account
            .ozon
            .as_ref()
            .context("Ozon report binding is unavailable")?
            .store_id
            .clone();
        ensure!(
            self.ozon_dry_run_stores.contains_key(&store_id),
            "Ozon report credentials are unavailable"
        );
        Ok(store_id)
    }

    /// Builds the read-only Performance client for the same policy-selected
    /// Ozon accounts as the Seller dry-run client. It is deliberately unused
    /// until the Performance response contract is separately verified.
    pub fn ozon_performance_dry_run_client(&self) -> Result<PerformanceClient> {
        ensure!(
            self.mode == ReportCollectorMode::OzonDryRun
                && !self.ozon_performance_dry_run_stores.is_empty(),
            "Ozon Performance dry-run credentials are unavailable in disabled mode"
        );
        PerformanceClient::new(
            OZON_DRY_RUN_TIMEOUT,
            self.ozon_performance_dry_run_stores.clone(),
        )
        .map_err(|_| anyhow::anyhow!("Ozon Performance dry-run client cannot be built"))
    }
}

fn resolve_ozon_dry_run_stores(
    registry: &AccessRegistry,
    policy: &DailyReportPolicy,
    lookup: &mut dyn FnMut(&str) -> Option<String>,
) -> Result<(
    BTreeMap<StoreId, StoreCredentials>,
    BTreeMap<StoreId, PerformanceCredentials>,
)> {
    let plan = build_collection_plan(policy, registry)
        .map_err(|error| anyhow::anyhow!("daily report collection plan is invalid: {error}"))?;
    let mut seller_stores = BTreeMap::new();
    let mut performance_stores = BTreeMap::new();
    for target in plan
        .into_iter()
        .filter(|target| target.marketplace == super::snapshot::Marketplace::Ozon)
    {
        let account = registry
            .accounts
            .iter()
            .find(|account| {
                account.id == target.account_id && account.marketplace == Marketplace::Ozon
            })
            .context("Ozon report account is unavailable")?;
        let binding = account
            .ozon
            .as_ref()
            .context("Ozon report binding is unavailable")?;
        let client_id = lookup(&binding.client_id_env)
            .with_context(|| format!("{} is required for Ozon dry-run", binding.client_id_env))?;
        let api_key = lookup(&binding.api_key_env)
            .with_context(|| format!("{} is required for Ozon dry-run", binding.api_key_env))?;
        let performance = binding
            .performance
            .as_ref()
            .context("Ozon Performance report binding is unavailable")?;
        let performance_client_id = lookup(&performance.client_id_env).with_context(|| {
            format!(
                "{} is required for Ozon Performance dry-run",
                performance.client_id_env
            )
        })?;
        let performance_client_secret =
            lookup(&performance.client_secret_env).with_context(|| {
                format!(
                    "{} is required for Ozon Performance dry-run",
                    performance.client_secret_env
                )
            })?;
        ensure!(
            !client_id.is_empty()
                && !api_key.is_empty()
                && !performance_client_id.is_empty()
                && !performance_client_secret.is_empty(),
            "Ozon dry-run credentials must not be empty"
        );
        // AccessRegistry validates store_id uniqueness before this point, so
        // insertion cannot silently replace credentials for another account.
        seller_stores.insert(
            binding.store_id.clone(),
            StoreCredentials { client_id, api_key },
        );
        performance_stores.insert(
            binding.store_id.clone(),
            PerformanceCredentials {
                client_id: performance_client_id,
                client_secret: performance_client_secret,
            },
        );
    }
    ensure!(
        !seller_stores.is_empty() && !performance_stores.is_empty(),
        "the report policy contains no Ozon account for Ozon dry-run"
    );
    Ok((seller_stores, performance_stores))
}

fn validate_database(config: &Config) -> Result<()> {
    ensure!(
        config.get_user() == Some("report_collector")
            && config
                .get_password()
                .is_some_and(|password| !password.is_empty())
            && config.get_dbname().is_some_and(|value| !value.is_empty())
            && config.get_hosts().len() == 1
            && matches!(config.get_hosts(), [Host::Tcp(host)] if !host.trim().is_empty())
            && config.get_options().is_none(),
        "REPORT_COLLECTOR_DATABASE_URL must use the restricted report_collector identity"
    );
    Ok(())
}

fn read_bounded_file(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let metadata = fs::metadata(path)?;
    ensure!(
        metadata.is_file() && metadata.len() <= limit,
        "policy file is invalid"
    );
    let bytes = fs::read(path)?;
    ensure!(bytes.len() as u64 <= limit, "policy file is too large");
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::*;

    static NEXT_FILE: AtomicU64 = AtomicU64::new(1);

    fn file(label: &str, body: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "mcp-ozon-report-collector-{label}-{}",
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, body).unwrap();
        path
    }

    fn entries() -> Vec<(&'static str, String)> {
        let registry = file(
            "registry",
            r#"{"version":1,"actors":[{"id":"diana","name":"Diana","role":"manager","oidc":{"username":"diana"}},{"id":"wb","name":"WB","role":"manager","oidc":{"username":"wb"}}],"accounts":[{"id":"ozon","organization":"Ozon","marketplace":"ozon","seller_client_id":"1","manager_id":"diana","ozon":{"store_id":"1","client_id_env":"ID","api_key_env":"KEY","performance":{"client_id_env":"PERF_ID","client_secret_env":"PERF_SECRET"}}},{"id":"wb","organization":"WB","marketplace":"wildberries","seller_client_id":"2","manager_id":"wb","wildberries":{"api_token_env":"WB_TOKEN"}}]}"#,
        );
        let policy = file(
            "policy",
            r#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"diana","account_ids":["ozon"]}]}]}"#,
        );
        vec![
            (
                DATABASE_URL_ENV,
                "postgresql://report_collector:password@position-db/ozon_positions".to_owned(),
            ),
            (ACCESS_CONFIG_ENV, registry.display().to_string()),
            (POLICY_PATH_ENV, policy.display().to_string()),
        ]
    }

    fn config(entries: &[(&str, String)]) -> Result<ReportCollectorConfig> {
        ReportCollectorConfig::from_lookup(|key| {
            entries
                .iter()
                .find_map(|(entry, value)| (*entry == key).then(|| value.clone()))
        })
    }

    #[test]
    fn disabled_config_has_exact_pilot_plan_and_restricted_database() {
        let config = config(&entries()).unwrap();
        assert_eq!(config.mode(), ReportCollectorMode::Disabled);
        assert!(!config.policy().enabled);
        assert_eq!(config.collection_plan().unwrap().len(), 1);
    }

    #[test]
    fn invalid_mode_database_and_required_files_fail_closed() {
        assert!(config(&[]).is_err());
        for url in [
            "not-a-url",
            "postgresql://report_worker:password@position-db/ozon_positions",
            "postgresql://report_collector@position-db/ozon_positions",
            "postgresql://report_collector:password@/ozon_positions",
            "postgresql://report_collector:password@position-db/ozon_positions?options=-csearch_path%3Dpublic",
        ] {
            let mut values = entries();
            values[0] = (DATABASE_URL_ENV, url.to_owned());
            assert!(config(&values).is_err());
        }
        let mut values = entries();
        values.push((MODE_ENV, "live".to_owned()));
        assert!(config(&values).is_err());
        let mut values = entries();
        values[2] = (POLICY_PATH_ENV, values[1].1.clone());
        assert!(config(&values).is_err());
    }

    #[test]
    fn explicit_ozon_dry_run_resolves_only_the_policy_scoped_seller_binding() {
        let disabled = config(&entries()).unwrap();
        assert!(disabled.ozon_dry_run_client().is_err());

        let mut values = entries();
        values.extend([
            (MODE_ENV, "ozon_dry_run".to_owned()),
            ("ID", "client-id".to_owned()),
            ("KEY", "api-key".to_owned()),
            ("PERF_ID", "performance-client-id".to_owned()),
            ("PERF_SECRET", "performance-client-secret".to_owned()),
            // This unrelated credential is deliberately not resolved: the
            // selected policy contains no WB dry-run source in this phase.
            ("WB_TOKEN", "unrelated-wb-token".to_owned()),
        ]);
        let dry_run = config(&values).unwrap();
        assert_eq!(dry_run.mode(), ReportCollectorMode::OzonDryRun);
        assert!(
            dry_run
                .ozon_dry_run_client()
                .unwrap()
                .is_configured(&StoreId::from("1"))
        );
        assert!(
            dry_run
                .ozon_performance_dry_run_client()
                .unwrap()
                .is_configured(&StoreId::from("1"))
        );
        assert_eq!(
            dry_run.ozon_dry_run_store("ozon").unwrap(),
            StoreId::from("1")
        );
        assert!(dry_run.ozon_dry_run_store("wb").is_err());

        let mixed_policy = file(
            "mixed-policy",
            r#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"diana","account_ids":["ozon"]},{"actor_id":"wb","account_ids":["wb"]}]}]}"#,
        );
        values[2] = (POLICY_PATH_ENV, mixed_policy.display().to_string());
        // The WB plan target is intentionally skipped rather than causing its
        // token to be read by the Ozon-only dry-run client.
        assert!(config(&values).unwrap().ozon_dry_run_client().is_ok());

        let mut missing_id = values.clone();
        missing_id.retain(|(key, _)| *key != "ID");
        assert!(config(&missing_id).is_err());
        values.retain(|(key, _)| *key != "KEY");
        assert!(config(&values).is_err());
    }
}
