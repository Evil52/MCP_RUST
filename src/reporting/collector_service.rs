use std::{collections::BTreeMap, fs, path::Path, str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::{Config, config::Host};

use crate::{
    config::{
        AccessRegistry, Marketplace, PerformanceCredentials, RegistrySource, StoreCredentials,
        StoreId, validate_wb_token_type,
    },
    ozon::OzonClient,
    ozon_performance::PerformanceClient,
    wb::{WbClient, WbCredentials},
};

use super::{
    collector_plan::{CollectionTarget, build_collection_plan},
    policy::DailyReportPolicy,
};

const DATABASE_URL_ENV: &str = "REPORT_COLLECTOR_DATABASE_URL";
const POLICY_PATH_ENV: &str = "DAILY_REPORT_POLICY";
const ACCESS_CONFIG_ENV: &str = "MCP_ACCESS_CONFIG";
const MODE_ENV: &str = "REPORT_COLLECTOR_MODE";
const MAX_POLICY_BYTES: u64 = 1024 * 1024;
// A completed daily snapshot needs bounded read-only Seller and Performance requests.
// Ozon can legitimately take longer than an interactive MCP request, so the
// explicit manual dry-run allows one minute per request. Automatic collection
// is still disabled and the source set is published atomically.
const OZON_DRY_RUN_TIMEOUT: Duration = Duration::from_secs(60);
const OZON_SELLER_API_BASE_URL: &str = "https://api-seller.ozon.ru";
const REPORT_EGRESS_PROXY: &str = "http://ozon-egress:3128";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportCollectorMode {
    Disabled,
    /// Explicit operator-only mode for one atomic Ozon Seller + Performance
    /// canary snapshot. Automatic scheduling remains unavailable.
    OzonDryRun,
    /// Explicit operator-only mode for one atomic Wildberries report canary.
    /// It loads only policy-scoped WB read credentials and never schedules.
    WbDryRun,
}

/// Configuration for the disabled runtime and explicit Ozon canary command.
///
/// Disabled mode never resolves marketplace credentials. Ozon dry-run resolves
/// only the exact marketplace bindings selected by the validated policy and
/// the explicit mode. Credentials for the other marketplace are never loaded.
pub struct ReportCollectorConfig {
    database: Config,
    mode: ReportCollectorMode,
    policy: DailyReportPolicy,
    registry: Arc<AccessRegistry>,
    collection_plan: Vec<CollectionTarget>,
    ozon_dry_run_stores: BTreeMap<StoreId, StoreCredentials>,
    ozon_dry_run_performance: BTreeMap<StoreId, PerformanceCredentials>,
    wb_dry_run_accounts: BTreeMap<String, WbCredentials>,
}

impl ReportCollectorConfig {
    pub fn from_lookup(lookup: &mut dyn FnMut(&str) -> Option<String>) -> Result<Self> {
        let mode = match lookup(MODE_ENV).as_deref().unwrap_or("disabled") {
            "disabled" => ReportCollectorMode::Disabled,
            "ozon_dry_run" => ReportCollectorMode::OzonDryRun,
            "wb_dry_run" => ReportCollectorMode::WbDryRun,
            _ => bail!("report-collector mode is unsupported"),
        };
        let raw_database =
            lookup(DATABASE_URL_ENV).context("REPORT_COLLECTOR_DATABASE_URL is required")?;
        let mut database = Config::from_str(&raw_database)
            .context("REPORT_COLLECTOR_DATABASE_URL must be a PostgreSQL URL")?;
        validate_database(&database)?;
        crate::postgres::harden(&mut database, "mcp-ozon-report-collector");

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
        let collection_plan = build_collection_plan(&policy, &registry)
            .context("daily report collection plan is invalid")?;
        let (ozon_dry_run_stores, ozon_dry_run_performance, wb_dry_run_accounts) = match mode {
            ReportCollectorMode::Disabled => (BTreeMap::new(), BTreeMap::new(), BTreeMap::new()),
            ReportCollectorMode::OzonDryRun => {
                let (seller, performance) =
                    resolve_ozon_dry_run_stores(&registry, &collection_plan, lookup)?;
                (seller, performance, BTreeMap::new())
            }
            ReportCollectorMode::WbDryRun => (
                BTreeMap::new(),
                BTreeMap::new(),
                resolve_wb_dry_run_accounts(&registry, &collection_plan, lookup)?,
            ),
        };
        Ok(Self {
            database,
            mode,
            policy,
            registry,
            collection_plan,
            ozon_dry_run_stores,
            ozon_dry_run_performance,
            wb_dry_run_accounts,
        })
    }

    pub fn mode(&self) -> ReportCollectorMode {
        self.mode
    }

    pub fn policy(&self) -> &DailyReportPolicy {
        &self.policy
    }

    pub fn collection_plan(&self) -> &[CollectionTarget] {
        &self.collection_plan
    }

    pub fn database_config(&self) -> &Config {
        &self.database
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
        Ok(OzonClient::new_with_https_proxy(
            OZON_SELLER_API_BASE_URL.to_owned(),
            OZON_DRY_RUN_TIMEOUT,
            self.ozon_dry_run_stores.clone(),
            REPORT_EGRESS_PROXY,
        )
        .expect("fixed Ozon dry-run client configuration is valid"))
    }

    /// Builds the read-only Ozon Performance client for the exact same
    /// policy-selected stores as the Seller client.
    pub fn ozon_dry_run_performance_client(&self) -> Result<PerformanceClient> {
        ensure!(
            self.mode == ReportCollectorMode::OzonDryRun
                && !self.ozon_dry_run_performance.is_empty(),
            "Ozon Performance dry-run credentials are unavailable in disabled mode"
        );
        PerformanceClient::new_with_https_proxy(
            OZON_DRY_RUN_TIMEOUT,
            self.ozon_dry_run_performance.clone(),
            REPORT_EGRESS_PROXY,
        )
        .context("fixed Ozon Performance dry-run client configuration is invalid")
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
            .collection_plan
            .iter()
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

    /// Builds a WB client containing only policy-selected accounts. It uses
    /// the deployment-owned egress proxy and is unavailable in every other
    /// collector mode.
    pub fn wb_dry_run_client(&self) -> Result<WbClient> {
        ensure!(
            self.mode == ReportCollectorMode::WbDryRun && !self.wb_dry_run_accounts.is_empty(),
            "WB dry-run credentials are unavailable outside WB dry-run mode"
        );
        WbClient::new_with_https_proxy(
            OZON_DRY_RUN_TIMEOUT,
            self.wb_dry_run_accounts.clone(),
            REPORT_EGRESS_PROXY,
        )
        .context("fixed WB dry-run client configuration is invalid")
    }

    /// Resolves a caller-supplied account only after matching the validated
    /// policy plan and the exact WB credential map.
    pub fn wb_dry_run_account(&self, account_id: &str) -> Result<String> {
        ensure!(
            self.mode == ReportCollectorMode::WbDryRun,
            "WB dry-run account is unavailable outside WB dry-run mode"
        );
        ensure!(
            self.collection_plan.iter().any(|target| {
                target.account_id == account_id
                    && target.marketplace == super::snapshot::Marketplace::Wildberries
            }) && self.wb_dry_run_accounts.contains_key(account_id),
            "WB report account is not selected by the policy"
        );
        Ok(account_id.to_owned())
    }
}

fn resolve_ozon_dry_run_stores(
    registry: &AccessRegistry,
    plan: &[CollectionTarget],
    lookup: &mut dyn FnMut(&str) -> Option<String>,
) -> Result<(
    BTreeMap<StoreId, StoreCredentials>,
    BTreeMap<StoreId, PerformanceCredentials>,
)> {
    let mut seller_stores = BTreeMap::new();
    let mut performance_stores = BTreeMap::new();
    for target in plan
        .iter()
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
        ensure!(
            !client_id.is_empty() && !api_key.is_empty(),
            "Ozon dry-run credentials must not be empty"
        );
        let performance = binding
            .performance
            .as_ref()
            .context("Ozon report Performance binding is unavailable")?;
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
            !performance_client_id.is_empty() && !performance_client_secret.is_empty(),
            "Ozon Performance dry-run credentials must not be empty"
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
        !seller_stores.is_empty(),
        "the report policy contains no Ozon account for Ozon dry-run"
    );
    Ok((seller_stores, performance_stores))
}

fn resolve_wb_dry_run_accounts(
    registry: &AccessRegistry,
    plan: &[CollectionTarget],
    lookup: &mut dyn FnMut(&str) -> Option<String>,
) -> Result<BTreeMap<String, WbCredentials>> {
    let mut accounts = BTreeMap::new();
    for target in plan
        .iter()
        .filter(|target| target.marketplace == super::snapshot::Marketplace::Wildberries)
    {
        let account = registry
            .accounts
            .iter()
            .find(|account| {
                account.id == target.account_id && account.marketplace == Marketplace::Wildberries
            })
            .context("WB report account is unavailable")?;
        let binding = account
            .wildberries
            .as_ref()
            .context("WB report binding is unavailable")?;
        let token = lookup(&binding.api_token_env)
            .with_context(|| format!("{} is required for WB dry-run", binding.api_token_env))?;
        validate_wb_token_type(&token, &binding.api_token_env)?;
        accounts.insert(account.id.clone(), WbCredentials { token });
    }
    ensure!(
        !accounts.is_empty(),
        "the report policy contains no WB account for WB dry-run"
    );
    Ok(accounts)
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

    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

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
        ReportCollectorConfig::from_lookup(&mut |key| {
            entries
                .iter()
                .find_map(|(entry, value)| (*entry == key).then(|| value.clone()))
        })
    }

    fn personal_wb_token() -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","typ":"JWT"}"#);
        let claims = URL_SAFE_NO_PAD.encode(br#"{"acc":3}"#);
        let signature = URL_SAFE_NO_PAD.encode([0_u8; 64]);
        format!("{header}.{claims}.{signature}")
    }

    #[test]
    fn disabled_config_has_exact_pilot_plan_and_restricted_database() {
        let config = config(&entries()).unwrap();
        assert_eq!(config.mode(), ReportCollectorMode::Disabled);
        assert!(!config.policy().enabled);
        assert_eq!(config.collection_plan().len(), 1);
        assert_eq!(
            config.database_config().get_user(),
            Some("report_collector")
        );
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
    fn explicit_ozon_dry_run_resolves_only_the_policy_scoped_read_bindings() {
        let disabled = config(&entries()).unwrap();
        assert!(disabled.ozon_dry_run_client().is_err());
        assert!(disabled.ozon_dry_run_performance_client().is_err());

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
                .ozon_dry_run_performance_client()
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
        for key in ["KEY", "PERF_ID", "PERF_SECRET"] {
            let mut missing = values.clone();
            missing.retain(|(entry, _)| *entry != key);
            assert!(config(&missing).is_err(), "missing {key}");
        }
    }

    #[test]
    fn explicit_wb_dry_run_resolves_only_policy_scoped_personal_token() {
        let mixed_policy = file(
            "wb-policy",
            r#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"diana","account_ids":["ozon"]},{"actor_id":"wb","account_ids":["wb"]}]}]}"#,
        );
        let mut values = entries();
        values[2] = (POLICY_PATH_ENV, mixed_policy.display().to_string());
        values.extend([
            (MODE_ENV, "wb_dry_run".to_owned()),
            ("WB_TOKEN", personal_wb_token()),
            // Ozon credentials are deliberately absent: WB mode must not
            // resolve or require bindings for the other marketplace.
        ]);
        let dry_run = config(&values).unwrap();
        assert_eq!(dry_run.mode(), ReportCollectorMode::WbDryRun);
        assert!(dry_run.wb_dry_run_client().unwrap().is_configured("wb"));
        assert_eq!(dry_run.wb_dry_run_account("wb").unwrap(), "wb");
        assert!(dry_run.wb_dry_run_account("ozon").is_err());
        assert!(dry_run.ozon_dry_run_client().is_err());

        let mut missing = values.clone();
        missing.retain(|(key, _)| *key != "WB_TOKEN");
        assert!(config(&missing).is_err());
        let mut wrong_type = values;
        wrong_type
            .iter_mut()
            .find(|(key, _)| *key == "WB_TOKEN")
            .unwrap()
            .1 = {
            let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","typ":"JWT"}"#);
            let claims = URL_SAFE_NO_PAD.encode(br#"{"acc":1}"#);
            let signature = URL_SAFE_NO_PAD.encode([0_u8; 64]);
            format!("{header}.{claims}.{signature}")
        };
        assert!(config(&wrong_type).is_err());

        let disabled = config(&entries()).unwrap();
        assert!(disabled.wb_dry_run_client().is_err());
        assert!(disabled.wb_dry_run_account("wb").is_err());
    }
}
