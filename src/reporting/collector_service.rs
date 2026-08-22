use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
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
    postgres_collector::CollectionClaim,
};

const DATABASE_URL_ENV: &str = "REPORT_COLLECTOR_DATABASE_URL";
const POLICY_PATH_ENV: &str = "DAILY_REPORT_POLICY";
const ACCESS_CONFIG_ENV: &str = "MCP_ACCESS_CONFIG";
const MODE_ENV: &str = "REPORT_COLLECTOR_MODE";
const CREDENTIAL_DIRECTORY_ENV: &str = "REPORT_COLLECTOR_CREDENTIAL_DIR";
const MAX_POLICY_BYTES: u64 = 1024 * 1024;
const MAX_CREDENTIAL_FILE_BYTES: u64 = 16_386;
// A completed daily snapshot needs bounded read-only Seller and Performance
// requests. Ozon can legitimately take longer than an interactive MCP request,
// so both canary and opt-in scheduled collection allow one minute per request.
// Every complete source set is still published atomically.
const REPORT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
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
    /// Explicit scheduled collection mode. The caller must invoke
    /// `collect-due` or `run-scheduler`; startup alone never performs I/O.
    Scheduled,
}

struct CredentialDirectory {
    root: PathBuf,
}

impl CredentialDirectory {
    fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let metadata = fs::symlink_metadata(root)
            .context("report collector credential directory is unavailable")?;
        ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "report collector credential directory is invalid"
        );
        let root = root
            .canonicalize()
            .context("report collector credential directory is invalid")?;
        Ok(Self { root })
    }

    fn read(&self, name: &str) -> Option<String> {
        if !is_valid_credential_name(name) {
            return None;
        }
        let path = self.root.join(name);
        let metadata = fs::symlink_metadata(&path).ok()?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() > MAX_CREDENTIAL_FILE_BYTES
        {
            return None;
        }
        let canonical = path.canonicalize().ok()?;
        (canonical.parent() == Some(self.root.as_path())).then_some(())?;
        let bytes = fs::read(canonical).ok()?;
        (bytes.len() as u64 <= MAX_CREDENTIAL_FILE_BYTES).then_some(())?;
        let value = String::from_utf8(bytes).ok()?;
        let value = value.trim_end_matches(['\r', '\n']);
        (!value.is_empty()).then(|| value.to_owned())
    }
}

fn is_valid_credential_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value.as_bytes()[0].is_ascii_digit()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

/// Configuration for the disabled runtime, explicit marketplace canaries and
/// the opt-in one-shot scheduled collector.
///
/// Startup loads only registry metadata and validates the optional credential
/// directory itself. A scheduled collection reads individual credential files
/// only after the exact account's database lease is acquired; credentials for
/// other accounts and marketplaces are not read.
pub struct ReportCollectorConfig {
    database: Config,
    mode: ReportCollectorMode,
    policy: DailyReportPolicy,
    registry: Arc<AccessRegistry>,
    collection_plan: Vec<CollectionTarget>,
    credential_directory: Option<CredentialDirectory>,
}

impl ReportCollectorConfig {
    pub fn from_lookup(lookup: &mut dyn FnMut(&str) -> Option<String>) -> Result<Self> {
        let mode = match lookup(MODE_ENV).as_deref().unwrap_or("disabled") {
            "disabled" => ReportCollectorMode::Disabled,
            "ozon_dry_run" => ReportCollectorMode::OzonDryRun,
            "wb_dry_run" => ReportCollectorMode::WbDryRun,
            "scheduled" => ReportCollectorMode::Scheduled,
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
        let credential_directory = lookup(CREDENTIAL_DIRECTORY_ENV)
            .map(CredentialDirectory::open)
            .transpose()?;
        ensure!(
            matches!(
                (mode, policy.enabled, credential_directory.is_some()),
                (ReportCollectorMode::Disabled, false, false)
                    | (
                        ReportCollectorMode::OzonDryRun | ReportCollectorMode::WbDryRun,
                        false,
                        false | true
                    )
                    | (ReportCollectorMode::Scheduled, true, true)
            ),
            "report collector mode, policy and credential directory are inconsistent"
        );
        Ok(Self {
            database,
            mode,
            policy,
            registry,
            collection_plan,
            credential_directory,
        })
    }

    #[must_use]
    pub fn mode(&self) -> ReportCollectorMode {
        self.mode
    }

    #[must_use]
    pub fn policy(&self) -> &DailyReportPolicy {
        &self.policy
    }

    #[must_use]
    pub fn collection_plan(&self) -> &[CollectionTarget] {
        &self.collection_plan
    }

    #[must_use]
    pub fn database_config(&self) -> &Config {
        &self.database
    }

    /// Resolves the exact claimed Ozon account after the caller has acquired
    /// its database lease. Startup and busy/completed claims therefore never
    /// read Seller or Performance secret values.
    pub fn resolve_ozon_dry_run(
        &self,
        claim: &CollectionClaim,
        lookup: &mut dyn FnMut(&str) -> Option<String>,
    ) -> Result<(OzonClient, PerformanceClient, StoreId)> {
        ensure!(
            self.mode == ReportCollectorMode::OzonDryRun,
            "Ozon dry-run credentials are unavailable outside Ozon dry-run mode"
        );
        ensure!(
            !self.policy.enabled,
            "Ozon dry-run credentials require a disabled daily report policy"
        );
        if let Some(directory) = &self.credential_directory {
            self.resolve_ozon_claim(claim, &mut |name| directory.read(name))
        } else {
            self.resolve_ozon_claim(claim, lookup)
        }
    }

    /// Resolves the exact Ozon account for an enabled scheduled run. The
    /// database lease must already belong to the caller; values come from the
    /// validated read-only credential directory rather than process variables.
    pub fn resolve_ozon_scheduled(
        &self,
        claim: &CollectionClaim,
    ) -> Result<(OzonClient, PerformanceClient, StoreId)> {
        ensure!(
            self.mode == ReportCollectorMode::Scheduled && self.policy.enabled,
            "scheduled Ozon credentials require scheduled mode and an enabled policy"
        );
        let directory = self
            .credential_directory
            .as_ref()
            .context("scheduled credential directory is unavailable")?;
        self.resolve_ozon_claim(claim, &mut |name| directory.read(name))
    }

    fn resolve_ozon_claim(
        &self,
        claim: &CollectionClaim,
        lookup: &mut dyn FnMut(&str) -> Option<String>,
    ) -> Result<(OzonClient, PerformanceClient, StoreId)> {
        ensure!(
            claim.lease_until() > Utc::now(),
            "collection claim has expired"
        );
        let target = self
            .collection_plan
            .iter()
            .find(|target| {
                target.account_id == claim.account_id()
                    && target.marketplace == claim.marketplace()
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
        let binding = account
            .ozon
            .as_ref()
            .context("Ozon report binding is unavailable")?;
        let client_id = required_secret(lookup, &binding.client_id_env, "Ozon report collection")?;
        let api_key = required_secret(lookup, &binding.api_key_env, "Ozon report collection")?;
        let performance = binding
            .performance
            .as_ref()
            .context("Ozon report Performance binding is unavailable")?;
        let performance_client_id = required_secret(
            lookup,
            &performance.client_id_env,
            "Ozon Performance report collection",
        )?;
        let performance_client_secret = required_secret(
            lookup,
            &performance.client_secret_env,
            "Ozon Performance report collection",
        )?;
        let store_id = binding.store_id.clone();
        let seller_stores =
            BTreeMap::from([(store_id.clone(), StoreCredentials { client_id, api_key })]);
        let performance_stores = BTreeMap::from([(
            store_id.clone(),
            PerformanceCredentials {
                client_id: performance_client_id,
                client_secret: performance_client_secret,
            },
        )]);
        let seller = OzonClient::new_with_https_proxy(
            OZON_SELLER_API_BASE_URL.to_owned(),
            REPORT_REQUEST_TIMEOUT,
            seller_stores,
            REPORT_EGRESS_PROXY,
        )
        .context("fixed Ozon report client configuration is invalid")?;
        let performance = PerformanceClient::new_with_https_proxy(
            REPORT_REQUEST_TIMEOUT,
            performance_stores,
            REPORT_EGRESS_PROXY,
        )
        .context("fixed Ozon Performance report client configuration is invalid")?;
        Ok((seller, performance, store_id))
    }

    /// Resolves only the exact claimed WB account. No other manager's token is
    /// read, and no token is read during startup, healthcheck or a lost claim.
    pub fn resolve_wb_dry_run(
        &self,
        claim: &CollectionClaim,
        lookup: &mut dyn FnMut(&str) -> Option<String>,
    ) -> Result<(WbClient, String)> {
        ensure!(
            self.mode == ReportCollectorMode::WbDryRun,
            "WB dry-run credentials are unavailable outside WB dry-run mode"
        );
        ensure!(
            !self.policy.enabled,
            "WB dry-run credentials require a disabled daily report policy"
        );
        if let Some(directory) = &self.credential_directory {
            self.resolve_wb_claim(claim, &mut |name| directory.read(name))
        } else {
            self.resolve_wb_claim(claim, lookup)
        }
    }

    /// Resolves the exact WB account for an enabled scheduled run. The database
    /// lease must already belong to the caller; the token comes from the
    /// validated read-only credential directory rather than process variables.
    pub fn resolve_wb_scheduled(&self, claim: &CollectionClaim) -> Result<(WbClient, String)> {
        ensure!(
            self.mode == ReportCollectorMode::Scheduled && self.policy.enabled,
            "scheduled WB credentials require scheduled mode and an enabled policy"
        );
        let directory = self
            .credential_directory
            .as_ref()
            .context("scheduled credential directory is unavailable")?;
        self.resolve_wb_claim(claim, &mut |name| directory.read(name))
    }

    fn resolve_wb_claim(
        &self,
        claim: &CollectionClaim,
        lookup: &mut dyn FnMut(&str) -> Option<String>,
    ) -> Result<(WbClient, String)> {
        ensure!(
            claim.lease_until() > Utc::now(),
            "collection claim has expired"
        );
        let target = self
            .collection_plan
            .iter()
            .find(|target| {
                target.account_id == claim.account_id()
                    && target.marketplace == claim.marketplace()
                    && target.marketplace == super::snapshot::Marketplace::Wildberries
            })
            .context("WB report account is not selected by the policy")?;
        let account = self
            .registry
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
        let token = required_secret(lookup, &binding.api_token_env, "WB report collection")?;
        validate_wb_token_type(&token, &binding.api_token_env)?;
        let accounts = BTreeMap::from([(account.id.clone(), WbCredentials { token })]);
        let client =
            WbClient::new_with_https_proxy(REPORT_REQUEST_TIMEOUT, accounts, REPORT_EGRESS_PROXY)
                .context("fixed WB report client configuration is invalid")?;
        Ok((client, account.id.clone()))
    }
}

fn required_secret(
    lookup: &mut dyn FnMut(&str) -> Option<String>,
    env_name: &str,
    purpose: &str,
) -> Result<String> {
    let value =
        lookup(env_name).with_context(|| format!("{env_name} is required for {purpose}"))?;
    ensure!(!value.is_empty(), "{purpose} credential must not be empty");
    Ok(value)
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
            "mcp-ozon-report-collector-{label}-{}-{}",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, body).unwrap();
        path
    }

    fn directory(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "mcp-ozon-report-collector-{label}-{}-{}",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
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

    fn claim(
        account_id: &str,
        marketplace: super::super::snapshot::Marketplace,
    ) -> CollectionClaim {
        CollectionClaim::for_test(
            account_id,
            marketplace,
            Utc::now() + Duration::from_secs(60),
        )
    }

    fn unexpected_secret_lookup(_: &str) -> Option<String> {
        panic!("secret lookup must not occur before account admission")
    }

    #[test]
    #[should_panic(expected = "secret lookup must not occur before account admission")]
    fn unexpected_secret_lookup_is_a_failing_test_sentinel() {
        let _ = unexpected_secret_lookup("UNEXPECTED_SECRET");
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
        let mut no_secrets = unexpected_secret_lookup;
        let ozon_claim = claim("ozon", super::super::snapshot::Marketplace::Ozon);
        assert!(
            disabled
                .resolve_ozon_dry_run(&ozon_claim, &mut no_secrets)
                .is_err()
        );

        let mut values = entries();
        values.push((MODE_ENV, "ozon_dry_run".to_owned()));
        let mut startup_keys = Vec::new();
        let dry_run = ReportCollectorConfig::from_lookup(&mut |key| {
            startup_keys.push(key.to_owned());
            values
                .iter()
                .find_map(|(entry, value)| (*entry == key).then(|| value.clone()))
        })
        .unwrap();
        assert_eq!(dry_run.mode(), ReportCollectorMode::OzonDryRun);
        assert!(
            ["ID", "KEY", "PERF_ID", "PERF_SECRET", "WB_TOKEN"]
                .into_iter()
                .all(|key| !startup_keys.iter().any(|requested| requested == key))
        );

        let secrets = [
            ("ID", "client-id".to_owned()),
            ("KEY", "api-key".to_owned()),
            ("PERF_ID", "performance-client-id".to_owned()),
            ("PERF_SECRET", "performance-client-secret".to_owned()),
            ("WB_TOKEN", "unrelated-wb-token".to_owned()),
        ];
        let mut resolved_keys = Vec::new();
        let (seller, performance, store_id) = dry_run
            .resolve_ozon_dry_run(&ozon_claim, &mut |key| {
                resolved_keys.push(key.to_owned());
                secrets
                    .iter()
                    .find_map(|(entry, value)| (*entry == key).then(|| value.clone()))
            })
            .unwrap();
        assert!(
            seller.is_configured(&StoreId::from("1"))
                && performance.is_configured(&StoreId::from("1"))
        );
        assert_eq!(store_id, StoreId::from("1"));
        assert_eq!(resolved_keys, ["ID", "KEY", "PERF_ID", "PERF_SECRET"]);
        assert!(
            dry_run
                .resolve_ozon_dry_run(
                    &claim("wb", super::super::snapshot::Marketplace::Wildberries),
                    &mut unexpected_secret_lookup,
                )
                .is_err()
        );

        let mixed_policy = file(
            "mixed-policy",
            r#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"diana","account_ids":["ozon"]},{"actor_id":"wb","account_ids":["wb"]}]}]}"#,
        );
        values[2] = (POLICY_PATH_ENV, mixed_policy.display().to_string());
        let mixed = config(&values).unwrap();
        let mut requested = Vec::new();
        assert!(
            mixed
                .resolve_ozon_dry_run(&ozon_claim, &mut |key| {
                    requested.push(key.to_owned());
                    secrets
                        .iter()
                        .find_map(|(entry, value)| (*entry == key).then(|| value.clone()))
                })
                .is_ok()
        );
        assert!(!requested.iter().any(|key| key == "WB_TOKEN"));

        for missing_key in ["ID", "KEY", "PERF_ID", "PERF_SECRET"] {
            assert!(
                mixed
                    .resolve_ozon_dry_run(&ozon_claim, &mut |key| {
                        (key != missing_key).then(|| "present".to_owned())
                    })
                    .is_err(),
                "missing {missing_key}"
            );
        }
        assert!(
            mixed
                .resolve_ozon_dry_run(&ozon_claim, &mut |_| Some(String::new()))
                .is_err()
        );
        let expired_claim = CollectionClaim::for_test(
            "ozon",
            super::super::snapshot::Marketplace::Ozon,
            Utc::now() - Duration::from_secs(1),
        );
        assert!(
            mixed
                .resolve_ozon_dry_run(&expired_claim, &mut unexpected_secret_lookup)
                .is_err()
        );
    }

    #[test]
    fn explicit_wb_dry_run_resolves_only_policy_scoped_personal_token() {
        let mixed_policy = file(
            "wb-policy",
            r#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"diana","account_ids":["ozon"]},{"actor_id":"wb","account_ids":["wb"]}]}]}"#,
        );
        let mut values = entries();
        values[2] = (POLICY_PATH_ENV, mixed_policy.display().to_string());
        values.push((MODE_ENV, "wb_dry_run".to_owned()));
        let dry_run = config(&values).unwrap();
        assert_eq!(dry_run.mode(), ReportCollectorMode::WbDryRun);
        let token = personal_wb_token();
        let mut resolved_keys = Vec::new();
        let wb_claim = claim("wb", super::super::snapshot::Marketplace::Wildberries);
        let (client, account_id) = dry_run
            .resolve_wb_dry_run(&wb_claim, &mut |key| {
                resolved_keys.push(key.to_owned());
                (key == "WB_TOKEN").then(|| token.clone())
            })
            .unwrap();
        assert!(client.is_configured("wb"));
        assert_eq!(account_id, "wb");
        assert_eq!(resolved_keys, ["WB_TOKEN"]);

        let ozon_claim = claim("ozon", super::super::snapshot::Marketplace::Ozon);
        assert!(
            dry_run
                .resolve_wb_dry_run(&ozon_claim, &mut unexpected_secret_lookup)
                .is_err()
        );
        assert!(
            dry_run
                .resolve_wb_dry_run(&wb_claim, &mut |_| None)
                .is_err()
        );
        let wrong_type = {
            let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","typ":"JWT"}"#);
            let claims = URL_SAFE_NO_PAD.encode(br#"{"acc":1}"#);
            let signature = URL_SAFE_NO_PAD.encode([0_u8; 64]);
            format!("{header}.{claims}.{signature}")
        };
        assert!(
            dry_run
                .resolve_wb_dry_run(&wb_claim, &mut |_| Some(wrong_type.clone()))
                .is_err()
        );

        let disabled = config(&entries()).unwrap();
        let mut no_secrets = unexpected_secret_lookup;
        assert!(
            disabled
                .resolve_wb_dry_run(&wb_claim, &mut no_secrets)
                .is_err()
        );
    }

    #[test]
    fn dry_runs_can_resolve_only_the_claimed_account_from_a_credential_directory() {
        let mixed_policy = file(
            "directory-canary-policy",
            r#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"diana","account_ids":["ozon"]},{"actor_id":"wb","account_ids":["wb"]}]}]}"#,
        );
        let credential_directory = directory("canary-credentials");
        for (name, value) in [
            ("ID", "client-id".to_owned()),
            ("KEY", "api-key".to_owned()),
            ("PERF_ID", "performance-client-id".to_owned()),
            ("PERF_SECRET", "performance-client-secret".to_owned()),
            ("WB_TOKEN", personal_wb_token()),
        ] {
            fs::write(credential_directory.join(name), value).unwrap();
        }

        let mut values = entries();
        values[2] = (POLICY_PATH_ENV, mixed_policy.display().to_string());
        values.extend([
            (MODE_ENV, "ozon_dry_run".to_owned()),
            (
                CREDENTIAL_DIRECTORY_ENV,
                credential_directory.display().to_string(),
            ),
        ]);
        let ozon = config(&values).unwrap();
        let (seller, performance, store) = ozon
            .resolve_ozon_dry_run(
                &claim("ozon", super::super::snapshot::Marketplace::Ozon),
                &mut unexpected_secret_lookup,
            )
            .unwrap();
        assert!(seller.is_configured(&store) && performance.is_configured(&store));

        values.retain(|(key, _)| *key != MODE_ENV);
        values.push((MODE_ENV, "wb_dry_run".to_owned()));
        let wb = config(&values).unwrap();
        let (client, account) = wb
            .resolve_wb_dry_run(
                &claim("wb", super::super::snapshot::Marketplace::Wildberries),
                &mut unexpected_secret_lookup,
            )
            .unwrap();
        assert_eq!(account, "wb");
        assert!(client.is_configured("wb"));

        fs::remove_file(credential_directory.join("WB_TOKEN")).unwrap();
        assert!(
            wb.resolve_wb_dry_run(
                &claim("wb", super::super::snapshot::Marketplace::Wildberries),
                &mut unexpected_secret_lookup,
            )
            .is_err()
        );
    }

    #[test]
    fn scheduled_mode_requires_enabled_policy_and_resolves_only_claimed_account() {
        let enabled_policy = file(
            "scheduled-policy",
            r#"{"version":1,"enabled":true,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"diana","account_ids":["ozon"]},{"actor_id":"wb","account_ids":["wb"]}]}]}"#,
        );
        let mut values = entries();
        values[2] = (POLICY_PATH_ENV, enabled_policy.display().to_string());
        values.push((MODE_ENV, "scheduled".to_owned()));
        let credential_directory = directory("scheduled-credentials");
        for (name, value) in [
            ("ID", "client-id"),
            ("KEY", "api-key"),
            ("PERF_ID", "performance-client-id"),
            ("PERF_SECRET", "performance-client-secret"),
        ] {
            fs::write(credential_directory.join(name), format!("{value}\n")).unwrap();
        }
        values.push((
            CREDENTIAL_DIRECTORY_ENV,
            credential_directory.display().to_string(),
        ));
        let mut startup_keys = Vec::new();
        let scheduled = ReportCollectorConfig::from_lookup(&mut |key| {
            startup_keys.push(key.to_owned());
            values
                .iter()
                .find_map(|(entry, value)| (*entry == key).then(|| value.clone()))
        })
        .unwrap();
        assert_eq!(scheduled.mode(), ReportCollectorMode::Scheduled);
        assert!(scheduled.policy().enabled);
        assert!(
            ["ID", "KEY", "PERF_ID", "PERF_SECRET", "WB_TOKEN"]
                .into_iter()
                .all(|key| !startup_keys.iter().any(|requested| requested == key))
        );

        assert!(
            scheduled
                .resolve_ozon_scheduled(&claim("ozon", super::super::snapshot::Marketplace::Ozon))
                .is_ok()
        );

        fs::write(credential_directory.join("WB_TOKEN"), personal_wb_token()).unwrap();
        assert!(
            scheduled
                .resolve_wb_scheduled(&claim(
                    "wb",
                    super::super::snapshot::Marketplace::Wildberries
                ))
                .is_ok()
        );
        assert!(
            scheduled
                .resolve_wb_scheduled(&claim("ozon", super::super::snapshot::Marketplace::Ozon))
                .is_err()
        );

        let mut disabled_policy_values = entries();
        disabled_policy_values.push((MODE_ENV, "scheduled".to_owned()));
        assert!(config(&disabled_policy_values).is_err());

        let mut missing_directory = values.clone();
        missing_directory.retain(|(key, _)| *key != CREDENTIAL_DIRECTORY_ENV);
        assert!(config(&missing_directory).is_err());

        let mut unexpected_directory = entries();
        unexpected_directory.push((
            CREDENTIAL_DIRECTORY_ENV,
            credential_directory.display().to_string(),
        ));
        assert!(config(&unexpected_directory).is_err());
    }

    #[test]
    fn credential_directory_is_bounded_exact_and_never_follows_symlinks() {
        let root = directory("credential-boundaries");
        let directory = CredentialDirectory::open(&root).unwrap();
        fs::write(root.join("TOKEN"), "secret\r\n").unwrap();
        assert_eq!(directory.read("TOKEN").as_deref(), Some("secret"));
        for invalid in [
            "",
            "1TOKEN",
            "../TOKEN",
            "lowercase",
            "HAS-DASH",
            &"A".repeat(129),
        ] {
            assert!(directory.read(invalid).is_none(), "{invalid}");
        }
        assert!(directory.read("MISSING").is_none());
        fs::write(root.join("EMPTY"), "\r\n").unwrap();
        assert!(directory.read("EMPTY").is_none());
        fs::write(root.join("BINARY"), [0xff]).unwrap();
        assert!(directory.read("BINARY").is_none());
        fs::write(
            root.join("OVERSIZED"),
            vec![b'x'; MAX_CREDENTIAL_FILE_BYTES as usize + 1],
        )
        .unwrap();
        assert!(directory.read("OVERSIZED").is_none());
        fs::create_dir(root.join("DIRECTORY")).unwrap();
        assert!(directory.read("DIRECTORY").is_none());

        let missing_root = root.with_extension("missing");
        assert!(CredentialDirectory::open(missing_root).is_err());
        assert!(CredentialDirectory::open(root.join("TOKEN")).is_err());

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(root.join("TOKEN"), root.join("LINK")).unwrap();
            assert!(directory.read("LINK").is_none());
            let root_link = root.with_extension("link");
            std::os::unix::fs::symlink(&root, &root_link).unwrap();
            assert!(CredentialDirectory::open(root_link).is_err());
        }
    }
}
