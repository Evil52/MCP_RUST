use std::{fs, path::Path, str::FromStr, sync::Arc};

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::{Config, config::Host};

use crate::config::{AccessRegistry, Marketplace as RegistryMarketplace, RegistrySource};

use super::{
    ReportKey,
    artifact_store::LocalArtifactStore,
    collector_plan::{CollectionPlanError, CollectionTarget, build_collection_plan},
    policy::DailyReportPolicy,
    postgres_outbox::PostgresOutboxRepository,
    postgres_snapshot::PostgresSnapshotRepository,
    snapshot::{AccountScope, Marketplace},
};

const DATABASE_URL_ENV: &str = "REPORT_WORKER_DATABASE_URL";
const POLICY_PATH_ENV: &str = "DAILY_REPORT_POLICY";
const ACCESS_CONFIG_ENV: &str = "MCP_ACCESS_CONFIG";
const ARTIFACT_ROOT_ENV: &str = "REPORT_ARTIFACT_ROOT";
const MODE_ENV: &str = "REPORT_WORKER_MODE";
const MAX_POLICY_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportWorkerMode {
    Disabled,
    DryRun,
}

/// Credential-isolated configuration for the reporting runtime.
///
/// This process reads registry metadata and routing policy only. It never
/// resolves marketplace credential environment variables, calls `dotenv`, or
/// accepts any mail/S3 configuration in this phase.
pub struct ReportWorkerConfig {
    database: Config,
    mode: ReportWorkerMode,
    policy: DailyReportPolicy,
    registry: Arc<AccessRegistry>,
    artifact_store: LocalArtifactStore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportPreviewScope {
    pub audience_id: String,
    pub actor_id: String,
    pub manager_name: String,
    pub accounts: Vec<AccountScope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportGenerationScope {
    pub audience_id: String,
    pub report_name: String,
    pub accounts: Vec<AccountScope>,
}

impl ReportWorkerConfig {
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let mode = match lookup(MODE_ENV).as_deref().unwrap_or("disabled") {
            "disabled" => ReportWorkerMode::Disabled,
            "dry_run" => ReportWorkerMode::DryRun,
            _ => bail!("report-worker mode must be disabled or dry_run"),
        };
        let raw_database =
            lookup(DATABASE_URL_ENV).context("REPORT_WORKER_DATABASE_URL is required")?;
        let mut database = Config::from_str(&raw_database)
            .context("REPORT_WORKER_DATABASE_URL must be a PostgreSQL URL")?;
        validate_database(&database)?;
        crate::postgres::harden(&mut database, "mcp-ozon-report-worker");

        let registry_path = lookup(ACCESS_CONFIG_ENV).context("MCP_ACCESS_CONFIG is required")?;
        let registry = RegistrySource::new(registry_path)
            .context("MCP_ACCESS_CONFIG must contain a valid access registry")?;
        let policy_path = lookup(POLICY_PATH_ENV).context("DAILY_REPORT_POLICY is required")?;
        let policy_bytes = read_bounded_file(Path::new(&policy_path), MAX_POLICY_BYTES)
            .context("DAILY_REPORT_POLICY cannot be read")?;
        let snapshot = registry
            .load()
            .context("MCP_ACCESS_CONFIG cannot be loaded")?;
        let policy = DailyReportPolicy::from_slice(&policy_bytes, &snapshot)
            .context("DAILY_REPORT_POLICY is invalid")?;
        let artifact_root =
            lookup(ARTIFACT_ROOT_ENV).context("REPORT_ARTIFACT_ROOT is required")?;
        let artifact_store = LocalArtifactStore::open(artifact_root)
            .context("REPORT_ARTIFACT_ROOT must be an existing safe directory")?;
        Ok(Self {
            database,
            mode,
            policy,
            registry: snapshot,
            artifact_store,
        })
    }

    pub fn mode(&self) -> ReportWorkerMode {
        self.mode
    }

    pub fn policy(&self) -> &DailyReportPolicy {
        &self.policy
    }

    pub fn artifact_store(&self) -> &LocalArtifactStore {
        &self.artifact_store
    }

    /// Performs the credential-free source preflight used by health checks.
    pub fn collection_plan(&self) -> Result<Vec<CollectionTarget>, CollectionPlanError> {
        build_collection_plan(&self.policy, &self.registry)
    }

    /// Resolves one manager preview exclusively through the validated policy.
    ///
    /// Callers cannot inject account identifiers. This is intentionally more
    /// restrictive than an audience-level report while the WB collector is
    /// unavailable and the pilot audience spans two marketplaces.
    pub fn preview_scope(&self, audience_id: &str, actor_id: &str) -> Result<ReportPreviewScope> {
        let audience = self
            .policy
            .audiences
            .iter()
            .find(|audience| audience.id == audience_id)
            .context("unknown daily report audience")?;
        let manager = audience
            .managers
            .iter()
            .find(|manager| manager.actor_id == actor_id)
            .context("manager is outside the selected daily report audience")?;
        let actor = self
            .registry
            .actor(actor_id)
            .context("daily report manager is unavailable")?;
        let accounts = self.account_scopes(manager.account_ids.iter())?;
        Ok(ReportPreviewScope {
            audience_id: audience.id.clone(),
            actor_id: actor.id.clone(),
            manager_name: actor.name.clone(),
            accounts,
        })
    }

    /// Resolves the complete audience attached to a persisted report key.
    ///
    /// Unlike manual preview, generation never accepts an actor or account
    /// list from the caller. Every account comes from the validated policy and
    /// access registry, and the persisted report version must match exactly.
    pub fn generation_scope(&self, key: &ReportKey) -> Result<ReportGenerationScope> {
        ensure!(
            key.report_version == self.policy.version,
            "report batch version does not match the active policy"
        );
        let audience = self
            .policy
            .audiences
            .iter()
            .find(|audience| audience.id == key.recipient_id)
            .context("report batch recipient is outside the active policy")?;
        let account_ids = audience
            .managers
            .iter()
            .flat_map(|manager| manager.account_ids.iter());
        let accounts = self.account_scopes(account_ids)?;
        let report_name = if let [manager] = audience.managers.as_slice() {
            self.registry
                .actor(&manager.actor_id)
                .context("daily report manager is unavailable")?
                .name
                .clone()
        } else {
            format!("Сводный отчёт ({})", audience.id)
        };
        Ok(ReportGenerationScope {
            audience_id: audience.id.clone(),
            report_name,
            accounts,
        })
    }

    fn account_scopes<'a>(
        &self,
        account_ids: impl Iterator<Item = &'a String>,
    ) -> Result<Vec<AccountScope>> {
        account_ids
            .map(|account_id| {
                let account = self
                    .registry
                    .accounts
                    .iter()
                    .find(|account| account.id == *account_id)
                    .context("daily report account is unavailable")?;
                let marketplace = match account.marketplace {
                    RegistryMarketplace::Ozon => Marketplace::Ozon,
                    RegistryMarketplace::Wildberries => Marketplace::Wildberries,
                };
                AccountScope::new(account.id.clone(), marketplace)
                    .context("daily report account scope is invalid")
            })
            .collect()
    }

    pub async fn connect(&self) -> Result<(PostgresOutboxRepository, PostgresSnapshotRepository)> {
        let outbox = PostgresOutboxRepository::connect(&self.database)
            .await
            .context("daily report outbox is unavailable")?;
        let snapshots = PostgresSnapshotRepository::connect(&self.database)
            .await
            .context("daily report snapshot reader is unavailable")?;
        Ok((outbox, snapshots))
    }
}

fn validate_database(config: &Config) -> Result<()> {
    ensure!(
        config.get_user() == Some("report_worker")
            && config
                .get_password()
                .is_some_and(|password| !password.is_empty())
            && config.get_dbname().is_some_and(|value| !value.is_empty())
            && config.get_hosts().len() == 1
            && matches!(config.get_hosts(), [Host::Tcp(host)] if !host.trim().is_empty())
            && config.get_options().is_none(),
        "REPORT_WORKER_DATABASE_URL must use the restricted report_worker identity"
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
            "mcp-ozon-report-worker-{label}-{}",
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, body).unwrap();
        path
    }

    fn registry() -> &'static str {
        r#"{"version":1,"actors":[{"id":"diana_serafimovich","name":"Diana","role":"manager","oidc":{"username":"diana"}},{"id":"wb6","name":"Vahrusheva / Torsunova","role":"manager","oidc":{"username":"wb6"}}],"accounts":[{"id":"furnitura_dlya_doma","organization":"Ozon","marketplace":"ozon","seller_client_id":"1","manager_id":"diana_serafimovich","ozon":{"store_id":"ozon-1","client_id_env":"OZON_ID","api_key_env":"OZON_KEY","performance":{"client_id_env":"OZON_PERFORMANCE_ID","client_secret_env":"OZON_PERFORMANCE_SECRET"}}},{"id":"ip_domnyshev_wb","organization":"WB","marketplace":"wildberries","seller_client_id":"2","manager_id":"wb6","wildberries":{"api_token_env":"WB_TOKEN"}}]}"#
    }

    fn policy(enabled: bool) -> String {
        format!(
            r#"{{"version":1,"enabled":{enabled},"timezone":"Asia/Yekaterinburg","sender_email_env":"DAILY_REPORT_SENDER_EMAIL","audiences":[{{"id":"pilot_owner","email_env":"DAILY_REPORT_PILOT_RECIPIENT_EMAIL","managers":[{{"actor_id":"diana_serafimovich","account_ids":["furnitura_dlya_doma"]}},{{"actor_id":"wb6","account_ids":["ip_domnyshev_wb"]}}]}}]}}"#
        )
    }

    fn config(entries: Vec<(&str, String)>) -> Result<ReportWorkerConfig> {
        ReportWorkerConfig::from_lookup(|key| {
            entries
                .iter()
                .find_map(|(entry, value)| (*entry == key).then(|| value.clone()))
        })
    }

    fn valid_entries(enabled: bool) -> Vec<(&'static str, String)> {
        let registry = file("registry", registry());
        let policy = file("policy", &policy(enabled));
        let artifact_root = std::env::temp_dir().join(format!(
            "mcp-ozon-report-artifacts-{}-{}",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&artifact_root).unwrap();
        vec![
            (
                DATABASE_URL_ENV,
                "postgresql://report_worker:fixture-password@position-db/ozon_positions".to_owned(),
            ),
            (ACCESS_CONFIG_ENV, registry.display().to_string()),
            (POLICY_PATH_ENV, policy.display().to_string()),
            (ARTIFACT_ROOT_ENV, artifact_root.display().to_string()),
        ]
    }

    #[test]
    fn disabled_worker_loads_only_registry_metadata_and_policy() {
        let config = config(valid_entries(false)).unwrap();
        assert_eq!(config.mode(), ReportWorkerMode::Disabled);
        assert!(!config.policy().enabled);
        assert_eq!(config.policy().audiences[0].id, "pilot_owner");
        assert_eq!(config.collection_plan().unwrap().len(), 2);
        config.artifact_store().verify_writable().unwrap();
        assert_eq!(
            config
                .preview_scope("pilot_owner", "diana_serafimovich")
                .unwrap(),
            ReportPreviewScope {
                audience_id: "pilot_owner".to_owned(),
                actor_id: "diana_serafimovich".to_owned(),
                manager_name: "Diana".to_owned(),
                accounts: vec![
                    AccountScope::new("furnitura_dlya_doma".to_owned(), Marketplace::Ozon,)
                        .unwrap()
                ],
            }
        );
        assert!(
            config
                .preview_scope("unknown", "diana_serafimovich")
                .is_err()
        );
        assert!(config.preview_scope("pilot_owner", "unknown").is_err());
        let wb = config.preview_scope("pilot_owner", "wb6").unwrap();
        assert_eq!(wb.manager_name, "Vahrusheva / Torsunova");
        assert_eq!(wb.accounts[0].marketplace(), Marketplace::Wildberries);

        let generation = config
            .generation_scope(&ReportKey {
                local_date: chrono::NaiveDate::from_ymd_opt(2026, 8, 18).unwrap(),
                kind: super::super::ReportKind::Morning,
                recipient_id: "pilot_owner".to_owned(),
                report_version: 1,
            })
            .unwrap();
        assert_eq!(generation.audience_id, "pilot_owner");
        assert_eq!(generation.report_name, "Сводный отчёт (pilot_owner)");
        assert_eq!(generation.accounts.len(), 2);
        assert_eq!(generation.accounts[0].marketplace(), Marketplace::Ozon);
        assert_eq!(
            generation.accounts[1].marketplace(),
            Marketplace::Wildberries
        );
        for (recipient_id, report_version) in [("unknown", 1), ("pilot_owner", 2)] {
            assert!(
                config
                    .generation_scope(&ReportKey {
                        local_date: chrono::NaiveDate::from_ymd_opt(2026, 8, 18).unwrap(),
                        kind: super::super::ReportKind::Morning,
                        recipient_id: recipient_id.to_owned(),
                        report_version,
                    })
                    .is_err()
            );
        }
    }

    #[test]
    fn invalid_runtime_mode_database_and_required_paths_fail_closed() {
        assert!(config(Vec::new()).is_err());
        for url in [
            "not-a-url",
            "postgresql://position_reader:password@position-db/ozon_positions",
            "postgresql://report_worker@position-db/ozon_positions",
            "postgresql://report_worker:password@/ozon_positions",
            "postgresql://report_worker:password@position-db/ozon_positions?options=-csearch_path%3Dpublic",
        ] {
            let mut entries = valid_entries(false);
            entries[0] = (DATABASE_URL_ENV, url.to_owned());
            assert!(config(entries).is_err());
        }
        let mut entries = valid_entries(false);
        entries.push((MODE_ENV, "live".to_owned()));
        assert!(config(entries).is_err());
        let mut entries = valid_entries(false);
        entries.retain(|(key, _)| *key != POLICY_PATH_ENV);
        assert!(config(entries).is_err());
        let mut entries = valid_entries(false);
        entries.retain(|(key, _)| *key != ARTIFACT_ROOT_ENV);
        assert!(config(entries).is_err());
        let mut entries = valid_entries(false);
        entries
            .iter_mut()
            .find(|(key, _)| *key == ARTIFACT_ROOT_ENV)
            .unwrap()
            .1 = file("artifact-file", "not a directory")
            .display()
            .to_string();
        assert!(config(entries).is_err());
    }

    #[test]
    fn single_manager_generation_uses_the_authoritative_actor_name() {
        let mut entries = valid_entries(false);
        let policy_path = entries
            .iter()
            .find(|(key, _)| *key == POLICY_PATH_ENV)
            .map(|(_, value)| value.clone())
            .unwrap();
        fs::write(
            policy_path,
            r#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"DAILY_REPORT_SENDER_EMAIL","audiences":[{"id":"diana","email_env":"DIANA_EMAIL","managers":[{"actor_id":"diana_serafimovich","account_ids":["furnitura_dlya_doma"]}]}]}"#,
        )
        .unwrap();
        let config = config(std::mem::take(&mut entries)).unwrap();
        let scope = config
            .generation_scope(&ReportKey {
                local_date: chrono::NaiveDate::from_ymd_opt(2026, 8, 18).unwrap(),
                kind: super::super::ReportKind::Evening,
                recipient_id: "diana".to_owned(),
                report_version: 1,
            })
            .unwrap();
        assert_eq!(scope.report_name, "Diana");
        assert_eq!(scope.accounts.len(), 1);
    }

    #[test]
    fn explicit_dry_run_mode_is_available_without_mail_configuration() {
        let mut entries = valid_entries(true);
        entries.push((MODE_ENV, "dry_run".to_owned()));
        let config = config(entries).unwrap();
        assert_eq!(config.mode(), ReportWorkerMode::DryRun);
        assert!(config.policy().enabled);
    }

    #[test]
    fn invalid_registry_or_policy_cannot_enable_the_worker() {
        let invalid_registry = file("bad-registry", "{}");
        let mut entries = valid_entries(false);
        entries[1] = (ACCESS_CONFIG_ENV, invalid_registry.display().to_string());
        assert!(config(entries).is_err());

        let oversized = file("oversized", &" ".repeat((MAX_POLICY_BYTES + 1) as usize));
        let mut entries = valid_entries(false);
        entries[2] = (POLICY_PATH_ENV, oversized.display().to_string());
        assert!(config(entries).is_err());

        let invalid_policy = file("bad-policy", "{}");
        let mut entries = valid_entries(true);
        entries[2] = (POLICY_PATH_ENV, invalid_policy.display().to_string());
        assert!(config(entries).is_err());
    }

    #[test]
    fn bounded_file_reader_rejects_non_files_and_invalid_limits() {
        let directory = std::env::temp_dir();
        assert!(read_bounded_file(&directory, MAX_POLICY_BYTES).is_err());
        let path = file("small", "{}{}");
        assert!(read_bounded_file(&path, 1).is_err());
        assert_eq!(read_bounded_file(&path, 4).unwrap(), b"{}{}".to_vec());
    }
}
