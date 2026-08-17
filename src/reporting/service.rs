use std::{fs, path::Path, str::FromStr, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::{Config, config::Host};

use crate::config::RegistrySource;

use super::{
    policy::DailyReportPolicy, postgres_outbox::PostgresOutboxRepository,
    postgres_snapshot::PostgresSnapshotRepository,
};

const DATABASE_URL_ENV: &str = "REPORT_WORKER_DATABASE_URL";
const POLICY_PATH_ENV: &str = "DAILY_REPORT_POLICY";
const ACCESS_CONFIG_ENV: &str = "MCP_ACCESS_CONFIG";
const MODE_ENV: &str = "REPORT_WORKER_MODE";
const MAX_POLICY_BYTES: u64 = 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportWorkerMode {
    Disabled,
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
}

impl ReportWorkerConfig {
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let mode = match lookup(MODE_ENV).as_deref().unwrap_or("disabled") {
            "disabled" => ReportWorkerMode::Disabled,
            _ => bail!("only the disabled report-worker runtime mode is available"),
        };
        let raw_database =
            lookup(DATABASE_URL_ENV).context("REPORT_WORKER_DATABASE_URL is required")?;
        let mut database = Config::from_str(&raw_database)
            .context("REPORT_WORKER_DATABASE_URL must be a PostgreSQL URL")?;
        validate_database(&database)?;
        database.connect_timeout(CONNECT_TIMEOUT);
        database.application_name("mcp-ozon-report-worker");

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
        Ok(Self {
            database,
            mode,
            policy,
        })
    }

    pub fn mode(&self) -> ReportWorkerMode {
        self.mode
    }

    pub fn policy(&self) -> &DailyReportPolicy {
        &self.policy
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
        r#"{"version":1,"actors":[{"id":"diana_serafimovich","name":"Diana","role":"manager","oidc":{"username":"diana"}}],"accounts":[{"id":"furnitura_dlya_doma","organization":"Ozon","marketplace":"ozon","seller_client_id":"1","manager_id":"diana_serafimovich","ozon":{"store_id":"ozon-1","client_id_env":"OZON_ID","api_key_env":"OZON_KEY"}}]}"#
    }

    fn policy(enabled: bool) -> String {
        format!(
            r#"{{"version":1,"enabled":{enabled},"timezone":"Asia/Yekaterinburg","sender_email_env":"DAILY_REPORT_SENDER_EMAIL","audiences":[{{"id":"pilot_owner","email_env":"DAILY_REPORT_PILOT_RECIPIENT_EMAIL","managers":[{{"actor_id":"diana_serafimovich","account_ids":["furnitura_dlya_doma"]}}]}}]}}"#
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
        vec![
            (
                DATABASE_URL_ENV,
                "postgresql://report_worker:fixture-password@position-db/ozon_positions".to_owned(),
            ),
            (ACCESS_CONFIG_ENV, registry.display().to_string()),
            (POLICY_PATH_ENV, policy.display().to_string()),
        ]
    }

    #[test]
    fn disabled_worker_loads_only_registry_metadata_and_policy() {
        let config = config(valid_entries(false)).unwrap();
        assert_eq!(config.mode(), ReportWorkerMode::Disabled);
        assert!(!config.policy().enabled);
        assert_eq!(config.policy().audiences[0].id, "pilot_owner");
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
