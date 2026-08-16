use std::{str::FromStr, time::Duration};

use thiserror::Error;
use tokio_postgres::{Config, config::Host};

use super::{PostgresRepository, RepositoryError};

const DATABASE_URL_ENV: &str = "POSITION_COLLECTOR_DATABASE_URL";
const MODE_ENV: &str = "POSITION_COLLECTOR_MODE";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollectorRuntimeMode {
    Disabled,
}

/// Credential-isolated configuration for the disabled collector runtime.
///
/// It intentionally does not load `.env` and accepts no marketplace URL,
/// cookie, token, browser profile, or live-source switch.
pub struct CollectorRuntimeConfig {
    database: Config,
    mode: CollectorRuntimeMode,
}

impl CollectorRuntimeConfig {
    pub fn from_env() -> Result<Self, RuntimeConfigError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, RuntimeConfigError> {
        let mode = match lookup(MODE_ENV).as_deref().unwrap_or("disabled") {
            "disabled" => CollectorRuntimeMode::Disabled,
            _ => return Err(RuntimeConfigError::UnsupportedMode),
        };
        let raw = lookup(DATABASE_URL_ENV).ok_or(RuntimeConfigError::MissingDatabaseUrl)?;
        let mut database =
            Config::from_str(&raw).map_err(|_| RuntimeConfigError::InvalidDatabaseUrl)?;
        validate_database_config(&database)?;
        database.connect_timeout(CONNECT_TIMEOUT);
        database.application_name("mcp-ozon-position-collector");
        Ok(Self { database, mode })
    }

    pub fn mode(&self) -> CollectorRuntimeMode {
        self.mode
    }

    pub async fn connect_repository(&self) -> Result<PostgresRepository, RepositoryError> {
        PostgresRepository::connect(&self.database).await
    }
}

fn validate_database_config(config: &Config) -> Result<(), RuntimeConfigError> {
    if config.get_user() != Some("position_collector")
        || config
            .get_password()
            .is_none_or(|password| password.is_empty())
        || config.get_dbname().is_none_or(str::is_empty)
        || config.get_hosts().len() != 1
        || !matches!(config.get_hosts(), [Host::Tcp(host)] if !host.trim().is_empty())
        || config.get_options().is_some()
    {
        return Err(RuntimeConfigError::InvalidDatabaseUrl);
    }
    Ok(())
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeConfigError {
    #[error("POSITION_COLLECTOR_DATABASE_URL is required")]
    MissingDatabaseUrl,
    #[error("POSITION_COLLECTOR_DATABASE_URL must be a bounded position_collector PostgreSQL URL")]
    InvalidDatabaseUrl,
    #[error("only the disabled collector runtime mode is available")]
    UnsupportedMode,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(entries: &[(&str, &str)]) -> Result<CollectorRuntimeConfig, RuntimeConfigError> {
        CollectorRuntimeConfig::from_lookup(|key| {
            entries
                .iter()
                .find_map(|(entry_key, value)| (*entry_key == key).then(|| (*value).to_owned()))
        })
    }

    #[test]
    fn disabled_config_accepts_only_the_restricted_database_identity() {
        let value = config(&[(
            DATABASE_URL_ENV,
            "postgresql://position_collector:fixture-password@position-db/ozon_positions",
        )])
        .unwrap();
        assert_eq!(value.mode(), CollectorRuntimeMode::Disabled);
    }

    #[test]
    fn configuration_fails_closed_without_a_valid_database_url() {
        assert!(matches!(
            config(&[]),
            Err(RuntimeConfigError::MissingDatabaseUrl)
        ));
        for url in [
            "not-a-url",
            "postgresql://position_reader:password@position-db/ozon_positions",
            "postgresql://position_collector@position-db/ozon_positions",
            "postgresql://position_collector:password@/ozon_positions",
            "postgresql://position_collector:password@position-db",
            "postgresql://position_collector:password@position-db/ozon_positions?options=-csearch_path%3Dpublic",
        ] {
            assert!(matches!(
                config(&[(DATABASE_URL_ENV, url)]),
                Err(RuntimeConfigError::InvalidDatabaseUrl)
            ));
        }
    }

    #[test]
    fn live_modes_are_not_present_in_the_scaffold() {
        assert!(matches!(
            config(&[
                (MODE_ENV, "live"),
                (
                    DATABASE_URL_ENV,
                    "postgresql://position_collector:password@position-db/ozon_positions",
                ),
            ]),
            Err(RuntimeConfigError::UnsupportedMode)
        ));
    }
}
