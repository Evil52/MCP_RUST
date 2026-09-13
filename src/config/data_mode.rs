//! Opt-in boundary for credentialless prepared analytics.

use super::{
    AccessRegistry, AppConfig, BTreeMap, FromStr, MarketplaceCredentials, Result, bail,
    finish_optional_dotenv_load, load_ozon_credentials, load_wildberries_credentials,
    validate_unique_performance_client_ids,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpDataMode {
    LiveAnalytics,
    ReportingOnly,
}

impl FromStr for McpDataMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "live_analytics" => Ok(Self::LiveAnalytics),
            "reporting_only" => Ok(Self::ReportingOnly),
            _ => bail!("MCP_DATA_MODE должен быть live_analytics или reporting_only"),
        }
    }
}

pub(super) fn load_process_config() -> Result<AppConfig> {
    // A reporting-only deployment must declare its boundary in the process
    // environment. Never import an adjacent legacy .env containing vendor keys.
    let process_mode: McpDataMode = match std::env::var("MCP_DATA_MODE") {
        Ok(value) => value.parse()?,
        Err(std::env::VarError::NotPresent) => McpDataMode::LiveAnalytics,
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("MCP_DATA_MODE содержит недопустимую кодировку")
        }
    };
    if process_mode == McpDataMode::LiveAnalytics {
        finish_optional_dotenv_load(dotenvy::dotenv())?;
    }
    let config = AppConfig::from_lookup(|key| std::env::var(key).ok())?;
    if config.data_mode == McpDataMode::ReportingOnly && process_mode != McpDataMode::ReportingOnly
    {
        bail!(
            "MCP_DATA_MODE=reporting_only должен быть задан явно в окружении процесса, не в .env"
        );
    }
    Ok(config)
}

pub(super) fn load_marketplace_credentials(
    snapshot: &AccessRegistry,
    lookup: &mut dyn FnMut(&str) -> Option<String>,
) -> Result<MarketplaceCredentials> {
    let mut credentials = MarketplaceCredentials {
        stores: BTreeMap::new(),
        performance_stores: BTreeMap::new(),
        wildberries_accounts: BTreeMap::new(),
    };
    for account in &snapshot.accounts {
        load_ozon_credentials(
            snapshot,
            account,
            lookup,
            &mut credentials.stores,
            &mut credentials.performance_stores,
        )?;
        load_wildberries_credentials(account, lookup, &mut credentials.wildberries_accounts)?;
    }
    validate_unique_performance_client_ids(&credentials.performance_stores)?;
    Ok(credentials)
}
