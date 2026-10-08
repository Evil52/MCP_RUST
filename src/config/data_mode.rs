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
    let ambient = ambient_write_credentials(
        std::env::vars_os()
            .filter_map(|(name, value)| Some((name.into_string().ok()?, !value.is_empty()))),
    );
    if !ambient.is_empty() {
        tracing::warn!(
            variables = ?ambient,
            "write credentials are present in the read-only analytics environment; \
             give this server its own env file (MCP_ENV_FILE) without them"
        );
    }
    Ok(config)
}

/// A read-only server never binds a marketplace write credential, whatever
/// the registry says: such a token would only widen what a leaked process
/// environment or a future code path could do.
fn reject_write_credential_binding(name: &str) -> Result<()> {
    if name.contains("WRITE") {
        bail!("{name}: read-only аналитика не может использовать write-credential");
    }
    Ok(())
}

/// Names of non-empty variables that carry write authority. Shared `.env`
/// files used to rely on per-shop blanking in Compose, which silently missed
/// any shop added later.
fn ambient_write_credentials(variables: impl Iterator<Item = (String, bool)>) -> Vec<String> {
    let mut names = variables
        .filter(|(name, present)| *present && name.contains("WRITE_TOKEN"))
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    names.sort_unstable();
    names
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
        let ozon = account.ozon.iter().flat_map(|ozon| {
            [ozon.client_id_env.as_str(), ozon.api_key_env.as_str()]
                .into_iter()
                .chain(ozon.performance.iter().flat_map(|performance| {
                    [
                        performance.client_id_env.as_str(),
                        performance.client_secret_env.as_str(),
                    ]
                }))
        });
        let wildberries = account
            .wildberries
            .iter()
            .map(|wildberries| wildberries.api_token_env.as_str());
        for name in ozon.chain(wildberries) {
            reject_write_credential_binding(name)?;
        }
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

#[cfg(test)]
mod write_credential_tests {
    use super::{ambient_write_credentials, reject_write_credential_binding};

    #[test]
    fn write_credentials_are_never_bound_and_ambient_ones_are_named() {
        assert!(reject_write_credential_binding("SHOP_WB_API_TOKEN").is_ok());
        let error = reject_write_credential_binding("SHOP_WB_PROMOTION_WRITE_TOKEN")
            .unwrap_err()
            .to_string();
        assert!(error.contains("SHOP_WB_PROMOTION_WRITE_TOKEN"), "{error}");
        assert_eq!(
            ambient_write_credentials(
                [
                    ("Z_WB_PROMOTION_WRITE_TOKEN".to_owned(), true),
                    ("BLANKED_WB_PROMOTION_WRITE_TOKEN".to_owned(), false),
                    ("A_WB_PROMOTION_WRITE_TOKEN".to_owned(), true),
                    ("SHOP_WB_API_TOKEN".to_owned(), true),
                ]
                .into_iter()
            ),
            ["A_WB_PROMOTION_WRITE_TOKEN", "Z_WB_PROMOTION_WRITE_TOKEN"]
        );
    }
}
