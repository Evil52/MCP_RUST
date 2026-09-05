use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use tokio_postgres::{Config as PostgresConfig, config::Host};

use crate::{
    config::{AccessRegistry, Marketplace, PerformanceCredentials, credential_sha256},
    control::policy::{ControlMode, ControlPolicy},
};

use super::{
    ControlAuthConfig, ControlOzonMarketplaceRuntimeConfig, ControlOzonRuntimeConfig,
    validation::{parse_strict_bool, value_or},
    wb_runtime::{read_control_token, validate_proxy_url},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OzonRuntimeIdentity {
    Planner,
    Executor,
}

pub(super) fn load_ozon_runtime(
    lookup: &mut dyn FnMut(&str) -> Option<String>,
    auth: &ControlAuthConfig,
    policy: &ControlPolicy,
    registry: &AccessRegistry,
    identity: OzonRuntimeIdentity,
) -> Result<Option<ControlOzonRuntimeConfig>> {
    let Some(account_id) = lookup("CONTROL_MCP_OZON_ACCOUNT_ID") else {
        return Ok(None);
    };
    if policy.mode == ControlMode::Disabled {
        bail!("CONTROL_MCP_OZON_ACCOUNT_ID нельзя задавать при disabled policy");
    }
    if !matches!(auth, ControlAuthConfig::Jwt(_)) {
        bail!("Ozon Control runtime разрешён только в JWT-режиме Control MCP");
    }
    if account_id.is_empty() || account_id.trim() != account_id {
        bail!("CONTROL_MCP_OZON_ACCOUNT_ID должен быть непустым без внешнего whitespace");
    }
    let account = registry
        .accounts
        .iter()
        .find(|account| account.id == account_id)
        .context("CONTROL_MCP_OZON_ACCOUNT_ID отсутствует в access registry")?;
    if !matches!(account.marketplace, Marketplace::Ozon) {
        bail!("CONTROL_MCP_OZON_ACCOUNT_ID должен ссылаться на Ozon Performance account");
    }
    if !policy.actors.iter().any(|actor| {
        actor
            .ozon_campaign_launch_targets
            .iter()
            .any(|target| target.account_id == account_id)
    }) {
        bail!("CONTROL_MCP_OZON_ACCOUNT_ID не имеет launch target в control policy");
    }

    let (database_url_key, database_role, database_application_name) = match identity {
        OzonRuntimeIdentity::Planner => (
            "CONTROL_MCP_OZON_PLANNER_DATABASE_URL",
            "ozon_control_planner",
            "mcp-ozon-control-planner",
        ),
        OzonRuntimeIdentity::Executor => (
            "CONTROL_MCP_OZON_EXECUTOR_DATABASE_URL",
            "ozon_control_executor",
            "mcp-ozon-control-executor",
        ),
    };
    let database_url = required_nonempty(lookup, database_url_key)?;
    let mut database = validate_ozon_database_url(&database_url, database_role).with_context(|| {
        format!(
            "{database_url_key} должен использовать restricted role {database_role} и один TCP host"
        )
    })?;
    crate::postgres::harden(&mut database, database_application_name);
    if identity == OzonRuntimeIdentity::Planner {
        if parse_strict_bool(
            &value_or(lookup, "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED", "false"),
            "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED",
        )? {
            bail!("credentialless Ozon planner cannot arm marketplace writes");
        }
        for forbidden in [
            "CONTROL_MCP_OZON_PLANNER_PERFORMANCE_CLIENT_ID_FILE",
            "CONTROL_MCP_OZON_PLANNER_PERFORMANCE_CLIENT_SECRET_FILE",
            "CONTROL_MCP_OZON_EXECUTOR_DATABASE_URL",
            "CONTROL_MCP_OZON_EXECUTOR_PERFORMANCE_CLIENT_ID_FILE",
            "CONTROL_MCP_OZON_EXECUTOR_PERFORMANCE_CLIENT_SECRET_FILE",
            "CONTROL_MCP_OZON_PROXY",
            "CONTROL_MCP_OZON_TIMEOUT_SECONDS",
        ] {
            if lookup(forbidden).is_some() {
                bail!(
                    "credentialless Ozon planner запрещает marketplace credential/egress setting {forbidden}"
                );
            }
        }
        return Ok(Some(ControlOzonRuntimeConfig {
            account_id,
            database,
            writer_enabled: false,
            marketplace: None,
        }));
    }

    let ozon = account
        .ozon
        .as_ref()
        .context("Ozon account binding отсутствует")?;
    let performance_binding = ozon
        .performance
        .as_ref()
        .context("Ozon Performance account binding отсутствует")?;
    let expected_client_id_sha256 = performance_binding
        .control_executor_client_id_sha256
        .as_deref()
        .context(
            "Ozon Control executor runtime требует account-bound Performance Client-Id fingerprint",
        )?;
    let client_id_file_key = "CONTROL_MCP_OZON_EXECUTOR_PERFORMANCE_CLIENT_ID_FILE";
    let client_secret_file_key = "CONTROL_MCP_OZON_EXECUTOR_PERFORMANCE_CLIENT_SECRET_FILE";
    let client_id_path = PathBuf::from(required_nonempty(lookup, client_id_file_key)?);
    let client_secret_path = PathBuf::from(required_nonempty(lookup, client_secret_file_key)?);
    if client_id_path == client_secret_path {
        bail!("Ozon Performance client id и secret должны быть в разных credential files");
    }
    let credentials = PerformanceCredentials {
        client_id: read_control_token(&client_id_path, client_id_file_key)?,
        client_secret: read_control_token(&client_secret_path, client_secret_file_key)?,
    };
    if credential_sha256(&credentials.client_id) != expected_client_id_sha256 {
        bail!(
            "Ozon Control executor Performance Client-Id не соответствует выбранному Ozon account"
        );
    }
    let writes_enabled = parse_strict_bool(
        &value_or(lookup, "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED", "false"),
        "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED",
    )?;
    let runtime_write_armed = policy.mode == ControlMode::Enabled && writes_enabled;
    let proxy_url = required_nonempty(lookup, "CONTROL_MCP_OZON_PROXY")?;
    validate_proxy_url(&proxy_url)?;
    let timeout_seconds = value_or(lookup, "CONTROL_MCP_OZON_TIMEOUT_SECONDS", "20")
        .parse::<u64>()
        .context("CONTROL_MCP_OZON_TIMEOUT_SECONDS должен быть целым числом")?;
    if !(1..=30).contains(&timeout_seconds) {
        bail!("CONTROL_MCP_OZON_TIMEOUT_SECONDS должен быть от 1 до 30");
    }
    Ok(Some(ControlOzonRuntimeConfig {
        account_id,
        database,
        writer_enabled: runtime_write_armed,
        marketplace: Some(ControlOzonMarketplaceRuntimeConfig {
            store_id: ozon.store_id.clone(),
            credentials,
            proxy_url,
            request_timeout: Duration::from_secs(timeout_seconds),
        }),
    }))
}

fn validate_ozon_database_url(value: &str, expected_role: &str) -> Result<PostgresConfig> {
    if value.is_empty() || value.trim() != value {
        bail!("Ozon Control database URL должен быть непустым без whitespace");
    }
    let config = value
        .parse::<PostgresConfig>()
        .context("Ozon Control database URL имеет недопустимый формат")?;
    let exactly_one_tcp_host = matches!(config.get_hosts(), [Host::Tcp(host)] if !host.is_empty());
    if config.get_user() != Some(expected_role)
        || config.get_password().is_none_or(<[u8]>::is_empty)
        || config.get_dbname().is_none_or(str::is_empty)
        || !exactly_one_tcp_host
        || !config.get_hostaddrs().is_empty()
        || !matches!(config.get_ports(), [port] if *port != 0)
    {
        bail!("Ozon Control database identity не прошла fail-closed validation");
    }
    Ok(config)
}

fn required_nonempty(lookup: &mut dyn FnMut(&str) -> Option<String>, key: &str) -> Result<String> {
    lookup(key)
        .filter(|value| !value.is_empty() && value.trim() == value)
        .with_context(|| format!("{key} обязателен для Ozon Control runtime"))
}
