use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};

use crate::{
    config::{AccessRegistry, Marketplace, PerformanceCredentials},
    control::policy::{ControlMode, ControlPolicy},
};

use super::{
    ControlAuthConfig, ControlOzonRuntimeConfig,
    validation::{parse_strict_bool, value_or},
    wb_runtime::{read_control_token, validate_proxy_url},
};

pub(super) fn load_ozon_runtime(
    lookup: &mut dyn FnMut(&str) -> Option<String>,
    auth: &ControlAuthConfig,
    policy: &ControlPolicy,
    registry: &AccessRegistry,
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
    if !matches!(account.marketplace, Marketplace::Ozon)
        || account
            .ozon
            .as_ref()
            .and_then(|ozon| ozon.performance.as_ref())
            .is_none()
    {
        bail!("CONTROL_MCP_OZON_ACCOUNT_ID должен ссылаться на Ozon Performance account");
    }
    let store_id = account
        .ozon
        .as_ref()
        .context("Ozon account binding отсутствует")?
        .store_id
        .clone();
    if !policy.actors.iter().any(|actor| {
        actor
            .ozon_campaign_launch_targets
            .iter()
            .any(|target| target.account_id == account_id)
    }) {
        bail!("CONTROL_MCP_OZON_ACCOUNT_ID не имеет launch target в control policy");
    }

    let client_id_path = PathBuf::from(required_nonempty(
        lookup,
        "CONTROL_MCP_OZON_PERFORMANCE_CLIENT_ID_FILE",
    )?);
    let client_secret_path = PathBuf::from(required_nonempty(
        lookup,
        "CONTROL_MCP_OZON_PERFORMANCE_CLIENT_SECRET_FILE",
    )?);
    if client_id_path == client_secret_path {
        bail!("Ozon Performance client id и secret должны быть в разных credential files");
    }
    let credentials = PerformanceCredentials {
        client_id: read_control_token(
            &client_id_path,
            "CONTROL_MCP_OZON_PERFORMANCE_CLIENT_ID_FILE",
        )?,
        client_secret: read_control_token(
            &client_secret_path,
            "CONTROL_MCP_OZON_PERFORMANCE_CLIENT_SECRET_FILE",
        )?,
    };
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
        store_id,
        credentials,
        writer_enabled: runtime_write_armed,
        proxy_url,
        request_timeout: Duration::from_secs(timeout_seconds),
    }))
}

fn required_nonempty(lookup: &mut dyn FnMut(&str) -> Option<String>, key: &str) -> Result<String> {
    lookup(key)
        .filter(|value| !value.is_empty() && value.trim() == value)
        .with_context(|| format!("{key} обязателен для Ozon Control runtime"))
}
