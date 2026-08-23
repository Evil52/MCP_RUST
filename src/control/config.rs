use std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use tokio_postgres::Config as PostgresConfig;

use crate::{
    config::{AuthMode, JwtConfig, RegistrySource, TransportMode},
    control::policy::ControlPolicy,
};

use jwt::load_jwt_config;
#[cfg(test)]
use jwt::{CONTROL_INTERNAL_JWKS_URL, CONTROL_REQUIRED_SCOPE};
#[cfg(test)]
use wb_runtime::{
    MAX_CONTROL_CREDENTIAL_BYTES, WB_PROMOTION_BIT, WB_READ_ONLY_BIT,
    normalize_control_token_bytes, read_control_token, validate_proxy_url,
    validate_wb_reader_token, validate_wb_writer_token,
};
use wb_runtime::{load_policy_database, load_wb_runtime};

const DEFAULT_CONTROL_ACCESS_CONFIG: &str = "config/access.json";
const DEFAULT_CONTROL_POLICY: &str = "config/control-policy.json";

mod jwt;
mod wb_runtime;

#[derive(Debug, Clone)]
pub enum ControlAuthConfig {
    Dev { actor_id: String },
    Jwt(JwtConfig),
}

#[derive(Debug, Clone)]
pub struct ControlAppConfig {
    pub bind: SocketAddr,
    pub max_sessions: NonZeroUsize,
    pub session_idle_timeout: Duration,
    pub transport: TransportMode,
    pub auth: ControlAuthConfig,
    pub registry: RegistrySource,
    pub policy: ControlPolicy,
    /// Optional restricted store used to persist even a disabled policy
    /// revision as a rollback-prevention tombstone. It contains no WB secret.
    pub policy_database: Option<ControlPolicyDatabaseConfig>,
    pub wb_runtime: Option<ControlWbRuntimeConfig>,
}

#[derive(Clone)]
pub struct ControlPolicyDatabaseConfig {
    pub database: PostgresConfig,
}

impl std::fmt::Debug for ControlPolicyDatabaseConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlPolicyDatabaseConfig")
            .field("database", &"<redacted>")
            .finish()
    }
}

#[derive(Clone)]
pub struct ControlWbRuntimeConfig {
    pub account_id: String,
    pub seller_sid: String,
    /// Dedicated Personal production token with only Promotion read access.
    pub reader_token: String,
    /// Dedicated Personal production token with only Promotion read/write access.
    /// It is absent in `plan_only`, so that process cannot construct a writer.
    pub writer_token: Option<String>,
    pub database: PostgresConfig,
    pub proxy_url: String,
    pub request_timeout: Duration,
}

impl std::fmt::Debug for ControlWbRuntimeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlWbRuntimeConfig")
            .field("account_id", &self.account_id)
            .field("seller_sid", &self.seller_sid)
            .field("reader_token", &"<redacted>")
            .field("writer_token_loaded", &self.writer_token.is_some())
            .field("database", &"<redacted>")
            .field("proxy_url", &self.proxy_url)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

impl ControlAppConfig {
    /// Loads only `CONTROL_MCP_*` variables.
    ///
    /// This function intentionally does not call `dotenvy::dotenv` and never
    /// resolves credential env names from `access.json`. The disabled scaffold
    /// therefore cannot inherit Seller, Performance, or WB keys from the
    /// analytics process environment.
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let bind = value_or(&mut lookup, "CONTROL_MCP_BIND", "127.0.0.1:8790")
            .parse::<SocketAddr>()
            .context("CONTROL_MCP_BIND должен иметь формат IP:PORT")?;
        let transport =
            value_or(&mut lookup, "CONTROL_MCP_TRANSPORT", "http").parse::<TransportMode>()?;
        let max_sessions = parse_max_sessions(lookup("CONTROL_MCP_MAX_SESSIONS").as_deref())?;
        let session_idle_timeout = parse_session_idle_timeout(
            lookup("CONTROL_MCP_SESSION_IDLE_TIMEOUT_SECONDS").as_deref(),
        )?;
        let registry_path = value_or(
            &mut lookup,
            "CONTROL_MCP_ACCESS_CONFIG",
            DEFAULT_CONTROL_ACCESS_CONFIG,
        );
        let registry = RegistrySource::new(registry_path)?;
        let snapshot = registry.load()?;
        let policy_path = value_or(&mut lookup, "CONTROL_MCP_POLICY", DEFAULT_CONTROL_POLICY);
        let policy = ControlPolicy::load(PathBuf::from(policy_path), &snapshot)?;

        let auth_mode =
            value_or(&mut lookup, "CONTROL_MCP_AUTH_MODE", "dev").parse::<AuthMode>()?;
        let auth = match auth_mode {
            AuthMode::Dev => {
                let actor_id = lookup("CONTROL_MCP_ACTOR_ID")
                    .context("CONTROL_MCP_ACTOR_ID обязателен в dev-режиме")?;
                snapshot.actor(&actor_id)?;
                let allow_non_loopback = parse_strict_bool(
                    &value_or(&mut lookup, "CONTROL_MCP_DEV_ALLOW_NON_LOOPBACK", "false"),
                    "CONTROL_MCP_DEV_ALLOW_NON_LOOPBACK",
                )?;
                if transport == TransportMode::Http
                    && !bind.ip().is_loopback()
                    && !allow_non_loopback
                {
                    bail!(
                        "dev Control MCP может слушать non-loopback только при явном CONTROL_MCP_DEV_ALLOW_NON_LOOPBACK=true"
                    );
                }
                ControlAuthConfig::Dev { actor_id }
            }
            AuthMode::Jwt => {
                if transport != TransportMode::Http {
                    bail!("JWT для Control MCP поддерживается только через HTTP");
                }
                ControlAuthConfig::Jwt(load_jwt_config(&mut lookup)?)
            }
        };

        let policy_database = load_policy_database(&mut lookup)?;
        if policy_database.is_some() && !matches!(auth, ControlAuthConfig::Jwt(_)) {
            bail!("Control policy store разрешён только в JWT-режиме");
        }
        let wb_runtime = load_wb_runtime(
            &mut lookup,
            &auth,
            &policy,
            &snapshot,
            policy_database.as_ref(),
        )?;

        Ok(Self {
            bind,
            max_sessions,
            session_idle_timeout,
            transport,
            auth,
            registry,
            policy,
            policy_database,
            wb_runtime,
        })
    }
}

fn value_or(lookup: &mut dyn FnMut(&str) -> Option<String>, key: &str, default: &str) -> String {
    lookup(key).unwrap_or_else(|| default.to_owned())
}

fn parse_max_sessions(value: Option<&str>) -> Result<NonZeroUsize> {
    let Some(value) = value else {
        return Ok(LocalSessionManager::DEFAULT_MAX_SESSIONS);
    };
    let parsed = value
        .parse::<NonZeroUsize>()
        .context("CONTROL_MCP_MAX_SESSIONS должен быть положительным целым числом")?;
    if parsed > LocalSessionManager::DEFAULT_MAX_SESSIONS {
        bail!("CONTROL_MCP_MAX_SESSIONS не может превышать встроенный лимит");
    }
    Ok(parsed)
}

fn parse_session_idle_timeout(value: Option<&str>) -> Result<Duration> {
    let seconds = value
        .unwrap_or("120")
        .parse::<u64>()
        .context("CONTROL_MCP_SESSION_IDLE_TIMEOUT_SECONDS должен быть целым числом")?;
    if !(90..=300).contains(&seconds) {
        bail!("CONTROL_MCP_SESSION_IDLE_TIMEOUT_SECONDS должен быть от 90 до 300");
    }
    Ok(Duration::from_secs(seconds))
}

fn parse_strict_bool(value: &str, name: &str) -> Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => bail!("{name} должен быть строго true или false"),
    }
}

#[cfg(test)]
mod tests;
