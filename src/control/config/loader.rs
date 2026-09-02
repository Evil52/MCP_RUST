use std::{net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result, bail};

use crate::{
    config::{AuthMode, RegistrySource, TransportMode},
    control::policy::ControlPolicy,
};

use super::{
    ControlAppConfig, ControlAuthConfig,
    jwt::load_jwt_config,
    load_ozon_runtime,
    validation::{parse_max_sessions, parse_session_idle_timeout, parse_strict_bool, value_or},
    wb_runtime::{load_policy_database, load_wb_runtime},
};

const DEFAULT_CONTROL_ACCESS_CONFIG: &str = "config/access.json";
const DEFAULT_CONTROL_POLICY: &str = "config/control-policy.json";

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

    pub(super) fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
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
        let ozon_runtime = load_ozon_runtime(&mut lookup, &auth, &policy, &snapshot)?;
        if ozon_runtime.is_some() && policy_database.is_none() {
            bail!("Ozon Control runtime требует CONTROL_MCP_DATABASE_URL");
        }

        Ok(Self {
            bind,
            max_sessions,
            session_idle_timeout,
            transport,
            auth,
            registry,
            policy,
            policy_database,
            ozon_runtime,
            wb_runtime,
        })
    }
}
