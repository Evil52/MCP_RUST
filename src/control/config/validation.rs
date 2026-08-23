use std::{num::NonZeroUsize, time::Duration};

use anyhow::{Context, Result, bail};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;

pub(super) fn value_or(
    lookup: &mut dyn FnMut(&str) -> Option<String>,
    key: &str,
    default: &str,
) -> String {
    lookup(key).unwrap_or_else(|| default.to_owned())
}

pub(super) fn parse_max_sessions(value: Option<&str>) -> Result<NonZeroUsize> {
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

pub(super) fn parse_session_idle_timeout(value: Option<&str>) -> Result<Duration> {
    let seconds = value
        .unwrap_or("120")
        .parse::<u64>()
        .context("CONTROL_MCP_SESSION_IDLE_TIMEOUT_SECONDS должен быть целым числом")?;
    if !(90..=300).contains(&seconds) {
        bail!("CONTROL_MCP_SESSION_IDLE_TIMEOUT_SECONDS должен быть от 90 до 300");
    }
    Ok(Duration::from_secs(seconds))
}

pub(super) fn parse_strict_bool(value: &str, name: &str) -> Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => bail!("{name} должен быть строго true или false"),
    }
}
