//! Sanitized, structured MCP tool-call telemetry.
//!
//! Arguments and response bodies never cross this boundary. The database role
//! can only open/finish one lifecycle row and read the bounded admin projection
//! through SECURITY DEFINER functions; it has no direct table privileges.

use std::{str::FromStr, sync::Arc, time::Duration};

use anyhow::{Result as AnyResult, anyhow};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::Serialize;
use tokio_postgres::{Config, Row, config::Host};

use crate::{postgres::SupervisedClient, reporting::snapshot::Marketplace};

const COMPONENT: &str = "mcp-ozon-tool-telemetry";
const MAX_TOOL_CALL_DURATION: Duration = Duration::from_secs(600);
pub const MAX_TOOL_CALL_LOG_ROWS: u16 = 200;

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum ToolTelemetryError {
    #[error("tool telemetry is disabled")]
    Disabled,
    #[error("tool telemetry request is invalid")]
    InvalidRequest,
    #[error("tool telemetry is temporarily unavailable")]
    Unavailable,
    #[error("tool telemetry returned invalid data")]
    InvalidData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolCallReceipt {
    id: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallOutcome {
    Succeeded,
    Failed,
    Cancelled,
    Overloaded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallLogOutcome {
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Overloaded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct ToolCallLogItem {
    pub call_id: String,
    pub actor_id: String,
    pub tool_name: String,
    pub account_id: Option<String>,
    pub marketplace: Option<Marketplace>,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub duration_ms: Option<u32>,
    pub outcome: ToolCallLogOutcome,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct ToolCallLogResult {
    pub calls: Vec<ToolCallLogItem>,
}

#[derive(Clone)]
pub struct ToolTelemetryService {
    client: Option<Arc<SupervisedClient>>,
}

impl std::fmt::Debug for ToolTelemetryService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolTelemetryService")
            .field("enabled", &self.is_enabled())
            .finish_non_exhaustive()
    }
}

impl ToolTelemetryService {
    #[must_use]
    pub const fn disabled() -> Self {
        Self { client: None }
    }

    pub async fn connect_optional(database_url: Option<&str>) -> AnyResult<Self> {
        let Some(database_url) = database_url else {
            return Ok(Self::disabled());
        };
        let mut config = Config::from_str(database_url)
            .map_err(|_| anyhow!("tool telemetry database configuration is invalid"))?;
        validate_database_config(&config)
            .map_err(|_| anyhow!("tool telemetry database configuration is invalid"))?;
        crate::postgres::harden(&mut config, COMPONENT);
        let service = Self {
            client: Some(Arc::new(
                SupervisedClient::connect(&config, COMPONENT)
                    .await
                    .map_err(|_| anyhow!("tool telemetry database contract is unavailable"))?,
            )),
        };
        service
            .verify_runtime_contract()
            .await
            .map_err(|_| anyhow!("tool telemetry database contract is unavailable"))?;
        Ok(service)
    }

    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.client.is_some()
    }

    pub async fn probe(&self) -> Result<(), ToolTelemetryError> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        client
            .probe()
            .await
            .map_err(|_| ToolTelemetryError::Unavailable)
    }

    pub async fn begin(
        &self,
        actor_id: &str,
        tool_name: &str,
        account_id: Option<&str>,
        marketplace: Option<Marketplace>,
    ) -> Result<Option<ToolCallReceipt>, ToolTelemetryError> {
        let Some(client) = &self.client else {
            return Ok(None);
        };
        validate_actor(actor_id)?;
        validate_tool(tool_name)?;
        if let Some(account_id) = account_id {
            validate_account(account_id)?;
        }
        let marketplace = marketplace.map(marketplace_name);
        let client = client
            .acquire()
            .await
            .map_err(|_| ToolTelemetryError::Unavailable)?;
        let id: i64 = client
            .query_one(
                "SELECT daily_reporting.begin_mcp_tool_call($1, $2, $3, $4)",
                &[&actor_id, &tool_name, &account_id, &marketplace],
            )
            .await
            .map_err(|_| ToolTelemetryError::Unavailable)?
            .try_get(0)
            .map_err(|_| ToolTelemetryError::InvalidData)?;
        drop(client);
        if id <= 0 {
            return Err(ToolTelemetryError::InvalidData);
        }
        Ok(Some(ToolCallReceipt { id }))
    }

    pub async fn finish(
        &self,
        receipt: Option<ToolCallReceipt>,
        outcome: ToolCallOutcome,
        duration: Duration,
        error_code: Option<&str>,
    ) -> Result<(), ToolTelemetryError> {
        let Some(receipt) = receipt else {
            return Ok(());
        };
        if duration > MAX_TOOL_CALL_DURATION {
            return Err(ToolTelemetryError::InvalidRequest);
        }
        if let Some(error_code) = error_code {
            validate_error_code(error_code)?;
        }
        let duration_ms =
            i32::try_from(duration.as_millis()).map_err(|_| ToolTelemetryError::InvalidRequest)?;
        let client = self
            .client
            .as_ref()
            .ok_or(ToolTelemetryError::Unavailable)?;
        let client = client
            .acquire()
            .await
            .map_err(|_| ToolTelemetryError::Unavailable)?;
        let completed: bool = client
            .query_one(
                "SELECT daily_reporting.finish_mcp_tool_call($1, $2, $3, $4)",
                &[
                    &receipt.id,
                    &outcome_name(outcome),
                    &duration_ms,
                    &error_code,
                ],
            )
            .await
            .map_err(|_| ToolTelemetryError::Unavailable)?
            .try_get(0)
            .map_err(|_| ToolTelemetryError::InvalidData)?;
        drop(client);
        completed
            .then_some(())
            .ok_or(ToolTelemetryError::InvalidData)
    }

    pub async fn list(&self, limit: u16) -> Result<ToolCallLogResult, ToolTelemetryError> {
        if !(1..=MAX_TOOL_CALL_LOG_ROWS).contains(&limit) {
            return Err(ToolTelemetryError::InvalidRequest);
        }
        let client = self.client.as_ref().ok_or(ToolTelemetryError::Disabled)?;
        let client = client
            .acquire()
            .await
            .map_err(|_| ToolTelemetryError::Unavailable)?;
        let rows = client
            .query(
                "SELECT call_id, actor_id, tool_name, account_id, marketplace, started_at, \
                        finished_at, duration_ms, outcome, error_code \
                 FROM daily_reporting.list_mcp_tool_calls($1)",
                &[&i32::from(limit)],
            )
            .await
            .map_err(|_| ToolTelemetryError::Unavailable)?;
        drop(client);
        let calls = rows
            .iter()
            .map(tool_call_log_item)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ToolCallLogResult { calls })
    }

    async fn verify_runtime_contract(&self) -> Result<(), ToolTelemetryError> {
        let client = self.client.as_ref().ok_or(ToolTelemetryError::Disabled)?;
        client
            .verify_session_bounds()
            .await
            .map_err(|_| ToolTelemetryError::Unavailable)?;
        let client = client
            .acquire()
            .await
            .map_err(|_| ToolTelemetryError::Unavailable)?;
        let valid: bool = client
            .query_one(
                "SELECT current_user = 'report_refresh_requester' \
                    AND has_function_privilege(current_user, \
                        'daily_reporting.begin_mcp_tool_call(text,text,text,text)', 'EXECUTE') \
                    AND has_function_privilege(current_user, \
                        'daily_reporting.finish_mcp_tool_call(bigint,text,integer,text)', 'EXECUTE') \
                    AND has_function_privilege(current_user, \
                        'daily_reporting.list_mcp_tool_calls(integer)', 'EXECUTE') \
                    AND NOT has_table_privilege(current_user, \
                        'daily_reporting.mcp_tool_calls', 'SELECT,INSERT,UPDATE,DELETE')",
                &[],
            )
            .await
            .map_err(|_| ToolTelemetryError::Unavailable)?
            .try_get(0)
            .map_err(|_| ToolTelemetryError::InvalidData)?;
        drop(client);
        valid.then_some(()).ok_or(ToolTelemetryError::Unavailable)
    }
}

fn tool_call_log_item(row: &Row) -> Result<ToolCallLogItem, ToolTelemetryError> {
    let call_id: i64 = row
        .try_get(0)
        .map_err(|_| ToolTelemetryError::InvalidData)?;
    let marketplace: Option<String> = row
        .try_get(4)
        .map_err(|_| ToolTelemetryError::InvalidData)?;
    let started_at: DateTime<Utc> = row
        .try_get(5)
        .map_err(|_| ToolTelemetryError::InvalidData)?;
    let finished_at: Option<DateTime<Utc>> = row
        .try_get(6)
        .map_err(|_| ToolTelemetryError::InvalidData)?;
    let duration_ms: Option<i32> = row
        .try_get(7)
        .map_err(|_| ToolTelemetryError::InvalidData)?;
    if call_id <= 0 || duration_ms.is_some_and(|value| value < 0) {
        return Err(ToolTelemetryError::InvalidData);
    }
    Ok(ToolCallLogItem {
        call_id: call_id.to_string(),
        actor_id: row
            .try_get(1)
            .map_err(|_| ToolTelemetryError::InvalidData)?,
        tool_name: row
            .try_get(2)
            .map_err(|_| ToolTelemetryError::InvalidData)?,
        account_id: row
            .try_get(3)
            .map_err(|_| ToolTelemetryError::InvalidData)?,
        marketplace: marketplace.as_deref().map(parse_marketplace).transpose()?,
        started_at: started_at.to_rfc3339(),
        finished_at: finished_at.map(|value| value.to_rfc3339()),
        duration_ms: duration_ms
            .map(u32::try_from)
            .transpose()
            .map_err(|_| ToolTelemetryError::InvalidData)?,
        outcome: parse_outcome(
            row.try_get::<_, &str>(8)
                .map_err(|_| ToolTelemetryError::InvalidData)?,
        )?,
        error_code: row
            .try_get(9)
            .map_err(|_| ToolTelemetryError::InvalidData)?,
    })
}

fn validate_database_config(config: &Config) -> Result<(), ToolTelemetryError> {
    if config.get_user() == Some("report_refresh_requester")
        && config
            .get_password()
            .is_some_and(|password| !password.is_empty())
        && config.get_dbname().is_some_and(|value| !value.is_empty())
        && config.get_hosts().len() == 1
        && matches!(config.get_hosts(), [Host::Tcp(host)] if !host.trim().is_empty())
        && config.get_options().is_none()
    {
        Ok(())
    } else {
        Err(ToolTelemetryError::InvalidRequest)
    }
}

fn validate_actor(value: &str) -> Result<(), ToolTelemetryError> {
    validate_ascii_identifier(value, 128, b"._:@-")
}

fn validate_account(value: &str) -> Result<(), ToolTelemetryError> {
    validate_ascii_identifier(value, 128, b"_-")
}

fn validate_tool(value: &str) -> Result<(), ToolTelemetryError> {
    if !value.is_empty()
        && value.len() <= 128
        && value.as_bytes()[0].is_ascii_lowercase()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
    {
        Ok(())
    } else {
        Err(ToolTelemetryError::InvalidRequest)
    }
}

fn validate_error_code(value: &str) -> Result<(), ToolTelemetryError> {
    if !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_uppercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        Ok(())
    } else {
        Err(ToolTelemetryError::InvalidRequest)
    }
}

fn validate_ascii_identifier(
    value: &str,
    maximum: usize,
    punctuation: &[u8],
) -> Result<(), ToolTelemetryError> {
    if !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || punctuation.contains(&byte))
    {
        Ok(())
    } else {
        Err(ToolTelemetryError::InvalidRequest)
    }
}

const fn marketplace_name(marketplace: Marketplace) -> &'static str {
    match marketplace {
        Marketplace::Ozon => "ozon",
        Marketplace::Wildberries => "wildberries",
    }
}

fn parse_marketplace(value: &str) -> Result<Marketplace, ToolTelemetryError> {
    match value {
        "ozon" => Ok(Marketplace::Ozon),
        "wildberries" => Ok(Marketplace::Wildberries),
        _ => Err(ToolTelemetryError::InvalidData),
    }
}

const fn outcome_name(outcome: ToolCallOutcome) -> &'static str {
    match outcome {
        ToolCallOutcome::Succeeded => "succeeded",
        ToolCallOutcome::Failed => "failed",
        ToolCallOutcome::Cancelled => "cancelled",
        ToolCallOutcome::Overloaded => "overloaded",
    }
}

fn parse_outcome(value: &str) -> Result<ToolCallLogOutcome, ToolTelemetryError> {
    match value {
        "running" => Ok(ToolCallLogOutcome::Running),
        "succeeded" => Ok(ToolCallLogOutcome::Succeeded),
        "failed" => Ok(ToolCallLogOutcome::Failed),
        "cancelled" => Ok(ToolCallLogOutcome::Cancelled),
        "overloaded" => Ok(ToolCallLogOutcome::Overloaded),
        _ => Err(ToolTelemetryError::InvalidData),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_mode_is_explicit_and_does_not_block_tool_calls() {
        let service = ToolTelemetryService::disabled();
        assert!(!service.is_enabled());
        assert_eq!(service.probe().await, Ok(()));
        assert_eq!(
            service
                .begin(
                    "admin",
                    "wb_ping",
                    Some("account_wb"),
                    Some(Marketplace::Wildberries)
                )
                .await,
            Ok(None)
        );
        assert_eq!(service.list(10).await, Err(ToolTelemetryError::Disabled));
    }

    #[test]
    fn identifiers_and_outcomes_are_bounded() {
        assert!(validate_actor("manager@example.test").is_ok());
        assert!(validate_actor("bad actor").is_err());
        assert!(validate_account("account_1").is_ok());
        assert!(validate_account("bad/account").is_err());
        assert!(validate_tool("ofk_collection_status").is_ok());
        assert!(validate_tool("BadTool").is_err());
        assert!(validate_error_code("ACCESS_DENIED").is_ok());
        assert!(validate_error_code("bad-code").is_err());
        assert_eq!(parse_outcome("running"), Ok(ToolCallLogOutcome::Running));
        assert_eq!(
            parse_marketplace("unknown"),
            Err(ToolTelemetryError::InvalidData)
        );
    }
}
