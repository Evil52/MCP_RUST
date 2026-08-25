#![expect(
    clippy::significant_drop_tightening,
    reason = "the campaign lease intentionally owns one supervised PostgreSQL session"
)]

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use tokio_postgres::{Config, error::SqlState};

use crate::postgres::{ClientGuard, SupervisedClient};

const COMPONENT: &str = "mcp-ozon-wb-automation";
const MAX_ACCOUNT_BYTES: usize = 128;
const MAX_JSON_BYTES: usize = 1024 * 1024;
const VERIFY_RUNTIME_CONTRACT_SQL: &str = include_str!("automation_postgres_contract.sql");

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum WbAutomationPostgresError {
    #[error("WB automation PostgreSQL store is unavailable")]
    Unavailable,
    #[error("WB automation PostgreSQL input is invalid")]
    InvalidInput,
    #[error("WB automation PostgreSQL state changed")]
    StateChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WbAutomationDatabaseState {
    pub account_id: String,
    pub campaign_id: u64,
    pub policy_digest: String,
    pub business_date: NaiveDate,
    pub actions_today: u32,
    pub last_action_at: Option<DateTime<Utc>>,
    pub paused_for_daily_cap_on: Option<NaiveDate>,
    pub pending_idempotency_key: Option<String>,
    pub incident_class: Option<String>,
    pub revision: u64,
    pub imported_legacy_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WbAutomationLegacyStateSeed {
    pub policy_digest: String,
    pub business_date: NaiveDate,
    pub actions_today: u32,
    pub last_action_at: Option<DateTime<Utc>>,
    pub paused_for_daily_cap_on: Option<NaiveDate>,
    pub incident_class: Option<String>,
    pub legacy_digest: String,
}

#[derive(Clone)]
pub struct WbAutomationPostgresStore {
    client: Arc<SupervisedClient>,
}

impl WbAutomationPostgresStore {
    pub async fn connect(config: &Config) -> Result<Self, WbAutomationPostgresError> {
        let client = SupervisedClient::connect(config, COMPONENT)
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        Ok(Self {
            client: Arc::new(client),
        })
    }

    pub async fn verify_runtime_contract(&self) -> Result<(), WbAutomationPostgresError> {
        self.client
            .verify_session_bounds()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        let valid = client
            .query_one(VERIFY_RUNTIME_CONTRACT_SQL, &[])
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?
            .get::<_, bool>(0);
        valid
            .then_some(())
            .ok_or(WbAutomationPostgresError::Unavailable)
    }

    /// Acquires the same session-level campaign lock used by manual Control
    /// transactions. `None` means another runtime or operator owns the exact
    /// account/campaign boundary and this cycle must safely do nothing.
    pub async fn try_acquire_campaign(
        &self,
        account_id: &str,
        campaign_id: u64,
    ) -> Result<Option<WbAutomationCampaignLease<'_>>, WbAutomationPostgresError> {
        validate_account(account_id)?;
        let campaign_id = to_i64(campaign_id)?;
        let lock_key = format!("wb/{account_id}/{campaign_id}");
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        let Ok(lock_row) = client
            .query_one(
                "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                &[&lock_key],
            )
            .await
        else {
            client.discard();
            return Err(WbAutomationPostgresError::Unavailable);
        };
        let locked = lock_row.get::<_, bool>(0);
        if !locked {
            return Ok(None);
        }
        Ok(Some(WbAutomationCampaignLease {
            client: Some(client),
            account_id: account_id.to_owned(),
            campaign_id,
            lock_key,
        }))
    }
}

pub struct WbAutomationCampaignLease<'a> {
    client: Option<ClientGuard<'a>>,
    account_id: String,
    campaign_id: i64,
    lock_key: String,
}

impl std::fmt::Debug for WbAutomationCampaignLease<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WbAutomationCampaignLease")
            .field("account_id", &self.account_id)
            .field("campaign_id", &self.campaign_id)
            .finish_non_exhaustive()
    }
}

impl WbAutomationCampaignLease<'_> {
    /// Imports the exact legacy safety state once. Pending legacy writes are
    /// deliberately outside this contract: the caller must refuse them rather
    /// than clear an unresolved marketplace mutation during migration.
    pub async fn initialize_from_legacy(
        &self,
        seed: &WbAutomationLegacyStateSeed,
    ) -> Result<bool, WbAutomationPostgresError> {
        validate_digest(&seed.policy_digest)?;
        validate_digest(&seed.legacy_digest)?;
        if seed.actions_today > 500
            || seed
                .paused_for_daily_cap_on
                .is_some_and(|paused_on| paused_on > seed.business_date)
            || seed
                .incident_class
                .as_deref()
                .is_some_and(|value| !validate_error_class(value))
        {
            return Err(WbAutomationPostgresError::InvalidInput);
        }
        let actions_today = i32::try_from(seed.actions_today)
            .map_err(|_| WbAutomationPostgresError::InvalidInput)?;
        let client = self
            .client
            .as_ref()
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        let row = client
            .query_one(
                "WITH inserted AS (\
                    INSERT INTO wb_automation.execution_state (\
                        account_id, advert_id, schema_version, policy_digest, business_date, \
                        actions_today, last_action_at, paused_for_daily_cap_on, incident_class, \
                        revision, imported_legacy_digest\
                    ) VALUES ($1,$2,1,$3,$4,$5,$6,$7,$8,1,$9) \
                    ON CONFLICT (account_id, advert_id) DO NOTHING \
                    RETURNING account_id, advert_id, policy_digest, business_date, \
                        actions_today, last_action_at, paused_for_daily_cap_on, \
                        pending_idempotency_key, incident_class, revision, \
                        imported_legacy_digest\
                 ) \
                 SELECT account_id, advert_id, policy_digest, business_date, actions_today, \
                    last_action_at, paused_for_daily_cap_on, pending_idempotency_key, \
                    incident_class, revision, imported_legacy_digest, true AS was_inserted \
                 FROM inserted \
                 UNION ALL \
                 SELECT account_id, advert_id, policy_digest, business_date, actions_today, \
                    last_action_at, paused_for_daily_cap_on, pending_idempotency_key, \
                    incident_class, revision, imported_legacy_digest, false AS was_inserted \
                 FROM wb_automation.execution_state \
                 WHERE account_id=$1 AND advert_id=$2 \
                    AND NOT EXISTS (SELECT 1 FROM inserted) \
                 LIMIT 1",
                &[
                    &self.account_id,
                    &self.campaign_id,
                    &seed.policy_digest,
                    &seed.business_date,
                    &actions_today,
                    &seed.last_action_at,
                    &seed.paused_for_daily_cap_on,
                    &seed.incident_class,
                    &seed.legacy_digest,
                ],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        if row.get::<_, bool>(11) {
            return Ok(true);
        }
        let existing = parse_state(&row)?;
        if existing.policy_digest == seed.policy_digest
            && existing.business_date == seed.business_date
            && existing.actions_today == seed.actions_today
            && existing.last_action_at == seed.last_action_at
            && existing.paused_for_daily_cap_on == seed.paused_for_daily_cap_on
            && existing.pending_idempotency_key.is_none()
            && existing.incident_class == seed.incident_class
            && existing.revision == 1
            && existing.imported_legacy_digest.as_deref() == Some(seed.legacy_digest.as_str())
        {
            Ok(false)
        } else {
            Err(WbAutomationPostgresError::StateChanged)
        }
    }

    pub async fn load_state(
        &self,
    ) -> Result<Option<WbAutomationDatabaseState>, WbAutomationPostgresError> {
        let client = self
            .client
            .as_ref()
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        let row = client
            .query_opt(
                "SELECT account_id, advert_id, policy_digest, business_date, \
                        actions_today, last_action_at, paused_for_daily_cap_on, \
                        pending_idempotency_key, incident_class, revision, \
                        imported_legacy_digest \
                 FROM wb_automation.execution_state \
                 WHERE account_id=$1 AND advert_id=$2",
                &[&self.account_id, &self.campaign_id],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        row.as_ref().map(parse_state).transpose()
    }

    /// Persists one immutable shadow observation while checking the exact
    /// state revision under the campaign lock. Duplicate delivery of the same
    /// cycle is accepted only when every persisted field is identical.
    #[allow(clippy::too_many_arguments)]
    pub async fn persist_shadow_cycle(
        &mut self,
        cycle_id: &str,
        policy_digest: &str,
        observed_at: DateTime<Utc>,
        business_date: NaiveDate,
        expected_state_revision: u64,
        snapshot_json: &str,
        decision_json: &str,
    ) -> Result<bool, WbAutomationPostgresError> {
        validate_digest(cycle_id)?;
        validate_digest(policy_digest)?;
        validate_json(snapshot_json, MAX_JSON_BYTES)?;
        validate_json(decision_json, 256 * 1024)?;
        let state_revision = to_i64(expected_state_revision)?;
        let client = self
            .client
            .as_mut()
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        let state = transaction
            .query_opt(
                "SELECT revision, policy_digest \
                 FROM wb_automation.execution_state \
                 WHERE account_id=$1 AND advert_id=$2 FOR UPDATE",
                &[&self.account_id, &self.campaign_id],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?
            .ok_or(WbAutomationPostgresError::StateChanged)?;
        if state.get::<_, i64>(0) != state_revision || state.get::<_, &str>(1) != policy_digest {
            return Err(WbAutomationPostgresError::StateChanged);
        }
        let inserted = transaction
            .query_opt(
                "INSERT INTO wb_automation.cycles (\
                    cycle_id, account_id, advert_id, policy_digest, observed_at, \
                    business_date, state_revision, snapshot_json, decision_json\
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) \
                 ON CONFLICT (cycle_id) DO NOTHING RETURNING cycle_id",
                &[
                    &cycle_id,
                    &self.account_id,
                    &self.campaign_id,
                    &policy_digest,
                    &observed_at,
                    &business_date,
                    &state_revision,
                    &snapshot_json,
                    &decision_json,
                ],
            )
            .await
            .map_err(|error| map_insert_error(&error))?;
        let was_inserted = inserted.is_some();
        if !was_inserted {
            let existing = transaction
                .query_opt(
                    "SELECT account_id, advert_id, policy_digest, observed_at, \
                            business_date, state_revision, snapshot_json, decision_json \
                     FROM wb_automation.cycles WHERE cycle_id=$1",
                    &[&cycle_id],
                )
                .await
                .map_err(|_| WbAutomationPostgresError::Unavailable)?
                .ok_or(WbAutomationPostgresError::StateChanged)?;
            let stored_snapshot = serde_json::from_str::<serde_json::Value>(existing.get(6))
                .map_err(|_| WbAutomationPostgresError::Unavailable)?;
            let stored_decision = serde_json::from_str::<serde_json::Value>(existing.get(7))
                .map_err(|_| WbAutomationPostgresError::Unavailable)?;
            let expected_snapshot = serde_json::from_str::<serde_json::Value>(snapshot_json)
                .map_err(|_| WbAutomationPostgresError::InvalidInput)?;
            let expected_decision = serde_json::from_str::<serde_json::Value>(decision_json)
                .map_err(|_| WbAutomationPostgresError::InvalidInput)?;
            let existing_matches = existing.get::<_, &str>(0) == self.account_id
                && existing.get::<_, i64>(1) == self.campaign_id
                && existing.get::<_, &str>(2) == policy_digest
                && existing.get::<_, DateTime<Utc>>(3) == observed_at
                && existing.get::<_, NaiveDate>(4) == business_date
                && existing.get::<_, i64>(5) == state_revision
                && stored_snapshot == expected_snapshot
                && stored_decision == expected_decision;
            if !existing_matches {
                return Err(WbAutomationPostgresError::StateChanged);
            }
        }
        transaction
            .commit()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        Ok(was_inserted)
    }

    /// Releases the session lock explicitly. If unlock is not proven, Drop
    /// discards the whole connection so PostgreSQL releases it server-side.
    pub async fn release(mut self) -> Result<(), WbAutomationPostgresError> {
        let client = self
            .client
            .as_ref()
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        let unlocked = client
            .query_one(
                "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                &[&self.lock_key],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?
            .get::<_, bool>(0);
        unlocked
            .then_some(())
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        self.client.take();
        Ok(())
    }
}

impl Drop for WbAutomationCampaignLease<'_> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            client.discard();
        }
    }
}

fn parse_state(
    row: &tokio_postgres::Row,
) -> Result<WbAutomationDatabaseState, WbAutomationPostgresError> {
    let campaign_id =
        u64::try_from(row.get::<_, i64>(1)).map_err(|_| WbAutomationPostgresError::Unavailable)?;
    let actions_today =
        u32::try_from(row.get::<_, i32>(4)).map_err(|_| WbAutomationPostgresError::Unavailable)?;
    let revision =
        u64::try_from(row.get::<_, i64>(9)).map_err(|_| WbAutomationPostgresError::Unavailable)?;
    Ok(WbAutomationDatabaseState {
        account_id: row.get(0),
        campaign_id,
        policy_digest: row.get(2),
        business_date: row.get(3),
        actions_today,
        last_action_at: row.get(5),
        paused_for_daily_cap_on: row.get(6),
        pending_idempotency_key: row.get(7),
        incident_class: row.get(8),
        revision,
        imported_legacy_digest: row.get(10),
    })
}

fn validate_account(value: &str) -> Result<(), WbAutomationPostgresError> {
    if !value.is_empty()
        && value.len() <= MAX_ACCOUNT_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        Ok(())
    } else {
        Err(WbAutomationPostgresError::InvalidInput)
    }
}

fn validate_digest(value: &str) -> Result<(), WbAutomationPostgresError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(WbAutomationPostgresError::InvalidInput)
    }
}

fn validate_json(value: &str, limit: usize) -> Result<(), WbAutomationPostgresError> {
    if value.len() < 2 || value.len() > limit {
        return Err(WbAutomationPostgresError::InvalidInput);
    }
    match serde_json::from_str::<serde_json::Value>(value) {
        Ok(serde_json::Value::Object(_)) => Ok(()),
        _ => Err(WbAutomationPostgresError::InvalidInput),
    }
}

fn validate_error_class(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn to_i64(value: u64) -> Result<i64, WbAutomationPostgresError> {
    i64::try_from(value).map_err(|_| WbAutomationPostgresError::InvalidInput)
}

fn map_insert_error(error: &tokio_postgres::Error) -> WbAutomationPostgresError {
    if error
        .as_db_error()
        .is_some_and(|database| database.code() == &SqlState::UNIQUE_VIOLATION)
    {
        WbAutomationPostgresError::StateChanged
    } else {
        WbAutomationPostgresError::Unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_digest_json_and_integer_inputs_are_bounded() {
        assert!(validate_account("ip_domnyshev_wb").is_ok());
        assert!(validate_account("").is_err());
        assert!(validate_account(&"a".repeat(129)).is_err());
        assert!(validate_account("bad/account").is_err());
        assert!(validate_digest(&"a".repeat(64)).is_ok());
        assert!(validate_digest(&"A".repeat(64)).is_err());
        assert!(validate_digest("short").is_err());
        assert!(validate_json("{}", 2).is_ok());
        assert!(validate_json("[]", 2).is_err());
        assert!(validate_json("{", 2).is_err());
        assert!(validate_json("{}", 1).is_err());
        assert!(validate_error_class("daily_spend_cap_breached"));
        assert!(!validate_error_class(""));
        assert!(!validate_error_class("Bad"));
        assert!(!validate_error_class(&"a".repeat(65)));
        assert!(to_i64(u64::MAX).is_err());
    }
}
