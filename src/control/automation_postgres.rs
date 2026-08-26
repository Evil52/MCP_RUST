#![expect(
    clippy::significant_drop_tightening,
    reason = "the campaign lease intentionally owns one supervised PostgreSQL session"
)]

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use sha2::{Digest, Sha256};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WbAutomationDurableActionKind {
    ChangeBids,
    PauseCampaignForDailyCap,
}

impl WbAutomationDurableActionKind {
    const fn as_database(self) -> &'static str {
        match self {
            Self::ChangeBids => "change_bids",
            Self::PauseCampaignForDailyCap => "pause_campaign_for_daily_cap",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WbAutomationDurableActionStatus {
    Reserved,
    WriteStarted,
    AwaitingReadback,
    Applied,
    ReconciliationRequired,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WbAutomationActionReservation {
    pub idempotency_key: String,
    pub cycle_id: String,
    pub policy_digest: String,
    pub request_digest: String,
    pub action_kind: WbAutomationDurableActionKind,
    pub request_json: String,
    pub business_date: NaiveDate,
    pub expected_state_revision: u64,
    pub max_actions_per_day: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WbAutomationDurableAction {
    pub idempotency_key: String,
    pub cycle_id: String,
    pub policy_digest: String,
    pub request_digest: String,
    pub action_kind: WbAutomationDurableActionKind,
    pub request_json: String,
    pub status: WbAutomationDurableActionStatus,
    pub reserved_at: DateTime<Utc>,
    pub write_started_at: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub readback_cycle_id: Option<String>,
    pub last_error_class: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WbAutomationReservationReceipt {
    pub inserted: bool,
    pub state_revision: u64,
    pub action: WbAutomationDurableAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WbAutomationStateTransitionReceipt {
    pub changed: bool,
    pub state_revision: u64,
}

#[derive(Debug)]
struct LockedStateSummary {
    business_date: NaiveDate,
    paused_for_daily_cap_on: Option<NaiveDate>,
    pending_idempotency_key: Option<String>,
    incident_class: Option<String>,
    revision: i64,
}

struct AuditEvent<'a> {
    event_key: &'a str,
    cycle_id: &'a str,
    account_id: &'a str,
    campaign_id: i64,
    event_type: &'a str,
    idempotency_key: Option<&'a str>,
    payload_json: &'a str,
}

#[derive(Debug, Clone, Copy)]
enum ResolutionKind {
    Cancelled,
    ReconciliationRequired,
}

impl ResolutionKind {
    const fn status(self) -> WbAutomationDurableActionStatus {
        match self {
            Self::Cancelled => WbAutomationDurableActionStatus::Cancelled,
            Self::ReconciliationRequired => WbAutomationDurableActionStatus::ReconciliationRequired,
        }
    }

    const fn event_type(self) -> &'static str {
        match self {
            Self::Cancelled => "action_cancelled",
            Self::ReconciliationRequired => "reconciliation_required",
        }
    }
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
            || matches!(
                seed.paused_for_daily_cap_on,
                Some(paused_on) if paused_on > seed.business_date
            )
            || matches!(
                seed.incident_class.as_deref(),
                Some(value) if !validate_error_class(value)
            )
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
            .map_err(map_insert_error)?;
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

    /// Atomically reserves one exact marketplace mutation, advances the
    /// campaign safety state and appends its audit event. Replaying the same
    /// idempotency key returns the existing reservation only when every
    /// immutable request field matches.
    pub async fn reserve_action(
        &mut self,
        reservation: &WbAutomationActionReservation,
    ) -> Result<WbAutomationReservationReceipt, WbAutomationPostgresError> {
        validate_reservation(reservation)?;
        let expected_revision = to_i64(reservation.expected_state_revision)?;
        let max_actions = i32::try_from(reservation.max_actions_per_day)
            .map_err(|_| WbAutomationPostgresError::InvalidInput)?;
        let client = self
            .client
            .as_mut()
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        let state_row = transaction
            .query_opt(
                "SELECT policy_digest, business_date, actions_today, \
                        pending_idempotency_key, incident_class, revision \
                 FROM wb_automation.execution_state \
                 WHERE account_id=$1 AND advert_id=$2 FOR UPDATE",
                &[&self.account_id, &self.campaign_id],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?
            .ok_or(WbAutomationPostgresError::StateChanged)?;
        let state_revision = state_row.get::<_, i64>(5);
        let pending = state_row.get::<_, Option<&str>>(3);
        if pending == Some(reservation.idempotency_key.as_str()) {
            let existing = match load_action_in_transaction(
                &transaction,
                &self.account_id,
                self.campaign_id,
                &reservation.idempotency_key,
            )
            .await
            {
                Ok(action) => action,
                Err(error) => return Err(error),
            };
            if action_matches_reservation(&existing, reservation) {
                transaction
                    .commit()
                    .await
                    .map_err(|_| WbAutomationPostgresError::Unavailable)?;
                return Ok(WbAutomationReservationReceipt {
                    inserted: false,
                    state_revision: u64::try_from(state_revision)
                        .map_err(|_| WbAutomationPostgresError::Unavailable)?,
                    action: existing,
                });
            }
            return Err(WbAutomationPostgresError::StateChanged);
        }
        if state_revision != expected_revision
            || state_row.get::<_, &str>(0) != reservation.policy_digest
            || pending.is_some()
            || state_row.get::<_, Option<&str>>(4).is_some()
        {
            return Err(WbAutomationPostgresError::StateChanged);
        }
        let state_date = state_row.get::<_, NaiveDate>(1);
        if reservation.business_date < state_date {
            return Err(WbAutomationPostgresError::StateChanged);
        }
        let actions_before = if reservation.business_date > state_date {
            0
        } else {
            state_row.get::<_, i32>(2)
        };
        if actions_before < 0 || actions_before >= max_actions {
            return Err(WbAutomationPostgresError::StateChanged);
        }
        let cycle_matches = transaction
            .query_opt(
                "SELECT 1 FROM wb_automation.cycles \
                 WHERE cycle_id=$1 AND account_id=$2 AND advert_id=$3 \
                   AND policy_digest=$4 AND business_date=$5 AND state_revision=$6",
                &[
                    &reservation.cycle_id,
                    &self.account_id,
                    &self.campaign_id,
                    &reservation.policy_digest,
                    &reservation.business_date,
                    &expected_revision,
                ],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?
            .is_some();
        if !cycle_matches {
            return Err(WbAutomationPostgresError::StateChanged);
        }
        let action_row = transaction
            .query_one(
                "INSERT INTO wb_automation.action_attempts (\
                    idempotency_key, account_id, advert_id, cycle_id, policy_digest, \
                    request_digest, action_kind, request_json, status\
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'reserved') \
                 RETURNING idempotency_key, cycle_id, policy_digest, request_digest, \
                    action_kind, request_json, status, reserved_at, write_started_at, \
                    resolved_at, readback_cycle_id, last_error_class",
                &[
                    &reservation.idempotency_key,
                    &self.account_id,
                    &self.campaign_id,
                    &reservation.cycle_id,
                    &reservation.policy_digest,
                    &reservation.request_digest,
                    &reservation.action_kind.as_database(),
                    &reservation.request_json,
                ],
            )
            .await
            .map_err(map_insert_error)?;
        let action = parse_action(&action_row)?;
        let new_revision = expected_revision
            .checked_add(1)
            .ok_or(WbAutomationPostgresError::InvalidInput)?;
        let updated = transaction
            .execute(
                "UPDATE wb_automation.execution_state SET \
                    business_date=$3, actions_today=$4, last_action_at=$5, \
                    pending_idempotency_key=$6, revision=$7 \
                 WHERE account_id=$1 AND advert_id=$2 AND revision=$8",
                &[
                    &self.account_id,
                    &self.campaign_id,
                    &reservation.business_date,
                    &(actions_before + 1),
                    &action.reserved_at,
                    &reservation.idempotency_key,
                    &new_revision,
                    &expected_revision,
                ],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        ensure_one_row(updated)?;
        insert_audit_event(
            &transaction,
            &AuditEvent {
                event_key: &audit_event_key(&reservation.idempotency_key, "reserved", ""),
                cycle_id: &reservation.cycle_id,
                account_id: &self.account_id,
                campaign_id: self.campaign_id,
                event_type: "action_reserved",
                idempotency_key: Some(&reservation.idempotency_key),
                payload_json: "{}",
            },
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        Ok(WbAutomationReservationReceipt {
            inserted: true,
            state_revision: u64::try_from(new_revision)
                .map_err(|_| WbAutomationPostgresError::Unavailable)?,
            action,
        })
    }

    pub async fn mark_write_started(
        &mut self,
        idempotency_key: &str,
        expected_state_revision: u64,
    ) -> Result<bool, WbAutomationPostgresError> {
        self.transition_without_state_change(
            idempotency_key,
            expected_state_revision,
            WbAutomationDurableActionStatus::Reserved,
            WbAutomationDurableActionStatus::WriteStarted,
            "write_started",
        )
        .await
    }

    pub async fn mark_awaiting_readback(
        &mut self,
        idempotency_key: &str,
        expected_state_revision: u64,
    ) -> Result<bool, WbAutomationPostgresError> {
        self.transition_without_state_change(
            idempotency_key,
            expected_state_revision,
            WbAutomationDurableActionStatus::WriteStarted,
            WbAutomationDurableActionStatus::AwaitingReadback,
            "awaiting_readback",
        )
        .await
    }

    pub async fn cancel_reserved(
        &mut self,
        idempotency_key: &str,
        expected_state_revision: u64,
        error_class: &str,
    ) -> Result<WbAutomationStateTransitionReceipt, WbAutomationPostgresError> {
        self.resolve_without_write(
            idempotency_key,
            expected_state_revision,
            error_class,
            ResolutionKind::Cancelled,
        )
        .await
    }

    pub async fn mark_reconciliation_required(
        &mut self,
        idempotency_key: &str,
        expected_state_revision: u64,
        error_class: &str,
    ) -> Result<WbAutomationStateTransitionReceipt, WbAutomationPostgresError> {
        self.resolve_without_write(
            idempotency_key,
            expected_state_revision,
            error_class,
            ResolutionKind::ReconciliationRequired,
        )
        .await
    }

    async fn resolve_without_write(
        &mut self,
        idempotency_key: &str,
        expected_state_revision: u64,
        error_class: &str,
        resolution: ResolutionKind,
    ) -> Result<WbAutomationStateTransitionReceipt, WbAutomationPostgresError> {
        validate_digest(idempotency_key)?;
        if !validate_error_class(error_class) {
            return Err(WbAutomationPostgresError::InvalidInput);
        }
        let target_status = resolution.status();
        let event_type = resolution.event_type();
        let expected_revision = to_i64(expected_state_revision)?;
        let client = self
            .client
            .as_mut()
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        let state =
            load_locked_state_summary(&transaction, &self.account_id, self.campaign_id).await?;
        let action = match load_action_in_transaction(
            &transaction,
            &self.account_id,
            self.campaign_id,
            idempotency_key,
        )
        .await
        {
            Ok(action) => action,
            Err(error) => return Err(error),
        };
        if action.status == target_status {
            let replay_revision = expected_revision
                .checked_add(1)
                .ok_or(WbAutomationPostgresError::InvalidInput)?;
            let replay_matches = action.last_error_class.as_deref() == Some(error_class)
                && state.revision == replay_revision
                && match resolution {
                    ResolutionKind::Cancelled => state.pending_idempotency_key.is_none(),
                    ResolutionKind::ReconciliationRequired => {
                        state.pending_idempotency_key.as_deref() == Some(idempotency_key)
                            && state.incident_class.as_deref() == Some(error_class)
                    }
                };
            if !replay_matches {
                return Err(WbAutomationPostgresError::StateChanged);
            }
            transaction
                .commit()
                .await
                .map_err(|_| WbAutomationPostgresError::Unavailable)?;
            return Ok(WbAutomationStateTransitionReceipt {
                changed: false,
                state_revision: u64::try_from(replay_revision)
                    .map_err(|_| WbAutomationPostgresError::Unavailable)?,
            });
        }
        if state.revision != expected_revision
            || state.pending_idempotency_key.as_deref() != Some(idempotency_key)
        {
            return Err(WbAutomationPostgresError::StateChanged);
        }
        let valid_source = match resolution {
            ResolutionKind::Cancelled => action.status == WbAutomationDurableActionStatus::Reserved,
            ResolutionKind::ReconciliationRequired => [
                WbAutomationDurableActionStatus::WriteStarted,
                WbAutomationDurableActionStatus::AwaitingReadback,
            ]
            .contains(&action.status),
        };
        if !valid_source {
            return Err(WbAutomationPostgresError::StateChanged);
        }
        let updated = transaction
            .execute(
                "UPDATE wb_automation.action_attempts \
                 SET status=$4, last_error_class=$5 \
                 WHERE idempotency_key=$1 AND account_id=$2 AND advert_id=$3 AND status=$6",
                &[
                    &idempotency_key,
                    &self.account_id,
                    &self.campaign_id,
                    &status_database(target_status),
                    &error_class,
                    &status_database(action.status),
                ],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        ensure_one_row(updated)?;
        let new_revision = expected_revision
            .checked_add(1)
            .ok_or(WbAutomationPostgresError::InvalidInput)?;
        let state_updated = match resolution {
            ResolutionKind::Cancelled => {
                transaction
                    .execute(
                        "UPDATE wb_automation.execution_state \
                         SET pending_idempotency_key=NULL, revision=$3 \
                         WHERE account_id=$1 AND advert_id=$2 AND revision=$4",
                        &[
                            &self.account_id,
                            &self.campaign_id,
                            &new_revision,
                            &expected_revision,
                        ],
                    )
                    .await
            }
            ResolutionKind::ReconciliationRequired => {
                transaction
                    .execute(
                        "UPDATE wb_automation.execution_state \
                         SET incident_class=$3, revision=$4 \
                         WHERE account_id=$1 AND advert_id=$2 AND revision=$5",
                        &[
                            &self.account_id,
                            &self.campaign_id,
                            &error_class,
                            &new_revision,
                            &expected_revision,
                        ],
                    )
                    .await
            }
        }
        .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        ensure_one_row(state_updated)?;
        let payload = serde_json::to_string(&serde_json::json!({
            "error_class": error_class,
        }))
        .map_err(|_| WbAutomationPostgresError::InvalidInput)?;
        insert_audit_event(
            &transaction,
            &AuditEvent {
                event_key: &audit_event_key(idempotency_key, event_type, error_class),
                cycle_id: &action.cycle_id,
                account_id: &self.account_id,
                campaign_id: self.campaign_id,
                event_type,
                idempotency_key: Some(idempotency_key),
                payload_json: &payload,
            },
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        Ok(WbAutomationStateTransitionReceipt {
            changed: true,
            state_revision: u64::try_from(new_revision)
                .map_err(|_| WbAutomationPostgresError::Unavailable)?,
        })
    }

    pub async fn mark_applied(
        &mut self,
        idempotency_key: &str,
        expected_state_revision: u64,
        readback_cycle_id: &str,
        paused_for_daily_cap_on: Option<NaiveDate>,
    ) -> Result<WbAutomationStateTransitionReceipt, WbAutomationPostgresError> {
        validate_digest(idempotency_key)?;
        validate_digest(readback_cycle_id)?;
        let expected_revision = to_i64(expected_state_revision)?;
        let client = self
            .client
            .as_mut()
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        let state =
            load_locked_state_summary(&transaction, &self.account_id, self.campaign_id).await?;
        let action = match load_action_in_transaction(
            &transaction,
            &self.account_id,
            self.campaign_id,
            idempotency_key,
        )
        .await
        {
            Ok(action) => action,
            Err(error) => return Err(error),
        };
        let pause_shape_matches = match action.action_kind {
            WbAutomationDurableActionKind::ChangeBids => paused_for_daily_cap_on.is_none(),
            WbAutomationDurableActionKind::PauseCampaignForDailyCap => {
                matches!(paused_for_daily_cap_on, Some(date) if date <= state.business_date)
            }
        };
        if !pause_shape_matches {
            return Err(WbAutomationPostgresError::InvalidInput);
        }
        if action.status == WbAutomationDurableActionStatus::Applied {
            let replay_revision = expected_revision
                .checked_add(1)
                .ok_or(WbAutomationPostgresError::InvalidInput)?;
            if action.readback_cycle_id.as_deref() != Some(readback_cycle_id)
                || state.revision != replay_revision
                || state.pending_idempotency_key.is_some()
                || (action.action_kind == WbAutomationDurableActionKind::PauseCampaignForDailyCap
                    && state.paused_for_daily_cap_on != paused_for_daily_cap_on)
            {
                return Err(WbAutomationPostgresError::StateChanged);
            }
            transaction
                .commit()
                .await
                .map_err(|_| WbAutomationPostgresError::Unavailable)?;
            return Ok(WbAutomationStateTransitionReceipt {
                changed: false,
                state_revision: u64::try_from(replay_revision)
                    .map_err(|_| WbAutomationPostgresError::Unavailable)?,
            });
        }
        if state.revision != expected_revision
            || state.pending_idempotency_key.as_deref() != Some(idempotency_key)
            || !matches!(
                action.status,
                WbAutomationDurableActionStatus::WriteStarted
                    | WbAutomationDurableActionStatus::AwaitingReadback
                    | WbAutomationDurableActionStatus::ReconciliationRequired
            )
        {
            return Err(WbAutomationPostgresError::StateChanged);
        }
        let readback_matches = transaction
            .query_opt(
                "SELECT 1 FROM wb_automation.cycles \
                 WHERE cycle_id=$1 AND account_id=$2 AND advert_id=$3 AND policy_digest=$4 \
                   AND cycle_id<>$5 AND state_revision=$6",
                &[
                    &readback_cycle_id,
                    &self.account_id,
                    &self.campaign_id,
                    &action.policy_digest,
                    &action.cycle_id,
                    &expected_revision,
                ],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?
            .is_some();
        if !readback_matches {
            return Err(WbAutomationPostgresError::StateChanged);
        }
        let updated = transaction
            .execute(
                "UPDATE wb_automation.action_attempts \
                 SET status='applied', readback_cycle_id=$4, last_error_class=NULL \
                 WHERE idempotency_key=$1 AND account_id=$2 AND advert_id=$3 AND status=$5",
                &[
                    &idempotency_key,
                    &self.account_id,
                    &self.campaign_id,
                    &readback_cycle_id,
                    &status_database(action.status),
                ],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        ensure_one_row(updated)?;
        let new_revision = expected_revision
            .checked_add(1)
            .ok_or(WbAutomationPostgresError::InvalidInput)?;
        let state_updated = match action.action_kind {
            WbAutomationDurableActionKind::ChangeBids => {
                transaction
                    .execute(
                        "UPDATE wb_automation.execution_state \
                         SET pending_idempotency_key=NULL, revision=$3 \
                         WHERE account_id=$1 AND advert_id=$2 AND revision=$4",
                        &[
                            &self.account_id,
                            &self.campaign_id,
                            &new_revision,
                            &expected_revision,
                        ],
                    )
                    .await
            }
            WbAutomationDurableActionKind::PauseCampaignForDailyCap => {
                transaction
                    .execute(
                        "UPDATE wb_automation.execution_state \
                         SET pending_idempotency_key=NULL, paused_for_daily_cap_on=$3, revision=$4 \
                         WHERE account_id=$1 AND advert_id=$2 AND revision=$5",
                        &[
                            &self.account_id,
                            &self.campaign_id,
                            &paused_for_daily_cap_on,
                            &new_revision,
                            &expected_revision,
                        ],
                    )
                    .await
            }
        }
        .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        ensure_one_row(state_updated)?;
        let payload = serde_json::to_string(&serde_json::json!({
            "readback_cycle_id": readback_cycle_id,
        }))
        .map_err(|_| WbAutomationPostgresError::InvalidInput)?;
        insert_audit_event(
            &transaction,
            &AuditEvent {
                event_key: &audit_event_key(idempotency_key, "applied", readback_cycle_id),
                cycle_id: readback_cycle_id,
                account_id: &self.account_id,
                campaign_id: self.campaign_id,
                event_type: "action_applied",
                idempotency_key: Some(idempotency_key),
                payload_json: &payload,
            },
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        Ok(WbAutomationStateTransitionReceipt {
            changed: true,
            state_revision: u64::try_from(new_revision)
                .map_err(|_| WbAutomationPostgresError::Unavailable)?,
        })
    }

    async fn transition_without_state_change(
        &mut self,
        idempotency_key: &str,
        expected_state_revision: u64,
        from: WbAutomationDurableActionStatus,
        to: WbAutomationDurableActionStatus,
        event_suffix: &str,
    ) -> Result<bool, WbAutomationPostgresError> {
        validate_digest(idempotency_key)?;
        let expected_revision = to_i64(expected_state_revision)?;
        let client = self
            .client
            .as_mut()
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        verify_pending_state(
            &transaction,
            &self.account_id,
            self.campaign_id,
            idempotency_key,
            expected_revision,
        )
        .await?;
        let action = match load_action_in_transaction(
            &transaction,
            &self.account_id,
            self.campaign_id,
            idempotency_key,
        )
        .await
        {
            Ok(action) => action,
            Err(error) => return Err(error),
        };
        if action.status == to {
            transaction
                .commit()
                .await
                .map_err(|_| WbAutomationPostgresError::Unavailable)?;
            return Ok(false);
        }
        if action.status != from {
            return Err(WbAutomationPostgresError::StateChanged);
        }
        let updated = transaction
            .execute(
                "UPDATE wb_automation.action_attempts SET status=$4 \
                 WHERE idempotency_key=$1 AND account_id=$2 AND advert_id=$3 AND status=$5",
                &[
                    &idempotency_key,
                    &self.account_id,
                    &self.campaign_id,
                    &status_database(to),
                    &status_database(from),
                ],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        ensure_one_row(updated)?;
        insert_audit_event(
            &transaction,
            &AuditEvent {
                event_key: &audit_event_key(idempotency_key, event_suffix, ""),
                cycle_id: &action.cycle_id,
                account_id: &self.account_id,
                campaign_id: self.campaign_id,
                event_type: event_suffix,
                idempotency_key: Some(idempotency_key),
                payload_json: "{}",
            },
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        Ok(true)
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

fn validate_reservation(
    reservation: &WbAutomationActionReservation,
) -> Result<(), WbAutomationPostgresError> {
    validate_digest(&reservation.idempotency_key)?;
    validate_digest(&reservation.cycle_id)?;
    validate_digest(&reservation.policy_digest)?;
    validate_digest(&reservation.request_digest)?;
    if reservation.expected_state_revision == 0
        || reservation.max_actions_per_day == 0
        || reservation.max_actions_per_day > 500
    {
        return Err(WbAutomationPostgresError::InvalidInput);
    }
    let object = validate_json(&reservation.request_json, 128 * 1024)?;
    if object.get("kind").and_then(serde_json::Value::as_str)
        != Some(reservation.action_kind.as_database())
    {
        return Err(WbAutomationPostgresError::InvalidInput);
    }
    match reservation.action_kind {
        WbAutomationDurableActionKind::ChangeBids => {
            if object
                .get("changes")
                .and_then(serde_json::Value::as_array)
                .is_none_or(|changes| changes.len() != 1)
            {
                return Err(WbAutomationPostgresError::InvalidInput);
            }
        }
        WbAutomationDurableActionKind::PauseCampaignForDailyCap => {
            if object.contains_key("changes") {
                return Err(WbAutomationPostgresError::InvalidInput);
            }
        }
    }
    Ok(())
}

async fn load_action_in_transaction(
    transaction: &tokio_postgres::Transaction<'_>,
    account_id: &str,
    campaign_id: i64,
    idempotency_key: &str,
) -> Result<WbAutomationDurableAction, WbAutomationPostgresError> {
    let row = transaction
        .query_opt(
            "SELECT idempotency_key, cycle_id, policy_digest, request_digest, \
                    action_kind, request_json, status, reserved_at, write_started_at, \
                    resolved_at, readback_cycle_id, last_error_class \
             FROM wb_automation.action_attempts \
             WHERE idempotency_key=$1 AND account_id=$2 AND advert_id=$3 FOR UPDATE",
            &[&idempotency_key, &account_id, &campaign_id],
        )
        .await
        .map_err(|_| WbAutomationPostgresError::Unavailable)?
        .ok_or(WbAutomationPostgresError::StateChanged)?;
    parse_action(&row)
}

async fn load_locked_state_summary(
    transaction: &tokio_postgres::Transaction<'_>,
    account_id: &str,
    campaign_id: i64,
) -> Result<LockedStateSummary, WbAutomationPostgresError> {
    let row = transaction
        .query_opt(
            "SELECT business_date, paused_for_daily_cap_on, pending_idempotency_key, \
                    incident_class, revision \
             FROM wb_automation.execution_state \
             WHERE account_id=$1 AND advert_id=$2 FOR UPDATE",
            &[&account_id, &campaign_id],
        )
        .await
        .map_err(|_| WbAutomationPostgresError::Unavailable)?
        .ok_or(WbAutomationPostgresError::StateChanged)?;
    Ok(LockedStateSummary {
        business_date: row.get(0),
        paused_for_daily_cap_on: row.get(1),
        pending_idempotency_key: row.get(2),
        incident_class: row.get(3),
        revision: row.get(4),
    })
}

async fn verify_pending_state(
    transaction: &tokio_postgres::Transaction<'_>,
    account_id: &str,
    campaign_id: i64,
    idempotency_key: &str,
    expected_revision: i64,
) -> Result<(), WbAutomationPostgresError> {
    let state = transaction
        .query_opt(
            "SELECT revision, pending_idempotency_key \
             FROM wb_automation.execution_state \
             WHERE account_id=$1 AND advert_id=$2 FOR UPDATE",
            &[&account_id, &campaign_id],
        )
        .await
        .map_err(|_| WbAutomationPostgresError::Unavailable)?
        .ok_or(WbAutomationPostgresError::StateChanged)?;
    if state.get::<_, i64>(0) != expected_revision
        || state.get::<_, Option<&str>>(1) != Some(idempotency_key)
    {
        return Err(WbAutomationPostgresError::StateChanged);
    }
    Ok(())
}

async fn insert_audit_event(
    transaction: &tokio_postgres::Transaction<'_>,
    event: &AuditEvent<'_>,
) -> Result<(), WbAutomationPostgresError> {
    validate_digest(event.event_key)?;
    validate_digest(event.cycle_id)?;
    validate_json(event.payload_json, 256 * 1024)?;
    transaction
        .execute(
            "INSERT INTO wb_automation.audit_events (\
                event_key, cycle_id, account_id, advert_id, event_type, \
                idempotency_key, payload_json\
             ) VALUES ($1,$2,$3,$4,$5,$6,$7) \
             ON CONFLICT (event_key) DO NOTHING",
            &[
                &event.event_key,
                &event.cycle_id,
                &event.account_id,
                &event.campaign_id,
                &event.event_type,
                &event.idempotency_key,
                &event.payload_json,
            ],
        )
        .await
        .map_err(|_| WbAutomationPostgresError::Unavailable)?;
    Ok(())
}

fn audit_event_key(idempotency_key: &str, event_type: &str, detail: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"wb-automation-audit-v1");
    digest.update([0]);
    digest.update(idempotency_key.as_bytes());
    digest.update([0]);
    digest.update(event_type.as_bytes());
    digest.update([0]);
    digest.update(detail.as_bytes());
    let mut output = String::with_capacity(64);
    for byte in digest.finalize() {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

fn action_matches_reservation(
    action: &WbAutomationDurableAction,
    reservation: &WbAutomationActionReservation,
) -> bool {
    let stored_json = serde_json::from_str::<serde_json::Value>(&action.request_json);
    let requested_json = serde_json::from_str::<serde_json::Value>(&reservation.request_json);
    let json_matches = matches!(
        (stored_json, requested_json),
        (Ok(stored), Ok(requested)) if stored == requested
    );
    action.idempotency_key == reservation.idempotency_key
        && action.cycle_id == reservation.cycle_id
        && action.policy_digest == reservation.policy_digest
        && action.request_digest == reservation.request_digest
        && action.action_kind == reservation.action_kind
        && json_matches
}

fn parse_action(
    row: &tokio_postgres::Row,
) -> Result<WbAutomationDurableAction, WbAutomationPostgresError> {
    Ok(WbAutomationDurableAction {
        idempotency_key: row.get(0),
        cycle_id: row.get(1),
        policy_digest: row.get(2),
        request_digest: row.get(3),
        action_kind: parse_action_kind(row.get(4))?,
        request_json: row.get(5),
        status: parse_action_status(row.get(6))?,
        reserved_at: row.get(7),
        write_started_at: row.get(8),
        resolved_at: row.get(9),
        readback_cycle_id: row.get(10),
        last_error_class: row.get(11),
    })
}

fn parse_action_kind(
    value: &str,
) -> Result<WbAutomationDurableActionKind, WbAutomationPostgresError> {
    match value {
        "change_bids" => Ok(WbAutomationDurableActionKind::ChangeBids),
        "pause_campaign_for_daily_cap" => {
            Ok(WbAutomationDurableActionKind::PauseCampaignForDailyCap)
        }
        _ => Err(WbAutomationPostgresError::Unavailable),
    }
}

fn parse_action_status(
    value: &str,
) -> Result<WbAutomationDurableActionStatus, WbAutomationPostgresError> {
    match value {
        "reserved" => Ok(WbAutomationDurableActionStatus::Reserved),
        "write_started" => Ok(WbAutomationDurableActionStatus::WriteStarted),
        "awaiting_readback" => Ok(WbAutomationDurableActionStatus::AwaitingReadback),
        "applied" => Ok(WbAutomationDurableActionStatus::Applied),
        "reconciliation_required" => Ok(WbAutomationDurableActionStatus::ReconciliationRequired),
        "cancelled" => Ok(WbAutomationDurableActionStatus::Cancelled),
        _ => Err(WbAutomationPostgresError::Unavailable),
    }
}

const fn status_database(status: WbAutomationDurableActionStatus) -> &'static str {
    match status {
        WbAutomationDurableActionStatus::Reserved => "reserved",
        WbAutomationDurableActionStatus::WriteStarted => "write_started",
        WbAutomationDurableActionStatus::AwaitingReadback => "awaiting_readback",
        WbAutomationDurableActionStatus::Applied => "applied",
        WbAutomationDurableActionStatus::ReconciliationRequired => "reconciliation_required",
        WbAutomationDurableActionStatus::Cancelled => "cancelled",
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

fn validate_json(
    value: &str,
    limit: usize,
) -> Result<serde_json::Map<String, serde_json::Value>, WbAutomationPostgresError> {
    if value.len() < 2 || value.len() > limit {
        return Err(WbAutomationPostgresError::InvalidInput);
    }
    match serde_json::from_str::<serde_json::Value>(value) {
        Ok(serde_json::Value::Object(object)) => Ok(object),
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

fn ensure_one_row(updated: u64) -> Result<(), WbAutomationPostgresError> {
    (updated == 1)
        .then_some(())
        .ok_or(WbAutomationPostgresError::StateChanged)
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "a function item keeps async call sites free of per-binary coverage-only closures"
)]
fn map_insert_error(error: tokio_postgres::Error) -> WbAutomationPostgresError {
    match error.as_db_error() {
        Some(database) if database.code() == &SqlState::UNIQUE_VIOLATION => {
            WbAutomationPostgresError::StateChanged
        }
        _ => WbAutomationPostgresError::Unavailable,
    }
}

#[cfg(coverage)]
#[doc(hidden)]
pub fn exercise_coverage_only_database_mappings() {
    assert_eq!(
        parse_action_kind("change_bids"),
        Ok(WbAutomationDurableActionKind::ChangeBids)
    );
    assert_eq!(
        parse_action_kind("pause_campaign_for_daily_cap"),
        Ok(WbAutomationDurableActionKind::PauseCampaignForDailyCap)
    );
    assert_eq!(
        parse_action_kind("unknown"),
        Err(WbAutomationPostgresError::Unavailable)
    );
    for (status, database) in [
        (WbAutomationDurableActionStatus::Reserved, "reserved"),
        (
            WbAutomationDurableActionStatus::WriteStarted,
            "write_started",
        ),
        (
            WbAutomationDurableActionStatus::AwaitingReadback,
            "awaiting_readback",
        ),
        (WbAutomationDurableActionStatus::Applied, "applied"),
        (
            WbAutomationDurableActionStatus::ReconciliationRequired,
            "reconciliation_required",
        ),
        (WbAutomationDurableActionStatus::Cancelled, "cancelled"),
    ] {
        assert_eq!(status_database(status), database);
    }
    let mut reservation = WbAutomationActionReservation {
        idempotency_key: "1".repeat(64),
        cycle_id: "2".repeat(64),
        policy_digest: "3".repeat(64),
        request_digest: "4".repeat(64),
        action_kind: WbAutomationDurableActionKind::ChangeBids,
        request_json: "{\"kind\":\"change_bids\",\"changes\":[{}]}".to_owned(),
        business_date: NaiveDate::from_ymd_opt(2026, 8, 26).expect("valid coverage date"),
        expected_state_revision: 1,
        max_actions_per_day: 2,
    };
    assert_eq!(validate_reservation(&reservation), Ok(()));
    reservation.expected_state_revision = 0;
    assert_eq!(
        validate_reservation(&reservation),
        Err(WbAutomationPostgresError::InvalidInput)
    );
    reservation.expected_state_revision = 1;
    reservation.max_actions_per_day = 0;
    assert_eq!(
        validate_reservation(&reservation),
        Err(WbAutomationPostgresError::InvalidInput)
    );
    reservation.max_actions_per_day = 501;
    assert_eq!(
        validate_reservation(&reservation),
        Err(WbAutomationPostgresError::InvalidInput)
    );
    reservation.max_actions_per_day = 2;
    reservation.request_json = "{".to_owned();
    assert_eq!(
        validate_reservation(&reservation),
        Err(WbAutomationPostgresError::InvalidInput)
    );
    reservation.request_json = "{\"kind\":\"pause_campaign_for_daily_cap\"}".to_owned();
    assert_eq!(
        validate_reservation(&reservation),
        Err(WbAutomationPostgresError::InvalidInput)
    );
    reservation.request_json = "{\"kind\":\"change_bids\",\"changes\":[]}".to_owned();
    assert_eq!(
        validate_reservation(&reservation),
        Err(WbAutomationPostgresError::InvalidInput)
    );
    reservation.action_kind = WbAutomationDurableActionKind::PauseCampaignForDailyCap;
    reservation.request_json = "{\"kind\":\"pause_campaign_for_daily_cap\"}".to_owned();
    assert_eq!(validate_reservation(&reservation), Ok(()));
    reservation.request_json =
        "{\"kind\":\"pause_campaign_for_daily_cap\",\"changes\":[]}".to_owned();
    assert_eq!(
        validate_reservation(&reservation),
        Err(WbAutomationPostgresError::InvalidInput)
    );
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
        assert!(ensure_one_row(1).is_ok());
        assert_eq!(
            ensure_one_row(0),
            Err(WbAutomationPostgresError::StateChanged)
        );
        assert_eq!(
            parse_action_kind("unknown"),
            Err(WbAutomationPostgresError::Unavailable)
        );
        assert_eq!(
            parse_action_status("unknown"),
            Err(WbAutomationPostgresError::Unavailable)
        );
        assert_eq!(
            status_database(WbAutomationDurableActionStatus::Applied),
            "applied"
        );
    }

    #[test]
    fn action_reservation_shape_is_exact() {
        let mut reservation = WbAutomationActionReservation {
            idempotency_key: "1".repeat(64),
            cycle_id: "2".repeat(64),
            policy_digest: "3".repeat(64),
            request_digest: "4".repeat(64),
            action_kind: WbAutomationDurableActionKind::ChangeBids,
            request_json: "{\"kind\":\"change_bids\",\"changes\":[{}]}".to_owned(),
            business_date: NaiveDate::from_ymd_opt(2026, 8, 26).expect("valid date"),
            expected_state_revision: 1,
            max_actions_per_day: 2,
        };
        assert!(validate_reservation(&reservation).is_ok());

        reservation.expected_state_revision = 0;
        assert_eq!(
            validate_reservation(&reservation),
            Err(WbAutomationPostgresError::InvalidInput)
        );
        reservation.expected_state_revision = 1;
        reservation.request_json = "{\"kind\":\"pause_campaign_for_daily_cap\"}".to_owned();
        assert_eq!(
            validate_reservation(&reservation),
            Err(WbAutomationPostgresError::InvalidInput)
        );
        reservation.request_json = "{\"kind\":\"change_bids\",\"changes\":[]}".to_owned();
        assert_eq!(
            validate_reservation(&reservation),
            Err(WbAutomationPostgresError::InvalidInput)
        );
        reservation.action_kind = WbAutomationDurableActionKind::PauseCampaignForDailyCap;
        reservation.request_json =
            "{\"kind\":\"pause_campaign_for_daily_cap\",\"changes\":[]}".to_owned();
        assert_eq!(
            validate_reservation(&reservation),
            Err(WbAutomationPostgresError::InvalidInput)
        );
    }
}
