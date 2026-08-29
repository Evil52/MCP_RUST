#![expect(
    clippy::significant_drop_tightening,
    reason = "PostgreSQL transactions borrow the supervised session guard until commit"
)]

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use tokio_postgres::{Config, Row, Transaction, error::SqlState};

#[cfg(test)]
use tokio_postgres::Client;

use crate::{
    control::wb::{
        WbBidChange, WbCampaignBidSnapshot, WbPreparedBidChange, snapshot_matches_plan_state,
    },
    postgres::SupervisedClient,
};

use super::{
    model::{
        PlanStoreError, WbActionQuota, WbApplyContext, WbControlPlan, WbPlanApproval, WbPlanFinish,
        WbPlanStatus, WbPrepareReservation,
    },
    validation::{
        cumulative_abs_delta, make_approval_id, make_plan_digest, make_plan_id,
        make_prepare_reservation_id, map_prepare_insert_error, validate_actor_or_account,
        validate_approval_reason, validate_digest, validate_plan_id,
    },
};

pub(super) const PLAN_TTL: Duration = Duration::minutes(5);
const APPROVAL_TTL: Duration = Duration::minutes(2);
const PREPARE_RESERVATION_TTL: Duration = Duration::minutes(2);
pub(super) const STALE_APPLY_AFTER: Duration = Duration::minutes(3);
const COMPONENT: &str = "mcp-ozon-control-writer";
const VERIFY_RUNTIME_CONTRACT_SQL: &str = include_str!("verify_runtime_contract.sql");

const PLAN_SELECT: &str = "SELECT p.plan_id, p.plan_digest, p.actor_id, p.account_id, p.advert_id, \
            p.schema_version, p.policy_revision, p.policy_digest, \
            p.quota_max_actions_per_hour, p.quota_max_actions_per_day, \
            p.quota_cooldown_seconds, \
            p.quota_max_cumulative_abs_delta_kopecks_per_day, p.status, \
            p.requested_json, p.changes_json, p.before_json, p.created_at, \
            p.expires_at, p.apply_started_at, p.last_error_class, \
            p.write_response_json, p.readback_json, p.prepare_reservation_id, \
            a.approval_id, a.approver_id, a.reason, a.approved_at, a.expires_at \
     FROM control.wb_plans p \
     LEFT JOIN control.wb_plan_approvals a ON a.plan_id = p.plan_id";

#[derive(Clone)]
pub struct WbPlanRepository {
    client: Arc<SupervisedClient>,
}

impl WbPlanRepository {
    pub async fn connect(config: &Config) -> Result<Self, PlanStoreError> {
        let client = SupervisedClient::connect(config, COMPONENT)
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        Ok(Self {
            client: Arc::new(client),
        })
    }

    #[must_use]
    #[cfg(test)]
    pub(super) fn from_client(client: Client) -> Self {
        Self {
            client: Arc::new(SupervisedClient::preconnected(client, COMPONENT)),
        }
    }

    pub async fn verify_runtime_contract(&self) -> Result<(), PlanStoreError> {
        self.client
            .verify_session_bounds()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let row = client
            .query_one(VERIFY_RUNTIME_CONTRACT_SQL, &[])
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        if row.get::<_, bool>(0) {
            Ok(())
        } else {
            Err(PlanStoreError::Unavailable)
        }
    }

    /// Confirms that the process-owned supervised database session can still
    /// complete a round trip. It performs no marketplace request or write.
    pub async fn probe(&self) -> Result<(), PlanStoreError> {
        self.client
            .probe()
            .await
            .map_err(|_| PlanStoreError::Unavailable)
    }

    /// Registers the next immutable policy identity. Re-registering the exact
    /// current identity is idempotent; rollback and revision reuse fail closed.
    pub async fn register_policy(
        &self,
        schema_version: u32,
        policy_revision: u64,
        policy_digest: &str,
        _now: DateTime<Utc>,
    ) -> Result<(), PlanStoreError> {
        validate_digest(policy_digest)?;
        let schema_version_i32 =
            i32::try_from(schema_version).map_err(|_| PlanStoreError::InvalidPlan)?;
        let policy_revision_i64 =
            i64::try_from(policy_revision).map_err(|_| PlanStoreError::InvalidPlan)?;
        if schema_version == 0 || policy_revision == 0 {
            return Err(PlanStoreError::InvalidPlan);
        }
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended('wb/policy-revision', 0))",
                &[],
            )
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let highest = transaction
            .query_opt(
                "SELECT policy_revision, schema_version, policy_digest \
                 FROM control.wb_policy_revisions \
                 ORDER BY policy_revision DESC LIMIT 1",
                &[],
            )
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        if let Some(highest) = highest {
            let highest_revision: i64 = highest.get(0);
            if policy_revision_i64 < highest_revision {
                return Err(PlanStoreError::PolicyChanged);
            }
            if policy_revision_i64 == highest_revision {
                let matches = highest.get::<_, i32>(1) == schema_version_i32
                    && highest.get::<_, &str>(2) == policy_digest;
                transaction
                    .commit()
                    .await
                    .map_err(|_| PlanStoreError::Unavailable)?;
                return if matches {
                    Ok(())
                } else {
                    Err(PlanStoreError::PolicyChanged)
                };
            }
        }
        let database_now = database_now(&transaction).await?;
        transaction
            .execute(
                "INSERT INTO control.wb_policy_revisions \
                    (policy_revision, schema_version, policy_digest, registered_at) \
                 VALUES ($1,$2,$3,$4)",
                &[
                    &policy_revision_i64,
                    &schema_version_i32,
                    &policy_digest,
                    &database_now,
                ],
            )
            .await
            .map_err(|error| {
                if error
                    .as_db_error()
                    .is_some_and(|db| db.code() == &SqlState::UNIQUE_VIOLATION)
                {
                    PlanStoreError::PolicyChanged
                } else {
                    PlanStoreError::Unavailable
                }
            })?;
        transaction
            .commit()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        Ok(())
    }

    /// Reserves the bounded read-side attempt before any WB campaign-details
    /// request. The append-only reservation expires quickly and one matching
    /// reservation can create at most one plan.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::control) async fn reserve_prepare_attempt(
        &self,
        actor_id: &str,
        account_id: &str,
        advert_id: u64,
        schema_version: u32,
        policy_revision: u64,
        policy_digest: &str,
        action_quota: WbActionQuota,
        _now: DateTime<Utc>,
    ) -> Result<WbPrepareReservation, PlanStoreError> {
        validate_actor_or_account(actor_id)?;
        validate_actor_or_account(account_id)?;
        validate_digest(policy_digest)?;
        action_quota.validate()?;
        if advert_id == 0 || schema_version == 0 || policy_revision == 0 {
            return Err(PlanStoreError::InvalidPlan);
        }
        let advert_id_i64 = i64::try_from(advert_id).map_err(|_| PlanStoreError::InvalidPlan)?;
        let schema_version_i32 =
            i32::try_from(schema_version).map_err(|_| PlanStoreError::InvalidPlan)?;
        let policy_revision_i64 =
            i64::try_from(policy_revision).map_err(|_| PlanStoreError::InvalidPlan)?;
        let max_actions_per_hour = i32::try_from(action_quota.max_actions_per_hour)
            .map_err(|_| PlanStoreError::InvalidPlan)?;
        let max_actions_per_day = i32::try_from(action_quota.max_actions_per_day)
            .map_err(|_| PlanStoreError::InvalidPlan)?;
        let cooldown_seconds = i32::try_from(action_quota.cooldown_seconds)
            .map_err(|_| PlanStoreError::InvalidPlan)?;
        let max_cumulative_delta =
            i64::try_from(action_quota.max_cumulative_abs_delta_kopecks_per_day)
                .map_err(|_| PlanStoreError::InvalidPlan)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        require_active_policy(&transaction, schema_version, policy_revision, policy_digest).await?;
        lock_prepare_actor(&transaction, actor_id).await?;
        lock_campaign(&transaction, account_id, advert_id).await?;
        if campaign_has_incident(&transaction, account_id, advert_id, None).await? {
            return Err(PlanStoreError::CampaignLocked);
        }
        let database_now = database_now(&transaction).await?;
        let hour_start = database_now - Duration::hours(1);
        let counts = transaction
            .query_one(
                "SELECT \
                    (SELECT count(*) FROM control.wb_prepare_reservations \
                     WHERE actor_id=$1 AND reserved_at>$4)::bigint, \
                    (SELECT count(*) FROM control.wb_prepare_reservations \
                     WHERE account_id=$2 AND advert_id=$3 AND reserved_at>$4)::bigint, \
                    ( \
                        (SELECT count(*) FROM control.wb_plans \
                         WHERE account_id=$2 AND advert_id=$3 \
                           AND status IN ('prepared','approved') AND expires_at>$5) \
                        + \
                        (SELECT count(*) FROM control.wb_prepare_reservations pending \
                         WHERE pending.account_id=$2 AND pending.advert_id=$3 \
                           AND pending.expires_at>$5 AND NOT EXISTS ( \
                             SELECT 1 FROM control.wb_plans plan \
                             WHERE plan.prepare_reservation_id=pending.reservation_id \
                           )) \
                    )::bigint",
                &[
                    &actor_id,
                    &account_id,
                    &advert_id_i64,
                    &hour_start,
                    &database_now,
                ],
            )
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let actor_attempts: i64 = counts.get(0);
        let campaign_attempts: i64 = counts.get(1);
        let outstanding: i64 = counts.get(2);
        if actor_attempts >= 60
            || campaign_attempts >= i64::from(action_quota.max_actions_per_hour)
            || outstanding >= 3
        {
            return Err(PlanStoreError::PrepareLimitExceeded);
        }
        let reservation_id = make_prepare_reservation_id(
            actor_id,
            account_id,
            advert_id,
            schema_version,
            policy_revision,
            policy_digest,
            database_now,
        );
        let expires_at = database_now + PREPARE_RESERVATION_TTL;
        let inserted_reservation = transaction
            .query_one(
                "INSERT INTO control.wb_prepare_reservations \
                    (reservation_id, actor_id, account_id, advert_id, schema_version, \
                     policy_revision, policy_digest, quota_max_actions_per_hour, \
                     quota_max_actions_per_day, quota_cooldown_seconds, \
                     quota_max_cumulative_abs_delta_kopecks_per_day, reserved_at, expires_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) \
                 RETURNING reserved_at, expires_at",
                &[
                    &reservation_id,
                    &actor_id,
                    &account_id,
                    &advert_id_i64,
                    &schema_version_i32,
                    &policy_revision_i64,
                    &policy_digest,
                    &max_actions_per_hour,
                    &max_actions_per_day,
                    &cooldown_seconds,
                    &max_cumulative_delta,
                    &database_now,
                    &expires_at,
                ],
            )
            .await
            .map_err(map_prepare_insert_error)?;
        let reserved_at: DateTime<Utc> = inserted_reservation.get(0);
        let expires_at: DateTime<Utc> = inserted_reservation.get(1);
        transaction
            .commit()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        Ok(WbPrepareReservation {
            reservation_id,
            actor_id: actor_id.to_owned(),
            account_id: account_id.to_owned(),
            advert_id,
            schema_version,
            policy_revision,
            policy_digest: policy_digest.to_owned(),
            action_quota,
            reserved_at,
            expires_at,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::control) async fn create(
        &self,
        actor_id: &str,
        account_id: &str,
        advert_id: u64,
        schema_version: u32,
        policy_revision: u64,
        policy_digest: &str,
        action_quota: WbActionQuota,
        prepare_reservation_id: &str,
        requested: &[WbBidChange],
        changes: &[WbPreparedBidChange],
        before: &WbCampaignBidSnapshot,
        _now: DateTime<Utc>,
    ) -> Result<WbControlPlan, PlanStoreError> {
        validate_actor_or_account(actor_id)?;
        validate_actor_or_account(account_id)?;
        validate_digest(policy_digest)?;
        validate_digest(prepare_reservation_id)?;
        action_quota.validate()?;
        if advert_id == 0
            || schema_version == 0
            || policy_revision == 0
            || changes.is_empty()
            || changes.len() != requested.len()
        {
            return Err(PlanStoreError::InvalidPlan);
        }
        let cumulative_abs_delta = cumulative_abs_delta(changes)?;
        if cumulative_abs_delta > action_quota.max_cumulative_abs_delta_kopecks_per_day {
            return Err(PlanStoreError::QuotaExceeded);
        }

        let advert_id_i64 = i64::try_from(advert_id).map_err(|_| PlanStoreError::InvalidPlan)?;
        let schema_version_i32 =
            i32::try_from(schema_version).map_err(|_| PlanStoreError::InvalidPlan)?;
        let policy_revision_i64 =
            i64::try_from(policy_revision).map_err(|_| PlanStoreError::InvalidPlan)?;
        let requested_json =
            serde_json::to_string(requested).map_err(|_| PlanStoreError::InvalidPlan)?;
        let changes_json =
            serde_json::to_string(changes).map_err(|_| PlanStoreError::InvalidPlan)?;
        let before_json = serde_json::to_string(before).map_err(|_| PlanStoreError::InvalidPlan)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended('wb/policy-revision', 0))",
                &[],
            )
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let active_policy = transaction
            .query_opt(
                "SELECT schema_version, policy_revision, policy_digest \
                 FROM control.wb_policy_revisions \
                 ORDER BY policy_revision DESC LIMIT 1",
                &[],
            )
            .await
            .map_err(|_| PlanStoreError::Unavailable)?
            .ok_or(PlanStoreError::PolicyChanged)?;
        if active_policy.get::<_, i32>(0) != schema_version_i32
            || active_policy.get::<_, i64>(1) != policy_revision_i64
            || active_policy.get::<_, &str>(2) != policy_digest
        {
            return Err(PlanStoreError::PolicyChanged);
        }
        lock_prepare_actor(&transaction, actor_id).await?;
        lock_campaign(&transaction, account_id, advert_id).await?;
        let database_now = database_now(&transaction).await?;
        let prepare_reservation = transaction
            .query_opt(
                "SELECT actor_id, account_id, advert_id, schema_version, policy_revision, \
                        policy_digest, quota_max_actions_per_hour, quota_max_actions_per_day, \
                        quota_cooldown_seconds, \
                        quota_max_cumulative_abs_delta_kopecks_per_day, expires_at, \
                        EXISTS(SELECT 1 FROM control.wb_plans \
                               WHERE prepare_reservation_id=$1) \
                 FROM control.wb_prepare_reservations WHERE reservation_id=$1",
                &[&prepare_reservation_id],
            )
            .await
            .map_err(|_| PlanStoreError::Unavailable)?
            .ok_or(PlanStoreError::InvalidPlan)?;
        if prepare_reservation.get::<_, &str>(0) != actor_id
            || prepare_reservation.get::<_, &str>(1) != account_id
            || prepare_reservation.get::<_, i64>(2) != advert_id_i64
            || prepare_reservation.get::<_, i32>(3) != schema_version_i32
            || prepare_reservation.get::<_, i64>(4) != policy_revision_i64
            || prepare_reservation.get::<_, &str>(5) != policy_digest
            || prepare_reservation.get::<_, i32>(6)
                != i32::try_from(action_quota.max_actions_per_hour)
                    .map_err(|_| PlanStoreError::InvalidPlan)?
            || prepare_reservation.get::<_, i32>(7)
                != i32::try_from(action_quota.max_actions_per_day)
                    .map_err(|_| PlanStoreError::InvalidPlan)?
            || prepare_reservation.get::<_, i32>(8)
                != i32::try_from(action_quota.cooldown_seconds)
                    .map_err(|_| PlanStoreError::InvalidPlan)?
            || prepare_reservation.get::<_, i64>(9)
                != i64::try_from(action_quota.max_cumulative_abs_delta_kopecks_per_day)
                    .map_err(|_| PlanStoreError::InvalidPlan)?
        {
            return Err(PlanStoreError::InvalidPlan);
        }
        if prepare_reservation.get::<_, DateTime<Utc>>(10) <= database_now {
            return Err(PlanStoreError::PrepareLimitExceeded);
        }
        if prepare_reservation.get::<_, bool>(11) {
            return Err(PlanStoreError::InvalidState);
        }
        if campaign_has_incident(&transaction, account_id, advert_id, None).await? {
            return Err(PlanStoreError::CampaignLocked);
        }
        let outstanding =
            count_outstanding_prepares(&transaction, account_id, advert_id, database_now).await?;
        if outstanding > 3 {
            return Err(PlanStoreError::PrepareLimitExceeded);
        }
        let expires_at = database_now + PLAN_TTL;
        let plan_digest = make_plan_digest(
            prepare_reservation_id,
            actor_id,
            account_id,
            advert_id,
            schema_version,
            policy_revision,
            policy_digest,
            action_quota,
            &requested_json,
            &changes_json,
            &before_json,
            database_now,
            expires_at,
        );
        let plan_id = make_plan_id(&plan_digest, database_now);
        let max_actions_per_hour = i32::try_from(action_quota.max_actions_per_hour)
            .map_err(|_| PlanStoreError::InvalidPlan)?;
        let max_actions_per_day = i32::try_from(action_quota.max_actions_per_day)
            .map_err(|_| PlanStoreError::InvalidPlan)?;
        let cooldown_seconds = i32::try_from(action_quota.cooldown_seconds)
            .map_err(|_| PlanStoreError::InvalidPlan)?;
        let max_cumulative_delta =
            i64::try_from(action_quota.max_cumulative_abs_delta_kopecks_per_day)
                .map_err(|_| PlanStoreError::InvalidPlan)?;

        transaction
            .execute(
                "INSERT INTO control.wb_plans \
                    (plan_id, plan_digest, prepare_reservation_id, \
                     actor_id, account_id, advert_id, \
                     schema_version, policy_revision, policy_digest, \
                     quota_max_actions_per_hour, quota_max_actions_per_day, \
                     quota_cooldown_seconds, \
                     quota_max_cumulative_abs_delta_kopecks_per_day, status, \
                     requested_json, changes_json, before_json, created_at, expires_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'prepared',$14,$15,$16,$17,$18)",
                &[
                    &plan_id,
                    &plan_digest,
                    &prepare_reservation_id,
                    &actor_id,
                    &account_id,
                    &advert_id_i64,
                    &schema_version_i32,
                    &policy_revision_i64,
                    &policy_digest,
                    &max_actions_per_hour,
                    &max_actions_per_day,
                    &cooldown_seconds,
                    &max_cumulative_delta,
                    &requested_json,
                    &changes_json,
                    &before_json,
                    &database_now,
                    &expires_at,
                ],
            )
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let audit_payload = serde_json::to_string(&serde_json::json!({
            "plan_digest": plan_digest,
            "policy_revision": policy_revision,
            "policy_digest": policy_digest,
        }))
        .map_err(|_| PlanStoreError::InvalidPlan)?;
        insert_audit(&transaction, &plan_id, actor_id, "prepared", &audit_payload).await?;
        transaction
            .commit()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        Ok(WbControlPlan {
            plan_id,
            plan_digest,
            prepare_reservation_id: prepare_reservation_id.to_owned(),
            actor_id: actor_id.to_owned(),
            account_id: account_id.to_owned(),
            advert_id,
            schema_version,
            policy_revision,
            policy_digest: policy_digest.to_owned(),
            action_quota,
            status: WbPlanStatus::Prepared,
            approval: None,
            requested: requested.to_vec(),
            changes: changes.to_vec(),
            before: before.clone(),
            created_at: database_now,
            expires_at,
            apply_started_at: None,
            last_error_class: None,
            write_response: None,
            readback: None,
        })
    }

    pub(in crate::control) async fn load_for_actor(
        &self,
        plan_id: &str,
        actor_id: &str,
    ) -> Result<WbControlPlan, PlanStoreError> {
        validate_plan_id(plan_id)?;
        validate_actor_or_account(actor_id)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let query = format!("{PLAN_SELECT} WHERE p.plan_id=$1 AND p.actor_id=$2");
        let row = client
            .query_opt(&query, &[&plan_id, &actor_id])
            .await
            .map_err(|_| PlanStoreError::Unavailable)?
            .ok_or(PlanStoreError::NotFound)?;
        plan_from_row(&row)
    }

    /// Read-only lookup used before approval authorization. The caller must
    /// validate the authenticated approver against the fresh registry/policy;
    /// `approve` independently rejects self-approval in the database.
    pub(in crate::control) async fn load_by_id_for_approval(
        &self,
        plan_id: &str,
    ) -> Result<WbControlPlan, PlanStoreError> {
        validate_plan_id(plan_id)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let query = format!("{PLAN_SELECT} WHERE p.plan_id=$1");
        let row = client
            .query_opt(&query, &[&plan_id])
            .await
            .map_err(|_| PlanStoreError::Unavailable)?
            .ok_or(PlanStoreError::NotFound)?;
        plan_from_row(&row)
    }

    /// Persists an append-only approval bound to the exact plan digest.
    /// `reason` is a strict opaque approval reference, never marketplace data
    /// or model-generated free-form text.
    pub(in crate::control) async fn approve(
        &self,
        plan_id: &str,
        approver_id: &str,
        expected_plan_digest: &str,
        reason: &str,
        _now: DateTime<Utc>,
    ) -> Result<WbControlPlan, PlanStoreError> {
        validate_plan_id(plan_id)?;
        validate_actor_or_account(approver_id)?;
        validate_digest(expected_plan_digest)?;
        validate_approval_reason(reason)?;

        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let query = format!("{PLAN_SELECT} WHERE p.plan_id=$1 FOR UPDATE OF p");
        let row = transaction
            .query_opt(&query, &[&plan_id])
            .await
            .map_err(|_| PlanStoreError::Unavailable)?
            .ok_or(PlanStoreError::NotFound)?;
        let mut plan = plan_from_row(&row)?;
        if plan.plan_digest != expected_plan_digest {
            return Err(PlanStoreError::PlanChanged);
        }
        if plan.actor_id == approver_id {
            return Err(PlanStoreError::InvalidState);
        }
        require_active_policy(
            &transaction,
            plan.schema_version,
            plan.policy_revision,
            &plan.policy_digest,
        )
        .await?;
        lock_campaign(&transaction, &plan.account_id, plan.advert_id).await?;
        let database_now = database_now(&transaction).await?;
        if plan.expires_at <= database_now {
            expire_plan(&transaction, &plan.plan_id, &plan.actor_id, database_now).await?;
            transaction
                .commit()
                .await
                .map_err(|_| PlanStoreError::Unavailable)?;
            return Err(PlanStoreError::Expired);
        }
        if plan.status == WbPlanStatus::Approved {
            let approval = plan.approval.as_ref().ok_or(PlanStoreError::Unavailable)?;
            if approval.expires_at <= database_now {
                expire_plan(&transaction, &plan.plan_id, &plan.actor_id, database_now).await?;
                transaction
                    .commit()
                    .await
                    .map_err(|_| PlanStoreError::Unavailable)?;
                return Err(PlanStoreError::ApprovalExpired);
            }
            return if approval.approver_id == approver_id && approval.reason == reason {
                Ok(plan)
            } else {
                Err(PlanStoreError::InvalidState)
            };
        }
        if plan.status != WbPlanStatus::Prepared {
            return Err(PlanStoreError::InvalidState);
        }
        if campaign_has_incident(
            &transaction,
            &plan.account_id,
            plan.advert_id,
            Some(&plan.plan_id),
        )
        .await?
        {
            return Err(PlanStoreError::CampaignLocked);
        }

        let approval_expires_at = std::cmp::min(plan.expires_at, database_now + APPROVAL_TTL);
        let approval_id = make_approval_id(
            &plan.plan_id,
            &plan.plan_digest,
            approver_id,
            reason,
            database_now,
        );
        let inserted_approval = transaction
            .query_one(
                "INSERT INTO control.wb_plan_approvals \
                    (approval_id, plan_id, plan_digest, approver_id, reason, approved_at, expires_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7) \
                 RETURNING approved_at, expires_at",
                &[
                    &approval_id,
                    &plan.plan_id,
                    &plan.plan_digest,
                    &approver_id,
                    &reason,
                    &database_now,
                    &approval_expires_at,
                ],
            )
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let approved_at: DateTime<Utc> = inserted_approval.get(0);
        let approval_expires_at: DateTime<Utc> = inserted_approval.get(1);
        let updated = transaction
            .execute(
                "UPDATE control.wb_plans SET status='approved' \
                 WHERE plan_id=$1 AND status='prepared'",
                &[&plan.plan_id],
            )
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        if updated != 1 {
            return Err(PlanStoreError::InvalidState);
        }
        let audit_payload = serde_json::to_string(&serde_json::json!({
            "approval_id": approval_id,
            "plan_digest": plan.plan_digest,
        }))
        .map_err(|_| PlanStoreError::InvalidPlan)?;
        insert_audit(
            &transaction,
            &plan.plan_id,
            approver_id,
            "approved",
            &audit_payload,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        plan.status = WbPlanStatus::Approved;
        plan.approval = Some(WbPlanApproval {
            approval_id,
            approver_id: approver_id.to_owned(),
            reason: reason.to_owned(),
            approved_at,
            expires_at: approval_expires_at,
        });
        Ok(plan)
    }

    /// Atomically validates the exact approved plan/policy, all three runtime
    /// leases, the incident lock and rolling quotas, then reserves the attempt
    /// and transitions `approved -> applying`.
    pub(in crate::control) async fn claim_for_apply(
        &self,
        context: WbApplyContext<'_>,
    ) -> Result<WbControlPlan, PlanStoreError> {
        let WbApplyContext {
            plan_id,
            actor_id,
            expected_plan_digest,
            expected_schema_version,
            expected_policy_revision,
            expected_policy_digest,
            now: _now,
        } = context;
        validate_plan_id(plan_id)?;
        validate_actor_or_account(actor_id)?;
        validate_digest(expected_plan_digest)?;
        validate_digest(expected_policy_digest)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let query = format!("{PLAN_SELECT} WHERE p.plan_id=$1 AND p.actor_id=$2 FOR UPDATE OF p");
        let row = transaction
            .query_opt(&query, &[&plan_id, &actor_id])
            .await
            .map_err(|_| PlanStoreError::Unavailable)?
            .ok_or(PlanStoreError::NotFound)?;
        let mut plan = plan_from_row(&row)?;
        if plan.plan_digest != expected_plan_digest {
            return Err(PlanStoreError::PlanChanged);
        }
        if plan.schema_version != expected_schema_version
            || plan.policy_revision != expected_policy_revision
            || plan.policy_digest != expected_policy_digest
        {
            return Err(PlanStoreError::PolicyChanged);
        }
        if plan.status == WbPlanStatus::Prepared {
            return Err(PlanStoreError::ApprovalRequired);
        }
        if plan.status != WbPlanStatus::Approved {
            return Err(PlanStoreError::InvalidState);
        }
        let approval = plan
            .approval
            .as_ref()
            .ok_or(PlanStoreError::ApprovalRequired)?;
        require_active_policy(
            &transaction,
            plan.schema_version,
            plan.policy_revision,
            &plan.policy_digest,
        )
        .await?;
        lock_campaign(&transaction, &plan.account_id, plan.advert_id).await?;
        let database_now = database_now(&transaction).await?;
        if plan.expires_at <= database_now || approval.expires_at <= database_now {
            expire_plan(&transaction, plan_id, actor_id, database_now).await?;
            transaction
                .commit()
                .await
                .map_err(|_| PlanStoreError::Unavailable)?;
            return if plan.expires_at <= database_now {
                Err(PlanStoreError::Expired)
            } else {
                Err(PlanStoreError::ApprovalExpired)
            };
        }

        if campaign_has_incident(
            &transaction,
            &plan.account_id,
            plan.advert_id,
            Some(plan_id),
        )
        .await?
        {
            return Err(PlanStoreError::CampaignLocked);
        }
        require_runtime_gates(&transaction, &plan.account_id, plan.advert_id).await?;
        reserve_action_quota(&transaction, &plan, database_now).await?;

        let update = transaction
            .query_one(
                "UPDATE control.wb_plans SET status='applying', apply_started_at=$2 \
                 WHERE plan_id=$1 AND status='approved' \
                 RETURNING apply_started_at",
                &[&plan_id, &database_now],
            )
            .await;
        let apply_started_at = match update {
            Ok(update) => update
                .get::<_, Option<DateTime<Utc>>>(0)
                .ok_or(PlanStoreError::Unavailable)?,
            Err(error)
                if error
                    .as_db_error()
                    .is_some_and(|db| db.code() == &SqlState::UNIQUE_VIOLATION) =>
            {
                return Err(PlanStoreError::Busy);
            }
            Err(_) => return Err(PlanStoreError::Unavailable),
        };
        let audit_payload = serde_json::to_string(&serde_json::json!({
            "plan_digest": plan.plan_digest,
            "policy_digest": plan.policy_digest,
            "quota": plan.action_quota,
        }))
        .map_err(|_| PlanStoreError::InvalidPlan)?;
        insert_audit(&transaction, plan_id, actor_id, "applying", &audit_payload).await?;
        transaction
            .commit()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        plan.status = WbPlanStatus::Applying;
        plan.apply_started_at = Some(apply_started_at);
        Ok(plan)
    }

    /// Final fail-closed check intended to run immediately before the single
    /// marketplace PATCH. It does not reserve another quota or mutate state.
    pub(in crate::control) async fn revalidate_before_write(
        &self,
        context: WbApplyContext<'_>,
    ) -> Result<(), PlanStoreError> {
        let WbApplyContext {
            plan_id,
            actor_id,
            expected_plan_digest,
            expected_schema_version,
            expected_policy_revision,
            expected_policy_digest,
            now: _now,
        } = context;
        validate_plan_id(plan_id)?;
        validate_actor_or_account(actor_id)?;
        validate_digest(expected_plan_digest)?;
        validate_digest(expected_policy_digest)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let query = format!("{PLAN_SELECT} WHERE p.plan_id=$1 AND p.actor_id=$2 FOR UPDATE OF p");
        let row = transaction
            .query_opt(&query, &[&plan_id, &actor_id])
            .await
            .map_err(|_| PlanStoreError::Unavailable)?
            .ok_or(PlanStoreError::NotFound)?;
        let plan = plan_from_row(&row)?;
        if plan.plan_digest != expected_plan_digest {
            return Err(PlanStoreError::PlanChanged);
        }
        if plan.schema_version != expected_schema_version
            || plan.policy_revision != expected_policy_revision
            || plan.policy_digest != expected_policy_digest
        {
            return Err(PlanStoreError::PolicyChanged);
        }
        if plan.status != WbPlanStatus::Applying {
            return Err(PlanStoreError::InvalidState);
        }
        let approval = plan
            .approval
            .as_ref()
            .ok_or(PlanStoreError::ApprovalRequired)?;
        require_active_policy(
            &transaction,
            plan.schema_version,
            plan.policy_revision,
            &plan.policy_digest,
        )
        .await?;
        lock_campaign(&transaction, &plan.account_id, plan.advert_id).await?;
        let database_now = database_now(&transaction).await?;
        if plan.expires_at <= database_now {
            return Err(PlanStoreError::Expired);
        }
        if approval.expires_at <= database_now {
            return Err(PlanStoreError::ApprovalExpired);
        }
        if campaign_has_incident(
            &transaction,
            &plan.account_id,
            plan.advert_id,
            Some(plan_id),
        )
        .await?
        {
            return Err(PlanStoreError::CampaignLocked);
        }
        require_runtime_gates(&transaction, &plan.account_id, plan.advert_id).await?;
        let reservation_exists = transaction
            .query_one(
                "SELECT EXISTS( \
                    SELECT 1 FROM control.wb_action_reservations \
                    WHERE plan_id=$1 AND account_id=$2 AND advert_id=$3 \
                )",
                &[
                    &plan_id,
                    &plan.account_id,
                    &i64::try_from(plan.advert_id).map_err(|_| PlanStoreError::InvalidPlan)?,
                ],
            )
            .await
            .map_err(|_| PlanStoreError::Unavailable)?
            .get::<_, bool>(0);
        if !reservation_exists {
            return Err(PlanStoreError::InvalidState);
        }
        transaction
            .commit()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        Ok(())
    }

    /// Converts an abandoned apply into an explicitly ambiguous result.
    /// This method never contacts WB and never retries the mutation.
    pub(in crate::control) async fn mark_stale_applying_ambiguous(
        &self,
        plan_id: &str,
        actor_id: &str,
        _now: DateTime<Utc>,
    ) -> Result<(), PlanStoreError> {
        validate_plan_id(plan_id)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let row = transaction
            .query_opt(
                "SELECT status, apply_started_at, account_id, advert_id \
                 FROM control.wb_plans \
                 WHERE plan_id=$1 AND actor_id=$2 FOR UPDATE",
                &[&plan_id, &actor_id],
            )
            .await
            .map_err(|_| PlanStoreError::Unavailable)?
            .ok_or(PlanStoreError::NotFound)?;
        let status = WbPlanStatus::from_db(row.get::<_, &str>(0))?;
        if matches!(
            status,
            WbPlanStatus::Ambiguous | WbPlanStatus::ReconciliationRequired | WbPlanStatus::Applied
        ) {
            return Ok(());
        }
        if status != WbPlanStatus::Applying {
            return Err(PlanStoreError::InvalidState);
        }
        let apply_started_at = row
            .get::<_, Option<DateTime<Utc>>>(1)
            .ok_or(PlanStoreError::Unavailable)?;
        let account_id: String = row.get(2);
        let advert_id_i64: i64 = row.get(3);
        let advert_id = u64::try_from(advert_id_i64).map_err(|_| PlanStoreError::Unavailable)?;
        lock_campaign(&transaction, &account_id, advert_id).await?;
        let database_now = database_now(&transaction).await?;
        if apply_started_at + STALE_APPLY_AFTER > database_now {
            return Err(PlanStoreError::ApplyInProgress);
        }
        let updated = transaction
            .execute(
                "UPDATE control.wb_plans \
                 SET status='ambiguous', finished_at=$3, \
                     last_error_class='stale_apply_unknown' \
                 WHERE plan_id=$1 AND actor_id=$2 AND status='applying'",
                &[&plan_id, &actor_id, &database_now],
            )
            .await
            .map_err(|error| {
                if error
                    .as_db_error()
                    .is_some_and(|db| db.code() == &SqlState::UNIQUE_VIOLATION)
                {
                    PlanStoreError::CampaignLocked
                } else {
                    PlanStoreError::Unavailable
                }
            })?;
        if updated != 1 {
            return Err(PlanStoreError::InvalidState);
        }
        insert_audit(
            &transaction,
            plan_id,
            actor_id,
            "stale_apply_ambiguous",
            r#"{"reason":"stale_apply_unknown"}"#,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        Ok(())
    }

    pub(in crate::control) async fn finish(
        &self,
        plan_id: &str,
        actor_id: &str,
        finish: WbPlanFinish<'_>,
    ) -> Result<(), PlanStoreError> {
        // Caller time is observational only. Persisted security timestamps are
        // always taken from PostgreSQL transaction time below.
        let _ = finish.now;
        if !matches!(
            finish.status,
            WbPlanStatus::Applied
                | WbPlanStatus::ReconciliationRequired
                | WbPlanStatus::Ambiguous
                | WbPlanStatus::Rejected
                | WbPlanStatus::Failed
        ) {
            return Err(PlanStoreError::InvalidState);
        }
        let write_json = finish
            .write_response
            .map(serde_json::to_string)
            .transpose()
            .map_err(|_| PlanStoreError::InvalidPlan)?;
        let readback_json = finish
            .readback
            .map(serde_json::to_string)
            .transpose()
            .map_err(|_| PlanStoreError::InvalidPlan)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let query = format!("{PLAN_SELECT} WHERE p.plan_id=$1 AND p.actor_id=$2 FOR UPDATE OF p");
        let row = transaction
            .query_opt(&query, &[&plan_id, &actor_id])
            .await
            .map_err(|_| PlanStoreError::Unavailable)?
            .ok_or(PlanStoreError::InvalidState)?;
        let plan = plan_from_row(&row)?;
        if plan.status != WbPlanStatus::Applying {
            return Err(PlanStoreError::InvalidState);
        }
        if finish.status == WbPlanStatus::Applied
            && (finish.write_response.is_none()
                || finish.readback.is_none_or(|readback| {
                    !snapshot_matches_plan_state(readback, &plan.before, &plan.changes, true)
                }))
        {
            return Err(PlanStoreError::InvalidPlan);
        }
        lock_campaign(&transaction, &plan.account_id, plan.advert_id).await?;
        let database_now = database_now(&transaction).await?;
        let updated = transaction
            .execute(
                "UPDATE control.wb_plans \
                 SET status=$3, finished_at=$4, last_error_class=$5, \
                     write_response_json=$6, readback_json=$7 \
                 WHERE plan_id=$1 AND actor_id=$2 AND status='applying'",
                &[
                    &plan_id,
                    &actor_id,
                    &finish.status.as_db(),
                    &database_now,
                    &finish.error_class,
                    &write_json,
                    &readback_json,
                ],
            )
            .await
            .map_err(|error| {
                if error
                    .as_db_error()
                    .is_some_and(|db| db.code() == &SqlState::UNIQUE_VIOLATION)
                {
                    PlanStoreError::CampaignLocked
                } else {
                    PlanStoreError::Unavailable
                }
            })?;
        if updated != 1 {
            return Err(PlanStoreError::InvalidState);
        }
        insert_audit(&transaction, plan_id, actor_id, finish.status.as_db(), "{}").await?;
        transaction
            .commit()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        Ok(())
    }

    pub(in crate::control) async fn confirm_reconciled(
        &self,
        plan_id: &str,
        actor_id: &str,
        readback: &WbCampaignBidSnapshot,
        _now: DateTime<Utc>,
    ) -> Result<(), PlanStoreError> {
        let readback_json =
            serde_json::to_string(readback).map_err(|_| PlanStoreError::InvalidPlan)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        let query = format!("{PLAN_SELECT} WHERE p.plan_id=$1 AND p.actor_id=$2 FOR UPDATE OF p");
        let row = transaction
            .query_opt(&query, &[&plan_id, &actor_id])
            .await
            .map_err(|_| PlanStoreError::Unavailable)?
            .ok_or(PlanStoreError::NotFound)?;
        let plan = plan_from_row(&row)?;
        if plan.status == WbPlanStatus::Applied {
            return Ok(());
        }
        if !matches!(
            plan.status,
            WbPlanStatus::ReconciliationRequired | WbPlanStatus::Ambiguous
        ) {
            return Err(PlanStoreError::InvalidState);
        }
        if !snapshot_matches_plan_state(readback, &plan.before, &plan.changes, true) {
            return Err(PlanStoreError::InvalidPlan);
        }
        lock_campaign(&transaction, &plan.account_id, plan.advert_id).await?;
        let database_now = database_now(&transaction).await?;
        let updated = transaction
            .execute(
                "UPDATE control.wb_plans SET status='applied', finished_at=$3, \
                     last_error_class=NULL, readback_json=$4 \
                 WHERE plan_id=$1 AND actor_id=$2 \
                   AND status IN ('reconciliation_required','ambiguous')",
                &[&plan_id, &actor_id, &database_now, &readback_json],
            )
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        if updated != 1 {
            return Err(PlanStoreError::InvalidState);
        }
        insert_audit(&transaction, plan_id, actor_id, "reconciled_applied", "{}").await?;
        transaction
            .commit()
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        Ok(())
    }
}

async fn require_active_policy(
    transaction: &Transaction<'_>,
    schema_version: u32,
    policy_revision: u64,
    policy_digest: &str,
) -> Result<(), PlanStoreError> {
    let schema_version_i32 =
        i32::try_from(schema_version).map_err(|_| PlanStoreError::InvalidPlan)?;
    let policy_revision_i64 =
        i64::try_from(policy_revision).map_err(|_| PlanStoreError::InvalidPlan)?;
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended('wb/policy-revision', 0))",
            &[],
        )
        .await
        .map_err(|_| PlanStoreError::Unavailable)?;
    let active = transaction
        .query_opt(
            "SELECT schema_version, policy_revision, policy_digest \
             FROM control.wb_policy_revisions \
             ORDER BY policy_revision DESC LIMIT 1",
            &[],
        )
        .await
        .map_err(|_| PlanStoreError::Unavailable)?;
    if active.is_some_and(|active| {
        active.get::<_, i32>(0) == schema_version_i32
            && active.get::<_, i64>(1) == policy_revision_i64
            && active.get::<_, &str>(2) == policy_digest
    }) {
        Ok(())
    } else {
        Err(PlanStoreError::PolicyChanged)
    }
}

async fn database_now(transaction: &Transaction<'_>) -> Result<DateTime<Utc>, PlanStoreError> {
    transaction
        .query_one("SELECT clock_timestamp()", &[])
        .await
        .map_err(|_| PlanStoreError::Unavailable)
        .map(|row| row.get(0))
}

async fn lock_prepare_actor(
    transaction: &Transaction<'_>,
    actor_id: &str,
) -> Result<(), PlanStoreError> {
    let lock_key = format!("wb/prepare/actor/{actor_id}");
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&lock_key],
        )
        .await
        .map_err(|_| PlanStoreError::Unavailable)?;
    Ok(())
}

async fn lock_campaign(
    transaction: &Transaction<'_>,
    account_id: &str,
    advert_id: u64,
) -> Result<(), PlanStoreError> {
    let lock_key = format!("wb/{account_id}/{advert_id}");
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&lock_key],
        )
        .await
        .map_err(|_| PlanStoreError::Unavailable)?;
    Ok(())
}

async fn count_outstanding_prepares(
    transaction: &Transaction<'_>,
    account_id: &str,
    advert_id: u64,
    now: DateTime<Utc>,
) -> Result<i64, PlanStoreError> {
    let advert_id_i64 = i64::try_from(advert_id).map_err(|_| PlanStoreError::InvalidPlan)?;
    transaction
        .query_one(
            "SELECT ( \
                (SELECT count(*) FROM control.wb_plans \
                 WHERE account_id=$1 AND advert_id=$2 \
                   AND status IN ('prepared','approved') AND expires_at>$3) \
                + \
                (SELECT count(*) FROM control.wb_prepare_reservations pending \
                 WHERE pending.account_id=$1 AND pending.advert_id=$2 \
                   AND pending.expires_at>$3 AND NOT EXISTS ( \
                     SELECT 1 FROM control.wb_plans plan \
                     WHERE plan.prepare_reservation_id=pending.reservation_id \
                   )) \
            )::bigint",
            &[&account_id, &advert_id_i64, &now],
        )
        .await
        .map_err(|_| PlanStoreError::Unavailable)
        .map(|row| row.get(0))
}

async fn campaign_has_incident(
    transaction: &Transaction<'_>,
    account_id: &str,
    advert_id: u64,
    except_plan_id: Option<&str>,
) -> Result<bool, PlanStoreError> {
    let advert_id_i64 = i64::try_from(advert_id).map_err(|_| PlanStoreError::InvalidPlan)?;
    let except = except_plan_id.unwrap_or("");
    transaction
        .query_one(
            "SELECT EXISTS( \
                SELECT 1 FROM control.wb_plans \
                WHERE account_id=$1 AND advert_id=$2 \
                  AND status IN ('reconciliation_required','ambiguous') \
                  AND plan_id <> $3 \
            )",
            &[&account_id, &advert_id_i64, &except],
        )
        .await
        .map_err(|_| PlanStoreError::Unavailable)
        .map(|row| row.get(0))
}

async fn require_runtime_gates(
    transaction: &Transaction<'_>,
    account_id: &str,
    advert_id: u64,
) -> Result<(), PlanStoreError> {
    let account_gate = format!("account/{account_id}");
    let campaign_gate = format!("campaign/{account_id}/{advert_id}");
    let active = transaction
        .query_one(
            "SELECT count(*) = 3 AND bool_and( \
                    enabled \
                    AND lease_expires_at > clock_timestamp() \
                    AND (disabled_until IS NULL OR disabled_until <= clock_timestamp()) \
                ) \
             FROM control.wb_runtime_gates \
             WHERE gate_key IN ($1,$2,$3)",
            &[&"global", &account_gate, &campaign_gate],
        )
        .await
        .map_err(|_| PlanStoreError::Unavailable)?
        .get::<_, bool>(0);
    if active {
        Ok(())
    } else {
        Err(PlanStoreError::RuntimeDisabled)
    }
}

pub(super) async fn reserve_action_quota(
    transaction: &Transaction<'_>,
    plan: &WbControlPlan,
    now: DateTime<Utc>,
) -> Result<(), PlanStoreError> {
    let advert_id_i64 = i64::try_from(plan.advert_id).map_err(|_| PlanStoreError::InvalidPlan)?;
    let hour_start = now - Duration::hours(1);
    let day_start = now - Duration::days(1);
    let row = transaction
        .query_one(
            "SELECT \
                count(*) FILTER (WHERE reserved_at > $3)::bigint, \
                count(*)::bigint, \
                COALESCE(sum(cumulative_abs_delta_kopecks), 0)::bigint, \
                max(reserved_at) \
             FROM control.wb_action_reservations \
             WHERE account_id=$1 AND advert_id=$2 AND reserved_at > $4",
            &[&plan.account_id, &advert_id_i64, &hour_start, &day_start],
        )
        .await
        .map_err(|_| PlanStoreError::Unavailable)?;
    let actions_hour: i64 = row.get(0);
    let actions_day: i64 = row.get(1);
    let reserved_delta_day: i64 = row.get(2);
    let last_reserved_at: Option<DateTime<Utc>> = row.get(3);
    let action_delta_u64 = cumulative_abs_delta(&plan.changes)?;
    let requested_delta =
        i64::try_from(action_delta_u64).map_err(|_| PlanStoreError::InvalidPlan)?;
    let max_hour = i64::from(plan.action_quota.max_actions_per_hour);
    let max_day = i64::from(plan.action_quota.max_actions_per_day);
    let max_delta = i64::try_from(plan.action_quota.max_cumulative_abs_delta_kopecks_per_day)
        .map_err(|_| PlanStoreError::InvalidPlan)?;
    let cooldown = i64::try_from(plan.action_quota.cooldown_seconds)
        .map_err(|_| PlanStoreError::InvalidPlan)?;

    if actions_hour >= max_hour
        || actions_day >= max_day
        || reserved_delta_day.saturating_add(requested_delta) > max_delta
        || last_reserved_at.is_some_and(|last| last + Duration::seconds(cooldown) > now)
    {
        return Err(PlanStoreError::QuotaExceeded);
    }

    let max_actions_per_hour = i32::try_from(plan.action_quota.max_actions_per_hour)
        .map_err(|_| PlanStoreError::InvalidPlan)?;
    let max_actions_per_day = i32::try_from(plan.action_quota.max_actions_per_day)
        .map_err(|_| PlanStoreError::InvalidPlan)?;
    let cooldown_seconds = i32::try_from(plan.action_quota.cooldown_seconds)
        .map_err(|_| PlanStoreError::InvalidPlan)?;
    transaction
        .execute(
            "INSERT INTO control.wb_action_reservations \
                (plan_id, account_id, advert_id, cumulative_abs_delta_kopecks, \
                 max_actions_per_hour, max_actions_per_day, cooldown_seconds, \
                 max_cumulative_abs_delta_kopecks_per_day, reserved_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            &[
                &plan.plan_id,
                &plan.account_id,
                &advert_id_i64,
                &requested_delta,
                &max_actions_per_hour,
                &max_actions_per_day,
                &cooldown_seconds,
                &max_delta,
                &now,
            ],
        )
        .await
        .map_err(|error| {
            if error
                .as_db_error()
                .is_some_and(|db| db.code() == &SqlState::UNIQUE_VIOLATION)
            {
                PlanStoreError::InvalidState
            } else {
                PlanStoreError::Unavailable
            }
        })?;
    Ok(())
}

pub(super) async fn expire_plan(
    transaction: &Transaction<'_>,
    plan_id: &str,
    actor_id: &str,
    now: DateTime<Utc>,
) -> Result<(), PlanStoreError> {
    let updated = transaction
        .execute(
            "UPDATE control.wb_plans SET status='expired', finished_at=$2 \
             WHERE plan_id=$1 AND status IN ('prepared','approved')",
            &[&plan_id, &now],
        )
        .await
        .map_err(|_| PlanStoreError::Unavailable)?;
    if updated != 1 {
        return Err(PlanStoreError::InvalidState);
    }
    insert_audit(transaction, plan_id, actor_id, "expired", "{}").await
}

async fn insert_audit(
    transaction: &Transaction<'_>,
    plan_id: &str,
    actor_id: &str,
    event_type: &str,
    payload_json: &str,
) -> Result<(), PlanStoreError> {
    transaction
        .execute(
            "INSERT INTO control.wb_audit_events (plan_id, actor_id, event_type, payload_json) \
             VALUES ($1,$2,$3,$4)",
            &[&plan_id, &actor_id, &event_type, &payload_json],
        )
        .await
        .map_err(|_| PlanStoreError::Unavailable)?;
    Ok(())
}

pub(super) fn plan_from_row(row: &Row) -> Result<WbControlPlan, PlanStoreError> {
    let optional_value = |value: Option<String>| -> Result<Option<Value>, PlanStoreError> {
        value
            .map(|json| {
                serde_json::from_str::<Value>(&json).map_err(|_| PlanStoreError::Unavailable)
            })
            .transpose()
    };
    let optional_snapshot =
        |value: Option<String>| -> Result<Option<WbCampaignBidSnapshot>, PlanStoreError> {
            value
                .map(|json| serde_json::from_str(&json).map_err(|_| PlanStoreError::Unavailable))
                .transpose()
        };
    let advert_id_i64: i64 = row.get(4);
    let schema_version_i32: i32 = row.get(5);
    let policy_revision_i64: i64 = row.get(6);
    let quota_hour: i32 = row.get(8);
    let quota_day: i32 = row.get(9);
    let cooldown_seconds: i32 = row.get(10);
    let quota_delta: i64 = row.get(11);
    let approval_id: Option<String> = row.get(23);
    let approval = if let Some(approval_id) = approval_id {
        Some(WbPlanApproval {
            approval_id,
            approver_id: row
                .get::<_, Option<String>>(24)
                .ok_or(PlanStoreError::Unavailable)?,
            reason: row
                .get::<_, Option<String>>(25)
                .ok_or(PlanStoreError::Unavailable)?,
            approved_at: row
                .get::<_, Option<DateTime<Utc>>>(26)
                .ok_or(PlanStoreError::Unavailable)?,
            expires_at: row
                .get::<_, Option<DateTime<Utc>>>(27)
                .ok_or(PlanStoreError::Unavailable)?,
        })
    } else {
        if row.get::<_, Option<String>>(24).is_some()
            || row.get::<_, Option<String>>(25).is_some()
            || row.get::<_, Option<DateTime<Utc>>>(26).is_some()
            || row.get::<_, Option<DateTime<Utc>>>(27).is_some()
        {
            return Err(PlanStoreError::Unavailable);
        }
        None
    };
    Ok(WbControlPlan {
        plan_id: row.get(0),
        plan_digest: row.get(1),
        prepare_reservation_id: row.get(22),
        actor_id: row.get(2),
        account_id: row.get(3),
        advert_id: u64::try_from(advert_id_i64).map_err(|_| PlanStoreError::Unavailable)?,
        schema_version: u32::try_from(schema_version_i32)
            .map_err(|_| PlanStoreError::Unavailable)?,
        policy_revision: u64::try_from(policy_revision_i64)
            .map_err(|_| PlanStoreError::Unavailable)?,
        policy_digest: row.get(7),
        action_quota: WbActionQuota {
            max_actions_per_hour: u32::try_from(quota_hour)
                .map_err(|_| PlanStoreError::Unavailable)?,
            max_actions_per_day: u32::try_from(quota_day)
                .map_err(|_| PlanStoreError::Unavailable)?,
            cooldown_seconds: u64::try_from(cooldown_seconds)
                .map_err(|_| PlanStoreError::Unavailable)?,
            max_cumulative_abs_delta_kopecks_per_day: u64::try_from(quota_delta)
                .map_err(|_| PlanStoreError::Unavailable)?,
        },
        status: WbPlanStatus::from_db(row.get::<_, &str>(12))?,
        approval,
        requested: serde_json::from_str::<Vec<WbBidChange>>(&row.get::<_, String>(13))
            .map_err(|_| PlanStoreError::Unavailable)?,
        changes: serde_json::from_str::<Vec<WbPreparedBidChange>>(&row.get::<_, String>(14))
            .map_err(|_| PlanStoreError::Unavailable)?,
        before: serde_json::from_str::<WbCampaignBidSnapshot>(&row.get::<_, String>(15))
            .map_err(|_| PlanStoreError::Unavailable)?,
        created_at: row.get(16),
        expires_at: row.get(17),
        apply_started_at: row.get(18),
        last_error_class: row.get(19),
        write_response: optional_value(row.get(20))?,
        readback: optional_snapshot(row.get(21))?,
    })
}
