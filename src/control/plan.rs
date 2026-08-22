#![expect(
    clippy::significant_drop_tightening,
    reason = "PostgreSQL transactions borrow the supervised session guard until commit"
)]

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use chrono::{DateTime, Duration, Utc};
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_postgres::{Client, Config, Row, Transaction, config::Host, error::SqlState};

use crate::postgres::SupervisedClient;

use super::wb::{
    WbBidChange, WbCampaignBidSnapshot, WbPreparedBidChange, snapshot_matches_plan_state,
};

const PLAN_TTL: Duration = Duration::minutes(5);
const APPROVAL_TTL: Duration = Duration::minutes(2);
const PREPARE_RESERVATION_TTL: Duration = Duration::minutes(2);
const STALE_APPLY_AFTER: Duration = Duration::minutes(3);
const COMPONENT: &str = "mcp-ozon-control-writer";
const PLAN_DIGEST_DOMAIN: &[u8] = b"mcp-ozon/wb-control-plan/v1";
static PLAN_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WbPlanStatus {
    Prepared,
    Approved,
    Applying,
    Applied,
    ReconciliationRequired,
    Ambiguous,
    Rejected,
    Failed,
    Expired,
}

impl WbPlanStatus {
    const fn as_db(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Approved => "approved",
            Self::Applying => "applying",
            Self::Applied => "applied",
            Self::ReconciliationRequired => "reconciliation_required",
            Self::Ambiguous => "ambiguous",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
            Self::Expired => "expired",
        }
    }

    fn from_db(value: &str) -> Result<Self, PlanStoreError> {
        match value {
            "prepared" => Ok(Self::Prepared),
            "approved" => Ok(Self::Approved),
            "applying" => Ok(Self::Applying),
            "applied" => Ok(Self::Applied),
            "reconciliation_required" => Ok(Self::ReconciliationRequired),
            "ambiguous" => Ok(Self::Ambiguous),
            "rejected" => Ok(Self::Rejected),
            "failed" => Ok(Self::Failed),
            "expired" => Ok(Self::Expired),
            _ => Err(PlanStoreError::Unavailable),
        }
    }
}

/// Immutable rolling limits copied from the policy into the plan digest.
///
/// A reservation is consumed as soon as apply is claimed, including definite
/// failures and ambiguous outcomes, so retries cannot bypass these limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbActionQuota {
    pub max_actions_per_hour: u32,
    pub max_actions_per_day: u32,
    pub cooldown_seconds: u64,
    pub max_cumulative_abs_delta_kopecks_per_day: u64,
}

impl WbActionQuota {
    fn validate(self) -> Result<(), PlanStoreError> {
        if self.max_actions_per_hour == 0
            || self.max_actions_per_hour > 60
            || self.max_actions_per_day < self.max_actions_per_hour
            || self.max_actions_per_day > 500
            || !(30..=86_400).contains(&self.cooldown_seconds)
            || self.max_cumulative_abs_delta_kopecks_per_day == 0
            || i32::try_from(self.max_actions_per_hour).is_err()
            || i32::try_from(self.max_actions_per_day).is_err()
            || i32::try_from(self.cooldown_seconds).is_err()
            || i64::try_from(self.max_cumulative_abs_delta_kopecks_per_day).is_err()
        {
            return Err(PlanStoreError::InvalidPlan);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WbPlanApproval {
    pub approval_id: String,
    pub approver_id: String,
    pub reason: String,
    pub approved_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// Short-lived, append-only authorization to perform the WB read needed to
/// prepare one control plan. It must be reserved before calling WB and can be
/// consumed by exactly one matching plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WbPrepareReservation {
    pub reservation_id: String,
    pub actor_id: String,
    pub account_id: String,
    pub advert_id: u64,
    pub schema_version: u32,
    pub policy_revision: u64,
    pub policy_digest: String,
    pub action_quota: WbActionQuota,
    pub reserved_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WbControlPlan {
    pub plan_id: String,
    pub plan_digest: String,
    pub prepare_reservation_id: String,
    pub actor_id: String,
    pub account_id: String,
    pub advert_id: u64,
    pub schema_version: u32,
    pub policy_revision: u64,
    pub policy_digest: String,
    pub action_quota: WbActionQuota,
    pub status: WbPlanStatus,
    pub approval: Option<WbPlanApproval>,
    pub requested: Vec<WbBidChange>,
    pub changes: Vec<WbPreparedBidChange>,
    pub before: WbCampaignBidSnapshot,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub apply_started_at: Option<DateTime<Utc>>,
    pub last_error_class: Option<String>,
    pub write_response: Option<Value>,
    pub readback: Option<WbCampaignBidSnapshot>,
}

#[expect(
    clippy::redundant_pub_crate,
    reason = "crate-only intent remains explicit if the parent module becomes public"
)]
pub(crate) struct WbPlanFinish<'a> {
    pub status: WbPlanStatus,
    pub error_class: Option<&'a str>,
    pub write_response: Option<&'a Value>,
    pub readback: Option<&'a WbCampaignBidSnapshot>,
    pub now: DateTime<Utc>,
}

#[derive(Debug)]
pub struct WbApplyContext<'a> {
    pub plan_id: &'a str,
    pub actor_id: &'a str,
    pub expected_plan_digest: &'a str,
    pub expected_schema_version: u32,
    pub expected_policy_revision: u64,
    pub expected_policy_digest: &'a str,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum PlanStoreError {
    #[error("WB control plan store недоступен")]
    Unavailable,
    #[error("WB control plan не найден")]
    NotFound,
    #[error("WB control plan уже использован или имеет неверное состояние")]
    InvalidState,
    #[error("WB control plan истёк")]
    Expired,
    #[error("WB control plan требует отдельного server-side approval")]
    ApprovalRequired,
    #[error("WB control plan approval истёк")]
    ApprovalExpired,
    #[error("WB control plan digest не совпадает с подтверждённым")]
    PlanChanged,
    #[error("WB control policy digest изменился после подготовки плана")]
    PolicyChanged,
    #[error("WB campaign заблокирована незакрытым incident")]
    CampaignLocked,
    #[error("WB runtime gate выключен, отсутствует или lease истекла")]
    RuntimeDisabled,
    #[error("WB action quota или cooldown исчерпаны")]
    QuotaExceeded,
    #[error("лимит попыток подготовки WB control plan исчерпан")]
    PrepareLimitExceeded,
    #[error("другая операция для этой WB campaign уже выполняется")]
    Busy,
    #[error("WB control plan всё ещё может выполняться")]
    ApplyInProgress,
    #[error("WB control plan имеет недопустимые данные")]
    InvalidPlan,
}

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
    pub fn from_client(client: Client) -> Self {
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
            .query_one(
                r"SELECT current_user = 'control_writer'
                    AND EXISTS (
                        SELECT 1 FROM pg_catalog.pg_roles runtime_role
                        WHERE runtime_role.rolname=current_user
                          AND runtime_role.rolcanlogin
                          AND NOT runtime_role.rolsuper
                          AND NOT runtime_role.rolcreatedb
                          AND NOT runtime_role.rolcreaterole
                          AND NOT runtime_role.rolinherit
                          AND NOT runtime_role.rolreplication
                          AND NOT runtime_role.rolbypassrls
                          AND runtime_role.rolconnlimit=4
                    )
                    AND has_database_privilege(current_user, current_database(), 'CONNECT')
                    AND NOT has_database_privilege(current_user, current_database(), 'TEMPORARY')
                    AND NOT has_database_privilege(current_user, current_database(), 'CREATE')
                    AND NOT EXISTS (
                        SELECT 1 FROM pg_catalog.pg_database database_row
                        WHERE database_row.datname <> current_database()
                          AND (
                            has_database_privilege(
                                current_user, database_row.oid, 'CONNECT'
                            ) OR has_database_privilege(
                                current_user, database_row.oid, 'TEMPORARY'
                            ) OR has_database_privilege(
                                current_user, database_row.oid, 'CREATE'
                            )
                          )
                    )
                    AND has_schema_privilege(current_user, 'control', 'USAGE')
                    AND NOT has_schema_privilege(current_user, 'control', 'CREATE')
                    AND NOT EXISTS (
                        SELECT 1 FROM pg_catalog.pg_namespace schema_row
                        WHERE schema_row.nspname <> 'information_schema'
                          AND schema_row.nspname !~ '^pg_'
                          AND has_schema_privilege(
                            current_user, schema_row.oid, 'CREATE'
                          )
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM pg_catalog.pg_auth_members memberships
                        JOIN pg_catalog.pg_roles role_member
                          ON role_member.oid=memberships.member
                        JOIN pg_catalog.pg_roles granted_role
                          ON granted_role.oid=memberships.roleid
                        WHERE role_member.rolname=current_user
                           OR granted_role.rolname=current_user
                    )
                    AND has_table_privilege(current_user, 'control.wb_plans', 'SELECT')
                    AND has_table_privilege(current_user, 'control.wb_plans', 'INSERT')
                    AND NOT has_table_privilege(current_user, 'control.wb_plans', 'UPDATE')
                    AND NOT has_table_privilege(current_user, 'control.wb_plans', 'DELETE')
                    AND (SELECT bool_and(has_column_privilege(
                            current_user, 'control.wb_plans', allowed.column_name, 'UPDATE'
                        ))
                         FROM unnest(ARRAY[
                            'status', 'apply_started_at', 'finished_at', 'last_error_class',
                            'write_response_json', 'readback_json'
                         ]) allowed(column_name))
                    AND NOT EXISTS (
                        SELECT 1 FROM information_schema.columns column_def
                        WHERE column_def.table_schema='control'
                          AND column_def.table_name='wb_plans'
                          AND column_def.column_name <> ALL(ARRAY[
                            'status', 'apply_started_at', 'finished_at', 'last_error_class',
                            'write_response_json', 'readback_json'
                          ])
                          AND has_column_privilege(
                            current_user, 'control.wb_plans', column_def.column_name, 'UPDATE'
                          )
                    )
                    AND has_table_privilege(current_user, 'control.wb_policy_revisions', 'SELECT')
                    AND has_table_privilege(current_user, 'control.wb_policy_revisions', 'INSERT')
                    AND NOT has_table_privilege(current_user, 'control.wb_policy_revisions', 'UPDATE')
                    AND NOT has_table_privilege(current_user, 'control.wb_policy_revisions', 'DELETE')
                    AND has_table_privilege(current_user, 'control.wb_prepare_reservations', 'SELECT')
                    AND has_table_privilege(current_user, 'control.wb_prepare_reservations', 'INSERT')
                    AND NOT has_table_privilege(current_user, 'control.wb_prepare_reservations', 'UPDATE')
                    AND NOT has_table_privilege(current_user, 'control.wb_prepare_reservations', 'DELETE')
                    AND has_table_privilege(current_user, 'control.wb_plan_approvals', 'SELECT')
                    AND has_table_privilege(current_user, 'control.wb_plan_approvals', 'INSERT')
                    AND NOT has_table_privilege(current_user, 'control.wb_plan_approvals', 'UPDATE')
                    AND NOT has_table_privilege(current_user, 'control.wb_plan_approvals', 'DELETE')
                    AND has_table_privilege(current_user, 'control.wb_runtime_gates', 'SELECT')
                    AND NOT has_table_privilege(current_user, 'control.wb_runtime_gates', 'INSERT')
                    AND NOT has_table_privilege(current_user, 'control.wb_runtime_gates', 'UPDATE')
                    AND NOT has_table_privilege(current_user, 'control.wb_runtime_gates', 'DELETE')
                    AND has_table_privilege(current_user, 'control.wb_action_reservations', 'SELECT')
                    AND has_table_privilege(current_user, 'control.wb_action_reservations', 'INSERT')
                    AND NOT has_table_privilege(current_user, 'control.wb_action_reservations', 'UPDATE')
                    AND NOT has_table_privilege(current_user, 'control.wb_action_reservations', 'DELETE')
                    AND has_table_privilege(current_user, 'control.wb_audit_events', 'SELECT')
                    AND has_table_privilege(current_user, 'control.wb_audit_events', 'INSERT')
                    AND NOT has_table_privilege(current_user, 'control.wb_audit_events', 'UPDATE')
                    AND NOT has_table_privilege(current_user, 'control.wb_audit_events', 'DELETE')
                    AND NOT EXISTS (
                        SELECT 1
                        FROM unnest(ARRAY[
                            'control.wb_action_reservations',
                            'control.wb_audit_events',
                            'control.wb_plan_approvals',
                            'control.wb_plans',
                            'control.wb_policy_revisions',
                            'control.wb_prepare_reservations',
                            'control.wb_runtime_gates'
                        ]) expected_relation(relation_name)
                        WHERE has_table_privilege(
                            current_user, expected_relation.relation_name, 'TRUNCATE'
                        ) OR has_table_privilege(
                            current_user, expected_relation.relation_name, 'REFERENCES'
                        ) OR has_table_privilege(
                            current_user, expected_relation.relation_name, 'TRIGGER'
                        )
                    )
                    AND NOT has_schema_privilege(current_user, 'daily_reporting', 'USAGE')
                    AND NOT has_schema_privilege(current_user, 'search_position', 'USAGE')
                    AND (
                        SELECT array_agg(
                            schemas.nspname || '.' || relations.relname
                            ORDER BY schemas.nspname, relations.relname
                        )
                        FROM pg_catalog.pg_class relations
                        JOIN pg_catalog.pg_namespace schemas ON schemas.oid=relations.relnamespace
                        WHERE relations.relkind IN ('r','p','v','m','f')
                          AND schemas.nspname <> 'information_schema'
                          AND schemas.nspname !~ '^pg_'
                          AND (
                            has_table_privilege(current_user, relations.oid, 'SELECT')
                            OR has_table_privilege(current_user, relations.oid, 'INSERT')
                            OR has_table_privilege(current_user, relations.oid, 'UPDATE')
                            OR has_table_privilege(current_user, relations.oid, 'DELETE')
                            OR has_table_privilege(current_user, relations.oid, 'TRUNCATE')
                            OR has_table_privilege(current_user, relations.oid, 'REFERENCES')
                            OR has_table_privilege(current_user, relations.oid, 'TRIGGER')
                          )
                    ) = ARRAY[
                        'control.wb_action_reservations',
                        'control.wb_audit_events',
                        'control.wb_plan_approvals',
                        'control.wb_plans',
                        'control.wb_policy_revisions',
                        'control.wb_prepare_reservations',
                        'control.wb_runtime_gates'
                    ]::text[]
                    AND (
                        SELECT array_agg(
                            schemas.nspname || '.' || sequences.relname
                            ORDER BY schemas.nspname, sequences.relname
                        )
                        FROM pg_catalog.pg_class sequences
                        JOIN pg_catalog.pg_namespace schemas ON schemas.oid=sequences.relnamespace
                        WHERE sequences.relkind='S'
                          AND schemas.nspname <> 'information_schema'
                          AND schemas.nspname !~ '^pg_'
                          AND (
                            has_sequence_privilege(current_user, sequences.oid, 'USAGE')
                            OR has_sequence_privilege(current_user, sequences.oid, 'SELECT')
                            OR has_sequence_privilege(current_user, sequences.oid, 'UPDATE')
                          )
                    ) = ARRAY['control.wb_audit_events_id_seq']::text[]
                    AND has_sequence_privilege(
                        current_user, 'control.wb_audit_events_id_seq', 'USAGE'
                    )
                    AND has_sequence_privilege(
                        current_user, 'control.wb_audit_events_id_seq', 'SELECT'
                    )
                    AND NOT has_sequence_privilege(
                        current_user, 'control.wb_audit_events_id_seq', 'UPDATE'
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM pg_catalog.pg_proc accessible_function
                        JOIN pg_catalog.pg_namespace function_schema
                          ON function_schema.oid=accessible_function.pronamespace
                        WHERE function_schema.nspname <> 'information_schema'
                          AND function_schema.nspname !~ '^pg_'
                          AND has_schema_privilege(
                            current_user, function_schema.oid, 'USAGE'
                          )
                          AND has_function_privilege(
                            current_user, accessible_function.oid, 'EXECUTE'
                          )
                    )
                    AND (
                        SELECT array_agg(
                            concat_ws('|', tables.relname, triggers.tgname, functions.proname,
                                triggers.tgtype::text, triggers.tgenabled::text)
                            ORDER BY tables.relname, triggers.tgname
                        )
                        FROM pg_catalog.pg_trigger triggers
                        JOIN pg_catalog.pg_class tables ON tables.oid=triggers.tgrelid
                        JOIN pg_catalog.pg_namespace schemas ON schemas.oid=tables.relnamespace
                        JOIN pg_catalog.pg_proc functions ON functions.oid=triggers.tgfoid
                        WHERE schemas.nspname='control' AND NOT triggers.tgisinternal
                    ) = ARRAY[
                        'wb_action_reservations|wb_action_reservations_append_only|reject_wb_append_only_mutation|27|O',
                        'wb_action_reservations|wb_action_reservations_validate|validate_wb_reservation_insert|7|O',
                        'wb_audit_events|wb_audit_events_append_only|reject_wb_append_only_mutation|27|O',
                        'wb_plan_approvals|wb_plan_approvals_append_only|reject_wb_append_only_mutation|27|O',
                        'wb_plan_approvals|wb_plan_approvals_validate|validate_wb_approval_insert|7|O',
                        'wb_plans|wb_plans_transition_guard|enforce_wb_plan_transition|19|O',
                        'wb_plans|wb_plans_validate_insert|validate_wb_plan_insert|7|O',
                        'wb_policy_revisions|wb_policy_revisions_append_only|reject_wb_append_only_mutation|27|O',
                        'wb_policy_revisions|wb_policy_revisions_validate|validate_wb_policy_revision_insert|7|O',
                        'wb_prepare_reservations|wb_prepare_reservations_append_only|reject_wb_append_only_mutation|27|O',
                        'wb_prepare_reservations|wb_prepare_reservations_validate|validate_wb_prepare_reservation_insert|7|O',
                        'wb_runtime_gates|wb_runtime_gates_validate_write|validate_wb_runtime_gate_write|23|O'
                    ]::text[]
                    AND (
                        SELECT array_agg(
                            concat_ws('|', functions.proname, functions.prosecdef::text,
                                functions.provolatile::text,
                                COALESCE(array_to_string(functions.proconfig, ','), ''),
                                has_function_privilege(
                                    current_user, functions.oid, 'EXECUTE'
                                )::text)
                            ORDER BY functions.proname::text
                        )
                        FROM pg_catalog.pg_proc functions
                        JOIN pg_catalog.pg_namespace schemas ON schemas.oid=functions.pronamespace
                        WHERE schemas.nspname='control'
                          AND functions.prorettype='pg_catalog.trigger'::regtype
                          AND functions.prokind='f'
                    ) = ARRAY[
                        'enforce_wb_plan_transition|false|v||false',
                        'reject_wb_append_only_mutation|false|v||false',
                        'validate_wb_approval_insert|false|v||false',
                        'validate_wb_plan_insert|false|v||false',
                        'validate_wb_policy_revision_insert|false|v||false',
                        'validate_wb_prepare_reservation_insert|false|v||false',
                        'validate_wb_reservation_insert|false|v||false',
                        'validate_wb_runtime_gate_write|false|v||false'
                    ]::text[]
                    AND (
                        SELECT array_agg(constraints.conname::text ORDER BY constraints.conname::text)
                        FROM pg_catalog.pg_constraint constraints
                        JOIN pg_catalog.pg_namespace schemas ON schemas.oid=constraints.connamespace
                        WHERE schemas.nspname='control'
                          AND constraints.conname = ANY(ARRAY[
                            'wb_approval_ttl', 'wb_plan_state_shape', 'wb_plan_ttl',
                            'wb_prepare_reservation_ttl', 'wb_runtime_gate_lease_bound',
                            'wb_runtime_gate_scope'
                          ])
                          AND constraints.contype='c'
                          AND constraints.convalidated
                    ) = ARRAY[
                        'wb_approval_ttl', 'wb_plan_state_shape', 'wb_plan_ttl',
                        'wb_prepare_reservation_ttl', 'wb_runtime_gate_lease_bound',
                        'wb_runtime_gate_scope'
                    ]::text[]",
                &[],
            )
            .await
            .map_err(|_| PlanStoreError::Unavailable)?;
        if row.get::<_, bool>(0) {
            Ok(())
        } else {
            Err(PlanStoreError::Unavailable)
        }
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
    pub async fn reserve_prepare_attempt(
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
    pub async fn create(
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

    pub async fn load_for_actor(
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
    pub async fn load_by_id_for_approval(
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
    pub async fn approve(
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
    pub async fn claim_for_apply(
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
    pub async fn revalidate_before_write(
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
    pub async fn mark_stale_applying_ambiguous(
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

    pub(crate) async fn finish(
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

    pub async fn confirm_reconciled(
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

async fn reserve_action_quota(
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

async fn expire_plan(
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

fn plan_from_row(row: &Row) -> Result<WbControlPlan, PlanStoreError> {
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

fn validate_plan_id(plan_id: &str) -> Result<(), PlanStoreError> {
    if is_lower_hex_digest(plan_id) {
        Ok(())
    } else {
        Err(PlanStoreError::NotFound)
    }
}

fn validate_digest(digest: &str) -> Result<(), PlanStoreError> {
    if is_lower_hex_digest(digest) {
        Ok(())
    } else {
        Err(PlanStoreError::InvalidPlan)
    }
}

fn is_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn validate_actor_or_account(value: &str) -> Result<(), PlanStoreError> {
    if !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        Ok(())
    } else {
        Err(PlanStoreError::InvalidPlan)
    }
}

fn validate_approval_reason(reason: &str) -> Result<(), PlanStoreError> {
    if !reason.is_empty()
        && reason.len() <= 128
        && reason.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'/' | b'-')
        })
    {
        Ok(())
    } else {
        Err(PlanStoreError::InvalidPlan)
    }
}

fn cumulative_abs_delta(changes: &[WbPreparedBidChange]) -> Result<u64, PlanStoreError> {
    let total = changes.iter().try_fold(0_u64, |total, change| {
        total.checked_add(change.before_bid_kopecks.abs_diff(change.bid_kopecks))
    });
    match total {
        Some(total) if total > 0 => Ok(total),
        _ => Err(PlanStoreError::InvalidPlan),
    }
}

#[allow(clippy::too_many_arguments)]
fn make_plan_digest(
    prepare_reservation_id: &str,
    actor_id: &str,
    account_id: &str,
    advert_id: u64,
    schema_version: u32,
    policy_revision: u64,
    policy_digest: &str,
    action_quota: WbActionQuota,
    requested_json: &str,
    changes_json: &str,
    before_json: &str,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> String {
    let mut hasher = Sha256::new();
    update_digest_field(&mut hasher, PLAN_DIGEST_DOMAIN);
    update_digest_field(&mut hasher, prepare_reservation_id.as_bytes());
    update_digest_field(&mut hasher, actor_id.as_bytes());
    update_digest_field(&mut hasher, account_id.as_bytes());
    update_digest_field(&mut hasher, &advert_id.to_be_bytes());
    update_digest_field(&mut hasher, &schema_version.to_be_bytes());
    update_digest_field(&mut hasher, &policy_revision.to_be_bytes());
    update_digest_field(&mut hasher, policy_digest.as_bytes());
    update_digest_field(
        &mut hasher,
        &action_quota.max_actions_per_hour.to_be_bytes(),
    );
    update_digest_field(&mut hasher, &action_quota.max_actions_per_day.to_be_bytes());
    update_digest_field(&mut hasher, &action_quota.cooldown_seconds.to_be_bytes());
    update_digest_field(
        &mut hasher,
        &action_quota
            .max_cumulative_abs_delta_kopecks_per_day
            .to_be_bytes(),
    );
    update_digest_field(&mut hasher, requested_json.as_bytes());
    update_digest_field(&mut hasher, changes_json.as_bytes());
    update_digest_field(&mut hasher, before_json.as_bytes());
    update_digest_field(
        &mut hasher,
        created_at
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
            .as_bytes(),
    );
    update_digest_field(
        &mut hasher,
        expires_at
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
            .as_bytes(),
    );
    hex_digest(hasher.finalize())
}

fn update_digest_field(hasher: &mut Sha256, field: &[u8]) {
    hasher.update((field.len() as u64).to_be_bytes());
    hasher.update(field);
}

fn make_plan_id(plan_digest: &str, now: DateTime<Utc>) -> String {
    let sequence = PLAN_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut hasher = Sha256::new();
    update_digest_field(&mut hasher, b"mcp-ozon/wb-control-plan-id/v1");
    update_digest_field(&mut hasher, plan_digest.as_bytes());
    update_digest_field(
        &mut hasher,
        &now.timestamp_nanos_opt().unwrap_or_default().to_be_bytes(),
    );
    update_digest_field(&mut hasher, &sequence.to_be_bytes());
    hex_digest(hasher.finalize())
}

#[allow(clippy::too_many_arguments)]
fn make_prepare_reservation_id(
    actor_id: &str,
    account_id: &str,
    advert_id: u64,
    schema_version: u32,
    policy_revision: u64,
    policy_digest: &str,
    now: DateTime<Utc>,
) -> String {
    let sequence = PLAN_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut hasher = Sha256::new();
    update_digest_field(&mut hasher, b"mcp-ozon/wb-control-prepare-reservation/v1");
    update_digest_field(&mut hasher, actor_id.as_bytes());
    update_digest_field(&mut hasher, account_id.as_bytes());
    update_digest_field(&mut hasher, &advert_id.to_be_bytes());
    update_digest_field(&mut hasher, &schema_version.to_be_bytes());
    update_digest_field(&mut hasher, &policy_revision.to_be_bytes());
    update_digest_field(&mut hasher, policy_digest.as_bytes());
    update_digest_field(
        &mut hasher,
        &now.timestamp_nanos_opt().unwrap_or_default().to_be_bytes(),
    );
    update_digest_field(&mut hasher, &sequence.to_be_bytes());
    hex_digest(hasher.finalize())
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Result::map_err needs the owned error function to keep coverage complete"
)]
fn map_prepare_insert_error(error: tokio_postgres::Error) -> PlanStoreError {
    let Some(database_error) = error.as_db_error() else {
        return PlanStoreError::Unavailable;
    };
    let message = database_error.message();
    if message.contains("unresolved incident") {
        PlanStoreError::CampaignLocked
    } else if message.contains("attempt limit") || message.contains("outstanding prepare limit") {
        PlanStoreError::PrepareLimitExceeded
    } else if message.contains("active policy") {
        PlanStoreError::PolicyChanged
    } else if database_error.code() == &SqlState::UNIQUE_VIOLATION {
        PlanStoreError::InvalidState
    } else {
        PlanStoreError::Unavailable
    }
}

fn make_approval_id(
    plan_id: &str,
    plan_digest: &str,
    approver_id: &str,
    reason: &str,
    now: DateTime<Utc>,
) -> String {
    let mut hasher = Sha256::new();
    update_digest_field(&mut hasher, b"mcp-ozon/wb-control-approval/v1");
    update_digest_field(&mut hasher, plan_id.as_bytes());
    update_digest_field(&mut hasher, plan_digest.as_bytes());
    update_digest_field(&mut hasher, approver_id.as_bytes());
    update_digest_field(&mut hasher, reason.as_bytes());
    update_digest_field(
        &mut hasher,
        &now.timestamp_nanos_opt().unwrap_or_default().to_be_bytes(),
    );
    hex_digest(hasher.finalize())
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    use std::fmt::Write as _;

    bytes.as_ref().iter().fold(
        String::with_capacity(bytes.as_ref().len().saturating_mul(2)),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        },
    )
}

pub fn validate_control_database_url(value: &str) -> Result<Config, PlanStoreError> {
    let config = value
        .parse::<Config>()
        .map_err(|_| PlanStoreError::InvalidPlan)?;
    let exactly_one_tcp_host = matches!(config.get_hosts(), [Host::Tcp(host)] if !host.is_empty());
    if config.get_user() != Some("control_writer")
        || config.get_password().is_none_or(<[u8]>::is_empty)
        || config.get_dbname().is_none_or(str::is_empty)
        || !exactly_one_tcp_host
        || !config.get_hostaddrs().is_empty()
        || !matches!(config.get_ports(), [port] if *port != 0)
    {
        return Err(PlanStoreError::InvalidPlan);
    }
    Ok(config)
}

#[cfg(test)]
#[expect(
    clippy::redundant_pub_crate,
    reason = "the shared test lock is deliberately restricted to this crate"
)]
pub(crate) static CONTROL_DB_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::policy::WbBidPlacement;

    const POLICY_DIGEST: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const NEXT_POLICY_DIGEST: &str =
        "3333333333333333333333333333333333333333333333333333333333333333";
    const FIXTURE_PREPARE_RESERVATION_ID: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn quota() -> WbActionQuota {
        WbActionQuota {
            max_actions_per_hour: 10,
            max_actions_per_day: 20,
            cooldown_seconds: 30,
            max_cumulative_abs_delta_kopecks_per_day: 1_000,
        }
    }

    fn apply_context<'a>(
        plan: &'a WbControlPlan,
        actor_id: &'a str,
        now: DateTime<Utc>,
    ) -> WbApplyContext<'a> {
        WbApplyContext {
            plan_id: &plan.plan_id,
            actor_id,
            expected_plan_digest: &plan.plan_digest,
            expected_schema_version: 1,
            expected_policy_revision: 7,
            expected_policy_digest: POLICY_DIGEST,
            now,
        }
    }

    fn fixture(
        advert_id: u64,
    ) -> (
        Vec<WbBidChange>,
        Vec<WbPreparedBidChange>,
        WbCampaignBidSnapshot,
    ) {
        let requested = vec![WbBidChange {
            nm_id: 1001,
            placement: WbBidPlacement::Search,
            bid_kopecks: 1050,
        }];
        let changes = vec![WbPreparedBidChange {
            nm_id: 1001,
            placement: WbBidPlacement::Search,
            before_bid_kopecks: 1000,
            bid_kopecks: 1050,
        }];
        let before = WbCampaignBidSnapshot {
            seller_sid: "123e4567-e89b-42d3-a456-426614174000".to_owned(),
            advert_id,
            status: 9,
            bid_type: "manual".to_owned(),
            payment_type: "cpm".to_owned(),
            bids: vec![super::super::wb::WbSnapshotBid {
                nm_id: 1001,
                placement: WbBidPlacement::Search,
                bid_kopecks: 1000,
            }],
        };
        (requested, changes, before)
    }

    #[test]
    fn plan_ids_and_digests_are_bounded_and_domain_separated() {
        let now = Utc::now();
        let (requested, changes, before) = fixture(1);
        let requested_json = serde_json::to_string(&requested).unwrap();
        let changes_json = serde_json::to_string(&changes).unwrap();
        let before_json = serde_json::to_string(&before).unwrap();
        let first_digest = make_plan_digest(
            FIXTURE_PREPARE_RESERVATION_ID,
            "actor",
            "account",
            1,
            1,
            7,
            POLICY_DIGEST,
            quota(),
            &requested_json,
            &changes_json,
            &before_json,
            now,
            now + PLAN_TTL,
        );
        let changed_policy_digest = make_plan_digest(
            FIXTURE_PREPARE_RESERVATION_ID,
            "actor",
            "account",
            1,
            1,
            8,
            POLICY_DIGEST,
            quota(),
            &requested_json,
            &changes_json,
            &before_json,
            now,
            now + PLAN_TTL,
        );
        assert_eq!(first_digest.len(), 64);
        assert_ne!(first_digest, changed_policy_digest);
        let first_id = make_plan_id(&first_digest, now);
        let second_id = make_plan_id(&first_digest, now);
        assert_ne!(first_id, second_id);
        assert!(validate_plan_id(&first_id).is_ok());
        assert!(validate_plan_id("1 OR 1=1").is_err());
    }

    #[test]
    fn quotas_are_bounded_and_delta_is_checked() {
        assert!(quota().validate().is_ok());
        assert!(
            WbActionQuota {
                max_actions_per_hour: 2,
                max_actions_per_day: 1,
                ..quota()
            }
            .validate()
            .is_err()
        );
        let (_, changes, _) = fixture(1);
        assert_eq!(cumulative_abs_delta(&changes).unwrap(), 50);
    }

    #[test]
    fn statuses_and_local_validation_cover_every_fail_closed_mapping() {
        for (status, database_value) in [
            (WbPlanStatus::Prepared, "prepared"),
            (WbPlanStatus::Approved, "approved"),
            (WbPlanStatus::Applying, "applying"),
            (WbPlanStatus::Applied, "applied"),
            (
                WbPlanStatus::ReconciliationRequired,
                "reconciliation_required",
            ),
            (WbPlanStatus::Ambiguous, "ambiguous"),
            (WbPlanStatus::Rejected, "rejected"),
            (WbPlanStatus::Failed, "failed"),
            (WbPlanStatus::Expired, "expired"),
        ] {
            assert_eq!(status.as_db(), database_value);
            assert_eq!(WbPlanStatus::from_db(database_value).unwrap(), status);
        }
        assert_eq!(
            WbPlanStatus::from_db("foreign_state"),
            Err(PlanStoreError::Unavailable)
        );

        assert_eq!(
            validate_digest("not-a-digest"),
            Err(PlanStoreError::InvalidPlan)
        );
        assert_eq!(
            validate_digest(&"A".repeat(64)),
            Err(PlanStoreError::InvalidPlan)
        );
        assert_eq!(
            validate_actor_or_account("contains/slash"),
            Err(PlanStoreError::InvalidPlan)
        );
        assert_eq!(
            validate_approval_reason("free form"),
            Err(PlanStoreError::InvalidPlan)
        );
        assert_eq!(cumulative_abs_delta(&[]), Err(PlanStoreError::InvalidPlan));
        let zero_delta = [WbPreparedBidChange {
            nm_id: 1,
            placement: WbBidPlacement::Search,
            before_bid_kopecks: 10,
            bid_kopecks: 10,
        }];
        assert_eq!(
            cumulative_abs_delta(&zero_delta),
            Err(PlanStoreError::InvalidPlan)
        );
        let overflowing_delta = [
            WbPreparedBidChange {
                nm_id: 1,
                placement: WbBidPlacement::Search,
                before_bid_kopecks: 0,
                bid_kopecks: u64::MAX,
            },
            WbPreparedBidChange {
                nm_id: 2,
                placement: WbBidPlacement::Search,
                before_bid_kopecks: 0,
                bid_kopecks: 1,
            },
        ];
        assert_eq!(
            cumulative_abs_delta(&overflowing_delta),
            Err(PlanStoreError::InvalidPlan)
        );
    }

    #[tokio::test]
    async fn prepare_error_mapper_handles_transport_errors_without_fabricating_db_state() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let closer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            drop(socket);
        });
        let connect_result = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=control_writer password=test dbname=test",
                address.port()
            ),
            tokio_postgres::NoTls,
        )
        .await;
        closer.await.unwrap();
        let error = connect_result
            .err()
            .expect("closed local socket must fail PostgreSQL startup");
        assert_eq!(map_prepare_insert_error(error), PlanStoreError::Unavailable);
    }

    async fn classify_database_failures_with_optional_test_database(
        admin_url: Result<String, std::env::VarError>,
    ) {
        let Ok(admin_url) = admin_url else {
            return;
        };
        let _database_guard = CONTROL_DB_TEST_LOCK.lock().await;
        let (mut admin, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let connection_task = tokio::spawn(connection);

        for (message, expected) in [
            ("unresolved incident", PlanStoreError::CampaignLocked),
            ("attempt limit", PlanStoreError::PrepareLimitExceeded),
            (
                "outstanding prepare limit",
                PlanStoreError::PrepareLimitExceeded,
            ),
            ("active policy", PlanStoreError::PolicyChanged),
            ("unclassified database failure", PlanStoreError::Unavailable),
        ] {
            let statement =
                format!("DO $coverage$ BEGIN RAISE EXCEPTION '{message}'; END $coverage$");
            let error = admin.batch_execute(&statement).await.unwrap_err();
            assert_eq!(map_prepare_insert_error(error), expected);
        }

        admin
            .batch_execute(
                "CREATE TEMP TABLE coverage_prepare_unique (id integer PRIMARY KEY); \
                 INSERT INTO coverage_prepare_unique VALUES (1);",
            )
            .await
            .unwrap();
        let unique_error = admin
            .execute("INSERT INTO coverage_prepare_unique VALUES (1)", &[])
            .await
            .unwrap_err();
        assert_eq!(
            map_prepare_insert_error(unique_error),
            PlanStoreError::InvalidState
        );

        let (_, _, before) = fixture(1);
        let orphan_approval_row = admin
            .query_one(
                "SELECT repeat('a',64)::text, repeat('b',64)::text, \
                        'coverage_actor'::text, 'coverage_account'::text, \
                        1::bigint, 1::integer, 7::bigint, $1::text, \
                        10::integer, 20::integer, 30::integer, 1000::bigint, \
                        'prepared'::text, '[]'::text, '[]'::text, $2::text, \
                        clock_timestamp(), clock_timestamp()+interval '5 minutes', \
                        NULL::timestamptz, NULL::text, NULL::text, NULL::text, \
                        repeat('c',64)::text, NULL::text, 'orphan_approver'::text, \
                        NULL::text, NULL::timestamptz, NULL::timestamptz",
                &[&POLICY_DIGEST, &serde_json::to_string(&before).unwrap()],
            )
            .await
            .unwrap();
        assert!(matches!(
            plan_from_row(&orphan_approval_row),
            Err(PlanStoreError::Unavailable)
        ));

        let transaction = admin.transaction().await.unwrap();
        assert_eq!(
            expire_plan(
                &transaction,
                "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                "coverage_actor",
                Utc::now(),
            )
            .await,
            Err(PlanStoreError::InvalidState)
        );
        transaction.rollback().await.unwrap();

        drop(admin);
        connection_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn prepare_error_mapper_classifies_database_failures() {
        classify_database_failures_with_optional_test_database(std::env::var(
            "POSITION_REPOSITORY_TEST_ADMIN_URL",
        ))
        .await;
        classify_database_failures_with_optional_test_database(Err(std::env::VarError::NotPresent))
            .await;
    }

    #[test]
    fn database_url_requires_restricted_identity_and_network_target() {
        assert!(
            validate_control_database_url(
                "postgresql://control_writer:secret@position-db:5432/ozon_positions"
            )
            .is_ok()
        );
        assert!(
            validate_control_database_url(
                "postgresql://postgres:secret@position-db/ozon_positions"
            )
            .is_err()
        );
        assert!(
            validate_control_database_url("postgresql://control_writer@/ozon_positions").is_err()
        );
        assert!(
            validate_control_database_url(
                "postgresql://control_writer:secret@first:5432,second:5432/ozon_positions"
            )
            .is_err()
        );
        assert!(
            validate_control_database_url(
                "user=control_writer password=secret dbname=ozon_positions host=/tmp port=5432"
            )
            .is_err()
        );
    }

    async fn set_gate(
        admin: &Client,
        gate_key: &str,
        scope_kind: &str,
        account_id: Option<&str>,
        advert_id: Option<i64>,
        enabled: bool,
        now: DateTime<Utc>,
    ) {
        admin
            .execute(
                "INSERT INTO control.wb_runtime_gates \
                    (gate_key, scope_kind, account_id, advert_id, enabled, lease_expires_at, \
                     disabled_until, revision, reason, updated_by, updated_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,NULL,1,'integration_test','integration_test',$7) \
                 ON CONFLICT (gate_key) DO UPDATE SET \
                    enabled=EXCLUDED.enabled, lease_expires_at=EXCLUDED.lease_expires_at, \
                    disabled_until=NULL, revision=control.wb_runtime_gates.revision+1, \
                    reason=EXCLUDED.reason, updated_by=EXCLUDED.updated_by, \
                    updated_at=EXCLUDED.updated_at",
                &[
                    &gate_key,
                    &scope_kind,
                    &account_id,
                    &advert_id,
                    &enabled,
                    &(now + Duration::minutes(10)),
                    &now,
                ],
            )
            .await
            .unwrap();
    }

    async fn enable_gates(admin: &Client, account_id: &str, advert_id: u64, now: DateTime<Utc>) {
        set_gate(admin, "global", "global", None, None, true, now).await;
        let account_gate = format!("account/{account_id}");
        set_gate(
            admin,
            &account_gate,
            "account",
            Some(account_id),
            None,
            true,
            now,
        )
        .await;
        let campaign_gate = format!("campaign/{account_id}/{advert_id}");
        set_gate(
            admin,
            &campaign_gate,
            "campaign",
            Some(account_id),
            Some(i64::try_from(advert_id).unwrap()),
            true,
            now,
        )
        .await;
    }

    async fn create_fixture_plan(
        repository: &WbPlanRepository,
        actor_id: &str,
        account_id: &str,
        advert_id: u64,
        action_quota: WbActionQuota,
        now: DateTime<Utc>,
    ) -> WbControlPlan {
        let (requested, changes, before) = fixture(advert_id);
        let prepare_reservation = repository
            .reserve_prepare_attempt(
                actor_id,
                account_id,
                advert_id,
                1,
                7,
                POLICY_DIGEST,
                action_quota,
                now,
            )
            .await
            .unwrap();
        repository
            .create(
                actor_id,
                account_id,
                advert_id,
                1,
                7,
                POLICY_DIGEST,
                action_quota,
                &prepare_reservation.reservation_id,
                &requested,
                &changes,
                &before,
                now,
            )
            .await
            .unwrap()
    }

    async fn create_approved_fixture_plan(
        repository: &WbPlanRepository,
        actor_id: &str,
        account_id: &str,
        advert_id: u64,
        action_quota: WbActionQuota,
        now: DateTime<Utc>,
    ) -> WbControlPlan {
        let plan = create_fixture_plan(
            repository,
            actor_id,
            account_id,
            advert_id,
            action_quota,
            now,
        )
        .await;
        repository
            .approve(
                &plan.plan_id,
                "integration_approver",
                &plan.plan_digest,
                "coverage/approval",
                now,
            )
            .await
            .unwrap();
        plan
    }

    async fn create_applying_fixture_plan(
        repository: &WbPlanRepository,
        admin: &Client,
        actor_id: &str,
        account_id: &str,
        advert_id: u64,
        action_quota: WbActionQuota,
        now: DateTime<Utc>,
    ) -> WbControlPlan {
        let plan = create_approved_fixture_plan(
            repository,
            actor_id,
            account_id,
            advert_id,
            action_quota,
            now,
        )
        .await;
        enable_gates(admin, account_id, advert_id, now).await;
        repository
            .claim_for_apply(apply_context(&plan, actor_id, now))
            .await
            .unwrap();
        plan
    }

    async fn run_repository_scenarios_with_optional_test_database(
        database_url: Result<String, std::env::VarError>,
        admin_url: Result<String, std::env::VarError>,
    ) {
        let (Ok(database_url), Ok(admin_url)) = (database_url, admin_url) else {
            return;
        };
        let _database_guard = CONTROL_DB_TEST_LOCK.lock().await;
        let config = validate_control_database_url(&database_url).unwrap();
        let repository = WbPlanRepository::connect(&config).await.unwrap();
        repository.verify_runtime_contract().await.unwrap();
        let (mut admin, admin_connection) =
            tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
                .await
                .unwrap();
        let admin_connection_task = tokio::spawn(admin_connection);
        let (preconnected_client, preconnected_connection) =
            tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
                .await
                .unwrap();
        let preconnected_connection_task = tokio::spawn(preconnected_connection);
        let preconnected_repository = WbPlanRepository::from_client(preconnected_client);
        preconnected_repository
            .verify_runtime_contract()
            .await
            .unwrap();
        drop(preconnected_repository);
        preconnected_connection_task.await.unwrap().unwrap();
        let (direct_writer, direct_writer_connection) =
            tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
                .await
                .unwrap();
        let direct_writer_connection_task = tokio::spawn(direct_writer_connection);

        admin
            .execute(
                "ALTER TABLE control.wb_prepare_reservations \
                 DISABLE TRIGGER wb_prepare_reservations_append_only",
                &[],
            )
            .await
            .unwrap();
        let disabled_trigger_contract = repository.verify_runtime_contract().await;
        admin
            .execute(
                "ALTER TABLE control.wb_prepare_reservations \
                 ENABLE TRIGGER wb_prepare_reservations_append_only",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            disabled_trigger_contract,
            Err(PlanStoreError::Unavailable)
        ));
        repository.verify_runtime_contract().await.unwrap();
        admin
            .execute("ALTER ROLE control_writer CONNECTION LIMIT 5", &[])
            .await
            .unwrap();
        let widened_role_contract = repository.verify_runtime_contract().await;
        admin
            .execute("ALTER ROLE control_writer CONNECTION LIMIT 4", &[])
            .await
            .unwrap();
        assert!(matches!(
            widened_role_contract,
            Err(PlanStoreError::Unavailable)
        ));
        admin
            .execute(
                "GRANT TEMPORARY ON DATABASE ozon_positions TO control_writer",
                &[],
            )
            .await
            .unwrap();
        let widened_database_contract = repository.verify_runtime_contract().await;
        admin
            .execute(
                "REVOKE TEMPORARY ON DATABASE ozon_positions FROM control_writer",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            widened_database_contract,
            Err(PlanStoreError::Unavailable)
        ));
        admin
            .execute(
                "GRANT CREATE ON DATABASE ozon_positions TO control_writer",
                &[],
            )
            .await
            .unwrap();
        let database_create_contract = repository.verify_runtime_contract().await;
        admin
            .execute(
                "REVOKE CREATE ON DATABASE ozon_positions FROM control_writer",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            database_create_contract,
            Err(PlanStoreError::Unavailable)
        ));
        admin
            .execute("GRANT CREATE ON SCHEMA public TO control_writer", &[])
            .await
            .unwrap();
        let schema_create_contract = repository.verify_runtime_contract().await;
        admin
            .execute("REVOKE CREATE ON SCHEMA public FROM control_writer", &[])
            .await
            .unwrap();
        assert!(matches!(
            schema_create_contract,
            Err(PlanStoreError::Unavailable)
        ));
        repository.verify_runtime_contract().await.unwrap();

        let now = Utc::now();
        repository
            .register_policy(1, 7, POLICY_DIGEST, now)
            .await
            .unwrap();
        assert_eq!(
            repository
                .reserve_prepare_attempt(
                    "wrong_policy_actor",
                    "wrong_policy_account",
                    41,
                    1,
                    6,
                    POLICY_DIGEST,
                    quota(),
                    now,
                )
                .await,
            Err(PlanStoreError::PolicyChanged)
        );
        assert_eq!(
            repository.register_policy(0, 7, POLICY_DIGEST, now).await,
            Err(PlanStoreError::InvalidPlan)
        );
        repository
            .register_policy(1, 7, POLICY_DIGEST, now)
            .await
            .unwrap();
        assert!(matches!(
            repository
                .register_policy(1, 7, NEXT_POLICY_DIGEST, now)
                .await,
            Err(PlanStoreError::PolicyChanged)
        ));
        assert_eq!(
            repository.register_policy(1, 8, POLICY_DIGEST, now).await,
            Err(PlanStoreError::PolicyChanged)
        );
        admin
            .execute(
                "REVOKE INSERT ON control.wb_policy_revisions FROM control_writer",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            repository
                .register_policy(1, 8, NEXT_POLICY_DIGEST, now)
                .await,
            Err(PlanStoreError::Unavailable)
        );
        admin
            .execute(
                "GRANT INSERT ON control.wb_policy_revisions TO control_writer",
                &[],
            )
            .await
            .unwrap();
        repository.verify_runtime_contract().await.unwrap();
        let plan = create_fixture_plan(
            &repository,
            "integration_actor",
            "integration_account",
            42,
            quota(),
            now + Duration::days(365),
        )
        .await;
        assert!(plan.created_at < now + Duration::minutes(1));
        assert_eq!(
            repository
                .load_by_id_for_approval(&plan.plan_id)
                .await
                .unwrap()
                .plan_digest,
            plan.plan_digest
        );
        assert_eq!(
            repository
                .reserve_prepare_attempt(
                    "integration_actor",
                    "integration_account",
                    0,
                    1,
                    7,
                    POLICY_DIGEST,
                    quota(),
                    now,
                )
                .await,
            Err(PlanStoreError::InvalidPlan)
        );
        let (fixture_requested, fixture_changes, fixture_before) = fixture(42);
        assert!(matches!(
            repository
                .create(
                    "integration_actor",
                    "integration_account",
                    42,
                    1,
                    7,
                    POLICY_DIGEST,
                    quota(),
                    &plan.prepare_reservation_id,
                    &fixture_requested,
                    &[],
                    &fixture_before,
                    now,
                )
                .await,
            Err(PlanStoreError::InvalidPlan)
        ));
        let mut excessive_change = fixture_changes.clone();
        excessive_change[0].bid_kopecks = 5_000;
        assert!(matches!(
            repository
                .create(
                    "integration_actor",
                    "integration_account",
                    42,
                    1,
                    7,
                    POLICY_DIGEST,
                    quota(),
                    &plan.prepare_reservation_id,
                    &fixture_requested,
                    &excessive_change,
                    &fixture_before,
                    now,
                )
                .await,
            Err(PlanStoreError::QuotaExceeded)
        ));
        assert!(matches!(
            repository
                .create(
                    "integration_actor",
                    "integration_account",
                    42,
                    1,
                    6,
                    POLICY_DIGEST,
                    quota(),
                    &plan.prepare_reservation_id,
                    &fixture_requested,
                    &fixture_changes,
                    &fixture_before,
                    now,
                )
                .await,
            Err(PlanStoreError::PolicyChanged)
        ));
        assert!(matches!(
            repository
                .create(
                    "different_actor",
                    "integration_account",
                    42,
                    1,
                    7,
                    POLICY_DIGEST,
                    quota(),
                    &plan.prepare_reservation_id,
                    &fixture_requested,
                    &fixture_changes,
                    &fixture_before,
                    now,
                )
                .await,
            Err(PlanStoreError::InvalidPlan)
        ));
        assert!(matches!(
            repository
                .create(
                    "integration_actor",
                    "integration_account",
                    42,
                    1,
                    7,
                    POLICY_DIGEST,
                    quota(),
                    &plan.prepare_reservation_id,
                    &fixture_requested,
                    &fixture_changes,
                    &fixture_before,
                    now,
                )
                .await,
            Err(PlanStoreError::InvalidState)
        ));

        let expired_prepare = repository
            .reserve_prepare_attempt(
                "expired_prepare_actor",
                "expired_prepare_account",
                43,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                now,
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_prepare_reservations \
                 DISABLE TRIGGER wb_prepare_reservations_append_only",
                &[],
            )
            .await
            .unwrap();
        admin
            .execute(
                "UPDATE control.wb_prepare_reservations reservation SET \
                     reserved_at=skew.reserved_at, expires_at=skew.reserved_at+interval '2 minutes' \
                 FROM (SELECT clock_timestamp()-interval '3 minutes' AS reserved_at) skew \
                 WHERE reservation.reservation_id=$1",
                &[&expired_prepare.reservation_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_prepare_reservations \
                 ENABLE TRIGGER wb_prepare_reservations_append_only",
                &[],
            )
            .await
            .unwrap();
        let (expired_requested, expired_changes, expired_before) = fixture(43);
        assert!(matches!(
            repository
                .create(
                    "expired_prepare_actor",
                    "expired_prepare_account",
                    43,
                    1,
                    7,
                    POLICY_DIGEST,
                    quota(),
                    &expired_prepare.reservation_id,
                    &expired_requested,
                    &expired_changes,
                    &expired_before,
                    now,
                )
                .await,
            Err(PlanStoreError::PrepareLimitExceeded)
        ));

        let mut outstanding_prepares = Vec::new();
        for _ in 0..3 {
            outstanding_prepares.push(
                repository
                    .reserve_prepare_attempt(
                        "outstanding_prepare_actor",
                        "outstanding_prepare_account",
                        49,
                        1,
                        7,
                        POLICY_DIGEST,
                        quota(),
                        now,
                    )
                    .await
                    .unwrap(),
            );
        }
        admin
            .execute(
                "ALTER TABLE control.wb_prepare_reservations \
                 DISABLE TRIGGER wb_prepare_reservations_validate",
                &[],
            )
            .await
            .unwrap();
        admin
            .execute(
                "INSERT INTO control.wb_prepare_reservations \
                    (reservation_id, actor_id, account_id, advert_id, schema_version, \
                     policy_revision, policy_digest, quota_max_actions_per_hour, \
                     quota_max_actions_per_day, quota_cooldown_seconds, \
                     quota_max_cumulative_abs_delta_kopecks_per_day, reserved_at, expires_at) \
                 SELECT repeat('e',64), actor_id, account_id, advert_id, schema_version, \
                        policy_revision, policy_digest, quota_max_actions_per_hour, \
                        quota_max_actions_per_day, quota_cooldown_seconds, \
                        quota_max_cumulative_abs_delta_kopecks_per_day, reserved_at, expires_at \
                 FROM control.wb_prepare_reservations WHERE reservation_id=$1",
                &[&outstanding_prepares[0].reservation_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_prepare_reservations \
                 ENABLE TRIGGER wb_prepare_reservations_validate",
                &[],
            )
            .await
            .unwrap();
        let (outstanding_requested, outstanding_changes, outstanding_before) = fixture(49);
        assert!(matches!(
            repository
                .create(
                    "outstanding_prepare_actor",
                    "outstanding_prepare_account",
                    49,
                    1,
                    7,
                    POLICY_DIGEST,
                    quota(),
                    &outstanding_prepares[0].reservation_id,
                    &outstanding_requested,
                    &outstanding_changes,
                    &outstanding_before,
                    now,
                )
                .await,
            Err(PlanStoreError::PrepareLimitExceeded)
        ));
        assert!(matches!(
            repository
                .claim_for_apply(apply_context(&plan, "integration_actor", now))
                .await,
            Err(PlanStoreError::ApprovalRequired)
        ));
        let mut wrong_claim_digest = apply_context(&plan, "integration_actor", now);
        wrong_claim_digest.expected_plan_digest = NEXT_POLICY_DIGEST;
        assert!(matches!(
            repository.claim_for_apply(wrong_claim_digest).await,
            Err(PlanStoreError::PlanChanged)
        ));
        assert!(matches!(
            repository
                .approve(
                    &plan.plan_id,
                    "integration_approver",
                    "2222222222222222222222222222222222222222222222222222222222222222",
                    "integration/approval",
                    now,
                )
                .await,
            Err(PlanStoreError::PlanChanged)
        ));
        assert!(matches!(
            repository
                .approve(
                    &plan.plan_id,
                    "integration_actor",
                    &plan.plan_digest,
                    "self-approval",
                    now,
                )
                .await,
            Err(PlanStoreError::InvalidState)
        ));
        let approved = repository
            .approve(
                &plan.plan_id,
                "integration_approver",
                &plan.plan_digest,
                "integration/approval",
                now + Duration::days(365),
            )
            .await
            .unwrap();
        assert_eq!(approved.status, WbPlanStatus::Approved);
        assert!(approved.approval.is_some());
        repository
            .approve(
                &plan.plan_id,
                "integration_approver",
                &plan.plan_digest,
                "integration/approval",
                now,
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .approve(
                    &plan.plan_id,
                    "integration_approver",
                    &plan.plan_digest,
                    "integration/different",
                    now,
                )
                .await,
            Err(PlanStoreError::InvalidState)
        ));
        let mut wrong_claim_policy = apply_context(&plan, "integration_actor", now);
        wrong_claim_policy.expected_policy_revision = 6;
        assert!(matches!(
            repository.claim_for_apply(wrong_claim_policy).await,
            Err(PlanStoreError::PolicyChanged)
        ));
        assert!(matches!(
            repository
                .claim_for_apply(apply_context(&plan, "integration_actor", now))
                .await,
            Err(PlanStoreError::RuntimeDisabled)
        ));
        enable_gates(&admin, "integration_account", 42, now).await;
        assert!(
            admin
                .execute(
                    "UPDATE control.wb_runtime_gates \
                     SET revision=revision+1, updated_at=$1, lease_expires_at=$2 \
                     WHERE gate_key='global'",
                    &[&(now + Duration::days(365)), &(now + Duration::days(365))],
                )
                .await
                .is_err()
        );
        let claimed = repository
            .claim_for_apply(apply_context(&plan, "integration_actor", now))
            .await
            .unwrap();
        assert_eq!(claimed.status, WbPlanStatus::Applying);
        assert!(matches!(
            repository
                .claim_for_apply(apply_context(&plan, "integration_actor", now))
                .await,
            Err(PlanStoreError::InvalidState)
        ));
        let mut wrong_revalidate_digest = apply_context(&plan, "integration_actor", now);
        wrong_revalidate_digest.expected_plan_digest = NEXT_POLICY_DIGEST;
        assert!(matches!(
            repository
                .revalidate_before_write(wrong_revalidate_digest)
                .await,
            Err(PlanStoreError::PlanChanged)
        ));
        let mut wrong_revalidate_policy = apply_context(&plan, "integration_actor", now);
        wrong_revalidate_policy.expected_policy_revision = 6;
        assert!(matches!(
            repository
                .revalidate_before_write(wrong_revalidate_policy)
                .await,
            Err(PlanStoreError::PolicyChanged)
        ));
        repository
            .revalidate_before_write(apply_context(&plan, "integration_actor", now))
            .await
            .unwrap();
        let (_, _, before) = fixture(42);
        let wrong_readback_json = serde_json::to_string(&before).unwrap();
        assert!(
            direct_writer
                .execute(
                    "UPDATE control.wb_plans SET status='applied', \
                         finished_at=clock_timestamp(), write_response_json='{}', \
                         readback_json=$2 WHERE plan_id=$1 AND status='applying'",
                    &[&plan.plan_id, &wrong_readback_json],
                )
                .await
                .is_err()
        );
        let mut after = before.clone();
        after.bids[0].bid_kopecks = 1050;
        let mut wrong_seller_readback = serde_json::to_value(&after).unwrap();
        wrong_seller_readback["seller_sid"] =
            Value::String("22222222-2222-4222-8222-222222222222".to_owned());
        let wrong_seller_readback = serde_json::to_string(&wrong_seller_readback).unwrap();
        assert!(
            direct_writer
                .execute(
                    "UPDATE control.wb_plans SET status='applied', \
                         finished_at=clock_timestamp(), write_response_json='{}', \
                         readback_json=$2 WHERE plan_id=$1 AND status='applying'",
                    &[&plan.plan_id, &wrong_seller_readback],
                )
                .await
                .is_err()
        );
        let exact_readback_json = serde_json::to_string(&after).unwrap();
        assert!(
            direct_writer
                .execute(
                    "UPDATE control.wb_plans SET status='applied', \
                         finished_at=clock_timestamp(), write_response_json=NULL, \
                         readback_json=$2 WHERE plan_id=$1 AND status='applying'",
                    &[&plan.plan_id, &exact_readback_json],
                )
                .await
                .is_err()
        );
        assert!(matches!(
            repository
                .finish(
                    &plan.plan_id,
                    "integration_actor",
                    WbPlanFinish {
                        status: WbPlanStatus::Prepared,
                        error_class: None,
                        write_response: None,
                        readback: None,
                        now,
                    },
                )
                .await,
            Err(PlanStoreError::InvalidState)
        ));
        assert!(matches!(
            repository
                .finish(
                    &plan.plan_id,
                    "integration_actor",
                    WbPlanFinish {
                        status: WbPlanStatus::Applied,
                        error_class: None,
                        write_response: None,
                        readback: Some(&after),
                        now,
                    },
                )
                .await,
            Err(PlanStoreError::InvalidPlan)
        ));
        repository
            .finish(
                &plan.plan_id,
                "integration_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Applied,
                    error_class: None,
                    write_response: Some(&serde_json::json!({"ok": true})),
                    readback: Some(&after),
                    now,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            repository
                .load_for_actor(&plan.plan_id, "integration_actor")
                .await
                .unwrap()
                .status,
            WbPlanStatus::Applied
        );
        repository
            .confirm_reconciled(&plan.plan_id, "integration_actor", &after, now)
            .await
            .unwrap();
        let rejected_reservation = admin.transaction().await.unwrap();
        assert_eq!(
            reserve_action_quota(&rejected_reservation, &plan, Utc::now() + Duration::days(2),)
                .await,
            Err(PlanStoreError::Unavailable)
        );
        rejected_reservation.rollback().await.unwrap();
        assert!(matches!(
            repository
                .approve(
                    &plan.plan_id,
                    "integration_approver",
                    &plan.plan_digest,
                    "integration/approval",
                    now,
                )
                .await,
            Err(PlanStoreError::InvalidState)
        ));
        assert!(matches!(
            repository
                .revalidate_before_write(apply_context(&plan, "integration_actor", now))
                .await,
            Err(PlanStoreError::InvalidState)
        ));
        assert!(matches!(
            repository
                .finish(
                    &plan.plan_id,
                    "integration_actor",
                    WbPlanFinish {
                        status: WbPlanStatus::Applied,
                        error_class: None,
                        write_response: Some(&serde_json::json!({"ok": true})),
                        readback: Some(&after),
                        now,
                    },
                )
                .await,
            Err(PlanStoreError::InvalidState)
        ));

        let expired_approval_plan = create_fixture_plan(
            &repository,
            "approval_expiry_actor",
            "integration_account",
            45,
            quota(),
            now,
        )
        .await;
        repository
            .approve(
                &expired_approval_plan.plan_id,
                "integration_approver",
                &expired_approval_plan.plan_digest,
                "approval/expiry",
                now,
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_plan_approvals \
                 DISABLE TRIGGER wb_plan_approvals_append_only",
                &[],
            )
            .await
            .unwrap();
        admin
            .execute(
                "UPDATE control.wb_plan_approvals approval \
                 SET approved_at=skew.approved_at, \
                     expires_at=skew.approved_at + interval '1 minute' \
                 FROM (SELECT clock_timestamp() - interval '3 minutes' AS approved_at) skew \
                 WHERE approval.plan_id=$1",
                &[&expired_approval_plan.plan_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_plan_approvals \
                 ENABLE TRIGGER wb_plan_approvals_append_only",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .approve(
                    &expired_approval_plan.plan_id,
                    "integration_approver",
                    &expired_approval_plan.plan_digest,
                    "approval/expiry",
                    now + Duration::days(365),
                )
                .await,
            Err(PlanStoreError::ApprovalExpired)
        ));
        assert_eq!(
            repository
                .load_for_actor(&expired_approval_plan.plan_id, "approval_expiry_actor")
                .await
                .unwrap()
                .status,
            WbPlanStatus::Expired
        );
        repository.verify_runtime_contract().await.unwrap();

        let expired_plan = create_fixture_plan(
            &repository,
            "expired_plan_actor",
            "coverage_account",
            50,
            quota(),
            now,
        )
        .await;
        admin
            .execute(
                "ALTER TABLE control.wb_plans DISABLE TRIGGER wb_plans_transition_guard",
                &[],
            )
            .await
            .unwrap();
        admin
            .execute(
                "UPDATE control.wb_plans plan SET \
                     created_at=skew.created_at, \
                     expires_at=skew.created_at+interval '5 minutes' \
                 FROM (SELECT clock_timestamp()-interval '6 minutes' AS created_at) skew \
                 WHERE plan.plan_id=$1",
                &[&expired_plan.plan_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_plans ENABLE TRIGGER wb_plans_transition_guard",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .approve(
                    &expired_plan.plan_id,
                    "integration_approver",
                    &expired_plan.plan_digest,
                    "coverage/expired-plan",
                    now,
                )
                .await,
            Err(PlanStoreError::Expired)
        ));
        assert!(matches!(
            repository
                .mark_stale_applying_ambiguous(&expired_plan.plan_id, "expired_plan_actor", now,)
                .await,
            Err(PlanStoreError::InvalidState)
        ));

        let claim_approval_expired = create_approved_fixture_plan(
            &repository,
            "claim_approval_expired_actor",
            "coverage_account",
            51,
            quota(),
            now,
        )
        .await;
        admin
            .execute(
                "ALTER TABLE control.wb_plan_approvals \
                 DISABLE TRIGGER wb_plan_approvals_append_only",
                &[],
            )
            .await
            .unwrap();
        admin
            .execute(
                "UPDATE control.wb_plan_approvals approval SET \
                     approved_at=skew.approved_at, \
                     expires_at=skew.approved_at+interval '1 minute' \
                 FROM (SELECT clock_timestamp()-interval '3 minutes' AS approved_at) skew \
                 WHERE approval.plan_id=$1",
                &[&claim_approval_expired.plan_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_plan_approvals \
                 ENABLE TRIGGER wb_plan_approvals_append_only",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .claim_for_apply(apply_context(
                    &claim_approval_expired,
                    "claim_approval_expired_actor",
                    now,
                ))
                .await,
            Err(PlanStoreError::ApprovalExpired)
        ));

        let claim_plan_expired = create_approved_fixture_plan(
            &repository,
            "claim_plan_expired_actor",
            "coverage_account",
            52,
            quota(),
            now,
        )
        .await;
        admin
            .execute(
                "ALTER TABLE control.wb_plans DISABLE TRIGGER wb_plans_transition_guard",
                &[],
            )
            .await
            .unwrap();
        admin
            .execute(
                "UPDATE control.wb_plans plan SET \
                     created_at=skew.created_at, \
                     expires_at=skew.created_at+interval '5 minutes' \
                 FROM (SELECT clock_timestamp()-interval '6 minutes' AS created_at) skew \
                 WHERE plan.plan_id=$1",
                &[&claim_plan_expired.plan_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_plans ENABLE TRIGGER wb_plans_transition_guard",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .claim_for_apply(apply_context(
                    &claim_plan_expired,
                    "claim_plan_expired_actor",
                    now,
                ))
                .await,
            Err(PlanStoreError::Expired)
        ));

        let revalidate_plan_expired = create_applying_fixture_plan(
            &repository,
            &admin,
            "revalidate_plan_expired_actor",
            "coverage_account",
            53,
            quota(),
            now,
        )
        .await;
        admin
            .execute(
                "ALTER TABLE control.wb_plans DISABLE TRIGGER wb_plans_transition_guard",
                &[],
            )
            .await
            .unwrap();
        admin
            .execute(
                "UPDATE control.wb_plans plan SET \
                     created_at=skew.created_at, \
                     expires_at=skew.created_at+interval '5 minutes' \
                 FROM (SELECT clock_timestamp()-interval '6 minutes' AS created_at) skew \
                 WHERE plan.plan_id=$1",
                &[&revalidate_plan_expired.plan_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_plans ENABLE TRIGGER wb_plans_transition_guard",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .revalidate_before_write(apply_context(
                    &revalidate_plan_expired,
                    "revalidate_plan_expired_actor",
                    now,
                ))
                .await,
            Err(PlanStoreError::Expired)
        ));
        repository
            .finish(
                &revalidate_plan_expired.plan_id,
                "revalidate_plan_expired_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Failed,
                    error_class: Some("coverage_expired"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await
            .unwrap();

        let revalidate_approval_expired = create_applying_fixture_plan(
            &repository,
            &admin,
            "revalidate_approval_expired_actor",
            "coverage_account",
            54,
            quota(),
            now,
        )
        .await;
        admin
            .execute(
                "ALTER TABLE control.wb_plan_approvals \
                 DISABLE TRIGGER wb_plan_approvals_append_only",
                &[],
            )
            .await
            .unwrap();
        admin
            .execute(
                "UPDATE control.wb_plan_approvals approval SET \
                     approved_at=skew.approved_at, \
                     expires_at=skew.approved_at+interval '1 minute' \
                 FROM (SELECT clock_timestamp()-interval '3 minutes' AS approved_at) skew \
                 WHERE approval.plan_id=$1",
                &[&revalidate_approval_expired.plan_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_plan_approvals \
                 ENABLE TRIGGER wb_plan_approvals_append_only",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .revalidate_before_write(apply_context(
                    &revalidate_approval_expired,
                    "revalidate_approval_expired_actor",
                    now,
                ))
                .await,
            Err(PlanStoreError::ApprovalExpired)
        ));
        repository
            .finish(
                &revalidate_approval_expired.plan_id,
                "revalidate_approval_expired_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Failed,
                    error_class: Some("coverage_approval_expired"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await
            .unwrap();

        let missing_reservation_plan = create_applying_fixture_plan(
            &repository,
            &admin,
            "missing_reservation_actor",
            "coverage_account",
            55,
            quota(),
            now,
        )
        .await;
        admin
            .execute(
                "ALTER TABLE control.wb_action_reservations \
                 DISABLE TRIGGER wb_action_reservations_append_only",
                &[],
            )
            .await
            .unwrap();
        admin
            .execute(
                "DELETE FROM control.wb_action_reservations WHERE plan_id=$1",
                &[&missing_reservation_plan.plan_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_action_reservations \
                 ENABLE TRIGGER wb_action_reservations_append_only",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .revalidate_before_write(apply_context(
                    &missing_reservation_plan,
                    "missing_reservation_actor",
                    now,
                ))
                .await,
            Err(PlanStoreError::InvalidState)
        ));
        repository
            .finish(
                &missing_reservation_plan.plan_id,
                "missing_reservation_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Failed,
                    error_class: Some("coverage_missing_reservation"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await
            .unwrap();

        let stale_plan = create_applying_fixture_plan(
            &repository,
            &admin,
            "stale_apply_actor",
            "coverage_account",
            56,
            quota(),
            now,
        )
        .await;
        admin
            .execute(
                "ALTER TABLE control.wb_plans DISABLE TRIGGER wb_plans_transition_guard",
                &[],
            )
            .await
            .unwrap();
        admin
            .execute(
                "UPDATE control.wb_plans SET \
                     apply_started_at=clock_timestamp()-interval '4 minutes' \
                 WHERE plan_id=$1",
                &[&stale_plan.plan_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_plans ENABLE TRIGGER wb_plans_transition_guard",
                &[],
            )
            .await
            .unwrap();
        repository
            .mark_stale_applying_ambiguous(&stale_plan.plan_id, "stale_apply_actor", now)
            .await
            .unwrap();
        repository
            .mark_stale_applying_ambiguous(&stale_plan.plan_id, "stale_apply_actor", now)
            .await
            .unwrap();

        let invalid_reconcile_plan = create_fixture_plan(
            &repository,
            "invalid_reconcile_actor",
            "coverage_account",
            57,
            quota(),
            now,
        )
        .await;
        let (_, _, mut invalid_reconcile_after) = fixture(57);
        invalid_reconcile_after.bids[0].bid_kopecks = 1050;
        assert!(matches!(
            repository
                .confirm_reconciled(
                    &invalid_reconcile_plan.plan_id,
                    "invalid_reconcile_actor",
                    &invalid_reconcile_after,
                    now,
                )
                .await,
            Err(PlanStoreError::InvalidState)
        ));

        let incident_now = now + Duration::seconds(1);
        enable_gates(&admin, "integration_account", 43, incident_now).await;
        let incident_plan = create_fixture_plan(
            &repository,
            "integration_actor",
            "integration_account",
            43,
            quota(),
            incident_now,
        )
        .await;
        repository
            .approve(
                &incident_plan.plan_id,
                "integration_approver",
                &incident_plan.plan_digest,
                "incident/test",
                incident_now,
            )
            .await
            .unwrap();
        repository
            .claim_for_apply(apply_context(
                &incident_plan,
                "integration_actor",
                incident_now,
            ))
            .await
            .unwrap();
        assert!(matches!(
            repository
                .mark_stale_applying_ambiguous(
                    &incident_plan.plan_id,
                    "integration_actor",
                    incident_now + STALE_APPLY_AFTER,
                )
                .await,
            Err(PlanStoreError::ApplyInProgress)
        ));
        repository
            .finish(
                &incident_plan.plan_id,
                "integration_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Ambiguous,
                    error_class: Some("integration_ambiguous"),
                    write_response: None,
                    readback: None,
                    now: incident_now,
                },
            )
            .await
            .unwrap();
        let incomplete_readback = serde_json::json!({
            "bids": [{
                "nm_id": 1001,
                "placement": "search",
                "bid_kopecks": 1050
            }]
        })
        .to_string();
        assert!(
            direct_writer
                .execute(
                    "UPDATE control.wb_plans \
                     SET status='applied', finished_at=clock_timestamp(), \
                         last_error_class=NULL, readback_json=$2 \
                     WHERE plan_id=$1 AND status='ambiguous'",
                    &[&incident_plan.plan_id, &incomplete_readback],
                )
                .await
                .is_err()
        );
        let (requested, changes, mut incident_before) = fixture(43);
        incident_before.status = 9;
        assert!(matches!(
            repository
                .reserve_prepare_attempt(
                    "integration_actor",
                    "integration_account",
                    43,
                    1,
                    7,
                    POLICY_DIGEST,
                    quota(),
                    incident_now + STALE_APPLY_AFTER,
                )
                .await,
            Err(PlanStoreError::CampaignLocked)
        ));
        assert!(matches!(
            repository
                .confirm_reconciled(
                    &incident_plan.plan_id,
                    "integration_actor",
                    &incident_before,
                    incident_now + STALE_APPLY_AFTER,
                )
                .await,
            Err(PlanStoreError::InvalidPlan)
        ));
        assert_eq!(
            repository
                .load_for_actor(&incident_plan.plan_id, "integration_actor")
                .await
                .unwrap()
                .status,
            WbPlanStatus::Ambiguous
        );
        let mut incident_after = incident_before.clone();
        incident_after.bids[0].bid_kopecks = 1050;
        let mut wrong_incident_seller = serde_json::to_value(&incident_after).unwrap();
        wrong_incident_seller["seller_sid"] =
            Value::String("22222222-2222-4222-8222-222222222222".to_owned());
        let wrong_incident_seller = serde_json::to_string(&wrong_incident_seller).unwrap();
        assert!(
            direct_writer
                .execute(
                    "UPDATE control.wb_plans \
                     SET status='applied', finished_at=clock_timestamp(), \
                         last_error_class=NULL, readback_json=$2 \
                     WHERE plan_id=$1 AND status='ambiguous'",
                    &[&incident_plan.plan_id, &wrong_incident_seller],
                )
                .await
                .is_err()
        );
        repository
            .confirm_reconciled(
                &incident_plan.plan_id,
                "integration_actor",
                &incident_after,
                incident_now + STALE_APPLY_AFTER,
            )
            .await
            .unwrap();
        let reconciled_prepare = repository
            .reserve_prepare_attempt(
                "integration_actor",
                "integration_account",
                43,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                incident_now + STALE_APPLY_AFTER,
            )
            .await
            .unwrap();
        assert!(
            repository
                .create(
                    "integration_actor",
                    "integration_account",
                    43,
                    1,
                    7,
                    POLICY_DIGEST,
                    quota(),
                    &reconciled_prepare.reservation_id,
                    &requested,
                    &changes,
                    &incident_before,
                    incident_now + STALE_APPLY_AFTER,
                )
                .await
                .is_ok()
        );

        let approval_lock_pending = repository
            .reserve_prepare_attempt(
                "approval_lock_actor",
                "approval_lock_account",
                58,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                now,
            )
            .await
            .unwrap();
        let approval_lock_plan = create_fixture_plan(
            &repository,
            "approval_lock_actor",
            "approval_lock_account",
            58,
            quota(),
            now,
        )
        .await;
        let approval_incident = create_applying_fixture_plan(
            &repository,
            &admin,
            "approval_incident_actor",
            "approval_lock_account",
            58,
            quota(),
            now,
        )
        .await;
        repository
            .finish(
                &approval_incident.plan_id,
                "approval_incident_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Ambiguous,
                    error_class: Some("coverage_approval_incident"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .approve(
                    &approval_lock_plan.plan_id,
                    "integration_approver",
                    &approval_lock_plan.plan_digest,
                    "coverage/incident-lock",
                    now,
                )
                .await,
            Err(PlanStoreError::CampaignLocked)
        ));
        let (lock_requested, lock_changes, lock_before) = fixture(58);
        assert!(matches!(
            repository
                .create(
                    "approval_lock_actor",
                    "approval_lock_account",
                    58,
                    1,
                    7,
                    POLICY_DIGEST,
                    quota(),
                    &approval_lock_pending.reservation_id,
                    &lock_requested,
                    &lock_changes,
                    &lock_before,
                    now,
                )
                .await,
            Err(PlanStoreError::CampaignLocked)
        ));

        let claim_incident = create_approved_fixture_plan(
            &repository,
            "claim_incident_actor",
            "claim_lock_account",
            59,
            quota(),
            now,
        )
        .await;
        let claim_lock_plan = create_approved_fixture_plan(
            &repository,
            "claim_lock_actor",
            "claim_lock_account",
            59,
            quota(),
            now,
        )
        .await;
        enable_gates(&admin, "claim_lock_account", 59, now).await;
        repository
            .claim_for_apply(apply_context(&claim_incident, "claim_incident_actor", now))
            .await
            .unwrap();
        repository
            .finish(
                &claim_incident.plan_id,
                "claim_incident_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Ambiguous,
                    error_class: Some("coverage_claim_incident"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .claim_for_apply(apply_context(&claim_lock_plan, "claim_lock_actor", now))
                .await,
            Err(PlanStoreError::CampaignLocked)
        ));

        let revalidate_incident_plan = create_applying_fixture_plan(
            &repository,
            &admin,
            "revalidate_incident_actor",
            "revalidate_lock_account",
            60,
            quota(),
            now,
        )
        .await;
        let injected_incident_plan = create_fixture_plan(
            &repository,
            "injected_incident_actor",
            "revalidate_lock_account",
            60,
            quota(),
            now,
        )
        .await;
        admin
            .execute(
                "ALTER TABLE control.wb_plans DISABLE TRIGGER wb_plans_transition_guard",
                &[],
            )
            .await
            .unwrap();
        admin
            .execute(
                "UPDATE control.wb_plans SET \
                     status='ambiguous', apply_started_at=clock_timestamp(), \
                     finished_at=clock_timestamp(), last_error_class='coverage_injected' \
                 WHERE plan_id=$1",
                &[&injected_incident_plan.plan_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "UPDATE control.wb_plans SET \
                     apply_started_at=clock_timestamp()-interval '4 minutes' \
                 WHERE plan_id=$1",
                &[&revalidate_incident_plan.plan_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_plans ENABLE TRIGGER wb_plans_transition_guard",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .revalidate_before_write(apply_context(
                    &revalidate_incident_plan,
                    "revalidate_incident_actor",
                    now,
                ))
                .await,
            Err(PlanStoreError::CampaignLocked)
        ));
        assert!(matches!(
            repository
                .mark_stale_applying_ambiguous(
                    &revalidate_incident_plan.plan_id,
                    "revalidate_incident_actor",
                    now,
                )
                .await,
            Err(PlanStoreError::CampaignLocked)
        ));
        assert!(matches!(
            repository
                .finish(
                    &revalidate_incident_plan.plan_id,
                    "revalidate_incident_actor",
                    WbPlanFinish {
                        status: WbPlanStatus::Ambiguous,
                        error_class: Some("coverage_duplicate_incident"),
                        write_response: None,
                        readback: None,
                        now,
                    },
                )
                .await,
            Err(PlanStoreError::CampaignLocked)
        ));
        repository
            .finish(
                &revalidate_incident_plan.plan_id,
                "revalidate_incident_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Failed,
                    error_class: Some("coverage_incident_cleanup"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await
            .unwrap();

        let busy_first = create_approved_fixture_plan(
            &repository,
            "busy_first_actor",
            "busy_account",
            61,
            quota(),
            now,
        )
        .await;
        let busy_second = create_approved_fixture_plan(
            &repository,
            "busy_second_actor",
            "busy_account",
            61,
            quota(),
            now,
        )
        .await;
        enable_gates(&admin, "busy_account", 61, now).await;
        repository
            .claim_for_apply(apply_context(&busy_first, "busy_first_actor", now))
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_action_reservations \
                 DISABLE TRIGGER wb_action_reservations_append_only",
                &[],
            )
            .await
            .unwrap();
        admin
            .execute(
                "DELETE FROM control.wb_action_reservations WHERE plan_id=$1",
                &[&busy_first.plan_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_action_reservations \
                 ENABLE TRIGGER wb_action_reservations_append_only",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .claim_for_apply(apply_context(&busy_second, "busy_second_actor", now))
                .await,
            Err(PlanStoreError::Busy)
        ));
        admin
            .execute(
                "ALTER TABLE control.wb_action_reservations \
                 DISABLE TRIGGER wb_action_reservations_validate",
                &[],
            )
            .await
            .unwrap();
        let duplicate_reservation = admin.transaction().await.unwrap();
        reserve_action_quota(&duplicate_reservation, &busy_second, Utc::now())
            .await
            .unwrap();
        assert_eq!(
            reserve_action_quota(
                &duplicate_reservation,
                &busy_second,
                Utc::now() + Duration::seconds(31),
            )
            .await,
            Err(PlanStoreError::InvalidState)
        );
        duplicate_reservation.rollback().await.unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_action_reservations \
                 ENABLE TRIGGER wb_action_reservations_validate",
                &[],
            )
            .await
            .unwrap();
        repository
            .finish(
                &busy_first.plan_id,
                "busy_first_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Failed,
                    error_class: Some("coverage_busy_cleanup"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await
            .unwrap();

        let suppressed_approval = create_fixture_plan(
            &repository,
            "suppress_approval_actor",
            "fault_account",
            62,
            quota(),
            now,
        )
        .await;
        let raised_approval = create_fixture_plan(
            &repository,
            "raise_approval_actor",
            "fault_account",
            69,
            quota(),
            now,
        )
        .await;
        let raised_claim = create_approved_fixture_plan(
            &repository,
            "raise_claim_actor",
            "fault_account",
            63,
            quota(),
            now,
        )
        .await;
        enable_gates(&admin, "fault_account", 63, now).await;
        let raised_stale = create_applying_fixture_plan(
            &repository,
            &admin,
            "raise_stale_actor",
            "fault_account",
            64,
            quota(),
            now,
        )
        .await;
        let suppressed_stale = create_applying_fixture_plan(
            &repository,
            &admin,
            "suppress_stale_actor",
            "fault_account",
            65,
            quota(),
            now,
        )
        .await;
        let raised_finish = create_applying_fixture_plan(
            &repository,
            &admin,
            "raise_finish_actor",
            "fault_account",
            66,
            quota(),
            now,
        )
        .await;
        let suppressed_finish = create_applying_fixture_plan(
            &repository,
            &admin,
            "suppress_finish_actor",
            "fault_account",
            67,
            quota(),
            now,
        )
        .await;
        let suppressed_reconcile = create_applying_fixture_plan(
            &repository,
            &admin,
            "suppress_reconcile_actor",
            "fault_account",
            68,
            quota(),
            now,
        )
        .await;
        let raised_reconcile = create_applying_fixture_plan(
            &repository,
            &admin,
            "raise_reconcile_actor",
            "fault_account",
            70,
            quota(),
            now,
        )
        .await;
        repository
            .finish(
                &suppressed_reconcile.plan_id,
                "suppress_reconcile_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Ambiguous,
                    error_class: Some("coverage_reconcile_fault"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await
            .unwrap();
        repository
            .finish(
                &raised_reconcile.plan_id,
                "raise_reconcile_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Ambiguous,
                    error_class: Some("coverage_reconcile_fault"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await
            .unwrap();
        let stale_plan_ids = vec![
            raised_stale.plan_id.clone(),
            suppressed_stale.plan_id.clone(),
        ];
        admin
            .execute(
                "ALTER TABLE control.wb_plans DISABLE TRIGGER wb_plans_transition_guard",
                &[],
            )
            .await
            .unwrap();
        admin
            .execute(
                "UPDATE control.wb_plans SET \
                     apply_started_at=clock_timestamp()-interval '4 minutes' \
                 WHERE plan_id=ANY($1)",
                &[&stale_plan_ids],
            )
            .await
            .unwrap();
        admin
            .execute(
                "ALTER TABLE control.wb_plans ENABLE TRIGGER wb_plans_transition_guard",
                &[],
            )
            .await
            .unwrap();
        admin
            .batch_execute(
                "CREATE FUNCTION control.coverage_plan_update_fault() \
                 RETURNS trigger LANGUAGE plpgsql AS $$ \
                 BEGIN \
                     IF OLD.actor_id LIKE 'raise_%' THEN \
                         RAISE EXCEPTION 'coverage injected plan update failure'; \
                     ELSIF OLD.actor_id LIKE 'suppress_%' THEN \
                         RETURN NULL; \
                     END IF; \
                     RETURN NEW; \
                 END $$; \
                 CREATE TRIGGER zz_coverage_plan_update_fault \
                 BEFORE UPDATE ON control.wb_plans FOR EACH ROW \
                 EXECUTE FUNCTION control.coverage_plan_update_fault();",
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .approve(
                    &suppressed_approval.plan_id,
                    "integration_approver",
                    &suppressed_approval.plan_digest,
                    "coverage/suppressed-approval",
                    now,
                )
                .await,
            Err(PlanStoreError::InvalidState)
        ));
        assert!(matches!(
            repository
                .approve(
                    &raised_approval.plan_id,
                    "integration_approver",
                    &raised_approval.plan_digest,
                    "coverage/raised-approval",
                    now,
                )
                .await,
            Err(PlanStoreError::Unavailable)
        ));
        assert!(matches!(
            repository
                .claim_for_apply(apply_context(&raised_claim, "raise_claim_actor", now))
                .await,
            Err(PlanStoreError::Unavailable)
        ));
        assert!(matches!(
            repository
                .mark_stale_applying_ambiguous(&raised_stale.plan_id, "raise_stale_actor", now,)
                .await,
            Err(PlanStoreError::Unavailable)
        ));
        assert!(matches!(
            repository
                .mark_stale_applying_ambiguous(
                    &suppressed_stale.plan_id,
                    "suppress_stale_actor",
                    now,
                )
                .await,
            Err(PlanStoreError::InvalidState)
        ));
        assert!(matches!(
            repository
                .finish(
                    &raised_finish.plan_id,
                    "raise_finish_actor",
                    WbPlanFinish {
                        status: WbPlanStatus::Failed,
                        error_class: Some("coverage_raised_finish"),
                        write_response: None,
                        readback: None,
                        now,
                    },
                )
                .await,
            Err(PlanStoreError::Unavailable)
        ));
        assert!(matches!(
            repository
                .finish(
                    &suppressed_finish.plan_id,
                    "suppress_finish_actor",
                    WbPlanFinish {
                        status: WbPlanStatus::Failed,
                        error_class: Some("coverage_suppressed_finish"),
                        write_response: None,
                        readback: None,
                        now,
                    },
                )
                .await,
            Err(PlanStoreError::InvalidState)
        ));
        let (_, _, mut suppress_reconcile_after) = fixture(68);
        suppress_reconcile_after.bids[0].bid_kopecks = 1050;
        assert!(matches!(
            repository
                .confirm_reconciled(
                    &suppressed_reconcile.plan_id,
                    "suppress_reconcile_actor",
                    &suppress_reconcile_after,
                    now,
                )
                .await,
            Err(PlanStoreError::InvalidState)
        ));
        let (_, _, mut raise_reconcile_after) = fixture(70);
        raise_reconcile_after.bids[0].bid_kopecks = 1050;
        assert!(matches!(
            repository
                .confirm_reconciled(
                    &raised_reconcile.plan_id,
                    "raise_reconcile_actor",
                    &raise_reconcile_after,
                    now,
                )
                .await,
            Err(PlanStoreError::Unavailable)
        ));
        admin
            .batch_execute(
                "DROP TRIGGER zz_coverage_plan_update_fault ON control.wb_plans; \
                 DROP FUNCTION control.coverage_plan_update_fault();",
            )
            .await
            .unwrap();
        for (fault_plan, actor_id) in [
            (&raised_stale, "raise_stale_actor"),
            (&suppressed_stale, "suppress_stale_actor"),
            (&raised_finish, "raise_finish_actor"),
            (&suppressed_finish, "suppress_finish_actor"),
        ] {
            repository
                .finish(
                    &fault_plan.plan_id,
                    actor_id,
                    WbPlanFinish {
                        status: WbPlanStatus::Failed,
                        error_class: Some("coverage_fault_cleanup"),
                        write_response: None,
                        readback: None,
                        now,
                    },
                )
                .await
                .unwrap();
        }

        let quota_now = now + Duration::seconds(2);
        enable_gates(&admin, "integration_account", 44, quota_now).await;
        let cooldown_quota = WbActionQuota {
            cooldown_seconds: 60,
            ..quota()
        };
        let quota_plan = create_fixture_plan(
            &repository,
            "integration_actor",
            "integration_account",
            44,
            cooldown_quota,
            quota_now,
        )
        .await;
        repository
            .approve(
                &quota_plan.plan_id,
                "integration_approver",
                &quota_plan.plan_digest,
                "quota/first",
                quota_now,
            )
            .await
            .unwrap();
        repository
            .claim_for_apply(apply_context(&quota_plan, "integration_actor", quota_now))
            .await
            .unwrap();
        let (_, _, mut quota_after) = fixture(44);
        quota_after.bids[0].bid_kopecks = 1050;
        repository
            .finish(
                &quota_plan.plan_id,
                "integration_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Applied,
                    error_class: None,
                    write_response: Some(&serde_json::json!({"ok": true})),
                    readback: Some(&quota_after),
                    now: quota_now,
                },
            )
            .await
            .unwrap();
        let second_quota_plan = create_fixture_plan(
            &repository,
            "integration_actor",
            "integration_account",
            44,
            cooldown_quota,
            quota_now + Duration::seconds(1),
        )
        .await;
        repository
            .approve(
                &second_quota_plan.plan_id,
                "integration_approver",
                &second_quota_plan.plan_digest,
                "quota/second",
                quota_now + Duration::seconds(1),
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .claim_for_apply(apply_context(
                    &second_quota_plan,
                    "integration_actor",
                    quota_now + Duration::seconds(1),
                ))
                .await,
            Err(PlanStoreError::QuotaExceeded)
        ));

        let claim_wait_plan = create_fixture_plan(
            &repository,
            "clock_claim_actor",
            "clock_account",
            46,
            quota(),
            now,
        )
        .await;
        repository
            .approve(
                &claim_wait_plan.plan_id,
                "integration_approver",
                &claim_wait_plan.plan_digest,
                "clock/claim",
                now,
            )
            .await
            .unwrap();
        enable_gates(&admin, "clock_account", 46, now).await;
        admin
            .execute(
                "UPDATE control.wb_runtime_gates \
                 SET revision=revision+1, \
                     lease_expires_at=clock_timestamp()+interval '750 milliseconds', \
                     updated_at=clock_timestamp() \
                 WHERE gate_key='campaign/clock_account/46'",
                &[],
            )
            .await
            .unwrap();
        let claim_lock = admin.transaction().await.unwrap();
        claim_lock
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&"wb/clock_account/46"],
            )
            .await
            .unwrap();
        let claim_repository = repository.clone();
        let claim_plan_id = claim_wait_plan.plan_id.clone();
        let claim_plan_digest = claim_wait_plan.plan_digest.clone();
        let delayed_claim = tokio::spawn(async move {
            claim_repository
                .claim_for_apply(WbApplyContext {
                    plan_id: &claim_plan_id,
                    actor_id: "clock_claim_actor",
                    expected_plan_digest: &claim_plan_digest,
                    expected_schema_version: 1,
                    expected_policy_revision: 7,
                    expected_policy_digest: POLICY_DIGEST,
                    now: Utc::now(),
                })
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        claim_lock.commit().await.unwrap();
        assert!(matches!(
            delayed_claim.await.unwrap(),
            Err(PlanStoreError::RuntimeDisabled)
        ));

        let revalidate_wait_plan = create_fixture_plan(
            &repository,
            "clock_revalidate_actor",
            "clock_account",
            47,
            quota(),
            now,
        )
        .await;
        repository
            .approve(
                &revalidate_wait_plan.plan_id,
                "integration_approver",
                &revalidate_wait_plan.plan_digest,
                "clock/revalidate",
                now,
            )
            .await
            .unwrap();
        enable_gates(&admin, "clock_account", 47, now).await;
        repository
            .claim_for_apply(apply_context(
                &revalidate_wait_plan,
                "clock_revalidate_actor",
                now,
            ))
            .await
            .unwrap();
        admin
            .execute(
                "UPDATE control.wb_runtime_gates \
                 SET revision=revision+1, \
                     lease_expires_at=clock_timestamp()+interval '750 milliseconds', \
                     updated_at=clock_timestamp() \
                 WHERE gate_key='campaign/clock_account/47'",
                &[],
            )
            .await
            .unwrap();
        let revalidate_lock = admin.transaction().await.unwrap();
        revalidate_lock
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&"wb/clock_account/47"],
            )
            .await
            .unwrap();
        let revalidate_repository = repository.clone();
        let revalidate_plan_id = revalidate_wait_plan.plan_id.clone();
        let revalidate_plan_digest = revalidate_wait_plan.plan_digest.clone();
        let delayed_revalidate = tokio::spawn(async move {
            revalidate_repository
                .revalidate_before_write(WbApplyContext {
                    plan_id: &revalidate_plan_id,
                    actor_id: "clock_revalidate_actor",
                    expected_plan_digest: &revalidate_plan_digest,
                    expected_schema_version: 1,
                    expected_policy_revision: 7,
                    expected_policy_digest: POLICY_DIGEST,
                    now: Utc::now(),
                })
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        revalidate_lock.commit().await.unwrap();
        assert!(matches!(
            delayed_revalidate.await.unwrap(),
            Err(PlanStoreError::RuntimeDisabled)
        ));
        repository
            .finish(
                &revalidate_wait_plan.plan_id,
                "clock_revalidate_actor",
                WbPlanFinish {
                    status: WbPlanStatus::Failed,
                    error_class: Some("runtime_gate_expired"),
                    write_response: None,
                    readback: None,
                    now,
                },
            )
            .await
            .unwrap();

        drop(direct_writer);
        direct_writer_connection_task.await.unwrap().unwrap();
        let concurrent_repository_a = WbPlanRepository::connect(&config).await.unwrap();
        let concurrent_repository_b = WbPlanRepository::connect(&config).await.unwrap();
        let concurrent_repository_c = WbPlanRepository::connect(&config).await.unwrap();
        let (attempt_a, attempt_b, attempt_c, attempt_d) = tokio::join!(
            repository.reserve_prepare_attempt(
                "concurrent_prepare_actor",
                "concurrent_prepare_account",
                900,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                now,
            ),
            concurrent_repository_a.reserve_prepare_attempt(
                "concurrent_prepare_actor",
                "concurrent_prepare_account",
                900,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                now,
            ),
            concurrent_repository_b.reserve_prepare_attempt(
                "concurrent_prepare_actor",
                "concurrent_prepare_account",
                900,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                now,
            ),
            concurrent_repository_c.reserve_prepare_attempt(
                "concurrent_prepare_actor",
                "concurrent_prepare_account",
                900,
                1,
                7,
                POLICY_DIGEST,
                quota(),
                now,
            ),
        );
        let concurrent_attempts: [_; 4] = (attempt_a, attempt_b, attempt_c, attempt_d).into();
        assert_eq!(
            concurrent_attempts
                .iter()
                .filter(|attempt| attempt.is_ok())
                .count(),
            3
        );
        assert_eq!(
            concurrent_attempts
                .iter()
                .filter(|attempt| matches!(attempt, Err(PlanStoreError::PrepareLimitExceeded)))
                .count(),
            1
        );

        for advert_id in 1_000..1_060 {
            repository
                .reserve_prepare_attempt(
                    "actor_hour_limit",
                    "actor_hour_account",
                    advert_id,
                    1,
                    7,
                    POLICY_DIGEST,
                    quota(),
                    now + Duration::days(365),
                )
                .await
                .unwrap();
        }
        assert!(matches!(
            repository
                .reserve_prepare_attempt(
                    "actor_hour_limit",
                    "actor_hour_account",
                    1_060,
                    1,
                    7,
                    POLICY_DIGEST,
                    quota(),
                    now - Duration::days(365),
                )
                .await,
            Err(PlanStoreError::PrepareLimitExceeded)
        ));

        repository
            .register_policy(1, 8, NEXT_POLICY_DIGEST, now)
            .await
            .unwrap();
        assert!(matches!(
            repository.register_policy(1, 7, POLICY_DIGEST, now).await,
            Err(PlanStoreError::PolicyChanged)
        ));
        drop(admin);
        admin_connection_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn repository_enforces_approval_gates_incidents_and_quotas_when_test_database_is_available()
     {
        Box::pin(run_repository_scenarios_with_optional_test_database(
            std::env::var("WB_CONTROL_TEST_DATABASE_URL"),
            std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        ))
        .await;
        Box::pin(run_repository_scenarios_with_optional_test_database(
            Err(std::env::VarError::NotPresent),
            Err(std::env::VarError::NotPresent),
        ))
        .await;
    }
}
