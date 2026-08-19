use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use tokio_postgres::{Client, Config, Transaction};

use crate::postgres::SupervisedClient;

use super::{
    PendingDelivery, ReportKey, ReportKind,
    bundle::artifact_object_key,
    business_date, delivery_deadline,
    outbox::{
        ArtifactIdentity, DeliveryErrorClass, DeliveryRecord, validate_artifact,
        validate_provider_message_id,
    },
    policy::DailyReportPolicy,
    scheduler::{ScheduledDelivery, due_for_audience},
    validate_identity,
};

const MAX_DELIVERY_ATTEMPTS: u8 = 5;
const MAX_GENERATION_CANDIDATES: u16 = 16;
const MAIL_CANARY_MAX_AGE: Duration = Duration::hours(24);

/// First generation retry delay. Each further attempt doubles it, so a batch
/// that keeps failing backs off to roughly a quarter of an hour before its
/// budget runs out.
const GENERATION_RETRY_BASE_SECONDS: f64 = 60.0;

/// Why a report could not be generated.
///
/// Deliberately coarse. Only these two are knowable at the call site today;
/// recording a more precise guess would put invented detail into an
/// append-only audit trail. Finer classes belong with typed generation errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationErrorClass {
    /// Generation exceeded its time budget.
    Timeout,
    /// Generation returned an error.
    Failed,
}

impl GenerationErrorClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy)]
enum AttemptOutcome {
    Sent,
    Transient,
    Permanent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOutcome {
    Inserted(i64),
    Existing(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedDelivery {
    pub batch_id: i64,
    pub recipient_id: String,
    pub report_version: u32,
    pub attempt_no: u8,
    pub artifact: ArtifactIdentity,
    pub covered_keys: Vec<ReportKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationStatus {
    Planned,
    Generating,
    Ready,
}

/// Immutable database context required to render one report artifact.
///
/// The key, recipient and generation timestamp are loaded from PostgreSQL;
/// callers cannot inject them through a command line or model request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationCandidate {
    pub batch_id: i64,
    pub key: ReportKey,
    pub generated_at: DateTime<Utc>,
    pub status: GenerationStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailActivationReceipt {
    pub canary_sent_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationDecision {
    ConfirmedSent { provider_message_id: String },
    SuppressedUnknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationOutcome {
    Applied,
    Existing,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum PostgresOutboxError {
    #[error("report outbox is unavailable")]
    Unavailable,
    #[error("report delivery conflicts with existing occurrence coverage")]
    Conflict,
    #[error("report delivery is invalid")]
    InvalidDelivery,
    #[error("a report delivery has an ambiguous sending state")]
    AmbiguousDelivery,
    #[error("a recent successful mail canary is unavailable")]
    CanaryMissing,
}

#[derive(Clone)]
pub struct PostgresOutboxRepository {
    client: std::sync::Arc<SupervisedClient>,
}

impl PostgresOutboxRepository {
    pub async fn connect(config: &Config) -> Result<Self, PostgresOutboxError> {
        let client = SupervisedClient::connect(config, "mcp-ozon-report-worker")
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        Ok(Self {
            client: std::sync::Arc::new(client),
        })
    }

    pub fn from_client(client: Client) -> Self {
        Self {
            client: std::sync::Arc::new(SupervisedClient::preconnected(
                client,
                "mcp-ozon-report-worker",
            )),
        }
    }

    pub async fn verify_runtime_contract(&self) -> Result<(), PostgresOutboxError> {
        // Checked before the guard is taken: the session mutex is not
        // reentrant, and this helper acquires it in its own right.
        self.client
            .verify_session_bounds()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let row = client
            .query_one(
                "SELECT current_user = 'report_worker' \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.delivery_batches', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.delivery_batches', 'INSERT') \
                    AND has_column_privilege(current_user, \
                        'daily_reporting.delivery_batches', 'status', 'UPDATE') \
                    AND has_column_privilege(current_user, \
                        'daily_reporting.delivery_batches', \
                        'artifact_html_sha256', 'UPDATE') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.delivery_coverage', 'SELECT,INSERT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.delivery_attempts', 'SELECT,INSERT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.delivery_reconciliations', 'SELECT,INSERT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.generation_attempts', 'SELECT,INSERT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.generatable_batches', 'SELECT') \
                    AND NOT has_schema_privilege(current_user, \
                        'search_position', 'USAGE')",
                &[],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        if row.get::<_, bool>(0) {
            Ok(())
        } else {
            Err(PostgresOutboxError::Unavailable)
        }
    }

    /// Requires recent provider-backed proof before scheduled mail activation.
    ///
    /// The canary must use the same immutable audience and report-policy
    /// version as the scheduler. A `sending` row is deliberately blocking: its
    /// provider outcome is unknown and must be reconciled by an operator before
    /// any further automatic delivery starts.
    pub async fn verify_mail_activation(
        &self,
        recipient_id: &str,
        report_version: u32,
        now: DateTime<Utc>,
    ) -> Result<MailActivationReceipt, PostgresOutboxError> {
        validate_identity(recipient_id, report_version)
            .map_err(|_| PostgresOutboxError::InvalidDelivery)?;
        let version =
            i32::try_from(report_version).map_err(|_| PostgresOutboxError::InvalidDelivery)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let row = client
            .query_one(
                "SELECT count(*) FILTER (WHERE status = 'sending'), \
                        max(sent_at) FILTER (WHERE status = 'sent') \
                 FROM daily_reporting.delivery_batches \
                 WHERE recipient_id = $1 AND report_version = $2",
                &[&recipient_id, &version],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        validate_mail_activation_state(row.get(0), row.get(1), now)
    }

    /// Closes one ambiguous `sending` attempt after an operator checks Gmail.
    ///
    /// `ConfirmedSent` requires the provider message ID. `SuppressedUnknown`
    /// permanently blocks resend when the provider outcome cannot be proven.
    /// The append-only reconciliation row and terminal batch transition commit
    /// together. Repeating the exact decision is idempotent; changing it is a
    /// conflict.
    pub async fn reconcile_sending(
        &self,
        batch_id: i64,
        attempt_no: u8,
        recipient_id: &str,
        report_version: u32,
        reconciled_at: DateTime<Utc>,
        decision: &ReconciliationDecision,
    ) -> Result<ReconciliationOutcome, PostgresOutboxError> {
        validate_identity(recipient_id, report_version)
            .map_err(|_| PostgresOutboxError::InvalidDelivery)?;
        if batch_id <= 0 || !(1..=MAX_DELIVERY_ATTEMPTS).contains(&attempt_no) {
            return Err(PostgresOutboxError::InvalidDelivery);
        }
        let report_version =
            i32::try_from(report_version).map_err(|_| PostgresOutboxError::InvalidDelivery)?;
        let (decision_text, provider_message_id, terminal_status, error_class, sent_at) =
            match decision {
                ReconciliationDecision::ConfirmedSent {
                    provider_message_id,
                } => {
                    validate_provider_message_id(provider_message_id)
                        .map_err(|_| PostgresOutboxError::InvalidDelivery)?;
                    (
                        "confirmed_sent",
                        Some(provider_message_id.as_str()),
                        "sent",
                        None,
                        Some(reconciled_at),
                    )
                }
                ReconciliationDecision::SuppressedUnknown => (
                    "suppressed_unknown",
                    None,
                    "permanent_failure",
                    Some("operator_reconciled_unknown"),
                    None,
                ),
            };
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let batch = transaction
            .query_opt(
                "SELECT status, attempts, provider_message_id, last_error_class \
                 FROM daily_reporting.delivery_batches \
                 WHERE id = $1 AND recipient_id = $2 AND report_version = $3 FOR UPDATE",
                &[&batch_id, &recipient_id, &report_version],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?
            .ok_or(PostgresOutboxError::Conflict)?;
        let existing = transaction
            .query_opt(
                "SELECT decision, provider_message_id \
                 FROM daily_reporting.delivery_reconciliations \
                 WHERE batch_id = $1 AND attempt_no = $2",
                &[&batch_id, &i16::from(attempt_no)],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        if let Some(existing) = existing {
            let exact_reconciliation = existing.get::<_, &str>(0) == decision_text
                && existing.get::<_, Option<&str>>(1) == provider_message_id;
            let exact_terminal = batch.get::<_, &str>(0) == terminal_status
                && batch.get::<_, Option<&str>>(2) == provider_message_id
                && batch.get::<_, Option<&str>>(3) == error_class;
            if exact_reconciliation && exact_terminal {
                transaction
                    .commit()
                    .await
                    .map_err(|_| PostgresOutboxError::Unavailable)?;
                return Ok(ReconciliationOutcome::Existing);
            }
            return Err(PostgresOutboxError::Conflict);
        }
        if batch.get::<_, &str>(0) != "sending" || batch.get::<_, i16>(1) != i16::from(attempt_no) {
            return Err(PostgresOutboxError::Conflict);
        }
        transaction
            .execute(
                "INSERT INTO daily_reporting.delivery_reconciliations ( \
                    batch_id, attempt_no, reconciled_at, decision, provider_message_id \
                 ) VALUES ($1, $2, $3, $4, $5)",
                &[
                    &batch_id,
                    &i16::from(attempt_no),
                    &reconciled_at,
                    &decision_text,
                    &provider_message_id,
                ],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let changed = transaction
            .execute(
                "UPDATE daily_reporting.delivery_batches \
                 SET status = $2, next_attempt_at = NULL, provider_message_id = $3, \
                     last_error_class = $4, sent_at = $5, \
                     updated_at = greatest(clock_timestamp(), updated_at + interval '1 microsecond') \
                 WHERE id = $1 AND status = 'sending' AND attempts = $6",
                &[
                    &batch_id,
                    &terminal_status,
                    &provider_message_id,
                    &error_class,
                    &sent_at,
                    &i16::from(attempt_no),
                ],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        exactly_one(changed)?;
        transaction
            .commit()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        Ok(ReconciliationOutcome::Applied)
    }

    pub async fn create_planned(
        &self,
        delivery: PendingDelivery,
    ) -> Result<CreateOutcome, PostgresOutboxError> {
        let record =
            DeliveryRecord::planned(delivery).map_err(|_| PostgresOutboxError::InvalidDelivery)?;
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let outcome = create_planned_inner(&transaction, &record).await?;
        transaction
            .commit()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        Ok(outcome)
    }

    /// Returns every reserved occurrence for one recipient/business date.
    ///
    /// All batch states count as covered: a plan is reserved before artifact
    /// generation starts, so an interrupted worker cannot create a duplicate
    /// identity on recovery.
    pub async fn covered_keys(
        &self,
        now: DateTime<Utc>,
        recipient_id: &str,
        report_version: u32,
    ) -> Result<BTreeSet<ReportKey>, PostgresOutboxError> {
        let version =
            i32::try_from(report_version).map_err(|_| PostgresOutboxError::InvalidDelivery)?;
        let date = business_date(now);
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let rows = client
            .query(
                "SELECT report_kind FROM daily_reporting.delivery_coverage \
                 WHERE recipient_id = $1 AND report_version = $2 AND local_date = $3",
                &[&recipient_id, &version, &date],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        rows.into_iter()
            .map(|row| {
                Ok(ReportKey {
                    local_date: date,
                    kind: parse_kind(row.get::<_, &str>(0))?,
                    recipient_id: recipient_id.to_owned(),
                    report_version,
                })
            })
            .collect()
    }

    /// Atomically reserves all currently due report identities in the outbox.
    ///
    /// This only creates `planned` rows. Rendering and delivery remain separate
    /// stages, so calling this function cannot send a message or access a
    /// marketplace. If another worker reserves a competing occurrence between
    /// the read and insert, the caller receives a conflict and can retry its
    /// next normal scheduling tick without producing a duplicate.
    pub async fn plan_due(
        &self,
        now: DateTime<Utc>,
        policy: &DailyReportPolicy,
    ) -> Result<Vec<(ScheduledDelivery, CreateOutcome)>, PostgresOutboxError> {
        let mut outcomes = Vec::new();
        for audience in &policy.audiences {
            let covered = self.covered_keys(now, &audience.id, policy.version).await?;
            for delivery in due_for_audience(now, &audience.id, policy.version, &covered)
                .map_err(|_| PostgresOutboxError::InvalidDelivery)?
            {
                let outcome = self.create_planned(delivery.delivery.clone()).await?;
                outcomes.push((delivery, outcome));
            }
        }
        Ok(outcomes)
    }

    pub async fn start_generation(&self, batch_id: i64) -> Result<(), PostgresOutboxError> {
        self.transition(
            batch_id,
            "UPDATE daily_reporting.delivery_batches \
             SET status = 'generating', \
                 updated_at = greatest(clock_timestamp(), updated_at + interval '1 microsecond') \
             WHERE id = $1 AND status = 'planned'",
        )
        .await
    }

    /// Loads one due, non-expired single-section batch for deterministic
    /// rendering or recovery. `ready` batches are accepted so an operator can
    /// verify an ambiguous post-persistence outcome without creating another
    /// delivery identity.
    pub async fn generation_candidate(
        &self,
        batch_id: i64,
        now: DateTime<Utc>,
    ) -> Result<GenerationCandidate, PostgresOutboxError> {
        if batch_id <= 0 {
            return Err(PostgresOutboxError::InvalidDelivery);
        }
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let rows = client
            .query(
                "SELECT batch.status, batch.recipient_id, batch.report_version, \
                        batch.created_at, coverage.local_date, coverage.report_kind \
                 FROM daily_reporting.delivery_batches AS batch \
                 JOIN daily_reporting.delivery_coverage AS coverage \
                   ON coverage.batch_id = batch.id \
                 WHERE batch.id = $1 \
                   AND batch.status IN ('planned', 'generating', 'ready') \
                   AND batch.scheduled_for <= $2 \
                   AND EXISTS ( \
                       SELECT 1 FROM daily_reporting.delivery_coverage AS due \
                       WHERE due.batch_id = batch.id AND due.deadline_at >= $2 \
                   ) \
                 ORDER BY coverage.report_kind",
                &[&batch_id, &now],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let [row] = rows.as_slice() else {
            return Err(PostgresOutboxError::Conflict);
        };
        let status = parse_generation_status(row.get(0))?;
        let report_version =
            u32::try_from(row.get::<_, i32>(2)).map_err(|_| PostgresOutboxError::Unavailable)?;
        Ok(GenerationCandidate {
            batch_id,
            key: ReportKey {
                local_date: row.get(4),
                kind: parse_kind(row.get(5))?,
                recipient_id: row.get(1),
                report_version,
            },
            generated_at: row.get(3),
            status,
        })
    }

    /// Returns a bounded set of due single-section batches that still need an
    /// artifact. This is a recovery scan, not a delivery claim.
    pub async fn pending_generation_ids(
        &self,
        now: DateTime<Utc>,
        limit: u16,
    ) -> Result<Vec<i64>, PostgresOutboxError> {
        if limit == 0 || limit > MAX_GENERATION_CANDIDATES {
            return Err(PostgresOutboxError::InvalidDelivery);
        }
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let rows = client
            .query(
                // `generatable_batches` already excludes work whose backoff has
                // not elapsed and work that exhausted its attempt budget, so a
                // batch that cannot be rendered stops occupying a candidate
                // slot instead of starving every healthy batch behind it.
                "SELECT id \
                 FROM daily_reporting.generatable_batches \
                 WHERE scheduled_for <= $1 \
                   AND deadline_at >= $1 \
                   AND (retry_after IS NULL OR retry_after <= $1) \
                 ORDER BY scheduled_for, id \
                 LIMIT $2",
                &[&now, &i64::from(limit)],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        Ok(rows.into_iter().map(|row| row.get(0)).collect())
    }

    /// Records a failed generation and holds the batch back from the next
    /// candidate scans.
    ///
    /// The delay grows with the attempt number so a batch that fails for a
    /// structural reason — a missing snapshot, an unrenderable dataset — stops
    /// consuming a slot every tick, and stops entirely once its budget is
    /// spent. The row is append-only: the attempt history stays auditable, and
    /// the budget cannot be rewound by a caller.
    pub async fn record_generation_failure(
        &self,
        batch_id: i64,
        now: DateTime<Utc>,
        error_class: GenerationErrorClass,
    ) -> Result<(), PostgresOutboxError> {
        let error_class = error_class.as_str();
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let changed = client
            .execute(
                // Every parameter is cast explicitly. In an `INSERT ... SELECT`
                // the target column types do not reach the inner select list,
                // so an uncast parameter is inferred as `text` and the insert
                // fails on a type it was never given.
                "INSERT INTO daily_reporting.generation_attempts \
                     (batch_id, attempt_no, failed_at, retry_after, error_class) \
                 SELECT $1::bigint, \
                        (count(*) + 1)::smallint, \
                        $2::timestamptz, \
                        $2::timestamptz + make_interval(secs => \
                            $3::double precision * \
                            power(2::double precision, count(*)::double precision)), \
                        $4::text \
                 FROM daily_reporting.generation_attempts \
                 WHERE batch_id = $1::bigint",
                &[
                    &batch_id,
                    &now,
                    &GENERATION_RETRY_BASE_SECONDS,
                    &error_class,
                ],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        exactly_one(changed)
    }

    pub(super) async fn verify_generation_artifact(
        &self,
        batch_id: i64,
        artifact: &ArtifactIdentity,
    ) -> Result<(), PostgresOutboxError> {
        validate_artifact(artifact).map_err(|_| PostgresOutboxError::InvalidDelivery)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let rows = client
            .query(
                "SELECT batch.status, batch.recipient_id, batch.report_version, \
                        coverage.local_date, coverage.report_kind \
                 FROM daily_reporting.delivery_batches AS batch \
                 JOIN daily_reporting.delivery_coverage AS coverage \
                   ON coverage.batch_id = batch.id \
                 WHERE batch.id = $1 ORDER BY coverage.report_kind",
                &[&batch_id],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let first = rows.first().ok_or(PostgresOutboxError::Conflict)?;
        let status: &str = first.get(0);
        if !matches!(status, "generating" | "ready")
            || rows.iter().any(|row| {
                row.get::<_, &str>(0) != status
                    || row.get::<_, &str>(1) != first.get::<_, &str>(1)
                    || row.get::<_, i32>(2) != first.get::<_, i32>(2)
                    || row.get::<_, chrono::NaiveDate>(3) != first.get::<_, chrono::NaiveDate>(3)
            })
        {
            return Err(PostgresOutboxError::Conflict);
        }
        let kind = rows
            .iter()
            .map(|row| parse_kind(row.get(4)))
            .collect::<Result<BTreeSet<_>, _>>()?
            .into_iter()
            .max()
            .ok_or(PostgresOutboxError::Unavailable)?;
        let report_version =
            u32::try_from(first.get::<_, i32>(2)).map_err(|_| PostgresOutboxError::Unavailable)?;
        let key = ReportKey {
            local_date: first.get(3),
            kind,
            recipient_id: first.get(1),
            report_version,
        };
        if artifact.object_key != artifact_object_key(&key) {
            return Err(PostgresOutboxError::Conflict);
        }
        Ok(())
    }

    pub async fn mark_ready(
        &self,
        batch_id: i64,
        artifact: &ArtifactIdentity,
    ) -> Result<(), PostgresOutboxError> {
        validate_artifact(artifact).map_err(|_| PostgresOutboxError::InvalidDelivery)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let changed = client
            .execute(
                "UPDATE daily_reporting.delivery_batches \
                 SET status = 'ready', artifact_object_key = $2, artifact_sha256 = $3, \
                     artifact_html_sha256 = $4, \
                     next_attempt_at = NULL, \
                     updated_at = greatest(clock_timestamp(), updated_at + interval '1 microsecond') \
                 WHERE id = $1 AND status = 'generating'",
                &[
                    &batch_id,
                    &artifact.object_key,
                    &artifact.sha256,
                    &artifact.html_sha256,
                ],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        if changed == 1 {
            return Ok(());
        }
        let existing = client
            .query_opt(
                "SELECT status = 'ready' AND artifact_object_key = $2 \
                        AND artifact_sha256 = $3 AND artifact_html_sha256 = $4 \
                 FROM daily_reporting.delivery_batches WHERE id = $1",
                &[
                    &batch_id,
                    &artifact.object_key,
                    &artifact.sha256,
                    &artifact.html_sha256,
                ],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        if existing.is_some_and(|row| row.get::<_, bool>(0)) {
            Ok(())
        } else {
            Err(PostgresOutboxError::Conflict)
        }
    }

    /// Claims one ready delivery. A process crash after this point deliberately
    /// leaves the row in `sending`; it is never auto-retried after an ambiguous
    /// provider outcome.
    pub async fn claim_ready(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<ClaimedDelivery>, PostgresOutboxError> {
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let row = transaction
            .query_opt(
                "SELECT batch.id, batch.recipient_id, batch.report_version, \
                        batch.attempts, batch.artifact_object_key, batch.artifact_sha256, \
                        batch.artifact_html_sha256 \
                 FROM daily_reporting.delivery_batches AS batch \
                 WHERE batch.status = 'ready' \
                   AND batch.attempts < 5 \
                   AND (batch.next_attempt_at IS NULL OR batch.next_attempt_at <= $1) \
                   AND EXISTS ( \
                       SELECT 1 FROM daily_reporting.delivery_coverage AS coverage \
                       WHERE coverage.batch_id = batch.id AND coverage.deadline_at >= $1 \
                   ) \
                 ORDER BY batch.scheduled_for, batch.id \
                 FOR UPDATE SKIP LOCKED LIMIT 1",
                &[&now],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let Some(row) = row else {
            transaction
                .commit()
                .await
                .map_err(|_| PostgresOutboxError::Unavailable)?;
            return Ok(None);
        };
        let batch_id: i64 = row.get(0);
        let recipient_id: String = row.get(1);
        let report_version =
            u32::try_from(row.get::<_, i32>(2)).map_err(|_| PostgresOutboxError::Unavailable)?;
        let attempt_no =
            u8::try_from(row.get::<_, i16>(3) + 1).map_err(|_| PostgresOutboxError::Unavailable)?;
        let object_key: String = row.get(4);
        let sha256: String = row.get(5);
        let html_sha256: String = row.get(6);
        let coverage = transaction
            .query(
                "SELECT local_date, report_kind \
                 FROM daily_reporting.delivery_coverage \
                 WHERE batch_id = $1 ORDER BY report_kind",
                &[&batch_id],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let covered_keys = coverage
            .into_iter()
            .map(|row| {
                Ok(ReportKey {
                    local_date: row.get(0),
                    kind: parse_kind(row.get::<_, &str>(1))?,
                    recipient_id: recipient_id.clone(),
                    report_version,
                })
            })
            .collect::<Result<Vec<_>, PostgresOutboxError>>()?;
        let changed = transaction
            .execute(
                "UPDATE daily_reporting.delivery_batches \
                 SET status = 'sending', attempts = attempts + 1, \
                     next_attempt_at = NULL, \
                     updated_at = greatest(clock_timestamp(), updated_at + interval '1 microsecond') \
                 WHERE id = $1 AND status = 'ready'",
                &[&batch_id],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        exactly_one(changed)?;
        transaction
            .commit()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        Ok(Some(ClaimedDelivery {
            batch_id,
            recipient_id,
            report_version,
            attempt_no,
            artifact: ArtifactIdentity {
                object_key,
                sha256,
                html_sha256,
            },
            covered_keys,
        }))
    }

    pub async fn record_sent(
        &self,
        claim: &ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        provider_message_id: &str,
    ) -> Result<(), PostgresOutboxError> {
        validate_attempt_times(started_at, finished_at)?;
        validate_provider_message_id(provider_message_id)
            .map_err(|_| PostgresOutboxError::InvalidDelivery)?;
        self.finish_attempt(
            claim,
            started_at,
            finished_at,
            AttemptOutcome::Sent,
            None,
            Some(provider_message_id),
            None,
        )
        .await
    }

    pub async fn record_transient_failure(
        &self,
        claim: &ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        class: DeliveryErrorClass,
        retry_at: DateTime<Utc>,
    ) -> Result<(), PostgresOutboxError> {
        let class = transient_error_text(class)?;
        validate_attempt_times(started_at, finished_at)?;
        if claim.attempt_no >= MAX_DELIVERY_ATTEMPTS {
            return self
                .finish_attempt(
                    claim,
                    started_at,
                    finished_at,
                    AttemptOutcome::Permanent,
                    Some(class),
                    None,
                    None,
                )
                .await;
        }
        let deadline = claim
            .covered_keys
            .iter()
            .map(delivery_deadline)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| PostgresOutboxError::InvalidDelivery)?
            .into_iter()
            .max()
            .ok_or(PostgresOutboxError::InvalidDelivery)?;
        if retry_at <= finished_at || retry_at > deadline {
            return Err(PostgresOutboxError::InvalidDelivery);
        }
        self.finish_attempt(
            claim,
            started_at,
            finished_at,
            AttemptOutcome::Transient,
            Some(class),
            None,
            Some(retry_at),
        )
        .await
    }

    pub async fn record_permanent_failure(
        &self,
        claim: &ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        class: DeliveryErrorClass,
    ) -> Result<(), PostgresOutboxError> {
        validate_attempt_times(started_at, finished_at)?;
        let class = permanent_error_text(class)?;
        self.finish_attempt(
            claim,
            started_at,
            finished_at,
            AttemptOutcome::Permanent,
            Some(class),
            None,
            None,
        )
        .await
    }

    /// Finishes a known pre-send transient failure when no bounded retry can
    /// fit inside the delivery window. This is distinct from an ambiguous send:
    /// the provider was not asked to accept a message, so the exact safe error
    /// class can be committed as an exhausted permanent attempt.
    pub async fn record_exhausted_failure(
        &self,
        claim: &ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        class: DeliveryErrorClass,
    ) -> Result<(), PostgresOutboxError> {
        validate_attempt_times(started_at, finished_at)?;
        let class = transient_error_text(class)?;
        self.finish_attempt(
            claim,
            started_at,
            finished_at,
            AttemptOutcome::Permanent,
            Some(class),
            None,
            None,
        )
        .await
    }

    async fn transition(&self, batch_id: i64, sql: &str) -> Result<(), PostgresOutboxError> {
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let changed = client
            .execute(sql, &[&batch_id])
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        exactly_one(changed)
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_attempt(
        &self,
        claim: &ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        outcome: AttemptOutcome,
        error_class: Option<&str>,
        provider_message_id: Option<&str>,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<(), PostgresOutboxError> {
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        transaction
            .execute(
                "INSERT INTO daily_reporting.delivery_attempts ( \
                    batch_id, attempt_no, started_at, finished_at, outcome, \
                    error_class, provider_message_id \
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    &claim.batch_id,
                    &i16::from(claim.attempt_no),
                    &started_at,
                    &finished_at,
                    &attempt_outcome_text(outcome),
                    &error_class,
                    &provider_message_id,
                ],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let (status, sent_at) = match outcome {
            AttemptOutcome::Sent => ("sent", Some(finished_at)),
            AttemptOutcome::Transient => ("ready", None),
            AttemptOutcome::Permanent => ("permanent_failure", None),
        };
        let changed = transaction
            .execute(
                "UPDATE daily_reporting.delivery_batches \
                 SET status = $2, next_attempt_at = $3, provider_message_id = $4, \
                     last_error_class = $5, sent_at = $6, \
                     updated_at = greatest(clock_timestamp(), updated_at + interval '1 microsecond') \
                 WHERE id = $1 AND status = 'sending' AND attempts = $7",
                &[
                    &claim.batch_id,
                    &status,
                    &retry_at,
                    &provider_message_id,
                    &error_class,
                    &sent_at,
                    &i16::from(claim.attempt_no),
                ],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        exactly_one(changed)?;
        transaction
            .commit()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)
    }
}

fn validate_mail_activation_state(
    ambiguous_count: i64,
    sent_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<MailActivationReceipt, PostgresOutboxError> {
    if ambiguous_count != 0 {
        return Err(PostgresOutboxError::AmbiguousDelivery);
    }
    let sent_at = sent_at.ok_or(PostgresOutboxError::CanaryMissing)?;
    let oldest_allowed = now
        .checked_sub_signed(MAIL_CANARY_MAX_AGE)
        .ok_or(PostgresOutboxError::InvalidDelivery)?;
    if sent_at < oldest_allowed || sent_at > now {
        return Err(PostgresOutboxError::CanaryMissing);
    }
    Ok(MailActivationReceipt {
        canary_sent_at: sent_at,
    })
}

async fn create_planned_inner(
    transaction: &Transaction<'_>,
    record: &DeliveryRecord,
) -> Result<CreateOutcome, PostgresOutboxError> {
    let first = record
        .covered_keys()
        .first()
        .ok_or(PostgresOutboxError::InvalidDelivery)?;
    let lock_key = format!(
        "{}:{}:{}",
        first.recipient_id, first.report_version, first.local_date
    );
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&lock_key],
        )
        .await
        .map_err(|_| PostgresOutboxError::Unavailable)?;
    let existing = transaction
        .query(
            "SELECT coverage.batch_id, coverage.report_kind \
             FROM daily_reporting.delivery_coverage AS coverage \
             WHERE coverage.recipient_id = $1 AND coverage.report_version = $2 \
               AND coverage.local_date = $3 ORDER BY coverage.report_kind",
            &[
                &first.recipient_id,
                &i32::try_from(first.report_version)
                    .map_err(|_| PostgresOutboxError::InvalidDelivery)?,
                &first.local_date,
            ],
        )
        .await
        .map_err(|_| PostgresOutboxError::Unavailable)?;
    if !existing.is_empty() {
        let batch_id: i64 = existing[0].get(0);
        let existing_kinds = existing
            .iter()
            .map(|row| parse_kind(row.get::<_, &str>(1)))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let requested_kinds = record
            .covered_keys()
            .iter()
            .map(|key| key.kind)
            .collect::<BTreeSet<_>>();
        if existing.iter().all(|row| row.get::<_, i64>(0) == batch_id)
            && existing_kinds == requested_kinds
        {
            return Ok(CreateOutcome::Existing(batch_id));
        }
        if !existing_kinds.is_disjoint(&requested_kinds) {
            return Err(PostgresOutboxError::Conflict);
        }
    }

    let report_version =
        i32::try_from(first.report_version).map_err(|_| PostgresOutboxError::InvalidDelivery)?;
    let deadline_at = record
        .deadline_at()
        .map_err(|_| PostgresOutboxError::InvalidDelivery)?;
    let row = transaction
        .query_one(
            "INSERT INTO daily_reporting.delivery_batches ( \
                recipient_id, report_version, scheduled_for, delayed \
             ) VALUES ($1, $2, $3, $4) RETURNING id",
            &[
                &first.recipient_id,
                &report_version,
                &record.scheduled_for(),
                &record.delayed(),
            ],
        )
        .await
        .map_err(|_| PostgresOutboxError::Unavailable)?;
    let batch_id: i64 = row.get(0);
    for key in record.covered_keys() {
        transaction
            .execute(
                "INSERT INTO daily_reporting.delivery_coverage ( \
                    batch_id, recipient_id, report_version, local_date, report_kind, \
                    scheduled_for, deadline_at \
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    &batch_id,
                    &key.recipient_id,
                    &report_version,
                    &key.local_date,
                    &kind_text(key.kind),
                    &record.scheduled_for(),
                    &deadline_at,
                ],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
    }
    Ok(CreateOutcome::Inserted(batch_id))
}

fn kind_text(kind: ReportKind) -> &'static str {
    match kind {
        ReportKind::Morning => "morning",
        ReportKind::Evening => "evening",
    }
}

fn attempt_outcome_text(outcome: AttemptOutcome) -> &'static str {
    match outcome {
        AttemptOutcome::Sent => "sent",
        AttemptOutcome::Transient => "transient",
        AttemptOutcome::Permanent => "permanent",
    }
}

fn parse_kind(value: &str) -> Result<ReportKind, PostgresOutboxError> {
    match value {
        "morning" => Ok(ReportKind::Morning),
        "evening" => Ok(ReportKind::Evening),
        _ => Err(PostgresOutboxError::Unavailable),
    }
}

fn parse_generation_status(value: &str) -> Result<GenerationStatus, PostgresOutboxError> {
    match value {
        "planned" => Ok(GenerationStatus::Planned),
        "generating" => Ok(GenerationStatus::Generating),
        "ready" => Ok(GenerationStatus::Ready),
        _ => Err(PostgresOutboxError::Unavailable),
    }
}

fn transient_error_text(class: DeliveryErrorClass) -> Result<&'static str, PostgresOutboxError> {
    match class {
        DeliveryErrorClass::RateLimited => Ok("rate_limited"),
        DeliveryErrorClass::ProviderUnavailable => Ok("provider_unavailable"),
        DeliveryErrorClass::Transport => Ok("transport"),
        DeliveryErrorClass::Authentication
        | DeliveryErrorClass::InvalidRecipient
        | DeliveryErrorClass::InvalidArtifact
        | DeliveryErrorClass::InvalidRouting
        | DeliveryErrorClass::ProviderRejected => Err(PostgresOutboxError::InvalidDelivery),
    }
}

fn permanent_error_text(class: DeliveryErrorClass) -> Result<&'static str, PostgresOutboxError> {
    match class {
        DeliveryErrorClass::Authentication => Ok("authentication"),
        DeliveryErrorClass::InvalidRecipient => Ok("invalid_recipient"),
        DeliveryErrorClass::InvalidArtifact => Ok("invalid_artifact"),
        DeliveryErrorClass::InvalidRouting => Ok("invalid_routing"),
        DeliveryErrorClass::ProviderRejected => Ok("provider_rejected"),
        DeliveryErrorClass::RateLimited
        | DeliveryErrorClass::ProviderUnavailable
        | DeliveryErrorClass::Transport => Err(PostgresOutboxError::InvalidDelivery),
    }
}

fn validate_attempt_times(
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
) -> Result<(), PostgresOutboxError> {
    if finished_at < started_at {
        Err(PostgresOutboxError::InvalidDelivery)
    } else {
        Ok(())
    }
}

fn exactly_one(changed: u64) -> Result<(), PostgresOutboxError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(PostgresOutboxError::Conflict)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{
        GenerationStatus, PostgresOutboxError, exactly_one, parse_generation_status, parse_kind,
        permanent_error_text, transient_error_text, validate_attempt_times,
        validate_mail_activation_state,
    };
    use crate::{reporting::ReportKind, reporting::outbox::DeliveryErrorClass};

    #[test]
    fn database_text_mappings_and_row_counts_fail_closed() {
        assert_eq!(parse_kind("morning").unwrap(), ReportKind::Morning);
        assert_eq!(parse_kind("evening").unwrap(), ReportKind::Evening);
        assert_eq!(parse_kind("unknown"), Err(PostgresOutboxError::Unavailable));
        for (value, expected) in [
            ("planned", GenerationStatus::Planned),
            ("generating", GenerationStatus::Generating),
            ("ready", GenerationStatus::Ready),
        ] {
            assert_eq!(parse_generation_status(value), Ok(expected));
        }
        assert_eq!(
            parse_generation_status("sent"),
            Err(PostgresOutboxError::Unavailable)
        );
        assert_eq!(exactly_one(1), Ok(()));
        for changed in [0, 2] {
            assert_eq!(exactly_one(changed), Err(PostgresOutboxError::Conflict));
        }
        for (class, expected) in [
            (DeliveryErrorClass::RateLimited, "rate_limited"),
            (
                DeliveryErrorClass::ProviderUnavailable,
                "provider_unavailable",
            ),
            (DeliveryErrorClass::Transport, "transport"),
        ] {
            assert_eq!(transient_error_text(class).unwrap(), expected);
        }
        for class in [
            DeliveryErrorClass::Authentication,
            DeliveryErrorClass::InvalidRecipient,
            DeliveryErrorClass::InvalidArtifact,
            DeliveryErrorClass::InvalidRouting,
            DeliveryErrorClass::ProviderRejected,
        ] {
            assert_eq!(
                transient_error_text(class),
                Err(PostgresOutboxError::InvalidDelivery)
            );
        }
        for (class, expected) in [
            (DeliveryErrorClass::Authentication, "authentication"),
            (DeliveryErrorClass::InvalidRecipient, "invalid_recipient"),
            (DeliveryErrorClass::InvalidArtifact, "invalid_artifact"),
            (DeliveryErrorClass::InvalidRouting, "invalid_routing"),
            (DeliveryErrorClass::ProviderRejected, "provider_rejected"),
        ] {
            assert_eq!(permanent_error_text(class).unwrap(), expected);
        }
        for class in [
            DeliveryErrorClass::RateLimited,
            DeliveryErrorClass::ProviderUnavailable,
            DeliveryErrorClass::Transport,
        ] {
            assert_eq!(
                permanent_error_text(class),
                Err(PostgresOutboxError::InvalidDelivery)
            );
        }
        let start = Utc.with_ymd_and_hms(2026, 8, 16, 3, 0, 0).unwrap();
        assert_eq!(validate_attempt_times(start, start), Ok(()));
        assert_eq!(
            validate_attempt_times(start, start - chrono::Duration::seconds(1)),
            Err(PostgresOutboxError::InvalidDelivery)
        );
    }

    #[test]
    fn mail_activation_requires_recent_unambiguous_canary_proof() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-19T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            validate_mail_activation_state(1, Some(now), now),
            Err(PostgresOutboxError::AmbiguousDelivery)
        );
        assert_eq!(
            validate_mail_activation_state(0, None, now),
            Err(PostgresOutboxError::CanaryMissing)
        );
        assert_eq!(
            validate_mail_activation_state(0, Some(now - chrono::Duration::hours(25)), now),
            Err(PostgresOutboxError::CanaryMissing)
        );
        assert_eq!(
            validate_mail_activation_state(0, Some(now + chrono::Duration::seconds(1)), now),
            Err(PostgresOutboxError::CanaryMissing)
        );
        assert_eq!(
            validate_mail_activation_state(0, Some(now), chrono::DateTime::<Utc>::MIN_UTC),
            Err(PostgresOutboxError::InvalidDelivery)
        );
        assert_eq!(
            validate_mail_activation_state(0, Some(now), now)
                .unwrap()
                .canary_sent_at,
            now
        );
    }
}
