use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tokio_postgres::{Client, Config, NoTls, Transaction};

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
};

const MAX_DELIVERY_ATTEMPTS: u8 = 5;

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

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum PostgresOutboxError {
    #[error("report outbox is unavailable")]
    Unavailable,
    #[error("report delivery conflicts with existing occurrence coverage")]
    Conflict,
    #[error("report delivery is invalid")]
    InvalidDelivery,
}

pub struct PostgresOutboxRepository {
    client: Mutex<Client>,
}

impl PostgresOutboxRepository {
    pub async fn connect(config: &Config) -> Result<Self, PostgresOutboxError> {
        let (client, connection) = config
            .connect(NoTls)
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        std::mem::drop(tokio::spawn(connection));
        Ok(Self::from_client(client))
    }

    pub fn from_client(client: Client) -> Self {
        Self {
            client: Mutex::new(client),
        }
    }

    pub async fn verify_runtime_contract(&self) -> Result<(), PostgresOutboxError> {
        let client = self.client.lock().await;
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

    pub async fn create_planned(
        &self,
        delivery: PendingDelivery,
    ) -> Result<CreateOutcome, PostgresOutboxError> {
        let record =
            DeliveryRecord::planned(delivery).map_err(|_| PostgresOutboxError::InvalidDelivery)?;
        let mut client = self.client.lock().await;
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
        let client = self.client.lock().await;
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

    pub(super) async fn verify_generation_artifact(
        &self,
        batch_id: i64,
        artifact: &ArtifactIdentity,
    ) -> Result<(), PostgresOutboxError> {
        validate_artifact(artifact).map_err(|_| PostgresOutboxError::InvalidDelivery)?;
        let client = self.client.lock().await;
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
        let client = self.client.lock().await;
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
        let mut client = self.client.lock().await;
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

    async fn transition(&self, batch_id: i64, sql: &str) -> Result<(), PostgresOutboxError> {
        let client = self.client.lock().await;
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
        let mut client = self.client.lock().await;
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
                    &scheduled_for(key)?,
                    &delivery_deadline(key).map_err(|_| PostgresOutboxError::InvalidDelivery)?,
                ],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
    }
    Ok(CreateOutcome::Inserted(batch_id))
}

fn scheduled_for(key: &ReportKey) -> Result<DateTime<Utc>, PostgresOutboxError> {
    delivery_deadline(key)
        .map(|deadline| deadline - chrono::Duration::hours(6))
        .map_err(|_| PostgresOutboxError::InvalidDelivery)
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

fn transient_error_text(class: DeliveryErrorClass) -> Result<&'static str, PostgresOutboxError> {
    match class {
        DeliveryErrorClass::RateLimited => Ok("rate_limited"),
        DeliveryErrorClass::ProviderUnavailable => Ok("provider_unavailable"),
        DeliveryErrorClass::Transport => Ok("transport"),
        DeliveryErrorClass::Authentication | DeliveryErrorClass::InvalidRecipient => {
            Err(PostgresOutboxError::InvalidDelivery)
        }
    }
}

fn permanent_error_text(class: DeliveryErrorClass) -> Result<&'static str, PostgresOutboxError> {
    match class {
        DeliveryErrorClass::Authentication => Ok("authentication"),
        DeliveryErrorClass::InvalidRecipient => Ok("invalid_recipient"),
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
        PostgresOutboxError, exactly_one, parse_kind, permanent_error_text, transient_error_text,
        validate_attempt_times,
    };
    use crate::{reporting::ReportKind, reporting::outbox::DeliveryErrorClass};

    #[test]
    fn database_text_mappings_and_row_counts_fail_closed() {
        assert_eq!(parse_kind("morning").unwrap(), ReportKind::Morning);
        assert_eq!(parse_kind("evening").unwrap(), ReportKind::Evening);
        assert_eq!(parse_kind("unknown"), Err(PostgresOutboxError::Unavailable));
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
        ] {
            assert_eq!(
                transient_error_text(class),
                Err(PostgresOutboxError::InvalidDelivery)
            );
        }
        assert_eq!(
            permanent_error_text(DeliveryErrorClass::Authentication).unwrap(),
            "authentication"
        );
        assert_eq!(
            permanent_error_text(DeliveryErrorClass::InvalidRecipient).unwrap(),
            "invalid_recipient"
        );
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
}
