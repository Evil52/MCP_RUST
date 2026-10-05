use chrono::{DateTime, Utc};
use std::collections::BTreeSet;

use super::{
    ArtifactIdentity, GENERATION_RETRY_BASE_SECONDS, GENERATION_SOURCE_SETTLE_DELAY,
    GenerationCandidate, GenerationErrorClass, MAX_GENERATION_CANDIDATES, PostgresOutboxError,
    PostgresOutboxRepository, ReportKey, artifact_object_key, exactly_one, parse_generation_status,
    parse_kind, validate_artifact,
};

impl PostgresOutboxRepository {
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

    /// Loads one settled, non-expired single-section batch for deterministic
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
        let generation_ready_before = now
            .checked_sub_signed(GENERATION_SOURCE_SETTLE_DELAY)
            .ok_or(PostgresOutboxError::InvalidDelivery)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let rows = client
            .query(
                "SELECT batch.status, batch.recipient_id, batch.report_version, \
                        COALESCE(batch.generation_started_at, batch.created_at), coverage.local_date, coverage.report_kind \
                 FROM daily_reporting.delivery_batches AS batch \
                 JOIN daily_reporting.delivery_coverage AS coverage \
                   ON coverage.batch_id = batch.id \
                 WHERE batch.id = $1 \
                   AND batch.status IN ('planned', 'generating', 'ready') \
                   AND batch.scheduled_for <= $2 \
                   AND EXISTS ( \
                       SELECT 1 FROM daily_reporting.delivery_coverage AS due \
                       WHERE due.batch_id = batch.id AND due.deadline_at >= $3 \
                   ) \
                 ORDER BY coverage.report_kind",
                &[&batch_id, &generation_ready_before, &now],
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

        let generation_ready_before = now
            .checked_sub_signed(GENERATION_SOURCE_SETTLE_DELAY)
            .ok_or(PostgresOutboxError::InvalidDelivery)?;
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
                   AND deadline_at >= $2 \
                   AND (retry_after IS NULL OR retry_after <= $2) \
                 ORDER BY scheduled_for, id \
                 LIMIT $3",
                &[&generation_ready_before, &now, &i64::from(limit)],
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

    pub(in crate::reporting) async fn verify_generation_artifact(
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
}
