BEGIN;

-- Candidate scans touch only planned/generating rows and return them in
-- scheduled order. Keep terminal history out of both the scan and this small
-- hot index.
CREATE INDEX delivery_batches_generatable_schedule_idx
    ON daily_reporting.delivery_batches (scheduled_for, id)
    WHERE status IN ('planned', 'generating');

-- Correlate generation history to the candidate row. The previous nullable
-- side aggregated the complete append-only attempt table before the outer
-- status/deadline/limit predicates could reduce delivery_batches.
CREATE OR REPLACE VIEW daily_reporting.generatable_batches AS
SELECT
    batch.id,
    batch.recipient_id,
    batch.report_version,
    batch.scheduled_for,
    batch.status,
    max(coverage.deadline_at) AS deadline_at,
    coalesce(attempts.failed_attempts, 0) AS failed_attempts,
    attempts.retry_after
FROM daily_reporting.delivery_batches AS batch
JOIN daily_reporting.delivery_coverage AS coverage
    ON coverage.batch_id = batch.id
LEFT JOIN LATERAL (
    SELECT count(*) AS failed_attempts,
           max(attempt.retry_after) AS retry_after
    FROM daily_reporting.generation_attempts AS attempt
    WHERE attempt.batch_id = batch.id
) AS attempts ON true
WHERE batch.status IN ('planned', 'generating')
GROUP BY batch.id, attempts.failed_attempts, attempts.retry_after
HAVING count(*) = 1
   AND coalesce(attempts.failed_attempts, 0) < 5;

COMMENT ON INDEX daily_reporting.delivery_batches_generatable_schedule_idx IS
    'Bounded hot index for scheduled outbox generation candidates; terminal history is excluded.';

COMMIT;
