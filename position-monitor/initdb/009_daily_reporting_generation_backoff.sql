BEGIN;

-- Generation had no failure memory. A batch whose report could not be rendered
-- stayed at the head of the candidate ordering and was retried on every tick,
-- forever. With a bounded number of candidates per tick, a handful of such
-- batches permanently starved every healthy one behind them.
--
-- The backoff deliberately does not live on delivery_batches. That table's
-- state trigger rejects any update which is not a status transition, and its
-- `attempts` column is the delivery budget — reusing either would entangle
-- generation retries with delivery semantics. An append-only attempt log
-- mirrors how delivery already records its own attempts.
CREATE TABLE daily_reporting.generation_attempts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES daily_reporting.delivery_batches(id)
        ON DELETE RESTRICT,
    attempt_no smallint NOT NULL CHECK (attempt_no BETWEEN 1 AND 5),
    failed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retry_after timestamptz NOT NULL,
    error_class text NOT NULL CHECK (error_class ~ '^[a-z][a-z0-9_]{0,63}$'),
    UNIQUE (batch_id, attempt_no),
    CHECK (retry_after > failed_at)
);

CREATE INDEX generation_attempts_batch_retry_idx
    ON daily_reporting.generation_attempts (batch_id, retry_after DESC);

-- An attempt may only be recorded against work that is still generatable, and
-- only as the next number in sequence. This makes the attempt count a true
-- budget rather than a value a caller can skip past or rewind.
CREATE FUNCTION daily_reporting.require_generatable_batch()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    batch_status text;
    recorded_attempts smallint;
BEGIN
    SELECT status INTO batch_status
    FROM daily_reporting.delivery_batches
    WHERE id = NEW.batch_id
    FOR KEY SHARE;
    IF batch_status IS DISTINCT FROM 'planned'
        AND batch_status IS DISTINCT FROM 'generating'
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'generation attempt must belong to a generatable report';
    END IF;
    SELECT count(*) INTO recorded_attempts
    FROM daily_reporting.generation_attempts
    WHERE batch_id = NEW.batch_id;
    IF NEW.attempt_no <> recorded_attempts + 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'generation attempts must advance one at a time';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER generation_attempts_require_generatable_batch
    BEFORE INSERT ON daily_reporting.generation_attempts
    FOR EACH ROW
    EXECUTE FUNCTION daily_reporting.require_generatable_batch();

CREATE FUNCTION daily_reporting.reject_generation_attempt_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = 'integrity_constraint_violation',
        MESSAGE = 'report generation attempts are append-only';
END;
$$;

CREATE TRIGGER generation_attempts_are_append_only
    BEFORE UPDATE OR DELETE ON daily_reporting.generation_attempts
    FOR EACH ROW
    EXECUTE FUNCTION daily_reporting.reject_generation_attempt_mutation();

-- Single-section work that still has generation budget, with the backoff
-- deadline exposed rather than applied.
--
-- The attempt budget is a property of the row and belongs here, next to the
-- CHECK that defines it. The time comparison deliberately does not: callers
-- pass the instant they are scheduling for, and a view that substituted
-- `clock_timestamp()` would silently ignore it.
--
-- A batch that exhausts its budget is not deleted or force-failed here. It
-- leaves this view and appears in `stalled_report_work`, so an operator
-- decides what happens to it.
CREATE VIEW daily_reporting.generatable_batches AS
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
LEFT JOIN (
    SELECT batch_id,
           count(*) AS failed_attempts,
           max(retry_after) AS retry_after
    FROM daily_reporting.generation_attempts
    GROUP BY batch_id
) AS attempts ON attempts.batch_id = batch.id
WHERE batch.status IN ('planned', 'generating')
GROUP BY batch.id, attempts.failed_attempts, attempts.retry_after
HAVING count(*) = 1
   AND coalesce(attempts.failed_attempts, 0) < 5;

-- Every way report work can come to rest without completing, in one place.
-- Three of these are deliberate designs rather than bugs — an ambiguous send
-- is never auto-retried, a crash between artifact commit and the ready
-- transition leaves `generating`, and an abandoned collection leaves
-- `running` — but each still needs an operator to see it and decide.
CREATE VIEW daily_reporting.stalled_report_work AS
SELECT
    'delivery_ambiguous'::text AS stall_kind,
    batch.id::text AS reference,
    batch.recipient_id,
    batch.updated_at AS stalled_since
FROM daily_reporting.delivery_batches AS batch
WHERE batch.status = 'sending'
UNION ALL
SELECT
    'generation_exhausted'::text,
    batch.id::text,
    batch.recipient_id,
    max(attempt.failed_at)
FROM daily_reporting.delivery_batches AS batch
JOIN daily_reporting.generation_attempts AS attempt
    ON attempt.batch_id = batch.id
WHERE batch.status IN ('planned', 'generating')
GROUP BY batch.id
HAVING max(attempt.attempt_no) >= 5
UNION ALL
SELECT
    'snapshot_abandoned'::text,
    snapshot.id::text,
    snapshot.account_id,
    snapshot.started_at
FROM daily_reporting.source_snapshots AS snapshot
WHERE snapshot.status = 'running';

GRANT SELECT, INSERT ON daily_reporting.generation_attempts TO report_worker;
GRANT USAGE, SELECT ON SEQUENCE
    daily_reporting.generation_attempts_id_seq
    TO report_worker;
GRANT SELECT ON daily_reporting.generatable_batches TO report_worker;

REVOKE ALL ON FUNCTION
    daily_reporting.require_generatable_batch(),
    daily_reporting.reject_generation_attempt_mutation()
    FROM PUBLIC, report_worker;

COMMIT;
