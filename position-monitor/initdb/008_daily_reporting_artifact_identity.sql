BEGIN;

ALTER TABLE daily_reporting.delivery_batches
    ADD COLUMN IF NOT EXISTS artifact_html_sha256 char(64);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM daily_reporting.delivery_batches
        WHERE artifact_object_key IS NOT NULL AND artifact_html_sha256 IS NULL
    ) THEN
        RAISE EXCEPTION
            'existing report artifacts require an audited HTML hash before migration';
    END IF;
END;
$$;

ALTER TABLE daily_reporting.delivery_batches
    ADD CONSTRAINT delivery_artifact_identity_shape
    CHECK (
        (
            artifact_object_key IS NULL
            AND artifact_sha256 IS NULL
            AND artifact_html_sha256 IS NULL
        )
        OR (
            artifact_object_key ~
                '^daily-reports/[0-9]{4}/[0-9]{2}/[0-9]{2}/[A-Za-z0-9_-]+/v[1-9][0-9]*/(morning|evening)\.xlsx$'
            AND artifact_sha256 ~ '^[0-9a-f]{64}$'
            AND artifact_html_sha256 ~ '^[0-9a-f]{64}$'
        )
    ) NOT VALID;

ALTER TABLE daily_reporting.delivery_batches
    VALIDATE CONSTRAINT delivery_artifact_identity_shape;

CREATE FUNCTION daily_reporting.enforce_delivery_artifact_identity()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    coverage_count integer;
    coverage_date date;
    coverage_kind text;
    expected_key text;
BEGIN
    IF NEW.status <> 'ready' THEN
        RETURN NEW;
    END IF;
    SELECT count(*), min(local_date),
           CASE
               WHEN bool_or(report_kind = 'evening') THEN 'evening'
               WHEN bool_and(report_kind = 'morning') THEN 'morning'
               ELSE NULL
           END
    INTO coverage_count, coverage_date, coverage_kind
    FROM daily_reporting.delivery_coverage
    WHERE batch_id = NEW.id;
    IF coverage_count NOT BETWEEN 1 AND 2 OR coverage_kind IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'report artifact coverage is invalid';
    END IF;
    expected_key := format(
        'daily-reports/%s/%s/%s/%s/v%s/%s.xlsx',
        to_char(coverage_date, 'YYYY'),
        to_char(coverage_date, 'MM'),
        to_char(coverage_date, 'DD'),
        NEW.recipient_id,
        NEW.report_version,
        coverage_kind
    );
    IF NEW.artifact_object_key IS DISTINCT FROM expected_key THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'report artifact identity does not match delivery coverage';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER delivery_batches_enforce_artifact_identity
    BEFORE UPDATE ON daily_reporting.delivery_batches
    FOR EACH ROW
    EXECUTE FUNCTION daily_reporting.enforce_delivery_artifact_identity();

CREATE OR REPLACE VIEW daily_reporting.claimable_deliveries AS
SELECT
    batch.id,
    batch.recipient_id,
    batch.report_version,
    batch.scheduled_for,
    batch.delayed,
    batch.attempts,
    batch.artifact_object_key,
    batch.artifact_sha256,
    batch.next_attempt_at,
    max(coverage.deadline_at) AS deadline_at,
    batch.artifact_html_sha256
FROM daily_reporting.delivery_batches AS batch
JOIN daily_reporting.delivery_coverage AS coverage
    ON coverage.batch_id = batch.id
WHERE batch.status = 'ready'
GROUP BY batch.id
HAVING max(coverage.deadline_at) >= clock_timestamp()
   AND (batch.next_attempt_at IS NULL OR batch.next_attempt_at <= clock_timestamp());

REVOKE ALL ON FUNCTION
    daily_reporting.enforce_delivery_artifact_identity()
    FROM PUBLIC, report_worker;

GRANT UPDATE (artifact_html_sha256)
    ON daily_reporting.delivery_batches TO report_worker;

COMMIT;
