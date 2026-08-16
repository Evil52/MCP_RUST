BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'report_worker') THEN
        RAISE EXCEPTION 'report_worker role must be created before the reporting migration';
    END IF;
END;
$$;

CREATE SCHEMA daily_reporting;
REVOKE ALL ON SCHEMA daily_reporting FROM PUBLIC;

CREATE TABLE daily_reporting.delivery_batches (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    recipient_id varchar(128) NOT NULL,
    report_version integer NOT NULL,
    scheduled_for timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'planned',
    delayed boolean NOT NULL DEFAULT false,
    attempts smallint NOT NULL DEFAULT 0,
    artifact_object_key varchar(512),
    artifact_sha256 char(64),
    next_attempt_at timestamptz,
    provider_message_id varchar(512),
    last_error_class text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    sent_at timestamptz,
    UNIQUE (id, recipient_id, report_version),
    CHECK (recipient_id ~ '^[A-Za-z0-9_-]+$'),
    CHECK (report_version > 0),
    CHECK (status IN (
        'planned', 'generating', 'ready', 'sending', 'sent', 'expired',
        'permanent_failure'
    )),
    CHECK (attempts BETWEEN 0 AND 5),
    CHECK (
        (artifact_object_key IS NULL AND artifact_sha256 IS NULL)
        OR (
            length(artifact_object_key) BETWEEN 1 AND 512
            AND artifact_sha256 ~ '^[0-9A-Fa-f]{64}$'
        )
    ),
    CHECK (
        status NOT IN ('ready', 'sending', 'sent')
        OR artifact_object_key IS NOT NULL
    ),
    CHECK (
        (status = 'ready' AND next_attempt_at IS NOT NULL)
        OR (status <> 'ready' AND next_attempt_at IS NULL)
        OR (status = 'ready' AND attempts = 0)
    ),
    CHECK (
        (status = 'sent' AND provider_message_id IS NOT NULL AND sent_at IS NOT NULL)
        OR (status <> 'sent' AND provider_message_id IS NULL AND sent_at IS NULL)
    ),
    CHECK (
        provider_message_id IS NULL
        OR provider_message_id ~ '^[A-Za-z0-9_.:@-]+$'
    ),
    CHECK (
        last_error_class IS NULL
        OR last_error_class IN (
            'authentication', 'invalid_recipient', 'rate_limited',
            'provider_unavailable', 'transport', 'artifact_generation',
            'data_incomplete', 'storage'
        )
    ),
    CHECK (status <> 'permanent_failure' OR last_error_class IS NOT NULL),
    CHECK (updated_at >= created_at)
);

CREATE TABLE daily_reporting.delivery_coverage (
    batch_id bigint NOT NULL,
    recipient_id varchar(128) NOT NULL,
    report_version integer NOT NULL,
    local_date date NOT NULL,
    report_kind text NOT NULL,
    scheduled_for timestamptz NOT NULL,
    deadline_at timestamptz NOT NULL,
    PRIMARY KEY (local_date, report_kind, recipient_id, report_version),
    UNIQUE (batch_id, report_kind),
    FOREIGN KEY (batch_id, recipient_id, report_version)
        REFERENCES daily_reporting.delivery_batches (id, recipient_id, report_version)
        ON DELETE RESTRICT,
    CHECK (report_kind IN ('morning', 'evening')),
    CHECK (
        (
            report_kind = 'morning'
            AND scheduled_for =
                ((local_date + time '08:00') AT TIME ZONE 'Asia/Yekaterinburg')
            AND deadline_at =
                ((local_date + time '14:00') AT TIME ZONE 'Asia/Yekaterinburg')
        )
        OR (
            report_kind = 'evening'
            AND scheduled_for =
                ((local_date + time '17:00') AT TIME ZONE 'Asia/Yekaterinburg')
            AND deadline_at =
                ((local_date + time '23:00') AT TIME ZONE 'Asia/Yekaterinburg')
        )
    )
);

CREATE TABLE daily_reporting.delivery_attempts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id bigint NOT NULL REFERENCES daily_reporting.delivery_batches(id)
        ON DELETE RESTRICT,
    attempt_no smallint NOT NULL CHECK (attempt_no BETWEEN 1 AND 5),
    started_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('sent', 'transient', 'permanent')),
    error_class text,
    provider_message_id varchar(512),
    UNIQUE (batch_id, attempt_no),
    CHECK (finished_at >= started_at),
    CHECK (
        (outcome = 'sent' AND error_class IS NULL AND provider_message_id IS NOT NULL)
        OR (
            outcome IN ('transient', 'permanent')
            AND error_class IN (
                'authentication', 'invalid_recipient', 'rate_limited',
                'provider_unavailable', 'transport'
            )
            AND provider_message_id IS NULL
        )
    ),
    CHECK (
        provider_message_id IS NULL
        OR provider_message_id ~ '^[A-Za-z0-9_.:@-]+$'
    ),
    CHECK (
        outcome <> 'transient'
        OR error_class IN ('rate_limited', 'provider_unavailable', 'transport')
    )
);

CREATE FUNCTION daily_reporting.enforce_delivery_batch_state()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    coverage_count integer;
    latest_schedule timestamptz;
    latest_deadline timestamptz;
    first_local_date date;
    last_local_date date;
BEGIN
    IF OLD.status IN ('sent', 'expired', 'permanent_failure') THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'terminal report delivery is immutable';
    END IF;
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.recipient_id IS DISTINCT FROM OLD.recipient_id
        OR NEW.report_version IS DISTINCT FROM OLD.report_version
        OR NEW.scheduled_for IS DISTINCT FROM OLD.scheduled_for
        OR NEW.delayed IS DISTINCT FROM OLD.delayed
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'report delivery identity is immutable';
    END IF;
    IF NEW.updated_at <= OLD.updated_at THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'report delivery updated_at must move forward';
    END IF;
    IF NOT (
        (OLD.status = 'planned' AND NEW.status IN ('generating', 'expired', 'permanent_failure'))
        OR (OLD.status = 'generating' AND NEW.status IN ('ready', 'expired', 'permanent_failure'))
        OR (OLD.status = 'ready' AND NEW.status IN ('sending', 'expired', 'permanent_failure'))
        OR (OLD.status = 'sending' AND NEW.status IN ('ready', 'sent', 'expired', 'permanent_failure'))
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'invalid report delivery state transition';
    END IF;
    IF OLD.status = 'ready' AND NEW.status = 'sending' THEN
        IF NEW.attempts <> OLD.attempts + 1 OR NEW.attempts > 5 THEN
            RAISE EXCEPTION USING
                ERRCODE = 'integrity_constraint_violation',
                MESSAGE = 'report delivery attempt budget is invalid';
        END IF;
    ELSIF NEW.attempts <> OLD.attempts THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'report delivery attempts may advance only when sending';
    END IF;
    SELECT count(*), max(scheduled_for), max(deadline_at),
           min(local_date), max(local_date)
    INTO coverage_count, latest_schedule, latest_deadline,
         first_local_date, last_local_date
    FROM daily_reporting.delivery_coverage
    WHERE batch_id = OLD.id;
    IF coverage_count NOT BETWEEN 1 AND 2
        OR latest_schedule <> OLD.scheduled_for
        OR first_local_date <> last_local_date
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'report delivery coverage is incomplete or mismatched';
    END IF;
    IF OLD.status = 'sending' AND NEW.status = 'ready' AND (
        NEW.next_attempt_at IS NULL
        OR NEW.next_attempt_at <= NEW.updated_at
        OR NEW.next_attempt_at > latest_deadline
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'report delivery retry must remain inside the delivery window';
    END IF;
    IF NEW.status IN ('generating', 'ready', 'sending')
        AND NEW.updated_at > latest_deadline
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'report delivery may not start or resume after its deadline';
    END IF;
    IF NEW.status = 'expired' AND NEW.updated_at <= latest_deadline THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'report delivery may expire only after its deadline';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER delivery_batches_enforce_state
    BEFORE UPDATE ON daily_reporting.delivery_batches
    FOR EACH ROW
    EXECUTE FUNCTION daily_reporting.enforce_delivery_batch_state();

CREATE FUNCTION daily_reporting.require_planned_delivery_coverage()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    batch_status text;
BEGIN
    SELECT status INTO batch_status
    FROM daily_reporting.delivery_batches
    WHERE id = NEW.batch_id
    FOR KEY SHARE;
    IF batch_status IS DISTINCT FROM 'planned' THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'report coverage may be added only to a planned delivery';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER delivery_coverage_requires_planned_batch
    BEFORE INSERT ON daily_reporting.delivery_coverage
    FOR EACH ROW
    EXECUTE FUNCTION daily_reporting.require_planned_delivery_coverage();

CREATE FUNCTION daily_reporting.reject_coverage_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = 'integrity_constraint_violation',
        MESSAGE = 'report delivery coverage is append-only';
END;
$$;

CREATE TRIGGER delivery_coverage_is_append_only
    BEFORE UPDATE OR DELETE ON daily_reporting.delivery_coverage
    FOR EACH ROW
    EXECUTE FUNCTION daily_reporting.reject_coverage_mutation();

CREATE FUNCTION daily_reporting.require_active_delivery_attempt()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    batch_status text;
    batch_attempts smallint;
BEGIN
    SELECT status, attempts INTO batch_status, batch_attempts
    FROM daily_reporting.delivery_batches
    WHERE id = NEW.batch_id
    FOR KEY SHARE;
    IF batch_status IS DISTINCT FROM 'sending' OR NEW.attempt_no <> batch_attempts THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'report attempt must belong to the active send';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER delivery_attempts_require_active_send
    BEFORE INSERT ON daily_reporting.delivery_attempts
    FOR EACH ROW
    EXECUTE FUNCTION daily_reporting.require_active_delivery_attempt();

CREATE FUNCTION daily_reporting.reject_attempt_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = 'integrity_constraint_violation',
        MESSAGE = 'report delivery attempts are append-only';
END;
$$;

CREATE TRIGGER delivery_attempts_are_append_only
    BEFORE UPDATE OR DELETE ON daily_reporting.delivery_attempts
    FOR EACH ROW
    EXECUTE FUNCTION daily_reporting.reject_attempt_mutation();

CREATE VIEW daily_reporting.claimable_deliveries AS
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
    max(coverage.deadline_at) AS deadline_at
FROM daily_reporting.delivery_batches AS batch
JOIN daily_reporting.delivery_coverage AS coverage
    ON coverage.batch_id = batch.id
WHERE batch.status = 'ready'
GROUP BY batch.id
HAVING max(coverage.deadline_at) >= clock_timestamp()
   AND (batch.next_attempt_at IS NULL OR batch.next_attempt_at <= clock_timestamp());

CREATE INDEX delivery_batches_claimable_idx
    ON daily_reporting.delivery_batches (status, next_attempt_at, scheduled_for)
    WHERE status = 'ready';

REVOKE ALL ON ALL TABLES IN SCHEMA daily_reporting FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA daily_reporting FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA daily_reporting FROM PUBLIC;

GRANT USAGE ON SCHEMA daily_reporting TO report_worker;
GRANT SELECT, INSERT ON daily_reporting.delivery_batches TO report_worker;
GRANT UPDATE (
    status,
    attempts,
    artifact_object_key,
    artifact_sha256,
    next_attempt_at,
    provider_message_id,
    last_error_class,
    updated_at,
    sent_at
) ON daily_reporting.delivery_batches TO report_worker;
GRANT SELECT, INSERT ON daily_reporting.delivery_coverage TO report_worker;
GRANT SELECT, INSERT ON daily_reporting.delivery_attempts TO report_worker;
GRANT SELECT ON daily_reporting.claimable_deliveries TO report_worker;
GRANT USAGE, SELECT ON SEQUENCE
    daily_reporting.delivery_batches_id_seq,
    daily_reporting.delivery_attempts_id_seq
    TO report_worker;

ALTER DEFAULT PRIVILEGES IN SCHEMA daily_reporting
    REVOKE ALL ON TABLES FROM PUBLIC, report_worker;
ALTER DEFAULT PRIVILEGES IN SCHEMA daily_reporting
    REVOKE ALL ON SEQUENCES FROM PUBLIC, report_worker;
ALTER DEFAULT PRIVILEGES IN SCHEMA daily_reporting
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC, report_worker;

COMMIT;
