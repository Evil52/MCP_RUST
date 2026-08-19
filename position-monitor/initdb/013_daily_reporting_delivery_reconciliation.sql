BEGIN;

-- Ambiguous provider outcomes are never retried automatically. An operator
-- may close exactly the active attempt only after checking Gmail outside this
-- process. The decision is append-only and remains distinct from the original
-- delivery attempt, whose actual finish time may be unknown.
ALTER TABLE daily_reporting.delivery_batches
    DROP CONSTRAINT delivery_batches_error_class_check;

ALTER TABLE daily_reporting.delivery_batches
    ADD CONSTRAINT delivery_batches_error_class_check CHECK (
        last_error_class IS NULL
        OR last_error_class IN (
            'authentication', 'invalid_recipient', 'invalid_artifact',
            'invalid_routing', 'provider_rejected', 'rate_limited',
            'provider_unavailable', 'transport', 'artifact_generation',
            'data_incomplete', 'storage', 'operator_reconciled_unknown'
        )
    );

CREATE TABLE daily_reporting.delivery_reconciliations (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id bigint NOT NULL
        REFERENCES daily_reporting.delivery_batches(id),
    attempt_no smallint NOT NULL CHECK (attempt_no BETWEEN 1 AND 5),
    reconciled_at timestamptz NOT NULL,
    decision text NOT NULL
        CHECK (decision IN ('confirmed_sent', 'suppressed_unknown')),
    provider_message_id text,
    UNIQUE (batch_id, attempt_no),
    CHECK (
        provider_message_id IS NULL
        OR (
            octet_length(provider_message_id) BETWEEN 1 AND 512
            AND provider_message_id ~ '^[A-Za-z0-9_.:@-]+$'
        )
    ),
    CHECK (
        (decision = 'confirmed_sent' AND provider_message_id IS NOT NULL)
        OR (decision = 'suppressed_unknown' AND provider_message_id IS NULL)
    )
);

CREATE FUNCTION daily_reporting.require_ambiguous_delivery_reconciliation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    batch_status text;
    batch_attempts smallint;
    batch_updated_at timestamptz;
BEGIN
    SELECT status, attempts, updated_at
    INTO batch_status, batch_attempts, batch_updated_at
    FROM daily_reporting.delivery_batches
    WHERE id = NEW.batch_id
    FOR UPDATE;
    IF batch_status IS DISTINCT FROM 'sending'
        OR NEW.attempt_no <> batch_attempts
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'reconciliation must belong to the active ambiguous send';
    END IF;
    IF NEW.reconciled_at < batch_updated_at
        OR NEW.reconciled_at > clock_timestamp() + interval '1 minute'
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'reconciliation timestamp is outside the active send window';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER delivery_reconciliations_require_active_send
    BEFORE INSERT ON daily_reporting.delivery_reconciliations
    FOR EACH ROW
    EXECUTE FUNCTION daily_reporting.require_ambiguous_delivery_reconciliation();

CREATE FUNCTION daily_reporting.reject_reconciliation_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = 'integrity_constraint_violation',
        MESSAGE = 'report delivery reconciliations are append-only';
END;
$$;

CREATE TRIGGER delivery_reconciliations_are_append_only
    BEFORE UPDATE OR DELETE ON daily_reporting.delivery_reconciliations
    FOR EACH ROW
    EXECUTE FUNCTION daily_reporting.reject_reconciliation_mutation();

REVOKE ALL ON daily_reporting.delivery_reconciliations FROM PUBLIC;
REVOKE ALL ON SEQUENCE
    daily_reporting.delivery_reconciliations_id_seq FROM PUBLIC;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'report_worker') THEN
        EXECUTE 'GRANT SELECT, INSERT ON daily_reporting.delivery_reconciliations TO report_worker';
        EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE daily_reporting.delivery_reconciliations_id_seq TO report_worker';
    END IF;
END;
$$;

COMMIT;
