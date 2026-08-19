BEGIN;

-- Replace only the three legacy anonymous checks whose vocabularies need to
-- grow. Constraint discovery is based on their exact semantics so this
-- additive migration works both with the original schema and a fresh image.
DO $$
DECLARE
    constraint_name name;
    matched integer;
BEGIN
    SELECT min(conname), count(*)
    INTO constraint_name, matched
    FROM pg_constraint
    WHERE conrelid = 'daily_reporting.delivery_batches'::regclass
      AND contype = 'c'
      AND position('last_error_class' IN pg_get_constraintdef(oid)) > 0
      AND position('artifact_generation' IN pg_get_constraintdef(oid)) > 0;
    IF matched <> 1 THEN
        RAISE EXCEPTION 'expected one delivery batch error-class constraint';
    END IF;
    EXECUTE format(
        'ALTER TABLE daily_reporting.delivery_batches DROP CONSTRAINT %I',
        constraint_name
    );

    SELECT min(conname), count(*)
    INTO constraint_name, matched
    FROM pg_constraint
    WHERE conrelid = 'daily_reporting.delivery_attempts'::regclass
      AND contype = 'c'
      AND position('provider_message_id IS NOT NULL' IN pg_get_constraintdef(oid)) > 0
      AND position('error_class' IN pg_get_constraintdef(oid)) > 0;
    IF matched <> 1 THEN
        RAISE EXCEPTION 'expected one delivery attempt shape constraint';
    END IF;
    EXECUTE format(
        'ALTER TABLE daily_reporting.delivery_attempts DROP CONSTRAINT %I',
        constraint_name
    );

    SELECT min(conname), count(*)
    INTO constraint_name, matched
    FROM pg_constraint
    WHERE conrelid = 'daily_reporting.delivery_attempts'::regclass
      AND contype = 'c'
      AND position('outcome <> ''transient''' IN pg_get_constraintdef(oid)) > 0;
    IF matched <> 1 THEN
        RAISE EXCEPTION 'expected one transient delivery error-class constraint';
    END IF;
    EXECUTE format(
        'ALTER TABLE daily_reporting.delivery_attempts DROP CONSTRAINT %I',
        constraint_name
    );
END;
$$;

ALTER TABLE daily_reporting.delivery_batches
    ADD CONSTRAINT delivery_batches_error_class_check CHECK (
        last_error_class IS NULL
        OR last_error_class IN (
            'authentication', 'invalid_recipient', 'invalid_artifact',
            'invalid_routing', 'provider_rejected', 'rate_limited',
            'provider_unavailable', 'transport', 'artifact_generation',
            'data_incomplete', 'storage'
        )
    );

ALTER TABLE daily_reporting.delivery_attempts
    ADD CONSTRAINT delivery_attempts_shape_check CHECK (
        (outcome = 'sent' AND error_class IS NULL AND provider_message_id IS NOT NULL)
        OR (
            outcome IN ('transient', 'permanent')
            AND error_class IN (
                'authentication', 'invalid_recipient', 'invalid_artifact',
                'invalid_routing', 'provider_rejected', 'rate_limited',
                'provider_unavailable', 'transport'
            )
            AND provider_message_id IS NULL
        )
    );

ALTER TABLE daily_reporting.delivery_attempts
    ADD CONSTRAINT delivery_attempts_transient_error_class_check CHECK (
        outcome <> 'transient'
        OR error_class IN ('rate_limited', 'provider_unavailable', 'transport')
    );

COMMIT;
