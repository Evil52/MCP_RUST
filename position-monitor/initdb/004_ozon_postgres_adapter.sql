BEGIN;

-- A cryptographic digest lets the restricted collector prove exact slot
-- idempotency without receiving SELECT access to raw measurement history.
ALTER TABLE search_position.collection_runs
    ADD COLUMN payload_digest text NOT NULL DEFAULT repeat('0', 64);

-- Existing rows predate the adapter and intentionally cannot be replayed as
-- identical payloads. A fixed valid marker makes any retry fail closed with a
-- slot conflict while preserving the historical row.
ALTER TABLE search_position.collection_runs
    ALTER COLUMN payload_digest DROP DEFAULT,
    ADD CONSTRAINT collection_runs_payload_digest_check CHECK (
        payload_digest ~ '^[0-9a-f]{64}$'
    );

CREATE FUNCTION search_position.enforce_ozon_payload_digest_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NEW.payload_digest IS DISTINCT FROM OLD.payload_digest THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'Ozon collection payload digest is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER collection_runs_payload_digest_immutable
    BEFORE UPDATE OF payload_digest ON search_position.collection_runs
    FOR EACH ROW
    EXECUTE FUNCTION search_position.enforce_ozon_payload_digest_immutable();

REVOKE ALL ON FUNCTION search_position.enforce_ozon_payload_digest_immutable()
    FROM PUBLIC;

COMMIT;
