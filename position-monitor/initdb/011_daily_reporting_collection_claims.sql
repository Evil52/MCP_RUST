BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'report_collector') THEN
        RAISE EXCEPTION 'report_collector role must exist before collection claims';
    END IF;
    IF to_regclass('daily_reporting.source_snapshots') IS NULL THEN
        RAISE EXCEPTION 'daily report snapshots must exist before collection claims';
    END IF;
END;
$$;

-- One row fences one account/marketplace/cutoff collection. An expired active
-- lease may be reclaimed with a strictly larger generation. A completed claim
-- is permanent and can never be reclaimed.
CREATE TABLE daily_reporting.collection_claims (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id varchar(128) NOT NULL,
    marketplace text NOT NULL,
    cutoff_at timestamptz NOT NULL,
    generation bigint NOT NULL DEFAULT 1,
    owner_id varchar(64) NOT NULL,
    claimed_at timestamptz NOT NULL,
    lease_until timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'active',
    completed_at timestamptz,
    UNIQUE (account_id, marketplace, cutoff_at),
    UNIQUE (id, account_id, marketplace, cutoff_at, generation),
    CHECK (account_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    CHECK (marketplace IN ('ozon', 'wildberries')),
    CHECK (generation BETWEEN 1 AND 2147483647),
    CHECK (owner_id ~ '^[A-Za-z0-9._:-]{1,64}$'),
    CHECK (lease_until > claimed_at),
    CHECK (lease_until <= claimed_at + interval '15 minutes'),
    CHECK (status IN ('active', 'completed')),
    CHECK (
        (status = 'active' AND completed_at IS NULL)
        OR
        (status = 'completed' AND completed_at IS NOT NULL
            AND completed_at >= claimed_at)
    )
);

ALTER TABLE daily_reporting.source_snapshots
    ADD COLUMN claim_id bigint,
    ADD COLUMN claim_generation bigint,
    ADD CONSTRAINT source_snapshots_claim_pair_check
        CHECK ((claim_id IS NULL) = (claim_generation IS NULL)),
    ADD CONSTRAINT source_snapshots_collection_claim_fkey
        FOREIGN KEY (
            claim_id, account_id, marketplace, cutoff_at, claim_generation
        )
        REFERENCES daily_reporting.collection_claims (
            id, account_id, marketplace, cutoff_at, generation
        )
        ON DELETE RESTRICT;

CREATE FUNCTION daily_reporting.claim_report_collection(
    requested_account_id text,
    requested_marketplace text,
    requested_cutoff_at timestamptz,
    requested_owner_id text
)
RETURNS TABLE (claim_id bigint, claim_generation bigint, lease_until timestamptz)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    now_at timestamptz := clock_timestamp();
BEGIN
    IF requested_account_id IS NULL
        OR requested_account_id !~ '^[A-Za-z0-9_-]{1,128}$'
        OR requested_marketplace IS NULL
        OR requested_marketplace NOT IN ('ozon', 'wildberries')
        OR requested_cutoff_at IS NULL
        OR requested_owner_id IS NULL
        OR requested_owner_id !~ '^[A-Za-z0-9._:-]{1,64}$'
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'daily report collection claim input is invalid';
    END IF;

    RETURN QUERY
    INSERT INTO daily_reporting.collection_claims AS claim
        (
            account_id, marketplace, cutoff_at, owner_id,
            claimed_at, lease_until
        )
    VALUES
        (
            requested_account_id, requested_marketplace, requested_cutoff_at,
            requested_owner_id, now_at, now_at + interval '15 minutes'
        )
    ON CONFLICT (account_id, marketplace, cutoff_at) DO UPDATE
    SET generation = CASE
            WHEN claim.owner_id = EXCLUDED.owner_id
             AND claim.lease_until > now_at
            THEN claim.generation
            ELSE claim.generation + 1
        END,
        owner_id = CASE
            WHEN claim.owner_id = EXCLUDED.owner_id
             AND claim.lease_until > now_at
            THEN claim.owner_id
            ELSE EXCLUDED.owner_id
        END,
        claimed_at = CASE
            WHEN claim.owner_id = EXCLUDED.owner_id
             AND claim.lease_until > now_at
            THEN claim.claimed_at
            ELSE EXCLUDED.claimed_at
        END,
        lease_until = CASE
            WHEN claim.owner_id = EXCLUDED.owner_id
             AND claim.lease_until > now_at
            THEN claim.lease_until
            ELSE EXCLUDED.lease_until
        END,
        completed_at = NULL,
        status = 'active'
    WHERE claim.status = 'active'
      AND (
          (claim.owner_id = EXCLUDED.owner_id AND claim.lease_until > now_at)
          OR
          (claim.lease_until <= now_at AND claim.generation < 2147483647)
      )
    RETURNING claim.id, claim.generation, claim.lease_until;
END;
$$;

CREATE FUNCTION daily_reporting.release_report_collection_claim(
    requested_claim_id bigint,
    requested_generation bigint,
    requested_owner_id text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    released boolean;
BEGIN
    UPDATE daily_reporting.collection_claims
    SET lease_until = GREATEST(
        clock_timestamp(), claimed_at + interval '1 microsecond'
    )
    WHERE id = requested_claim_id
      AND generation = requested_generation
      AND owner_id = requested_owner_id
      AND status = 'active'
      AND lease_until > clock_timestamp()
    RETURNING true INTO released;
    RETURN COALESCE(released, false);
END;
$$;

CREATE FUNCTION daily_reporting.require_active_collection_claim()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    claim_is_active boolean;
BEGIN
    IF NEW.claim_id IS NULL OR NEW.claim_generation IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'object_not_in_prerequisite_state',
            MESSAGE = 'new source snapshot requires an active collection claim';
    END IF;

    SELECT true INTO claim_is_active
    FROM daily_reporting.collection_claims
    WHERE id = NEW.claim_id
      AND account_id = NEW.account_id
      AND marketplace = NEW.marketplace
      AND cutoff_at = NEW.cutoff_at
      AND generation = NEW.claim_generation
      AND status = 'active'
      AND lease_until > clock_timestamp()
    FOR KEY SHARE;

    IF claim_is_active IS DISTINCT FROM true THEN
        RAISE EXCEPTION USING
            ERRCODE = 'object_not_in_prerequisite_state',
            MESSAGE = 'source snapshot collection claim is absent, stale, or expired';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER source_snapshots_require_active_collection_claim
    BEFORE INSERT ON daily_reporting.source_snapshots
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.require_active_collection_claim();

CREATE FUNCTION daily_reporting.complete_report_collection_claim(
    requested_claim_id bigint,
    requested_generation bigint,
    requested_owner_id text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    completed boolean;
BEGIN
    UPDATE daily_reporting.collection_claims AS claim
    SET status = 'completed', completed_at = clock_timestamp()
    WHERE claim.id = requested_claim_id
      AND claim.generation = requested_generation
      AND claim.owner_id = requested_owner_id
      AND claim.status = 'active'
      AND claim.lease_until > clock_timestamp()
      AND (
          SELECT count(*) = 4 AND count(DISTINCT snapshot.source) = 4
          FROM daily_reporting.source_snapshots AS snapshot
          WHERE snapshot.claim_id = claim.id
            AND snapshot.claim_generation = claim.generation
            AND snapshot.account_id = claim.account_id
            AND snapshot.marketplace = claim.marketplace
            AND snapshot.cutoff_at = claim.cutoff_at
            AND snapshot.status IN ('succeeded', 'partial')
      )
    RETURNING true INTO completed;
    RETURN COALESCE(completed, false);
END;
$$;

REVOKE ALL ON TABLE daily_reporting.collection_claims FROM PUBLIC;
REVOKE ALL ON SEQUENCE daily_reporting.collection_claims_id_seq FROM PUBLIC;
REVOKE ALL ON FUNCTION
    daily_reporting.claim_report_collection(text, text, timestamptz, text),
    daily_reporting.release_report_collection_claim(bigint, bigint, text),
    daily_reporting.require_active_collection_claim(),
    daily_reporting.complete_report_collection_claim(bigint, bigint, text)
FROM PUBLIC;

REVOKE ALL ON TABLE daily_reporting.collection_claims FROM report_collector, report_worker;
REVOKE ALL ON SEQUENCE daily_reporting.collection_claims_id_seq
    FROM report_collector, report_worker;
GRANT EXECUTE ON FUNCTION
    daily_reporting.claim_report_collection(text, text, timestamptz, text),
    daily_reporting.release_report_collection_claim(bigint, bigint, text),
    daily_reporting.complete_report_collection_claim(bigint, bigint, text)
TO report_collector;

COMMIT;
