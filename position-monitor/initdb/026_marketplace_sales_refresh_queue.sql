BEGIN;

-- Migration 023 shipped an Ozon-only queue. Preserve its rows and public MCP
-- compatibility functions while extending the durable identity to include the
-- marketplace. The table name remains unchanged to avoid a risky relation
-- rename during a rolling deployment.
ALTER TABLE daily_reporting.ozon_sales_refresh_requests
    ADD COLUMN marketplace text NOT NULL DEFAULT 'ozon';
ALTER TABLE daily_reporting.ozon_sales_refresh_requests
    ALTER COLUMN marketplace DROP DEFAULT;
ALTER TABLE daily_reporting.ozon_sales_refresh_requests
    ADD CONSTRAINT marketplace_sales_refresh_marketplace_check
    CHECK (marketplace IN ('ozon', 'wildberries'));

-- A retryable expired lease returns to the queue without changing its logical
-- cutoff. Decouple the latest attempt start from that stable cutoff while
-- retaining all other state invariants from migration 023.
DO $$
DECLARE
    state_constraint name;
BEGIN
    SELECT constraint_row.conname INTO STRICT state_constraint
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conrelid =
            'daily_reporting.ozon_sales_refresh_requests'::regclass
      AND constraint_row.contype = 'c'
      AND strpos(
            pg_get_constraintdef(constraint_row.oid),
            'snapshot_cutoff_at = started_at'
          ) > 0;
    EXECUTE format(
        'ALTER TABLE daily_reporting.ozon_sales_refresh_requests DROP CONSTRAINT %I',
        state_constraint
    );
END;
$$;

ALTER TABLE daily_reporting.ozon_sales_refresh_requests
    ADD CONSTRAINT marketplace_sales_refresh_state_check CHECK (
        (status = 'queued'
            AND owner_id IS NULL
            AND lease_until IS NULL
            AND started_at IS NULL
            AND finished_at IS NULL
            AND error_class IS NULL)
        OR
        (status = 'running'
            AND generation > 0
            AND attempt_count > 0
            AND owner_id IS NOT NULL
            AND lease_until > started_at
            AND lease_until <= started_at + interval '15 minutes'
            AND snapshot_cutoff_at IS NOT NULL
            AND started_at IS NOT NULL
            AND finished_at IS NULL
            AND error_class IS NULL)
        OR
        (status = 'succeeded'
            AND generation > 0
            AND attempt_count > 0
            AND owner_id IS NOT NULL
            AND lease_until IS NOT NULL
            AND snapshot_cutoff_at IS NOT NULL
            AND started_at IS NOT NULL
            AND finished_at >= started_at
            AND error_class IS NULL)
        OR
        (status = 'failed'
            AND finished_at IS NOT NULL
            AND error_class IS NOT NULL)
    );

DROP INDEX daily_reporting.ozon_sales_refresh_one_active_account_idx;
CREATE UNIQUE INDEX marketplace_sales_refresh_one_active_account_idx
    ON daily_reporting.ozon_sales_refresh_requests (account_id, marketplace)
    WHERE status IN ('queued', 'running');

CREATE INDEX marketplace_sales_refresh_history_idx
    ON daily_reporting.ozon_sales_refresh_requests
        (account_id, marketplace, requested_at DESC, id DESC);

CREATE FUNCTION daily_reporting.request_marketplace_sales_refresh(
    requested_account_id text,
    requested_marketplace text,
    requested_actor_id text,
    requested_business_date date
)
RETURNS TABLE (
    request_id bigint,
    request_status text,
    marketplace text,
    business_date date,
    requested_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    snapshot_cutoff_at timestamptz,
    created boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    now_at timestamptz := clock_timestamp();
    current_business_date date :=
        (clock_timestamp() AT TIME ZONE 'Asia/Yekaterinburg')::date;
BEGIN
    IF requested_account_id IS NULL
        OR requested_account_id !~ '^[A-Za-z0-9_-]{1,128}$'
        OR requested_marketplace IS NULL
        OR requested_marketplace NOT IN ('ozon', 'wildberries')
        OR requested_actor_id IS NULL
        OR requested_actor_id !~ '^[A-Za-z0-9._:@-]{1,128}$'
        OR requested_business_date IS DISTINCT FROM current_business_date
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'marketplace sales refresh request input is invalid';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended(requested_account_id || ':' || requested_marketplace, 917243)
    );

    UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
    SET status = 'queued',
        not_before = GREATEST(refresh.requested_at, now_at),
        owner_id = NULL,
        lease_until = NULL,
        started_at = NULL,
        finished_at = NULL,
        error_class = NULL
    WHERE refresh.account_id = requested_account_id
      AND refresh.marketplace = requested_marketplace
      AND refresh.status = 'running'
      AND refresh.lease_until <= now_at
      AND refresh.attempt_count < 3
      AND refresh.business_date = current_business_date
      AND refresh.requested_at >= now_at - interval '4 hours';

    UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
    SET status = 'failed',
        finished_at = now_at,
        error_class = CASE
            WHEN refresh.status = 'running' THEN 'worker_lease_expired'
            ELSE 'queue_expired'
        END
    WHERE refresh.account_id = requested_account_id
      AND refresh.marketplace = requested_marketplace
      AND (
          (refresh.status = 'running' AND refresh.lease_until <= now_at)
          OR
          (refresh.status = 'queued'
              AND (refresh.business_date <> current_business_date
                   OR refresh.requested_at < now_at - interval '4 hours'))
      );

    RETURN QUERY
    SELECT refresh.id,
           refresh.status,
           refresh.marketplace,
           refresh.business_date,
           refresh.requested_at,
           refresh.started_at,
           refresh.finished_at,
           refresh.snapshot_cutoff_at,
           false
    FROM daily_reporting.ozon_sales_refresh_requests AS refresh
    WHERE refresh.account_id = requested_account_id
      AND refresh.marketplace = requested_marketplace
      AND refresh.business_date = requested_business_date
      AND (
          refresh.status IN ('queued', 'running')
          OR
          (refresh.status = 'succeeded'
              AND refresh.finished_at > now_at - interval '10 minutes')
      )
    ORDER BY
        CASE refresh.status
            WHEN 'running' THEN 0
            WHEN 'queued' THEN 1
            ELSE 2
        END,
        refresh.id DESC
    LIMIT 1;
    IF FOUND THEN
        RETURN;
    END IF;

    RETURN QUERY
    INSERT INTO daily_reporting.ozon_sales_refresh_requests AS refresh (
        account_id, marketplace, business_date, requested_by, requested_at, not_before
    ) VALUES (
        requested_account_id,
        requested_marketplace,
        requested_business_date,
        requested_actor_id,
        now_at,
        now_at
    )
    RETURNING refresh.id,
              refresh.status,
              refresh.marketplace,
              refresh.business_date,
              refresh.requested_at,
              refresh.started_at,
              refresh.finished_at,
              refresh.snapshot_cutoff_at,
              true;
END;
$$;

CREATE FUNCTION daily_reporting.marketplace_sales_refresh_status(
    requested_account_id text,
    requested_marketplace text
)
RETURNS TABLE (
    request_id bigint,
    request_status text,
    marketplace text,
    business_date date,
    requested_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    snapshot_cutoff_at timestamptz
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    IF requested_account_id IS NULL
        OR requested_account_id !~ '^[A-Za-z0-9_-]{1,128}$'
        OR requested_marketplace IS NULL
        OR requested_marketplace NOT IN ('ozon', 'wildberries')
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'marketplace sales refresh status input is invalid';
    END IF;

    RETURN QUERY
    SELECT refresh.id,
           refresh.status,
           refresh.marketplace,
           refresh.business_date,
           refresh.requested_at,
           refresh.started_at,
           refresh.finished_at,
           refresh.snapshot_cutoff_at
    FROM daily_reporting.ozon_sales_refresh_requests AS refresh
    WHERE refresh.account_id = requested_account_id
      AND refresh.marketplace = requested_marketplace
    ORDER BY refresh.id DESC
    LIMIT 1;
END;
$$;

CREATE FUNCTION daily_reporting.claim_marketplace_sales_refresh_for(
    requested_owner_id text,
    requested_marketplace text
)
RETURNS TABLE (
    request_id bigint,
    request_generation integer,
    account_id text,
    marketplace text,
    business_date date,
    snapshot_cutoff_at timestamptz,
    lease_until timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    now_at timestamptz := clock_timestamp();
    current_business_date date :=
        (clock_timestamp() AT TIME ZONE 'Asia/Yekaterinburg')::date;
    selected_id bigint;
BEGIN
    IF requested_owner_id IS NULL
        OR requested_owner_id !~ '^[A-Za-z0-9._:-]{1,64}$'
        OR (requested_marketplace IS NOT NULL
            AND requested_marketplace NOT IN ('ozon', 'wildberries'))
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'marketplace sales refresh claim input is invalid';
    END IF;

    PERFORM pg_advisory_xact_lock(917244);

    UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
    SET status = 'queued',
        not_before = GREATEST(refresh.requested_at, now_at),
        owner_id = NULL,
        lease_until = NULL,
        started_at = NULL,
        finished_at = NULL,
        error_class = NULL
    WHERE refresh.status = 'running'
      AND refresh.lease_until <= now_at
      AND refresh.attempt_count < 3
      AND refresh.business_date = current_business_date
      AND refresh.requested_at >= now_at - interval '4 hours';

    UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
    SET status = 'failed',
        finished_at = now_at,
        error_class = CASE
            WHEN refresh.status = 'running' THEN 'worker_lease_expired'
            ELSE 'queue_expired'
        END
    WHERE (refresh.status = 'running' AND refresh.lease_until <= now_at)
       OR (refresh.status = 'queued'
           AND (refresh.business_date <> current_business_date
                OR refresh.requested_at < now_at - interval '4 hours'));

    IF EXISTS (
        SELECT 1
        FROM daily_reporting.ozon_sales_refresh_requests AS refresh
        WHERE refresh.status = 'running'
          AND refresh.lease_until > now_at
    ) THEN
        RETURN;
    END IF;

    SELECT refresh.id INTO selected_id
    FROM daily_reporting.ozon_sales_refresh_requests AS refresh
    WHERE refresh.status = 'queued'
      AND refresh.business_date = current_business_date
      AND refresh.not_before <= now_at
      AND (requested_marketplace IS NULL
           OR refresh.marketplace = requested_marketplace)
    ORDER BY refresh.requested_at, refresh.id
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

    IF selected_id IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
    SET status = 'running',
        generation = refresh.generation + 1,
        attempt_count = refresh.attempt_count + 1,
        owner_id = requested_owner_id,
        lease_until = now_at + interval '15 minutes',
        snapshot_cutoff_at = COALESCE(refresh.snapshot_cutoff_at, now_at),
        started_at = now_at,
        finished_at = NULL,
        error_class = NULL
    WHERE refresh.id = selected_id
      AND refresh.status = 'queued'
      AND refresh.attempt_count < 3
    RETURNING refresh.id,
              refresh.generation,
              refresh.account_id::text,
              refresh.marketplace,
              refresh.business_date,
              refresh.snapshot_cutoff_at,
              refresh.lease_until;
END;
$$;

CREATE FUNCTION daily_reporting.claim_marketplace_sales_refresh(
    requested_owner_id text
)
RETURNS TABLE (
    request_id bigint,
    request_generation integer,
    account_id text,
    marketplace text,
    business_date date,
    snapshot_cutoff_at timestamptz,
    lease_until timestamptz
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT *
    FROM daily_reporting.claim_marketplace_sales_refresh_for(
        requested_owner_id,
        NULL
    );
$$;

CREATE FUNCTION daily_reporting.finish_marketplace_sales_refresh(
    requested_request_id bigint,
    requested_generation integer,
    requested_owner_id text,
    requested_snapshot_cutoff_at timestamptz,
    requested_marketplace text,
    requested_error_class text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    finished boolean;
BEGIN
    IF requested_request_id IS NULL
        OR requested_request_id <= 0
        OR requested_generation IS NULL
        OR requested_generation <= 0
        OR requested_owner_id IS NULL
        OR requested_owner_id !~ '^[A-Za-z0-9._:-]{1,64}$'
        OR (requested_marketplace IS NOT NULL
            AND requested_marketplace NOT IN ('ozon', 'wildberries'))
        OR (requested_error_class IS NULL
            AND requested_snapshot_cutoff_at IS NULL)
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'marketplace sales refresh finish input is invalid';
    END IF;
    IF requested_error_class IS NOT NULL
        AND requested_error_class !~ '^[a-z][a-z0-9_]{0,63}$'
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'marketplace sales refresh error class is invalid';
    END IF;

    IF requested_error_class IS NOT NULL THEN
        UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
        SET status = 'failed',
            finished_at = clock_timestamp(),
            error_class = requested_error_class
        WHERE refresh.id = requested_request_id
          AND refresh.generation = requested_generation
          AND refresh.owner_id = requested_owner_id
          AND refresh.status = 'running'
          AND (requested_marketplace IS NULL
               OR refresh.marketplace = requested_marketplace)
        RETURNING true INTO finished;
        RETURN COALESCE(finished, false);
    END IF;

    UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
    SET status = 'succeeded',
        finished_at = clock_timestamp()
    WHERE refresh.id = requested_request_id
      AND refresh.generation = requested_generation
      AND refresh.owner_id = requested_owner_id
      AND refresh.snapshot_cutoff_at = requested_snapshot_cutoff_at
      AND refresh.status = 'running'
      AND refresh.lease_until > clock_timestamp()
      AND (requested_marketplace IS NULL
           OR refresh.marketplace = requested_marketplace)
      AND (
          SELECT count(*) = CASE refresh.marketplace
                     WHEN 'ozon' THEN 5
                     WHEN 'wildberries' THEN 4
                     ELSE 0
                 END
             AND count(DISTINCT snapshot.source) = CASE refresh.marketplace
                     WHEN 'ozon' THEN 5
                     WHEN 'wildberries' THEN 4
                     ELSE 0
                 END
             AND bool_and(snapshot.status = 'succeeded')
             AND bool_and(snapshot.pagination_complete)
             AND bool_and(
                 (refresh.marketplace = 'ozon'
                     AND snapshot.source IN ('sales', 'advertising', 'finance', 'stocks', 'prices'))
                 OR
                 (refresh.marketplace = 'wildberries'
                     AND snapshot.source IN ('sales', 'advertising', 'stocks', 'prices'))
             )
          FROM daily_reporting.source_snapshots AS snapshot
          WHERE snapshot.account_id = refresh.account_id
            AND snapshot.marketplace = refresh.marketplace
            AND snapshot.cutoff_at = refresh.snapshot_cutoff_at
      )
    RETURNING true INTO finished;
    RETURN COALESCE(finished, false);
END;
$$;

CREATE FUNCTION daily_reporting.complete_marketplace_sales_refresh(
    requested_request_id bigint,
    requested_generation integer,
    requested_owner_id text,
    requested_snapshot_cutoff_at timestamptz
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT daily_reporting.finish_marketplace_sales_refresh(
        requested_request_id,
        requested_generation,
        requested_owner_id,
        requested_snapshot_cutoff_at,
        NULL,
        NULL
    );
$$;

CREATE FUNCTION daily_reporting.fail_marketplace_sales_refresh(
    requested_request_id bigint,
    requested_generation integer,
    requested_owner_id text,
    requested_error_class text
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT daily_reporting.finish_marketplace_sales_refresh(
        requested_request_id,
        requested_generation,
        requested_owner_id,
        NULL,
        NULL,
        requested_error_class
    );
$$;

-- Compatibility surface for an old MCP or collector during a rolling
-- deployment. Crucially, the legacy claim can never return a WB row.
CREATE OR REPLACE FUNCTION daily_reporting.request_ozon_sales_refresh(
    requested_account_id text,
    requested_actor_id text,
    requested_business_date date
)
RETURNS TABLE (
    request_id bigint,
    request_status text,
    business_date date,
    requested_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    snapshot_cutoff_at timestamptz,
    created boolean
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT request_id,
           request_status,
           business_date,
           requested_at,
           started_at,
           finished_at,
           snapshot_cutoff_at,
           created
    FROM daily_reporting.request_marketplace_sales_refresh(
        requested_account_id,
        'ozon',
        requested_actor_id,
        requested_business_date
    );
$$;

CREATE OR REPLACE FUNCTION daily_reporting.ozon_sales_refresh_status(
    requested_account_id text
)
RETURNS TABLE (
    request_id bigint,
    request_status text,
    business_date date,
    requested_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    snapshot_cutoff_at timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT request_id,
           request_status,
           business_date,
           requested_at,
           started_at,
           finished_at,
           snapshot_cutoff_at
    FROM daily_reporting.marketplace_sales_refresh_status(
        requested_account_id,
        'ozon'
    );
$$;

CREATE OR REPLACE FUNCTION daily_reporting.claim_ozon_sales_refresh(
    requested_owner_id text
)
RETURNS TABLE (
    request_id bigint,
    request_generation integer,
    account_id text,
    business_date date,
    snapshot_cutoff_at timestamptz,
    lease_until timestamptz
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT request_id,
           request_generation,
           account_id,
           business_date,
           snapshot_cutoff_at,
           lease_until
    FROM daily_reporting.claim_marketplace_sales_refresh_for(
        requested_owner_id,
        'ozon'
    );
$$;

CREATE OR REPLACE FUNCTION daily_reporting.complete_ozon_sales_refresh(
    requested_request_id bigint,
    requested_generation integer,
    requested_owner_id text,
    requested_snapshot_cutoff_at timestamptz
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT daily_reporting.finish_marketplace_sales_refresh(
        requested_request_id,
        requested_generation,
        requested_owner_id,
        requested_snapshot_cutoff_at,
        'ozon',
        NULL
    );
$$;

CREATE OR REPLACE FUNCTION daily_reporting.fail_ozon_sales_refresh(
    requested_request_id bigint,
    requested_generation integer,
    requested_owner_id text,
    requested_error_class text
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT daily_reporting.finish_marketplace_sales_refresh(
        requested_request_id,
        requested_generation,
        requested_owner_id,
        NULL,
        'ozon',
        requested_error_class
    );
$$;

-- Normalized staging checkpoints close the crash window between a completed
-- marketplace fetch and atomic publication. They contain serialized internal
-- facts, never raw vendor responses. A replacement worker may reclaim the same
-- logical collection claim, validate the stored digest and publish without
-- repeating external requests.
CREATE TABLE daily_reporting.collection_staging_snapshots (
    claim_id bigint NOT NULL
        REFERENCES daily_reporting.collection_claims(id) ON DELETE RESTRICT,
    source text NOT NULL,
    payload_json text NOT NULL,
    payload_sha256 char(64) NOT NULL,
    staged_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (claim_id, source),
    CHECK (source IN ('sales', 'advertising', 'finance', 'stocks', 'prices')),
    CHECK (jsonb_typeof(payload_json::jsonb) = 'object'),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (octet_length(payload_json) BETWEEN 2 AND 67108864)
);

CREATE INDEX collection_staging_snapshots_age_idx
    ON daily_reporting.collection_staging_snapshots (staged_at, claim_id);

CREATE FUNCTION daily_reporting.require_active_staging_claim()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    claim_marketplace text;
BEGIN
    SELECT claim.marketplace INTO claim_marketplace
    FROM daily_reporting.collection_claims AS claim
    WHERE claim.id = NEW.claim_id
      AND claim.status = 'active'
      AND claim.lease_until > clock_timestamp()
    FOR KEY SHARE;
    IF claim_marketplace IS NULL
        OR (claim_marketplace = 'wildberries' AND NEW.source = 'finance')
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'object_not_in_prerequisite_state',
            MESSAGE = 'staging snapshot requires a compatible active collection claim';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER collection_staging_snapshots_require_active_claim
    BEFORE INSERT ON daily_reporting.collection_staging_snapshots
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.require_active_staging_claim();

-- Structured MCP telemetry deliberately excludes arguments, response bodies,
-- credentials and vendor payloads. The application can only open/finish a
-- bounded call and read the sanitized projection through SECURITY DEFINER
-- functions; direct table access remains denied.
CREATE TABLE daily_reporting.mcp_tool_calls (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    actor_id varchar(128) NOT NULL,
    tool_name varchar(128) NOT NULL,
    account_id varchar(128),
    marketplace text,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    duration_ms integer,
    outcome text NOT NULL DEFAULT 'running',
    error_code varchar(64),
    CHECK (actor_id ~ '^[A-Za-z0-9._:@-]{1,128}$'),
    CHECK (tool_name ~ '^[a-z][a-z0-9_-]{0,127}$'),
    CHECK (account_id IS NULL OR account_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    CHECK (marketplace IS NULL OR marketplace IN ('ozon', 'wildberries')),
    CHECK (outcome IN ('running', 'succeeded', 'failed', 'cancelled', 'overloaded')),
    CHECK (duration_ms IS NULL OR duration_ms BETWEEN 0 AND 600000),
    CHECK (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'),
    CHECK (
        (outcome = 'running'
            AND finished_at IS NULL
            AND duration_ms IS NULL
            AND error_code IS NULL)
        OR
        (outcome <> 'running'
            AND finished_at >= started_at
            AND duration_ms IS NOT NULL)
    )
);

CREATE INDEX mcp_tool_calls_admin_log_idx
    ON daily_reporting.mcp_tool_calls (started_at DESC, id DESC);
CREATE INDEX mcp_tool_calls_running_idx
    ON daily_reporting.mcp_tool_calls (started_at, id)
    WHERE outcome = 'running';

CREATE FUNCTION daily_reporting.begin_mcp_tool_call(
    requested_actor_id text,
    requested_tool_name text,
    requested_account_id text,
    requested_marketplace text
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    call_id bigint;
BEGIN
    IF requested_actor_id IS NULL
        OR requested_actor_id !~ '^[A-Za-z0-9._:@-]{1,128}$'
        OR requested_tool_name IS NULL
        OR requested_tool_name !~ '^[a-z][a-z0-9_-]{0,127}$'
        OR (requested_account_id IS NOT NULL
            AND requested_account_id !~ '^[A-Za-z0-9_-]{1,128}$')
        OR (requested_marketplace IS NOT NULL
            AND requested_marketplace NOT IN ('ozon', 'wildberries'))
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'MCP tool telemetry identity is invalid';
    END IF;

    INSERT INTO daily_reporting.mcp_tool_calls (
        actor_id, tool_name, account_id, marketplace
    ) VALUES (
        requested_actor_id,
        requested_tool_name,
        requested_account_id,
        requested_marketplace
    )
    RETURNING id INTO call_id;
    RETURN call_id;
END;
$$;

CREATE FUNCTION daily_reporting.finish_mcp_tool_call(
    requested_call_id bigint,
    requested_outcome text,
    requested_duration_ms integer,
    requested_error_code text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    completed boolean;
BEGIN
    IF requested_call_id IS NULL
        OR requested_call_id <= 0
        OR requested_outcome NOT IN ('succeeded', 'failed', 'cancelled', 'overloaded')
        OR requested_duration_ms IS NULL
        OR requested_duration_ms NOT BETWEEN 0 AND 600000
        OR (requested_error_code IS NOT NULL
            AND requested_error_code !~ '^[A-Z][A-Z0-9_]{0,63}$')
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'MCP tool telemetry completion is invalid';
    END IF;

    UPDATE daily_reporting.mcp_tool_calls AS call
    SET outcome = requested_outcome,
        finished_at = clock_timestamp(),
        duration_ms = requested_duration_ms,
        error_code = requested_error_code
    WHERE call.id = requested_call_id
      AND call.outcome = 'running'
    RETURNING true INTO completed;
    RETURN COALESCE(completed, false);
END;
$$;

CREATE FUNCTION daily_reporting.list_mcp_tool_calls(requested_limit integer)
RETURNS TABLE (
    call_id bigint,
    actor_id text,
    tool_name text,
    account_id text,
    marketplace text,
    started_at timestamptz,
    finished_at timestamptz,
    duration_ms integer,
    outcome text,
    error_code text
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    IF requested_limit IS NULL OR requested_limit NOT BETWEEN 1 AND 200 THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'MCP tool telemetry limit is invalid';
    END IF;
    RETURN QUERY
    SELECT call.id,
           call.actor_id::text,
           call.tool_name::text,
           call.account_id::text,
           call.marketplace,
           call.started_at,
           call.finished_at,
           call.duration_ms,
           call.outcome,
           call.error_code::text
    FROM daily_reporting.mcp_tool_calls AS call
    ORDER BY call.started_at DESC, call.id DESC
    LIMIT requested_limit;
END;
$$;

REVOKE ALL ON TABLE daily_reporting.ozon_sales_refresh_requests
    FROM PUBLIC, report_refresh_requester, report_collector, report_worker, position_reader;
REVOKE ALL ON SEQUENCE daily_reporting.ozon_sales_refresh_requests_id_seq
    FROM PUBLIC, report_refresh_requester, report_collector, report_worker, position_reader;
REVOKE ALL ON TABLE daily_reporting.mcp_tool_calls
    FROM PUBLIC, report_refresh_requester, report_collector, report_worker, position_reader;
REVOKE ALL ON SEQUENCE daily_reporting.mcp_tool_calls_id_seq
    FROM PUBLIC, report_refresh_requester, report_collector, report_worker, position_reader;
REVOKE ALL ON TABLE daily_reporting.collection_staging_snapshots
    FROM PUBLIC, report_refresh_requester, report_collector, report_worker, position_reader;
REVOKE ALL ON FUNCTION
    daily_reporting.request_marketplace_sales_refresh(text, text, text, date),
    daily_reporting.marketplace_sales_refresh_status(text, text),
    daily_reporting.claim_marketplace_sales_refresh_for(text, text),
    daily_reporting.claim_marketplace_sales_refresh(text),
    daily_reporting.finish_marketplace_sales_refresh(bigint, integer, text, timestamptz, text, text),
    daily_reporting.complete_marketplace_sales_refresh(bigint, integer, text, timestamptz),
    daily_reporting.fail_marketplace_sales_refresh(bigint, integer, text, text),
    daily_reporting.request_ozon_sales_refresh(text, text, date),
    daily_reporting.ozon_sales_refresh_status(text),
    daily_reporting.claim_ozon_sales_refresh(text),
    daily_reporting.complete_ozon_sales_refresh(bigint, integer, text, timestamptz),
    daily_reporting.fail_ozon_sales_refresh(bigint, integer, text, text),
    daily_reporting.begin_mcp_tool_call(text, text, text, text),
    daily_reporting.finish_mcp_tool_call(bigint, text, integer, text),
    daily_reporting.list_mcp_tool_calls(integer)
FROM PUBLIC, report_refresh_requester, report_collector;

GRANT EXECUTE ON FUNCTION
    daily_reporting.request_marketplace_sales_refresh(text, text, text, date),
    daily_reporting.marketplace_sales_refresh_status(text, text),
    daily_reporting.request_ozon_sales_refresh(text, text, date),
    daily_reporting.ozon_sales_refresh_status(text),
    daily_reporting.begin_mcp_tool_call(text, text, text, text),
    daily_reporting.finish_mcp_tool_call(bigint, text, integer, text),
    daily_reporting.list_mcp_tool_calls(integer)
TO report_refresh_requester;

GRANT EXECUTE ON FUNCTION
    daily_reporting.claim_marketplace_sales_refresh(text),
    daily_reporting.complete_marketplace_sales_refresh(bigint, integer, text, timestamptz),
    daily_reporting.fail_marketplace_sales_refresh(bigint, integer, text, text),
    daily_reporting.claim_ozon_sales_refresh(text),
    daily_reporting.complete_ozon_sales_refresh(bigint, integer, text, timestamptz),
    daily_reporting.fail_ozon_sales_refresh(bigint, integer, text, text)
TO report_collector;

GRANT SELECT, INSERT, DELETE ON TABLE
    daily_reporting.collection_staging_snapshots
TO report_collector;

COMMENT ON COLUMN daily_reporting.ozon_sales_refresh_requests.marketplace IS
    'Durable marketplace identity; table name is retained for rolling-deploy compatibility';
COMMENT ON FUNCTION daily_reporting.request_marketplace_sales_refresh(text, text, text, date) IS
    'Deduplicated current-day Ozon or WB snapshot refresh request';

COMMIT;
