BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'report_refresh_requester'
    ) THEN
        RAISE EXCEPTION
            'report_refresh_requester role must exist before the refresh queue';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'report_collector'
    ) THEN
        RAISE EXCEPTION
            'report_collector role must exist before the refresh queue';
    END IF;
    IF to_regclass('daily_reporting.source_snapshots') IS NULL THEN
        RAISE EXCEPTION
            'daily report snapshots must exist before the refresh queue';
    END IF;
END;
$$;

-- One active row represents one account-wide request. Concurrent manager calls
-- reuse it, while a recently completed row is reused for ten minutes. The MCP
-- role has no table privileges and can only execute the two bounded request
-- functions below. The collector sees work only through fenced claim/finish
-- functions and continues to own every marketplace credential and snapshot
-- write. A queued request may wait four hours, which covers fourteen distinct
-- accounts at the collector's twelve-minute per-account deadline plus one
-- scheduled-report reservation window.
CREATE TABLE daily_reporting.ozon_sales_refresh_requests (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id varchar(128) NOT NULL,
    business_date date NOT NULL,
    requested_by varchar(128) NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    not_before timestamptz NOT NULL DEFAULT clock_timestamp(),
    status text NOT NULL DEFAULT 'queued',
    generation integer NOT NULL DEFAULT 0,
    attempt_count integer NOT NULL DEFAULT 0,
    owner_id varchar(64),
    lease_until timestamptz,
    snapshot_cutoff_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    error_class varchar(64),
    CHECK (account_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    CHECK (requested_by ~ '^[A-Za-z0-9._:@-]{1,128}$'),
    CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    CHECK (generation BETWEEN 0 AND 2147483647),
    CHECK (attempt_count BETWEEN 0 AND 3),
    CHECK (owner_id IS NULL OR owner_id ~ '^[A-Za-z0-9._:-]{1,64}$'),
    CHECK (error_class IS NULL OR error_class ~ '^[a-z][a-z0-9_]{0,63}$'),
    CHECK (not_before >= requested_at),
    CHECK (
        (status = 'queued'
            AND owner_id IS NULL
            AND lease_until IS NULL
            AND snapshot_cutoff_at IS NULL
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
            AND snapshot_cutoff_at = started_at
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
    )
);

CREATE UNIQUE INDEX ozon_sales_refresh_one_active_account_idx
    ON daily_reporting.ozon_sales_refresh_requests (account_id)
    WHERE status IN ('queued', 'running');

-- Even if the collector service is accidentally scaled to multiple replicas,
-- only one marketplace refresh may run globally. The claim function also uses
-- a transaction advisory lock so competing replicas return an empty claim
-- instead of surfacing a unique-index race.
CREATE UNIQUE INDEX ozon_sales_refresh_one_running_global_idx
    ON daily_reporting.ozon_sales_refresh_requests ((true))
    WHERE status = 'running';

CREATE INDEX ozon_sales_refresh_claim_idx
    ON daily_reporting.ozon_sales_refresh_requests
        (status, not_before, requested_at, id);

CREATE FUNCTION daily_reporting.request_ozon_sales_refresh(
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
        OR requested_actor_id IS NULL
        OR requested_actor_id !~ '^[A-Za-z0-9._:@-]{1,128}$'
        OR requested_business_date IS DISTINCT FROM current_business_date
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'Ozon sales refresh request input is invalid';
    END IF;

    PERFORM pg_advisory_xact_lock(hashtextextended(requested_account_id, 917243));

    UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
    SET status = 'failed',
        finished_at = now_at,
        error_class = CASE
            WHEN refresh.status = 'running' THEN 'worker_lease_expired'
            ELSE 'queue_expired'
        END
    WHERE refresh.account_id = requested_account_id
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
           refresh.business_date,
           refresh.requested_at,
           refresh.started_at,
           refresh.finished_at,
           refresh.snapshot_cutoff_at,
           false
    FROM daily_reporting.ozon_sales_refresh_requests AS refresh
    WHERE refresh.account_id = requested_account_id
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
        account_id, business_date, requested_by, requested_at, not_before
    ) VALUES (
        requested_account_id,
        requested_business_date,
        requested_actor_id,
        now_at,
        now_at
    )
    RETURNING refresh.id,
              refresh.status,
              refresh.business_date,
              refresh.requested_at,
              refresh.started_at,
              refresh.finished_at,
              refresh.snapshot_cutoff_at,
              true;
END;
$$;

CREATE FUNCTION daily_reporting.ozon_sales_refresh_status(
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
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    IF requested_account_id IS NULL
        OR requested_account_id !~ '^[A-Za-z0-9_-]{1,128}$'
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'Ozon sales refresh status input is invalid';
    END IF;

    RETURN QUERY
    SELECT refresh.id,
           refresh.status,
           refresh.business_date,
           refresh.requested_at,
           refresh.started_at,
           refresh.finished_at,
           refresh.snapshot_cutoff_at
    FROM daily_reporting.ozon_sales_refresh_requests AS refresh
    WHERE refresh.account_id = requested_account_id
    ORDER BY refresh.id DESC
    LIMIT 1;
END;
$$;

CREATE FUNCTION daily_reporting.claim_ozon_sales_refresh(
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
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'Ozon sales refresh owner is invalid';
    END IF;

    PERFORM pg_advisory_xact_lock(917244);

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
        snapshot_cutoff_at = now_at,
        started_at = now_at,
        finished_at = NULL,
        error_class = NULL
    WHERE refresh.id = selected_id
      AND refresh.status = 'queued'
      AND refresh.attempt_count < 3
    RETURNING refresh.id,
              refresh.generation,
              refresh.account_id::text,
              refresh.business_date,
              refresh.snapshot_cutoff_at,
              refresh.lease_until;
END;
$$;

CREATE FUNCTION daily_reporting.complete_ozon_sales_refresh(
    requested_request_id bigint,
    requested_generation integer,
    requested_owner_id text,
    requested_snapshot_cutoff_at timestamptz
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    completed boolean;
BEGIN
    UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
    SET status = 'succeeded',
        finished_at = clock_timestamp()
    WHERE refresh.id = requested_request_id
      AND refresh.generation = requested_generation
      AND refresh.owner_id = requested_owner_id
      AND refresh.snapshot_cutoff_at = requested_snapshot_cutoff_at
      AND refresh.status = 'running'
      AND refresh.lease_until > clock_timestamp()
      AND (
          SELECT count(*) = 5
             AND count(DISTINCT snapshot.source) = 5
             AND bool_and(snapshot.status = 'succeeded')
             AND bool_and(snapshot.pagination_complete)
          FROM daily_reporting.source_snapshots AS snapshot
          WHERE snapshot.account_id = refresh.account_id
            AND snapshot.marketplace = 'ozon'
            AND snapshot.cutoff_at = refresh.snapshot_cutoff_at
      )
    RETURNING true INTO completed;
    RETURN COALESCE(completed, false);
END;
$$;

CREATE FUNCTION daily_reporting.fail_ozon_sales_refresh(
    requested_request_id bigint,
    requested_generation integer,
    requested_owner_id text,
    requested_error_class text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    failed boolean;
BEGIN
    IF requested_error_class IS NULL
        OR requested_error_class !~ '^[a-z][a-z0-9_]{0,63}$'
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'Ozon sales refresh error class is invalid';
    END IF;

    UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = requested_error_class
    WHERE refresh.id = requested_request_id
      AND refresh.generation = requested_generation
      AND refresh.owner_id = requested_owner_id
      AND refresh.status = 'running'
    RETURNING true INTO failed;
    RETURN COALESCE(failed, false);
END;
$$;

REVOKE ALL ON TABLE daily_reporting.ozon_sales_refresh_requests FROM PUBLIC;
REVOKE ALL ON SEQUENCE daily_reporting.ozon_sales_refresh_requests_id_seq FROM PUBLIC;
REVOKE ALL ON FUNCTION
    daily_reporting.request_ozon_sales_refresh(text, text, date),
    daily_reporting.ozon_sales_refresh_status(text),
    daily_reporting.claim_ozon_sales_refresh(text),
    daily_reporting.complete_ozon_sales_refresh(bigint, integer, text, timestamptz),
    daily_reporting.fail_ozon_sales_refresh(bigint, integer, text, text)
FROM PUBLIC, report_refresh_requester, report_collector;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA daily_reporting FROM report_refresh_requester;

GRANT USAGE ON SCHEMA daily_reporting TO report_refresh_requester;
GRANT EXECUTE ON FUNCTION
    daily_reporting.request_ozon_sales_refresh(text, text, date),
    daily_reporting.ozon_sales_refresh_status(text)
TO report_refresh_requester;

GRANT EXECUTE ON FUNCTION
    daily_reporting.claim_ozon_sales_refresh(text),
    daily_reporting.complete_ozon_sales_refresh(bigint, integer, text, timestamptz),
    daily_reporting.fail_ozon_sales_refresh(bigint, integer, text, text)
TO report_collector;

REVOKE ALL ON TABLE daily_reporting.ozon_sales_refresh_requests
    FROM report_refresh_requester, report_collector, report_worker, position_reader;
REVOKE ALL ON SEQUENCE daily_reporting.ozon_sales_refresh_requests_id_seq
    FROM report_refresh_requester, report_collector, report_worker, position_reader;

COMMIT;
