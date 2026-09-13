-- Every process using the same marketplace credential and endpoint family
-- shares a durable departure deadline. Keys are one-way digests; credentials,
-- account names and request bodies must never be stored here.
BEGIN;

-- Two overlapping WB automation runtimes each need one state-store session
-- and one process-wide quota session. Keep a bounded four-session allowance;
-- role privileges and statement/transaction timeouts are unchanged.
ALTER ROLE wb_automation_writer CONNECTION LIMIT 4;

CREATE SCHEMA IF NOT EXISTS marketplace_quota;

CREATE TABLE IF NOT EXISTS marketplace_quota.departures (
    key text PRIMARY KEY CHECK (key ~ '^[0-9a-f]{64}$'),
    next_allowed_at timestamptz NOT NULL
);

CREATE OR REPLACE FUNCTION marketplace_quota.try_acquire(
    quota_key text, interval_millis bigint
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    deadline timestamptz;
    database_now timestamptz;
BEGIN
    IF quota_key IS NULL OR quota_key !~ '^[0-9a-f]{64}$'
       OR interval_millis IS NULL OR interval_millis NOT BETWEEN 1 AND 86400000 THEN
        RAISE EXCEPTION 'invalid marketplace quota input' USING ERRCODE = '22023';
    END IF;

    INSERT INTO marketplace_quota.departures (key, next_allowed_at)
    VALUES (quota_key, '-infinity'::timestamptz)
    ON CONFLICT (key) DO NOTHING;

    SELECT next_allowed_at INTO STRICT deadline
    FROM marketplace_quota.departures WHERE key = quota_key FOR UPDATE;
    -- A competing insert/update may have blocked us. Sample the database clock
    -- only after obtaining the lock, never at statement/transaction start.
    database_now := clock_timestamp();
    IF deadline = 'infinity'::timestamptz THEN
        -- An unrepresentably long provider cooldown requires administrator
        -- reconciliation. Keep refusing with a bounded retry; never overflow.
        RETURN 86400000;
    END IF;
    IF deadline > database_now THEN
        -- A rejected attempt does not reserve a future slot or move the queue.
        RETURN ceil(extract(epoch FROM deadline - database_now) * 1000)::bigint;
    END IF;

    UPDATE marketplace_quota.departures
    SET next_allowed_at = database_now + interval_millis * interval '1 millisecond'
    WHERE key = quota_key;
    RETURN 0;
END
$$;

CREATE OR REPLACE FUNCTION marketplace_quota.extend_cooldown(
    quota_key text, delay_millis bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    deadline timestamptz;
    database_now timestamptz;
BEGIN
    IF quota_key IS NULL OR quota_key !~ '^[0-9a-f]{64}$'
       OR delay_millis IS NULL OR delay_millis < 1 THEN
        RAISE EXCEPTION 'invalid marketplace quota input' USING ERRCODE = '22023';
    END IF;

    INSERT INTO marketplace_quota.departures (key, next_allowed_at)
    VALUES (quota_key, '-infinity'::timestamptz)
    ON CONFLICT (key) DO NOTHING;

    SELECT next_allowed_at INTO STRICT deadline
    FROM marketplace_quota.departures WHERE key = quota_key FOR UPDATE;
    database_now := clock_timestamp();
    UPDATE marketplace_quota.departures
    SET next_allowed_at = greatest(
        deadline,
        -- Preserve actual provider delays up to 366 days. Beyond that horizon
        -- quarantine the key instead of overflowing or shortening Retry-After.
        -- A shorter later cooldown can never clear an infinite deadline.
        CASE WHEN delay_millis > 31622400000 THEN 'infinity'::timestamptz
             ELSE database_now + delay_millis * interval '1 millisecond'
        END
    )
    WHERE key = quota_key;
END
$$;

REVOKE ALL ON SCHEMA marketplace_quota
    FROM PUBLIC,position_reader,report_worker,position_collector,
         report_collector,report_refresh_requester,control_writer,
         ozon_control_planner,ozon_control_executor,wb_automation_writer;
REVOKE ALL ON ALL TABLES IN SCHEMA marketplace_quota
    FROM PUBLIC,position_reader,report_worker,position_collector,
         report_collector,report_refresh_requester,control_writer,
         ozon_control_planner,ozon_control_executor,wb_automation_writer;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA marketplace_quota
    FROM PUBLIC,position_reader,report_worker,position_collector,
         report_collector,report_refresh_requester,control_writer,
         ozon_control_planner,ozon_control_executor,wb_automation_writer;

GRANT USAGE ON SCHEMA marketplace_quota
    TO position_collector,report_collector,report_refresh_requester,control_writer,
       ozon_control_planner,ozon_control_executor,wb_automation_writer;
GRANT EXECUTE ON FUNCTION marketplace_quota.try_acquire(text,bigint),
    marketplace_quota.extend_cooldown(text,bigint)
    TO position_collector,report_collector,report_refresh_requester,control_writer,
       ozon_control_planner,ozon_control_executor,wb_automation_writer;

COMMIT;
