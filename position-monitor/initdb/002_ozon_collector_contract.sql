BEGIN;

-- Align persisted monitors with the reviewed collector core. Existing rows
-- outside this contract make the migration fail; no row is deleted or coerced.
ALTER TABLE search_position.monitors
    ALTER COLUMN interval_minutes SET DEFAULT 30,
    DROP CONSTRAINT IF EXISTS monitors_interval_minutes_check,
    DROP CONSTRAINT IF EXISTS monitors_max_position_check,
    ADD CONSTRAINT monitors_interval_minutes_check CHECK (interval_minutes = 30),
    ADD CONSTRAINT monitors_max_position_check
        CHECK (max_position BETWEEN 1 AND 100),
    ADD CONSTRAINT monitors_product_id_collector_check
        CHECK (product_id ~ '^[0-9]+$');

ALTER TABLE search_position.collection_runs
    ADD COLUMN source text NOT NULL DEFAULT 'ozon_public_search',
    ADD COLUMN scheduled_for timestamptz,
    ADD COLUMN monitors_planned integer NOT NULL DEFAULT 0,
    ADD COLUMN queries_planned integer NOT NULL DEFAULT 0,
    ADD COLUMN queries_attempted integer NOT NULL DEFAULT 0,
    ADD COLUMN queries_succeeded integer NOT NULL DEFAULT 0;

UPDATE search_position.collection_runs
SET
    scheduled_for = date_bin(
        INTERVAL '30 minutes',
        started_at,
        TIMESTAMPTZ '2000-01-01 00:00:00+00'
    ),
    monitors_planned = GREATEST(monitors_attempted, monitors_succeeded),
    queries_planned = GREATEST(monitors_attempted, monitors_succeeded),
    queries_attempted = monitors_attempted,
    queries_succeeded = monitors_succeeded;

ALTER TABLE search_position.collection_runs
    ALTER COLUMN scheduled_for SET NOT NULL,
    ADD CONSTRAINT collection_runs_source_check
        CHECK (source = 'ozon_public_search'),
    ADD CONSTRAINT collection_runs_scheduled_slot_check CHECK (
        scheduled_for = date_bin(
            INTERVAL '30 minutes',
            scheduled_for,
            TIMESTAMPTZ '2000-01-01 00:00:00+00'
        )
    ),
    ADD CONSTRAINT collection_runs_monitor_counters_check CHECK (
        monitors_succeeded <= monitors_attempted
        AND monitors_attempted <= monitors_planned
    ),
    ADD CONSTRAINT collection_runs_query_counters_check CHECK (
        queries_succeeded <= queries_attempted
        AND queries_attempted <= queries_planned
    ),
    ADD CONSTRAINT collection_runs_terminal_time_check CHECK (
        (status = 'running' AND finished_at IS NULL)
        OR (status <> 'running' AND finished_at IS NOT NULL)
    ),
    ADD CONSTRAINT collection_runs_source_slot_key
        UNIQUE (source, scheduled_for);

CREATE FUNCTION search_position.enforce_ozon_collection_run_state()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'running'
            OR NEW.finished_at IS NOT NULL
            OR NEW.monitors_attempted <> 0
            OR NEW.monitors_succeeded <> 0
            OR NEW.queries_attempted <> 0
            OR NEW.queries_succeeded <> 0
            OR NEW.error_class IS NOT NULL
            OR NEW.http_status IS NOT NULL
        THEN
            RAISE EXCEPTION USING
                ERRCODE = 'integrity_constraint_violation',
                MESSAGE = 'Ozon collection run must start clean in running';
        END IF;
        RETURN NEW;
    END IF;

    IF ROW(
        NEW.id,
        NEW.source,
        NEW.scheduled_for,
        NEW.started_at,
        NEW.monitors_planned,
        NEW.queries_planned,
        NEW.collector_version
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.source,
        OLD.scheduled_for,
        OLD.started_at,
        OLD.monitors_planned,
        OLD.queries_planned,
        OLD.collector_version
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'Ozon collection run provenance is immutable';
    END IF;

    IF OLD.status <> 'running' THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'terminal Ozon collection run is immutable';
    END IF;

    IF NEW.monitors_attempted < OLD.monitors_attempted
        OR NEW.monitors_succeeded < OLD.monitors_succeeded
        OR NEW.queries_attempted < OLD.queries_attempted
        OR NEW.queries_succeeded < OLD.queries_succeeded
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'Ozon collection run counters cannot decrease';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER collection_runs_enforce_state
    BEFORE INSERT OR UPDATE ON search_position.collection_runs
    FOR EACH ROW
    EXECUTE FUNCTION search_position.enforce_ozon_collection_run_state();

ALTER TABLE search_position.measurements
    ADD COLUMN overall_position integer,
    ADD COLUMN placement text;

UPDATE search_position.measurements
SET
    overall_position = LEAST(organic_position, sponsored_position),
    placement = CASE
        WHEN organic_position IS NOT NULL
            AND (sponsored_position IS NULL OR organic_position <= sponsored_position)
            THEN 'organic'
        WHEN sponsored_position IS NOT NULL THEN 'sponsored'
        ELSE NULL
    END
WHERE outcome = 'found';

ALTER TABLE search_position.measurements
    DROP CONSTRAINT IF EXISTS measurements_check,
    DROP CONSTRAINT IF EXISTS measurements_check1,
    ADD CONSTRAINT measurements_position_range_check CHECK (
        (overall_position IS NULL OR overall_position BETWEEN 1 AND 100)
        AND (organic_position IS NULL OR organic_position BETWEEN 1 AND 100)
        AND (sponsored_position IS NULL OR sponsored_position BETWEEN 1 AND 100)
    ),
    ADD CONSTRAINT measurements_outcome_position_check CHECK (
        (
            outcome = 'found'
            AND overall_position IS NOT NULL
            AND placement IN ('organic', 'sponsored', 'unknown')
        )
        OR
        (
            outcome <> 'found'
            AND overall_position IS NULL
            AND organic_position IS NULL
            AND sponsored_position IS NULL
            AND placement IS NULL
        )
    );

CREATE FUNCTION search_position.require_running_ozon_measurement_run()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    run_status text;
    run_slot timestamptz;
BEGIN
    SELECT status, scheduled_for
    INTO run_status, run_slot
    FROM search_position.collection_runs
    WHERE id = NEW.run_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'foreign_key_violation',
            MESSAGE = 'Ozon measurement references an unknown collection run';
    END IF;

    IF run_status <> 'running' THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'Ozon measurements can only be appended to a running run';
    END IF;

    IF NEW.observed_at < run_slot
        OR NEW.observed_at >= run_slot + INTERVAL '30 minutes'
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'Ozon measurement is outside its logical slot';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER measurements_require_running_run
    BEFORE INSERT ON search_position.measurements
    FOR EACH ROW
    EXECUTE FUNCTION search_position.require_running_ozon_measurement_run();

CREATE FUNCTION search_position.require_running_ozon_alert_run()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    run_status text;
BEGIN
    SELECT run.status
    INTO run_status
    FROM search_position.measurements AS measurement
    JOIN search_position.collection_runs AS run
        ON run.id = measurement.run_id
    WHERE measurement.id = NEW.measurement_id
      AND measurement.monitor_id = NEW.monitor_id
    FOR UPDATE OF run;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'foreign_key_violation',
            MESSAGE = 'Ozon alert references an unknown measurement';
    END IF;

    IF run_status <> 'running' THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'Ozon alerts can only be appended to a running run';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER alerts_require_running_run
    BEFORE INSERT ON search_position.alerts
    FOR EACH ROW
    EXECUTE FUNCTION search_position.require_running_ozon_alert_run();

CREATE VIEW search_position.published_measurements
WITH (security_barrier = true)
AS
SELECT
    measurement.id,
    measurement.run_id,
    measurement.monitor_id,
    monitor.store_id,
    monitor.product_id,
    monitor.offer_id,
    monitor.search_phrase,
    monitor.region_code,
    monitor.region_name,
    run.scheduled_for,
    run.status AS run_status,
    (run.status = 'partial') AS is_partial,
    run.monitors_planned,
    run.monitors_attempted,
    run.monitors_succeeded,
    run.queries_planned,
    run.queries_attempted,
    run.queries_succeeded,
    run.error_class AS run_error_class,
    run.http_status AS run_http_status,
    run.finished_at AS run_finished_at,
    measurement.observed_at,
    measurement.outcome,
    measurement.overall_position,
    measurement.placement,
    measurement.organic_position,
    measurement.sponsored_position,
    measurement.result_page,
    measurement.price,
    measurement.original_price,
    measurement.delivery_days,
    measurement.in_stock,
    measurement.response_ms,
    measurement.error_class,
    measurement.http_status
FROM search_position.measurements AS measurement
JOIN search_position.collection_runs AS run ON run.id = measurement.run_id
JOIN search_position.monitors AS monitor ON monitor.id = measurement.monitor_id
WHERE run.status IN ('succeeded', 'partial');

CREATE VIEW search_position.published_alerts
WITH (security_barrier = true)
AS
SELECT
    alert.id,
    alert.monitor_id,
    alert.measurement_id,
    alert.kind,
    alert.previous_position,
    alert.current_position,
    alert.created_at,
    measurement.store_id,
    measurement.product_id,
    measurement.search_phrase,
    measurement.region_code,
    measurement.scheduled_for,
    measurement.run_status,
    measurement.is_partial
FROM search_position.alerts AS alert
JOIN search_position.published_measurements AS measurement
    ON measurement.id = alert.measurement_id
    AND measurement.monitor_id = alert.monitor_id;

CREATE OR REPLACE VIEW search_position.latest_measurements AS
SELECT DISTINCT ON (measurement.monitor_id)
    measurement.monitor_id,
    measurement.store_id,
    measurement.product_id,
    measurement.offer_id,
    measurement.search_phrase,
    measurement.region_code,
    measurement.region_name,
    measurement.observed_at,
    measurement.outcome,
    measurement.organic_position,
    measurement.sponsored_position,
    measurement.result_page,
    measurement.price,
    measurement.original_price,
    measurement.delivery_days,
    measurement.in_stock,
    measurement.response_ms,
    measurement.error_class,
    measurement.http_status,
    measurement.overall_position,
    measurement.placement,
    measurement.run_id,
    measurement.scheduled_for,
    measurement.run_status,
    measurement.is_partial
FROM search_position.published_measurements AS measurement
ORDER BY
    measurement.monitor_id,
    measurement.scheduled_for DESC,
    measurement.observed_at DESC,
    measurement.id DESC;

CREATE OR REPLACE VIEW search_position.hourly_position_summary AS
SELECT
    measurement.monitor_id,
    date_trunc('hour', measurement.observed_at) AS observed_hour,
    min(measurement.organic_position) AS best_organic_position,
    max(measurement.organic_position) AS worst_organic_position,
    avg(measurement.organic_position)::numeric(10, 2) AS average_organic_position,
    min(measurement.sponsored_position) AS best_sponsored_position,
    max(measurement.sponsored_position) AS worst_sponsored_position,
    avg(measurement.sponsored_position)::numeric(10, 2) AS average_sponsored_position,
    count(*) AS measurements,
    min(measurement.overall_position) AS best_overall_position,
    max(measurement.overall_position) AS worst_overall_position,
    avg(measurement.overall_position)::numeric(10, 2) AS average_overall_position,
    count(*) FILTER (WHERE measurement.outcome = 'found') AS found_measurements,
    count(*) FILTER (WHERE measurement.outcome = 'not_found') AS not_found_measurements,
    count(*) FILTER (WHERE measurement.placement = 'unknown') AS unknown_placement_measurements
FROM search_position.published_measurements AS measurement
GROUP BY measurement.monitor_id, date_trunc('hour', measurement.observed_at);

CREATE TABLE search_position.ozon_collector_circuit (
    source text PRIMARY KEY CHECK (source = 'ozon_public_search'),
    circuit_open boolean NOT NULL DEFAULT false,
    opened_at timestamptz,
    opened_by_run_id bigint
        REFERENCES search_position.collection_runs(id) ON DELETE RESTRICT,
    reason text CHECK (
        reason IS NULL OR reason IN (
            'captcha',
            'http_forbidden',
            'rate_limited',
            'markup_changed',
            'invalid_observation'
        )
    ),
    reset_at timestamptz,
    reset_by text CHECK (
        reset_by IS NULL OR (
            char_length(reset_by) BETWEEN 1 AND 128
            AND reset_by = btrim(reset_by)
            AND reset_by !~ '[[:cntrl:]]'
        )
    ),
    CHECK (
        (
            circuit_open
            AND opened_at IS NOT NULL
            AND opened_by_run_id IS NOT NULL
            AND reason IS NOT NULL
            AND reset_at IS NULL
            AND reset_by IS NULL
        )
        OR
        (
            NOT circuit_open
            AND (
                (
                    opened_at IS NULL
                    AND opened_by_run_id IS NULL
                    AND reason IS NULL
                    AND reset_at IS NULL
                    AND reset_by IS NULL
                )
                OR
                (
                    opened_at IS NOT NULL
                    AND opened_by_run_id IS NOT NULL
                    AND reason IS NOT NULL
                    AND reset_at IS NOT NULL
                    AND reset_by IS NOT NULL
                    AND reset_at >= opened_at
                )
            )
        )
    )
);

INSERT INTO search_position.ozon_collector_circuit (source)
VALUES ('ozon_public_search');

CREATE FUNCTION search_position.open_ozon_collector_circuit(
    run_id bigint,
    block_reason text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    IF block_reason NOT IN (
        'captcha',
        'http_forbidden',
        'rate_limited',
        'markup_changed',
        'invalid_observation'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'invalid Ozon collector circuit reason';
    END IF;

    PERFORM 1
    FROM search_position.collection_runs
    WHERE id = run_id AND status = 'running'
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'Ozon collector circuit requires a running run';
    END IF;

    UPDATE search_position.ozon_collector_circuit
    SET
        circuit_open = true,
        opened_at = statement_timestamp(),
        opened_by_run_id = run_id,
        reason = block_reason,
        reset_at = NULL,
        reset_by = NULL
    WHERE source = 'ozon_public_search'
      AND NOT circuit_open;
END;
$$;

CREATE TABLE search_position.ozon_region_request_budgets (
    region_code text PRIMARY KEY CHECK (
        octet_length(region_code) BETWEEN 1 AND 64
        AND region_code = btrim(region_code)
        AND region_code !~ '[[:cntrl:]]'
    ),
    daily_limit integer NOT NULL CHECK (daily_limit BETWEEN 1 AND 5000),
    active boolean NOT NULL DEFAULT true,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE search_position.ozon_request_budget_usage (
    budget_day date NOT NULL,
    region_code text NOT NULL
        REFERENCES search_position.ozon_region_request_budgets(region_code)
        ON DELETE RESTRICT,
    requests_started integer NOT NULL CHECK (requests_started > 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (budget_day, region_code)
);

CREATE FUNCTION search_position.claim_ozon_request_budget(
    requested_region_code text,
    requested_slot timestamptz
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    claimed_count integer;
    circuit_is_open boolean;
BEGIN
    IF requested_region_code IS NULL
        OR octet_length(requested_region_code) NOT BETWEEN 1 AND 64
        OR requested_slot IS NULL
    THEN
        RETURN false;
    END IF;

    IF requested_region_code <> btrim(requested_region_code)
        OR requested_region_code ~ '[[:cntrl:]]'
        OR requested_slot <> date_bin(
            INTERVAL '30 minutes',
            requested_slot,
            TIMESTAMPTZ '2000-01-01 00:00:00+00'
        )
    THEN
        RETURN false;
    END IF;

    SELECT circuit_open
    INTO circuit_is_open
    FROM search_position.ozon_collector_circuit
    WHERE source = 'ozon_public_search'
    FOR UPDATE;

    IF NOT FOUND OR circuit_is_open THEN
        RETURN false;
    END IF;

    INSERT INTO search_position.ozon_request_budget_usage (
        budget_day,
        region_code,
        requests_started,
        updated_at
    )
    SELECT
        (requested_slot AT TIME ZONE 'UTC')::date,
        budget.region_code,
        1,
        statement_timestamp()
    FROM search_position.ozon_region_request_budgets AS budget
    WHERE budget.region_code = requested_region_code
      AND budget.active
    ON CONFLICT (budget_day, region_code) DO UPDATE
    SET
        requests_started =
            search_position.ozon_request_budget_usage.requests_started + 1,
        updated_at = statement_timestamp()
    WHERE search_position.ozon_request_budget_usage.requests_started < (
        SELECT budget.daily_limit
        FROM search_position.ozon_region_request_budgets AS budget
        WHERE budget.region_code = requested_region_code
          AND budget.active
    )
    RETURNING requests_started INTO claimed_count;

    RETURN claimed_count IS NOT NULL;
END;
$$;

REVOKE ALL ON TABLE search_position.published_measurements FROM PUBLIC;
REVOKE ALL ON TABLE search_position.published_alerts FROM PUBLIC;
REVOKE ALL ON TABLE search_position.ozon_collector_circuit FROM PUBLIC;
REVOKE ALL ON TABLE search_position.ozon_region_request_budgets FROM PUBLIC;
REVOKE ALL ON TABLE search_position.ozon_request_budget_usage FROM PUBLIC;
REVOKE ALL ON FUNCTION search_position.enforce_ozon_collection_run_state()
    FROM PUBLIC;
REVOKE ALL ON FUNCTION search_position.require_running_ozon_measurement_run()
    FROM PUBLIC;
REVOKE ALL ON FUNCTION search_position.require_running_ozon_alert_run()
    FROM PUBLIC;
REVOKE ALL ON FUNCTION search_position.open_ozon_collector_circuit(bigint, text)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION search_position.claim_ozon_request_budget(text, timestamptz)
    FROM PUBLIC;

-- Fresh databases apply this migration before the restricted roles exist.
-- Existing volumes can apply the same file once and converge to the new ACL.
DO $grant_existing_roles$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'position_collector') THEN
        EXECUTE 'REVOKE ALL ON search_position.collection_runs FROM position_collector';
        EXECUTE 'REVOKE ALL ON search_position.measurements FROM position_collector';
        EXECUTE 'REVOKE ALL ON search_position.alerts FROM position_collector';
        EXECUTE 'GRANT SELECT, INSERT ON search_position.collection_runs TO position_collector';
        EXECUTE 'GRANT UPDATE (finished_at, status, monitors_attempted, monitors_succeeded, queries_attempted, queries_succeeded, error_class, http_status) ON search_position.collection_runs TO position_collector';
        EXECUTE 'GRANT INSERT ON search_position.measurements TO position_collector';
        EXECUTE 'GRANT INSERT ON search_position.alerts TO position_collector';
        EXECUTE 'GRANT EXECUTE ON FUNCTION search_position.open_ozon_collector_circuit(bigint, text) TO position_collector';
        EXECUTE 'GRANT EXECUTE ON FUNCTION search_position.claim_ozon_request_budget(text, timestamptz) TO position_collector';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'position_reader') THEN
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA search_position REVOKE SELECT ON TABLES FROM position_reader';
        EXECUTE 'REVOKE ALL ON search_position.collection_runs FROM position_reader';
        EXECUTE 'REVOKE ALL ON search_position.measurements FROM position_reader';
        EXECUTE 'REVOKE ALL ON search_position.alerts FROM position_reader';
        EXECUTE 'GRANT SELECT ON search_position.monitors TO position_reader';
        EXECUTE 'GRANT SELECT ON search_position.published_measurements TO position_reader';
        EXECUTE 'GRANT SELECT ON search_position.published_alerts TO position_reader';
        EXECUTE 'GRANT SELECT ON search_position.latest_measurements TO position_reader';
        EXECUTE 'GRANT SELECT ON search_position.hourly_position_summary TO position_reader';
    END IF;
END;
$grant_existing_roles$;

COMMIT;
