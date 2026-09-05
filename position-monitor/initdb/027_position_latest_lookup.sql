BEGIN;

-- The campaign guard reads one monitor, but the old latest_measurements view
-- had to order the history of every monitor because scheduled_for lived only
-- on collection_runs. Copy the immutable logical slot onto each measurement
-- so one monitor-specific descending index can answer the hot lookup.
ALTER TABLE search_position.measurements
    ADD COLUMN scheduled_for timestamptz;

UPDATE search_position.measurements AS measurement
SET scheduled_for = run.scheduled_for
FROM search_position.collection_runs AS run
WHERE run.id = measurement.run_id;

ALTER TABLE search_position.measurements
    ALTER COLUMN scheduled_for SET NOT NULL;

CREATE INDEX measurements_monitor_slot
    ON search_position.measurements
        (monitor_id, scheduled_for DESC, observed_at DESC, id DESC);

CREATE OR REPLACE FUNCTION search_position.require_running_ozon_measurement_run()
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

    -- Ignore any caller-supplied value. The run row is the sole authority for
    -- this denormalized key and is locked until the insert completes.
    NEW.scheduled_for := run_slot;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE VIEW search_position.published_measurements
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
    measurement.scheduled_for,
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

COMMENT ON COLUMN search_position.measurements.scheduled_for IS
    'Immutable logical run slot copied by the trusted measurement trigger for indexed latest-position reads.';

COMMIT;
