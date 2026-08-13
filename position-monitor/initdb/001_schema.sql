BEGIN;

CREATE SCHEMA search_position;

CREATE TABLE search_position.monitors (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_id text NOT NULL CHECK (char_length(store_id) BETWEEN 1 AND 128),
    product_id text NOT NULL CHECK (char_length(product_id) BETWEEN 1 AND 128),
    offer_id text CHECK (offer_id IS NULL OR char_length(offer_id) BETWEEN 1 AND 128),
    search_phrase text NOT NULL CHECK (char_length(search_phrase) BETWEEN 1 AND 256),
    region_code text NOT NULL CHECK (char_length(region_code) BETWEEN 1 AND 64),
    region_name text NOT NULL CHECK (char_length(region_name) BETWEEN 1 AND 128),
    interval_minutes smallint NOT NULL DEFAULT 15
        CHECK (interval_minutes IN (15, 30, 60)),
    max_position smallint NOT NULL DEFAULT 100
        CHECK (max_position BETWEEN 1 AND 500),
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX monitors_natural_key
    ON search_position.monitors (
        store_id,
        product_id,
        lower(search_phrase),
        region_code
    );

CREATE INDEX monitors_due_configuration
    ON search_position.monitors (active, interval_minutes, region_code);

CREATE TABLE search_position.collection_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    status text NOT NULL
        CHECK (status IN ('running', 'succeeded', 'partial', 'failed', 'blocked')),
    monitors_attempted integer NOT NULL DEFAULT 0 CHECK (monitors_attempted >= 0),
    monitors_succeeded integer NOT NULL DEFAULT 0 CHECK (monitors_succeeded >= 0),
    error_class text CHECK (
        error_class IS NULL OR
        (char_length(error_class) BETWEEN 1 AND 64 AND error_class !~ '[[:space:]]')
    ),
    http_status smallint CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    collector_version text NOT NULL CHECK (char_length(collector_version) BETWEEN 1 AND 64),
    CHECK (finished_at IS NULL OR finished_at >= started_at),
    CHECK (monitors_succeeded <= monitors_attempted)
);

CREATE INDEX collection_runs_started_at
    ON search_position.collection_runs (started_at DESC);

CREATE TABLE search_position.measurements (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id bigint NOT NULL
        REFERENCES search_position.collection_runs(id) ON DELETE CASCADE,
    monitor_id bigint NOT NULL
        REFERENCES search_position.monitors(id) ON DELETE RESTRICT,
    observed_at timestamptz NOT NULL,
    outcome text NOT NULL
        CHECK (outcome IN ('found', 'not_found', 'blocked', 'error')),
    organic_position integer CHECK (organic_position IS NULL OR organic_position > 0),
    sponsored_position integer CHECK (sponsored_position IS NULL OR sponsored_position > 0),
    result_page smallint CHECK (result_page IS NULL OR result_page > 0),
    price numeric(14, 2) CHECK (price IS NULL OR price >= 0),
    original_price numeric(14, 2) CHECK (original_price IS NULL OR original_price >= 0),
    delivery_days smallint CHECK (delivery_days IS NULL OR delivery_days >= 0),
    in_stock boolean,
    response_ms integer CHECK (response_ms IS NULL OR response_ms >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, monitor_id),
    CHECK (
        outcome <> 'found' OR
        organic_position IS NOT NULL OR
        sponsored_position IS NOT NULL
    )
);

CREATE INDEX measurements_monitor_time
    ON search_position.measurements (monitor_id, observed_at DESC);

CREATE INDEX measurements_observed_at
    ON search_position.measurements (observed_at DESC);

CREATE TABLE search_position.alerts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    monitor_id bigint NOT NULL
        REFERENCES search_position.monitors(id) ON DELETE RESTRICT,
    measurement_id bigint NOT NULL
        REFERENCES search_position.measurements(id) ON DELETE CASCADE,
    kind text NOT NULL
        CHECK (kind IN ('position_drop', 'not_found', 'blocked', 'collector_error')),
    previous_position integer CHECK (previous_position IS NULL OR previous_position > 0),
    current_position integer CHECK (current_position IS NULL OR current_position > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (measurement_id, kind)
);

CREATE INDEX alerts_created_at
    ON search_position.alerts (created_at DESC);

CREATE VIEW search_position.latest_measurements AS
SELECT DISTINCT ON (measurement.monitor_id)
    measurement.monitor_id,
    monitor.store_id,
    monitor.product_id,
    monitor.offer_id,
    monitor.search_phrase,
    monitor.region_code,
    monitor.region_name,
    measurement.observed_at,
    measurement.outcome,
    measurement.organic_position,
    measurement.sponsored_position,
    measurement.result_page,
    measurement.price,
    measurement.original_price,
    measurement.delivery_days,
    measurement.in_stock
FROM search_position.measurements AS measurement
JOIN search_position.monitors AS monitor ON monitor.id = measurement.monitor_id
ORDER BY measurement.monitor_id, measurement.observed_at DESC, measurement.id DESC;

CREATE VIEW search_position.hourly_position_summary AS
SELECT
    measurement.monitor_id,
    date_trunc('hour', measurement.observed_at) AS observed_hour,
    min(measurement.organic_position) AS best_organic_position,
    max(measurement.organic_position) AS worst_organic_position,
    avg(measurement.organic_position)::numeric(10, 2) AS average_organic_position,
    min(measurement.sponsored_position) AS best_sponsored_position,
    max(measurement.sponsored_position) AS worst_sponsored_position,
    avg(measurement.sponsored_position)::numeric(10, 2) AS average_sponsored_position,
    count(*) AS measurements
FROM search_position.measurements AS measurement
GROUP BY measurement.monitor_id, date_trunc('hour', measurement.observed_at);

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA search_position FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA search_position FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA search_position FROM PUBLIC;

COMMIT;
