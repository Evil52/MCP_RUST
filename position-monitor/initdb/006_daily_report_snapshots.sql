BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'report_collector') THEN
        RAISE EXCEPTION 'report_collector role must be created before the snapshot migration';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'report_worker') THEN
        RAISE EXCEPTION 'report_worker role must be created before the snapshot migration';
    END IF;
END;
$$;

CREATE TABLE daily_reporting.source_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id varchar(128) NOT NULL,
    marketplace text NOT NULL,
    source text NOT NULL,
    cutoff_at timestamptz NOT NULL,
    source_as_of timestamptz NOT NULL,
    period_start timestamptz NOT NULL,
    period_end timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'running',
    pagination_complete boolean NOT NULL DEFAULT false,
    row_count integer NOT NULL DEFAULT 0,
    payload_sha256 char(64),
    collector_version varchar(64) NOT NULL,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    error_class varchar(64),
    http_status smallint,
    UNIQUE (account_id, marketplace, source, cutoff_at),
    UNIQUE (id, source),
    CHECK (account_id ~ '^[A-Za-z0-9_-]+$'),
    CHECK (marketplace IN ('ozon', 'wildberries')),
    CHECK (source IN ('sales', 'advertising', 'stocks', 'prices')),
    CHECK (status IN ('running', 'succeeded', 'partial', 'failed')),
    CHECK (row_count BETWEEN 0 AND 1000000),
    CHECK (collector_version ~ '^[A-Za-z0-9._-]{1,64}$'),
    CHECK (source_as_of <= cutoff_at),
    CHECK (started_at <= cutoff_at + interval '30 minutes'),
    CHECK (
        (source IN ('sales', 'advertising') AND period_start < period_end
            AND period_end <= cutoff_at)
        OR
        (source IN ('stocks', 'prices') AND period_start = period_end
            AND period_end = source_as_of)
    ),
    CHECK (
        (status = 'running' AND finished_at IS NULL AND payload_sha256 IS NULL
            AND error_class IS NULL AND http_status IS NULL)
        OR
        (status IN ('succeeded', 'partial') AND finished_at IS NOT NULL
            AND payload_sha256 ~ '^[0-9A-Fa-f]{64}$' AND error_class IS NULL
            AND http_status IS NULL)
        OR
        (status = 'failed' AND finished_at IS NOT NULL AND error_class IS NOT NULL)
    ),
    CHECK (status <> 'succeeded' OR pagination_complete),
    CHECK (finished_at IS NULL OR finished_at >= started_at),
    CHECK (error_class IS NULL OR error_class ~ '^[a-z][a-z0-9_]{0,63}$'),
    CHECK (http_status IS NULL OR http_status BETWEEN 400 AND 599)
);

CREATE TABLE daily_reporting.sales_facts (
    snapshot_id bigint NOT NULL,
    source text NOT NULL DEFAULT 'sales' CHECK (source = 'sales'),
    business_date date NOT NULL,
    sku bigint NOT NULL CHECK (sku > 0),
    ordered_units integer NOT NULL CHECK (ordered_units >= 0),
    operational_gmv_minor bigint NOT NULL CHECK (operational_gmv_minor >= 0),
    cancelled_units integer NOT NULL DEFAULT 0 CHECK (cancelled_units >= 0),
    returned_units integer NOT NULL DEFAULT 0 CHECK (returned_units >= 0),
    currency char(3) NOT NULL DEFAULT 'RUB' CHECK (currency = 'RUB'),
    PRIMARY KEY (snapshot_id, business_date, sku),
    FOREIGN KEY (snapshot_id, source)
        REFERENCES daily_reporting.source_snapshots (id, source) ON DELETE RESTRICT
);

CREATE TABLE daily_reporting.advertising_facts (
    snapshot_id bigint NOT NULL,
    source text NOT NULL DEFAULT 'advertising' CHECK (source = 'advertising'),
    business_date date NOT NULL,
    campaign_id bigint NOT NULL CHECK (campaign_id > 0),
    sku bigint NOT NULL DEFAULT 0 CHECK (sku >= 0),
    impressions bigint NOT NULL CHECK (impressions >= 0),
    clicks bigint NOT NULL CHECK (clicks >= 0 AND clicks <= impressions),
    spend_minor bigint NOT NULL CHECK (spend_minor >= 0),
    attributed_orders integer NOT NULL CHECK (attributed_orders >= 0),
    attributed_revenue_minor bigint NOT NULL CHECK (attributed_revenue_minor >= 0),
    currency char(3) NOT NULL DEFAULT 'RUB' CHECK (currency = 'RUB'),
    PRIMARY KEY (snapshot_id, business_date, campaign_id, sku),
    FOREIGN KEY (snapshot_id, source)
        REFERENCES daily_reporting.source_snapshots (id, source) ON DELETE RESTRICT
);

CREATE TABLE daily_reporting.stock_facts (
    snapshot_id bigint NOT NULL,
    source text NOT NULL DEFAULT 'stocks' CHECK (source = 'stocks'),
    sku bigint NOT NULL CHECK (sku > 0),
    warehouse_id varchar(128) NOT NULL,
    sellable_units integer NOT NULL CHECK (sellable_units >= 0),
    PRIMARY KEY (snapshot_id, sku, warehouse_id),
    FOREIGN KEY (snapshot_id, source)
        REFERENCES daily_reporting.source_snapshots (id, source) ON DELETE RESTRICT,
    CHECK (warehouse_id ~ '^[A-Za-z0-9._:-]{1,128}$')
);

CREATE TABLE daily_reporting.price_facts (
    snapshot_id bigint NOT NULL,
    source text NOT NULL DEFAULT 'prices' CHECK (source = 'prices'),
    sku bigint NOT NULL CHECK (sku > 0),
    price_minor bigint NOT NULL CHECK (price_minor >= 0),
    old_price_minor bigint CHECK (old_price_minor IS NULL OR old_price_minor >= price_minor),
    currency char(3) NOT NULL DEFAULT 'RUB' CHECK (currency = 'RUB'),
    PRIMARY KEY (snapshot_id, sku),
    FOREIGN KEY (snapshot_id, source)
        REFERENCES daily_reporting.source_snapshots (id, source) ON DELETE RESTRICT
);

CREATE FUNCTION daily_reporting.require_running_snapshot_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NEW.status <> 'running' THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'source snapshot must start in running state';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER source_snapshots_start_running
    BEFORE INSERT ON daily_reporting.source_snapshots
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.require_running_snapshot_insert();

CREATE FUNCTION daily_reporting.require_running_fact_snapshot()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    snapshot_status text;
BEGIN
    SELECT status INTO snapshot_status
    FROM daily_reporting.source_snapshots
    WHERE id = NEW.snapshot_id AND source = NEW.source
    FOR KEY SHARE;
    IF snapshot_status IS DISTINCT FROM 'running' THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'facts may be appended only to a running matching snapshot';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER sales_facts_require_running_snapshot
    BEFORE INSERT ON daily_reporting.sales_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.require_running_fact_snapshot();
CREATE TRIGGER advertising_facts_require_running_snapshot
    BEFORE INSERT ON daily_reporting.advertising_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.require_running_fact_snapshot();
CREATE TRIGGER stock_facts_require_running_snapshot
    BEFORE INSERT ON daily_reporting.stock_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.require_running_fact_snapshot();
CREATE TRIGGER price_facts_require_running_snapshot
    BEFORE INSERT ON daily_reporting.price_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.require_running_fact_snapshot();

CREATE FUNCTION daily_reporting.reject_fact_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = 'integrity_constraint_violation',
        MESSAGE = 'daily reporting facts are append-only';
END;
$$;

CREATE TRIGGER sales_facts_append_only BEFORE UPDATE OR DELETE ON daily_reporting.sales_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_fact_mutation();
CREATE TRIGGER advertising_facts_append_only BEFORE UPDATE OR DELETE ON daily_reporting.advertising_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_fact_mutation();
CREATE TRIGGER stock_facts_append_only BEFORE UPDATE OR DELETE ON daily_reporting.stock_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_fact_mutation();
CREATE TRIGGER price_facts_append_only BEFORE UPDATE OR DELETE ON daily_reporting.price_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_fact_mutation();

CREATE FUNCTION daily_reporting.enforce_source_snapshot_state()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    actual_rows bigint;
BEGIN
    IF OLD.status <> 'running' THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'terminal source snapshot is immutable';
    END IF;
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.account_id IS DISTINCT FROM OLD.account_id
        OR NEW.marketplace IS DISTINCT FROM OLD.marketplace
        OR NEW.source IS DISTINCT FROM OLD.source
        OR NEW.cutoff_at IS DISTINCT FROM OLD.cutoff_at
        OR NEW.source_as_of IS DISTINCT FROM OLD.source_as_of
        OR NEW.period_start IS DISTINCT FROM OLD.period_start
        OR NEW.period_end IS DISTINCT FROM OLD.period_end
        OR NEW.collector_version IS DISTINCT FROM OLD.collector_version
        OR NEW.started_at IS DISTINCT FROM OLD.started_at
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'source snapshot provenance is immutable';
    END IF;
    IF NEW.status NOT IN ('succeeded', 'partial', 'failed') THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'source snapshot must transition directly to a terminal state';
    END IF;
    IF OLD.source = 'sales' THEN
        SELECT count(*) INTO actual_rows FROM daily_reporting.sales_facts
        WHERE snapshot_id = OLD.id;
    ELSIF OLD.source = 'advertising' THEN
        SELECT count(*) INTO actual_rows FROM daily_reporting.advertising_facts
        WHERE snapshot_id = OLD.id;
    ELSIF OLD.source = 'stocks' THEN
        SELECT count(*) INTO actual_rows FROM daily_reporting.stock_facts
        WHERE snapshot_id = OLD.id;
    ELSE
        SELECT count(*) INTO actual_rows FROM daily_reporting.price_facts
        WHERE snapshot_id = OLD.id;
    END IF;
    IF NEW.row_count <> actual_rows THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'source snapshot row_count does not match persisted facts';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER source_snapshots_enforce_state
    BEFORE UPDATE ON daily_reporting.source_snapshots
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.enforce_source_snapshot_state();

CREATE VIEW daily_reporting.published_source_snapshots AS
SELECT id, account_id, marketplace, source, cutoff_at, source_as_of,
       period_start, period_end, status, pagination_complete, row_count,
       payload_sha256, collector_version, finished_at
FROM daily_reporting.source_snapshots
WHERE status IN ('succeeded', 'partial');

CREATE VIEW daily_reporting.published_sales_facts AS
SELECT snapshot.account_id, snapshot.marketplace, snapshot.cutoff_at,
       snapshot.source_as_of, snapshot.status AS snapshot_status,
       snapshot.pagination_complete, fact.*
FROM daily_reporting.sales_facts AS fact
JOIN daily_reporting.published_source_snapshots AS snapshot
    ON snapshot.id = fact.snapshot_id;

CREATE VIEW daily_reporting.published_advertising_facts AS
SELECT snapshot.account_id, snapshot.marketplace, snapshot.cutoff_at,
       snapshot.source_as_of, snapshot.status AS snapshot_status,
       snapshot.pagination_complete, fact.*
FROM daily_reporting.advertising_facts AS fact
JOIN daily_reporting.published_source_snapshots AS snapshot
    ON snapshot.id = fact.snapshot_id;

CREATE VIEW daily_reporting.published_stock_facts AS
SELECT snapshot.account_id, snapshot.marketplace, snapshot.cutoff_at,
       snapshot.source_as_of, snapshot.status AS snapshot_status,
       snapshot.pagination_complete, fact.*
FROM daily_reporting.stock_facts AS fact
JOIN daily_reporting.published_source_snapshots AS snapshot
    ON snapshot.id = fact.snapshot_id;

CREATE VIEW daily_reporting.published_price_facts AS
SELECT snapshot.account_id, snapshot.marketplace, snapshot.cutoff_at,
       snapshot.source_as_of, snapshot.status AS snapshot_status,
       snapshot.pagination_complete, fact.*
FROM daily_reporting.price_facts AS fact
JOIN daily_reporting.published_source_snapshots AS snapshot
    ON snapshot.id = fact.snapshot_id;

CREATE INDEX source_snapshots_published_idx
    ON daily_reporting.source_snapshots (account_id, marketplace, source, cutoff_at DESC)
    WHERE status IN ('succeeded', 'partial');

REVOKE ALL ON ALL TABLES IN SCHEMA daily_reporting FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA daily_reporting FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA daily_reporting FROM PUBLIC;

GRANT USAGE ON SCHEMA daily_reporting TO report_collector;
GRANT SELECT, INSERT ON daily_reporting.source_snapshots TO report_collector;
GRANT UPDATE (
    status, pagination_complete, row_count, payload_sha256, finished_at,
    error_class, http_status
) ON daily_reporting.source_snapshots TO report_collector;
GRANT INSERT ON daily_reporting.sales_facts,
    daily_reporting.advertising_facts,
    daily_reporting.stock_facts,
    daily_reporting.price_facts TO report_collector;
GRANT USAGE, SELECT ON SEQUENCE daily_reporting.source_snapshots_id_seq
    TO report_collector;

GRANT SELECT ON daily_reporting.published_source_snapshots,
    daily_reporting.published_sales_facts,
    daily_reporting.published_advertising_facts,
    daily_reporting.published_stock_facts,
    daily_reporting.published_price_facts TO report_worker;

ALTER DEFAULT PRIVILEGES IN SCHEMA daily_reporting
    REVOKE ALL ON TABLES FROM PUBLIC, report_collector, report_worker;
ALTER DEFAULT PRIVILEGES IN SCHEMA daily_reporting
    REVOKE ALL ON SEQUENCES FROM PUBLIC, report_collector, report_worker;
ALTER DEFAULT PRIVILEGES IN SCHEMA daily_reporting
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC, report_collector, report_worker;

COMMIT;
