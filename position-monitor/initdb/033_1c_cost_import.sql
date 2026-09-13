BEGIN;

-- Needed for a concurrency-safe account/SKU/date exclusion constraint.
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Dormant by default. A separate deployment provisions this role's credential;
-- it has no marketplace keys and receives no grants on marketplace tables.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'report_cost_importer') THEN
        CREATE ROLE report_cost_importer NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 2;
    END IF;
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO report_cost_importer', current_database());
END
$$;
ALTER ROLE report_cost_importer SET statement_timeout = '30s';
ALTER ROLE report_cost_importer SET idle_in_transaction_session_timeout = '30s';

CREATE TABLE daily_reporting.cost_import_batches (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version integer NOT NULL CHECK (version = 1),
    account_id text NOT NULL CHECK (account_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    marketplace text NOT NULL CHECK (marketplace IN ('ozon', 'wildberries')),
    source_id text NOT NULL CHECK (source_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    export_id text NOT NULL CHECK (export_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    sha256 text NOT NULL CHECK (sha256 ~ '^[a-f0-9]{64}$'),
    row_count integer NOT NULL CHECK (row_count BETWEEN 1 AND 10000),
    exported_at timestamptz NOT NULL CHECK (
        exported_at >= '2000-01-01T00:00:00Z' AND exported_at < '2201-01-01T00:00:00Z'
    ),
    imported_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    imported_by text NOT NULL CHECK (imported_by ~ '^[A-Za-z0-9_-]{1,128}$'),
    import_transaction xid8 NOT NULL DEFAULT pg_current_xact_id(),
    UNIQUE (account_id, marketplace, source_id, export_id),
    UNIQUE (id, account_id, marketplace)
);

CREATE TABLE daily_reporting.cost_import_entries (
    batch_id bigint NOT NULL,
    account_id text NOT NULL,
    marketplace text NOT NULL,
    source_row_id text NOT NULL CHECK (source_row_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    sku bigint NOT NULL CHECK (sku > 0),
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    currency text NOT NULL CHECK (currency = 'RUB'),
    allocation text NOT NULL CHECK (allocation = 'per_unit'),
    vat_treatment text NOT NULL CHECK (vat_treatment IN ('included', 'excluded', 'not_applicable')),
    vat_rate_bps integer,
    effective_from date NOT NULL CHECK (effective_from >= '2000-01-01' AND effective_from <= '2200-12-31'),
    effective_to date NOT NULL CHECK (effective_to >= effective_from AND effective_to <= '2200-12-31'),
    PRIMARY KEY (batch_id, source_row_id),
    FOREIGN KEY (batch_id, account_id, marketplace)
        REFERENCES daily_reporting.cost_import_batches (id, account_id, marketplace),
    CHECK (
        (vat_treatment = 'not_applicable' AND vat_rate_bps IS NULL)
        OR (vat_treatment IN ('included', 'excluded') AND vat_rate_bps IS NOT NULL
            AND vat_rate_bps BETWEEN 0 AND 10000)
    ),
    EXCLUDE USING gist (
        account_id WITH =,
        marketplace WITH =,
        sku WITH =,
        daterange(effective_from, effective_to, '[]') WITH &&
    )
);

CREATE FUNCTION daily_reporting.verify_cost_batch_count() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, daily_reporting AS $$
BEGIN
    IF (SELECT count(*) FROM daily_reporting.cost_import_entries WHERE batch_id = NEW.id)
        <> NEW.row_count THEN
        RAISE EXCEPTION 'cost batch row count mismatch' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END
$$;
CREATE CONSTRAINT TRIGGER cost_batch_count
AFTER INSERT ON daily_reporting.cost_import_batches DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION daily_reporting.verify_cost_batch_count();

CREATE FUNCTION daily_reporting.verify_cost_batch_open() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, daily_reporting AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM daily_reporting.cost_import_batches
        WHERE id = NEW.batch_id AND import_transaction = pg_current_xact_id()) THEN
        RAISE EXCEPTION 'cost batch is sealed' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER cost_batch_open BEFORE INSERT ON daily_reporting.cost_import_entries
FOR EACH ROW EXECUTE FUNCTION daily_reporting.verify_cost_batch_open();

REVOKE ALL ON daily_reporting.cost_import_batches, daily_reporting.cost_import_entries
    FROM PUBLIC, report_collector, report_worker, position_reader, report_refresh_requester;
REVOKE ALL ON FUNCTION daily_reporting.verify_cost_batch_count() FROM PUBLIC;
REVOKE ALL ON FUNCTION daily_reporting.verify_cost_batch_open() FROM PUBLIC;
GRANT USAGE ON SCHEMA daily_reporting TO report_cost_importer;
GRANT SELECT ON daily_reporting.cost_import_batches, daily_reporting.cost_import_entries
    TO report_cost_importer;
GRANT INSERT (version, account_id, marketplace, source_id, export_id, sha256, row_count, exported_at, imported_by)
    ON daily_reporting.cost_import_batches TO report_cost_importer;
GRANT INSERT ON daily_reporting.cost_import_entries TO report_cost_importer;
GRANT USAGE, SELECT ON SEQUENCE daily_reporting.cost_import_batches_id_seq TO report_cost_importer;
GRANT SELECT ON daily_reporting.cost_import_batches, daily_reporting.cost_import_entries TO report_worker;

COMMIT;
