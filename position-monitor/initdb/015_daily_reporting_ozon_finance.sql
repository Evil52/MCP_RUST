BEGIN;

ALTER TABLE daily_reporting.source_snapshots
    DROP CONSTRAINT source_snapshots_source_check,
    DROP CONSTRAINT source_snapshots_period_window_check;

ALTER TABLE daily_reporting.source_snapshots
    ADD CONSTRAINT source_snapshots_source_check
        CHECK (source IN ('sales', 'advertising', 'finance', 'stocks', 'prices')),
    ADD CONSTRAINT source_snapshots_marketplace_source_check
        CHECK (source <> 'finance' OR marketplace = 'ozon'),
    ADD CONSTRAINT source_snapshots_period_window_check
        CHECK (
            (source IN ('sales', 'advertising', 'finance')
                AND period_start < period_end
                AND period_end <= cutoff_at)
            OR
            (source IN ('stocks', 'prices')
                AND period_start = period_end
                AND period_end = source_as_of)
        );

CREATE TABLE daily_reporting.finance_facts (
    snapshot_id bigint NOT NULL,
    source text NOT NULL DEFAULT 'finance' CHECK (source = 'finance'),
    business_date date NOT NULL,
    sku bigint CHECK (sku IS NULL OR sku > 0),
    sku_key bigint GENERATED ALWAYS AS (coalesce(sku, 0)) STORED,
    category text NOT NULL CHECK (category IN (
        'sale', 'commission', 'acquiring', 'logistics', 'storage',
        'paid_acceptance', 'compensation', 'marketplace_discount',
        'advertising', 'other'
    )),
    amount_minor bigint NOT NULL,
    line_count integer NOT NULL CHECK (line_count > 0),
    unknown_type_count integer NOT NULL CHECK (
        unknown_type_count >= 0 AND unknown_type_count <= line_count
    ),
    PRIMARY KEY (snapshot_id, business_date, sku_key, category),
    FOREIGN KEY (snapshot_id, source)
        REFERENCES daily_reporting.source_snapshots (id, source) ON DELETE RESTRICT
);

CREATE TRIGGER finance_facts_require_running_snapshot
    BEFORE INSERT ON daily_reporting.finance_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.require_running_fact_snapshot();
CREATE TRIGGER finance_facts_append_only
    BEFORE UPDATE OR DELETE ON daily_reporting.finance_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_fact_mutation();

-- Collection claims predate the Ozon-only finance source. Keep Wildberries
-- at four sources while requiring all five Ozon sources atomically.
CREATE OR REPLACE FUNCTION daily_reporting.complete_report_collection_claim(
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
          SELECT count(*) = CASE claim.marketplace WHEN 'ozon' THEN 5 ELSE 4 END
             AND count(DISTINCT snapshot.source) =
                 CASE claim.marketplace WHEN 'ozon' THEN 5 ELSE 4 END
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

CREATE OR REPLACE FUNCTION daily_reporting.enforce_source_snapshot_state()
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
    CASE OLD.source
        WHEN 'sales' THEN
            SELECT count(*) INTO actual_rows FROM daily_reporting.sales_facts
            WHERE snapshot_id = OLD.id;
        WHEN 'advertising' THEN
            SELECT count(*) INTO actual_rows FROM daily_reporting.advertising_facts
            WHERE snapshot_id = OLD.id;
        WHEN 'finance' THEN
            SELECT count(*) INTO actual_rows FROM daily_reporting.finance_facts
            WHERE snapshot_id = OLD.id;
        WHEN 'stocks' THEN
            SELECT count(*) INTO actual_rows FROM daily_reporting.stock_facts
            WHERE snapshot_id = OLD.id;
        WHEN 'prices' THEN
            SELECT count(*) INTO actual_rows FROM daily_reporting.price_facts
            WHERE snapshot_id = OLD.id;
        ELSE
            RAISE EXCEPTION USING
                ERRCODE = 'integrity_constraint_violation',
                MESSAGE = 'unknown source snapshot';
    END CASE;
    IF NEW.row_count <> actual_rows THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'source snapshot row_count does not match persisted facts';
    END IF;
    RETURN NEW;
END;
$$;

CREATE VIEW daily_reporting.published_finance_facts AS
SELECT snapshot.account_id, snapshot.marketplace, snapshot.cutoff_at,
       snapshot.source_as_of, snapshot.status AS snapshot_status,
       snapshot.pagination_complete, fact.*
FROM daily_reporting.finance_facts AS fact
JOIN daily_reporting.published_source_snapshots AS snapshot
    ON snapshot.id = fact.snapshot_id;

REVOKE ALL ON daily_reporting.finance_facts,
    daily_reporting.published_finance_facts FROM PUBLIC;
GRANT INSERT ON daily_reporting.finance_facts TO report_collector;
GRANT SELECT ON daily_reporting.published_finance_facts TO report_worker;

COMMIT;
