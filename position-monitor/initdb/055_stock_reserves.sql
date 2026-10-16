BEGIN;

-- Additive only. Old observations remain unknown; reserves are never backfilled
-- from available stock or overwritten in append-only published history.
ALTER TABLE daily_reporting.stock_facts
    ADD COLUMN present_units integer CHECK (present_units >= 0),
    ADD COLUMN reserved_units integer CHECK (reserved_units >= 0),
    ADD CONSTRAINT stock_reserve_requires_gross CHECK (
        reserved_units IS NULL OR (present_units IS NOT NULL AND reserved_units <= present_units)
    );

CREATE OR REPLACE VIEW daily_reporting.published_stock_facts AS
SELECT snapshot.account_id, snapshot.marketplace, snapshot.cutoff_at,
       snapshot.source_as_of, snapshot.status AS snapshot_status,
       snapshot.pagination_complete, fact.*
FROM daily_reporting.stock_facts AS fact
JOIN daily_reporting.published_source_snapshots AS snapshot
    ON snapshot.id = fact.snapshot_id;

CREATE OR REPLACE VIEW daily_reporting.mcp_stock_facts
WITH (security_barrier = true) AS
SELECT fact.account_id, fact.marketplace, fact.cutoff_at, fact.source_as_of,
       fact.snapshot_status, fact.pagination_complete, fact.snapshot_id,
       fact.source, fact.sku, fact.warehouse_id, fact.sellable_units,
       fact.present_units, fact.reserved_units
FROM daily_reporting.published_stock_facts AS fact;

COMMIT;
