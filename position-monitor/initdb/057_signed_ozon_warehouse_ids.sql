BEGIN;

-- Ozon analytics returns signed int64 warehouse identifiers, including
-- negative identifiers with real cluster stock. Zero remains the reserved
-- key for an unknown SKU/warehouse pair, represented by SQL NULL.
ALTER TABLE daily_reporting.cluster_stock_facts
    DROP CONSTRAINT cluster_stock_facts_warehouse_id_check,
    ADD CONSTRAINT cluster_stock_facts_warehouse_id_check CHECK(warehouse_id <> 0);

COMMIT;
