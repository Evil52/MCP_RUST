BEGIN;

-- Ozon may reuse a warehouse identifier in different cluster coordinates.
-- Keep each returned SKU/warehouse/cluster/macrolocal row independently.
ALTER TABLE daily_reporting.cluster_stock_facts
    ADD COLUMN cluster_key bigint GENERATED ALWAYS AS (coalesce(cluster_id,0)) STORED,
    ADD COLUMN macrolocal_cluster_key bigint GENERATED ALWAYS AS (coalesce(macrolocal_cluster_id,0)) STORED,
    DROP CONSTRAINT cluster_stock_facts_pkey,
    ADD PRIMARY KEY(snapshot_id,sku,warehouse_key,cluster_key,macrolocal_cluster_key);

COMMIT;
