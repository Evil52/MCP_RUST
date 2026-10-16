\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE report_collector;
INSERT INTO daily_reporting.source_snapshots
    (account_id,marketplace,source,cutoff_at,source_as_of,period_start,period_end,collector_version,claim_id,claim_generation)
SELECT 'cluster-schema-test','ozon','cluster_stocks','2099-08-16 03:00:00+00','2099-08-16 03:20:00+00','2099-08-16 03:20:00+00','2099-08-16 03:20:00+00','test',claim_id,claim_generation
FROM daily_reporting.claim_report_collection('cluster-schema-test','ozon','2099-08-16 03:00:00+00','test');
INSERT INTO daily_reporting.cluster_stock_facts(snapshot_id,sku,warehouse_id,cluster_id,macrolocal_cluster_id,data)
SELECT id,11,99,154,4039,'{"source_row":true,"data_classification":"untrusted_external_marketplace_data","available_stock_count":0,"return_from_customer_stock_count":2}' FROM daily_reporting.source_snapshots WHERE account_id='cluster-schema-test';
INSERT INTO daily_reporting.cluster_stock_facts(snapshot_id,sku,warehouse_id,cluster_id,macrolocal_cluster_id,data)
SELECT id,11,99,2,4007,'{"source_row":true,"data_classification":"untrusted_external_marketplace_data","available_stock_count":3}' FROM daily_reporting.source_snapshots WHERE account_id='cluster-schema-test';
INSERT INTO daily_reporting.cluster_stock_facts(snapshot_id,sku,data)
SELECT id,12,'{"source_row":false,"data_classification":"untrusted_external_marketplace_data"}' FROM daily_reporting.source_snapshots WHERE account_id='cluster-schema-test';
DO $$
BEGIN
    BEGIN
        INSERT INTO daily_reporting.cluster_stock_facts(snapshot_id,sku,warehouse_id,cluster_id,macrolocal_cluster_id,data)
        SELECT id,11,99,2,4007,'{"source_row":true,"data_classification":"untrusted_external_marketplace_data","available_stock_count":3}' FROM daily_reporting.source_snapshots WHERE account_id='cluster-schema-test';
        RAISE EXCEPTION 'exact cluster duplicate accepted';
    EXCEPTION WHEN unique_violation THEN NULL;
    END;
    BEGIN
        INSERT INTO daily_reporting.cluster_stock_facts(snapshot_id,sku,data)
        SELECT id,13,'{}' FROM daily_reporting.source_snapshots WHERE account_id='cluster-schema-test';
        RAISE EXCEPTION 'missing provenance was accepted';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
    BEGIN
        UPDATE daily_reporting.source_snapshots SET status='succeeded',pagination_complete=true,row_count=3,payload_sha256=repeat('a',64),finished_at='2099-08-16 03:21:00+00' WHERE account_id='cluster-schema-test';
        RAISE EXCEPTION 'missing warehouse was published complete';
    EXCEPTION WHEN integrity_constraint_violation OR raise_exception THEN
        IF SQLERRM='missing warehouse was published complete' THEN RAISE; END IF;
    END;
END $$;
UPDATE daily_reporting.source_snapshots SET status='partial',pagination_complete=true,row_count=3,payload_sha256=repeat('a',64),finished_at='2099-08-16 03:21:00+00' WHERE account_id='cluster-schema-test';
RESET ROLE;
SET LOCAL ROLE position_reader;
DO $$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM daily_reporting.mcp_cluster_stock_facts WHERE account_id='cluster-schema-test' AND sku=11 AND data->>'available_stock_count'='0') THEN RAISE EXCEPTION 'explicit zero lost'; END IF;
    IF (SELECT count(*) FROM daily_reporting.mcp_cluster_stock_facts WHERE account_id='cluster-schema-test' AND sku=11 AND warehouse_id=99) <> 2 THEN RAISE EXCEPTION 'cluster coordinates were merged'; END IF;
    IF NOT EXISTS(SELECT 1 FROM daily_reporting.mcp_cluster_stock_facts WHERE account_id='cluster-schema-test' AND sku=12 AND warehouse_id IS NULL AND NOT data ? 'available_stock_count') THEN RAISE EXCEPTION 'unknown changed to zero'; END IF;
    IF has_table_privilege(current_user,'daily_reporting.cluster_stock_facts','INSERT,UPDATE,DELETE') OR has_table_privilege(current_user,'daily_reporting.posting_movement_facts','INSERT,UPDATE,DELETE') THEN RAISE EXCEPTION 'reader gained writes'; END IF;
END $$;
ROLLBACK;
