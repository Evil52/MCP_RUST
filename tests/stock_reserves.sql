\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE report_collector;
INSERT INTO daily_reporting.source_snapshots
    (account_id, marketplace, source, cutoff_at, source_as_of,
     period_start, period_end, collector_version, claim_id, claim_generation)
SELECT 'stock-reserve-test','ozon','stocks','2099-08-16 03:00:00+00',
       '2099-08-16 03:20:00+00','2099-08-16 03:20:00+00',
       '2099-08-16 03:20:00+00','reserve-test',claim_id,claim_generation
FROM daily_reporting.claim_report_collection(
    'stock-reserve-test','ozon','2099-08-16 03:00:00+00','reserve-test'
);
INSERT INTO daily_reporting.stock_facts
    (snapshot_id,sku,warehouse_id,sellable_units,present_units,reserved_units)
SELECT id,11,'fbs:99',5,7,2 FROM daily_reporting.source_snapshots
WHERE account_id='stock-reserve-test';
INSERT INTO daily_reporting.stock_facts(snapshot_id,sku,warehouse_id,sellable_units)
SELECT id,12,'sku-fulfillment-v2:fbs',0 FROM daily_reporting.source_snapshots
WHERE account_id='stock-reserve-test';
DO $$
BEGIN
    BEGIN
        INSERT INTO daily_reporting.stock_facts
            (snapshot_id,sku,warehouse_id,sellable_units,present_units,reserved_units)
        SELECT id,13,'fbs:99',0,7,8 FROM daily_reporting.source_snapshots
        WHERE account_id='stock-reserve-test';
        RAISE EXCEPTION 'reserve above gross was accepted';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
    BEGIN
        INSERT INTO daily_reporting.stock_facts
            (snapshot_id,sku,warehouse_id,sellable_units,reserved_units)
        SELECT id,14,'fbs:99',0,0 FROM daily_reporting.source_snapshots
        WHERE account_id='stock-reserve-test';
        RAISE EXCEPTION 'reserve without gross was accepted';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
END $$;
UPDATE daily_reporting.source_snapshots
SET status='succeeded',pagination_complete=true,row_count=2,
    payload_sha256=repeat('a',64),finished_at='2099-08-16 03:21:00+00'
WHERE account_id='stock-reserve-test';
RESET ROLE;
SET LOCAL ROLE position_reader;
DO $$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM daily_reporting.mcp_stock_facts
        WHERE account_id='stock-reserve-test' AND sku=11
          AND sellable_units=5 AND present_units=7 AND reserved_units=2) THEN
        RAISE EXCEPTION 'published reserve lost';
    END IF;
    IF NOT EXISTS(SELECT 1 FROM daily_reporting.mcp_stock_facts
        WHERE account_id='stock-reserve-test' AND sku=12
          AND present_units IS NULL AND reserved_units IS NULL) THEN
        RAISE EXCEPTION 'legacy unknown reserve changed to zero';
    END IF;
    IF has_table_privilege(current_user,'daily_reporting.stock_facts','INSERT,UPDATE,DELETE') THEN
        RAISE EXCEPTION 'reader gained write privileges';
    END IF;
END $$;
ROLLBACK;
