BEGIN;

-- Do not rewrite migration 032 or historical hashes. Preserve integer values
-- exactly while admitting WB values with up to 18 fractional digits. Reader
-- projections expose coefficients as strings so JSON consumers never round.
DROP VIEW daily_reporting.mcp_financial_ledger_rows;
ALTER TABLE daily_reporting.financial_ledger_amounts
    ALTER COLUMN units TYPE numeric(39,0) USING units::numeric,
    DROP CONSTRAINT financial_ledger_amounts_scale_check,
    ADD CONSTRAINT financial_ledger_amounts_scale_check CHECK (scale BETWEEN 0 AND 18),
    ADD CONSTRAINT financial_ledger_amounts_units_i128_check CHECK (units BETWEEN
        -170141183460469231731687303715884105728 AND 170141183460469231731687303715884105727);
ALTER TABLE daily_reporting.financial_ledger_batches
    ADD COLUMN payload_encoding text NOT NULL DEFAULT 'i64_number_v1'
        CHECK (payload_encoding IN ('i64_number_v1','i128_string_v2'));
ALTER TABLE daily_reporting.financial_ledger_batches
    ALTER COLUMN payload_encoding SET DEFAULT 'i128_string_v2';

CREATE OR REPLACE FUNCTION daily_reporting.publish_wb_financial_ledger(
    p_account text, p_from date, p_to date, p_payload text,
    p_sha256 text, p_terminal_status integer
) RETURNS TABLE (batch_id bigint, content_sha256 text, already_present boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    v_payload jsonb;
    v_batch bigint;
    v_existing_hash text;
    v_existing_encoding text;
    v_existing_rows jsonb;
    v_row jsonb;
    v_amount record;
    v_cursor bigint := 0;
    v_rrd bigint;
BEGIN
    IF p_account IS NULL OR p_account !~ '^[A-Za-z0-9_-]{1,128}$'
       OR p_from IS NULL OR p_to IS NULL OR p_from < DATE '2024-01-29'
       OR p_to < p_from OR p_to - p_from >= 31
       OR p_terminal_status IS DISTINCT FROM 204
       OR p_payload IS NULL OR octet_length(p_payload) > 33554432
       OR p_sha256 IS NULL
       OR p_sha256 IS DISTINCT FROM encode(sha256(convert_to(p_payload, 'UTF8')), 'hex') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid financial ledger batch';
    END IF;
    v_payload := p_payload::jsonb;
    IF jsonb_typeof(v_payload) IS DISTINCT FROM 'array'
       OR jsonb_array_length(v_payload) > 25000 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid financial ledger projection';
    END IF;
    -- Serialize overlapping publications for one cabinet. Source row identities
    -- across windows are unique as well: callers cannot silently double-count.
    PERFORM pg_advisory_xact_lock(hashtextextended('wb-finance:' || p_account, 0));
    SELECT b.id, b.content_sha256, b.payload_encoding INTO v_batch, v_existing_hash, v_existing_encoding
      FROM daily_reporting.financial_ledger_batches b
     WHERE b.account_id = p_account AND b.date_from = p_from AND b.date_to = p_to;
    IF FOUND THEN
        IF v_existing_hash <> p_sha256 THEN
            IF v_existing_encoding = 'i64_number_v1' THEN
                -- A serialization upgrade is not a financial correction. Compare
                -- every normalized value exactly; keep the original evidence hash.
                SELECT coalesce(jsonb_agg(jsonb_build_object(
                    'rrd_id',r.rrd_id,'report_id',r.report_id,
                    'business_date',r.business_date,'sku',r.sku,'currency',r.currency,
                    'document_type',r.document_type,'operation_type',r.operation_type,
                    'quantity',r.quantity,'amounts',(SELECT jsonb_object_agg(a.field,
                        jsonb_build_object('units',a.units::text,'scale',a.scale))
                        FROM daily_reporting.financial_ledger_amounts a
                        WHERE a.batch_id=r.batch_id AND a.rrd_id=r.rrd_id))
                    ORDER BY r.rrd_id),'[]'::jsonb) INTO v_existing_rows
                  FROM daily_reporting.financial_ledger_rows r WHERE r.batch_id=v_batch;
                IF v_existing_rows = v_payload THEN
                    RETURN QUERY SELECT v_batch,v_existing_hash,true;
                    RETURN;
                END IF;
            END IF;
            RAISE EXCEPTION USING ERRCODE = '23505', MESSAGE = 'financial report revision conflict';
        END IF;
        RETURN QUERY SELECT v_batch, v_existing_hash, true;
        RETURN;
    END IF;
    INSERT INTO daily_reporting.financial_ledger_batches
        (account_id, date_from, date_to, row_count, content_sha256, terminal_http_status)
    VALUES (p_account, p_from, p_to, jsonb_array_length(v_payload), p_sha256, 204)
    RETURNING id INTO v_batch;
    FOR v_row IN SELECT value FROM jsonb_array_elements(v_payload) LOOP
        IF jsonb_typeof(v_row) IS DISTINCT FROM 'object'
           OR v_row - ARRAY['rrd_id', 'report_id', 'business_date', 'sku', 'currency',
               'document_type', 'operation_type', 'quantity', 'amounts'] <> '{}'::jsonb
           OR NOT v_row ?& ARRAY['rrd_id', 'report_id', 'business_date', 'sku', 'currency',
               'document_type', 'operation_type', 'quantity', 'amounts']
           OR jsonb_typeof(v_row->'rrd_id') IS DISTINCT FROM 'number'
           OR jsonb_typeof(v_row->'report_id') IS DISTINCT FROM 'number'
           OR jsonb_typeof(v_row->'business_date') IS DISTINCT FROM 'string'
           OR jsonb_typeof(v_row->'currency') IS DISTINCT FROM 'string'
           OR jsonb_typeof(v_row->'amounts') IS DISTINCT FROM 'object'
           OR v_row->'amounts' = '{}'::jsonb
           OR jsonb_typeof(v_row->'sku') NOT IN ('number', 'null')
           OR jsonb_typeof(v_row->'quantity') NOT IN ('number', 'null')
           OR jsonb_typeof(v_row->'document_type') NOT IN ('string', 'null')
           OR jsonb_typeof(v_row->'operation_type') NOT IN ('string', 'null')
           OR (v_row->>'business_date') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
           OR (v_row->>'business_date')::date NOT BETWEEN p_from AND p_to THEN
            RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid financial ledger row';
        END IF;
        v_rrd := (v_row->>'rrd_id')::bigint;
        IF v_rrd <= v_cursor THEN
            RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid financial ledger cursor';
        END IF;
        v_cursor := v_rrd;
        INSERT INTO daily_reporting.financial_ledger_rows
            (batch_id, account_id, report_id, rrd_id, business_date, sku, currency,
             document_type, operation_type, quantity)
        VALUES (v_batch, p_account, (v_row->>'report_id')::bigint, v_rrd,
            (v_row->>'business_date')::date, (v_row->>'sku')::bigint,
            v_row->>'currency', v_row->>'document_type', v_row->>'operation_type',
            (v_row->>'quantity')::bigint);
        FOR v_amount IN SELECT key, value FROM jsonb_each(v_row->'amounts') LOOP
            IF jsonb_typeof(v_amount.value) IS DISTINCT FROM 'object'
               OR v_amount.value - ARRAY['units', 'scale'] <> '{}'::jsonb
               OR NOT v_amount.value ?& ARRAY['units', 'scale']
               OR jsonb_typeof(v_amount.value->'units') IS DISTINCT FROM 'string'
               OR v_amount.value->>'units' !~ '^(0|-?[1-9][0-9]{0,38})$'
               OR jsonb_typeof(v_amount.value->'scale') IS DISTINCT FROM 'number' THEN
                RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid financial ledger amount';
            END IF;
            INSERT INTO daily_reporting.financial_ledger_amounts
                (batch_id, rrd_id, field, units, scale)
            VALUES (v_batch, v_rrd, v_amount.key,
                (v_amount.value->>'units')::numeric, (v_amount.value->>'scale')::integer);
        END LOOP;
    END LOOP;
    RETURN QUERY SELECT v_batch, p_sha256, false;
END;
$$;

CREATE VIEW daily_reporting.mcp_financial_ledger_rows
WITH (security_barrier = true) AS
SELECT r.batch_id, r.account_id, b.marketplace, b.source, b.date_from, b.date_to,
       r.report_id, r.rrd_id, r.business_date, r.sku, r.currency,
       r.document_type, r.operation_type, r.quantity,
       a.field, a.units::text AS units, a.scale
  FROM daily_reporting.financial_ledger_rows r
  JOIN daily_reporting.financial_ledger_batches b ON b.id = r.batch_id
  JOIN daily_reporting.financial_ledger_amounts a
    ON a.batch_id = r.batch_id AND a.rrd_id = r.rrd_id;

COMMENT ON VIEW daily_reporting.mcp_financial_ledger_rows IS
    'Projected WB financial columns; missing is unknown, overlapping columns must not be summed as profit. Application RBAC must bind account_id.';

REVOKE ALL ON daily_reporting.financial_ledger_batches,
    daily_reporting.financial_ledger_rows,daily_reporting.financial_ledger_amounts
    FROM PUBLIC,position_reader,report_worker,report_collector;
REVOKE ALL ON FUNCTION daily_reporting.publish_wb_financial_ledger(text,date,date,text,text,integer)
    FROM PUBLIC,position_reader,report_worker;
GRANT EXECUTE ON FUNCTION daily_reporting.publish_wb_financial_ledger(text,date,date,text,text,integer)
    TO report_collector;
GRANT SELECT ON daily_reporting.mcp_financial_ledger_rows
    TO position_reader,report_worker,report_collector;
COMMIT;
