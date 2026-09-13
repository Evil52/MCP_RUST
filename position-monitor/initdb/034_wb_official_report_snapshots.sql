BEGIN;

-- Separate from date-slice ledger 032: rrDate corrections may lie outside the
-- report period. Identity is the complete official report, not an rrDate window.
CREATE TABLE daily_reporting.wb_official_reports (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id varchar(128) NOT NULL CHECK (account_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    report_id bigint NOT NULL CHECK (report_id > 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    period text NOT NULL CHECK (period IN ('daily', 'weekly')),
    date_from date NOT NULL CHECK (date_from >= DATE '2025-01-01'),
    date_to date NOT NULL,
    created_date date NOT NULL CHECK (created_date >= date_to),
    report_type integer NOT NULL CHECK (report_type >= 0),
    row_count integer NOT NULL CHECK (row_count BETWEEN 0 AND 25000),
    actor_id varchar(128) NOT NULL CHECK (actor_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[a-f0-9]{64}$'),
    summary_source_sha256 text NOT NULL CHECK (summary_source_sha256 ~ '^[a-f0-9]{64}$'),
    details_source_sha256 text NOT NULL CHECK (details_source_sha256 ~ '^[a-f0-9]{64}$'),
    summary_observation_id varchar(128) NOT NULL,
    details_observation_id varchar(128) NOT NULL,
    terminal_http_status integer NOT NULL DEFAULT 204 CHECK (terminal_http_status = 204),
    mapping_version text NOT NULL DEFAULT 'wb_weekly_primary_totals_v1'
        CHECK (mapping_version = 'wb_weekly_primary_totals_v1'),
    comparison_status text NOT NULL CHECK
        (comparison_status IN ('primary_totals_match', 'mismatch', 'unavailable')),
    unavailable_reason text CHECK (unavailable_reason IN
        ('unsupported_period', 'empty_details', 'missing_summary_amount',
         'missing_detail_amount', 'unsupported_document_type')),
    published_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (account_id, report_id),
    CHECK (date_to >= date_from AND date_to - date_from < 31),
    CHECK (summary_source_sha256 <> details_source_sha256),
    CHECK (summary_observation_id <> details_observation_id),
    CHECK ((comparison_status = 'unavailable') = (unavailable_reason IS NOT NULL))
);

CREATE TABLE daily_reporting.wb_official_report_rows (
    snapshot_id bigint NOT NULL REFERENCES daily_reporting.wb_official_reports(id),
    rrd_id bigint NOT NULL CHECK (rrd_id > 0),
    business_date date NOT NULL,
    sku bigint CHECK (sku > 0),
    document_type varchar(512) CHECK (document_type !~ '[[:cntrl:]]'),
    operation_type varchar(512) CHECK (operation_type !~ '[[:cntrl:]]'),
    quantity bigint,
    PRIMARY KEY (snapshot_id, rrd_id)
);

CREATE TABLE daily_reporting.wb_official_report_amounts (
    snapshot_id bigint NOT NULL,
    rrd_id bigint NOT NULL,
    field text NOT NULL CHECK (field IN (
        'retailPrice', 'retailAmount', 'retailPriceWithDisc', 'ppvzSalesCommission',
        'forPay', 'ppvzReward', 'acquiringFee', 'vw', 'vwNds', 'deliveryService',
        'penalty', 'additionalPayment', 'rebillLogisticCost', 'paidStorage',
        'deduction', 'paidAcceptance', 'installmentCofinancingAmount',
        'cashbackAmount', 'cashbackDiscount', 'cashbackCommissionChange', 'paymentSchedule')),
    units numeric(39,0) NOT NULL CHECK (units BETWEEN -170141183460469231731687303715884105728 AND 170141183460469231731687303715884105727),
    scale integer NOT NULL CHECK (scale BETWEEN 0 AND 18),
    PRIMARY KEY (snapshot_id, rrd_id, field),
    FOREIGN KEY (snapshot_id, rrd_id)
        REFERENCES daily_reporting.wb_official_report_rows(snapshot_id, rrd_id)
);

CREATE TABLE daily_reporting.wb_official_report_summary_amounts (
    snapshot_id bigint NOT NULL REFERENCES daily_reporting.wb_official_reports(id),
    field text NOT NULL CHECK (field IN (
        'retailAmountSum', 'forPaySum', 'deliveryServiceSum', 'paidStorageSum',
        'paidAcceptanceSum', 'deductionSum', 'penaltySum', 'additionalPaymentSum',
        'cashbackAmountSum', 'cashbackDiscountSum', 'cashbackCommissionChangeSum',
        'paymentSchedule', 'bankPaymentSum')),
    units numeric(39,0) NOT NULL CHECK (units BETWEEN -170141183460469231731687303715884105728 AND 170141183460469231731687303715884105727),
    scale integer NOT NULL CHECK (scale BETWEEN 0 AND 18),
    PRIMARY KEY (snapshot_id, field)
);

CREATE TABLE daily_reporting.wb_official_report_comparisons (
    snapshot_id bigint NOT NULL REFERENCES daily_reporting.wb_official_reports(id),
    detail_column text NOT NULL CHECK (detail_column IN ('retailAmount', 'forPay')),
    summary_column text NOT NULL,
    detail_units numeric(39,0) CHECK (detail_units BETWEEN -170141183460469231731687303715884105728 AND 170141183460469231731687303715884105727),
    summary_units numeric(39,0) CHECK (summary_units BETWEEN -170141183460469231731687303715884105728 AND 170141183460469231731687303715884105727),
    difference_units numeric(39,0) CHECK (difference_units BETWEEN -170141183460469231731687303715884105728 AND 170141183460469231731687303715884105727),
    scale integer NOT NULL DEFAULT 18 CHECK (scale = 18),
    status text NOT NULL CHECK (status IN ('primary_totals_match', 'mismatch', 'unavailable')),
    unavailable_reason text CHECK (unavailable_reason IN
        ('missing_summary_amount', 'missing_detail_amount', 'unsupported_document_type')),
    PRIMARY KEY (snapshot_id, detail_column),
    CHECK ((detail_column = 'retailAmount' AND summary_column = 'retailAmountSum')
        OR (detail_column = 'forPay' AND summary_column = 'forPaySum')),
    CHECK ((status = 'unavailable') = (unavailable_reason IS NOT NULL)),
    CHECK ((difference_units IS NULL) = (detail_units IS NULL OR summary_units IS NULL)),
    CHECK (difference_units IS NULL OR difference_units = detail_units - summary_units),
    CHECK ((status = 'primary_totals_match') = (difference_units IS NOT NULL AND difference_units = 0)),
    CHECK (status <> 'mismatch' OR difference_units <> 0)
);

-- JSON is accepted only as a bounded transport envelope and never stored.
-- Source strings retain the bytes hashed by the collector; all persisted values
-- pass strict projections. SQL computes the comparison itself, so a caller
-- cannot submit a fabricated successful reconciliation flag.
CREATE FUNCTION daily_reporting.publish_wb_official_report(
    p_actor text, p_payload text, p_sha256 text
) RETURNS TABLE (snapshot_id bigint, content_sha256 text, already_present boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
AS $$
DECLARE
    v_payload jsonb;
    v_scope jsonb;
    v_summary jsonb;
    v_rows jsonb;
    v_evidence jsonb;
    v_scope_text text;
    v_summary_text text;
    v_rows_text text;
    v_source text;
    v_expected_hash text;
    v_snapshot bigint;
    v_existing_hash text;
    v_account text;
    v_report bigint;
    v_currency text;
    v_period text;
    v_from date;
    v_to date;
    v_row jsonb;
    v_amount record;
    v_rrd bigint;
    v_cursor bigint := 0;
    v_detail_field text;
    v_summary_field text;
    v_actual numeric;
    v_expected numeric;
    v_difference numeric;
    v_reason text;
    v_status text;
    v_overall_reason text;
    v_overall_status text := 'primary_totals_match';
BEGIN
    IF p_actor IS NULL OR p_actor !~ '^[A-Za-z0-9_-]{1,128}$'
       OR p_payload IS NULL OR octet_length(p_payload) > 33554432
       OR p_sha256 IS NULL
       OR p_sha256 IS DISTINCT FROM encode(sha256(convert_to(p_payload, 'UTF8')), 'hex') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid official report envelope';
    END IF;
    v_payload := p_payload::jsonb;
    IF jsonb_typeof(v_payload) IS DISTINCT FROM 'object'
       OR v_payload - ARRAY['scope_json','summary_json','rows_json','summary_evidence','details_evidence'] <> '{}'::jsonb
       OR NOT v_payload ?& ARRAY['scope_json','summary_json','rows_json','summary_evidence','details_evidence']
       OR jsonb_typeof(v_payload->'scope_json') IS DISTINCT FROM 'string'
       OR jsonb_typeof(v_payload->'summary_json') IS DISTINCT FROM 'string'
       OR jsonb_typeof(v_payload->'rows_json') IS DISTINCT FROM 'string' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid official report envelope projection';
    END IF;
    v_scope_text := v_payload->>'scope_json';
    v_summary_text := v_payload->>'summary_json';
    v_rows_text := v_payload->>'rows_json';
    v_scope := v_scope_text::jsonb;
    v_summary := v_summary_text::jsonb;
    v_rows := v_rows_text::jsonb;
    IF jsonb_typeof(v_scope) IS DISTINCT FROM 'object'
       OR v_scope - ARRAY['account_id','report_id','currency','period','date_from','date_to'] <> '{}'::jsonb
       OR NOT v_scope ?& ARRAY['account_id','report_id','currency','period','date_from','date_to']
       OR jsonb_typeof(v_scope->'account_id') IS DISTINCT FROM 'string'
       OR jsonb_typeof(v_scope->'report_id') IS DISTINCT FROM 'number'
       OR jsonb_typeof(v_scope->'currency') IS DISTINCT FROM 'string'
       OR jsonb_typeof(v_scope->'period') IS DISTINCT FROM 'string'
       OR jsonb_typeof(v_scope->'date_from') IS DISTINCT FROM 'string'
       OR jsonb_typeof(v_scope->'date_to') IS DISTINCT FROM 'string'
       OR v_scope->>'date_from' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
       OR v_scope->>'date_to' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
       OR jsonb_typeof(v_summary) IS DISTINCT FROM 'object'
       OR v_summary - ARRAY['scope','created_date','report_type','amounts'] <> '{}'::jsonb
       OR NOT v_summary ?& ARRAY['scope','created_date','report_type','amounts']
       OR v_summary->'scope' IS DISTINCT FROM v_scope
       OR jsonb_typeof(v_summary->'created_date') IS DISTINCT FROM 'string'
       OR v_summary->>'created_date' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
       OR jsonb_typeof(v_summary->'report_type') IS DISTINCT FROM 'number'
       OR jsonb_typeof(v_summary->'amounts') IS DISTINCT FROM 'object'
       OR v_summary->'amounts' = '{}'::jsonb
       OR jsonb_typeof(v_rows) IS DISTINCT FROM 'array'
       OR jsonb_array_length(v_rows) > 25000 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid official report scope';
    END IF;
    v_account := v_scope->>'account_id';
    v_report := (v_scope->>'report_id')::bigint;
    v_currency := v_scope->>'currency';
    v_period := v_scope->>'period';
    v_from := (v_scope->>'date_from')::date;
    v_to := (v_scope->>'date_to')::date;
    IF v_to >= (clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date
       OR (v_summary->>'created_date')::date > (clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'official report is not closed';
    END IF;
    FOREACH v_source IN ARRAY ARRAY['wb_report_summary_v1', 'wb_report_id_details_v1'] LOOP
        IF v_source = 'wb_report_summary_v1' THEN
            v_evidence := v_payload->'summary_evidence';
            v_expected_hash := encode(sha256(convert_to(
                '["' || v_source || '",' || v_scope_text || ',' || v_summary_text || ']', 'UTF8')), 'hex');
        ELSE
            v_evidence := v_payload->'details_evidence';
            v_expected_hash := encode(sha256(convert_to(
                '["' || v_source || '",' || v_scope_text || ',' || v_rows_text || ']', 'UTF8')), 'hex');
        END IF;
        IF jsonb_typeof(v_evidence) IS DISTINCT FROM 'object'
           OR v_evidence - ARRAY['scope','observation_id','source_sha256','terminal_observed','covers_entire_report'] <> '{}'::jsonb
           OR NOT v_evidence ?& ARRAY['scope','observation_id','source_sha256','terminal_observed','covers_entire_report']
           OR v_evidence->'scope' IS DISTINCT FROM v_scope
           OR v_evidence->'terminal_observed' IS DISTINCT FROM 'true'::jsonb
           OR v_evidence->'covers_entire_report' IS DISTINCT FROM 'true'::jsonb
           OR jsonb_typeof(v_evidence->'source_sha256') IS DISTINCT FROM 'string'
           OR jsonb_typeof(v_evidence->'observation_id') IS DISTINCT FROM 'string'
           OR v_evidence->>'source_sha256' IS DISTINCT FROM v_expected_hash
           OR v_evidence->>'observation_id' IS DISTINCT FROM v_source || '_' || v_expected_hash THEN
            RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid official report source evidence';
        END IF;
    END LOOP;
    PERFORM pg_advisory_xact_lock(hashtextextended('wb-official-report:' || v_account || ':' || v_report, 0));
    SELECT r.id, r.content_sha256 INTO v_snapshot, v_existing_hash
      FROM daily_reporting.wb_official_reports r
     WHERE r.account_id = v_account AND r.report_id = v_report;
    IF FOUND THEN
        IF v_existing_hash <> p_sha256 THEN
            RAISE EXCEPTION USING ERRCODE = '23505', MESSAGE = 'official report revision conflict';
        END IF;
        RETURN QUERY SELECT v_snapshot, p_sha256, true;
        RETURN;
    END IF;
    -- The transaction remains invisible until all projected rows and the
    -- independently calculated comparison are inserted successfully.
    INSERT INTO daily_reporting.wb_official_reports
        (account_id, report_id, currency, period, date_from, date_to, created_date,
         report_type, row_count, actor_id, content_sha256, summary_source_sha256,
         details_source_sha256, summary_observation_id, details_observation_id,
         comparison_status, unavailable_reason)
    VALUES (v_account, v_report, v_currency, v_period, v_from, v_to,
        (v_summary->>'created_date')::date, (v_summary->>'report_type')::integer,
        jsonb_array_length(v_rows), p_actor, p_sha256,
        v_payload->'summary_evidence'->>'source_sha256',
        v_payload->'details_evidence'->>'source_sha256',
        v_payload->'summary_evidence'->>'observation_id',
        v_payload->'details_evidence'->>'observation_id', 'primary_totals_match', NULL)
    RETURNING id INTO v_snapshot;
    FOR v_amount IN SELECT key, value FROM jsonb_each(v_summary->'amounts') LOOP
        IF jsonb_typeof(v_amount.value) IS DISTINCT FROM 'object'
           OR v_amount.value - ARRAY['units','scale'] <> '{}'::jsonb
           OR NOT v_amount.value ?& ARRAY['units','scale']
           OR jsonb_typeof(v_amount.value->'units') IS DISTINCT FROM 'string'
           OR v_amount.value->>'units' !~ '^(0|-?[1-9][0-9]{0,38})$'
           OR jsonb_typeof(v_amount.value->'scale') IS DISTINCT FROM 'number' THEN
            RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid official summary amount';
        END IF;
        INSERT INTO daily_reporting.wb_official_report_summary_amounts(snapshot_id, field, units, scale)
        VALUES (v_snapshot, v_amount.key, (v_amount.value->>'units')::numeric,
            (v_amount.value->>'scale')::integer);
    END LOOP;
    FOR v_row IN SELECT value FROM jsonb_array_elements(v_rows) LOOP
        IF jsonb_typeof(v_row) IS DISTINCT FROM 'object'
           OR v_row - ARRAY['rrd_id','report_id','business_date','sku','currency','document_type','operation_type','quantity','amounts'] <> '{}'::jsonb
           OR NOT v_row ?& ARRAY['rrd_id','report_id','business_date','sku','currency','document_type','operation_type','quantity','amounts']
           OR jsonb_typeof(v_row->'rrd_id') IS DISTINCT FROM 'number'
           OR jsonb_typeof(v_row->'report_id') IS DISTINCT FROM 'number'
           OR (v_row->>'report_id')::bigint IS DISTINCT FROM v_report
           OR jsonb_typeof(v_row->'currency') IS DISTINCT FROM 'string'
           OR v_row->>'currency' IS DISTINCT FROM v_currency
           OR jsonb_typeof(v_row->'business_date') IS DISTINCT FROM 'string'
           OR v_row->>'business_date' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
           OR jsonb_typeof(v_row->'sku') NOT IN ('number','null')
           OR jsonb_typeof(v_row->'quantity') NOT IN ('number','null')
           OR jsonb_typeof(v_row->'document_type') NOT IN ('string','null')
           OR jsonb_typeof(v_row->'operation_type') NOT IN ('string','null')
           OR jsonb_typeof(v_row->'amounts') IS DISTINCT FROM 'object'
           OR v_row->'amounts' = '{}'::jsonb THEN
            RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid official report detail projection';
        END IF;
        v_rrd := (v_row->>'rrd_id')::bigint;
        IF v_rrd <= v_cursor THEN
            RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid official report cursor';
        END IF;
        v_cursor := v_rrd;
        INSERT INTO daily_reporting.wb_official_report_rows
            (snapshot_id,rrd_id,business_date,sku,document_type,operation_type,quantity)
        VALUES (v_snapshot,v_rrd,(v_row->>'business_date')::date,(v_row->>'sku')::bigint,
            v_row->>'document_type',v_row->>'operation_type',(v_row->>'quantity')::bigint);
        FOR v_amount IN SELECT key, value FROM jsonb_each(v_row->'amounts') LOOP
            IF jsonb_typeof(v_amount.value) IS DISTINCT FROM 'object'
               OR v_amount.value - ARRAY['units','scale'] <> '{}'::jsonb
               OR NOT v_amount.value ?& ARRAY['units','scale']
               OR jsonb_typeof(v_amount.value->'units') IS DISTINCT FROM 'string'
           OR v_amount.value->>'units' !~ '^(0|-?[1-9][0-9]{0,38})$'
               OR jsonb_typeof(v_amount.value->'scale') IS DISTINCT FROM 'number' THEN
                RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid official report detail amount';
            END IF;
            INSERT INTO daily_reporting.wb_official_report_amounts(snapshot_id,rrd_id,field,units,scale)
            VALUES (v_snapshot,v_rrd,v_amount.key,(v_amount.value->>'units')::numeric,
                (v_amount.value->>'scale')::integer);
        END LOOP;
    END LOOP;
    IF v_period <> 'weekly' THEN
        v_overall_status := 'unavailable';
        v_overall_reason := 'unsupported_period';
    ELSIF jsonb_array_length(v_rows) = 0 THEN
        v_overall_status := 'unavailable';
        v_overall_reason := 'empty_details';
    ELSE
        FOREACH v_detail_field IN ARRAY ARRAY['retailAmount','forPay'] LOOP
            v_summary_field := v_detail_field || 'Sum';
            v_reason := NULL;
            v_actual := NULL;
            v_expected := NULL;
            v_difference := NULL;
            SELECT CASE WHEN a.units IS NULL THEN 'missing_detail_amount'
                        ELSE 'unsupported_document_type' END INTO v_reason
              FROM daily_reporting.wb_official_report_rows r
              LEFT JOIN daily_reporting.wb_official_report_amounts a
                ON a.snapshot_id = r.snapshot_id AND a.rrd_id = r.rrd_id AND a.field = v_detail_field
             WHERE r.snapshot_id = v_snapshot
               AND (a.units IS NULL OR (a.units <> 0 AND coalesce(r.document_type,'') NOT IN ('Продажа','Возврат')))
             ORDER BY r.rrd_id LIMIT 1;
            IF v_reason IS NULL THEN
                SELECT coalesce(sum(CASE r.document_type
                    WHEN 'Продажа' THEN a.units::numeric * power(10::numeric,18-a.scale)
                    WHEN 'Возврат' THEN -a.units::numeric * power(10::numeric,18-a.scale)
                    ELSE 0 END),0) INTO v_actual
                  FROM daily_reporting.wb_official_report_rows r
                  JOIN daily_reporting.wb_official_report_amounts a
                    ON a.snapshot_id=r.snapshot_id AND a.rrd_id=r.rrd_id AND a.field=v_detail_field
                 WHERE r.snapshot_id=v_snapshot;
            END IF;
            SELECT a.units::numeric * power(10::numeric,18-a.scale) INTO v_expected
              FROM daily_reporting.wb_official_report_summary_amounts a
             WHERE a.snapshot_id=v_snapshot AND a.field=v_summary_field;
            IF v_expected IS NULL AND v_reason IS NULL THEN
                v_reason := 'missing_summary_amount';
            END IF;
            IF v_reason IS NOT NULL THEN
                v_status := 'unavailable';
                v_overall_status := 'unavailable';
                v_overall_reason := coalesce(v_overall_reason,v_reason);
            ELSE
                v_difference := v_actual-v_expected;
                v_status := CASE WHEN v_difference=0 THEN 'primary_totals_match' ELSE 'mismatch' END;
                IF v_status='mismatch' AND v_overall_status<>'unavailable' THEN
                    v_overall_status := 'mismatch';
                END IF;
            END IF;
            INSERT INTO daily_reporting.wb_official_report_comparisons
                (snapshot_id,detail_column,summary_column,detail_units,summary_units,
                 difference_units,status,unavailable_reason)
            VALUES (v_snapshot,v_detail_field,v_summary_field,v_actual,v_expected,
                v_difference,v_status,v_reason);
        END LOOP;
    END IF;
    UPDATE daily_reporting.wb_official_reports
       SET comparison_status=v_overall_status, unavailable_reason=v_overall_reason
     WHERE id=v_snapshot;
    RETURN QUERY SELECT v_snapshot,p_sha256,false;
END;
$$;

CREATE VIEW daily_reporting.mcp_wb_official_reports WITH (security_barrier=true) AS
SELECT id AS snapshot_id, account_id, report_id, currency, period, date_from, date_to,
       created_date, report_type, row_count, actor_id, content_sha256,
       summary_source_sha256, details_source_sha256, summary_observation_id,
       details_observation_id, terminal_http_status, mapping_version,
       comparison_status, unavailable_reason, published_at
  FROM daily_reporting.wb_official_reports;
CREATE VIEW daily_reporting.mcp_wb_official_report_rows WITH (security_barrier=true) AS
SELECT b.id AS snapshot_id,b.account_id,b.report_id,b.currency,r.rrd_id,r.business_date,
       r.sku,r.document_type,r.operation_type,r.quantity,a.field,a.units::text AS units,a.scale
  FROM daily_reporting.wb_official_reports b
  JOIN daily_reporting.wb_official_report_rows r ON r.snapshot_id=b.id
  JOIN daily_reporting.wb_official_report_amounts a ON a.snapshot_id=r.snapshot_id AND a.rrd_id=r.rrd_id;
CREATE VIEW daily_reporting.mcp_wb_official_report_summary_amounts WITH (security_barrier=true) AS
SELECT b.id AS snapshot_id,b.account_id,b.report_id,a.field,a.units::text AS units,a.scale
  FROM daily_reporting.wb_official_reports b
  JOIN daily_reporting.wb_official_report_summary_amounts a ON a.snapshot_id=b.id;
CREATE VIEW daily_reporting.mcp_wb_official_report_comparisons WITH (security_barrier=true) AS
SELECT b.id AS snapshot_id,b.account_id,b.report_id,c.detail_column,c.summary_column,
       c.detail_units::text AS detail_units,c.summary_units::text AS summary_units,
       c.difference_units::text AS difference_units,c.scale,c.status,c.unavailable_reason
  FROM daily_reporting.wb_official_reports b
  JOIN daily_reporting.wb_official_report_comparisons c ON c.snapshot_id=b.id;

COMMENT ON VIEW daily_reporting.mcp_wb_official_reports IS
    'Immutable whole-report evidence. primary_totals_match covers only retailAmountSum and forPaySum, not bank payment or profit. Application RBAC must bind account_id on every read.';
REVOKE ALL ON daily_reporting.wb_official_reports,daily_reporting.wb_official_report_rows,
    daily_reporting.wb_official_report_amounts,daily_reporting.wb_official_report_summary_amounts,
    daily_reporting.wb_official_report_comparisons FROM PUBLIC,position_reader,report_worker,report_collector;
REVOKE ALL ON FUNCTION daily_reporting.publish_wb_official_report(text,text,text)
    FROM PUBLIC,position_reader,report_worker;
GRANT EXECUTE ON FUNCTION daily_reporting.publish_wb_official_report(text,text,text) TO report_collector;
GRANT SELECT ON daily_reporting.mcp_wb_official_reports,daily_reporting.mcp_wb_official_report_rows,
    daily_reporting.mcp_wb_official_report_summary_amounts,daily_reporting.mcp_wb_official_report_comparisons
    TO position_reader,report_worker,report_collector;
COMMIT;
