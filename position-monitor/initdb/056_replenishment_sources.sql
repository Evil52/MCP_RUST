BEGIN;

ALTER TABLE daily_reporting.source_collection_jobs DROP CONSTRAINT source_collection_jobs_source_check;
ALTER TABLE daily_reporting.source_collection_jobs
    ADD CONSTRAINT source_collection_jobs_source_check CHECK(source IN ('sales','stocks','seller_stocks','prices','advertising','finance','cluster_stocks','posting_movement')),
    ADD CONSTRAINT replenishment_jobs_ozon_only CHECK(source NOT IN ('cluster_stocks','posting_movement') OR marketplace='ozon');
ALTER TABLE daily_reporting.source_snapshots DROP CONSTRAINT source_snapshots_source_check,
    DROP CONSTRAINT source_snapshots_period_window_check;
ALTER TABLE daily_reporting.source_snapshots
    ADD CONSTRAINT source_snapshots_source_check CHECK(source IN ('sales','stocks','seller_stocks','prices','advertising','finance','cluster_stocks','posting_movement')),
    ADD CONSTRAINT replenishment_snapshots_ozon_only CHECK(source NOT IN ('cluster_stocks','posting_movement') OR marketplace='ozon'),
    ADD CONSTRAINT source_snapshots_period_window_check CHECK(
        (source IN ('sales','advertising','finance','posting_movement') AND period_start<period_end AND period_end<=cutoff_at)
        OR (source IN ('stocks','seller_stocks','prices','cluster_stocks') AND period_start=period_end AND period_end=source_as_of));

CREATE TABLE daily_reporting.cluster_stock_facts (
    snapshot_id bigint NOT NULL,
    source text NOT NULL DEFAULT 'cluster_stocks' CHECK(source='cluster_stocks'),
    sku bigint NOT NULL CHECK(sku>0),
    warehouse_id bigint CHECK(warehouse_id>0),
    warehouse_key bigint GENERATED ALWAYS AS (coalesce(warehouse_id,0)) STORED,
    cluster_id bigint CHECK(cluster_id>0),
    macrolocal_cluster_id bigint CHECK(macrolocal_cluster_id>0),
    data jsonb NOT NULL CHECK(jsonb_typeof(data)='object' AND octet_length(data::text)<=8192),
    PRIMARY KEY(snapshot_id,sku,warehouse_key),
    FOREIGN KEY(snapshot_id,source) REFERENCES daily_reporting.source_snapshots(id,source) ON DELETE RESTRICT,
    CHECK((warehouse_id IS NULL)=(cluster_id IS NULL) AND (warehouse_id IS NULL)=(macrolocal_cluster_id IS NULL)),
    CHECK(data ?& ARRAY['source_row','data_classification']),
    CHECK(data->>'data_classification' IS NOT NULL AND data->>'data_classification'='untrusted_external_marketplace_data'),
    CHECK(jsonb_typeof(data->'source_row')='boolean' AND (data->>'source_row')::boolean=(warehouse_id IS NOT NULL)),
    CHECK(data - ARRAY['source_row','data_classification','available_stock_count','transit_stock_count','requested_stock_count','inbound_replenishment','valid_stock_count','outbound_pending_delivery','return_from_customer_stock_count','stock_not_being_sold','other_stock_count','stock_defect_stock_count','transit_defect_stock_count','days_without_sales','days_without_sales_cluster','excess_stock_count','expiring_stock_count','ads','ads_cluster','idc','idc_cluster','offer_id','cluster_name','warehouse_name','turnover_grade','turnover_grade_cluster'] = '{}'::jsonb)
);
ALTER TABLE daily_reporting.cluster_stock_facts
    ADD CHECK(NOT data ? 'available_stock_count' OR data->'available_stock_count'='null'::jsonb OR (jsonb_typeof(data->'available_stock_count')='number' AND (data->>'available_stock_count')::numeric>=0 AND (data->>'available_stock_count')::numeric<=2147483647 AND trunc((data->>'available_stock_count')::numeric)=(data->>'available_stock_count')::numeric)),
    ADD CHECK(NOT data ? 'transit_stock_count' OR data->'transit_stock_count'='null'::jsonb OR (jsonb_typeof(data->'transit_stock_count')='number' AND (data->>'transit_stock_count')::numeric>=0 AND (data->>'transit_stock_count')::numeric<=2147483647 AND trunc((data->>'transit_stock_count')::numeric)=(data->>'transit_stock_count')::numeric)),
    ADD CHECK(NOT data ? 'requested_stock_count' OR data->'requested_stock_count'='null'::jsonb OR (jsonb_typeof(data->'requested_stock_count')='number' AND (data->>'requested_stock_count')::numeric>=0 AND (data->>'requested_stock_count')::numeric<=2147483647 AND trunc((data->>'requested_stock_count')::numeric)=(data->>'requested_stock_count')::numeric)),
    ADD CHECK(NOT data ? 'inbound_replenishment' OR data->'inbound_replenishment'='null'::jsonb OR (jsonb_typeof(data->'inbound_replenishment')='number' AND (data->>'inbound_replenishment')::numeric>=0 AND (data->>'inbound_replenishment')::numeric<=2147483647 AND trunc((data->>'inbound_replenishment')::numeric)=(data->>'inbound_replenishment')::numeric)),
    ADD CHECK(NOT data ? 'valid_stock_count' OR data->'valid_stock_count'='null'::jsonb OR (jsonb_typeof(data->'valid_stock_count')='number' AND (data->>'valid_stock_count')::numeric>=0 AND (data->>'valid_stock_count')::numeric<=2147483647 AND trunc((data->>'valid_stock_count')::numeric)=(data->>'valid_stock_count')::numeric)),
    ADD CHECK(NOT data ? 'outbound_pending_delivery' OR data->'outbound_pending_delivery'='null'::jsonb OR (jsonb_typeof(data->'outbound_pending_delivery')='number' AND (data->>'outbound_pending_delivery')::numeric>=0 AND (data->>'outbound_pending_delivery')::numeric<=2147483647 AND trunc((data->>'outbound_pending_delivery')::numeric)=(data->>'outbound_pending_delivery')::numeric)),
    ADD CHECK(NOT data ? 'return_from_customer_stock_count' OR data->'return_from_customer_stock_count'='null'::jsonb OR (jsonb_typeof(data->'return_from_customer_stock_count')='number' AND (data->>'return_from_customer_stock_count')::numeric>=0 AND (data->>'return_from_customer_stock_count')::numeric<=2147483647 AND trunc((data->>'return_from_customer_stock_count')::numeric)=(data->>'return_from_customer_stock_count')::numeric)),
    ADD CHECK(NOT data ? 'stock_not_being_sold' OR data->'stock_not_being_sold'='null'::jsonb OR (jsonb_typeof(data->'stock_not_being_sold')='number' AND (data->>'stock_not_being_sold')::numeric>=0 AND (data->>'stock_not_being_sold')::numeric<=2147483647 AND trunc((data->>'stock_not_being_sold')::numeric)=(data->>'stock_not_being_sold')::numeric)),
    ADD CHECK(NOT data ? 'other_stock_count' OR data->'other_stock_count'='null'::jsonb OR (jsonb_typeof(data->'other_stock_count')='number' AND (data->>'other_stock_count')::numeric>=0 AND (data->>'other_stock_count')::numeric<=2147483647 AND trunc((data->>'other_stock_count')::numeric)=(data->>'other_stock_count')::numeric)),
    ADD CHECK(NOT data ? 'stock_defect_stock_count' OR data->'stock_defect_stock_count'='null'::jsonb OR (jsonb_typeof(data->'stock_defect_stock_count')='number' AND (data->>'stock_defect_stock_count')::numeric>=0 AND (data->>'stock_defect_stock_count')::numeric<=2147483647 AND trunc((data->>'stock_defect_stock_count')::numeric)=(data->>'stock_defect_stock_count')::numeric)),
    ADD CHECK(NOT data ? 'transit_defect_stock_count' OR data->'transit_defect_stock_count'='null'::jsonb OR (jsonb_typeof(data->'transit_defect_stock_count')='number' AND (data->>'transit_defect_stock_count')::numeric>=0 AND (data->>'transit_defect_stock_count')::numeric<=2147483647 AND trunc((data->>'transit_defect_stock_count')::numeric)=(data->>'transit_defect_stock_count')::numeric)),
    ADD CHECK(NOT data ? 'days_without_sales' OR data->'days_without_sales'='null'::jsonb OR (jsonb_typeof(data->'days_without_sales')='number' AND (data->>'days_without_sales')::numeric>=0 AND (data->>'days_without_sales')::numeric<=2147483647 AND trunc((data->>'days_without_sales')::numeric)=(data->>'days_without_sales')::numeric)),
    ADD CHECK(NOT data ? 'days_without_sales_cluster' OR data->'days_without_sales_cluster'='null'::jsonb OR (jsonb_typeof(data->'days_without_sales_cluster')='number' AND (data->>'days_without_sales_cluster')::numeric>=0 AND (data->>'days_without_sales_cluster')::numeric<=2147483647 AND trunc((data->>'days_without_sales_cluster')::numeric)=(data->>'days_without_sales_cluster')::numeric)),
    ADD CHECK(NOT data ? 'excess_stock_count' OR data->'excess_stock_count'='null'::jsonb OR (jsonb_typeof(data->'excess_stock_count')='number' AND (data->>'excess_stock_count')::numeric>=0 AND (data->>'excess_stock_count')::numeric<=2147483647 AND trunc((data->>'excess_stock_count')::numeric)=(data->>'excess_stock_count')::numeric)),
    ADD CHECK(NOT data ? 'expiring_stock_count' OR data->'expiring_stock_count'='null'::jsonb OR (jsonb_typeof(data->'expiring_stock_count')='number' AND (data->>'expiring_stock_count')::numeric>=0 AND (data->>'expiring_stock_count')::numeric<=2147483647 AND trunc((data->>'expiring_stock_count')::numeric)=(data->>'expiring_stock_count')::numeric)),
    ADD CHECK(NOT data ? 'ads' OR data->'ads'='null'::jsonb OR (jsonb_typeof(data->'ads')='number' AND (data->>'ads')::numeric>=0)),
    ADD CHECK(NOT data ? 'ads_cluster' OR data->'ads_cluster'='null'::jsonb OR (jsonb_typeof(data->'ads_cluster')='number' AND (data->>'ads_cluster')::numeric>=0)),
    ADD CHECK(NOT data ? 'idc' OR data->'idc'='null'::jsonb OR (jsonb_typeof(data->'idc')='number' AND (data->>'idc')::numeric>=0)),
    ADD CHECK(NOT data ? 'idc_cluster' OR data->'idc_cluster'='null'::jsonb OR (jsonb_typeof(data->'idc_cluster')='number' AND (data->>'idc_cluster')::numeric>=0));
CREATE TABLE daily_reporting.posting_movement_facts (
    snapshot_id bigint NOT NULL,
    source text NOT NULL DEFAULT 'posting_movement' CHECK(source='posting_movement'),
    sku bigint NOT NULL CHECK(sku>0),
    data jsonb NOT NULL CHECK(jsonb_typeof(data)='object' AND octet_length(data::text)<=8192),
    PRIMARY KEY(snapshot_id,sku),
    FOREIGN KEY(snapshot_id,source) REFERENCES daily_reporting.source_snapshots(id,source) ON DELETE RESTRICT,
    CHECK(data ?& ARRAY['sku','fbo_non_cancelled_units','fbs_non_cancelled_units','total_non_cancelled_units','fbo_cancelled_units','fbs_cancelled_units','total_cancelled_units','fbo_status_units','fbs_status_units']),
    CHECK(jsonb_typeof(data->'sku')='number' AND (data->>'sku')::numeric=sku),
    CHECK(jsonb_typeof(data->'fbo_status_units')='object' AND jsonb_typeof(data->'fbs_status_units')='object'),
    CHECK((data->>'fbo_non_cancelled_units')::numeric+(data->>'fbs_non_cancelled_units')::numeric=(data->>'total_non_cancelled_units')::numeric),
    CHECK((data->>'fbo_cancelled_units')::numeric+(data->>'fbs_cancelled_units')::numeric=(data->>'total_cancelled_units')::numeric),
    CHECK(data - ARRAY['sku','fbo_non_cancelled_units','fbs_non_cancelled_units','total_non_cancelled_units','fbo_cancelled_units','fbs_cancelled_units','total_cancelled_units','fbo_status_units','fbs_status_units'] = '{}'::jsonb)
);
ALTER TABLE daily_reporting.posting_movement_facts
    ADD CHECK(jsonb_typeof(data->'fbo_non_cancelled_units')='number' AND (data->>'fbo_non_cancelled_units')::numeric BETWEEN 0 AND 9223372036854775807 AND trunc((data->>'fbo_non_cancelled_units')::numeric)=(data->>'fbo_non_cancelled_units')::numeric),
    ADD CHECK(jsonb_typeof(data->'fbs_non_cancelled_units')='number' AND (data->>'fbs_non_cancelled_units')::numeric BETWEEN 0 AND 9223372036854775807 AND trunc((data->>'fbs_non_cancelled_units')::numeric)=(data->>'fbs_non_cancelled_units')::numeric),
    ADD CHECK(jsonb_typeof(data->'total_non_cancelled_units')='number' AND (data->>'total_non_cancelled_units')::numeric BETWEEN 0 AND 9223372036854775807 AND trunc((data->>'total_non_cancelled_units')::numeric)=(data->>'total_non_cancelled_units')::numeric),
    ADD CHECK(jsonb_typeof(data->'fbo_cancelled_units')='number' AND (data->>'fbo_cancelled_units')::numeric BETWEEN 0 AND 9223372036854775807 AND trunc((data->>'fbo_cancelled_units')::numeric)=(data->>'fbo_cancelled_units')::numeric),
    ADD CHECK(jsonb_typeof(data->'fbs_cancelled_units')='number' AND (data->>'fbs_cancelled_units')::numeric BETWEEN 0 AND 9223372036854775807 AND trunc((data->>'fbs_cancelled_units')::numeric)=(data->>'fbs_cancelled_units')::numeric),
    ADD CHECK(jsonb_typeof(data->'total_cancelled_units')='number' AND (data->>'total_cancelled_units')::numeric BETWEEN 0 AND 9223372036854775807 AND trunc((data->>'total_cancelled_units')::numeric)=(data->>'total_cancelled_units')::numeric);
CREATE TRIGGER cluster_stock_facts_require_running_snapshot BEFORE INSERT ON daily_reporting.cluster_stock_facts FOR EACH ROW EXECUTE FUNCTION daily_reporting.require_running_fact_snapshot();
CREATE TRIGGER posting_movement_facts_require_running_snapshot BEFORE INSERT ON daily_reporting.posting_movement_facts FOR EACH ROW EXECUTE FUNCTION daily_reporting.require_running_fact_snapshot();
CREATE TRIGGER cluster_stock_facts_append_only BEFORE UPDATE OR DELETE ON daily_reporting.cluster_stock_facts FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_fact_mutation();
CREATE TRIGGER posting_movement_facts_append_only BEFORE UPDATE OR DELETE ON daily_reporting.posting_movement_facts FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_fact_mutation();
REVOKE ALL ON daily_reporting.cluster_stock_facts,daily_reporting.posting_movement_facts FROM PUBLIC;
GRANT INSERT ON daily_reporting.cluster_stock_facts,daily_reporting.posting_movement_facts TO report_collector;

CREATE VIEW daily_reporting.mcp_cluster_stock_facts WITH(security_barrier=true) AS
SELECT s.account_id,s.marketplace,s.cutoff_at,s.source_as_of,s.status AS snapshot_status,s.pagination_complete,f.snapshot_id,f.source,f.sku,f.warehouse_id,f.warehouse_key,f.cluster_id,f.macrolocal_cluster_id,f.data
FROM daily_reporting.cluster_stock_facts f JOIN daily_reporting.published_source_snapshots s ON s.id=f.snapshot_id;
CREATE VIEW daily_reporting.mcp_posting_movement_facts WITH(security_barrier=true) AS
SELECT s.account_id,s.marketplace,s.cutoff_at,s.source_as_of,s.status AS snapshot_status,s.pagination_complete,f.snapshot_id,f.source,f.sku,f.data,
    'non_cancelled_posting_units'::text AS metric,
    'untrusted_external_marketplace_data'::text AS data_classification
FROM daily_reporting.posting_movement_facts f JOIN daily_reporting.published_source_snapshots s ON s.id=f.snapshot_id;
REVOKE ALL ON daily_reporting.mcp_cluster_stock_facts,daily_reporting.mcp_posting_movement_facts FROM PUBLIC;
GRANT SELECT ON daily_reporting.mcp_cluster_stock_facts,daily_reporting.mcp_posting_movement_facts TO position_reader;

-- The terminal transition below preserves every existing provenance and
-- append-only check, adding only the two new fact-count branches.

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
        WHEN 'seller_stocks' THEN
            SELECT count(*) INTO actual_rows FROM daily_reporting.seller_stock_facts
            WHERE snapshot_id = OLD.id;
            IF NEW.status = 'succeeded' AND EXISTS (SELECT 1 FROM daily_reporting.seller_stock_facts WHERE snapshot_id=OLD.id AND sellable_units IS NULL) THEN
                RAISE EXCEPTION USING ERRCODE='integrity_constraint_violation', MESSAGE='missing FBS rows cannot publish as complete';
            END IF;
        WHEN 'cluster_stocks' THEN
            SELECT count(*) INTO actual_rows FROM daily_reporting.cluster_stock_facts WHERE snapshot_id=OLD.id;
            IF NEW.status='succeeded' AND EXISTS(SELECT 1 FROM daily_reporting.cluster_stock_facts WHERE snapshot_id=OLD.id AND warehouse_id IS NULL) THEN
                RAISE EXCEPTION 'missing cluster rows cannot publish as complete';
            END IF;
        WHEN 'posting_movement' THEN
            SELECT count(*) INTO actual_rows FROM daily_reporting.posting_movement_facts WHERE snapshot_id=OLD.id;
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

-- Optional sources keep the existing lease and manifest semantics.

CREATE OR REPLACE FUNCTION daily_reporting.require_active_collection_claim()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE valid boolean;
BEGIN
    IF NEW.source_job_id IS NOT NULL AND NEW.claim_id IS NULL AND NEW.claim_generation IS NULL THEN
        SELECT true INTO valid FROM daily_reporting.source_collection_jobs j
        WHERE j.id=NEW.source_job_id AND j.generation=NEW.source_job_generation
          AND j.account_id=NEW.account_id AND j.marketplace=NEW.marketplace AND j.source=NEW.source
          AND j.cutoff_at=NEW.cutoff_at AND j.status='running' AND j.lease_until>clock_timestamp()
          AND j.deadline_at>clock_timestamp()
          AND (j.source NOT IN ('stocks','seller_stocks','prices','cluster_stocks') OR j.first_observed_at>=clock_timestamp()-interval '30 minutes')
        FOR KEY SHARE;
    ELSIF NEW.source_job_id IS NULL AND NEW.source_job_generation IS NULL THEN
        IF NEW.claim_id IS NULL OR NEW.claim_generation IS NULL THEN
            RAISE EXCEPTION USING ERRCODE='object_not_in_prerequisite_state', MESSAGE='new source snapshot requires an active collection claim';
        END IF;
        SELECT true INTO valid FROM daily_reporting.collection_claims j
        WHERE j.id=NEW.claim_id AND j.generation=NEW.claim_generation
          AND j.account_id=NEW.account_id AND j.marketplace=NEW.marketplace AND j.cutoff_at=NEW.cutoff_at
          AND j.status='active' AND j.lease_until>clock_timestamp() FOR KEY SHARE;
    END IF;
    IF valid IS DISTINCT FROM true THEN
        IF NEW.source_job_id IS NULL THEN
            RAISE EXCEPTION USING ERRCODE='object_not_in_prerequisite_state', MESSAGE='source snapshot collection claim is absent, stale, or expired';
        END IF;
        RAISE EXCEPTION USING ERRCODE='object_not_in_prerequisite_state', MESSAGE='source snapshot collection lease is absent, stale, or expired';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION daily_reporting.finish_source_collection(jid bigint, gen bigint, owner text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE done boolean;
BEGIN
    UPDATE daily_reporting.source_collection_jobs j SET status='published',lease_until=NULL,finished_at=clock_timestamp(),
        error_class=CASE WHEN EXISTS(SELECT 1 FROM daily_reporting.source_snapshots s WHERE s.source_job_id=j.id AND s.source_job_generation=j.generation AND s.status='partial') THEN CASE WHEN j.source='cluster_stocks' THEN 'cluster_missing_values' ELSE 'seller_missing_values' END ELSE NULL END,cache_bytes=0
    WHERE j.id=jid AND j.generation=gen AND j.owner_id=owner AND j.status='running' AND j.lease_until>clock_timestamp()
      AND EXISTS(SELECT 1 FROM daily_reporting.source_snapshots s WHERE s.source_job_id=j.id AND s.source_job_generation=j.generation AND s.pagination_complete
        AND (s.status='succeeded' OR (j.source IN ('seller_stocks','cluster_stocks') AND s.status='partial')))
    RETURNING true INTO done;
    IF COALESCE(done,false) THEN DELETE FROM daily_reporting.source_collection_pages WHERE job_id=jid; END IF;
    RETURN COALESCE(done,false);
END;
$$;

CREATE OR REPLACE FUNCTION daily_reporting.finish_marketplace_sales_refresh(
    requested_request_id bigint,
    requested_generation integer,
    requested_owner_id text,
    requested_snapshot_cutoff_at timestamptz,
    requested_marketplace text,
    requested_error_class text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    finished boolean;
BEGIN
    IF requested_request_id IS NULL
        OR requested_request_id <= 0
        OR requested_generation IS NULL
        OR requested_generation <= 0
        OR requested_owner_id IS NULL
        OR requested_owner_id !~ '^[A-Za-z0-9._:-]{1,64}$'
        OR (requested_marketplace IS NOT NULL
            AND requested_marketplace NOT IN ('ozon', 'wildberries'))
        OR (requested_error_class IS NULL
            AND requested_snapshot_cutoff_at IS NULL)
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'marketplace sales refresh finish input is invalid';
    END IF;
    IF requested_error_class IS NOT NULL
        AND requested_error_class !~ '^[a-z][a-z0-9_]{0,63}$'
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'marketplace sales refresh error class is invalid';
    END IF;

    IF requested_error_class IS NOT NULL THEN
        UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
        SET status = 'failed',
            finished_at = clock_timestamp(),
            error_class = requested_error_class
        WHERE refresh.id = requested_request_id
          AND refresh.generation = requested_generation
          AND refresh.owner_id = requested_owner_id
          AND refresh.status = 'running'
          AND (requested_marketplace IS NULL
               OR refresh.marketplace = requested_marketplace)
        RETURNING true INTO finished;
        RETURN COALESCE(finished, false);
    END IF;

    UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
    SET status = 'succeeded',
        finished_at = clock_timestamp()
    WHERE refresh.id = requested_request_id
      AND refresh.generation = requested_generation
      AND refresh.owner_id = requested_owner_id
      AND refresh.snapshot_cutoff_at = requested_snapshot_cutoff_at
      AND refresh.status = 'running'
      AND refresh.lease_until > clock_timestamp()
      AND (requested_marketplace IS NULL
           OR refresh.marketplace = requested_marketplace)
      AND (
          SELECT count(*) = CASE refresh.marketplace
                     WHEN 'ozon' THEN 5
                     WHEN 'wildberries' THEN 4
                     ELSE 0
                 END
             AND count(DISTINCT snapshot.source) = CASE refresh.marketplace
                     WHEN 'ozon' THEN 5
                     WHEN 'wildberries' THEN 4
                     ELSE 0
                 END
             AND bool_and(snapshot.status = 'succeeded')
             AND bool_and(snapshot.pagination_complete)
             AND bool_and(
                 (refresh.marketplace = 'ozon'
                     AND snapshot.source IN ('sales', 'advertising', 'finance', 'stocks', 'prices'))
                 OR
                 (refresh.marketplace = 'wildberries'
                     AND snapshot.source IN ('sales', 'advertising', 'stocks', 'prices'))
             )
          FROM daily_reporting.source_snapshots AS snapshot
          WHERE snapshot.account_id = refresh.account_id
            AND snapshot.marketplace = refresh.marketplace
            AND snapshot.cutoff_at = refresh.snapshot_cutoff_at
            AND snapshot.source NOT IN ('seller_stocks','cluster_stocks','posting_movement')
      )
    RETURNING true INTO finished;
    RETURN COALESCE(finished, false);
END;
$$;

CREATE OR REPLACE FUNCTION daily_reporting.dispatch_source_refreshes(scope jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r daily_reporting.ozon_sales_refresh_requests; t timestamptz:=clock_timestamp(); src text; complete boolean;
BEGIN
    IF scope IS NULL OR jsonb_typeof(scope)<>'array' OR jsonb_array_length(scope) NOT BETWEEN 1 AND 64 THEN
        RAISE EXCEPTION 'invalid source refresh scope';
    END IF;
    PERFORM pg_advisory_xact_lock(917244);
    FOR r IN SELECT refresh.* FROM daily_reporting.ozon_sales_refresh_requests refresh
        JOIN jsonb_to_recordset(scope) allowed(account_id text,marketplace text)
            ON allowed.account_id=refresh.account_id AND allowed.marketplace=refresh.marketplace
        WHERE refresh.status='queued' ORDER BY refresh.id FOR UPDATE OF refresh SKIP LOCKED
    LOOP
        IF r.requested_at<t-interval '4 hours' OR r.business_date<>(t AT TIME ZONE 'Asia/Yekaterinburg')::date THEN
            UPDATE daily_reporting.ozon_sales_refresh_requests SET status='failed',error_class='queue_expired',finished_at=t WHERE id=r.id;
            CONTINUE;
        END IF;
        IF NOT r.source_jobs_dispatched THEN
            r.snapshot_cutoff_at:=COALESCE(r.snapshot_cutoff_at,t);
            UPDATE daily_reporting.ozon_sales_refresh_requests
                SET source_jobs_dispatched=true,snapshot_cutoff_at=r.snapshot_cutoff_at WHERE id=r.id;
            FOREACH src IN ARRAY CASE r.marketplace WHEN 'ozon'
                THEN ARRAY['sales','stocks','prices','advertising','finance']
                ELSE ARRAY['sales','stocks','prices','advertising','seller_stocks'] END
            LOOP
                PERFORM daily_reporting.enqueue_source_collection(r.account_id,r.marketplace,src,r.snapshot_cutoff_at,
                    r.business_date::timestamp AT TIME ZONE 'Asia/Yekaterinburg',r.snapshot_cutoff_at);
            END LOOP;
        END IF;
        IF EXISTS (SELECT 1 FROM daily_reporting.source_collection_jobs j WHERE j.account_id=r.account_id
            AND j.marketplace=r.marketplace AND j.cutoff_at=r.snapshot_cutoff_at AND j.status='failed' AND j.source NOT IN ('seller_stocks','cluster_stocks','posting_movement')) THEN
            UPDATE daily_reporting.ozon_sales_refresh_requests SET status='failed',error_class='source_collection_failed',finished_at=t WHERE id=r.id;
        ELSIF (SELECT count(*) FROM daily_reporting.source_snapshots s WHERE s.account_id=r.account_id
            AND s.marketplace=r.marketplace AND s.cutoff_at=r.snapshot_cutoff_at AND s.status='succeeded' AND s.pagination_complete AND s.source NOT IN ('seller_stocks','cluster_stocks','posting_movement'))
            = (CASE r.marketplace WHEN 'ozon' THEN 5 ELSE 4 END) THEN
            UPDATE daily_reporting.ozon_sales_refresh_requests SET status='running',generation=generation+1,attempt_count=GREATEST(attempt_count,1),
                owner_id='source-jobs',started_at=t,lease_until=t+interval '15 minutes' WHERE id=r.id RETURNING * INTO r;
            complete:=daily_reporting.finish_marketplace_sales_refresh(r.id,r.generation,'source-jobs',r.snapshot_cutoff_at,r.marketplace,NULL);
            IF NOT complete THEN
                UPDATE daily_reporting.ozon_sales_refresh_requests SET status='failed',error_class='invalid_source_manifest',finished_at=t WHERE id=r.id;
            END IF;
        END IF;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION daily_reporting.claim_source_collection(scope jsonb, owner text)
RETURNS SETOF daily_reporting.source_collection_jobs
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE t timestamptz := clock_timestamp(); chosen bigint; purge_id bigint;
BEGIN
    IF owner IS NULL OR owner !~ '^[A-Za-z0-9._:-]{1,64}$' OR jsonb_typeof(scope) <> 'array'
       OR jsonb_array_length(scope) NOT BETWEEN 1 AND 64 THEN
        RAISE EXCEPTION 'invalid source collection scope';
    END IF;
    PERFORM pg_advisory_xact_lock(917244, 29);
    t := clock_timestamp();
    UPDATE daily_reporting.source_collection_jobs
    SET status='failed', error_class='collection_expired', finished_at=t, lease_until=NULL
    WHERE status IN ('ready','running') AND (deadline_at<=t OR
        (source IN ('stocks','seller_stocks','prices','cluster_stocks') AND first_observed_at < t - interval '30 minutes'));
    -- At most four jobs (128 MiB) per quantum. Ineligible old pages cannot be
    -- resumed even while awaiting this opportunistic cleanup.
    FOR purge_id IN
        SELECT j.id FROM daily_reporting.source_collection_jobs j
        WHERE (j.status='published' OR (j.status='failed' AND
            (j.source NOT IN ('stocks','seller_stocks') OR j.finished_at<=t-interval '24 hours')))
          AND EXISTS (SELECT 1 FROM daily_reporting.source_collection_pages p WHERE p.job_id=j.id)
        ORDER BY j.finished_at,j.id LIMIT 4 FOR UPDATE OF j SKIP LOCKED
    LOOP
        DELETE FROM daily_reporting.source_collection_pages WHERE job_id=purge_id;
        UPDATE daily_reporting.source_collection_jobs SET cache_bytes=0 WHERE id=purge_id;
    END LOOP;
    IF EXISTS (SELECT 1 FROM daily_reporting.source_collection_jobs WHERE status='running' AND lease_until>t) THEN RETURN; END IF;
    SELECT j.id INTO chosen FROM daily_reporting.source_collection_jobs j
    JOIN jsonb_to_recordset(scope) AS allowed(account_id text, marketplace text, replenishment_enabled boolean)
      ON j.account_id=allowed.account_id AND j.marketplace=allowed.marketplace
    WHERE j.cutoff_at<=t AND j.next_attempt_at<=t AND j.generation<2147483647
      AND (j.source NOT IN ('cluster_stocks','posting_movement') OR COALESCE(allowed.replenishment_enabled,false))
      AND (j.status='ready' OR (j.status='running' AND j.lease_until<=t))
      AND NOT EXISTS (SELECT 1 FROM daily_reporting.collection_claims old
          WHERE old.account_id=j.account_id AND old.marketplace=j.marketplace AND old.cutoff_at=j.cutoff_at
            AND old.status='active' AND old.lease_until>t)
    -- Finish already admitted short-lived observations and work within five
    -- minutes of its hard deadline. Otherwise publish the newest cutoff before
    -- recovering older batches, so a restart cannot starve today's reports.
    -- Backoff, the global lease and page-level marketplace quotas still apply.
    ORDER BY CASE WHEN j.source IN ('cluster_stocks','posting_movement') THEN 1 ELSE 0 END, CASE WHEN
        (j.source IN ('stocks','seller_stocks','prices','cluster_stocks') AND j.first_observed_at IS NOT NULL)
        OR j.deadline_at<=t+interval '5 minutes'
        THEN LEAST(j.deadline_at, CASE
            WHEN j.source IN ('stocks','seller_stocks','prices','cluster_stocks')
                THEN j.first_observed_at+interval '30 minutes' ELSE NULL END)
        ELSE 'infinity'::timestamptz END,
        j.cutoff_at DESC,
        LEAST(j.deadline_at, CASE WHEN j.source IN ('stocks','seller_stocks','prices','cluster_stocks')
            THEN j.first_observed_at+interval '30 minutes' ELSE NULL END),
        j.next_attempt_at, CASE j.source WHEN 'stocks' THEN 0 WHEN 'seller_stocks' THEN 0 WHEN 'prices' THEN 1 WHEN 'sales' THEN 2 ELSE 3 END,j.id
    LIMIT 1 FOR UPDATE OF j SKIP LOCKED;
    RETURN QUERY UPDATE daily_reporting.source_collection_jobs SET status='running',generation=generation+1,
        owner_id=owner,lease_until=t + interval '2 minutes'
        WHERE id=chosen RETURNING *;
END;
$$;

COMMIT;
