BEGIN;

-- Independent seller inventory keeps the upstream delivery type on each row.
ALTER TABLE daily_reporting.source_collection_jobs DROP CONSTRAINT source_collection_jobs_source_check;
ALTER TABLE daily_reporting.source_collection_jobs ADD CONSTRAINT source_collection_jobs_source_check CHECK (source IN ('sales','stocks','seller_stocks','prices','advertising','finance')),
    ADD CONSTRAINT source_collection_jobs_fbs_marketplace CHECK (source <> 'seller_stocks' OR marketplace='wildberries');
ALTER TABLE daily_reporting.source_snapshots DROP CONSTRAINT source_snapshots_source_check,
    DROP CONSTRAINT source_snapshots_period_window_check;
ALTER TABLE daily_reporting.source_snapshots ADD CONSTRAINT source_snapshots_source_check CHECK (source IN ('sales','advertising','finance','stocks','seller_stocks','prices')),
    ADD CONSTRAINT source_snapshots_fbs_marketplace CHECK (source <> 'seller_stocks' OR marketplace='wildberries'),
    ADD CONSTRAINT source_snapshots_period_window_check CHECK (
        (source IN ('sales','advertising','finance') AND period_start<period_end AND period_end<=cutoff_at)
        OR (source IN ('stocks','seller_stocks','prices') AND period_start=period_end AND period_end=source_as_of));
CREATE TABLE daily_reporting.seller_stock_facts (
    snapshot_id bigint NOT NULL,
    source text NOT NULL DEFAULT 'seller_stocks' CHECK (source='seller_stocks'),
    sku bigint NOT NULL CHECK (sku>0),
    chrt_id bigint NOT NULL CHECK (chrt_id>0),
    warehouse_id bigint NOT NULL CHECK (warehouse_id>0),
    delivery_type integer NOT NULL CHECK (delivery_type>0),
    sellable_units integer CHECK (sellable_units IS NULL OR sellable_units>=0),
    PRIMARY KEY(snapshot_id,chrt_id,warehouse_id),
    FOREIGN KEY(snapshot_id,source) REFERENCES daily_reporting.source_snapshots(id,source) ON DELETE RESTRICT
);
-- Match the stable reader order for paginated exports of a pinned snapshot.
CREATE INDEX seller_stock_facts_snapshot_read_idx
    ON daily_reporting.seller_stock_facts(snapshot_id,sku,chrt_id,warehouse_id);
CREATE TRIGGER seller_stock_facts_require_running_snapshot BEFORE INSERT ON daily_reporting.seller_stock_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.require_running_fact_snapshot();
CREATE TRIGGER seller_stock_facts_append_only BEFORE UPDATE OR DELETE ON daily_reporting.seller_stock_facts
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_fact_mutation();
REVOKE ALL ON daily_reporting.seller_stock_facts FROM PUBLIC;
GRANT INSERT ON daily_reporting.seller_stock_facts TO report_collector;

-- Extend the existing operation-specific stock quota contract to the new
-- independent source. Sales restarts and the default three-argument wrapper
-- from migrations 036/037 retain their behavior.
CREATE OR REPLACE FUNCTION daily_reporting.admit_source_page(jid bigint, gen bigint, owner text, quota text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE j daily_reporting.source_collection_jobs; t timestamptz; delay_seconds integer;
    admitted boolean; quota_source text;
BEGIN
    IF quota IS NULL OR quota NOT IN ('default','wb_stock_content','wb_seller_inventory') THEN
        RAISE EXCEPTION 'unsupported collection quota';
    END IF;
    SELECT * INTO j FROM daily_reporting.source_collection_jobs WHERE id=jid AND generation=gen
        AND owner_id=owner AND status='running' FOR UPDATE;
    t := clock_timestamp();
    IF NOT FOUND OR j.lease_until<=t OR j.deadline_at<=t THEN
        RAISE EXCEPTION 'source collection lease lost';
    END IF;
    IF quota <> 'default' AND (j.marketplace <> 'wildberries' OR j.source NOT IN ('stocks','seller_stocks')) THEN
        RAISE EXCEPTION 'stock quota requires a WB stock lease';
    END IF;
    quota_source := CASE WHEN quota <> 'default' THEN quota
        WHEN j.marketplace='wildberries' AND j.source IN ('sales','stocks') THEN 'analytics' ELSE j.source END;
    delay_seconds := CASE WHEN quota <> 'default' THEN 2
        WHEN j.marketplace='ozon' AND j.source='sales' THEN 65
        WHEN j.marketplace='wildberries' AND j.source IN ('sales','stocks','advertising') THEN 20 ELSE 2 END;
    UPDATE daily_reporting.source_collection_jobs SET active_quota_source=NULLIF(quota,'default') WHERE id=jid;
    INSERT INTO daily_reporting.source_collection_departures AS d VALUES(j.account_id,j.marketplace,quota_source,t + make_interval(secs=>delay_seconds))
    ON CONFLICT(account_id,marketplace,source) DO UPDATE SET next_allowed_at=EXCLUDED.next_allowed_at
        WHERE d.next_allowed_at<=t RETURNING true INTO admitted;
    IF COALESCE(admitted,false) THEN
        UPDATE daily_reporting.source_collection_jobs SET first_observed_at=COALESCE(first_observed_at,t) WHERE id=jid;
    END IF;
    RETURN COALESCE(admitted,false);
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
        WHEN 'seller_stocks' THEN
            SELECT count(*) INTO actual_rows FROM daily_reporting.seller_stock_facts
            WHERE snapshot_id = OLD.id;
            IF NEW.status = 'succeeded' AND EXISTS (SELECT 1 FROM daily_reporting.seller_stock_facts WHERE snapshot_id=OLD.id AND sellable_units IS NULL) THEN
                RAISE EXCEPTION USING ERRCODE='integrity_constraint_violation', MESSAGE='missing FBS rows cannot publish as complete';
            END IF;
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
          AND (j.source NOT IN ('stocks','seller_stocks','prices') OR j.first_observed_at>=clock_timestamp()-interval '30 minutes')
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

-- Partial FBS completion means every planned pair was requested, with NULL for
-- any omitted row. It never satisfies the existing complete-only report views.
CREATE OR REPLACE FUNCTION daily_reporting.finish_source_collection(jid bigint, gen bigint, owner text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE done boolean;
BEGIN
    UPDATE daily_reporting.source_collection_jobs j SET status='published',lease_until=NULL,finished_at=clock_timestamp(),
        error_class=CASE WHEN EXISTS(SELECT 1 FROM daily_reporting.source_snapshots s WHERE s.source_job_id=j.id AND s.source_job_generation=j.generation AND s.status='partial') THEN 'seller_missing_values' ELSE NULL END,cache_bytes=0
    WHERE j.id=jid AND j.generation=gen AND j.owner_id=owner AND j.status='running' AND j.lease_until>clock_timestamp()
      AND EXISTS(SELECT 1 FROM daily_reporting.source_snapshots s WHERE s.source_job_id=j.id AND s.source_job_generation=j.generation AND s.pagination_complete
        AND (s.status='succeeded' OR (j.source='seller_stocks' AND s.status='partial')))
    RETURNING true INTO done;
    IF COALESCE(done,false) THEN DELETE FROM daily_reporting.source_collection_pages WHERE job_id=jid; END IF;
    RETURN COALESCE(done,false);
END;
$$;

CREATE VIEW daily_reporting.mcp_seller_stock_facts WITH(security_barrier=true) AS
SELECT s.account_id,s.marketplace,s.cutoff_at,s.source_as_of,s.status AS snapshot_status,s.pagination_complete,
    f.snapshot_id,f.source,f.sku,f.chrt_id,f.warehouse_id,f.delivery_type,f.sellable_units
FROM daily_reporting.seller_stock_facts f JOIN daily_reporting.published_source_snapshots s ON s.id=f.snapshot_id;
REVOKE ALL ON daily_reporting.mcp_seller_stock_facts FROM PUBLIC;
GRANT SELECT ON daily_reporting.mcp_seller_stock_facts TO position_reader;

-- Optional FBS never gates the existing sales-refresh manifest.
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
            AND snapshot.source <> 'seller_stocks'
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
            AND j.marketplace=r.marketplace AND j.cutoff_at=r.snapshot_cutoff_at AND j.status='failed' AND j.source <> 'seller_stocks') THEN
            UPDATE daily_reporting.ozon_sales_refresh_requests SET status='failed',error_class='source_collection_failed',finished_at=t WHERE id=r.id;
        ELSIF (SELECT count(*) FROM daily_reporting.source_snapshots s WHERE s.account_id=r.account_id
            AND s.marketplace=r.marketplace AND s.cutoff_at=r.snapshot_cutoff_at AND s.status='succeeded' AND s.pagination_complete AND s.source <> 'seller_stocks')
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
COMMIT;
