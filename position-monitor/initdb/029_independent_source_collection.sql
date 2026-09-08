BEGIN;

-- Durable work belongs to one source. Report completeness remains a read-side
-- manifest check over the existing immutable source_snapshots table.
CREATE TABLE daily_reporting.source_collection_jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id varchar(128) NOT NULL CHECK (account_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    marketplace text NOT NULL CHECK (marketplace IN ('ozon','wildberries')),
    source text NOT NULL CHECK (source IN ('sales','stocks','prices','advertising','finance')),
    cutoff_at timestamptz NOT NULL,
    period_start timestamptz NOT NULL,
    period_end timestamptz NOT NULL CHECK (period_end > period_start),
    deadline_at timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'ready' CHECK (status IN ('ready','running','published','failed')),
    generation bigint NOT NULL DEFAULT 0 CHECK (generation BETWEEN 0 AND 2147483647),
    owner_id varchar(64),
    lease_until timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    first_observed_at timestamptz,
    last_observed_at timestamptz,
    completed_pages integer NOT NULL DEFAULT 0 CHECK (completed_pages BETWEEN 0 AND 4096),
    cache_bytes bigint NOT NULL DEFAULT 0 CHECK (cache_bytes BETWEEN 0 AND 33554432),
    consecutive_failures integer NOT NULL DEFAULT 0 CHECK (consecutive_failures BETWEEN 0 AND 8),
    error_class varchar(64) CHECK (error_class ~ '^[a-z][a-z0-9_]{0,63}$'),
    finished_at timestamptz,
    UNIQUE (account_id, marketplace, source, cutoff_at),
    UNIQUE (id, generation),
    CHECK (deadline_at > cutoff_at AND deadline_at <= cutoff_at + interval '24 hours'),
    CHECK (marketplace = 'ozon' OR source <> 'finance'),
    CHECK (status <> 'running' OR (owner_id ~ '^[A-Za-z0-9._:-]{1,64}$' AND lease_until IS NOT NULL))
);
CREATE INDEX source_collection_jobs_due_idx ON daily_reporting.source_collection_jobs(next_attempt_at, id)
    WHERE status IN ('ready','running');
CREATE TABLE daily_reporting.source_collection_pages (
    job_id bigint NOT NULL REFERENCES daily_reporting.source_collection_jobs(id) ON DELETE RESTRICT,
    request_key char(64) NOT NULL CHECK (request_key ~ '^[0-9a-f]{64}$'),
    payload jsonb NOT NULL CHECK (octet_length(payload::text) <= 4194304),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (job_id, request_key)
);
CREATE TABLE daily_reporting.source_collection_departures (
    account_id varchar(128) NOT NULL,
    marketplace text NOT NULL,
    source text NOT NULL,
    next_allowed_at timestamptz NOT NULL,
    PRIMARY KEY (account_id, marketplace, source)
);

CREATE FUNCTION daily_reporting.enqueue_source_collection(a text, m text, s text, c timestamptz, p_start timestamptz, p_end timestamptz)
RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    INSERT INTO daily_reporting.source_collection_jobs(account_id,marketplace,source,cutoff_at,period_start,period_end,deadline_at)
    SELECT a,m,s,c,p_start,p_end,c + interval '24 hours'
    WHERE NOT EXISTS (SELECT 1 FROM daily_reporting.source_snapshots
        WHERE account_id=a AND marketplace=m AND source=s AND cutoff_at=c AND status='succeeded' AND pagination_complete)
    ON CONFLICT(account_id,marketplace,source,cutoff_at) DO NOTHING;
$$;

CREATE FUNCTION daily_reporting.claim_source_collection(scope jsonb, owner text)
RETURNS SETOF daily_reporting.source_collection_jobs
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE t timestamptz := clock_timestamp(); chosen bigint;
BEGIN
    IF owner IS NULL OR owner !~ '^[A-Za-z0-9._:-]{1,64}$' OR jsonb_typeof(scope) <> 'array'
       OR jsonb_array_length(scope) NOT BETWEEN 1 AND 64 THEN
        RAISE EXCEPTION 'invalid source collection scope';
    END IF;
    PERFORM pg_advisory_xact_lock(917244, 29);
    UPDATE daily_reporting.source_collection_jobs
    SET status='failed', error_class='collection_expired', finished_at=t, lease_until=NULL, cache_bytes=0
    WHERE status IN ('ready','running') AND (deadline_at<=t OR
        (source IN ('stocks','prices') AND first_observed_at < t - interval '30 minutes'));
    DELETE FROM daily_reporting.source_collection_pages p USING daily_reporting.source_collection_jobs j
        WHERE p.job_id=j.id AND j.status IN ('published','failed');
    IF EXISTS (SELECT 1 FROM daily_reporting.source_collection_jobs WHERE status='running' AND lease_until>t) THEN RETURN; END IF;
    SELECT j.id INTO chosen FROM daily_reporting.source_collection_jobs j
    JOIN jsonb_to_recordset(scope) AS allowed(account_id text, marketplace text)
      ON j.account_id=allowed.account_id AND j.marketplace=allowed.marketplace
    WHERE j.cutoff_at<=t AND j.next_attempt_at<=t AND j.generation<2147483647
      AND (j.status='ready' OR (j.status='running' AND j.lease_until<=t))
      AND NOT EXISTS (SELECT 1 FROM daily_reporting.collection_claims old
          WHERE old.account_id=j.account_id AND old.marketplace=j.marketplace AND old.cutoff_at=j.cutoff_at
            AND old.status='active' AND old.lease_until>t)
    ORDER BY j.next_attempt_at, CASE j.source WHEN 'stocks' THEN 0 WHEN 'prices' THEN 1 WHEN 'sales' THEN 2 ELSE 3 END,j.id
    LIMIT 1 FOR UPDATE OF j SKIP LOCKED;
    RETURN QUERY UPDATE daily_reporting.source_collection_jobs SET status='running',generation=generation+1,
        owner_id=owner,lease_until=t + interval '2 minutes'
        WHERE id=chosen RETURNING *;
END;
$$;

CREATE FUNCTION daily_reporting.admit_source_page(jid bigint, gen bigint, owner text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE j daily_reporting.source_collection_jobs; t timestamptz := clock_timestamp(); delay_seconds integer; admitted boolean; quota_source text;
BEGIN
    SELECT * INTO j FROM daily_reporting.source_collection_jobs WHERE id=jid AND generation=gen
        AND owner_id=owner AND status='running' AND lease_until>t AND deadline_at>t FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'source collection lease lost'; END IF;
    quota_source := CASE WHEN j.marketplace='wildberries' AND j.source IN ('sales','stocks') THEN 'analytics' ELSE j.source END;
    delay_seconds := CASE WHEN j.marketplace='ozon' AND j.source='sales' THEN 65
        WHEN j.marketplace='wildberries' AND j.source IN ('sales','stocks','advertising') THEN 20 ELSE 2 END;
    INSERT INTO daily_reporting.source_collection_departures AS d VALUES(j.account_id,j.marketplace,quota_source,t + make_interval(secs=>delay_seconds))
    ON CONFLICT(account_id,marketplace,source) DO UPDATE SET next_allowed_at=EXCLUDED.next_allowed_at
        WHERE d.next_allowed_at<=t RETURNING true INTO admitted;
    IF COALESCE(admitted,false) THEN
        UPDATE daily_reporting.source_collection_jobs SET first_observed_at=COALESCE(first_observed_at,t) WHERE id=jid;
    END IF;
    RETURN COALESCE(admitted,false);
END;
$$;

CREATE FUNCTION daily_reporting.save_source_page(jid bigint, gen bigint, owner text, k text, page jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE n integer := octet_length(page::text); j daily_reporting.source_collection_jobs;
BEGIN
    SELECT * INTO j FROM daily_reporting.source_collection_jobs WHERE id=jid AND generation=gen
        AND owner_id=owner AND status='running' AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp() FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'source collection lease lost'; END IF;
    IF n IS NULL OR n>4194304 OR j.completed_pages>=4096 OR j.cache_bytes+n>33554432 THEN RAISE EXCEPTION USING ERRCODE='program_limit_exceeded', MESSAGE='source checkpoint bound exceeded'; END IF;
    INSERT INTO daily_reporting.source_collection_pages(job_id,request_key,payload) VALUES(jid,k,page);
    UPDATE daily_reporting.source_collection_jobs SET completed_pages=completed_pages+1,cache_bytes=cache_bytes+n,
        last_observed_at=clock_timestamp(),consecutive_failures=0 WHERE id=jid;
END;
$$;

CREATE FUNCTION daily_reporting.defer_source_collection(jid bigint, gen bigint, owner text, failure text, wait_seconds integer, terminal boolean)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE j daily_reporting.source_collection_jobs; t timestamptz := clock_timestamp(); gate timestamptz; quota_source text;
BEGIN
    IF wait_seconds IS NULL OR wait_seconds NOT BETWEEN 1 AND 2147483647 OR terminal IS NULL THEN RAISE EXCEPTION 'invalid collection delay'; END IF;
    SELECT * INTO j FROM daily_reporting.source_collection_jobs WHERE id=jid AND generation=gen
        AND owner_id=owner AND status='running' AND lease_until>t FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    quota_source := CASE WHEN j.marketplace='wildberries' AND j.source IN ('sales','stocks') THEN 'analytics' ELSE j.source END;
    IF failure = 'rate_limited' THEN
        INSERT INTO daily_reporting.source_collection_departures AS d
            VALUES(j.account_id,j.marketplace,quota_source,t+make_interval(secs=>wait_seconds))
        ON CONFLICT(account_id,marketplace,source) DO UPDATE
            SET next_allowed_at=GREATEST(d.next_allowed_at,EXCLUDED.next_allowed_at);
    END IF;
    SELECT next_allowed_at INTO gate FROM daily_reporting.source_collection_departures
        WHERE account_id=j.account_id AND marketplace=j.marketplace AND source=quota_source;
    UPDATE daily_reporting.source_collection_jobs SET
        status=CASE WHEN terminal OR (failure IS NOT NULL AND consecutive_failures>=7) THEN 'failed' ELSE 'ready' END,
        next_attempt_at=GREATEST(t+make_interval(secs=>wait_seconds),gate),
        error_class=failure,consecutive_failures=LEAST(8,consecutive_failures+CASE WHEN failure IS NULL THEN 0 ELSE 1 END),
        cache_bytes=CASE WHEN terminal OR (failure IS NOT NULL AND consecutive_failures>=7) THEN 0 ELSE cache_bytes END,
        lease_until=NULL,finished_at=CASE WHEN terminal OR (failure IS NOT NULL AND consecutive_failures>=7) THEN t ELSE NULL END
    WHERE id=jid;
    DELETE FROM daily_reporting.source_collection_pages p USING daily_reporting.source_collection_jobs finished_job
        WHERE p.job_id=finished_job.id AND finished_job.id=jid AND finished_job.status='failed';
    RETURN true;
END;
$$;

ALTER TABLE daily_reporting.source_snapshots ADD COLUMN source_job_id bigint,
    ADD COLUMN source_job_generation bigint,
    ADD CONSTRAINT source_snapshot_job_pair CHECK ((source_job_id IS NULL)=(source_job_generation IS NULL)),
    ADD CONSTRAINT source_snapshot_job_fk FOREIGN KEY(source_job_id,source_job_generation)
        REFERENCES daily_reporting.source_collection_jobs(id,generation);
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
          AND (j.source NOT IN ('stocks','prices') OR j.first_observed_at>=clock_timestamp()-interval '30 minutes')
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
CREATE FUNCTION daily_reporting.source_job_identity_immutable()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF NEW.source_job_id IS DISTINCT FROM OLD.source_job_id OR NEW.source_job_generation IS DISTINCT FROM OLD.source_job_generation THEN
        RAISE EXCEPTION 'source snapshot job identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER source_snapshot_job_identity_immutable BEFORE UPDATE ON daily_reporting.source_snapshots
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.source_job_identity_immutable();
REVOKE ALL ON FUNCTION daily_reporting.source_job_identity_immutable() FROM PUBLIC;

CREATE FUNCTION daily_reporting.finish_source_collection(jid bigint, gen bigint, owner text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE done boolean;
BEGIN
    UPDATE daily_reporting.source_collection_jobs j SET status='published',lease_until=NULL,finished_at=clock_timestamp(),error_class=NULL,cache_bytes=0
    WHERE j.id=jid AND j.generation=gen AND j.owner_id=owner AND j.status='running' AND j.lease_until>clock_timestamp()
      AND EXISTS (SELECT 1 FROM daily_reporting.source_snapshots s WHERE s.source_job_id=j.id AND s.source_job_generation=j.generation AND s.status='succeeded' AND s.pagination_complete)
    RETURNING true INTO done;
    IF COALESCE(done,false) THEN DELETE FROM daily_reporting.source_collection_pages WHERE job_id=jid; END IF;
    RETURN COALESCE(done,false);
END;
$$;

-- Row locks require UPDATE privilege in PostgreSQL. Keep it inside this
-- fenced function instead of granting the collector direct job mutations.
CREATE FUNCTION daily_reporting.lock_source_publication(jid bigint, gen bigint, owner text)
RETURNS TABLE(first_observed_at timestamptz,last_observed_at timestamptz)
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
    SELECT j.first_observed_at,j.last_observed_at FROM daily_reporting.source_collection_jobs j
    WHERE j.id=jid AND j.generation=gen AND j.owner_id=owner AND j.status='running'
        AND j.lease_until>clock_timestamp() AND j.deadline_at>clock_timestamp()
    FOR UPDATE;
$$;
REVOKE ALL ON FUNCTION daily_reporting.lock_source_publication(bigint,bigint,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION daily_reporting.lock_source_publication(bigint,bigint,text) TO report_collector;

CREATE VIEW daily_reporting.mcp_source_collection_jobs WITH(security_barrier=true) AS
SELECT account_id,marketplace,source,cutoff_at,status,next_attempt_at,first_observed_at,last_observed_at,
       completed_pages,consecutive_failures,error_class,finished_at FROM daily_reporting.source_collection_jobs;
REVOKE ALL ON daily_reporting.source_collection_jobs,daily_reporting.source_collection_pages,daily_reporting.source_collection_departures FROM PUBLIC;
GRANT SELECT ON daily_reporting.source_collection_jobs,daily_reporting.source_collection_pages TO report_collector;
GRANT SELECT ON daily_reporting.mcp_source_collection_jobs TO position_reader;
REVOKE ALL ON FUNCTION daily_reporting.enqueue_source_collection(text,text,text,timestamptz,timestamptz,timestamptz),
    daily_reporting.claim_source_collection(jsonb,text),daily_reporting.admit_source_page(bigint,bigint,text),
    daily_reporting.save_source_page(bigint,bigint,text,text,jsonb),daily_reporting.defer_source_collection(bigint,bigint,text,text,integer,boolean),
    daily_reporting.finish_source_collection(bigint,bigint,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION daily_reporting.enqueue_source_collection(text,text,text,timestamptz,timestamptz,timestamptz),
    daily_reporting.claim_source_collection(jsonb,text),daily_reporting.admit_source_page(bigint,bigint,text),
    daily_reporting.save_source_page(bigint,bigint,text,text,jsonb),daily_reporting.defer_source_collection(bigint,bigint,text,text,integer,boolean),
    daily_reporting.finish_source_collection(bigint,bigint,text) TO report_collector;

ALTER TABLE daily_reporting.ozon_sales_refresh_requests
    ADD COLUMN source_jobs_dispatched boolean NOT NULL DEFAULT false;
CREATE OR REPLACE FUNCTION daily_reporting.claim_marketplace_sales_refresh_for(
    requested_owner_id text,
    requested_marketplace text
)
RETURNS TABLE (
    request_id bigint,
    request_generation integer,
    account_id text,
    marketplace text,
    business_date date,
    snapshot_cutoff_at timestamptz,
    lease_until timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    now_at timestamptz := clock_timestamp();
    current_business_date date :=
        (clock_timestamp() AT TIME ZONE 'Asia/Yekaterinburg')::date;
    selected_id bigint;
BEGIN
    IF requested_owner_id IS NULL
        OR requested_owner_id !~ '^[A-Za-z0-9._:-]{1,64}$'
        OR (requested_marketplace IS NOT NULL
            AND requested_marketplace NOT IN ('ozon', 'wildberries'))
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'marketplace sales refresh claim input is invalid';
    END IF;

    PERFORM pg_advisory_xact_lock(917244);

    UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
    SET status = 'queued',
        not_before = GREATEST(refresh.requested_at, now_at),
        owner_id = NULL,
        lease_until = NULL,
        started_at = NULL,
        finished_at = NULL,
        error_class = NULL
    WHERE refresh.status = 'running'
      AND refresh.lease_until <= now_at
      AND refresh.attempt_count < 3
      AND refresh.business_date = current_business_date
      AND refresh.requested_at >= now_at - interval '4 hours';

    UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
    SET status = 'failed',
        finished_at = now_at,
        error_class = CASE
            WHEN refresh.status = 'running' THEN 'worker_lease_expired'
            ELSE 'queue_expired'
        END
    WHERE (refresh.status = 'running' AND refresh.lease_until <= now_at)
       OR (refresh.status = 'queued'
           AND (refresh.business_date <> current_business_date
                OR refresh.requested_at < now_at - interval '4 hours'));

    IF EXISTS (
        SELECT 1
        FROM daily_reporting.ozon_sales_refresh_requests AS refresh
        WHERE refresh.status = 'running'
          AND refresh.lease_until > now_at
    ) THEN
        RETURN;
    END IF;

    SELECT refresh.id INTO selected_id
    FROM daily_reporting.ozon_sales_refresh_requests AS refresh
    WHERE refresh.status = 'queued'
      AND NOT refresh.source_jobs_dispatched
      AND refresh.business_date = current_business_date
      AND refresh.not_before <= now_at
      AND (requested_marketplace IS NULL
           OR refresh.marketplace = requested_marketplace)
    ORDER BY refresh.requested_at, refresh.id
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

    IF selected_id IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    UPDATE daily_reporting.ozon_sales_refresh_requests AS refresh
    SET status = 'running',
        generation = refresh.generation + 1,
        attempt_count = refresh.attempt_count + 1,
        owner_id = requested_owner_id,
        lease_until = now_at + interval '15 minutes',
        snapshot_cutoff_at = COALESCE(refresh.snapshot_cutoff_at, now_at),
        started_at = now_at,
        finished_at = NULL,
        error_class = NULL
    WHERE refresh.id = selected_id
      AND refresh.status = 'queued'
      AND refresh.attempt_count < 3
    RETURNING refresh.id,
              refresh.generation,
              refresh.account_id::text,
              refresh.marketplace,
              refresh.business_date,
              refresh.snapshot_cutoff_at,
              refresh.lease_until;
END;
$$;


-- The legacy queue remains the public deduplication and completion contract.
-- No long lease is held while pages are paused. Its queued state encompasses
-- independently collecting sources; the source read tool exposes progress.
CREATE FUNCTION daily_reporting.dispatch_source_refreshes(scope jsonb)
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
                ELSE ARRAY['sales','stocks','prices','advertising'] END
            LOOP
                PERFORM daily_reporting.enqueue_source_collection(r.account_id,r.marketplace,src,r.snapshot_cutoff_at,
                    r.business_date::timestamp AT TIME ZONE 'Asia/Yekaterinburg',r.snapshot_cutoff_at);
            END LOOP;
        END IF;
        IF EXISTS (SELECT 1 FROM daily_reporting.source_collection_jobs j WHERE j.account_id=r.account_id
            AND j.marketplace=r.marketplace AND j.cutoff_at=r.snapshot_cutoff_at AND j.status='failed') THEN
            UPDATE daily_reporting.ozon_sales_refresh_requests SET status='failed',error_class='source_collection_failed',finished_at=t WHERE id=r.id;
        ELSIF (SELECT count(*) FROM daily_reporting.source_snapshots s WHERE s.account_id=r.account_id
            AND s.marketplace=r.marketplace AND s.cutoff_at=r.snapshot_cutoff_at AND s.status='succeeded' AND s.pagination_complete)
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
REVOKE ALL ON FUNCTION daily_reporting.dispatch_source_refreshes(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION daily_reporting.dispatch_source_refreshes(jsonb) TO report_collector;

COMMIT;
