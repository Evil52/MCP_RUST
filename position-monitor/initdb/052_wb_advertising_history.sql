BEGIN;

-- History has no daily-report deadline. Every WB response is a revision;
-- the current projection chooses one observation per campaign/calendar day.
CREATE TABLE daily_reporting.wb_history_jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id varchar(128) NOT NULL CHECK(account_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    actor_id varchar(128) NOT NULL,
    requested_from date,
    date_from date,
    date_to date NOT NULL CHECK(date_to >= DATE '2010-01-01'),
    status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','succeeded','partial','failed')),
    inventory_initialized boolean NOT NULL DEFAULT false,
    inventory jsonb,
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    generation bigint NOT NULL DEFAULT 0,
    owner_id text,
    lease_until timestamptz,
    failures integer NOT NULL DEFAULT 0 CHECK(failures BETWEEN 0 AND 8),
    error_class text CHECK(error_class ~ '^[a-z][a-z0-9_]{0,63}$'),
    CHECK(requested_from IS NULL OR requested_from BETWEEN DATE '2010-01-01' AND date_to)
);
CREATE INDEX wb_history_jobs_due ON daily_reporting.wb_history_jobs(next_attempt_at,id)
    WHERE status IN ('queued','running');
CREATE INDEX wb_history_jobs_account ON daily_reporting.wb_history_jobs(account_id,id DESC);
CREATE TABLE daily_reporting.wb_history_campaigns (
    account_id varchar(128) NOT NULL,
    campaign_id bigint NOT NULL CHECK(campaign_id>0),
    status integer NOT NULL,
    created_on date,
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(account_id,campaign_id)
);
CREATE TABLE daily_reporting.wb_history_tasks (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id bigint NOT NULL REFERENCES daily_reporting.wb_history_jobs(id),
    kind text NOT NULL DEFAULT 'stats' CHECK(kind IN ('details','stats')),
    date_from date NOT NULL,
    date_to date NOT NULL CHECK(date_to>=date_from AND date_to-date_from<31),
    campaign_ids bigint[] NOT NULL CHECK(cardinality(campaign_ids) BETWEEN 1 AND 50),
    status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','succeeded','partial')),
    UNIQUE(job_id,kind,date_from,campaign_ids)
);
CREATE INDEX wb_history_tasks_pending ON daily_reporting.wb_history_tasks(job_id,id) WHERE status='queued';
CREATE TABLE daily_reporting.wb_history_observations (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id bigint NOT NULL UNIQUE REFERENCES daily_reporting.wb_history_tasks(id),
    account_id varchar(128) NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    payload jsonb NOT NULL CHECK(octet_length(payload::text)<=4194304)
);
CREATE TABLE daily_reporting.wb_history_days (
    observation_id bigint NOT NULL REFERENCES daily_reporting.wb_history_observations(id),
    account_id varchar(128) NOT NULL,
    campaign_id bigint NOT NULL CHECK(campaign_id>0),
    business_date date NOT NULL,
    state text NOT NULL CHECK(state IN ('observed','no_data','missing')),
    metrics jsonb,
    sku_reconciled boolean NOT NULL,
    products jsonb NOT NULL CHECK(jsonb_typeof(products)='array'),
    PRIMARY KEY(observation_id,campaign_id,business_date),
    CHECK((state='observed')=(metrics IS NOT NULL)),
    CHECK(state='observed' OR (NOT sku_reconciled AND products='[]'::jsonb))
);
CREATE INDEX wb_history_days_current ON daily_reporting.wb_history_days(account_id,campaign_id,business_date,observation_id DESC);
CREATE INDEX wb_history_days_dates ON daily_reporting.wb_history_days(account_id,business_date);
CREATE VIEW daily_reporting.wb_history_current_days AS
    SELECT DISTINCT ON(account_id,campaign_id,business_date) *
    FROM daily_reporting.wb_history_days ORDER BY account_id,campaign_id,business_date,observation_id DESC;

CREATE FUNCTION daily_reporting.wb_history_job_status(jid bigint) RETURNS jsonb
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
        SELECT jsonb_build_object('job_id',j.id,'status',j.status,'date_from',j.date_from,
            'date_to',j.date_to,'requested_at',j.requested_at,'finished_at',j.finished_at,
            'next_attempt_at',j.next_attempt_at,'error_class',j.error_class,
            'total_requests',(SELECT count(*) FROM daily_reporting.wb_history_tasks WHERE job_id=j.id),
            'completed_requests',(SELECT count(*) FROM daily_reporting.wb_history_tasks WHERE job_id=j.id AND status<>'queued'),
            'requests_with_gaps',(SELECT count(*) FROM daily_reporting.wb_history_tasks WHERE job_id=j.id AND status='partial'))
        FROM daily_reporting.wb_history_jobs j WHERE id=jid;
$$;

CREATE FUNCTION daily_reporting.wb_history_status(a text) RETURNS jsonb
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
    SELECT jsonb_build_object('account_id',a,'job',daily_reporting.wb_history_job_status(
        (SELECT id FROM daily_reporting.wb_history_jobs WHERE account_id=a ORDER BY id DESC LIMIT 1)));
$$;

CREATE FUNCTION daily_reporting.wb_history_request(a text, actor text, f date, t date) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE jid bigint;
BEGIN
    IF a IS NULL OR a !~ '^[A-Za-z0-9_-]{1,128}$' OR actor IS NULL OR actor !~ '^[A-Za-z0-9._:@-]{1,128}$'
        OR t IS NULL OR t<DATE '2010-01-01' OR t >= (clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date
        OR (f IS NOT NULL AND (f<DATE '2010-01-01' OR f>t)) THEN RAISE EXCEPTION 'invalid history scope'; END IF;
    PERFORM pg_advisory_xact_lock(hashtext(a),52);
    SELECT id INTO jid FROM daily_reporting.wb_history_jobs WHERE account_id=a AND status IN ('queued','running')
        AND requested_from IS NOT DISTINCT FROM f AND date_to=t ORDER BY id DESC LIMIT 1;
    IF jid IS NULL THEN
        IF (SELECT count(*) FROM daily_reporting.wb_history_jobs WHERE account_id=a AND status IN ('queued','running'))>=8 THEN
            RAISE EXCEPTION 'history queue capacity exceeded';
        END IF;
        INSERT INTO daily_reporting.wb_history_jobs(account_id,actor_id,requested_from,date_to) VALUES(a,actor,f,t) RETURNING id INTO jid;
    END IF;
    RETURN jsonb_build_object('account_id',a,'job',daily_reporting.wb_history_job_status(jid));
END;
$$;

CREATE FUNCTION daily_reporting.wb_history_claim(scope text[], owner text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j daily_reporting.wb_history_jobs; task daily_reporting.wb_history_tasks; now_at timestamptz:=clock_timestamp();
BEGIN
    IF cardinality(scope) NOT BETWEEN 1 AND 64 OR owner IS NULL OR owner !~ '^[A-Za-z0-9._:-]{1,64}$' THEN
        RAISE EXCEPTION 'invalid history worker scope'; END IF;
    PERFORM pg_advisory_xact_lock(917244,52);
    IF EXISTS(SELECT 1 FROM daily_reporting.wb_history_jobs WHERE status='running' AND lease_until>now_at) THEN RETURN NULL; END IF;
    SELECT * INTO j FROM daily_reporting.wb_history_jobs h
    WHERE account_id=ANY(scope) AND next_attempt_at<=now_at
        AND (status='queued' OR (status='running' AND lease_until<=now_at))
        -- Current daily collection has priority over archival requests.
        AND NOT EXISTS(SELECT 1 FROM daily_reporting.source_collection_jobs s
            WHERE s.account_id=h.account_id AND s.marketplace='wildberries' AND s.source='advertising'
            AND s.deadline_at>now_at AND ((s.status='ready' AND s.next_attempt_at<=now_at)
                OR (s.status='running' AND s.lease_until>now_at)))
        AND NOT EXISTS(SELECT 1 FROM daily_reporting.source_collection_departures d
            WHERE d.account_id=h.account_id AND d.marketplace='wildberries' AND d.source='advertising' AND d.next_allowed_at>now_at)
    ORDER BY (requested_from IS NOT NULL) DESC,next_attempt_at,id LIMIT 1 FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN RETURN NULL; END IF;
    UPDATE daily_reporting.wb_history_jobs SET status='running',owner_id=owner,generation=generation+1,
        lease_until=now_at+interval '2 minutes' WHERE id=j.id RETURNING * INTO j;
    SELECT * INTO task FROM daily_reporting.wb_history_tasks WHERE job_id=j.id AND status='queued' ORDER BY id LIMIT 1;
    -- Share the persisted advertising gate with report collection. WbClient
    -- additionally enforces the token/endpoint quota shared by all processes.
    INSERT INTO daily_reporting.source_collection_departures AS d VALUES(j.account_id,'wildberries','advertising',now_at+interval '20 seconds')
    ON CONFLICT(account_id,marketplace,source) DO UPDATE SET next_allowed_at=EXCLUDED.next_allowed_at;
    RETURN jsonb_build_object('job_id',j.id,'generation',j.generation,'account_id',j.account_id,
        'lease_until',j.lease_until,'task_id',task.id,'kind',task.kind,'date_from',task.date_from,'date_to',task.date_to,'campaign_ids',task.campaign_ids);
END;
$$;

-- Refresh the last 30 closed days once per day, only for initialized accounts.
-- Existing tasks and responses survive process restarts. A failed refresh may
-- be requested explicitly again; no fresh job flood is caused by restarts.
CREATE FUNCTION daily_reporting.wb_history_refresh_recent(a text, t date) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(a),52);
    IF EXISTS(SELECT 1 FROM daily_reporting.wb_history_jobs WHERE account_id=a AND inventory_initialized)
        AND (SELECT count(*) FROM daily_reporting.wb_history_jobs WHERE account_id=a AND status IN('queued','running'))<8
        AND NOT EXISTS(SELECT 1 FROM daily_reporting.wb_history_jobs WHERE account_id=a AND actor_id='report-collector' AND date_to=t) THEN
        PERFORM daily_reporting.wb_history_request(a,'report-collector',t-29,t);
    END IF;
END;
$$;

CREATE FUNCTION daily_reporting.wb_history_plan(jid bigint) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j daily_reporting.wb_history_jobs; start_date date; requests bigint;
BEGIN
    SELECT * INTO j FROM daily_reporting.wb_history_jobs WHERE id=jid FOR UPDATE;
    -- Previous manifests retain campaigns that disappear from WB's current list.
    SELECT COALESCE(j.requested_from,CASE WHEN bool_or(created_on IS NULL) THEN DATE '2010-01-01'
        ELSE LEAST(min(created_on),j.date_to) END,j.date_to) INTO start_date
    FROM daily_reporting.wb_history_campaigns WHERE account_id=j.account_id AND status IN(7,9,11);
    start_date:=GREATEST(start_date,DATE '2010-01-01');
    requests:=((j.date_to-start_date)/31+1)*
        (((SELECT count(*) FROM daily_reporting.wb_history_campaigns WHERE account_id=j.account_id AND status IN(7,9,11))+49)/50);
    IF requests>25000 THEN RAISE EXCEPTION 'history request bound exceeded'; END IF;
    INSERT INTO daily_reporting.wb_history_tasks(job_id,date_from,date_to,campaign_ids)
    SELECT j.id,d::date,LEAST(d::date+30,j.date_to),array_agg(campaign_id ORDER BY campaign_id)
    FROM generate_series(start_date::timestamp,j.date_to::timestamp,interval '31 days') d
    CROSS JOIN (SELECT campaign_id,created_on,(row_number() OVER(ORDER BY campaign_id)-1)/50 AS batch
        FROM daily_reporting.wb_history_campaigns WHERE account_id=j.account_id AND status IN(7,9,11)) c
    WHERE c.created_on IS NULL OR c.created_on<=LEAST(d::date+30,j.date_to)
    GROUP BY d,batch;
    UPDATE daily_reporting.wb_history_jobs SET date_from=start_date,
        status=CASE WHEN EXISTS(SELECT 1 FROM daily_reporting.wb_history_tasks WHERE job_id=jid AND status='queued') THEN 'queued' ELSE 'succeeded' END,
        next_attempt_at=clock_timestamp()+interval '20 seconds',lease_until=NULL,failures=0,error_class=NULL,
        finished_at=CASE WHEN NOT EXISTS(SELECT 1 FROM daily_reporting.wb_history_tasks WHERE job_id=jid AND status='queued') THEN clock_timestamp() END
    WHERE id=jid;
END;
$$;

CREATE FUNCTION daily_reporting.wb_history_inventory(jid bigint, gen bigint, owner text, inventory_payload jsonb) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j daily_reporting.wb_history_jobs;
BEGIN
    SELECT * INTO j FROM daily_reporting.wb_history_jobs WHERE id=jid AND generation=gen AND owner_id=owner
        AND status='running' AND lease_until>clock_timestamp() AND NOT inventory_initialized FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'history lease lost'; END IF;
    IF jsonb_typeof(inventory_payload)<>'array' OR jsonb_array_length(inventory_payload)>5000 THEN RAISE EXCEPTION 'invalid history inventory'; END IF;
    INSERT INTO daily_reporting.wb_history_campaigns AS c(account_id,campaign_id,status,created_on)
    SELECT j.account_id,x.campaign_id,x.status,x.created_on
    FROM jsonb_to_recordset(inventory_payload) AS x(campaign_id bigint,status integer,created_on date)
    ON CONFLICT(account_id,campaign_id) DO UPDATE SET status=EXCLUDED.status,
        created_on=LEAST(c.created_on,EXCLUDED.created_on),last_seen_at=clock_timestamp();
    UPDATE daily_reporting.wb_history_jobs SET inventory=inventory_payload,inventory_initialized=true WHERE id=jid;
    INSERT INTO daily_reporting.wb_history_tasks(job_id,kind,date_from,date_to,campaign_ids)
    SELECT jid,'details',DATE '2010-01-01',DATE '2010-01-01',array_agg(campaign_id ORDER BY campaign_id)
    FROM (SELECT campaign_id,(row_number() OVER(ORDER BY campaign_id)-1)/50 AS batch
        FROM daily_reporting.wb_history_campaigns WHERE account_id=j.account_id AND created_on IS NULL AND status IN(7,9,11) AND j.requested_from IS NULL) c
    GROUP BY batch;
    IF NOT EXISTS(SELECT 1 FROM daily_reporting.wb_history_tasks WHERE job_id=jid) THEN
        PERFORM daily_reporting.wb_history_plan(jid);
    ELSE
        UPDATE daily_reporting.wb_history_jobs SET status='queued',next_attempt_at=clock_timestamp()+interval '20 seconds',
            lease_until=NULL,failures=0,error_class=NULL WHERE id=jid;
    END IF;
END;
$$;

CREATE FUNCTION daily_reporting.wb_history_details(jid bigint, gen bigint, owner text, tid bigint, details jsonb) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j daily_reporting.wb_history_jobs; task daily_reporting.wb_history_tasks;
BEGIN
    SELECT * INTO j FROM daily_reporting.wb_history_jobs WHERE id=jid AND generation=gen AND owner_id=owner
        AND status='running' AND lease_until>clock_timestamp() FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'history lease lost'; END IF;
    SELECT * INTO task FROM daily_reporting.wb_history_tasks WHERE id=tid AND job_id=jid AND kind='details' AND status='queued';
    IF NOT FOUND OR jsonb_typeof(details)<>'array' OR jsonb_array_length(details)>cardinality(task.campaign_ids) THEN
        RAISE EXCEPTION 'invalid history details'; END IF;
    IF EXISTS(SELECT 1 FROM jsonb_to_recordset(details) AS x(campaign_id bigint) WHERE campaign_id<>ALL(task.campaign_ids) OR campaign_id IS NULL) THEN
        RAISE EXCEPTION 'history details outside scope'; END IF;
    UPDATE daily_reporting.wb_history_campaigns c SET created_on=x.created_on
    FROM jsonb_to_recordset(details) AS x(campaign_id bigint,created_on date)
    WHERE c.account_id=j.account_id AND c.campaign_id=x.campaign_id;
    UPDATE daily_reporting.wb_history_tasks SET status='succeeded' WHERE id=tid;
    IF NOT EXISTS(SELECT 1 FROM daily_reporting.wb_history_tasks WHERE job_id=jid AND kind='details' AND status='queued') THEN
        PERFORM daily_reporting.wb_history_plan(jid);
    ELSE
        UPDATE daily_reporting.wb_history_jobs SET status='queued',next_attempt_at=clock_timestamp()+interval '20 seconds',lease_until=NULL,failures=0,error_class=NULL WHERE id=jid;
    END IF;
END;
$$;

CREATE FUNCTION daily_reporting.wb_history_publish(jid bigint, gen bigint, owner text, tid bigint, raw jsonb, days jsonb) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j daily_reporting.wb_history_jobs; task daily_reporting.wb_history_tasks; oid bigint; gaps boolean;
BEGIN
    SELECT * INTO j FROM daily_reporting.wb_history_jobs WHERE id=jid AND generation=gen AND owner_id=owner
        AND status='running' AND lease_until>clock_timestamp() FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'history lease lost'; END IF;
    SELECT * INTO task FROM daily_reporting.wb_history_tasks WHERE id=tid AND job_id=jid AND kind='stats' AND status='queued' FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'history task lost'; END IF;
    IF jsonb_typeof(days)<>'array' OR jsonb_array_length(days)<>cardinality(task.campaign_ids)*(task.date_to-task.date_from+1)
        OR octet_length(days::text)>8388608 THEN RAISE EXCEPTION 'invalid history day coverage'; END IF;
    IF EXISTS(SELECT 1 FROM jsonb_to_recordset(days) AS x(campaign_id bigint,business_date date,state text,metrics jsonb,products jsonb)
        WHERE campaign_id<>ALL(task.campaign_ids) OR business_date NOT BETWEEN task.date_from AND task.date_to
            OR campaign_id IS NULL OR business_date IS NULL OR state IS NULL OR state NOT IN('observed','no_data','missing')
            OR (state='observed')<>(metrics IS NOT NULL) OR jsonb_typeof(products) IS DISTINCT FROM 'array'
            OR (state='observed' AND (NOT(metrics ?& ARRAY['spend_minor','revenue_minor','orders','clicks','impressions'])
                OR EXISTS(SELECT 1 FROM jsonb_each_text(metrics) e WHERE e.value !~ '^[0-9]{1,15}$')
                OR (metrics->>'clicks')::bigint>(metrics->>'impressions')::bigint))) THEN
        RAISE EXCEPTION 'invalid history facts'; END IF;
    INSERT INTO daily_reporting.wb_history_observations(task_id,account_id,payload) VALUES(tid,j.account_id,raw) RETURNING id INTO oid;
    INSERT INTO daily_reporting.wb_history_days(observation_id,account_id,campaign_id,business_date,state,metrics,sku_reconciled,products)
    SELECT oid,j.account_id,x.campaign_id,x.business_date,x.state,x.metrics,x.sku_reconciled,x.products
    FROM jsonb_to_recordset(days) AS x(campaign_id bigint,business_date date,state text,metrics jsonb,sku_reconciled boolean,products jsonb);
    SELECT EXISTS(SELECT 1 FROM daily_reporting.wb_history_days WHERE observation_id=oid AND state='missing') INTO gaps;
    UPDATE daily_reporting.wb_history_tasks SET status=CASE WHEN gaps THEN 'partial' ELSE 'succeeded' END WHERE id=tid;
    -- A sparse batch cannot prove that an omitted campaign has zero activity.
    -- Verify those IDs individually; a successful null then closes the gap.
    -- A missing single-ID response remains a gap, without an infinite loop.
    IF gaps AND cardinality(task.campaign_ids)>1 AND
        (SELECT count(*) FROM daily_reporting.wb_history_tasks WHERE job_id=jid)+cardinality(task.campaign_ids)<=25000 THEN
        INSERT INTO daily_reporting.wb_history_tasks(job_id,date_from,date_to,campaign_ids)
        SELECT DISTINCT jid,task.date_from,task.date_to,ARRAY[campaign_id]
        FROM daily_reporting.wb_history_days WHERE observation_id=oid AND state='missing'
        ON CONFLICT(job_id,kind,date_from,campaign_ids) DO NOTHING;
    END IF;
    UPDATE daily_reporting.wb_history_jobs SET status=CASE
        WHEN EXISTS(SELECT 1 FROM daily_reporting.wb_history_tasks WHERE job_id=jid AND status='queued') THEN 'queued'
        WHEN EXISTS(SELECT 1 FROM daily_reporting.wb_history_current_days WHERE account_id=j.account_id
            AND business_date BETWEEN j.date_from AND j.date_to AND state='missing') THEN 'partial' ELSE 'succeeded' END,
        finished_at=CASE WHEN NOT EXISTS(SELECT 1 FROM daily_reporting.wb_history_tasks WHERE job_id=jid AND status='queued') THEN clock_timestamp() END,
        next_attempt_at=clock_timestamp()+interval '20 seconds',lease_until=NULL,failures=0,error_class=NULL WHERE id=jid;
END;
$$;

CREATE FUNCTION daily_reporting.wb_history_defer(jid bigint, gen bigint, owner text, failure text, delay_seconds integer, terminal boolean) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j daily_reporting.wb_history_jobs; now_at timestamptz:=clock_timestamp();
BEGIN
    IF delay_seconds IS NULL OR delay_seconds NOT BETWEEN 1 AND 2147483647 OR terminal IS NULL THEN RAISE EXCEPTION 'invalid history delay'; END IF;
    SELECT * INTO j FROM daily_reporting.wb_history_jobs WHERE id=jid AND generation=gen AND owner_id=owner
        AND status='running' AND lease_until>now_at FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'history lease lost'; END IF;
    UPDATE daily_reporting.wb_history_jobs SET status=CASE WHEN terminal OR failures>=7 THEN 'failed' ELSE 'queued' END,
        next_attempt_at=now_at+make_interval(secs=>delay_seconds),lease_until=NULL,error_class=failure,
        failures=LEAST(8,failures+CASE WHEN failure='rate_limited' THEN 0 ELSE 1 END),
        finished_at=CASE WHEN terminal OR failures>=7 THEN now_at END WHERE id=jid;
    IF failure='rate_limited' THEN
        INSERT INTO daily_reporting.source_collection_departures AS d VALUES(j.account_id,'wildberries','advertising',now_at+make_interval(secs=>delay_seconds))
        ON CONFLICT(account_id,marketplace,source) DO UPDATE SET next_allowed_at=GREATEST(d.next_allowed_at,EXCLUDED.next_allowed_at);
    END IF;
END;
$$;

-- SQL sums the campaign-day parent once. SKU rows are an independent projection.
-- All-time account completeness remains unverified: WB's current inventory may
-- omit campaigns deleted before this archive was first initialized.
CREATE FUNCTION daily_reporting.wb_history_stats(a text, f date, t date, grp text, lim integer, off integer) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE start_date date; end_date date; result jsonb; initialized boolean;
BEGIN
    IF grp NOT IN('campaign','day','sku') OR lim NOT BETWEEN 1 AND 1000 OR off NOT BETWEEN 0 AND 100000 THEN
        RAISE EXCEPTION 'invalid history projection'; END IF;
    SELECT COALESCE(f,min(date_from)),COALESCE(t,max(date_to)) INTO start_date,end_date
    FROM daily_reporting.wb_history_jobs WHERE account_id=a AND inventory_initialized;
    SELECT EXISTS(SELECT 1 FROM daily_reporting.wb_history_jobs WHERE account_id=a AND inventory_initialized AND date_from IS NOT NULL) INTO initialized;
    IF start_date>end_date THEN RAISE EXCEPTION 'invalid history range'; END IF;
    WITH current_days AS MATERIALIZED (
        SELECT d.* FROM daily_reporting.wb_history_current_days d WHERE account_id=a AND business_date BETWEEN start_date AND end_date
            AND NOT EXISTS(SELECT 1 FROM daily_reporting.wb_history_campaigns c WHERE c.account_id=a AND c.campaign_id=d.campaign_id AND c.created_on>d.business_date)
    ), totals AS (
        SELECT COALESCE(sum((metrics->>'spend_minor')::numeric),0) AS spend,
            COALESCE(sum((metrics->>'revenue_minor')::numeric),0) AS revenue,
            COALESCE(sum((metrics->>'orders')::numeric),0) AS orders,
            COALESCE(sum((metrics->>'clicks')::numeric),0) AS clicks,
            COALESCE(sum((metrics->>'impressions')::numeric),0) AS impressions FROM current_days WHERE state='observed'
    ), expected AS (
        SELECT COALESCE(sum(GREATEST(0,end_date-GREATEST(start_date,COALESCE(created_on,start_date))+1)),0) AS n
        FROM daily_reporting.wb_history_campaigns WHERE account_id=a AND status IN(7,9,11)
    ), coverage AS (
        SELECT count(*) FILTER(WHERE state<>'missing') AS covered,
            count(*) FILTER(WHERE state='no_data') AS no_data,
            count(*) FILTER(WHERE state='observed' AND NOT sku_reconciled) AS unallocated FROM current_days
    ), grouped AS (
        SELECT CASE grp WHEN 'campaign' THEN campaign_id::text WHEN 'day' THEN business_date::text ELSE p->>'sku' END AS dimension,
            CASE WHEN grp='sku' THEN p->'metrics' ELSE COALESCE(metrics,'{"spend_minor":0,"revenue_minor":0,"orders":0,"clicks":0,"impressions":0}'::jsonb) END AS m
        FROM current_days LEFT JOIN LATERAL jsonb_array_elements(CASE WHEN grp='sku' THEN products ELSE '[null]'::jsonb END) p ON true
        WHERE state<>'missing' AND (grp<>'sku' OR sku_reconciled)
    ), all_rows AS (
        SELECT dimension, sum((m->>'spend_minor')::numeric) AS spend,sum((m->>'revenue_minor')::numeric) AS revenue,
            sum((m->>'orders')::numeric) AS orders,sum((m->>'clicks')::numeric) AS clicks,sum((m->>'impressions')::numeric) AS impressions
        FROM grouped WHERE dimension IS NOT NULL GROUP BY dimension
    ), page AS (SELECT * FROM all_rows ORDER BY dimension LIMIT lim OFFSET off)
    SELECT jsonb_build_object('account_id',a,'date_from',start_date,'date_to',end_date,
        'storage','wb_calendar_day_archive','revenue_basis','attributed_orders_including_associated_conversions',
        'all_time_verified',false,'inventory_scope','available_and_previously_archived_campaigns',
        'state',CASE WHEN NOT initialized OR start_date IS NULL OR (c.covered=0 AND e.n>0) THEN 'unavailable'
            WHEN e.n<>c.covered OR EXISTS(SELECT 1 FROM daily_reporting.wb_history_campaigns WHERE account_id=a AND status NOT IN(7,9,11)) THEN 'partial' ELSE 'complete_for_known_campaigns' END,
        'coverage',jsonb_build_object('expected_campaign_days',e.n,'covered_campaign_days',c.covered,
            'missing_campaign_days',GREATEST(0,e.n-c.covered),'no_data_campaign_days',c.no_data,
            'unallocated_sku_days',c.unallocated,'complete_for_known_campaigns',initialized AND start_date IS NOT NULL AND e.n=c.covered
                AND NOT EXISTS(SELECT 1 FROM daily_reporting.wb_history_campaigns WHERE account_id=a AND status NOT IN(7,9,11)),
            'unsupported_campaigns',(SELECT count(*) FROM daily_reporting.wb_history_campaigns WHERE account_id=a AND status NOT IN(7,9,11)),
            'unknown_creation_dates',(SELECT count(*) FROM daily_reporting.wb_history_campaigns WHERE account_id=a AND created_on IS NULL),
            'last_observed_at',(SELECT max(o.observed_at) FROM daily_reporting.wb_history_observations o JOIN current_days d ON d.observation_id=o.id)),
        'totals',CASE WHEN initialized AND start_date IS NOT NULL AND (c.covered>0 OR e.n=0) THEN jsonb_build_object('spend_minor',tot.spend,'revenue_minor',tot.revenue,'orders',tot.orders,'clicks',tot.clicks,'impressions',tot.impressions,
            'drr_percent',CASE WHEN tot.revenue>0 THEN round(tot.spend*100/tot.revenue,4)::text END) END,
        'group_by',grp,'total_rows',(SELECT count(*) FROM all_rows),'offset',off,
        'next_offset',CASE WHEN off+lim<(SELECT count(*) FROM all_rows) THEN off+lim END,
        'rows',COALESCE((SELECT jsonb_agg(jsonb_build_object('dimension',dimension,'spend_minor',spend,'revenue_minor',revenue,'orders',orders,
            'clicks',clicks,'impressions',impressions,'drr_percent',CASE WHEN revenue>0 THEN round(spend*100/revenue,4)::text END) ORDER BY dimension) FROM page),'[]'::jsonb))
    INTO result FROM totals tot CROSS JOIN expected e CROSS JOIN coverage c;
    RETURN result;
END;
$$;

CREATE TRIGGER wb_history_observations_immutable BEFORE UPDATE OR DELETE ON daily_reporting.wb_history_observations
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_fact_mutation();
CREATE TRIGGER wb_history_days_immutable BEFORE UPDATE OR DELETE ON daily_reporting.wb_history_days
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_fact_mutation();

REVOKE ALL ON daily_reporting.wb_history_jobs,daily_reporting.wb_history_campaigns,daily_reporting.wb_history_tasks,
    daily_reporting.wb_history_observations,daily_reporting.wb_history_days,daily_reporting.wb_history_current_days FROM PUBLIC;
REVOKE ALL ON FUNCTION daily_reporting.wb_history_request(text,text,date,date),daily_reporting.wb_history_status(text),
    daily_reporting.wb_history_stats(text,date,date,text,integer,integer),daily_reporting.wb_history_claim(text[],text),
    daily_reporting.wb_history_inventory(bigint,bigint,text,jsonb),daily_reporting.wb_history_publish(bigint,bigint,text,bigint,jsonb,jsonb),
    daily_reporting.wb_history_defer(bigint,bigint,text,text,integer,boolean),daily_reporting.wb_history_refresh_recent(text,date),
    daily_reporting.wb_history_plan(bigint),daily_reporting.wb_history_job_status(bigint),daily_reporting.wb_history_details(bigint,bigint,text,bigint,jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION daily_reporting.wb_history_request(text,text,date,date),daily_reporting.wb_history_status(text),
    daily_reporting.wb_history_stats(text,date,date,text,integer,integer) TO report_refresh_requester,report_collector;
GRANT EXECUTE ON FUNCTION daily_reporting.wb_history_claim(text[],text),daily_reporting.wb_history_inventory(bigint,bigint,text,jsonb),
    daily_reporting.wb_history_publish(bigint,bigint,text,bigint,jsonb,jsonb),
    daily_reporting.wb_history_defer(bigint,bigint,text,text,integer,boolean),daily_reporting.wb_history_refresh_recent(text,date),
    daily_reporting.wb_history_details(bigint,bigint,text,bigint,jsonb) TO report_collector;

COMMIT;
