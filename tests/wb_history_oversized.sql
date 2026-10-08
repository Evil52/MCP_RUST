-- Run only against the disposable schema-test database; no WB calls.
BEGIN;
CREATE FUNCTION pg_temp.history_size_job(a text, ids bigint[], f date, t date, k text DEFAULT 'stats')
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE jid bigint;
BEGIN
    INSERT INTO daily_reporting.wb_history_jobs(
        account_id,actor_id,requested_from,date_from,date_to,inventory_initialized,
        status,generation,owner_id,lease_until,failures
    ) VALUES(a,'schema-test',f,f,t,true,'running',1,'size-test',clock_timestamp()+interval '2 minutes',7)
        RETURNING id INTO jid;
    INSERT INTO daily_reporting.wb_history_tasks(job_id,kind,date_from,date_to,campaign_ids)
        VALUES(jid,k,f,t,ids);
    RETURN jid;
END;
$$;
CREATE FUNCTION pg_temp.history_size_assert(ok boolean, label text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF ok IS DISTINCT FROM true THEN RAISE EXCEPTION 'history size regression: %',label; END IF;
    RAISE NOTICE 'PASS %',label;
END;
$$;

DO $$
DECLARE jid bigint; original bigint; item daily_reporting.wb_history_tasks; rejected boolean;
BEGIN
    jid:=pg_temp.history_size_job('history_size_campaigns',ARRAY[1,2,3,4,5]::bigint[],'2023-10-01','2023-10-03');
    SELECT id INTO original FROM daily_reporting.wb_history_tasks WHERE job_id=jid;
    INSERT INTO daily_reporting.source_collection_departures
        VALUES('history_size_campaigns','wildberries','advertising',clock_timestamp()+interval '1 hour');
    PERFORM daily_reporting.wb_history_defer(jid,1,'size-test','response_too_large',120,false);
    PERFORM pg_temp.history_size_assert((SELECT status='queued' AND failures=0 AND lease_until IS NULL
        AND next_attempt_at>=clock_timestamp()+interval '18 seconds' FROM daily_reporting.wb_history_jobs WHERE id=jid),
        'oversized request becomes runnable smaller work after delay');
    PERFORM pg_temp.history_size_assert((SELECT count(*)=2 AND sum(cardinality(campaign_ids))=5
        AND bool_and(date_from='2023-10-01' AND date_to='2023-10-03')
        FROM daily_reporting.wb_history_tasks WHERE job_id=jid), 'campaign split retains exact date coverage');
    PERFORM pg_temp.history_size_assert((SELECT count(*)=count(DISTINCT campaign)
        FROM daily_reporting.wb_history_tasks CROSS JOIN LATERAL unnest(campaign_ids) campaign WHERE job_id=jid),
        'campaign split has no duplicates');
    PERFORM pg_temp.history_size_assert((SELECT count(*)=1 AND bool_and(axis='campaigns'
        AND task_id=original AND campaign_ids=ARRAY[1,2,3,4,5]::bigint[])
        FROM daily_reporting.wb_history_task_splits WHERE job_id=jid), 'original oversized scope remains audited');
    PERFORM pg_temp.history_size_assert((SELECT next_allowed_at>clock_timestamp()+interval '59 minutes'
        FROM daily_reporting.source_collection_departures WHERE account_id='history_size_campaigns'), 'split preserves WB cooldown');
    rejected:=false;
    BEGIN
        PERFORM daily_reporting.wb_history_defer(jid,1,'size-test','response_too_large',120,false);
    EXCEPTION WHEN raise_exception THEN rejected:=SQLERRM='history lease lost'; END;
    PERFORM pg_temp.history_size_assert(rejected AND (SELECT count(*)=2 FROM daily_reporting.wb_history_tasks WHERE job_id=jid),
        'replayed split cannot duplicate work');
    rejected:=false;
    BEGIN
        UPDATE daily_reporting.wb_history_task_splits SET axis='dates' WHERE job_id=jid;
    EXCEPTION WHEN integrity_constraint_violation THEN rejected:=true; END;
    PERFORM pg_temp.history_size_assert(rejected,'split audit is immutable');

    jid:=pg_temp.history_size_job('history_size_dates',ARRAY[9]::bigint[],'2023-10-01','2023-10-04');
    PERFORM daily_reporting.wb_history_defer(jid,1,'size-test','response_too_large',120,false);
    PERFORM pg_temp.history_size_assert((SELECT count(*)=2 AND sum(date_to-date_from+1)=4
        AND min(date_from)='2023-10-01' AND max(date_to)='2023-10-04'
        FROM daily_reporting.wb_history_tasks WHERE job_id=jid), 'single campaign splits into adjacent dates');
    PERFORM pg_temp.history_size_assert((SELECT count(*)=count(DISTINCT d) FROM daily_reporting.wb_history_tasks
        CROSS JOIN LATERAL generate_series(date_from::timestamp,date_to::timestamp,interval '1 day') d WHERE job_id=jid),
        'date split has no overlap');
    -- Publish the retained first half, then the second half. Earlier facts are
    -- neither rewritten nor added to the total a second time after splitting.
    FOR item IN SELECT * FROM daily_reporting.wb_history_tasks WHERE job_id=jid ORDER BY id LOOP
        UPDATE daily_reporting.wb_history_jobs SET status='running',generation=generation+1,
            owner_id='size-test',lease_until=clock_timestamp()+interval '2 minutes' WHERE id=jid;
        PERFORM daily_reporting.wb_history_publish(jid,
            (SELECT generation FROM daily_reporting.wb_history_jobs WHERE id=jid),'size-test',item.id,
            'null'::jsonb,(SELECT jsonb_agg(jsonb_build_object('campaign_id',9,'business_date',d::date,
                'state','no_data','metrics',NULL,'sku_reconciled',false,'products','[]'::jsonb))
                FROM generate_series(item.date_from::timestamp,item.date_to::timestamp,interval '1 day') d));
    END LOOP;
    PERFORM pg_temp.history_size_assert((SELECT status='succeeded' FROM daily_reporting.wb_history_jobs WHERE id=jid)
        AND (SELECT count(*)=4 FROM daily_reporting.wb_history_days WHERE account_id='history_size_dates'),
        'split leaves publish and finish without lost or duplicated days');

    jid:=pg_temp.history_size_job('history_size_smallest',ARRAY[9]::bigint[],'2023-10-01','2023-10-01');
    PERFORM daily_reporting.wb_history_defer(jid,1,'size-test','response_too_large',120,false);
    PERFORM pg_temp.history_size_assert((SELECT status='failed' AND error_class='response_too_large'
        AND finished_at IS NOT NULL FROM daily_reporting.wb_history_jobs WHERE id=jid)
        AND NOT EXISTS(SELECT 1 FROM daily_reporting.wb_history_task_splits WHERE job_id=jid),
        'one campaign day stops with explicit failure');

    jid:=pg_temp.history_size_job('history_size_fence',ARRAY[1,2]::bigint[],'2023-10-01','2023-10-02');
    rejected:=false;
    BEGIN
        PERFORM daily_reporting.wb_history_defer(jid,2,'size-test','response_too_large',120,false);
    EXCEPTION WHEN raise_exception THEN rejected:=SQLERRM='history lease lost'; END;
    PERFORM pg_temp.history_size_assert(rejected,'stale generation cannot split');
    rejected:=false;
    BEGIN
        PERFORM daily_reporting.wb_history_defer(jid,1,'wrong-owner','response_too_large',120,false);
    EXCEPTION WHEN raise_exception THEN rejected:=SQLERRM='history lease lost'; END;
    PERFORM pg_temp.history_size_assert(rejected,'wrong owner cannot split');
    UPDATE daily_reporting.wb_history_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=jid;
    rejected:=false;
    BEGIN
        PERFORM daily_reporting.wb_history_defer(jid,1,'size-test','response_too_large',120,false);
    EXCEPTION WHEN raise_exception THEN rejected:=SQLERRM='history lease lost'; END;
    PERFORM pg_temp.history_size_assert(rejected AND (SELECT count(*)=1 FROM daily_reporting.wb_history_tasks WHERE job_id=jid),
        'expired lease cannot split');

    jid:=pg_temp.history_size_job('history_size_details',ARRAY[1,2]::bigint[],'2010-01-01','2010-01-01','details');
    PERFORM daily_reporting.wb_history_defer(jid,1,'size-test','response_too_large',120,false);
    PERFORM pg_temp.history_size_assert((SELECT count(*)=2 AND bool_and(kind='details')
        FROM daily_reporting.wb_history_tasks WHERE job_id=jid),'details split keeps task kind');
    jid:=pg_temp.history_size_job('history_size_one_detail',ARRAY[1]::bigint[],'2010-01-01','2010-01-01','details');
    PERFORM daily_reporting.wb_history_defer(jid,1,'size-test','response_too_large',120,false);
    PERFORM pg_temp.history_size_assert((SELECT status='failed' FROM daily_reporting.wb_history_jobs WHERE id=jid),
        'one details ID cannot split dates');

    jid:=pg_temp.history_size_job('history_size_quota',ARRAY[1,2]::bigint[],'2023-10-01','2023-10-03');
    UPDATE daily_reporting.wb_history_jobs SET failures=0 WHERE id=jid;
    PERFORM daily_reporting.wb_history_defer(jid,1,'size-test','rate_limited',3600,false);
    PERFORM pg_temp.history_size_assert((SELECT status='queued' AND failures=0 FROM daily_reporting.wb_history_jobs WHERE id=jid)
        AND (SELECT next_allowed_at>clock_timestamp()+interval '59 minutes'
            FROM daily_reporting.source_collection_departures WHERE account_id='history_size_quota')
        AND (SELECT count(*)=1 FROM daily_reporting.wb_history_tasks WHERE job_id=jid),
        '429 still preserves cooldown and does not split');

    jid:=pg_temp.history_size_job('history_size_bound',ARRAY[1,2]::bigint[],'2023-10-01','2023-10-03');
    INSERT INTO daily_reporting.wb_history_tasks(job_id,date_from,date_to,campaign_ids)
        SELECT jid,'2023-10-01','2023-10-03',ARRAY[c] FROM generate_series(10::bigint,25008::bigint) c;
    PERFORM daily_reporting.wb_history_defer(jid,1,'size-test','response_too_large',120,false);
    PERFORM pg_temp.history_size_assert((SELECT status='failed' FROM daily_reporting.wb_history_jobs WHERE id=jid)
        AND (SELECT count(*)=25000 FROM daily_reporting.wb_history_tasks WHERE job_id=jid),
        'split cannot exceed durable queue bound');

    PERFORM pg_temp.history_size_assert(
        has_function_privilege('report_collector','daily_reporting.wb_history_defer(bigint,bigint,text,text,integer,boolean)','EXECUTE')
        AND NOT has_function_privilege('report_refresh_requester','daily_reporting.wb_history_defer(bigint,bigint,text,text,integer,boolean)','EXECUTE')
        AND NOT has_table_privilege('report_collector','daily_reporting.wb_history_task_splits','SELECT,INSERT,UPDATE,DELETE')
        AND NOT has_table_privilege('report_refresh_requester','daily_reporting.wb_history_task_splits','SELECT,INSERT,UPDATE,DELETE'),
        'migration retains restricted worker and requester capabilities');
END;
$$;
ROLLBACK;
