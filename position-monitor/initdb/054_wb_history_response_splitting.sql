BEGIN;

-- Only an unpublished leased request is reduced. Record its original scope
-- before retaining one half under the same task ID and queueing the other.
-- Published observations/days and the per-token WB departure gate are intact.
CREATE TABLE daily_reporting.wb_history_task_splits (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id bigint NOT NULL REFERENCES daily_reporting.wb_history_jobs(id),
    task_id bigint NOT NULL REFERENCES daily_reporting.wb_history_tasks(id),
    new_task_id bigint NOT NULL REFERENCES daily_reporting.wb_history_tasks(id),
    generation bigint NOT NULL,
    owner_id text NOT NULL,
    date_from date NOT NULL,
    date_to date NOT NULL,
    campaign_ids bigint[] NOT NULL,
    axis text NOT NULL CHECK(axis IN ('campaigns','dates')),
    split_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(job_id,generation)
);
REVOKE ALL ON daily_reporting.wb_history_task_splits FROM PUBLIC;
CREATE TRIGGER wb_history_task_splits_immutable
    BEFORE UPDATE OR DELETE ON daily_reporting.wb_history_task_splits
    FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_fact_mutation();

CREATE OR REPLACE FUNCTION daily_reporting.wb_history_defer(
    jid bigint, gen bigint, owner text, failure text, delay_seconds integer, terminal boolean
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE
    j daily_reporting.wb_history_jobs;
    task daily_reporting.wb_history_tasks;
    now_at timestamptz:=clock_timestamp();
    split_axis text;
    midpoint integer;
    next_task bigint;
    stop_retry boolean:=terminal;
BEGIN
    IF delay_seconds IS NULL OR delay_seconds NOT BETWEEN 1 AND 2147483647
        OR terminal IS NULL THEN RAISE EXCEPTION 'invalid history delay'; END IF;
    SELECT * INTO j FROM daily_reporting.wb_history_jobs
        WHERE id=jid AND generation=gen AND owner_id=owner
        AND status='running' AND lease_until>now_at FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'history lease lost'; END IF;

    IF failure='response_too_large' AND NOT terminal THEN
        SELECT * INTO task FROM daily_reporting.wb_history_tasks
            WHERE job_id=jid AND status='queued' ORDER BY id LIMIT 1 FOR UPDATE;
        IF FOUND
            AND NOT EXISTS(SELECT 1 FROM daily_reporting.wb_history_observations WHERE task_id=task.id)
            AND (SELECT count(*) FROM daily_reporting.wb_history_tasks WHERE job_id=jid)<25000 THEN
            IF cardinality(task.campaign_ids)>1 THEN
                split_axis:='campaigns';
                midpoint:=cardinality(task.campaign_ids)/2;
                UPDATE daily_reporting.wb_history_tasks
                    SET campaign_ids=task.campaign_ids[1:midpoint] WHERE id=task.id;
                INSERT INTO daily_reporting.wb_history_tasks(job_id,kind,date_from,date_to,campaign_ids)
                    VALUES(jid,task.kind,task.date_from,task.date_to,
                        task.campaign_ids[midpoint+1:cardinality(task.campaign_ids)])
                    RETURNING id INTO next_task;
            ELSIF task.kind='stats' AND task.date_to>task.date_from THEN
                split_axis:='dates';
                midpoint:=(task.date_to-task.date_from)/2;
                UPDATE daily_reporting.wb_history_tasks
                    SET date_to=task.date_from+midpoint WHERE id=task.id;
                INSERT INTO daily_reporting.wb_history_tasks(job_id,kind,date_from,date_to,campaign_ids)
                    VALUES(jid,task.kind,task.date_from+midpoint+1,task.date_to,task.campaign_ids)
                    RETURNING id INTO next_task;
            END IF;
        END IF;
        IF split_axis IS NOT NULL THEN
            INSERT INTO daily_reporting.wb_history_task_splits(
                job_id,task_id,new_task_id,generation,owner_id,date_from,date_to,campaign_ids,axis
            ) VALUES(jid,task.id,next_task,gen,owner,task.date_from,task.date_to,task.campaign_ids,split_axis);
            UPDATE daily_reporting.wb_history_jobs
                SET status='queued',next_attempt_at=now_at+interval '20 seconds',
                    lease_until=NULL,failures=0,error_class=failure,finished_at=NULL WHERE id=jid;
            RETURN;
        END IF;
        -- One campaign/day (or one details ID), inventory and a full queue
        -- cannot be reduced further. Repeating an identical payload cannot help.
        stop_retry:=true;
    END IF;

    UPDATE daily_reporting.wb_history_jobs
        SET status=CASE WHEN stop_retry OR failures>=7 THEN 'failed' ELSE 'queued' END,
            next_attempt_at=now_at+make_interval(secs=>delay_seconds),lease_until=NULL,error_class=failure,
            failures=LEAST(8,failures+CASE WHEN failure='rate_limited' THEN 0 ELSE 1 END),
            finished_at=CASE WHEN stop_retry OR failures>=7 THEN now_at END WHERE id=jid;
    IF failure='rate_limited' THEN
        INSERT INTO daily_reporting.source_collection_departures AS d
            VALUES(j.account_id,'wildberries','advertising',now_at+make_interval(secs=>delay_seconds))
        ON CONFLICT(account_id,marketplace,source)
            DO UPDATE SET next_allowed_at=GREATEST(d.next_allowed_at,EXCLUDED.next_allowed_at);
    END IF;
END;
$$;

COMMIT;
