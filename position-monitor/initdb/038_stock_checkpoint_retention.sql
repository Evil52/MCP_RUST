BEGIN;

-- Failed stock pages are private diagnostic evidence, never published facts.
-- Retention is bounded by the existing 4 MiB/page and 32 MiB/job limits and by
-- 24 hours after failure. Explicit recovery preserves the observation clock.
CREATE TABLE daily_reporting.stock_collection_resumes (
    job_id bigint NOT NULL REFERENCES daily_reporting.source_collection_jobs(id),
    failed_generation bigint NOT NULL,
    resumed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    requested_by text NOT NULL,
    previous_error_class varchar(64) NOT NULL,
    reason varchar(128) NOT NULL CHECK (reason ~ '^[A-Za-z0-9 _.:/-]{1,128}$'),
    PRIMARY KEY (job_id, failed_generation)
);
REVOKE ALL ON daily_reporting.stock_collection_resumes
    FROM PUBLIC, position_reader, report_worker, report_collector;

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
        (source IN ('stocks','seller_stocks','prices') AND first_observed_at < t - interval '30 minutes'));
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
    JOIN jsonb_to_recordset(scope) AS allowed(account_id text, marketplace text)
      ON j.account_id=allowed.account_id AND j.marketplace=allowed.marketplace
    WHERE j.cutoff_at<=t AND j.next_attempt_at<=t AND j.generation<2147483647
      AND (j.status='ready' OR (j.status='running' AND j.lease_until<=t))
      AND NOT EXISTS (SELECT 1 FROM daily_reporting.collection_claims old
          WHERE old.account_id=j.account_id AND old.marketplace=j.marketplace AND old.cutoff_at=j.cutoff_at
            AND old.status='active' AND old.lease_until>t)
    ORDER BY j.next_attempt_at, CASE j.source WHEN 'stocks' THEN 0 WHEN 'seller_stocks' THEN 0 WHEN 'prices' THEN 1 WHEN 'sales' THEN 2 ELSE 3 END,j.id
    LIMIT 1 FOR UPDATE OF j SKIP LOCKED;
    RETURN QUERY UPDATE daily_reporting.source_collection_jobs SET status='running',generation=generation+1,
        owner_id=owner,lease_until=t + interval '2 minutes'
        WHERE id=chosen RETURNING *;
END;
$$;

CREATE OR REPLACE FUNCTION daily_reporting.defer_source_collection(jid bigint, gen bigint, owner text, failure text, wait_seconds integer, terminal boolean)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE j daily_reporting.source_collection_jobs; t timestamptz := clock_timestamp(); gate timestamptz; quota_source text; stopped boolean;
BEGIN
    IF wait_seconds IS NULL OR wait_seconds NOT BETWEEN 1 AND 2147483647 OR terminal IS NULL THEN RAISE EXCEPTION 'invalid collection delay'; END IF;
    SELECT * INTO j FROM daily_reporting.source_collection_jobs WHERE id=jid AND generation=gen
        AND owner_id=owner AND status='running' AND lease_until>t FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    t := clock_timestamp();
    IF j.lease_until <= t THEN RETURN false; END IF;
    -- Preserve migration 037's operation-specific quota across retries.
    quota_source := COALESCE(j.active_quota_source,
        CASE WHEN j.marketplace='wildberries' AND j.source IN ('sales','stocks') THEN 'analytics' ELSE j.source END);
    IF failure = 'rate_limited' THEN
        INSERT INTO daily_reporting.source_collection_departures AS d
            VALUES(j.account_id,j.marketplace,quota_source,t+make_interval(secs=>wait_seconds))
        ON CONFLICT(account_id,marketplace,source) DO UPDATE
            SET next_allowed_at=GREATEST(d.next_allowed_at,EXCLUDED.next_allowed_at);
    END IF;
    SELECT next_allowed_at INTO gate FROM daily_reporting.source_collection_departures
        WHERE account_id=j.account_id AND marketplace=j.marketplace AND source=quota_source;
    -- A retained checkpoint does not authorize automatic malformed/auth retries.
    stopped := terminal OR (failure IS NOT NULL AND j.consecutive_failures>=7)
        OR COALESCE(failure IN ('invalid_json','unauthorized','forbidden','missing_credentials','credentials_unavailable'),false);
    UPDATE daily_reporting.source_collection_jobs SET
        status=CASE WHEN stopped THEN 'failed' ELSE 'ready' END,
        next_attempt_at=GREATEST(t+make_interval(secs=>wait_seconds),gate),
        error_class=failure,consecutive_failures=LEAST(8,consecutive_failures+CASE WHEN failure IS NULL THEN 0 ELSE 1 END),
        cache_bytes=CASE WHEN stopped AND source NOT IN ('stocks','seller_stocks') THEN 0 ELSE cache_bytes END,
        lease_until=NULL,finished_at=CASE WHEN stopped THEN t ELSE NULL END
    WHERE id=jid;
    IF stopped AND j.source NOT IN ('stocks','seller_stocks') THEN
        DELETE FROM daily_reporting.source_collection_pages WHERE job_id=jid;
    END IF;
    RETURN true;
END;
$$;

-- This is an operator-only action after the specific failure has been fixed.
-- It cannot create a job, renew the observation window or erase last-good data.
CREATE FUNCTION daily_reporting.resume_stock_collection(
    a text, m text, jid bigint, expected_generation bigint, expected_failure text, resume_reason text
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE j daily_reporting.source_collection_jobs; t timestamptz := clock_timestamp();
BEGIN
    IF a IS NULL OR a !~ '^[A-Za-z0-9_-]{1,128}$'
       OR m IS NULL OR m NOT IN ('ozon','wildberries')
       OR jid IS NULL OR expected_generation IS NULL
       OR expected_failure IS NULL OR expected_failure !~ '^[a-z][a-z0-9_]{0,63}$'
       OR resume_reason IS NULL OR resume_reason !~ '^[A-Za-z0-9 _.:/-]{1,128}$'
       OR length(btrim(resume_reason))=0 THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='invalid stock recovery scope';
    END IF;
    SELECT * INTO j FROM daily_reporting.source_collection_jobs
      WHERE id=jid AND account_id=a AND marketplace=m AND generation=expected_generation
        AND status='failed' AND source IN ('stocks','seller_stocks')
        AND error_class=expected_failure AND generation<2147483647
        AND deadline_at>t AND first_observed_at BETWEEN t-interval '30 minutes' AND t
        AND last_observed_at BETWEEN first_observed_at AND t
        AND completed_pages>0 AND cache_bytes>0
      FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    t := clock_timestamp();
    IF j.deadline_at<=t OR j.first_observed_at<t-interval '30 minutes' THEN RETURN false; END IF;
    IF (SELECT count(*) FROM daily_reporting.stock_collection_resumes WHERE job_id=jid)>=3
       OR (SELECT count(*) FROM daily_reporting.source_collection_pages WHERE job_id=jid)<>j.completed_pages
       OR EXISTS (SELECT 1 FROM daily_reporting.stock_collection_resumes WHERE job_id=jid AND failed_generation=expected_generation)
       OR EXISTS (SELECT 1 FROM daily_reporting.source_snapshots
           WHERE account_id=a AND marketplace=m AND source=j.source
             AND cutoff_at>=j.cutoff_at AND pagination_complete
             AND (status='succeeded' OR (j.source='seller_stocks' AND status='partial'))) THEN
        RETURN false;
    END IF;
    INSERT INTO daily_reporting.stock_collection_resumes
        (job_id,failed_generation,requested_by,previous_error_class,reason)
    VALUES(jid,expected_generation,session_user,expected_failure,resume_reason);
    UPDATE daily_reporting.source_collection_jobs
       SET status='ready',owner_id=NULL,lease_until=NULL,finished_at=NULL,error_class=NULL,
           consecutive_failures=0,next_attempt_at=GREATEST(t,next_attempt_at)
     WHERE id=jid;
    RETURN true;
END;
$$;
REVOKE ALL ON FUNCTION daily_reporting.resume_stock_collection(text,text,bigint,bigint,text,text)
    FROM PUBLIC, position_reader, report_worker, report_collector;

COMMIT;
