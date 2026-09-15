BEGIN;

-- Quota choice is fixed by the collector operation, never an MCP argument.
ALTER TABLE daily_reporting.source_collection_jobs
    ADD COLUMN active_quota_source text CHECK (active_quota_source IN ('wb_stock_content','wb_seller_inventory'));

CREATE FUNCTION daily_reporting.admit_source_page(jid bigint, gen bigint, owner text, quota text)
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
    IF quota <> 'default' AND (j.marketplace <> 'wildberries' OR j.source <> 'stocks') THEN
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

-- Existing callers retain the original pacing and reset the previous quota.
CREATE OR REPLACE FUNCTION daily_reporting.admit_source_page(jid bigint, gen bigint, owner text)
RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT daily_reporting.admit_source_page(jid,gen,owner,'default');
$$;

REVOKE ALL ON FUNCTION daily_reporting.admit_source_page(bigint,bigint,text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION daily_reporting.admit_source_page(bigint,bigint,text,text) TO report_collector;

CREATE OR REPLACE FUNCTION daily_reporting.defer_source_collection(jid bigint, gen bigint, owner text, failure text, wait_seconds integer, terminal boolean)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE j daily_reporting.source_collection_jobs; t timestamptz := clock_timestamp(); gate timestamptz; quota_source text;
BEGIN
    IF wait_seconds IS NULL OR wait_seconds NOT BETWEEN 1 AND 2147483647 OR terminal IS NULL THEN RAISE EXCEPTION 'invalid collection delay'; END IF;
    SELECT * INTO j FROM daily_reporting.source_collection_jobs WHERE id=jid AND generation=gen
        AND owner_id=owner AND status='running' AND lease_until>t FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    t := clock_timestamp();
    IF j.lease_until <= t THEN RETURN false; END IF;
    quota_source := COALESCE(j.active_quota_source, CASE WHEN j.marketplace='wildberries' AND j.source IN ('sales','stocks') THEN 'analytics' ELSE j.source END);
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

COMMIT;
