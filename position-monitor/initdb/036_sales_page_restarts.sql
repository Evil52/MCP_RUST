BEGIN;

-- Separate from consecutive_failures, which is reset by every saved page.
ALTER TABLE daily_reporting.source_collection_jobs ADD COLUMN page_restarts integer NOT NULL DEFAULT 0
    CHECK (page_restarts BETWEEN 0 AND 2);

CREATE FUNCTION daily_reporting.restart_overlapping_sales(jid bigint, gen bigint, owner text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, daily_reporting AS $$
DECLARE j daily_reporting.source_collection_jobs; t timestamptz := clock_timestamp();
    gate timestamptz; retry boolean;
BEGIN
    SELECT * INTO j FROM daily_reporting.source_collection_jobs
    WHERE id=jid AND generation=gen AND owner_id=owner AND source='sales'
      AND status='running' AND lease_until>t AND deadline_at>t FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    -- Recheck after acquiring the row lock; waiting must not extend a lease.
    t := clock_timestamp();
    IF j.lease_until <= t OR j.deadline_at <= t THEN RETURN false; END IF;
    retry := j.page_restarts < 2;
    SELECT next_allowed_at INTO gate FROM daily_reporting.source_collection_departures
    WHERE account_id=j.account_id AND marketplace=j.marketplace
      AND source=CASE WHEN j.marketplace='wildberries' THEN 'analytics' ELSE 'sales' END;
    -- No prior observations can be mixed with the replacement attempt.
    DELETE FROM daily_reporting.source_collection_pages WHERE job_id=jid;
    UPDATE daily_reporting.source_collection_jobs SET
        status=CASE WHEN retry THEN 'ready' ELSE 'failed' END,
        error_class=CASE WHEN retry THEN 'sales_page_overlap' ELSE 'sales_page_overlap_exhausted' END,
        page_restarts=page_restarts + CASE WHEN retry THEN 1 ELSE 0 END,
        completed_pages=CASE WHEN retry THEN 0 ELSE completed_pages END,
        first_observed_at=CASE WHEN retry THEN NULL ELSE first_observed_at END,
        last_observed_at=CASE WHEN retry THEN NULL ELSE last_observed_at END,
        consecutive_failures=0, cache_bytes=0, lease_until=NULL,
        next_attempt_at=GREATEST(t + interval '65 seconds',gate),
        finished_at=CASE WHEN retry THEN NULL ELSE t END
    WHERE id=jid;
    RETURN true;
END $$;
REVOKE ALL ON FUNCTION daily_reporting.restart_overlapping_sales(bigint,bigint,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION daily_reporting.restart_overlapping_sales(bigint,bigint,text) TO report_collector;
COMMIT;
