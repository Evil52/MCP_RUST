-- One audited operator recovery after a failed, inconsistent page chain.
-- Invalid pages never become snapshots; automatic retry/deadline bounds remain.
BEGIN;
CREATE TABLE daily_reporting.sales_collection_resumes (
    job_id bigint PRIMARY KEY REFERENCES daily_reporting.source_collection_jobs(id) ON DELETE RESTRICT,
    failed_generation bigint NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    requested_by text NOT NULL,
    previous_job jsonb NOT NULL,
    reason text NOT NULL
);
REVOKE ALL ON daily_reporting.sales_collection_resumes FROM PUBLIC, position_reader, report_worker, report_collector;

CREATE FUNCTION daily_reporting.reject_recovery_audit_change()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN RAISE EXCEPTION 'operator recovery audit is append-only'; END;
$$;
REVOKE ALL ON FUNCTION daily_reporting.reject_recovery_audit_change() FROM PUBLIC;
CREATE TRIGGER sales_collection_resumes_immutable BEFORE UPDATE OR DELETE ON daily_reporting.sales_collection_resumes
FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_recovery_audit_change();

CREATE FUNCTION daily_reporting.resume_failed_sales(
    expected_account text, expected_marketplace text, jid bigint,
    expected_generation bigint, expected_cutoff timestamptz, resume_reason text
) RETURNS boolean LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog AS $$
DECLARE j daily_reporting.source_collection_jobs; t timestamptz := clock_timestamp(); gate timestamptz;
BEGIN
    IF current_user <> 'position_admin' OR expected_account IS NULL
       OR expected_account !~ '^[A-Za-z0-9_-]{1,128}$'
       OR expected_marketplace IS NULL OR expected_marketplace NOT IN ('ozon','wildberries')
       OR jid IS NULL OR expected_generation IS NULL OR expected_cutoff IS NULL
       OR resume_reason IS NULL OR resume_reason !~ '^[A-Za-z0-9 _.:/-]{8,128}$' THEN
        RAISE EXCEPTION 'invalid sales recovery authorization or scope';
    END IF;
    SELECT * INTO j FROM daily_reporting.source_collection_jobs
      WHERE id=jid AND account_id=expected_account AND marketplace=expected_marketplace
        AND source='sales' AND cutoff_at=expected_cutoff AND generation=expected_generation
        AND status='failed' AND error_class='sales_page_overlap_exhausted' AND page_restarts=2
        AND generation<2147483647 AND deadline_at>t AND cutoff_at<=t
      FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    t := clock_timestamp();
    IF j.deadline_at<=t OR EXISTS (
        SELECT 1 FROM daily_reporting.source_snapshots s
        WHERE s.account_id=j.account_id AND s.marketplace=j.marketplace
          AND s.source=j.source AND s.cutoff_at=j.cutoff_at
          AND s.status='succeeded' AND s.pagination_complete
    ) OR EXISTS (SELECT 1 FROM daily_reporting.sales_collection_resumes WHERE job_id=jid)
      OR EXISTS (SELECT 1 FROM daily_reporting.source_collection_pages WHERE job_id=jid)
    THEN RETURN false; END IF;
    SELECT next_allowed_at INTO gate FROM daily_reporting.source_collection_departures
      WHERE account_id=j.account_id AND marketplace=j.marketplace
        AND source=CASE WHEN j.marketplace='wildberries' THEN 'analytics' ELSE 'sales' END;
    INSERT INTO daily_reporting.sales_collection_resumes
        (job_id,failed_generation,requested_by,previous_job,reason)
    VALUES(jid,expected_generation,session_user,to_jsonb(j),resume_reason);
    UPDATE daily_reporting.source_collection_jobs SET
        status='ready',owner_id=NULL,lease_until=NULL,finished_at=NULL,
        first_observed_at=NULL,last_observed_at=NULL,completed_pages=0,cache_bytes=0,
        page_restarts=0,consecutive_failures=0,next_attempt_at=GREATEST(t+interval '65 seconds',gate)
      WHERE id=jid;
    RETURN true;
END;
$$;
REVOKE ALL ON FUNCTION daily_reporting.resume_failed_sales(text,text,bigint,bigint,timestamptz,text)
    FROM PUBLIC, position_reader, report_worker, report_collector;
COMMIT;
