-- Reopen one prematurely terminal WB advertising job after fixing the cutoff-date
-- retry check. The failed response remains visible in this operator-only audit.
BEGIN;
CREATE TABLE daily_reporting.advertising_collection_resumes (
    job_id bigint PRIMARY KEY REFERENCES daily_reporting.source_collection_jobs(id) ON DELETE RESTRICT,
    failed_generation bigint NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    requested_by text NOT NULL,
    previous_error_class text NOT NULL,
    reason text NOT NULL
);
REVOKE ALL ON daily_reporting.advertising_collection_resumes FROM PUBLIC, position_reader, report_worker, report_collector;

CREATE FUNCTION daily_reporting.resume_failed_wb_advertising(
    expected_account text, jid bigint, expected_generation bigint, expected_cutoff timestamptz, resume_reason text
) RETURNS boolean LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog AS $$
DECLARE j daily_reporting.source_collection_jobs; t timestamptz := clock_timestamp();
BEGIN
    IF current_user <> 'position_admin' OR expected_account IS NULL
       OR expected_account !~ '^[A-Za-z0-9_-]{1,128}$' OR jid IS NULL OR expected_generation IS NULL
       OR expected_cutoff IS NULL OR resume_reason IS NULL
       OR resume_reason !~ '^[A-Za-z0-9 _.:/-]{8,128}$' THEN
        RAISE EXCEPTION 'invalid advertising recovery authorization or scope';
    END IF;
    SELECT * INTO j FROM daily_reporting.source_collection_jobs
      WHERE id=jid AND account_id=expected_account AND marketplace='wildberries'
        AND source='advertising' AND cutoff_at=expected_cutoff
        AND generation=expected_generation AND status='failed'
        AND error_class='promotion_counts_inconsistent' AND consecutive_failures=1
        AND generation<2147483647 AND deadline_at>t
        AND cutoff_at<=t
        AND (cutoff_at AT TIME ZONE 'Asia/Yekaterinburg')::date =
            (t AT TIME ZONE 'Asia/Yekaterinburg')::date
      FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    t := clock_timestamp();
    IF j.deadline_at<=t OR EXISTS (
        SELECT 1 FROM daily_reporting.source_snapshots s
        WHERE s.account_id=j.account_id AND s.marketplace=j.marketplace
          AND s.source=j.source AND s.cutoff_at=j.cutoff_at
          AND s.status='succeeded' AND s.pagination_complete
    ) OR EXISTS (
        SELECT 1 FROM daily_reporting.source_collection_pages p WHERE p.job_id=jid
    ) THEN RETURN false; END IF;
    INSERT INTO daily_reporting.advertising_collection_resumes
        (job_id,failed_generation,requested_by,previous_error_class,reason)
    VALUES(jid,expected_generation,session_user,j.error_class,resume_reason);
    UPDATE daily_reporting.source_collection_jobs SET
        status='ready',owner_id=NULL,lease_until=NULL,finished_at=NULL,
        completed_pages=0,cache_bytes=0,next_attempt_at=t
      WHERE id=jid;
    RETURN true;
END;
$$;
REVOKE ALL ON FUNCTION daily_reporting.resume_failed_wb_advertising(text,bigint,bigint,timestamptz,text)
    FROM PUBLIC, position_reader, report_worker, report_collector;
COMMIT;
