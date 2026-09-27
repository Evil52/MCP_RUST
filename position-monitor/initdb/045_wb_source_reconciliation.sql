-- Keep the exact source amounts and the independent reconciliation proof.
BEGIN;
CREATE TABLE daily_reporting.snapshot_reconciliation_evidence (
    snapshot_id bigint PRIMARY KEY REFERENCES daily_reporting.source_snapshots(id) ON DELETE RESTRICT,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence)='object' AND octet_length(evidence::text)<2048)
);
REVOKE ALL ON daily_reporting.snapshot_reconciliation_evidence FROM PUBLIC, position_reader, report_worker, report_collector;
GRANT SELECT, INSERT ON daily_reporting.snapshot_reconciliation_evidence TO report_collector;
GRANT SELECT ON daily_reporting.snapshot_reconciliation_evidence TO position_reader;
CREATE TRIGGER snapshot_reconciliation_evidence_immutable BEFORE UPDATE OR DELETE ON daily_reporting.snapshot_reconciliation_evidence
FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_recovery_audit_change();

CREATE FUNCTION daily_reporting.validate_sales_reconciliation_evidence()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE s daily_reporting.source_snapshots; e jsonb:=NEW.evidence; units numeric; amount numeric; skus bigint;
BEGIN
    SELECT * INTO s FROM daily_reporting.source_snapshots WHERE id=NEW.snapshot_id;
    IF NOT FOUND OR s.marketplace<>'wildberries' OR s.source<>'sales'
       OR s.status<>'succeeded' OR NOT s.pagination_complete THEN
        RAISE EXCEPTION 'invalid reconciliation snapshot';
    END IF;
    IF e->>'kind' IS DISTINCT FROM 'wb_sales_whole_ruble_v1'
       OR e->'history_matches' IS DISTINCT FROM 'true'::jsonb
       OR e->'group_stable' IS DISTINCT FROM 'true'::jsonb
       OR NOT (e ?& ARRAY['business_date','ordered_units','sku_gmv_minor','group_gmv_minor','verified_skus'])
       OR (e->>'business_date')::date IS DISTINCT FROM (s.period_start AT TIME ZONE 'Asia/Yekaterinburg')::date
       OR e->>'group_gmv_minor' IS NULL OR e->>'group_gmv_minor' !~ '^[0-9]+$'
       OR (e->>'ordered_units')::numeric<=0 OR (e->>'verified_skus')::numeric<=0
       OR mod((e->>'sku_gmv_minor')::numeric,100)<>0
       OR mod((e->>'group_gmv_minor')::numeric,100)<>0
       OR abs((e->>'sku_gmv_minor')::numeric-(e->>'group_gmv_minor')::numeric)<>100 THEN
        RAISE EXCEPTION 'invalid WB reconciliation certificate';
    END IF;
    SELECT sum(ordered_units),sum(operational_gmv_minor),count(*) FILTER (WHERE ordered_units>0)
      INTO units,amount,skus FROM daily_reporting.sales_facts WHERE snapshot_id=NEW.snapshot_id;
    IF units IS DISTINCT FROM (e->>'ordered_units')::numeric
       OR amount IS DISTINCT FROM (e->>'sku_gmv_minor')::numeric
       OR skus IS DISTINCT FROM (e->>'verified_skus')::bigint THEN
        RAISE EXCEPTION 'reconciliation differs from immutable sales';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION daily_reporting.validate_sales_reconciliation_evidence() FROM PUBLIC;
CREATE TRIGGER snapshot_reconciliation_evidence_valid BEFORE INSERT ON daily_reporting.snapshot_reconciliation_evidence
FOR EACH ROW EXECUTE FUNCTION daily_reporting.validate_sales_reconciliation_evidence();

CREATE TABLE daily_reporting.advertising_reconciliation_resumes (
    job_id bigint PRIMARY KEY REFERENCES daily_reporting.source_collection_jobs(id) ON DELETE RESTRICT,
    failed_generation bigint NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    requested_by text NOT NULL,
    previous_job jsonb NOT NULL,
    reason text NOT NULL
);
REVOKE ALL ON daily_reporting.advertising_reconciliation_resumes FROM PUBLIC,position_reader,report_worker,report_collector;
CREATE TRIGGER advertising_reconciliation_resumes_immutable BEFORE UPDATE OR DELETE ON daily_reporting.advertising_reconciliation_resumes
FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_recovery_audit_change();
CREATE FUNCTION daily_reporting.resume_wb_advertising_reconciliation(
    expected_account text, jid bigint, expected_generation bigint, expected_cutoff timestamptz, resume_reason text
) RETURNS boolean LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE j daily_reporting.source_collection_jobs; t timestamptz:=clock_timestamp(); gate timestamptz;
BEGIN
    IF current_user<>'position_admin' OR expected_account IS NULL
       OR expected_account !~ '^[A-Za-z0-9_-]{1,128}$' OR jid IS NULL
       OR expected_generation IS NULL OR expected_cutoff IS NULL
       OR resume_reason IS NULL OR resume_reason !~ '^[A-Za-z0-9 _.:/-]{8,128}$' THEN
        RAISE EXCEPTION 'invalid advertising reconciliation recovery scope';
    END IF;
    SELECT * INTO j FROM daily_reporting.source_collection_jobs
      WHERE id=jid AND account_id=expected_account AND marketplace='wildberries' AND source='advertising'
        AND generation=expected_generation AND cutoff_at=expected_cutoff AND cutoff_at<=t
        AND status='failed' AND error_class='promotion_counts_inconsistent' AND consecutive_failures=8
        AND generation<2147483647 AND deadline_at>t FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    t:=clock_timestamp();
    IF j.deadline_at<=t OR EXISTS (SELECT 1 FROM daily_reporting.advertising_reconciliation_resumes WHERE job_id=jid)
       OR EXISTS (SELECT 1 FROM daily_reporting.source_collection_pages WHERE job_id=jid)
       OR EXISTS (SELECT 1 FROM daily_reporting.source_snapshots s WHERE s.account_id=j.account_id
          AND s.marketplace=j.marketplace AND s.source=j.source AND s.cutoff_at=j.cutoff_at
          AND s.status='succeeded' AND s.pagination_complete) THEN RETURN false; END IF;
    SELECT next_allowed_at INTO gate FROM daily_reporting.source_collection_departures
      WHERE account_id=j.account_id AND marketplace=j.marketplace AND source='advertising';
    INSERT INTO daily_reporting.advertising_reconciliation_resumes(job_id,failed_generation,requested_by,previous_job,reason)
      VALUES(jid,expected_generation,session_user,to_jsonb(j),resume_reason);
    UPDATE daily_reporting.source_collection_jobs SET status='ready',owner_id=NULL,lease_until=NULL,finished_at=NULL,
        first_observed_at=NULL,last_observed_at=NULL,completed_pages=0,cache_bytes=0,consecutive_failures=0,
        next_attempt_at=GREATEST(t+interval '65 seconds',gate) WHERE id=jid;
    RETURN true;
END;
$$;
REVOKE ALL ON FUNCTION daily_reporting.resume_wb_advertising_reconciliation(text,bigint,bigint,timestamptz,text)
    FROM PUBLIC,position_reader,report_worker,report_collector;
COMMIT;
