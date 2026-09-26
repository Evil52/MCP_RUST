-- A completed campaign remains locked. Archive its historical cap incident
-- only while fresh observations prove it is terminal and unauthorized to run.
BEGIN;
CREATE TABLE wb_automation.terminal_incident_archives (
    account_id varchar(128) NOT NULL,
    advert_id bigint NOT NULL,
    state_revision bigint NOT NULL,
    policy_digest varchar(64) NOT NULL,
    incident_class varchar(64) NOT NULL,
    readback_cycle_id varchar(64) NOT NULL REFERENCES wb_automation.cycles(cycle_id),
    archived_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    archived_by text NOT NULL,
    authorization_reference text NOT NULL,
    PRIMARY KEY(account_id,advert_id,state_revision),
    FOREIGN KEY(account_id,advert_id) REFERENCES wb_automation.execution_state(account_id,advert_id)
);
REVOKE ALL ON wb_automation.terminal_incident_archives FROM PUBLIC, position_reader, wb_automation_writer;

CREATE TRIGGER terminal_incident_archives_immutable BEFORE UPDATE OR DELETE ON wb_automation.terminal_incident_archives
FOR EACH ROW EXECUTE FUNCTION daily_reporting.reject_recovery_audit_change();

CREATE FUNCTION wb_automation.archive_terminal_incident(
    expected_account text, expected_advert bigint, expected_revision bigint,
    expected_policy text, readback_id text, authorization_reference text
) RETURNS boolean LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE s wb_automation.execution_state; c wb_automation.cycles; o jsonb;
BEGIN
    IF current_user <> 'position_admin' OR expected_account IS NULL OR expected_advert IS NULL
       OR expected_revision IS NULL OR expected_policy IS NULL OR readback_id IS NULL
       OR authorization_reference IS NULL OR authorization_reference !~ '^[A-Za-z0-9_.:/-]{8,128}$'
       OR NOT pg_try_advisory_xact_lock(hashtextextended('wb/'||expected_account||'/'||expected_advert::text,0)) THEN
        RAISE EXCEPTION 'terminal incident authorization or campaign lease unavailable';
    END IF;
    SELECT * INTO s FROM wb_automation.execution_state
      WHERE account_id=expected_account AND advert_id=expected_advert FOR UPDATE;
    IF NOT FOUND OR s.revision IS DISTINCT FROM expected_revision
       OR s.policy_digest IS DISTINCT FROM expected_policy
       OR s.incident_class IS DISTINCT FROM 'daily_spend_cap_breached'
       OR s.pending_idempotency_key IS NOT NULL OR s.paused_for_daily_cap_on IS NOT NULL THEN
        RETURN false;
    END IF;
    SELECT * INTO c FROM wb_automation.cycles WHERE cycle_id=readback_id
      AND account_id=s.account_id AND advert_id=s.advert_id;
    IF NOT FOUND OR c.policy_digest<>expected_policy OR c.state_revision<>expected_revision
       OR c.observed_at<clock_timestamp()-interval '90 seconds' OR c.observed_at>clock_timestamp()
       OR EXISTS (SELECT 1 FROM wb_automation.cycles newer
           WHERE newer.account_id=s.account_id AND newer.advert_id=s.advert_id
             AND (newer.observed_at,newer.cycle_id)>(c.observed_at,c.cycle_id)) THEN
        RETURN false;
    END IF;
    o:=c.snapshot_json::jsonb->'observation';
    IF ((o->>'campaign_status')::int=7 AND (o->>'budget_remaining_minor')::bigint=0
        AND NOT (o->>'paused_by_automation')::boolean
        AND c.decision_json::jsonb#>>'{action,hold,reason}'='authorization_expired') IS NOT TRUE THEN
        RETURN false;
    END IF;
    INSERT INTO wb_automation.terminal_incident_archives
        (account_id,advert_id,state_revision,policy_digest,incident_class,readback_cycle_id,archived_by,authorization_reference)
    VALUES(s.account_id,s.advert_id,s.revision,s.policy_digest,s.incident_class,c.cycle_id,session_user,authorization_reference)
    ON CONFLICT DO NOTHING;
    RETURN FOUND;
END;
$$;
REVOKE ALL ON FUNCTION wb_automation.archive_terminal_incident(text,bigint,bigint,text,text,text)
    FROM PUBLIC, position_reader, wb_automation_writer;

CREATE VIEW wb_automation.open_incidents WITH (security_invoker=true) AS
SELECT s.account_id,s.advert_id,s.incident_class
FROM wb_automation.execution_state s
WHERE s.incident_class IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM wb_automation.terminal_incident_archives a
    CROSS JOIN LATERAL (
        SELECT c.observed_at,c.state_revision,c.policy_digest,c.snapshot_json,c.decision_json
        FROM wb_automation.cycles c WHERE c.account_id=s.account_id AND c.advert_id=s.advert_id
        ORDER BY c.observed_at DESC,c.cycle_id DESC LIMIT 1
    ) latest
    WHERE a.account_id=s.account_id AND a.advert_id=s.advert_id AND a.state_revision=s.revision
      AND a.policy_digest=s.policy_digest AND a.incident_class=s.incident_class
      AND s.pending_idempotency_key IS NULL AND s.paused_for_daily_cap_on IS NULL
      AND latest.state_revision=s.revision AND latest.policy_digest=s.policy_digest
      AND latest.observed_at BETWEEN clock_timestamp()-interval '30 minutes' AND clock_timestamp()
      AND (latest.snapshot_json::jsonb#>>'{observation,campaign_status}')::int=7
      AND (latest.snapshot_json::jsonb#>>'{observation,budget_remaining_minor}')::bigint=0
      AND (latest.snapshot_json::jsonb#>>'{observation,paused_by_automation}')::boolean=false
      AND latest.decision_json::jsonb#>>'{action,hold,reason}'='authorization_expired'
);
REVOKE ALL ON wb_automation.open_incidents FROM PUBLIC, position_reader, wb_automation_writer;
COMMIT;
