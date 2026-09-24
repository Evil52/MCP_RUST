-- Operator-only closure of one unconfirmed Nexus bid write. No marketplace request.
-- Existing write and state triggers retain the original request and audit.
BEGIN;
CREATE FUNCTION wb_automation.recover_nexus_unconfirmed_bid(
    expected_key text, expected_revision bigint, expected_policy text,
    readback_id text, expected_bids jsonb, authorization_reference text
) RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER
SET search_path = pg_catalog, wb_automation AS $$
DECLARE
    s wb_automation.execution_state%ROWTYPE;
    a wb_automation.action_attempts%ROWTYPE;
    c wb_automation.cycles%ROWTYPE;
    o jsonb;
    actual_bids jsonb;
    today date;
BEGIN
    IF current_user <> 'position_admin' OR authorization_reference IS NULL
       OR authorization_reference !~ '^[A-Za-z0-9_.:/-]{8,128}$'
       OR expected_bids IS NULL OR jsonb_typeof(expected_bids) <> 'object'
       OR NOT pg_try_advisory_xact_lock(hashtextextended('wb/ofk_region_wb/40141836',0)) THEN
        RAISE EXCEPTION 'operator authorization or campaign lease unavailable';
    END IF;
    SELECT * INTO STRICT s FROM wb_automation.execution_state
      WHERE account_id='ofk_region_wb' AND advert_id=40141836 FOR UPDATE;
    IF s.revision IS DISTINCT FROM expected_revision
       OR s.policy_digest IS DISTINCT FROM expected_policy
       OR s.pending_idempotency_key IS DISTINCT FROM expected_key
       OR s.incident_class NOT IN ('write_result_ambiguous','write_not_reconciled')
       OR s.incident_class IS NULL OR s.paused_for_daily_cap_on IS NOT NULL THEN
        RAISE EXCEPTION 'incident identity changed or protected pause exists';
    END IF;
    SELECT * INTO STRICT a FROM wb_automation.action_attempts
      WHERE idempotency_key=expected_key AND account_id=s.account_id AND advert_id=s.advert_id FOR UPDATE;
    IF a.status <> 'reconciliation_required' OR a.action_kind <> 'change_bids'
       OR a.policy_digest <> expected_policy
       OR a.write_started_at IS NULL
       OR a.write_started_at > clock_timestamp()-interval '30 seconds'
       OR a.last_error_class NOT IN ('write_result_ambiguous','write_not_reconciled') THEN
        RAISE EXCEPTION 'only settled-in-time ambiguous bid writes can be reviewed';
    END IF;
    SELECT * INTO STRICT c FROM wb_automation.cycles
      WHERE cycle_id=readback_id AND account_id=s.account_id AND advert_id=s.advert_id;
    IF c.policy_digest <> expected_policy OR c.state_revision <> expected_revision
       OR c.observed_at < clock_timestamp()-interval '90 seconds'
       OR c.observed_at > clock_timestamp() OR c.observed_at <= a.write_started_at THEN
        RAISE EXCEPTION 'fresh same-revision independent readback required';
    END IF;
    o := c.snapshot_json::jsonb->'observation';
    SELECT jsonb_object_agg(x->>'nm_id',x->'current_bid_kopecks') INTO actual_bids
      FROM jsonb_array_elements(o->'skus') x;
    IF (jsonb_array_length(o->'skus')=5
        AND actual_bids=expected_bids
        AND actual_bids ?& ARRAY['190904855','207418966','218972074','455101276','529996417']
        AND (o->>'campaign_status')::int=9
        AND (o->>'budget_remaining_minor')::bigint>0
        AND (o->>'daily_spend_complete')::boolean
        AND (o->>'daily_spend_minor')::bigint BETWEEN 0 AND 44999
        AND NOT (o->>'paused_by_automation')::boolean) IS NOT TRUE THEN
        RAISE EXCEPTION 'current bids, budget or protective spend evidence incompatible';
    END IF;
    IF actual_bids->(a.request_json::jsonb#>>'{changes,0,nm_id}')
         = a.request_json::jsonb#>'{changes,0,to_bid_kopecks}' THEN
        RAISE EXCEPTION 'target bid is visible; use normal reconciliation';
    END IF;
    today := (clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date;
    IF today <> c.business_date THEN RAISE EXCEPTION 'readback crosses business day'; END IF;
    INSERT INTO wb_automation.audit_events(event_key,cycle_id,account_id,advert_id,
        event_type,idempotency_key,payload_json)
    VALUES(expected_key,readback_id,s.account_id,s.advert_id,'operator_closed_unconfirmed',expected_key,
        jsonb_build_object('authorization_reference',authorization_reference,
          'expected_revision',expected_revision,'previous_error',a.last_error_class,
          'preserved_bids',actual_bids,'marketplace_write_sent',false,
          'old_write_applied','unknown','retry_old_request',false)::text);
    UPDATE wb_automation.action_attempts SET status='cancelled',
      readback_cycle_id=readback_id,last_error_class='operator_closed_unconfirmed'
      WHERE idempotency_key=expected_key;
    UPDATE wb_automation.execution_state SET pending_idempotency_key=NULL,incident_class=NULL,
      revision=revision+1,business_date=today,
      actions_today=CASE WHEN today>s.business_date THEN 0 ELSE s.actions_today END,
      last_action_at=clock_timestamp()
      WHERE account_id=s.account_id AND advert_id=s.advert_id;
    RETURN jsonb_build_object('outcome','closed_unconfirmed','campaign_id',s.advert_id,
      'state_revision',s.revision+1,'preserved_bids',actual_bids,'marketplace_write_sent',false);
END $$;
REVOKE ALL ON FUNCTION wb_automation.recover_nexus_unconfirmed_bid(text,bigint,text,text,jsonb,text) FROM PUBLIC;
COMMIT;
