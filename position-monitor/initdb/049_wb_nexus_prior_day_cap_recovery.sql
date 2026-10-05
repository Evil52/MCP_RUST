-- Operator-only repair of a stale local pause; no WB request or policy change.
BEGIN;
CREATE OR REPLACE FUNCTION wb_automation.enforce_state_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_actions integer;
    pending_status text;
    pending_action_kind text;
    pending_reserved_at timestamptz;
BEGIN
    -- Only the operator procedure may clear an old daily-cap incident after
    -- a fresh independent read proves the campaign is already active today.
    -- The automatic writer cannot use this exception or manufacture its audit.
    IF TG_OP='UPDATE' AND current_user='position_admin'
       AND OLD.account_id='ofk_region_wb' AND OLD.advert_id=40141836
       AND OLD.incident_class='daily_spend_cap_breached'
       AND OLD.pending_idempotency_key IS NULL
       AND OLD.business_date < (clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date
       AND OLD.paused_for_daily_cap_on IS NOT NULL
       AND NEW.business_date=(clock_timestamp() AT TIME ZONE 'Europe/Moscow')::date
       AND NEW.actions_today=0 AND NEW.revision=OLD.revision+1
       AND NEW.incident_class IS NULL AND NEW.paused_for_daily_cap_on IS NULL
       AND (to_jsonb(NEW)-ARRAY['business_date','actions_today','revision','incident_class','paused_for_daily_cap_on','updated_at'])
           =(to_jsonb(OLD)-ARRAY['business_date','actions_today','revision','incident_class','paused_for_daily_cap_on','updated_at'])
       AND EXISTS (SELECT 1 FROM wb_automation.audit_events e
           WHERE e.account_id=OLD.account_id AND e.advert_id=OLD.advert_id
             AND e.event_type='operator_recovered_prior_day_cap'
             AND e.payload_json::jsonb->'previous_state'=to_jsonb(OLD)
             AND e.occurred_at BETWEEN clock_timestamp()-interval '5 seconds' AND clock_timestamp()) THEN
        NEW.updated_at:=clock_timestamp();
        RETURN NEW;
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.revision <> 1 THEN
            RAISE EXCEPTION 'WB automation initial revision must be one';
        END IF;
        IF NEW.pending_idempotency_key IS NOT NULL THEN
            SELECT status, reserved_at
            INTO pending_status, pending_reserved_at
            FROM wb_automation.action_attempts
            WHERE idempotency_key = NEW.pending_idempotency_key
              AND account_id = NEW.account_id
              AND advert_id = NEW.advert_id;
            IF pending_status NOT IN (
                'reserved', 'write_started', 'awaiting_readback',
                'reconciliation_required'
            ) OR NEW.last_action_at IS DISTINCT FROM pending_reserved_at THEN
                RAISE EXCEPTION 'initial WB automation pending state is unsafe';
            END IF;
        END IF;
        NEW.created_at := clock_timestamp();
        NEW.updated_at := NEW.created_at;
        RETURN NEW;
    END IF;

    IF OLD.paused_for_daily_cap_on IS NOT NULL
       AND NEW.paused_for_daily_cap_on IS NULL THEN
        SELECT status, action_kind
        INTO pending_status, pending_action_kind
        FROM wb_automation.action_attempts
        WHERE idempotency_key = OLD.pending_idempotency_key
          AND account_id = OLD.account_id
          AND advert_id = OLD.advert_id;
        IF (
            OLD.pending_idempotency_key IS NOT NULL
            AND NEW.pending_idempotency_key IS NULL
            AND pending_status = 'applied'
            AND pending_action_kind = 'resume_campaign_after_daily_cap'
        ) IS NOT TRUE THEN
            RAISE EXCEPTION
                'WB automation daily-cap pause requires an applied explicit resume';
        END IF;
    END IF;

    IF NEW.account_id <> OLD.account_id
       OR NEW.advert_id <> OLD.advert_id
       OR NEW.schema_version <> OLD.schema_version
       OR NEW.imported_legacy_digest
            IS DISTINCT FROM OLD.imported_legacy_digest
       OR NEW.created_at <> OLD.created_at
       OR NEW.business_date < OLD.business_date
       OR NEW.revision <> OLD.revision + 1
       OR (
           OLD.last_action_at IS NOT NULL
           AND (
               NEW.last_action_at IS NULL
               OR NEW.last_action_at < OLD.last_action_at
           )
       )
       OR (
           OLD.incident_class IS NOT NULL
           AND NEW.incident_class IS DISTINCT FROM OLD.incident_class
           AND NOT (
               current_user = 'position_admin'
               AND OLD.incident_class IN ('write_result_ambiguous','write_not_reconciled')
               AND NEW.incident_class IS NULL
               AND NEW.pending_idempotency_key IS NULL
               AND EXISTS (
                   SELECT 1 FROM wb_automation.action_attempts a
                   JOIN wb_automation.audit_events e
                     ON e.event_key=a.idempotency_key AND e.idempotency_key=a.idempotency_key
                   WHERE a.idempotency_key=OLD.pending_idempotency_key
                     AND a.account_id=OLD.account_id AND a.advert_id=OLD.advert_id
                     AND a.status='cancelled'
                     AND a.last_error_class='operator_closed_unconfirmed'
                     AND e.event_type='operator_closed_unconfirmed'
                     AND (e.payload_json::jsonb->>'expected_revision')::bigint=OLD.revision
               )
           )
       ) THEN
        RAISE EXCEPTION 'invalid WB automation state transition';
    END IF;

    IF NEW.policy_digest <> OLD.policy_digest AND (
        NEW.business_date IS DISTINCT FROM OLD.business_date
        OR NEW.actions_today IS DISTINCT FROM OLD.actions_today
        OR NEW.last_action_at IS DISTINCT FROM OLD.last_action_at
        OR NEW.paused_for_daily_cap_on
            IS DISTINCT FROM OLD.paused_for_daily_cap_on
        OR NEW.pending_idempotency_key
            IS DISTINCT FROM OLD.pending_idempotency_key
        OR NEW.incident_class IS DISTINCT FROM OLD.incident_class
    ) THEN
        RAISE EXCEPTION
            'WB automation policy migration must preserve safety state';
    END IF;

    expected_actions := CASE
        WHEN NEW.business_date > OLD.business_date THEN 0
        ELSE OLD.actions_today
    END;
    IF OLD.pending_idempotency_key IS NULL
       AND NEW.pending_idempotency_key IS NOT NULL THEN
        SELECT status, reserved_at
        INTO pending_status, pending_reserved_at
        FROM wb_automation.action_attempts
        WHERE idempotency_key = NEW.pending_idempotency_key
          AND account_id = NEW.account_id
          AND advert_id = NEW.advert_id;
        expected_actions := expected_actions + 1;
        IF pending_status <> 'reserved'
           OR OLD.incident_class IS NOT NULL
           OR NEW.incident_class IS NOT NULL
           OR NEW.last_action_at IS DISTINCT FROM pending_reserved_at THEN
            RAISE EXCEPTION 'WB automation pending reservation is unsafe';
        END IF;
    ELSIF OLD.pending_idempotency_key IS NOT NULL
          AND NEW.pending_idempotency_key IS NOT NULL
          AND NEW.pending_idempotency_key <> OLD.pending_idempotency_key THEN
        RAISE EXCEPTION 'WB automation pending action cannot be replaced';
    ELSIF OLD.pending_idempotency_key IS NOT NULL
          AND NEW.pending_idempotency_key IS NULL THEN
        SELECT status INTO pending_status
        FROM wb_automation.action_attempts
        WHERE idempotency_key = OLD.pending_idempotency_key
          AND account_id = OLD.account_id
          AND advert_id = OLD.advert_id;
        IF pending_status NOT IN ('applied', 'cancelled') THEN
            RAISE EXCEPTION
                'WB automation unresolved pending action cannot be cleared';
        END IF;
    END IF;
    IF NEW.actions_today <> expected_actions THEN
        RAISE EXCEPTION 'WB automation action counter transition is invalid';
    END IF;

    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END
$$;

CREATE FUNCTION wb_automation.recover_nexus_prior_day_cap(
    expected_revision bigint, expected_policy text, readback_id text,
    expected_bids jsonb, authorization_reference text
) RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE
    s wb_automation.execution_state;
    c wb_automation.cycles;
    o jsonb;
    bids jsonb;
    t timestamptz;
    today date;
    event_key text;
BEGIN
    IF current_user <> 'position_admin' OR authorization_reference IS NULL
       OR authorization_reference !~ '^[A-Za-z0-9_.:/-]{8,128}$'
       OR expected_bids IS NULL OR jsonb_typeof(expected_bids) <> 'object'
       OR NOT pg_try_advisory_xact_lock(hashtextextended('wb/ofk_region_wb/40141836',0)) THEN
        RAISE EXCEPTION 'operator authorization or campaign lease unavailable';
    END IF;
    t:=clock_timestamp(); today:=(t AT TIME ZONE 'Europe/Moscow')::date;
    SELECT * INTO STRICT s FROM wb_automation.execution_state
      WHERE account_id='ofk_region_wb' AND advert_id=40141836 FOR UPDATE;
    IF s.revision IS DISTINCT FROM expected_revision OR s.policy_digest IS DISTINCT FROM expected_policy
       OR s.incident_class IS DISTINCT FROM 'daily_spend_cap_breached'
       OR s.pending_idempotency_key IS NOT NULL
       OR s.paused_for_daily_cap_on IS NULL OR s.paused_for_daily_cap_on>=today
       OR s.business_date>=today
       OR EXISTS (SELECT 1 FROM wb_automation.action_attempts a
           WHERE a.account_id=s.account_id AND a.advert_id=s.advert_id
             AND a.status NOT IN ('applied','cancelled')) THEN
        RAISE EXCEPTION 'current-day cap or unresolved action cannot be recovered';
    END IF;
    SELECT * INTO STRICT c FROM wb_automation.cycles
      WHERE cycle_id=readback_id AND account_id=s.account_id AND advert_id=s.advert_id;
    IF c.policy_digest IS DISTINCT FROM expected_policy OR c.state_revision IS DISTINCT FROM expected_revision
       OR c.business_date<>today OR c.observed_at<t-interval '90 seconds' OR c.observed_at>t
       OR EXISTS (SELECT 1 FROM wb_automation.cycles newer
           WHERE newer.account_id=s.account_id AND newer.advert_id=s.advert_id
             AND (newer.observed_at,newer.cycle_id)>(c.observed_at,c.cycle_id)) THEN
        RAISE EXCEPTION 'fresh latest same-policy same-revision readback required';
    END IF;
    o:=c.snapshot_json::jsonb->'observation';
    SELECT jsonb_object_agg(x->>'nm_id',x->'current_bid_kopecks') INTO bids
      FROM jsonb_array_elements(o->'skus') x;
    IF (jsonb_array_length(o->'skus')=5 AND bids=expected_bids
        AND bids ?& ARRAY['190904855','207418966','218972074','455101276','529996417']
        AND NOT EXISTS (SELECT 1 FROM jsonb_each(bids) b
            WHERE jsonb_typeof(b.value) IS DISTINCT FROM 'number' OR b.value::text !~ '^[1-9][0-9]*$')
        AND (o->>'campaign_status')::int=9 AND (o->>'budget_remaining_minor')::bigint>0
        AND (o->>'daily_spend_complete')::boolean
        AND (o->>'daily_spend_minor')::bigint BETWEEN 0 AND 44999
        AND (o->>'paused_by_automation')::boolean) IS NOT TRUE THEN
        RAISE EXCEPTION 'active campaign, bids or existing protective threshold not confirmed';
    END IF;
    event_key:=md5('nexus-prior-cap/'||s.revision::text)||md5('nexus-prior-cap/'||s.policy_digest);
    INSERT INTO wb_automation.audit_events(event_key,cycle_id,account_id,advert_id,event_type,payload_json)
    VALUES(event_key,c.cycle_id,s.account_id,s.advert_id,'operator_recovered_prior_day_cap',
        jsonb_build_object('authorization_reference',authorization_reference,'previous_state',to_jsonb(s),
          'preserved_bids',bids,'daily_spend_minor',o->'daily_spend_minor',
          'marketplace_write_sent',false,'policy_changed',false)::text);
    UPDATE wb_automation.execution_state SET business_date=today,actions_today=0,
      paused_for_daily_cap_on=NULL,incident_class=NULL,revision=revision+1
      WHERE account_id=s.account_id AND advert_id=s.advert_id;
    RETURN jsonb_build_object('outcome','prior_day_cap_recovered','campaign_id',s.advert_id,
      'state_revision',s.revision+1,'preserved_bids',bids,'marketplace_write_sent',false,'policy_changed',false);
END;
$$;
REVOKE ALL ON FUNCTION wb_automation.recover_nexus_prior_day_cap(bigint,text,text,jsonb,text)
    FROM PUBLIC, position_reader, wb_automation_writer;
COMMIT;
