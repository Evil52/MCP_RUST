-- Audited administrative closure is not proof that the old WB write applied.
-- No new grants to the automatic runtime; no marketplace requests.
BEGIN;
ALTER TABLE wb_automation.action_attempts
DROP CONSTRAINT wb_automation_action_state_shape;
ALTER TABLE wb_automation.action_attempts ADD CONSTRAINT wb_automation_action_state_shape CHECK (
        (
            status = 'reserved'
            AND write_started_at IS NULL
            AND resolved_at IS NULL
            AND readback_cycle_id IS NULL
            AND last_error_class IS NULL
        ) OR (
            status IN ('write_started', 'awaiting_readback')
            AND write_started_at IS NOT NULL
            AND resolved_at IS NULL
            AND readback_cycle_id IS NULL
            AND last_error_class IS NULL
        ) OR (
            status = 'applied'
            AND write_started_at IS NOT NULL
            AND resolved_at IS NOT NULL
            AND readback_cycle_id IS NOT NULL
            AND last_error_class IS NULL
        ) OR (
            status = 'reconciliation_required'
            AND write_started_at IS NOT NULL
            AND resolved_at IS NULL
            AND last_error_class IS NOT NULL
        ) OR (
            status = 'cancelled'
            AND (write_started_at IS NULL OR (
                write_started_at IS NOT NULL
                AND last_error_class='operator_closed_unconfirmed'
                AND readback_cycle_id IS NOT NULL
            ))
            AND resolved_at IS NOT NULL
            AND last_error_class IS NOT NULL
        )
    );
CREATE OR REPLACE FUNCTION wb_automation.enforce_action_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'reserved'
           OR NEW.write_started_at IS NOT NULL
           OR NEW.resolved_at IS NOT NULL
           OR NEW.readback_cycle_id IS NOT NULL
           OR NEW.last_error_class IS NOT NULL THEN
            RAISE EXCEPTION 'WB automation action must start reserved';
        END IF;
        NEW.reserved_at := clock_timestamp();
        RETURN NEW;
    END IF;

    IF NEW.idempotency_key <> OLD.idempotency_key
       OR NEW.account_id <> OLD.account_id
       OR NEW.advert_id <> OLD.advert_id
       OR NEW.cycle_id <> OLD.cycle_id
       OR NEW.policy_digest <> OLD.policy_digest
       OR NEW.request_digest <> OLD.request_digest
       OR NEW.action_kind <> OLD.action_kind
       OR NEW.request_json::jsonb <> OLD.request_json::jsonb
       OR NEW.reserved_at <> OLD.reserved_at THEN
        RAISE EXCEPTION 'WB automation action identity is immutable';
    END IF;

    IF OLD.status = 'reconciliation_required' AND NEW.status = 'cancelled' THEN
        IF current_user <> 'position_admin'
           OR NEW.last_error_class <> 'operator_closed_unconfirmed'
           OR NEW.readback_cycle_id IS NULL
           OR NOT EXISTS (
               SELECT 1 FROM wb_automation.audit_events e
               WHERE e.event_key=OLD.idempotency_key
                 AND e.idempotency_key=OLD.idempotency_key
                 AND e.account_id=OLD.account_id AND e.advert_id=OLD.advert_id
                 AND e.cycle_id=NEW.readback_cycle_id
                 AND e.event_type='operator_closed_unconfirmed'
           ) THEN
            RAISE EXCEPTION 'unconfirmed close requires an audited administrator recovery';
        END IF;
        NEW.write_started_at := OLD.write_started_at;
        NEW.resolved_at := clock_timestamp();
    ELSIF OLD.status = 'reserved' AND NEW.status = 'write_started' THEN
        NEW.write_started_at := clock_timestamp();
        NEW.resolved_at := NULL;
        NEW.readback_cycle_id := NULL;
        NEW.last_error_class := NULL;
    ELSIF OLD.status = 'reserved' AND NEW.status = 'cancelled' THEN
        IF NEW.last_error_class IS NULL THEN
            RAISE EXCEPTION 'cancelled WB automation action requires a reason';
        END IF;
        NEW.write_started_at := NULL;
        NEW.resolved_at := clock_timestamp();
    ELSIF OLD.status = 'write_started'
          AND NEW.status = 'awaiting_readback' THEN
        NEW.write_started_at := OLD.write_started_at;
        NEW.resolved_at := NULL;
        NEW.readback_cycle_id := NULL;
        NEW.last_error_class := NULL;
    ELSIF OLD.status IN ('write_started', 'awaiting_readback')
          AND NEW.status = 'reconciliation_required' THEN
        IF NEW.last_error_class IS NULL THEN
            RAISE EXCEPTION 'WB automation reconciliation requires a reason';
        END IF;
        NEW.write_started_at := OLD.write_started_at;
        NEW.resolved_at := NULL;
    ELSIF OLD.status IN (
        'write_started', 'awaiting_readback', 'reconciliation_required'
    ) AND NEW.status = 'applied' THEN
        IF NEW.readback_cycle_id IS NULL THEN
            RAISE EXCEPTION 'applied WB automation action requires readback';
        END IF;
        NEW.write_started_at := OLD.write_started_at;
        NEW.resolved_at := clock_timestamp();
        NEW.last_error_class := NULL;
    ELSE
        RAISE EXCEPTION 'invalid WB automation action transition % -> %',
            OLD.status, NEW.status;
    END IF;
    RETURN NEW;
END
$$;

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

-- One narrowly scoped operator entry point, never callable by the robot.
CREATE FUNCTION wb_automation.recover_oduvanchik_unconfirmed_bid(
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
       OR NOT pg_try_advisory_xact_lock(hashtextextended('wb/ofk_region_wb/39807762',0)) THEN
        RAISE EXCEPTION 'operator authorization or campaign lease unavailable';
    END IF;
    SELECT * INTO STRICT s FROM wb_automation.execution_state
      WHERE account_id='ofk_region_wb' AND advert_id=39807762 FOR UPDATE;
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
        AND actual_bids ?& ARRAY['38943938','41774347','44081434','44081446','99236811']
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
REVOKE ALL ON FUNCTION wb_automation.recover_oduvanchik_unconfirmed_bid(text,bigint,text,text,jsonb,text) FROM PUBLIC;
COMMIT;
