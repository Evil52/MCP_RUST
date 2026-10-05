-- Known pre-HTTP failures may cancel without an incident. Preserve the
-- durable permit timestamp; awaiting/readback and ambiguous outcomes cannot cancel.
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
                AND ((last_error_class='operator_closed_unconfirmed'
                      AND readback_cycle_id IS NOT NULL)
                     OR (last_error_class='write_not_sent'
                         AND readback_cycle_id IS NULL))
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
    ELSIF OLD.status = 'write_started' AND NEW.status = 'cancelled' THEN
        IF NEW.last_error_class IS DISTINCT FROM 'write_not_sent'
           OR NEW.readback_cycle_id IS NOT NULL THEN
            RAISE EXCEPTION 'write_started cancellation requires proven non-departure';
        END IF;
        NEW.write_started_at := OLD.write_started_at;
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

COMMIT;
