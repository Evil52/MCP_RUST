BEGIN;

ALTER TABLE wb_automation.action_attempts
    DROP CONSTRAINT IF EXISTS action_attempts_action_kind_check,
    DROP CONSTRAINT IF EXISTS action_attempts_check;

ALTER TABLE wb_automation.action_attempts
    ADD CONSTRAINT action_attempts_action_kind_check CHECK (
        action_kind IN (
            'change_bids',
            'pause_campaign_for_daily_cap',
            'resume_campaign_after_daily_cap'
        )
    ),
    ADD CONSTRAINT action_attempts_check CHECK (
        octet_length(request_json) BETWEEN 2 AND 131072
        AND jsonb_typeof(request_json::jsonb) = 'object'
        AND request_json::jsonb->>'kind' = action_kind
        AND (
            (
                action_kind = 'change_bids'
                AND jsonb_typeof(request_json::jsonb->'changes') = 'array'
                AND jsonb_array_length(request_json::jsonb->'changes') = 1
            ) OR (
                action_kind IN (
                    'pause_campaign_for_daily_cap',
                    'resume_campaign_after_daily_cap'
                )
                AND NOT request_json::jsonb ? 'changes'
            )
        )
    );

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

COMMIT;
