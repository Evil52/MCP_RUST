\set ON_ERROR_STOP on

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'wb_automation_writer'
    ) THEN
        RAISE EXCEPTION
            'wb_automation_writer role must be created before WB automation migration';
    END IF;
END
$$;

CREATE SCHEMA IF NOT EXISTS wb_automation;
REVOKE ALL ON SCHEMA wb_automation FROM PUBLIC;
GRANT USAGE ON SCHEMA wb_automation TO wb_automation_writer;

-- Immutable input and decision pair for one five-minute cycle. The Moscow
-- business date is derived by PostgreSQL as well as Rust so a caller cannot
-- persist a snapshot against the wrong daily limit window.
CREATE TABLE IF NOT EXISTS wb_automation.cycles (
    cycle_id varchar(64) PRIMARY KEY
        CHECK (cycle_id ~ '^[0-9a-f]{64}$'),
    account_id varchar(128) NOT NULL
        CHECK (account_id ~ '^[A-Za-z0-9_.-]+$'),
    advert_id bigint NOT NULL CHECK (advert_id > 0),
    policy_digest varchar(64) NOT NULL
        CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    observed_at timestamptz NOT NULL,
    business_date date NOT NULL,
    state_revision bigint NOT NULL CHECK (state_revision > 0),
    snapshot_json text NOT NULL CHECK (
        octet_length(snapshot_json) BETWEEN 2 AND 1048576
        AND jsonb_typeof(snapshot_json::jsonb) = 'object'
    ),
    decision_json text NOT NULL CHECK (
        octet_length(decision_json) BETWEEN 2 AND 262144
        AND jsonb_typeof(decision_json::jsonb) = 'object'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT wb_automation_cycle_moscow_date CHECK (
        business_date = (observed_at AT TIME ZONE 'Europe/Moscow')::date
    ),
    UNIQUE (cycle_id, account_id, advert_id),
    UNIQUE (account_id, advert_id, observed_at)
);
CREATE INDEX IF NOT EXISTS wb_automation_cycles_campaign_time
    ON wb_automation.cycles (account_id, advert_id, observed_at DESC);

-- A write attempt remains forever after reconciliation, keeping its
-- idempotency key consumed. The partial unique index is the durable
-- MAX_PENDING_ACTIONS=1 invariant for one campaign.
CREATE TABLE IF NOT EXISTS wb_automation.action_attempts (
    idempotency_key varchar(64) PRIMARY KEY
        CHECK (idempotency_key ~ '^[0-9a-f]{64}$'),
    account_id varchar(128) NOT NULL
        CHECK (account_id ~ '^[A-Za-z0-9_.-]+$'),
    advert_id bigint NOT NULL CHECK (advert_id > 0),
    cycle_id varchar(64) NOT NULL,
    policy_digest varchar(64) NOT NULL
        CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    request_digest varchar(64) NOT NULL
        CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    action_kind text NOT NULL CHECK (
        action_kind IN ('change_bids', 'pause_campaign_for_daily_cap')
    ),
    request_json text NOT NULL CHECK (
        octet_length(request_json) BETWEEN 2 AND 131072
        AND jsonb_typeof(request_json::jsonb) = 'object'
        AND request_json::jsonb->>'kind' = action_kind
        AND (
            (
                action_kind = 'change_bids'
                AND jsonb_typeof(request_json::jsonb->'changes') = 'array'
                AND jsonb_array_length(request_json::jsonb->'changes') = 1
            ) OR (
                action_kind = 'pause_campaign_for_daily_cap'
                AND NOT request_json::jsonb ? 'changes'
            )
        )
    ),
    status text NOT NULL CHECK (status IN (
        'reserved', 'write_started', 'awaiting_readback', 'applied',
        'reconciliation_required', 'cancelled'
    )),
    reserved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    write_started_at timestamptz,
    resolved_at timestamptz,
    readback_cycle_id varchar(64),
    last_error_class varchar(64) CHECK (
        last_error_class IS NULL OR last_error_class ~ '^[a-z0-9_]+$'
    ),
    CONSTRAINT wb_automation_action_cycle_fkey
        FOREIGN KEY (cycle_id, account_id, advert_id)
        REFERENCES wb_automation.cycles(cycle_id, account_id, advert_id),
    CONSTRAINT wb_automation_action_readback_cycle_fkey
        FOREIGN KEY (readback_cycle_id, account_id, advert_id)
        REFERENCES wb_automation.cycles(cycle_id, account_id, advert_id),
    CONSTRAINT wb_automation_action_state_shape CHECK (
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
            AND write_started_at IS NULL
            AND resolved_at IS NOT NULL
            AND last_error_class IS NOT NULL
        )
    ),
    UNIQUE (idempotency_key, account_id, advert_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS wb_automation_one_unresolved_action
    ON wb_automation.action_attempts (account_id, advert_id)
    WHERE status IN (
        'reserved', 'write_started', 'awaiting_readback',
        'reconciliation_required'
    );
CREATE INDEX IF NOT EXISTS wb_automation_actions_campaign_time
    ON wb_automation.action_attempts (account_id, advert_id, reserved_at DESC);

-- Mutable safety state for one exact campaign. Pending points to the durable
-- action row instead of embedding deletable request bytes.
CREATE TABLE IF NOT EXISTS wb_automation.execution_state (
    account_id varchar(128) NOT NULL
        CHECK (account_id ~ '^[A-Za-z0-9_.-]+$'),
    advert_id bigint NOT NULL CHECK (advert_id > 0),
    schema_version integer NOT NULL CHECK (schema_version = 1),
    policy_digest varchar(64) NOT NULL
        CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    business_date date NOT NULL,
    actions_today integer NOT NULL CHECK (actions_today BETWEEN 0 AND 500),
    last_action_at timestamptz,
    paused_for_daily_cap_on date CHECK (
        paused_for_daily_cap_on IS NULL
        OR paused_for_daily_cap_on <= business_date
    ),
    pending_idempotency_key varchar(64),
    incident_class varchar(64) CHECK (
        incident_class IS NULL OR incident_class ~ '^[a-z0-9_]+$'
    ),
    revision bigint NOT NULL CHECK (revision > 0),
    imported_legacy_digest varchar(64) CHECK (
        imported_legacy_digest IS NULL
        OR imported_legacy_digest ~ '^[0-9a-f]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (account_id, advert_id),
    CONSTRAINT wb_automation_state_pending_fkey
        FOREIGN KEY (pending_idempotency_key, account_id, advert_id)
        REFERENCES wb_automation.action_attempts(
            idempotency_key, account_id, advert_id
        )
);

-- Idempotent append-only audit. event_key is deterministic, so replaying a
-- transaction cannot manufacture duplicate lifecycle evidence.
CREATE TABLE IF NOT EXISTS wb_automation.audit_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_key varchar(64) NOT NULL UNIQUE
        CHECK (event_key ~ '^[0-9a-f]{64}$'),
    cycle_id varchar(64) NOT NULL,
    account_id varchar(128) NOT NULL
        CHECK (account_id ~ '^[A-Za-z0-9_.-]+$'),
    advert_id bigint NOT NULL CHECK (advert_id > 0),
    event_type varchar(64) NOT NULL
        CHECK (event_type ~ '^[a-z0-9_]+$'),
    idempotency_key varchar(64),
    payload_json text NOT NULL DEFAULT '{}' CHECK (
        octet_length(payload_json) BETWEEN 2 AND 262144
        AND jsonb_typeof(payload_json::jsonb) = 'object'
    ),
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT wb_automation_audit_cycle_fkey
        FOREIGN KEY (cycle_id, account_id, advert_id)
        REFERENCES wb_automation.cycles(cycle_id, account_id, advert_id),
    CONSTRAINT wb_automation_audit_action_fkey
        FOREIGN KEY (idempotency_key, account_id, advert_id)
        REFERENCES wb_automation.action_attempts(
            idempotency_key, account_id, advert_id
        )
);
CREATE INDEX IF NOT EXISTS wb_automation_audit_campaign_time
    ON wb_automation.audit_events (account_id, advert_id, occurred_at DESC, id DESC);

CREATE OR REPLACE FUNCTION wb_automation.reject_append_only_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'WB automation history is append-only';
END
$$;

CREATE OR REPLACE FUNCTION wb_automation.stamp_cycle_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.created_at := clock_timestamp();
    RETURN NEW;
END
$$;

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

    IF OLD.status = 'reserved' AND NEW.status = 'write_started' THEN
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
       )
       OR (
           OLD.paused_for_daily_cap_on IS NOT NULL
           AND NEW.paused_for_daily_cap_on IS NULL
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

CREATE OR REPLACE FUNCTION wb_automation.stamp_audit_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.occurred_at := clock_timestamp();
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS wb_automation_cycles_stamp
    ON wb_automation.cycles;
CREATE TRIGGER wb_automation_cycles_stamp
BEFORE INSERT ON wb_automation.cycles
FOR EACH ROW EXECUTE FUNCTION wb_automation.stamp_cycle_insert();

DROP TRIGGER IF EXISTS wb_automation_cycles_append_only
    ON wb_automation.cycles;
CREATE TRIGGER wb_automation_cycles_append_only
BEFORE UPDATE OR DELETE ON wb_automation.cycles
FOR EACH ROW EXECUTE FUNCTION wb_automation.reject_append_only_mutation();

DROP TRIGGER IF EXISTS wb_automation_action_transition
    ON wb_automation.action_attempts;
CREATE TRIGGER wb_automation_action_transition
BEFORE INSERT OR UPDATE ON wb_automation.action_attempts
FOR EACH ROW EXECUTE FUNCTION wb_automation.enforce_action_transition();

DROP TRIGGER IF EXISTS wb_automation_state_transition
    ON wb_automation.execution_state;
CREATE TRIGGER wb_automation_state_transition
BEFORE INSERT OR UPDATE ON wb_automation.execution_state
FOR EACH ROW EXECUTE FUNCTION wb_automation.enforce_state_transition();

DROP TRIGGER IF EXISTS wb_automation_audit_stamp
    ON wb_automation.audit_events;
CREATE TRIGGER wb_automation_audit_stamp
BEFORE INSERT ON wb_automation.audit_events
FOR EACH ROW EXECUTE FUNCTION wb_automation.stamp_audit_insert();

DROP TRIGGER IF EXISTS wb_automation_audit_append_only
    ON wb_automation.audit_events;
CREATE TRIGGER wb_automation_audit_append_only
BEFORE UPDATE OR DELETE ON wb_automation.audit_events
FOR EACH ROW EXECUTE FUNCTION wb_automation.reject_append_only_mutation();

REVOKE ALL ON TABLE wb_automation.cycles,
    wb_automation.action_attempts, wb_automation.execution_state,
    wb_automation.audit_events FROM PUBLIC, wb_automation_writer;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA wb_automation
    FROM PUBLIC, wb_automation_writer;
REVOKE ALL ON FUNCTION wb_automation.reject_append_only_mutation()
    FROM PUBLIC, wb_automation_writer;
REVOKE ALL ON FUNCTION wb_automation.stamp_cycle_insert()
    FROM PUBLIC, wb_automation_writer;
REVOKE ALL ON FUNCTION wb_automation.enforce_action_transition()
    FROM PUBLIC, wb_automation_writer;
REVOKE ALL ON FUNCTION wb_automation.enforce_state_transition()
    FROM PUBLIC, wb_automation_writer;
REVOKE ALL ON FUNCTION wb_automation.stamp_audit_insert()
    FROM PUBLIC, wb_automation_writer;

GRANT SELECT, INSERT ON wb_automation.cycles TO wb_automation_writer;
GRANT SELECT, INSERT ON wb_automation.action_attempts TO wb_automation_writer;
GRANT UPDATE (status, readback_cycle_id, last_error_class)
    ON wb_automation.action_attempts TO wb_automation_writer;
GRANT SELECT, INSERT ON wb_automation.execution_state TO wb_automation_writer;
GRANT UPDATE (
    policy_digest, business_date, actions_today, last_action_at,
    paused_for_daily_cap_on, pending_idempotency_key, incident_class, revision
) ON wb_automation.execution_state TO wb_automation_writer;
GRANT SELECT, INSERT ON wb_automation.audit_events TO wb_automation_writer;
GRANT USAGE, SELECT ON SEQUENCE wb_automation.audit_events_id_seq
    TO wb_automation_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA wb_automation
    REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA wb_automation
    REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA wb_automation
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

COMMIT;
