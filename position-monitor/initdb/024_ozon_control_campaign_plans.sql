\set ON_ERROR_STOP on

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'control_writer') THEN
        RAISE EXCEPTION 'control_writer role must be created before Ozon control migration';
    END IF;
END
$$;

CREATE SCHEMA IF NOT EXISTS control;
REVOKE ALL ON SCHEMA control FROM PUBLIC;
GRANT USAGE ON SCHEMA control TO control_writer;

CREATE TABLE IF NOT EXISTS control.ozon_policy_revisions (
    policy_revision bigint PRIMARY KEY CHECK (policy_revision > 0),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    policy_digest varchar(64) NOT NULL UNIQUE CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    registered_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS control.ozon_campaign_plans (
    plan_id varchar(64) PRIMARY KEY CHECK (plan_id ~ '^[0-9a-f]{64}$'),
    plan_digest varchar(64) NOT NULL UNIQUE CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
    actor_id varchar(128) NOT NULL CHECK (actor_id ~ '^[A-Za-z0-9_.-]+$'),
    account_id varchar(128) NOT NULL CHECK (account_id ~ '^[A-Za-z0-9_.-]+$'),
    sku bigint NOT NULL CHECK (sku > 0),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    policy_revision bigint NOT NULL REFERENCES control.ozon_policy_revisions(policy_revision),
    policy_digest varchar(64) NOT NULL CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    manifest_json text NOT NULL CHECK (
        octet_length(manifest_json) BETWEEN 2 AND 65536
        AND manifest_json::jsonb IS NOT NULL
    ),
    status text NOT NULL CHECK (status IN (
        'prepared','approved','creating','created','adding_products',
        'products_added','activating','applied','ambiguous','failed','expired'
    )),
    campaign_id bigint CHECK (campaign_id IS NULL OR campaign_id > 0),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (
        expires_at = created_at + interval '15 minutes'
    ),
    operation_started_at timestamptz,
    finished_at timestamptz,
    last_error_class varchar(64) CHECK (
        last_error_class IS NULL OR last_error_class ~ '^[a-z0-9_]+$'
    ),
    readback_json text CHECK (
        readback_json IS NULL OR (
            octet_length(readback_json) <= 131072
            AND readback_json::jsonb IS NOT NULL
        )
    ),
    CONSTRAINT ozon_plan_state_shape CHECK (
        (
            status IN ('prepared','approved')
            AND campaign_id IS NULL AND operation_started_at IS NULL
            AND finished_at IS NULL AND last_error_class IS NULL
            AND readback_json IS NULL
        ) OR (
            status='creating'
            AND campaign_id IS NULL AND operation_started_at IS NOT NULL
            AND finished_at IS NULL AND last_error_class IS NULL
            AND readback_json IS NULL
        ) OR (
            status IN ('created','adding_products','products_added','activating')
            AND campaign_id IS NOT NULL AND operation_started_at IS NOT NULL
            AND finished_at IS NULL AND last_error_class IS NULL
            AND readback_json IS NULL
        ) OR (
            status='applied'
            AND campaign_id IS NOT NULL AND operation_started_at IS NOT NULL
            AND finished_at IS NOT NULL AND last_error_class IS NULL
            AND readback_json IS NOT NULL
        ) OR (
            status IN ('ambiguous','failed')
            AND operation_started_at IS NOT NULL AND finished_at IS NOT NULL
            AND last_error_class IS NOT NULL
        ) OR (
            status='expired'
            AND campaign_id IS NULL AND operation_started_at IS NULL
            AND finished_at IS NOT NULL AND readback_json IS NULL
        )
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS ozon_one_open_plan_per_sku
    ON control.ozon_campaign_plans(account_id, sku)
    WHERE status IN (
        'prepared','approved','creating','created','adding_products',
        'products_added','activating','ambiguous'
    );
CREATE UNIQUE INDEX IF NOT EXISTS ozon_campaign_id_unique
    ON control.ozon_campaign_plans(campaign_id) WHERE campaign_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS control.ozon_campaign_plan_approvals (
    approval_id varchar(64) PRIMARY KEY CHECK (approval_id ~ '^[0-9a-f]{64}$'),
    plan_id varchar(64) NOT NULL UNIQUE REFERENCES control.ozon_campaign_plans(plan_id),
    plan_digest varchar(64) NOT NULL CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
    approver_id varchar(128) NOT NULL CHECK (approver_id ~ '^[A-Za-z0-9_.-]+$'),
    reference varchar(128) NOT NULL CHECK (
        reference ~ '^[A-Za-z0-9_.:/-]+$'
    ),
    approved_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (
        expires_at > approved_at
        AND expires_at <= approved_at + interval '3 minutes'
    )
);

CREATE TABLE IF NOT EXISTS control.ozon_runtime_gates (
    gate_key varchar(320) PRIMARY KEY,
    scope_kind text NOT NULL CHECK (scope_kind IN ('global','account','sku')),
    account_id varchar(128),
    sku bigint,
    enabled boolean NOT NULL DEFAULT false,
    lease_expires_at timestamptz NOT NULL,
    disabled_until timestamptz,
    revision bigint NOT NULL CHECK (revision > 0),
    reason varchar(512) NOT NULL CHECK (reason !~ '[[:cntrl:]]'),
    updated_by varchar(128) NOT NULL CHECK (updated_by ~ '^[A-Za-z0-9_.-]+$'),
    updated_at timestamptz NOT NULL,
    CHECK (
        (scope_kind='global' AND gate_key='global' AND account_id IS NULL AND sku IS NULL)
        OR (scope_kind='account' AND gate_key='account/' || account_id AND account_id IS NOT NULL AND sku IS NULL)
        OR (scope_kind='sku' AND gate_key='sku/' || account_id || '/' || sku::text AND account_id IS NOT NULL AND sku > 0)
    ),
    CHECK (
        NOT enabled OR (
            lease_expires_at > updated_at
            AND lease_expires_at <= updated_at + interval '15 minutes'
        )
    )
);
INSERT INTO control.ozon_runtime_gates(
    gate_key,scope_kind,enabled,lease_expires_at,disabled_until,
    revision,reason,updated_by,updated_at
) VALUES(
    'global','global',false,'-infinity','infinity',1,
    'fail_closed_default','migration',clock_timestamp()
) ON CONFLICT(gate_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS control.ozon_campaign_action_reservations (
    plan_id varchar(64) PRIMARY KEY REFERENCES control.ozon_campaign_plans(plan_id),
    account_id varchar(128) NOT NULL CHECK (account_id ~ '^[A-Za-z0-9_.-]+$'),
    sku bigint NOT NULL CHECK (sku > 0),
    reserved_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS control.ozon_campaign_audit_events (
    event_id bigserial PRIMARY KEY,
    plan_id varchar(64) NOT NULL REFERENCES control.ozon_campaign_plans(plan_id),
    actor_id varchar(128) NOT NULL CHECK (actor_id ~ '^[A-Za-z0-9_.-]+$'),
    event_type varchar(64) NOT NULL CHECK (event_type ~ '^[a-z0-9_]+$'),
    payload_json text NOT NULL CHECK (
        octet_length(payload_json) BETWEEN 2 AND 65536
        AND payload_json::jsonb IS NOT NULL
    ),
    created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS control.ozon_campaign_guards (
    plan_id varchar(64) PRIMARY KEY REFERENCES control.ozon_campaign_plans(plan_id),
    account_id varchar(128) NOT NULL CHECK (account_id ~ '^[A-Za-z0-9_.-]+$'),
    sku bigint NOT NULL CHECK (sku > 0),
    campaign_id bigint NOT NULL UNIQUE CHECK (campaign_id > 0),
    date_from char(10) NOT NULL CHECK (date_from ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'),
    spend_cap_microrubles bigint NOT NULL CHECK (spend_cap_microrubles > 0),
    target_drr_percent smallint NOT NULL CHECK (target_drr_percent BETWEEN 10 AND 100),
    status text NOT NULL CHECK (status IN ('active','stopping','stopped','incident')),
    stop_reason varchar(64) CHECK (stop_reason IS NULL OR stop_reason ~ '^[a-z0-9_]+$'),
    last_spend_minor bigint CHECK (last_spend_minor IS NULL OR last_spend_minor >= 0),
    last_revenue_minor bigint CHECK (last_revenue_minor IS NULL OR last_revenue_minor >= 0),
    last_checked_at timestamptz,
    created_at timestamptz NOT NULL,
    stopped_at timestamptz,
    CHECK (
        (status='active' AND stop_reason IS NULL AND stopped_at IS NULL)
        OR (status IN ('stopping','incident') AND stop_reason IS NOT NULL AND stopped_at IS NULL)
        OR (status='stopped' AND stop_reason IS NOT NULL AND stopped_at IS NOT NULL)
    )
);

CREATE OR REPLACE FUNCTION control.reject_ozon_append_only_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Ozon control evidence is append-only';
END
$$;

CREATE OR REPLACE FUNCTION control.validate_ozon_policy_revision_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    highest_revision bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('ozon/policy-revision',0));
    SELECT max(policy_revision) INTO highest_revision
    FROM control.ozon_policy_revisions;
    IF highest_revision IS NOT NULL AND NEW.policy_revision<=highest_revision THEN
        RAISE EXCEPTION 'Ozon policy revision must be strictly monotonic';
    END IF;
    NEW.registered_at := clock_timestamp();
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION control.validate_ozon_plan_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    active_policy control.ozon_policy_revisions%ROWTYPE;
    security_now timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('ozon/policy-revision',0));
    SELECT * INTO active_policy FROM control.ozon_policy_revisions
    ORDER BY policy_revision DESC LIMIT 1;
    IF NOT FOUND
       OR active_policy.policy_revision<>NEW.policy_revision
       OR active_policy.schema_version<>NEW.schema_version
       OR active_policy.policy_digest<>NEW.policy_digest THEN
        RAISE EXCEPTION 'Ozon plan does not use active policy';
    END IF;
    PERFORM pg_advisory_xact_lock(
        hashtextextended('ozon/'||NEW.account_id||'/'||NEW.sku::text,0)
    );
    IF NEW.status IS DISTINCT FROM 'prepared'
       OR NEW.manifest_json::jsonb->>'actor_id' IS DISTINCT FROM NEW.actor_id
       OR NEW.manifest_json::jsonb->>'policy_digest' IS DISTINCT FROM NEW.policy_digest
       OR (NEW.manifest_json::jsonb->>'policy_schema_version')::integer IS DISTINCT FROM NEW.schema_version
       OR (NEW.manifest_json::jsonb->>'policy_revision')::bigint IS DISTINCT FROM NEW.policy_revision
       OR NEW.manifest_json::jsonb#>>'{spec,account_id}' IS DISTINCT FROM NEW.account_id
       OR jsonb_array_length(NEW.manifest_json::jsonb#>'{spec,skus}') IS DISTINCT FROM 1
       OR (NEW.manifest_json::jsonb#>>'{spec,skus,0}')::bigint IS DISTINCT FROM NEW.sku
       OR NEW.manifest_json::jsonb#>>'{create_request,productAutopilotStrategy}' IS DISTINCT FROM 'TARGET_BIDS'
       OR NEW.manifest_json::jsonb#>>'{create_request,placement}' IS DISTINCT FROM 'PLACEMENT_SEARCH_AND_CATEGORY'
       OR (NEW.manifest_json::jsonb#>>'{create_request,weeklyBudget}')::bigint
          IS DISTINCT FROM (NEW.manifest_json::jsonb#>>'{spec,weekly_budget_microrubles}')::bigint
       OR (NEW.manifest_json::jsonb#>>'{spec,weekly_budget_microrubles}')::bigint
          IS DISTINCT FROM (NEW.manifest_json::jsonb#>>'{spec,per_sku_spend_cap_microrubles}')::bigint
       OR ((NEW.manifest_json::jsonb#>>'{spec,target_drr_percent}')::integer BETWEEN 10 AND 100)
          IS DISTINCT FROM true
       OR ((NEW.manifest_json::jsonb#>>'{spec,weekly_budget_microrubles}')::bigint>0)
          IS DISTINCT FROM true
       OR NEW.manifest_json::jsonb#>>'{products_request,bids,0,sku}' IS DISTINCT FROM NEW.sku::text
       OR (NEW.manifest_json::jsonb#>>'{products_request,bids,0,bid}')::bigint
          IS DISTINCT FROM (NEW.manifest_json::jsonb#>>'{spec,initial_cpc_bid_microrubles}')::bigint
       OR NEW.manifest_json::jsonb#>'{products_request,bids,0}'?'targetCir'
       OR NEW.manifest_json::jsonb#>'{products_request,bids,0}'?'topPosition'
       OR NEW.manifest_json::jsonb->>'activation_required' IS DISTINCT FROM 'true' THEN
        RAISE EXCEPTION 'Ozon plan manifest does not match immutable columns';
    END IF;
    security_now := clock_timestamp();
    NEW.created_at := security_now;
    NEW.expires_at := security_now + interval '15 minutes';
    RETURN NEW;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range
    OR null_value_not_allowed THEN
    RAISE EXCEPTION 'Ozon plan manifest is invalid';
END
$$;

CREATE OR REPLACE FUNCTION control.validate_ozon_approval_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    stored_plan control.ozon_campaign_plans%ROWTYPE;
    security_now timestamptz;
BEGIN
    SELECT * INTO stored_plan FROM control.ozon_campaign_plans
    WHERE plan_id=NEW.plan_id FOR UPDATE;
    security_now := clock_timestamp();
    IF NOT FOUND OR stored_plan.status<>'prepared'
       OR stored_plan.plan_digest<>NEW.plan_digest
       OR stored_plan.actor_id=NEW.approver_id
       OR stored_plan.expires_at<=security_now THEN
        RAISE EXCEPTION 'Ozon approval does not match an active prepared plan';
    END IF;
    NEW.approved_at := security_now;
    NEW.expires_at := LEAST(stored_plan.expires_at,security_now+interval '3 minutes');
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION control.validate_ozon_reservation_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    stored_plan control.ozon_campaign_plans%ROWTYPE;
    security_now timestamptz := clock_timestamp();
BEGIN
    SELECT * INTO stored_plan FROM control.ozon_campaign_plans
    WHERE plan_id=NEW.plan_id FOR UPDATE;
    IF NOT FOUND OR stored_plan.status<>'approved'
       OR stored_plan.account_id<>NEW.account_id OR stored_plan.sku<>NEW.sku
       OR NOT EXISTS(
           SELECT 1 FROM control.ozon_campaign_plan_approvals approval
           WHERE approval.plan_id=stored_plan.plan_id
             AND approval.plan_digest=stored_plan.plan_digest
             AND approval.expires_at>security_now
       ) THEN
        RAISE EXCEPTION 'Ozon reservation does not match an approved plan';
    END IF;
    NEW.reserved_at := security_now;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION control.validate_ozon_guard_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    stored_plan control.ozon_campaign_plans%ROWTYPE;
BEGIN
    SELECT * INTO stored_plan FROM control.ozon_campaign_plans
    WHERE plan_id=NEW.plan_id FOR UPDATE;
    IF NOT FOUND OR stored_plan.status<>'applied'
       OR stored_plan.account_id<>NEW.account_id OR stored_plan.sku<>NEW.sku
       OR stored_plan.campaign_id<>NEW.campaign_id
       OR NEW.status<>'active'
       OR NEW.date_from IS DISTINCT FROM stored_plan.manifest_json::jsonb#>>'{spec,from_date}'
       OR NEW.spend_cap_microrubles IS DISTINCT FROM
          (stored_plan.manifest_json::jsonb#>>'{spec,per_sku_spend_cap_microrubles}')::bigint
       OR NEW.target_drr_percent IS DISTINCT FROM
          (stored_plan.manifest_json::jsonb#>>'{spec,target_drr_percent}')::smallint THEN
        RAISE EXCEPTION 'Ozon guard does not match an applied plan';
    END IF;
    NEW.created_at := clock_timestamp();
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION control.enforce_ozon_guard_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.plan_id,NEW.account_id,NEW.sku,NEW.campaign_id,NEW.date_from,
        NEW.spend_cap_microrubles,NEW.target_drr_percent,NEW.created_at)
       IS DISTINCT FROM
       (OLD.plan_id,OLD.account_id,OLD.sku,OLD.campaign_id,OLD.date_from,
        OLD.spend_cap_microrubles,OLD.target_drr_percent,OLD.created_at) THEN
        RAISE EXCEPTION 'Ozon guard immutable fields cannot change';
    END IF;
    IF OLD.status='active' AND NEW.status='active' THEN
        IF NEW.stop_reason IS NOT NULL OR NEW.stopped_at IS NOT NULL THEN
            RAISE EXCEPTION 'Ozon active guard observation is invalid';
        END IF;
    ELSIF OLD.status='active' AND NEW.status='stopping' THEN
        IF NEW.stop_reason IS NULL OR NEW.stopped_at IS NOT NULL THEN
            RAISE EXCEPTION 'Ozon guard stop claim is invalid';
        END IF;
    ELSIF OLD.status='stopping' AND NEW.status='stopped' THEN
        IF NEW.stop_reason IS NULL OR NEW.stopped_at IS NULL THEN
            RAISE EXCEPTION 'Ozon guard stop proof is invalid';
        END IF;
    ELSIF OLD.status='stopping' AND NEW.status='incident' THEN
        IF NEW.stop_reason IS NULL OR NEW.stopped_at IS NOT NULL THEN
            RAISE EXCEPTION 'Ozon guard incident is invalid';
        END IF;
    ELSE
        RAISE EXCEPTION 'invalid Ozon guard transition % -> %',OLD.status,NEW.status;
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION control.enforce_ozon_plan_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    security_now timestamptz := clock_timestamp();
    gates_active boolean;
BEGIN
    IF (NEW.plan_id,NEW.plan_digest,NEW.actor_id,NEW.account_id,NEW.sku,
        NEW.schema_version,NEW.policy_revision,NEW.policy_digest,NEW.manifest_json,
        NEW.created_at,NEW.expires_at)
       IS DISTINCT FROM
       (OLD.plan_id,OLD.plan_digest,OLD.actor_id,OLD.account_id,OLD.sku,
        OLD.schema_version,OLD.policy_revision,OLD.policy_digest,OLD.manifest_json,
        OLD.created_at,OLD.expires_at) THEN
        RAISE EXCEPTION 'Ozon immutable plan fields cannot change';
    END IF;
    IF NEW.status=OLD.status THEN
        RAISE EXCEPTION 'Ozon mutable fields require a state transition';
    END IF;

    IF OLD.status='prepared' AND NEW.status='approved' THEN
        IF NOT EXISTS(
            SELECT 1 FROM control.ozon_campaign_plan_approvals a
            WHERE a.plan_id=OLD.plan_id AND a.plan_digest=OLD.plan_digest
              AND a.expires_at>security_now
        ) THEN RAISE EXCEPTION 'Ozon approval artifact is missing'; END IF;
    ELSIF OLD.status='approved' AND NEW.status='creating' THEN
        SELECT count(*)=3 AND bool_and(
            enabled AND lease_expires_at>security_now
            AND (disabled_until IS NULL OR disabled_until<=security_now)
        ) INTO gates_active FROM control.ozon_runtime_gates
        WHERE gate_key IN (
            'global','account/'||OLD.account_id,
            'sku/'||OLD.account_id||'/'||OLD.sku::text
        );
        IF gates_active IS DISTINCT FROM true
           OR NOT EXISTS(SELECT 1 FROM control.ozon_campaign_action_reservations r WHERE r.plan_id=OLD.plan_id)
           OR NOT EXISTS(SELECT 1 FROM control.ozon_campaign_plan_approvals a WHERE a.plan_id=OLD.plan_id AND a.plan_digest=OLD.plan_digest AND a.expires_at>security_now)
           OR OLD.policy_revision<>(SELECT max(policy_revision) FROM control.ozon_policy_revisions)
        THEN RAISE EXCEPTION 'Ozon apply authorization is not active'; END IF;
        NEW.operation_started_at := security_now;
    ELSIF OLD.status='creating' AND NEW.status='created' THEN
        IF NEW.campaign_id IS NULL THEN RAISE EXCEPTION 'Ozon campaign id is required'; END IF;
    ELSIF OLD.status='created' AND NEW.status='adding_products' THEN
        NULL;
    ELSIF OLD.status='adding_products' AND NEW.status='products_added' THEN
        NULL;
    ELSIF OLD.status='products_added' AND NEW.status='activating' THEN
        NULL;
    ELSIF OLD.status='activating' AND NEW.status='applied' THEN
        IF NEW.campaign_id IS NULL OR NEW.readback_json IS NULL
           OR NEW.readback_json::jsonb->>'campaign_id' <> NEW.campaign_id::text
           OR NEW.readback_json::jsonb->>'sku' <> NEW.sku::text
           OR NEW.readback_json::jsonb->>'state' <> 'CAMPAIGN_STATE_RUNNING'
        THEN RAISE EXCEPTION 'Ozon exact readback is required'; END IF;
        NEW.finished_at := security_now;
    ELSIF OLD.status IN ('creating','created','adding_products','products_added','activating')
          AND NEW.status IN ('ambiguous','failed') THEN
        IF NEW.last_error_class IS NULL THEN RAISE EXCEPTION 'Ozon terminal error class is required'; END IF;
        NEW.finished_at := security_now;
    ELSIF OLD.status IN ('prepared','approved') AND NEW.status='expired' THEN
        NEW.finished_at := security_now;
    ELSIF OLD.status='ambiguous' AND NEW.status='applied' THEN
        IF NEW.campaign_id IS NULL OR NEW.readback_json IS NULL
           OR NEW.readback_json::jsonb->>'campaign_id' <> NEW.campaign_id::text
           OR NEW.readback_json::jsonb->>'sku' <> NEW.sku::text
           OR NEW.readback_json::jsonb->>'state' <> 'CAMPAIGN_STATE_RUNNING'
        THEN RAISE EXCEPTION 'Ozon reconciliation proof is required'; END IF;
        NEW.finished_at := security_now;
        NEW.last_error_class := NULL;
    ELSE
        RAISE EXCEPTION 'invalid Ozon plan transition % -> %', OLD.status, NEW.status;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER ozon_plans_transition_guard
BEFORE UPDATE ON control.ozon_campaign_plans
FOR EACH ROW EXECUTE FUNCTION control.enforce_ozon_plan_transition();

CREATE TRIGGER ozon_policy_revisions_validate
BEFORE INSERT ON control.ozon_policy_revisions
FOR EACH ROW EXECUTE FUNCTION control.validate_ozon_policy_revision_insert();
CREATE TRIGGER ozon_plans_validate_insert
BEFORE INSERT ON control.ozon_campaign_plans
FOR EACH ROW EXECUTE FUNCTION control.validate_ozon_plan_insert();
CREATE TRIGGER ozon_approvals_validate
BEFORE INSERT ON control.ozon_campaign_plan_approvals
FOR EACH ROW EXECUTE FUNCTION control.validate_ozon_approval_insert();
CREATE TRIGGER ozon_reservations_validate
BEFORE INSERT ON control.ozon_campaign_action_reservations
FOR EACH ROW EXECUTE FUNCTION control.validate_ozon_reservation_insert();
CREATE TRIGGER ozon_guards_validate_insert
BEFORE INSERT ON control.ozon_campaign_guards
FOR EACH ROW EXECUTE FUNCTION control.validate_ozon_guard_insert();
CREATE TRIGGER ozon_guards_transition_guard
BEFORE UPDATE ON control.ozon_campaign_guards
FOR EACH ROW EXECUTE FUNCTION control.enforce_ozon_guard_transition();

CREATE TRIGGER ozon_policy_revisions_append_only
BEFORE UPDATE OR DELETE ON control.ozon_policy_revisions
FOR EACH ROW EXECUTE FUNCTION control.reject_ozon_append_only_mutation();
CREATE TRIGGER ozon_approvals_append_only
BEFORE UPDATE OR DELETE ON control.ozon_campaign_plan_approvals
FOR EACH ROW EXECUTE FUNCTION control.reject_ozon_append_only_mutation();
CREATE TRIGGER ozon_reservations_append_only
BEFORE UPDATE OR DELETE ON control.ozon_campaign_action_reservations
FOR EACH ROW EXECUTE FUNCTION control.reject_ozon_append_only_mutation();
CREATE TRIGGER ozon_audit_append_only
BEFORE UPDATE OR DELETE ON control.ozon_campaign_audit_events
FOR EACH ROW EXECUTE FUNCTION control.reject_ozon_append_only_mutation();

REVOKE ALL ON TABLE control.ozon_policy_revisions,
    control.ozon_campaign_plans,control.ozon_campaign_plan_approvals,
    control.ozon_runtime_gates,control.ozon_campaign_action_reservations,
    control.ozon_campaign_audit_events,control.ozon_campaign_guards
    FROM PUBLIC,control_writer;
REVOKE ALL ON SEQUENCE control.ozon_campaign_audit_events_event_id_seq FROM PUBLIC,control_writer;
REVOKE ALL ON FUNCTION control.reject_ozon_append_only_mutation() FROM PUBLIC,control_writer;
REVOKE ALL ON FUNCTION control.enforce_ozon_plan_transition() FROM PUBLIC,control_writer;
REVOKE ALL ON FUNCTION control.validate_ozon_policy_revision_insert() FROM PUBLIC,control_writer;
REVOKE ALL ON FUNCTION control.validate_ozon_plan_insert() FROM PUBLIC,control_writer;
REVOKE ALL ON FUNCTION control.validate_ozon_approval_insert() FROM PUBLIC,control_writer;
REVOKE ALL ON FUNCTION control.validate_ozon_reservation_insert() FROM PUBLIC,control_writer;
REVOKE ALL ON FUNCTION control.validate_ozon_guard_insert() FROM PUBLIC,control_writer;
REVOKE ALL ON FUNCTION control.enforce_ozon_guard_transition() FROM PUBLIC,control_writer;

GRANT SELECT,INSERT ON control.ozon_policy_revisions TO control_writer;
GRANT SELECT,INSERT ON control.ozon_campaign_plans TO control_writer;
GRANT UPDATE(status,campaign_id,operation_started_at,finished_at,last_error_class,readback_json)
    ON control.ozon_campaign_plans TO control_writer;
GRANT SELECT,INSERT ON control.ozon_campaign_plan_approvals TO control_writer;
GRANT SELECT ON control.ozon_runtime_gates TO control_writer;
GRANT SELECT,INSERT ON control.ozon_campaign_action_reservations TO control_writer;
GRANT SELECT,INSERT ON control.ozon_campaign_audit_events TO control_writer;
GRANT SELECT,INSERT ON control.ozon_campaign_guards TO control_writer;
GRANT UPDATE(status,stop_reason,last_spend_minor,last_revenue_minor,last_checked_at,stopped_at)
    ON control.ozon_campaign_guards TO control_writer;
GRANT USAGE,SELECT ON SEQUENCE control.ozon_campaign_audit_events_event_id_seq TO control_writer;

COMMIT;
