\set ON_ERROR_STOP on

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'control_writer') THEN
        RAISE EXCEPTION 'control_writer role must be created before WB control migration';
    END IF;
END
$$;

CREATE SCHEMA IF NOT EXISTS control;
REVOKE ALL ON SCHEMA control FROM PUBLIC;
GRANT USAGE ON SCHEMA control TO control_writer;

-- The highest registered revision is the only revision from which a new plan
-- may be prepared. Rows are append-only, preventing policy rollback or reuse
-- of one revision number with different bytes.
CREATE TABLE IF NOT EXISTS control.wb_policy_revisions (
    policy_revision bigint PRIMARY KEY CHECK (policy_revision > 0),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    policy_digest varchar(64) NOT NULL UNIQUE
        CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    registered_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS control.wb_prepare_reservations (
    reservation_id varchar(64) PRIMARY KEY
        CHECK (reservation_id ~ '^[0-9a-f]{64}$'),
    actor_id varchar(128) NOT NULL
        CHECK (actor_id ~ '^[A-Za-z0-9_.-]+$'),
    account_id varchar(128) NOT NULL
        CHECK (account_id ~ '^[A-Za-z0-9_.-]+$'),
    advert_id bigint NOT NULL CHECK (advert_id > 0),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    policy_revision bigint NOT NULL
        REFERENCES control.wb_policy_revisions(policy_revision),
    policy_digest varchar(64) NOT NULL
        CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    quota_max_actions_per_hour integer NOT NULL
        CHECK (quota_max_actions_per_hour BETWEEN 1 AND 60),
    quota_max_actions_per_day integer NOT NULL
        CHECK (
            quota_max_actions_per_day BETWEEN 1 AND 500
            AND quota_max_actions_per_day >= quota_max_actions_per_hour
        ),
    quota_cooldown_seconds integer NOT NULL
        CHECK (quota_cooldown_seconds BETWEEN 30 AND 86400),
    quota_max_cumulative_abs_delta_kopecks_per_day bigint NOT NULL
        CHECK (quota_max_cumulative_abs_delta_kopecks_per_day > 0),
    reserved_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL
        CONSTRAINT wb_prepare_reservation_ttl
        CHECK (expires_at = reserved_at + interval '2 minutes')
);
CREATE INDEX IF NOT EXISTS wb_prepare_reservations_actor_time
    ON control.wb_prepare_reservations (actor_id, reserved_at DESC);
CREATE INDEX IF NOT EXISTS wb_prepare_reservations_campaign_time
    ON control.wb_prepare_reservations (account_id, advert_id, reserved_at DESC);

CREATE TABLE IF NOT EXISTS control.wb_plans (
    plan_id varchar(64) PRIMARY KEY
        CHECK (plan_id ~ '^[0-9a-f]{64}$'),
    plan_digest varchar(64) NOT NULL UNIQUE
        CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
    prepare_reservation_id varchar(64) NOT NULL UNIQUE
        REFERENCES control.wb_prepare_reservations(reservation_id),
    actor_id varchar(128) NOT NULL
        CHECK (actor_id ~ '^[A-Za-z0-9_.-]+$'),
    account_id varchar(128) NOT NULL
        CHECK (account_id ~ '^[A-Za-z0-9_.-]+$'),
    advert_id bigint NOT NULL CHECK (advert_id > 0),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    policy_revision bigint NOT NULL
        REFERENCES control.wb_policy_revisions(policy_revision),
    policy_digest varchar(64) NOT NULL
        CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    quota_max_actions_per_hour integer NOT NULL
        CHECK (quota_max_actions_per_hour BETWEEN 1 AND 60),
    quota_max_actions_per_day integer NOT NULL
        CHECK (
            quota_max_actions_per_day BETWEEN 1 AND 500
            AND quota_max_actions_per_day >= quota_max_actions_per_hour
        ),
    quota_cooldown_seconds integer NOT NULL
        CHECK (quota_cooldown_seconds BETWEEN 30 AND 86400),
    quota_max_cumulative_abs_delta_kopecks_per_day bigint NOT NULL
        CHECK (quota_max_cumulative_abs_delta_kopecks_per_day > 0),
    status text NOT NULL CHECK (status IN (
        'prepared', 'approved', 'applying', 'applied', 'reconciliation_required',
        'ambiguous', 'rejected', 'failed', 'expired'
    )),
    requested_json text NOT NULL CHECK (
        octet_length(requested_json) BETWEEN 2 AND 65536
        AND requested_json::jsonb IS NOT NULL
    ),
    changes_json text NOT NULL CHECK (
        octet_length(changes_json) BETWEEN 2 AND 65536
        AND changes_json::jsonb IS NOT NULL
    ),
    before_json text NOT NULL CHECK (
        octet_length(before_json) BETWEEN 2 AND 131072
        AND before_json::jsonb IS NOT NULL
    ),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL
        CONSTRAINT wb_plan_ttl
        CHECK (expires_at = created_at + interval '5 minutes'),
    apply_started_at timestamptz,
    finished_at timestamptz,
    last_error_class varchar(64)
        CHECK (last_error_class IS NULL OR last_error_class ~ '^[a-z0-9_]+$'),
    write_response_json text CHECK (
        write_response_json IS NULL OR (
            octet_length(write_response_json) <= 1048576
            AND write_response_json::jsonb IS NOT NULL
        )
    ),
    readback_json text CHECK (
        readback_json IS NULL OR (
            octet_length(readback_json) <= 131072
            AND readback_json::jsonb IS NOT NULL
        )
    ),
    CONSTRAINT wb_plan_state_shape CHECK (
        (
            status IN ('prepared', 'approved')
            AND apply_started_at IS NULL
            AND finished_at IS NULL
            AND last_error_class IS NULL
            AND write_response_json IS NULL
            AND readback_json IS NULL
        ) OR (
            status = 'applying'
            AND apply_started_at IS NOT NULL
            AND finished_at IS NULL
            AND last_error_class IS NULL
            AND write_response_json IS NULL
            AND readback_json IS NULL
        ) OR (
            status = 'expired'
            AND apply_started_at IS NULL
            AND finished_at IS NOT NULL
            AND write_response_json IS NULL
            AND readback_json IS NULL
        ) OR (
            status IN (
                'applied', 'reconciliation_required', 'ambiguous', 'rejected', 'failed'
            )
            AND apply_started_at IS NOT NULL
            AND finished_at IS NOT NULL
        )
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS wb_plans_one_applying_per_campaign
    ON control.wb_plans (account_id, advert_id)
    WHERE status = 'applying';
CREATE UNIQUE INDEX IF NOT EXISTS wb_plans_one_incident_per_campaign
    ON control.wb_plans (account_id, advert_id)
    WHERE status IN ('reconciliation_required', 'ambiguous');
CREATE INDEX IF NOT EXISTS wb_plans_actor_created
    ON control.wb_plans (actor_id, created_at DESC);
CREATE INDEX IF NOT EXISTS wb_plans_campaign_created
    ON control.wb_plans (account_id, advert_id, created_at DESC);

-- An approval is an append-only artifact. The plan state points to it only
-- through the unique plan_id relationship, so approval evidence cannot be
-- overwritten during apply or reconciliation.
CREATE TABLE IF NOT EXISTS control.wb_plan_approvals (
    approval_id varchar(64) PRIMARY KEY
        CHECK (approval_id ~ '^[0-9a-f]{64}$'),
    plan_id varchar(64) NOT NULL UNIQUE
        REFERENCES control.wb_plans(plan_id),
    plan_digest varchar(64) NOT NULL
        CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
    approver_id varchar(128) NOT NULL
        CHECK (approver_id ~ '^[A-Za-z0-9_.-]+$'),
    reason varchar(512) NOT NULL
        CHECK (
            octet_length(reason) BETWEEN 1 AND 128
            AND reason ~ '^[A-Za-z0-9_.:/-]+$'
        ),
    approved_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL
        CONSTRAINT wb_approval_ttl CHECK (
            expires_at > approved_at
            AND expires_at <= approved_at + interval '2 minutes'
        )
);

-- Gates are maintained by the database owner/operator. The application role
-- can only read them. A write requires an active global, account and campaign
-- lease; a missing row is intentionally equivalent to disabled.
CREATE TABLE IF NOT EXISTS control.wb_runtime_gates (
    gate_key varchar(320) PRIMARY KEY,
    scope_kind text NOT NULL
        CHECK (scope_kind IN ('global', 'account', 'campaign')),
    account_id varchar(128)
        CHECK (account_id IS NULL OR account_id ~ '^[A-Za-z0-9_.-]+$'),
    advert_id bigint CHECK (advert_id IS NULL OR advert_id > 0),
    enabled boolean NOT NULL DEFAULT false,
    lease_expires_at timestamptz NOT NULL,
    disabled_until timestamptz,
    revision bigint NOT NULL CHECK (revision > 0),
    reason varchar(512) NOT NULL
        CHECK (
            octet_length(reason) BETWEEN 1 AND 512
            AND reason !~ '[[:cntrl:]]'
        ),
    updated_by varchar(128) NOT NULL
        CHECK (updated_by ~ '^[A-Za-z0-9_.-]+$'),
    updated_at timestamptz NOT NULL,
    CONSTRAINT wb_runtime_gate_scope CHECK (
        (
            scope_kind = 'global'
            AND gate_key = 'global'
            AND account_id IS NULL
            AND advert_id IS NULL
        ) OR (
            scope_kind = 'account'
            AND gate_key = 'account/' || account_id
            AND account_id IS NOT NULL
            AND advert_id IS NULL
        ) OR (
            scope_kind = 'campaign'
            AND gate_key = 'campaign/' || account_id || '/' || advert_id::text
            AND account_id IS NOT NULL
            AND advert_id IS NOT NULL
        )
    ),
    CONSTRAINT wb_runtime_gate_lease_bound CHECK (
        NOT enabled OR (
            lease_expires_at > updated_at
            AND lease_expires_at <= updated_at + interval '15 minutes'
        )
    )
);

INSERT INTO control.wb_runtime_gates (
    gate_key, scope_kind, account_id, advert_id, enabled,
    lease_expires_at, disabled_until, revision, reason, updated_by, updated_at
)
VALUES (
    'global', 'global', NULL, NULL, false,
    '-infinity', 'infinity', 1, 'fail_closed_default', 'migration', clock_timestamp()
)
ON CONFLICT (gate_key) DO NOTHING;

-- Reservations are consumed attempts, not successful writes. Retaining a
-- failed/ambiguous reservation prevents rapid retries from bypassing quotas.
CREATE TABLE IF NOT EXISTS control.wb_action_reservations (
    plan_id varchar(64) PRIMARY KEY
        REFERENCES control.wb_plans(plan_id),
    account_id varchar(128) NOT NULL
        CHECK (account_id ~ '^[A-Za-z0-9_.-]+$'),
    advert_id bigint NOT NULL CHECK (advert_id > 0),
    action_count integer NOT NULL DEFAULT 1 CHECK (action_count = 1),
    cumulative_abs_delta_kopecks bigint NOT NULL
        CHECK (cumulative_abs_delta_kopecks > 0),
    max_actions_per_hour integer NOT NULL
        CHECK (max_actions_per_hour BETWEEN 1 AND 60),
    max_actions_per_day integer NOT NULL
        CHECK (
            max_actions_per_day BETWEEN 1 AND 500
            AND max_actions_per_day >= max_actions_per_hour
        ),
    cooldown_seconds integer NOT NULL
        CHECK (cooldown_seconds BETWEEN 30 AND 86400),
    max_cumulative_abs_delta_kopecks_per_day bigint NOT NULL
        CHECK (max_cumulative_abs_delta_kopecks_per_day > 0),
    reserved_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS wb_action_reservations_campaign_time
    ON control.wb_action_reservations (account_id, advert_id, reserved_at DESC);

CREATE TABLE IF NOT EXISTS control.wb_audit_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    plan_id varchar(64) NOT NULL REFERENCES control.wb_plans(plan_id),
    actor_id varchar(128) NOT NULL
        CHECK (actor_id ~ '^[A-Za-z0-9_.-]+$'),
    event_type varchar(64) NOT NULL
        CHECK (event_type ~ '^[a-z_]+$'),
    payload_json text NOT NULL DEFAULT '{}'
        CHECK (octet_length(payload_json) <= 131072 AND payload_json::jsonb IS NOT NULL),
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION control.validate_wb_policy_revision_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    highest_revision bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('wb/policy-revision', 0));
    SELECT max(policy_revision) INTO highest_revision
    FROM control.wb_policy_revisions;
    IF highest_revision IS NOT NULL AND NEW.policy_revision <= highest_revision THEN
        RAISE EXCEPTION 'WB policy revision must be strictly monotonic';
    END IF;
    NEW.registered_at := clock_timestamp();
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION control.validate_wb_prepare_reservation_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    active_policy control.wb_policy_revisions%ROWTYPE;
    actor_attempts bigint;
    campaign_attempts bigint;
    outstanding_count bigint;
    security_now timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('wb/policy-revision', 0));
    SELECT * INTO active_policy
    FROM control.wb_policy_revisions
    ORDER BY policy_revision DESC
    LIMIT 1;
    IF NOT FOUND
       OR active_policy.policy_revision <> NEW.policy_revision
       OR active_policy.schema_version <> NEW.schema_version
       OR active_policy.policy_digest <> NEW.policy_digest THEN
        RAISE EXCEPTION 'WB prepare reservation does not use active policy';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended('wb/prepare/actor/' || NEW.actor_id, 0)
    );
    PERFORM pg_advisory_xact_lock(
        hashtextextended('wb/' || NEW.account_id || '/' || NEW.advert_id::text, 0)
    );
    security_now := clock_timestamp();
    IF EXISTS (
        SELECT 1 FROM control.wb_plans incident
        WHERE incident.account_id = NEW.account_id
          AND incident.advert_id = NEW.advert_id
          AND incident.status IN ('reconciliation_required', 'ambiguous')
    ) THEN
        RAISE EXCEPTION 'WB campaign has an unresolved incident';
    END IF;
    SELECT count(*) INTO actor_attempts
    FROM control.wb_prepare_reservations
    WHERE actor_id = NEW.actor_id
      AND reserved_at > security_now - interval '1 hour';
    SELECT count(*) INTO campaign_attempts
    FROM control.wb_prepare_reservations
    WHERE account_id = NEW.account_id
      AND advert_id = NEW.advert_id
      AND reserved_at > security_now - interval '1 hour';
    SELECT
        (SELECT count(*) FROM control.wb_plans plan
         WHERE plan.account_id = NEW.account_id
           AND plan.advert_id = NEW.advert_id
           AND plan.status IN ('prepared', 'approved')
           AND plan.expires_at > security_now)
        +
        (SELECT count(*) FROM control.wb_prepare_reservations reservation
         WHERE reservation.account_id = NEW.account_id
           AND reservation.advert_id = NEW.advert_id
           AND reservation.expires_at > security_now
           AND NOT EXISTS (
               SELECT 1 FROM control.wb_plans plan
               WHERE plan.prepare_reservation_id = reservation.reservation_id
           ))
    INTO outstanding_count;
    IF actor_attempts >= 60
       OR campaign_attempts >= NEW.quota_max_actions_per_hour
       OR outstanding_count >= 3 THEN
        RAISE EXCEPTION 'WB prepare attempt limit is exhausted';
    END IF;
    NEW.reserved_at := security_now;
    NEW.expires_at := security_now + interval '2 minutes';
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION control.validate_wb_plan_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    active_policy control.wb_policy_revisions%ROWTYPE;
    prepare_reservation control.wb_prepare_reservations%ROWTYPE;
    outstanding_count bigint;
    security_now timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('wb/policy-revision', 0));
    SELECT * INTO active_policy
    FROM control.wb_policy_revisions
    ORDER BY policy_revision DESC
    LIMIT 1;
    IF NOT FOUND
       OR active_policy.policy_revision <> NEW.policy_revision
       OR active_policy.schema_version <> NEW.schema_version
       OR active_policy.policy_digest <> NEW.policy_digest THEN
        RAISE EXCEPTION 'WB plan does not use the active policy revision';
    END IF;

    SELECT stored.* INTO prepare_reservation
    FROM control.wb_prepare_reservations stored
    WHERE stored.reservation_id = NEW.prepare_reservation_id;
    IF NOT FOUND
       OR prepare_reservation.actor_id <> NEW.actor_id
       OR prepare_reservation.account_id <> NEW.account_id
       OR prepare_reservation.advert_id <> NEW.advert_id
       OR prepare_reservation.schema_version <> NEW.schema_version
       OR prepare_reservation.policy_revision <> NEW.policy_revision
       OR prepare_reservation.policy_digest <> NEW.policy_digest
       OR prepare_reservation.quota_max_actions_per_hour
          <> NEW.quota_max_actions_per_hour
       OR prepare_reservation.quota_max_actions_per_day
          <> NEW.quota_max_actions_per_day
       OR prepare_reservation.quota_cooldown_seconds
          <> NEW.quota_cooldown_seconds
       OR prepare_reservation.quota_max_cumulative_abs_delta_kopecks_per_day
          <> NEW.quota_max_cumulative_abs_delta_kopecks_per_day THEN
        RAISE EXCEPTION 'WB plan has no matching active prepare reservation';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended('wb/prepare/actor/' || NEW.actor_id, 0)
    );
    PERFORM pg_advisory_xact_lock(
        hashtextextended('wb/' || NEW.account_id || '/' || NEW.advert_id::text, 0)
    );
    security_now := clock_timestamp();
    IF prepare_reservation.expires_at <= security_now
       OR EXISTS (
           SELECT 1 FROM control.wb_plans consumed
           WHERE consumed.prepare_reservation_id = NEW.prepare_reservation_id
       ) THEN
        RAISE EXCEPTION 'WB plan has no matching active prepare reservation';
    END IF;
    IF EXISTS (
        SELECT 1 FROM control.wb_plans incident
        WHERE incident.account_id = NEW.account_id
          AND incident.advert_id = NEW.advert_id
          AND incident.status IN ('reconciliation_required', 'ambiguous')
    ) THEN
        RAISE EXCEPTION 'WB campaign has an unresolved incident';
    END IF;

    SELECT
        (SELECT count(*) FROM control.wb_plans plan
         WHERE plan.account_id = NEW.account_id
           AND plan.advert_id = NEW.advert_id
           AND plan.status IN ('prepared', 'approved')
           AND plan.expires_at > security_now)
        +
        (SELECT count(*) FROM control.wb_prepare_reservations pending
         WHERE pending.account_id = NEW.account_id
           AND pending.advert_id = NEW.advert_id
           AND pending.expires_at > security_now
           AND NOT EXISTS (
               SELECT 1 FROM control.wb_plans plan
               WHERE plan.prepare_reservation_id = pending.reservation_id
           ))
    INTO outstanding_count;
    IF outstanding_count > 3 THEN
        RAISE EXCEPTION 'WB campaign outstanding prepare limit is exhausted';
    END IF;

    IF NEW.created_at > security_now
       OR NEW.expires_at <= security_now
       OR NEW.status <> 'prepared' THEN
        RAISE EXCEPTION 'WB plan timestamps/state must use database transaction time';
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION control.validate_wb_runtime_gate_write()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    security_now timestamptz;
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.revision <= OLD.revision THEN
        RAISE EXCEPTION 'WB runtime gate revision must increase';
    END IF;
    security_now := clock_timestamp();
    NEW.updated_at := security_now;
    IF NEW.enabled AND (
        NEW.lease_expires_at <= security_now
        OR NEW.lease_expires_at
           > security_now + interval '15 minutes'
    ) THEN
        RAISE EXCEPTION 'WB runtime gate lease must use database time and be <= 15 minutes';
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION control.reject_wb_append_only_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END
$$;

CREATE OR REPLACE FUNCTION control.validate_wb_approval_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    plan_row control.wb_plans%ROWTYPE;
    plan_found boolean;
    security_now timestamptz;
BEGIN
    SELECT * INTO plan_row
    FROM control.wb_plans
    WHERE plan_id = NEW.plan_id
    FOR UPDATE;
    plan_found := FOUND;

    PERFORM pg_advisory_xact_lock(hashtextextended('wb/policy-revision', 0));
    security_now := clock_timestamp();

    IF NOT plan_found
       OR plan_row.status <> 'prepared'
       OR plan_row.plan_digest <> NEW.plan_digest
       OR plan_row.actor_id = NEW.approver_id
       OR plan_row.policy_revision <> (
           SELECT max(policy_revision) FROM control.wb_policy_revisions
       )
       OR plan_row.expires_at <= security_now THEN
        RAISE EXCEPTION 'approval does not match an active prepared WB plan';
    END IF;
    NEW.approved_at := security_now;
    NEW.expires_at := LEAST(
        plan_row.expires_at,
        security_now + interval '2 minutes'
    );
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION control.validate_wb_reservation_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    plan_row control.wb_plans%ROWTYPE;
    plan_found boolean;
    actions_hour bigint;
    actions_day bigint;
    delta_day bigint;
    latest_reservation timestamptz;
    action_delta bigint;
    gates_active boolean;
    security_now timestamptz;
BEGIN
    SELECT * INTO plan_row
    FROM control.wb_plans
    WHERE plan_id = NEW.plan_id
    FOR UPDATE;
    plan_found := FOUND;

    PERFORM pg_advisory_xact_lock(hashtextextended('wb/policy-revision', 0));

    IF NOT plan_found
       OR plan_row.status <> 'approved'
       OR plan_row.account_id <> NEW.account_id
       OR plan_row.advert_id <> NEW.advert_id
       OR plan_row.policy_revision <> (
           SELECT max(policy_revision) FROM control.wb_policy_revisions
       ) THEN
        RAISE EXCEPTION 'reservation does not match an approved WB plan';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended('wb/' || plan_row.account_id || '/' || plan_row.advert_id::text, 0)
    );
    security_now := clock_timestamp();
    IF plan_row.expires_at <= security_now
       OR NOT EXISTS (
           SELECT 1 FROM control.wb_plan_approvals approval
           WHERE approval.plan_id = plan_row.plan_id
             AND approval.plan_digest = plan_row.plan_digest
             AND approval.expires_at > security_now
       ) THEN
        RAISE EXCEPTION 'reservation does not match an approved WB plan';
    END IF;
    IF EXISTS (
        SELECT 1 FROM control.wb_plans incident
        WHERE incident.account_id = plan_row.account_id
          AND incident.advert_id = plan_row.advert_id
          AND incident.plan_id <> plan_row.plan_id
          AND incident.status IN ('reconciliation_required', 'ambiguous')
    ) THEN
        RAISE EXCEPTION 'WB campaign has an unresolved incident';
    END IF;
    SELECT count(*) = 3 AND bool_and(
        enabled
        AND lease_expires_at > security_now
        AND (disabled_until IS NULL OR disabled_until <= security_now)
    ) INTO gates_active
    FROM control.wb_runtime_gates
    WHERE gate_key IN (
        'global',
        'account/' || plan_row.account_id,
        'campaign/' || plan_row.account_id || '/' || plan_row.advert_id::text
    );
    IF gates_active IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'WB runtime gate is not active';
    END IF;

    SELECT
        count(*) FILTER (
            WHERE reserved_at > security_now - interval '1 hour'
        ),
        count(*),
        COALESCE(sum(cumulative_abs_delta_kopecks), 0),
        max(reserved_at)
    INTO actions_hour, actions_day, delta_day, latest_reservation
    FROM control.wb_action_reservations
    WHERE account_id = plan_row.account_id
      AND advert_id = plan_row.advert_id
      AND reserved_at > security_now - interval '1 day';

    SELECT COALESCE(sum(abs(
        (change->>'bid_kopecks')::bigint
        - (change->>'before_bid_kopecks')::bigint
    )), 0)
    INTO action_delta
    FROM jsonb_array_elements(plan_row.changes_json::jsonb) change;
    IF action_delta <= 0
       OR actions_hour >= plan_row.quota_max_actions_per_hour
       OR actions_day >= plan_row.quota_max_actions_per_day
       OR delta_day + action_delta
          > plan_row.quota_max_cumulative_abs_delta_kopecks_per_day
       OR latest_reservation + make_interval(
              secs => plan_row.quota_cooldown_seconds
          ) > security_now THEN
        RAISE EXCEPTION 'WB action quota or cooldown is exhausted';
    END IF;

    NEW.account_id := plan_row.account_id;
    NEW.advert_id := plan_row.advert_id;
    NEW.action_count := 1;
    NEW.cumulative_abs_delta_kopecks := action_delta;
    NEW.max_actions_per_hour := plan_row.quota_max_actions_per_hour;
    NEW.max_actions_per_day := plan_row.quota_max_actions_per_day;
    NEW.cooldown_seconds := plan_row.quota_cooldown_seconds;
    NEW.max_cumulative_abs_delta_kopecks_per_day :=
        plan_row.quota_max_cumulative_abs_delta_kopecks_per_day;
    NEW.reserved_at := security_now;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION control.enforce_wb_plan_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    gates_active boolean;
    security_now timestamptz;
    expected_after_proven boolean := false;
BEGIN
    security_now := clock_timestamp();
    IF (NEW.plan_id, NEW.plan_digest, NEW.prepare_reservation_id,
        NEW.actor_id, NEW.account_id, NEW.advert_id,
        NEW.schema_version, NEW.policy_revision, NEW.policy_digest,
        NEW.quota_max_actions_per_hour, NEW.quota_max_actions_per_day,
        NEW.quota_cooldown_seconds,
        NEW.quota_max_cumulative_abs_delta_kopecks_per_day,
        NEW.requested_json, NEW.changes_json, NEW.before_json,
        NEW.created_at, NEW.expires_at)
       IS DISTINCT FROM
       (OLD.plan_id, OLD.plan_digest, OLD.prepare_reservation_id,
        OLD.actor_id, OLD.account_id, OLD.advert_id,
        OLD.schema_version, OLD.policy_revision, OLD.policy_digest,
        OLD.quota_max_actions_per_hour, OLD.quota_max_actions_per_day,
        OLD.quota_cooldown_seconds,
        OLD.quota_max_cumulative_abs_delta_kopecks_per_day,
        OLD.requested_json, OLD.changes_json, OLD.before_json,
        OLD.created_at, OLD.expires_at) THEN
        RAISE EXCEPTION 'WB control plan immutable fields cannot change';
    END IF;

    IF NEW.status = OLD.status THEN
        IF (NEW.apply_started_at, NEW.finished_at, NEW.last_error_class,
            NEW.write_response_json, NEW.readback_json)
           IS DISTINCT FROM
           (OLD.apply_started_at, OLD.finished_at, OLD.last_error_class,
            OLD.write_response_json, OLD.readback_json) THEN
            RAISE EXCEPTION 'WB control plan mutable fields require a state transition';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.status = 'applied' THEN
        BEGIN
            SELECT
                NEW.readback_json IS NOT NULL
                AND NEW.readback_json::jsonb->>'advert_id'
                    IS NOT DISTINCT FROM OLD.advert_id::text
                AND COALESCE(
                    OLD.before_json::jsonb->>'seller_sid'
                        ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
                    false
                )
                AND OLD.before_json::jsonb->>'seller_sid'
                    <> '00000000-0000-0000-0000-000000000000'
                AND NEW.readback_json::jsonb->>'seller_sid'
                    IS NOT DISTINCT FROM OLD.before_json::jsonb->>'seller_sid'
                AND NEW.readback_json::jsonb->>'status'
                    IS NOT DISTINCT FROM OLD.before_json::jsonb->>'status'
                AND NEW.readback_json::jsonb->>'bid_type'
                    IS NOT DISTINCT FROM OLD.before_json::jsonb->>'bid_type'
                AND NEW.readback_json::jsonb->>'payment_type'
                    IS NOT DISTINCT FROM OLD.before_json::jsonb->>'payment_type'
                AND jsonb_typeof(NEW.readback_json::jsonb->'bids') = 'array'
                AND jsonb_typeof(OLD.before_json::jsonb->'bids') = 'array'
                AND jsonb_typeof(OLD.changes_json::jsonb) = 'array'
                AND jsonb_array_length(NEW.readback_json::jsonb->'bids')
                    = jsonb_array_length(OLD.before_json::jsonb->'bids')
                AND jsonb_array_length(OLD.before_json::jsonb->'bids')
                    = jsonb_array_length(OLD.changes_json::jsonb)
                AND (
                    SELECT count(*) = count(DISTINCT (
                        bid->>'nm_id', bid->>'placement'
                    ))
                    FROM jsonb_array_elements(NEW.readback_json::jsonb->'bids') bid
                )
                AND (
                    SELECT count(*) = count(DISTINCT (
                        bid->>'nm_id', bid->>'placement'
                    ))
                    FROM jsonb_array_elements(OLD.before_json::jsonb->'bids') bid
                )
                AND (
                    SELECT count(*) = count(DISTINCT (
                        change->>'nm_id', change->>'placement'
                    ))
                    FROM jsonb_array_elements(OLD.changes_json::jsonb) change
                )
                AND NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements(OLD.changes_json::jsonb) change
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(OLD.before_json::jsonb->'bids') bid
                        WHERE bid->>'nm_id' = change->>'nm_id'
                          AND bid->>'placement' = change->>'placement'
                          AND bid->>'bid_kopecks' = change->>'before_bid_kopecks'
                    ) OR NOT EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(NEW.readback_json::jsonb->'bids') bid
                        WHERE bid->>'nm_id' = change->>'nm_id'
                          AND bid->>'placement' = change->>'placement'
                          AND bid->>'bid_kopecks' = change->>'bid_kopecks'
                    )
                )
            INTO expected_after_proven;
        EXCEPTION WHEN OTHERS THEN
            expected_after_proven := false;
        END;
    END IF;

    IF OLD.status = 'prepared' AND NEW.status = 'approved' THEN
        IF NOT EXISTS (
            SELECT 1 FROM control.wb_plan_approvals approval
            WHERE approval.plan_id = OLD.plan_id
              AND approval.plan_digest = OLD.plan_digest
              AND approval.expires_at > security_now
        ) THEN
            RAISE EXCEPTION 'WB control plan approval artifact is missing';
        END IF;
    ELSIF OLD.status = 'approved' AND NEW.status = 'applying' THEN
        PERFORM pg_advisory_xact_lock(hashtextextended('wb/policy-revision', 0));
        PERFORM pg_advisory_xact_lock(
            hashtextextended('wb/' || OLD.account_id || '/' || OLD.advert_id::text, 0)
        );
        security_now := clock_timestamp();
        SELECT count(*) = 3 AND bool_and(
            enabled
            AND lease_expires_at > security_now
            AND (disabled_until IS NULL OR disabled_until <= security_now)
        ) INTO gates_active
        FROM control.wb_runtime_gates
        WHERE gate_key IN (
            'global',
            'account/' || OLD.account_id,
            'campaign/' || OLD.account_id || '/' || OLD.advert_id::text
        );
        IF NOT EXISTS (
            SELECT 1 FROM control.wb_plan_approvals approval
            WHERE approval.plan_id = OLD.plan_id
              AND approval.plan_digest = OLD.plan_digest
              AND approval.expires_at > security_now
        ) OR NOT EXISTS (
            SELECT 1 FROM control.wb_action_reservations reservation
            WHERE reservation.plan_id = OLD.plan_id
        ) OR OLD.policy_revision <> (
            SELECT max(policy_revision) FROM control.wb_policy_revisions
        ) OR gates_active IS DISTINCT FROM true
          OR EXISTS (
            SELECT 1 FROM control.wb_plans incident
            WHERE incident.account_id = OLD.account_id
              AND incident.advert_id = OLD.advert_id
              AND incident.plan_id <> OLD.plan_id
              AND incident.status IN ('reconciliation_required', 'ambiguous')
        ) THEN
            RAISE EXCEPTION 'WB control plan has no active approval/reservation';
        END IF;
        NEW.apply_started_at := security_now;
    ELSIF OLD.status IN ('prepared', 'approved') AND NEW.status = 'expired' THEN
        NEW.finished_at := security_now;
    ELSIF OLD.status = 'applying' AND NEW.status IN (
        'applied', 'reconciliation_required', 'ambiguous', 'rejected', 'failed'
    ) THEN
        IF NEW.status = 'applied' AND (
            NEW.write_response_json IS NULL
            OR expected_after_proven IS DISTINCT FROM true
        ) THEN
            RAISE EXCEPTION 'WB applied transition requires receipt and exact readback';
        END IF;
        NEW.finished_at := security_now;
    ELSIF OLD.status IN ('reconciliation_required', 'ambiguous')
          AND NEW.status = 'applied' THEN
        IF expected_after_proven IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'WB reconciliation readback does not prove the requested state';
        END IF;
        NEW.finished_at := security_now;
    ELSE
        RAISE EXCEPTION 'invalid WB control plan transition % -> %', OLD.status, NEW.status;
    END IF;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS wb_plans_transition_guard ON control.wb_plans;
CREATE TRIGGER wb_plans_transition_guard
BEFORE UPDATE ON control.wb_plans
FOR EACH ROW EXECUTE FUNCTION control.enforce_wb_plan_transition();

DROP TRIGGER IF EXISTS wb_policy_revisions_validate
    ON control.wb_policy_revisions;
CREATE TRIGGER wb_policy_revisions_validate
BEFORE INSERT ON control.wb_policy_revisions
FOR EACH ROW EXECUTE FUNCTION control.validate_wb_policy_revision_insert();

DROP TRIGGER IF EXISTS wb_policy_revisions_append_only
    ON control.wb_policy_revisions;
CREATE TRIGGER wb_policy_revisions_append_only
BEFORE UPDATE OR DELETE ON control.wb_policy_revisions
FOR EACH ROW EXECUTE FUNCTION control.reject_wb_append_only_mutation();

DROP TRIGGER IF EXISTS wb_prepare_reservations_validate
    ON control.wb_prepare_reservations;
CREATE TRIGGER wb_prepare_reservations_validate
BEFORE INSERT ON control.wb_prepare_reservations
FOR EACH ROW EXECUTE FUNCTION control.validate_wb_prepare_reservation_insert();

DROP TRIGGER IF EXISTS wb_prepare_reservations_append_only
    ON control.wb_prepare_reservations;
CREATE TRIGGER wb_prepare_reservations_append_only
BEFORE UPDATE OR DELETE ON control.wb_prepare_reservations
FOR EACH ROW EXECUTE FUNCTION control.reject_wb_append_only_mutation();

DROP TRIGGER IF EXISTS wb_plans_validate_insert ON control.wb_plans;
CREATE TRIGGER wb_plans_validate_insert
BEFORE INSERT ON control.wb_plans
FOR EACH ROW EXECUTE FUNCTION control.validate_wb_plan_insert();

DROP TRIGGER IF EXISTS wb_runtime_gates_validate_write
    ON control.wb_runtime_gates;
CREATE TRIGGER wb_runtime_gates_validate_write
BEFORE INSERT OR UPDATE ON control.wb_runtime_gates
FOR EACH ROW EXECUTE FUNCTION control.validate_wb_runtime_gate_write();

DROP TRIGGER IF EXISTS wb_plan_approvals_validate ON control.wb_plan_approvals;
CREATE TRIGGER wb_plan_approvals_validate
BEFORE INSERT ON control.wb_plan_approvals
FOR EACH ROW EXECUTE FUNCTION control.validate_wb_approval_insert();

DROP TRIGGER IF EXISTS wb_plan_approvals_append_only ON control.wb_plan_approvals;
CREATE TRIGGER wb_plan_approvals_append_only
BEFORE UPDATE OR DELETE ON control.wb_plan_approvals
FOR EACH ROW EXECUTE FUNCTION control.reject_wb_append_only_mutation();

DROP TRIGGER IF EXISTS wb_action_reservations_validate
    ON control.wb_action_reservations;
CREATE TRIGGER wb_action_reservations_validate
BEFORE INSERT ON control.wb_action_reservations
FOR EACH ROW EXECUTE FUNCTION control.validate_wb_reservation_insert();

DROP TRIGGER IF EXISTS wb_action_reservations_append_only
    ON control.wb_action_reservations;
CREATE TRIGGER wb_action_reservations_append_only
BEFORE UPDATE OR DELETE ON control.wb_action_reservations
FOR EACH ROW EXECUTE FUNCTION control.reject_wb_append_only_mutation();

DROP TRIGGER IF EXISTS wb_audit_events_append_only ON control.wb_audit_events;
CREATE TRIGGER wb_audit_events_append_only
BEFORE UPDATE OR DELETE ON control.wb_audit_events
FOR EACH ROW EXECUTE FUNCTION control.reject_wb_append_only_mutation();

REVOKE ALL ON TABLE control.wb_policy_revisions,
    control.wb_prepare_reservations, control.wb_plans,
    control.wb_plan_approvals,
    control.wb_runtime_gates, control.wb_action_reservations,
    control.wb_audit_events FROM PUBLIC, control_writer;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA control FROM PUBLIC, control_writer;
REVOKE ALL ON FUNCTION control.reject_wb_append_only_mutation() FROM PUBLIC, control_writer;
REVOKE ALL ON FUNCTION control.validate_wb_policy_revision_insert() FROM PUBLIC, control_writer;
REVOKE ALL ON FUNCTION control.validate_wb_prepare_reservation_insert() FROM PUBLIC, control_writer;
REVOKE ALL ON FUNCTION control.validate_wb_plan_insert() FROM PUBLIC, control_writer;
REVOKE ALL ON FUNCTION control.validate_wb_runtime_gate_write() FROM PUBLIC, control_writer;
REVOKE ALL ON FUNCTION control.validate_wb_approval_insert() FROM PUBLIC, control_writer;
REVOKE ALL ON FUNCTION control.validate_wb_reservation_insert() FROM PUBLIC, control_writer;
REVOKE ALL ON FUNCTION control.enforce_wb_plan_transition() FROM PUBLIC, control_writer;

GRANT SELECT, INSERT ON control.wb_policy_revisions TO control_writer;
GRANT SELECT, INSERT ON control.wb_prepare_reservations TO control_writer;
GRANT SELECT, INSERT ON control.wb_plans TO control_writer;
GRANT UPDATE (
    status, apply_started_at, finished_at, last_error_class,
    write_response_json, readback_json
) ON control.wb_plans TO control_writer;
GRANT SELECT, INSERT ON control.wb_plan_approvals TO control_writer;
GRANT SELECT ON control.wb_runtime_gates TO control_writer;
GRANT SELECT, INSERT ON control.wb_action_reservations TO control_writer;
GRANT SELECT, INSERT ON control.wb_audit_events TO control_writer;
GRANT USAGE, SELECT ON SEQUENCE control.wb_audit_events_id_seq TO control_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA control REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA control REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA control REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

COMMIT;
