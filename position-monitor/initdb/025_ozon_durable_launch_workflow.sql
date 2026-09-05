\set ON_ERROR_STOP on

BEGIN;

-- One row is both the durable continuation pointer and the fenced lease for a
-- launch plan.  A stable plan status means the action may execute once; an
-- in-progress or ambiguous status means the same action is readback-only.
CREATE TABLE control.ozon_campaign_launch_workflows (
    plan_id varchar(64) PRIMARY KEY
        REFERENCES control.ozon_campaign_plans(plan_id),
    action text NOT NULL CHECK (action IN (
        'create_campaign','add_products','activate_campaign'
    )),
    generation bigint NOT NULL DEFAULT 0 CHECK (generation >= 0),
    lease_owner_id varchar(128) CHECK (
        lease_owner_id IS NULL OR lease_owner_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    lease_token varchar(64) CHECK (
        lease_token IS NULL OR lease_token ~ '^[0-9a-f]{64}$'
    ),
    lease_claimed_at timestamptz,
    lease_expires_at timestamptz,
    write_started_at timestamptz,
    -- Approval is not a write command. Only an explicit authenticated apply
    -- request makes this outbox row visible to the launch consumer.
    requested_at timestamptz,
    requested_by_actor_id varchar(128) CHECK (
        requested_by_actor_id IS NULL
        OR requested_by_actor_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    create_identity_preflight_at timestamptz,
    create_identity_preflight_digest varchar(64) CHECK (
        create_identity_preflight_digest IS NULL
        OR create_identity_preflight_digest ~ '^[0-9a-f]{64}$'
    ),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_completed_at timestamptz,
    last_error_class varchar(64) CHECK (
        last_error_class IS NULL OR last_error_class ~ '^[a-z0-9_]+$'
    ),
    last_readback_json text CHECK (
        last_readback_json IS NULL OR (
            octet_length(last_readback_json) <= 131072
            AND jsonb_typeof(last_readback_json::jsonb) = 'object'
        )
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ozon_launch_workflow_lease_shape CHECK (
        (
            lease_owner_id IS NULL AND lease_token IS NULL
            AND lease_claimed_at IS NULL AND lease_expires_at IS NULL
            AND write_started_at IS NULL
        ) OR (
            generation > 0 AND lease_owner_id IS NOT NULL
            AND lease_token IS NOT NULL AND lease_claimed_at IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND lease_expires_at > lease_claimed_at
            AND lease_expires_at <= lease_claimed_at + interval '5 minutes'
            AND (
                write_started_at IS NULL
                OR (
                    write_started_at >= lease_claimed_at
                    AND write_started_at < lease_expires_at
                )
            )
        )
    ),
    CONSTRAINT ozon_launch_workflow_request_shape CHECK (
        (requested_at IS NULL AND requested_by_actor_id IS NULL)
        OR (requested_at IS NOT NULL AND requested_by_actor_id IS NOT NULL)
    ),
    CONSTRAINT ozon_launch_workflow_identity_preflight_shape CHECK (
        (create_identity_preflight_at IS NULL
         AND create_identity_preflight_digest IS NULL)
        OR (create_identity_preflight_at IS NOT NULL
            AND create_identity_preflight_digest IS NOT NULL
            AND requested_at IS NOT NULL
            AND create_identity_preflight_at>=requested_at)
    )
);
CREATE INDEX ozon_launch_workflow_recovery_idx
    ON control.ozon_campaign_launch_workflows(
        available_at,lease_expires_at,plan_id
    );

-- Static mode keeps its crash marker in a private local file, but the
-- authorization boundary is also recorded in PostgreSQL so it cannot be
-- erased or rewritten by the runtime process after a provider mutation.
CREATE TABLE control.ozon_static_guard_audit_events (
    event_id bigserial PRIMARY KEY,
    account_id varchar(128) NOT NULL CHECK (
        account_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    sku bigint CHECK (sku IS NULL OR sku>0),
    campaign_id bigint CHECK (campaign_id IS NULL OR campaign_id>0),
    mutation text CHECK (
        mutation IS NULL OR mutation IN ('activate','deactivate','set_bid')
    ),
    target_bid_microrubles bigint CHECK (target_bid_microrubles>0),
    config_digest varchar(64) NOT NULL CHECK (
        config_digest ~ '^[0-9a-f]{64}$'
    ),
    schema_version integer NOT NULL CHECK (schema_version>0),
    policy_revision bigint NOT NULL,
    policy_digest varchar(64) NOT NULL CHECK (
        policy_digest ~ '^[0-9a-f]{64}$'
    ),
    worker_id varchar(128) NOT NULL CHECK (
        worker_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    event_type text NOT NULL CHECK (
        event_type IN ('state_initialized','write_authorized')
    ),
    occurred_at timestamptz NOT NULL,
    CHECK (
        (
            event_type='state_initialized'
            AND sku IS NULL AND campaign_id IS NULL
            AND mutation IS NULL AND target_bid_microrubles IS NULL
        ) OR (
            event_type='write_authorized'
            AND sku IS NOT NULL AND campaign_id IS NOT NULL
            AND mutation IS NOT NULL
            AND (mutation='set_bid')=(target_bid_microrubles IS NOT NULL)
        )
    )
);
CREATE INDEX ozon_static_guard_audit_identity_idx
    ON control.ozon_static_guard_audit_events(
        account_id,campaign_id,event_id
    );
CREATE TRIGGER ozon_static_guard_audit_append_only
BEFORE UPDATE OR DELETE ON control.ozon_static_guard_audit_events
FOR EACH ROW EXECUTE FUNCTION control.reject_ozon_append_only_mutation();

-- Planner and executor deliberately have no UPDATE privilege on runtime
-- gates.  This narrow definer function takes row locks for the caller's
-- transaction, closing the revoke-after-check race without granting either
-- capability the ability to rewrite a gate.
CREATE OR REPLACE FUNCTION control.ozon_runtime_gates_active_locked(
    expected_account text,
    expected_sku bigint
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog,control
AS $$
DECLARE
    security_now timestamptz := clock_timestamp();
    active boolean;
BEGIN
    IF expected_account IS NULL
       OR expected_account !~ '^[A-Za-z0-9_.-]+$'
       OR octet_length(expected_account) NOT BETWEEN 1 AND 128
       OR expected_sku IS NULL OR expected_sku<=0 THEN
        RETURN false;
    END IF;
    SELECT count(*)=3 AND coalesce(bool_and(
        enabled AND lease_expires_at>security_now
        AND (disabled_until IS NULL OR disabled_until<=security_now)
    ),false)
    INTO active
    FROM (
        SELECT enabled,lease_expires_at,disabled_until
        FROM control.ozon_runtime_gates
        WHERE gate_key IN (
            'global','account/'||expected_account,
            'sku/'||expected_account||'/'||expected_sku::text
        )
        FOR SHARE
    ) locked_gates;
    RETURN active;
END
$$;

-- Preserve the exact uncertain mutation for plans created before this
-- migration.  The audit row is authoritative for ambiguous/failed states;
-- refusing to guess is safer than replaying a mutation.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM control.ozon_campaign_plans plan
        WHERE plan.status IN ('ambiguous','failed')
          AND NOT EXISTS (
              SELECT 1
              FROM control.ozon_campaign_audit_events event
              WHERE event.plan_id=plan.plan_id
                AND event.event_type IN (
                    'creating','adding_products','activating'
                )
          )
    ) THEN
        RAISE EXCEPTION
            'cannot infer the pending action for an existing Ozon plan';
    END IF;
END
$$;

-- A pre-025 create mutation has no durable proof that the title was absent
-- immediately before its HTTP boundary, so uncertain create remains a manual
-- reconciliation blocker. Failed add/activate rows already bind a provider
-- campaign id and can safely become readback-only ambiguous work, but only
-- after the manifest/audit validation below succeeds in this transaction.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM control.ozon_campaign_plans plan
        WHERE plan.status='creating'
           OR (
               plan.status='ambiguous'
               AND (
                   SELECT event.event_type
                   FROM control.ozon_campaign_audit_events event
                   WHERE event.plan_id=plan.plan_id
                     AND event.event_type IN (
                         'creating','adding_products','activating'
                     )
                   ORDER BY event.event_id DESC
                   LIMIT 1
               )='creating'
           )
           OR (
               plan.status='failed'
               AND (
                   SELECT event.event_type
                   FROM control.ozon_campaign_audit_events event
                   WHERE event.plan_id=plan.plan_id
                     AND event.event_type IN (
                         'creating','adding_products','activating'
                     )
                   ORDER BY event.event_id DESC
                   LIMIT 1
               )='creating'
           )
           OR (
               plan.status='failed'
               AND plan.campaign_id IS NULL
           )
    ) THEN
        RAISE EXCEPTION
            'legacy uncertain Ozon post-write state requires manual reconciliation before migration 025';
    END IF;
END
$$;

-- The 024 state trigger has no failed->ambiguous transition. Drop it only
-- inside this migration transaction; the stricter 025 trigger is recreated
-- after its replacement function below.
DROP TRIGGER ozon_plans_transition_guard
    ON control.ozon_campaign_plans;

WITH recoverable AS (
    SELECT plan.plan_id,plan.actor_id,plan.last_error_class,
           (
               SELECT event.event_type
               FROM control.ozon_campaign_audit_events event
               WHERE event.plan_id=plan.plan_id
                 AND event.event_type IN ('adding_products','activating')
               ORDER BY event.event_id DESC
               LIMIT 1
           ) AS action_event
    FROM control.ozon_campaign_plans plan
    WHERE plan.status='failed' AND plan.campaign_id IS NOT NULL
), reclassified AS (
    UPDATE control.ozon_campaign_plans plan
    SET status='ambiguous'
    FROM recoverable
    WHERE plan.plan_id=recoverable.plan_id
      AND recoverable.action_event IS NOT NULL
    RETURNING plan.plan_id,recoverable.actor_id,recoverable.last_error_class,
              recoverable.action_event
)
INSERT INTO control.ozon_campaign_audit_events(
    plan_id,actor_id,event_type,payload_json,created_at
)
SELECT plan_id,actor_id,'legacy_failed_reclassified',jsonb_build_object(
           'previous_status','failed',
           'action_event',action_event,
           'error_class',last_error_class,
           'recovery_mode','readback_only'
       )::text,clock_timestamp()
FROM reclassified;

INSERT INTO control.ozon_campaign_launch_workflows(
    plan_id,action,requested_at,requested_by_actor_id
)
SELECT plan.plan_id,
       CASE
           WHEN plan.status IN ('prepared','approved','creating')
               THEN 'create_campaign'
           WHEN plan.status IN ('created','adding_products')
               THEN 'add_products'
           WHEN plan.status IN ('products_added','activating','applied')
               THEN 'activate_campaign'
           WHEN plan.status IN ('ambiguous','failed') THEN (
               SELECT CASE event.event_type
                   WHEN 'creating' THEN 'create_campaign'
                   WHEN 'adding_products' THEN 'add_products'
                   WHEN 'activating' THEN 'activate_campaign'
               END
               FROM control.ozon_campaign_audit_events event
               WHERE event.plan_id=plan.plan_id
                 AND event.event_type IN (
                     'creating','adding_products','activating'
                 )
               ORDER BY event.event_id DESC
               LIMIT 1
           )
           ELSE 'create_campaign'
       END,
       CASE WHEN plan.status IN (
           'creating','created','adding_products','products_added',
           'activating','ambiguous','failed','applied'
       ) THEN COALESCE(plan.operation_started_at,plan.created_at) END,
       CASE WHEN plan.status IN (
           'creating','created','adding_products','products_added',
           'activating','ambiguous','failed','applied'
       ) THEN plan.actor_id END
FROM control.ozon_campaign_plans plan;

-- Rebind every provider request field to the signed manifest intent.  The
-- application recomputes manifest_digest; this trigger is defense in depth
-- against a direct control_writer inserting a different title/date/bid shape.
CREATE OR REPLACE FUNCTION control.validate_ozon_plan_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    active_policy control.ozon_policy_revisions%ROWTYPE;
    security_now timestamptz := clock_timestamp();
    manifest jsonb := NEW.manifest_json::jsonb;
    digest_input bytea := ''::bytea;
    digest_field bytea;
    computed_manifest_digest text;
    computed_plan_digest text;
    computed_plan_id text;
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
    IF jsonb_typeof(manifest) IS DISTINCT FROM 'object'
       OR jsonb_typeof(manifest#>'{spec}') IS DISTINCT FROM 'object'
       OR jsonb_typeof(manifest#>'{create_request}') IS DISTINCT FROM 'object'
       OR jsonb_typeof(manifest#>'{products_request}') IS DISTINCT FROM 'object'
       OR jsonb_typeof(manifest#>'{spec,skus}') IS DISTINCT FROM 'array'
       OR jsonb_typeof(manifest#>'{products_request,bids}') IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'Ozon plan manifest is invalid';
    END IF;
    FOREACH digest_field IN ARRAY ARRAY[
        convert_to('mcp-ozon/ozon-campaign-launch/v1','UTF8'),
        convert_to(manifest->>'actor_id','UTF8'),
        int4send((manifest->>'policy_schema_version')::integer),
        int8send((manifest->>'policy_revision')::bigint),
        convert_to(manifest->>'policy_digest','UTF8'),
        convert_to(manifest#>>'{spec,account_id}','UTF8'),
        convert_to(manifest#>>'{spec,title}','UTF8'),
        convert_to(manifest#>>'{spec,from_date}','UTF8'),
        convert_to(manifest#>>'{spec,to_date}','UTF8'),
        int8send((manifest#>>'{spec,weekly_budget_microrubles}')::bigint),
        int8send((manifest#>>'{spec,per_sku_spend_cap_microrubles}')::bigint),
        int8send((manifest#>>'{spec,initial_cpc_bid_microrubles}')::bigint),
        int8send((manifest#>>'{spec,max_cpc_bid_microrubles}')::bigint),
        decode(lpad(to_hex((manifest#>>'{spec,target_drr_percent}')::integer),2,'0'),'hex'),
        decode(lpad(to_hex((manifest#>>'{spec,target_position}')::integer),2,'0'),'hex'),
        int8send((manifest#>>'{spec,skus,0}')::bigint)
    ] LOOP
        digest_input := digest_input
            || int8send(octet_length(digest_field)::bigint)
            || digest_field;
    END LOOP;
    computed_manifest_digest := encode(sha256(digest_input),'hex');
    digest_input := ''::bytea;
    FOREACH digest_field IN ARRAY ARRAY[
        convert_to('mcp-ozon/ozon-plan/v1','UTF8'),
        convert_to(computed_manifest_digest,'UTF8'),
        int8send((extract(epoch FROM NEW.created_at)*1000000)::bigint),
        int8send((extract(epoch FROM NEW.expires_at)*1000000)::bigint)
    ] LOOP
        digest_input := digest_input
            || int8send(octet_length(digest_field)::bigint)
            || digest_field;
    END LOOP;
    computed_plan_digest := encode(sha256(digest_input),'hex');
    digest_input := ''::bytea;
    FOREACH digest_field IN ARRAY ARRAY[
        convert_to('mcp-ozon/ozon-plan-id/v1','UTF8'),
        convert_to(computed_plan_digest,'UTF8')
    ] LOOP
        digest_input := digest_input
            || int8send(octet_length(digest_field)::bigint)
            || digest_field;
    END LOOP;
    computed_plan_id := encode(sha256(digest_input),'hex');
    IF NEW.status IS DISTINCT FROM 'prepared'
       OR (manifest->>'manifest_digest' ~ '^[0-9a-f]{64}$') IS DISTINCT FROM true
       OR manifest->>'manifest_digest' IS DISTINCT FROM computed_manifest_digest
       OR NEW.created_at<security_now-interval '1 second'
       OR NEW.created_at>security_now+interval '1 second'
       OR NEW.expires_at IS DISTINCT FROM NEW.created_at+interval '15 minutes'
       OR NEW.plan_digest IS DISTINCT FROM computed_plan_digest
       OR NEW.plan_id IS DISTINCT FROM computed_plan_id
       OR manifest->>'actor_id' IS DISTINCT FROM NEW.actor_id
       OR manifest->>'policy_digest' IS DISTINCT FROM NEW.policy_digest
       OR (manifest->>'policy_schema_version')::integer IS DISTINCT FROM NEW.schema_version
       OR (manifest->>'policy_revision')::bigint IS DISTINCT FROM NEW.policy_revision
       OR manifest#>>'{spec,account_id}' IS DISTINCT FROM NEW.account_id
       OR jsonb_array_length(manifest#>'{spec,skus}') IS DISTINCT FROM 1
       OR (manifest#>>'{spec,skus,0}')::bigint IS DISTINCT FROM NEW.sku
       OR octet_length(manifest#>>'{spec,title}') NOT BETWEEN 1 AND 128
       OR (manifest#>>'{spec,from_date}' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') IS DISTINCT FROM true
       OR (manifest#>>'{spec,to_date}' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') IS DISTINCT FROM true
       OR (manifest#>>'{spec,to_date}') < (manifest#>>'{spec,from_date}')
       OR manifest#>>'{create_request,title}' IS DISTINCT FROM 'mcp-ozon-'||NEW.plan_id
       OR manifest#>>'{create_request,fromDate}' IS DISTINCT FROM manifest#>>'{spec,from_date}'
       OR manifest#>>'{create_request,toDate}' IS DISTINCT FROM manifest#>>'{spec,to_date}'
       OR manifest#>>'{create_request,productAutopilotStrategy}' IS DISTINCT FROM 'TARGET_BIDS'
       OR manifest#>>'{create_request,placement}' IS DISTINCT FROM 'PLACEMENT_SEARCH_AND_CATEGORY'
       OR (manifest#>>'{create_request,weeklyBudget}')::bigint
          IS DISTINCT FROM (manifest#>>'{spec,weekly_budget_microrubles}')::bigint
       OR (manifest#>>'{spec,weekly_budget_microrubles}')::bigint
          IS DISTINCT FROM (manifest#>>'{spec,per_sku_spend_cap_microrubles}')::bigint
       OR ((manifest#>>'{spec,target_drr_percent}')::integer BETWEEN 10 AND 100)
          IS DISTINCT FROM true
       OR ((manifest#>>'{spec,target_position}')::integer BETWEEN 1 AND 30)
          IS DISTINCT FROM true
       OR ((manifest#>>'{spec,initial_cpc_bid_microrubles}')::bigint>0)
          IS DISTINCT FROM true
       OR (manifest#>>'{spec,initial_cpc_bid_microrubles}')::bigint
          > (manifest#>>'{spec,max_cpc_bid_microrubles}')::bigint
       OR ((manifest#>>'{spec,weekly_budget_microrubles}')::bigint>0)
          IS DISTINCT FROM true
       OR jsonb_array_length(manifest#>'{products_request,bids}') IS DISTINCT FROM 1
       OR manifest#>>'{products_request,bids,0,sku}' IS DISTINCT FROM NEW.sku::text
       OR (manifest#>>'{products_request,bids,0,bid}')::bigint
          IS DISTINCT FROM (manifest#>>'{spec,initial_cpc_bid_microrubles}')::bigint
       OR manifest#>'{products_request,bids,0}'?'targetCir'
       OR manifest#>'{products_request,bids,0}'?'topPosition'
       OR manifest->>'activation_required' IS DISTINCT FROM 'true' THEN
        RAISE EXCEPTION 'Ozon plan manifest does not match immutable columns';
    END IF;
    RETURN NEW;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range
    OR null_value_not_allowed OR invalid_parameter_value THEN
    RAISE EXCEPTION 'Ozon plan manifest is invalid';
END
$$;

-- Old 024 rows were not fully bound to their provider request projection.
-- Abort the upgrade on any drift; silently legitimising such a row would let
-- a previously approved human intent execute different title/date fields.
DO $$
DECLARE
    stored record;
    manifest jsonb;
    digest_input bytea;
    digest_field bytea;
    computed_manifest_digest text;
    computed_plan_id text;
BEGIN
    FOR stored IN
        SELECT plan_id,plan_digest,actor_id,account_id,sku,schema_version,
               policy_revision,policy_digest,manifest_json,created_at,expires_at
        FROM control.ozon_campaign_plans
    LOOP
        manifest := stored.manifest_json::jsonb;
        IF jsonb_typeof(manifest) IS DISTINCT FROM 'object'
           OR jsonb_typeof(manifest#>'{spec}') IS DISTINCT FROM 'object'
           OR jsonb_typeof(manifest#>'{create_request}') IS DISTINCT FROM 'object'
           OR jsonb_typeof(manifest#>'{products_request,bids}') IS DISTINCT FROM 'array'
           OR jsonb_array_length(manifest#>'{products_request,bids}') IS DISTINCT FROM 1 THEN
            RAISE EXCEPTION 'legacy Ozon plan manifest requires manual reconciliation before migration 025';
        END IF;
        digest_input := ''::bytea;
        FOREACH digest_field IN ARRAY ARRAY[
            convert_to('mcp-ozon/ozon-campaign-launch/v1','UTF8'),
            convert_to(manifest->>'actor_id','UTF8'),
            int4send((manifest->>'policy_schema_version')::integer),
            int8send((manifest->>'policy_revision')::bigint),
            convert_to(manifest->>'policy_digest','UTF8'),
            convert_to(manifest#>>'{spec,account_id}','UTF8'),
            convert_to(manifest#>>'{spec,title}','UTF8'),
            convert_to(manifest#>>'{spec,from_date}','UTF8'),
            convert_to(manifest#>>'{spec,to_date}','UTF8'),
            int8send((manifest#>>'{spec,weekly_budget_microrubles}')::bigint),
            int8send((manifest#>>'{spec,per_sku_spend_cap_microrubles}')::bigint),
            int8send((manifest#>>'{spec,initial_cpc_bid_microrubles}')::bigint),
            int8send((manifest#>>'{spec,max_cpc_bid_microrubles}')::bigint),
            decode(lpad(to_hex((manifest#>>'{spec,target_drr_percent}')::integer),2,'0'),'hex'),
            decode(lpad(to_hex((manifest#>>'{spec,target_position}')::integer),2,'0'),'hex'),
            int8send((manifest#>>'{spec,skus,0}')::bigint)
        ] LOOP
            digest_input := digest_input
                || int8send(octet_length(digest_field)::bigint)
                || digest_field;
        END LOOP;
        computed_manifest_digest := encode(sha256(digest_input),'hex');
        -- Migration 024 replaced created_at in its BEFORE INSERT trigger
        -- after the old repository had hashed an earlier database timestamp.
        -- That preimage is unrecoverable. Retain the legacy plan digest as an
        -- opaque artifact of the formerly trusted writer, bind plan_id to it,
        -- and require the append-only prepared event below to bind both it and
        -- the independently recomputed manifest digest. New 025 INSERTs still
        -- recompute the complete timestamp-bound chain.
        digest_input := ''::bytea;
        FOREACH digest_field IN ARRAY ARRAY[
            convert_to('mcp-ozon/ozon-plan-id/v1','UTF8'),
            convert_to(stored.plan_digest,'UTF8')
        ] LOOP
            digest_input := digest_input
                || int8send(octet_length(digest_field)::bigint)
                || digest_field;
        END LOOP;
        computed_plan_id := encode(sha256(digest_input),'hex');
        IF manifest->>'manifest_digest' IS DISTINCT FROM computed_manifest_digest
           OR stored.plan_id IS DISTINCT FROM computed_plan_id
           OR NOT EXISTS (
               SELECT 1
               FROM control.ozon_campaign_audit_events prepared_event
               WHERE prepared_event.plan_id=stored.plan_id
                 AND prepared_event.event_type='prepared'
                 AND prepared_event.payload_json::jsonb->>'plan_digest'=stored.plan_digest
                 AND prepared_event.payload_json::jsonb->>'manifest_digest'=computed_manifest_digest
           )
           OR manifest->>'actor_id' IS DISTINCT FROM stored.actor_id
           OR manifest->>'policy_digest' IS DISTINCT FROM stored.policy_digest
           OR (manifest->>'policy_schema_version')::integer IS DISTINCT FROM stored.schema_version
           OR (manifest->>'policy_revision')::bigint IS DISTINCT FROM stored.policy_revision
           OR manifest#>>'{spec,account_id}' IS DISTINCT FROM stored.account_id
           OR (manifest#>>'{spec,skus,0}')::bigint IS DISTINCT FROM stored.sku
           OR manifest#>>'{create_request,title}' IS DISTINCT FROM manifest#>>'{spec,title}'
           OR manifest#>>'{create_request,fromDate}' IS DISTINCT FROM manifest#>>'{spec,from_date}'
           OR manifest#>>'{create_request,toDate}' IS DISTINCT FROM manifest#>>'{spec,to_date}'
           OR (manifest#>>'{create_request,weeklyBudget}')::bigint
              IS DISTINCT FROM (manifest#>>'{spec,weekly_budget_microrubles}')::bigint
           OR manifest#>>'{create_request,productAutopilotStrategy}' IS DISTINCT FROM 'TARGET_BIDS'
           OR manifest#>>'{create_request,placement}' IS DISTINCT FROM 'PLACEMENT_SEARCH_AND_CATEGORY'
           OR manifest#>>'{products_request,bids,0,sku}' IS DISTINCT FROM stored.sku::text
           OR (manifest#>>'{products_request,bids,0,bid}')::bigint
              IS DISTINCT FROM (manifest#>>'{spec,initial_cpc_bid_microrubles}')::bigint
           OR manifest#>'{products_request,bids,0}'?'targetCir'
           OR manifest#>'{products_request,bids,0}'?'topPosition'
           OR manifest->>'activation_required' IS DISTINCT FROM 'true' THEN
            RAISE EXCEPTION 'legacy Ozon plan manifest requires manual reconciliation before migration 025';
        END IF;
    END LOOP;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range
    OR null_value_not_allowed OR invalid_parameter_value THEN
    RAISE EXCEPTION 'legacy Ozon plan manifest requires manual reconciliation before migration 025';
END
$$;

CREATE OR REPLACE FUNCTION control.initialize_ozon_launch_workflow()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog,control
AS $$
BEGIN
    INSERT INTO control.ozon_campaign_launch_workflows(plan_id,action)
    VALUES(NEW.plan_id,'create_campaign');
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION control.enforce_ozon_launch_workflow_update()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    security_now timestamptz := clock_timestamp();
    plan_status text;
    plan_actor_id text;
BEGIN
    IF NEW.plan_id<>OLD.plan_id OR NEW.created_at<>OLD.created_at THEN
        RAISE EXCEPTION 'Ozon launch workflow identity is immutable';
    END IF;
    SELECT status,actor_id INTO STRICT plan_status,plan_actor_id
    FROM control.ozon_campaign_plans WHERE plan_id=OLD.plan_id;

    IF NEW.generation=OLD.generation
       AND OLD.requested_at IS NULL
       AND OLD.requested_by_actor_id IS NULL
       AND NEW.requested_at IS NOT NULL
       AND NEW.requested_by_actor_id IS NOT NULL THEN
        IF plan_status<>'approved'
           OR OLD.lease_owner_id IS NOT NULL OR NEW.lease_owner_id IS NOT NULL
           OR OLD.lease_token IS NOT NULL OR NEW.lease_token IS NOT NULL
           OR OLD.lease_claimed_at IS NOT NULL OR NEW.lease_claimed_at IS NOT NULL
           OR OLD.lease_expires_at IS NOT NULL OR NEW.lease_expires_at IS NOT NULL
           OR OLD.write_started_at IS NOT NULL OR NEW.write_started_at IS NOT NULL
           OR NEW.action<>OLD.action
           OR NEW.last_completed_at IS DISTINCT FROM OLD.last_completed_at
           OR NEW.last_error_class IS DISTINCT FROM OLD.last_error_class
           OR NEW.last_readback_json IS DISTINCT FROM OLD.last_readback_json
           OR NEW.create_identity_preflight_at IS DISTINCT FROM OLD.create_identity_preflight_at
           OR NEW.create_identity_preflight_digest IS DISTINCT FROM OLD.create_identity_preflight_digest
           OR NEW.created_at<>OLD.created_at
           OR NEW.requested_at<security_now-interval '1 second'
           OR NEW.requested_at>security_now+interval '1 second'
           OR NEW.requested_by_actor_id<>plan_actor_id
           OR NEW.available_at<>NEW.requested_at
        THEN
            RAISE EXCEPTION 'invalid Ozon launch enqueue';
        END IF;
    ELSIF NEW.requested_at IS DISTINCT FROM OLD.requested_at
          OR NEW.requested_by_actor_id IS DISTINCT FROM OLD.requested_by_actor_id THEN
        RAISE EXCEPTION 'Ozon launch request identity is immutable';
    ELSIF NEW.generation=OLD.generation+1 THEN
        IF NEW.action<>OLD.action
           OR OLD.requested_at IS NULL
           OR OLD.available_at>security_now
           OR (OLD.lease_expires_at IS NOT NULL
               AND OLD.lease_expires_at>security_now)
           OR NEW.lease_owner_id IS NULL OR NEW.lease_token IS NULL
           OR NEW.lease_claimed_at IS NULL OR NEW.lease_expires_at IS NULL
           OR NEW.write_started_at IS NOT NULL
           OR NEW.last_completed_at IS DISTINCT FROM OLD.last_completed_at
           OR NEW.last_error_class IS DISTINCT FROM OLD.last_error_class
           OR NEW.last_readback_json IS DISTINCT FROM OLD.last_readback_json
           OR NEW.create_identity_preflight_at IS DISTINCT FROM OLD.create_identity_preflight_at
           OR NEW.create_identity_preflight_digest IS DISTINCT FROM OLD.create_identity_preflight_digest
           OR NEW.available_at IS DISTINCT FROM OLD.available_at
           OR NEW.lease_claimed_at<security_now-interval '1 second'
           OR NEW.lease_claimed_at>security_now+interval '1 second'
           OR NEW.lease_expires_at>security_now+interval '5 minutes'
        THEN
            RAISE EXCEPTION 'invalid Ozon launch workflow claim';
        END IF;
    ELSIF NEW.generation=OLD.generation THEN
        IF OLD.lease_owner_id IS NULL OR OLD.lease_token IS NULL
           OR OLD.lease_claimed_at IS NULL OR OLD.lease_expires_at IS NULL
           OR NEW.action NOT IN (OLD.action, CASE OLD.action
               WHEN 'create_campaign' THEN 'add_products'
               WHEN 'add_products' THEN 'activate_campaign'
               ELSE 'activate_campaign'
           END)
        THEN
            RAISE EXCEPTION 'unclaimed Ozon launch workflow cannot change';
        END IF;

        IF NEW.lease_owner_id IS NOT DISTINCT FROM OLD.lease_owner_id
           AND NEW.lease_token IS NOT DISTINCT FROM OLD.lease_token
           AND NEW.lease_claimed_at IS NOT DISTINCT FROM OLD.lease_claimed_at
           AND NEW.lease_expires_at IS NOT DISTINCT FROM OLD.lease_expires_at
           AND OLD.write_started_at IS NULL
           AND NEW.write_started_at IS NOT NULL THEN
            IF NEW.action<>OLD.action
               OR NEW.last_completed_at IS DISTINCT FROM OLD.last_completed_at
               OR NEW.last_error_class IS DISTINCT FROM OLD.last_error_class
               OR NEW.last_readback_json IS DISTINCT FROM OLD.last_readback_json
               OR NEW.available_at IS DISTINCT FROM OLD.available_at
               OR NEW.write_started_at<security_now-interval '1 second'
               OR NEW.write_started_at>security_now+interval '1 second'
               OR OLD.lease_expires_at<=security_now THEN
                RAISE EXCEPTION 'invalid Ozon launch write start';
            END IF;
            IF OLD.action='create_campaign' THEN
                IF NEW.create_identity_preflight_at IS DISTINCT FROM NEW.write_started_at
                   OR NEW.create_identity_preflight_digest IS NULL THEN
                    RAISE EXCEPTION 'Ozon create identity preflight is missing';
                END IF;
            ELSIF NEW.create_identity_preflight_at IS DISTINCT FROM OLD.create_identity_preflight_at
                  OR NEW.create_identity_preflight_digest IS DISTINCT FROM OLD.create_identity_preflight_digest THEN
                RAISE EXCEPTION 'unexpected Ozon create identity preflight';
            END IF;
        ELSIF NEW.lease_owner_id IS NOT DISTINCT FROM OLD.lease_owner_id
              AND NEW.lease_token IS NOT DISTINCT FROM OLD.lease_token
              AND NEW.lease_claimed_at IS NOT DISTINCT FROM OLD.lease_claimed_at
              AND NEW.lease_expires_at IS NOT DISTINCT FROM OLD.lease_expires_at
              AND NEW.write_started_at IS NOT DISTINCT FROM OLD.write_started_at
              AND NEW.action=OLD.action THEN
            -- A recovery readback is durably attached before the guarded plan
            -- transition consumes it.  No other evidence may change here.
            IF plan_status NOT IN (
                    'creating','adding_products','activating','ambiguous'
               )
               OR NEW.last_completed_at IS DISTINCT FROM OLD.last_completed_at
               OR NEW.last_error_class IS DISTINCT FROM OLD.last_error_class
               OR NEW.last_readback_json IS NULL
               OR NEW.available_at IS DISTINCT FROM OLD.available_at
               OR NEW.create_identity_preflight_at IS DISTINCT FROM OLD.create_identity_preflight_at
               OR NEW.create_identity_preflight_digest IS DISTINCT FROM OLD.create_identity_preflight_digest
               OR OLD.lease_expires_at<=security_now THEN
                RAISE EXCEPTION 'invalid Ozon recovery readback';
            END IF;
        ELSIF NEW.lease_owner_id IS NULL AND NEW.lease_token IS NULL
              AND NEW.lease_claimed_at IS NULL AND NEW.lease_expires_at IS NULL
              AND NEW.write_started_at IS NULL THEN
            IF OLD.lease_expires_at<=security_now THEN
                RAISE EXCEPTION 'expired Ozon launch lease cannot commit';
            END IF;
            IF NEW.create_identity_preflight_at IS DISTINCT FROM OLD.create_identity_preflight_at
               OR NEW.create_identity_preflight_digest IS DISTINCT FROM OLD.create_identity_preflight_digest THEN
                RAISE EXCEPTION 'Ozon create identity evidence is immutable';
            END IF;
            IF plan_status IN ('created','products_added','applied')
               AND NEW.last_completed_at IS DISTINCT FROM OLD.last_completed_at THEN
                IF NEW.last_completed_at IS NULL OR NEW.last_error_class IS NOT NULL
                   OR NEW.available_at<security_now-interval '1 second'
                   OR NEW.available_at>security_now+interval '1 second'
                   OR (plan_status='created' AND NOT (
                       OLD.action='create_campaign' AND NEW.action='add_products'
                   ))
                   OR (plan_status='products_added' AND NOT (
                       OLD.action='add_products' AND NEW.action='activate_campaign'
                   ))
                   OR (plan_status='applied' AND NEW.action<>'activate_campaign')
                THEN
                    RAISE EXCEPTION 'invalid Ozon launch workflow completion';
                END IF;
            ELSIF plan_status IN ('approved','created','products_added') THEN
                IF OLD.write_started_at IS NOT NULL OR NEW.action<>OLD.action
                   OR NEW.available_at<security_now+interval '119 seconds'
                   OR NEW.available_at>security_now+interval '121 seconds'
                   OR NEW.last_completed_at IS DISTINCT FROM OLD.last_completed_at
                   OR NEW.last_error_class IS DISTINCT FROM (CASE OLD.action
                       WHEN 'create_campaign' THEN 'ozon_create_not_started'
                       WHEN 'add_products' THEN 'ozon_products_not_started'
                       WHEN 'activate_campaign' THEN 'ozon_activate_not_started'
                   END)
                   OR NEW.last_readback_json IS DISTINCT FROM OLD.last_readback_json
                THEN
                    RAISE EXCEPTION 'invalid Ozon launch lease release';
                END IF;
            ELSIF plan_status IN ('ambiguous','failed') THEN
                IF NEW.action<>OLD.action OR NEW.last_error_class IS NULL
                   OR NEW.available_at<security_now+interval '119 seconds'
                   OR NEW.available_at>security_now+interval '121 seconds' THEN
                    RAISE EXCEPTION 'ambiguous Ozon launch evidence is incomplete';
                END IF;
            ELSIF plan_status IN ('creating','adding_products','activating') THEN
                -- A caller may relinquish an in-progress lease after a local
                -- interruption.  The next claim is necessarily reconciliation.
                IF NEW.action<>OLD.action
                   OR NEW.available_at<security_now-interval '1 second'
                   OR NEW.available_at>security_now+interval '121 seconds'
                   OR NEW.last_completed_at IS DISTINCT FROM OLD.last_completed_at
                   OR NEW.last_error_class IS DISTINCT FROM OLD.last_error_class
                   OR NEW.last_readback_json IS DISTINCT FROM OLD.last_readback_json
                THEN
                    RAISE EXCEPTION 'invalid Ozon recovery lease release';
                END IF;
            ELSE
                RAISE EXCEPTION 'invalid terminal Ozon launch workflow update';
            END IF;
        ELSE
            RAISE EXCEPTION 'Ozon launch lease fencing fields cannot change';
        END IF;
    ELSE
        RAISE EXCEPTION 'Ozon launch workflow generation must advance by one';
    END IF;
    NEW.updated_at := security_now;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS ozon_launch_workflow_initialize
    ON control.ozon_campaign_plans;
CREATE TRIGGER ozon_launch_workflow_initialize
AFTER INSERT ON control.ozon_campaign_plans
FOR EACH ROW EXECUTE FUNCTION control.initialize_ozon_launch_workflow();

CREATE TRIGGER ozon_launch_workflow_update_guard
BEFORE UPDATE ON control.ozon_campaign_launch_workflows
FOR EACH ROW EXECUTE FUNCTION control.enforce_ozon_launch_workflow_update();

CREATE TRIGGER ozon_launch_workflow_no_delete
BEFORE DELETE ON control.ozon_campaign_launch_workflows
FOR EACH ROW EXECUTE FUNCTION control.reject_ozon_append_only_mutation();

-- Replace the old transition trigger with a workflow-fenced contract.  Only a
-- lease that is still current may cross an external-write boundary or commit
-- its outcome.  Recovery may skip directly to Applied only with exact running
-- readback; it can never repeat the uncertain mutation.
CREATE OR REPLACE FUNCTION control.enforce_ozon_plan_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    security_now timestamptz := clock_timestamp();
    gates_active boolean;
    workflow control.ozon_campaign_launch_workflows%ROWTYPE;
    expected_action text;
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

    expected_action := CASE
        WHEN OLD.status IN ('approved','creating') THEN 'create_campaign'
        WHEN OLD.status IN ('created','adding_products') THEN 'add_products'
        WHEN OLD.status IN ('products_added','activating') THEN 'activate_campaign'
        ELSE NULL
    END;
    IF expected_action IS NOT NULL OR OLD.status='ambiguous' THEN
        SELECT * INTO STRICT workflow
        FROM control.ozon_campaign_launch_workflows
        WHERE plan_id=OLD.plan_id;
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
           OR OLD.policy_revision IS DISTINCT FROM
              (SELECT max(policy_revision) FROM control.ozon_policy_revisions)
           OR workflow.action<>'create_campaign'
           OR workflow.requested_at IS NULL
           OR workflow.requested_by_actor_id<>OLD.actor_id
           OR workflow.write_started_at IS NULL
           OR workflow.lease_expires_at<=security_now
        THEN RAISE EXCEPTION 'Ozon apply authorization is not active'; END IF;
        NEW.operation_started_at := security_now;
    ELSIF OLD.status='creating' AND NEW.status='created' THEN
        IF NEW.campaign_id IS NULL OR workflow.action<>'create_campaign'
           OR workflow.create_identity_preflight_at IS NULL
           OR workflow.create_identity_preflight_digest IS NULL
           OR workflow.lease_expires_at<=security_now
           OR workflow.last_readback_json IS NULL
           OR workflow.last_readback_json::jsonb->>'campaign_id'
                IS DISTINCT FROM NEW.campaign_id::text
           OR workflow.last_readback_json::jsonb->>'title'
                IS DISTINCT FROM OLD.manifest_json::jsonb#>>'{create_request,title}'
           OR (workflow.last_readback_json::jsonb->>'state' = ANY(ARRAY[
                'CAMPAIGN_STATE_STOPPED','CAMPAIGN_STATE_INACTIVE',
                'CAMPAIGN_STATE_PLANNED'
           ])) IS DISTINCT FROM true
           OR workflow.last_readback_json::jsonb->>'action'
                IS DISTINCT FROM 'create_campaign'
           OR workflow.last_readback_json::jsonb->>'verified'
                IS DISTINCT FROM 'true'
        THEN RAISE EXCEPTION 'Ozon campaign creation evidence is invalid'; END IF;
    ELSIF OLD.status='created' AND NEW.status='adding_products' THEN
        SELECT count(*)=3 AND bool_and(
            enabled AND lease_expires_at>security_now
            AND (disabled_until IS NULL OR disabled_until<=security_now)
        ) INTO gates_active FROM control.ozon_runtime_gates
        WHERE gate_key IN (
            'global','account/'||OLD.account_id,
            'sku/'||OLD.account_id||'/'||OLD.sku::text
        );
        IF gates_active IS DISTINCT FROM true
           OR NOT EXISTS(SELECT 1 FROM control.ozon_campaign_plan_approvals a
               WHERE a.plan_id=OLD.plan_id AND a.plan_digest=OLD.plan_digest)
           OR OLD.policy_revision IS DISTINCT FROM
              (SELECT max(policy_revision) FROM control.ozon_policy_revisions)
           OR workflow.action<>'add_products' OR workflow.write_started_at IS NULL
           OR workflow.lease_expires_at<=security_now
        THEN RAISE EXCEPTION 'Ozon add-products authorization is not active'; END IF;
    ELSIF OLD.status='adding_products' AND NEW.status='products_added' THEN
        IF workflow.action<>'add_products' OR workflow.lease_expires_at<=security_now
           OR workflow.last_readback_json IS NULL
           OR workflow.last_readback_json::jsonb->>'campaign_id' IS DISTINCT FROM NEW.campaign_id::text
           OR workflow.last_readback_json::jsonb->>'sku' IS DISTINCT FROM NEW.sku::text
           OR workflow.last_readback_json::jsonb->>'title'
                IS DISTINCT FROM OLD.manifest_json::jsonb#>>'{create_request,title}'
           OR workflow.last_readback_json::jsonb->>'bid_microrubles'
                IS DISTINCT FROM OLD.manifest_json::jsonb#>>'{spec,initial_cpc_bid_microrubles}'
           OR (workflow.last_readback_json::jsonb->>'state' = ANY(ARRAY[
                'CAMPAIGN_STATE_STOPPED','CAMPAIGN_STATE_INACTIVE',
                'CAMPAIGN_STATE_PLANNED'
           ])) IS DISTINCT FROM true
           OR workflow.last_readback_json::jsonb->>'action' IS DISTINCT FROM 'add_products'
           OR workflow.last_readback_json::jsonb->>'verified' IS DISTINCT FROM 'true'
        THEN RAISE EXCEPTION 'Ozon product attachment evidence is invalid'; END IF;
    ELSIF OLD.status='products_added' AND NEW.status='activating' THEN
        SELECT count(*)=3 AND bool_and(
            enabled AND lease_expires_at>security_now
            AND (disabled_until IS NULL OR disabled_until<=security_now)
        ) INTO gates_active FROM control.ozon_runtime_gates
        WHERE gate_key IN (
            'global','account/'||OLD.account_id,
            'sku/'||OLD.account_id||'/'||OLD.sku::text
        );
        IF gates_active IS DISTINCT FROM true
           OR NOT EXISTS(SELECT 1 FROM control.ozon_campaign_plan_approvals a
               WHERE a.plan_id=OLD.plan_id AND a.plan_digest=OLD.plan_digest)
           OR OLD.policy_revision IS DISTINCT FROM
              (SELECT max(policy_revision) FROM control.ozon_policy_revisions)
           OR workflow.action<>'activate_campaign' OR workflow.write_started_at IS NULL
           OR workflow.lease_expires_at<=security_now
        THEN RAISE EXCEPTION 'Ozon activation authorization is not active'; END IF;
    ELSIF OLD.status='activating' AND NEW.status='applied' THEN
        IF workflow.action<>'activate_campaign' OR workflow.lease_expires_at<=security_now
           OR NEW.campaign_id IS NULL OR NEW.readback_json IS NULL
           OR workflow.last_readback_json IS NULL
           OR NEW.readback_json IS DISTINCT FROM workflow.last_readback_json
           OR NEW.readback_json::jsonb->>'campaign_id' IS DISTINCT FROM NEW.campaign_id::text
           OR NEW.readback_json::jsonb->>'sku' IS DISTINCT FROM NEW.sku::text
           OR NEW.readback_json::jsonb->>'title'
                IS DISTINCT FROM OLD.manifest_json::jsonb#>>'{create_request,title}'
           OR NEW.readback_json::jsonb->>'bid_microrubles'
                IS DISTINCT FROM OLD.manifest_json::jsonb#>>'{spec,initial_cpc_bid_microrubles}'
           OR NEW.readback_json::jsonb->>'state' IS DISTINCT FROM 'CAMPAIGN_STATE_RUNNING'
        THEN RAISE EXCEPTION 'Ozon exact readback is required'; END IF;
        NEW.finished_at := security_now;
    ELSIF OLD.status IN ('creating','adding_products') AND NEW.status='applied' THEN
        IF workflow.action<>expected_action OR workflow.lease_expires_at<=security_now
           OR NEW.campaign_id IS NULL OR NEW.readback_json IS NULL
           OR workflow.last_readback_json IS NULL
           OR NEW.readback_json IS DISTINCT FROM workflow.last_readback_json
           OR NEW.readback_json::jsonb->>'campaign_id' IS DISTINCT FROM NEW.campaign_id::text
           OR NEW.readback_json::jsonb->>'sku' IS DISTINCT FROM NEW.sku::text
           OR NEW.readback_json::jsonb->>'title'
                IS DISTINCT FROM OLD.manifest_json::jsonb#>>'{create_request,title}'
           OR NEW.readback_json::jsonb->>'bid_microrubles'
                IS DISTINCT FROM OLD.manifest_json::jsonb#>>'{spec,initial_cpc_bid_microrubles}'
           OR NEW.readback_json::jsonb->>'state' IS DISTINCT FROM 'CAMPAIGN_STATE_RUNNING'
        THEN RAISE EXCEPTION 'Ozon recovery exact readback is required'; END IF;
        NEW.finished_at := security_now;
    ELSIF OLD.status IN ('approved','created','products_added')
          AND NEW.status='failed' THEN
        IF workflow.lease_expires_at<=security_now
           OR workflow.write_started_at IS NOT NULL
           OR workflow.action<>expected_action
           OR workflow.requested_at IS NULL
           OR NEW.last_error_class IS DISTINCT FROM (CASE OLD.status
               WHEN 'approved' THEN 'ozon_create_precondition_conflict'
               WHEN 'created' THEN 'ozon_products_precondition_conflict'
               WHEN 'products_added' THEN 'ozon_activate_precondition_conflict'
           END)
        THEN RAISE EXCEPTION 'Ozon pre-write conflict evidence is invalid'; END IF;
        IF OLD.status='approved' THEN
            NEW.operation_started_at := security_now;
        END IF;
        NEW.finished_at := security_now;
    ELSIF OLD.status IN ('creating','adding_products','activating')
          AND NEW.status='ambiguous' THEN
        IF NEW.last_error_class IS NULL OR workflow.action<>expected_action
           OR workflow.lease_expires_at<=security_now
        THEN RAISE EXCEPTION 'Ozon ambiguous error evidence is required'; END IF;
        NEW.finished_at := security_now;
    ELSIF OLD.status IN ('prepared','approved') AND NEW.status='expired' THEN
        NEW.finished_at := security_now;
    ELSIF OLD.status='ambiguous' AND NEW.status IN ('created','products_added','applied') THEN
        IF workflow.lease_expires_at<=security_now
           OR workflow.write_started_at IS NOT NULL
           OR workflow.last_readback_json IS NULL
           OR (
               NEW.status='created'
               AND (
                   workflow.action<>'create_campaign'
                   OR NEW.campaign_id IS NULL
                   OR workflow.create_identity_preflight_at IS NULL
                   OR workflow.create_identity_preflight_digest IS NULL
                   OR workflow.last_readback_json::jsonb->>'campaign_id'
                        IS DISTINCT FROM NEW.campaign_id::text
                   OR workflow.last_readback_json::jsonb->>'title'
                        IS DISTINCT FROM OLD.manifest_json::jsonb#>>'{create_request,title}'
                   OR (workflow.last_readback_json::jsonb->>'state' = ANY(ARRAY[
                        'CAMPAIGN_STATE_STOPPED','CAMPAIGN_STATE_INACTIVE',
                        'CAMPAIGN_STATE_PLANNED'
                   ])) IS DISTINCT FROM true
                   OR workflow.last_readback_json::jsonb->>'action' IS DISTINCT FROM 'create_campaign'
                   OR workflow.last_readback_json::jsonb->>'verified' IS DISTINCT FROM 'true'
               )
           ) OR (
               NEW.status='products_added'
               AND (
                   workflow.action<>'add_products'
                   OR NEW.campaign_id IS NULL
                   OR workflow.last_readback_json::jsonb->>'campaign_id'
                        IS DISTINCT FROM NEW.campaign_id::text
                   OR workflow.last_readback_json::jsonb->>'sku' IS DISTINCT FROM NEW.sku::text
                   OR workflow.last_readback_json::jsonb->>'title'
                        IS DISTINCT FROM OLD.manifest_json::jsonb#>>'{create_request,title}'
                   OR workflow.last_readback_json::jsonb->>'bid_microrubles'
                        IS DISTINCT FROM OLD.manifest_json::jsonb#>>'{spec,initial_cpc_bid_microrubles}'
                   OR (workflow.last_readback_json::jsonb->>'state' = ANY(ARRAY[
                        'CAMPAIGN_STATE_STOPPED','CAMPAIGN_STATE_INACTIVE',
                        'CAMPAIGN_STATE_PLANNED'
                   ])) IS DISTINCT FROM true
                   OR workflow.last_readback_json::jsonb->>'action' IS DISTINCT FROM 'add_products'
                   OR workflow.last_readback_json::jsonb->>'verified' IS DISTINCT FROM 'true'
               )
           ) OR (
               NEW.status='applied'
               AND (
                   NEW.campaign_id IS NULL OR NEW.readback_json IS NULL
                   OR NEW.readback_json IS DISTINCT FROM workflow.last_readback_json
                   OR NEW.readback_json::jsonb->>'campaign_id' IS DISTINCT FROM NEW.campaign_id::text
                   OR NEW.readback_json::jsonb->>'sku' IS DISTINCT FROM NEW.sku::text
                   OR NEW.readback_json::jsonb->>'title'
                        IS DISTINCT FROM OLD.manifest_json::jsonb#>>'{create_request,title}'
                   OR NEW.readback_json::jsonb->>'bid_microrubles'
                        IS DISTINCT FROM OLD.manifest_json::jsonb#>>'{spec,initial_cpc_bid_microrubles}'
                   OR NEW.readback_json::jsonb->>'state' IS DISTINCT FROM 'CAMPAIGN_STATE_RUNNING'
               )
           )
        THEN RAISE EXCEPTION 'Ozon reconciliation proof is required'; END IF;
        NEW.finished_at := CASE WHEN NEW.status='applied' THEN security_now ELSE NULL END;
        NEW.last_error_class := NULL;
        IF NEW.status<>'applied' THEN NEW.readback_json := NULL; END IF;
    ELSE
        RAISE EXCEPTION 'invalid Ozon plan transition % -> %',OLD.status,NEW.status;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER ozon_plans_transition_guard
BEFORE UPDATE ON control.ozon_campaign_plans
FOR EACH ROW EXECUTE FUNCTION control.enforce_ozon_plan_transition();

-- Stop leases make `stopping` a recoverable state instead of a black hole.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM control.ozon_campaign_guards
        WHERE (last_spend_minor IS NULL)<>(last_revenue_minor IS NULL)
    ) THEN
        RAISE EXCEPTION
            'Ozon guard telemetry evidence must be present or absent as a pair';
    END IF;
END
$$;

ALTER TABLE control.ozon_campaign_guards
    ADD COLUMN stop_generation bigint NOT NULL DEFAULT 0 CHECK (stop_generation>=0),
    ADD COLUMN stop_lease_owner_id varchar(128) CHECK (
        stop_lease_owner_id IS NULL OR stop_lease_owner_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    ADD COLUMN stop_lease_token varchar(64) CHECK (
        stop_lease_token IS NULL OR stop_lease_token ~ '^[0-9a-f]{64}$'
    ),
    ADD COLUMN stop_lease_claimed_at timestamptz,
    ADD COLUMN stop_lease_expires_at timestamptz,
    ADD COLUMN stop_write_started_at timestamptz,
    ADD COLUMN incident_error_class varchar(64) CHECK (
        incident_error_class IS NULL
        OR incident_error_class ~ '^[a-z0-9_]+$'
    );

-- The 024 trigger intentionally rejects same-state rewrites; disable it only
-- for this transactional evidence backfill, then install the stricter 025
-- transition contract below.
DROP TRIGGER ozon_guards_transition_guard
    ON control.ozon_campaign_guards;

UPDATE control.ozon_campaign_guards
SET stop_generation=1,
    stop_lease_owner_id='migration',
    stop_lease_token=md5(plan_id||'/stop')||md5(campaign_id::text||'/stop'),
    stop_lease_claimed_at=created_at,
    stop_lease_expires_at=created_at+interval '1 second',
    -- A legacy stopping row is ambiguous about whether deactivate reached the
    -- provider. Mark the boundary conservatively so migration recovery is
    -- readback-only and can never duplicate that mutation.
    stop_write_started_at=created_at,
    incident_error_class=CASE WHEN status='incident' THEN stop_reason END
WHERE status IN ('stopping','stopped','incident');

ALTER TABLE control.ozon_campaign_guards
    ADD CONSTRAINT ozon_guard_metric_evidence_pair CHECK (
        (last_spend_minor IS NULL)=(last_revenue_minor IS NULL)
    ),
    ADD CONSTRAINT ozon_guard_stop_lease_shape CHECK (
        (
            status='active' AND stop_generation=0
            AND stop_lease_owner_id IS NULL AND stop_lease_token IS NULL
            AND stop_lease_claimed_at IS NULL AND stop_lease_expires_at IS NULL
            AND stop_write_started_at IS NULL AND incident_error_class IS NULL
        ) OR (
            status IN ('stopping','stopped','incident') AND stop_generation>0
            AND stop_lease_owner_id IS NOT NULL AND stop_lease_token IS NOT NULL
            AND stop_lease_claimed_at IS NOT NULL AND stop_lease_expires_at IS NOT NULL
            AND stop_lease_expires_at>stop_lease_claimed_at
            AND stop_lease_expires_at<=stop_lease_claimed_at+interval '5 minutes'
            AND (
                stop_write_started_at IS NULL OR (
                    stop_write_started_at>=created_at
                    AND stop_write_started_at<stop_lease_expires_at
                )
            )
            AND ((status='incident')=(incident_error_class IS NOT NULL))
        )
    );
CREATE INDEX ozon_guard_stop_recovery_idx
    ON control.ozon_campaign_guards(stop_lease_expires_at,plan_id)
    WHERE status='stopping';

CREATE OR REPLACE FUNCTION control.enforce_ozon_guard_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    security_now timestamptz := clock_timestamp();
    stored_plan control.ozon_campaign_plans%ROWTYPE;
BEGIN
    IF (NEW.plan_id,NEW.account_id,NEW.sku,NEW.campaign_id,NEW.date_from,
        NEW.spend_cap_microrubles,NEW.target_drr_percent,NEW.created_at)
       IS DISTINCT FROM
       (OLD.plan_id,OLD.account_id,OLD.sku,OLD.campaign_id,OLD.date_from,
        OLD.spend_cap_microrubles,OLD.target_drr_percent,OLD.created_at) THEN
        RAISE EXCEPTION 'Ozon guard immutable fields cannot change';
    END IF;
    IF OLD.status='active' AND NEW.status='active' THEN
        IF NEW.stop_reason IS NOT NULL OR NEW.stopped_at IS NOT NULL
           OR NEW.stop_generation<>0 OR NEW.stop_lease_owner_id IS NOT NULL
           OR NEW.stop_lease_token IS NOT NULL OR NEW.stop_lease_claimed_at IS NOT NULL
           OR NEW.stop_lease_expires_at IS NOT NULL
           OR NEW.stop_write_started_at IS NOT NULL
           OR NEW.incident_error_class IS NOT NULL THEN
            RAISE EXCEPTION 'Ozon active guard observation is invalid';
        END IF;
    ELSIF OLD.status='active' AND NEW.status='stopping' THEN
        IF NEW.stop_reason IS NULL OR NEW.stopped_at IS NOT NULL
           OR NEW.stop_generation<>1 OR NEW.stop_lease_owner_id IS NULL
           OR NEW.stop_lease_token IS NULL OR NEW.stop_lease_claimed_at IS NULL
           OR NEW.stop_lease_claimed_at<security_now-interval '1 second'
           OR NEW.stop_lease_claimed_at>security_now+interval '1 second'
           OR NEW.stop_lease_expires_at<=security_now
           OR NEW.stop_lease_expires_at>security_now+interval '5 minutes'
           OR NEW.stop_write_started_at IS NOT NULL
           OR NEW.incident_error_class IS NOT NULL
           OR NEW.last_checked_at IS NULL
           OR NEW.last_checked_at<security_now-interval '1 second'
           OR NEW.last_checked_at>security_now+interval '1 second' THEN
            RAISE EXCEPTION 'Ozon guard stop claim is invalid';
        END IF;
    ELSIF OLD.status='stopping' AND NEW.status='stopping' THEN
        IF OLD.stop_write_started_at IS NULL
           AND NEW.stop_write_started_at IS NOT NULL
           AND NEW.stop_generation=OLD.stop_generation
           AND NEW.stop_lease_owner_id IS NOT DISTINCT FROM OLD.stop_lease_owner_id
           AND NEW.stop_lease_token IS NOT DISTINCT FROM OLD.stop_lease_token
           AND NEW.stop_lease_claimed_at IS NOT DISTINCT FROM OLD.stop_lease_claimed_at
           AND NEW.stop_lease_expires_at IS NOT DISTINCT FROM OLD.stop_lease_expires_at THEN
            SELECT * INTO STRICT stored_plan
            FROM control.ozon_campaign_plans
            WHERE plan_id=OLD.plan_id AND account_id=OLD.account_id
              AND sku=OLD.sku AND campaign_id=OLD.campaign_id
              AND status='applied';
            PERFORM pg_advisory_xact_lock(
                hashtextextended('ozon/policy-revision',0)
            );
            IF NOT EXISTS (
                SELECT 1 FROM control.ozon_policy_revisions policy
                WHERE policy.schema_version=stored_plan.schema_version
                  AND policy.policy_revision=stored_plan.policy_revision
                  AND policy.policy_digest=stored_plan.policy_digest
                  AND policy.policy_revision=(
                      SELECT max(latest.policy_revision)
                      FROM control.ozon_policy_revisions latest
                  )
            ) THEN
                RAISE EXCEPTION 'Ozon guard policy permit is stale';
            END IF;
            IF control.ozon_runtime_gates_active_locked(
                OLD.account_id,OLD.sku
            ) IS DISTINCT FROM true THEN
                RAISE EXCEPTION 'Ozon guard runtime permit is disabled';
            END IF;
            IF NEW.stop_reason IS DISTINCT FROM OLD.stop_reason
               OR NEW.incident_error_class IS NOT NULL
               OR (NEW.last_spend_minor,NEW.last_revenue_minor,NEW.last_checked_at)
                  IS DISTINCT FROM
                  (OLD.last_spend_minor,OLD.last_revenue_minor,OLD.last_checked_at)
               OR NEW.stopped_at IS NOT NULL
               OR OLD.stop_lease_expires_at<=security_now
               OR NEW.stop_write_started_at<security_now-interval '1 second'
               OR NEW.stop_write_started_at>security_now+interval '1 second' THEN
                RAISE EXCEPTION 'Ozon guard write start is invalid';
            END IF;
        ELSIF NEW.stop_reason IS DISTINCT FROM OLD.stop_reason
           OR (NEW.last_spend_minor,NEW.last_revenue_minor,NEW.last_checked_at)
              IS DISTINCT FROM
              (OLD.last_spend_minor,OLD.last_revenue_minor,OLD.last_checked_at)
           OR NEW.stop_write_started_at IS DISTINCT FROM OLD.stop_write_started_at
           OR NEW.incident_error_class IS NOT NULL
           OR NEW.stopped_at IS NOT NULL
           OR NEW.stop_generation<>OLD.stop_generation+1
           OR OLD.stop_lease_expires_at>security_now
           OR NEW.stop_lease_owner_id IS NULL OR NEW.stop_lease_token IS NULL
           OR NEW.stop_lease_claimed_at IS NULL
           OR NEW.stop_lease_claimed_at<security_now-interval '1 second'
           OR NEW.stop_lease_claimed_at>security_now+interval '1 second'
           OR NEW.stop_lease_expires_at<=security_now
           OR NEW.stop_lease_expires_at>security_now+interval '5 minutes' THEN
            RAISE EXCEPTION 'Ozon guard stop reclaim is invalid';
        END IF;
    ELSIF OLD.status='stopping' AND NEW.status='stopped' THEN
        IF NEW.stop_reason IS DISTINCT FROM OLD.stop_reason
           OR NEW.incident_error_class IS NOT NULL OR NEW.stopped_at IS NULL
           OR (NEW.last_spend_minor,NEW.last_revenue_minor,NEW.last_checked_at)
              IS DISTINCT FROM
              (OLD.last_spend_minor,OLD.last_revenue_minor,OLD.last_checked_at)
           OR NEW.stop_write_started_at IS DISTINCT FROM OLD.stop_write_started_at
           OR NEW.stop_generation<>OLD.stop_generation
           OR NEW.stop_lease_owner_id IS DISTINCT FROM OLD.stop_lease_owner_id
           OR NEW.stop_lease_token IS DISTINCT FROM OLD.stop_lease_token
           OR NEW.stop_lease_claimed_at IS DISTINCT FROM OLD.stop_lease_claimed_at
           OR NEW.stop_lease_expires_at IS DISTINCT FROM OLD.stop_lease_expires_at
           OR OLD.stop_lease_expires_at<=security_now THEN
            RAISE EXCEPTION 'Ozon guard stop proof is invalid';
        END IF;
    ELSIF OLD.status='stopping' AND NEW.status='incident' THEN
        IF NEW.stop_reason IS DISTINCT FROM OLD.stop_reason
           OR NEW.incident_error_class IS NULL OR NEW.stopped_at IS NOT NULL
           OR (NEW.last_spend_minor,NEW.last_revenue_minor,NEW.last_checked_at)
              IS DISTINCT FROM
              (OLD.last_spend_minor,OLD.last_revenue_minor,OLD.last_checked_at)
           OR NEW.stop_write_started_at IS NULL
           OR NEW.stop_write_started_at IS DISTINCT FROM OLD.stop_write_started_at
           OR NEW.stop_generation<>OLD.stop_generation
           OR NEW.stop_lease_owner_id IS DISTINCT FROM OLD.stop_lease_owner_id
           OR NEW.stop_lease_token IS DISTINCT FROM OLD.stop_lease_token
           OR NEW.stop_lease_claimed_at IS DISTINCT FROM OLD.stop_lease_claimed_at
           OR NEW.stop_lease_expires_at IS DISTINCT FROM OLD.stop_lease_expires_at
           OR OLD.stop_lease_expires_at<=security_now THEN
            RAISE EXCEPTION 'Ozon guard incident is invalid';
        END IF;
    ELSE
        RAISE EXCEPTION 'invalid Ozon guard transition % -> %',OLD.status,NEW.status;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER ozon_guards_transition_guard
BEFORE UPDATE ON control.ozon_campaign_guards
FOR EACH ROW EXECUTE FUNCTION control.enforce_ozon_guard_transition();

REVOKE ALL ON SCHEMA control
    FROM PUBLIC,ozon_control_planner,ozon_control_executor;
GRANT USAGE ON SCHEMA control
    TO ozon_control_planner,ozon_control_executor;

REVOKE ALL ON TABLE control.ozon_policy_revisions,
    control.ozon_campaign_plans,
    control.ozon_campaign_plan_approvals,
    control.ozon_runtime_gates,
    control.ozon_campaign_action_reservations,
    control.ozon_campaign_audit_events,
    control.ozon_campaign_guards,
    control.ozon_campaign_launch_workflows,
    control.ozon_static_guard_audit_events
    FROM PUBLIC,control_writer,ozon_control_planner,ozon_control_executor;
REVOKE ALL ON SEQUENCE control.ozon_campaign_audit_events_event_id_seq,
    control.ozon_static_guard_audit_events_event_id_seq
    FROM PUBLIC,control_writer,ozon_control_planner,ozon_control_executor;
REVOKE ALL ON FUNCTION control.initialize_ozon_launch_workflow()
    FROM PUBLIC,control_writer,ozon_control_planner,ozon_control_executor;
REVOKE ALL ON FUNCTION control.enforce_ozon_launch_workflow_update()
    FROM PUBLIC,control_writer,ozon_control_planner,ozon_control_executor;
REVOKE ALL ON FUNCTION control.ozon_runtime_gates_active_locked(text,bigint)
    FROM PUBLIC,control_writer,ozon_control_planner,ozon_control_executor;

-- MCP ingress can prepare, approve and explicitly enqueue, but it has no
-- mutation lease/evidence privileges.  Approval therefore cannot turn into a
-- provider write without the independent executor identity.
GRANT SELECT,INSERT ON control.ozon_policy_revisions TO ozon_control_planner;
GRANT SELECT,INSERT ON control.ozon_campaign_plans TO ozon_control_planner;
GRANT UPDATE(status,finished_at) ON control.ozon_campaign_plans TO ozon_control_planner;
GRANT SELECT,INSERT ON control.ozon_campaign_plan_approvals TO ozon_control_planner;
GRANT SELECT ON control.ozon_runtime_gates TO ozon_control_planner;
GRANT SELECT,INSERT ON control.ozon_campaign_audit_events TO ozon_control_planner;
GRANT SELECT ON control.ozon_campaign_launch_workflows TO ozon_control_planner;
GRANT UPDATE(requested_at,requested_by_actor_id,available_at)
    ON control.ozon_campaign_launch_workflows TO ozon_control_planner;
GRANT USAGE,SELECT ON SEQUENCE control.ozon_campaign_audit_events_event_id_seq
    TO ozon_control_planner;
GRANT EXECUTE ON FUNCTION control.ozon_runtime_gates_active_locked(text,bigint)
    TO ozon_control_planner;

-- The durable worker can claim/fence/complete only already-enqueued work.  It
-- cannot create or approve a plan and cannot forge the operator request
-- identity columns.
GRANT SELECT ON control.ozon_policy_revisions,
    control.ozon_campaign_plans,
    control.ozon_campaign_plan_approvals,
    control.ozon_runtime_gates,
    control.ozon_campaign_action_reservations,
    control.ozon_campaign_audit_events,
    control.ozon_campaign_guards,
    control.ozon_campaign_launch_workflows,
    control.ozon_static_guard_audit_events
    TO ozon_control_executor;
GRANT INSERT ON control.ozon_campaign_action_reservations,
    control.ozon_campaign_audit_events,
    control.ozon_campaign_guards,
    control.ozon_static_guard_audit_events
    TO ozon_control_executor;
GRANT UPDATE(status,campaign_id,operation_started_at,finished_at,last_error_class,readback_json)
    ON control.ozon_campaign_plans TO ozon_control_executor;
GRANT UPDATE(
    action,generation,lease_owner_id,lease_token,lease_claimed_at,
    lease_expires_at,write_started_at,
    create_identity_preflight_at,create_identity_preflight_digest,
    available_at,last_completed_at,
    last_error_class,last_readback_json
) ON control.ozon_campaign_launch_workflows TO ozon_control_executor;
GRANT UPDATE(
    status,stop_reason,last_spend_minor,last_revenue_minor,last_checked_at,stopped_at,
    stop_generation,stop_lease_owner_id,stop_lease_token,
    stop_lease_claimed_at,stop_lease_expires_at,stop_write_started_at,
    incident_error_class
) ON control.ozon_campaign_guards TO ozon_control_executor;
GRANT USAGE,SELECT ON SEQUENCE control.ozon_campaign_audit_events_event_id_seq,
    control.ozon_static_guard_audit_events_event_id_seq
    TO ozon_control_executor;
GRANT EXECUTE ON FUNCTION control.ozon_runtime_gates_active_locked(text,bigint)
    TO ozon_control_executor;

COMMIT;
