-- A readback-only recovery can prove the exact campaign is already running
-- even when the uncertain operation was create. The guarded plan transition
-- validates that evidence and moves to applied before the workflow is closed.
-- Allow that terminal action jump while preserving every lease, evidence,
-- completion-time and generation check below. No new write is authorized.
BEGIN;

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
           OR (NEW.action NOT IN (OLD.action, CASE OLD.action
               WHEN 'create_campaign' THEN 'add_products'
               WHEN 'add_products' THEN 'activate_campaign'
               ELSE 'activate_campaign'
           END) AND NOT (
               plan_status='applied' AND NEW.action='activate_campaign'
           ))
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

COMMIT;
