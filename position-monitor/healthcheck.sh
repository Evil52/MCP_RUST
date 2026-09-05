#!/bin/sh
set -eu

: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

require_migration_ledger="${POSITION_DB_REQUIRE_MIGRATION_LEDGER:-true}"
case "$require_migration_ledger" in
  true | false) ;;
  *) exit 1 ;;
esac

healthy="$({
  PGPASSWORD="$POSTGRES_PASSWORD" psql \
    --host=127.0.0.1 \
    --username="$POSTGRES_USER" \
    --dbname="$POSTGRES_DB" \
    --no-psqlrc \
    --no-align \
    --tuples-only \
    --set=ON_ERROR_STOP=1 \
    --set=require_migration_ledger="$require_migration_ledger" <<'SQL'
SELECT
    to_regclass('search_position.monitors') IS NOT NULL
    AND to_regclass('search_position.collection_runs') IS NOT NULL
    AND to_regclass('search_position.measurements') IS NOT NULL
    AND to_regclass('search_position.measurements_monitor_slot') IS NOT NULL
    AND to_regclass('search_position.latest_measurements') IS NOT NULL
    AND to_regclass('search_position.published_measurements') IS NOT NULL
    AND to_regclass('search_position.published_alerts') IS NOT NULL
    AND to_regclass('search_position.ozon_collector_circuit') IS NOT NULL
    AND to_regclass('search_position.ozon_region_request_budgets') IS NOT NULL
    AND to_regclass('search_position.ozon_request_budget_usage') IS NOT NULL
    AND to_regclass('search_position.wb_search_targets') IS NOT NULL
    AND to_regclass('search_position.wb_bid_targets') IS NOT NULL
    AND to_regclass('search_position.wb_collection_runs') IS NOT NULL
    AND to_regclass('search_position.wb_search_snapshots') IS NOT NULL
    AND to_regclass('search_position.wb_bid_snapshots') IS NOT NULL
    AND to_regclass('search_position.latest_wb_search_snapshots') IS NOT NULL
    AND to_regclass('search_position.latest_wb_bid_snapshots') IS NOT NULL
    AND to_regclass('daily_reporting.delivery_batches') IS NOT NULL
    AND to_regclass('daily_reporting.delivery_coverage') IS NOT NULL
    AND to_regclass('daily_reporting.delivery_attempts') IS NOT NULL
    AND to_regclass('daily_reporting.delivery_reconciliations') IS NOT NULL
    AND to_regclass('daily_reporting.claimable_deliveries') IS NOT NULL
    AND to_regclass('daily_reporting.generation_attempts') IS NOT NULL
    AND to_regclass('daily_reporting.generatable_batches') IS NOT NULL
    AND to_regclass('daily_reporting.delivery_batches_generatable_schedule_idx') IS NOT NULL
    AND to_regclass('daily_reporting.stalled_report_work') IS NOT NULL
    AND to_regclass('daily_reporting.source_snapshots') IS NOT NULL
    AND to_regclass('daily_reporting.sales_facts') IS NOT NULL
    AND to_regclass('daily_reporting.advertising_facts') IS NOT NULL
    AND to_regclass('daily_reporting.advertising_expense_facts') IS NOT NULL
    AND to_regclass('daily_reporting.finance_facts') IS NOT NULL
    AND to_regclass('daily_reporting.stock_facts') IS NOT NULL
    AND to_regclass('daily_reporting.price_facts') IS NOT NULL
    AND to_regclass('daily_reporting.unit_economics_inputs') IS NOT NULL
    AND to_regclass('daily_reporting.collection_claims') IS NOT NULL
    AND to_regclass('daily_reporting.ozon_sales_refresh_requests') IS NOT NULL
    AND to_regclass('daily_reporting.marketplace_sales_refresh_one_active_account_idx') IS NOT NULL
    AND to_regclass('daily_reporting.marketplace_sales_refresh_history_idx') IS NOT NULL
    AND to_regclass('daily_reporting.mcp_tool_calls') IS NOT NULL
    AND to_regclass('daily_reporting.mcp_tool_calls_admin_log_idx') IS NOT NULL
    AND to_regclass('daily_reporting.mcp_tool_calls_running_idx') IS NOT NULL
    AND to_regclass('daily_reporting.collection_staging_snapshots') IS NOT NULL
    AND to_regclass('daily_reporting.collection_staging_snapshots_age_idx') IS NOT NULL
    AND to_regclass('daily_reporting.ozon_sales_refresh_one_running_global_idx') IS NOT NULL
    AND to_regclass('daily_reporting.published_source_snapshots') IS NOT NULL
    AND to_regclass('daily_reporting.published_sales_facts') IS NOT NULL
    AND to_regclass('daily_reporting.published_advertising_facts') IS NOT NULL
    AND to_regclass('daily_reporting.published_advertising_expense_facts') IS NOT NULL
    AND to_regclass('daily_reporting.published_finance_facts') IS NOT NULL
    AND to_regclass('daily_reporting.published_stock_facts') IS NOT NULL
    AND to_regclass('daily_reporting.published_price_facts') IS NOT NULL
    AND to_regclass('daily_reporting.mcp_collection_status') IS NOT NULL
    AND to_regclass('daily_reporting.mcp_published_source_snapshots') IS NOT NULL
    AND to_regclass('daily_reporting.mcp_sales_facts') IS NOT NULL
    AND to_regclass('daily_reporting.mcp_advertising_facts') IS NOT NULL
    AND to_regclass('daily_reporting.mcp_advertising_expense_facts') IS NOT NULL
    AND to_regclass('daily_reporting.mcp_finance_facts') IS NOT NULL
    AND to_regclass('daily_reporting.mcp_stock_facts') IS NOT NULL
    AND to_regclass('daily_reporting.mcp_price_facts') IS NOT NULL
    AND to_regclass('daily_reporting.mcp_ready_reports') IS NOT NULL
    AND to_regclass('control.wb_policy_revisions') IS NOT NULL
    AND to_regclass('control.wb_prepare_reservations') IS NOT NULL
    AND to_regclass('control.wb_plans') IS NOT NULL
    AND to_regclass('control.wb_plan_approvals') IS NOT NULL
    AND to_regclass('control.wb_runtime_gates') IS NOT NULL
    AND to_regclass('control.wb_action_reservations') IS NOT NULL
    AND to_regclass('control.wb_audit_events') IS NOT NULL
    AND to_regclass('control.ozon_policy_revisions') IS NOT NULL
    AND to_regclass('control.ozon_campaign_plans') IS NOT NULL
    AND to_regclass('control.ozon_campaign_plan_approvals') IS NOT NULL
    AND to_regclass('control.ozon_runtime_gates') IS NOT NULL
    AND to_regclass('control.ozon_campaign_action_reservations') IS NOT NULL
    AND to_regclass('control.ozon_campaign_audit_events') IS NOT NULL
    AND to_regclass('control.ozon_campaign_guards') IS NOT NULL
    AND to_regclass('control.ozon_campaign_launch_workflows') IS NOT NULL
    AND to_regclass('wb_automation.cycles') IS NOT NULL
    AND to_regclass('wb_automation.action_attempts') IS NOT NULL
    AND to_regclass('wb_automation.execution_state') IS NOT NULL
    AND to_regclass('wb_automation.audit_events') IS NOT NULL
    AND to_regprocedure(
        'wb_automation.reject_append_only_mutation()'
    ) IS NOT NULL
    AND to_regprocedure('wb_automation.stamp_cycle_insert()') IS NOT NULL
    AND to_regprocedure(
        'wb_automation.enforce_action_transition()'
    ) IS NOT NULL
    AND to_regprocedure(
        'wb_automation.enforce_state_transition()'
    ) IS NOT NULL
    AND to_regprocedure('wb_automation.stamp_audit_insert()') IS NOT NULL
    AND EXISTS (
        SELECT 1 FROM pg_roles
        WHERE rolname = 'wb_automation_writer'
          AND rolcanlogin
          AND NOT rolsuper
          AND NOT rolcreatedb
          AND NOT rolcreaterole
          AND NOT rolinherit
          AND NOT rolreplication
          AND NOT rolbypassrls
          AND rolconnlimit = 2
    )
    AND to_regprocedure(
        'control.validate_wb_policy_revision_insert()'
    ) IS NOT NULL
    AND to_regprocedure(
        'control.validate_wb_prepare_reservation_insert()'
    ) IS NOT NULL
    AND to_regprocedure('control.validate_wb_plan_insert()') IS NOT NULL
    AND to_regprocedure(
        'control.validate_wb_runtime_gate_write()'
    ) IS NOT NULL
    AND to_regprocedure(
        'control.reject_wb_append_only_mutation()'
    ) IS NOT NULL
    AND to_regprocedure('control.validate_wb_approval_insert()') IS NOT NULL
    AND to_regprocedure(
        'control.validate_wb_reservation_insert()'
    ) IS NOT NULL
    AND to_regprocedure('control.enforce_wb_plan_transition()') IS NOT NULL
    AND to_regprocedure('control.enforce_ozon_plan_transition()') IS NOT NULL
    AND to_regprocedure('control.enforce_ozon_guard_transition()') IS NOT NULL
    AND to_regprocedure(
        'control.enforce_ozon_launch_workflow_update()'
    ) IS NOT NULL
    AND to_regprocedure('control.initialize_ozon_launch_workflow()') IS NOT NULL
    AND to_regprocedure('control.reject_ozon_append_only_mutation()') IS NOT NULL
    AND to_regprocedure('control.validate_ozon_policy_revision_insert()') IS NOT NULL
    AND to_regprocedure('control.validate_ozon_plan_insert()') IS NOT NULL
    AND to_regprocedure('control.validate_ozon_approval_insert()') IS NOT NULL
    AND to_regprocedure('control.validate_ozon_reservation_insert()') IS NOT NULL
    AND to_regprocedure('control.validate_ozon_guard_insert()') IS NOT NULL
    AND (
        SELECT string_agg(relation.relname::text, ',' ORDER BY relation.relname)
               = 'ozon_campaign_action_reservations,ozon_campaign_audit_events,' ||
                 'ozon_campaign_guards,ozon_campaign_launch_workflows,' ||
                 'ozon_campaign_plan_approvals,' ||
                 'ozon_campaign_plans,ozon_policy_revisions,ozon_runtime_gates,' ||
                 'ozon_static_guard_audit_events,' ||
                 'wb_action_reservations,wb_audit_events,wb_plan_approvals,' ||
                 'wb_plans,wb_policy_revisions,wb_prepare_reservations,' ||
                 'wb_runtime_gates'
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'control'
          AND relation.relkind IN ('r', 'p')
    )
    AND (
        SELECT string_agg(routine.proname::text, ',' ORDER BY routine.proname)
               = 'enforce_ozon_guard_transition,' ||
                 'enforce_ozon_launch_workflow_update,enforce_ozon_plan_transition,' ||
                 'enforce_wb_plan_transition,initialize_ozon_launch_workflow,' ||
                 'reject_ozon_append_only_mutation,' ||
                 'reject_wb_append_only_mutation,validate_ozon_approval_insert,' ||
                 'validate_ozon_guard_insert,validate_ozon_plan_insert,' ||
                 'validate_ozon_policy_revision_insert,' ||
                 'validate_ozon_reservation_insert,' ||
                 'validate_wb_approval_insert,validate_wb_plan_insert,' ||
                 'validate_wb_policy_revision_insert,' ||
                 'validate_wb_prepare_reservation_insert,' ||
                 'validate_wb_reservation_insert,validate_wb_runtime_gate_write'
        FROM pg_proc AS routine
        JOIN pg_namespace AS namespace
          ON namespace.oid = routine.pronamespace
        WHERE namespace.nspname = 'control'
          AND routine.prorettype = 'trigger'::regtype
    )
    AND (
        SELECT count(*) = 27
        FROM pg_trigger AS trigger
        JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
        JOIN pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'control'
          AND NOT trigger.tgisinternal
    )
    AND (
        SELECT count(*) = 27
        FROM pg_trigger AS trigger
        JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
        JOIN pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'control'
          AND NOT trigger.tgisinternal
          AND trigger.tgenabled = 'O'
          AND (
              (
                  relation.relname = 'wb_plans'
                  AND trigger.tgname = 'wb_plans_transition_guard'
                  AND trigger.tgtype = 19
                  AND trigger.tgfoid =
                      'control.enforce_wb_plan_transition()'::regprocedure
              )
              OR (
                  relation.relname = 'wb_policy_revisions'
                  AND trigger.tgname = 'wb_policy_revisions_validate'
                  AND trigger.tgtype = 7
                  AND trigger.tgfoid =
                      'control.validate_wb_policy_revision_insert()'::regprocedure
              )
              OR (
                  relation.relname = 'wb_policy_revisions'
                  AND trigger.tgname = 'wb_policy_revisions_append_only'
                  AND trigger.tgtype = 27
                  AND trigger.tgfoid =
                      'control.reject_wb_append_only_mutation()'::regprocedure
              )
              OR (
                  relation.relname = 'wb_prepare_reservations'
                  AND trigger.tgname = 'wb_prepare_reservations_validate'
                  AND trigger.tgtype = 7
                  AND trigger.tgfoid =
                      'control.validate_wb_prepare_reservation_insert()'::regprocedure
              )
              OR (
                  relation.relname = 'wb_prepare_reservations'
                  AND trigger.tgname = 'wb_prepare_reservations_append_only'
                  AND trigger.tgtype = 27
                  AND trigger.tgfoid =
                      'control.reject_wb_append_only_mutation()'::regprocedure
              )
              OR (
                  relation.relname = 'wb_plans'
                  AND trigger.tgname = 'wb_plans_validate_insert'
                  AND trigger.tgtype = 7
                  AND trigger.tgfoid =
                      'control.validate_wb_plan_insert()'::regprocedure
              )
              OR (
                  relation.relname = 'wb_runtime_gates'
                  AND trigger.tgname = 'wb_runtime_gates_validate_write'
                  AND trigger.tgtype = 23
                  AND trigger.tgfoid =
                      'control.validate_wb_runtime_gate_write()'::regprocedure
              )
              OR (
                  relation.relname = 'wb_plan_approvals'
                  AND trigger.tgname = 'wb_plan_approvals_validate'
                  AND trigger.tgtype = 7
                  AND trigger.tgfoid =
                      'control.validate_wb_approval_insert()'::regprocedure
              )
              OR (
                  relation.relname = 'wb_plan_approvals'
                  AND trigger.tgname = 'wb_plan_approvals_append_only'
                  AND trigger.tgtype = 27
                  AND trigger.tgfoid =
                      'control.reject_wb_append_only_mutation()'::regprocedure
              )
              OR (
                  relation.relname = 'wb_action_reservations'
                  AND trigger.tgname = 'wb_action_reservations_validate'
                  AND trigger.tgtype = 7
                  AND trigger.tgfoid =
                      'control.validate_wb_reservation_insert()'::regprocedure
              )
              OR (
                  relation.relname = 'wb_action_reservations'
                  AND trigger.tgname = 'wb_action_reservations_append_only'
                  AND trigger.tgtype = 27
                  AND trigger.tgfoid =
                      'control.reject_wb_append_only_mutation()'::regprocedure
              )
              OR (
                  relation.relname = 'wb_audit_events'
                  AND trigger.tgname = 'wb_audit_events_append_only'
                  AND trigger.tgtype = 27
                  AND trigger.tgfoid =
                      'control.reject_wb_append_only_mutation()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_campaign_plans'
                  AND trigger.tgname = 'ozon_plans_transition_guard'
                  AND trigger.tgtype = 19
                  AND trigger.tgfoid =
                      'control.enforce_ozon_plan_transition()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_campaign_plans'
                  AND trigger.tgname = 'ozon_plans_validate_insert'
                  AND trigger.tgtype = 7
                  AND trigger.tgfoid =
                      'control.validate_ozon_plan_insert()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_policy_revisions'
                  AND trigger.tgname = 'ozon_policy_revisions_validate'
                  AND trigger.tgtype = 7
                  AND trigger.tgfoid =
                      'control.validate_ozon_policy_revision_insert()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_policy_revisions'
                  AND trigger.tgname = 'ozon_policy_revisions_append_only'
                  AND trigger.tgtype = 27
                  AND trigger.tgfoid =
                      'control.reject_ozon_append_only_mutation()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_campaign_plan_approvals'
                  AND trigger.tgname = 'ozon_approvals_validate'
                  AND trigger.tgtype = 7
                  AND trigger.tgfoid =
                      'control.validate_ozon_approval_insert()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_campaign_plan_approvals'
                  AND trigger.tgname = 'ozon_approvals_append_only'
                  AND trigger.tgtype = 27
                  AND trigger.tgfoid =
                      'control.reject_ozon_append_only_mutation()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_campaign_action_reservations'
                  AND trigger.tgname = 'ozon_reservations_validate'
                  AND trigger.tgtype = 7
                  AND trigger.tgfoid =
                      'control.validate_ozon_reservation_insert()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_campaign_action_reservations'
                  AND trigger.tgname = 'ozon_reservations_append_only'
                  AND trigger.tgtype = 27
                  AND trigger.tgfoid =
                      'control.reject_ozon_append_only_mutation()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_campaign_audit_events'
                  AND trigger.tgname = 'ozon_audit_append_only'
                  AND trigger.tgtype = 27
                  AND trigger.tgfoid =
                      'control.reject_ozon_append_only_mutation()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_campaign_guards'
                  AND trigger.tgname = 'ozon_guards_validate_insert'
                  AND trigger.tgtype = 7
                  AND trigger.tgfoid =
                      'control.validate_ozon_guard_insert()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_campaign_guards'
                  AND trigger.tgname = 'ozon_guards_transition_guard'
                  AND trigger.tgtype = 19
                  AND trigger.tgfoid =
                      'control.enforce_ozon_guard_transition()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_campaign_launch_workflows'
                  AND trigger.tgname = 'ozon_launch_workflow_update_guard'
                  AND trigger.tgtype = 19
                  AND trigger.tgfoid =
                      'control.enforce_ozon_launch_workflow_update()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_campaign_launch_workflows'
                  AND trigger.tgname = 'ozon_launch_workflow_no_delete'
                  AND trigger.tgtype = 11
                  AND trigger.tgfoid =
                      'control.reject_ozon_append_only_mutation()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_campaign_plans'
                  AND trigger.tgname = 'ozon_launch_workflow_initialize'
                  AND trigger.tgtype = 5
                  AND trigger.tgfoid =
                      'control.initialize_ozon_launch_workflow()'::regprocedure
              )
              OR (
                  relation.relname = 'ozon_static_guard_audit_events'
                  AND trigger.tgname = 'ozon_static_guard_audit_append_only'
                  AND trigger.tgtype = 27
                  AND trigger.tgfoid =
                      'control.reject_ozon_append_only_mutation()'::regprocedure
              )
          )
    )
    AND (
        SELECT string_agg(
                   relation.relname::text || ':' || constraint_row.conname ||
                   ':' || constraint_row.contype::text,
                   ',' ORDER BY relation.relname, constraint_row.conname
               ) =
               'ozon_campaign_guards:ozon_guard_metric_evidence_pair:c,' ||
               'ozon_campaign_guards:ozon_guard_stop_lease_shape:c,' ||
               'ozon_campaign_launch_workflows:ozon_campaign_launch_workflows_pkey:p,' ||
               'ozon_campaign_launch_workflows:ozon_campaign_launch_workflows_plan_id_fkey:f,' ||
               'ozon_campaign_launch_workflows:ozon_launch_workflow_identity_preflight_shape:c,' ||
               'ozon_campaign_launch_workflows:ozon_launch_workflow_lease_shape:c,' ||
               'ozon_campaign_launch_workflows:ozon_launch_workflow_request_shape:c,' ||
               'wb_plan_approvals:wb_approval_ttl:c,' ||
               'wb_plans:wb_plan_state_shape:c,' ||
               'wb_plans:wb_plan_ttl:c,' ||
               'wb_plans:wb_plans_prepare_reservation_id_fkey:f,' ||
               'wb_plans:wb_plans_prepare_reservation_id_key:u,' ||
               'wb_prepare_reservations:wb_prepare_reservation_ttl:c,' ||
               'wb_prepare_reservations:wb_prepare_reservations_pkey:p,' ||
               'wb_prepare_reservations:' ||
               'wb_prepare_reservations_policy_revision_fkey:f,' ||
               'wb_runtime_gates:wb_runtime_gate_lease_bound:c,' ||
               'wb_runtime_gates:wb_runtime_gate_scope:c'
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS relation ON relation.oid = constraint_row.conrelid
        JOIN pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
          WHERE namespace.nspname = 'control'
          AND constraint_row.conname IN (
              'ozon_guard_metric_evidence_pair',
              'ozon_guard_stop_lease_shape',
              'ozon_campaign_launch_workflows_pkey',
              'ozon_campaign_launch_workflows_plan_id_fkey',
              'ozon_launch_workflow_identity_preflight_shape',
              'ozon_launch_workflow_lease_shape',
              'ozon_launch_workflow_request_shape',
              'wb_approval_ttl',
              'wb_plan_state_shape',
              'wb_plan_ttl',
              'wb_plans_prepare_reservation_id_fkey',
              'wb_plans_prepare_reservation_id_key',
              'wb_prepare_reservation_ttl',
              'wb_prepare_reservations_pkey',
              'wb_prepare_reservations_policy_revision_fkey',
              'wb_runtime_gate_lease_bound',
              'wb_runtime_gate_scope'
          )
    )
    AND (
        SELECT count(*) = 9
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'daily_reporting'
          AND relation.relkind = 'v'
          AND relation.relname IN (
              'mcp_collection_status',
              'mcp_published_source_snapshots',
              'mcp_sales_facts',
              'mcp_advertising_facts',
              'mcp_advertising_expense_facts',
              'mcp_finance_facts',
              'mcp_stock_facts',
              'mcp_price_facts',
              'mcp_ready_reports'
          )
          AND coalesce(relation.reloptions, ARRAY[]::text[])
              @> ARRAY['security_barrier=true']
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'daily_reporting.source_snapshots'::regclass
          AND conname = 'source_snapshots_observation_window_check'
          AND contype = 'c'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'daily_reporting.delivery_batches'::regclass
          AND conname = 'delivery_batches_error_class_check'
          AND contype = 'c'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'daily_reporting.delivery_attempts'::regclass
          AND conname = 'delivery_attempts_shape_check'
          AND contype = 'c'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'daily_reporting.delivery_attempts'::regclass
          AND conname = 'delivery_attempts_transient_error_class_check'
          AND contype = 'c'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'daily_reporting.delivery_coverage'::regclass
          AND conname = 'delivery_coverage_schedule_check'
          AND contype = 'c'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'daily_reporting.source_snapshots'::regclass
          AND conname = 'source_snapshots_period_window_check'
          AND contype = 'c'
    )
    AND to_regprocedure(
        'daily_reporting.enforce_delivery_batch_state()'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.require_planned_delivery_coverage()'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.require_active_delivery_attempt()'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.require_ambiguous_delivery_reconciliation()'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.reject_reconciliation_mutation()'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.enforce_delivery_artifact_identity()'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.enforce_source_snapshot_state()'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.require_running_fact_snapshot()'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.require_generatable_batch()'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.reject_generation_attempt_mutation()'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.claim_report_collection(text,text,timestamp with time zone,text)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.release_report_collection_claim(bigint,bigint,text)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.complete_report_collection_claim(bigint,bigint,text)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.require_active_collection_claim()'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.request_ozon_sales_refresh(text,text,date)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.ozon_sales_refresh_status(text)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.claim_ozon_sales_refresh(text)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.complete_ozon_sales_refresh(bigint,integer,text,timestamp with time zone)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.fail_ozon_sales_refresh(bigint,integer,text,text)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.request_marketplace_sales_refresh(text,text,text,date)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.marketplace_sales_refresh_status(text,text)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.claim_marketplace_sales_refresh(text)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.complete_marketplace_sales_refresh(bigint,integer,text,timestamp with time zone)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.fail_marketplace_sales_refresh(bigint,integer,text,text)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.begin_mcp_tool_call(text,text,text,text)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.finish_mcp_tool_call(bigint,text,integer,text)'
    ) IS NOT NULL
    AND to_regprocedure(
        'daily_reporting.list_mcp_tool_calls(integer)'
    ) IS NOT NULL
    AND EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'source_snapshots_require_active_collection_claim'
          AND NOT tgisinternal
    )
    AND EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'collection_staging_snapshots_require_active_claim'
          AND NOT tgisinternal
    )
    AND (
        SELECT count(*) = 6
        FROM pg_trigger
        WHERE tgname IN (
            'delivery_batches_enforce_state',
            'delivery_batches_enforce_artifact_identity',
            'delivery_coverage_requires_planned_batch',
            'delivery_coverage_is_append_only',
            'delivery_attempts_require_active_send',
            'delivery_attempts_are_append_only'
        )
          AND NOT tgisinternal
    )
    AND (
        SELECT count(*) = 2
        FROM pg_trigger
        WHERE tgname IN (
            'delivery_reconciliations_require_active_send',
            'delivery_reconciliations_are_append_only'
        )
          AND NOT tgisinternal
    )
    AND (
        SELECT count(*) = 2
        FROM pg_trigger
        WHERE tgname IN (
            'generation_attempts_require_generatable_batch',
            'generation_attempts_are_append_only'
        )
          AND NOT tgisinternal
    )
    AND to_regprocedure(
        'search_position.enforce_wb_search_target_update()'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.enforce_ozon_collection_run_state()'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.require_running_ozon_measurement_run()'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.require_running_ozon_alert_run()'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.open_ozon_collector_circuit(bigint,text)'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.claim_ozon_request_budget(text,timestamp with time zone)'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.enforce_ozon_payload_digest_immutable()'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.enforce_wb_bid_target_update()'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.enforce_wb_collection_run_state()'
    ) IS NOT NULL
    AND to_regprocedure(
        'search_position.require_running_wb_snapshot_run()'
    ) IS NOT NULL
    AND (
        SELECT count(*) = 5
        FROM pg_trigger
        WHERE tgname IN (
            'wb_search_targets_enforce_update',
            'wb_bid_targets_enforce_update',
            'wb_collection_runs_enforce_state',
            'wb_search_snapshots_require_running_run',
            'wb_bid_snapshots_require_running_run'
        )
          AND NOT tgisinternal
    )
    AND (
        SELECT count(*) = 4
        FROM pg_trigger
        WHERE tgname IN (
            'collection_runs_enforce_state',
            'collection_runs_payload_digest_immutable',
            'measurements_require_running_run',
            'alerts_require_running_run'
        )
          AND NOT tgisinternal
    )
    AND (
        SELECT count(*) = 2
        FROM information_schema.columns
        WHERE table_schema = 'search_position'
          AND table_name = 'measurements'
          AND column_name IN ('overall_position', 'placement')
    )
    AND (
        SELECT count(*) = 6
        FROM information_schema.columns
        WHERE table_schema = 'search_position'
          AND table_name = 'collection_runs'
          AND column_name IN (
              'scheduled_for',
              'queries_planned',
              'queries_attempted',
              'queries_succeeded',
              'monitors_planned',
              'payload_digest'
          )
    )
    AND (
        SELECT count(*) = 0
        FROM information_schema.columns
        WHERE table_schema = 'search_position'
          AND table_name = 'wb_search_snapshots'
          AND column_name IN (
              'region',
              'region_code',
              'organic_position',
              'sponsored_position',
              'live_position'
          )
    )
    AND (
        SELECT count(*) = 3
        FROM information_schema.columns
        WHERE table_schema = 'search_position'
          AND table_name = 'latest_wb_search_snapshots'
          AND column_name IN (
              'is_live_position',
              'region',
              'placement_split_available'
          )
    )
    AND (
        SELECT count(*) = 2
        FROM information_schema.columns
        WHERE table_schema = 'search_position'
          AND table_name IN (
              'latest_wb_search_snapshots',
              'latest_wb_bid_snapshots'
          )
          AND column_name = 'run_status'
    )
    AND (
        SELECT string_agg(column_name, ',' ORDER BY ordinal_position) =
               'batch_id,recipient_id,report_version,local_date,report_kind,' ||
               'scheduled_for,deadline_at,status,delayed,created_at,updated_at,sent_at'
        FROM information_schema.columns
        WHERE table_schema = 'daily_reporting'
          AND table_name = 'mcp_ready_reports'
    )
    AND (
        SELECT count(*) = 0
        FROM information_schema.columns
        WHERE table_schema = 'daily_reporting'
          AND table_name = 'mcp_ready_reports'
          AND column_name IN (
              'recipient_email',
              'provider_message_id',
              'artifact_object_key',
              'artifact_sha256',
              'artifact_html_sha256',
              'last_error_class',
              'attempts',
              'next_attempt_at'
          )
    )
    AND has_database_privilege('position_collector', current_database(), 'CONNECT')
    AND has_database_privilege('position_reader', current_database(), 'CONNECT')
    AND has_database_privilege('report_refresh_requester', current_database(), 'CONNECT')
    AND NOT has_database_privilege('position_collector', current_database(), 'TEMP')
    AND NOT has_database_privilege('position_reader', current_database(), 'TEMP')
    AND NOT has_database_privilege('report_refresh_requester', current_database(), 'TEMP')
    AND EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = 'report_refresh_requester'
          AND rolcanlogin
          AND NOT rolsuper
          AND NOT rolcreatedb
          AND NOT rolcreaterole
          AND NOT rolinherit
          AND NOT rolreplication
          AND NOT rolbypassrls
          AND rolconnlimit = 4
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_auth_members AS membership
        WHERE membership.roleid = 'report_refresh_requester'::regrole
           OR membership.member = 'report_refresh_requester'::regrole
    )
    AND has_function_privilege(
        'report_refresh_requester',
        'daily_reporting.request_ozon_sales_refresh(text,text,date)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_refresh_requester',
        'daily_reporting.ozon_sales_refresh_status(text)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_refresh_requester',
        'daily_reporting.request_marketplace_sales_refresh(text,text,text,date)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_refresh_requester',
        'daily_reporting.marketplace_sales_refresh_status(text,text)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_refresh_requester',
        'daily_reporting.begin_mcp_tool_call(text,text,text,text)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_refresh_requester',
        'daily_reporting.finish_mcp_tool_call(bigint,text,integer,text)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_refresh_requester',
        'daily_reporting.list_mcp_tool_calls(integer)',
        'EXECUTE'
    )
    AND NOT has_table_privilege(
        'report_refresh_requester',
        'daily_reporting.ozon_sales_refresh_requests',
        'SELECT,INSERT,UPDATE,DELETE'
    )
    AND NOT has_table_privilege(
        'report_refresh_requester',
        'daily_reporting.mcp_tool_calls',
        'SELECT,INSERT,UPDATE,DELETE'
    )
    AND NOT has_table_privilege(
        'report_refresh_requester',
        'daily_reporting.collection_staging_snapshots',
        'SELECT,INSERT,UPDATE,DELETE'
    )
    AND NOT has_table_privilege(
        'report_refresh_requester', 'daily_reporting.source_snapshots',
        'SELECT,INSERT,UPDATE,DELETE'
    )
    AND NOT has_function_privilege(
        'report_refresh_requester',
        'daily_reporting.claim_ozon_sales_refresh(text)',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'report_refresh_requester',
        'daily_reporting.claim_marketplace_sales_refresh(text)',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'report_refresh_requester',
        'daily_reporting.claim_marketplace_sales_refresh_for(text,text)',
        'EXECUTE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.monitors', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.monitors', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.monitors', 'DELETE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.collection_runs', 'INSERT'
    )
    AND has_column_privilege(
        'position_collector', 'search_position.collection_runs', 'status', 'UPDATE'
    )
    AND NOT has_column_privilege(
        'position_collector', 'search_position.collection_runs', 'scheduled_for', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.collection_runs', 'UPDATE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.measurements', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.measurements', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.measurements', 'DELETE'
    )
    AND has_function_privilege(
        'position_collector',
        'search_position.open_ozon_collector_circuit(bigint,text)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'position_collector',
        'search_position.claim_ozon_request_budget(text,timestamp with time zone)',
        'EXECUTE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.ozon_collector_circuit', 'UPDATE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.wb_search_targets', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_search_targets', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_search_targets', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_search_targets', 'DELETE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.wb_bid_targets', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_bid_targets', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_bid_targets', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_bid_targets', 'DELETE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.wb_collection_runs', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_collection_runs', 'DELETE'
    )
    AND has_column_privilege(
        'position_collector', 'search_position.wb_collection_runs', 'status', 'UPDATE'
    )
    AND NOT has_column_privilege(
        'position_collector', 'search_position.wb_collection_runs', 'scheduled_for', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_collection_runs', 'UPDATE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.wb_search_snapshots', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_search_snapshots', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_search_snapshots', 'DELETE'
    )
    AND has_table_privilege(
        'position_collector', 'search_position.wb_bid_snapshots', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_bid_snapshots', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_collector', 'search_position.wb_bid_snapshots', 'DELETE'
    )
    AND NOT has_sequence_privilege(
        'position_collector', 'search_position.wb_search_targets_id_seq', 'USAGE'
    )
    AND NOT has_sequence_privilege(
        'position_collector', 'search_position.wb_bid_targets_id_seq', 'USAGE'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.monitors', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.monitors', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.monitors', 'DELETE'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.collection_runs', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.measurements', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.alerts', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.published_measurements', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.published_alerts', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.latest_measurements', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.ozon_collector_circuit', 'SELECT'
    )
    AND NOT has_function_privilege(
        'position_reader',
        'search_position.claim_ozon_request_budget(text,timestamp with time zone)',
        'EXECUTE'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.wb_search_snapshots', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.wb_search_targets', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.wb_bid_targets', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.wb_collection_runs', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.wb_bid_snapshots', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.latest_wb_search_snapshots', 'SELECT'
    )
    AND has_table_privilege(
        'position_reader', 'search_position.latest_wb_bid_snapshots', 'SELECT'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_default_acl AS defaults
        CROSS JOIN LATERAL aclexplode(defaults.defaclacl) AS expanded_acl
        WHERE defaults.defaclnamespace = 'search_position'::regnamespace
          AND defaults.defaclobjtype = 'r'
          AND expanded_acl.grantee = 'position_reader'::regrole
          AND expanded_acl.privilege_type = 'SELECT'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_default_acl AS defaults
        CROSS JOIN LATERAL aclexplode(defaults.defaclacl) AS expanded_acl
        WHERE defaults.defaclnamespace = 'daily_reporting'::regnamespace
          AND expanded_acl.grantee = 'position_reader'::regrole
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.wb_search_snapshots', 'INSERT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'search_position.wb_bid_snapshots', 'UPDATE'
    )
    AND NOT has_function_privilege(
        'position_collector',
        'search_position.enforce_wb_search_target_update()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_reader',
        'search_position.enforce_wb_search_target_update()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_collector',
        'search_position.enforce_wb_bid_target_update()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_reader',
        'search_position.enforce_wb_bid_target_update()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_collector',
        'search_position.enforce_wb_collection_run_state()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_reader',
        'search_position.enforce_wb_collection_run_state()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_collector',
        'search_position.require_running_wb_snapshot_run()',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'position_reader',
        'search_position.require_running_wb_snapshot_run()',
        'EXECUTE'
    )
    AND has_database_privilege('report_worker', current_database(), 'CONNECT')
    AND NOT has_database_privilege('report_worker', current_database(), 'TEMP')
    AND has_schema_privilege('report_worker', 'daily_reporting', 'USAGE')
    AND has_table_privilege(
        'report_worker', 'daily_reporting.delivery_batches', 'SELECT,INSERT'
    )
    AND has_column_privilege(
        'report_worker', 'daily_reporting.delivery_batches', 'status', 'UPDATE'
    )
    AND has_column_privilege(
        'report_worker', 'daily_reporting.delivery_batches',
        'artifact_html_sha256', 'UPDATE'
    )
    AND NOT has_table_privilege(
        'report_worker', 'daily_reporting.delivery_batches', 'DELETE'
    )
    AND has_table_privilege(
        'report_worker', 'daily_reporting.delivery_coverage', 'SELECT,INSERT'
    )
    AND NOT has_table_privilege(
        'report_worker', 'daily_reporting.delivery_coverage', 'UPDATE'
    )
    AND has_table_privilege(
        'report_worker', 'daily_reporting.delivery_attempts', 'SELECT,INSERT'
    )
    AND NOT has_table_privilege(
        'report_worker', 'daily_reporting.delivery_attempts', 'UPDATE'
    )
    AND has_table_privilege(
        'report_worker', 'daily_reporting.claimable_deliveries', 'SELECT'
    )
    AND has_table_privilege(
        'report_worker', 'daily_reporting.generation_attempts', 'SELECT,INSERT'
    )
    AND NOT has_table_privilege(
        'report_worker', 'daily_reporting.generation_attempts', 'UPDATE,DELETE'
    )
    AND has_table_privilege(
        'report_worker', 'daily_reporting.generatable_batches', 'SELECT'
    )
    AND NOT has_table_privilege(
        'report_worker', 'search_position.monitors', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_collector', 'daily_reporting.delivery_batches', 'SELECT'
    )
    AND has_schema_privilege(
        'position_reader', 'daily_reporting', 'USAGE'
    )
    AND NOT has_schema_privilege(
        'position_reader', 'daily_reporting', 'CREATE'
    )
    AND (
        SELECT bool_and(
            has_table_privilege('position_reader', readable.object_name, 'SELECT')
            AND NOT has_table_privilege(
                'position_reader', readable.object_name,
                'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
            )
        )
        FROM unnest(ARRAY[
            'daily_reporting.mcp_collection_status',
            'daily_reporting.mcp_published_source_snapshots',
            'daily_reporting.mcp_sales_facts',
            'daily_reporting.mcp_advertising_facts',
            'daily_reporting.mcp_advertising_expense_facts',
            'daily_reporting.mcp_finance_facts',
            'daily_reporting.mcp_stock_facts',
            'daily_reporting.mcp_price_facts',
            'daily_reporting.mcp_ready_reports'
        ]::text[]) AS readable(object_name)
    )
    AND NOT EXISTS (
        SELECT 1
        FROM unnest(ARRAY[
            'daily_reporting.source_snapshots',
            'daily_reporting.sales_facts',
            'daily_reporting.advertising_facts',
            'daily_reporting.advertising_expense_facts',
            'daily_reporting.finance_facts',
            'daily_reporting.stock_facts',
            'daily_reporting.price_facts',
            'daily_reporting.unit_economics_inputs',
            'daily_reporting.collection_claims',
            'daily_reporting.delivery_batches',
            'daily_reporting.delivery_coverage',
            'daily_reporting.delivery_attempts',
            'daily_reporting.delivery_reconciliations',
            'daily_reporting.generation_attempts',
            'daily_reporting.claimable_deliveries',
            'daily_reporting.generatable_batches',
            'daily_reporting.stalled_report_work',
            'daily_reporting.published_source_snapshots',
            'daily_reporting.published_sales_facts',
            'daily_reporting.published_advertising_facts',
            'daily_reporting.published_advertising_expense_facts',
            'daily_reporting.published_finance_facts',
            'daily_reporting.published_stock_facts',
            'daily_reporting.published_price_facts'
        ]::text[]) AS protected(object_name)
        WHERE has_table_privilege(
            'position_reader', protected.object_name, 'SELECT'
        )
    )
    AND NOT has_table_privilege(
        'position_reader', 'daily_reporting.delivery_batches', 'SELECT'
    )
    AND NOT has_function_privilege(
        'report_worker', 'daily_reporting.enforce_delivery_batch_state()', 'EXECUTE'
    )
    AND NOT has_function_privilege(
        'report_worker', 'daily_reporting.require_generatable_batch()', 'EXECUTE'
    )
    AND NOT has_function_privilege(
        'report_worker',
        'daily_reporting.reject_generation_attempt_mutation()', 'EXECUTE'
    )
    AND has_database_privilege('report_collector', current_database(), 'CONNECT')
    AND NOT has_database_privilege('report_collector', current_database(), 'TEMP')
    AND has_schema_privilege('report_collector', 'daily_reporting', 'USAGE')
    AND has_table_privilege(
        'report_collector', 'daily_reporting.source_snapshots', 'SELECT,INSERT'
    )
    AND has_column_privilege(
        'report_collector', 'daily_reporting.source_snapshots', 'status', 'UPDATE'
    )
    AND has_table_privilege(
        'report_collector', 'daily_reporting.sales_facts', 'INSERT'
    )
    AND NOT has_table_privilege(
        'report_collector', 'daily_reporting.collection_claims',
        'SELECT,INSERT,UPDATE,DELETE'
    )
    AND has_function_privilege(
        'report_collector',
        'daily_reporting.claim_report_collection(text,text,timestamp with time zone,text)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_collector',
        'daily_reporting.release_report_collection_claim(bigint,bigint,text)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_collector',
        'daily_reporting.complete_report_collection_claim(bigint,bigint,text)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_collector',
        'daily_reporting.claim_ozon_sales_refresh(text)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_collector',
        'daily_reporting.complete_ozon_sales_refresh(bigint,integer,text,timestamp with time zone)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_collector',
        'daily_reporting.fail_ozon_sales_refresh(bigint,integer,text,text)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_collector',
        'daily_reporting.claim_marketplace_sales_refresh(text)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_collector',
        'daily_reporting.complete_marketplace_sales_refresh(bigint,integer,text,timestamp with time zone)',
        'EXECUTE'
    )
    AND has_function_privilege(
        'report_collector',
        'daily_reporting.fail_marketplace_sales_refresh(bigint,integer,text,text)',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'report_collector',
        'daily_reporting.claim_marketplace_sales_refresh_for(text,text)',
        'EXECUTE'
    )
    AND NOT has_function_privilege(
        'report_collector',
        'daily_reporting.finish_marketplace_sales_refresh(bigint,integer,text,timestamp with time zone,text,text)',
        'EXECUTE'
    )
    AND NOT has_table_privilege(
        'report_collector', 'daily_reporting.ozon_sales_refresh_requests',
        'SELECT,INSERT,UPDATE,DELETE'
    )
    AND has_table_privilege(
        'report_collector', 'daily_reporting.collection_staging_snapshots',
        'SELECT,INSERT,DELETE'
    )
    AND NOT has_table_privilege(
        'report_collector', 'daily_reporting.collection_staging_snapshots',
        'UPDATE,TRUNCATE,REFERENCES,TRIGGER'
    )
    AND NOT has_function_privilege(
        'report_worker',
        'daily_reporting.claim_report_collection(text,text,timestamp with time zone,text)',
        'EXECUTE'
    )
    AND NOT has_table_privilege(
        'report_collector', 'daily_reporting.delivery_batches', 'SELECT'
    )
    AND has_table_privilege(
        'report_worker', 'daily_reporting.published_source_snapshots', 'SELECT'
    )
    AND has_table_privilege(
        'report_worker', 'daily_reporting.published_sales_facts', 'SELECT'
    )
    AND NOT has_table_privilege(
        'report_worker', 'daily_reporting.source_snapshots', 'SELECT'
    )
    AND NOT has_table_privilege(
        'position_reader', 'daily_reporting.published_source_snapshots', 'SELECT'
    )
    AND NOT has_function_privilege(
        'report_collector', 'daily_reporting.enforce_source_snapshot_state()', 'EXECUTE'
    )
    AND (
        SELECT count(*) = 3 AND bool_and(
              rolcanlogin
          AND NOT rolsuper
          AND NOT rolcreatedb
          AND NOT rolcreaterole
          AND NOT rolinherit
          AND NOT rolreplication
          AND NOT rolbypassrls
          AND rolconnlimit = 4
        )
        FROM pg_roles
        WHERE rolname IN (
            'control_writer','ozon_control_planner','ozon_control_executor'
        )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_auth_members AS membership
        WHERE membership.roleid IN (
                  'control_writer'::regrole,
                  'ozon_control_planner'::regrole,
                  'ozon_control_executor'::regrole
              )
           OR membership.member IN (
                  'control_writer'::regrole,
                  'ozon_control_planner'::regrole,
                  'ozon_control_executor'::regrole
              )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_namespace AS namespace
        CROSS JOIN unnest(ARRAY[
            'position_collector', 'position_reader', 'report_worker',
            'report_collector', 'report_refresh_requester', 'control_writer',
            'ozon_control_planner','ozon_control_executor'
        ]::name[]) AS application_role(role_name)
        WHERE namespace.nspname <> 'information_schema'
          AND namespace.nspname !~ '^pg_'
          AND has_schema_privilege(
              role_name, namespace.nspname, 'CREATE'
          )
    )
    AND has_database_privilege('control_writer', current_database(), 'CONNECT')
    AND NOT has_database_privilege('control_writer', current_database(), 'CREATE')
    AND NOT has_database_privilege('control_writer', current_database(), 'TEMP')
    AND (
        SELECT count(*) = 8 AND bool_and(
            has_database_privilege(role_name, current_database(), 'CONNECT')
            AND NOT has_database_privilege(
                role_name, current_database(), 'CREATE'
            )
            AND NOT has_database_privilege(
                role_name, current_database(), 'TEMP'
            )
        )
        FROM unnest(ARRAY[
            'position_collector', 'position_reader', 'report_worker',
            'report_collector', 'report_refresh_requester', 'control_writer',
            'ozon_control_planner','ozon_control_executor'
        ]::name[]) AS application_role(role_name)
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_database AS database_row
        CROSS JOIN unnest(ARRAY[
            'position_collector', 'position_reader', 'report_worker',
            'report_collector', 'report_refresh_requester', 'control_writer',
            'ozon_control_planner','ozon_control_executor'
        ]::name[]) AS application_role(role_name)
        WHERE database_row.datname <> current_database()
          AND (
              has_database_privilege(
                  role_name, database_row.datname, 'CONNECT'
              )
              OR has_database_privilege(
                  role_name, database_row.datname, 'TEMP'
              )
              OR has_database_privilege(
                  role_name, database_row.datname, 'CREATE'
              )
          )
    )
    AND has_schema_privilege('control_writer', 'control', 'USAGE')
    AND NOT has_schema_privilege('control_writer', 'control', 'CREATE')
    AND NOT has_schema_privilege('control_writer', 'daily_reporting', 'USAGE')
    AND NOT has_schema_privilege('control_writer', 'search_position', 'USAGE')
    AND (
        SELECT string_agg(
                   relation.relname::text || ':' || acl.privilege_type,
                   ',' ORDER BY relation.relname, acl.privilege_type
               ) =
               'wb_action_reservations:INSERT,wb_action_reservations:SELECT,' ||
               'wb_audit_events:INSERT,wb_audit_events:SELECT,' ||
               'wb_plan_approvals:INSERT,wb_plan_approvals:SELECT,' ||
               'wb_plans:INSERT,wb_plans:SELECT,' ||
               'wb_policy_revisions:INSERT,wb_policy_revisions:SELECT,' ||
               'wb_prepare_reservations:INSERT,wb_prepare_reservations:SELECT,' ||
               'wb_runtime_gates:SELECT'
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        CROSS JOIN LATERAL aclexplode(
            coalesce(relation.relacl, acldefault('r', relation.relowner))
        ) AS acl
        WHERE namespace.nspname = 'control'
          AND relation.relkind IN ('r', 'p')
          AND acl.grantee = 'control_writer'::regrole
    )
    AND (
        SELECT string_agg(
                   relation.relname::text || '.' || attribute.attname::text ||
                   ':' || acl.privilege_type,
                   ',' ORDER BY relation.relname, attribute.attname,
                                acl.privilege_type
               ) =
               'wb_plans.apply_started_at:UPDATE,' ||
               'wb_plans.finished_at:UPDATE,' ||
               'wb_plans.last_error_class:UPDATE,' ||
               'wb_plans.readback_json:UPDATE,' ||
               'wb_plans.status:UPDATE,' ||
               'wb_plans.write_response_json:UPDATE'
        FROM pg_attribute AS attribute
        JOIN pg_class AS relation ON relation.oid = attribute.attrelid
        JOIN pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        CROSS JOIN LATERAL aclexplode(attribute.attacl) AS acl
        WHERE namespace.nspname = 'control'
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
          AND acl.grantee = 'control_writer'::regrole
    )
    AND (
        SELECT string_agg(acl.privilege_type, ',' ORDER BY acl.privilege_type)
               = 'USAGE'
        FROM pg_namespace AS namespace
        CROSS JOIN LATERAL aclexplode(
            coalesce(namespace.nspacl, acldefault('n', namespace.nspowner))
        ) AS acl
        WHERE namespace.nspname = 'control'
          AND acl.grantee = 'control_writer'::regrole
    )
    AND (
        SELECT string_agg(acl.privilege_type, ',' ORDER BY acl.privilege_type)
               = 'SELECT,USAGE'
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        CROSS JOIN LATERAL aclexplode(
            coalesce(relation.relacl, acldefault('S', relation.relowner))
        ) AS acl
        WHERE namespace.nspname = 'control'
          AND relation.relkind = 'S'
          AND relation.relname = 'wb_audit_events_id_seq'
          AND acl.grantee = 'control_writer'::regrole
    )
    AND NOT has_sequence_privilege(
        'control_writer','control.ozon_campaign_audit_events_event_id_seq',
        'SELECT,USAGE'
    )
    AND (
        SELECT array_agg(
                   relation.relname::text || ':' || acl.privilege_type
                   ORDER BY relation.relname,acl.privilege_type
               )
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
        CROSS JOIN LATERAL aclexplode(
            coalesce(relation.relacl,acldefault('r',relation.relowner))
        ) AS acl
        WHERE namespace.nspname='control'
          AND relation.relname LIKE 'ozon\_%' ESCAPE '\'
          AND relation.relkind IN ('r','p')
          AND acl.grantee='ozon_control_planner'::regrole
    ) = ARRAY[
        'ozon_campaign_audit_events:INSERT','ozon_campaign_audit_events:SELECT',
        'ozon_campaign_launch_workflows:SELECT',
        'ozon_campaign_plan_approvals:INSERT','ozon_campaign_plan_approvals:SELECT',
        'ozon_campaign_plans:INSERT','ozon_campaign_plans:SELECT',
        'ozon_policy_revisions:INSERT','ozon_policy_revisions:SELECT',
        'ozon_runtime_gates:SELECT'
    ]::text[]
    AND (
        SELECT array_agg(
                   relation.relname::text || '.' || attribute.attname::text ||
                   ':' || acl.privilege_type
                   ORDER BY relation.relname,attribute.attname,acl.privilege_type
               )
        FROM pg_attribute AS attribute
        JOIN pg_class AS relation ON relation.oid=attribute.attrelid
        JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
        CROSS JOIN LATERAL aclexplode(attribute.attacl) AS acl
        WHERE namespace.nspname='control' AND attribute.attnum>0
          AND NOT attribute.attisdropped
          AND acl.grantee='ozon_control_planner'::regrole
    ) = ARRAY[
        'ozon_campaign_launch_workflows.available_at:UPDATE',
        'ozon_campaign_launch_workflows.requested_at:UPDATE',
        'ozon_campaign_launch_workflows.requested_by_actor_id:UPDATE',
        'ozon_campaign_plans.finished_at:UPDATE',
        'ozon_campaign_plans.status:UPDATE'
    ]::text[]
    AND (
        SELECT string_agg(acl.privilege_type,',' ORDER BY acl.privilege_type)
        FROM pg_namespace AS namespace
        CROSS JOIN LATERAL aclexplode(
            coalesce(namespace.nspacl,acldefault('n',namespace.nspowner))
        ) AS acl
        WHERE namespace.nspname='control'
          AND acl.grantee='ozon_control_planner'::regrole
    ) = 'USAGE'
    AND (
        SELECT string_agg(acl.privilege_type,',' ORDER BY acl.privilege_type)
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
        CROSS JOIN LATERAL aclexplode(
            coalesce(relation.relacl,acldefault('S',relation.relowner))
        ) AS acl
        WHERE namespace.nspname='control'
          AND relation.relname='ozon_campaign_audit_events_event_id_seq'
          AND acl.grantee='ozon_control_planner'::regrole
    ) = 'SELECT,USAGE'
    AND (
        SELECT array_agg(
                   relation.relname::text || ':' || acl.privilege_type
                   ORDER BY relation.relname,acl.privilege_type
               )
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
        CROSS JOIN LATERAL aclexplode(
            coalesce(relation.relacl,acldefault('r',relation.relowner))
        ) AS acl
        WHERE namespace.nspname='control'
          AND relation.relname LIKE 'ozon\_%' ESCAPE '\'
          AND relation.relkind IN ('r','p')
          AND acl.grantee='ozon_control_executor'::regrole
    ) = ARRAY[
        'ozon_campaign_action_reservations:INSERT','ozon_campaign_action_reservations:SELECT',
        'ozon_campaign_audit_events:INSERT','ozon_campaign_audit_events:SELECT',
        'ozon_campaign_guards:INSERT','ozon_campaign_guards:SELECT',
        'ozon_campaign_launch_workflows:SELECT',
        'ozon_campaign_plan_approvals:SELECT','ozon_campaign_plans:SELECT',
        'ozon_policy_revisions:SELECT','ozon_runtime_gates:SELECT',
        'ozon_static_guard_audit_events:INSERT',
        'ozon_static_guard_audit_events:SELECT'
    ]::text[]
    AND (
        SELECT array_agg(
                   relation.relname::text || '.' || attribute.attname::text ||
                   ':' || acl.privilege_type
                   ORDER BY relation.relname,attribute.attname,acl.privilege_type
               )
        FROM pg_attribute AS attribute
        JOIN pg_class AS relation ON relation.oid=attribute.attrelid
        JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
        CROSS JOIN LATERAL aclexplode(attribute.attacl) AS acl
        WHERE namespace.nspname='control' AND attribute.attnum>0
          AND NOT attribute.attisdropped
          AND acl.grantee='ozon_control_executor'::regrole
    ) = ARRAY[
        'ozon_campaign_guards.incident_error_class:UPDATE',
        'ozon_campaign_guards.last_checked_at:UPDATE',
        'ozon_campaign_guards.last_revenue_minor:UPDATE',
        'ozon_campaign_guards.last_spend_minor:UPDATE',
        'ozon_campaign_guards.status:UPDATE',
        'ozon_campaign_guards.stop_generation:UPDATE',
        'ozon_campaign_guards.stop_lease_claimed_at:UPDATE',
        'ozon_campaign_guards.stop_lease_expires_at:UPDATE',
        'ozon_campaign_guards.stop_lease_owner_id:UPDATE',
        'ozon_campaign_guards.stop_lease_token:UPDATE',
        'ozon_campaign_guards.stop_reason:UPDATE',
        'ozon_campaign_guards.stop_write_started_at:UPDATE',
        'ozon_campaign_guards.stopped_at:UPDATE',
        'ozon_campaign_launch_workflows.action:UPDATE',
        'ozon_campaign_launch_workflows.available_at:UPDATE',
        'ozon_campaign_launch_workflows.create_identity_preflight_at:UPDATE',
        'ozon_campaign_launch_workflows.create_identity_preflight_digest:UPDATE',
        'ozon_campaign_launch_workflows.generation:UPDATE',
        'ozon_campaign_launch_workflows.last_completed_at:UPDATE',
        'ozon_campaign_launch_workflows.last_error_class:UPDATE',
        'ozon_campaign_launch_workflows.last_readback_json:UPDATE',
        'ozon_campaign_launch_workflows.lease_claimed_at:UPDATE',
        'ozon_campaign_launch_workflows.lease_expires_at:UPDATE',
        'ozon_campaign_launch_workflows.lease_owner_id:UPDATE',
        'ozon_campaign_launch_workflows.lease_token:UPDATE',
        'ozon_campaign_launch_workflows.write_started_at:UPDATE',
        'ozon_campaign_plans.campaign_id:UPDATE',
        'ozon_campaign_plans.finished_at:UPDATE',
        'ozon_campaign_plans.last_error_class:UPDATE',
        'ozon_campaign_plans.operation_started_at:UPDATE',
        'ozon_campaign_plans.readback_json:UPDATE',
        'ozon_campaign_plans.status:UPDATE'
    ]::text[]
    AND (
        SELECT string_agg(acl.privilege_type,',' ORDER BY acl.privilege_type)
        FROM pg_namespace AS namespace
        CROSS JOIN LATERAL aclexplode(
            coalesce(namespace.nspacl,acldefault('n',namespace.nspowner))
        ) AS acl
        WHERE namespace.nspname='control'
          AND acl.grantee='ozon_control_executor'::regrole
    ) = 'USAGE'
    AND (
        SELECT string_agg(acl.privilege_type,',' ORDER BY acl.privilege_type)
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
        CROSS JOIN LATERAL aclexplode(
            coalesce(relation.relacl,acldefault('S',relation.relowner))
        ) AS acl
        WHERE namespace.nspname='control'
          AND relation.relname='ozon_campaign_audit_events_event_id_seq'
          AND acl.grantee='ozon_control_executor'::regrole
    ) = 'SELECT,USAGE'
    AND (
        SELECT string_agg(acl.privilege_type,',' ORDER BY acl.privilege_type)
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
        CROSS JOIN LATERAL aclexplode(
            coalesce(relation.relacl,acldefault('S',relation.relowner))
        ) AS acl
        WHERE namespace.nspname='control'
          AND relation.relname='ozon_static_guard_audit_events_event_id_seq'
          AND acl.grantee='ozon_control_executor'::regrole
    ) = 'SELECT,USAGE'
    AND NOT EXISTS (
        SELECT 1
        FROM unnest(ARRAY[
            'position_collector', 'position_reader',
            'report_collector', 'report_refresh_requester', 'report_worker'
        ]::text[]) AS application_role(role_name)
        WHERE has_schema_privilege(role_name::name, 'control', 'USAGE')
           OR has_schema_privilege(role_name::name, 'control', 'CREATE')
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_namespace AS namespace
        CROSS JOIN LATERAL aclexplode(
            coalesce(namespace.nspacl, acldefault('n', namespace.nspowner))
        ) AS acl
        WHERE namespace.nspname = 'control'
          AND (
              acl.grantee = 0
              OR acl.grantee IN (
                  SELECT oid
                  FROM pg_roles
                  WHERE rolname IN (
                      'position_collector', 'position_reader',
                      'report_collector', 'report_refresh_requester', 'report_worker'
                  )
              )
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        CROSS JOIN LATERAL aclexplode(
            coalesce(
                relation.relacl,
                acldefault(
                    (CASE WHEN relation.relkind = 'S' THEN 'S' ELSE 'r' END)::"char",
                    relation.relowner
                )
            )
        ) AS acl
        WHERE namespace.nspname = 'control'
          AND (
              acl.grantee = 0
              OR acl.grantee IN (
                  SELECT oid
                  FROM pg_roles
                  WHERE rolname IN (
                      'position_collector', 'position_reader',
                      'report_collector', 'report_refresh_requester', 'report_worker'
                  )
              )
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_proc AS routine
        JOIN pg_namespace AS namespace
          ON namespace.oid = routine.pronamespace
        CROSS JOIN LATERAL aclexplode(
            coalesce(routine.proacl, acldefault('f', routine.proowner))
        ) AS acl
        WHERE namespace.nspname = 'control'
          AND (
              acl.grantee = 0
              OR acl.grantee = 'control_writer'::regrole
              OR (
                  acl.grantee IN (
                      'ozon_control_planner'::regrole,
                      'ozon_control_executor'::regrole
                  )
                  AND routine.proname<>'ozon_runtime_gates_active_locked'
              )
              OR acl.grantee IN (
                  SELECT oid
                  FROM pg_roles
                  WHERE rolname IN (
                      'position_collector', 'position_reader',
                      'report_collector', 'report_refresh_requester', 'report_worker'
                  )
              )
          )
    )
    AND has_function_privilege(
        'ozon_control_planner',
        'control.ozon_runtime_gates_active_locked(text,bigint)','EXECUTE'
    )
    AND has_function_privilege(
        'ozon_control_executor',
        'control.ozon_runtime_gates_active_locked(text,bigint)','EXECUTE'
    )
    AND NOT has_function_privilege(
        'control_writer',
        'control.ozon_runtime_gates_active_locked(text,bigint)','EXECUTE'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_default_acl AS defaults
        CROSS JOIN LATERAL aclexplode(defaults.defaclacl) AS acl
        WHERE defaults.defaclnamespace = 'control'::regnamespace
          AND (
              acl.grantee = 0
              OR acl.grantee = 'control_writer'::regrole
              OR acl.grantee = 'ozon_control_planner'::regrole
              OR acl.grantee = 'ozon_control_executor'::regrole
              OR acl.grantee IN (
                  SELECT oid
                  FROM pg_roles
                  WHERE rolname IN (
                      'position_collector', 'position_reader',
                      'report_collector', 'report_refresh_requester', 'report_worker'
                  )
              )
          )
    )
    AND (
        SELECT rolconfig @> ARRAY['default_transaction_read_only=on']
        FROM pg_roles
        WHERE rolname = 'position_reader'
    )
    AND (
        :'require_migration_ledger' = 'false'
        OR (
            to_regclass('mcp_runtime.schema_migrations') IS NOT NULL
            AND (
                SELECT count(*) = 28
                    AND bool_and(state = 'applied')
                    AND bool_and(applied_at IS NOT NULL)
                FROM mcp_runtime.schema_migrations
            )
        )
    );
SQL
} 2>/dev/null)"

[ "$healthy" = "t" ]
