SELECT current_user IN ('ozon_control_planner','ozon_control_executor')
AND EXISTS (
    SELECT 1 FROM pg_roles
    WHERE rolname=current_user AND rolcanlogin AND NOT rolsuper
      AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolinherit
      AND NOT rolreplication AND NOT rolbypassrls AND rolconnlimit=4
)
AND has_database_privilege(current_user,current_database(),'CONNECT')
AND NOT has_database_privilege(current_user,current_database(),'CREATE,TEMPORARY')
AND NOT EXISTS (
    SELECT 1 FROM pg_auth_members membership
    JOIN pg_roles member ON member.oid=membership.member
    JOIN pg_roles granted ON granted.oid=membership.roleid
    WHERE member.rolname=current_user OR granted.rolname=current_user
)
AND has_schema_privilege(current_user,'control','USAGE')
AND NOT has_schema_privilege(current_user,'control','CREATE')
AND NOT has_schema_privilege(current_user,'daily_reporting','USAGE')
AND NOT has_schema_privilege(current_user,'search_position','USAGE')
AND (
    SELECT array_agg(relation.relname||':'||acl.privilege_type
                     ORDER BY relation.relname,acl.privilege_type)
    FROM pg_class relation
    JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
    CROSS JOIN LATERAL aclexplode(
        coalesce(relation.relacl,acldefault('r',relation.relowner))
    ) acl
    WHERE namespace.nspname='control'
      AND relation.relname LIKE 'ozon\_%' ESCAPE '\'
      AND relation.relkind IN ('r','p')
      AND acl.grantee=current_user::regrole
)=CASE current_user
    WHEN 'ozon_control_planner' THEN ARRAY[
        'ozon_campaign_audit_events:INSERT','ozon_campaign_audit_events:SELECT',
        'ozon_campaign_launch_workflows:SELECT',
        'ozon_campaign_plan_approvals:INSERT','ozon_campaign_plan_approvals:SELECT',
        'ozon_campaign_plans:INSERT','ozon_campaign_plans:SELECT',
        'ozon_policy_revisions:INSERT','ozon_policy_revisions:SELECT',
        'ozon_runtime_gates:SELECT'
    ]::text[]
    WHEN 'ozon_control_executor' THEN ARRAY[
        'ozon_campaign_action_reservations:INSERT','ozon_campaign_action_reservations:SELECT',
        'ozon_campaign_audit_events:INSERT','ozon_campaign_audit_events:SELECT',
        'ozon_campaign_guards:INSERT','ozon_campaign_guards:SELECT',
        'ozon_campaign_launch_workflows:SELECT',
        'ozon_campaign_plan_approvals:SELECT',
        'ozon_campaign_plans:SELECT',
        'ozon_policy_revisions:SELECT',
        'ozon_runtime_gates:SELECT',
        'ozon_static_guard_audit_events:INSERT','ozon_static_guard_audit_events:SELECT'
    ]::text[]
END
AND (
    SELECT array_agg(relation.relname||'.'||attribute.attname||':'||acl.privilege_type
                     ORDER BY relation.relname,attribute.attname,acl.privilege_type)
    FROM pg_attribute attribute
    JOIN pg_class relation ON relation.oid=attribute.attrelid
    JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
    CROSS JOIN LATERAL aclexplode(attribute.attacl) acl
    WHERE namespace.nspname='control'
      AND relation.relname LIKE 'ozon\_%' ESCAPE '\'
      AND attribute.attnum>0 AND NOT attribute.attisdropped
      AND acl.grantee=current_user::regrole
)=CASE current_user
    WHEN 'ozon_control_planner' THEN ARRAY[
        'ozon_campaign_launch_workflows.available_at:UPDATE',
        'ozon_campaign_launch_workflows.requested_at:UPDATE',
        'ozon_campaign_launch_workflows.requested_by_actor_id:UPDATE',
        'ozon_campaign_plans.finished_at:UPDATE',
        'ozon_campaign_plans.status:UPDATE'
    ]::text[]
    WHEN 'ozon_control_executor' THEN ARRAY[
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
END
AND (
    SELECT array_agg(
        relation.relname||':'||acl.privilege_type
        ORDER BY relation.relname,acl.privilege_type
    )
    FROM pg_class relation
    JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
    CROSS JOIN LATERAL aclexplode(
        coalesce(relation.relacl,acldefault('S',relation.relowner))
    ) acl
    WHERE namespace.nspname='control'
      AND relation.relname LIKE 'ozon\_%' ESCAPE '\'
      AND relation.relkind='S'
      AND acl.grantee=current_user::regrole
)=CASE current_user
    WHEN 'ozon_control_planner' THEN ARRAY[
        'ozon_campaign_audit_events_event_id_seq:SELECT',
        'ozon_campaign_audit_events_event_id_seq:USAGE'
    ]::text[]
    WHEN 'ozon_control_executor' THEN ARRAY[
        'ozon_campaign_audit_events_event_id_seq:SELECT',
        'ozon_campaign_audit_events_event_id_seq:USAGE',
        'ozon_static_guard_audit_events_event_id_seq:SELECT',
        'ozon_static_guard_audit_events_event_id_seq:USAGE'
    ]::text[]
END
AND (
    SELECT coalesce(
        array_agg(routine.proname::text ORDER BY routine.proname),
        ARRAY[]::text[]
    )
    FROM pg_proc routine
    JOIN pg_namespace namespace ON namespace.oid=routine.pronamespace
    WHERE namespace.nspname='control' AND routine.proname LIKE '%ozon%'
      AND has_function_privilege(current_user,routine.oid,'EXECUTE')
)=ARRAY['ozon_runtime_gates_active_locked']::text[]
AND (
    SELECT array_agg(
        relation.relname||'|'||trigger.tgname||'|'||routine.proname||'|'||
        trigger.tgtype::text||'|'||trigger.tgenabled::text
        ORDER BY relation.relname,trigger.tgname
    )
    FROM pg_trigger trigger
    JOIN pg_class relation ON relation.oid=trigger.tgrelid
    JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
    JOIN pg_proc routine ON routine.oid=trigger.tgfoid
    WHERE namespace.nspname='control' AND relation.relname LIKE 'ozon\_%' ESCAPE '\'
      AND NOT trigger.tgisinternal
)=ARRAY[
    'ozon_campaign_action_reservations|ozon_reservations_append_only|reject_ozon_append_only_mutation|27|O',
    'ozon_campaign_action_reservations|ozon_reservations_validate|validate_ozon_reservation_insert|7|O',
    'ozon_campaign_audit_events|ozon_audit_append_only|reject_ozon_append_only_mutation|27|O',
    'ozon_campaign_guards|ozon_guards_transition_guard|enforce_ozon_guard_transition|19|O',
    'ozon_campaign_guards|ozon_guards_validate_insert|validate_ozon_guard_insert|7|O',
    'ozon_campaign_launch_workflows|ozon_launch_workflow_no_delete|reject_ozon_append_only_mutation|11|O',
    'ozon_campaign_launch_workflows|ozon_launch_workflow_update_guard|enforce_ozon_launch_workflow_update|19|O',
    'ozon_campaign_plan_approvals|ozon_approvals_append_only|reject_ozon_append_only_mutation|27|O',
    'ozon_campaign_plan_approvals|ozon_approvals_validate|validate_ozon_approval_insert|7|O',
    'ozon_campaign_plans|ozon_launch_workflow_initialize|initialize_ozon_launch_workflow|5|O',
    'ozon_campaign_plans|ozon_plans_transition_guard|enforce_ozon_plan_transition|19|O',
    'ozon_campaign_plans|ozon_plans_validate_insert|validate_ozon_plan_insert|7|O',
    'ozon_policy_revisions|ozon_policy_revisions_append_only|reject_ozon_append_only_mutation|27|O',
    'ozon_policy_revisions|ozon_policy_revisions_validate|validate_ozon_policy_revision_insert|7|O',
    'ozon_static_guard_audit_events|ozon_static_guard_audit_append_only|reject_ozon_append_only_mutation|27|O'
]::text[];
