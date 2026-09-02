SELECT current_user = 'control_writer'
AND EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles runtime_role
    WHERE runtime_role.rolname=current_user
      AND runtime_role.rolcanlogin
      AND NOT runtime_role.rolsuper
      AND NOT runtime_role.rolcreatedb
      AND NOT runtime_role.rolcreaterole
      AND NOT runtime_role.rolinherit
      AND NOT runtime_role.rolreplication
      AND NOT runtime_role.rolbypassrls
      AND runtime_role.rolconnlimit=4
)
AND has_database_privilege(current_user, current_database(), 'CONNECT')
AND NOT has_database_privilege(current_user, current_database(), 'TEMPORARY')
AND NOT has_database_privilege(current_user, current_database(), 'CREATE')
AND NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_database database_row
    WHERE database_row.datname <> current_database()
      AND (
        has_database_privilege(
            current_user, database_row.oid, 'CONNECT'
        ) OR has_database_privilege(
            current_user, database_row.oid, 'TEMPORARY'
        ) OR has_database_privilege(
            current_user, database_row.oid, 'CREATE'
        )
      )
)
AND has_schema_privilege(current_user, 'control', 'USAGE')
AND NOT has_schema_privilege(current_user, 'control', 'CREATE')
AND NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_namespace schema_row
    WHERE schema_row.nspname <> 'information_schema'
      AND schema_row.nspname !~ '^pg_'
      AND has_schema_privilege(
        current_user, schema_row.oid, 'CREATE'
      )
)
AND NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_auth_members memberships
    JOIN pg_catalog.pg_roles role_member
      ON role_member.oid=memberships.member
    JOIN pg_catalog.pg_roles granted_role
      ON granted_role.oid=memberships.roleid
    WHERE role_member.rolname=current_user
       OR granted_role.rolname=current_user
)
AND has_table_privilege(current_user, 'control.wb_plans', 'SELECT')
AND has_table_privilege(current_user, 'control.wb_plans', 'INSERT')
AND NOT has_table_privilege(current_user, 'control.wb_plans', 'UPDATE')
AND NOT has_table_privilege(current_user, 'control.wb_plans', 'DELETE')
AND (SELECT bool_and(has_column_privilege(
        current_user, 'control.wb_plans', allowed.column_name, 'UPDATE'
    ))
     FROM unnest(ARRAY[
        'status', 'apply_started_at', 'finished_at', 'last_error_class',
        'write_response_json', 'readback_json'
     ]) allowed(column_name))
AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns column_def
    WHERE column_def.table_schema='control'
      AND column_def.table_name='wb_plans'
      AND column_def.column_name <> ALL(ARRAY[
        'status', 'apply_started_at', 'finished_at', 'last_error_class',
        'write_response_json', 'readback_json'
      ])
      AND has_column_privilege(
        current_user, 'control.wb_plans', column_def.column_name, 'UPDATE'
      )
)
AND has_table_privilege(current_user, 'control.wb_policy_revisions', 'SELECT')
AND has_table_privilege(current_user, 'control.wb_policy_revisions', 'INSERT')
AND NOT has_table_privilege(current_user, 'control.wb_policy_revisions', 'UPDATE')
AND NOT has_table_privilege(current_user, 'control.wb_policy_revisions', 'DELETE')
AND has_table_privilege(current_user, 'control.wb_prepare_reservations', 'SELECT')
AND has_table_privilege(current_user, 'control.wb_prepare_reservations', 'INSERT')
AND NOT has_table_privilege(current_user, 'control.wb_prepare_reservations', 'UPDATE')
AND NOT has_table_privilege(current_user, 'control.wb_prepare_reservations', 'DELETE')
AND has_table_privilege(current_user, 'control.wb_plan_approvals', 'SELECT')
AND has_table_privilege(current_user, 'control.wb_plan_approvals', 'INSERT')
AND NOT has_table_privilege(current_user, 'control.wb_plan_approvals', 'UPDATE')
AND NOT has_table_privilege(current_user, 'control.wb_plan_approvals', 'DELETE')
AND has_table_privilege(current_user, 'control.wb_runtime_gates', 'SELECT')
AND NOT has_table_privilege(current_user, 'control.wb_runtime_gates', 'INSERT')
AND NOT has_table_privilege(current_user, 'control.wb_runtime_gates', 'UPDATE')
AND NOT has_table_privilege(current_user, 'control.wb_runtime_gates', 'DELETE')
AND has_table_privilege(current_user, 'control.wb_action_reservations', 'SELECT')
AND has_table_privilege(current_user, 'control.wb_action_reservations', 'INSERT')
AND NOT has_table_privilege(current_user, 'control.wb_action_reservations', 'UPDATE')
AND NOT has_table_privilege(current_user, 'control.wb_action_reservations', 'DELETE')
AND has_table_privilege(current_user, 'control.wb_audit_events', 'SELECT')
AND has_table_privilege(current_user, 'control.wb_audit_events', 'INSERT')
AND NOT has_table_privilege(current_user, 'control.wb_audit_events', 'UPDATE')
AND NOT has_table_privilege(current_user, 'control.wb_audit_events', 'DELETE')
AND NOT EXISTS (
    SELECT 1
    FROM unnest(ARRAY[
        'control.wb_action_reservations',
        'control.wb_audit_events',
        'control.wb_plan_approvals',
        'control.wb_plans',
        'control.wb_policy_revisions',
        'control.wb_prepare_reservations',
        'control.wb_runtime_gates'
    ]) expected_relation(relation_name)
    WHERE has_table_privilege(
        current_user, expected_relation.relation_name, 'TRUNCATE'
    ) OR has_table_privilege(
        current_user, expected_relation.relation_name, 'REFERENCES'
    ) OR has_table_privilege(
        current_user, expected_relation.relation_name, 'TRIGGER'
    )
)
AND NOT has_schema_privilege(current_user, 'daily_reporting', 'USAGE')
AND NOT has_schema_privilege(current_user, 'search_position', 'USAGE')
AND (
    SELECT array_agg(
        schemas.nspname || '.' || relations.relname
        ORDER BY schemas.nspname, relations.relname
    )
    FROM pg_catalog.pg_class relations
    JOIN pg_catalog.pg_namespace schemas ON schemas.oid=relations.relnamespace
    WHERE relations.relkind IN ('r','p','v','m','f')
      AND schemas.nspname <> 'information_schema'
      AND schemas.nspname !~ '^pg_'
      AND relations.relname LIKE 'wb\_%' ESCAPE '\'
      AND (
        has_table_privilege(current_user, relations.oid, 'SELECT')
        OR has_table_privilege(current_user, relations.oid, 'INSERT')
        OR has_table_privilege(current_user, relations.oid, 'UPDATE')
        OR has_table_privilege(current_user, relations.oid, 'DELETE')
        OR has_table_privilege(current_user, relations.oid, 'TRUNCATE')
        OR has_table_privilege(current_user, relations.oid, 'REFERENCES')
        OR has_table_privilege(current_user, relations.oid, 'TRIGGER')
      )
) = ARRAY[
    'control.wb_action_reservations',
    'control.wb_audit_events',
    'control.wb_plan_approvals',
    'control.wb_plans',
    'control.wb_policy_revisions',
    'control.wb_prepare_reservations',
    'control.wb_runtime_gates'
]::text[]
AND (
    SELECT array_agg(
        schemas.nspname || '.' || sequences.relname
        ORDER BY schemas.nspname, sequences.relname
    )
    FROM pg_catalog.pg_class sequences
    JOIN pg_catalog.pg_namespace schemas ON schemas.oid=sequences.relnamespace
    WHERE sequences.relkind='S'
      AND schemas.nspname <> 'information_schema'
      AND schemas.nspname !~ '^pg_'
      AND sequences.relname LIKE 'wb\_%' ESCAPE '\'
      AND (
        has_sequence_privilege(current_user, sequences.oid, 'USAGE')
        OR has_sequence_privilege(current_user, sequences.oid, 'SELECT')
        OR has_sequence_privilege(current_user, sequences.oid, 'UPDATE')
      )
) = ARRAY['control.wb_audit_events_id_seq']::text[]
AND has_sequence_privilege(
    current_user, 'control.wb_audit_events_id_seq', 'USAGE'
)
AND has_sequence_privilege(
    current_user, 'control.wb_audit_events_id_seq', 'SELECT'
)
AND NOT has_sequence_privilege(
    current_user, 'control.wb_audit_events_id_seq', 'UPDATE'
)
AND NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_proc accessible_function
    JOIN pg_catalog.pg_namespace function_schema
      ON function_schema.oid=accessible_function.pronamespace
    WHERE function_schema.nspname <> 'information_schema'
      AND function_schema.nspname !~ '^pg_'
      AND has_schema_privilege(
        current_user, function_schema.oid, 'USAGE'
      )
      AND has_function_privilege(
        current_user, accessible_function.oid, 'EXECUTE'
      )
)
AND (
    SELECT array_agg(
        concat_ws('|', tables.relname, triggers.tgname, functions.proname,
            triggers.tgtype::text, triggers.tgenabled::text)
        ORDER BY tables.relname, triggers.tgname
    )
    FROM pg_catalog.pg_trigger triggers
    JOIN pg_catalog.pg_class tables ON tables.oid=triggers.tgrelid
    JOIN pg_catalog.pg_namespace schemas ON schemas.oid=tables.relnamespace
    JOIN pg_catalog.pg_proc functions ON functions.oid=triggers.tgfoid
    WHERE schemas.nspname='control'
      AND tables.relname LIKE 'wb\_%' ESCAPE '\'
      AND NOT triggers.tgisinternal
) = ARRAY[
    'wb_action_reservations|wb_action_reservations_append_only|reject_wb_append_only_mutation|27|O',
    'wb_action_reservations|wb_action_reservations_validate|validate_wb_reservation_insert|7|O',
    'wb_audit_events|wb_audit_events_append_only|reject_wb_append_only_mutation|27|O',
    'wb_plan_approvals|wb_plan_approvals_append_only|reject_wb_append_only_mutation|27|O',
    'wb_plan_approvals|wb_plan_approvals_validate|validate_wb_approval_insert|7|O',
    'wb_plans|wb_plans_transition_guard|enforce_wb_plan_transition|19|O',
    'wb_plans|wb_plans_validate_insert|validate_wb_plan_insert|7|O',
    'wb_policy_revisions|wb_policy_revisions_append_only|reject_wb_append_only_mutation|27|O',
    'wb_policy_revisions|wb_policy_revisions_validate|validate_wb_policy_revision_insert|7|O',
    'wb_prepare_reservations|wb_prepare_reservations_append_only|reject_wb_append_only_mutation|27|O',
    'wb_prepare_reservations|wb_prepare_reservations_validate|validate_wb_prepare_reservation_insert|7|O',
    'wb_runtime_gates|wb_runtime_gates_validate_write|validate_wb_runtime_gate_write|23|O'
]::text[]
AND (
    SELECT array_agg(
        concat_ws('|', functions.proname, functions.prosecdef::text,
            functions.provolatile::text,
            COALESCE(array_to_string(functions.proconfig, ','), ''),
            has_function_privilege(
                current_user, functions.oid, 'EXECUTE'
            )::text)
        ORDER BY functions.proname::text
    )
    FROM pg_catalog.pg_proc functions
    JOIN pg_catalog.pg_namespace schemas ON schemas.oid=functions.pronamespace
    WHERE schemas.nspname='control'
      AND functions.prorettype='pg_catalog.trigger'::regtype
      AND functions.prokind='f'
      AND functions.proname LIKE '%wb%'
) = ARRAY[
    'enforce_wb_plan_transition|false|v||false',
    'reject_wb_append_only_mutation|false|v||false',
    'validate_wb_approval_insert|false|v||false',
    'validate_wb_plan_insert|false|v||false',
    'validate_wb_policy_revision_insert|false|v||false',
    'validate_wb_prepare_reservation_insert|false|v||false',
    'validate_wb_reservation_insert|false|v||false',
    'validate_wb_runtime_gate_write|false|v||false'
]::text[]
AND (
    SELECT array_agg(constraints.conname::text ORDER BY constraints.conname::text)
    FROM pg_catalog.pg_constraint constraints
    JOIN pg_catalog.pg_namespace schemas ON schemas.oid=constraints.connamespace
    WHERE schemas.nspname='control'
      AND constraints.conname = ANY(ARRAY[
        'wb_approval_ttl', 'wb_plan_state_shape', 'wb_plan_ttl',
        'wb_prepare_reservation_ttl', 'wb_runtime_gate_lease_bound',
        'wb_runtime_gate_scope'
      ])
      AND constraints.contype='c'
      AND constraints.convalidated
) = ARRAY[
    'wb_approval_ttl', 'wb_plan_state_shape', 'wb_plan_ttl',
    'wb_prepare_reservation_ttl', 'wb_runtime_gate_lease_bound',
    'wb_runtime_gate_scope'
]::text[];
