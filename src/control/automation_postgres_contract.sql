SELECT current_user = 'wb_automation_writer'
AND EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles runtime_role
    WHERE runtime_role.rolname = current_user
      AND runtime_role.rolcanlogin
      AND NOT runtime_role.rolsuper
      AND NOT runtime_role.rolcreatedb
      AND NOT runtime_role.rolcreaterole
      AND NOT runtime_role.rolinherit
      AND NOT runtime_role.rolreplication
      AND NOT runtime_role.rolbypassrls
      AND runtime_role.rolconnlimit = 2
)
AND has_database_privilege(current_user, current_database(), 'CONNECT')
AND NOT has_database_privilege(current_user, current_database(), 'TEMPORARY')
AND NOT has_database_privilege(current_user, current_database(), 'CREATE')
AND has_schema_privilege(current_user, 'wb_automation', 'USAGE')
AND NOT has_schema_privilege(current_user, 'wb_automation', 'CREATE')
AND NOT has_schema_privilege(current_user, 'control', 'USAGE')
AND NOT has_schema_privilege(current_user, 'daily_reporting', 'USAGE')
AND NOT has_schema_privilege(current_user, 'search_position', 'USAGE')
AND has_table_privilege(current_user, 'wb_automation.cycles', 'SELECT')
AND has_table_privilege(current_user, 'wb_automation.cycles', 'INSERT')
AND NOT has_table_privilege(current_user, 'wb_automation.cycles', 'UPDATE')
AND NOT has_table_privilege(current_user, 'wb_automation.cycles', 'DELETE')
AND has_table_privilege(current_user, 'wb_automation.action_attempts', 'SELECT')
AND has_table_privilege(current_user, 'wb_automation.action_attempts', 'INSERT')
AND NOT has_table_privilege(current_user, 'wb_automation.action_attempts', 'UPDATE')
AND NOT has_table_privilege(current_user, 'wb_automation.action_attempts', 'DELETE')
AND has_column_privilege(
    current_user, 'wb_automation.action_attempts', 'status', 'UPDATE'
)
AND has_column_privilege(
    current_user, 'wb_automation.action_attempts', 'readback_cycle_id', 'UPDATE'
)
AND has_column_privilege(
    current_user, 'wb_automation.action_attempts', 'last_error_class', 'UPDATE'
)
AND has_table_privilege(current_user, 'wb_automation.execution_state', 'SELECT')
AND has_table_privilege(current_user, 'wb_automation.execution_state', 'INSERT')
AND NOT has_table_privilege(current_user, 'wb_automation.execution_state', 'UPDATE')
AND NOT has_table_privilege(current_user, 'wb_automation.execution_state', 'DELETE')
AND (SELECT bool_and(has_column_privilege(
        current_user, 'wb_automation.execution_state', allowed.column_name, 'UPDATE'
    ))
    FROM unnest(ARRAY[
        'policy_digest', 'business_date', 'actions_today', 'last_action_at',
        'paused_for_daily_cap_on', 'pending_idempotency_key', 'incident_class',
        'revision'
    ]) allowed(column_name)
)
AND has_table_privilege(current_user, 'wb_automation.audit_events', 'SELECT')
AND has_table_privilege(current_user, 'wb_automation.audit_events', 'INSERT')
AND NOT has_table_privilege(current_user, 'wb_automation.audit_events', 'UPDATE')
AND NOT has_table_privilege(current_user, 'wb_automation.audit_events', 'DELETE')
AND (
    SELECT array_agg(
        schemas.nspname || '.' || relations.relname
        ORDER BY schemas.nspname, relations.relname
    )
    FROM pg_catalog.pg_class relations
    JOIN pg_catalog.pg_namespace schemas ON schemas.oid = relations.relnamespace
    WHERE relations.relkind IN ('r', 'p', 'v', 'm', 'f')
      AND schemas.nspname <> 'information_schema'
      AND schemas.nspname !~ '^pg_'
      AND (
        has_table_privilege(current_user, relations.oid, 'SELECT')
        OR has_table_privilege(current_user, relations.oid, 'INSERT')
        OR has_table_privilege(current_user, relations.oid, 'UPDATE')
        OR has_table_privilege(current_user, relations.oid, 'DELETE')
      )
) = ARRAY[
    'wb_automation.action_attempts',
    'wb_automation.audit_events',
    'wb_automation.cycles',
    'wb_automation.execution_state'
]::text[]
AND has_sequence_privilege(
    current_user, 'wb_automation.audit_events_id_seq', 'USAGE'
)
AND has_sequence_privilege(
    current_user, 'wb_automation.audit_events_id_seq', 'SELECT'
)
AND NOT has_sequence_privilege(
    current_user, 'wb_automation.audit_events_id_seq', 'UPDATE'
)
AND NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_proc accessible_function
    JOIN pg_catalog.pg_namespace function_schema
      ON function_schema.oid = accessible_function.pronamespace
    WHERE function_schema.nspname = 'wb_automation'
      AND has_function_privilege(
        current_user, accessible_function.oid, 'EXECUTE'
      )
);
