-- Run with psql -X -q -A -t -v ON_ERROR_STOP=1 against the reporting database.
-- Save dated outputs to measure real growth; one sample is not a daily forecast.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '10s';
SET LOCAL lock_timeout = '2s';
WITH relations AS (
    SELECT c.oid, c.relname,
           pg_table_size(c.oid) AS table_bytes,
           pg_indexes_size(c.oid) AS index_bytes,
           pg_total_relation_size(c.oid) AS total_bytes,
           s.n_live_tup AS estimated_live_rows,
           s.n_dead_tup AS estimated_dead_rows
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
    WHERE n.nspname = 'daily_reporting' AND c.relkind = 'r'
), cutoffs AS (
    SELECT cutoff_at, count(*) AS source_snapshots,
           count(DISTINCT (account_id, marketplace)) AS represented_accounts,
           sum(row_count) AS fact_rows,
           min(source_as_of) AS first_observed_at,
           max(source_as_of) AS last_observed_at,
           bool_and(status = 'succeeded' AND pagination_complete) AS all_present_sources_complete
    FROM daily_reporting.source_snapshots
    GROUP BY cutoff_at
    ORDER BY cutoff_at DESC
    LIMIT 20
)
SELECT json_build_object(
    'measured_at', statement_timestamp(),
    'database_bytes', pg_database_size(current_database()),
    'reporting_bytes', (SELECT coalesce(sum(total_bytes), 0) FROM relations),
    'source_snapshot_count', (SELECT count(*) FROM daily_reporting.source_snapshots),
    'tables', (SELECT json_agg(r ORDER BY r.total_bytes DESC) FROM (
        SELECT relname, table_bytes, index_bytes, total_bytes,
               estimated_live_rows, estimated_dead_rows FROM relations
    ) r),
    'recent_cutoffs', (SELECT json_agg(c ORDER BY c.cutoff_at DESC) FROM cutoffs c)
);
COMMIT;
