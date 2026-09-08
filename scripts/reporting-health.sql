-- $1 is the validated JSON account/marketplace scope (text). $2 is NULL in
-- production; the injected timestamp supports deterministic boundary tests.
-- This SELECT is also executed under default_transaction_read_only=on.
WITH clock AS (
    SELECT COALESCE($2::timestamptz, statement_timestamp()) AS checked_at
), expected AS (
    SELECT max(cutoff) AS cutoff_at, clock.checked_at
    FROM clock
    CROSS JOIN LATERAL (
        SELECT ((clock.checked_at AT TIME ZONE 'Asia/Yekaterinburg')::date
                - day_offset + cutoff_time) AT TIME ZONE 'Asia/Yekaterinburg' AS cutoff
        FROM generate_series(0, 1) AS days(day_offset)
        CROSS JOIN (VALUES (time '08:00'), (time '17:00')) AS cutoffs(cutoff_time)
    ) AS candidate
    WHERE cutoff + interval '30 minutes' < clock.checked_at
    GROUP BY clock.checked_at
), accounts AS (
    SELECT account_id, marketplace
    FROM jsonb_to_recordset($1::text::jsonb) AS scope(account_id text, marketplace text)
), required AS (
    SELECT account.account_id, account.marketplace, source
    FROM accounts AS account
    CROSS JOIN unnest(ARRAY['sales', 'advertising', 'stocks', 'prices', 'finance']) AS source
    WHERE source <> 'finance' OR account.marketplace = 'ozon'
), coverage AS (
    SELECT required.account_id, required.marketplace, expected.cutoff_at,
           string_agg(required.source, ',' ORDER BY required.source) FILTER (
               WHERE snapshot.status IS DISTINCT FROM 'succeeded'
                  OR NOT COALESCE(snapshot.pagination_complete, false)
           ) AS missing_sources
    FROM required CROSS JOIN expected
    LEFT JOIN daily_reporting.source_snapshots AS snapshot
      ON snapshot.account_id = required.account_id
     AND snapshot.marketplace = required.marketplace
     AND snapshot.source = required.source
     AND snapshot.cutoff_at = expected.cutoff_at
    GROUP BY required.account_id, required.marketplace, expected.cutoff_at
), latest_refresh AS (
    SELECT account.account_id, account.marketplace, refresh.*
    FROM accounts AS account
    CROSS JOIN LATERAL (
        SELECT request.id, request.status, request.requested_at, request.business_date,
               request.lease_until, request.error_class
        FROM daily_reporting.ozon_sales_refresh_requests AS request
        WHERE request.account_id = account.account_id
          AND request.marketplace = account.marketplace
        ORDER BY request.requested_at DESC, request.id DESC
        LIMIT 1
    ) AS refresh
), findings AS (
    SELECT coverage.account_id, coverage.marketplace,
           'cutoff_incomplete'::text AS finding,
           to_char(coverage.cutoff_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
               || '|missing=' || coverage.missing_sources AS detail
    FROM coverage
    WHERE coverage.missing_sources IS NOT NULL
    UNION ALL
    SELECT refresh.account_id, refresh.marketplace, 'refresh_failed',
           refresh.id::text || '|' || COALESCE(refresh.error_class, 'unknown')
    FROM latest_refresh AS refresh CROSS JOIN expected
    WHERE refresh.status = 'failed' AND refresh.requested_at >= expected.cutoff_at
    UNION ALL
    SELECT refresh.account_id, refresh.marketplace, 'refresh_expired', refresh.id::text
    FROM latest_refresh AS refresh CROSS JOIN expected
    WHERE (refresh.status = 'running' AND refresh.lease_until <= expected.checked_at)
       OR (refresh.status = 'queued' AND (
           refresh.requested_at < expected.checked_at - interval '4 hours'
           OR refresh.business_date <> (expected.checked_at AT TIME ZONE 'Asia/Yekaterinburg')::date
       ))
    UNION ALL
    SELECT job.account_id,job.marketplace,'source_collection_failed',
           job.source || '|' || COALESCE(job.error_class,'unknown')
    FROM daily_reporting.source_collection_jobs job
    JOIN accounts USING(account_id,marketplace) CROSS JOIN expected
    WHERE job.status='failed' AND job.cutoff_at>=expected.cutoff_at
    UNION ALL
    SELECT job.account_id,job.marketplace,'source_collection_stalled',job.source
    FROM daily_reporting.source_collection_jobs job
    JOIN accounts USING(account_id,marketplace) CROSS JOIN expected
    WHERE job.cutoff_at>=expected.cutoff_at AND (
        (job.status='running' AND job.lease_until<expected.checked_at-interval '2 minutes') OR
        (job.status='ready' AND job.next_attempt_at<expected.checked_at-interval '15 minutes'))
    UNION ALL
    SELECT claim.account_id, claim.marketplace, 'collection_lease_expired', claim.id::text
    FROM daily_reporting.collection_claims AS claim
    JOIN accounts USING (account_id, marketplace)
    CROSS JOIN expected
    WHERE claim.status = 'active' AND claim.lease_until <= expected.checked_at
      AND claim.cutoff_at >= expected.cutoff_at
)
SELECT 'reporting|' || account_id || '|' || marketplace || '|' || finding || '|' || detail
FROM findings
ORDER BY account_id, marketplace, finding, detail
