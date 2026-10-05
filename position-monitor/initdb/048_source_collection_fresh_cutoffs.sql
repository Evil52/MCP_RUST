BEGIN;

CREATE OR REPLACE FUNCTION daily_reporting.claim_source_collection(scope jsonb, owner text)
RETURNS SETOF daily_reporting.source_collection_jobs
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE t timestamptz := clock_timestamp(); chosen bigint; purge_id bigint;
BEGIN
    IF owner IS NULL OR owner !~ '^[A-Za-z0-9._:-]{1,64}$' OR jsonb_typeof(scope) <> 'array'
       OR jsonb_array_length(scope) NOT BETWEEN 1 AND 64 THEN
        RAISE EXCEPTION 'invalid source collection scope';
    END IF;
    PERFORM pg_advisory_xact_lock(917244, 29);
    t := clock_timestamp();
    UPDATE daily_reporting.source_collection_jobs
    SET status='failed', error_class='collection_expired', finished_at=t, lease_until=NULL
    WHERE status IN ('ready','running') AND (deadline_at<=t OR
        (source IN ('stocks','seller_stocks','prices') AND first_observed_at < t - interval '30 minutes'));
    -- At most four jobs (128 MiB) per quantum. Ineligible old pages cannot be
    -- resumed even while awaiting this opportunistic cleanup.
    FOR purge_id IN
        SELECT j.id FROM daily_reporting.source_collection_jobs j
        WHERE (j.status='published' OR (j.status='failed' AND
            (j.source NOT IN ('stocks','seller_stocks') OR j.finished_at<=t-interval '24 hours')))
          AND EXISTS (SELECT 1 FROM daily_reporting.source_collection_pages p WHERE p.job_id=j.id)
        ORDER BY j.finished_at,j.id LIMIT 4 FOR UPDATE OF j SKIP LOCKED
    LOOP
        DELETE FROM daily_reporting.source_collection_pages WHERE job_id=purge_id;
        UPDATE daily_reporting.source_collection_jobs SET cache_bytes=0 WHERE id=purge_id;
    END LOOP;
    IF EXISTS (SELECT 1 FROM daily_reporting.source_collection_jobs WHERE status='running' AND lease_until>t) THEN RETURN; END IF;
    SELECT j.id INTO chosen FROM daily_reporting.source_collection_jobs j
    JOIN jsonb_to_recordset(scope) AS allowed(account_id text, marketplace text)
      ON j.account_id=allowed.account_id AND j.marketplace=allowed.marketplace
    WHERE j.cutoff_at<=t AND j.next_attempt_at<=t AND j.generation<2147483647
      AND (j.status='ready' OR (j.status='running' AND j.lease_until<=t))
      AND NOT EXISTS (SELECT 1 FROM daily_reporting.collection_claims old
          WHERE old.account_id=j.account_id AND old.marketplace=j.marketplace AND old.cutoff_at=j.cutoff_at
            AND old.status='active' AND old.lease_until>t)
    -- Finish already admitted short-lived observations and work within five
    -- minutes of its hard deadline. Otherwise publish the newest cutoff before
    -- recovering older batches, so a restart cannot starve today's reports.
    -- Backoff, the global lease and page-level marketplace quotas still apply.
    ORDER BY CASE WHEN
        (j.source IN ('stocks','seller_stocks','prices') AND j.first_observed_at IS NOT NULL)
        OR j.deadline_at<=t+interval '5 minutes'
        THEN LEAST(j.deadline_at, CASE
            WHEN j.source IN ('stocks','seller_stocks','prices')
                THEN j.first_observed_at+interval '30 minutes' ELSE NULL END)
        ELSE 'infinity'::timestamptz END,
        j.cutoff_at DESC,
        LEAST(j.deadline_at, CASE WHEN j.source IN ('stocks','seller_stocks','prices')
            THEN j.first_observed_at+interval '30 minutes' ELSE NULL END),
        j.next_attempt_at, CASE j.source WHEN 'stocks' THEN 0 WHEN 'seller_stocks' THEN 0 WHEN 'prices' THEN 1 WHEN 'sales' THEN 2 ELSE 3 END,j.id
    LIMIT 1 FOR UPDATE OF j SKIP LOCKED;
    RETURN QUERY UPDATE daily_reporting.source_collection_jobs SET status='running',generation=generation+1,
        owner_id=owner,lease_until=t + interval '2 minutes'
        WHERE id=chosen RETURNING *;
END;
$$;

COMMIT;
