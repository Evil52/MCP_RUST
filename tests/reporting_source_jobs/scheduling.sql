BEGIN;
SAVEPOINT priority;
DO $$
DECLARE
    t timestamptz := clock_timestamp();
    scope jsonb := '[{"account_id":"deadline_ozon","marketplace":"ozon"},{"account_id":"deadline_wb","marketplace":"wildberries"}]';
    j daily_reporting.source_collection_jobs;
    expected text;
BEGIN
    INSERT INTO daily_reporting.source_collection_jobs
        (account_id,marketplace,source,cutoff_at,period_start,period_end,deadline_at,next_attempt_at,first_observed_at)
    VALUES
        ('deadline_ozon','ozon','stocks',t-interval '1 hour',t-interval '1 day',t-interval '1 hour',t+interval '23 hours',t-interval '1 second',t-interval '28 minutes'),
        ('deadline_ozon','ozon','prices',t-interval '1 hour',t-interval '1 day',t-interval '1 hour',t+interval '23 hours',t-interval '2 seconds',t-interval '27 minutes'),
        ('deadline_wb','wildberries','seller_stocks',t-interval '1 hour',t-interval '1 day',t-interval '1 hour',t+interval '23 hours',t-interval '3 seconds',t-interval '26 minutes'),
        ('deadline_ozon','ozon','sales',t-interval '1 hour',t-interval '1 day',t-interval '1 hour',t+interval '23 hours',t-interval '10 minutes',NULL),
        ('deadline_ozon','ozon','advertising',t-interval '1 hour',t-interval '1 day',t-interval '1 hour',t+interval '23 hours',t-interval '20 minutes',NULL),
        ('deadline_outside','ozon','stocks',t-interval '1 hour',t-interval '1 day',t-interval '1 hour',t+interval '23 hours',t-interval '1 hour',t-interval '29 minutes');
    FOREACH expected IN ARRAY ARRAY['stocks','prices','seller_stocks','advertising','sales'] LOOP
        SELECT * INTO STRICT j FROM daily_reporting.claim_source_collection(scope,'deadline-test');
        ASSERT j.source=expected, format('expected %s ahead of old background work, got %s',expected,j.source);
        ASSERT j.account_id<>'deadline_outside', 'scope must remain fenced';
        ASSERT NOT EXISTS (SELECT FROM daily_reporting.claim_source_collection(scope,'other-owner')), 'live lease must exclude another claim';
        ASSERT daily_reporting.defer_source_collection(j.id,j.generation,'deadline-test',NULL,3600,false);
    END LOOP;
    ASSERT (SELECT first_observed_at=t-interval '28 minutes' FROM daily_reporting.source_collection_jobs WHERE account_id='deadline_ozon' AND source='stocks'), 'priority must not renew the observation clock';
    -- A long-lived source close to its own deadline must not starve behind stock.
    UPDATE daily_reporting.source_collection_jobs SET next_attempt_at=t-interval '1 second'
        WHERE account_id='deadline_ozon' AND source IN ('sales','stocks');
    UPDATE daily_reporting.source_collection_jobs SET deadline_at=t+interval '1 minute'
        WHERE account_id='deadline_ozon' AND source='sales';
    SELECT * INTO STRICT j FROM daily_reporting.claim_source_collection(scope,'deadline-test');
    ASSERT j.source='sales', 'earlier absolute deadline must win';
    ASSERT daily_reporting.defer_source_collection(j.id,j.generation,'deadline-test',NULL,3600,false);
    -- Expired observations are failed, never restarted by the new ordering.
    UPDATE daily_reporting.source_collection_jobs SET first_observed_at=t-interval '31 minutes'
        WHERE account_id='deadline_ozon' AND source='stocks';
    ASSERT NOT EXISTS (SELECT FROM daily_reporting.claim_source_collection(scope,'deadline-test'));
    ASSERT (SELECT status='failed' AND error_class='collection_expired' FROM daily_reporting.source_collection_jobs WHERE account_id='deadline_ozon' AND source='stocks');
END;
$$;
ROLLBACK TO SAVEPOINT priority;

SAVEPOINT eligibility;
DO $$
DECLARE
    t timestamptz := clock_timestamp();
    scope jsonb := '[{"account_id":"deadline_gates","marketplace":"ozon"}]';
    j daily_reporting.source_collection_jobs;
BEGIN
    INSERT INTO daily_reporting.source_collection_jobs
        (account_id,marketplace,source,cutoff_at,period_start,period_end,deadline_at,next_attempt_at,first_observed_at)
    VALUES
        ('deadline_gates','ozon','stocks',t-interval '1 hour',t-interval '1 day',t-interval '1 hour',t+interval '23 hours',t-interval '1 second',t-interval '29 minutes'),
        ('deadline_gates','ozon','prices',t-interval '1 hour',t-interval '1 day',t-interval '1 hour',t+interval '23 hours',t-interval '1 second',NULL),
        ('deadline_gates','ozon','sales',t+interval '1 hour',t-interval '1 day',t,t+interval '2 hours',t-interval '1 hour',NULL);
    INSERT INTO daily_reporting.source_collection_departures VALUES ('deadline_gates','ozon','stocks',t+interval '10 minutes');
    SELECT * INTO STRICT j FROM daily_reporting.claim_source_collection(scope,'deadline-test');
    ASSERT j.source='stocks';
    ASSERT NOT daily_reporting.admit_source_page(j.id,j.generation,'deadline-test'), 'priority must not bypass API quota';
    ASSERT daily_reporting.defer_source_collection(j.id,j.generation,'deadline-test',NULL,1,false);
    ASSERT (SELECT next_attempt_at>=t+interval '10 minutes' FROM daily_reporting.source_collection_jobs WHERE id=j.id);
    SELECT * INTO STRICT j FROM daily_reporting.claim_source_collection(scope,'deadline-test');
    ASSERT j.source='prices', 'quota-blocked urgent work must yield to eligible work';
    -- A crashed worker can be reclaimed but its old generation stays fenced.
    UPDATE daily_reporting.source_collection_jobs SET lease_until=t-interval '1 second' WHERE id=j.id;
    SELECT * INTO STRICT j FROM daily_reporting.claim_source_collection(scope,'replacement');
    ASSERT j.source='prices' AND j.generation=2;
    ASSERT NOT daily_reporting.defer_source_collection(j.id,1,'deadline-test',NULL,1,false);
    ASSERT daily_reporting.defer_source_collection(j.id,j.generation,'replacement',NULL,3600,false);
    ASSERT NOT EXISTS (SELECT FROM daily_reporting.claim_source_collection(scope,'deadline-test')), 'future cutoffs and backoff remain ineligible';
END;
$$;
ROLLBACK TO SAVEPOINT eligibility;

SAVEPOINT recovery_backlog;
DO $$
DECLARE
    t timestamptz := clock_timestamp();
    scope jsonb := '[{"account_id":"recovery_priority","marketplace":"ozon"}]';
    j daily_reporting.source_collection_jobs;
BEGIN
    INSERT INTO daily_reporting.source_collection_jobs
        (account_id,marketplace,source,cutoff_at,period_start,period_end,deadline_at,next_attempt_at)
    VALUES
        ('recovery_priority','ozon','sales',t-interval '12 hours',t-interval '36 hours',t-interval '12 hours',t+interval '12 hours',t-interval '12 hours'),
        ('recovery_priority','ozon','sales',t-interval '1 hour',t-interval '25 hours',t-interval '1 hour',t+interval '23 hours',t-interval '1 hour');
    SELECT * INTO STRICT j FROM daily_reporting.claim_source_collection(scope,'restart-priority');
    ASSERT j.cutoff_at=t-interval '1 hour', 'latest cutoff must outrank unstarted historical work';
    ASSERT daily_reporting.defer_source_collection(j.id,j.generation,'restart-priority',NULL,3600,false);
    INSERT INTO daily_reporting.source_collection_jobs
        (account_id,marketplace,source,cutoff_at,period_start,period_end,deadline_at,next_attempt_at,first_observed_at)
    VALUES
        ('recovery_priority','ozon','stocks',t-interval '12 hours',t-interval '36 hours',t-interval '12 hours',t+interval '12 hours',t-interval '1 second',t-interval '10 minutes');
    SELECT * INTO STRICT j FROM daily_reporting.claim_source_collection(scope,'restart-priority');
    ASSERT j.source='stocks', 'an admitted observation must finish within its original window';
    ASSERT daily_reporting.defer_source_collection(j.id,j.generation,'restart-priority',NULL,3600,false);
    SELECT * INTO STRICT j FROM daily_reporting.claim_source_collection(scope,'restart-priority');
    ASSERT j.source='sales' AND j.cutoff_at=t-interval '12 hours', 'historical work remains recoverable after current work';
END;
$$;
ROLLBACK TO SAVEPOINT recovery_backlog;

-- Reproduce a busy cutoff: 14 accounts, 70 sources, four pages each, 20 seconds
-- of work per page. Advancing only disposable fixture clocks avoids sleeps.
-- FIFO next_attempt_at expires started stock/price jobs on this same workload.
DO $$
DECLARE
    t timestamptz := clock_timestamp();
    scope jsonb;
    j daily_reporting.source_collection_jobs;
    steps integer := 0;
BEGIN
    INSERT INTO daily_reporting.source_collection_jobs
        (account_id,marketplace,source,cutoff_at,period_start,period_end,deadline_at,next_attempt_at)
    SELECT 'deadline_load_'||a, CASE WHEN a<=7 THEN 'ozon' ELSE 'wildberries' END,
        CASE WHEN a>7 AND s='finance' THEN 'seller_stocks' ELSE s END,
        t-interval '1 minute',t-interval '1 day',t-interval '1 minute',t+interval '23 hours',t-interval '1 second'
    FROM generate_series(1,14) a CROSS JOIN unnest(ARRAY['stocks','prices','sales','advertising','finance']) s;
    SELECT jsonb_agg(x) INTO scope FROM (SELECT DISTINCT account_id,marketplace FROM daily_reporting.source_collection_jobs WHERE account_id LIKE 'deadline_load_%') x;
    LOOP
        SELECT * INTO j FROM daily_reporting.claim_source_collection(scope,'deadline-load');
        EXIT WHEN NOT FOUND;
        steps := steps+1;
        ASSERT steps<=280, 'workload must finish within its page budget';
        ASSERT daily_reporting.admit_source_page(j.id,j.generation,'deadline-load');
        PERFORM daily_reporting.save_source_page(j.id,j.generation,'deadline-load',md5(j.generation::text)||md5(j.generation::text),'{}');
        IF j.completed_pages=3 THEN
            -- Simulate publication; fact validation is covered by the caller's
            -- full writer tests. This fixture isolates scheduling throughput.
            UPDATE daily_reporting.source_collection_jobs SET status='published',lease_until=NULL,finished_at=clock_timestamp() WHERE id=j.id;
        ELSE
            ASSERT daily_reporting.defer_source_collection(j.id,j.generation,'deadline-load',NULL,1,false);
        END IF;
        UPDATE daily_reporting.source_collection_jobs SET
            cutoff_at=cutoff_at-interval '20 seconds',deadline_at=deadline_at-interval '20 seconds',
            first_observed_at=first_observed_at-interval '20 seconds',last_observed_at=last_observed_at-interval '20 seconds',
            next_attempt_at=next_attempt_at-interval '20 seconds'
            WHERE account_id LIKE 'deadline_load_%';
        UPDATE daily_reporting.source_collection_departures SET next_allowed_at=next_allowed_at-interval '20 seconds' WHERE account_id LIKE 'deadline_load_%';
    END LOOP;
    ASSERT (SELECT count(*)=70 AND bool_and(status='published') FROM daily_reporting.source_collection_jobs WHERE account_id LIKE 'deadline_load_%'), 'every source must finish, including background work';
    ASSERT steps=280, 'all 280 pages must be processed';
END;
$$;
ROLLBACK;
