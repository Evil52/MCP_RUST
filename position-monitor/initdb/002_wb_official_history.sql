BEGIN;

-- This migration deliberately does not reuse measurements. The official WB
-- Search Analytics sources refresh approximately once per hour. Product
-- orders are stored as daily rows; search texts remain a bounded requested-
-- period aggregate. Neither source is a live
-- regional rank nor an organic/sponsored split.

CREATE TABLE search_position.wb_search_targets (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id text NOT NULL CHECK (
        char_length(account_id) BETWEEN 1 AND 128
        AND account_id = btrim(account_id)
        AND account_id !~ '[[:cntrl:]]'
    ),
    store_id text NOT NULL CHECK (
        char_length(store_id) BETWEEN 1 AND 128
        AND store_id = btrim(store_id)
        AND store_id !~ '[[:cntrl:]]'
    ),
    nm_id bigint NOT NULL CHECK (nm_id > 0),
    search_phrase text NOT NULL CHECK (
        char_length(search_phrase) BETWEEN 1 AND 256
        AND search_phrase = btrim(search_phrase)
        AND search_phrase !~ '[[:cntrl:]]'
    ),
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (account_id, store_id, nm_id, search_phrase),
    UNIQUE (id, account_id, store_id, nm_id, search_phrase)
);

CREATE INDEX wb_search_targets_active_account
    ON search_position.wb_search_targets (active, account_id, store_id, nm_id);

CREATE TABLE search_position.wb_bid_targets (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id text NOT NULL CHECK (
        char_length(account_id) BETWEEN 1 AND 128
        AND account_id = btrim(account_id)
        AND account_id !~ '[[:cntrl:]]'
    ),
    store_id text NOT NULL CHECK (
        char_length(store_id) BETWEEN 1 AND 128
        AND store_id = btrim(store_id)
        AND store_id !~ '[[:cntrl:]]'
    ),
    source text NOT NULL CHECK (
        source IN (
            'promotion_cluster_bids',
            'promotion_minimum_bids',
            'promotion_bid_recommendations'
        )
    ),
    campaign_id bigint NOT NULL CHECK (campaign_id > 0),
    nm_id bigint NOT NULL CHECK (nm_id > 0),
    payment_type text CHECK (
        payment_type IS NULL OR payment_type IN ('cpm', 'cpc')
    ),
    placement text CHECK (
        placement IS NULL
        OR placement IN ('combined', 'search', 'recommendation')
    ),
    payment_type_key text GENERATED ALWAYS AS (
        coalesce(payment_type, 'unspecified')
    ) STORED,
    placement_key text GENERATED ALWAYS AS (
        coalesce(placement, 'unspecified')
    ) STORED,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (
        (
            source = 'promotion_cluster_bids'
            AND payment_type IS NULL
            AND placement IS NULL
        )
        OR
        (
            source = 'promotion_minimum_bids'
            AND payment_type IN ('cpm', 'cpc')
            AND placement IN ('combined', 'search', 'recommendation')
        )
        OR
        (
            source = 'promotion_bid_recommendations'
            AND payment_type = 'cpm'
            AND placement IS NULL
        )
    ),
    UNIQUE (
        account_id,
        store_id,
        source,
        campaign_id,
        nm_id,
        payment_type_key,
        placement_key
    ),
    UNIQUE (
        id,
        account_id,
        store_id,
        source,
        campaign_id,
        nm_id
    ),
    UNIQUE (
        id,
        account_id,
        store_id,
        source,
        campaign_id,
        nm_id,
        payment_type_key,
        placement_key
    )
);

CREATE INDEX wb_bid_targets_active_account
    ON search_position.wb_bid_targets (
        active,
        account_id,
        store_id,
        source,
        campaign_id,
        nm_id
    );

-- A target may be paused/resumed, but changing its identity would silently
-- relabel all historical snapshots. Keep every identity field and created_at
-- immutable, and let the database own updated_at.
CREATE FUNCTION search_position.enforce_wb_search_target_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF ROW(
        NEW.id,
        NEW.account_id,
        NEW.store_id,
        NEW.nm_id,
        NEW.search_phrase,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.account_id,
        OLD.store_id,
        OLD.nm_id,
        OLD.search_phrase,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'WB search target identity is immutable';
    END IF;
    NEW.updated_at = statement_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER wb_search_targets_enforce_update
    BEFORE UPDATE ON search_position.wb_search_targets
    FOR EACH ROW
    EXECUTE FUNCTION search_position.enforce_wb_search_target_update();

CREATE FUNCTION search_position.enforce_wb_bid_target_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF ROW(
        NEW.id,
        NEW.account_id,
        NEW.store_id,
        NEW.source,
        NEW.campaign_id,
        NEW.nm_id,
        NEW.payment_type,
        NEW.placement,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.account_id,
        OLD.store_id,
        OLD.source,
        OLD.campaign_id,
        OLD.nm_id,
        OLD.payment_type,
        OLD.placement,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'WB bid target identity is immutable';
    END IF;
    NEW.updated_at = statement_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER wb_bid_targets_enforce_update
    BEFORE UPDATE ON search_position.wb_bid_targets
    FOR EACH ROW
    EXECUTE FUNCTION search_position.enforce_wb_bid_target_update();

CREATE TABLE search_position.wb_collection_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id text NOT NULL CHECK (
        char_length(account_id) BETWEEN 1 AND 128
        AND account_id = btrim(account_id)
        AND account_id !~ '[[:cntrl:]]'
    ),
    store_id text NOT NULL CHECK (
        char_length(store_id) BETWEEN 1 AND 128
        AND store_id = btrim(store_id)
        AND store_id !~ '[[:cntrl:]]'
    ),
    source text NOT NULL CHECK (
        source IN (
            'search_product_orders',
            'search_product_texts',
            'promotion_cluster_bids',
            'promotion_minimum_bids',
            'promotion_bid_recommendations'
        )
    ),
    source_host text NOT NULL,
    source_method text NOT NULL,
    source_path text NOT NULL,
    scheduled_for timestamptz NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    source_updated_at timestamptz,
    status text NOT NULL CHECK (
        status IN ('running', 'succeeded', 'partial', 'failed', 'blocked')
    ),
    targets_attempted integer NOT NULL DEFAULT 0
        CHECK (targets_attempted >= 0),
    targets_succeeded integer NOT NULL DEFAULT 0
        CHECK (targets_succeeded >= 0),
    error_class text CHECK (
        error_class IS NULL OR error_class IN (
            'authentication',
            'authorization',
            'entitlement',
            'rate_limited',
            'timeout',
            'transport',
            'upstream',
            'invalid_response',
            'response_too_large',
            'database'
        )
    ),
    http_status smallint
        CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    collector_version text NOT NULL CHECK (
        char_length(collector_version) BETWEEN 1 AND 64
        AND collector_version = btrim(collector_version)
        AND collector_version !~ '[[:cntrl:]]'
    ),
    CHECK (scheduled_for = date_trunc('hour', scheduled_for, 'UTC')),
    CHECK (finished_at IS NULL OR finished_at >= started_at),
    CHECK (source_updated_at IS NULL OR source_updated_at <= finished_at),
    CHECK (targets_succeeded <= targets_attempted),
    CHECK (
        (status = 'running' AND finished_at IS NULL)
        OR (status <> 'running' AND finished_at IS NOT NULL)
    ),
    CHECK (
        (
            source IN ('search_product_orders', 'search_product_texts')
            AND source_host = 'seller-analytics-api.wildberries.ru'
            AND source_method = 'POST'
            AND source_path = CASE source
                WHEN 'search_product_orders'
                    THEN '/api/v2/search-report/product/orders'
                WHEN 'search_product_texts'
                    THEN '/api/v2/search-report/product/search-texts'
            END
        )
        OR
        (
            source IN (
                'promotion_cluster_bids',
                'promotion_minimum_bids',
                'promotion_bid_recommendations'
            )
            AND source_host = 'advert-api.wildberries.ru'
            AND source_method = CASE source
                WHEN 'promotion_bid_recommendations' THEN 'GET'
                ELSE 'POST'
            END
            AND source_path = CASE source
                WHEN 'promotion_cluster_bids'
                    THEN '/adv/v0/normquery/get-bids'
                WHEN 'promotion_minimum_bids'
                    THEN '/api/advert/v1/bids/min'
                WHEN 'promotion_bid_recommendations'
                    THEN '/api/advert/v0/bids/recommendations'
            END
        )
    ),
    UNIQUE (account_id, store_id, source, scheduled_for),
    UNIQUE (id, account_id, store_id, source)
);

CREATE INDEX wb_collection_runs_scheduled
    ON search_position.wb_collection_runs (
        account_id,
        store_id,
        scheduled_for DESC,
        id DESC
    );

-- Collection is a one-way state machine. Provenance never changes, every run
-- starts clean in running, counters cannot decrease, and a terminal result is
-- frozen. Snapshot triggers below lock this row while appending facts, making
-- "append facts, then finalize" an atomic publish boundary.
CREATE FUNCTION search_position.enforce_wb_collection_run_state()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'running'
            OR NEW.finished_at IS NOT NULL
            OR NEW.source_updated_at IS NOT NULL
            OR NEW.targets_attempted <> 0
            OR NEW.targets_succeeded <> 0
            OR NEW.error_class IS NOT NULL
            OR NEW.http_status IS NOT NULL
        THEN
            RAISE EXCEPTION USING
                ERRCODE = 'integrity_constraint_violation',
                MESSAGE = 'WB collection run must start clean in running';
        END IF;
        RETURN NEW;
    END IF;

    IF ROW(
        NEW.id,
        NEW.account_id,
        NEW.store_id,
        NEW.source,
        NEW.source_host,
        NEW.source_method,
        NEW.source_path,
        NEW.scheduled_for,
        NEW.started_at,
        NEW.collector_version
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.account_id,
        OLD.store_id,
        OLD.source,
        OLD.source_host,
        OLD.source_method,
        OLD.source_path,
        OLD.scheduled_for,
        OLD.started_at,
        OLD.collector_version
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'WB collection run provenance is immutable';
    END IF;

    IF OLD.status <> 'running' THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'terminal WB collection run is immutable';
    END IF;

    IF NEW.targets_attempted < OLD.targets_attempted
        OR NEW.targets_succeeded < OLD.targets_succeeded
    THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'WB collection run counters cannot decrease';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER wb_collection_runs_enforce_state
    BEFORE INSERT OR UPDATE ON search_position.wb_collection_runs
    FOR EACH ROW
    EXECUTE FUNCTION search_position.enforce_wb_collection_run_state();

CREATE FUNCTION search_position.require_running_wb_snapshot_run()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    run_status text;
BEGIN
    SELECT status
    INTO run_status
    FROM search_position.wb_collection_runs
    WHERE id = NEW.run_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'foreign_key_violation',
            MESSAGE = 'WB snapshot references an unknown collection run';
    END IF;

    IF run_status <> 'running' THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'WB snapshots can only be appended to a running run';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TABLE search_position.wb_search_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id bigint NOT NULL,
    target_id bigint NOT NULL,
    source text NOT NULL CHECK (
        source IN ('search_product_orders', 'search_product_texts')
    ),
    account_id text NOT NULL CHECK (
        char_length(account_id) BETWEEN 1 AND 128
        AND account_id = btrim(account_id)
        AND account_id !~ '[[:cntrl:]]'
    ),
    store_id text NOT NULL CHECK (
        char_length(store_id) BETWEEN 1 AND 128
        AND store_id = btrim(store_id)
        AND store_id !~ '[[:cntrl:]]'
    ),
    nm_id bigint NOT NULL CHECK (nm_id > 0),
    search_phrase text NOT NULL CHECK (
        char_length(search_phrase) BETWEEN 1 AND 256
        AND search_phrase = btrim(search_phrase)
        AND search_phrase !~ '[[:cntrl:]]'
    ),
    period_start date NOT NULL,
    period_end date NOT NULL,
    observed_at timestamptz NOT NULL,
    source_updated_at timestamptz,
    data_granularity text NOT NULL
        CHECK (data_granularity IN ('daily', 'period_aggregate')),
    average_position numeric(12, 4)
        CHECK (average_position IS NULL OR average_position > 0),
    median_position numeric(12, 4)
        CHECK (median_position IS NULL OR median_position > 0),
    orders bigint CHECK (orders IS NULL OR orders >= 0),
    frequency bigint CHECK (frequency IS NULL OR frequency >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (period_end >= period_start),
    CHECK (
        (
            source = 'search_product_orders'
            AND data_granularity = 'daily'
            AND period_start = period_end
            AND median_position IS NULL
            AND frequency IS NULL
        )
        OR
        (
            source = 'search_product_texts'
            AND data_granularity = 'period_aggregate'
            AND period_end - period_start <= 30
        )
    ),
    CHECK (source_updated_at IS NULL OR source_updated_at <= observed_at),
    CHECK (
        average_position IS NOT NULL
        OR median_position IS NOT NULL
        OR orders IS NOT NULL
        OR frequency IS NOT NULL
    ),
    FOREIGN KEY (run_id, account_id, store_id, source)
        REFERENCES search_position.wb_collection_runs (
            id,
            account_id,
            store_id,
            source
        ) ON DELETE RESTRICT,
    FOREIGN KEY (target_id, account_id, store_id, nm_id, search_phrase)
        REFERENCES search_position.wb_search_targets (
            id,
            account_id,
            store_id,
            nm_id,
            search_phrase
        ) ON DELETE RESTRICT,
    UNIQUE (run_id, target_id, period_start, period_end)
);

CREATE TRIGGER wb_search_snapshots_require_running_run
    BEFORE INSERT ON search_position.wb_search_snapshots
    FOR EACH ROW
    EXECUTE FUNCTION search_position.require_running_wb_snapshot_run();

CREATE INDEX wb_search_snapshots_target_time
    ON search_position.wb_search_snapshots (
        target_id,
        observed_at DESC,
        id DESC
    );

CREATE INDEX wb_search_snapshots_account_time
    ON search_position.wb_search_snapshots (
        account_id,
        observed_at DESC,
        id DESC
    );

CREATE TABLE search_position.wb_bid_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id bigint NOT NULL,
    target_id bigint NOT NULL,
    source text NOT NULL CHECK (
        source IN (
            'promotion_cluster_bids',
            'promotion_minimum_bids',
            'promotion_bid_recommendations'
        )
    ),
    account_id text NOT NULL CHECK (
        char_length(account_id) BETWEEN 1 AND 128
        AND account_id = btrim(account_id)
        AND account_id !~ '[[:cntrl:]]'
    ),
    store_id text NOT NULL CHECK (
        char_length(store_id) BETWEEN 1 AND 128
        AND store_id = btrim(store_id)
        AND store_id !~ '[[:cntrl:]]'
    ),
    campaign_id bigint NOT NULL CHECK (campaign_id > 0),
    nm_id bigint NOT NULL CHECK (nm_id > 0),
    payment_type text CHECK (
        payment_type IS NULL OR payment_type IN ('cpm', 'cpc')
    ),
    placement text CHECK (
        placement IS NULL
        OR placement IN ('combined', 'search', 'recommendation')
    ),
    payment_type_key text GENERATED ALWAYS AS (
        coalesce(payment_type, 'unspecified')
    ) STORED,
    placement_key text GENERATED ALWAYS AS (
        coalesce(placement, 'unspecified')
    ) STORED,
    scope text NOT NULL CHECK (scope IN ('product', 'search_cluster')),
    query_phrase text NOT NULL DEFAULT '' CHECK (
        char_length(query_phrase) <= 256
        AND query_phrase = btrim(query_phrase)
        AND query_phrase !~ '[[:cntrl:]]'
        AND (
            (scope = 'product' AND query_phrase = '')
            OR (scope = 'search_cluster' AND char_length(query_phrase) >= 1)
        )
    ),
    bid_kind text NOT NULL CHECK (
        bid_kind IN (
            'current',
            'minimum',
            'competitive',
            'leaders',
            'top2',
            'reach_max',
            'reach_max_minimum',
            'reach_medium',
            'reach_medium_minimum',
            'reach_min',
            'reach_min_minimum'
        )
    ),
    bid_kopecks bigint NOT NULL CHECK (bid_kopecks >= 0),
    observed_at timestamptz NOT NULL,
    source_updated_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (source_updated_at IS NULL OR source_updated_at <= observed_at),
    CHECK (
        (
            source = 'promotion_cluster_bids'
            AND payment_type IS NULL
            AND placement IS NULL
            AND scope = 'search_cluster'
            AND bid_kind = 'current'
        )
        OR
        (
            source = 'promotion_minimum_bids'
            AND payment_type IN ('cpm', 'cpc')
            AND placement IN ('combined', 'search', 'recommendation')
            AND scope = 'product'
            AND bid_kind = 'minimum'
        )
        OR
        (
            source = 'promotion_bid_recommendations'
            AND payment_type = 'cpm'
            AND placement IS NULL
            AND (
                (
                    scope = 'product'
                    AND bid_kind IN ('competitive', 'leaders', 'top2')
                )
                OR
                (
                    scope = 'search_cluster'
                    AND bid_kind IN (
                        'reach_max',
                        'reach_max_minimum',
                        'reach_medium',
                        'reach_medium_minimum',
                        'reach_min',
                        'reach_min_minimum'
                    )
                )
            )
        )
    ),
    FOREIGN KEY (run_id, account_id, store_id, source)
        REFERENCES search_position.wb_collection_runs (
            id,
            account_id,
            store_id,
            source
        ) ON DELETE RESTRICT,
    FOREIGN KEY (
        target_id,
        account_id,
        store_id,
        source,
        campaign_id,
        nm_id,
        payment_type_key,
        placement_key
    ) REFERENCES search_position.wb_bid_targets (
        id,
        account_id,
        store_id,
        source,
        campaign_id,
        nm_id,
        payment_type_key,
        placement_key
    ) ON DELETE RESTRICT,
    UNIQUE (run_id, target_id, scope, query_phrase, bid_kind)
);

CREATE TRIGGER wb_bid_snapshots_require_running_run
    BEFORE INSERT ON search_position.wb_bid_snapshots
    FOR EACH ROW
    EXECUTE FUNCTION search_position.require_running_wb_snapshot_run();

CREATE INDEX wb_bid_snapshots_target_time
    ON search_position.wb_bid_snapshots (
        target_id,
        observed_at DESC,
        id DESC
    );

CREATE INDEX wb_bid_snapshots_account_time
    ON search_position.wb_bid_snapshots (
        account_id,
        store_id,
        observed_at DESC,
        id DESC
    );

CREATE VIEW search_position.latest_wb_search_snapshots AS
SELECT DISTINCT ON (
    snapshot.target_id,
    snapshot.source,
    snapshot.period_start,
    snapshot.period_end
)
    snapshot.id,
    snapshot.run_id,
    snapshot.target_id,
    snapshot.source,
    snapshot.account_id,
    snapshot.store_id,
    snapshot.nm_id,
    snapshot.search_phrase,
    snapshot.period_start,
    snapshot.period_end,
    snapshot.observed_at,
    snapshot.source_updated_at,
    snapshot.data_granularity,
    snapshot.average_position,
    snapshot.median_position,
    snapshot.orders,
    snapshot.frequency,
    false AS is_live_position,
    NULL::text AS region,
    false AS placement_split_available,
    run.source_host,
    run.source_method,
    run.source_path,
    run.scheduled_for,
    run.status AS run_status,
    (run.status = 'partial') AS is_partial,
    run.targets_attempted,
    run.targets_succeeded,
    run.error_class AS run_error_class,
    run.http_status AS run_http_status,
    run.finished_at AS run_finished_at
FROM search_position.wb_search_snapshots AS snapshot
JOIN search_position.wb_collection_runs AS run
    ON run.id = snapshot.run_id
    AND run.account_id = snapshot.account_id
    AND run.store_id = snapshot.store_id
    AND run.source = snapshot.source
WHERE run.status IN ('succeeded', 'partial')
ORDER BY
    snapshot.target_id,
    snapshot.source,
    snapshot.period_start,
    snapshot.period_end,
    snapshot.observed_at DESC,
    snapshot.id DESC;

CREATE VIEW search_position.latest_wb_bid_snapshots AS
SELECT DISTINCT ON (
    snapshot.target_id,
    snapshot.source,
    snapshot.scope,
    snapshot.query_phrase,
    snapshot.bid_kind
)
    snapshot.id,
    snapshot.run_id,
    snapshot.target_id,
    snapshot.source,
    snapshot.account_id,
    snapshot.store_id,
    snapshot.campaign_id,
    snapshot.nm_id,
    snapshot.payment_type,
    snapshot.placement,
    snapshot.scope,
    snapshot.query_phrase,
    snapshot.bid_kind,
    snapshot.bid_kopecks,
    snapshot.observed_at,
    snapshot.source_updated_at,
    run.source_host,
    run.source_method,
    run.source_path,
    run.scheduled_for,
    run.status AS run_status,
    (run.status = 'partial') AS is_partial,
    run.targets_attempted,
    run.targets_succeeded,
    run.error_class AS run_error_class,
    run.http_status AS run_http_status,
    run.finished_at AS run_finished_at
FROM search_position.wb_bid_snapshots AS snapshot
JOIN search_position.wb_collection_runs AS run
    ON run.id = snapshot.run_id
    AND run.account_id = snapshot.account_id
    AND run.store_id = snapshot.store_id
    AND run.source = snapshot.source
WHERE run.status IN ('succeeded', 'partial')
ORDER BY
    snapshot.target_id,
    snapshot.source,
    snapshot.scope,
    snapshot.query_phrase,
    snapshot.bid_kind,
    snapshot.observed_at DESC,
    snapshot.id DESC;

REVOKE ALL ON TABLE search_position.wb_search_targets FROM PUBLIC;
REVOKE ALL ON TABLE search_position.wb_bid_targets FROM PUBLIC;
REVOKE ALL ON TABLE search_position.wb_collection_runs FROM PUBLIC;
REVOKE ALL ON TABLE search_position.wb_search_snapshots FROM PUBLIC;
REVOKE ALL ON TABLE search_position.wb_bid_snapshots FROM PUBLIC;
REVOKE ALL ON TABLE search_position.latest_wb_search_snapshots FROM PUBLIC;
REVOKE ALL ON TABLE search_position.latest_wb_bid_snapshots FROM PUBLIC;
REVOKE ALL ON FUNCTION search_position.enforce_wb_search_target_update()
    FROM PUBLIC;
REVOKE ALL ON FUNCTION search_position.enforce_wb_bid_target_update()
    FROM PUBLIC;
REVOKE ALL ON FUNCTION search_position.enforce_wb_collection_run_state()
    FROM PUBLIC;
REVOKE ALL ON FUNCTION search_position.require_running_wb_snapshot_run()
    FROM PUBLIC;
REVOKE ALL ON SEQUENCE search_position.wb_search_targets_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE search_position.wb_bid_targets_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE search_position.wb_collection_runs_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE search_position.wb_search_snapshots_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE search_position.wb_bid_snapshots_id_seq FROM PUBLIC;

-- Fresh databases apply this migration before restricted roles are created.
-- Existing volumes already have the roles, so applying this same migration
-- must grant the new objects without requiring a second, divergent SQL file.
DO $grant_existing_roles$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'position_collector') THEN
        EXECUTE 'GRANT SELECT ON search_position.wb_search_targets TO position_collector';
        EXECUTE 'GRANT SELECT ON search_position.wb_bid_targets TO position_collector';
        EXECUTE 'GRANT SELECT, INSERT ON search_position.wb_collection_runs TO position_collector';
        EXECUTE 'GRANT UPDATE (finished_at, source_updated_at, status, targets_attempted, targets_succeeded, error_class, http_status) ON search_position.wb_collection_runs TO position_collector';
        EXECUTE 'GRANT INSERT ON search_position.wb_search_snapshots TO position_collector';
        EXECUTE 'GRANT INSERT ON search_position.wb_bid_snapshots TO position_collector';
        EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE search_position.wb_collection_runs_id_seq TO position_collector';
        EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE search_position.wb_search_snapshots_id_seq TO position_collector';
        EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE search_position.wb_bid_snapshots_id_seq TO position_collector';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'position_reader') THEN
        -- Older installations granted reader SELECT through the schema
        -- owner's default ACL. Revoke it here as well as on current raw WB
        -- tables so future migrations cannot bypass the published views.
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA search_position REVOKE SELECT ON TABLES FROM position_reader';
        EXECUTE 'REVOKE ALL ON search_position.wb_collection_runs FROM position_reader';
        EXECUTE 'REVOKE ALL ON search_position.wb_search_snapshots FROM position_reader';
        EXECUTE 'REVOKE ALL ON search_position.wb_bid_snapshots FROM position_reader';
        EXECUTE 'GRANT SELECT ON search_position.wb_search_targets TO position_reader';
        EXECUTE 'GRANT SELECT ON search_position.wb_bid_targets TO position_reader';
        EXECUTE 'GRANT SELECT ON search_position.latest_wb_search_snapshots TO position_reader';
        EXECUTE 'GRANT SELECT ON search_position.latest_wb_bid_snapshots TO position_reader';
    END IF;
END;
$grant_existing_roles$;

COMMIT;
