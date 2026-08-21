BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'position_reader') THEN
        RAISE EXCEPTION 'position_reader role must be created before the MCP read-view migration';
    END IF;
END;
$$;

-- The MCP process receives only curated, explicitly projected data.  In
-- particular, snapshot payload digests, collection-claim internals and report
-- delivery credentials/artifact identities never cross this boundary.
CREATE OR REPLACE VIEW daily_reporting.mcp_collection_status
WITH (security_barrier = true) AS
WITH latest_attempt AS (
    SELECT snapshot.*,
           row_number() OVER (
               PARTITION BY snapshot.account_id, snapshot.marketplace, snapshot.source
               ORDER BY snapshot.cutoff_at DESC, snapshot.id DESC
           ) AS attempt_rank
    FROM daily_reporting.source_snapshots AS snapshot
),
latest_published AS (
    SELECT snapshot.*,
           row_number() OVER (
               PARTITION BY snapshot.account_id, snapshot.marketplace, snapshot.source
               ORDER BY snapshot.cutoff_at DESC, snapshot.id DESC
           ) AS published_rank
    FROM daily_reporting.source_snapshots AS snapshot
    WHERE snapshot.status IN ('succeeded', 'partial')
)
SELECT attempt.id AS snapshot_id,
       attempt.account_id,
       attempt.marketplace,
       attempt.source,
       attempt.cutoff_at,
       attempt.source_as_of,
       attempt.period_start,
       attempt.period_end,
       attempt.status,
       attempt.pagination_complete,
       attempt.row_count,
       attempt.collector_version,
       attempt.started_at,
       attempt.finished_at,
       attempt.error_class,
       attempt.http_status,
       published.cutoff_at AS last_published_cutoff_at,
       published.source_as_of AS last_published_source_as_of,
       published.status AS last_published_status,
       published.row_count AS last_published_row_count
FROM latest_attempt AS attempt
LEFT JOIN latest_published AS published
    ON published.account_id = attempt.account_id
   AND published.marketplace = attempt.marketplace
   AND published.source = attempt.source
   AND published.published_rank = 1
WHERE attempt.attempt_rank = 1;

CREATE OR REPLACE VIEW daily_reporting.mcp_published_source_snapshots
WITH (security_barrier = true) AS
SELECT snapshot.id AS snapshot_id,
       snapshot.account_id,
       snapshot.marketplace,
       snapshot.source,
       snapshot.cutoff_at,
       snapshot.source_as_of,
       snapshot.period_start,
       snapshot.period_end,
       snapshot.status,
       snapshot.pagination_complete,
       snapshot.row_count,
       snapshot.collector_version,
       snapshot.finished_at
FROM daily_reporting.published_source_snapshots AS snapshot;

CREATE OR REPLACE VIEW daily_reporting.mcp_sales_facts
WITH (security_barrier = true) AS
SELECT fact.account_id,
       fact.marketplace,
       fact.cutoff_at,
       fact.source_as_of,
       fact.snapshot_status,
       fact.pagination_complete,
       fact.snapshot_id,
       fact.source,
       fact.business_date,
       fact.sku,
       fact.ordered_units,
       fact.operational_gmv_minor,
       fact.cancelled_units,
       fact.returned_units,
       fact.currency
FROM daily_reporting.published_sales_facts AS fact;

CREATE OR REPLACE VIEW daily_reporting.mcp_advertising_facts
WITH (security_barrier = true) AS
SELECT fact.account_id,
       fact.marketplace,
       fact.cutoff_at,
       fact.source_as_of,
       fact.snapshot_status,
       fact.pagination_complete,
       fact.snapshot_id,
       fact.source,
       fact.business_date,
       fact.campaign_id,
       fact.sku,
       fact.impressions,
       fact.clicks,
       fact.spend_minor,
       fact.attributed_orders,
       fact.attributed_revenue_minor,
       fact.currency,
       fact.basket_additions,
       fact.model_attributed_orders,
       fact.model_attributed_revenue_minor,
       fact.product_price_minor,
       fact.average_cpc_minor,
       fact.cpm_minor,
       fact.cpl_minor
FROM daily_reporting.published_advertising_facts AS fact;

CREATE OR REPLACE VIEW daily_reporting.mcp_advertising_expense_facts
WITH (security_barrier = true) AS
SELECT fact.account_id,
       fact.marketplace,
       fact.cutoff_at,
       fact.source_as_of,
       fact.snapshot_status,
       fact.pagination_complete,
       fact.snapshot_id,
       fact.source,
       fact.business_date,
       fact.campaign_id,
       fact.money_spent_minor,
       fact.bonus_spent_minor,
       fact.prepayment_spent_minor,
       fact.currency
FROM daily_reporting.published_advertising_expense_facts AS fact;

CREATE OR REPLACE VIEW daily_reporting.mcp_finance_facts
WITH (security_barrier = true) AS
SELECT fact.account_id,
       fact.marketplace,
       fact.cutoff_at,
       fact.source_as_of,
       fact.snapshot_status,
       fact.pagination_complete,
       fact.snapshot_id,
       fact.source,
       fact.business_date,
       fact.sku,
       fact.sku_key,
       fact.category,
       fact.amount_minor,
       fact.line_count,
       fact.unknown_type_count
FROM daily_reporting.published_finance_facts AS fact;

CREATE OR REPLACE VIEW daily_reporting.mcp_stock_facts
WITH (security_barrier = true) AS
SELECT fact.account_id,
       fact.marketplace,
       fact.cutoff_at,
       fact.source_as_of,
       fact.snapshot_status,
       fact.pagination_complete,
       fact.snapshot_id,
       fact.source,
       fact.sku,
       fact.warehouse_id,
       fact.sellable_units
FROM daily_reporting.published_stock_facts AS fact;

CREATE OR REPLACE VIEW daily_reporting.mcp_price_facts
WITH (security_barrier = true) AS
SELECT fact.account_id,
       fact.marketplace,
       fact.cutoff_at,
       fact.source_as_of,
       fact.snapshot_status,
       fact.pagination_complete,
       fact.snapshot_id,
       fact.source,
       fact.sku,
       fact.price_minor,
       fact.old_price_minor,
       fact.currency
FROM daily_reporting.published_price_facts AS fact;

-- Application RBAC limits this catalog to administrators.  The database view
-- deliberately contains routing-neutral metadata only: no recipient email,
-- provider identifier, object path, digest, retry state or error detail.
CREATE OR REPLACE VIEW daily_reporting.mcp_ready_reports
WITH (security_barrier = true) AS
SELECT batch.id AS batch_id,
       batch.recipient_id,
       batch.report_version,
       min(coverage.local_date) AS local_date,
       CASE
           WHEN bool_or(coverage.report_kind = 'evening') THEN 'evening'
           ELSE 'morning'
       END AS report_kind,
       batch.scheduled_for,
       max(coverage.deadline_at) AS deadline_at,
       batch.status,
       batch.delayed,
       batch.created_at,
       batch.updated_at,
       batch.sent_at
FROM daily_reporting.delivery_batches AS batch
JOIN daily_reporting.delivery_coverage AS coverage
    ON coverage.batch_id = batch.id
WHERE batch.status IN ('ready', 'sent')
GROUP BY batch.id,
         batch.recipient_id,
         batch.report_version,
         batch.scheduled_for,
         batch.status,
         batch.delayed,
         batch.created_at,
         batch.updated_at,
         batch.sent_at;

COMMENT ON VIEW daily_reporting.mcp_collection_status IS
    'MCP-safe latest collection attempt and last published snapshot by account/source';
COMMENT ON VIEW daily_reporting.mcp_published_source_snapshots IS
    'MCP-safe immutable snapshot manifest without payload or claim metadata';
COMMENT ON VIEW daily_reporting.mcp_ready_reports IS
    'Admin-only MCP report catalog without delivery or artifact secrets';

-- Converge an existing volume even if a legacy deployment accidentally gave
-- position_reader broad daily_reporting privileges.
REVOKE ALL ON ALL TABLES IN SCHEMA daily_reporting FROM position_reader;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA daily_reporting FROM position_reader;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA daily_reporting FROM position_reader;
REVOKE ALL ON SCHEMA daily_reporting FROM position_reader;

GRANT USAGE ON SCHEMA daily_reporting TO position_reader;
GRANT SELECT ON
    daily_reporting.mcp_collection_status,
    daily_reporting.mcp_published_source_snapshots,
    daily_reporting.mcp_sales_facts,
    daily_reporting.mcp_advertising_facts,
    daily_reporting.mcp_advertising_expense_facts,
    daily_reporting.mcp_finance_facts,
    daily_reporting.mcp_stock_facts,
    daily_reporting.mcp_price_facts,
    daily_reporting.mcp_ready_reports
TO position_reader;

-- Future relations remain private until a later migration explicitly adds a
-- curated projection and grants it.  This migration grants no default access.
ALTER DEFAULT PRIVILEGES IN SCHEMA daily_reporting
    REVOKE ALL ON TABLES FROM position_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA daily_reporting
    REVOKE ALL ON SEQUENCES FROM position_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA daily_reporting
    REVOKE ALL ON FUNCTIONS FROM position_reader;

COMMIT;
