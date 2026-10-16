//! Fixed read projections checked before accepting reporting traffic.
pub(super) const CONTRACT_PROBES: &[&str] = &[
    "SELECT account_id,marketplace,source,cutoff_at,status,next_attempt_at,completed_pages,error_class,first_observed_at,last_observed_at FROM daily_reporting.mcp_source_collection_jobs LIMIT 0",
    "SELECT snapshot_id, account_id, marketplace, source, cutoff_at, source_as_of, \
            period_start, period_end, status, pagination_complete, row_count, \
            collector_version, started_at, finished_at, error_class, http_status, \
            last_published_cutoff_at, last_published_source_as_of, \
            last_published_status, last_published_row_count \
     FROM daily_reporting.mcp_collection_status LIMIT 0",
    "SELECT snapshot_id, account_id, marketplace, source, cutoff_at, source_as_of, \
            period_start, period_end, status, pagination_complete, row_count, \
            collector_version, finished_at \
     FROM daily_reporting.mcp_published_source_snapshots LIMIT 0",
    "SELECT snapshot_id, sku, warehouse_id, cluster_id, macrolocal_cluster_id, data FROM daily_reporting.mcp_cluster_stock_facts LIMIT 0",
    "SELECT snapshot_id, sku, data, metric FROM daily_reporting.mcp_posting_movement_facts LIMIT 0",
    "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, sku, \
            ordered_units, operational_gmv_minor, cancelled_units, returned_units, currency \
     FROM daily_reporting.mcp_sales_facts LIMIT 0",
    "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, campaign_id, sku, \
            impressions, clicks, spend_minor, attributed_orders, attributed_revenue_minor, \
            currency, basket_additions, model_attributed_orders, \
            model_attributed_revenue_minor, product_price_minor, average_cpc_minor, \
            cpm_minor, cpl_minor FROM daily_reporting.mcp_advertising_facts LIMIT 0",
    "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, campaign_id, \
            money_spent_minor, bonus_spent_minor, prepayment_spent_minor, currency \
     FROM daily_reporting.mcp_advertising_expense_facts LIMIT 0",
    "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, sku, sku_key, category, \
            amount_minor, line_count, unknown_type_count \
     FROM daily_reporting.mcp_finance_facts LIMIT 0",
    "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, sku, warehouse_id, sellable_units, \
            present_units, reserved_units FROM daily_reporting.mcp_stock_facts LIMIT 0",
    "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, sku, price_minor, old_price_minor, currency \
     FROM daily_reporting.mcp_price_facts LIMIT 0",
    "SELECT batch_id, recipient_id, report_version, local_date, report_kind, \
            scheduled_for, deadline_at, status, delayed, created_at, updated_at, sent_at \
     FROM daily_reporting.mcp_ready_reports LIMIT 0",
];
