use super::{
    AccountScope, DataQuality, DateTime, Deserialize, Duration, JsonSchema, Marketplace,
    PostgresReportingRepository, ReportingMarketplace, ReportingReadError, ReportingReader,
    Serialize, SnapshotSource, Utc, marketplace_str, timestamp_string,
};
use serde_json::Value;
use tokio_postgres::Client;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct SourceSnapshotQuery {
    pub source: SnapshotSource,
    pub snapshot_id: Option<i64>,
    pub limit: u16,
    pub offset: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct SourceSnapshotResult {
    pub account_id: String,
    pub marketplace: ReportingMarketplace,
    pub source: SnapshotSource,
    pub storage: String,
    pub state: String,
    pub data_state: String,
    pub catalog_scope: Option<String>,
    pub quality: Option<DataQuality>,
    pub inventory_scope: Option<String>,
    pub pagination_complete: Option<bool>,
    /// Coverage counts describe the entire selected snapshot, never one page.
    pub coverage: Option<SourceSnapshotCoverage>,
    pub snapshot_id: Option<String>,
    pub cutoff_at: Option<String>,
    pub source_as_of: Option<String>,
    pub observed_from: Option<String>,
    pub period_start: Option<String>,
    pub period_end: Option<String>,
    pub total_rows: u64,
    pub rows: Vec<Value>,
    pub next_offset: Option<u32>,
    /// Source progress is separate from the selected last-good snapshot.
    pub latest_collection: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct SourceSnapshotCoverage {
    pub total_pairs: u64,
    pub known_pairs: u64,
    pub missing_pairs: u64,
}

const fn source_row_limit(source: SnapshotSource) -> u32 {
    if matches!(source, SnapshotSource::SellerStocks) {
        2_500_000
    } else {
        25_000
    }
}

impl ReportingReader {
    pub async fn source_snapshot(
        &self,
        account: &AccountScope,
        query: SourceSnapshotQuery,
    ) -> Result<SourceSnapshotResult, ReportingReadError> {
        if !(1..=1000).contains(&query.limit)
            || query.offset > source_row_limit(query.source)
            || query.snapshot_id.is_some_and(|id| id <= 0)
            || (query.offset > 0 && query.snapshot_id.is_none())
            || (query.source == SnapshotSource::Finance
                && account.marketplace() != Marketplace::Ozon)
            || (query.source == SnapshotSource::SellerStocks
                && account.marketplace() != Marketplace::Wildberries)
        {
            return Err(ReportingReadError::InvalidRequest);
        }
        self.repository.source_snapshot(account, query).await
    }
}

impl PostgresReportingRepository {
    pub(super) async fn source_snapshot_impl(
        &self,
        account: &AccountScope,
        query: SourceSnapshotQuery,
    ) -> Result<SourceSnapshotResult, ReportingReadError> {
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        let market = marketplace_str(account.marketplace());
        let source = source_name(query.source);
        let latest=client.query_opt("SELECT json_build_object('cutoff_at',cutoff_at,'status',status,'next_attempt_at',next_attempt_at,'completed_pages',completed_pages,'error_class',error_class,'first_observed_at',first_observed_at,'last_observed_at',last_observed_at)::text FROM daily_reporting.mcp_source_collection_jobs WHERE account_id=$1 AND marketplace=$2 AND source=$3 ORDER BY cutoff_at DESC LIMIT 1", &[&account.account_id(),&market,&source])
            .await.map_err(|_| ReportingReadError::Unavailable)?;
        let latest_collection = latest
            .map(|row| {
                serde_json::from_str::<Value>(&row.get::<_, String>(0))
                    .map_err(|_| ReportingReadError::InvalidPublishedData)
            })
            .transpose()?;
        let row=client.query_opt("SELECT s.snapshot_id,s.cutoff_at,s.source_as_of,s.period_start,s.period_end,s.row_count,COALESCE(j.first_observed_at,s.source_as_of),s.status,s.pagination_complete FROM daily_reporting.mcp_published_source_snapshots s LEFT JOIN daily_reporting.mcp_source_collection_jobs j ON j.account_id=s.account_id AND j.marketplace=s.marketplace AND j.source=s.source AND j.cutoff_at=s.cutoff_at WHERE s.account_id=$1 AND s.marketplace=$2 AND s.source=$3 AND ((s.status='succeeded' AND s.pagination_complete) OR ($3='seller_stocks' AND s.status='partial' AND s.pagination_complete)) AND ($4::bigint IS NULL OR s.snapshot_id=$4) ORDER BY s.cutoff_at DESC,s.snapshot_id DESC LIMIT 1", &[&account.account_id(),&market,&source,&query.snapshot_id])
            .await.map_err(|_| ReportingReadError::Unavailable)?;
        let mut result = missing_result(account, query, latest_collection);
        let Some(row) = row else {
            return Ok(result);
        };
        let id: i64 = row.get(0);
        let observed: DateTime<Utc> = row.get(2);
        let first: DateTime<Utc> = row.get(6);
        let total: u64 = u64::try_from(row.get::<_, i32>(5))
            .map_err(|_| ReportingReadError::InvalidPublishedData)?;
        if first > observed
            || observed > Utc::now() + Duration::minutes(5)
            || total > u64::from(source_row_limit(query.source))
        {
            return Err(ReportingReadError::InvalidPublishedData);
        }
        let pagination_complete: bool = row.get(8);
        let mut partial = row.get::<_, String>(7) == "partial" || !pagination_complete;
        if query.source == SnapshotSource::SellerStocks {
            let coverage = seller_stock_coverage(&client, account, market, id, total).await?;
            partial |= coverage.missing_pairs > 0;
            result.coverage = Some(coverage);
        }
        if account.marketplace() == Marketplace::Wildberries
            && query.source == SnapshotSource::Stocks
            && has_legacy_seller_rows(&client, account, market, id).await?
        {
            result.inventory_scope = Some("mixed".to_owned());
        }
        let (state, quality) = freshness(query.source, first, partial);
        state.clone_into(&mut result.state);
        data_state(total, partial).clone_into(&mut result.data_state);
        result.quality = Some(quality);
        result.pagination_complete = Some(pagination_complete);
        result.snapshot_id = Some(id.to_string());
        result.cutoff_at = Some(timestamp_string(row.get(1)));
        result.source_as_of = Some(timestamp_string(observed));
        result.observed_from = Some(timestamp_string(first));
        result.period_start = Some(timestamp_string(row.get(3)));
        result.period_end = Some(timestamp_string(row.get(4)));
        result.total_rows = total;
        // Both the relation and projection are fixed by the enum, never user SQL.
        let (relation, order) = fact_relation(query.source);
        let sql = format!(
            "SELECT to_jsonb(f)::text FROM {relation} f WHERE account_id=$1 AND marketplace=$2 AND snapshot_id=$3 ORDER BY {order} LIMIT $4 OFFSET $5"
        );
        let rows = client
            .query(
                &sql,
                &[
                    &account.account_id(),
                    &market,
                    &id,
                    &i64::from(query.limit),
                    &i64::from(query.offset),
                ],
            )
            .await
            .map_err(|_| ReportingReadError::Unavailable)?;
        result.rows = rows
            .iter()
            .map(|r| {
                serde_json::from_str(&r.get::<_, String>(0))
                    .map_err(|_| ReportingReadError::InvalidPublishedData)
            })
            .collect::<Result<Vec<_>, _>>()?;
        result.next_offset = next_page_offset(query, total, &result.rows)?;
        Ok(result)
    }
}

const fn source_name(source: SnapshotSource) -> &'static str {
    match source {
        SnapshotSource::Sales => "sales",
        SnapshotSource::Stocks => "stocks",
        SnapshotSource::SellerStocks => "seller_stocks",
        SnapshotSource::Prices => "prices",
        SnapshotSource::Finance => "finance",
        SnapshotSource::Advertising => "advertising",
    }
}

/// The result for a source without a selectable published snapshot.
fn missing_result(
    account: &AccountScope,
    query: SourceSnapshotQuery,
    latest_collection: Option<Value>,
) -> SourceSnapshotResult {
    SourceSnapshotResult {
        account_id: account.account_id().to_owned(),
        marketplace: account.marketplace().into(),
        source: query.source,
        storage: "published_postgresql_snapshots".to_owned(),
        state: "missing".to_owned(),
        data_state: "missing".to_owned(),
        catalog_scope: (query.source == SnapshotSource::SellerStocks)
            .then(|| "active_non_trash_cards".to_owned()),
        quality: None,
        inventory_scope: match (account.marketplace(), query.source) {
            (Marketplace::Wildberries, SnapshotSource::Stocks) => Some("fbw".to_owned()),
            (Marketplace::Wildberries, SnapshotSource::SellerStocks) => Some("seller".to_owned()),
            _ => None,
        },
        pagination_complete: None,
        coverage: None,
        snapshot_id: None,
        cutoff_at: None,
        source_as_of: None,
        observed_from: None,
        period_start: None,
        period_end: None,
        total_rows: 0,
        rows: Vec::new(),
        next_offset: None,
        latest_collection,
    }
}

/// Counts known seller-stock values across the entire selected snapshot.
async fn seller_stock_coverage(
    client: &Client,
    account: &AccountScope,
    market: &str,
    id: i64,
    total: u64,
) -> Result<SourceSnapshotCoverage, ReportingReadError> {
    let counts = client
        .query_one(
            "SELECT count(*)::bigint,count(sellable_units)::bigint \
             FROM daily_reporting.mcp_seller_stock_facts \
             WHERE account_id=$1 AND marketplace=$2 AND snapshot_id=$3",
            &[&account.account_id(), &market, &id],
        )
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    let actual = u64::try_from(counts.get::<_, i64>(0))
        .map_err(|_| ReportingReadError::InvalidPublishedData)?;
    let known = u64::try_from(counts.get::<_, i64>(1))
        .map_err(|_| ReportingReadError::InvalidPublishedData)?;
    if actual != total || known > total {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    Ok(SourceSnapshotCoverage {
        total_pairs: total,
        known_pairs: known,
        missing_pairs: total - known,
    })
}

/// PR 102 published some combined snapshots. Inspect the entire immutable
/// snapshot, since the current page may contain only FBW.
async fn has_legacy_seller_rows(
    client: &Client,
    account: &AccountScope,
    market: &str,
    id: i64,
) -> Result<bool, ReportingReadError> {
    Ok(client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM daily_reporting.mcp_stock_facts \
             WHERE account_id=$1 AND marketplace=$2 AND snapshot_id=$3 \
               AND warehouse_id LIKE 'wb:seller:%')",
            &[&account.account_id(), &market, &id],
        )
        .await
        .map_err(|_| ReportingReadError::Unavailable)?
        .get(0))
}

fn freshness(
    source: SnapshotSource,
    first_observed: DateTime<Utc>,
    partial: bool,
) -> (&'static str, DataQuality) {
    let sla = match source {
        SnapshotSource::Stocks | SnapshotSource::SellerStocks | SnapshotSource::Prices => {
            Duration::hours(1)
        }
        SnapshotSource::Advertising => Duration::hours(2),
        _ => Duration::hours(6),
    };
    if Utc::now() - first_observed > sla {
        ("stale", DataQuality::Stale)
    } else if partial {
        ("partial", DataQuality::Partial)
    } else {
        ("available", DataQuality::Complete)
    }
}

const fn data_state(total: u64, partial: bool) -> &'static str {
    if total == 0 {
        "no_data"
    } else if partial {
        "partial"
    } else {
        "available"
    }
}

const fn fact_relation(source: SnapshotSource) -> (&'static str, &'static str) {
    match source {
        SnapshotSource::Sales => ("daily_reporting.mcp_sales_facts", "business_date,sku"),
        SnapshotSource::Stocks => ("daily_reporting.mcp_stock_facts", "sku,warehouse_id"),
        SnapshotSource::SellerStocks => (
            "daily_reporting.mcp_seller_stock_facts",
            "sku,chrt_id,warehouse_id",
        ),
        SnapshotSource::Prices => ("daily_reporting.mcp_price_facts", "sku"),
        SnapshotSource::Advertising => (
            "daily_reporting.mcp_advertising_facts",
            "business_date,campaign_id,sku",
        ),
        SnapshotSource::Finance => (
            "daily_reporting.mcp_finance_facts",
            "business_date,sku_key,category",
        ),
    }
}

/// Validates one decoded page against its snapshot and returns the offset of
/// the next page, if any.
fn next_page_offset(
    query: SourceSnapshotQuery,
    total: u64,
    rows: &[Value],
) -> Result<Option<u32>, ReportingReadError> {
    if query.source == SnapshotSource::SellerStocks
        && rows.iter().any(|fact| {
            fact["delivery_type"]
                .as_u64()
                .is_none_or(|delivery_type| delivery_type == 0)
        })
    {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    let expected = total
        .saturating_sub(u64::from(query.offset))
        .min(u64::from(query.limit));
    if u64::try_from(rows.len()).ok() != Some(expected) {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    let next = query.offset
        + u32::try_from(rows.len()).map_err(|_| ReportingReadError::InvalidPublishedData)?;
    Ok((u64::from(next) < total && !rows.is_empty()).then_some(next))
}
