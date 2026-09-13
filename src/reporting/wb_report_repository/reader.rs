use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, Config};

use crate::postgres::SupervisedClient;
use crate::reporting::{
    finance_reconciliation::WbFinanceComparisonEvidence, wb_finance_source::WbFinanceDetailRow,
    wb_official_reconciliation::WbOfficialComparison, wb_report_source::WbOfficialReportSummary,
};

use super::{WbReportRepositoryError, unavailable, validate_identifier, verify_contract};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredWbOfficialReport {
    pub snapshot_id: i64,
    pub summary: WbOfficialReportSummary,
    pub summary_evidence: WbFinanceComparisonEvidence,
    pub details_evidence: WbFinanceComparisonEvidence,
    pub comparison: WbOfficialComparison,
    pub row_count: i32,
    pub actor_id: String,
    pub content_sha256: String,
    pub published_at: DateTime<Utc>,
}

/// Published projections only; callers enforce employee and cabinet access
/// before entering this account-bound repository on every request.
pub struct PostgresWbReportReader {
    client: SupervisedClient,
}

impl PostgresWbReportReader {
    pub async fn connect(config: &Config) -> Result<Self, WbReportRepositoryError> {
        let client = SupervisedClient::connect(config, "mcp-ozon-wb-official-report-reader")
            .await
            .map_err(unavailable)?;
        let reader = Self { client };
        reader.verify_runtime_contract().await?;
        Ok(reader)
    }

    #[must_use]
    pub fn from_client(client: Client) -> Self {
        Self {
            client: SupervisedClient::preconnected(client, "mcp-ozon-wb-official-report-reader"),
        }
    }

    pub async fn verify_runtime_contract(&self) -> Result<(), WbReportRepositoryError> {
        verify_contract(&self.client, false).await
    }

    pub async fn read(
        &self,
        account: &str,
        report_id: u64,
    ) -> Result<Option<StoredWbOfficialReport>, WbReportRepositoryError> {
        let report = validate_scope(account, report_id)?;
        self.verify_runtime_contract().await?;
        read_report(&self.client, account, report).await
    }

    pub async fn read_rows(
        &self,
        account: &str,
        report_id: u64,
        after_rrd_id: u64,
        limit: u32,
    ) -> Result<Vec<WbFinanceDetailRow>, WbReportRepositoryError> {
        let report = validate_scope(account, report_id)?;
        let cursor = validate_page(after_rrd_id, limit)?;
        self.verify_runtime_contract().await?;
        read_rows(&self.client, account, report, cursor, limit).await
    }
}

/// Account-scoped manifest and bounded detail page for the MCP reporting layer.
/// Authentication is resolved before this function; no credentials are needed.
pub async fn read_wb_official_report_page(
    session: &SupervisedClient,
    account: &str,
    report_id: u64,
    after_rrd_id: u64,
    limit: u32,
) -> Result<Option<(StoredWbOfficialReport, Vec<WbFinanceDetailRow>, bool)>, WbReportRepositoryError>
{
    let report = validate_scope(account, report_id)?;
    let cursor = validate_page(after_rrd_id, limit)?;
    verify_contract(session, false).await?;
    let Some(stored) = read_report(session, account, report).await? else {
        return Ok(None);
    };
    let mut rows = read_rows(session, account, report, cursor, limit + 1).await?;
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    Ok(Some((stored, rows, has_more)))
}

fn validate_scope(account: &str, report_id: u64) -> Result<i64, WbReportRepositoryError> {
    validate_identifier(account)?;
    let report = i64::try_from(report_id).map_err(|_| WbReportRepositoryError::InvalidInput)?;
    if report <= 0 {
        return Err(WbReportRepositoryError::InvalidInput);
    }
    Ok(report)
}

fn validate_page(after_rrd_id: u64, limit: u32) -> Result<i64, WbReportRepositoryError> {
    let cursor = i64::try_from(after_rrd_id).map_err(|_| WbReportRepositoryError::InvalidInput)?;
    if !(1..=1000).contains(&limit) {
        return Err(WbReportRepositoryError::InvalidInput);
    }
    Ok(cursor)
}

async fn read_report(
    session: &SupervisedClient,
    account: &str,
    report: i64,
) -> Result<Option<StoredWbOfficialReport>, WbReportRepositoryError> {
    let row = session
        .acquire()
        .await
        .map_err(unavailable)?
        .query_opt(REPORT_QUERY, &[&account, &report])
        .await
        .map_err(unavailable)?;
    row.map(|row| serde_json::from_str(row.get::<_, &str>(0)).map_err(unavailable))
        .transpose()
}

async fn read_rows(
    session: &SupervisedClient,
    account: &str,
    report: i64,
    cursor: i64,
    limit: u32,
) -> Result<Vec<WbFinanceDetailRow>, WbReportRepositoryError> {
    let rows = session.acquire().await.map_err(unavailable)?.query(
        "WITH page AS (SELECT DISTINCT rrd_id FROM daily_reporting.mcp_wb_official_report_rows \
            WHERE account_id=$1 AND report_id=$2 AND rrd_id>$3 ORDER BY rrd_id LIMIT $4) \
         SELECT jsonb_build_object('rrd_id',r.rrd_id,'report_id',r.report_id, \
            'business_date',r.business_date,'sku',r.sku,'currency',r.currency, \
            'document_type',r.document_type,'operation_type',r.operation_type,'quantity',r.quantity, \
            'amounts',jsonb_object_agg(r.field,jsonb_build_object('units',r.units::text,'scale',r.scale)))::text \
         FROM daily_reporting.mcp_wb_official_report_rows r JOIN page USING(rrd_id) \
         WHERE r.account_id=$1 AND r.report_id=$2 \
         GROUP BY r.rrd_id,r.report_id,r.business_date,r.sku,r.currency, \
            r.document_type,r.operation_type,r.quantity ORDER BY r.rrd_id",
        &[&account,&report,&cursor,&i64::from(limit)],
    ).await.map_err(unavailable)?;
    rows.into_iter()
        .map(|row| {
            let normalized: WbFinanceDetailRow =
                serde_json::from_str(row.get::<_, &str>(0)).map_err(unavailable)?;
            normalized.validate().map_err(unavailable)?;
            Ok(normalized)
        })
        .collect()
}

const REPORT_QUERY: &str = "WITH scoped AS ( \
 SELECT b.*,jsonb_build_object('account_id',account_id,'report_id',report_id,'currency',currency, \
 'period',period,'date_from',date_from,'date_to',date_to) AS scope \
 FROM daily_reporting.mcp_wb_official_reports b WHERE account_id=$1 AND report_id=$2) \
 SELECT jsonb_build_object('snapshot_id',b.snapshot_id,'row_count',b.row_count, \
 'actor_id',b.actor_id,'content_sha256',b.content_sha256,'published_at',b.published_at, \
 'summary',jsonb_build_object('scope',b.scope,'created_date',b.created_date, \
 'report_type',b.report_type,'amounts',(SELECT jsonb_object_agg(a.field, \
 jsonb_build_object('units',a.units::text,'scale',a.scale)) \
 FROM daily_reporting.mcp_wb_official_report_summary_amounts a \
 WHERE a.account_id=$1 AND a.report_id=$2 AND a.snapshot_id=b.snapshot_id)), \
 'summary_evidence',jsonb_build_object('scope',b.scope,'observation_id',b.summary_observation_id, \
 'source_sha256',b.summary_source_sha256,'terminal_observed',true,'covers_entire_report',true), \
 'details_evidence',jsonb_build_object('scope',b.scope,'observation_id',b.details_observation_id, \
 'source_sha256',b.details_source_sha256,'terminal_observed',true,'covers_entire_report',true), \
 'comparison',jsonb_build_object('mapping_version',b.mapping_version,'status',b.comparison_status, \
 'unavailable_reason',b.unavailable_reason,'columns',coalesce((SELECT jsonb_agg(jsonb_build_object( \
 'detail_column',c.detail_column,'summary_column',c.summary_column, \
 'detail_total',CASE WHEN c.detail_units IS NULL THEN NULL ELSE jsonb_build_object('units',c.detail_units,'scale',c.scale) END, \
 'summary_total',CASE WHEN c.summary_units IS NULL THEN NULL ELSE jsonb_build_object('units',c.summary_units,'scale',c.scale) END, \
 'difference',CASE WHEN c.difference_units IS NULL THEN NULL ELSE jsonb_build_object('units',c.difference_units,'scale',c.scale) END, \
 'status',c.status,'unavailable_reason',c.unavailable_reason) ORDER BY CASE c.detail_column WHEN 'retailAmount' THEN 0 ELSE 1 END) \
 FROM daily_reporting.mcp_wb_official_report_comparisons c \
 WHERE c.account_id=$1 AND c.report_id=$2 AND c.snapshot_id=b.snapshot_id),'[]'::jsonb)))::text FROM scoped b";
