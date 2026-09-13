#![expect(
    clippy::significant_drop_tightening,
    reason = "rows borrow the supervised database session"
)]

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use tokio_postgres::{Client, Config};

use crate::postgres::SupervisedClient;

use super::{FinanceLedgerError, WbFinanceDetailRow, unavailable, validate_account};

#[derive(Debug, Clone, Serialize)]
pub struct FinanceLedgerBatch {
    pub batch_id: i64,
    pub account_id: String,
    pub marketplace: String,
    pub source: String,
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
    pub row_count: i32,
    pub terminal_http_status: i32,
    pub published_at: DateTime<Utc>,
}

/// Access is limited to published projected views. Callers must resolve the
/// account from the authenticated employee's allowed scope before calling.
pub struct PostgresFinanceLedgerReader {
    client: SupervisedClient,
}

impl PostgresFinanceLedgerReader {
    pub async fn connect(config: &Config) -> Result<Self, FinanceLedgerError> {
        let client = SupervisedClient::connect(config, "mcp-ozon-financial-ledger-reader")
            .await
            .map_err(unavailable)?;
        let reader = Self { client };
        reader.verify_runtime_contract().await?;
        Ok(reader)
    }

    #[must_use]
    pub fn from_client(client: Client) -> Self {
        Self {
            client: SupervisedClient::preconnected(client, "mcp-ozon-financial-ledger-reader"),
        }
    }

    pub async fn verify_runtime_contract(&self) -> Result<(), FinanceLedgerError> {
        verify_reader_contract(&self.client).await
    }

    pub async fn list_wb_batches(
        &self,
        account: &str,
        limit: u32,
    ) -> Result<Vec<FinanceLedgerBatch>, FinanceLedgerError> {
        validate_account(account)?;
        if !(1..=100).contains(&limit) {
            return Err(FinanceLedgerError::InvalidInput);
        }
        self.verify_runtime_contract().await?;
        let client = self.client.acquire().await.map_err(unavailable)?;
        let rows = client
            .query(
                "SELECT batch_id, account_id, marketplace, source, date_from, date_to, \
              row_count, terminal_http_status, published_at \
             FROM daily_reporting.mcp_financial_ledger_batches \
             WHERE account_id = $1 ORDER BY published_at DESC, batch_id DESC LIMIT $2",
                &[&account, &i64::from(limit)],
            )
            .await
            .map_err(unavailable)?;
        Ok(rows
            .into_iter()
            .map(|row| FinanceLedgerBatch {
                batch_id: row.get(0),
                account_id: row.get(1),
                marketplace: row.get(2),
                source: row.get(3),
                date_from: row.get(4),
                date_to: row.get(5),
                row_count: row.get(6),
                terminal_http_status: row.get(7),
                published_at: row.get(8),
            })
            .collect())
    }

    pub async fn read_wb_rows(
        &self,
        account: &str,
        batch_id: i64,
        after_rrd_id: u64,
        limit: u32,
    ) -> Result<Vec<WbFinanceDetailRow>, FinanceLedgerError> {
        validate_account(account)?;
        let cursor = i64::try_from(after_rrd_id).map_err(|_| FinanceLedgerError::InvalidInput)?;
        if batch_id <= 0 || !(1..=1000).contains(&limit) {
            return Err(FinanceLedgerError::InvalidInput);
        }
        self.verify_runtime_contract().await?;
        read_rows(&self.client, account, batch_id, cursor, limit).await
    }
}

async fn verify_reader_contract(session: &SupervisedClient) -> Result<(), FinanceLedgerError> {
    session.verify_session_bounds().await.map_err(unavailable)?;
    let client = session.acquire().await.map_err(unavailable)?;
    let valid: bool = client.query_one(
            "SELECT current_user IN ('position_reader', 'report_worker') \
             AND has_table_privilege(current_user, \
                'daily_reporting.mcp_financial_ledger_batches', 'SELECT') \
             AND has_table_privilege(current_user, \
                'daily_reporting.mcp_financial_ledger_rows', 'SELECT') \
             AND NOT has_table_privilege(current_user, \
                'daily_reporting.financial_ledger_batches', 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE') \
             AND NOT has_table_privilege(current_user, \
                'daily_reporting.financial_ledger_rows', 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE') \
             AND NOT has_table_privilege(current_user, \
                'daily_reporting.financial_ledger_amounts', 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE') \
             AND NOT has_function_privilege(current_user, \
                'daily_reporting.publish_wb_financial_ledger(text,date,date,text,text,integer)', 'EXECUTE')",
            &[],
        ).await.map_err(unavailable)?.get(0);
    if !valid {
        return Err(FinanceLedgerError::Unavailable);
    }
    Ok(())
}

async fn read_rows(
    session: &SupervisedClient,
    account: &str,
    batch_id: i64,
    cursor: i64,
    limit: u32,
) -> Result<Vec<WbFinanceDetailRow>, FinanceLedgerError> {
    let client = session.acquire().await.map_err(unavailable)?;
    let rows = client.query(
            "WITH page AS (SELECT DISTINCT rrd_id \
                FROM daily_reporting.mcp_financial_ledger_rows \
                WHERE account_id = $1 AND batch_id = $2 AND rrd_id > $3 \
                ORDER BY rrd_id LIMIT $4) \
             SELECT jsonb_build_object( \
                'rrd_id', r.rrd_id, 'report_id', r.report_id, \
                'business_date', r.business_date, 'sku', r.sku, 'currency', r.currency, \
                'document_type', r.document_type, 'operation_type', r.operation_type, \
                'quantity', r.quantity, 'amounts', \
                jsonb_object_agg(r.field, jsonb_build_object('units', r.units, 'scale', r.scale)))::text \
             FROM daily_reporting.mcp_financial_ledger_rows r JOIN page USING (rrd_id) \
             WHERE r.account_id = $1 AND r.batch_id = $2 \
             GROUP BY r.rrd_id, r.report_id, r.business_date, r.sku, r.currency, \
                r.document_type, r.operation_type, r.quantity ORDER BY r.rrd_id",
            &[&account, &batch_id, &cursor, &i64::from(limit)],
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

/// Read one account-scoped immutable batch and an exact bounded row page.
///
/// Caller authentication is performed before entering this database boundary.
pub async fn read_wb_ledger_page(
    session: &SupervisedClient,
    account: &str,
    batch_id: Option<i64>,
    after_rrd_id: u64,
    limit: u32,
) -> Result<Option<(FinanceLedgerBatch, Vec<WbFinanceDetailRow>, bool)>, FinanceLedgerError> {
    validate_account(account)?;
    let cursor = i64::try_from(after_rrd_id).map_err(|_| FinanceLedgerError::InvalidInput)?;
    if !(1..=1000).contains(&limit)
        || batch_id.is_some_and(|id| id <= 0)
        || (cursor > 0 && batch_id.is_none())
    {
        return Err(FinanceLedgerError::InvalidInput);
    }
    verify_reader_contract(session).await?;
    let batch = {
        let client = session.acquire().await.map_err(unavailable)?;
        client
            .query_opt(
                "SELECT batch_id, account_id, marketplace, source, date_from, date_to, \
             row_count, terminal_http_status, published_at \
             FROM daily_reporting.mcp_financial_ledger_batches \
             WHERE account_id = $1 AND ($2::bigint IS NULL OR batch_id = $2) \
             ORDER BY published_at DESC, batch_id DESC LIMIT 1",
                &[&account, &batch_id],
            )
            .await
            .map_err(unavailable)?
            .map(|row| FinanceLedgerBatch {
                batch_id: row.get(0),
                account_id: row.get(1),
                marketplace: row.get(2),
                source: row.get(3),
                date_from: row.get(4),
                date_to: row.get(5),
                row_count: row.get(6),
                terminal_http_status: row.get(7),
                published_at: row.get(8),
            })
    };
    let Some(batch) = batch else {
        return Ok(None);
    };
    let mut rows = read_rows(session, account, batch.batch_id, cursor, limit + 1).await?;
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    Ok(Some((batch, rows, has_more)))
}
