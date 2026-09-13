#![expect(
    clippy::significant_drop_tightening,
    reason = "query rows borrow data associated with the supervised PostgreSQL session"
)]

//! Complete-only, exact-decimal WB financial journal. This is source evidence,
//! not a profit calculation or a substitute for an independent reconciliation.

mod reader;
pub(crate) use reader::read_wb_ledger_page;
pub use reader::{FinanceLedgerBatch, PostgresFinanceLedgerReader};

use std::fmt::Write;

use chrono::NaiveDate;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Config, error::SqlState};

use crate::postgres::SupervisedClient;

use super::{
    checkpoint::Checkpoints,
    wb_finance_source::{
        WB_FINANCE_MAX_ROWS, WbFinanceDetailRow, WbFinanceTransport,
        collect_finance_details_checkpointed,
    },
    wb_source::WbReportSourceError,
};

const MAX_PAYLOAD_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum FinanceLedgerError {
    #[error("invalid or unbounded financial ledger input")]
    InvalidInput,
    #[error(
        "financial report revision or overlapping source identity conflicts with published data"
    )]
    RevisionConflict,
    #[error("financial ledger is unavailable or its restricted database contract is not satisfied")]
    Unavailable,
    #[error("financial source collection failed: {0}")]
    Source(#[from] WbReportSourceError),
}

/// A completion capability: only successful bounded collection ending in HTTP
/// 204 constructs this value. No Deserialize or public unchecked constructor.
#[derive(Debug)]
pub struct WbFinanceBatch {
    account_id: String,
    date_from: NaiveDate,
    date_to: NaiveDate,
    rows: Vec<WbFinanceDetailRow>,
}

impl WbFinanceBatch {
    pub async fn collect(
        transport: &dyn WbFinanceTransport,
        account_id: String,
        date_from: NaiveDate,
        date_to: NaiveDate,
        checkpoints: &Checkpoints,
    ) -> Result<Self, FinanceLedgerError> {
        validate_account(&account_id)?;
        let rows = collect_finance_details_checkpointed(transport, date_from, date_to, checkpoints)
            .await?;
        if rows.len() > WB_FINANCE_MAX_ROWS
            || rows
                .iter()
                .any(|row| row.business_date < date_from || row.business_date > date_to)
        {
            return Err(FinanceLedgerError::InvalidInput);
        }
        Ok(Self {
            account_id,
            date_from,
            date_to,
            rows,
        })
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub const fn date_from(&self) -> NaiveDate {
        self.date_from
    }

    #[must_use]
    pub const fn date_to(&self) -> NaiveDate {
        self.date_to
    }

    #[must_use]
    pub fn rows(&self) -> &[WbFinanceDetailRow] {
        &self.rows
    }

    fn payload(&self) -> Result<(String, String), FinanceLedgerError> {
        let payload =
            serde_json::to_string(&self.rows).map_err(|_| FinanceLedgerError::InvalidInput)?;
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(FinanceLedgerError::InvalidInput);
        }
        let hash = sha256(payload.as_bytes());
        Ok((payload, hash))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LedgerPublication {
    pub batch_id: i64,
    pub content_sha256: String,
    pub already_present: bool,
}

pub struct PostgresFinanceLedger {
    client: SupervisedClient,
}

impl PostgresFinanceLedger {
    pub async fn connect(config: &Config) -> Result<Self, FinanceLedgerError> {
        let client = SupervisedClient::connect(config, "mcp-ozon-financial-ledger-writer")
            .await
            .map_err(|_| FinanceLedgerError::Unavailable)?;
        let writer = Self { client };
        writer.verify_runtime_contract().await?;
        Ok(writer)
    }

    #[must_use]
    pub fn from_client(client: Client) -> Self {
        Self {
            client: SupervisedClient::preconnected(client, "mcp-ozon-financial-ledger-writer"),
        }
    }

    pub async fn verify_runtime_contract(&self) -> Result<(), FinanceLedgerError> {
        self.client
            .verify_session_bounds()
            .await
            .map_err(unavailable)?;
        let client = self.client.acquire().await.map_err(unavailable)?;
        let valid: bool = client.query_one(
            "SELECT current_user = 'report_collector' \
             AND has_function_privilege(current_user, \
               'daily_reporting.publish_wb_financial_ledger(text,date,date,text,text,integer)', 'EXECUTE') \
             AND NOT has_table_privilege(current_user, \
               'daily_reporting.financial_ledger_batches', 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE') \
             AND NOT has_table_privilege(current_user, \
               'daily_reporting.financial_ledger_rows', 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE') \
             AND NOT has_table_privilege(current_user, \
               'daily_reporting.financial_ledger_amounts', 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE')",
            &[],
        ).await.map_err(unavailable)?.get(0);
        if !valid {
            return Err(FinanceLedgerError::Unavailable);
        }
        Ok(())
    }

    pub async fn publish_wb(
        &self,
        batch: &WbFinanceBatch,
    ) -> Result<LedgerPublication, FinanceLedgerError> {
        // Recheck even injected/reconnected sessions. The database itself also
        // denies base table access and bounds the sole publication function.
        self.verify_runtime_contract().await?;
        let (payload, hash) = batch.payload()?;
        let client = self.client.acquire().await.map_err(unavailable)?;
        let row = client
            .query_one(
                "SELECT batch_id, content_sha256, already_present \
             FROM daily_reporting.publish_wb_financial_ledger($1,$2,$3,$4,$5,$6)",
                &[
                    &batch.account_id,
                    &batch.date_from,
                    &batch.date_to,
                    &payload,
                    &hash,
                    &204_i32,
                ],
            )
            .await
            .map_err(|error| classify_error(&error))?;
        Ok(LedgerPublication {
            batch_id: row.get(0),
            content_sha256: row.get(1),
            already_present: row.get(2),
        })
    }
}

fn validate_account(account: &str) -> Result<(), FinanceLedgerError> {
    if account.is_empty()
        || account.len() > 128
        || !account
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(FinanceLedgerError::InvalidInput);
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        let _ = write!(result, "{byte:02x}");
    }
    result
}

fn unavailable<T>(_: T) -> FinanceLedgerError {
    FinanceLedgerError::Unavailable
}

fn classify_error(error: &tokio_postgres::Error) -> FinanceLedgerError {
    match error.code() {
        Some(&SqlState::UNIQUE_VIOLATION) => FinanceLedgerError::RevisionConflict,
        Some(
            &SqlState::INVALID_PARAMETER_VALUE
            | &SqlState::CHECK_VIOLATION
            | &SqlState::INVALID_TEXT_REPRESENTATION
            | &SqlState::NUMERIC_VALUE_OUT_OF_RANGE,
        ) => FinanceLedgerError::InvalidInput,
        _ => FinanceLedgerError::Unavailable,
    }
}
