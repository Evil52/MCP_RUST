//! Immutable whole-report WB evidence and primary-total reconciliation.

mod reader;
pub(crate) use reader::read_wb_official_report_page;
pub use reader::{PostgresWbReportReader, StoredWbOfficialReport};

use std::fmt::Write;

use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Config, error::SqlState};

use crate::postgres::SupervisedClient;

use super::{
    finance_reconciliation::WbFinanceComparisonEvidence,
    wb_official_reconciliation::reconcile_wb_official_report,
    wb_report_source::{WbCompleteReportDetails, WbSelectedReport},
};

const MAX_PAYLOAD_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum WbReportRepositoryError {
    #[error("invalid official financial report input")]
    InvalidInput,
    #[error("official financial report revision conflicts with immutable published evidence")]
    RevisionConflict,
    #[error(
        "official financial report repository is unavailable or its restricted contract is invalid"
    )]
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WbReportPublication {
    pub snapshot_id: i64,
    pub content_sha256: String,
    pub already_present: bool,
}

#[derive(Serialize)]
struct PublicationEnvelope<'a> {
    scope_json: String,
    summary_json: String,
    rows_json: String,
    summary_evidence: &'a WbFinanceComparisonEvidence,
    details_evidence: &'a WbFinanceComparisonEvidence,
}

/// The collector role can only execute a bounded publication function.
///
/// The caller resolves the actor and cabinet from server authorization; neither
/// the actor nor the marketplace credential is taken from source report text.
pub struct PostgresWbReportRepository {
    client: SupervisedClient,
}

impl PostgresWbReportRepository {
    pub async fn connect(config: &Config) -> Result<Self, WbReportRepositoryError> {
        let client = SupervisedClient::connect(config, "mcp-ozon-wb-official-report-writer")
            .await
            .map_err(unavailable)?;
        let repository = Self { client };
        repository.verify_runtime_contract().await?;
        Ok(repository)
    }

    #[must_use]
    pub fn from_client(client: Client) -> Self {
        Self {
            client: SupervisedClient::preconnected(client, "mcp-ozon-wb-official-report-writer"),
        }
    }

    pub async fn verify_runtime_contract(&self) -> Result<(), WbReportRepositoryError> {
        verify_contract(&self.client, true).await
    }

    /// Publish a selected closed report and all of its independently collected
    /// detail pages atomically. Replay returns the original immutable receipt;
    /// a changed report is an explicit revision conflict, never an overwrite.
    pub async fn publish(
        &self,
        selected: &WbSelectedReport,
        details: &WbCompleteReportDetails,
        actor_id: &str,
    ) -> Result<WbReportPublication, WbReportRepositoryError> {
        validate_identifier(actor_id)?;
        self.verify_runtime_contract().await?;
        let payload = publication_payload(selected, details)?;
        let content_sha256 = sha256(payload.as_bytes());
        let row = self
            .client
            .acquire()
            .await
            .map_err(unavailable)?
            .query_one(
                "SELECT snapshot_id,content_sha256,already_present \
                 FROM daily_reporting.publish_wb_official_report($1,$2,$3)",
                &[&actor_id, &payload, &content_sha256],
            )
            .await
            .map_err(|error| classify_error(&error))?;
        Ok(WbReportPublication {
            snapshot_id: row.get(0),
            content_sha256: row.get(1),
            already_present: row.get(2),
        })
    }
}

fn publication_payload(
    selected: &WbSelectedReport,
    details: &WbCompleteReportDetails,
) -> Result<String, WbReportRepositoryError> {
    // Types are completion capabilities; the independent pure verifier adds
    // defence against accidentally pairing two successfully collected scopes.
    if selected.evidence().scope != details.evidence().scope {
        return Err(WbReportRepositoryError::InvalidInput);
    }
    reconcile_wb_official_report(
        details.rows(),
        details.evidence(),
        Some(&selected.baseline()),
    )
    .map_err(|_| WbReportRepositoryError::InvalidInput)?;
    let envelope = PublicationEnvelope {
        scope_json: serde_json::to_string(&selected.summary().scope).map_err(invalid_input)?,
        summary_json: serde_json::to_string(selected.summary()).map_err(invalid_input)?,
        rows_json: serde_json::to_string(details.rows()).map_err(invalid_input)?,
        summary_evidence: selected.evidence(),
        details_evidence: details.evidence(),
    };
    let payload = serde_json::to_string(&envelope).map_err(invalid_input)?;
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(WbReportRepositoryError::InvalidInput);
    }
    Ok(payload)
}

async fn verify_contract(
    session: &SupervisedClient,
    writer: bool,
) -> Result<(), WbReportRepositoryError> {
    session.verify_session_bounds().await.map_err(unavailable)?;
    let row = session.acquire().await.map_err(unavailable)?.query_one(
        "SELECT CASE WHEN $1 THEN current_user='report_collector' \
                      ELSE current_user IN ('position_reader','report_worker') END \
         AND has_function_privilege(current_user, \
             'daily_reporting.publish_wb_official_report(text,text,text)','EXECUTE')=$1 \
         AND NOT EXISTS (SELECT 1 FROM unnest(ARRAY[ \
             'daily_reporting.wb_official_reports', \
             'daily_reporting.wb_official_report_rows', \
             'daily_reporting.wb_official_report_amounts', \
             'daily_reporting.wb_official_report_summary_amounts', \
             'daily_reporting.wb_official_report_comparisons']) AS t(name) \
             WHERE has_table_privilege(current_user,t.name,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE')) \
         AND NOT EXISTS (SELECT 1 FROM unnest(ARRAY[ \
             'daily_reporting.mcp_wb_official_reports', \
             'daily_reporting.mcp_wb_official_report_rows', \
             'daily_reporting.mcp_wb_official_report_summary_amounts', \
             'daily_reporting.mcp_wb_official_report_comparisons']) AS t(name) \
             WHERE NOT has_table_privilege(current_user,t.name,'SELECT') \
                OR has_table_privilege(current_user,t.name,'INSERT,UPDATE,DELETE,TRUNCATE'))",
        &[&writer],
    ).await.map_err(unavailable)?;
    if !row.get::<_, bool>(0) {
        return Err(WbReportRepositoryError::Unavailable);
    }
    Ok(())
}

fn validate_identifier(value: &str) -> Result<(), WbReportRepositoryError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(WbReportRepositoryError::InvalidInput);
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn invalid_input<T>(_: T) -> WbReportRepositoryError {
    WbReportRepositoryError::InvalidInput
}

fn unavailable<T>(_: T) -> WbReportRepositoryError {
    WbReportRepositoryError::Unavailable
}

fn classify_error(error: &tokio_postgres::Error) -> WbReportRepositoryError {
    match error.code() {
        Some(&SqlState::UNIQUE_VIOLATION) => WbReportRepositoryError::RevisionConflict,
        Some(
            &SqlState::INVALID_PARAMETER_VALUE
            | &SqlState::CHECK_VIOLATION
            | &SqlState::INVALID_TEXT_REPRESENTATION
            | &SqlState::DATETIME_FIELD_OVERFLOW
            | &SqlState::NUMERIC_VALUE_OUT_OF_RANGE,
        ) => WbReportRepositoryError::InvalidInput,
        _ => WbReportRepositoryError::Unavailable,
    }
}
