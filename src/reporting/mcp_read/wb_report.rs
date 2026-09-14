//! Published official WB reports, isolated from marketplace credentials.

use std::collections::BTreeMap;

use super::{
    AccountScope, DataState, FinancialLedgerAmount, FinancialLedgerRow, JsonSchema, Marketplace,
    PostgresReportingRepository, ReportingMarketplace, ReportingReadError, ReportingReader,
    Serialize, timestamp_string,
};
use crate::reporting::{
    finance_reconciliation::{
        ExactFinanceTotal, WbFinanceBaseline, WbFinanceBaselineKind, WbFinanceReportPeriod,
    },
    wb_finance_source::{WB_FINANCE_MAX_ROWS, WbFinanceDetailRow},
    wb_official_reconciliation::{
        WB_OFFICIAL_MAPPING_VERSION, WbOfficialColumnComparison, WbOfficialComparison,
        WbOfficialComparisonStatus, WbOfficialUnavailable, reconcile_wb_official_report,
    },
    wb_report_repository::{
        StoredWbOfficialReport, WbReportRepositoryError, read_wb_official_report_page,
    },
    wb_report_source::WB_REPORT_SUMMARY_AMOUNTS,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WbReportReconciliationQuery {
    pub report_id: u64,
    pub after_rrd_id: u64,
    pub limit: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WbReportComparisonStatus {
    PrimaryTotalsMatch,
    Mismatch,
    Unavailable,
}

impl From<WbOfficialComparisonStatus> for WbReportComparisonStatus {
    fn from(value: WbOfficialComparisonStatus) -> Self {
        match value {
            WbOfficialComparisonStatus::PrimaryTotalsMatch => Self::PrimaryTotalsMatch,
            WbOfficialComparisonStatus::Mismatch => Self::Mismatch,
            WbOfficialComparisonStatus::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct WbReportColumnComparison {
    pub detail_column: String,
    pub summary_column: String,
    pub detail_total: Option<FinancialLedgerAmount>,
    pub summary_total: Option<FinancialLedgerAmount>,
    pub difference: Option<FinancialLedgerAmount>,
    pub status: WbReportComparisonStatus,
    pub unavailable_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct WbReportComparison {
    pub mapping_version: String,
    /// Matching primary totals never proves final bank payment or profit.
    pub status: WbReportComparisonStatus,
    pub unavailable_reason: Option<String>,
    pub columns: Vec<WbReportColumnComparison>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct WbOfficialReportProvenance {
    pub snapshot_id: String,
    pub date_from: String,
    pub date_to: String,
    pub created_date: String,
    pub period: String,
    pub currency: String,
    pub row_count: u32,
    pub summary_amounts: BTreeMap<String, FinancialLedgerAmount>,
    pub summary_source_sha256: String,
    pub details_source_sha256: String,
    pub content_sha256: String,
    pub terminal_http_status: u16,
    pub published_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct WbReportReconciliationResult {
    pub account_id: String,
    pub marketplace: ReportingMarketplace,
    pub report_id: String,
    pub storage: String,
    /// N/D means no published report. COMPLETE means a fully collected report.
    pub state: DataState,
    pub report: Option<WbOfficialReportProvenance>,
    /// The stored official comparison is independent of this detail page.
    pub comparison: Option<WbReportComparison>,
    pub rows: Vec<FinancialLedgerRow>,
    /// Repeat the same report ID when continuing with this exact string cursor.
    pub next_after_rrd_id: Option<String>,
}

impl ReportingReader {
    pub async fn wb_report_reconciliation(
        &self,
        account: &AccountScope,
        query: WbReportReconciliationQuery,
    ) -> Result<WbReportReconciliationResult, ReportingReadError> {
        validate_query(account, query)?;
        let result = self
            .repository
            .wb_report_reconciliation(account, query)
            .await?;
        if result.account_id != account.account_id()
            || result.marketplace != ReportingMarketplace::Wildberries
            || result.report_id != query.report_id.to_string()
        {
            return Err(ReportingReadError::InvalidPublishedData);
        }
        Ok(result)
    }
}

fn validate_query(
    account: &AccountScope,
    query: WbReportReconciliationQuery,
) -> Result<(), ReportingReadError> {
    if account.marketplace() != Marketplace::Wildberries
        || query.report_id == 0
        || query.report_id > i64::MAX.unsigned_abs()
        || query.after_rrd_id > i64::MAX.unsigned_abs()
        || !(1..=1000).contains(&query.limit)
    {
        return Err(ReportingReadError::InvalidRequest);
    }
    Ok(())
}

impl PostgresReportingRepository {
    pub(super) async fn wb_report_reconciliation_impl(
        &self,
        account: &AccountScope,
        query: WbReportReconciliationQuery,
    ) -> Result<WbReportReconciliationResult, ReportingReadError> {
        validate_query(account, query)?;
        let page = read_wb_official_report_page(
            &self.client,
            account.account_id(),
            query.report_id,
            query.after_rrd_id,
            u32::from(query.limit),
        )
        .await
        .map_err(|error| match error {
            WbReportRepositoryError::InvalidInput => ReportingReadError::InvalidRequest,
            _ => ReportingReadError::Unavailable,
        })?;
        page_result(account, query, page)
    }
}

fn page_result(
    account: &AccountScope,
    query: WbReportReconciliationQuery,
    page: Option<(StoredWbOfficialReport, Vec<WbFinanceDetailRow>, bool)>,
) -> Result<WbReportReconciliationResult, ReportingReadError> {
    let mut result = WbReportReconciliationResult {
        account_id: account.account_id().to_owned(),
        marketplace: ReportingMarketplace::Wildberries,
        report_id: query.report_id.to_string(),
        storage: "published_postgresql_wb_official_reports".to_owned(),
        state: DataState::Unavailable,
        report: None,
        comparison: None,
        rows: Vec::new(),
        next_after_rrd_id: None,
    };
    let Some((stored, rows, has_more)) = page else {
        return Ok(result);
    };
    validate_report(account, query, &stored)?;
    let count =
        usize::try_from(stored.row_count).map_err(|_| ReportingReadError::InvalidPublishedData)?;
    if rows.len() > usize::from(query.limit)
        || rows.len() > count
        || (has_more && (rows.len() != usize::from(query.limit) || rows.len() >= count))
        || (query.after_rrd_id == 0 && !has_more && rows.len() != count)
    {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    let mut cursor = query.after_rrd_id;
    for row in &rows {
        row.validate()
            .map_err(|_| ReportingReadError::InvalidPublishedData)?;
        if row.rrd_id <= cursor
            || row.report_id != query.report_id
            || row.currency != stored.summary.scope.currency
        {
            return Err(ReportingReadError::InvalidPublishedData);
        }
        cursor = row.rrd_id;
    }
    if query.after_rrd_id == 0 && !has_more {
        let baseline = WbFinanceBaseline {
            kind: WbFinanceBaselineKind::OfficialReportSummary,
            evidence: stored.summary_evidence.clone(),
            totals: stored
                .summary
                .amounts
                .iter()
                .map(|(name, amount)| {
                    (
                        name.clone(),
                        ExactFinanceTotal {
                            units: amount.units,
                            scale: amount.scale,
                        },
                    )
                })
                .collect(),
        };
        let computed =
            reconcile_wb_official_report(&rows, &stored.details_evidence, Some(&baseline))
                .map_err(|_| ReportingReadError::InvalidPublishedData)?;
        if normalize_comparison(computed) != normalize_comparison(stored.comparison.clone()) {
            return Err(ReportingReadError::InvalidPublishedData);
        }
    }
    result.state = DataState::Complete;
    result.report = Some(WbOfficialReportProvenance {
        snapshot_id: stored.snapshot_id.to_string(),
        date_from: stored.summary.scope.date_from.to_string(),
        date_to: stored.summary.scope.date_to.to_string(),
        created_date: stored.summary.created_date.to_string(),
        period: match stored.summary.scope.period {
            WbFinanceReportPeriod::Daily => "daily",
            WbFinanceReportPeriod::Weekly => "weekly",
        }
        .to_owned(),
        currency: stored.summary.scope.currency.clone(),
        row_count: u32::try_from(count).map_err(|_| ReportingReadError::InvalidPublishedData)?,
        summary_amounts: stored
            .summary
            .amounts
            .into_iter()
            .map(|(field, amount)| {
                (
                    field,
                    FinancialLedgerAmount {
                        units: amount.units.to_string(),
                        scale: amount.scale,
                    },
                )
            })
            .collect(),
        summary_source_sha256: stored.summary_evidence.source_sha256,
        details_source_sha256: stored.details_evidence.source_sha256,
        content_sha256: stored.content_sha256,
        terminal_http_status: 204,
        published_at: timestamp_string(stored.published_at),
    });
    result.comparison = Some(public_comparison(stored.comparison)?);
    result.rows = rows
        .into_iter()
        .map(super::finance_ledger::public_row)
        .collect();
    result.next_after_rrd_id = has_more.then(|| cursor.to_string());
    Ok(result)
}

fn validate_report(
    account: &AccountScope,
    query: WbReportReconciliationQuery,
    stored: &StoredWbOfficialReport,
) -> Result<(), ReportingReadError> {
    let scope = &stored.summary.scope;
    let digest = |value: &str| {
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    if stored.snapshot_id <= 0
        || scope.account_id != account.account_id()
        || scope.report_id != query.report_id
        || scope.currency.len() != 3
        || !scope.currency.bytes().all(|b| b.is_ascii_uppercase())
        || scope.date_to < scope.date_from
        || !usize::try_from(stored.row_count).is_ok_and(|count| count <= WB_FINANCE_MAX_ROWS)
        || !digest(&stored.content_sha256)
        || stored.summary_evidence.scope != *scope
        || stored.details_evidence.scope != *scope
        || !stored.summary_evidence.terminal_observed
        || !stored.details_evidence.terminal_observed
        || !stored.summary_evidence.covers_entire_report
        || !stored.details_evidence.covers_entire_report
        || !digest(&stored.summary_evidence.source_sha256)
        || !digest(&stored.details_evidence.source_sha256)
        || stored.summary_evidence.source_sha256 == stored.details_evidence.source_sha256
        || stored.summary_evidence.observation_id == stored.details_evidence.observation_id
        || stored.summary.amounts.is_empty()
        || stored.summary.amounts.iter().any(|(name, amount)| {
            !WB_REPORT_SUMMARY_AMOUNTS.contains(&name.as_str()) || amount.scale > 18
        })
        || stored.comparison.mapping_version != WB_OFFICIAL_MAPPING_VERSION
    {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    validate_comparison(&stored.comparison)?;
    for column in &stored.comparison.columns {
        let expected = stored
            .summary
            .amounts
            .get(&column.summary_column)
            .map(|amount| {
                normalize_amount(ExactFinanceTotal {
                    units: amount.units,
                    scale: amount.scale,
                })
            });
        if expected != column.summary_total.map(normalize_amount) {
            return Err(ReportingReadError::InvalidPublishedData);
        }
    }
    Ok(())
}

fn validate_comparison(comparison: &WbOfficialComparison) -> Result<(), ReportingReadError> {
    if comparison.columns.is_empty() {
        return if comparison.status == WbOfficialComparisonStatus::Unavailable
            && comparison.unavailable_reason.is_some()
        {
            Ok(())
        } else {
            Err(ReportingReadError::InvalidPublishedData)
        };
    }
    if comparison.columns.len() != 2 {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    for (column, (detail, summary)) in comparison
        .columns
        .iter()
        .zip([("retailAmount", "retailAmountSum"), ("forPay", "forPaySum")])
    {
        validate_comparison_column(column, detail, summary)?;
    }
    let (expected, reason) = expected_comparison_status(&comparison.columns);
    if comparison.status != expected || comparison.unavailable_reason != reason {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    Ok(())
}

/// Checks one column against its fixed detail/summary pair: bounded scales, a
/// status consistent with its difference, and an exact detail-minus-summary
/// difference.
fn validate_comparison_column(
    column: &WbOfficialColumnComparison,
    detail: &str,
    summary: &str,
) -> Result<(), ReportingReadError> {
    let amounts_valid = [column.detail_total, column.summary_total, column.difference]
        .into_iter()
        .flatten()
        .all(|amount| amount.scale <= 18);
    if !amounts_valid {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    let status_valid = match column.status {
        WbOfficialComparisonStatus::PrimaryTotalsMatch => {
            column
                .difference
                .is_some_and(|difference| difference.units == 0)
                && column.unavailable_reason.is_none()
        }
        WbOfficialComparisonStatus::Mismatch => {
            column
                .difference
                .is_some_and(|difference| difference.units != 0)
                && column.unavailable_reason.is_none()
        }
        WbOfficialComparisonStatus::Unavailable => {
            column.difference.is_none() && column.unavailable_reason.is_some()
        }
    };
    let expected_difference = column
        .detail_total
        .zip(column.summary_total)
        .map(|(detail, summary)| subtract(detail, summary))
        .transpose()?;
    let difference_valid =
        expected_difference.map(normalize_amount) == column.difference.map(normalize_amount);
    if column.detail_column != detail
        || column.summary_column != summary
        || !status_valid
        || !difference_valid
    {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    Ok(())
}

/// The report-level status the columns imply: any unavailable reason wins,
/// then any mismatch, otherwise the primary totals match.
fn expected_comparison_status(
    columns: &[WbOfficialColumnComparison],
) -> (WbOfficialComparisonStatus, Option<WbOfficialUnavailable>) {
    let reason = columns.iter().find_map(|column| column.unavailable_reason);
    let status = if reason.is_some() {
        WbOfficialComparisonStatus::Unavailable
    } else if columns
        .iter()
        .any(|column| column.status == WbOfficialComparisonStatus::Mismatch)
    {
        WbOfficialComparisonStatus::Mismatch
    } else {
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    };
    (status, reason)
}

fn subtract(
    detail: ExactFinanceTotal,
    summary: ExactFinanceTotal,
) -> Result<ExactFinanceTotal, ReportingReadError> {
    let scale = detail.scale.max(summary.scale);
    if scale > 18 {
        return Err(ReportingReadError::InvalidPublishedData);
    }
    let at_scale =
        |amount: ExactFinanceTotal| amount.units.checked_mul(10_i128.pow(scale - amount.scale));
    let units = at_scale(detail)
        .zip(at_scale(summary))
        .and_then(|(detail, summary)| detail.checked_sub(summary))
        .ok_or(ReportingReadError::InvalidPublishedData)?;
    Ok(ExactFinanceTotal { units, scale })
}

const fn normalize_amount(mut amount: ExactFinanceTotal) -> ExactFinanceTotal {
    while amount.scale > 0 && amount.units % 10 == 0 {
        amount.units /= 10;
        amount.scale -= 1;
    }
    amount
}

fn normalize_comparison(mut comparison: WbOfficialComparison) -> WbOfficialComparison {
    for column in &mut comparison.columns {
        column.detail_total = column.detail_total.map(normalize_amount);
        column.summary_total = column.summary_total.map(normalize_amount);
        column.difference = column.difference.map(normalize_amount);
    }
    comparison
}

fn public_comparison(
    value: WbOfficialComparison,
) -> Result<WbReportComparison, ReportingReadError> {
    let reason = |value| {
        serde_json::to_value(value)
            .map(|value| value.as_str().map(str::to_owned))
            .map_err(|_| ReportingReadError::InvalidPublishedData)
    };
    let amount = |value: ExactFinanceTotal| FinancialLedgerAmount {
        units: value.units.to_string(),
        scale: value.scale,
    };
    Ok(WbReportComparison {
        mapping_version: value.mapping_version,
        status: value.status.into(),
        unavailable_reason: reason(value.unavailable_reason)?,
        columns: value
            .columns
            .into_iter()
            .map(|column| {
                Ok(WbReportColumnComparison {
                    detail_column: column.detail_column,
                    summary_column: column.summary_column,
                    detail_total: column.detail_total.map(amount),
                    summary_total: column.summary_total.map(amount),
                    difference: column.difference.map(amount),
                    status: column.status.into(),
                    unavailable_reason: reason(column.unavailable_reason)?,
                })
            })
            .collect::<Result<_, ReportingReadError>>()?,
    })
}

#[cfg(test)]
mod tests;
