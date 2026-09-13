//! Exact WB financial-column comparisons, without inferred payout formulas.
//!
//! Matching raw columns proves only equality of those columns against an
//! independently acquired baseline. It never certifies net sales, profit or
//! the official report balance. WB document types affect the business signs
//! of some columns; corrections and cashback use different rules. The official
//! report-list mapping remains explicitly unavailable until verified.

use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::wb_finance_source::{WB_FINANCE_MAX_ROWS, WbFinanceDecimal, WbFinanceDetailRow};

/// Raw measures are separate columns, never an additive accrual vector.
pub const WB_RAW_AMOUNT_COLUMNS: &[&str] = &[
    "retailPrice",
    "retailAmount",
    "retailPriceWithDisc",
    "ppvzSalesCommission",
    "forPay",
    "ppvzReward",
    "acquiringFee",
    "vw",
    "vwNds",
    "deliveryService",
    "penalty",
    "additionalPayment",
    "rebillLogisticCost",
    "paidStorage",
    "deduction",
    "paidAcceptance",
    "installmentCofinancingAmount",
    "cashbackAmount",
    "cashbackDiscount",
    "cashbackCommissionChange",
    "paymentSchedule",
];

const MAX_SCALE: u32 = 9;
const MAX_SIGNED_ID: u64 = u64::MAX >> 1;

/// Exact `units / 10^scale`; JSON uses a string coefficient to preserve values
/// above JavaScript's integer precision and above a single row's i64 bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactFinanceTotal {
    #[serde(with = "string_i128")]
    pub units: i128,
    pub scale: u32,
}

impl ExactFinanceTotal {
    fn checked_add(self, value: WbFinanceDecimal) -> Result<Self, FinanceReconciliationError> {
        let scale = self.scale.max(value.scale);
        let left = self.rescaled_units(scale)?;
        let right = Self {
            units: i128::from(value.units),
            scale: value.scale,
        }
        .rescaled_units(scale)?;
        Ok(Self {
            units: left
                .checked_add(right)
                .ok_or(FinanceReconciliationError::InvalidAmount)?,
            scale,
        })
    }

    fn rescaled_units(self, scale: u32) -> Result<i128, FinanceReconciliationError> {
        if self.scale > scale || scale > MAX_SCALE {
            return Err(FinanceReconciliationError::InvalidAmount);
        }
        self.units
            .checked_mul(10_i128.pow(scale - self.scale))
            .ok_or(FinanceReconciliationError::InvalidAmount)
    }

    fn numeric_eq(self, other: Self) -> Result<bool, FinanceReconciliationError> {
        let scale = self.scale.max(other.scale);
        Ok(self.rescaled_units(scale)? == other.rescaled_units(scale)?)
    }
}

/// Classification describes the document label only. It does not authorize
/// multiplying any amount by a guessed sale/return sign.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WbFinanceDocumentKind {
    Sale,
    Return,
    Unspecified,
    Unknown,
}

#[must_use]
pub fn wb_finance_document_kind(value: Option<&str>) -> WbFinanceDocumentKind {
    match value {
        Some("Продажа") => WbFinanceDocumentKind::Sale,
        Some("Возврат") => WbFinanceDocumentKind::Return,
        None | Some("") => WbFinanceDocumentKind::Unspecified,
        Some(_) => WbFinanceDocumentKind::Unknown,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinanceColumnTotal {
    /// None if even one row lacks this column. A partial sum is not a total.
    pub total: Option<ExactFinanceTotal>,
    pub present_rows: usize,
    pub missing_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbFinanceReportTotals {
    pub report_id: u64,
    pub currency: String,
    pub row_count: usize,
    pub document_type_counts: BTreeMap<WbFinanceDocumentKind, usize>,
    pub columns: BTreeMap<String, FinanceColumnTotal>,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum FinanceReconciliationError {
    #[error("financial rows violate the normalized bounded contract")]
    InvalidRows,
    #[error("financial detail contains a duplicate row identifier")]
    DuplicateRow,
    #[error("financial comparison evidence or scope is invalid")]
    InvalidEvidence,
    #[error("financial comparison has an invalid or overflowing exact amount")]
    InvalidAmount,
}

/// Aggregates each raw column separately by report ID and currency.
///
/// Unknown
/// documents and positive return values are retained exactly as supplied.
/// Empty input produces no reports, never synthetic zero-balance reports.
pub fn aggregate_wb_finance_rows(
    rows: &[WbFinanceDetailRow],
) -> Result<Vec<WbFinanceReportTotals>, FinanceReconciliationError> {
    if rows.len() > WB_FINANCE_MAX_ROWS {
        return Err(FinanceReconciliationError::InvalidRows);
    }
    let mut seen = BTreeSet::new();
    let mut reports: BTreeMap<(u64, String), WbFinanceReportTotals> = BTreeMap::new();
    for row in rows {
        row.validate()
            .map_err(|_| FinanceReconciliationError::InvalidRows)?;
        if !seen.insert(row.rrd_id) {
            return Err(FinanceReconciliationError::DuplicateRow);
        }
        let report = reports
            .entry((row.report_id, row.currency.clone()))
            .or_insert_with(|| empty_report(row.report_id, &row.currency));
        report.row_count += 1;
        *report
            .document_type_counts
            .entry(wb_finance_document_kind(row.document_type.as_deref()))
            .or_default() += 1;
        for (name, column) in &mut report.columns {
            if let Some(amount) = row.amounts.get(name) {
                column.present_rows += 1;
                if let Some(total) = column.total {
                    column.total = Some(total.checked_add(*amount)?);
                }
            } else {
                column.missing_rows += 1;
                column.total = None;
            }
        }
    }
    Ok(reports.into_values().collect())
}

fn empty_report(report_id: u64, currency: &str) -> WbFinanceReportTotals {
    WbFinanceReportTotals {
        report_id,
        currency: currency.to_owned(),
        row_count: 0,
        document_type_counts: BTreeMap::new(),
        columns: WB_RAW_AMOUNT_COLUMNS
            .iter()
            .map(|name| {
                (
                    (*name).to_owned(),
                    FinanceColumnTotal {
                        total: Some(ExactFinanceTotal { units: 0, scale: 0 }),
                        present_rows: 0,
                        missing_rows: 0,
                    },
                )
            })
            .collect(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WbFinanceReportPeriod {
    Daily,
    Weekly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbFinanceReportScope {
    pub account_id: String,
    pub report_id: u64,
    pub currency: String,
    pub period: WbFinanceReportPeriod,
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
}

/// Receipt from a trusted collector/importer, not a model-selected API URL.
/// A hash identifies evidence; it does not verify credentials or authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbFinanceComparisonEvidence {
    pub scope: WbFinanceReportScope,
    pub observation_id: String,
    /// Digest of the separately retained, normalized source evidence.
    pub source_sha256: String,
    /// HTTP 204 for API details, or equivalent explicit export completeness.
    pub terminal_observed: bool,
    /// A terminal response for a subset of days does not prove a whole report.
    pub covers_entire_report: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WbFinanceBaselineKind {
    /// Same raw columns independently collected by report ID or seller export.
    IndependentRawColumns,
    /// `/sales-reports/list` totals. Business-sign mapping is not yet verified.
    OfficialReportSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbFinanceBaseline {
    pub kind: WbFinanceBaselineKind,
    pub evidence: WbFinanceComparisonEvidence,
    /// Raw baseline keys match `WB_RAW_AMOUNT_COLUMNS`. Official summary keys
    /// cannot currently be mapped, so that kind always remains unavailable.
    pub totals: BTreeMap<String, ExactFinanceTotal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinanceComparisonStatus {
    /// Equality of every explicitly compared raw column; never net profit or
    /// official financial reconciliation, and never columns absent in baseline.
    RawColumnsMatch,
    Mismatch,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinanceComparisonUnavailable {
    MissingBaseline,
    IncompleteDetails,
    IncompleteBaseline,
    ScopeMismatch,
    NonIndependentBaseline,
    UnverifiedOfficialSummaryMapping,
    MissingColumnAmount,
    EmptyBaseline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinanceColumnComparison {
    pub column: String,
    pub detail_total: Option<ExactFinanceTotal>,
    pub baseline_total: ExactFinanceTotal,
    pub status: FinanceComparisonStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbFinanceComparison {
    pub status: FinanceComparisonStatus,
    pub unavailable_reason: Option<FinanceComparisonUnavailable>,
    pub columns: Vec<FinanceColumnComparison>,
}

/// Compares a report with an independent baseline of the same scope.
///
/// Requires whole-report terminal evidence, matching account/report/currency/
/// period and independently acquired baseline provenance. This deterministic
/// check neither fetches data nor certifies the authenticity of that receipt.
pub fn reconcile_wb_finance_report(
    details: &WbFinanceReportTotals,
    evidence: &WbFinanceComparisonEvidence,
    baseline: Option<&WbFinanceBaseline>,
) -> Result<WbFinanceComparison, FinanceReconciliationError> {
    validate_evidence(evidence)?;
    validate_totals(details)?;
    if details.report_id != evidence.scope.report_id || details.currency != evidence.scope.currency
    {
        return Err(FinanceReconciliationError::InvalidEvidence);
    }
    if !evidence.terminal_observed || !evidence.covers_entire_report {
        return Ok(unavailable(FinanceComparisonUnavailable::IncompleteDetails));
    }
    let Some(baseline) = baseline else {
        return Ok(unavailable(FinanceComparisonUnavailable::MissingBaseline));
    };
    validate_evidence(&baseline.evidence)?;
    if baseline.evidence.scope != evidence.scope {
        return Ok(unavailable(FinanceComparisonUnavailable::ScopeMismatch));
    }
    if !baseline.evidence.terminal_observed || !baseline.evidence.covers_entire_report {
        return Ok(unavailable(
            FinanceComparisonUnavailable::IncompleteBaseline,
        ));
    }
    if baseline.evidence.observation_id == evidence.observation_id
        || baseline.evidence.source_sha256 == evidence.source_sha256
    {
        return Ok(unavailable(
            FinanceComparisonUnavailable::NonIndependentBaseline,
        ));
    }
    if baseline.kind == WbFinanceBaselineKind::OfficialReportSummary {
        return Ok(unavailable(
            FinanceComparisonUnavailable::UnverifiedOfficialSummaryMapping,
        ));
    }
    if baseline.totals.is_empty() {
        return Ok(unavailable(FinanceComparisonUnavailable::EmptyBaseline));
    }
    compare_columns(details, &baseline.totals)
}

fn compare_columns(
    details: &WbFinanceReportTotals,
    baseline: &BTreeMap<String, ExactFinanceTotal>,
) -> Result<WbFinanceComparison, FinanceReconciliationError> {
    let mut result = WbFinanceComparison {
        status: FinanceComparisonStatus::RawColumnsMatch,
        unavailable_reason: None,
        columns: Vec::new(),
    };
    for (name, expected) in baseline {
        if !WB_RAW_AMOUNT_COLUMNS.contains(&name.as_str()) || expected.scale > MAX_SCALE {
            return Err(FinanceReconciliationError::InvalidAmount);
        }
        let actual = details.columns.get(name).and_then(|column| column.total);
        let status = match actual {
            None => FinanceComparisonStatus::Unavailable,
            Some(value) if value.numeric_eq(*expected)? => FinanceComparisonStatus::RawColumnsMatch,
            Some(_) => FinanceComparisonStatus::Mismatch,
        };
        if status == FinanceComparisonStatus::Unavailable {
            result.status = FinanceComparisonStatus::Unavailable;
            result.unavailable_reason = Some(FinanceComparisonUnavailable::MissingColumnAmount);
        } else if status == FinanceComparisonStatus::Mismatch
            && result.status != FinanceComparisonStatus::Unavailable
        {
            result.status = FinanceComparisonStatus::Mismatch;
        }
        result.columns.push(FinanceColumnComparison {
            column: name.clone(),
            detail_total: actual,
            baseline_total: *expected,
            status,
        });
    }
    Ok(result)
}

const fn unavailable(reason: FinanceComparisonUnavailable) -> WbFinanceComparison {
    WbFinanceComparison {
        status: FinanceComparisonStatus::Unavailable,
        unavailable_reason: Some(reason),
        columns: Vec::new(),
    }
}

fn validate_evidence(
    evidence: &WbFinanceComparisonEvidence,
) -> Result<(), FinanceReconciliationError> {
    let scope = &evidence.scope;
    if !valid_identifier(&scope.account_id)
        || !valid_identifier(&evidence.observation_id)
        || scope.report_id == 0
        || scope.report_id > MAX_SIGNED_ID
        || !valid_currency(&scope.currency)
        || scope.date_to < scope.date_from
        || evidence.source_sha256.len() != 64
        || !evidence
            .source_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(FinanceReconciliationError::InvalidEvidence);
    }
    Ok(())
}

fn validate_totals(details: &WbFinanceReportTotals) -> Result<(), FinanceReconciliationError> {
    if details.row_count == 0
        || details.row_count > WB_FINANCE_MAX_ROWS
        || details.report_id == 0
        || details.report_id > MAX_SIGNED_ID
        || !valid_currency(&details.currency)
        || details.columns.len() != WB_RAW_AMOUNT_COLUMNS.len()
        || details
            .document_type_counts
            .values()
            .try_fold(0_usize, |sum, count| sum.checked_add(*count))
            != Some(details.row_count)
    {
        return Err(FinanceReconciliationError::InvalidRows);
    }
    for (name, column) in &details.columns {
        if !WB_RAW_AMOUNT_COLUMNS.contains(&name.as_str())
            || column.present_rows.checked_add(column.missing_rows) != Some(details.row_count)
            || column.total.is_some() != (column.missing_rows == 0)
            || column.total.is_some_and(|total| total.scale > MAX_SCALE)
        {
            return Err(FinanceReconciliationError::InvalidRows);
        }
    }
    Ok(())
}

fn valid_currency(value: &str) -> bool {
    value.len() == 3 && value.bytes().all(|byte| byte.is_ascii_uppercase())
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

mod string_i128 {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(value: &i128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i128, D::Error> {
        let raw = String::deserialize(deserializer)?;
        let value = raw
            .parse::<i128>()
            .map_err(|_| D::Error::custom("invalid exact finance coefficient"))?;
        if value.to_string() != raw {
            return Err(D::Error::custom("noncanonical exact finance coefficient"));
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests;
