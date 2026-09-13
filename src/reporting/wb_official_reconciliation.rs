//! Reconcile two documented weekly WB summary totals against complete details.
//!
//! The official seller guide defines both totals as sale-document values minus
//! return-document values. Operations, quantity and the sign already present
//! in an amount do not replace the document-type sign. In particular, never
//! take absolute values of corrections. This is not a bank-payment or profit
//! reconciliation. See `docs/wb-official-reconciliation.md` for source evidence.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::finance_reconciliation::{
    ExactFinanceTotal, FinanceReconciliationError, WbFinanceBaseline, WbFinanceBaselineKind,
    WbFinanceComparisonEvidence, WbFinanceDocumentKind, WbFinanceReportPeriod,
    wb_finance_document_kind,
};
use super::wb_finance_source::{WB_FINANCE_MAX_ROWS, WbFinanceDecimal, WbFinanceDetailRow};

/// Version the business mapping independently of the source projection.
pub const WB_OFFICIAL_MAPPING_VERSION: &str = "wb_weekly_primary_totals_v1";

const COLUMNS: [(&str, &str); 2] = [("retailAmount", "retailAmountSum"), ("forPay", "forPaySum")];
const MAX_SCALE: u32 = 18;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WbOfficialComparisonStatus {
    /// Only the two primary totals match, not the final bank payment or profit.
    PrimaryTotalsMatch,
    Mismatch,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WbOfficialUnavailable {
    MissingBaseline,
    IncompleteDetails,
    IncompleteBaseline,
    ScopeMismatch,
    NonIndependentBaseline,
    UnsupportedPeriod,
    NotOfficialSummary,
    EmptyDetails,
    MissingSummaryAmount,
    MissingDetailAmount,
    UnsupportedDocumentType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbOfficialColumnComparison {
    pub detail_column: String,
    pub summary_column: String,
    pub detail_total: Option<ExactFinanceTotal>,
    pub summary_total: Option<ExactFinanceTotal>,
    /// Details minus summary, with no rounding or tolerance.
    pub difference: Option<ExactFinanceTotal>,
    pub status: WbOfficialComparisonStatus,
    pub unavailable_reason: Option<WbOfficialUnavailable>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbOfficialComparison {
    pub mapping_version: String,
    pub status: WbOfficialComparisonStatus,
    pub unavailable_reason: Option<WbOfficialUnavailable>,
    pub columns: Vec<WbOfficialColumnComparison>,
}

/// Compare primary weekly summary totals from an independently collected
/// `/sales-reports/list` record with all pages of `/detailed/{reportId}`.
///
/// Evidence must come from the trusted collection boundary. This pure function
/// checks scope, completeness and independent provenance; it cannot authenticate
/// a supplied receipt. A daily report, incomplete input or unknown nonzero
/// document semantics is unavailable rather than a synthetic zero or success.
pub fn reconcile_wb_official_report(
    rows: &[WbFinanceDetailRow],
    evidence: &WbFinanceComparisonEvidence,
    baseline: Option<&WbFinanceBaseline>,
) -> Result<WbOfficialComparison, FinanceReconciliationError> {
    validate_evidence(evidence)?;
    validate_rows(rows, evidence)?;
    if !evidence.terminal_observed || !evidence.covers_entire_report {
        return Ok(unavailable(WbOfficialUnavailable::IncompleteDetails));
    }
    let Some(baseline) = baseline else {
        return Ok(unavailable(WbOfficialUnavailable::MissingBaseline));
    };
    validate_evidence(&baseline.evidence)?;
    if evidence.scope != baseline.evidence.scope {
        return Ok(unavailable(WbOfficialUnavailable::ScopeMismatch));
    }
    if !baseline.evidence.terminal_observed || !baseline.evidence.covers_entire_report {
        return Ok(unavailable(WbOfficialUnavailable::IncompleteBaseline));
    }
    if evidence.observation_id == baseline.evidence.observation_id
        || evidence.source_sha256 == baseline.evidence.source_sha256
    {
        return Ok(unavailable(WbOfficialUnavailable::NonIndependentBaseline));
    }
    if baseline.kind != WbFinanceBaselineKind::OfficialReportSummary {
        return Ok(unavailable(WbOfficialUnavailable::NotOfficialSummary));
    }
    if evidence.scope.period != WbFinanceReportPeriod::Weekly {
        return Ok(unavailable(WbOfficialUnavailable::UnsupportedPeriod));
    }
    if rows.is_empty() {
        return Ok(unavailable(WbOfficialUnavailable::EmptyDetails));
    }
    let columns: Vec<_> = COLUMNS
        .iter()
        .map(|(detail, summary)| compare_column(rows, baseline, detail, summary))
        .collect::<Result<_, _>>()?;
    let unavailable_reason = columns.iter().find_map(|column| column.unavailable_reason);
    let status = if unavailable_reason.is_some() {
        WbOfficialComparisonStatus::Unavailable
    } else if columns
        .iter()
        .any(|column| column.status == WbOfficialComparisonStatus::Mismatch)
    {
        WbOfficialComparisonStatus::Mismatch
    } else {
        WbOfficialComparisonStatus::PrimaryTotalsMatch
    };
    Ok(WbOfficialComparison {
        mapping_version: WB_OFFICIAL_MAPPING_VERSION.to_owned(),
        status,
        unavailable_reason,
        columns,
    })
}

fn compare_column(
    rows: &[WbFinanceDetailRow],
    baseline: &WbFinanceBaseline,
    detail: &str,
    summary: &str,
) -> Result<WbOfficialColumnComparison, FinanceReconciliationError> {
    let summary_total = baseline.totals.get(summary).copied();
    if summary_total.is_some_and(|amount| amount.scale > MAX_SCALE) {
        return Err(FinanceReconciliationError::InvalidAmount);
    }
    let actual = signed_column_total(rows, detail)?;
    let (detail_total, unavailable_reason) = match actual {
        Ok(total) => (Some(total), None),
        Err(reason) => (None, Some(reason)),
    };
    let unavailable_reason = unavailable_reason.or_else(|| {
        summary_total
            .is_none()
            .then_some(WbOfficialUnavailable::MissingSummaryAmount)
    });
    let difference = detail_total
        .zip(summary_total)
        .map(|(actual, expected)| subtract(actual, expected))
        .transpose()?;
    let status = match difference {
        None => WbOfficialComparisonStatus::Unavailable,
        Some(amount) if amount.units == 0 => WbOfficialComparisonStatus::PrimaryTotalsMatch,
        Some(_) => WbOfficialComparisonStatus::Mismatch,
    };
    Ok(WbOfficialColumnComparison {
        detail_column: detail.to_owned(),
        summary_column: summary.to_owned(),
        detail_total,
        summary_total,
        difference,
        status,
        unavailable_reason,
    })
}

fn signed_column_total(
    rows: &[WbFinanceDetailRow],
    column: &str,
) -> Result<Result<ExactFinanceTotal, WbOfficialUnavailable>, FinanceReconciliationError> {
    let mut total = ExactFinanceTotal { units: 0, scale: 0 };
    for row in rows {
        let Some(amount) = row.amounts.get(column).copied() else {
            return Ok(Err(WbOfficialUnavailable::MissingDetailAmount));
        };
        let sign = match wb_finance_document_kind(row.document_type.as_deref()) {
            WbFinanceDocumentKind::Sale => 1,
            WbFinanceDocumentKind::Return => -1,
            WbFinanceDocumentKind::Unspecified | WbFinanceDocumentKind::Unknown => {
                // A zero is harmless regardless of document semantics. An
                // absent amount or an unknown nonzero sign is never guessed.
                if amount.units == 0 {
                    continue;
                }
                return Ok(Err(WbOfficialUnavailable::UnsupportedDocumentType));
            }
        };
        total = add_signed(total, amount, sign)?;
    }
    Ok(Ok(total))
}

fn add_signed(
    total: ExactFinanceTotal,
    amount: WbFinanceDecimal,
    sign: i128,
) -> Result<ExactFinanceTotal, FinanceReconciliationError> {
    let amount = ExactFinanceTotal {
        units: amount
            .units
            .checked_mul(sign)
            .ok_or(FinanceReconciliationError::InvalidAmount)?,
        scale: amount.scale,
    };
    let scale = total.scale.max(amount.scale);
    let units = rescale(total, scale)?
        .checked_add(rescale(amount, scale)?)
        .ok_or(FinanceReconciliationError::InvalidAmount)?;
    Ok(ExactFinanceTotal { units, scale })
}

fn subtract(
    actual: ExactFinanceTotal,
    expected: ExactFinanceTotal,
) -> Result<ExactFinanceTotal, FinanceReconciliationError> {
    let scale = actual.scale.max(expected.scale);
    let units = rescale(actual, scale)?
        .checked_sub(rescale(expected, scale)?)
        .ok_or(FinanceReconciliationError::InvalidAmount)?;
    Ok(ExactFinanceTotal { units, scale })
}

fn rescale(amount: ExactFinanceTotal, scale: u32) -> Result<i128, FinanceReconciliationError> {
    if amount.scale > scale || scale > MAX_SCALE {
        return Err(FinanceReconciliationError::InvalidAmount);
    }
    amount
        .units
        .checked_mul(10_i128.pow(scale - amount.scale))
        .ok_or(FinanceReconciliationError::InvalidAmount)
}

fn validate_rows(
    rows: &[WbFinanceDetailRow],
    evidence: &WbFinanceComparisonEvidence,
) -> Result<(), FinanceReconciliationError> {
    if rows.len() > WB_FINANCE_MAX_ROWS {
        return Err(FinanceReconciliationError::InvalidRows);
    }
    let mut seen = BTreeSet::new();
    for row in rows {
        row.validate()
            .map_err(|_| FinanceReconciliationError::InvalidRows)?;
        if row.report_id != evidence.scope.report_id || row.currency != evidence.scope.currency {
            return Err(FinanceReconciliationError::InvalidEvidence);
        }
        if !seen.insert(row.rrd_id) {
            return Err(FinanceReconciliationError::DuplicateRow);
        }
    }
    Ok(())
}

fn validate_evidence(
    evidence: &WbFinanceComparisonEvidence,
) -> Result<(), FinanceReconciliationError> {
    let scope = &evidence.scope;
    if !valid_identifier(&scope.account_id)
        || !valid_identifier(&evidence.observation_id)
        || scope.report_id == 0
        || scope.report_id > i64::MAX.unsigned_abs()
        || scope.currency.len() != 3
        || !scope.currency.bytes().all(|byte| byte.is_ascii_uppercase())
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

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn unavailable(reason: WbOfficialUnavailable) -> WbOfficialComparison {
    WbOfficialComparison {
        mapping_version: WB_OFFICIAL_MAPPING_VERSION.to_owned(),
        status: WbOfficialComparisonStatus::Unavailable,
        unavailable_reason: Some(reason),
        columns: Vec::new(),
    }
}

#[cfg(test)]
mod tests;
