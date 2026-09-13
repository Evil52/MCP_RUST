//! Official WB report summaries and complete details collected independently.
//!
//! The fixed endpoints are `/api/finance/v1/sales-reports/list` and
//! `/api/finance/v1/sales-reports/detailed/{reportId}`. Only an observed HTTP
//! 204 completes a traversal. Financial decimals and signed-int64 IDs never
//! pass through floating point, and checkpoints retain only normalized fields.
//!
//! Contract checked on 2026-09-13 against the official documentation mirror:
//! <https://dev.wildberries.cn/docs/openapi/documents-and-accounting>.

mod transport;
pub use transport::{
    WbClientOfficialReportTransport, WbOfficialReportFuture, WbOfficialReportTransport,
};

use std::{collections::BTreeMap, fmt::Write};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{
    checkpoint::{Checkpoints, checkpointed},
    finance_reconciliation::{
        ExactFinanceTotal, WbFinanceBaseline, WbFinanceBaselineKind, WbFinanceComparisonEvidence,
        WbFinanceReportPeriod, WbFinanceReportScope,
    },
    wb_finance_source::{
        WB_FINANCE_FIELDS, WB_FINANCE_MAX_ROWS, WB_FINANCE_PAGE_SIZE, WbFinanceDecimal,
        WbFinanceDetailRow, parse_decimal, parse_page,
    },
    wb_source::WbReportSourceError,
};

const MAX_PAGES: usize = 1_000;
const MAX_SIGNED_ID: u64 = u64::MAX >> 1;
const FIRST_REPORT_DATE: NaiveDate =
    NaiveDate::from_ymd_opt(2025, 1, 1).expect("valid constant date");

/// Explicit normalized summary projection.
///
/// WB's list endpoint has no `fields`
/// parameter; seller names and every other unneeded field are discarded before
/// checkpointing. Missing financial values remain absent, never zero.
pub const WB_REPORT_SUMMARY_AMOUNTS: &[&str] = &[
    "retailAmountSum",
    "forPaySum",
    "deliveryServiceSum",
    "paidStorageSum",
    "paidAcceptanceSum",
    "deductionSum",
    "penaltySum",
    "additionalPaymentSum",
    "cashbackAmountSum",
    "cashbackDiscountSum",
    "cashbackCommissionChangeSum",
    "paymentSchedule",
    "bankPaymentSum",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WbOfficialReportSummary {
    pub scope: WbFinanceReportScope,
    pub created_date: NaiveDate,
    /// Vendor report type, preserved without inventing its business meaning.
    pub report_type: i64,
    pub amounts: BTreeMap<String, WbFinanceDecimal>,
}

impl WbOfficialReportSummary {
    fn validate(&self) -> Result<(), WbReportSourceError> {
        validate_account(&self.scope.account_id)?;
        if !valid_id(self.scope.report_id)
            || !valid_currency(&self.scope.currency)
            || !valid_dates(self.scope.date_from, self.scope.date_to)
            || self.created_date < FIRST_REPORT_DATE
            || self.report_type < 0
            || self.report_type > i64::from(i32::MAX)
            || self.amounts.is_empty()
            || self.amounts.iter().any(|(name, amount)| {
                !WB_REPORT_SUMMARY_AMOUNTS.contains(&name.as_str()) || amount.scale > 18
            })
        {
            return Err(WbReportSourceError::InvalidResponse);
        }
        Ok(())
    }
}

/// Constructed only after traversing the list through a terminal HTTP 204.
/// A list is a discovery result; an explicit report selection binds the scope.
#[derive(Debug, Serialize)]
pub struct WbReportList {
    reports: Vec<WbOfficialReportSummary>,
}

impl WbReportList {
    #[must_use]
    pub fn reports(&self) -> &[WbOfficialReportSummary] {
        &self.reports
    }

    /// Select an exact report and currency whose reporting period has ended.
    /// `as_of` is the collector's trusted Moscow business date, not response
    /// text. An ongoing or not-yet-created report never becomes a baseline.
    pub fn select_closed(
        &self,
        report_id: u64,
        currency: &str,
        as_of: NaiveDate,
    ) -> Result<WbSelectedReport, WbReportSourceError> {
        if !valid_id(report_id) || !valid_currency(currency) {
            return Err(WbReportSourceError::InvalidSnapshotInput);
        }
        let summary = self
            .reports
            .iter()
            .find(|summary| summary.scope.report_id == report_id)
            .ok_or(WbReportSourceError::InvalidSnapshotInput)?;
        if summary.scope.currency != currency
            || summary.scope.date_to >= as_of
            || summary.created_date > as_of
            || summary.created_date < summary.scope.date_to
        {
            return Err(WbReportSourceError::InvalidSnapshotInput);
        }
        let evidence = evidence("wb_report_summary_v1", &summary.scope, summary)?;
        Ok(WbSelectedReport {
            summary: summary.clone(),
            evidence,
        })
    }
}

/// No Deserialize or unchecked public constructor: only a closed, normalized
/// report observed in a completely traversed official list is selectable.
#[derive(Debug, Serialize)]
pub struct WbSelectedReport {
    summary: WbOfficialReportSummary,
    evidence: WbFinanceComparisonEvidence,
}

impl WbSelectedReport {
    #[must_use]
    pub const fn summary(&self) -> &WbOfficialReportSummary {
        &self.summary
    }

    #[must_use]
    pub const fn evidence(&self) -> &WbFinanceComparisonEvidence {
        &self.evidence
    }

    #[must_use]
    pub fn baseline(&self) -> WbFinanceBaseline {
        WbFinanceBaseline {
            kind: WbFinanceBaselineKind::OfficialReportSummary,
            evidence: self.evidence.clone(),
            totals: self
                .summary
                .amounts
                .iter()
                .map(|(name, value)| {
                    (
                        name.clone(),
                        ExactFinanceTotal {
                            units: value.units,
                            scale: value.scale,
                        },
                    )
                })
                .collect(),
        }
    }
}

/// Full report-ID traversal. The evidence is separate from the summary source
/// and cannot be obtained from a date slice or an incomplete page collection.
#[derive(Debug, Serialize)]
pub struct WbCompleteReportDetails {
    rows: Vec<WbFinanceDetailRow>,
    evidence: WbFinanceComparisonEvidence,
}

impl WbCompleteReportDetails {
    pub async fn collect(
        transport: &dyn WbOfficialReportTransport,
        selected: &WbSelectedReport,
        checkpoints: &Checkpoints,
    ) -> Result<Self, WbReportSourceError> {
        let scope = &selected.summary.scope;
        if transport.account_id() != scope.account_id {
            return Err(WbReportSourceError::InvalidSnapshotInput);
        }
        let mut rows = Vec::new();
        let mut cursor = 0;
        for _ in 0..MAX_PAGES {
            let page: Option<Vec<WbFinanceDetailRow>> = checkpointed(
                checkpoints,
                json!([
                    "wb_report_id_details_v1",
                    scope,
                    selected.evidence.source_sha256,
                    WB_FINANCE_PAGE_SIZE,
                    cursor,
                    WB_FINANCE_FIELDS
                ]),
                || async {
                    transport
                        .report_details(scope.report_id, WB_FINANCE_PAGE_SIZE, cursor)
                        .await?
                        .as_ref()
                        .map(|response| {
                            let page = parse_page(response)?;
                            validate_detail_page(&page, scope, cursor)?;
                            Ok::<_, WbReportSourceError>(page)
                        })
                        .transpose()
                },
            )
            .await?;
            let Some(page) = page else {
                let evidence = evidence("wb_report_id_details_v1", scope, &rows)?;
                return Ok(Self { rows, evidence });
            };
            cursor = validate_detail_page(&page, scope, cursor)?;
            if rows.len().saturating_add(page.len()) > WB_FINANCE_MAX_ROWS {
                return Err(WbReportSourceError::PaginationLimit);
            }
            rows.extend(page);
        }
        Err(WbReportSourceError::PaginationLimit)
    }

    #[must_use]
    pub fn rows(&self) -> &[WbFinanceDetailRow] {
        &self.rows
    }

    #[must_use]
    pub const fn evidence(&self) -> &WbFinanceComparisonEvidence {
        &self.evidence
    }
}

pub async fn collect_report_list_checkpointed(
    transport: &dyn WbOfficialReportTransport,
    account_id: &str,
    date_from: NaiveDate,
    date_to: NaiveDate,
    period: WbFinanceReportPeriod,
    checkpoints: &Checkpoints,
) -> Result<WbReportList, WbReportSourceError> {
    validate_account(account_id)?;
    if transport.account_id() != account_id || !valid_dates(date_from, date_to) {
        return Err(WbReportSourceError::InvalidSnapshotInput);
    }
    let mut reports = BTreeMap::new();
    let mut offset = 0;
    for _ in 0..MAX_PAGES {
        let page: Option<Vec<WbOfficialReportSummary>> = checkpointed(
            checkpoints,
            json!([
                "wb_report_list_v1",
                account_id,
                date_from,
                date_to,
                period,
                WB_FINANCE_PAGE_SIZE,
                offset,
                WB_REPORT_SUMMARY_AMOUNTS
            ]),
            || async {
                transport
                    .list_reports(date_from, date_to, period, WB_FINANCE_PAGE_SIZE, offset)
                    .await?
                    .as_ref()
                    .map(|response| {
                        parse_summary_page(response, account_id, period, date_from, date_to)
                    })
                    .transpose()
            },
        )
        .await?;
        let Some(page) = page else {
            return Ok(WbReportList {
                reports: reports.into_values().collect(),
            });
        };
        validate_summary_page(&page, account_id, period, date_from, date_to)?;
        if reports.len().saturating_add(page.len()) > WB_FINANCE_MAX_ROWS {
            return Err(WbReportSourceError::PaginationLimit);
        }
        offset = offset
            .checked_add(
                u32::try_from(page.len()).map_err(|_| WbReportSourceError::PaginationLimit)?,
            )
            .ok_or(WbReportSourceError::PaginationLimit)?;
        for summary in page {
            // Duplicate IDs (including changed revisions) make this discovery
            // unstable. Never silently overwrite or merge them across pages.
            if reports.insert(summary.scope.report_id, summary).is_some() {
                return Err(WbReportSourceError::InvalidResponse);
            }
        }
    }
    Err(WbReportSourceError::PaginationLimit)
}

fn parse_summary_page(
    response: &Value,
    account_id: &str,
    period: WbFinanceReportPeriod,
    date_from: NaiveDate,
    date_to: NaiveDate,
) -> Result<Vec<WbOfficialReportSummary>, WbReportSourceError> {
    let rows = response
        .as_array()
        .ok_or(WbReportSourceError::InvalidResponse)?;
    if rows.is_empty() || rows.len() > WB_FINANCE_PAGE_SIZE as usize {
        return Err(WbReportSourceError::InvalidResponse);
    }
    let parsed = rows
        .iter()
        .map(|row| parse_summary(row, account_id, period))
        .collect::<Result<Vec<_>, _>>()?;
    validate_summary_page(&parsed, account_id, period, date_from, date_to)?;
    Ok(parsed)
}

fn parse_summary(
    row: &Value,
    account_id: &str,
    period: WbFinanceReportPeriod,
) -> Result<WbOfficialReportSummary, WbReportSourceError> {
    let row = row
        .as_object()
        .ok_or(WbReportSourceError::InvalidResponse)?;
    let mut amounts = BTreeMap::new();
    for name in WB_REPORT_SUMMARY_AMOUNTS {
        if let Some(value) = row.get(*name).filter(|value| !value.is_null()) {
            let raw = value.as_str().ok_or(WbReportSourceError::InvalidResponse)?;
            amounts.insert((*name).to_owned(), parse_decimal(raw)?);
        }
    }
    let summary = WbOfficialReportSummary {
        scope: WbFinanceReportScope {
            account_id: account_id.to_owned(),
            report_id: row
                .get("reportId")
                .and_then(Value::as_u64)
                .ok_or(WbReportSourceError::InvalidResponse)?,
            currency: row
                .get("currency")
                .and_then(Value::as_str)
                .ok_or(WbReportSourceError::InvalidResponse)?
                .to_owned(),
            period,
            date_from: parse_date(row.get("dateFrom"))?,
            date_to: parse_date(row.get("dateTo"))?,
        },
        created_date: parse_date(row.get("createDate"))?,
        report_type: row
            .get("reportType")
            .and_then(Value::as_i64)
            .ok_or(WbReportSourceError::InvalidResponse)?,
        amounts,
    };
    summary.validate()?;
    Ok(summary)
}

fn validate_summary_page(
    page: &[WbOfficialReportSummary],
    account_id: &str,
    period: WbFinanceReportPeriod,
    date_from: NaiveDate,
    date_to: NaiveDate,
) -> Result<(), WbReportSourceError> {
    if page.is_empty() || page.len() > WB_FINANCE_PAGE_SIZE as usize {
        return Err(WbReportSourceError::InvalidResponse);
    }
    let mut ids = std::collections::BTreeSet::new();
    for summary in page {
        summary.validate()?;
        if summary.scope.account_id != account_id
            || summary.scope.period != period
            // A weekly report can straddle the requested date interval, but
            // a wholly unrelated report must not enter the discovered scope.
            || summary.scope.date_to < date_from
            || summary.scope.date_from > date_to
            || !ids.insert(summary.scope.report_id)
        {
            return Err(WbReportSourceError::InvalidResponse);
        }
    }
    Ok(())
}

fn validate_detail_page(
    page: &[WbFinanceDetailRow],
    scope: &WbFinanceReportScope,
    mut cursor: u64,
) -> Result<u64, WbReportSourceError> {
    if page.is_empty() || page.len() > WB_FINANCE_PAGE_SIZE as usize {
        return Err(WbReportSourceError::InvalidResponse);
    }
    for row in page {
        row.validate()?;
        if row.report_id != scope.report_id
            || row.currency != scope.currency
            || row.rrd_id <= cursor
        {
            return Err(WbReportSourceError::InvalidResponse);
        }
        // `rrDate` can describe a historical correction. The authoritative
        // scope is the requested report ID; never drop rows by a date slice.
        cursor = row.rrd_id;
    }
    Ok(cursor)
}

fn evidence<T: Serialize>(
    source: &str,
    scope: &WbFinanceReportScope,
    normalized: &T,
) -> Result<WbFinanceComparisonEvidence, WbReportSourceError> {
    let bytes = serde_json::to_vec(&(source, scope, normalized))
        .map_err(|_| WbReportSourceError::InvalidResponse)?;
    let digest = Sha256::digest(bytes);
    let mut source_sha256 = String::with_capacity(64);
    for byte in digest {
        let _ = write!(source_sha256, "{byte:02x}");
    }
    Ok(WbFinanceComparisonEvidence {
        scope: scope.clone(),
        observation_id: format!("{source}_{source_sha256}"),
        source_sha256,
        terminal_observed: true,
        covers_entire_report: true,
    })
}

fn parse_date(value: Option<&Value>) -> Result<NaiveDate, WbReportSourceError> {
    let raw = value
        .and_then(Value::as_str)
        .ok_or(WbReportSourceError::InvalidResponse)?;
    let date = NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .map_err(|_| WbReportSourceError::InvalidResponse)?;
    if raw.len() != 10 || date.to_string() != raw {
        return Err(WbReportSourceError::InvalidResponse);
    }
    Ok(date)
}

fn validate_account(account_id: &str) -> Result<(), WbReportSourceError> {
    if account_id.is_empty()
        || account_id.len() > 128
        || !account_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
    {
        return Err(WbReportSourceError::InvalidSnapshotInput);
    }
    Ok(())
}

const fn valid_id(id: u64) -> bool {
    id > 0 && id <= MAX_SIGNED_ID
}

fn valid_currency(currency: &str) -> bool {
    currency.len() == 3 && currency.bytes().all(|byte| byte.is_ascii_uppercase())
}

fn valid_dates(from: NaiveDate, to: NaiveDate) -> bool {
    from >= FIRST_REPORT_DATE && (0..31).contains(&to.signed_duration_since(from).num_days())
}

#[cfg(test)]
mod tests;
