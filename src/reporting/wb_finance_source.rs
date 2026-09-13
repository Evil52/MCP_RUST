//! Bounded collection of WB sales-report details, separate from additive accrual facts.
//!
//! Contract: `POST /api/finance/v1/sales-reports/detailed`, checked against
//! <https://dev.wildberries.ru/docs/openapi/financial-reports-and-accounting>
//! and the official documentation mirror on 2026-09-13. Requests use the
//! `daily` period, a fixed field projection and the last response row's `rrdId`.
//! Only HTTP 204 proves completion; an empty or short HTTP 200 does not.
//!
//! These amounts overlap: retail amount, commission and seller payout must not
//! be summed as independent accruals. Refund/correction signs are preserved
//! exactly as received alongside the document and operation types. Mapping
//! them into profit categories requires a separately verified reconciliation.

use std::{collections::BTreeMap, future::Future, pin::Pin};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::wb::{WbClient, WbError};

use super::{
    checkpoint::{Checkpoints, checkpointed},
    wb_source::WbReportSourceError,
};

pub const WB_FINANCE_PAGE_SIZE: u32 = 1_000;
pub const WB_FINANCE_MAX_ROWS: usize = 25_000;
const MAX_PAGES: usize = 1_000;
const MAX_DAYS: i64 = 31;
const MAX_TYPE_BYTES: usize = 512;
const MAX_DECIMAL_BYTES: usize = 32;
const MAX_DECIMAL_SCALE: u32 = 9;
const MAX_SIGNED_ID: u64 = u64::MAX >> 1;
const FIRST_SUPPORTED_DATE: NaiveDate =
    NaiveDate::from_ymd_opt(2024, 1, 29).expect("valid constant date");

/// The only requested fields. No customer identifiers, contact details,
/// supplier tax identifiers, office addresses, report links or raw documents.
pub const WB_FINANCE_FIELDS: &[&str] = &[
    "reportId",
    "rrdId",
    "rrDate",
    "nmId",
    "currency",
    "docTypeName",
    "sellerOperName",
    "quantity",
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

const AMOUNT_FIELDS: &[&str] = &[
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

/// Exact decimal: `units / 10^scale`. WB documents amounts as JSON strings,
/// including a three-decimal logistics amount. Never round them to kopecks
/// or deserialize them through floating point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WbFinanceDecimal {
    pub units: i64,
    pub scale: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WbFinanceDetailRow {
    pub rrd_id: u64,
    pub report_id: u64,
    /// WB's `rrDate`, kept separately from the requested report interval.
    pub business_date: NaiveDate,
    /// `nmId`, not WB's `sku` barcode field. Missing/zero means unattributed.
    pub sku: Option<u64>,
    pub currency: String,
    pub document_type: Option<String>,
    pub operation_type: Option<String>,
    pub quantity: Option<i64>,
    /// Missing/null upstream amounts are absent, never synthesized as zero.
    /// Keys are limited to the documented `AMOUNT_FIELDS` projection.
    pub amounts: BTreeMap<String, WbFinanceDecimal>,
}

impl WbFinanceDetailRow {
    /// Validate again after durable checkpoint replay or before persistence.
    pub fn validate(&self) -> Result<(), WbReportSourceError> {
        if self.rrd_id == 0
            || self.rrd_id > MAX_SIGNED_ID
            || self.report_id == 0
            || self.report_id > MAX_SIGNED_ID
            || self.sku.is_some_and(|sku| sku == 0 || sku > MAX_SIGNED_ID)
            || !valid_currency(&self.currency)
            || self.amounts.is_empty()
            || self.amounts.iter().any(|(name, value)| {
                !AMOUNT_FIELDS.contains(&name.as_str()) || value.scale > MAX_DECIMAL_SCALE
            })
            || [&self.document_type, &self.operation_type]
                .into_iter()
                .flatten()
                .any(|text| !valid_type_text(text))
        {
            return Err(WbReportSourceError::InvalidResponse);
        }
        Ok(())
    }
}

pub trait WbFinanceTransport: Send + Sync {
    /// `None` means an observed HTTP 204. HTTP 200 JSON null must stay
    /// `Some(Value::Null)` and fail validation rather than prove completion.
    /// Implementations fix the endpoint, daily period and field projection.
    fn finance_page<'a>(
        &'a self,
        start: NaiveDate,
        end: NaiveDate,
        limit: u32,
        rrd_id: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, WbReportSourceError>> + Send + 'a>>;
}

#[derive(Clone)]
pub struct WbClientFinanceTransport {
    client: WbClient,
    account_id: String,
}

impl WbClientFinanceTransport {
    #[must_use]
    pub const fn new(client: WbClient, account_id: String) -> Self {
        Self { client, account_id }
    }
}

impl WbFinanceTransport for WbClientFinanceTransport {
    fn finance_page<'a>(
        &'a self,
        start: NaiveDate,
        end: NaiveDate,
        limit: u32,
        rrd_id: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, WbReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .financial_report_page(&self.account_id, start, end, limit, rrd_id)
                .await
                .map_err(|error| match error {
                    WbError::RateLimited {
                        retry_after: Some(delay),
                        ..
                    }
                    | WbError::LocalRateLimited { retry_after: delay } => {
                        WbReportSourceError::RetryAfter {
                            seconds: super::checkpoint::delay_seconds(delay),
                        }
                    }
                    _ => WbReportSourceError::Upstream(error.kind()),
                })
        })
    }
}

pub async fn collect_finance_details_checkpointed(
    transport: &dyn WbFinanceTransport,
    date_from: NaiveDate,
    date_to: NaiveDate,
    checkpoints: &Checkpoints,
) -> Result<Vec<WbFinanceDetailRow>, WbReportSourceError> {
    let days = date_to.signed_duration_since(date_from).num_days();
    if date_from < FIRST_SUPPORTED_DATE || !(0..MAX_DAYS).contains(&days) {
        return Err(WbReportSourceError::InvalidSnapshotInput);
    }
    let mut result = Vec::new();
    let mut cursor = 0;
    for _ in 0..MAX_PAGES {
        let page = checkpointed(
            checkpoints,
            serde_json::json!([
                "wb_finance_details_v1",
                date_from,
                date_to,
                "daily",
                WB_FINANCE_PAGE_SIZE,
                cursor,
                WB_FINANCE_FIELDS
            ]),
            || async {
                transport
                    .finance_page(date_from, date_to, WB_FINANCE_PAGE_SIZE, cursor)
                    .await?
                    .as_ref()
                    .map(|response| {
                        let page = parse_page(response)?;
                        validate_page_cursor(&page, cursor)?;
                        Ok::<_, WbReportSourceError>(page)
                    })
                    .transpose()
            },
        )
        .await?;
        let Some(page) = page else {
            return Ok(result);
        };
        // Repeat validation after journal replay. Strictly increasing row IDs
        // prevent duplicate rows, repeated cursors and backwards traversal.
        let next_cursor = validate_page_cursor(&page, cursor)?;
        if result.len().saturating_add(page.len()) > WB_FINANCE_MAX_ROWS {
            return Err(WbReportSourceError::PaginationLimit);
        }
        cursor = next_cursor;
        result.extend(page);
    }
    Err(WbReportSourceError::PaginationLimit)
}

fn validate_page_cursor(
    page: &[WbFinanceDetailRow],
    mut cursor: u64,
) -> Result<u64, WbReportSourceError> {
    if page.is_empty() || page.len() > WB_FINANCE_PAGE_SIZE as usize {
        return Err(WbReportSourceError::InvalidResponse);
    }
    for row in page {
        row.validate()?;
        if row.rrd_id <= cursor {
            return Err(WbReportSourceError::InvalidResponse);
        }
        cursor = row.rrd_id;
    }
    Ok(cursor)
}

fn parse_page(response: &Value) -> Result<Vec<WbFinanceDetailRow>, WbReportSourceError> {
    let rows = response
        .as_array()
        .ok_or(WbReportSourceError::InvalidResponse)?;
    if rows.is_empty() || rows.len() > WB_FINANCE_PAGE_SIZE as usize {
        return Err(WbReportSourceError::InvalidResponse);
    }
    rows.iter().map(parse_row).collect()
}

fn parse_row(row: &Value) -> Result<WbFinanceDetailRow, WbReportSourceError> {
    let row = row
        .as_object()
        .ok_or(WbReportSourceError::InvalidResponse)?;
    let mut amounts = BTreeMap::new();
    for name in AMOUNT_FIELDS {
        if let Some(value) = row.get(*name).filter(|value| !value.is_null()) {
            let raw = value.as_str().ok_or(WbReportSourceError::InvalidResponse)?;
            amounts.insert((*name).to_owned(), parse_decimal(raw)?);
        }
    }
    let raw_date = row
        .get("rrDate")
        .and_then(Value::as_str)
        .ok_or(WbReportSourceError::InvalidResponse)?;
    let business_date = NaiveDate::parse_from_str(raw_date, "%Y-%m-%d")
        .map_err(|_| WbReportSourceError::InvalidResponse)?;
    if raw_date.len() != 10 || business_date.to_string() != raw_date {
        return Err(WbReportSourceError::InvalidResponse);
    }
    let normalized = WbFinanceDetailRow {
        rrd_id: required_u64(row.get("rrdId"))?,
        report_id: required_u64(row.get("reportId"))?,
        business_date,
        sku: row
            .get("nmId")
            .filter(|value| !value.is_null())
            .map(|value| required_u64(Some(value)))
            .transpose()?
            .filter(|value| *value != 0),
        currency: row
            .get("currency")
            .and_then(Value::as_str)
            .ok_or(WbReportSourceError::InvalidResponse)?
            .to_owned(),
        document_type: optional_type_text(row.get("docTypeName"))?,
        operation_type: optional_type_text(row.get("sellerOperName"))?,
        quantity: row
            .get("quantity")
            .filter(|value| !value.is_null())
            .map(|value| value.as_i64().ok_or(WbReportSourceError::InvalidResponse))
            .transpose()?,
        amounts,
    };
    normalized.validate()?;
    Ok(normalized)
}

fn required_u64(value: Option<&Value>) -> Result<u64, WbReportSourceError> {
    value
        .and_then(Value::as_u64)
        .ok_or(WbReportSourceError::InvalidResponse)
}

fn valid_currency(value: &str) -> bool {
    value.len() == 3 && value.bytes().all(|byte| byte.is_ascii_uppercase())
}

fn valid_type_text(value: &str) -> bool {
    value.len() <= MAX_TYPE_BYTES && !value.chars().any(char::is_control)
}

fn optional_type_text(value: Option<&Value>) -> Result<Option<String>, WbReportSourceError> {
    value
        .filter(|value| !value.is_null())
        .map(|value| {
            let text = value.as_str().ok_or(WbReportSourceError::InvalidResponse)?;
            if !valid_type_text(text) {
                return Err(WbReportSourceError::InvalidResponse);
            }
            Ok(text.to_owned())
        })
        .transpose()
}

fn parse_decimal(raw: &str) -> Result<WbFinanceDecimal, WbReportSourceError> {
    if raw.is_empty() || raw.len() > MAX_DECIMAL_BYTES {
        return Err(WbReportSourceError::InvalidResponse);
    }
    let (negative, unsigned) = raw.strip_prefix('-').map_or((false, raw), |s| (true, s));
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty()
        || (whole.len() > 1 && whole.starts_with('0'))
        || (unsigned.contains('.') && fraction.is_empty())
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return Err(WbReportSourceError::InvalidResponse);
    }
    let scale = u32::try_from(fraction.len()).map_err(|_| WbReportSourceError::InvalidResponse)?;
    if scale > MAX_DECIMAL_SCALE {
        return Err(WbReportSourceError::InvalidResponse);
    }
    let unsigned_units = whole
        .bytes()
        .chain(fraction.bytes())
        .try_fold(0_i128, |n, b| {
            n.checked_mul(10)
                .and_then(|n| n.checked_add(i128::from(b - b'0')))
                .ok_or(WbReportSourceError::InvalidResponse)
        })?;
    let signed_units = if negative {
        -unsigned_units
    } else {
        unsigned_units
    };
    let units = i64::try_from(signed_units).map_err(|_| WbReportSourceError::InvalidResponse)?;
    Ok(WbFinanceDecimal { units, scale })
}

#[cfg(test)]
mod tests;
