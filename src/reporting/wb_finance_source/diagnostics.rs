//! Fixed-schema counters for protocol failures; never upstream field values.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use serde::Serialize;
use serde_json::Value;

use super::{
    AMOUNT_FIELDS, MAX_SIGNED_ID, WB_FINANCE_PAGE_SIZE, parse_decimal, parse_row, valid_currency,
    valid_type_text,
};

#[derive(Debug, Serialize)]
pub struct WbFinancePageDiagnostics {
    page_type: &'static str,
    row_count: Option<usize>,
    scanned_rows: usize,
    max_decimal_scale: usize,
    max_date_bytes: usize,
    issues: Vec<ProtocolIssue>,
}

#[derive(Debug, Serialize)]
struct ProtocolIssue {
    field: &'static str,
    reason: &'static str,
    observed_type: &'static str,
    rows: usize,
}

type Issues = BTreeMap<(&'static str, &'static str, &'static str), usize>;

/// Diagnose a rejected by-ID finance page using fixed schema counters.
///
/// Output contains field names, scalar type labels, counts and format lengths.
/// No field value, unknown key,
/// document text, ID, date, monetary amount or credential is emitted.
///
/// A valid page returns None. At most 1,000 rows are inspected. This helper
/// does not relax the normalizer or convert an invalid response into success.
#[must_use]
pub fn diagnose_finance_page(
    response: &Value,
    expected_report_id: u64,
    expected_currency: &str,
    after_rrd_id: u64,
) -> Option<WbFinancePageDiagnostics> {
    let mut result = WbFinancePageDiagnostics {
        page_type: value_type(Some(response)),
        row_count: response.as_array().map(Vec::len),
        scanned_rows: 0,
        max_decimal_scale: 0,
        max_date_bytes: 0,
        issues: Vec::new(),
    };
    let mut issues = Issues::new();
    let Some(rows) = response.as_array() else {
        record(&mut issues, "page", "expected_array", Some(response));
        return Some(finish(result, issues));
    };
    if rows.is_empty() || rows.len() > WB_FINANCE_PAGE_SIZE as usize {
        record(&mut issues, "page", "invalid_row_count", Some(response));
    }
    let mut cursor = after_rrd_id;
    for row in rows.iter().take(WB_FINANCE_PAGE_SIZE as usize) {
        result.scanned_rows += 1;
        let Some(fields) = row.as_object() else {
            record(&mut issues, "row", "expected_object", Some(row));
            continue;
        };
        if parse_row(row).is_err() {
            record(&mut issues, "row", "normalization_failed", None);
        }
        diagnose_ids(fields, expected_report_id, &mut cursor, &mut issues);
        diagnose_currency(fields.get("currency"), expected_currency, &mut issues);
        diagnose_date(fields.get("rrDate"), &mut result, &mut issues);
        diagnose_optional_scalars(fields, &mut issues);
        diagnose_amounts(fields, &mut result, &mut issues);
    }
    if issues.is_empty() {
        None
    } else {
        Some(finish(result, issues))
    }
}

type Fields = serde_json::Map<String, Value>;

fn diagnose_ids(fields: &Fields, expected_report_id: u64, cursor: &mut u64, issues: &mut Issues) {
    for field in ["reportId", "rrdId", "nmId"] {
        let value = fields.get(field);
        if field == "nmId" && value.is_none_or(Value::is_null) {
            continue;
        }
        if value
            .and_then(Value::as_u64)
            .is_none_or(|id| id > MAX_SIGNED_ID || (id == 0 && field != "nmId"))
        {
            record(issues, field, "expected_int64_id", value);
        }
    }
    if fields
        .get("reportId")
        .and_then(Value::as_u64)
        .is_some_and(|id| id != expected_report_id)
    {
        record(issues, "reportId", "scope_mismatch", fields.get("reportId"));
    }
    if let Some(id) = fields.get("rrdId").and_then(Value::as_u64) {
        if id <= *cursor {
            record(
                issues,
                "rrdId",
                "non_increasing_cursor",
                fields.get("rrdId"),
            );
        }
        *cursor = id;
    }
}

fn diagnose_currency(value: Option<&Value>, expected_currency: &str, issues: &mut Issues) {
    match value.and_then(Value::as_str) {
        Some(currency) if valid_currency(currency) => {
            if currency != expected_currency {
                record(issues, "currency", "scope_mismatch", value);
            }
        }
        _ => record(issues, "currency", "invalid_currency", value),
    }
}

fn diagnose_optional_scalars(fields: &Fields, issues: &mut Issues) {
    for field in ["docTypeName", "sellerOperName"] {
        let value = fields.get(field).filter(|value| !value.is_null());
        if value.is_some_and(|value| value.as_str().is_none_or(|text| !valid_type_text(text))) {
            record(issues, field, "invalid_bounded_text", value);
        }
    }
    let quantity = fields.get("quantity").filter(|value| !value.is_null());
    if quantity.is_some_and(|value| value.as_i64().is_none()) {
        record(issues, "quantity", "expected_int64", quantity);
    }
}

fn diagnose_amounts(fields: &Fields, result: &mut WbFinancePageDiagnostics, issues: &mut Issues) {
    let mut present_amounts = 0;
    for field in AMOUNT_FIELDS {
        let Some(value) = fields.get(*field).filter(|value| !value.is_null()) else {
            continue;
        };
        present_amounts += 1;
        let Some(raw) = value.as_str() else {
            record(issues, field, "expected_decimal_string", Some(value));
            continue;
        };
        let scale = raw
            .split_once('.')
            .map_or(0, |(_, fraction)| fraction.len());
        result.max_decimal_scale = result.max_decimal_scale.max(scale);
        if parse_decimal(raw).is_err() {
            record(
                issues,
                field,
                decimal_failure_reason(raw, scale),
                Some(value),
            );
        }
    }
    if present_amounts == 0 {
        record(issues, "amounts", "no_amounts_present", None);
    }
}

const fn decimal_failure_reason(raw: &str, scale: usize) -> &'static str {
    if scale > 18 {
        "decimal_scale_exceeds_18"
    } else if raw.len() > 32 {
        "decimal_bytes_exceed_32"
    } else {
        "invalid_decimal_format_or_i128_overflow"
    }
}

fn diagnose_date(
    value: Option<&Value>,
    result: &mut WbFinancePageDiagnostics,
    issues: &mut Issues,
) {
    let Some(raw) = value.and_then(Value::as_str) else {
        record(issues, "rrDate", "expected_date_string", value);
        return;
    };
    result.max_date_bytes = result.max_date_bytes.max(raw.len());
    let canonical = raw.len() == 10
        && NaiveDate::parse_from_str(raw, "%Y-%m-%d").is_ok_and(|date| date.to_string() == raw);
    if canonical {
        return;
    }
    let reason = if raw.len() <= 128 && DateTime::parse_from_rfc3339(raw).is_ok() {
        if raw.ends_with('Z') {
            "datetime_utc_instead_of_date"
        } else {
            "datetime_offset_instead_of_date"
        }
    } else if raw.len() <= 128 && NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
    {
        "datetime_without_offset_instead_of_date"
    } else {
        "noncanonical_date_format"
    };
    record(issues, "rrDate", reason, value);
}

fn record(issues: &mut Issues, field: &'static str, reason: &'static str, value: Option<&Value>) {
    *issues
        .entry((field, reason, value_type(value)))
        .or_default() += 1;
}

fn finish(mut result: WbFinancePageDiagnostics, issues: Issues) -> WbFinancePageDiagnostics {
    result.issues = issues
        .into_iter()
        .map(|((field, reason, observed_type), rows)| ProtocolIssue {
            field,
            reason,
            observed_type,
            rows,
        })
        .collect();
    result
}

fn value_type(value: Option<&Value>) -> &'static str {
    match value {
        None => "missing",
        Some(Value::Null) => "null",
        Some(Value::Bool(_)) => "boolean",
        Some(Value::Number(value)) if value.is_i64() || value.is_u64() => "integer",
        Some(Value::Number(_)) => "number",
        Some(Value::String(_)) => "string",
        Some(Value::Array(_)) => "array",
        Some(Value::Object(_)) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row() -> Value {
        json!({"rrdId":1,"reportId":2,"rrDate":"2026-09-06","currency":"RUB",
            "nmId":3,"quantity":1,"docTypeName":"Продажа","retailAmount":"1.00"})
    }

    #[test]
    fn valid_page_is_silent_and_failures_expose_only_fixed_counters() {
        assert!(diagnose_finance_page(&json!([row()]), 2, "RUB", 0).is_none());
        let mut bad = row();
        bad["rrDate"] = json!("2026-09-06T00:00:00Z");
        bad["retailAmount"] = json!("9876.1234567891234567891");
        bad["supplierSecret"] = json!("private_value_do_not_emit");
        let diagnostics = diagnose_finance_page(&json!([bad]), 2, "RUB", 1).unwrap();
        let text = serde_json::to_string(&diagnostics).unwrap();
        assert!(text.contains("datetime_utc_instead_of_date"));
        assert!(text.contains("decimal_scale_exceeds_18"));
        assert!(text.contains("non_increasing_cursor"));
        for forbidden in ["2026", "9876", "supplierSecret", "private_value", "Продажа"] {
            assert!(!text.contains(forbidden));
        }
    }

    #[test]
    fn scope_and_type_mismatches_are_counted_without_values() {
        let mut bad = row();
        bad["reportId"] = json!(55);
        bad["quantity"] = json!("private quantity");
        bad["forPay"] = json!(4.567);
        bad["currency"] = json!("USD");
        let text = serde_json::to_string(&diagnose_finance_page(&json!([bad, row()]), 2, "RUB", 0))
            .unwrap();
        assert!(text.contains("expected_decimal_string"));
        assert!(text.contains("scope_mismatch"));
        assert!(
            !text.contains("private quantity") && !text.contains("USD") && !text.contains("4.567")
        );
        for body in [Value::Null, json!({"private":[]}), json!([])] {
            assert!(diagnose_finance_page(&body, 2, "RUB", 0).is_some());
        }
    }
}
