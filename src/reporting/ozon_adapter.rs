//! Strict normalization of the read-only Ozon Seller responses used by daily
//! reports.
//!
//! This module deliberately parses only the three Seller API sources whose
//! response envelopes have been verified against a bounded live read: sales
//! analytics, current stock and current prices. It performs no I/O and never
//! resolves credentials; the future network adapter must pass its responses
//! through these functions before a snapshot can be persisted.

use std::collections::BTreeMap;

use chrono::NaiveDate;
use serde_json::Value;
use thiserror::Error;

use super::postgres_collector::{CollectedPriceFact, CollectedSalesFact, CollectedStockFact};

const MAX_PAGE_ROWS: usize = 1_000;
const MAX_WAREHOUSE_KIND_BYTES: usize = 64;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonReportParseError {
    #[error("Ozon report response has an unsupported shape")]
    Shape,
    #[error("Ozon report response has an invalid value")]
    Value,
    #[error("Ozon report response is too large")]
    TooManyRows,
    #[error("Ozon report response has an unsupported currency")]
    Currency,
}

/// Normalizes `/v1/analytics/data` for dimensions `["sku", "day"]` and
/// metrics `["revenue", "ordered_units", "cancellations", "returns"]`.
///
/// The metric order is part of this local contract. A different query must
/// not be parsed by this function because positional metric arrays otherwise
/// permit silent swaps between revenue and counts.
pub fn parse_sales_page(response: &Value) -> Result<Vec<CollectedSalesFact>, OzonReportParseError> {
    let result = object_field(response, "result")?;
    let data = array_field_value(result.get("data"))?;
    if data.len() > MAX_PAGE_ROWS {
        return Err(OzonReportParseError::TooManyRows);
    }
    let mut facts = Vec::with_capacity(data.len());
    for row in data {
        let row = row.as_object().ok_or(OzonReportParseError::Shape)?;
        let dimensions = array_field_value(row.get("dimensions"))?;
        if dimensions.len() != 2 {
            return Err(OzonReportParseError::Shape);
        }
        let sku = parse_u64(field(dimensions[0].as_object(), "id")?)?;
        let business_date = parse_date(field(dimensions[1].as_object(), "id")?)?;
        let metrics = array_field_value(row.get("metrics"))?;
        if metrics.len() != 4 {
            return Err(OzonReportParseError::Shape);
        }
        facts.push(CollectedSalesFact {
            business_date,
            sku,
            operational_gmv_minor: parse_minor(&metrics[0])?,
            ordered_units: parse_u64(&metrics[1])?,
            cancelled_units: parse_u64(&metrics[2])?,
            returned_units: parse_u64(&metrics[3])?,
        });
    }
    Ok(facts)
}

/// Normalizes one `/v4/product/info/stocks` page.
///
/// Ozon reports inventory by fulfillment type in the verified envelope. The
/// daily-report storage calls this dimension `warehouse_id`; using the type as
/// its stable value prevents a made-up warehouse split and lets report-level
/// stock sum FBO/FBS rows correctly.
pub fn parse_stock_page(response: &Value) -> Result<Vec<CollectedStockFact>, OzonReportParseError> {
    let items = array_field(response, "items")?;
    if items.len() > MAX_PAGE_ROWS {
        return Err(OzonReportParseError::TooManyRows);
    }
    let mut totals = BTreeMap::<(u64, String), u64>::new();
    for item in items {
        let item = item.as_object().ok_or(OzonReportParseError::Shape)?;
        let sku = parse_u64(field(Some(item), "product_id")?)?;
        for stock in array_field_value(item.get("stocks"))? {
            let stock = stock.as_object().ok_or(OzonReportParseError::Shape)?;
            let kind = parse_warehouse_kind(field(Some(stock), "type")?)?;
            let present = parse_u64(field(Some(stock), "present")?)?;
            let total = totals.entry((sku, kind)).or_default();
            *total = total
                .checked_add(present)
                .ok_or(OzonReportParseError::Value)?;
        }
    }
    Ok(totals
        .into_iter()
        .map(|((sku, warehouse_id), sellable_units)| CollectedStockFact {
            sku,
            warehouse_id,
            sellable_units,
        })
        .collect())
}

/// Normalizes one `/v5/product/info/prices` page.
///
/// Only RUB prices are accepted because report storage is denominated in
/// integer kopecks. `old_price` of zero denotes an absent before-price.
pub fn parse_price_page(response: &Value) -> Result<Vec<CollectedPriceFact>, OzonReportParseError> {
    let items = array_field(response, "items")?;
    if items.len() > MAX_PAGE_ROWS {
        return Err(OzonReportParseError::TooManyRows);
    }
    let mut facts = Vec::with_capacity(items.len());
    for item in items {
        let item = item.as_object().ok_or(OzonReportParseError::Shape)?;
        let price = object_field_value(item.get("price"))?;
        if field(Some(price), "currency_code")?.as_str() != Some("RUB") {
            return Err(OzonReportParseError::Currency);
        }
        let old_price = parse_minor(field(Some(price), "old_price")?)?;
        facts.push(CollectedPriceFact {
            sku: parse_u64(field(Some(item), "product_id")?)?,
            price_minor: parse_minor(field(Some(price), "price")?)?,
            old_price_minor: (old_price != 0).then_some(old_price),
        });
    }
    Ok(facts)
}

fn object_field<'a>(
    value: &'a Value,
    name: &str,
) -> Result<&'a serde_json::Map<String, Value>, OzonReportParseError> {
    object_field_value(value.get(name))
}

fn object_field_value(
    value: Option<&Value>,
) -> Result<&serde_json::Map<String, Value>, OzonReportParseError> {
    value
        .and_then(Value::as_object)
        .ok_or(OzonReportParseError::Shape)
}

fn array_field<'a>(value: &'a Value, name: &str) -> Result<&'a Vec<Value>, OzonReportParseError> {
    array_field_value(value.get(name))
}

fn array_field_value(value: Option<&Value>) -> Result<&Vec<Value>, OzonReportParseError> {
    value
        .and_then(Value::as_array)
        .ok_or(OzonReportParseError::Shape)
}

fn field<'a>(
    object: Option<&'a serde_json::Map<String, Value>>,
    name: &str,
) -> Result<&'a Value, OzonReportParseError> {
    object
        .and_then(|object| object.get(name))
        .ok_or(OzonReportParseError::Shape)
}

fn parse_date(value: &Value) -> Result<NaiveDate, OzonReportParseError> {
    NaiveDate::parse_from_str(
        value.as_str().ok_or(OzonReportParseError::Value)?,
        "%Y-%m-%d",
    )
    .map_err(|_| OzonReportParseError::Value)
}

fn parse_u64(value: &Value) -> Result<u64, OzonReportParseError> {
    match value {
        Value::Number(number) => number.as_u64().ok_or(OzonReportParseError::Value),
        Value::String(value) => value.parse().map_err(|_| OzonReportParseError::Value),
        _ => Err(OzonReportParseError::Value),
    }
}

fn parse_warehouse_kind(value: &Value) -> Result<String, OzonReportParseError> {
    let value = value.as_str().ok_or(OzonReportParseError::Value)?;
    if value.is_empty()
        || value.len() > MAX_WAREHOUSE_KIND_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(OzonReportParseError::Value);
    }
    Ok(value.to_owned())
}

fn parse_minor(value: &Value) -> Result<u64, OzonReportParseError> {
    let source = match value {
        Value::Number(number) => number.to_string(),
        Value::String(value) => value.to_owned(),
        _ => return Err(OzonReportParseError::Value),
    };
    let (whole, fraction) = source
        .split_once('.')
        .map_or((source.as_str(), ""), |(whole, fraction)| (whole, fraction));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 2
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(OzonReportParseError::Value);
    }
    let whole = whole
        .parse::<u64>()
        .map_err(|_| OzonReportParseError::Value)?;
    let fraction_bytes = fraction.as_bytes();
    let fraction = fraction_bytes
        .first()
        .map_or(0, |digit| u64::from(*digit - b'0') * 10)
        + fraction_bytes
            .get(1)
            .map_or(0, |digit| u64::from(*digit - b'0'));
    whole
        .checked_mul(100)
        .and_then(|minor| minor.checked_add(fraction))
        .ok_or(OzonReportParseError::Value)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn verified_ozon_envelopes_normalize_without_guessing() {
        let sales = parse_sales_page(&json!({
            "result": {"data": [{
                "dimensions": [{"id": "123"}, {"id": "2026-08-16"}],
                "metrics": ["12.34", 5, 1, 2]
            }]}
        }))
        .unwrap();
        assert_eq!(sales[0].operational_gmv_minor, 1_234);
        assert_eq!(sales[0].ordered_units, 5);

        let stocks = parse_stock_page(&json!({"items": [{
            "product_id": 123,
            "stocks": [
                {"type": "FBO", "present": 2},
                {"type": "FBO", "present": 3},
                {"type": "FBS", "present": 7}
            ]
        }]}))
        .unwrap();
        assert_eq!(stocks.len(), 2);
        assert_eq!(stocks[0].warehouse_id, "FBO");
        assert_eq!(stocks[0].sellable_units, 5);

        let prices = parse_price_page(&json!({"items": [{
            "product_id": 123,
            "price": {"currency_code": "RUB", "price": "99.95", "old_price": 0}
        }]}))
        .unwrap();
        assert_eq!(prices[0].price_minor, 9_995);
        assert_eq!(prices[0].old_price_minor, None);
    }

    #[test]
    fn unexpected_shapes_types_currencies_and_precisions_fail_closed() {
        for value in [
            json!({}),
            json!({"result": {"data": [{"dimensions": [], "metrics": []}]}}),
            json!({"result": {"data": [{
                "dimensions": [{"id": "1"}, {"id": "2026-08-16"}],
                "metrics": []
            }]}}),
        ] {
            assert!(parse_sales_page(&value).is_err());
        }
        assert_eq!(
            parse_sales_page(&json!({"result": {"data": []}})),
            Ok(Vec::new())
        );
        assert_eq!(
            parse_price_page(&json!({"items": [{
                "product_id": 1,
                "price": {"currency_code": "USD", "price": "1", "old_price": "0"}
            }]})),
            Err(OzonReportParseError::Currency)
        );
        assert!(
            parse_price_page(&json!({"items": [{
                "product_id": 1,
                "price": {"currency_code": "RUB", "price": "1.001", "old_price": "0"}
            }]}))
            .is_err()
        );
        assert!(
            parse_stock_page(&json!({"items": [{
                "product_id": 1,
                "stocks": [{"type": "mixed", "present": 1}]
            }]}))
            .is_err()
        );
    }

    #[test]
    fn minor_parser_rejects_negative_nonfinite_and_overflowing_values() {
        for value in [
            json!(-1),
            json!("1e3"),
            json!(""),
            json!("18446744073709551616"),
        ] {
            assert_eq!(parse_minor(&value), Err(OzonReportParseError::Value));
        }
        assert_eq!(parse_minor(&json!("0.1")), Ok(10));
        assert_eq!(parse_minor(&json!("42")), Ok(4_200));
    }

    #[test]
    fn page_limits_and_all_validation_boundaries_fail_closed() {
        let too_many_sales = vec![json!({}); MAX_PAGE_ROWS + 1];
        assert_eq!(
            parse_sales_page(&json!({"result": {"data": too_many_sales}})),
            Err(OzonReportParseError::TooManyRows)
        );

        let too_many_stocks = vec![json!({}); MAX_PAGE_ROWS + 1];
        assert_eq!(
            parse_stock_page(&json!({"items": too_many_stocks})),
            Err(OzonReportParseError::TooManyRows)
        );

        let too_many_prices = vec![json!({}); MAX_PAGE_ROWS + 1];
        assert_eq!(
            parse_price_page(&json!({"items": too_many_prices})),
            Err(OzonReportParseError::TooManyRows)
        );

        assert_eq!(
            parse_sales_page(&json!({
                "result": {"data": [{
                    "dimensions": [{"id": "1"}, {"id": "not-a-date"}],
                    "metrics": ["0", 0, 0, 0]
                }]}
            })),
            Err(OzonReportParseError::Value)
        );
        assert_eq!(
            parse_stock_page(&json!({"items": [{
                "product_id": 1,
                "stocks": [{"type": "", "present": 1}]
            }]})),
            Err(OzonReportParseError::Value)
        );
        assert_eq!(parse_u64(&json!(true)), Err(OzonReportParseError::Value));
        assert_eq!(parse_minor(&json!(true)), Err(OzonReportParseError::Value));
        assert_eq!(
            parse_minor(&json!("184467440737095516.16")),
            Err(OzonReportParseError::Value)
        );
    }
}
