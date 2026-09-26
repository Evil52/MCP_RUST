//! SKU-based fulfillment totals from `/v4/product/info/stocks`.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::{
    CollectedStockFact, OzonReportParseError, PRODUCT_PAGE_ROWS, array_field, array_field_value,
    field, parse_u64,
};

/// New dimensions distinguish corrected SKU/available totals from historical
/// `FBO`/`FBS`/`RFBS` rows that contained product IDs and gross inventory.
#[must_use]
pub fn is_sku_fulfillment_dimension(value: &str) -> bool {
    matches!(
        value,
        "sku-fulfillment-v2:fbo" | "sku-fulfillment-v2:fbs" | "sku-fulfillment-v2:rfbs"
    )
}

fn dimension(value: &Value) -> Result<&'static str, OzonReportParseError> {
    match value.as_str() {
        Some("fbo" | "FBO") => Ok("sku-fulfillment-v2:fbo"),
        Some("fbs" | "FBS") => Ok("sku-fulfillment-v2:fbs"),
        Some("rfbs" | "RFBS") => Ok("sku-fulfillment-v2:rfbs"),
        _ => Err(OzonReportParseError::Value),
    }
}

fn positive_id(value: &Value) -> Result<u64, OzonReportParseError> {
    let value = parse_u64(value)?;
    if value == 0 || value > i64::MAX.cast_unsigned() {
        return Err(OzonReportParseError::Value);
    }
    Ok(value)
}

/// Normalizes the actual SKU and available quantity per fulfillment row.
///
/// The parent `product_id` is never a substitute. Available quantity is
/// present minus reserved, with neither
/// missing reserves nor underflow silently converted to zero. Duplicate
/// SKU/scheme rows are ambiguous totals and must not be counted twice.
pub fn parse_stock_page(response: &Value) -> Result<Vec<CollectedStockFact>, OzonReportParseError> {
    let items = array_field(response, "items")?;
    if items.len() > PRODUCT_PAGE_ROWS {
        return Err(OzonReportParseError::TooManyRows);
    }
    let mut facts = BTreeMap::new();
    let mut products = BTreeSet::new();
    for item in items {
        let item = item.as_object().ok_or(OzonReportParseError::Shape)?;
        let product_id = positive_id(field(Some(item), "product_id")?)?;
        if !products.insert(product_id) {
            return Err(OzonReportParseError::Value);
        }
        let stocks = array_field_value(item.get("stocks"))?;
        if stocks.len() > PRODUCT_PAGE_ROWS {
            return Err(OzonReportParseError::TooManyRows);
        }
        for stock in stocks {
            let stock = stock.as_object().ok_or(OzonReportParseError::Shape)?;
            let warehouse_id = dimension(field(Some(stock), "type")?)?;
            let sku = positive_id(field(Some(stock), "sku")?)?;
            let present = parse_u64(field(Some(stock), "present")?)?;
            let reserved = parse_u64(field(Some(stock), "reserved")?)?;
            let sellable_units = present
                .checked_sub(reserved)
                .filter(|units| *units <= i32::MAX.cast_unsigned().into())
                .ok_or(OzonReportParseError::Value)?;
            if facts.insert((sku, warehouse_id), sellable_units).is_some() {
                return Err(OzonReportParseError::Value);
            }
        }
    }
    Ok(facts
        .into_iter()
        .map(|((sku, warehouse_id), sellable_units)| CollectedStockFact {
            sku,
            warehouse_id: warehouse_id.to_owned(),
            sellable_units,
        })
        .collect())
}
