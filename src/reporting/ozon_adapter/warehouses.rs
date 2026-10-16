//! Native physical warehouse stock normalization.
use super::{
    CollectedStockFact, MAX_CURSOR_BYTES, OzonReportParseError, PRODUCT_PAGE_ROWS, Value,
    array_field, field, invalid_cursor, parse_u64,
};

/// Normalizes one warehouse-granular FBO or FBS stock page.
///
/// Warehouse identifiers are prefixed with the fulfillment scheme. This
/// preserves the upstream numeric identifier while preventing an accidental
/// collision if Ozon ever reuses the same number in the two namespaces.
pub fn parse_warehouse_stock_page(
    response: &Value,
    scheme: &'static str,
) -> Result<Vec<CollectedStockFact>, OzonReportParseError> {
    if !matches!(scheme, "fbo" | "fbs") {
        return Err(OzonReportParseError::Value);
    }
    let products = array_field(response, "products")?;
    if products.len() > PRODUCT_PAGE_ROWS {
        return Err(OzonReportParseError::TooManyRows);
    }
    products
        .iter()
        .map(|product| {
            let product = product.as_object().ok_or(OzonReportParseError::Shape)?;
            let present = parse_u64(field(Some(product), "present")?)?;
            let reserved = product
                .get("reserved")
                .filter(|v| !v.is_null())
                .map(parse_u64)
                .transpose()?;
            if present > i32::MAX.cast_unsigned().into()
                || reserved.is_some_and(|units| units > present)
            {
                return Err(OzonReportParseError::Value);
            }
            let sellable_units = if scheme == "fbs" {
                parse_u64(field(Some(product), "free_stock")?)?
            } else {
                present
                    .checked_sub(reserved.ok_or(OzonReportParseError::Shape)?)
                    .ok_or(OzonReportParseError::Value)?
            };
            let warehouse = field(Some(product), "warehouse_id")?;
            let warehouse_id = warehouse
                .as_i64()
                .or_else(|| warehouse.as_str().and_then(|v| v.parse::<i64>().ok()))
                .ok_or(OzonReportParseError::Value)?;
            if warehouse_id == 0 {
                return Err(OzonReportParseError::Value);
            }
            Ok(CollectedStockFact {
                sku: parse_u64(field(Some(product), "sku")?)?,
                warehouse_id: format!("{scheme}:{warehouse_id}"),
                sellable_units,
                present_units: Some(present),
                reserved_units: reserved,
            })
        })
        .collect()
}

/// Extracts the next cursor from a warehouse stock response without guessing.
pub fn next_warehouse_stock_cursor(
    response: &Value,
) -> Result<Option<String>, OzonReportParseError> {
    let object = response.as_object().ok_or(OzonReportParseError::Shape)?;
    let has_next = field(Some(object), "has_next")?
        .as_bool()
        .ok_or(OzonReportParseError::Shape)?;
    let cursor = field(Some(object), "cursor")?
        .as_str()
        .ok_or(OzonReportParseError::Shape)?;
    if has_next {
        if invalid_cursor(cursor) {
            return Err(OzonReportParseError::Value);
        }
        Ok(Some(cursor.to_owned()))
    } else if cursor.len() <= MAX_CURSOR_BYTES
        && !cursor.bytes().any(|byte| byte.is_ascii_control())
    {
        Ok(None)
    } else {
        Err(OzonReportParseError::Value)
    }
}
