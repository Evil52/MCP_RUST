use serde_json::{Map, Value};

use super::{WbReportParseError, array, object};

/// Per-SKU rows of one campaign day, collected across its apps.
pub(super) fn campaign_day_product_rows(
    day: &Map<String, Value>,
) -> Result<Vec<&Map<String, Value>>, WbReportParseError> {
    let mut product_rows = Vec::new();
    let Some(apps) = day.get("apps") else {
        return Ok(product_rows);
    };
    for app in array(apps)? {
        let app = object(app)?;
        // Fullstats v3 uses nms. Reject ambiguous dual shapes.
        if app.contains_key("nm") && app.contains_key("nms") {
            return Err(WbReportParseError::Shape);
        }
        if let Some(products) = app.get("nms").or_else(|| app.get("nm")) {
            for product in array(products)? {
                product_rows.push(object(product)?);
            }
        }
    }
    Ok(product_rows)
}
