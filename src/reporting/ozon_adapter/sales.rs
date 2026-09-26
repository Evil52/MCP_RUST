use super::{
    MAX_PAGE_ROWS, NaiveDate, OzonReportParseError, OzonReportRequest, Value, array_field,
    array_field_value, field, object_field, parse_count, parse_date, parse_minor,
};

/// Builds the only sales request accepted by the daily-report normalizer.
///
/// It requires a non-empty, inclusive UTC business-date window and preserves
/// a fixed positional metrics contract for [`super::parse_sales_page`].
pub fn sales_request(
    date_from: NaiveDate,
    date_to: NaiveDate,
    offset: u32,
) -> Result<OzonReportRequest, OzonReportParseError> {
    if date_from > date_to {
        return Err(OzonReportParseError::Value);
    }
    Ok(OzonReportRequest {
        path: "/v1/analytics/data",
        payload: serde_json::json!({
            "date_from": date_from.format("%Y-%m-%d").to_string(),
            "date_to": date_to.format("%Y-%m-%d").to_string(),
            "metrics": ["revenue", "ordered_units"],
            "dimension": ["sku", "day"],
            "filters": [],
            // Changing sales metrics must not move rows across offsets.
            "sort": [{"key": "sku", "order": "ASC"}, {"key": "day", "order": "ASC"}],
            "limit": MAX_PAGE_ROWS,
            "offset": offset,
        }),
    })
}

/// Independent unfiltered day totals used only to verify a zero-only overlap.
pub(in crate::reporting) fn parse_sales_control_totals(
    response: &Value,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<crate::reporting::sales_integrity::SalesTotals, OzonReportParseError> {
    let result = object_field(response, "result")?;
    let rows = array_field_value(result.get("data"))?;
    if rows.len() > MAX_PAGE_ROWS {
        return Err(OzonReportParseError::TooManyRows);
    }
    let mut totals = crate::reporting::sales_integrity::SalesTotals::new();
    for row in rows {
        let dimensions = array_field(row, "dimensions")?;
        let metrics = array_field(row, "metrics")?;
        if dimensions.len() != 1 || metrics.len() != 2 {
            return Err(OzonReportParseError::Shape);
        }
        let day = parse_date(field(dimensions[0].as_object(), "id")?)?;
        if day < from
            || day > to
            || totals
                .insert(day, (parse_count(&metrics[1])?, parse_minor(&metrics[0])?))
                .is_some()
        {
            return Err(OzonReportParseError::Value);
        }
    }
    Ok(totals)
}
