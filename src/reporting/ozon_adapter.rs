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

use super::postgres_collector::{
    CollectedAdvertisingFact, CollectedPriceFact, CollectedSalesFact, CollectedStockFact,
};

const MAX_PAGE_ROWS: usize = 1_000;
// Product stocks and prices may include a much larger nested structure than
// analytics rows. Keep their requested page small enough that a legitimate
// response remains well below the client's 2 MiB decoded-body ceiling.
const PRODUCT_PAGE_ROWS: usize = 100;
const MAX_CURSOR_BYTES: usize = 4_096;
const MAX_CAMPAIGN_TITLE_BYTES: usize = 512;

/// One campaign-day aggregate from the verified Ozon Performance daily
/// statistics response. This is deliberately not a `CollectedAdvertisingFact`:
/// the endpoint has no SKU dimension, so treating a campaign aggregate as a
/// per-SKU fact would produce a false attribution in a manager report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OzonPerformanceDailyCampaignFact {
    pub business_date: NaiveDate,
    pub campaign_id: u64,
    pub campaign_title: String,
    pub impressions: u64,
    pub clicks: u64,
    pub spend_minor: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
}

/// One exact Seller API request approved for the daily-report Ozon source.
///
/// The network runtime must submit it through [`crate::ozon::OzonClient`],
/// whose fixed read-only allowlist is the final egress control.
#[derive(Debug, Clone, PartialEq)]
pub struct OzonReportRequest {
    pub path: &'static str,
    pub payload: Value,
}

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

/// Builds the only sales request accepted by the daily-report normalizer.
///
/// It requires a non-empty, inclusive UTC business-date window and preserves
/// a fixed positional metrics contract for [`parse_sales_page`].
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
            "sort": [],
            "limit": MAX_PAGE_ROWS,
            "offset": offset,
        }),
    })
}

/// Builds the shared product-page request for current stocks or prices.
pub fn product_page_request(
    path: &'static str,
    cursor: Option<&str>,
) -> Result<OzonReportRequest, OzonReportParseError> {
    if !matches!(path, "/v4/product/info/stocks" | "/v5/product/info/prices")
        || cursor.is_some_and(|cursor| {
            cursor.is_empty()
                || cursor.len() > MAX_CURSOR_BYTES
                || cursor.bytes().any(|byte| byte.is_ascii_control())
        })
    {
        return Err(OzonReportParseError::Value);
    }
    Ok(OzonReportRequest {
        path,
        payload: serde_json::json!({
            "cursor": cursor.unwrap_or_default(),
            "filter": {"offer_id": [], "product_id": [], "visibility": "ALL"},
            "limit": PRODUCT_PAGE_ROWS,
        }),
    })
}

/// Normalizes `/v1/analytics/data` for dimensions `["sku", "day"]` and
/// metrics `["revenue", "ordered_units"]`.
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
        if metrics.len() != 2 {
            return Err(OzonReportParseError::Shape);
        }
        facts.push(CollectedSalesFact {
            business_date,
            sku,
            operational_gmv_minor: parse_minor(&metrics[0])?,
            ordered_units: parse_count(&metrics[1])?,
            cancelled_units: None,
            returned_units: None,
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
    if items.len() > PRODUCT_PAGE_ROWS {
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
    if items.len() > PRODUCT_PAGE_ROWS {
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

/// Normalizes the observed `/api/client/statistics/daily/json` response.
///
/// Ozon Performance returns campaign-day aggregates, not SKU rows. This
/// parser therefore stays separate from the per-SKU advertising persistence
/// contract until a verified product-level report is added. Amounts use the
/// Russian decimal comma observed in the live read, while a decimal point is
/// also accepted for an equivalent API representation.
pub fn parse_performance_daily_campaigns(
    response: &Value,
) -> Result<Vec<OzonPerformanceDailyCampaignFact>, OzonReportParseError> {
    let rows = array_field(response, "rows")?;
    if rows.len() > MAX_PAGE_ROWS {
        return Err(OzonReportParseError::TooManyRows);
    }
    let mut facts = Vec::with_capacity(rows.len());
    for row in rows {
        let row = row.as_object().ok_or(OzonReportParseError::Shape)?;
        let impressions = parse_u64(field(Some(row), "views")?)?;
        let clicks = parse_u64(field(Some(row), "clicks")?)?;
        if clicks > impressions {
            return Err(OzonReportParseError::Value);
        }
        facts.push(OzonPerformanceDailyCampaignFact {
            business_date: parse_date(field(Some(row), "date")?)?,
            campaign_id: parse_u64(field(Some(row), "id")?)?,
            campaign_title: parse_campaign_title(field(Some(row), "title")?)?,
            impressions,
            clicks,
            spend_minor: parse_performance_minor(field(Some(row), "moneySpent")?)?,
            attributed_orders: parse_u64(field(Some(row), "orders")?)?,
            attributed_revenue_minor: parse_performance_minor(field(Some(row), "ordersMoney")?)?,
        });
    }
    Ok(facts)
}

/// Converts the verified campaign-level Performance response into the
/// persistence contract. `sku = 0` is the explicit campaign-wide sentinel in
/// `daily_reporting.advertising_facts`; it must be rendered as unavailable,
/// never as a real product identifier.
pub fn parse_performance_daily_advertising(
    response: &Value,
) -> Result<Vec<CollectedAdvertisingFact>, OzonReportParseError> {
    parse_performance_daily_campaigns(response).map(|rows| {
        rows.into_iter()
            .map(|row| CollectedAdvertisingFact {
                business_date: row.business_date,
                campaign_id: row.campaign_id,
                sku: 0,
                impressions: row.impressions,
                clicks: row.clicks,
                spend_minor: row.spend_minor,
                attributed_orders: row.attributed_orders,
                attributed_revenue_minor: row.attributed_revenue_minor,
            })
            .collect()
    })
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

/// Ozon documents analytics `metrics` as doubles, including count metrics.
/// A count may therefore arrive as `5.0`, but never as a fractional or
/// negative value in this reporting contract.
fn parse_count(value: &Value) -> Result<u64, OzonReportParseError> {
    match value {
        Value::Number(number) => {
            if let Some(value) = number.as_u64() {
                return Ok(value);
            }
            let value = number.as_f64().ok_or(OzonReportParseError::Value)?;
            if !value.is_finite()
                || value.is_sign_negative()
                || value.fract() != 0.0
                || value > u64::MAX as f64
            {
                return Err(OzonReportParseError::Value);
            }
            Ok(value as u64)
        }
        Value::String(_) => parse_u64(value),
        _ => Err(OzonReportParseError::Value),
    }
}

fn parse_warehouse_kind(value: &Value) -> Result<String, OzonReportParseError> {
    let value = value.as_str().ok_or(OzonReportParseError::Value)?;
    // `/v4/product/info/stocks` currently returns lowercase fulfillment types
    // (`fbo` / `fbs`), while earlier verified fixtures used uppercase values.
    // The API meaning is identical, so persist one canonical identifier rather
    // than treating a casing-only upstream change as a new warehouse kind.
    match value {
        "fbo" | "FBO" => Ok("FBO".to_owned()),
        "fbs" | "FBS" => Ok("FBS".to_owned()),
        _ => Err(OzonReportParseError::Value),
    }
}

fn parse_campaign_title(value: &Value) -> Result<String, OzonReportParseError> {
    let value = value.as_str().ok_or(OzonReportParseError::Value)?;
    if value.is_empty()
        || value.len() > MAX_CAMPAIGN_TITLE_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(OzonReportParseError::Value);
    }
    Ok(value.to_owned())
}

fn parse_performance_minor(value: &Value) -> Result<u64, OzonReportParseError> {
    let Value::String(value) = value else {
        return parse_minor(value);
    };
    if value.matches(',').count() > 1 || value.contains(',') && value.contains('.') {
        return Err(OzonReportParseError::Value);
    }
    parse_minor(&Value::String(value.replace(',', ".")))
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
                "metrics": ["12.34", 5]
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

        let performance = parse_performance_daily_campaigns(&json!({"rows": [{
            "id": "35751912",
            "title": "Реклама с Тёмой",
            "date": "2026-08-16",
            "views": "3764",
            "clicks": "126",
            "moneySpent": "701,78",
            "orders": "0",
            "ordersMoney": "0,00"
        }]}))
        .unwrap();
        assert_eq!(performance[0].campaign_id, 35_751_912);
        assert_eq!(performance[0].spend_minor, 70_178);
        assert_eq!(performance[0].attributed_revenue_minor, 0);
        let persisted = parse_performance_daily_advertising(&json!({"rows": [{
            "id": "35751912",
            "title": "Реклама с Тёмой",
            "date": "2026-08-16",
            "views": "3764",
            "clicks": "126",
            "moneySpent": "701,78",
            "orders": "0",
            "ordersMoney": "0,00"
        }]}))
        .unwrap();
        assert_eq!(persisted[0].campaign_id, 35_751_912);
        assert_eq!(persisted[0].sku, 0);
        assert_eq!(persisted[0].spend_minor, 70_178);
    }

    #[test]
    fn stock_fulfillment_type_is_case_normalized() {
        let stocks = parse_stock_page(&json!({"items": [{
            "product_id": 123,
            "stocks": [{"type": "fbs", "present": 7}]
        }]}))
        .unwrap();
        assert_eq!(stocks[0].warehouse_id, "FBS");
    }

    #[test]
    fn analytics_count_metrics_accept_only_integral_doubles() {
        let accepted = parse_sales_page(&json!({
            "result": {"data": [{
                "dimensions": [{"id": "123"}, {"id": "2026-08-16"}],
                "metrics": ["1.00", 2.0]
            }]}
        }))
        .unwrap();
        assert_eq!(accepted[0].ordered_units, 2);
        assert_eq!(accepted[0].returned_units, None);

        let fractional = parse_sales_page(&json!({
            "result": {"data": [{
                "dimensions": [{"id": "123"}, {"id": "2026-08-16"}],
                "metrics": ["1.00", 1.5]
            }]}
        }));
        assert_eq!(fractional, Err(OzonReportParseError::Value));

        let invalid_kind = parse_sales_page(&json!({
            "result": {"data": [{
                "dimensions": [{"id": "123"}, {"id": "2026-08-16"}],
                "metrics": ["1.00", true]
            }]}
        }));
        assert_eq!(invalid_kind, Err(OzonReportParseError::Value));

        let string_count = parse_sales_page(&json!({
            "result": {"data": [{
                "dimensions": [{"id": "123"}, {"id": "2026-08-16"}],
                "metrics": ["1.00", "2"]
            }]}
        }))
        .unwrap();
        assert_eq!(string_count[0].ordered_units, 2);
    }

    #[test]
    fn report_requests_are_exact_and_fail_closed() {
        let from = NaiveDate::from_ymd_opt(2026, 8, 15).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        assert_eq!(
            sales_request(from, to, 7).unwrap(),
            OzonReportRequest {
                path: "/v1/analytics/data",
                payload: json!({
                    "date_from": "2026-08-15", "date_to": "2026-08-16",
                    "metrics": ["revenue", "ordered_units"],
                    "dimension": ["sku", "day"], "filters": [], "sort": [],
                    "limit": 1_000, "offset": 7,
                }),
            }
        );
        assert_eq!(sales_request(to, from, 0), Err(OzonReportParseError::Value));

        assert_eq!(
            product_page_request("/v4/product/info/stocks", Some("opaque-cursor"))
                .unwrap()
                .payload,
            json!({
                "cursor": "opaque-cursor",
                "filter": {"offer_id": [], "product_id": [], "visibility": "ALL"},
                "limit": 100,
            })
        );
        let oversized = "x".repeat(MAX_CURSOR_BYTES + 1);
        for cursor in [Some(""), Some("unsafe\n"), Some(oversized.as_str())] {
            assert!(product_page_request("/v5/product/info/prices", cursor).is_err());
        }
        assert!(product_page_request("/v1/product/update", None).is_err());
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
    fn performance_campaign_daily_contract_is_bounded_and_fail_closed() {
        assert_eq!(
            parse_performance_daily_campaigns(&json!({"rows": []})),
            Ok(Vec::new())
        );
        for response in [
            json!({}),
            json!({"rows": [{}]}),
            json!({"rows": [{
                "id":"1", "title":"x", "date":"2026-08-16", "views":"1",
                "clicks":"2", "moneySpent":"0", "orders":"0", "ordersMoney":"0"
            }]}),
            json!({"rows": [{
                "id":"1", "title":"x", "date":"2026-08-16", "views":"1",
                "clicks":"1", "moneySpent":"1,000.00", "orders":"0", "ordersMoney":"0"
            }]}),
            json!({"rows": [{
                "id":"1", "title":"", "date":"2026-08-16", "views":"1",
                "clicks":"1", "moneySpent":"0", "orders":"0", "ordersMoney":"0"
            }]}),
        ] {
            assert!(parse_performance_daily_campaigns(&response).is_err());
        }
        let numeric_money = parse_performance_daily_campaigns(&json!({"rows": [{
            "id":"1", "title":"x", "date":"2026-08-16", "views":"1",
            "clicks":"1", "moneySpent":1, "orders":"0", "ordersMoney":0
        }]}))
        .unwrap();
        assert_eq!(numeric_money[0].spend_minor, 100);
        assert_eq!(
            parse_performance_daily_campaigns(&json!({"rows": vec![json!({}); MAX_PAGE_ROWS + 1]})),
            Err(OzonReportParseError::TooManyRows)
        );
    }

    #[test]
    fn page_limits_and_all_validation_boundaries_fail_closed() {
        let too_many_sales = vec![json!({}); MAX_PAGE_ROWS + 1];
        assert_eq!(
            parse_sales_page(&json!({"result": {"data": too_many_sales}})),
            Err(OzonReportParseError::TooManyRows)
        );

        let too_many_stocks = vec![json!({}); PRODUCT_PAGE_ROWS + 1];
        assert_eq!(
            parse_stock_page(&json!({"items": too_many_stocks})),
            Err(OzonReportParseError::TooManyRows)
        );

        let too_many_prices = vec![json!({}); PRODUCT_PAGE_ROWS + 1];
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
