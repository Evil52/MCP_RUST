//! Strict normalization of the read-only Ozon Seller responses used by daily
//! reports.
//!
//! This module parses only fixed read-only contracts whose response envelopes
//! have been verified against bounded live reads: sales analytics, real
//! warehouse stock, current prices and Performance SKU statistics. It performs
//! no I/O and never resolves credentials; the network adapter must pass its
//! responses through these functions before a snapshot can be persisted.

use std::collections::BTreeMap;

use chrono::NaiveDate;
use serde_json::Value;
use thiserror::Error;

use super::postgres_collector::{
    CollectedAdvertisingExpenseFact, CollectedAdvertisingFact, CollectedPriceFact,
    CollectedSalesFact, CollectedStockFact,
};

const MAX_PAGE_ROWS: usize = 1_000;
// SKU statistics are returned as one non-paginated campaign×SKU dataset.
// The Performance client already enforces a 2 MiB decoded-body ceiling; this
// higher parser guard keeps direct callers bounded without rejecting normal
// stores that legitimately have more than 1,000 advertised SKUs.
const MAX_PERFORMANCE_SKU_ROWS: usize = 20_000;
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

/// Builds one cursor page for the two warehouse-granular stock sources.
///
/// The endpoint is selected from a fixed pair so callers cannot use this
/// helper to smuggle an arbitrary Seller API path through the report source.
pub fn warehouse_stock_page_request(
    path: &'static str,
    cursor: Option<&str>,
) -> Result<OzonReportRequest, OzonReportParseError> {
    if !matches!(
        path,
        "/v1/product/info/stocks-by-warehouse/fbo" | "/v2/product/info/stocks-by-warehouse/fbs"
    ) || cursor.is_some_and(invalid_cursor)
    {
        return Err(OzonReportParseError::Value);
    }
    let mut payload = serde_json::json!({
        "cursor": cursor.unwrap_or_default(),
        "limit": PRODUCT_PAGE_ROWS,
    });
    let object = payload
        .as_object_mut()
        .expect("warehouse stock request payload is an object");
    if path.ends_with("/fbo") {
        object.insert("offer_ids".to_owned(), serde_json::json!([]));
        object.insert("skus".to_owned(), serde_json::json!([]));
    } else {
        object.insert("offer_id".to_owned(), serde_json::json!([]));
        object.insert("sku".to_owned(), serde_json::json!([]));
    }
    Ok(OzonReportRequest { path, payload })
}

/// Builds the shared product-page request for current stocks or prices.
pub fn product_page_request(
    path: &'static str,
    cursor: Option<&str>,
) -> Result<OzonReportRequest, OzonReportParseError> {
    if !matches!(path, "/v4/product/info/stocks" | "/v5/product/info/prices")
        || cursor.is_some_and(invalid_cursor)
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
            // Ozon returns and cancellations require separate, differently
            // attributed sources. Never turn unavailable events into zeros.
            cancelled_units: None,
            returned_units: None,
        });
    }
    Ok(facts)
}

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
            let sellable_units = if scheme == "fbs" {
                parse_u64(field(Some(product), "free_stock")?)?
            } else {
                let reserved = parse_u64(field(Some(product), "reserved")?)?;
                present
                    .checked_sub(reserved)
                    .ok_or(OzonReportParseError::Value)?
            };
            let warehouse_id = parse_u64(field(Some(product), "warehouse_id")?)?;
            if warehouse_id == 0 {
                return Err(OzonReportParseError::Value);
            }
            Ok(CollectedStockFact {
                sku: parse_u64(field(Some(product), "sku")?)?,
                warehouse_id: format!("{scheme}:{warehouse_id}"),
                sellable_units,
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

/// Normalizes the observed `/api/client/statistics/daily` response.
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
                basket_additions: 0,
                model_attributed_orders: 0,
                model_attributed_revenue_minor: 0,
                product_price_minor: 0,
                average_cpc_minor: None,
                cpm_minor: None,
                cpl_minor: None,
            })
            .collect()
    })
}

/// Normalizes the verified SKU-level Performance statistics response.
///
/// Unlike the legacy daily campaign aggregate, every row carries a real SKU,
/// so it can be persisted directly in `advertising_facts` without the `sku=0`
/// sentinel and joined to sales, prices and warehouse stock.
pub fn parse_performance_sku_advertising(
    response: &Value,
) -> Result<Vec<CollectedAdvertisingFact>, OzonReportParseError> {
    let rows = array_field(response, "rows")?;
    if rows.len() > MAX_PERFORMANCE_SKU_ROWS {
        return Err(OzonReportParseError::TooManyRows);
    }
    rows.iter()
        .map(|row| {
            let row = row.as_object().ok_or(OzonReportParseError::Shape)?;
            let campaign_id = parse_u64(field(Some(row), "campaignId")?)?;
            let sku = parse_u64(field(Some(row), "sku")?)?;
            let impressions = parse_u64(field(Some(row), "views")?)?;
            let clicks = parse_u64(field(Some(row), "clicks")?)?;
            let spend_minor = parse_performance_minor(field(Some(row), "expense")?)?;
            let basket_additions = parse_u64(field(Some(row), "toCart")?)?;
            if campaign_id == 0 || sku == 0 || clicks > impressions {
                return Err(OzonReportParseError::Value);
            }
            Ok(CollectedAdvertisingFact {
                business_date: parse_date(field(Some(row), "date")?)?,
                campaign_id,
                sku,
                impressions,
                clicks,
                spend_minor,
                attributed_orders: parse_u64(field(Some(row), "orders")?)?,
                attributed_revenue_minor: parse_performance_minor(field(Some(row), "sales")?)?,
                basket_additions,
                model_attributed_orders: parse_u64(field(Some(row), "modelOrders")?)?,
                model_attributed_revenue_minor: parse_performance_minor(field(
                    Some(row),
                    "modelSales",
                )?)?,
                // Ozon emits an empty string when the campaign row has no
                // observable product price. The persistence contract already
                // uses zero as the explicit "unavailable" sentinel for this
                // field; keep every other malformed monetary value fail-closed.
                product_price_minor: parse_optional_performance_price(field(Some(row), "price")?)?,
                average_cpc_minor: Some(parse_performance_minor(field(Some(row), "avgCpc")?)?),
                cpm_minor: per_thousand(spend_minor, impressions)?,
                cpl_minor: per_event(spend_minor, basket_additions)?,
            })
        })
        .collect()
}

pub fn parse_performance_expenses(
    response: &Value,
) -> Result<Vec<CollectedAdvertisingExpenseFact>, OzonReportParseError> {
    let rows = array_field(response, "rows")?;
    if rows.len() > MAX_PERFORMANCE_SKU_ROWS {
        return Err(OzonReportParseError::TooManyRows);
    }
    rows.iter()
        .map(|row| {
            let row = row.as_object().ok_or(OzonReportParseError::Shape)?;
            let campaign_id = parse_u64(field(Some(row), "id")?)?;
            if campaign_id == 0 {
                return Err(OzonReportParseError::Value);
            }
            Ok(CollectedAdvertisingExpenseFact {
                business_date: parse_date(field(Some(row), "date")?)?,
                campaign_id,
                money_spent_minor: parse_performance_minor(field(Some(row), "moneySpent")?)?,
                bonus_spent_minor: parse_performance_minor(field(Some(row), "bonusSpent")?)?,
                prepayment_spent_minor: parse_performance_minor(field(
                    Some(row),
                    "prepaymentSpent",
                )?)?,
            })
        })
        .collect()
}

fn per_thousand(amount_minor: u64, events: u64) -> Result<Option<u64>, OzonReportParseError> {
    if events == 0 {
        return Ok(None);
    }
    let value = u128::from(amount_minor)
        .checked_mul(1_000)
        .and_then(|value| value.checked_add(u128::from(events / 2)))
        .ok_or(OzonReportParseError::Value)?
        / u128::from(events);
    u64::try_from(value)
        .map(Some)
        .map_err(|_| OzonReportParseError::Value)
}

fn per_event(amount_minor: u64, events: u64) -> Result<Option<u64>, OzonReportParseError> {
    if events == 0 {
        return Ok(None);
    }
    let value = (u128::from(amount_minor) + u128::from(events / 2)) / u128::from(events);
    u64::try_from(value)
        .map(Some)
        .map_err(|_| OzonReportParseError::Value)
}

fn invalid_cursor(cursor: &str) -> bool {
    cursor.is_empty()
        || cursor.len() > MAX_CURSOR_BYTES
        || cursor.bytes().any(|byte| byte.is_ascii_control())
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

fn parse_optional_performance_price(value: &Value) -> Result<u64, OzonReportParseError> {
    if value.as_str() == Some("") {
        Ok(0)
    } else {
        parse_performance_minor(value)
    }
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
        assert_eq!(sales[0].returned_units, None);
        assert_eq!(sales[0].cancelled_units, None);

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

        let sku_performance = parse_performance_sku_advertising(&json!({"rows": [{
            "campaignId": "35751912", "sku": "123", "date": "2026-08-16",
            "views": "3764", "clicks": "126", "expense": "701,78",
            "orders": "4", "sales": "4321.09", "toCart": "21",
            "modelOrders": "6", "modelSales": "5000,00",
            "price": "1234,56", "avgCpc": "5,57"
        }]}))
        .unwrap();
        assert_eq!(sku_performance[0].sku, 123);
        assert_eq!(sku_performance[0].spend_minor, 70_178);
        assert_eq!(sku_performance[0].attributed_revenue_minor, 432_109);
        assert_eq!(sku_performance[0].basket_additions, 21);
        assert_eq!(sku_performance[0].model_attributed_orders, 6);
        assert_eq!(sku_performance[0].model_attributed_revenue_minor, 500_000);
        assert_eq!(sku_performance[0].product_price_minor, 123_456);
        assert_eq!(sku_performance[0].average_cpc_minor, Some(557));
        assert_eq!(sku_performance[0].cpm_minor, Some(18_645));
        assert_eq!(sku_performance[0].cpl_minor, Some(3_342));

        let expenses = parse_performance_expenses(&json!({"rows": [{
            "id": "35751912", "date": "2026-08-16",
            "moneySpent": "701,78", "bonusSpent": "20,10",
            "prepaymentSpent": "650,00", "title": "Реклама с Тёмой"
        }]}))
        .unwrap();
        assert_eq!(expenses[0].money_spent_minor, 70_178);
        assert_eq!(expenses[0].bonus_spent_minor, 2_010);
        assert_eq!(expenses[0].prepayment_spent_minor, 65_000);

        let fbo = parse_warehouse_stock_page(
            &json!({"products": [{
                "sku": 123, "warehouse_id": 77, "present": 11, "reserved": 3
            }]}),
            "fbo",
        )
        .unwrap();
        assert_eq!(fbo[0].warehouse_id, "fbo:77");
        assert_eq!(fbo[0].sellable_units, 8);
        let fbs = parse_warehouse_stock_page(
            &json!({"products": [{
                "sku": "123", "warehouse_id": "88", "present": 11,
                "reserved": 3, "free_stock": 6
            }]}),
            "fbs",
        )
        .unwrap();
        assert_eq!(fbs[0].warehouse_id, "fbs:88");
        assert_eq!(fbs[0].sellable_units, 6);
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

        assert_eq!(
            warehouse_stock_page_request(
                "/v1/product/info/stocks-by-warehouse/fbo",
                Some("opaque-cursor")
            )
            .unwrap()
            .payload,
            json!({
                "cursor": "opaque-cursor", "limit": 100,
                "offer_ids": [], "skus": [],
            })
        );
        assert_eq!(
            warehouse_stock_page_request("/v2/product/info/stocks-by-warehouse/fbs", None)
                .unwrap()
                .payload,
            json!({"cursor": "", "limit": 100, "offer_id": [], "sku": []})
        );
        assert!(warehouse_stock_page_request("/v1/product/update", None).is_err());
        assert_eq!(
            next_warehouse_stock_cursor(&json!({"has_next": true, "cursor": "next"})),
            Ok(Some("next".to_owned()))
        );
        assert_eq!(
            next_warehouse_stock_cursor(&json!({"has_next": false, "cursor": ""})),
            Ok(None)
        );
        assert_eq!(
            next_warehouse_stock_cursor(&json!({"has_next": true, "cursor": ""})),
            Err(OzonReportParseError::Value)
        );
        assert_eq!(
            next_warehouse_stock_cursor(&json!({"has_next": false, "cursor": "unsafe\n"})),
            Err(OzonReportParseError::Value)
        );
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
        assert!(
            parse_warehouse_stock_page(
                &json!({"products": [{
                    "sku": 1, "warehouse_id": 1, "present": 1, "reserved": 2
                }]}),
                "fbo"
            )
            .is_err()
        );
        assert_eq!(
            parse_warehouse_stock_page(&json!({"products": []}), "rfbs"),
            Err(OzonReportParseError::Value)
        );
        assert_eq!(
            parse_warehouse_stock_page(
                &json!({"products": vec![json!({}); PRODUCT_PAGE_ROWS + 1]}),
                "fbo"
            ),
            Err(OzonReportParseError::TooManyRows)
        );
        assert_eq!(
            parse_warehouse_stock_page(
                &json!({"products": [{
                    "sku": 1, "warehouse_id": 0, "present": 1, "reserved": 0
                }]}),
                "fbo"
            ),
            Err(OzonReportParseError::Value)
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
        for response in [
            json!({}),
            json!({"rows": [{}]}),
            json!({"rows": [{
                "campaignId":"1", "sku":"2", "date":"2026-08-16",
                "views":"1", "clicks":"2", "expense":"0", "orders":"0", "sales":"0"
            }]}),
            json!({"rows": [{
                "campaignId":"1", "sku":"0", "date":"2026-08-16",
                "views":"1", "clicks":"1", "expense":"0", "orders":"0", "sales":"0"
            }]}),
        ] {
            assert!(parse_performance_sku_advertising(&response).is_err());
        }

        let valid_sku_row = json!({
            "campaignId":"1", "sku":"2", "date":"2026-08-16",
            "views":"1", "clicks":"1", "expense":"0", "orders":"0", "sales":"0",
            "toCart":"0", "modelOrders":"0", "modelSales":"0",
            "price":"0", "avgCpc":"0"
        });
        let large_valid_response = json!({"rows": vec![valid_sku_row; MAX_PAGE_ROWS + 1]});
        assert_eq!(
            parse_performance_sku_advertising(&large_valid_response)
                .unwrap()
                .len(),
            MAX_PAGE_ROWS + 1
        );
        assert_eq!(
            parse_performance_sku_advertising(
                &json!({"rows": vec![json!({}); MAX_PERFORMANCE_SKU_ROWS + 1]})
            ),
            Err(OzonReportParseError::TooManyRows)
        );

        let sku_row = |campaign_id: Value, sku: Value, views: Value, clicks: Value| {
            json!({
                "campaignId":campaign_id, "sku":sku, "date":"2026-08-16",
                "views":views, "clicks":clicks, "expense":"0", "orders":"0",
                "sales":"0", "toCart":"0", "modelOrders":"0", "modelSales":"0",
                "price":"0", "avgCpc":"0"
            })
        };
        for row in [
            sku_row(json!(0), json!(2), json!(1), json!(1)),
            sku_row(json!(1), json!(0), json!(1), json!(1)),
            sku_row(json!(1), json!(2), json!(1), json!(2)),
        ] {
            assert_eq!(
                parse_performance_sku_advertising(&json!({"rows":[row]})),
                Err(OzonReportParseError::Value)
            );
        }
        let zero_events = parse_performance_sku_advertising(&json!({
            "rows":[sku_row(json!(1), json!(2), json!(0), json!(0))]
        }))
        .unwrap();
        assert_eq!(zero_events[0].cpm_minor, None);
        assert_eq!(zero_events[0].cpl_minor, None);

        let mut unavailable_price = sku_row(json!(1), json!(2), json!(1), json!(1));
        unavailable_price["price"] = json!("");
        let unavailable_price =
            parse_performance_sku_advertising(&json!({"rows":[unavailable_price]})).unwrap();
        assert_eq!(unavailable_price[0].product_price_minor, 0);

        let mut unavailable_spend = sku_row(json!(1), json!(2), json!(1), json!(1));
        unavailable_spend["expense"] = json!("");
        assert_eq!(
            parse_performance_sku_advertising(&json!({"rows":[unavailable_spend]})),
            Err(OzonReportParseError::Value)
        );

        let mut invalid_model_sales = sku_row(json!(1), json!(2), json!(1), json!(1));
        invalid_model_sales["modelSales"] = json!("invalid");
        assert!(parse_performance_sku_advertising(&json!({"rows":[invalid_model_sales]})).is_err());

        assert_eq!(
            parse_performance_expenses(
                &json!({"rows": vec![Value::Null; MAX_PERFORMANCE_SKU_ROWS + 1]})
            ),
            Err(OzonReportParseError::TooManyRows)
        );
        for row in [
            json!({"id":0, "date":"2026-08-16", "moneySpent":"0", "bonusSpent":"0", "prepaymentSpent":"0"}),
            json!({"id":1, "date":"2026-08-16", "moneySpent":"0", "bonusSpent":"0", "prepaymentSpent":"invalid"}),
        ] {
            assert!(parse_performance_expenses(&json!({"rows":[row]})).is_err());
        }
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
                    "metrics": ["0", 0]
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
