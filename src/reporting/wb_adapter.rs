//! Strict normalization for the Wildberries sources used by daily reports.
//!
//! The adapter accepts only the documented response envelopes and converts
//! ruble amounts into integer kopecks. It performs no I/O and never retains
//! product titles, buyer data, credentials, or upstream error bodies.

use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDate;
use serde_json::{Map, Value};
use thiserror::Error;

use super::postgres_collector::{
    CollectedAdvertisingFact, CollectedPriceFact, CollectedSalesFact, CollectedStockFact,
};

const MAX_ROWS: usize = 25_000;
const MAX_CAMPAIGNS: usize = 500;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum WbReportParseError {
    #[error("Wildberries daily-report response has an invalid shape")]
    Shape,
    #[error("Wildberries daily-report response contains an invalid value")]
    Value,
    #[error("Wildberries daily-report response exceeds its fixed row bound")]
    TooManyRows,
}

pub fn parse_sales_history(
    response: &Value,
) -> Result<Vec<CollectedSalesFact>, WbReportParseError> {
    let products = array(response)?;
    let mut facts = Vec::new();
    for product in products {
        let product = object(product)?;
        let identity = object(field(product, "product")?)?;
        let sku = unsigned(field(identity, "nmId")?)?;
        ensure_positive(sku)?;
        if field(product, "currency")?.as_str() != Some("RUB") {
            return Err(WbReportParseError::Value);
        }
        for row in array(field(product, "history")?)? {
            let row = object(row)?;
            facts.push(CollectedSalesFact {
                business_date: date(field(row, "date")?)?,
                sku,
                ordered_units: unsigned(field(row, "orderCount")?)?,
                operational_gmv_minor: minor(field(row, "orderSum")?)?,
                cancelled_units: optional_unsigned(row.get("cancelCount"))?,
                returned_units: optional_unsigned(row.get("returnCount"))?,
            });
            if facts.len() > MAX_ROWS {
                return Err(WbReportParseError::TooManyRows);
            }
        }
    }
    ensure_unique(&facts, |fact| (fact.business_date, fact.sku))?;
    Ok(facts)
}

pub fn parse_stock_page(
    response: &Value,
) -> Result<(Vec<CollectedStockFact>, usize), WbReportParseError> {
    let data = object(field(object(response)?, "data")?)?;
    let rows = array(field(data, "items")?)?;
    if rows.len() > MAX_ROWS {
        return Err(WbReportParseError::TooManyRows);
    }
    let mut totals = BTreeMap::<(u64, u64), u64>::new();
    for row in rows {
        let row = object(row)?;
        let sku = unsigned(field(row, "nmId")?)?;
        let warehouse = unsigned(field(row, "warehouseId")?)?;
        ensure_positive(sku)?;
        ensure_positive(warehouse)?;
        let quantity = unsigned(field(row, "quantity")?)?;
        let total = totals.entry((sku, warehouse)).or_default();
        *total = total
            .checked_add(quantity)
            .ok_or(WbReportParseError::Value)?;
    }
    Ok((
        totals
            .into_iter()
            .map(|((sku, warehouse), sellable_units)| CollectedStockFact {
                sku,
                warehouse_id: format!("wb:{warehouse}"),
                sellable_units,
            })
            .collect(),
        rows.len(),
    ))
}

pub fn parse_price_page(
    response: &Value,
) -> Result<(Vec<CollectedPriceFact>, usize), WbReportParseError> {
    let data = object(field(object(response)?, "data")?)?;
    let goods = array(field(data, "listGoods")?)?;
    if goods.len() > MAX_ROWS {
        return Err(WbReportParseError::TooManyRows);
    }
    let mut facts = Vec::with_capacity(goods.len());
    for good in goods {
        let good = object(good)?;
        if field(good, "currencyIsoCode4217")?.as_str() != Some("RUB") {
            return Err(WbReportParseError::Value);
        }
        let sku = unsigned(field(good, "nmID")?)?;
        ensure_positive(sku)?;
        let mut selected: Option<(u64, u64)> = None;
        for size in array(field(good, "sizes")?)? {
            let size = object(size)?;
            let current = minor(field(size, "discountedPrice")?)?;
            let old = minor(field(size, "price")?)?;
            if old < current {
                return Err(WbReportParseError::Value);
            }
            if selected.is_none_or(|candidate| (current, old) < candidate) {
                selected = Some((current, old));
            }
        }
        let (price_minor, old_price) = selected.ok_or(WbReportParseError::Shape)?;
        facts.push(CollectedPriceFact {
            sku,
            price_minor,
            old_price_minor: (old_price != price_minor).then_some(old_price),
        });
    }
    ensure_unique(&facts, |fact| fact.sku)?;
    Ok((facts, goods.len()))
}

pub fn parse_campaign_ids(response: &Value) -> Result<Vec<u64>, WbReportParseError> {
    let root = object(response)?;
    let groups = array(field(root, "adverts")?)?;
    let mut ids = BTreeSet::new();
    for group in groups {
        let group = object(group)?;
        let status = signed(field(group, "status")?)?;
        let list = array(field(group, "advert_list")?)?;
        if !matches!(status, 7 | 9 | 11) {
            continue;
        }
        for advert in list {
            let id = unsigned(field(object(advert)?, "advertId")?)?;
            ensure_positive(id)?;
            ids.insert(id);
            if ids.len() > MAX_CAMPAIGNS {
                return Err(WbReportParseError::TooManyRows);
            }
        }
    }
    Ok(ids.into_iter().collect())
}

pub fn parse_promotion_stats(
    response: &Value,
) -> Result<Vec<CollectedAdvertisingFact>, WbReportParseError> {
    let campaigns = array(response)?;
    let mut totals = BTreeMap::<(NaiveDate, u64, u64), AdvertisingTotals>::new();
    for campaign in campaigns {
        let campaign = object(campaign)?;
        let campaign_id = unsigned(
            campaign
                .get("advertId")
                .or_else(|| campaign.get("advert_id"))
                .ok_or(WbReportParseError::Shape)?,
        )?;
        ensure_positive(campaign_id)?;
        if let Some(days) = campaign.get("days") {
            for day in array(days)? {
                parse_campaign_day(object(day)?, campaign_id, &mut totals)?;
            }
        } else if let Some(stats) = campaign.get("stats") {
            for row in array(stats)? {
                let row = object(row)?;
                let sku = optional_unsigned(row.get("nm_id"))?.unwrap_or(0);
                add_advertising_row(row, campaign_id, sku, &mut totals)?;
            }
        } else {
            return Err(WbReportParseError::Shape);
        }
    }
    if totals.len() > MAX_ROWS {
        return Err(WbReportParseError::TooManyRows);
    }
    totals
        .into_iter()
        .map(|((business_date, campaign_id, sku), value)| {
            if value.clicks > value.impressions {
                return Err(WbReportParseError::Value);
            }
            Ok(CollectedAdvertisingFact {
                business_date,
                campaign_id,
                sku,
                impressions: value.impressions,
                clicks: value.clicks,
                spend_minor: value.spend_minor,
                attributed_orders: value.orders,
                attributed_revenue_minor: value.revenue_minor,
            })
        })
        .collect()
}

#[derive(Default)]
struct AdvertisingTotals {
    impressions: u64,
    clicks: u64,
    spend_minor: u64,
    orders: u64,
    revenue_minor: u64,
}

fn parse_campaign_day(
    day: &Map<String, Value>,
    campaign_id: u64,
    totals: &mut BTreeMap<(NaiveDate, u64, u64), AdvertisingTotals>,
) -> Result<(), WbReportParseError> {
    let mut product_rows = Vec::new();
    if let Some(apps) = day.get("apps") {
        for app in array(apps)? {
            let app = object(app)?;
            if let Some(products) = app.get("nm") {
                for product in array(products)? {
                    product_rows.push(object(product)?);
                }
            }
        }
    }
    if product_rows.is_empty() {
        add_advertising_row(day, campaign_id, 0, totals)
    } else {
        for row in product_rows {
            let sku = unsigned(field(row, "nmId")?)?;
            ensure_positive(sku)?;
            add_advertising_row_with_date(row, field(day, "date")?, campaign_id, sku, totals)?;
        }
        Ok(())
    }
}

fn add_advertising_row(
    row: &Map<String, Value>,
    campaign_id: u64,
    sku: u64,
    totals: &mut BTreeMap<(NaiveDate, u64, u64), AdvertisingTotals>,
) -> Result<(), WbReportParseError> {
    add_advertising_row_with_date(row, field(row, "date")?, campaign_id, sku, totals)
}

fn add_advertising_row_with_date(
    row: &Map<String, Value>,
    date_value: &Value,
    campaign_id: u64,
    sku: u64,
    totals: &mut BTreeMap<(NaiveDate, u64, u64), AdvertisingTotals>,
) -> Result<(), WbReportParseError> {
    let key = (date(date_value)?, campaign_id, sku);
    let value = totals.entry(key).or_default();
    value.impressions = checked_add(value.impressions, unsigned(field(row, "views")?)?)?;
    value.clicks = checked_add(value.clicks, unsigned(field(row, "clicks")?)?)?;
    value.spend_minor = checked_add(value.spend_minor, minor(field(row, "sum")?)?)?;
    value.orders = checked_add(value.orders, unsigned(field(row, "orders")?)?)?;
    let revenue = match row.get("sum_price") {
        Some(value) => value,
        None => field(row, "sumPrice")?,
    };
    value.revenue_minor = checked_add(value.revenue_minor, minor(revenue)?)?;
    Ok(())
}

fn checked_add(left: u64, right: u64) -> Result<u64, WbReportParseError> {
    left.checked_add(right).ok_or(WbReportParseError::Value)
}

fn object(value: &Value) -> Result<&Map<String, Value>, WbReportParseError> {
    value.as_object().ok_or(WbReportParseError::Shape)
}

fn array(value: &Value) -> Result<&Vec<Value>, WbReportParseError> {
    value.as_array().ok_or(WbReportParseError::Shape)
}

fn field<'a>(object: &'a Map<String, Value>, name: &str) -> Result<&'a Value, WbReportParseError> {
    object.get(name).ok_or(WbReportParseError::Shape)
}

fn unsigned(value: &Value) -> Result<u64, WbReportParseError> {
    match value {
        Value::Number(number) => number.as_u64().ok_or(WbReportParseError::Value),
        Value::String(value) => value.parse().map_err(|_| WbReportParseError::Value),
        _ => Err(WbReportParseError::Value),
    }
}

fn signed(value: &Value) -> Result<i64, WbReportParseError> {
    value.as_i64().ok_or(WbReportParseError::Value)
}

fn optional_unsigned(value: Option<&Value>) -> Result<Option<u64>, WbReportParseError> {
    value.map(unsigned).transpose()
}

fn ensure_positive(value: u64) -> Result<(), WbReportParseError> {
    (value > 0 && i64::try_from(value).is_ok())
        .then_some(())
        .ok_or(WbReportParseError::Value)
}

fn date(value: &Value) -> Result<NaiveDate, WbReportParseError> {
    let raw = value.as_str().ok_or(WbReportParseError::Value)?;
    let prefix = raw.get(..10).ok_or(WbReportParseError::Value)?;
    NaiveDate::parse_from_str(prefix, "%Y-%m-%d").map_err(|_| WbReportParseError::Value)
}

fn minor(value: &Value) -> Result<u64, WbReportParseError> {
    let raw = match value {
        Value::Number(number) => number.to_string(),
        Value::String(value) => value.clone(),
        _ => return Err(WbReportParseError::Value),
    };
    let (whole, fraction) = raw.split_once('.').unwrap_or((&raw, ""));
    if whole.is_empty()
        || whole.starts_with('-')
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 2
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(WbReportParseError::Value);
    }
    let whole: u64 = whole.parse().map_err(|_| WbReportParseError::Value)?;
    let fractional = if fraction.is_empty() {
        0
    } else if fraction.len() == 1 {
        fraction
            .parse::<u64>()
            .map_err(|_| WbReportParseError::Value)?
            * 10
    } else {
        // The validation above has already restricted this branch to exactly
        // two ASCII digits, so no defensive unreachable branch is needed.
        fraction.parse().map_err(|_| WbReportParseError::Value)?
    };
    whole
        .checked_mul(100)
        .and_then(|value| value.checked_add(fractional))
        .ok_or(WbReportParseError::Value)
}

fn ensure_unique<T, K: Ord>(values: &[T], key: impl Fn(&T) -> K) -> Result<(), WbReportParseError> {
    let mut seen = BTreeSet::new();
    values
        .iter()
        .all(|value| seen.insert(key(value)))
        .then_some(())
        .ok_or(WbReportParseError::Value)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use serde_json::json;

    use super::*;

    #[test]
    fn documented_sales_stock_and_price_shapes_are_normalized() {
        let sales = parse_sales_history(&json!([{
            "product":{"nmId":268913787},"currency":"RUB",
            "history":[{"date":"2026-08-17","orderCount":19,"orderSum":1262.5,
                "cancelCount":2,"returnCount":1}]
        }]))
        .unwrap();
        assert_eq!(
            sales[0].business_date,
            NaiveDate::from_ymd_opt(2026, 8, 17).unwrap()
        );
        assert_eq!((sales[0].sku, sales[0].ordered_units), (268913787, 19));
        assert_eq!(
            (sales[0].operational_gmv_minor, sales[0].cancelled_units),
            (126250, Some(2))
        );

        let (stocks, stock_rows) = parse_stock_page(&json!({"data":{"items":[
            {"nmId":7,"warehouseId":507,"quantity":43},
            {"nmId":7,"warehouseId":507,"quantity":2}
        ]}}))
        .unwrap();
        assert_eq!(stock_rows, 2);
        assert_eq!(
            (
                stocks[0].sku,
                stocks[0].warehouse_id.as_str(),
                stocks[0].sellable_units
            ),
            (7, "wb:507", 45)
        );

        let (prices, price_rows) = parse_price_page(&json!({"data":{"listGoods":[{
            "nmID":98486,"currencyIsoCode4217":"RUB","sizes":[
                {"price":500,"discountedPrice":350},
                {"price":450,"discountedPrice":340}
            ]
        }]}}))
        .unwrap();
        assert_eq!(price_rows, 1);
        assert_eq!(
            (
                prices[0].sku,
                prices[0].price_minor,
                prices[0].old_price_minor
            ),
            (98486, 34000, Some(45000))
        );
    }

    #[test]
    fn campaigns_and_both_stats_shapes_are_normalized_without_double_counting() {
        let ids = parse_campaign_ids(&json!({"adverts":[
            {"status":9,"advert_list":[{"advertId":2},{"advertId":1}]},
            {"status":8,"advert_list":[{"advertId":3}]}
        ]}))
        .unwrap();
        assert_eq!(ids, vec![1, 2]);

        let facts = parse_promotion_stats(&json!([{
            "advertId":1,"days":[{"date":"2026-08-17T00:00:00Z","views":99,
                "clicks":9,"sum":10,"orders":2,"sum_price":200,
                "apps":[{"nm":[
                    {"nmId":7,"views":20,"clicks":2,"sum":4.25,"orders":1,"sum_price":100},
                    {"nmId":7,"views":10,"clicks":1,"sum":2,"orders":0,"sum_price":0}
                ]}]}]
        },{
            "advert_id":2,"stats":[{"date":"2026-08-17","nm_id":8,"views":5,
                "clicks":1,"sum":"1.20","orders":1,"sumPrice":50}]
        }]))
        .unwrap();
        assert_eq!(facts.len(), 2);
        assert_eq!(
            (
                facts[0].campaign_id,
                facts[0].sku,
                facts[0].impressions,
                facts[0].spend_minor
            ),
            (1, 7, 30, 625)
        );
        assert_eq!(
            (
                facts[1].campaign_id,
                facts[1].sku,
                facts[1].attributed_revenue_minor
            ),
            (2, 8, 5000)
        );

        let campaign_only = parse_promotion_stats(&json!([{
            "advertId":3,"days":[{"date":"2026-08-17","views":8,"clicks":1,
                "sum":2,"orders":0,"sum_price":0}]
        }]))
        .unwrap();
        assert_eq!(campaign_only[0].sku, 0);
    }

    #[test]
    fn malformed_oversized_and_ambiguous_values_fail_closed() {
        for value in [json!(null), json!({}), json!([{"product":{}}])] {
            assert!(parse_sales_history(&value).is_err());
        }
        assert_eq!(minor(&json!("1.234")), Err(WbReportParseError::Value));
        assert_eq!(minor(&json!(-1)), Err(WbReportParseError::Value));
        assert_eq!(minor(&json!(u64::MAX)), Err(WbReportParseError::Value));
        assert_eq!(date(&json!("bad")), Err(WbReportParseError::Value));
        assert_eq!(unsigned(&json!(-1)), Err(WbReportParseError::Value));
        assert_eq!(unsigned(&json!("7")), Ok(7));
        assert_eq!(unsigned(&json!("bad")), Err(WbReportParseError::Value));
        assert_eq!(unsigned(&json!(true)), Err(WbReportParseError::Value));
        assert_eq!(signed(&json!(u64::MAX)), Err(WbReportParseError::Value));
        assert_eq!(ensure_positive(0), Err(WbReportParseError::Value));
        assert_eq!(minor(&json!(true)), Err(WbReportParseError::Value));
        assert!(
            parse_sales_history(&json!([{
                "product":{"nmId":1},"currency":"USD","history":[]
            }]))
            .is_err()
        );
        assert!(parse_stock_page(&json!({"data":{"items":[{"nmId":1,"warehouseId":1,"quantity":u64::MAX},{"nmId":1,"warehouseId":1,"quantity":1}]}})).is_err());
        assert!(
            parse_price_page(
                &json!({"data":{"listGoods":[{"nmID":1,"currencyIsoCode4217":"USD","sizes":[]}]}})
            )
            .is_err()
        );
        assert!(
            parse_price_page(
                &json!({"data":{"listGoods":[{"nmID":1,"currencyIsoCode4217":"RUB","sizes":[]}]}})
            )
            .is_err()
        );
        assert!(parse_price_page(&json!({"data":{"listGoods":[{"nmID":1,"currencyIsoCode4217":"RUB","sizes":[{"price":1,"discountedPrice":2}]}]}})).is_err());
        assert!(parse_sales_history(&json!([{"product":{"nmId":1},"currency":"RUB","history":[{"date":"2026-08-17","orderCount":1,"orderSum":1},{"date":"2026-08-17","orderCount":1,"orderSum":1}]}])).is_err());
        assert!(parse_promotion_stats(&json!([{"advertId":1}])).is_err());
        assert!(parse_promotion_stats(&json!([{"stats":[]}])).is_err());
        assert!(parse_promotion_stats(&json!([{"advertId":true,"stats":[]}])).is_err());
        assert!(
            parse_promotion_stats(&json!([{"advertId":1,"days":[{
                "date":"2026-08-17","views":1,"clicks":0,"sum":0,"orders":0,
                "sum_price":0,"apps":[{}]
            }]}]))
            .is_ok()
        );
        assert!(
            parse_promotion_stats(&json!([{"advertId":1,"stats":[{
                "date":"2026-08-17","nm_id":1,"views":1,"clicks":0,"sum":0,"orders":0
            }]}]))
            .is_err()
        );
        assert!(parse_promotion_stats(&json!([{"advertId":1,"stats":[{"date":"2026-08-17","views":1,"clicks":2,"sum":0,"orders":0,"sum_price":0}]}])).is_err());

        let too_many = (0..=MAX_CAMPAIGNS)
            .map(|id| json!({"advertId":id + 1}))
            .collect::<Vec<_>>();
        assert_eq!(
            parse_campaign_ids(&json!({"adverts":[{"status":9,"advert_list":too_many}]})),
            Err(WbReportParseError::TooManyRows)
        );
    }

    #[test]
    fn exact_row_caps_reject_the_first_excess_row() {
        let history = vec![json!({"date":"2026-08-17","orderCount":0,"orderSum":0}); MAX_ROWS + 1];
        assert_eq!(
            parse_sales_history(&json!([{
                "product":{"nmId":1},"currency":"RUB","history":history
            }])),
            Err(WbReportParseError::TooManyRows)
        );
        assert_eq!(
            parse_stock_page(&json!({"data":{"items":vec![Value::Null; MAX_ROWS + 1]}})),
            Err(WbReportParseError::TooManyRows)
        );
        assert_eq!(
            parse_price_page(&json!({"data":{"listGoods":vec![Value::Null; MAX_ROWS + 1]}})),
            Err(WbReportParseError::TooManyRows)
        );

        let stats = (1_u64..=MAX_ROWS as u64 + 1)
            .map(|sku| {
                json!({
                    "date":"2026-08-17","nm_id":sku,"views":0,"clicks":0,
                    "sum":0,"orders":0,"sum_price":0
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            parse_promotion_stats(&json!([{"advertId":1,"stats":stats}])),
            Err(WbReportParseError::TooManyRows)
        );
    }
}
