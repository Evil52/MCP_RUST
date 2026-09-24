//! Strict WB Fullstats normalization and per-SKU counter diagnostics.
use std::collections::BTreeMap;

use chrono::NaiveDate;
use serde_json::{Map, Value};

use super::{
    MAX_ROWS, WbReportParseError, array, campaign_day::campaign_day_product_rows, date,
    ensure_positive, field, minor, object, optional_unsigned, unsigned,
};
use crate::reporting::postgres_collector::CollectedAdvertisingFact;

pub fn parse_promotion_stats(
    response: &Value,
) -> Result<Vec<CollectedAdvertisingFact>, WbReportParseError> {
    // WB returns a JSON `null` body for a successful fullstats request when
    // none of the requested campaigns has statistics in the selected period.
    if response.is_null() {
        return Ok(Vec::new());
    }
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
                tracing::warn!(
                    %business_date,
                    campaign_id,
                    sku,
                    impressions = value.impressions,
                    clicks = value.clicks,
                    "WB promotion counters are inconsistent"
                );
                return Err(WbReportParseError::InconsistentAdvertisingCounts);
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
                basket_additions: 0,
                model_attributed_orders: 0,
                model_attributed_revenue_minor: 0,
                product_price_minor: 0,
                average_cpc_minor: None,
                cpm_minor: None,
                cpl_minor: None,
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
    let product_rows = campaign_day_product_rows(day)?;
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
