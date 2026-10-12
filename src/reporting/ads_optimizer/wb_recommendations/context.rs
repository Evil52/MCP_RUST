use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};
use serde_json::Value;

use super::{OptimizerError, RecommendationEvidence};

pub(super) struct Campaign<'a> {
    pub id: u64,
    pub name: &'a str,
    pub active: bool,
    pub supported: bool,
    pub bids: BTreeMap<u64, Option<u64>>,
}

pub(super) fn campaigns(
    input: &RecommendationEvidence,
) -> Result<Vec<Campaign<'_>>, OptimizerError> {
    let rows = input.composition["adverts"]
        .as_array()
        .ok_or(OptimizerError::InvalidInput)?;
    let mut ids = BTreeSet::new();
    let mut result = Vec::new();
    for row in rows {
        let id = number(row, "id")?;
        if !input.campaign_ids.contains(&id) || !ids.insert(id) {
            return Err(OptimizerError::InvalidInput);
        }
        let mut bids = BTreeMap::new();
        for setting in row["nm_settings"]
            .as_array()
            .ok_or(OptimizerError::InvalidInput)?
        {
            let sku = number(setting, "nm_id")?;
            if sku == 0
                || bids
                    .insert(
                        sku,
                        setting
                            .pointer("/bids_kopecks/search")
                            .and_then(Value::as_u64),
                    )
                    .is_some()
            {
                return Err(OptimizerError::InvalidInput);
            }
        }
        if bids.len() > 10_000 {
            return Err(OptimizerError::LimitExceeded);
        }
        result.push(Campaign {
            id,
            name: row
                .pointer("/settings/name")
                .and_then(Value::as_str)
                .unwrap_or(""),
            active: row["status"].as_u64() == Some(9),
            supported: row["bid_type"].as_str() == Some("manual")
                && row["currency"].as_str() == Some("RUB")
                && row
                    .pointer("/settings/payment_type")
                    .and_then(Value::as_str)
                    == Some("cpc")
                && row
                    .pointer("/settings/placements/search")
                    .and_then(Value::as_bool)
                    == Some(true)
                && row
                    .pointer("/settings/placements/recommendations")
                    .and_then(Value::as_bool)
                    == Some(false),
            bids,
        });
    }
    if ids.len() != input.campaign_ids.len() {
        return Err(OptimizerError::InvalidInput);
    }
    result.sort_by_key(|row| row.id);
    Ok(result)
}

pub(super) fn number(row: &Value, field: &str) -> Result<u64, OptimizerError> {
    row[field].as_u64().ok_or(OptimizerError::InvalidInput)
}

pub(super) fn timestamp(value: &Value) -> Option<DateTime<Utc>> {
    value.as_str()?.parse().ok()
}

pub(super) fn fresh(at: DateTime<Utc>, now: DateTime<Utc>, minutes: i64) -> bool {
    at <= now && now - at <= Duration::minutes(minutes)
}

pub(super) fn snapshot_totals(
    input: &RecommendationEvidence,
    source: &str,
) -> Result<BTreeMap<u64, u64>, OptimizerError> {
    let snapshot = if source == "stocks" {
        &input.stocks
    } else {
        &input.sales
    };
    if snapshot["account_id"].as_str() != Some(&input.account_id)
        || snapshot["marketplace"].as_str() != Some("wildberries")
        || snapshot["source"].as_str() != Some(source)
    {
        return Err(OptimizerError::InvalidInput);
    }
    let rows = snapshot["rows"]
        .as_array()
        .ok_or(OptimizerError::InvalidInput)?;
    if rows.len() > 25_000 {
        return Err(OptimizerError::LimitExceeded);
    }
    if snapshot["next_offset"].as_u64().is_some()
        || snapshot["total_rows"].as_u64() != Some(rows.len() as u64)
    {
        return Err(OptimizerError::InvalidInput);
    }
    let max_minutes = if source == "stocks" { 60 } else { 360 };
    if snapshot["quality"].as_str() != Some("complete")
        || snapshot["pagination_complete"].as_bool() != Some(true)
        || !timestamp(&snapshot["source_as_of"])
            .is_some_and(|at| fresh(at, input.as_of, max_minutes))
        || (source == "stocks" && snapshot["inventory_scope"].as_str() != Some("fbw"))
    {
        return Ok(BTreeMap::new());
    }
    let mut sums = BTreeMap::<u64, u64>::new();
    let mut keys = BTreeSet::new();
    for row in rows {
        if row["account_id"].as_str() != Some(&input.account_id)
            || row["marketplace"].as_str() != Some("wildberries")
            || row["snapshot_id"].as_i64().map(|id| id.to_string())
                != snapshot["snapshot_id"].as_str().map(str::to_owned)
        {
            return Err(OptimizerError::InvalidInput);
        }
        let sku = number(row, "sku")?;
        let identity = if source == "stocks" {
            "warehouse_id"
        } else {
            "business_date"
        };
        let key = row[identity].as_str().ok_or(OptimizerError::InvalidInput)?;
        if sku == 0 || !keys.insert((sku, key)) || key.starts_with("wb:seller:") {
            return Err(OptimizerError::InvalidInput);
        }
        let field = if source == "stocks" {
            "sellable_units"
        } else {
            "ordered_units"
        };
        let value = number(row, field)?;
        let total = sums.entry(sku).or_default();
        *total = total.checked_add(value).ok_or(OptimizerError::Overflow)?;
    }
    Ok(sums)
}
