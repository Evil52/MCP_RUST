use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use chrono::{Duration, NaiveDate};
use serde_json::{Value, json};

use crate::reporting::{
    postgres_collector::CollectedAdvertisingFact, wb_adapter::parse_promotion_stats,
};

/// The inventory's changeTime and a campaign's last restart are not creation
/// dates. Only timestamps.created from campaign details can bound all-time work.
pub fn normalize_details(raw: &Value, ids: &[u64]) -> Result<Value> {
    let rows = raw
        .get("adverts")
        .and_then(Value::as_array)
        .context("invalid campaign details")?;
    let mut seen = BTreeSet::new();
    let mut result = Vec::new();
    for row in rows {
        let id = row
            .get("id")
            .and_then(Value::as_u64)
            .context("invalid detail identity")?;
        ensure!(
            ids.contains(&id) && seen.insert(id),
            "details outside scope or duplicated"
        );
        let created = row
            .pointer("/timestamps/created")
            .filter(|v| !v.is_null())
            .map(|v| {
                let raw = v.as_str().context("invalid creation date")?;
                NaiveDate::parse_from_str(
                    raw.get(..10).context("invalid creation date")?,
                    "%Y-%m-%d",
                )
                .context("invalid creation date")
            })
            .transpose()?;
        result.push(json!({"campaign_id":id,"created_on":created}));
    }
    Ok(Value::Array(result))
}

/// Retain unsupported statuses as explicit inventory exclusions. Missing creation
/// dates remain unknown; the planner then uses the conservative archive floor.
pub fn normalize_inventory(raw: &Value) -> Result<Value> {
    let mut campaigns = BTreeMap::new();
    let groups = match raw.get("adverts") {
        Some(Value::Array(groups)) => groups.as_slice(),
        None | Some(Value::Null) if raw.get("all").and_then(Value::as_u64) == Some(0) => &[],
        _ => anyhow::bail!("invalid campaign inventory"),
    };
    let mut count = 0_u64;
    for group in groups {
        let status = group
            .get("status")
            .and_then(Value::as_i64)
            .context("invalid campaign status")?;
        let adverts = group
            .get("advert_list")
            .and_then(Value::as_array)
            .context("invalid campaign list")?;
        ensure!(
            group.get("count").and_then(Value::as_u64) == Some(u64::try_from(adverts.len())?),
            "truncated campaign group"
        );
        for advert in adverts {
            let id = advert
                .get("advertId")
                .and_then(Value::as_u64)
                .context("invalid campaign identity")?;
            ensure!(
                id > 0 && i64::try_from(id).is_ok(),
                "invalid campaign identity"
            );
            let created = advert
                .get("createTime")
                .filter(|v| !v.is_null())
                .map(|v| {
                    let text = v.as_str().context("invalid campaign creation date")?;
                    NaiveDate::parse_from_str(
                        text.get(..10).context("invalid creation date")?,
                        "%Y-%m-%d",
                    )
                    .context("invalid creation date")
                })
                .transpose()?;
            ensure!(
                campaigns
                    .insert(
                        id,
                        json!({"campaign_id":id,"status":status,"created_on":created})
                    )
                    .is_none(),
                "duplicate campaign identity"
            );
            count += 1;
            ensure!(
                count <= 5_000,
                "campaign inventory exceeds archive capacity"
            );
        }
    }
    ensure!(
        raw.get("all").and_then(Value::as_u64) == Some(count),
        "truncated campaign inventory"
    );
    Ok(Value::Array(campaigns.into_values().collect()))
}

fn metrics(fact: &CollectedAdvertisingFact) -> Value {
    json!({"spend_minor":fact.spend_minor,"revenue_minor":fact.attributed_revenue_minor,
        "orders":fact.attributed_orders,"clicks":fact.clicks,"impressions":fact.impressions})
}

fn reconciles(total: &CollectedAdvertisingFact, products: &[CollectedAdvertisingFact]) -> bool {
    let parent = [
        total.spend_minor,
        total.attributed_revenue_minor,
        total.attributed_orders,
        total.clicks,
        total.impressions,
    ];
    let sum = products.iter().try_fold([0_u64; 5], |mut sums, row| {
        for (sum, value) in sums.iter_mut().zip([
            row.spend_minor,
            row.attributed_revenue_minor,
            row.attributed_orders,
            row.clicks,
            row.impressions,
        ]) {
            *sum = sum.checked_add(value)?;
        }
        Some(sums)
    });
    !products.is_empty() && sum == Some(parent) && products.iter().all(|row| row.sku > 0)
}

/// Store authoritative campaign-day totals separately from reconciled SKU rows.
///
/// Omitted campaign IDs are gaps. A documented successful null is `no_data`.
/// Sparse days inside a returned campaign are also explicit `no_data` observations.
pub fn normalize_statistics(
    raw: &Value,
    ids: &[u64],
    from: NaiveDate,
    to: NaiveDate,
) -> Result<Value> {
    ensure!(
        from <= to && (to - from).num_days() < 31 && !ids.is_empty() && ids.len() <= 50,
        "invalid statistics scope"
    );
    let scope = ids.iter().copied().collect::<BTreeSet<_>>();
    ensure!(scope.len() == ids.len(), "duplicate requested campaigns");
    let statistics = collect_statistics(raw, &scope, from, to)?;
    let mut result = Vec::new();
    for id in ids {
        let mut date = from;
        while date <= to {
            let total = statistics.totals.get(&(*id, date));
            let sku_rows = statistics
                .products
                .get(&(*id, date))
                .map_or(&[][..], Vec::as_slice);
            let reconciled = total.is_some_and(|total| reconciles(total, sku_rows));
            let state = if raw.is_null() {
                "no_data"
            } else if !statistics.seen.contains(id) {
                "missing"
            } else if total.is_some() {
                "observed"
            } else {
                "no_data"
            };
            result.push(json!({"campaign_id":id,"business_date":date,"state":state,
                "metrics":total.map(metrics),"sku_reconciled":reconciled,
                "products":if reconciled { sku_rows.iter().map(|row| json!({"sku":row.sku,"metrics":metrics(row)})).collect::<Vec<_>>() } else { Vec::new() }}));
            date += Duration::days(1);
        }
    }
    Ok(Value::Array(result))
}

#[derive(Default)]
struct StatisticsDays {
    totals: BTreeMap<(u64, NaiveDate), CollectedAdvertisingFact>,
    products: BTreeMap<(u64, NaiveDate), Vec<CollectedAdvertisingFact>>,
    seen: BTreeSet<u64>,
}

fn collect_statistics(
    raw: &Value,
    scope: &BTreeSet<u64>,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<StatisticsDays> {
    let mut statistics = StatisticsDays::default();
    if raw.is_null() {
        return Ok(statistics);
    }
    for campaign in raw.as_array().context("invalid statistics envelope")? {
        collect_campaign_days(campaign, scope, from, to, &mut statistics)?;
    }
    Ok(statistics)
}

/// Validates campaign-day totals while retaining an independently valid SKU breakdown.
fn collect_campaign_days(
    campaign: &Value,
    scope: &BTreeSet<u64>,
    from: NaiveDate,
    to: NaiveDate,
    statistics: &mut StatisticsDays,
) -> Result<()> {
    let id = campaign
        .get("advertId")
        .or_else(|| campaign.get("advert_id"))
        .and_then(Value::as_u64)
        .context("invalid statistics identity")?;
    ensure!(
        scope.contains(&id) && statistics.seen.insert(id),
        "statistics campaign outside scope or duplicated"
    );
    let days = campaign
        .get("days")
        .and_then(Value::as_array)
        .context("missing statistics days")?;
    let mut parents = Vec::new();
    let mut dates = BTreeSet::new();
    for day in days {
        let mut parent = day.as_object().context("invalid campaign day")?.clone();
        parent.remove("apps");
        let text = parent
            .get("date")
            .and_then(Value::as_str)
            .context("invalid day date")?;
        let date =
            NaiveDate::parse_from_str(text.get(..10).context("invalid day date")?, "%Y-%m-%d")?;
        ensure!(dates.insert(date), "duplicate campaign day");
        parents.push(Value::Object(parent));
    }
    let parent_rows = parse_promotion_stats(&json!([{"advertId":id,"days":parents}]))?;
    for row in parent_rows {
        ensure!(
            row.business_date >= from && row.business_date <= to,
            "statistics day outside scope"
        );
        ensure!(
            statistics
                .totals
                .insert((id, row.business_date), row)
                .is_none(),
            "duplicate campaign day"
        );
    }
    // A broken product breakdown must not invalidate authoritative totals.
    if let Ok(rows) = parse_promotion_stats(&json!([campaign])) {
        for row in rows {
            statistics
                .products
                .entry((id, row.business_date))
                .or_default()
                .push(row);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
