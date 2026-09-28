//! Launch stock preflight combines WB-owned stock with current FBS inventory.
//! Seller Analytics gives nmID-to-size identity; live Marketplace stock gives
//! the amount. Unknown, incomplete or unsupported inventory fails closed.
use super::Operator;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

use crate::wb::WbClient;

const MAX_FBS_WAREHOUSES: usize = 100;
const STOCK_REPORT_PAGE_LIMIT: u32 = 1_000;
const MAX_STOCK_REPORT_PAGES: usize = 5;

impl Operator {
    pub(super) async fn verified_stock_totals(&self) -> Result<BTreeMap<u64, u64>> {
        let stocks = self
            .reader
            .warehouse_stocks(
                &self.manifest.account_id,
                json!({"nmIds":self.manifest.nm_ids(),"chrtIds":[],"limit":100,"offset":0}),
            )
            .await?;
        let (stocks, count) = crate::reporting::wb_adapter::parse_stock_page(&stocks)?;
        ensure!(count < 100, "stock page may be truncated");
        let mut totals = BTreeMap::<u64, u64>::new();
        for row in stocks {
            add_stock(&mut totals, row.sku, row.sellable_units)?;
        }
        let minimum = self.manifest.minimum_launch_stock(&self.policy);
        let missing = self
            .manifest
            .nm_ids()
            .into_iter()
            .filter(|nm| totals.get(nm).copied().unwrap_or(0) < minimum)
            .collect::<Vec<_>>();
        if self.manifest.version == 2 && !missing.is_empty() {
            add_live_fbs_stocks(
                &self.reader,
                &self.manifest.account_id,
                &missing,
                &mut totals,
            )
            .await?;
        }
        for nm in self.manifest.nm_ids() {
            ensure!(
                totals.get(&nm).copied().unwrap_or(0) >= minimum,
                "SKU {nm} has insufficient verified WB stock"
            );
        }
        Ok(totals)
    }
}

pub(in crate::control) async fn add_live_fbs_stocks(
    reader: &WbClient,
    account_id: &str,
    missing: &[u64],
    totals: &mut BTreeMap<u64, u64>,
) -> Result<()> {
    let warehouses = reader
        .seller_warehouses(account_id)
        .await
        .context("FBS warehouse list unavailable for launch stock preflight")?;
    let fbs = fbs_warehouse_ids(&warehouses)?;
    if fbs.is_empty() {
        return Ok(());
    }
    let mut identities = BTreeMap::<u64, BTreeMap<u64, u64>>::new();
    let mut seen = BTreeSet::new();
    let mut offset = 0;
    let mut complete = false;
    for _ in 0..MAX_STOCK_REPORT_PAGES {
        let page = reader
            .seller_warehouses_stock_report(
                account_id,
                missing,
                &[],
                STOCK_REPORT_PAGE_LIMIT,
                offset,
            )
            .await
            .context("FBS size identities unavailable for launch stock preflight")?;
        let items = page
            .data
            .pointer("/data/items")
            .and_then(Value::as_array)
            .context("FBS stock report page incomplete")?;
        for item in items {
            let nm = positive_id(item.get("nmId"), "FBS nmId missing")?;
            let chrt = positive_id(item.get("chrtId"), "FBS chrtId missing")?;
            let warehouse = positive_id(item.get("warehouseId"), "FBS warehouseId missing")?;
            ensure!(
                seen.insert((warehouse, chrt)),
                "duplicate FBS stock identity"
            );
            if fbs.contains(&warehouse) {
                ensure!(missing.contains(&nm), "foreign FBS nmId");
                identities.entry(warehouse).or_default().insert(chrt, nm);
            }
        }
        if let Some(next) = page.next_offset {
            offset = next;
        } else {
            complete = true;
            break;
        }
    }
    ensure!(complete, "FBS stock report exceeds bounded scan");
    for (warehouse, sizes) in identities {
        for chunk in sizes.keys().copied().collect::<Vec<_>>().chunks(1_000) {
            let response = reader
                .seller_warehouse_stocks(account_id, warehouse, chunk.to_vec())
                .await
                .context("live FBS stock unavailable for launch preflight")?;
            add_live_stock_response(&response, &sizes, chunk, totals)?;
        }
    }
    Ok(())
}

fn positive_id(value: Option<&Value>, message: &'static str) -> Result<u64> {
    value
        .and_then(Value::as_u64)
        .filter(|id| *id > 0 && *id <= (u64::MAX >> 1))
        .context(message)
}

fn fbs_warehouse_ids(value: &Value) -> Result<BTreeSet<u64>> {
    let rows = value.as_array().context("FBS warehouse list malformed")?;
    ensure!(
        rows.len() <= MAX_FBS_WAREHOUSES,
        "too many seller warehouses"
    );
    let mut all = BTreeSet::new();
    let mut fbs = BTreeSet::new();
    for row in rows {
        let id = positive_id(row.get("id"), "seller warehouse ID missing")?;
        let kind = positive_id(row.get("deliveryType"), "seller delivery type missing")?;
        ensure!(all.insert(id), "duplicate seller warehouse");
        if kind == 1 {
            fbs.insert(id);
        }
    }
    Ok(fbs)
}

fn add_live_stock_response(
    response: &Value,
    sizes: &BTreeMap<u64, u64>,
    requested: &[u64],
    totals: &mut BTreeMap<u64, u64>,
) -> Result<()> {
    let rows = response
        .get("stocks")
        .and_then(Value::as_array)
        .context("live FBS stock response incomplete")?;
    let mut seen = BTreeSet::new();
    for row in rows {
        let chrt = positive_id(row.get("chrtId"), "live FBS chrtId missing")?;
        let amount = row
            .get("amount")
            .and_then(Value::as_u64)
            .context("live FBS amount missing")?;
        ensure!(
            requested.contains(&chrt) && seen.insert(chrt),
            "unexpected live FBS stock identity"
        );
        add_stock(totals, sizes[&chrt], amount)?;
    }
    ensure!(
        seen == requested.iter().copied().collect(),
        "live FBS stock response omitted a size"
    );
    Ok(())
}

fn add_stock(totals: &mut BTreeMap<u64, u64>, nm: u64, amount: u64) -> Result<()> {
    let total = totals.entry(nm).or_default();
    *total = total.checked_add(amount).context("stock overflow")?;
    Ok(())
}
