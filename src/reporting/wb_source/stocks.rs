//! Aggregate size rows across page boundaries before publishing SKU facts.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{
    CollectedStockFact, MAX_PAGES, PAGE_SIZE, PAGE_SIZE_U32, Value, WbReportSource,
    WbReportSourceError, checkpointed, json, page_offset, parse_stock_page,
};

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct SizeIdentity {
    sku: u64,
    chrt_id: u64,
    warehouse_id: i64,
    warehouse_labels: Option<(String, String)>,
}

#[derive(Deserialize, Serialize)]
struct StockPage {
    facts: Vec<CollectedStockFact>,
    source_rows: usize,
    sizes: Vec<SizeIdentity>,
}

impl WbReportSource {
    pub async fn collect_stock_pages(
        &self,
    ) -> Result<Vec<CollectedStockFact>, WbReportSourceError> {
        self.collect_stock_pages_with_limit(MAX_PAGES).await
    }

    pub(super) async fn collect_stock_pages_with_limit(
        &self,
        max_pages: usize,
    ) -> Result<Vec<CollectedStockFact>, WbReportSourceError> {
        let mut totals = BTreeMap::<(u64, String), u64>::new();
        let mut seen_sizes = BTreeSet::new();
        for page in 0..max_pages {
            let offset = page_offset(page, PAGE_SIZE_U32)?;
            // v1 discarded size identities, so cannot prove non-overlap on replay.
            let page: StockPage = checkpointed(
                &self.checkpoints,
                json!(["wb_stock_v2", PAGE_SIZE_U32, offset]),
                || async { stock_page(&self.transport.stock_page(PAGE_SIZE_U32, offset).await?) },
            )
            .await?;
            if page.source_rows > PAGE_SIZE
                || page.sizes.into_iter().any(|size| !seen_sizes.insert(size))
            {
                return Err(WbReportSourceError::InvalidStockResponse);
            }
            for fact in page.facts {
                let total = totals.entry((fact.sku, fact.warehouse_id)).or_default();
                *total = total
                    .checked_add(fact.sellable_units)
                    .ok_or(WbReportSourceError::InvalidStockResponse)?;
            }
            // Distinct sizes can collapse to one fact even across pages. Only
            // the raw row count proves that pagination reached its terminal page.
            if page.source_rows < PAGE_SIZE {
                return Ok(totals
                    .into_iter()
                    .map(|((sku, warehouse_id), sellable_units)| CollectedStockFact {
                        sku,
                        warehouse_id,
                        sellable_units,
                    })
                    .collect());
            }
        }
        Err(WbReportSourceError::PaginationLimit)
    }
}

fn stock_page(response: &Value) -> Result<StockPage, WbReportSourceError> {
    let invalid = WbReportSourceError::InvalidStockResponse;
    let (facts, source_rows) = parse_stock_page(response).map_err(|_| invalid)?;
    if source_rows > PAGE_SIZE {
        return Err(invalid);
    }
    let rows = response
        .pointer("/data/items")
        .and_then(Value::as_array)
        .ok_or(invalid)?;
    let mut sizes = BTreeSet::new();
    for row in rows {
        // Older payloads omit chrtId. They still support checked aggregation,
        // but cannot distinguish equal totals from repeated size observations.
        let Some(chrt_id) = row.get("chrtId") else {
            continue;
        };
        let chrt_id = chrt_id.as_u64().filter(|id| *id > 0).ok_or(invalid)?;
        let warehouse_id = row["warehouseId"].as_i64().ok_or(invalid)?;
        let warehouse_labels = if warehouse_id < 0 {
            Some((
                row["regionName"].as_str().ok_or(invalid)?.to_owned(),
                row["warehouseName"].as_str().ok_or(invalid)?.to_owned(),
            ))
        } else {
            None
        };
        if !sizes.insert(SizeIdentity {
            sku: row["nmId"].as_u64().ok_or(invalid)?,
            chrt_id,
            warehouse_id,
            warehouse_labels,
        }) {
            return Err(invalid);
        }
    }
    Ok(StockPage {
        facts,
        source_rows,
        sizes: sizes.into_iter().collect(),
    })
}
