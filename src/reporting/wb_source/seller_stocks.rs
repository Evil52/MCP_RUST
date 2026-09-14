//! Seller inventory uses size IDs, never barcode guesses or missing-as-zero.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{CollectedStockFact, WbClientReportTransport, WbReportSource, WbReportSourceError};
use crate::reporting::checkpoint::{StockPageScope, checkpointed, stock_checkpoints};

const CARD_LIMIT: usize = 100;
const STOCK_BATCH: usize = 1_000;
const MAX_IDENTITIES: usize = 25_000;
const MAX_WAREHOUSES: usize = 100;

pub enum SellerStockRequest {
    Warehouses,
    Cards {
        cursor: Option<(String, u64)>,
    },
    Stocks {
        warehouse_id: u64,
        chrt_ids: Vec<u64>,
    },
}

#[derive(Clone, Deserialize, Serialize)]
struct Warehouse {
    id: u64,
    delivery_type: u64,
}

#[derive(Deserialize, Serialize)]
struct CardPage {
    /// One entry per card, retaining size identity without descriptions or PII.
    products: Vec<(u64, Vec<u64>)>,
    next: Option<(String, u64)>,
}

impl WbClientReportTransport {
    pub(super) async fn fetch_seller_inventory(
        &self,
        request: SellerStockRequest,
    ) -> Result<Value, WbReportSourceError> {
        let response = match request {
            SellerStockRequest::Warehouses => self.client.seller_warehouses(&self.account_id).await,
            SellerStockRequest::Cards { cursor } => {
                let mut cursor_body = json!({"limit":CARD_LIMIT});
                if let Some((updated_at, nm_id)) = cursor {
                    cursor_body["updatedAt"] = json!(updated_at);
                    cursor_body["nmID"] = json!(nm_id);
                }
                self.client
                    .product_cards(
                        &self.account_id,
                        None,
                        json!({"settings":{
                            "sort":{"ascending":true}, "cursor":cursor_body,
                            "filter":{"withPhoto":-1}
                        }}),
                    )
                    .await
            }
            SellerStockRequest::Stocks {
                warehouse_id,
                chrt_ids,
            } => {
                self.client
                    .seller_warehouse_stocks(&self.account_id, warehouse_id, chrt_ids)
                    .await
            }
        };
        response.map_err(|error| super::wb_source_failure(&error))
    }
}

impl WbReportSource {
    /// Publication is complete only after both WB and seller warehouses have
    /// been traversed. Existing snapshots cannot acquire inferred FBS zeros.
    pub async fn collect_complete_stock_pages(
        &self,
    ) -> Result<Vec<CollectedStockFact>, WbReportSourceError> {
        let mut facts = self.collect_stock_pages().await?;
        facts.extend(self.collect_seller_stock_pages().await?);
        if facts.len() > MAX_IDENTITIES {
            return Err(WbReportSourceError::PaginationLimit);
        }
        Ok(facts)
    }

    pub async fn collect_seller_stock_pages(
        &self,
    ) -> Result<Vec<CollectedStockFact>, WbReportSourceError> {
        let inventory = stock_checkpoints(&self.checkpoints, StockPageScope::SellerInventory);
        let warehouses: Vec<Warehouse> =
            checkpointed(&inventory, json!(["wb_seller_warehouses_v1"]), || async {
                parse_warehouses(
                    &self
                        .transport
                        .seller_inventory(SellerStockRequest::Warehouses)
                        .await?,
                )
            })
            .await?;
        if warehouses.is_empty() {
            return Ok(Vec::new());
        }
        let sizes = self.seller_size_catalog().await?;
        let chrt_ids = sizes.keys().copied().collect::<Vec<_>>();
        let mut facts = Vec::new();
        for warehouse in warehouses {
            let mut totals = BTreeMap::<u64, u64>::new();
            for chunk in chrt_ids.chunks(STOCK_BATCH) {
                let amounts: Vec<(u64, u64)> = checkpointed(
                    &inventory,
                    json!(["wb_seller_stock_v1", warehouse.id, chunk]),
                    || async {
                        parse_amounts(
                            &self
                                .transport
                                .seller_inventory(SellerStockRequest::Stocks {
                                    warehouse_id: warehouse.id,
                                    chrt_ids: chunk.to_vec(),
                                })
                                .await?,
                            chunk,
                        )
                    },
                )
                .await?;
                for (chrt_id, amount) in amounts {
                    let sku = sizes
                        .get(&chrt_id)
                        .ok_or(WbReportSourceError::InvalidStockResponse)?;
                    let total = totals.entry(*sku).or_default();
                    *total = total
                        .checked_add(amount)
                        .ok_or(WbReportSourceError::InvalidStockResponse)?;
                }
            }
            for (sku, sellable_units) in totals {
                facts.push(CollectedStockFact {
                    sku,
                    sellable_units,
                    // The vendor delivery type is preserved, including models
                    // other than FBS (1); warehouse IDs cannot collide with FBW.
                    warehouse_id: format!("wb:seller:{}:{}", warehouse.delivery_type, warehouse.id),
                });
                if facts.len() > MAX_IDENTITIES {
                    return Err(WbReportSourceError::PaginationLimit);
                }
            }
        }
        Ok(facts)
    }

    async fn seller_size_catalog(&self) -> Result<BTreeMap<u64, u64>, WbReportSourceError> {
        let content = stock_checkpoints(&self.checkpoints, StockPageScope::Content);
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut products = BTreeSet::new();
        let mut sizes = BTreeMap::new();
        // A terminal empty page is allowed at the exact catalog bound.
        for _ in 0..=MAX_IDENTITIES / CARD_LIMIT {
            let page: CardPage =
                checkpointed(&content, json!(["wb_stock_cards_v1", cursor]), || async {
                    parse_cards(
                        &self
                            .transport
                            .seller_inventory(SellerStockRequest::Cards {
                                cursor: cursor.clone(),
                            })
                            .await?,
                    )
                })
                .await?;
            for (sku, ids) in page.products {
                if !products.insert(sku) {
                    return Err(WbReportSourceError::InvalidStockResponse);
                }
                for id in ids {
                    if sizes.insert(id, sku).is_some() {
                        return Err(WbReportSourceError::InvalidStockResponse);
                    }
                    if sizes.len() > MAX_IDENTITIES {
                        return Err(WbReportSourceError::PaginationLimit);
                    }
                }
            }
            let Some(next) = page.next else {
                return Ok(sizes);
            };
            if !cursors.insert(next.clone()) {
                return Err(WbReportSourceError::InvalidStockResponse);
            }
            cursor = Some(next);
        }
        Err(WbReportSourceError::PaginationLimit)
    }
}

fn id(value: Option<&Value>) -> Result<u64, WbReportSourceError> {
    value
        .and_then(Value::as_u64)
        .filter(|id| *id > 0 && *id <= (u64::MAX >> 1))
        .ok_or(WbReportSourceError::InvalidStockResponse)
}

fn parse_warehouses(value: &Value) -> Result<Vec<Warehouse>, WbReportSourceError> {
    let rows = value
        .as_array()
        .ok_or(WbReportSourceError::InvalidStockResponse)?;
    if rows.len() > MAX_WAREHOUSES {
        return Err(WbReportSourceError::PaginationLimit);
    }
    let mut ids = BTreeSet::new();
    rows.iter()
        .map(|row| {
            let warehouse = Warehouse {
                id: id(row.get("id"))?,
                delivery_type: id(row.get("deliveryType"))?,
            };
            if !ids.insert(warehouse.id) {
                return Err(WbReportSourceError::InvalidStockResponse);
            }
            Ok(warehouse)
        })
        .collect()
}

fn parse_cards(value: &Value) -> Result<CardPage, WbReportSourceError> {
    let rows = value
        .get("cards")
        .and_then(Value::as_array)
        .ok_or(WbReportSourceError::InvalidStockResponse)?;
    if rows.len() > CARD_LIMIT {
        return Err(WbReportSourceError::PaginationLimit);
    }
    let cursor = value
        .get("cursor")
        .ok_or(WbReportSourceError::InvalidStockResponse)?;
    if cursor.get("total").and_then(Value::as_u64) != Some(rows.len() as u64) {
        return Err(WbReportSourceError::InvalidStockResponse);
    }
    let products = rows
        .iter()
        .map(|row| {
            let sku = id(row.get("nmID"))?;
            let sizes = row
                .get("sizes")
                .and_then(Value::as_array)
                .ok_or(WbReportSourceError::InvalidStockResponse)?;
            if sizes.is_empty() || sizes.len() > MAX_IDENTITIES {
                return Err(WbReportSourceError::InvalidStockResponse);
            }
            Ok((
                sku,
                sizes
                    .iter()
                    .map(|size| id(size.get("chrtID")))
                    .collect::<Result<_, _>>()?,
            ))
        })
        .collect::<Result<_, _>>()?;
    let next = if rows.len() == CARD_LIMIT {
        let updated = cursor
            .get("updatedAt")
            .and_then(Value::as_str)
            .filter(|value| value.len() <= 64)
            .ok_or(WbReportSourceError::InvalidStockResponse)?;
        chrono::DateTime::parse_from_rfc3339(updated)
            .map_err(|_| WbReportSourceError::InvalidStockResponse)?;
        Some((updated.to_owned(), id(cursor.get("nmID"))?))
    } else {
        None
    };
    Ok(CardPage { products, next })
}

fn parse_amounts(value: &Value, requested: &[u64]) -> Result<Vec<(u64, u64)>, WbReportSourceError> {
    let rows = value
        .get("stocks")
        .and_then(Value::as_array)
        .ok_or(WbReportSourceError::InvalidStockResponse)?;
    let mut observed = BTreeSet::new();
    let mut amounts = Vec::new();
    for row in rows {
        let chrt_id = id(row.get("chrtId"))?;
        let amount = row
            .get("amount")
            .and_then(Value::as_u64)
            .ok_or(WbReportSourceError::InvalidStockResponse)?;
        if !requested.contains(&chrt_id) || !observed.insert(chrt_id) {
            return Err(WbReportSourceError::InvalidStockResponse);
        }
        amounts.push((chrt_id, amount));
    }
    if observed.len() != requested.len() {
        return Err(WbReportSourceError::SellerStockCoverageIncomplete);
    }
    Ok(amounts)
}

#[cfg(test)]
mod tests;
