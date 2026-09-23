//! Durable, bounded seller inventory collection independent of FBW. Only normalized pages
//! enter the existing source journal; missing values are never converted to zero.
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    checkpoint::{
        CheckpointError, Checkpoints, StockPageScope, checkpointed, delay_seconds,
        stock_checkpoints,
    },
    postgres_collector::CollectedSellerStockFact,
    source_collection::SourceFailure,
};
use crate::wb::{WbClient, WbError};

const CARD_LIMIT: usize = 100;
const MAX_CARD_PAGES: usize = MAX_IDENTITIES / CARD_LIMIT + 1;
const BATCH_SIZE: usize = 1_000;
const MAX_IDENTITIES: usize = 25_000;
const MAX_AGGREGATE_FACTS: usize = 25_000;
const MAX_WAREHOUSES: usize = 100;
const MAX_SIZE_PAIRS: usize = MAX_IDENTITIES * MAX_WAREHOUSES;

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct Warehouse {
    id: u64,
    delivery_type: u64,
}
type Fetch<'a> = Pin<Box<dyn Future<Output = Result<Value, SourceFailure>> + Send + 'a>>;

pub trait SellerTransport: Send + Sync {
    fn warehouses(&self) -> Fetch<'_>;
    fn cards(&self, cursor: Value) -> Fetch<'_>;
    fn stocks(&self, warehouse: u64, ids: Vec<u64>) -> Fetch<'_>;
}

struct ClientTransport {
    client: WbClient,
    account: String,
}
impl SellerTransport for ClientTransport {
    fn warehouses(&self) -> Fetch<'_> {
        Box::pin(async {
            self.client
                .seller_warehouses(&self.account)
                .await
                .map_err(|error| failure(&error))
        })
    }
    fn cards(&self, cursor: Value) -> Fetch<'_> {
        Box::pin(async move {
            self.client.product_cards(&self.account, None, json!({"settings":{"cursor":cursor,"sort":{"ascending":true},"filter":{"withPhoto":-1}}})).await.map_err(|error| failure(&error))
        })
    }
    fn stocks(&self, warehouse: u64, ids: Vec<u64>) -> Fetch<'_> {
        Box::pin(async move {
            self.client
                .seller_warehouse_stocks(&self.account, warehouse, ids)
                .await
                .map_err(|error| failure(&error))
        })
    }
}

fn failure(error: &WbError) -> SourceFailure {
    if let WbError::Api { status, .. } = error {
        return SourceFailure::from(if status.is_server_error() {
            "upstream_server_error"
        } else {
            "seller_upstream_rejected"
        });
    }
    let retry_after = match error {
        WbError::RateLimited {
            retry_after: Some(delay),
            ..
        }
        | WbError::LocalRateLimited { retry_after: delay } => Some(delay_seconds(*delay)),
        _ => None,
    };
    SourceFailure {
        code: if retry_after.is_some() {
            "rate_limited"
        } else {
            error.kind().code()
        },
        retry_after,
    }
}

impl From<CheckpointError> for SourceFailure {
    fn from(error: CheckpointError) -> Self {
        error.code().into()
    }
}

pub struct WbSellerSource {
    transport: Arc<dyn SellerTransport>,
    checkpoints: Checkpoints,
}
impl WbSellerSource {
    #[must_use]
    pub fn new(client: WbClient, account: String) -> Self {
        Self {
            transport: Arc::new(ClientTransport { client, account }),
            checkpoints: None,
        }
    }
    #[must_use]
    pub fn with_checkpoints(mut self, checkpoints: Checkpoints) -> Self {
        self.checkpoints = checkpoints;
        self
    }

    pub async fn collect(&self) -> Result<Vec<CollectedSellerStockFact>, SourceFailure> {
        let inventory = stock_checkpoints(&self.checkpoints, StockPageScope::SellerInventory);
        let warehouses: Vec<Warehouse> =
            checkpointed(&inventory, json!(["wb_seller_warehouses_v2"]), || async {
                parse_warehouses(&self.transport.warehouses().await?)
            })
            .await?;
        if warehouses.is_empty() {
            return Ok(Vec::new());
        }
        let catalogue = self.catalogue().await?;
        let products = catalogue.values().copied().collect::<BTreeSet<_>>().len();
        let pairs = catalogue.len().saturating_mul(warehouses.len());
        if pairs > MAX_SIZE_PAIRS || products.saturating_mul(warehouses.len()) > MAX_AGGREGATE_FACTS
        {
            return Err("seller_pair_limit".into());
        }
        let sizes = catalogue.into_iter().collect::<Vec<_>>();
        let mut facts = Vec::with_capacity(pairs);
        for warehouse in warehouses {
            for batch in sizes.chunks(BATCH_SIZE) {
                // Persist only amounts in frozen batch order. The catalogue and
                // request key retain IDs; repeating full facts wastes journal space.
                let amounts: Vec<Option<u64>> = checkpointed(
                    &inventory,
                    json!(["wb_seller_stock_amounts_v2", warehouse, batch]),
                    || async {
                        let data = self
                            .transport
                            .stocks(warehouse.id, batch.iter().map(|(id, _)| *id).collect())
                            .await?;
                        parse_stocks(&data, batch)
                    },
                )
                .await?;
                if amounts.len() != batch.len() {
                    return Err("seller_invalid_response".into());
                }
                facts.extend(
                    batch
                        .iter()
                        .zip(amounts)
                        .map(|((size, sku), sellable_units)| CollectedSellerStockFact {
                            sku: *sku,
                            chrt_id: *size,
                            warehouse_id: warehouse.id,
                            delivery_type: warehouse.delivery_type,
                            sellable_units,
                        }),
                );
            }
        }
        Ok(facts)
    }

    async fn catalogue(&self) -> Result<BTreeMap<u64, u64>, SourceFailure> {
        let content = stock_checkpoints(&self.checkpoints, StockPageScope::Content);
        let mut cursor = json!({"limit":CARD_LIMIT});
        let mut cursors = BTreeSet::new();
        let mut items = BTreeSet::new();
        let mut catalogue = BTreeMap::new();
        for _ in 0..MAX_CARD_PAGES {
            let page: CardPage =
                checkpointed(&content, json!(["wb_seller_cards_v1", cursor]), || async {
                    parse_cards(&self.transport.cards(cursor.clone()).await?)
                })
                .await?;
            extend_catalogue(&mut items, &mut catalogue, page.items)?;
            let Some(next) = page.next else {
                return Ok(catalogue);
            };
            if !cursors.insert(next.to_string()) {
                return Err("seller_cursor_repeated".into());
            }
            cursor = next;
        }
        Err("seller_catalogue_page_limit".into())
    }
}

/// Adds one card page to the size-to-card catalogue. A repeated card or
/// size identity fails closed instead of silently merging two cards.
fn extend_catalogue(
    items: &mut BTreeSet<u64>,
    catalogue: &mut BTreeMap<u64, u64>,
    page: Vec<(u64, Vec<u64>)>,
) -> Result<(), SourceFailure> {
    for (sku, sizes) in page {
        if !items.insert(sku) {
            return Err("seller_catalogue_duplicate".into());
        }
        for id in sizes {
            if catalogue.insert(id, sku).is_some() {
                return Err("seller_catalogue_duplicate".into());
            }
        }
    }
    if catalogue.len() > MAX_IDENTITIES {
        return Err("seller_pair_limit".into());
    }
    Ok(())
}

fn id(value: Option<&Value>) -> Result<u64, SourceFailure> {
    value
        .and_then(Value::as_u64)
        .filter(|id| *id > 0 && i64::try_from(*id).is_ok())
        .ok_or_else(|| "seller_invalid_response".into())
}

fn parse_warehouses(data: &Value) -> Result<Vec<Warehouse>, SourceFailure> {
    let rows = data.as_array().ok_or("seller_invalid_response")?;
    if rows.len() > MAX_WAREHOUSES {
        return Err("seller_warehouse_limit".into());
    }
    let mut seen = BTreeSet::new();
    let mut warehouses = Vec::with_capacity(rows.len());
    for row in rows {
        let warehouse = Warehouse {
            id: id(row.get("id"))?,
            delivery_type: id(row.get("deliveryType"))?,
        };
        if i32::try_from(warehouse.delivery_type).is_err() {
            return Err("seller_invalid_response".into());
        }
        if !seen.insert(warehouse.id) {
            return Err("seller_invalid_response".into());
        }
        warehouses.push(warehouse);
    }
    warehouses.sort_unstable();
    Ok(warehouses)
}

#[derive(Deserialize, Serialize)]
struct CardPage {
    items: Vec<(u64, Vec<u64>)>,
    next: Option<Value>,
}

fn parse_cards(data: &Value) -> Result<CardPage, SourceFailure> {
    let cards = data
        .get("cards")
        .and_then(Value::as_array)
        .ok_or("seller_invalid_response")?;
    let total = data
        .pointer("/cursor/total")
        .and_then(Value::as_u64)
        .ok_or("seller_invalid_response")?;
    if cards.len() > CARD_LIMIT || usize::try_from(total).ok() != Some(cards.len()) {
        return Err("seller_invalid_response".into());
    }
    let mut items = Vec::new();
    for card in cards {
        let sku = id(card.get("nmID"))?;
        let sizes = card
            .get("sizes")
            .and_then(Value::as_array)
            .filter(|sizes| !sizes.is_empty() && sizes.len() <= MAX_IDENTITIES)
            .ok_or("seller_invalid_response")?;
        items.push((
            sku,
            sizes
                .iter()
                .map(|size| id(size.get("chrtID")))
                .collect::<Result<Vec<_>, _>>()?,
        ));
    }
    let next = if cards.len() == CARD_LIMIT {
        let nm = id(data.pointer("/cursor/nmID"))?;
        let updated = data
            .pointer("/cursor/updatedAt")
            .and_then(Value::as_str)
            .filter(|value| value.len() <= 64)
            .ok_or("seller_invalid_response")?;
        chrono::DateTime::parse_from_rfc3339(updated)
            .map_err(|_| SourceFailure::from("seller_invalid_response"))?;
        Some(json!({"limit":CARD_LIMIT,"nmID":nm,"updatedAt":updated}))
    } else {
        None
    };
    Ok(CardPage { items, next })
}

fn parse_stocks(data: &Value, batch: &[(u64, u64)]) -> Result<Vec<Option<u64>>, SourceFailure> {
    let rows = data
        .get("stocks")
        .and_then(Value::as_array)
        .ok_or("seller_invalid_response")?;
    let expected = batch.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>();
    let mut returned = BTreeMap::new();
    for row in rows {
        let size = id(row.get("chrtId"))?;
        let amount = row
            .get("amount")
            .and_then(Value::as_u64)
            .filter(|amount| i32::try_from(*amount).is_ok())
            .ok_or("seller_invalid_response")?;
        if !expected.contains(&size) || returned.insert(size, amount).is_some() {
            return Err("seller_invalid_response".into());
        }
    }
    Ok(batch
        .iter()
        .map(|(size, _)| returned.get(size).copied())
        .collect())
}

#[cfg(test)]
mod tests;
