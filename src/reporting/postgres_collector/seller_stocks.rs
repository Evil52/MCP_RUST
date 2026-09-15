use super::{
    CollectedStockFact, Deserialize, PostgresCollectorError, Serialize, Transaction, as_i32,
    collect_i64, ensure_unique, fits_i32, fits_i64,
};

pub const MAX_SIZE_PAIRS: usize = 2_500_000;

/// One requested size/warehouse pair. Missing upstream rows remain SQL NULL.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CollectedSellerStockFact {
    pub sku: u64,
    pub chrt_id: u64,
    pub warehouse_id: u64,
    pub delivery_type: u64,
    pub sellable_units: Option<u64>,
}

pub(super) fn validate(facts: &[CollectedSellerStockFact]) -> Result<(), PostgresCollectorError> {
    if facts.len() > MAX_SIZE_PAIRS {
        return Err(PostgresCollectorError::InvalidInput);
    }
    ensure_unique(
        facts,
        |fact| (fact.chrt_id, fact.warehouse_id),
        |fact| {
            [fact.sku, fact.chrt_id, fact.warehouse_id]
                .into_iter()
                .all(|id| id > 0 && fits_i64(id))
                && fact.delivery_type > 0
                && fits_i32(fact.delivery_type)
                && fact.sellable_units.is_none_or(fits_i32)
        },
    )
}

pub(super) async fn insert(
    transaction: &Transaction<'_>,
    snapshot_id: i64,
    facts: &[CollectedSellerStockFact],
) -> Result<(), PostgresCollectorError> {
    for batch in facts.chunks(super::MAX_FACT_ROWS) {
        insert_batch(transaction, snapshot_id, batch).await?;
    }
    Ok(())
}

async fn insert_batch(
    transaction: &Transaction<'_>,
    snapshot_id: i64,
    facts: &[CollectedSellerStockFact],
) -> Result<(), PostgresCollectorError> {
    let skus = collect_i64(facts.iter().map(|fact| fact.sku))?;
    let sizes = collect_i64(facts.iter().map(|fact| fact.chrt_id))?;
    let warehouses = collect_i64(facts.iter().map(|fact| fact.warehouse_id))?;
    let delivery_types = facts
        .iter()
        .map(|fact| as_i32(fact.delivery_type))
        .collect::<Result<Vec<_>, _>>()?;
    let units = facts
        .iter()
        .map(|fact| fact.sellable_units.map(as_i32).transpose())
        .collect::<Result<Vec<_>, _>>()?;
    transaction.execute("INSERT INTO daily_reporting.seller_stock_facts(snapshot_id,sku,chrt_id,warehouse_id,delivery_type,sellable_units) SELECT $1,batch.* FROM unnest($2::bigint[],$3::bigint[],$4::bigint[],$5::integer[],$6::integer[]) AS batch", &[&snapshot_id,&skus,&sizes,&warehouses,&delivery_types,&units]).await.map_err(|_| PostgresCollectorError::Unavailable)?;
    Ok(())
}

pub(super) async fn insert_fbw(
    transaction: &Transaction<'_>,
    snapshot_id: i64,
    facts: &[CollectedStockFact],
) -> Result<(), PostgresCollectorError> {
    if facts.is_empty() {
        return Ok(());
    }
    let skus = collect_i64(facts.iter().map(|fact| fact.sku))?;
    let warehouse_ids = facts
        .iter()
        .map(|fact| fact.warehouse_id.clone())
        .collect::<Vec<_>>();
    let sellable_units = facts
        .iter()
        .map(|fact| as_i32(fact.sellable_units))
        .collect::<Result<Vec<_>, _>>()?;
    transaction
        .execute(
            "INSERT INTO daily_reporting.stock_facts \
             (snapshot_id, sku, warehouse_id, sellable_units) \
             SELECT $1, batch.* \
             FROM unnest($2::bigint[], $3::text[], $4::integer[]) AS batch",
            &[&snapshot_id, &skus, &warehouse_ids, &sellable_units],
        )
        .await
        .map_err(|_| PostgresCollectorError::Unavailable)?;
    Ok(())
}
