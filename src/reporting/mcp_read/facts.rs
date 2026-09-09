//! Bounded reads and validation of published reporting facts.

use super::{
    AdvertisingMetricInput, BTreeMap, Client, DateTime, KpiSummary, MAX_FACT_ROWS, Marketplace,
    MetricSourceIds, PublishedAdvertisingExpenseFact, PublishedAdvertisingFact,
    PublishedFinanceFact, PublishedPriceFact, PublishedSalesFact, PublishedStockFact,
    ReportingReadError, Row, SalesMetricInput, SnapshotDescriptor, SnapshotSource, SnapshotStatus,
    Utc, calculate_kpis, column, complete_metric_pair, insert_metric_source_id, metric_fact_count,
    nonnegative_i32, nonnegative_i64, nonnegative_optional_i32, nonnegative_optional_i64,
    parse_finance_category, parse_marketplace, parse_snapshot_status, parse_source, positive_i32,
    positive_i64, valid_warehouse_id, validate_currency, validate_expected_fact_rows,
    validate_snapshot_fact_count,
};

pub(super) async fn load_history_kpis(
    client: &Client,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<BTreeMap<DateTime<Utc>, KpiSummary>, ReportingReadError> {
    let metric_descriptors = expected
        .values()
        .filter(|descriptor| {
            matches!(
                descriptor.source(),
                SnapshotSource::Sales | SnapshotSource::Advertising
            )
        })
        .collect::<Vec<_>>();
    let expected_rows = metric_descriptors
        .iter()
        .try_fold(0usize, |total, descriptor| {
            total.checked_add(descriptor.row_count() as usize)
        })
        .ok_or(ReportingReadError::InvalidPublishedData)?;
    validate_expected_fact_rows(expected_rows)?;
    let snapshot_ids = metric_descriptors
        .iter()
        .map(|descriptor| descriptor.snapshot_id())
        .collect::<Vec<_>>();
    let sales_rows = client
        .query(SALES_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    let advertising_rows = client
        .query(ADVERTISING_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(sales_rows.len())?;
    validate_fact_query_size(advertising_rows.len())?;

    let mut sales = BTreeMap::<i64, Vec<SalesMetricInput>>::new();
    for row in sales_rows {
        let (snapshot_id, fact) = sales_fact(&row, expected)?;
        sales
            .entry(snapshot_id)
            .or_default()
            .push(SalesMetricInput {
                ordered_units: fact.ordered_units,
                operational_gmv_minor: fact.operational_gmv_minor,
                cancelled_units: fact.cancelled_units,
                returned_units: fact.returned_units,
            });
    }
    let mut advertising = BTreeMap::<i64, Vec<AdvertisingMetricInput>>::new();
    for row in advertising_rows {
        let (snapshot_id, fact) = advertising_fact(&row, expected)?;
        advertising
            .entry(snapshot_id)
            .or_default()
            .push(AdvertisingMetricInput {
                impressions: fact.impressions,
                clicks: fact.clicks,
                spend_minor: fact.spend_minor,
                attributed_orders: fact.attributed_orders,
                attributed_revenue_minor: fact.attributed_revenue_minor,
            });
    }
    for descriptor in &metric_descriptors {
        let sales_count = sales.get(&descriptor.snapshot_id()).map_or(0, Vec::len);
        let advertising_count = advertising
            .get(&descriptor.snapshot_id())
            .map_or(0, Vec::len);
        let actual = metric_fact_count(descriptor.source(), sales_count, advertising_count)?;
        validate_snapshot_fact_count(actual, descriptor.row_count())?;
    }

    let mut source_ids = MetricSourceIds::new();
    for descriptor in metric_descriptors {
        insert_metric_source_id(&mut source_ids, descriptor)?;
    }
    source_ids
        .into_iter()
        .filter_map(|(cutoff, pair)| {
            let (sales_id, advertising_id) = complete_metric_pair(pair)?;
            let sales = sales.remove(&sales_id).unwrap_or_default();
            let advertising = advertising.remove(&advertising_id).unwrap_or_default();
            Some(
                calculate_kpis(&sales, &advertising)
                    .map(|summary| (cutoff, summary))
                    .map_err(|_| ReportingReadError::InvalidPublishedData),
            )
        })
        .collect()
}

pub(super) async fn query_sales_facts(
    client: &Client,
    snapshot_ids: &[i64],
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<Vec<PublishedSalesFact>, ReportingReadError> {
    let rows = client
        .query(SALES_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(rows.len())?;
    rows.iter()
        .map(|row| sales_fact(row, expected).map(|(_, fact)| fact))
        .collect()
}

pub(super) async fn query_advertising_facts(
    client: &Client,
    snapshot_ids: &[i64],
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<Vec<PublishedAdvertisingFact>, ReportingReadError> {
    let rows = client
        .query(ADVERTISING_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(rows.len())?;
    rows.iter()
        .map(|row| advertising_fact(row, expected).map(|(_, fact)| fact))
        .collect()
}

pub(super) async fn query_advertising_expense_facts(
    client: &Client,
    snapshot_ids: &[i64],
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<Vec<PublishedAdvertisingExpenseFact>, ReportingReadError> {
    let rows = client
        .query(ADVERTISING_EXPENSE_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(rows.len())?;
    rows.iter()
        .map(|row| advertising_expense_fact(row, expected).map(|(_, fact)| fact))
        .collect()
}

pub(super) async fn query_finance_facts(
    client: &Client,
    snapshot_ids: &[i64],
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<Vec<PublishedFinanceFact>, ReportingReadError> {
    let rows = client
        .query(FINANCE_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(rows.len())?;
    rows.iter()
        .map(|row| finance_fact(row, expected).map(|(_, fact)| fact))
        .collect()
}

pub(super) async fn query_stock_facts(
    client: &Client,
    snapshot_ids: &[i64],
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<Vec<PublishedStockFact>, ReportingReadError> {
    let rows = client
        .query(STOCK_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(rows.len())?;
    rows.iter()
        .map(|row| stock_fact(row, expected).map(|(_, fact)| fact))
        .collect()
}

pub(super) async fn query_price_facts(
    client: &Client,
    snapshot_ids: &[i64],
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<Vec<PublishedPriceFact>, ReportingReadError> {
    let rows = client
        .query(PRICE_FACTS_QUERY, &[&snapshot_ids])
        .await
        .map_err(|_| ReportingReadError::Unavailable)?;
    validate_fact_query_size(rows.len())?;
    rows.iter()
        .map(|row| price_fact(row, expected).map(|(_, fact)| fact))
        .collect()
}

pub(super) const SALES_FACTS_QUERY: &str = "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, sku, ordered_units, \
            operational_gmv_minor, cancelled_units, returned_units, currency \
     FROM daily_reporting.mcp_sales_facts \
     WHERE snapshot_id = ANY($1::bigint[]) \
     ORDER BY snapshot_id, business_date, sku \
     LIMIT 25001";

pub(super) const ADVERTISING_FACTS_QUERY: &str = "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, campaign_id, sku, \
            impressions, clicks, spend_minor, attributed_orders, attributed_revenue_minor, \
            currency, basket_additions, model_attributed_orders, \
            model_attributed_revenue_minor, product_price_minor, average_cpc_minor, \
            cpm_minor, cpl_minor \
     FROM daily_reporting.mcp_advertising_facts \
     WHERE snapshot_id = ANY($1::bigint[]) \
     ORDER BY snapshot_id, business_date, campaign_id, sku \
     LIMIT 25001";

pub(super) const ADVERTISING_EXPENSE_FACTS_QUERY: &str = "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, campaign_id, \
            money_spent_minor, bonus_spent_minor, prepayment_spent_minor, currency \
     FROM daily_reporting.mcp_advertising_expense_facts \
     WHERE snapshot_id = ANY($1::bigint[]) \
     ORDER BY snapshot_id, business_date, campaign_id \
     LIMIT 25001";

pub(super) const FINANCE_FACTS_QUERY: &str = "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, business_date, sku, sku_key, category, \
            amount_minor, line_count, unknown_type_count \
     FROM daily_reporting.mcp_finance_facts \
     WHERE snapshot_id = ANY($1::bigint[]) \
     ORDER BY snapshot_id, business_date, sku NULLS FIRST, category \
     LIMIT 25001";

pub(super) const STOCK_FACTS_QUERY: &str = "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, sku, warehouse_id, sellable_units \
     FROM daily_reporting.mcp_stock_facts \
     WHERE snapshot_id = ANY($1::bigint[]) \
     ORDER BY snapshot_id, sku, warehouse_id \
     LIMIT 25001";

pub(super) const PRICE_FACTS_QUERY: &str = "SELECT account_id, marketplace, cutoff_at, source_as_of, snapshot_status, \
            pagination_complete, snapshot_id, source, sku, price_minor, old_price_minor, currency \
     FROM daily_reporting.mcp_price_facts \
     WHERE snapshot_id = ANY($1::bigint[]) \
     ORDER BY snapshot_id, sku \
     LIMIT 25001";

pub(super) fn validate_fact_query_size(rows: usize) -> Result<(), ReportingReadError> {
    (rows <= MAX_FACT_ROWS)
        .then_some(())
        .ok_or(ReportingReadError::InvalidPublishedData)
}

pub(super) fn validate_advertising_counts(
    impressions: u64,
    clicks: u64,
) -> Result<(), ReportingReadError> {
    (clicks <= impressions)
        .then_some(())
        .ok_or(ReportingReadError::InvalidPublishedData)
}

pub(super) fn validate_finance_identity(
    sku: Option<u64>,
    sku_key: i64,
    line_count: u64,
    unknown_type_count: u64,
) -> Result<(), ReportingReadError> {
    let expected_sku_key = match sku {
        None => 0,
        Some(0) => return Err(ReportingReadError::InvalidPublishedData),
        Some(sku) => i64::try_from(sku).map_err(|_| ReportingReadError::InvalidPublishedData)?,
    };
    if sku_key == expected_sku_key && unknown_type_count <= line_count {
        Ok(())
    } else {
        Err(ReportingReadError::InvalidPublishedData)
    }
}

pub(super) fn validate_warehouse_id(value: &str) -> Result<(), ReportingReadError> {
    valid_warehouse_id(value)
        .then_some(())
        .ok_or(ReportingReadError::InvalidPublishedData)
}

pub(super) fn validate_price_relation(
    price_minor: u64,
    old_price_minor: Option<u64>,
) -> Result<(), ReportingReadError> {
    old_price_minor
        .is_none_or(|old| old >= price_minor)
        .then_some(())
        .ok_or(ReportingReadError::InvalidPublishedData)
}

pub(super) fn sales_fact(
    row: &Row,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<(i64, PublishedSalesFact), ReportingReadError> {
    let (snapshot_id, descriptor) = fact_descriptor(row, expected, SnapshotSource::Sales)?;
    validate_currency(&column::<String>(row, 14)?)?;
    Ok((
        snapshot_id,
        PublishedSalesFact {
            account_id: descriptor.account_id().to_owned(),
            business_date: column(row, 8)?,
            sku: positive_i64(column(row, 9)?)?,
            ordered_units: nonnegative_i32(column(row, 10)?)?,
            operational_gmv_minor: nonnegative_i64(column(row, 11)?)?,
            cancelled_units: nonnegative_optional_i32(column(row, 12)?)?,
            returned_units: nonnegative_optional_i32(column(row, 13)?)?,
        },
    ))
}

pub(super) fn advertising_fact(
    row: &Row,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<(i64, PublishedAdvertisingFact), ReportingReadError> {
    let (snapshot_id, descriptor) = fact_descriptor(row, expected, SnapshotSource::Advertising)?;
    validate_currency(&column::<String>(row, 16)?)?;
    let impressions = nonnegative_i64(column(row, 11)?)?;
    let clicks = nonnegative_i64(column(row, 12)?)?;
    validate_advertising_counts(impressions, clicks)?;
    Ok((
        snapshot_id,
        PublishedAdvertisingFact {
            account_id: descriptor.account_id().to_owned(),
            business_date: column(row, 8)?,
            campaign_id: positive_i64(column(row, 9)?)?,
            sku: nonnegative_i64(column(row, 10)?)?,
            impressions,
            clicks,
            spend_minor: nonnegative_i64(column(row, 13)?)?,
            attributed_orders: nonnegative_i32(column(row, 14)?)?,
            attributed_revenue_minor: nonnegative_i64(column(row, 15)?)?,
            basket_additions: nonnegative_i32(column(row, 17)?)?,
            model_attributed_orders: nonnegative_i32(column(row, 18)?)?,
            model_attributed_revenue_minor: nonnegative_i64(column(row, 19)?)?,
            product_price_minor: nonnegative_i64(column(row, 20)?)?,
            average_cpc_minor: nonnegative_optional_i64(column(row, 21)?)?,
            cpm_minor: nonnegative_optional_i64(column(row, 22)?)?,
            cpl_minor: nonnegative_optional_i64(column(row, 23)?)?,
        },
    ))
}

pub(super) fn advertising_expense_fact(
    row: &Row,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<(i64, PublishedAdvertisingExpenseFact), ReportingReadError> {
    let (snapshot_id, descriptor) = fact_descriptor(row, expected, SnapshotSource::Advertising)?;
    validate_currency(&column::<String>(row, 13)?)?;
    Ok((
        snapshot_id,
        PublishedAdvertisingExpenseFact {
            account_id: descriptor.account_id().to_owned(),
            business_date: column(row, 8)?,
            campaign_id: positive_i64(column(row, 9)?)?,
            money_spent_minor: nonnegative_i64(column(row, 10)?)?,
            bonus_spent_minor: nonnegative_i64(column(row, 11)?)?,
            prepayment_spent_minor: nonnegative_i64(column(row, 12)?)?,
        },
    ))
}

pub(super) fn finance_fact(
    row: &Row,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<(i64, PublishedFinanceFact), ReportingReadError> {
    let (snapshot_id, descriptor) = fact_descriptor(row, expected, SnapshotSource::Finance)?;
    let sku = nonnegative_optional_i64(column(row, 9)?)?;
    let sku_key: i64 = column(row, 10)?;
    let line_count = positive_i32(column(row, 13)?)?;
    let unknown_type_count = nonnegative_i32(column(row, 14)?)?;
    validate_finance_identity(sku, sku_key, line_count, unknown_type_count)?;
    Ok((
        snapshot_id,
        PublishedFinanceFact {
            account_id: descriptor.account_id().to_owned(),
            business_date: column(row, 8)?,
            sku,
            category: parse_finance_category(&column::<String>(row, 11)?)?,
            amount_minor: column(row, 12)?,
            line_count,
            unknown_type_count,
        },
    ))
}

pub(super) fn stock_fact(
    row: &Row,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<(i64, PublishedStockFact), ReportingReadError> {
    let (snapshot_id, descriptor) = fact_descriptor(row, expected, SnapshotSource::Stocks)?;
    let warehouse_id: String = column(row, 9)?;
    validate_warehouse_id(&warehouse_id)?;
    Ok((
        snapshot_id,
        PublishedStockFact {
            account_id: descriptor.account_id().to_owned(),
            sku: positive_i64(column(row, 8)?)?,
            warehouse_id,
            sellable_units: nonnegative_i32(column(row, 10)?)?,
            observed_at: descriptor.source_as_of(),
        },
    ))
}

pub(super) fn price_fact(
    row: &Row,
    expected: &BTreeMap<i64, SnapshotDescriptor>,
) -> Result<(i64, PublishedPriceFact), ReportingReadError> {
    let (snapshot_id, descriptor) = fact_descriptor(row, expected, SnapshotSource::Prices)?;
    validate_currency(&column::<String>(row, 11)?)?;
    let price_minor = nonnegative_i64(column(row, 9)?)?;
    let old_price_minor = nonnegative_optional_i64(column(row, 10)?)?;
    validate_price_relation(price_minor, old_price_minor)?;
    Ok((
        snapshot_id,
        PublishedPriceFact {
            account_id: descriptor.account_id().to_owned(),
            sku: positive_i64(column(row, 8)?)?,
            price_minor,
            old_price_minor,
            observed_at: descriptor.source_as_of(),
        },
    ))
}

#[derive(Debug, Clone, Copy)]
pub(super) struct FactProvenance<'a> {
    pub(super) snapshot_id: i64,
    pub(super) source: SnapshotSource,
    pub(super) account_id: &'a str,
    pub(super) marketplace: Marketplace,
    pub(super) cutoff_at: DateTime<Utc>,
    pub(super) source_as_of: DateTime<Utc>,
    pub(super) status: SnapshotStatus,
    pub(super) pagination_complete: bool,
}

pub(super) fn validate_fact_provenance(
    actual: FactProvenance<'_>,
    descriptor: &SnapshotDescriptor,
    expected_source: SnapshotSource,
) -> Result<(), ReportingReadError> {
    if actual.snapshot_id > 0
        && actual.source == expected_source
        && descriptor.source() == expected_source
        && descriptor.account_id() == actual.account_id
        && descriptor.marketplace() == actual.marketplace
        && descriptor.cutoff_at() == actual.cutoff_at
        && descriptor.source_as_of() == actual.source_as_of
        && descriptor.status() == actual.status
        && descriptor.pagination_complete() == actual.pagination_complete
    {
        Ok(())
    } else {
        Err(ReportingReadError::InvalidPublishedData)
    }
}

pub(super) fn fact_descriptor<'a>(
    row: &Row,
    expected: &'a BTreeMap<i64, SnapshotDescriptor>,
    expected_source: SnapshotSource,
) -> Result<(i64, &'a SnapshotDescriptor), ReportingReadError> {
    let account_id: String = column(row, 0)?;
    let marketplace = parse_marketplace(&column::<String>(row, 1)?)?;
    let cutoff_at: DateTime<Utc> = column(row, 2)?;
    let source_as_of: DateTime<Utc> = column(row, 3)?;
    let status = parse_snapshot_status(&column::<String>(row, 4)?)?;
    let pagination_complete: bool = column(row, 5)?;
    let snapshot_id: i64 = column(row, 6)?;
    let source = parse_source(&column::<String>(row, 7)?)?;
    let descriptor = expected
        .get(&snapshot_id)
        .ok_or(ReportingReadError::InvalidPublishedData)?;
    let provenance = FactProvenance {
        snapshot_id,
        source,
        account_id: &account_id,
        marketplace,
        cutoff_at,
        source_as_of,
        status,
        pagination_complete,
    };
    validate_fact_provenance(provenance, descriptor, expected_source)?;
    Ok((snapshot_id, descriptor))
}
