use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use super::{
    kpi::{AdvertisingMetricInput, KpiError, KpiSummary, SalesMetricInput, calculate_kpis},
    postgres_snapshot::{
        PublishedAdvertisingExpenseFact, PublishedFinanceFact, PublishedReportFacts,
        PublishedSalesFact,
    },
    snapshot::{FrozenSnapshotManifest, SnapshotQuality, SnapshotSource},
    xlsx::{AdvertisingDetail, InventoryDetail, SalesDetail, SourceQualityDetail},
};

const MAX_DATASET_ROWS: usize = 25_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SalesReportRow {
    pub account_id: String,
    pub sku: String,
    pub ordered_units: u64,
    pub operational_gmv_minor: u64,
    pub cancelled_units: Option<u64>,
    pub returned_units: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvertisingReportRow {
    pub account_id: String,
    pub campaign_id: String,
    pub sku: String,
    pub impressions: u64,
    pub clicks: u64,
    pub spend_minor: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryReportRow {
    pub account_id: String,
    pub sku: String,
    pub sellable_stock: u64,
    pub stock_observed: bool,
    pub price_minor: Option<u64>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceQualityRow {
    pub account_id: String,
    pub source: SnapshotSource,
    pub quality: SnapshotQuality,
    pub source_as_of: DateTime<Utc>,
    pub row_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportDataset {
    pub kpis: KpiSummary,
    pub sales: Vec<SalesReportRow>,
    pub advertising: Vec<AdvertisingReportRow>,
    pub advertising_expenses: Vec<PublishedAdvertisingExpenseFact>,
    pub finance: Vec<PublishedFinanceFact>,
    pub inventory: Vec<InventoryReportRow>,
    pub source_quality: Vec<SourceQualityRow>,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum DatasetError {
    #[error("published report facts do not match the frozen manifest")]
    InvalidFacts,
    #[error("published report fact aggregation overflowed")]
    Overflow,
}

impl ReportDataset {
    pub fn from_published(
        manifest: &FrozenSnapshotManifest,
        facts: PublishedReportFacts,
    ) -> Result<Self, DatasetError> {
        if let Err(error) = validate_row_counts(manifest, &facts) {
            tracing::warn!(stage = "row_counts", "published report dataset rejected");
            return Err(error);
        }
        let expected_accounts = manifest
            .snapshots()
            .iter()
            .map(super::snapshot::SnapshotDescriptor::account_id)
            .collect::<std::collections::BTreeSet<_>>();
        if facts
            .sales
            .iter()
            .map(|fact| fact.account_id.as_str())
            .chain(
                facts
                    .advertising
                    .iter()
                    .map(|fact| fact.account_id.as_str()),
            )
            .chain(
                facts
                    .advertising_expenses
                    .iter()
                    .map(|fact| fact.account_id.as_str()),
            )
            .chain(facts.finance.iter().map(|fact| fact.account_id.as_str()))
            .chain(facts.stocks.iter().map(|fact| fact.account_id.as_str()))
            .chain(facts.prices.iter().map(|fact| fact.account_id.as_str()))
            .any(|account| !expected_accounts.contains(account))
        {
            tracing::warn!(stage = "account_scope", "published report dataset rejected");
            return Err(DatasetError::InvalidFacts);
        }

        let kpis = calculate_kpis(
            &facts.sales.iter().map(sales_metric).collect::<Vec<_>>(),
            &facts
                .advertising
                .iter()
                .map(|fact| AdvertisingMetricInput {
                    impressions: fact.impressions,
                    clicks: fact.clicks,
                    spend_minor: fact.spend_minor,
                    attributed_orders: fact.attributed_orders,
                    attributed_revenue_minor: fact.attributed_revenue_minor,
                })
                .collect::<Vec<_>>(),
        )
        .map_err(|error| {
            tracing::warn!(stage = "kpi", "published report dataset rejected");
            map_kpi(error)
        })?;

        let advertising_expenses = facts.advertising_expenses;
        let finance = facts.finance;
        let mut sales = BTreeMap::new();
        for fact in facts.sales {
            let row = sales.entry((fact.account_id, fact.sku)).or_insert((
                0u64,
                0u64,
                Some(0u64),
                Some(0u64),
            ));
            row.0 = add(row.0, fact.ordered_units)?;
            row.1 = add(row.1, fact.operational_gmv_minor)?;
            row.2 = add_available(row.2, fact.cancelled_units)?;
            row.3 = add_available(row.3, fact.returned_units)?;
        }
        let sales = sales
            .into_iter()
            .map(
                |((account_id, sku), (ordered, gmv, cancelled, returned))| SalesReportRow {
                    account_id,
                    sku: sku.to_string(),
                    ordered_units: ordered,
                    operational_gmv_minor: gmv,
                    cancelled_units: cancelled,
                    returned_units: returned,
                },
            )
            .collect();

        let mut advertising = BTreeMap::new();
        for fact in facts.advertising {
            let row = advertising
                .entry((fact.account_id, fact.campaign_id, fact.sku))
                .or_insert((0u64, 0u64, 0u64, 0u64, 0u64));
            row.0 = add(row.0, fact.impressions)?;
            row.1 = add(row.1, fact.clicks)?;
            row.2 = add(row.2, fact.spend_minor)?;
            row.3 = add(row.3, fact.attributed_orders)?;
            row.4 = add(row.4, fact.attributed_revenue_minor)?;
        }
        let advertising = advertising
            .into_iter()
            .map(
                |(
                    (account_id, campaign_id, sku),
                    (impressions, clicks, spend, orders, revenue),
                )| AdvertisingReportRow {
                    account_id,
                    campaign_id: campaign_id.to_string(),
                    // Ozon Performance daily statistics is campaign-level.
                    // Persistence uses zero as an explicit unavailable-SKU
                    // sentinel; never render it as if product 0 existed.
                    sku: if sku == 0 {
                        "N/D".to_owned()
                    } else {
                        sku.to_string()
                    },
                    impressions,
                    clicks,
                    spend_minor: spend,
                    attributed_orders: orders,
                    attributed_revenue_minor: revenue,
                },
            )
            .collect();

        let mut inventory = BTreeMap::new();
        for fact in facts.stocks {
            let row = inventory.entry((fact.account_id, fact.sku)).or_insert((
                0u64,
                None,
                fact.observed_at,
                true,
            ));
            row.0 = add(row.0, fact.sellable_units)?;
            row.2 = row.2.max(fact.observed_at);
            row.3 = true;
        }
        for fact in facts.prices {
            let row = inventory.entry((fact.account_id, fact.sku)).or_insert((
                0u64,
                None,
                fact.observed_at,
                false,
            ));
            if row.1.replace(fact.price_minor).is_some() {
                tracing::warn!(
                    stage = "duplicate_price",
                    "published report dataset rejected"
                );
                return Err(DatasetError::InvalidFacts);
            }
            row.2 = row.2.max(fact.observed_at);
        }
        let inventory = inventory
            .into_iter()
            .map(
                |((account_id, sku), (stock, price, observed_at, stock_observed))| {
                    InventoryReportRow {
                        account_id,
                        sku: sku.to_string(),
                        sellable_stock: stock,
                        stock_observed,
                        price_minor: price,
                        observed_at,
                    }
                },
            )
            .collect();
        let source_quality = manifest
            .snapshots()
            .iter()
            .map(|snapshot| SourceQualityRow {
                account_id: snapshot.account_id().to_owned(),
                source: snapshot.source(),
                quality: snapshot.quality(),
                source_as_of: snapshot.source_as_of(),
                row_count: snapshot.row_count(),
            })
            .collect();
        Ok(Self {
            kpis,
            sales,
            advertising,
            advertising_expenses,
            finance,
            inventory,
            source_quality,
        })
    }

    #[must_use]
    pub fn sales_details(&self) -> Vec<SalesDetail<'_>> {
        self.sales
            .iter()
            .map(|row| SalesDetail {
                account_id: &row.account_id,
                sku: &row.sku,
                ordered_units: row.ordered_units,
                operational_gmv_minor: row.operational_gmv_minor,
                cancelled_units: row.cancelled_units,
                returned_units: row.returned_units,
            })
            .collect()
    }

    #[must_use]
    pub fn advertising_details(&self) -> Vec<AdvertisingDetail<'_>> {
        self.advertising
            .iter()
            .map(|row| AdvertisingDetail {
                account_id: &row.account_id,
                campaign_id: &row.campaign_id,
                sku: &row.sku,
                impressions: row.impressions,
                clicks: row.clicks,
                spend_minor: row.spend_minor,
                attributed_orders: row.attributed_orders,
                attributed_revenue_minor: row.attributed_revenue_minor,
            })
            .collect()
    }

    #[must_use]
    pub fn inventory_details(&self) -> Vec<InventoryDetail<'_>> {
        self.inventory
            .iter()
            .map(|row| InventoryDetail {
                account_id: &row.account_id,
                sku: &row.sku,
                sellable_stock: row.sellable_stock,
                price_minor: row.price_minor,
                observed_at: row.observed_at,
            })
            .collect()
    }

    #[must_use]
    pub fn quality_details(&self) -> Vec<SourceQualityDetail<'_>> {
        self.source_quality
            .iter()
            .map(|row| SourceQualityDetail {
                account_id: &row.account_id,
                source: row.source,
                quality: row.quality,
                source_as_of: row.source_as_of,
                row_count: row.row_count,
            })
            .collect()
    }
}

fn sales_metric(fact: &PublishedSalesFact) -> SalesMetricInput {
    SalesMetricInput {
        ordered_units: fact.ordered_units,
        operational_gmv_minor: fact.operational_gmv_minor,
        cancelled_units: fact.cancelled_units,
        returned_units: fact.returned_units,
    }
}

fn validate_row_counts(
    manifest: &FrozenSnapshotManifest,
    facts: &PublishedReportFacts,
) -> Result<(), DatasetError> {
    let total_rows = facts
        .sales
        .len()
        .checked_add(facts.advertising.len())
        .and_then(|value| value.checked_add(facts.finance.len()))
        .and_then(|value| value.checked_add(facts.stocks.len()))
        .and_then(|value| value.checked_add(facts.prices.len()))
        .ok_or(DatasetError::Overflow)?;
    let expected_rows = |source| {
        manifest
            .snapshots()
            .iter()
            .filter(|snapshot| snapshot.source() == source)
            .try_fold(0usize, |total, snapshot| {
                total.checked_add(snapshot.row_count() as usize)
            })
    };
    if total_rows > MAX_DATASET_ROWS
        || expected_rows(SnapshotSource::Sales) != Some(facts.sales.len())
        || expected_rows(SnapshotSource::Advertising) != Some(facts.advertising.len())
        || expected_rows(SnapshotSource::Finance) != Some(facts.finance.len())
        || expected_rows(SnapshotSource::Stocks) != Some(facts.stocks.len())
        || expected_rows(SnapshotSource::Prices) != Some(facts.prices.len())
    {
        Err(DatasetError::InvalidFacts)
    } else {
        Ok(())
    }
}

fn map_kpi(error: KpiError) -> DatasetError {
    match error {
        KpiError::InvalidAdvertisingCounters => DatasetError::InvalidFacts,
        KpiError::Overflow => DatasetError::Overflow,
    }
}

fn add(left: u64, right: u64) -> Result<u64, DatasetError> {
    left.checked_add(right).ok_or(DatasetError::Overflow)
}

fn add_available(left: Option<u64>, right: Option<u64>) -> Result<Option<u64>, DatasetError> {
    match (left, right) {
        (Some(left), Some(right)) => add(left, right).map(Some),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::*;
    use crate::reporting::{
        postgres_snapshot::{PublishedAdvertisingFact, PublishedPriceFact, PublishedStockFact},
        snapshot::{AccountScope, Marketplace, SnapshotDescriptor, SnapshotStatus},
    };

    fn cutoff() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 16, 12, 0, 0).unwrap()
    }

    fn manifest_with_counts(accounts: &[&str], counts: [u32; 5]) -> FrozenSnapshotManifest {
        let mut id = 1;
        let mut snapshots = Vec::new();
        for account in accounts {
            for (source_index, source) in [
                SnapshotSource::Sales,
                SnapshotSource::Advertising,
                SnapshotSource::Finance,
                SnapshotSource::Stocks,
                SnapshotSource::Prices,
            ]
            .into_iter()
            .enumerate()
            {
                let source_as_of = cutoff() - Duration::minutes(30);
                let (start, end) = if matches!(
                    source,
                    SnapshotSource::Sales | SnapshotSource::Advertising | SnapshotSource::Finance
                ) {
                    (cutoff() - Duration::days(1), cutoff())
                } else {
                    (source_as_of, source_as_of)
                };
                snapshots.push(
                    SnapshotDescriptor::new(
                        id,
                        (*account).to_owned(),
                        Marketplace::Ozon,
                        source,
                        cutoff(),
                        source_as_of,
                        start,
                        end,
                        counts[source_index],
                        true,
                        SnapshotStatus::Succeeded,
                    )
                    .unwrap(),
                );
                id += 1;
            }
        }
        FrozenSnapshotManifest::new(
            cutoff(),
            accounts
                .iter()
                .map(|account| AccountScope::new((*account).to_owned(), Marketplace::Ozon).unwrap())
                .collect(),
            snapshots,
        )
        .unwrap()
    }

    fn manifest(accounts: &[&str]) -> FrozenSnapshotManifest {
        manifest_with_counts(accounts, [1, 1, 0, 1, 1])
    }

    fn facts(account: &str) -> PublishedReportFacts {
        PublishedReportFacts {
            sales: vec![PublishedSalesFact {
                account_id: account.to_owned(),
                business_date: cutoff().date_naive(),
                sku: 10,
                ordered_units: 2,
                operational_gmv_minor: 20_000,
                cancelled_units: Some(1),
                returned_units: Some(0),
            }],
            advertising: vec![PublishedAdvertisingFact {
                account_id: account.to_owned(),
                business_date: cutoff().date_naive(),
                campaign_id: 20,
                sku: 10,
                impressions: 100,
                clicks: 10,
                spend_minor: 1_000,
                attributed_orders: 1,
                attributed_revenue_minor: 10_000,
                basket_additions: 2,
                model_attributed_orders: 1,
                model_attributed_revenue_minor: 10_000,
                product_price_minor: 10_000,
                average_cpc_minor: Some(100),
                cpm_minor: Some(10_000),
                cpl_minor: Some(500),
            }],
            advertising_expenses: vec![],
            finance: vec![],
            stocks: vec![PublishedStockFact {
                account_id: account.to_owned(),
                sku: 10,
                warehouse_id: "fbo".to_owned(),
                sellable_units: 3,
                observed_at: cutoff() - Duration::minutes(20),
            }],
            prices: vec![PublishedPriceFact {
                account_id: account.to_owned(),
                sku: 10,
                price_minor: 12_345,
                old_price_minor: None,
                observed_at: cutoff() - Duration::minutes(10),
            }],
        }
    }

    #[test]
    fn dataset_aggregates_rows_and_produces_renderer_views() {
        let mut input = facts("store");
        input
            .advertising_expenses
            .push(PublishedAdvertisingExpenseFact {
                account_id: "store".to_owned(),
                business_date: cutoff().date_naive(),
                campaign_id: 20,
                money_spent_minor: 1_000,
                bonus_spent_minor: 100,
                prepayment_spent_minor: 900,
            });
        input.finance.push(PublishedFinanceFact {
            account_id: "store".to_owned(),
            business_date: cutoff().date_naive(),
            sku: Some(10),
            category: crate::reporting::postgres_collector::FinanceCategory::Sale,
            amount_minor: 20_000,
            line_count: 1,
            unknown_type_count: 0,
        });
        input.sales.push(PublishedSalesFact {
            ordered_units: 3,
            operational_gmv_minor: 30_000,
            cancelled_units: Some(0),
            returned_units: Some(1),
            ..input.sales[0].clone()
        });
        input.stocks.push(PublishedStockFact {
            warehouse_id: "fbs".to_owned(),
            sellable_units: 4,
            observed_at: cutoff() - Duration::minutes(5),
            ..input.stocks[0].clone()
        });
        let dataset = ReportDataset::from_published(
            &manifest_with_counts(&["store"], [2, 1, 1, 2, 1]),
            input,
        )
        .unwrap();
        assert_eq!(dataset.kpis.ordered_units, 5);
        assert_eq!(dataset.sales[0].operational_gmv_minor, 50_000);
        assert_eq!(dataset.inventory[0].sellable_stock, 7);
        assert!(dataset.inventory[0].stock_observed);
        assert_eq!(dataset.inventory[0].price_minor, Some(12_345));
        assert_eq!(
            dataset.inventory[0].observed_at,
            cutoff() - Duration::minutes(5)
        );
        assert_eq!(dataset.sales_details()[0].sku, "10");
        assert_eq!(dataset.advertising_details()[0].campaign_id, "20");
        assert_eq!(dataset.inventory_details()[0].sellable_stock, 7);
        assert_eq!(dataset.quality_details().len(), 5);
        assert_eq!(dataset.advertising_expenses.len(), 1);
        assert_eq!(dataset.finance.len(), 1);
    }

    #[test]
    fn campaign_level_advertising_never_claims_a_product_sku() {
        let mut input = facts("store");
        input.advertising[0].sku = 0;
        let dataset = ReportDataset::from_published(&manifest(&["store"]), input).unwrap();
        assert_eq!(dataset.advertising[0].sku, "N/D");
        assert_eq!(dataset.advertising_details()[0].sku, "N/D");
        assert_eq!(add_available(None, Some(1)), Ok(None));
    }

    #[test]
    fn price_only_inventory_does_not_claim_a_stock_observation() {
        let mut input = facts("store");
        input.stocks.clear();
        let dataset = ReportDataset::from_published(
            &manifest_with_counts(&["store"], [1, 1, 0, 0, 1]),
            input,
        )
        .unwrap();
        assert_eq!(dataset.inventory.len(), 1);
        assert!(!dataset.inventory[0].stock_observed);
        assert_eq!(dataset.inventory[0].sellable_stock, 0);
    }

    #[test]
    fn dataset_keeps_same_sku_in_different_accounts_separate() {
        let mut input = facts("first");
        let second = facts("second");
        input.sales.extend(second.sales);
        input.advertising.extend(second.advertising);
        input.stocks.extend(second.stocks);
        input.prices.extend(second.prices);
        let dataset =
            ReportDataset::from_published(&manifest(&["first", "second"]), input).unwrap();
        assert_eq!(dataset.sales.len(), 2);
        assert_eq!(dataset.inventory.len(), 2);
    }

    #[test]
    fn foreign_duplicate_invalid_and_overflowing_facts_fail_closed() {
        assert_eq!(
            ReportDataset::from_published(&manifest(&["store"]), facts("foreign")),
            Err(DatasetError::InvalidFacts)
        );
        let mut duplicate_price = facts("store");
        duplicate_price
            .prices
            .push(duplicate_price.prices[0].clone());
        assert_eq!(
            ReportDataset::from_published(
                &manifest_with_counts(&["store"], [1, 1, 0, 1, 2]),
                duplicate_price
            ),
            Err(DatasetError::InvalidFacts)
        );
        let mut invalid_ad = facts("store");
        invalid_ad.advertising[0].clicks = 101;
        assert_eq!(
            ReportDataset::from_published(&manifest(&["store"]), invalid_ad),
            Err(DatasetError::InvalidFacts)
        );
        let mut missing_stock = facts("store");
        missing_stock.stocks.clear();
        assert_eq!(
            validate_row_counts(&manifest(&["store"]), &missing_stock),
            Err(DatasetError::InvalidFacts)
        );
        assert_eq!(
            ReportDataset::from_published(&manifest(&["store"]), missing_stock),
            Err(DatasetError::InvalidFacts)
        );
        let mut excessive = facts("store");
        excessive.sales = vec![excessive.sales[0].clone(); MAX_DATASET_ROWS + 1];
        assert_eq!(
            validate_row_counts(
                &manifest_with_counts(&["store"], [MAX_DATASET_ROWS as u32 + 1, 1, 0, 1, 1],),
                &excessive
            ),
            Err(DatasetError::InvalidFacts)
        );
        assert_eq!(add(u64::MAX, 1), Err(DatasetError::Overflow));
        assert_eq!(map_kpi(KpiError::Overflow), DatasetError::Overflow);
    }
}
