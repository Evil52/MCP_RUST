use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};

use super::{
    ReportKey, ReportKind,
    bundle::{
        BundleError, DryRunReceipt, ReportBundle, ReportBundleRequest, inspect_dry_run,
        render_bundle,
    },
    dataset::{DatasetError, ReportDataset},
    postgres_snapshot::PublishedReportFacts,
    reporting_interval,
    rules::{PriorityProblem, RuleError, RuleInput, priority_problems},
    snapshot::{FrozenSnapshotManifest, SnapshotSource},
};

/// A fully rendered report that has only been inspected in memory.
///
/// The receipt deliberately remains `persisted = false`; callers must not put
/// this object into the delivery outbox or treat it as an uploaded artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportPreview {
    pub bundle: ReportBundle,
    pub receipt: DryRunReceipt,
    pub problems: Vec<PriorityProblem>,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum PreviewError {
    #[error("published snapshots do not match the requested report interval")]
    InvalidManifest,
    #[error("published report facts are invalid")]
    InvalidDataset,
    #[error("daily report rules rejected the published facts")]
    InvalidRules,
    #[error("daily report artifact rendering failed")]
    Rendering,
}

pub fn render_published_preview(
    key: &ReportKey,
    manager_name: &str,
    generated_at: DateTime<Utc>,
    manifest: &FrozenSnapshotManifest,
    facts: PublishedReportFacts,
) -> Result<ReportPreview, PreviewError> {
    validate_manifest_period(key, generated_at, manifest)?;
    let dataset = ReportDataset::from_published(manifest, facts).map_err(map_dataset)?;
    let inputs = rule_inputs(&dataset)?;
    // Until a target-CPO/minimum-spend policy is approved, the maximum value
    // suppresses spend-without-order actions. Campaign-level Ozon Performance
    // rows also use `N/D` for SKU and are never attributed to a product here.
    let problems = priority_problems(&inputs, manifest.recommendations_allowed(), u64::MAX)
        .map_err(map_rules)?;
    let bundle = render_bundle(ReportBundleRequest {
        key,
        manager_name,
        generated_at,
        dataset: &dataset,
        problems: &problems,
    })
    .map_err(map_bundle)?;
    let receipt = inspect_dry_run(&bundle).map_err(map_bundle)?;
    Ok(ReportPreview {
        bundle,
        receipt,
        problems,
    })
}

fn validate_manifest_period(
    key: &ReportKey,
    generated_at: DateTime<Utc>,
    manifest: &FrozenSnapshotManifest,
) -> Result<(), PreviewError> {
    let expected = reporting_interval(key).map_err(|_| PreviewError::InvalidManifest)?;
    let expected_cutoff = match key.kind {
        ReportKind::Morning => expected
            .1
            .checked_add_signed(Duration::hours(8))
            .ok_or(PreviewError::InvalidManifest)?,
        ReportKind::Evening => expected.1,
    };
    if manifest.cutoff_at() != expected_cutoff
        || generated_at < manifest.cutoff_at()
        || manifest.snapshots().iter().any(|snapshot| {
            matches!(
                snapshot.source(),
                SnapshotSource::Sales | SnapshotSource::Advertising
            ) && snapshot.period() != expected
        })
    {
        Err(PreviewError::InvalidManifest)
    } else {
        Ok(())
    }
}

#[derive(Default)]
struct RuleAccumulator {
    stock: Option<u64>,
    sold: u64,
    sales_gmv: u64,
    ad_clicks: u64,
    ad_spend: u64,
    attributed_orders: u64,
    attributed_revenue: u64,
}

fn rule_inputs(dataset: &ReportDataset) -> Result<Vec<RuleInput>, PreviewError> {
    let mut rows = BTreeMap::<(String, u64), RuleAccumulator>::new();
    for inventory in &dataset.inventory {
        if !inventory.stock_observed {
            continue;
        }
        let sku = inventory
            .sku
            .parse::<u64>()
            .map_err(|_| PreviewError::InvalidRules)?;
        let row = rows.entry((inventory.account_id.clone(), sku)).or_default();
        if row.stock.replace(inventory.sellable_stock).is_some() {
            return Err(PreviewError::InvalidRules);
        }
    }
    for sale in &dataset.sales {
        let sku = sale
            .sku
            .parse::<u64>()
            .map_err(|_| PreviewError::InvalidRules)?;
        let row = rows.entry((sale.account_id.clone(), sku)).or_default();
        row.sold = checked_add(row.sold, sale.ordered_units)?;
        row.sales_gmv = checked_add(row.sales_gmv, sale.operational_gmv_minor)?;
    }
    for advertising in &dataset.advertising {
        let Ok(sku) = advertising.sku.parse::<u64>() else {
            continue;
        };
        let row = rows
            .entry((advertising.account_id.clone(), sku))
            .or_default();
        row.ad_clicks = checked_add(row.ad_clicks, advertising.clicks)?;
        row.ad_spend = checked_add(row.ad_spend, advertising.spend_minor)?;
        row.attributed_orders = checked_add(row.attributed_orders, advertising.attributed_orders)?;
        row.attributed_revenue =
            checked_add(row.attributed_revenue, advertising.attributed_revenue_minor)?;
    }
    rows.into_iter()
        // A stock recommendation is only safe when the published inventory
        // source actually produced an observation for this account/SKU.
        .filter_map(|((account_id, sku), row)| {
            row.stock.map(|stock| {
                Ok(RuleInput {
                    account_id,
                    sku,
                    sellable_stock: stock,
                    sold_units: row.sold,
                    sales_window_days: 1,
                    sales_gmv_minor: row.sales_gmv,
                    lead_time_days: None,
                    ad_clicks: row.ad_clicks,
                    ad_spend_minor: row.ad_spend,
                    attributed_orders: row.attributed_orders,
                    attributed_revenue_minor: row.attributed_revenue,
                    target_cpo_minor: None,
                    target_drr_bps: None,
                })
            })
        })
        .collect()
}

fn checked_add(left: u64, right: u64) -> Result<u64, PreviewError> {
    left.checked_add(right).ok_or(PreviewError::InvalidRules)
}

fn map_dataset(_: DatasetError) -> PreviewError {
    PreviewError::InvalidDataset
}

fn map_rules(_: RuleError) -> PreviewError {
    PreviewError::InvalidRules
}

fn map_bundle(error: BundleError) -> PreviewError {
    match error {
        BundleError::InvalidInput => PreviewError::InvalidDataset,
        BundleError::Rendering | BundleError::Integrity => PreviewError::Rendering,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone};

    use super::*;
    use crate::reporting::{
        postgres_snapshot::{
            PublishedAdvertisingFact, PublishedPriceFact, PublishedSalesFact, PublishedStockFact,
        },
        snapshot::{AccountScope, Marketplace, SnapshotDescriptor, SnapshotStatus},
    };

    fn utc(day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, day, hour, 0, 0).unwrap()
    }

    fn key(kind: ReportKind) -> ReportKey {
        ReportKey {
            local_date: NaiveDate::from_ymd_opt(2026, 8, 17).unwrap(),
            kind,
            recipient_id: "diana".to_owned(),
            report_version: 1,
        }
    }

    fn manifest(kind: ReportKind, status: SnapshotStatus) -> FrozenSnapshotManifest {
        let key = key(kind);
        let period = reporting_interval(&key).unwrap();
        let cutoff = match kind {
            ReportKind::Morning => period.1 + Duration::hours(8),
            ReportKind::Evening => period.1,
        };
        let sources = [
            (SnapshotSource::Sales, 1),
            (SnapshotSource::Advertising, 1),
            (SnapshotSource::Stocks, 1),
            (SnapshotSource::Prices, 1),
        ];
        let snapshots = sources
            .into_iter()
            .enumerate()
            .map(|(index, (source, rows))| {
                let observed = cutoff - Duration::minutes(30);
                let (start, end) =
                    if matches!(source, SnapshotSource::Sales | SnapshotSource::Advertising) {
                        period
                    } else {
                        (observed, observed)
                    };
                SnapshotDescriptor::new(
                    index as i64 + 1,
                    "store".to_owned(),
                    Marketplace::Ozon,
                    source,
                    cutoff,
                    observed,
                    start,
                    end,
                    rows,
                    status == SnapshotStatus::Succeeded,
                    status,
                )
                .unwrap()
            })
            .collect();
        FrozenSnapshotManifest::new(
            cutoff,
            vec![AccountScope::new("store".to_owned(), Marketplace::Ozon).unwrap()],
            snapshots,
        )
        .unwrap()
    }

    fn facts(ad_sku: u64) -> PublishedReportFacts {
        PublishedReportFacts {
            sales: vec![PublishedSalesFact {
                account_id: "store".to_owned(),
                business_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
                sku: 10,
                ordered_units: 4,
                operational_gmv_minor: 40_000,
                cancelled_units: Some(0),
                returned_units: Some(0),
            }],
            advertising: vec![PublishedAdvertisingFact {
                account_id: "store".to_owned(),
                business_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
                campaign_id: 20,
                sku: ad_sku,
                impressions: 100,
                clicks: 10,
                spend_minor: 1_000,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            }],
            stocks: vec![PublishedStockFact {
                account_id: "store".to_owned(),
                sku: 10,
                warehouse_id: "fbo".to_owned(),
                sellable_units: 0,
                observed_at: utc(17, 2),
            }],
            prices: vec![PublishedPriceFact {
                account_id: "store".to_owned(),
                sku: 10,
                price_minor: 10_000,
                old_price_minor: None,
                observed_at: utc(17, 2),
            }],
        }
    }

    #[test]
    fn complete_published_input_renders_one_inspected_preview() {
        let key = key(ReportKind::Morning);
        let preview = render_published_preview(
            &key,
            "Диана",
            utc(17, 4),
            &manifest(ReportKind::Morning, SnapshotStatus::Succeeded),
            facts(0),
        )
        .unwrap();
        assert_eq!(preview.problems.len(), 1);
        assert_eq!(preview.problems[0].sku, 10);
        assert_eq!(
            preview.problems[0].kind,
            super::super::rules::ProblemKind::Stockout
        );
        assert!(preview.bundle.html.contains("товар закончился"));
        assert!(!preview.bundle.xlsx.is_empty());
        assert_eq!(preview.receipt.size_bytes, preview.bundle.xlsx.len());
        assert!(!preview.receipt.persisted);
    }

    #[test]
    fn partial_input_renders_facts_but_suppresses_actions() {
        let key = key(ReportKind::Morning);
        let preview = render_published_preview(
            &key,
            "Диана",
            utc(17, 4),
            &manifest(ReportKind::Morning, SnapshotStatus::Partial),
            facts(10),
        )
        .unwrap();
        assert!(preview.problems.is_empty());
        assert!(preview.bundle.html.contains("Рекомендации отключены"));
    }

    #[test]
    fn wrong_period_future_generation_and_invalid_facts_fail_closed() {
        let key = key(ReportKind::Morning);
        let mut wrong_period = manifest(ReportKind::Morning, SnapshotStatus::Succeeded);
        wrong_period = FrozenSnapshotManifest::new(
            wrong_period.cutoff_at(),
            vec![AccountScope::new("store".to_owned(), Marketplace::Ozon).unwrap()],
            wrong_period
                .snapshots()
                .iter()
                .enumerate()
                .map(|(index, snapshot)| {
                    let (start, end) = if snapshot.source() == SnapshotSource::Sales {
                        (utc(15, 19), utc(16, 18))
                    } else {
                        snapshot.period()
                    };
                    SnapshotDescriptor::new(
                        index as i64 + 10,
                        snapshot.account_id().to_owned(),
                        snapshot.marketplace(),
                        snapshot.source(),
                        snapshot.cutoff_at(),
                        snapshot.source_as_of(),
                        start,
                        end,
                        snapshot.row_count(),
                        snapshot.pagination_complete(),
                        snapshot.status(),
                    )
                    .unwrap()
                })
                .collect(),
        )
        .unwrap();
        assert_eq!(
            render_published_preview(&key, "Диана", utc(17, 4), &wrong_period, facts(0)),
            Err(PreviewError::InvalidManifest)
        );
        assert_eq!(
            render_published_preview(
                &key,
                "Диана",
                utc(17, 2),
                &manifest(ReportKind::Morning, SnapshotStatus::Succeeded),
                facts(0),
            ),
            Err(PreviewError::InvalidManifest)
        );
        let mut invalid = facts(0);
        invalid.sales[0].account_id = "foreign".to_owned();
        assert_eq!(
            render_published_preview(
                &key,
                "Диана",
                utc(17, 4),
                &manifest(ReportKind::Morning, SnapshotStatus::Succeeded),
                invalid,
            ),
            Err(PreviewError::InvalidDataset)
        );
    }

    #[test]
    fn campaign_level_ads_are_not_assigned_to_a_product() {
        let dataset = ReportDataset::from_published(
            &manifest(ReportKind::Morning, SnapshotStatus::Succeeded),
            facts(0),
        )
        .unwrap();
        let rows = rule_inputs(&dataset).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ad_clicks, 0);
        assert_eq!(rows[0].ad_spend_minor, 0);
        let mut price_only = dataset;
        price_only.inventory[0].stock_observed = false;
        assert!(rule_inputs(&price_only).unwrap().is_empty());
    }

    #[test]
    fn duplicate_inventory_and_overflow_are_rejected() {
        let mut dataset = ReportDataset::from_published(
            &manifest(ReportKind::Morning, SnapshotStatus::Succeeded),
            facts(10),
        )
        .unwrap();
        dataset.inventory.push(dataset.inventory[0].clone());
        assert_eq!(rule_inputs(&dataset), Err(PreviewError::InvalidRules));
        assert_eq!(checked_add(u64::MAX, 1), Err(PreviewError::InvalidRules));
    }

    #[test]
    fn internal_error_mappings_are_stable_and_payload_free() {
        for error in [RuleError::InvalidInput, RuleError::DuplicateSku] {
            assert_eq!(map_rules(error), PreviewError::InvalidRules);
        }
        assert_eq!(
            map_bundle(BundleError::InvalidInput),
            PreviewError::InvalidDataset
        );
        for error in [BundleError::Rendering, BundleError::Integrity] {
            assert_eq!(map_bundle(error), PreviewError::Rendering);
        }
    }

    #[test]
    fn evening_preview_uses_the_exact_seventeen_hour_cutoff() {
        let key = key(ReportKind::Evening);
        let preview = render_published_preview(
            &key,
            "Диана",
            utc(17, 13),
            &manifest(ReportKind::Evening, SnapshotStatus::Succeeded),
            facts(0),
        )
        .unwrap();
        assert!(preview.bundle.html.contains("показатели предварительные"));
    }
}
