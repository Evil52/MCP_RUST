use mcp_ozon::reporting::{
    ReportKey, ReportKind,
    postgres_snapshot::{PublishedPriceFact, PublishedReportFacts, PublishedStockFact},
    preview::render_published_preview,
    report_cutoff, reporting_interval,
    snapshot::{
        AccountScope, FrozenSnapshotManifest, Marketplace, SnapshotDescriptor, SnapshotSource,
        SnapshotStatus,
    },
};
use std::time::Duration;

#[test]
fn generation_after_collection_accepts_snapshots_observed_after_batch_creation() {
    let key = ReportKey {
        local_date: "2026-10-05".parse().unwrap(),
        kind: ReportKind::Morning,
        recipient_id: "review".into(),
        report_version: 1,
    };
    let cutoff = report_cutoff(&key).unwrap();
    let observed = cutoff + Duration::from_mins(5);
    let period = reporting_interval(&key).unwrap();
    let sources = [
        SnapshotSource::Sales,
        SnapshotSource::Advertising,
        SnapshotSource::Stocks,
        SnapshotSource::Prices,
    ];
    let snapshots = sources
        .into_iter()
        .enumerate()
        .map(|(index, source)| {
            let current_state = matches!(source, SnapshotSource::Stocks | SnapshotSource::Prices);
            let (start, end) = if current_state {
                (observed, observed)
            } else {
                period
            };
            SnapshotDescriptor::new(
                i64::try_from(index).unwrap() + 1,
                "review_wb".into(),
                Marketplace::Wildberries,
                source,
                cutoff,
                observed,
                start,
                end,
                u32::from(current_state),
                true,
                SnapshotStatus::Succeeded,
            )
            .unwrap()
        })
        .collect();
    let manifest = FrozenSnapshotManifest::new(
        cutoff,
        vec![AccountScope::new("review_wb".into(), Marketplace::Wildberries).unwrap()],
        snapshots,
    )
    .unwrap();
    let facts = PublishedReportFacts {
        sales: vec![],
        advertising: vec![],
        advertising_expenses: vec![],
        finance: vec![],
        stocks: vec![PublishedStockFact {
            account_id: "review_wb".into(),
            sku: 1,
            warehouse_id: "wb:1".into(),
            sellable_units: 10,
            observed_at: observed,
        }],
        prices: vec![PublishedPriceFact {
            account_id: "review_wb".into(),
            sku: 1,
            price_minor: 10000,
            old_price_minor: None,
            observed_at: observed,
        }],
    };
    // Batch creation is not the report generation clock.
    let early = render_published_preview(&key, "Review", cutoff, &manifest, facts.clone());
    assert!(early.is_err());
    let actual = render_published_preview(
        &key,
        "Review",
        cutoff + Duration::from_mins(30),
        &manifest,
        facts,
    );
    assert!(actual.is_ok());
}
