//! Whole-inventory aggregation, overlap rejection and durable stock replay.
use super::*;
use crate::reporting::checkpoint::tests::{MemoryPages, journal};

fn size_page(first_id: u64, count: u32, quantity: u64) -> Value {
    json!({"data":{"items":(first_id..first_id + u64::from(count)).map(|id| json!({
        "nmId":7,"chrtId":id,"warehouseId":2,"quantity":quantity
    })).collect::<Vec<_>>()}})
}

fn fixture(responses: Vec<Value>) -> FixtureTransport {
    let fixture = FixtureTransport::complete();
    *fixture.stocks.lock().unwrap() = responses.into();
    fixture
}

#[tokio::test]
async fn sizes_split_across_stock_pages_publish_one_summed_sku_fact() {
    let fixture = fixture(vec![size_page(1, 1_000, 1), size_page(1_001, 1, 2)]);
    let facts = WbReportSource::new(fixture.clone())
        .collect_stock_pages()
        .await
        .unwrap();
    assert_eq!(
        facts,
        vec![CollectedStockFact {
            sku: 7,
            warehouse_id: "wb:2".to_owned(),
            sellable_units: 1_002,
        }]
    );
    assert_eq!(
        *fixture.requested_stocks.lock().unwrap(),
        vec![(1_000, 0), (1_000, 1_000)]
    );
    let at = Utc.with_ymd_and_hms(2026, 9, 14, 3, 0, 0).unwrap();
    assert!(
        CollectedSnapshot::new(
            "wb_account".to_owned(),
            Marketplace::Wildberries,
            at,
            at,
            at,
            at,
            SnapshotStatus::Succeeded,
            true,
            "test".to_owned(),
            CollectedFacts::Stocks(facts),
        )
        .is_ok()
    );
}

#[tokio::test]
async fn stock_cross_page_sum_rejects_overflow() {
    let mut first = size_page(1, 1_000, 0);
    first["data"]["items"][0]["quantity"] = json!(u64::MAX);
    let source = WbReportSource::new(fixture(vec![first, size_page(1_001, 1, 1)]));
    assert_eq!(
        source.collect_stock_pages().await,
        Err(WbReportSourceError::InvalidStockResponse)
    );
}

#[tokio::test]
async fn stock_resume_rebuilds_totals_without_refetching_or_reusing_v1_pages() {
    let pages = MemoryPages::default();
    checkpointed(&journal(&pages), json!(["wb_stock", 0]), || async {
        parse_stock_page(&size_page(50_000, 1, 999))
            .map_err(|_| WbReportSourceError::InvalidStockResponse)
    })
    .await
    .unwrap();
    let fixture = fixture(vec![size_page(1, 1_000, 1), size_page(1_001, 1, 2)]);
    let source = WbReportSource::new(fixture.clone()).with_checkpoints(journal(&pages));
    assert_eq!(
        source.collect_stock_pages().await,
        Err(WbReportSourceError::Checkpoint(CheckpointError::Deferred))
    );
    assert_eq!(fixture.requested_stocks.lock().unwrap().len(), 1);
    let source = WbReportSource::new(fixture.clone()).with_checkpoints(journal(&pages));
    let facts = source.collect_stock_pages().await.unwrap();
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].sellable_units, 1_002);
    assert_eq!(pages.lock().unwrap().len(), 3);
    let source = WbReportSource::new(fixture.clone()).with_checkpoints(journal(&pages));
    assert_eq!(source.collect_stock_pages().await.unwrap(), facts);
    assert_eq!(
        *fixture.requested_stocks.lock().unwrap(),
        vec![(1_000, 0), (1_000, 1_000)]
    );
}

#[tokio::test]
async fn repeated_or_overlapping_stock_pages_fail_including_after_replay() {
    for second in [size_page(1, 1_000, 1), size_page(1_000, 1, 2)] {
        let pages = MemoryPages::default();
        let fixture = fixture(vec![size_page(1, 1_000, 1), second]);
        let source = WbReportSource::new(fixture.clone()).with_checkpoints(journal(&pages));
        assert_eq!(
            source.collect_stock_pages().await,
            Err(WbReportSourceError::Checkpoint(CheckpointError::Deferred))
        );
        let source = WbReportSource::new(fixture.clone()).with_checkpoints(journal(&pages));
        assert_eq!(
            source.collect_stock_pages().await,
            Err(WbReportSourceError::InvalidStockResponse)
        );
        assert_eq!(fixture.requested_stocks.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn stock_size_identity_keeps_distinct_warehouses_separate() {
    let first = size_page(1, 1_000, 1);
    let mut second = size_page(1, 1, 2);
    second["data"]["items"][0]["warehouseId"] = json!(3);
    let facts = WbReportSource::new(fixture(vec![first, second]))
        .collect_stock_pages()
        .await
        .unwrap();
    assert_eq!(facts.len(), 2);
    assert_eq!(facts[0].sellable_units, 1_000);
    assert_eq!(facts[1].warehouse_id, "wb:3");
    assert_eq!(facts[1].sellable_units, 2);
    let page = json!({"data":{"items":[
        {"nmId":7,"chrtId":1,"warehouseId":-999_999,"regionName":"A","warehouseName":"X","quantity":2},
        {"nmId":7,"chrtId":1,"warehouseId":-999_999,"regionName":"A","warehouseName":"Y","quantity":3}
    ]}});
    let facts = WbReportSource::new(fixture(vec![page]))
        .collect_stock_pages()
        .await
        .unwrap();
    assert_eq!(facts.len(), 2);
    assert_eq!(facts.iter().map(|row| row.sellable_units).sum::<u64>(), 5);
}

#[tokio::test]
async fn malformed_size_ids_duplicates_and_oversized_stock_pages_are_not_cached() {
    let mut duplicate = size_page(1, 2, 1);
    duplicate["data"]["items"][1]["chrtId"] = json!(1);
    let mut malformed = size_page(1, 1, 1);
    malformed["data"]["items"][0]["chrtId"] = json!(0);
    for page in [duplicate, malformed, size_page(1, 1_001, 1)] {
        let pages = MemoryPages::default();
        let source = WbReportSource::new(fixture(vec![page])).with_checkpoints(journal(&pages));
        assert_eq!(
            source.collect_stock_pages().await,
            Err(WbReportSourceError::InvalidStockResponse)
        );
        assert!(pages.lock().unwrap().is_empty());
    }
}
