use chrono::{Duration, Utc};
use mcp_ozon::reporting::{
    checkpoint::{CheckpointError, checkpointed},
    collector_plan::CollectionTarget,
    mcp_read::{ReportingReader, SourceSnapshotQuery},
    ozon_adapter::parse_stock_page,
    postgres_collector::{CollectedFacts, CollectedStockFact, PostgresSnapshotWriter},
    snapshot::{AccountScope, Marketplace, SnapshotSource},
};
use serde_json::json;
use std::{str::FromStr, sync::Arc};
use tokio_postgres::Config;

#[tokio::test]
async fn rfbs_fallback_is_published_and_read_as_a_distinct_fulfillment_dimension() {
    let (Ok(collector_url), Ok(reader_url)) = (
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
        std::env::var("POSITION_REPOSITORY_TEST_READER_URL"),
    ) else {
        return;
    };
    let writer = Arc::new(
        PostgresSnapshotWriter::connect(&Config::from_str(&collector_url).unwrap())
            .await
            .unwrap(),
    );
    writer.verify_source_job_contract().await.unwrap();
    let reader = ReportingReader::connect_optional(Some(&reader_url))
        .await
        .unwrap();
    let account = format!("rfbs_source_{}", std::process::id());
    let target = CollectionTarget {
        account_id: account.clone(),
        marketplace: Marketplace::Ozon,
        // Coverage plans must contain every required source. Enqueue stocks
        // first so this isolated fixture claims its one exercised source.
        sources: vec![
            SnapshotSource::Stocks,
            SnapshotSource::Prices,
            SnapshotSource::Sales,
            SnapshotSource::Advertising,
            SnapshotSource::Finance,
        ],
    };
    let now = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    writer
        .enqueue_source_jobs(
            std::slice::from_ref(&target),
            now - Duration::minutes(1),
            now - Duration::days(1),
            now,
        )
        .await
        .unwrap();
    let claim = writer
        .claim_source_job(std::slice::from_ref(&target), "rfbs-regression")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.source, SnapshotSource::Stocks);
    let journal = writer.source_checkpoints(&claim);
    let facts: Vec<CollectedStockFact> =
        checkpointed(&journal, json!(["rfbs-fixture", 0]), || async {
            parse_stock_page(&json!({"items":[{"product_id":1,"stocks":[
                {"type":"fbo","present":2},
                {"type":"fbs","present":3},
                {"type":"rfbs","present":5}
            ]}],"cursor":""}))
            .map_err(|_| CheckpointError::Invalid)
        })
        .await
        .unwrap();
    let snapshot = writer
        .publish_source_job(
            &claim,
            CollectedFacts::Stocks(facts),
            vec![],
            "rfbs-regression",
        )
        .await
        .unwrap();
    let scope = AccountScope::new(account, Marketplace::Ozon).unwrap();
    let readback = reader
        .source_snapshot(
            &scope,
            SourceSnapshotQuery {
                source: SnapshotSource::Stocks,
                snapshot_id: Some(snapshot),
                limit: 10,
                offset: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(readback.state, "available");
    assert_eq!(readback.total_rows, 3);
    assert_eq!(readback.next_offset, None);
    assert!(readback.source_as_of.is_some() && readback.observed_from.is_some());
    let latest = readback.latest_collection.as_ref().unwrap();
    assert_eq!(latest["status"], "published");
    assert_eq!(latest["completed_pages"], 1);
    let dimensions = readback
        .rows
        .iter()
        .map(|row| {
            (
                row["warehouse_id"].as_str().unwrap(),
                row["sellable_units"].as_u64().unwrap(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        dimensions,
        std::collections::BTreeMap::from([("FBO", 2), ("FBS", 3), ("RFBS", 5),])
    );
}
