use super::*;
use mcp_ozon::{
    ozon_posting_sales::PostingSalesRow, reporting::postgres_collector::CollectedClusterStockFact,
};

pub async fn verify(
    admin: &Client,
    collector: &Client,
    reader: &ReportingReader,
    writer: &Arc<PostgresSnapshotWriter>,
) {
    let account = format!("replenishment_publication_{}", std::process::id());
    let target = CollectionTarget {
        account_id: account.clone(),
        marketplace: Marketplace::Ozon,
        sources: vec![
            SnapshotSource::Sales,
            SnapshotSource::Advertising,
            SnapshotSource::Finance,
            SnapshotSource::Stocks,
            SnapshotSource::Prices,
        ],
    };
    let wb = CollectionTarget {
        account_id: format!("wb_replenishment_{}", std::process::id()),
        marketplace: Marketplace::Wildberries,
        sources: vec![
            SnapshotSource::Sales,
            SnapshotSource::Advertising,
            SnapshotSource::Stocks,
            SnapshotSource::Prices,
        ],
    };
    let (cutoff, start, end) = collection_window(Utc::now());
    writer
        .enqueue_replenishment_jobs(&[target.clone(), wb.clone()], cutoff, start, end)
        .await
        .unwrap();
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM daily_reporting.source_collection_jobs WHERE account_id=$1",
                &[&wb.account_id]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    let scope = AccountScope::new(account.clone(), Marketplace::Ozon).unwrap();
    assert!(
        writer
            .claim_source_job(std::slice::from_ref(&target), "source-test")
            .await
            .unwrap()
            .is_none(),
        "disabled collection must not claim optional jobs"
    );
    select_source(admin, &account, "cluster_stocks").await;
    let c = writer
        .claim_source_job_with_replenishment(std::slice::from_ref(&target), "source-test", true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(c.source, SnapshotSource::ClusterStocks);
    let rows = vec![
        CollectedClusterStockFact {
            sku: 11,
            warehouse_id: Some(-1_051_784),
            cluster_id: Some(154),
            macrolocal_cluster_id: Some(4039),
            data: json!({"source_row":true,"data_classification":"untrusted_external_marketplace_data","available_stock_count":5,"ads_cluster":0.5}),
        },
        CollectedClusterStockFact {
            sku: 11,
            warehouse_id: Some(-1_051_784),
            cluster_id: Some(2),
            macrolocal_cluster_id: Some(4007),
            data: json!({"source_row":true,"data_classification":"untrusted_external_marketplace_data","available_stock_count":2,"ads_cluster":0.25}),
        },
        CollectedClusterStockFact {
            sku: 12,
            warehouse_id: None,
            cluster_id: None,
            macrolocal_cluster_id: None,
            data: json!({"source_row":false,"data_classification":"untrusted_external_marketplace_data"}),
        },
    ];
    checkpointed(
        &writer.source_checkpoints(&c),
        json!(["cluster", 0]),
        || async { Ok::<_, CheckpointError>(rows.clone()) },
    )
    .await
    .unwrap();
    let id = writer
        .publish_source_job(&c, CollectedFacts::ClusterStocks(rows), vec![], "test")
        .await
        .unwrap();
    let data = reader
        .source_snapshot(&scope, query(SnapshotSource::ClusterStocks))
        .await
        .unwrap();
    assert_eq!(data.state, "partial");
    assert_eq!(data.total_rows, 3);
    assert_eq!(data.rows[0]["warehouse_id"], -1_051_784);
    assert_eq!(data.rows[0]["cluster_id"], 2);
    assert_eq!(data.rows[0]["data"]["available_stock_count"], 2);
    let second = reader
        .source_snapshot(
            &scope,
            SourceSnapshotQuery {
                snapshot_id: Some(id),
                offset: 1,
                ..query(SnapshotSource::ClusterStocks)
            },
        )
        .await
        .unwrap();
    assert_eq!(second.rows[0]["warehouse_id"], -1_051_784);
    assert_eq!(second.rows[0]["cluster_id"], 154);
    assert_eq!(second.rows[0]["data"]["available_stock_count"], 5);
    assert_eq!(data.coverage.unwrap().missing_pairs, 1);
    assert_eq!(
        data.catalog_scope.as_deref(),
        Some("listed_fulfillment_skus")
    );
    let next = reader
        .source_snapshot(
            &scope,
            SourceSnapshotQuery {
                snapshot_id: Some(id),
                offset: 2,
                ..query(SnapshotSource::ClusterStocks)
            },
        )
        .await
        .unwrap();
    assert!(next.rows[0]["warehouse_id"].is_null());
    assert!(next.rows[0]["data"].get("available_stock_count").is_none());
    assert_eq!(admin.query_one("SELECT status,error_class FROM daily_reporting.source_collection_jobs WHERE account_id=$1 AND source='cluster_stocks'",&[&account]).await.unwrap().get::<_,String>(1),"cluster_missing_values");
    assert!(
        collector
            .execute(
                "DELETE FROM daily_reporting.cluster_stock_facts WHERE snapshot_id=$1",
                &[&id]
            )
            .await
            .is_err()
    );
    select_source(admin, &account, "posting_movement").await;
    let c = writer
        .claim_source_job_with_replenishment(std::slice::from_ref(&target), "source-test", true)
        .await
        .unwrap()
        .unwrap();
    let row = PostingSalesRow {
        sku: 11,
        fbo_non_cancelled_units: 3,
        fbs_non_cancelled_units: 1,
        total_non_cancelled_units: 4,
        fbo_cancelled_units: 2,
        fbs_cancelled_units: 0,
        total_cancelled_units: 2,
        fbo_status_units: [("delivered".to_owned(), 3), ("cancelled".to_owned(), 2)].into(),
        fbs_status_units: [("awaiting_packaging".to_owned(), 1)].into(),
    };
    checkpointed(
        &writer.source_checkpoints(&c),
        json!(["postings", 0]),
        || async { Ok::<_, CheckpointError>(row.clone()) },
    )
    .await
    .unwrap();
    writer
        .publish_source_job(
            &c,
            CollectedFacts::PostingMovement(vec![row]),
            vec![],
            "test",
        )
        .await
        .unwrap();
    let data = reader
        .source_snapshot(&scope, query(SnapshotSource::PostingMovement))
        .await
        .unwrap();
    assert_eq!(data.state, "available");
    assert_eq!(
        data.period_start.unwrap().parse::<DateTime<Utc>>().unwrap(),
        start
    );
    assert_eq!(data.rows[0]["data"]["total_cancelled_units"], 2);
    let foreign = AccountScope::new(wb.account_id, Marketplace::Wildberries).unwrap();
    assert_eq!(
        reader
            .source_snapshot(&foreign, query(SnapshotSource::ClusterStocks))
            .await
            .err(),
        Some(ReportingReadError::InvalidRequest)
    );
    // Optional sources have no effect on the required daily manifest.
    writer
        .enqueue_source_jobs(std::slice::from_ref(&target), cutoff, start, end)
        .await
        .unwrap();
    for source in &target.sources {
        let name = match source {
            SnapshotSource::Sales => "sales",
            SnapshotSource::Advertising => "advertising",
            SnapshotSource::Finance => "finance",
            SnapshotSource::Stocks => "stocks",
            SnapshotSource::Prices => "prices",
            _ => unreachable!(),
        };
        select_source(admin, &account, name).await;
        let c = writer
            .claim_source_job_with_replenishment(std::slice::from_ref(&target), "source-test", true)
            .await
            .unwrap()
            .unwrap();
        checkpointed(&writer.source_checkpoints(&c), json!([name, 0]), || async {
            Ok::<_, CheckpointError>(vec![1])
        })
        .await
        .unwrap();
        let facts = match source {
            SnapshotSource::Sales => CollectedFacts::Sales(vec![]),
            SnapshotSource::Advertising => CollectedFacts::Advertising(vec![]),
            SnapshotSource::Finance => CollectedFacts::Finance(vec![]),
            SnapshotSource::Stocks => CollectedFacts::Stocks(vec![]),
            SnapshotSource::Prices => CollectedFacts::Prices(vec![]),
            _ => unreachable!(),
        };
        writer
            .publish_source_job(&c, facts, vec![], "test")
            .await
            .unwrap();
    }
    let sources = reader
        .data_completeness(&scope, Some(cutoff))
        .await
        .unwrap();
    assert_eq!(sources.sources.len(), 5);
    assert_eq!(sources.state, DataState::Complete);
}
