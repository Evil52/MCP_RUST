use super::*;
use mcp_ozon::reporting::postgres_collector::CollectedSellerStockFact;

pub async fn verify(
    admin: &Client,
    collector: &Client,
    reader: &ReportingReader,
    writer: &Arc<PostgresSnapshotWriter>,
) {
    verify_large_size_snapshot(admin, reader, writer).await;
    let account = format!("seller_publication_{}", std::process::id());
    let target = CollectionTarget {
        account_id: account.clone(),
        marketplace: Marketplace::Wildberries,
        sources: vec![
            SnapshotSource::Sales,
            SnapshotSource::Advertising,
            SnapshotSource::Stocks,
            SnapshotSource::Prices,
        ],
    };
    let scope = AccountScope::new(account.clone(), Marketplace::Wildberries).unwrap();
    let (cutoff, start, end) = collection_window(Utc::now());
    writer
        .enqueue_source_jobs(std::slice::from_ref(&target), cutoff, start, end)
        .await
        .unwrap();
    select_source(admin, &account, "seller_stocks").await;
    let first = claim(writer, &target).await;
    assert_eq!(first.source, SnapshotSource::SellerStocks);
    let rows = vec![
        CollectedSellerStockFact {
            sku: 123,
            chrt_id: 1001,
            warehouse_id: 2,
            delivery_type: 1,
            sellable_units: Some(0),
        },
        CollectedSellerStockFact {
            sku: 124,
            chrt_id: 1002,
            warehouse_id: 2,
            delivery_type: 1,
            sellable_units: None,
        },
    ];
    checkpointed(
        &writer.source_checkpoints(&first),
        json!(["fbs_publication", 0]),
        || async { Ok::<_, CheckpointError>(rows.clone()) },
    )
    .await
    .unwrap();
    let partial = writer
        .publish_source_job(
            &first,
            CollectedFacts::SellerStocks(rows.clone()),
            vec![],
            "test",
        )
        .await
        .unwrap();
    let status = admin.query_one(
        "SELECT s.status,s.pagination_complete,j.status,j.error_class,j.cache_bytes FROM daily_reporting.source_snapshots s JOIN daily_reporting.source_collection_jobs j ON j.id=s.source_job_id WHERE s.id=$1",
        &[&partial],
    ).await.unwrap();
    assert_eq!(status.get::<_, String>(0), "partial");
    assert!(status.get::<_, bool>(1));
    assert_eq!(status.get::<_, String>(2), "published");
    assert_eq!(status.get::<_, String>(3), "seller_missing_values");
    assert_eq!(status.get::<_, i64>(4), 0);
    let data = reader
        .source_snapshot(&scope, query(SnapshotSource::SellerStocks))
        .await
        .unwrap();
    assert_eq!(data.state, "partial");
    assert_eq!(data.total_rows, 2);
    assert_eq!(data.next_offset, Some(1));
    assert_eq!(data.snapshot_id, Some(partial.to_string()));
    assert_eq!(data.rows[0]["sellable_units"], 0);
    assert_eq!(data.rows[0]["delivery_type"], 1);
    assert_eq!(data.inventory_scope.as_deref(), Some("seller"));
    let next = reader
        .source_snapshot(
            &scope,
            SourceSnapshotQuery {
                snapshot_id: Some(partial),
                offset: 1,
                ..query(SnapshotSource::SellerStocks)
            },
        )
        .await
        .unwrap();
    assert!(next.rows[0]["sellable_units"].is_null());
    assert_eq!(next.next_offset, None);
    assert!(
        collector
            .execute(
                "UPDATE daily_reporting.seller_stock_facts SET sellable_units=0 WHERE snapshot_id=$1",
                &[&partial],
            )
            .await
            .is_err(),
        "published unknown values remain immutable"
    );
    // FBS does not silently supply FBW inventory or complete the standard
    // report's required-source manifest.
    assert_eq!(
        reader
            .source_snapshot(&scope, query(SnapshotSource::Stocks))
            .await
            .unwrap()
            .state,
        "missing"
    );
    assert_ne!(
        reader
            .data_completeness(&scope, Some(cutoff))
            .await
            .unwrap()
            .state,
        DataState::Complete
    );

    writer
        .enqueue_source_jobs(
            std::slice::from_ref(&target),
            cutoff + Duration::seconds(1),
            start,
            end,
        )
        .await
        .unwrap();
    select_source(admin, &account, "seller_stocks").await;
    let stale = claim(writer, &target).await;
    checkpointed(
        &writer.source_checkpoints(&stale),
        json!(["fbs_publication", 0]),
        || async { Ok::<_, CheckpointError>(rows.clone()) },
    )
    .await
    .unwrap();
    admin.execute(
        "UPDATE daily_reporting.source_collection_jobs SET first_observed_at=clock_timestamp()-interval '31 minutes' WHERE account_id=$1 AND status='running'",
        &[&account],
    ).await.unwrap();
    assert!(
        writer
            .publish_source_job(&stale, CollectedFacts::SellerStocks(rows), vec![], "test")
            .await
            .is_err(),
        "FBS publication cannot relabel expired checkpoint observations"
    );
    assert!(
        writer
            .claim_source_job(std::slice::from_ref(&target), "source-test")
            .await
            .unwrap()
            .is_none()
    );
    let expired = admin.query_one(
        "SELECT j.status,j.error_class,count(p.*) FROM daily_reporting.source_collection_jobs j LEFT JOIN daily_reporting.source_collection_pages p ON p.job_id=j.id WHERE j.account_id=$1 AND j.status='failed' GROUP BY j.id",
        &[&account],
    ).await.unwrap();
    assert_eq!(expired.get::<_, String>(0), "failed");
    assert_eq!(expired.get::<_, String>(1), "collection_expired");
    assert_eq!(expired.get::<_, i64>(2), 1);
    assert_eq!(
        reader
            .source_snapshot(&scope, query(SnapshotSource::SellerStocks))
            .await
            .unwrap()
            .snapshot_id,
        Some(partial.to_string())
    );
}

async fn verify_large_size_snapshot(
    admin: &Client,
    reader: &ReportingReader,
    writer: &Arc<PostgresSnapshotWriter>,
) {
    let account = format!("seller_size_rows_{}", std::process::id());
    let target = CollectionTarget {
        account_id: account.clone(),
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
        .enqueue_source_jobs(std::slice::from_ref(&target), cutoff, start, end)
        .await
        .unwrap();
    select_source(admin, &account, "seller_stocks").await;
    let claim = claim(writer, &target).await;
    checkpointed(
        &writer.source_checkpoints(&claim),
        json!(["size_rows_fixture"]),
        || async { Ok::<_, CheckpointError>(true) },
    )
    .await
    .unwrap();
    // 12,501 sizes across two delivery models: only two aggregate SKU facts,
    // but more than one INSERT chunk of size-level rows.
    let facts = (1..=12_501)
        .flat_map(|chrt_id| {
            [1, 2].map(|warehouse_id| CollectedSellerStockFact {
                sku: 123,
                chrt_id,
                warehouse_id,
                delivery_type: warehouse_id,
                sellable_units: Some(0),
            })
        })
        .collect();
    let snapshot = writer
        .publish_source_job(&claim, CollectedFacts::SellerStocks(facts), vec![], "test")
        .await
        .unwrap();
    let scope = AccountScope::new(account, Marketplace::Wildberries).unwrap();
    let result = reader
        .source_snapshot(&scope, query(SnapshotSource::SellerStocks))
        .await
        .unwrap();
    assert_eq!(result.snapshot_id, Some(snapshot.to_string()));
    assert_eq!(result.total_rows, 25_002);
    assert_eq!(result.state, "available");
    assert_eq!(result.coverage.unwrap().known_pairs, 25_002);
    let persisted: i64 = admin
        .query_one(
            "SELECT count(*) FROM daily_reporting.seller_stock_facts WHERE snapshot_id=$1",
            &[&snapshot],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(persisted, 25_002);
}
