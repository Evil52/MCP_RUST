use super::*;

#[path = "seller_quotas.rs"]
mod seller_quotas;

const RESUME_SQL: &str = "SELECT daily_reporting.resume_stock_collection($1,'wildberries',$2,$3,$4,'fixed response parser')";

async fn failed_identity(admin: &Client, account: &str) -> (i64, i64, DateTime<Utc>) {
    let row = admin
        .query_one(
            "SELECT id,generation,first_observed_at FROM daily_reporting.source_collection_jobs WHERE account_id=$1 AND status='failed' ORDER BY cutoff_at DESC LIMIT 1",
            &[&account],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1), row.get(2))
}

async fn resume(admin: &Client, account: &str, job: i64, generation: i64, failure: &str) -> bool {
    admin
        .query_one(RESUME_SQL, &[&account, &job, &generation, &failure])
        .await
        .unwrap()
        .get(0)
}

async fn retained_pages(admin: &Client, job: i64) -> i64 {
    admin
        .query_one(
            "SELECT count(*) FROM daily_reporting.source_collection_pages WHERE job_id=$1",
            &[&job],
        )
        .await
        .unwrap()
        .get(0)
}

pub async fn verify(
    admin: &Client,
    collector: &Client,
    reader_db: &Client,
    reader: &ReportingReader,
    writer: &Arc<PostgresSnapshotWriter>,
) {
    let account = format!("stock_recovery_{}", std::process::id());
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
    select_source(admin, &account, "stocks").await;
    let first = claim(writer, &target).await;
    let journal = writer.source_checkpoints(&first);
    let rows = vec![CollectedStockFact {
        sku: 123,
        warehouse_id: "stock-recovery".to_owned(),
        sellable_units: 7,
    }];
    checkpointed(&journal, json!(["stock_recovery", 0]), || async {
        Ok::<_, CheckpointError>(rows.clone())
    })
    .await
    .unwrap();
    // Terminal protocol errors cannot become automatic retries even if a
    // caller accidentally marks the failure as transient.
    writer
        .defer_source_job(&first, Some("invalid_json"), 1, false)
        .await
        .unwrap();
    let (job, generation, observed_from) = failed_identity(admin, &account).await;
    assert!(
        writer
            .claim_source_job(std::slice::from_ref(&target), "source-test")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(retained_pages(admin, job).await, 1);
    assert!(!resume(admin, "other_account", job, generation, "invalid_json").await);
    assert!(!resume(admin, &account, job, generation + 1, "invalid_json").await);
    assert!(!resume(admin, &account, job, generation, "forbidden").await);
    // A stale checkpoint can remain inspectable but cannot be resumed.
    admin.execute(
        "UPDATE daily_reporting.source_collection_jobs SET first_observed_at=clock_timestamp()-interval '31 minutes' WHERE id=$1",
        &[&job],
    ).await.unwrap();
    assert!(!resume(admin, &account, job, generation, "invalid_json").await);
    admin
        .execute(
            "UPDATE daily_reporting.source_collection_jobs SET first_observed_at=$2 WHERE id=$1",
            &[&job, &observed_from],
        )
        .await
        .unwrap();
    for denied in [collector, reader_db] {
        assert!(
            denied
                .query_one(RESUME_SQL, &[&account, &job, &generation, &"invalid_json"])
                .await
                .is_err(),
            "automatic collectors and readers cannot explicitly recover jobs"
        );
    }
    assert!(resume(admin, &account, job, generation, "invalid_json").await);
    assert!(!resume(admin, &account, job, generation, "invalid_json").await);
    select_source(admin, &account, "stocks").await;
    let resumed = claim(writer, &target).await;
    let identity = admin.query_one(
        "SELECT generation,first_observed_at FROM daily_reporting.source_collection_jobs WHERE id=$1",
        &[&job],
    ).await.unwrap();
    assert_eq!(identity.get::<_, i64>(0), generation + 1);
    assert_eq!(identity.get::<_, DateTime<Utc>>(1), observed_from);
    assert!(journal.unwrap().load(&"a".repeat(64)).await.is_err());
    let restored: Vec<CollectedStockFact> = checkpointed(
        &writer.source_checkpoints(&resumed),
        json!(["stock_recovery", 0]),
        || async {
            panic!("explicit stock recovery must replay saved page");
            #[allow(unreachable_code)]
            Ok::<_, CheckpointError>(vec![])
        },
    )
    .await
    .unwrap();
    assert_eq!(restored, rows);
    let published = writer
        .publish_source_job(&resumed, CollectedFacts::Stocks(restored), vec![], "test")
        .await
        .unwrap();
    assert_eq!(retained_pages(admin, job).await, 0);

    // Repeated failure has a bounded explicit recovery budget. The already
    // published inventory remains readable throughout the failed replacement.
    writer
        .enqueue_source_jobs(
            std::slice::from_ref(&target),
            cutoff + Duration::seconds(1),
            start,
            end,
        )
        .await
        .unwrap();
    select_source(admin, &account, "stocks").await;
    let replacement = claim(writer, &target).await;
    checkpointed(
        &writer.source_checkpoints(&replacement),
        json!(["stock_recovery", 0]),
        || async { Ok::<_, CheckpointError>(rows) },
    )
    .await
    .unwrap();
    writer
        .defer_source_job(&replacement, Some("unauthorized"), 1, false)
        .await
        .unwrap();
    for attempt in 0..3 {
        let (job, generation, _) = failed_identity(admin, &account).await;
        assert!(resume(admin, &account, job, generation, "unauthorized").await);
        select_source(admin, &account, "stocks").await;
        let retry = claim(writer, &target).await;
        writer
            .defer_source_job(&retry, Some("unauthorized"), 1, false)
            .await
            .unwrap();
        assert_eq!(retained_pages(admin, job).await, 1);
        assert_eq!(
            admin
                .query_one(
                    "SELECT count(*) FROM daily_reporting.stock_collection_resumes WHERE job_id=$1",
                    &[&job],
                )
                .await
                .unwrap()
                .get::<_, i64>(0),
            attempt + 1
        );
    }
    let (job, generation, _) = failed_identity(admin, &account).await;
    assert!(!resume(admin, &account, job, generation, "unauthorized").await);
    let available = reader
        .source_snapshot(&scope, query(SnapshotSource::Stocks))
        .await
        .unwrap();
    assert_eq!(available.snapshot_id, Some(published.to_string()));
    assert_eq!(available.latest_collection.unwrap()["status"], "failed");
    admin.execute(
        "UPDATE daily_reporting.source_collection_jobs SET finished_at=clock_timestamp()-interval '25 hours' WHERE id=$1",
        &[&job],
    ).await.unwrap();
    assert!(
        writer
            .claim_source_job(std::slice::from_ref(&target), "source-test")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(retained_pages(admin, job).await, 0);
    assert_eq!(
        admin
            .query_one(
                "SELECT cache_bytes FROM daily_reporting.source_collection_jobs WHERE id=$1",
                &[&job],
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    seller_quotas::verify(admin, writer).await;
}
