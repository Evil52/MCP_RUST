use super::*;

#[tokio::test]
async fn reconciliation_certificate_is_atomic_immutable_and_survives_page_cleanup() {
    let (Ok(admin_url), Ok(collector_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };
    let admin = connect(&admin_url).await;
    let writer = Arc::new(
        PostgresSnapshotWriter::connect(&Config::from_str(&collector_url).unwrap())
            .await
            .unwrap(),
    );
    let account = "wb_reconciliation_certificate";
    let target = CollectionTarget {
        account_id: account.to_owned(),
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
    select_source(&admin, account, "sales").await;
    let c = claim(&writer, &target).await;
    let journal = writer.source_checkpoints(&c);
    let evidence = json!({"kind":"wb_sales_whole_ruble_v1","business_date":business_date(start),
        "ordered_units":1,"sku_gmv_minor":10000,"group_gmv_minor":10100,"verified_skus":1,
        "history_matches":true,"group_stable":true});
    checkpointed(
        &journal,
        json!(["reconciliation-final-control"]),
        || async { Ok::<_, CheckpointError>(evidence.clone()) },
    )
    .await
    .unwrap();
    let mut row = CollectedSalesFact {
        business_date: business_date(start),
        sku: 5,
        ordered_units: 1,
        operational_gmv_minor: 9900,
        cancelled_units: None,
        returned_units: None,
    };
    assert!(
        writer
            .publish_source_job(
                &c,
                CollectedFacts::Sales(vec![row.clone()]),
                vec![],
                "reconciliation-test"
            )
            .await
            .is_err(),
        "a certificate for different sums must roll back publication"
    );
    row.operational_gmv_minor = 10000;
    let id = writer
        .publish_source_job(
            &c,
            CollectedFacts::Sales(vec![row]),
            vec![],
            "reconciliation-test",
        )
        .await
        .unwrap();
    let stored:String=admin.query_one("SELECT evidence::text FROM daily_reporting.snapshot_reconciliation_evidence WHERE snapshot_id=$1",&[&id]).await.unwrap().get(0);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&stored).unwrap(),
        evidence
    );
    let pages:i64=admin.query_one("SELECT count(*) FROM daily_reporting.source_collection_pages p JOIN daily_reporting.source_collection_jobs j ON j.id=p.job_id WHERE j.account_id=$1",&[&account]).await.unwrap().get(0);
    assert_eq!(pages, 0);
    assert!(admin.execute("UPDATE daily_reporting.snapshot_reconciliation_evidence SET evidence='{}' WHERE snapshot_id=$1",&[&id]).await.is_err());
    assert!(
        admin
            .execute(
                "DELETE FROM daily_reporting.snapshot_reconciliation_evidence WHERE snapshot_id=$1",
                &[&id]
            )
            .await
            .is_err()
    );
}
