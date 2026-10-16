use super::*;

#[tokio::test]
async fn started_observation_deadlines_precede_source_preference_without_bypassing_backoff() {
    let (Ok(admin_url), Ok(collector_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };
    let _database = DATABASE.lock().await;
    let mut fixture = Fixture::new(&admin_url, &collector_url).await;
    fixture.config =
        Fixture::config_with_replenishment(&fixture.root, &collector_url, "scheduled", true);
    let target = fixture.config.collection_plan()[0].clone();
    assert_eq!(target.marketplace, Marketplace::Ozon);
    let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    let old_cutoff = now - Duration::hours(1);
    let new_cutoff = now - Duration::minutes(1);
    fixture
        .writer
        .enqueue_replenishment_jobs(
            std::slice::from_ref(&target),
            old_cutoff,
            old_cutoff - Duration::days(1),
            old_cutoff,
        )
        .await
        .unwrap();
    fixture
        .writer
        .enqueue_source_jobs(
            std::slice::from_ref(&target),
            new_cutoff,
            new_cutoff - Duration::days(1),
            new_cutoff,
        )
        .await
        .unwrap();
    for (cluster_age, stock_age, cluster_due, expected) in [
        (Some(20), None, true, SnapshotSource::ClusterStocks),
        (Some(20), Some(10), true, SnapshotSource::ClusterStocks),
        (Some(20), Some(25), true, SnapshotSource::Stocks),
        (None, None, true, SnapshotSource::Stocks),
        (Some(20), None, false, SnapshotSource::Stocks),
    ] {
        let cluster_first = cluster_age.map(|minutes| now - Duration::minutes(minutes));
        let stock_first = stock_age.map(|minutes| now - Duration::minutes(minutes));
        fixture
            .admin
            .execute(
                "UPDATE daily_reporting.source_collection_jobs SET
              first_observed_at=CASE source WHEN 'cluster_stocks' THEN $2::timestamptz
                WHEN 'stocks' THEN $3::timestamptz ELSE NULL END,
              next_attempt_at=CASE WHEN source='stocks' OR (source='cluster_stocks' AND $4)
                THEN clock_timestamp()-interval '1 second'
                ELSE clock_timestamp()+interval '1 hour' END
             WHERE account_id=$1 AND status='ready'",
                &[
                    &target.account_id,
                    &cluster_first,
                    &stock_first,
                    &cluster_due,
                ],
            )
            .await
            .unwrap();
        let claim = fixture
            .writer
            .claim_source_job_with_replenishment(
                std::slice::from_ref(&target),
                "deadline-priority-test",
                true,
            )
            .await
            .unwrap()
            .unwrap();
        fixture
            .writer
            .defer_source_job(&claim, None, 3600, false)
            .await
            .unwrap();
        assert_eq!(
            claim.source, expected,
            "cluster age={cluster_age:?}, stock age={stock_age:?}, cluster due={cluster_due}"
        );
    }
}
