use super::*;
use crate::reporting::postgres_collector::CollectionClaim;

fn retry_claim(marketplace: Marketplace, source: SnapshotSource) -> SourceJobClaim {
    SourceJobClaim::for_test(
        CollectionClaim::for_test(
            "retry-fixture",
            marketplace,
            Utc::now() + Duration::minutes(1),
        ),
        source,
    )
}

#[test]
fn cluster_server_retries_grow_within_the_inventory_window_and_honor_vendor_delay() {
    let mut claim = retry_claim(Marketplace::Ozon, SnapshotSource::ClusterStocks);
    let mut failure = SourceFailure::from("upstream_server_error");
    for (attempt, seconds) in [5, 10, 20, 40, 80].into_iter().enumerate() {
        claim.consecutive_failures = u32::try_from(attempt).unwrap();
        assert_eq!(source_retry_delay(&claim, failure), seconds);
    }
    claim.consecutive_failures = u32::MAX;
    assert_eq!(source_retry_delay(&claim, failure), 80);
    failure.retry_after = Some(600);
    assert_eq!(source_retry_delay(&claim, failure), 600);
    failure.retry_after = Some(u64::MAX);
    assert_eq!(
        source_retry_delay(&claim, failure),
        i32::MAX.cast_unsigned()
    );
}

#[test]
fn short_cluster_retry_excludes_unknown_throttling_other_sources_and_wb() {
    let mut claim = retry_claim(Marketplace::Ozon, SnapshotSource::ClusterStocks);
    for code in [
        "rate_limited",
        "timeout",
        "network_error",
        "local_overloaded",
    ] {
        assert_eq!(source_retry_delay(&claim, SourceFailure::from(code)), 65);
    }
    for source in [
        SnapshotSource::Stocks,
        SnapshotSource::Sales,
        SnapshotSource::Prices,
        SnapshotSource::Advertising,
        SnapshotSource::Finance,
        SnapshotSource::PostingMovement,
    ] {
        claim.source = source;
        assert_eq!(
            source_retry_delay(&claim, SourceFailure::from("upstream_server_error")),
            65
        );
    }
    let mut wb = retry_claim(Marketplace::Wildberries, SnapshotSource::Stocks);
    wb.consecutive_failures = 4;
    assert_eq!(
        source_retry_delay(&wb, SourceFailure::from("upstream_server_error")),
        900
    );
}

#[test]
fn known_cluster_cooldowns_keep_the_reported_floor_without_an_extra_minute() {
    let claim = retry_claim(Marketplace::Ozon, SnapshotSource::ClusterStocks);
    for (seconds, expected) in [(1, 5), (60, 60), (600, 600)] {
        let failure = SourceFailure {
            code: "rate_limited",
            retry_after: Some(seconds),
        };
        assert_eq!(source_retry_delay(&claim, failure), expected);
        let stocks = retry_claim(Marketplace::Ozon, SnapshotSource::Stocks);
        assert_eq!(source_retry_delay(&stocks, failure), expected.max(65));
    }
}

#[tokio::test]
async fn persisted_cluster_retries_keep_pages_and_cannot_shorten_departure_gates() {
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
    let cutoff = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    fixture
        .writer
        .enqueue_replenishment_jobs(
            std::slice::from_ref(&target),
            cutoff,
            cutoff - Duration::days(1),
            cutoff,
        )
        .await
        .unwrap();
    fixture
        .select(&target.account_id, "cluster_stocks", cutoff)
        .await;
    let claim = fixture
        .writer
        .claim_source_job_with_replenishment(std::slice::from_ref(&target), "retry-test", true)
        .await
        .unwrap()
        .unwrap();
    fixture
        .seed(&claim, json!(["retained-page"]), json!([]))
        .await;
    let before: DateTime<Utc> = fixture
        .admin
        .query_one("SELECT clock_timestamp()", &[])
        .await
        .unwrap()
        .get(0);
    complete_quantum(
        &fixture.writer,
        &claim,
        Err(SourceFailure::from("upstream_server_error")),
    )
    .await
    .unwrap();
    let row = fixture.admin.query_one("SELECT next_attempt_at,first_observed_at,deadline_at,completed_pages FROM daily_reporting.source_collection_jobs WHERE account_id=$1 AND source='cluster_stocks' AND cutoff_at=$2", &[&target.account_id,&cutoff]).await.unwrap();
    let next: DateTime<Utc> = row.get(0);
    let observed: DateTime<Utc> = row.get(1);
    let deadline: DateTime<Utc> = row.get(2);
    assert!(next >= before + Duration::seconds(5) && next < before + Duration::seconds(20));
    assert_eq!(row.get::<_, i32>(3), 1);
    assert_eq!(
        fixture.state(&claim).await,
        ("ready".into(), Some("upstream_server_error".into()), 1)
    );

    fixture
        .select(&target.account_id, "cluster_stocks", cutoff)
        .await;
    let claim = fixture
        .writer
        .claim_source_job_with_replenishment(std::slice::from_ref(&target), "retry-test", true)
        .await
        .unwrap()
        .unwrap();
    fixture.admin.execute("UPDATE daily_reporting.source_collection_departures SET next_allowed_at=clock_timestamp()+interval '120 seconds' WHERE account_id=$1 AND source='cluster_stocks'", &[&target.account_id]).await.unwrap();
    complete_quantum(
        &fixture.writer,
        &claim,
        Err(SourceFailure {
            code: "rate_limited",
            retry_after: Some(70),
        }),
    )
    .await
    .unwrap();
    let row = fixture.admin.query_one("SELECT extract(epoch from next_attempt_at-clock_timestamp())::float8,first_observed_at,deadline_at,completed_pages FROM daily_reporting.source_collection_jobs WHERE account_id=$1 AND source='cluster_stocks' AND cutoff_at=$2", &[&target.account_id,&cutoff]).await.unwrap();
    assert!(row.get::<_, f64>(0) > 100.0);
    assert_eq!(row.get::<_, DateTime<Utc>>(1), observed);
    assert_eq!(row.get::<_, DateTime<Utc>>(2), deadline);
    assert_eq!(row.get::<_, i32>(3), 1);
}
