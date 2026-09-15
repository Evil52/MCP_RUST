use super::*;
use crate::reporting::postgres_collector::CollectedSalesFact;

#[tokio::test]
async fn production_audit_sales_requires_observed_pages_and_recovers() {
    use crate::reporting::postgres_collector::PostgresCollectorError;

    let (Ok(admin_url), Ok(collector_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };
    let _database = DATABASE.lock().await;
    let fixture = Fixture::new(&admin_url, &collector_url).await;
    let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    fixture
        .writer
        .enqueue_source_jobs(
            fixture.config.collection_plan(),
            now,
            now - Duration::days(1),
            now,
        )
        .await
        .unwrap();

    for target in fixture.config.collection_plan() {
        fixture.select(&target.account_id, "sales", now).await;
        let claim = fixture.claim().await;
        // A claimed job is not observation evidence. Neither an empty input
        // nor an admitted-but-unfinished page may publish an empty snapshot.
        for admitted in [false, true] {
            if admitted {
                fixture
                    .writer
                    .source_checkpoints(&claim)
                    .unwrap()
                    .admit()
                    .await
                    .unwrap();
            }
            assert_eq!(
                fixture
                    .writer
                    .publish_source_job(
                        &claim,
                        CollectedFacts::Sales(vec![]),
                        vec![],
                        "observation-regression"
                    )
                    .await,
                Err(PostgresCollectorError::InvalidInput)
            );
            let row = fixture.admin.query_one(
                "SELECT first_observed_at IS NOT NULL, last_observed_at IS NOT NULL,
                 (SELECT count(*) FROM daily_reporting.source_snapshots WHERE account_id=$1 AND source='sales' AND cutoff_at=$2)
                 FROM daily_reporting.source_collection_jobs WHERE account_id=$1 AND source='sales' AND cutoff_at=$2",
                &[&target.account_id, &now]).await.unwrap();
            assert_eq!(row.get::<_, bool>(0), admitted);
            assert!(!row.get::<_, bool>(1));
            assert_eq!(row.get::<_, i64>(2), 0);
            assert_eq!(fixture.state(&claim).await, ("running".to_owned(), None, 0));
        }
        // Recording a genuine terminal empty page makes this same live
        // lease publishable; a failed validation must not poison its session.
        let (identity, page) = empty_pages(&claim).remove(0);
        fixture.seed(&claim, identity, page).await;
        let snapshot = fixture
            .writer
            .publish_source_job(
                &claim,
                CollectedFacts::Sales(vec![]),
                vec![],
                "observation-regression",
            )
            .await
            .unwrap();
        assert!(snapshot > 0);
        assert_eq!(
            fixture.state(&claim).await,
            ("published".to_owned(), None, 0)
        );
    }
}

#[tokio::test]
async fn sales_publication_replayed_overlap_restarts_without_publishing_or_mixing_pages() {
    let (Ok(admin_url), Ok(collector_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };
    let _database = DATABASE.lock().await;
    let fixture = Fixture::new(&admin_url, &collector_url).await;
    let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    let start = now - Duration::days(1);
    fixture
        .writer
        .enqueue_source_jobs(fixture.config.collection_plan(), now, start, now)
        .await
        .unwrap();

    for target in fixture.config.collection_plan() {
        fixture.select(&target.account_id, "sales", now).await;
        let claim = fixture.claim().await;
        let from = business_date(claim.period_start);
        let to = business_date(claim.period_end - Duration::microseconds(1));
        let page_size = if target.marketplace == Marketplace::Ozon {
            1_000_u32
        } else {
            250
        };
        let rows: Vec<_> = (1..=page_size)
            .map(|sku| CollectedSalesFact {
                business_date: from,
                sku: u64::from(sku),
                ordered_units: 1,
                operational_gmv_minor: 100,
                cancelled_units: None,
                returned_units: None,
            })
            .collect();
        // Each page is valid on its own. A product moved across the offset
        // boundary between observations; the final page repeats its identity.
        let mut overlap = rows[0].clone();
        overlap.ordered_units = 2;
        for (offset, page) in [(0, rows), (page_size, vec![overlap])] {
            let (identity, payload) = if target.marketplace == Marketplace::Ozon {
                (
                    request_key(&sales_request(from, to, offset).unwrap()),
                    json!(page),
                )
            } else {
                (
                    json!(["wb_sales_v2", from, page_size, offset]),
                    json!([page, page.len()]),
                )
            };
            fixture.seed(&claim, identity, payload).await;
        }
        fixture
            .writer
            .defer_source_job(&claim, None, 1, false)
            .await
            .unwrap();
        fixture.select(&target.account_id, "sales", now).await;
        // Replay is entirely local; a missing checkpoint cannot cause vendor I/O.
        fixture.forbid_departures(&target.account_id).await;
        assert!(
            run_quantum(&fixture.config, &fixture.writer, "sales-overlap-test")
                .await
                .unwrap()
        );
        assert_eq!(
            fixture.state(&claim).await,
            ("ready".to_owned(), Some("sales_page_overlap".to_owned()), 0)
        );
        let published: i64 = fixture.admin.query_one(
            "SELECT count(*) FROM daily_reporting.source_snapshots WHERE account_id=$1 AND source='sales' AND cutoff_at=$2",
            &[&target.account_id, &now]).await.unwrap().get(0);
        assert_eq!(
            published, 0,
            "overlap must never be silently deduplicated or published"
        );
        let reset = fixture.admin.query_one(
            "SELECT completed_pages, page_restarts, first_observed_at IS NULL AND last_observed_at IS NULL,
             next_attempt_at >= (SELECT next_allowed_at FROM daily_reporting.source_collection_departures d
                 WHERE d.account_id=j.account_id AND d.marketplace=j.marketplace
                   AND d.source=CASE WHEN j.marketplace='wildberries' THEN 'analytics' ELSE 'sales' END),
             (SELECT count(*) FROM daily_reporting.source_collection_pages p WHERE p.job_id=j.id)
             FROM daily_reporting.source_collection_jobs j WHERE account_id=$1 AND source='sales' AND cutoff_at=$2",
            &[&target.account_id, &now]).await.unwrap();
        assert_eq!(reset.get::<_, i32>(0), 0);
        assert_eq!(reset.get::<_, i32>(1), 1);
        assert!(reset.get::<_, bool>(2));
        assert!(
            reset.get::<_, bool>(3),
            "persisted departure cooldown must survive restart"
        );
        assert_eq!(reset.get::<_, i64>(4), 0);
        assert_eq!(
            fixture.writer.restart_overlapping_sales(&claim).await,
            Err(PostgresCollectorError::ClaimLost)
        );
        fixture.select(&target.account_id, "sales", now).await;
        let fresh = fixture.claim().await;
        for (identity, page) in empty_pages(&fresh) {
            fixture.seed(&fresh, identity, page).await;
        }
        fixture
            .writer
            .defer_source_job(&fresh, None, 1, false)
            .await
            .unwrap();
        fixture.select(&target.account_id, "sales", now).await;
        fixture.forbid_departures(&target.account_id).await;
        run_quantum(&fixture.config, &fixture.writer, "fresh-sales-test")
            .await
            .unwrap();
        assert_eq!(fixture.state(&fresh).await.0, "published");
    }
}
