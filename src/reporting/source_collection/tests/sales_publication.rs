use super::*;
use crate::reporting::postgres_collector::CollectedSalesFact;

#[tokio::test]
async fn sales_publication_replayed_offset_overlap_fails_closed_for_both_marketplaces() {
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
            (
                "failed".to_owned(),
                Some("invalid_source_publication".to_owned()),
                1
            )
        );
        let published: i64 = fixture.admin.query_one(
            "SELECT count(*) FROM daily_reporting.source_snapshots WHERE account_id=$1 AND source='sales' AND cutoff_at=$2",
            &[&target.account_id, &now]).await.unwrap().get(0);
        assert_eq!(
            published, 0,
            "overlap must never be silently deduplicated or published"
        );
    }
}
