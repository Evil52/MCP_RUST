use super::*;

#[tokio::test]
async fn sales_restart_budget_survives_saved_pages_and_never_revives_terminal_jobs() {
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
        let expired = fixture.claim().await;
        fixture.admin.execute("UPDATE daily_reporting.source_collection_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE account_id=$1 AND source='sales' AND cutoff_at=$2", &[&target.account_id, &now]).await.unwrap();
        // Losing a lease during validation must not terminate the worker or
        // consume a restart from its successor's budget.
        complete_quantum(&fixture.writer, &expired, Err("sales_page_overlap".into()))
            .await
            .unwrap();
        assert_eq!(fixture.state(&expired).await.0, "running");
        for attempt in 0..3 {
            fixture.select(&target.account_id, "sales", now).await;
            let claim = fixture.claim().await;
            for (identity, page) in empty_pages(&claim) {
                fixture.seed(&claim, identity, page).await;
            }
            complete_quantum(&fixture.writer, &claim, Err("sales_page_overlap".into()))
                .await
                .unwrap();
            let state = fixture.state(&claim).await;
            assert_eq!(state.0, if attempt < 2 { "ready" } else { "failed" });
            assert_eq!(
                state.1.as_deref(),
                Some(if attempt < 2 {
                    "sales_page_overlap"
                } else {
                    "sales_page_overlap_exhausted"
                })
            );
            assert_eq!(
                fixture.writer.restart_overlapping_sales(&claim).await,
                Err(PostgresCollectorError::ClaimLost)
            );
        }
        fixture.select(&target.account_id, "prices", now).await;
        let price = fixture.claim().await;
        assert_eq!(
            fixture.writer.restart_overlapping_sales(&price).await,
            Err(PostgresCollectorError::ClaimLost)
        );
        fixture
            .writer
            .defer_source_job(&price, Some("test_done"), 1, true)
            .await
            .unwrap();
    }
    let allowed: bool = fixture.admin.query_one(
        "SELECT has_function_privilege('report_collector','daily_reporting.restart_overlapping_sales(bigint,bigint,text)','EXECUTE')
         AND NOT has_function_privilege('position_reader','daily_reporting.restart_overlapping_sales(bigint,bigint,text)','EXECUTE')
         AND NOT has_function_privilege('report_refresh_requester','daily_reporting.restart_overlapping_sales(bigint,bigint,text)','EXECUTE')", &[]).await.unwrap().get(0);
    assert!(allowed);
}

#[tokio::test]
async fn inconsistent_current_wb_ads_retry_but_closed_days_and_other_errors_fail_closed() {
    let (Ok(admin_url), Ok(collector_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };
    let _database = DATABASE.lock().await;
    let fixture = Fixture::new(&admin_url, &collector_url).await;
    let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    let start = business_date(now).and_hms_opt(0, 0, 0).unwrap().and_utc() - Duration::hours(5);
    fixture
        .writer
        .enqueue_source_jobs(fixture.config.collection_plan(), now, start, now)
        .await
        .unwrap();
    for target in fixture.config.collection_plan() {
        fixture.select(&target.account_id, "advertising", now).await;
        let mut claim = fixture.claim().await;
        let code = "promotion_counts_inconsistent";
        assert_eq!(
            retry_current_advertising_counts(code, &claim, now),
            target.marketplace == Marketplace::Wildberries
        );
        assert!(!retry_current_advertising_counts(
            "invalid_promotion_response",
            &claim,
            now
        ));
        assert!(!retry_current_advertising_counts(
            code,
            &claim,
            now + Duration::days(1)
        ));
        let attempts = if target.marketplace == Marketplace::Wildberries {
            8
        } else {
            1
        };
        for attempt in 0..attempts {
            complete_quantum(&fixture.writer, &claim, Err(code.into()))
                .await
                .unwrap();
            let state = fixture.state(&claim).await;
            assert_eq!(
                state.0,
                if attempt + 1 < attempts {
                    "ready"
                } else {
                    "failed"
                }
            );
            if attempt + 1 < attempts {
                let delay: bool = fixture.admin.query_one("SELECT next_attempt_at > clock_timestamp() + interval '60 seconds' FROM daily_reporting.source_collection_jobs WHERE account_id=$1 AND source='advertising' AND cutoff_at=$2", &[&target.account_id,&now]).await.unwrap().get(0);
                assert!(delay);
                fixture.select(&target.account_id, "advertising", now).await;
                claim = fixture.claim().await;
            }
        }
    }
}
