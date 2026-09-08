use chrono::{Duration, Utc};
use mcp_ozon::reporting::{
    business_date,
    checkpoint::{CheckpointError, checkpointed},
    collector_plan::CollectionTarget,
    mcp_read::{DataState, ReportingReader, SourceSnapshotQuery},
    postgres_collector::{
        CollectedFacts, CollectedPriceFact, CollectedSalesFact, CollectedStockFact,
        PostgresCollectorError, PostgresSnapshotWriter, SourceJobClaim,
    },
    snapshot::{AccountScope, Marketplace, SnapshotSource},
};
use serde_json::json;
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Client, Config, NoTls};

async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    client
}
async fn select_source(admin: &Client, account: &str, source: &str) {
    admin.execute("UPDATE daily_reporting.source_collection_jobs SET next_attempt_at=CASE WHEN source=$2 THEN clock_timestamp()-interval '1 second' ELSE clock_timestamp()+interval '1 hour' END WHERE account_id=$1 AND status='ready'", &[&account,&source]).await.unwrap();
    admin.execute("UPDATE daily_reporting.source_collection_departures SET next_allowed_at=clock_timestamp()-interval '1 second' WHERE account_id=$1", &[&account]).await.unwrap();
}
async fn claim(writer: &PostgresSnapshotWriter, target: &CollectionTarget) -> SourceJobClaim {
    writer
        .claim_source_job(std::slice::from_ref(target), "source-test")
        .await
        .unwrap()
        .unwrap()
}
const fn query(source: SnapshotSource) -> SourceSnapshotQuery {
    SourceSnapshotQuery {
        source,
        snapshot_id: None,
        limit: 1,
        offset: 0,
    }
}

#[tokio::test]
async fn independent_pages_survive_restart_and_ad_failure_preserves_published_data() {
    let (Ok(admin_url), Ok(collector_url), Ok(reader_url), Ok(requester_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
        std::env::var("POSITION_REPOSITORY_TEST_READER_URL"),
        std::env::var("REPORT_REFRESH_TEST_REQUESTER_URL"),
    ) else {
        return;
    };
    let admin = connect(&admin_url).await;
    let collector = connect(&collector_url).await;
    let reader_db = connect(&reader_url).await;
    let requester = connect(&requester_url).await;
    let writer = Arc::new(
        PostgresSnapshotWriter::connect(&Config::from_str(&collector_url).unwrap())
            .await
            .unwrap(),
    );
    writer.verify_source_job_contract().await.unwrap();
    let reader = ReportingReader::connect_optional(Some(&reader_url))
        .await
        .unwrap();
    let account = format!("source_resume_{}", std::process::id());
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
    // Match PostgreSQL timestamp precision on hosts whose clock exposes
    // nanoseconds; the requested cutoff must equal its persisted identity.
    let now = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    let start = business_date(now)
        .pred_opt()
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        - Duration::hours(5);
    let cutoff = now - Duration::minutes(10);
    writer
        .enqueue_source_jobs(
            std::slice::from_ref(&target),
            cutoff,
            start,
            start + Duration::days(1),
        )
        .await
        .unwrap();
    writer
        .enqueue_source_jobs(
            std::slice::from_ref(&target),
            cutoff,
            start,
            start + Duration::days(1),
        )
        .await
        .unwrap();
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM daily_reporting.source_collection_jobs WHERE account_id=$1",
                &[&account]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        4
    );
    select_source(&admin, &account, "prices").await;
    let first = claim(&writer, &target).await;
    let journal = writer.source_checkpoints(&first);
    let page = vec![CollectedPriceFact {
        sku: 1,
        price_minor: 100,
        old_price_minor: None,
    }];
    let saved: Vec<CollectedPriceFact> = checkpointed(&journal, json!(["prices", 0]), || async {
        Ok::<_, CheckpointError>(page.clone())
    })
    .await
    .unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(
        checkpointed(&journal, json!(["prices", 1]), || async {
            Ok::<_, CheckpointError>(Vec::<CollectedPriceFact>::new())
        })
        .await,
        Err(CheckpointError::Deferred)
    );
    // A real lease expires; a fresh writer resumes using only PostgreSQL state.
    admin.execute("UPDATE daily_reporting.source_collection_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE account_id=$1 AND status='running'", &[&account]).await.unwrap();
    let restarted = Arc::new(
        PostgresSnapshotWriter::connect(&Config::from_str(&collector_url).unwrap())
            .await
            .unwrap(),
    );
    select_source(&admin, &account, "prices").await;
    let second = claim(&restarted, &target).await;
    assert!(
        journal
            .as_ref()
            .unwrap()
            .load(&"a".repeat(64))
            .await
            .is_err()
    );
    assert_eq!(
        writer
            .publish_source_job(&first, CollectedFacts::Prices(page.clone()), vec![], "test")
            .await,
        Err(PostgresCollectorError::ClaimLost)
    );
    let resumed = restarted.source_checkpoints(&second);
    let mut rows: Vec<CollectedPriceFact> =
        checkpointed(&resumed, json!(["prices", 0]), || async {
            panic!("saved page must not hit API");
            #[allow(unreachable_code)]
            Ok::<_, CheckpointError>(vec![])
        })
        .await
        .unwrap();
    rows.extend(
        checkpointed(&resumed, json!(["prices", 1]), || async {
            Ok::<_, CheckpointError>(vec![CollectedPriceFact {
                sku: 2,
                price_minor: 200,
                old_price_minor: None,
            }])
        })
        .await
        .unwrap(),
    );
    let snapshot = restarted
        .publish_source_job(&second, CollectedFacts::Prices(rows), vec![], "test")
        .await
        .unwrap();
    assert_eq!(admin.query_one("SELECT count(*) FROM daily_reporting.source_collection_pages p JOIN daily_reporting.source_collection_jobs j ON p.job_id=j.id WHERE j.account_id=$1", &[&account]).await.unwrap().get::<_,i64>(0),0);
    let prices = reader
        .source_snapshot(&scope, query(SnapshotSource::Prices))
        .await
        .unwrap();
    assert_eq!(prices.state, "available");
    assert_eq!(prices.total_rows, 2);
    assert_eq!(prices.next_offset, Some(1));
    assert!(prices.source_as_of.is_some() && prices.observed_from.is_some());
    let next = reader
        .source_snapshot(
            &scope,
            SourceSnapshotQuery {
                snapshot_id: Some(snapshot),
                offset: 1,
                ..query(SnapshotSource::Prices)
            },
        )
        .await
        .unwrap();
    assert_eq!(next.rows[0]["sku"], 2);
    assert!(
        reader
            .source_snapshot(
                &scope,
                SourceSnapshotQuery {
                    offset: 1,
                    ..query(SnapshotSource::Prices)
                }
            )
            .await
            .is_err()
    );
    let other = AccountScope::new("source_other".to_owned(), Marketplace::Wildberries).unwrap();
    assert_eq!(
        reader
            .source_snapshot(
                &other,
                SourceSnapshotQuery {
                    snapshot_id: Some(snapshot),
                    ..query(SnapshotSource::Prices)
                }
            )
            .await
            .unwrap()
            .state,
        "missing"
    );
    for (name, facts) in [
        (
            "sales",
            CollectedFacts::Sales(vec![CollectedSalesFact {
                business_date: business_date(start),
                sku: 1,
                ordered_units: 3,
                operational_gmv_minor: 300,
                cancelled_units: None,
                returned_units: None,
            }]),
        ),
        (
            "stocks",
            CollectedFacts::Stocks(vec![CollectedStockFact {
                sku: 1,
                warehouse_id: "warehouse-1".to_owned(),
                sellable_units: 5,
            }]),
        ),
    ] {
        if name == "stocks" {
            admin.execute("UPDATE daily_reporting.source_collection_jobs SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE account_id=$1 AND source='stocks'", &[&account]).await.unwrap();
            let blocked = claim(&restarted, &target).await;
            assert_eq!(
                restarted
                    .source_checkpoints(&blocked)
                    .unwrap()
                    .admit()
                    .await,
                Err(CheckpointError::Deferred),
                "WB stock reads share the Sales Analytics quota after restart"
            );
            restarted
                .defer_source_job(&blocked, None, 1, false)
                .await
                .unwrap();
        }
        select_source(&admin, &account, name).await;
        let c = claim(&restarted, &target).await;
        checkpointed(
            &restarted.source_checkpoints(&c),
            json!([name, 0]),
            || async { Ok::<_, CheckpointError>(vec![1]) },
        )
        .await
        .unwrap();
        restarted
            .publish_source_job(&c, facts, vec![], "test")
            .await
            .unwrap();
    }
    select_source(&admin, &account, "advertising").await;
    let ads = claim(&restarted, &target).await;
    restarted
        .defer_source_job(&ads, Some("rate_limited"), 3600, false)
        .await
        .unwrap();
    let gate:bool=admin.query_one("SELECT next_allowed_at>clock_timestamp()+interval '59 minutes' FROM daily_reporting.source_collection_departures WHERE account_id=$1 AND source='advertising'", &[&account]).await.unwrap().get(0);
    assert!(gate);
    select_source(&admin, &account, "advertising").await;
    let ads = claim(&restarted, &target).await;
    restarted
        .defer_source_job(&ads, Some("invalid_promotion_response"), 1, true)
        .await
        .unwrap();
    assert_eq!(
        reader
            .source_snapshot(&scope, query(SnapshotSource::Prices))
            .await
            .unwrap()
            .snapshot_id,
        Some(snapshot.to_string())
    );
    let ads_status = reader
        .source_snapshot(&scope, query(SnapshotSource::Advertising))
        .await
        .unwrap();
    assert_eq!(ads_status.state, "missing");
    assert_eq!(ads_status.latest_collection.unwrap()["status"], "failed");
    assert_ne!(
        reader
            .data_completeness(&scope, Some(cutoff))
            .await
            .unwrap()
            .state,
        DataState::Complete
    );
    for source in [SnapshotSource::Sales, SnapshotSource::Stocks] {
        let data = reader.source_snapshot(&scope, query(source)).await.unwrap();
        assert_eq!(data.state, "available");
        assert_eq!(data.total_rows, 1);
    }
    // Long pauses expire point observations, never relabel them as current.
    let expiry_account = format!("source_expiry_{}", std::process::id());
    let expiry_target = CollectionTarget {
        account_id: expiry_account.clone(),
        ..target.clone()
    };
    restarted
        .enqueue_source_jobs(
            std::slice::from_ref(&expiry_target),
            cutoff,
            start,
            start + Duration::days(1),
        )
        .await
        .unwrap();
    select_source(&admin, &expiry_account, "stocks").await;
    let stocks = claim(&restarted, &expiry_target).await;
    let stock_journal = restarted.source_checkpoints(&stocks);
    checkpointed(&stock_journal, json!("stocks"), || async {
        Ok::<_, CheckpointError>(vec![1])
    })
    .await
    .unwrap();
    restarted
        .defer_source_job(&stocks, None, 1, false)
        .await
        .unwrap();
    admin.execute("UPDATE daily_reporting.source_collection_jobs SET first_observed_at=clock_timestamp()-interval '31 minutes' WHERE account_id=$1 AND source='stocks'", &[&expiry_account]).await.unwrap();
    select_source(&admin, &expiry_account, "stocks").await;
    assert!(
        restarted
            .claim_source_job(std::slice::from_ref(&expiry_target), "source-test")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(admin.query_one("SELECT error_class FROM daily_reporting.source_collection_jobs WHERE account_id=$1 AND source='stocks'", &[&expiry_account]).await.unwrap().get::<_,String>(0),"collection_expired");
    // Least privilege: readers can see progress, not unpublished pages or jobs.
    assert!(
        reader_db
            .query("SELECT * FROM daily_reporting.source_collection_pages", &[])
            .await
            .is_err()
    );
    assert!(
        collector
            .execute(
                "UPDATE daily_reporting.source_collection_jobs SET status='ready'",
                &[]
            )
            .await
            .is_err()
    );
    assert!(collector.execute("UPDATE daily_reporting.source_snapshots SET source_job_id=NULL,source_job_generation=NULL WHERE id=$1", &[&snapshot]).await.is_err());
    // Manual refresh deduplicates and drives the same per-source queue.
    let refresh_account = format!("source_refresh_{}", std::process::id());
    let refresh_target = CollectionTarget {
        account_id: refresh_account.clone(),
        ..target.clone()
    };
    let date = business_date(Utc::now());
    let request=requester.query_one("SELECT * FROM daily_reporting.request_marketplace_sales_refresh($1,'wildberries','test',$2)", &[&refresh_account,&date]).await.unwrap();
    let request_id: i64 = request.get("request_id");
    assert_eq!(requester.query_one("SELECT * FROM daily_reporting.request_marketplace_sales_refresh($1,'wildberries','test',$2)", &[&refresh_account,&date]).await.unwrap().get::<_,i64>("request_id"),request_id);
    restarted
        .dispatch_source_refreshes(std::slice::from_ref(&refresh_target))
        .await
        .unwrap();
    assert!(
        collector
            .query_opt(
                "SELECT * FROM daily_reporting.claim_marketplace_sales_refresh('legacy-worker')",
                &[]
            )
            .await
            .unwrap()
            .is_none()
    );
    for (name, facts) in [
        ("prices", CollectedFacts::Prices(vec![])),
        ("stocks", CollectedFacts::Stocks(vec![])),
        ("sales", CollectedFacts::Sales(vec![])),
        ("advertising", CollectedFacts::Advertising(vec![])),
    ] {
        select_source(&admin, &refresh_account, name).await;
        let c = claim(&restarted, &refresh_target).await;
        checkpointed(
            &restarted.source_checkpoints(&c),
            json!([name, 0]),
            || async { Ok::<_, CheckpointError>(Vec::<i32>::new()) },
        )
        .await
        .unwrap();
        restarted
            .publish_source_job(&c, facts, vec![], "test")
            .await
            .unwrap();
        restarted
            .dispatch_source_refreshes(std::slice::from_ref(&refresh_target))
            .await
            .unwrap();
        let status: String = admin
            .query_one(
                "SELECT status FROM daily_reporting.ozon_sales_refresh_requests WHERE id=$1",
                &[&request_id],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            status,
            if name == "advertising" {
                "succeeded"
            } else {
                "queued"
            }
        );
    }
}
