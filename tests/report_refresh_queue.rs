use std::{collections::BTreeSet, str::FromStr, sync::Arc};

use chrono::{FixedOffset, TimeZone, Utc};
use mcp_ozon::reporting::{
    business_date,
    collector_plan::CollectionTarget,
    postgres_collector::{CollectedFacts, CollectedSnapshot, PostgresSnapshotWriter},
    refresh_queue::{RefreshRequestService, SalesRefreshState},
    snapshot::{Marketplace, SnapshotSource, SnapshotStatus},
};
use tokio::sync::Barrier;
use tokio_postgres::Config;

#[tokio::test]
async fn fourteen_parallel_manager_requests_create_one_refresh_job() {
    let Some(database_url) = std::env::var("REPORT_REFRESH_TEST_REQUESTER_URL").ok() else {
        return;
    };
    let service = RefreshRequestService::connect_optional(Some(&database_url))
        .await
        .expect("restricted refresh requester must connect");
    let account_id = format!(
        "parallel_{}_{}",
        std::process::id(),
        Utc::now().timestamp_micros()
    );
    let date = business_date(Utc::now());
    let barrier = Arc::new(Barrier::new(15));
    let mut tasks = Vec::with_capacity(14);
    for sequence in 1..=14 {
        let service = service.clone();
        let account_id = account_id.clone();
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            service
                .request(&account_id, &format!("manager_{sequence}"), date)
                .await
        }));
    }
    barrier.wait().await;

    let mut request_ids = BTreeSet::new();
    let mut created = 0_usize;
    for task in tasks {
        let status = task
            .await
            .expect("manager request task must not panic")
            .expect("manager request must be accepted");
        request_ids.insert(status.request_id.expect("queued request has an id"));
        created += usize::from(status.created == Some(true));
    }
    assert_eq!(request_ids.len(), 1);
    assert_eq!(created, 1);

    let status = service
        .status(&account_id)
        .await
        .expect("status projection must remain readable");
    assert_eq!(status.request_id, request_ids.into_iter().next());
    let second_account = format!(
        "second_{}_{}",
        std::process::id(),
        Utc::now().timestamp_micros()
    );
    service
        .request(&second_account, "manager_second", date)
        .await
        .expect("a different account may queue behind the first one");

    let collector_url = std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL")
        .expect("collector URL accompanies the requester test URL");
    let writer = PostgresSnapshotWriter::connect(
        &Config::from_str(&collector_url).expect("collector URL must parse"),
    )
    .await
    .expect("restricted collector must connect");
    writer
        .verify_runtime_contract()
        .await
        .expect("collector queue privileges must remain exact");
    let claim = writer
        .claim_sales_refresh("integration-test")
        .await
        .expect("collector claim must execute")
        .expect("the deduplicated job must be claimable once");
    assert_eq!(claim.account_id(), account_id);
    assert!(
        writer
            .claim_sales_refresh("integration-test-2")
            .await
            .expect("second collector claim must execute")
            .is_none()
    );
    assert!(
        writer
            .fail_sales_refresh(&claim, "integration_test_finished")
            .await
            .expect("collector must close the test job")
    );
    let failed = service
        .status(&account_id)
        .await
        .expect("requester must observe the collector result");
    assert_eq!(
        failed.state,
        mcp_ozon::reporting::refresh_queue::SalesRefreshState::Failed
    );
    let second_claim = writer
        .claim_sales_refresh("integration-test-2")
        .await
        .expect("the next account becomes claimable after terminal completion")
        .expect("the second account must remain queued");
    assert_eq!(second_claim.account_id(), second_account);
    assert!(
        writer
            .fail_sales_refresh(&second_claim, "integration_test_finished")
            .await
            .expect("collector must close the second test job")
    );

    let atomic_account = format!(
        "atomic_{}_{}",
        std::process::id(),
        Utc::now().timestamp_micros()
    );
    service
        .request(&atomic_account, "manager_atomic", date)
        .await
        .expect("atomic refresh request must queue");
    let refresh_claim = writer
        .claim_sales_refresh("atomic-test")
        .await
        .expect("atomic refresh claim must execute")
        .expect("atomic refresh must be claimable");
    assert_eq!(refresh_claim.account_id(), atomic_account);
    let target = CollectionTarget {
        account_id: atomic_account.clone(),
        marketplace: Marketplace::Ozon,
        sources: vec![
            SnapshotSource::Sales,
            SnapshotSource::Advertising,
            SnapshotSource::Stocks,
            SnapshotSource::Prices,
            SnapshotSource::Finance,
        ],
    };
    let collection_claim = writer
        .claim_target(&target, refresh_claim.cutoff_at(), "atomic-test")
        .await
        .expect("snapshot claim must execute")
        .expect("snapshot identity must be claimable");
    let offset = FixedOffset::east_opt(5 * 60 * 60).unwrap();
    let period_start = offset
        .from_local_datetime(&date.and_hms_opt(0, 0, 0).unwrap())
        .single()
        .unwrap()
        .with_timezone(&Utc);
    let cutoff = refresh_claim.cutoff_at();
    let snapshots = [
        empty_snapshot(
            &atomic_account,
            cutoff,
            period_start,
            cutoff,
            CollectedFacts::Sales(Vec::new()),
        ),
        empty_snapshot(
            &atomic_account,
            cutoff,
            period_start,
            cutoff,
            CollectedFacts::Advertising(Vec::new()),
        ),
        empty_snapshot(
            &atomic_account,
            cutoff,
            period_start,
            cutoff,
            CollectedFacts::Finance(Vec::new()),
        ),
        empty_snapshot(
            &atomic_account,
            cutoff,
            cutoff,
            cutoff,
            CollectedFacts::Stocks(Vec::new()),
        ),
        empty_snapshot(
            &atomic_account,
            cutoff,
            cutoff,
            cutoff,
            CollectedFacts::Prices(Vec::new()),
        ),
    ];
    assert_eq!(
        writer
            .persist_refresh_claimed_batch(&collection_claim, &refresh_claim, &snapshots)
            .await
            .expect("snapshot and queue completion must commit atomically")
            .len(),
        5
    );
    let succeeded = service
        .status(&atomic_account)
        .await
        .expect("requester must observe atomic completion");
    assert_eq!(succeeded.state, SalesRefreshState::Succeeded);
}

fn empty_snapshot(
    account_id: &str,
    cutoff: chrono::DateTime<Utc>,
    period_start: chrono::DateTime<Utc>,
    period_end: chrono::DateTime<Utc>,
    facts: CollectedFacts,
) -> CollectedSnapshot {
    CollectedSnapshot::new(
        account_id.to_owned(),
        Marketplace::Ozon,
        cutoff,
        cutoff,
        period_start,
        period_end,
        SnapshotStatus::Succeeded,
        true,
        "integration-test".to_owned(),
        facts,
    )
    .expect("empty complete source snapshot must be valid")
}
