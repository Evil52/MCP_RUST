use std::{collections::BTreeSet, str::FromStr, sync::Arc, time::Duration};

use chrono::{FixedOffset, TimeZone, Utc};
use mcp_ozon::reporting::{
    business_date,
    collector_plan::CollectionTarget,
    postgres_collector::{CollectedFacts, CollectedSnapshot, PostgresSnapshotWriter},
    refresh_queue::{RefreshRequestService, SalesRefreshState},
    snapshot::{Marketplace, SnapshotSource, SnapshotStatus},
};
use mcp_ozon::tool_telemetry::{ToolCallLogOutcome, ToolCallOutcome, ToolTelemetryService};
use tokio::sync::Barrier;
use tokio_postgres::{Client, Config, NoTls};

#[tokio::test]
async fn fourteen_parallel_manager_requests_create_one_refresh_job() {
    let Some(database_url) = std::env::var("REPORT_REFRESH_TEST_REQUESTER_URL").ok() else {
        return;
    };
    let service = RefreshRequestService::connect_optional(Some(&database_url))
        .await
        .expect("restricted refresh requester must connect");
    let telemetry = ToolTelemetryService::connect_optional(Some(&database_url))
        .await
        .expect("restricted telemetry writer must connect");
    let receipt = telemetry
        .begin(
            "admin",
            "ofk_collection_status",
            Some("telemetry_test"),
            Some(Marketplace::Ozon),
        )
        .await
        .expect("telemetry begin must execute");
    telemetry
        .finish(
            receipt,
            ToolCallOutcome::Succeeded,
            Duration::from_millis(17),
            None,
        )
        .await
        .expect("telemetry finish must execute");
    let log = telemetry
        .list(10)
        .await
        .expect("telemetry log must be readable");
    let recorded = log
        .calls
        .iter()
        .find(|call| call.account_id.as_deref() == Some("telemetry_test"))
        .expect("completed telemetry row must be projected");
    assert_eq!(recorded.outcome, ToolCallLogOutcome::Succeeded);
    assert_eq!(recorded.duration_ms, Some(17));
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
                .request(
                    &account_id,
                    Marketplace::Ozon,
                    &format!("manager_{sequence}"),
                    date,
                )
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
        .status(&account_id, Marketplace::Ozon)
        .await
        .expect("status projection must remain readable");
    assert_eq!(status.request_id, request_ids.into_iter().next());
    let second_account = format!(
        "second_{}_{}",
        std::process::id(),
        Utc::now().timestamp_micros()
    );
    service
        .request(&second_account, Marketplace::Ozon, "manager_second", date)
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
        .status(&account_id, Marketplace::Ozon)
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
        .request(&atomic_account, Marketplace::Ozon, "manager_atomic", date)
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
            Marketplace::Ozon,
            cutoff,
            period_start,
            cutoff,
            CollectedFacts::Sales(Vec::new()),
        ),
        empty_snapshot(
            &atomic_account,
            Marketplace::Ozon,
            cutoff,
            period_start,
            cutoff,
            CollectedFacts::Advertising(Vec::new()),
        ),
        empty_snapshot(
            &atomic_account,
            Marketplace::Ozon,
            cutoff,
            period_start,
            cutoff,
            CollectedFacts::Finance(Vec::new()),
        ),
        empty_snapshot(
            &atomic_account,
            Marketplace::Ozon,
            cutoff,
            cutoff,
            cutoff,
            CollectedFacts::Stocks(Vec::new()),
        ),
        empty_snapshot(
            &atomic_account,
            Marketplace::Ozon,
            cutoff,
            cutoff,
            cutoff,
            CollectedFacts::Prices(Vec::new()),
        ),
    ];
    writer
        .stage_claimed_batch(&collection_claim, &snapshots)
        .await
        .expect("normalized Ozon staging checkpoint must commit");

    let admin = connect_admin().await;
    admin
        .execute(
            "UPDATE daily_reporting.ozon_sales_refresh_requests \
             SET lease_until = started_at + interval '1 microsecond' \
             WHERE account_id = $1 AND marketplace = 'ozon' AND status = 'running'",
            &[&atomic_account],
        )
        .await
        .expect("test administrator must expire the refresh lease");
    admin
        .execute(
            "UPDATE daily_reporting.collection_claims \
             SET lease_until = claimed_at + interval '1 microsecond' \
             WHERE account_id = $1 AND marketplace = 'ozon' AND status = 'active'",
            &[&atomic_account],
        )
        .await
        .expect("test administrator must expire the collection lease");
    let recovered_refresh_claim = writer
        .claim_sales_refresh("atomic-recovery-test")
        .await
        .expect("expired refresh lease must be reclaimable")
        .expect("retryable refresh must retain its queue identity");
    assert_eq!(recovered_refresh_claim.account_id(), atomic_account);
    assert_eq!(recovered_refresh_claim.cutoff_at(), cutoff);
    let recovered_collection_claim = writer
        .claim_target(&target, cutoff, "atomic-recovery-test")
        .await
        .expect("expired collection lease must be reclaimable")
        .expect("retryable collection must retain its durable identity");
    let staged = writer
        .load_staged_batch(&recovered_collection_claim)
        .await
        .expect("Ozon staging readback after lease recovery must execute")
        .expect("complete Ozon staging batch must survive owner recovery");
    assert_eq!(staged.len(), snapshots.len());
    assert_eq!(
        writer
            .persist_refresh_claimed_batch(
                &recovered_collection_claim,
                &recovered_refresh_claim,
                &staged,
            )
            .await
            .expect("snapshot and queue completion must commit atomically")
            .len(),
        5
    );
    let succeeded = service
        .status(&atomic_account, Marketplace::Ozon)
        .await
        .expect("requester must observe atomic completion");
    assert_eq!(succeeded.state, SalesRefreshState::Succeeded);
    assert!(
        writer
            .load_staged_batch(&recovered_collection_claim)
            .await
            .expect("published Ozon staging lookup must execute")
            .is_none(),
        "atomic publication must clear normalized staging rows"
    );

    let wb_account = format!(
        "wb_atomic_{}_{}",
        std::process::id(),
        Utc::now().timestamp_micros()
    );
    service
        .request(
            &wb_account,
            Marketplace::Wildberries,
            "manager_wb_atomic",
            date,
        )
        .await
        .expect("WB refresh request must queue");
    let wb_refresh_claim = writer
        .claim_sales_refresh("wb-atomic-test")
        .await
        .expect("WB refresh claim must execute")
        .expect("WB refresh must be claimable");
    assert_eq!(wb_refresh_claim.account_id(), wb_account);
    assert_eq!(wb_refresh_claim.marketplace(), Marketplace::Wildberries);
    let wb_target = CollectionTarget {
        account_id: wb_account.clone(),
        marketplace: Marketplace::Wildberries,
        sources: vec![
            SnapshotSource::Sales,
            SnapshotSource::Advertising,
            SnapshotSource::Stocks,
            SnapshotSource::Prices,
        ],
    };
    let wb_collection_claim = writer
        .claim_target(&wb_target, wb_refresh_claim.cutoff_at(), "wb-atomic-test")
        .await
        .expect("WB snapshot claim must execute")
        .expect("WB snapshot identity must be claimable");
    let wb_cutoff = wb_refresh_claim.cutoff_at();
    let wb_snapshots = [
        empty_snapshot(
            &wb_account,
            Marketplace::Wildberries,
            wb_cutoff,
            period_start,
            wb_cutoff,
            CollectedFacts::Sales(Vec::new()),
        ),
        empty_snapshot(
            &wb_account,
            Marketplace::Wildberries,
            wb_cutoff,
            period_start,
            wb_cutoff,
            CollectedFacts::Advertising(Vec::new()),
        ),
        empty_snapshot(
            &wb_account,
            Marketplace::Wildberries,
            wb_cutoff,
            wb_cutoff,
            wb_cutoff,
            CollectedFacts::Stocks(Vec::new()),
        ),
        empty_snapshot(
            &wb_account,
            Marketplace::Wildberries,
            wb_cutoff,
            wb_cutoff,
            wb_cutoff,
            CollectedFacts::Prices(Vec::new()),
        ),
    ];
    writer
        .stage_claimed_batch(&wb_collection_claim, &wb_snapshots)
        .await
        .expect("normalized WB staging checkpoint must commit");
    let staged_wb = writer
        .load_staged_batch(&wb_collection_claim)
        .await
        .expect("WB staging readback must execute")
        .expect("complete WB staging batch must be recoverable");
    assert_eq!(staged_wb.len(), wb_snapshots.len());
    assert_eq!(
        writer
            .persist_refresh_claimed_batch(&wb_collection_claim, &wb_refresh_claim, &staged_wb)
            .await
            .expect("WB snapshots and queue completion must commit atomically")
            .len(),
        4
    );
    let wb_succeeded = service
        .status(&wb_account, Marketplace::Wildberries)
        .await
        .expect("requester must observe WB atomic completion");
    assert_eq!(wb_succeeded.marketplace, Marketplace::Wildberries);
    assert_eq!(wb_succeeded.state, SalesRefreshState::Succeeded);
}

async fn connect_admin() -> Client {
    let database_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL")
        .expect("administrator URL accompanies the requester test URL");
    let (client, connection) = Config::from_str(&database_url)
        .expect("administrator URL must parse")
        .connect(NoTls)
        .await
        .expect("test administrator must connect");
    tokio::spawn(async move {
        connection.await.expect("administrator connection must run");
    });
    client
}

fn empty_snapshot(
    account_id: &str,
    marketplace: Marketplace,
    cutoff: chrono::DateTime<Utc>,
    period_start: chrono::DateTime<Utc>,
    period_end: chrono::DateTime<Utc>,
    facts: CollectedFacts,
) -> CollectedSnapshot {
    CollectedSnapshot::new(
        account_id.to_owned(),
        marketplace,
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
