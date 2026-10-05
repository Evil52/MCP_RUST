use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::*;
use crate::marketplace_quota::{QuotaKey, SharedQuota};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

#[tokio::test]
async fn file_executor_clears_only_proven_not_sent_pending() {
    let fixture = Fixture::new();
    let observed_at = now();
    let (reader, _) = reader_server(9, 102, "2026-08-25", Some(0), 10);
    let (writer, requests) = mock_http(Vec::new());
    let mut executor = fixture.executor(&reader, &writer);
    executor.writer = executor
        .writer
        .clone()
        .with_authorization_window(observed_at - chrono::Duration::hours(1), observed_at)
        .with_authorization_clock(Arc::new(move || observed_at));
    let receipt = executor.run_once(observed_at).await.unwrap();
    assert_eq!(
        receipt.outcome,
        WbAutomationExecutionOutcome::ReservationCancelled
    );
    let state = read_state_file(&fixture.root.join("execution-state.json"))
        .unwrap()
        .unwrap();
    assert!(state.pending.is_none());
    assert!(state.incident_class.is_none());
    assert_eq!(state.actions_today, 1);
    assert!(requests.recv_timeout(Duration::from_millis(25)).is_err());
}

#[tokio::test]
#[ignore = "requires disposable WB automation PostgreSQL role"]
#[expect(
    clippy::significant_drop_tightening,
    reason = "the campaign lease is consumed by asynchronous release"
)]
async fn postgres_not_sent_cancels_reserved_and_write_started_without_incident() {
    let url = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL").unwrap();
    let _serial = POSTGRES_EXECUTOR_TEST_LOCK.lock().await;
    let store = WbAutomationPostgresStore::connect(&Config::from_str(&url).unwrap())
        .await
        .unwrap();
    let observed_at = postgres_clock::observation_time();
    let current_date = postgres_clock::stats_date(observed_at);
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").unwrap();
    let (admin, connection) = Config::from_str(&admin_url)
        .unwrap()
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(async move {
        connection.await.unwrap();
    });
    for kind in 0..4 {
        let campaign_id = 39_682_780 + kind;
        let fixture = postgres_clock::fixture(campaign_id, observed_at);
        let (reader, _) =
            campaign_level_reader_server_for(campaign_id, 9, [117, 117, 117], &current_date, 2, 10);
        let (writer, requests) = mock_http(Vec::new());
        let mut executor = fixture.executor(&reader, &writer);
        if kind == 0 {
            executor.writer =
                executor
                    .writer
                    .clone()
                    .with_shared_quota(SharedQuota::from_database_url(
                        "invalid quota configuration",
                    ));
        } else if kind == 3 {
            let quota_url = std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL").unwrap();
            let token = format!(
                "e30.{}.signature",
                URL_SAFE_NO_PAD.encode(br#"{"sid":"e4000000-0000-4000-8000-000000000078"}"#)
            );
            let quota = SharedQuota::from_database_url(&quota_url);
            quota
                .admit(
                    &QuotaKey::wb(&token, "promotion_bids_write").unwrap(),
                    Duration::from_secs(30),
                )
                .await
                .unwrap();
            executor.writer = crate::control::wb::WbBidWriteClient::new_for_test(
                &writer,
                &token,
                Duration::from_secs(1),
            )
            .with_shared_quota(quota);
        } else {
            let checks = AtomicUsize::new(0);
            executor.writer = executor
                .writer
                .clone()
                .with_authorization_window(
                    observed_at - chrono::Duration::hours(1),
                    observed_at + chrono::Duration::seconds(1),
                )
                .with_authorization_clock(Arc::new(move || {
                    if kind == 2 && checks.fetch_add(1, Ordering::SeqCst) == 0 {
                        observed_at
                    } else {
                        observed_at + chrono::Duration::seconds(1)
                    }
                }));
        }
        let legacy = WbAutomationLegacyStateSeed {
            policy_digest: executor.policy_sha256().to_owned(),
            business_date: wb_automation_business_date(observed_at),
            actions_today: 0,
            last_action_at: None,
            paused_for_daily_cap_on: None,
            incident_class: None,
            legacy_digest: "d".repeat(64),
        };
        let receipt = executor
            .run_once_postgres(&store, &legacy, observed_at)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            receipt.outcome,
            WbAutomationExecutionOutcome::ReservationCancelled
        );
        let lease = store
            .try_acquire_campaign("ip_domnyshev_wb", campaign_id)
            .await
            .unwrap()
            .unwrap();
        let state = lease.load_state().await.unwrap().unwrap();
        assert!(state.pending_idempotency_key.is_none());
        assert!(state.incident_class.is_none());
        assert_eq!(state.actions_today, 1);
        assert_eq!(state.revision, 3);
        lease.release().await.unwrap();
        let row = admin.query_one("SELECT status, write_started_at, last_error_class FROM wb_automation.action_attempts WHERE advert_id=$1 AND account_id='ip_domnyshev_wb'", &[&i64::try_from(campaign_id).unwrap()]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "cancelled");
        assert_eq!(
            row.get::<_, Option<chrono::DateTime<Utc>>>(1).is_some(),
            kind >= 2
        );
        assert_eq!(row.get::<_, String>(2), "write_not_sent");
        assert!(requests.recv_timeout(Duration::from_millis(25)).is_err());
    }
    drop(admin);
    driver.await.unwrap();
}
