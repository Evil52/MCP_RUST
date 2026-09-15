//! The daily spend ceiling: an active campaign is paused before the lock.

use chrono::Duration as ChronoDuration;
use std::{str::FromStr, time::Duration};
use tokio_postgres::Config;

use super::{
    Fixture, POSTGRES_EXECUTOR_TEST_LOCK, WbAutomationExecutionOutcome, WbAutomationPostgresStore,
    mock_http, now, postgres_clock, postgres_legacy, read_state_file, reader_server,
    reader_server_for, test_reader,
};

/// `daily_pause_threshold_minor` (250 RUB) is the soft pause and
/// `daily_spend_cap_minor` (300 RUB) is the ceiling that pause exists to
/// defend. WB statistics can cross both limits in one observation, so an
/// active campaign at the ceiling is still paused through the ordinary
/// pending and read-back path. The next cycle observes the paused campaign
/// above the ceiling and locks it for an operator.
#[tokio::test]
async fn an_active_campaign_at_the_daily_cap_is_paused_before_the_lock() {
    let fixture = Fixture::new();
    let (reader_url, _) = reader_server(9, 102, "2026-08-25", Some(300), 10);
    let (writer_url, writer_requests) = mock_http(vec![(200, "{}".to_owned())]);
    let executor = fixture.executor(&reader_url, &writer_url);

    let receipt = executor.run_once(now()).await.unwrap();

    assert_eq!(
        receipt.outcome,
        WbAutomationExecutionOutcome::WriteSentReconciliationRequired
    );
    assert!(
        writer_requests
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .starts_with("GET /adv/v0/pause")
    );
    let state = read_state_file(&fixture.root.join("execution-state.json"))
        .unwrap()
        .unwrap();
    assert!(state.incident_class.is_none());

    // Read-back confirms the pause. An unroutable writer proves that no
    // further write is attempted from here on.
    let (paused_url, _) = reader_server(11, 102, "2026-08-25", Some(300), 10);
    let readback = fixture.executor(&paused_url, "http://127.0.0.1:1");
    assert_eq!(
        readback
            .run_once(now() + ChronoDuration::minutes(1))
            .await
            .unwrap()
            .outcome,
        WbAutomationExecutionOutcome::Reconciled
    );
    let (still_capped_url, _) = reader_server(11, 102, "2026-08-25", Some(300), 10);
    let locked = fixture.executor(&still_capped_url, "http://127.0.0.1:1");
    assert_eq!(
        locked
            .run_once(now() + ChronoDuration::minutes(2))
            .await
            .unwrap()
            .outcome,
        WbAutomationExecutionOutcome::IncidentLocked
    );
    let state = read_state_file(&fixture.root.join("execution-state.json"))
        .unwrap()
        .unwrap();
    assert_eq!(
        state.incident_class.as_deref(),
        Some("daily_spend_cap_breached")
    );
    assert_eq!(state.paused_for_daily_cap_on, Some(now().date_naive()));
    assert!(state.pending.is_none());

    // The lock is sticky: a later run reports the incident and still does
    // not act, even though the reader now serves a clean observation.
    let (clean_url, _) = reader_server(9, 102, "2026-08-25", None, 10);
    let relocked = fixture.executor(&clean_url, "http://127.0.0.1:1");
    assert_eq!(
        relocked
            .run_once(now() + ChronoDuration::minutes(3))
            .await
            .unwrap()
            .outcome,
        WbAutomationExecutionOutcome::IncidentLocked
    );
}

/// A campaign that is not active at the ceiling has nothing left for the
/// robot to stop, so the breach locks immediately without a write.
#[tokio::test]
async fn a_campaign_not_active_at_the_daily_cap_locks_without_writing() {
    let fixture = Fixture::new();
    let (reader_url, _) = reader_server(11, 102, "2026-08-25", Some(300), 10);
    // An unroutable writer proves no write is attempted on this path.
    let executor = fixture.executor(&reader_url, "http://127.0.0.1:1");

    let receipt = executor.run_once(now()).await.unwrap();

    assert_eq!(
        receipt.outcome,
        WbAutomationExecutionOutcome::IncidentLocked
    );
    let state = read_state_file(&fixture.root.join("execution-state.json"))
        .unwrap()
        .unwrap();
    assert_eq!(
        state.incident_class.as_deref(),
        Some("daily_spend_cap_breached")
    );
    assert!(
        state.pending.is_none(),
        "a breach without a protective pause must not reserve a write"
    );
}

#[tokio::test]
#[ignore = "requires disposable WB automation PostgreSQL role"]
async fn postgres_daily_cap_breach_pauses_active_campaigns_before_the_lock() {
    let database_url = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL")
        .expect("test wrapper must provide the WB automation role");
    let _serial = POSTGRES_EXECUTOR_TEST_LOCK.lock().await;
    let config = Config::from_str(&database_url).unwrap();
    let store = WbAutomationPostgresStore::connect(&config).await.unwrap();
    let observed_at = chrono::Utc::now();
    let current_date = postgres_clock::stats_date(observed_at);

    // Active at the ceiling: the protective pause is sent first, and the
    // cycle after read-back locks the paused campaign for an operator.
    let breach_campaign = 39_682_704;
    let breach_fixture = postgres_clock::fixture(breach_campaign, observed_at);
    let (breach_url, _) = reader_server_for(breach_campaign, 9, 102, &current_date, Some(300), 10);
    let (breach_writer_url, breach_requests) = mock_http(vec![(200, "{}".to_owned())]);
    let mut breach_executor = breach_fixture.executor(&breach_url, &breach_writer_url);
    let breach_legacy = postgres_legacy(&breach_executor, observed_at, None);
    assert_eq!(
        breach_executor
            .run_once_postgres(&store, &breach_legacy, observed_at)
            .await
            .unwrap()
            .unwrap()
            .outcome,
        WbAutomationExecutionOutcome::WriteSentReconciliationRequired
    );
    assert!(
        breach_requests
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .starts_with("GET /adv/v0/pause")
    );
    for (minutes, expected) in [
        (1, WbAutomationExecutionOutcome::Reconciled),
        (2, WbAutomationExecutionOutcome::IncidentLocked),
    ] {
        let at = observed_at + ChronoDuration::minutes(minutes);
        let (paused_url, _) = reader_server_for(
            breach_campaign,
            11,
            102,
            &postgres_clock::stats_date(at),
            Some(300),
            10,
        );
        breach_executor
            .observer
            .replace_client_for_test(test_reader(&paused_url));
        assert_eq!(
            breach_executor
                .run_once_postgres(&store, &breach_legacy, at)
                .await
                .unwrap()
                .unwrap()
                .outcome,
            expected
        );
    }

    // Without an action left in the daily quota the pause cannot be
    // reserved, so the breach falls back to the lock without a write.
    let exhausted_campaign = 39_682_705;
    let exhausted_fixture = postgres_clock::fixture(exhausted_campaign, observed_at);
    let (exhausted_url, _) =
        reader_server_for(exhausted_campaign, 9, 102, &current_date, Some(300), 10);
    let exhausted_executor = exhausted_fixture.executor(&exhausted_url, "http://127.0.0.1:1");
    let mut exhausted_legacy = postgres_legacy(&exhausted_executor, observed_at, None);
    exhausted_legacy.actions_today = exhausted_executor.policy().max_actions_per_day;
    assert_eq!(
        exhausted_executor
            .run_once_postgres(&store, &exhausted_legacy, observed_at)
            .await
            .unwrap()
            .unwrap()
            .outcome,
        WbAutomationExecutionOutcome::IncidentLocked
    );
}
