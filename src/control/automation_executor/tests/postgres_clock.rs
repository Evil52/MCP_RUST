use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Timelike, Utc};
use std::{fs, time::Duration};

use super::{
    Fixture, PendingAction, PendingActionKind, WbAutomationExecutionOutcome, WbAutomationPolicy,
    mock_http, observation, read_state_file, reader_server, reconcile_pending, state, test_reader,
    wb_automation_business_date,
};

pub(super) fn stats_date(observed_at: DateTime<Utc>) -> String {
    wb_automation_business_date(observed_at).to_string()
}

pub(super) fn feedback_observation_time(current_time: DateTime<Utc>) -> DateTime<Utc> {
    // The next hour's midpoint leaves all observations on the same business
    // date. It is 30–90 minutes ahead of the real PostgreSQL write clock, so
    // a two-hour feedback window still contains the last +302-second read.
    current_time
        .date_naive()
        .and_hms_opt(current_time.hour(), 30, 0)
        .expect("the current hour is valid")
        .and_utc()
        + ChronoDuration::hours(1)
}

#[test]
fn feedback_clock_stays_inside_one_day_and_the_configured_feedback_window() {
    for (hour, minute, second) in [(20, 0, 0), (20, 59, 59), (23, 59, 59), (0, 0, 0)] {
        let current_time = Utc
            .with_ymd_and_hms(2026, 8, 25, hour, minute, second)
            .unwrap();
        let observed_at = feedback_observation_time(current_time);
        let final_readback = observed_at + ChronoDuration::seconds(302);
        let advance = observed_at - current_time;
        assert!((ChronoDuration::minutes(30)..=ChronoDuration::minutes(90)).contains(&advance));
        assert_eq!(stats_date(observed_at), stats_date(final_readback));
        assert!(final_readback - current_time < ChronoDuration::hours(2));
        // Even at 00:30 Moscow, the fixture's 200 minor units of spend are
        // below 80% of the production-paced 25,000-minor-unit daily budget.
        let expected_spend = super::super::feedback::paced_value(25_000, observed_at);
        assert!(200 * 10_000 < expected_spend * 8_000);
    }
}

pub(super) fn observation_time() -> DateTime<Utc> {
    // The quota override scenario needs stable traffic pacing at 15:00 Moscow
    // and an instant after PostgreSQL's real write_started_at clock.
    Utc::now()
        .date_naive()
        .succ_opt()
        .expect("test date has a successor")
        .and_hms_opt(12, 0, 0)
        .expect("test time is valid")
        .and_utc()
}

pub(super) fn fixture(campaign_id: u64, observed_at: DateTime<Utc>) -> Fixture {
    let fixture = Fixture::new_for_campaign(campaign_id);
    let mut policy =
        serde_json::from_slice::<WbAutomationPolicy>(&fs::read(&fixture.policy).unwrap()).unwrap();
    policy.authorized_at = observed_at - ChronoDuration::hours(2);
    policy.observe_until = observed_at - ChronoDuration::hours(1);
    // One scenario explicitly observes the following day before resuming.
    policy.authorization_expires_at = observed_at + ChronoDuration::days(2);
    fs::write(&fixture.policy, serde_json::to_vec_pretty(&policy).unwrap()).unwrap();
    fixture
}

#[tokio::test]
async fn midnight_readback_rejects_stale_dates_then_reconciles_current_scope() {
    let reserved_at = Utc.with_ymd_and_hms(2026, 8, 25, 20, 59, 30).unwrap();
    let reconciled_at = reserved_at + ChronoDuration::minutes(1);
    let reservation_date = wb_automation_business_date(reserved_at);
    let readback_date = wb_automation_business_date(reconciled_at);
    assert_eq!(reservation_date.succ_opt().unwrap(), readback_date);

    let fixture = Fixture::new();
    let (reader_url, _) = reader_server(9, 102, &reservation_date.to_string(), Some(0), 10);
    let (writer_url, writer_requests) = mock_http(vec![(200, "{}".to_owned())]);
    let mut executor = fixture.executor(&reader_url, &writer_url);
    let first = executor.run_once(reserved_at).await.unwrap();
    assert_eq!(
        first.outcome,
        WbAutomationExecutionOutcome::WriteSentReconciliationRequired
    );
    assert!(
        writer_requests
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .starts_with("PATCH /api/advert/v1/bids")
    );
    let state_path = fixture.root.join("execution-state.json");
    let pending_state = fs::read(&state_path).unwrap();
    assert!(
        read_state_file(&state_path)
            .unwrap()
            .unwrap()
            .pending
            .is_some()
    );

    let (stale_url, _) = reader_server(9, 117, &reservation_date.to_string(), None, 10);
    executor
        .observer
        .replace_client_for_test(test_reader(&stale_url));
    let error = executor.run_once(reconciled_at).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "WB automation stats вышли за campaign/date/SKU scope"
    );
    assert_eq!(fs::read(&state_path).unwrap(), pending_state);
    assert!(writer_requests.try_recv().is_err());

    let (current_url, _) = reader_server(9, 117, &readback_date.to_string(), None, 10);
    executor
        .observer
        .replace_client_for_test(test_reader(&current_url));
    let reconciled = executor.run_once(reconciled_at).await.unwrap();
    assert_eq!(reconciled.outcome, WbAutomationExecutionOutcome::Reconciled);
    assert_eq!(reconciled.observed_at, reconciled_at);
    let state = read_state_file(&state_path).unwrap().unwrap();
    assert_eq!(state.business_date, readback_date);
    assert!(state.pending.is_none());
    assert!(writer_requests.try_recv().is_err());
}

/// A pause reserved just before the Moscow business-date rollover is
/// reconciled by the next run, which already sees the following business
/// date. Recording the reconciliation date rather than the reservation date
/// would make `paused_by_automation` (`paused_on < business_date`) false for
/// the whole of the new day, so the campaign would stay paused an extra day.
#[test]
fn daily_pause_reconciled_after_rollover_records_the_reservation_date() {
    // 20:50 UTC is 23:50 in Moscow: still business date 2026-08-25.
    let reserved_at = Utc.with_ymd_and_hms(2026, 8, 25, 20, 50, 0).unwrap();
    // 21:30 UTC is 00:30 the next day: business date 2026-08-26.
    let reconciled_at = Utc.with_ymd_and_hms(2026, 8, 25, 21, 30, 0).unwrap();
    let reserved_date = wb_automation_business_date(reserved_at);
    let reconciled_date = wb_automation_business_date(reconciled_at);
    assert_eq!(reserved_date.succ_opt().unwrap(), reconciled_date);

    let pause = PendingAction {
        reserved_at,
        kind: PendingActionKind::PauseCampaignForDailyCap,
    };
    let mut paused = state(pause.clone());
    // `run_once` rolls the stored business date forward before reconciling.
    paused.business_date = reconciled_date;
    let mut readback = observation(11, 115);
    readback.observed_at = reconciled_at;
    assert_eq!(
        reconcile_pending(&readback, &mut paused, &pause),
        WbAutomationExecutionOutcome::Reconciled
    );

    assert_eq!(
        paused.paused_for_daily_cap_on,
        Some(reserved_date),
        "the pause belongs to the business date it was reserved on"
    );
    assert!(
        paused
            .paused_for_daily_cap_on
            .is_some_and(|paused_on| paused_on < reconciled_date),
        "the campaign must be resumable on the new business date, not the day after"
    );
}
