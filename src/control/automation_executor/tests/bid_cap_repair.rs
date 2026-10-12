use super::*;

fn bid_cap_repair_snapshot() -> (WbAutomationPolicy, WbAutomationSnapshot) {
    let (mut policy, snapshot) = explicit_exposure_snapshot();
    policy.target_orders_per_day = 0;
    policy.traffic_frontier_min_feedback_impressions = None;
    policy.traffic_frontier_min_feedback_clicks = None;
    policy.authorized_at = snapshot.observation.observed_at - ChronoDuration::hours(2);
    policy.observe_until = snapshot.observation.observed_at - ChronoDuration::hours(1);
    policy.authorization_expires_at = snapshot.observation.observed_at + ChronoDuration::days(2);
    (policy, snapshot)
}

#[test]
fn bid_cap_repair_only_lowers_excess_and_preserves_ordinary_cooldown() {
    let (policy, mut snapshot) = bid_cap_repair_snapshot();
    snapshot.observation.last_action_at = Some(snapshot.observation.observed_at);
    snapshot.observation.skus[0].current_bid_kopecks = policy.max_bid_kopecks + 73;
    snapshot.decision =
        super::super::super::automation::evaluate_wb_automation(&policy, &snapshot.observation)
            .unwrap();
    assert!(matches!(
        snapshot.decision.action,
        WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::CooldownActive
        }
    ));
    let result = explicit_bid_cap_repair_decision(&policy, &snapshot).unwrap();
    assert!(
        matches!(result.action, WbAutomationAction::ChangeBids { ref changes }
        if changes.len() == 1
            && changes[0].from_bid_kopecks == policy.max_bid_kopecks + 73
            && changes[0].to_bid_kopecks == policy.max_bid_kopecks
            && changes[0].reason == WbAutomationBidReason::PolicyMaximumExceeded)
    );

    snapshot.observation.skus[0].current_bid_kopecks = policy.max_bid_kopecks;
    assert!(matches!(
        explicit_bid_cap_repair_decision(&policy, &snapshot)
            .unwrap()
            .action,
        WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::NoMaterialChange
        }
    ));
}

#[test]
fn bid_cap_repair_requires_pause_for_overspend_and_preserves_other_guards() {
    let (policy, mut snapshot) = bid_cap_repair_snapshot();
    snapshot.observation.skus[0].current_bid_kopecks = policy.max_bid_kopecks + 73;
    snapshot.observation.daily_spend_minor = policy.daily_spend_cap_minor + 1;
    assert!(explicit_bid_cap_repair_decision(&policy, &snapshot).is_err());
    snapshot.observation.campaign_status = 11;
    snapshot.observation.budget_remaining_minor = 0;
    assert!(matches!(
        explicit_bid_cap_repair_decision(&policy, &snapshot)
            .unwrap()
            .action,
        WbAutomationAction::ChangeBids { .. }
    ));

    let mut invalid = snapshot.clone();
    invalid.observation.daily_spend_complete = false;
    assert!(explicit_bid_cap_repair_decision(&policy, &invalid).is_err());
    invalid = snapshot.clone();
    invalid.observation.actions_today = policy.max_actions_per_day;
    assert!(explicit_bid_cap_repair_decision(&policy, &invalid).is_err());
    invalid = snapshot.clone();
    invalid.observation.observed_at = policy.authorization_expires_at;
    assert!(explicit_bid_cap_repair_decision(&policy, &invalid).is_err());
    invalid = snapshot.clone();
    invalid.observation.campaign_status = 7;
    assert!(explicit_bid_cap_repair_decision(&policy, &invalid).is_err());
    let mut shadow = policy;
    shadow.write_enabled = false;
    assert!(explicit_bid_cap_repair_decision(&shadow, &snapshot).is_err());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "campaign leases are explicitly released"
)]
#[ignore = "requires disposable WB automation PostgreSQL role"]
async fn postgres_bid_cap_repair_is_journaled_on_paused_overspent_campaign() {
    let database_url = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL").unwrap();
    let _serial = POSTGRES_EXECUTOR_TEST_LOCK.lock().await;
    let observed_at = Utc::now();
    let campaign = 49_682_633;
    let fixture = postgres_clock::fixture(campaign, observed_at);
    let date = postgres_clock::stats_date(observed_at);
    let (reader_url, _) = reader_server_for(campaign, 11, 700, &date, Some(400), 10);
    let (writer_url, requests) = mock_http(vec![(200, "{}".to_owned())]);
    let mut executor = fixture.executor(&reader_url, &writer_url);
    let store = WbAutomationPostgresStore::connect(&Config::from_str(&database_url).unwrap())
        .await
        .unwrap();
    store.verify_runtime_contract().await.unwrap();
    let business_date = wb_automation_business_date(observed_at);
    let legacy = WbAutomationLegacyStateSeed {
        policy_digest: executor.policy_sha256().to_owned(),
        business_date,
        actions_today: 0,
        last_action_at: Some(observed_at - ChronoDuration::seconds(10)),
        paused_for_daily_cap_on: Some(business_date),
        incident_class: None,
        legacy_digest: "e".repeat(64),
    };
    let first = executor
        .run_explicit_bid_cap_repair_once_postgres(&store, &legacy, observed_at)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        first.outcome,
        WbAutomationExecutionOutcome::WriteSentReconciliationRequired
    );
    let request = requests.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(request.starts_with("PATCH /api/advert/v1/bids"));
    assert!(request.contains("\"bid_kopecks\":500"));
    assert!(request.contains("\"placement\":\"search\""));
    assert!(!request.contains("recommendations"));
    assert!(
        executor
            .run_explicit_bid_cap_repair_once_postgres(
                &store,
                &legacy,
                observed_at + ChronoDuration::seconds(1)
            )
            .await
            .is_err()
    );
    assert!(requests.try_recv().is_err());

    let (readback_url, _) = reader_server_for(campaign, 11, 500, &date, Some(400), 10);
    executor
        .observer
        .replace_client_for_test(test_reader(&readback_url));
    let readback = executor
        .run_once_postgres(&store, &legacy, observed_at + ChronoDuration::seconds(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(readback.outcome, WbAutomationExecutionOutcome::Reconciled);
    let lease = store
        .try_acquire_campaign("ip_domnyshev_wb", campaign)
        .await
        .unwrap()
        .unwrap();
    let state = lease.load_state().await.unwrap().unwrap();
    assert_eq!(state.paused_for_daily_cap_on, Some(business_date));
    assert_eq!(state.actions_today, 1);
    assert!(state.pending_idempotency_key.is_none());
    assert!(state.incident_class.is_none());
    lease.release().await.unwrap();
}
