use super::*;

#[test]
fn production_filter_keeps_extracted_guard_library_events_visible() {
    let subscriber = tracing_subscriber::registry().with(tracing_subscriber::EnvFilter::new(
        "mcp_ozon::control::ozon=info",
    ));
    tracing::subscriber::with_default(subscriber, || {
        assert!(tracing::enabled!(
            target: "mcp_ozon::control::ozon::guard_workflow",
            tracing::Level::INFO
        ));
        assert!(tracing::enabled!(
            target: "mcp_ozon::control::ozon::guard_runtime",
            tracing::Level::ERROR
        ));
        assert!(!tracing::enabled!(
            target: "mcp_ozon::control::ozon::guard_runtime",
            tracing::Level::DEBUG
        ));
        assert!(!tracing::enabled!(
            target: "ozon_campaign_guard",
            tracing::Level::INFO
        ));
    });
}

#[tokio::test]
async fn successful_static_write_with_unavailable_readback_locks_without_retry() {
    let directory = TestDirectory::new();
    let state_path = directory.0.join("state.json");
    let mut state = StaticGuardState::default();
    let io = FakeStaticStopIo::default();
    io.writes.lock().unwrap().push_back(Ok(()));
    io.readbacks
        .lock()
        .unwrap()
        .push_back(Err("readback unavailable".to_owned()));
    let clock = RecordingClock::default();
    let static_guard = test_static_guard(10, 10_000_000);

    let error = guard_campaign_static_with_io(
        &mut state,
        &state_path,
        &io,
        &clock,
        &StaticFailpoints::default(),
        &static_guard,
        Some(2_000_000_000),
        Some(0),
        Some("spend_cap_reached"),
        Duration::from_secs(2),
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("readback unavailable"));
    assert_eq!(io.write_calls.load(Ordering::Relaxed), 1);
    assert_eq!(io.read_calls.load(Ordering::Relaxed), 1);
    assert!(
        state
            .incident_campaign_ids
            .contains(&static_guard.guard.campaign_id)
    );
    let incident = state
        .incidents
        .get(&static_guard.guard.campaign_id)
        .unwrap();
    assert_eq!(incident.stop_reason.as_deref(), Some("spend_cap_reached"));
    assert_eq!(incident.spend_minor, Some(2_000_000_000));
    assert_eq!(incident.revenue_minor, Some(0));
    assert_eq!(load_static_state(&state_path).unwrap(), state);
    assert_eq!(
        clock.0.lock().unwrap().as_slice(),
        &[Duration::from_secs(2); 2]
    );

    io.readbacks
        .lock()
        .unwrap()
        .push_back(Err("still unavailable".to_owned()));
    recover_pending_static_campaign_mutations_with_io(
        &mut state,
        &state_path,
        &io,
        std::slice::from_ref(&static_guard),
    )
    .await
    .unwrap();
    assert_eq!(io.write_calls.load(Ordering::Relaxed), 1);
    assert_eq!(io.read_calls.load(Ordering::Relaxed), 2);
    assert!(
        state
            .pending_campaign_mutations
            .contains_key(&static_guard.guard.campaign_id)
    );
}

#[tokio::test]
async fn static_stop_accepts_one_stopped_readback_after_write_error() {
    let directory = TestDirectory::new();
    let state_path = directory.0.join("state.json");
    let mut state = StaticGuardState::default();
    let io = FakeStaticStopIo::default();
    io.writes
        .lock()
        .unwrap()
        .push_back(Err("ambiguous write".to_owned()));
    io.readbacks.lock().unwrap().push_back(Ok(false));
    let static_guard = test_static_guard(11, 10_000_000);

    guard_campaign_static_with_io(
        &mut state,
        &state_path,
        &io,
        &RecordingClock::default(),
        &StaticFailpoints::default(),
        &static_guard,
        None,
        None,
        Some("telemetry_unavailable"),
        Duration::ZERO,
    )
    .await
    .unwrap();

    assert_eq!(io.write_calls.load(Ordering::Relaxed), 1);
    assert_eq!(io.read_calls.load(Ordering::Relaxed), 1);
    assert!(state.incident_campaign_ids.is_empty());
    assert!(state.pending_campaign_mutations.is_empty());
}

#[tokio::test]
async fn static_stop_crash_before_marker_neither_persists_intent_nor_writes() {
    let directory = TestDirectory::new();
    let state_path = directory.0.join("state.json");
    let mut state = StaticGuardState::default();
    let io = FakeStaticStopIo::default();
    let static_guard = test_static_guard(12, 10_000_000);
    let failpoints = StaticFailpoints(BTreeSet::from([OzonStaticMutationFailpoint::BeforeMarker]));

    guard_campaign_static_with_io(
        &mut state,
        &state_path,
        &io,
        &RecordingClock::default(),
        &failpoints,
        &static_guard,
        Some(100),
        Some(10),
        Some("spend_cap_reached"),
        Duration::ZERO,
    )
    .await
    .unwrap_err();

    assert!(state.pending_campaign_mutations.is_empty());
    assert_eq!(state.last_static_audit_event_id, None);
    assert_eq!(io.write_calls.load(Ordering::Relaxed), 0);
    assert_eq!(io.read_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn static_pre_marker_client_failure_remains_retryable_without_an_incident() {
    let directory = TestDirectory::new();
    let state_path = directory.0.join("state.json");
    let mut state = StaticGuardState::default();
    let io = FakeStaticStopIo::default();
    io.pre_marker_failures
        .lock()
        .unwrap()
        .push_back("token unavailable".to_owned());
    let static_guard = test_static_guard(112, 10_000_000);

    let error = guard_campaign_static_with_io(
        &mut state,
        &state_path,
        &io,
        &RecordingClock::default(),
        &StaticFailpoints::default(),
        &static_guard,
        None,
        None,
        Some("telemetry_unavailable"),
        Duration::ZERO,
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("before durable marker"));
    assert!(state.pending_campaign_mutations.is_empty());
    assert!(state.incident_campaign_ids.is_empty());
    assert_eq!(io.write_calls.load(Ordering::Relaxed), 0);
    assert_eq!(io.read_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn static_stop_crash_after_marker_recovers_readback_only() {
    let directory = TestDirectory::new();
    let state_path = directory.0.join("state.json");
    let mut state = StaticGuardState::default();
    let io = FakeStaticStopIo::default();
    let static_guard = test_static_guard(13, 10_000_000);
    let failpoints = StaticFailpoints(BTreeSet::from([OzonStaticMutationFailpoint::AfterMarker]));

    guard_campaign_static_with_io(
        &mut state,
        &state_path,
        &io,
        &RecordingClock::default(),
        &failpoints,
        &static_guard,
        Some(100),
        Some(10),
        Some("spend_cap_reached"),
        Duration::ZERO,
    )
    .await
    .unwrap_err();
    assert_eq!(io.write_calls.load(Ordering::Relaxed), 0);
    assert!(
        state
            .pending_campaign_mutations
            .contains_key(&static_guard.guard.campaign_id)
    );
    assert_eq!(state.last_static_audit_event_id, Some(1));
    assert_eq!(load_static_state(&state_path).unwrap(), state);

    io.readbacks.lock().unwrap().push_back(Ok(true));
    recover_pending_static_campaign_mutations_with_io(
        &mut state,
        &state_path,
        &io,
        std::slice::from_ref(&static_guard),
    )
    .await
    .unwrap();

    assert_eq!(io.write_calls.load(Ordering::Relaxed), 0);
    assert!(
        state
            .incident_campaign_ids
            .contains(&static_guard.guard.campaign_id)
    );
    assert!(
        state
            .pending_campaign_mutations
            .contains_key(&static_guard.guard.campaign_id)
    );
}

#[tokio::test]
async fn static_stop_crash_after_post_finishes_from_readback_without_second_write() {
    let directory = TestDirectory::new();
    let state_path = directory.0.join("state.json");
    let mut state = StaticGuardState::default();
    let io = FakeStaticStopIo::default();
    io.writes.lock().unwrap().push_back(Ok(()));
    let static_guard = test_static_guard(14, 10_000_000);
    let failpoints = StaticFailpoints(BTreeSet::from([OzonStaticMutationFailpoint::AfterWrite]));

    guard_campaign_static_with_io(
        &mut state,
        &state_path,
        &io,
        &RecordingClock::default(),
        &failpoints,
        &static_guard,
        None,
        None,
        Some("telemetry_unavailable"),
        Duration::ZERO,
    )
    .await
    .unwrap_err();
    assert_eq!(io.write_calls.load(Ordering::Relaxed), 1);

    io.readbacks.lock().unwrap().push_back(Ok(false));
    recover_pending_static_campaign_mutations_with_io(
        &mut state,
        &state_path,
        &io,
        std::slice::from_ref(&static_guard),
    )
    .await
    .unwrap();

    assert_eq!(io.write_calls.load(Ordering::Relaxed), 1);
    assert!(state.pending_campaign_mutations.is_empty());
    assert!(state.incident_campaign_ids.is_empty());
}

#[tokio::test]
async fn static_activation_crash_after_post_recovers_without_second_write() {
    let directory = TestDirectory::new();
    let state_path = directory.0.join("state.json");
    let mut state = StaticGuardState::default();
    let io = FakeStaticStopIo::default();
    io.activations.lock().unwrap().push_back(Ok(()));
    let static_guard = test_static_guard(15, 10_000_000);
    let failpoints = StaticFailpoints(BTreeSet::from([OzonStaticMutationFailpoint::AfterWrite]));

    activate_static_campaign_with_io(
        &mut state,
        &state_path,
        &io,
        &RecordingClock::default(),
        &failpoints,
        &static_guard,
        Duration::ZERO,
    )
    .await
    .unwrap_err();
    assert_eq!(io.activation_calls.load(Ordering::Relaxed), 1);

    io.readbacks.lock().unwrap().push_back(Ok(true));
    recover_pending_static_campaign_mutations_with_io(
        &mut state,
        &state_path,
        &io,
        std::slice::from_ref(&static_guard),
    )
    .await
    .unwrap();

    assert_eq!(io.activation_calls.load(Ordering::Relaxed), 1);
    assert!(state.pending_campaign_mutations.is_empty());
    assert!(state.incident_campaign_ids.is_empty());
}

#[tokio::test]
async fn static_activation_readback_outage_keeps_marker_and_structured_lock() {
    let directory = TestDirectory::new();
    let state_path = directory.0.join("state.json");
    let mut state = StaticGuardState::default();
    let io = FakeStaticStopIo::default();
    io.activations.lock().unwrap().push_back(Ok(()));
    io.readbacks
        .lock()
        .unwrap()
        .push_back(Err("activation readback unavailable".to_owned()));
    let static_guard = test_static_guard(16, 10_000_000);

    activate_static_campaign_with_io(
        &mut state,
        &state_path,
        &io,
        &RecordingClock::default(),
        &StaticFailpoints::default(),
        &static_guard,
        Duration::ZERO,
    )
    .await
    .unwrap_err();

    assert_eq!(io.activation_calls.load(Ordering::Relaxed), 1);
    assert!(
        state
            .pending_campaign_mutations
            .contains_key(&static_guard.guard.campaign_id)
    );
    let incident = state
        .incidents
        .get(&static_guard.guard.campaign_id)
        .unwrap();
    assert_eq!(incident.error_class, "activate_readback_unavailable");
    assert_eq!(incident.stop_reason, None);

    io.readbacks
        .lock()
        .unwrap()
        .push_back(Err("still unavailable".to_owned()));
    recover_pending_static_campaign_mutations_with_io(
        &mut state,
        &state_path,
        &io,
        std::slice::from_ref(&static_guard),
    )
    .await
    .unwrap();
    assert_eq!(io.activation_calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        state
            .incidents
            .get(&static_guard.guard.campaign_id)
            .unwrap()
            .error_class,
        "activate_readback_unavailable"
    );
}

#[test]
fn pending_static_bid_is_resolved_only_by_exact_readback() {
    let directory = TestDirectory::new();
    let state_path = directory.0.join("state.json");
    let started_at = DateTime::UNIX_EPOCH;
    let first_guard = test_static_guard(1, 10_000_000);
    let mut state = StaticGuardState {
        pending_bid_changes: BTreeMap::from([(1, test_pending_bid(&first_guard, started_at))]),
        ..StaticGuardState::default()
    };
    reconcile_pending_static_bid(&mut state, &state_path, &first_guard, 8_000_000).unwrap();
    assert!(state.pending_bid_changes.is_empty());
    assert_eq!(state.last_bid_change_at.get(&1), Some(&started_at));
    assert_eq!(load_static_state(&state_path).unwrap(), state);

    let second_guard = test_static_guard(2, 10_000_000);
    state
        .pending_bid_changes
        .insert(2, test_pending_bid(&second_guard, started_at));
    reconcile_pending_static_bid(&mut state, &state_path, &second_guard, 7_000_000).unwrap();
    assert!(state.incident_campaign_ids.contains(&2));
    assert!(state.pending_bid_changes.contains_key(&2));
    assert_eq!(load_static_state(&state_path).unwrap(), state);
}

#[test]
fn pending_static_bid_is_locked_when_reviewed_config_changes() {
    let directory = TestDirectory::new();
    let state_path = directory.0.join("state.json");
    let original_guard = test_static_guard(3, 10_000_000);
    let changed_guard = test_static_guard(3, 9_000_000);
    let mut state = StaticGuardState {
        pending_bid_changes: BTreeMap::from([(
            3,
            test_pending_bid(&original_guard, DateTime::UNIX_EPOCH),
        )]),
        ..StaticGuardState::default()
    };

    reconcile_pending_static_bid(&mut state, &state_path, &changed_guard, 8_000_000).unwrap();

    assert!(state.incident_campaign_ids.contains(&3));
    assert!(state.pending_bid_changes.contains_key(&3));
    assert_eq!(load_static_state(&state_path).unwrap(), state);

    let legacy_guard = test_static_guard(4, 10_000_000);
    state.pending_bid_changes.insert(
        4,
        PendingStaticBidChange {
            account_id: None,
            sku: None,
            min_cpc_bid_microrubles: None,
            max_cpc_bid_microrubles: None,
            date_from: None,
            spend_cap_microrubles: None,
            target_drr_percent: None,
            from_microrubles: 7_000_000,
            to_microrubles: 8_000_000,
            started_at: DateTime::UNIX_EPOCH,
        },
    );
    reconcile_pending_static_bid(&mut state, &state_path, &legacy_guard, 8_000_000).unwrap();
    assert!(state.incident_campaign_ids.contains(&4));
    assert!(state.pending_bid_changes.contains_key(&4));
}

#[test]
fn pending_static_mutations_bind_every_reviewed_guard_field() {
    let original = test_static_guard(5, 10_000_000);
    let pending_bid = test_pending_bid(&original, DateTime::UNIX_EPOCH);
    let pending_campaign = PendingStaticCampaignMutation {
        account_id: original.guard.account_id.clone(),
        sku: original.guard.sku,
        min_cpc_bid_microrubles: original.min_cpc_bid_microrubles,
        max_cpc_bid_microrubles: original.max_cpc_bid_microrubles,
        date_from: original.guard.date_from.clone(),
        spend_cap_microrubles: original.guard.spend_cap_microrubles,
        target_drr_percent: original.guard.target_drr_percent,
        kind: OzonStaticCampaignMutationKind::Deactivate,
        stop_reason: Some("spend_cap_reached".to_owned()),
        spend_minor: Some(100),
        revenue_minor: Some(10),
        started_at: DateTime::UNIX_EPOCH,
    };
    let mut changed_guards = Vec::new();
    let mut changed = original.clone();
    changed.guard.account_id = "other-account".to_owned();
    changed_guards.push(changed);
    let mut changed = original.clone();
    changed.guard.sku += 1;
    changed_guards.push(changed);
    let mut changed = original.clone();
    changed.guard.date_from = "2026-09-02".to_owned();
    changed_guards.push(changed);
    let mut changed = original.clone();
    changed.guard.spend_cap_microrubles += 10_000;
    changed_guards.push(changed);
    let mut changed = original.clone();
    changed.guard.target_drr_percent += 1;
    changed_guards.push(changed);
    let mut changed = original.clone();
    changed.min_cpc_bid_microrubles += 1_000_000;
    changed_guards.push(changed);
    let mut changed = original.clone();
    changed.max_cpc_bid_microrubles -= 1_000_000;
    changed_guards.push(changed);

    for changed in changed_guards {
        assert!(!pending_matches_static_guard(&pending_bid, &changed));
        assert!(!pending_campaign_mutation_matches_guard(
            &pending_campaign,
            &changed
        ));
    }
    assert!(pending_matches_static_guard(&pending_bid, &original));
    assert!(pending_campaign_mutation_matches_guard(
        &pending_campaign,
        &original
    ));
}

#[test]
fn explicit_reconcile_clears_only_the_proven_campaign_lock_and_intent() {
    let directory = TestDirectory::new();
    let state_path = directory.0.join("state.json");
    let first_guard = test_static_guard(20, 10_000_000);
    let second_guard = test_static_guard(21, 10_000_000);
    let mut state = StaticGuardState {
        incident_campaign_ids: BTreeSet::from([20, 21]),
        incidents: BTreeMap::from([
            (20, test_incident(&first_guard)),
            (21, test_incident(&second_guard)),
        ]),
        pending_bid_changes: BTreeMap::from([
            (20, test_pending_bid(&first_guard, DateTime::UNIX_EPOCH)),
            (21, test_pending_bid(&second_guard, DateTime::UNIX_EPOCH)),
        ]),
        ..StaticGuardState::default()
    };
    persist_static_state(&state_path, &state).unwrap();

    clear_reconciled_static_campaign_state(&mut state, &state_path, 20).unwrap();

    assert!(!state.incident_campaign_ids.contains(&20));
    assert!(!state.incidents.contains_key(&20));
    assert!(!state.pending_bid_changes.contains_key(&20));
    assert!(state.incident_campaign_ids.contains(&21));
    assert!(state.incidents.contains_key(&21));
    assert!(state.pending_bid_changes.contains_key(&21));
    assert_eq!(load_static_state(&state_path).unwrap(), state);
}
