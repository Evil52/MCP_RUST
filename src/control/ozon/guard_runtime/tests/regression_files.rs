use super::*;

#[test]
fn static_guard_config_loader_rejects_oversized_and_symlink_inputs() {
    let directory = TestDirectory::new();
    let oversized_path = directory.0.join("oversized.json");
    fs::write(
        &oversized_path,
        vec![b' '; MAX_OZON_STATIC_GUARD_FILE_BYTES + 1],
    )
    .unwrap();
    assert!(
        load_static_guards(&oversized_path, "account")
            .unwrap_err()
            .to_string()
            .contains("byte limit")
    );

    let target_path = directory.0.join("target.json");
    fs::write(&target_path, b"{}").unwrap();
    let symlink_path = directory.0.join("guards.json");
    symlink(&target_path, &symlink_path).unwrap();
    assert!(
        load_static_guards(&symlink_path, "account")
            .unwrap_err()
            .to_string()
            .contains("non-symlink")
    );
}

#[test]
fn static_audit_watermark_rejects_missing_rolled_back_or_replayed_state() {
    let mut state = StaticGuardState::default();
    assert!(validate_static_audit_continuity(&state, None).is_err());
    assert!(validate_static_audit_continuity(&state, Some(1)).is_err());

    assert_eq!(advance_static_audit_watermark(&mut state, 7), Ok(None));
    assert!(validate_static_audit_continuity(&state, Some(7)).is_ok());
    assert!(validate_static_audit_continuity(&state, Some(8)).is_err());
    assert!(validate_static_audit_continuity(&state, None).is_err());
    assert!(advance_static_audit_watermark(&mut state, 7).is_err());
    assert!(advance_static_audit_watermark(&mut state, 6).is_err());
    assert_eq!(advance_static_audit_watermark(&mut state, 9), Ok(Some(7)));
}

#[test]
fn cursor_mismatch_allows_only_read_only_audit_and_never_adopts_state() {
    let state = StaticGuardState {
        last_static_audit_event_id: Some(7),
        ..StaticGuardState::default()
    };
    let original = state.clone();

    assert_eq!(
        validate_static_command_audit_continuity(Command::AuditStaticOnce, &state, Some(8))
            .unwrap(),
        StaticAuditContinuity::ReadOnlyAudit
    );
    for command in [
        Command::Serve,
        Command::InitializeStaticState,
        Command::ReconcileStaticOnce,
        Command::Healthcheck,
    ] {
        assert!(validate_static_command_audit_continuity(command, &state, Some(8)).is_err());
    }
    assert_eq!(state, original);
}

#[test]
fn static_state_requires_explicit_genesis_before_serve_or_health() {
    let state = StaticGuardState::default();
    assert_eq!(
        validate_static_command_audit_continuity(Command::InitializeStaticState, &state, None,)
            .unwrap(),
        StaticAuditContinuity::InitializeState
    );
    assert_eq!(
        validate_static_command_audit_continuity(Command::AuditStaticOnce, &state, None).unwrap(),
        StaticAuditContinuity::ReadOnlyAudit
    );
    for command in [
        Command::Serve,
        Command::ReconcileStaticOnce,
        Command::Healthcheck,
    ] {
        assert!(validate_static_command_audit_continuity(command, &state, None).is_err());
    }

    let initialized = StaticGuardState {
        last_static_audit_event_id: Some(1),
        ..StaticGuardState::default()
    };
    assert_eq!(
        validate_static_command_audit_continuity(Command::Serve, &initialized, Some(1)).unwrap(),
        StaticAuditContinuity::Matched
    );
    assert!(
        validate_static_command_audit_continuity(
            Command::InitializeStaticState,
            &initialized,
            Some(1),
        )
        .is_err()
    );
}

#[test]
fn static_state_initialization_requires_exact_confirmation_and_preserves_legacy_evidence() {
    let command = vec![
        INITIALIZE_STATIC_STATE_COMMAND.to_owned(),
        INITIALIZE_STATIC_STATE_CONFIRMATION.to_owned(),
    ];
    assert_eq!(
        parse_command(&command).unwrap(),
        Command::InitializeStaticState
    );
    assert!(parse_command(&[INITIALIZE_STATIC_STATE_COMMAND.to_owned()]).is_err());

    let directory = TestDirectory::new();
    let state_path = directory.0.join("state.json");
    let incident_guard = test_static_guard(31, 10_000_000);
    let pending_guard = test_static_guard(32, 10_000_000);
    let mut state = StaticGuardState::default();
    state
        .incident_campaign_ids
        .insert(incident_guard.guard.campaign_id);
    state.incidents.insert(
        incident_guard.guard.campaign_id,
        test_incident(&incident_guard),
    );
    state.pending_bid_changes.insert(
        pending_guard.guard.campaign_id,
        test_pending_bid(&pending_guard, DateTime::UNIX_EPOCH),
    );
    state.last_bid_change_at.insert(33, DateTime::UNIX_EPOCH);
    let mut expected = state.clone();
    expected.last_static_audit_event_id = Some(17);

    persist_static_initialization_cursor(&mut state, &state_path, 17).unwrap();

    assert_eq!(state, expected);
    assert_eq!(load_static_state(&state_path).unwrap(), expected);
}

#[test]
fn failed_static_state_initialization_persistence_restores_the_local_cursor() {
    let directory = TestDirectory::new();
    let invalid_state_path = directory.0.join("state.json");
    fs::create_dir(&invalid_state_path).unwrap();
    let mut state = StaticGuardState::default();

    assert!(persist_static_initialization_cursor(&mut state, &invalid_state_path, 17).is_err());
    assert_eq!(state.last_static_audit_event_id, None);
}

#[test]
fn static_health_rejects_incidents_and_stale_pending_but_accepts_fresh_inflight() {
    let now = DateTime::UNIX_EPOCH + chrono::Duration::minutes(10);
    let guard = test_static_guard(88, 10_000_000);
    let mut state = StaticGuardState::default();
    assert!(validate_static_state_health(&state, now).is_ok());

    state.pending_bid_changes.insert(
        guard.guard.campaign_id,
        test_pending_bid(&guard, now - STATIC_PENDING_HEALTH_GRACE),
    );
    assert!(validate_static_state_health(&state, now).is_ok());
    state
        .pending_bid_changes
        .get_mut(&guard.guard.campaign_id)
        .unwrap()
        .started_at = now - STATIC_PENDING_HEALTH_GRACE - chrono::Duration::seconds(1);
    assert!(validate_static_state_health(&state, now).is_err());

    state.pending_bid_changes.clear();
    state.pending_campaign_mutations.insert(
        guard.guard.campaign_id,
        PendingStaticCampaignMutation {
            account_id: guard.guard.account_id.clone(),
            sku: guard.guard.sku,
            min_cpc_bid_microrubles: guard.min_cpc_bid_microrubles,
            max_cpc_bid_microrubles: guard.max_cpc_bid_microrubles,
            date_from: guard.guard.date_from.clone(),
            spend_cap_microrubles: guard.guard.spend_cap_microrubles,
            target_drr_percent: guard.guard.target_drr_percent,
            kind: OzonStaticCampaignMutationKind::Deactivate,
            stop_reason: Some("spend_cap_reached".to_owned()),
            spend_minor: Some(1),
            revenue_minor: Some(0),
            started_at: now + chrono::Duration::seconds(1),
        },
    );
    assert!(validate_static_state_health(&state, now).is_err());

    state.pending_campaign_mutations.clear();
    state.incident_campaign_ids.insert(guard.guard.campaign_id);
    state
        .incidents
        .insert(guard.guard.campaign_id, test_incident(&guard));
    assert!(validate_static_state_health(&state, now).is_err());
}

#[test]
fn final_static_permit_rejects_config_swap_and_out_of_corridor_target() {
    let static_guard = test_static_guard(1, 10_000_000);
    let config = OzonStaticGuardConfig {
        guards: vec![static_guard.clone()],
        dynamic_bid_control: None,
    };
    let policy: ControlPolicy = serde_json::from_value(serde_json::json!({
        "version": 1,
        "revision": 1,
        "mode": "enabled",
        "actors": [{
            "actor_id": "operator",
            "ozon_campaign_launch_targets": [{
                "account_id": "account",
                "skus": [101],
                "weekly_budget_microrubles": 2_000_000_000_u64,
                "per_sku_spend_cap_microrubles": 2_000_000_000_u64,
                "initial_cpc_bid_microrubles": 7_000_000_u64,
                "max_cpc_bid_microrubles": 10_000_000_u64,
                "target_drr_percent": 15,
                "target_position": 5,
                "approver_actor_ids": ["approver"]
            }]
        }]
    }))
    .unwrap();

    assert!(
        validate_reloaded_static_guard(
            &config,
            &"a".repeat(64),
            &"a".repeat(64),
            &policy,
            &static_guard,
            OzonStaticGuardMutation::SetBid,
            Some(8_000_000),
        )
        .is_ok()
    );
    assert!(
        validate_reloaded_static_guard(
            &config,
            &"b".repeat(64),
            &"a".repeat(64),
            &policy,
            &static_guard,
            OzonStaticGuardMutation::Deactivate,
            None,
        )
        .unwrap_err()
        .contains("config changed")
    );
    assert!(
        validate_reloaded_static_guard(
            &config,
            &"a".repeat(64),
            &"a".repeat(64),
            &policy,
            &static_guard,
            OzonStaticGuardMutation::SetBid,
            Some(11_000_000),
        )
        .unwrap_err()
        .contains("corridor")
    );
}

#[test]
fn provider_4xx_after_durable_stop_marker_is_ambiguous_until_readback() {
    let error = OzonGuardedWriteError::Write(crate::control::OzonWriteError::Http {
        status: reqwest::StatusCode::BAD_REQUEST,
    });

    assert_eq!(
        classify_guard_stop_write_failure(&error, true),
        OzonGuardWriteFailure::Ambiguous
    );
}

#[tokio::test]
async fn blocked_guard_cycle_does_not_delay_launch_consumer_ticks() {
    let tasks = BlockingGuardTasks::default();
    let runtime = run_independent_workflow_loops(
        &tasks,
        Duration::from_millis(5),
        Duration::from_millis(1),
        tokio::time::sleep(Duration::from_secs(1)),
    );
    tokio::pin!(runtime);

    tokio::time::timeout(Duration::from_millis(250), async {
        loop {
            tokio::select! {
                _ = &mut runtime => panic!("runtime exited before shutdown"),
                () = tokio::task::yield_now() => {}
            }
            if tasks.guard_calls.load(Ordering::Relaxed) == 1
                && tasks.launch_calls.load(Ordering::Relaxed) >= 2
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("independent launch loop was starved by a blocked guard cycle");
}

#[tokio::test]
async fn persistent_workflow_failures_terminate_the_runtime() {
    let tasks = FailingWorkflowTasks::default();
    let result = tokio::time::timeout(
        Duration::from_millis(250),
        run_independent_workflow_loops(
            &tasks,
            Duration::from_millis(1),
            Duration::from_secs(60),
            std::future::pending(),
        ),
    )
    .await
    .expect("persistent workflow failures did not terminate the runtime");

    assert_eq!(result, Err(OzonWorkflowLoopFailure::Launch));
    assert_eq!(
        tasks.launch_calls.load(Ordering::Relaxed),
        MAX_CONSECUTIVE_WORKFLOW_FAILURES
    );
}

#[test]
fn successful_cycle_resets_the_static_and_durable_failure_budget() {
    let mut consecutive_failures = 0;
    assert!(!record_cycle_outcome(&mut consecutive_failures, false));
    assert!(!record_cycle_outcome(&mut consecutive_failures, false));
    assert!(!record_cycle_outcome(&mut consecutive_failures, true));
    assert_eq!(consecutive_failures, 0);
    for failure in 1..MAX_CONSECUTIVE_WORKFLOW_FAILURES {
        assert!(!record_cycle_outcome(&mut consecutive_failures, false));
        assert_eq!(consecutive_failures, failure);
    }
    assert!(record_cycle_outcome(&mut consecutive_failures, false));
}
