use super::{static_adapter_fixture::*, static_safety::GuardLogs, *};
use crate::control::ozon::launch_workflow::tests::adapter_fixture::mock_reader;
use tracing::instrument::WithSubscriber as _;

fn pending_campaign(guard: &OzonStaticCampaignGuard) -> PendingStaticCampaignMutation {
    PendingStaticCampaignMutation {
        account_id: guard.guard.account_id.clone(),
        sku: guard.guard.sku,
        min_cpc_bid_microrubles: guard.min_cpc_bid_microrubles,
        max_cpc_bid_microrubles: guard.max_cpc_bid_microrubles,
        date_from: guard.guard.date_from.clone(),
        spend_cap_microrubles: guard.guard.spend_cap_microrubles,
        target_drr_percent: guard.guard.target_drr_percent,
        kind: OzonStaticCampaignMutationKind::Activate,
        stop_reason: None,
        spend_minor: None,
        revenue_minor: None,
        started_at: observed_at(),
    }
}

fn assert_unsafe_state_file(error: &anyhow::Error) {
    assert!(matches!(
        error.downcast_ref::<crate::control::OzonStaticGuardStateError>(),
        Some(crate::control::OzonStaticGuardStateError::UnsafeFile)
    ));
}

#[tokio::test]
async fn campaign_recovery_propagates_failed_incident_persistence_without_writes() {
    for case in ["config", "mismatch", "unavailable"] {
        let fixture = StaticFixture::new();
        fs::create_dir(&fixture.state_path).unwrap();
        let mut guard = fixture.guard.clone();
        let mut state = StaticGuardState {
            last_static_audit_event_id: Some(1),
            pending_campaign_mutations: BTreeMap::from([(21, pending_campaign(&guard))]),
            ..StaticGuardState::default()
        };
        let io = FakeStaticStopIo::default();
        if case == "config" {
            guard.max_cpc_bid_microrubles -= 1_000_000;
        } else {
            io.readbacks
                .lock()
                .unwrap()
                .push_back(if case == "mismatch" {
                    Ok(false)
                } else {
                    Err("readback unavailable".to_owned())
                });
        }
        let error = recover_pending_static_campaign_mutations_with_io(
            &mut state,
            &fixture.state_path,
            &io,
            std::slice::from_ref(&guard),
        )
        .await
        .unwrap_err();
        assert_unsafe_state_file(&error);
        assert!(state.pending_campaign_mutations.contains_key(&21));
        assert_eq!(state.last_static_audit_event_id, Some(1));
        assert_eq!(io.activation_calls.load(Ordering::Relaxed), 0);
        assert_eq!(io.write_calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            io.read_calls.load(Ordering::Relaxed),
            usize::from(case != "config")
        );
    }
}

#[tokio::test]
async fn bid_recovery_propagates_failed_incident_persistence_without_writes() {
    for case in ["config", "mismatch", "unavailable"] {
        let fixture = StaticFixture::new();
        fs::create_dir(&fixture.state_path).unwrap();
        let mut guard = fixture.guard.clone();
        let mut state = StaticGuardState {
            last_static_audit_event_id: Some(1),
            pending_bid_changes: BTreeMap::from([(21, test_pending_bid(&guard, observed_at()))]),
            ..StaticGuardState::default()
        };
        if case == "config" {
            guard.max_cpc_bid_microrubles -= 1_000_000;
        }
        let (reader, reads) = mock_reader(match case {
            "config" => vec![],
            "mismatch" => vec![(200, product(7_000_000))],
            _ => vec![(400, "{}".to_owned())],
        });
        let error = recover_pending_static_bids(
            &mut state,
            &fixture.state_path,
            &reader,
            &fixture.store,
            std::slice::from_ref(&guard),
        )
        .await
        .unwrap_err();
        assert_unsafe_state_file(&error);
        assert!(state.pending_bid_changes.contains_key(&21));
        assert_eq!(state.last_static_audit_event_id, Some(1));
        assert_eq!(
            reads.try_iter().count(),
            if case == "config" { 0 } else { 2 }
        );
    }
}

async fn assert_postwrite_disk_failure(activate: bool, unavailable: bool) {
    let fixture = StaticFixture::new();
    let mut state = StaticGuardState::default();
    let io = FakeStaticStopIo::default();
    if activate {
        io.activations.lock().unwrap().push_back(Ok(()));
    } else {
        io.writes.lock().unwrap().push_back(Ok(()));
    }
    io.readbacks.lock().unwrap().push_back(if unavailable {
        Err("readback unavailable".to_owned())
    } else {
        Ok(!activate)
    });
    let path = fixture.state_path.clone();
    *io.before_readback.lock().unwrap() = Some(Box::new(move || {
        assert!(
            load_static_state(&path)
                .unwrap()
                .pending_campaign_mutations
                .contains_key(&21)
        );
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
    }));
    let clock = RecordingClock::default();
    let failpoints = StaticFailpoints::default();
    let result = if activate {
        activate_static_campaign_with_io(
            &mut state,
            &fixture.state_path,
            &io,
            &clock,
            &failpoints,
            &fixture.guard,
            Duration::ZERO,
        )
        .await
    } else {
        guard_campaign_static_with_io(
            &mut state,
            &fixture.state_path,
            &io,
            &clock,
            &failpoints,
            &fixture.guard,
            Some(200_000),
            Some(100_000),
            Some("spend_cap_reached"),
            Duration::ZERO,
        )
        .await
    };
    assert_unsafe_state_file(&result.unwrap_err());
    assert!(state.pending_campaign_mutations.contains_key(&21));
    assert_eq!(state.last_static_audit_event_id, Some(1));
    assert_eq!(
        io.activation_calls.load(Ordering::Relaxed) + io.write_calls.load(Ordering::Relaxed),
        1
    );
    assert_eq!(io.read_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn postwrite_incident_storage_failure_never_retries_activation_or_stop() {
    for (activate, unavailable) in [(true, false), (true, true), (false, false), (false, true)] {
        assert_postwrite_disk_failure(activate, unavailable).await;
    }
}

#[tokio::test]
async fn observation_only_adapter_never_opens_state_or_calls_marketplace_ports() {
    let fixture = StaticFixture::new();
    let mut state = StaticGuardState::default();
    let io = FakeStaticStopIo::default();
    let logs = GuardLogs::default();
    guard_campaign_static_with_io(
        &mut state,
        &fixture.state_path,
        &io,
        &RecordingClock::default(),
        &StaticFailpoints::default(),
        &fixture.guard,
        Some(50),
        Some(10_000),
        None,
        Duration::ZERO,
    )
    .with_subscriber(logs.subscriber())
    .await
    .unwrap();
    assert!(logs.contains("static Ozon guard observation"));
    assert_eq!(io.activation_calls.load(Ordering::Relaxed), 0);
    assert_eq!(io.write_calls.load(Ordering::Relaxed), 0);
    assert_eq!(io.read_calls.load(Ordering::Relaxed), 0);
    clear_reconciled_static_campaign_state(&mut state, &fixture.state_path, 21).unwrap();
    assert!(!fixture.state_path.exists());
}

#[tokio::test]
async fn unmarked_adapter_success_is_rejected_before_readback() {
    for activate in [true, false] {
        let fixture = StaticFixture::new();
        let mut state = StaticGuardState::default();
        let io = FakeStaticStopIo {
            omit_marker: true,
            ..FakeStaticStopIo::default()
        };
        let clock = RecordingClock::default();
        let failpoints = StaticFailpoints::default();
        let result = if activate {
            activate_static_campaign_with_io(
                &mut state,
                &fixture.state_path,
                &io,
                &clock,
                &failpoints,
                &fixture.guard,
                Duration::ZERO,
            )
            .await
        } else {
            guard_campaign_static_with_io(
                &mut state,
                &fixture.state_path,
                &io,
                &clock,
                &failpoints,
                &fixture.guard,
                None,
                None,
                Some("telemetry_unavailable"),
                Duration::ZERO,
            )
            .await
        };
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("completed without its durable marker")
        );
        assert!(state.pending_campaign_mutations.is_empty());
        assert_eq!(state.last_static_audit_event_id, None);
        assert_eq!(io.read_calls.load(Ordering::Relaxed), 0);
        assert!(!fixture.state_path.exists());
    }
}
