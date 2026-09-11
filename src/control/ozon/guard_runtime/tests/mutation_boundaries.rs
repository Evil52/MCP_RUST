use super::{static_safety::GuardLogs, *};
use tracing::instrument::WithSubscriber as _;

#[derive(Clone, Copy)]
enum Mutation {
    Activate,
    Deactivate,
}

impl Mutation {
    const fn name(self) -> &'static str {
        match self {
            Self::Activate => "activate",
            Self::Deactivate => "deactivate",
        }
    }

    const fn running(self) -> bool {
        matches!(self, Self::Activate)
    }

    fn queue_write(self, io: &FakeStaticStopIo, failed: bool) {
        let result = if failed {
            Err("ambiguous response".to_owned())
        } else {
            Ok(())
        };
        match self {
            Self::Activate => io.activations.lock().unwrap().push_back(result),
            Self::Deactivate => io.writes.lock().unwrap().push_back(result),
        }
    }

    async fn apply(
        self,
        state: &mut StaticGuardState,
        state_path: &Path,
        io: &FakeStaticStopIo,
        failpoints: &StaticFailpoints,
        guard: &OzonStaticCampaignGuard,
    ) -> Result<()> {
        let clock = RecordingClock::default();
        match self {
            Self::Activate => {
                activate_static_campaign_with_io(
                    state,
                    state_path,
                    io,
                    &clock,
                    failpoints,
                    guard,
                    Duration::ZERO,
                )
                .await
            }
            Self::Deactivate => {
                guard_campaign_static_with_io(
                    state,
                    state_path,
                    io,
                    &clock,
                    failpoints,
                    guard,
                    Some(100),
                    Some(10),
                    Some("spend_cap_reached"),
                    Duration::ZERO,
                )
                .await
            }
        }
    }
}

async fn assert_ambiguous_mutation(kind: Mutation, write_failed: bool, readback_unavailable: bool) {
    let directory = TestDirectory::new();
    let path = directory.0.join("state.json");
    let guard = test_static_guard(51, 10_000_000);
    let mut state = StaticGuardState::default();
    let io = FakeStaticStopIo::default();
    kind.queue_write(&io, write_failed);
    io.readbacks
        .lock()
        .unwrap()
        .push_back(if readback_unavailable {
            Err("readback unavailable".to_owned())
        } else {
            Ok(!kind.running())
        });
    kind.apply(&mut state, &path, &io, &StaticFailpoints::default(), &guard)
        .await
        .unwrap_err();
    let reason = if readback_unavailable {
        "readback_unavailable"
    } else if write_failed {
        "write_failed"
    } else {
        "readback_mismatch"
    };
    assert_eq!(
        state.incidents[&51].error_class,
        format!("{}_{reason}", kind.name())
    );
    assert_eq!(state.last_static_audit_event_id, Some(1));
    assert!(state.pending_campaign_mutations.contains_key(&51));
    assert_eq!(load_static_state(&path).unwrap(), state);
    let original = state.clone();
    // The durable intent blocks a second marketplace call even if a caller
    // erroneously asks to repeat the explicit action after an uncertain result.
    assert!(
        kind.apply(&mut state, &path, &io, &StaticFailpoints::default(), &guard)
            .await
            .unwrap_err()
            .to_string()
            .contains("already has a pending state mutation")
    );
    assert_eq!(state, original);
    assert_eq!(
        io.write_calls.load(Ordering::Relaxed) + io.activation_calls.load(Ordering::Relaxed),
        1
    );
    assert_eq!(io.read_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn uncertain_static_mutations_persist_evidence_and_reject_repeat_writes() {
    for kind in [Mutation::Activate, Mutation::Deactivate] {
        for (write_failed, readback_unavailable) in [(false, false), (true, false), (true, true)] {
            assert_ambiguous_mutation(kind, write_failed, readback_unavailable).await;
        }
    }
}

#[tokio::test]
async fn static_marker_filesystem_failure_rolls_back_memory_before_any_write() {
    for kind in [Mutation::Activate, Mutation::Deactivate] {
        let directory = TestDirectory::new();
        let path = directory.0.join("state.json");
        fs::create_dir(&path).unwrap();
        let guard = test_static_guard(52, 10_000_000);
        let mut state = StaticGuardState {
            last_static_audit_event_id: Some(4),
            ..StaticGuardState::default()
        };
        let original = state.clone();
        let io = FakeStaticStopIo::default();
        io.audit_event_sequence.store(4, Ordering::Relaxed);
        assert!(
            kind.apply(&mut state, &path, &io, &StaticFailpoints::default(), &guard)
                .await
                .unwrap_err()
                .to_string()
                .contains("before durable marker")
        );
        assert_eq!(state, original);
        assert!(path.is_dir());
        assert_eq!(io.write_calls.load(Ordering::Relaxed), 0);
        assert_eq!(io.activation_calls.load(Ordering::Relaxed), 0);
        assert_eq!(io.read_calls.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn activation_crash_after_marker_recovers_only_exact_current_config() {
    for config_changed in [false, true] {
        let directory = TestDirectory::new();
        let path = directory.0.join("state.json");
        let mut guard = test_static_guard(53, 10_000_000);
        let mut state = StaticGuardState::default();
        let io = FakeStaticStopIo::default();
        let failpoints =
            StaticFailpoints(BTreeSet::from([OzonStaticMutationFailpoint::AfterMarker]));
        assert!(
            Mutation::Activate
                .apply(&mut state, &path, &io, &failpoints, &guard)
                .await
                .unwrap_err()
                .to_string()
                .contains("AfterMarker")
        );
        assert!(state.pending_campaign_mutations.contains_key(&53));
        if config_changed {
            guard.max_cpc_bid_microrubles = 9_000_000;
        } else {
            io.readbacks.lock().unwrap().push_back(Ok(false));
        }
        recover_pending_static_campaign_mutations_with_io(
            &mut state,
            &path,
            &io,
            std::slice::from_ref(&guard),
        )
        .await
        .unwrap();
        assert!(state.pending_campaign_mutations.contains_key(&53));
        assert_eq!(
            state.incidents[&53].error_class,
            if config_changed {
                "pending_mutation_config_mismatch"
            } else {
                "pending_activate_readback_mismatch"
            }
        );
        assert_eq!(io.activation_calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            io.read_calls.load(Ordering::Relaxed),
            usize::from(!config_changed)
        );
        assert_eq!(load_static_state(&path).unwrap(), state);
    }
}

#[tokio::test]
async fn exact_static_readback_resolves_write_error_and_logs_the_ambiguity() {
    for kind in [Mutation::Activate, Mutation::Deactivate] {
        let directory = TestDirectory::new();
        let path = directory.0.join("state.json");
        let guard = test_static_guard(54, 10_000_000);
        let mut state = StaticGuardState::default();
        let io = FakeStaticStopIo::default();
        kind.queue_write(&io, true);
        io.readbacks.lock().unwrap().push_back(Ok(kind.running()));
        let logs = GuardLogs::default();
        kind.apply(&mut state, &path, &io, &StaticFailpoints::default(), &guard)
            .with_subscriber(logs.subscriber())
            .await
            .unwrap();
        assert!(logs.contains("write_reported_error=true"));
        assert!(state.pending_campaign_mutations.is_empty());
        assert!(state.incidents.is_empty());
        assert_eq!(state.last_static_audit_event_id, Some(1));
        assert_eq!(io.read_calls.load(Ordering::Relaxed), 1);
        assert_eq!(load_static_state(&path).unwrap(), state);
    }
}
