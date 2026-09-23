//! File-backed execution state: private, bounded, policy-bound and owned by
//! at most one live executor process at a time.

use super::*;

#[test]
fn state_files_are_private_bounded_and_policy_bound() {
    let fixture = Fixture::new();
    let business_date = now().date_naive();
    assert!(
        read_state_file(&fixture.root.join("execution-state.json"))
            .unwrap()
            .is_none()
    );
    let initial = load_execution_state(
        &fixture.root,
        "a",
        "ip_domnyshev_wb",
        39_682_633,
        business_date,
        false,
    )
    .unwrap();
    assert_eq!(initial.schema_version, STATE_SCHEMA_VERSION);

    let mut stored = execution_state(business_date);
    stored.policy_sha256 = "a".to_owned();
    let pending = PendingAction {
        reserved_at: now(),
        kind: PendingActionKind::PauseCampaignForDailyCap,
    };
    stored.pending = Some(pending.clone());
    save_execution_state(&fixture.root, &stored).unwrap();
    assert_eq!(
        load_execution_state(
            &fixture.root,
            "a",
            "ip_domnyshev_wb",
            39_682_633,
            business_date,
            false
        )
        .unwrap(),
        stored
    );
    verify_pending_permit(&fixture.root.join("execution-state.json"), &pending).unwrap();
    let different = PendingAction {
        reserved_at: now(),
        kind: PendingActionKind::ResumeCampaignAfterDailyCap,
    };
    assert!(verify_pending_permit(&fixture.root.join("execution-state.json"), &different).is_err());
    assert!(
        load_execution_state(
            &fixture.root,
            "wrong",
            "ip_domnyshev_wb",
            39_682_633,
            business_date,
            false
        )
        .is_err()
    );
    let migrated = load_execution_state(
        &fixture.root,
        "shadow-policy",
        "ip_domnyshev_wb",
        39_682_633,
        business_date,
        true,
    )
    .unwrap();
    assert_eq!(migrated.policy_sha256, "shadow-policy");
    assert_eq!(migrated.pending.as_ref(), Some(&pending));
    assert_eq!(migrated.actions_today, stored.actions_today);

    fs::write(fixture.root.join("execution-state.json"), b"not-json").unwrap();
    assert!(read_state_file(&fixture.root.join("execution-state.json")).is_err());

    let regular_parent = fixture.root.join("regular-parent");
    fs::write(&regular_parent, b"not a directory").unwrap();
    assert!(read_state_file(&regular_parent.join("child")).is_err());

    assert!(
        save_execution_state_bytes(&fixture.root, b"{}", |_, _| Err(anyhow::anyhow!(
            "injected write failure"
        )))
        .is_err()
    );
    fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(validate_private_directory(&fixture.root).is_err());
    fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o700)).unwrap();
}

/// A second executor, for example a manual run racing the scheduled one,
/// must not observe or write while another live process owns the state: both
/// could read the same pending-free state and each send a write.
#[tokio::test]
async fn a_second_live_executor_is_refused_before_any_marketplace_call() {
    let fixture = Fixture::new();
    let (writer_url, writer_requests) = mock_http(vec![(200, "{}".to_owned())]);
    // Any reader request would fail on this closed port with another error.
    let blocked = fixture.executor("http://127.0.0.1:1", &writer_url);
    let held = try_acquire_private_lease(&fixture.root, EXECUTION_LEASE_FILE, "test")
        .unwrap()
        .expect("the test process owns the lease");

    let error = blocked.run_once(now()).await.unwrap_err();
    assert!(
        error.to_string().contains("уже обслуживает другой процесс"),
        "{error:#}"
    );
    assert!(
        read_state_file(&fixture.root.join("execution-state.json"))
            .unwrap()
            .is_none()
    );
    assert!(
        writer_requests
            .recv_timeout(Duration::from_millis(200))
            .is_err()
    );

    drop(held);
    let (reader_url, _) = reader_server(9, 102, "2026-08-25", Some(0), 10);
    let receipt = fixture
        .executor(&reader_url, &writer_url)
        .run_once(now())
        .await
        .unwrap();
    assert_eq!(
        receipt.outcome,
        WbAutomationExecutionOutcome::WriteSentReconciliationRequired
    );
    assert!(
        try_acquire_private_lease(&fixture.root, EXECUTION_LEASE_FILE, "test")
            .unwrap()
            .is_some(),
        "a finished cycle releases its lease"
    );
}
