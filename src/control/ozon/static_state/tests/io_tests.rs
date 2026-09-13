use super::*;

#[test]
fn metadata_errors_fail_closed_without_creating_state_or_lease_files() {
    let directory = TestDirectory::new();
    let unrepresentable = directory.0.join("x".repeat(256));
    assert!(matches!(
        load_ozon_static_guard_state(&unrepresentable),
        Err(OzonStaticGuardStateError::Io(_))
    ));
    assert!(matches!(
        persist_ozon_static_guard_state(&unrepresentable, &OzonStaticGuardState::default()),
        Err(OzonStaticGuardStateError::Io(_))
    ));
    assert!(matches!(
        OzonStaticGuardStateLease::acquire(&unrepresentable),
        Err(OzonStaticGuardStateError::Io(_))
    ));
    let invalid_parent = unrepresentable.join("state.json");
    assert!(matches!(
        load_ozon_static_guard_state(&invalid_parent),
        Err(OzonStaticGuardStateError::Io(_))
    ));
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 0);
}

#[test]
fn failed_temporary_creation_preserves_the_previous_snapshot() {
    let directory = TestDirectory::new();
    // The state name is valid, but adding the private temporary suffix exceeds
    // the filesystem's component limit before any write can replace it.
    let state_path = directory.0.join("x".repeat(249));
    let original = serde_json::to_vec(&populated_state()).unwrap();
    fs::write(&state_path, &original).unwrap();
    fs::set_permissions(&state_path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(matches!(
        persist_ozon_static_guard_state(&state_path, &OzonStaticGuardState::default()),
        Err(OzonStaticGuardStateError::Io(_))
    ));
    assert_eq!(fs::read(&state_path).unwrap(), original);
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);
}

#[test]
fn temporary_collisions_preserve_existing_files() {
    const CHILD: &str = "MCP_OZON_TEMP_COLLISION_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        verify_collision_preservation();
        return;
    }
    // The real process-local sequence must be isolated from concurrently
    // running state tests; the child executes this one filesystem regression.
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "control::ozon::static_state::tests::io_tests::temporary_collisions_preserve_existing_files", "--nocapture"])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn verify_collision_preservation() {
    let directory = TestDirectory::new();
    let state_path = directory.state();
    let candidate = |sequence| {
        directory
            .0
            .join(format!(".state.json.{}.{sequence}.tmp", std::process::id()))
    };
    let first_sequence = TEMPORARY_SEQUENCE.load(Ordering::Relaxed);
    let existing = candidate(first_sequence);
    fs::write(&existing, b"preserve earlier crash evidence").unwrap();
    let (temporary, file) = create_unique_temporary(&directory.0, &state_path).unwrap();
    assert_ne!(temporary, existing);
    assert_eq!(
        fs::read(&existing).unwrap(),
        b"preserve earlier crash evidence"
    );
    assert_eq!(file.metadata().unwrap().permissions().mode() & 0o777, 0o600);
    drop(file);
    fs::remove_file(temporary).unwrap();
    let next_sequence = TEMPORARY_SEQUENCE.load(Ordering::Relaxed);
    for sequence in next_sequence..next_sequence + 16 {
        fs::write(candidate(sequence), b"existing temporary").unwrap();
    }
    let error =
        persist_ozon_static_guard_state(&state_path, &OzonStaticGuardState::default()).unwrap_err();
    let OzonStaticGuardStateError::Io(error) = error else {
        panic!("temporary exhaustion must be an IO failure")
    };
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert!(!state_path.exists());
    for sequence in next_sequence..next_sequence + 16 {
        assert_eq!(
            fs::read(candidate(sequence)).unwrap(),
            b"existing temporary"
        );
    }
}
