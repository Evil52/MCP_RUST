use std::{
    os::unix::fs::symlink,
    sync::atomic::{AtomicU64, Ordering},
};

use super::*;

const EVIDENCE: &[u8] = include_bytes!("../../../config/ads-optimizer.example.json");
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct FixtureDirectory(PathBuf);

impl FixtureDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "ads-optimizer-journal-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }

    fn run(&self) -> PathBuf {
        self.0.join("run")
    }
}

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn report(evidence: &[u8]) -> (ShadowReport, Vec<u8>) {
    let report = recommend(parse_input(evidence).unwrap()).unwrap();
    let bytes = json_bytes(&report).unwrap();
    (report, bytes)
}

#[test]
fn artifact_hash_uses_standard_sha256_of_exact_file_bytes() {
    let identity = artifact("fixture.json", b"abc");
    assert_eq!(identity.file_name, "fixture.json");
    assert_eq!(identity.size_bytes, 3);
    assert_eq!(
        identity.sha256,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn archives_exact_bytes_private_permissions_and_distinct_evidence_clock() {
    let directory = FixtureDirectory::new();
    let (report, report_bytes) = report(EVIDENCE);
    let started = Utc::now();
    let manifest = record_run(&directory.run(), EVIDENCE, &report, &report_bytes).unwrap();
    let finished = Utc::now();
    assert!((started..=finished).contains(&manifest.recorded_at));
    assert_eq!(manifest.as_of, report.as_of);
    assert_eq!(manifest.input_sha256, report.input_sha256);
    assert_eq!(manifest.optimizer_version, env!("CARGO_PKG_VERSION"));
    let input = parse_input(EVIDENCE).unwrap();
    assert_eq!(manifest.observed_at, input.observed_at);
    assert_eq!(manifest.window_start, input.window_start);
    assert_eq!(manifest.window_end, input.window_end);
    assert_eq!(manifest.account_id, input.account_id);
    assert_eq!(manifest.objective, input.objective);
    assert_eq!(manifest.evidence, artifact("evidence.json", EVIDENCE));
    assert_eq!(manifest.report, artifact("report.json", &report_bytes));
    assert_eq!(
        fs::read(directory.run().join("evidence.json")).unwrap(),
        EVIDENCE
    );
    assert_eq!(
        fs::read(directory.run().join("report.json")).unwrap(),
        report_bytes
    );
    let saved: RunManifest =
        serde_json::from_slice(&fs::read(directory.run().join("manifest.json")).unwrap()).unwrap();
    assert_eq!(saved, manifest);
    assert_eq!(fs::read_dir(directory.run()).unwrap().count(), 3);
    assert_eq!(
        fs::metadata(directory.run()).unwrap().permissions().mode() & 0o777,
        0o700
    );
    for name in ["evidence.json", "report.json", "manifest.json"] {
        assert_eq!(
            fs::metadata(directory.run().join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[test]
fn raw_format_changes_are_separate_from_reproducible_report_identity() {
    let directory = FixtureDirectory::new();
    let (first_report, first_bytes) = report(EVIDENCE);
    let mut reformatted = EVIDENCE.to_vec();
    reformatted.extend_from_slice(b"\n   \n");
    let (second_report, second_bytes) = report(&reformatted);
    assert_eq!(first_report, second_report);
    assert_eq!(first_bytes, second_bytes);
    let first = record_run(&directory.run(), EVIDENCE, &first_report, &first_bytes).unwrap();
    let second = record_run(
        &directory.0.join("second"),
        &reformatted,
        &second_report,
        &second_bytes,
    )
    .unwrap();
    assert_eq!(first.input_sha256, second.input_sha256);
    assert_eq!(first.report, second.report);
    assert_ne!(first.evidence.sha256, second.evidence.sha256);
    assert_eq!(first.as_of, second.as_of);
}

#[test]
fn forged_report_or_different_report_bytes_fail_before_creating_archive() {
    let directory = FixtureDirectory::new();
    let (mut report, report_bytes) = report(EVIDENCE);
    report.allocated_daily_budget_minor += 1;
    assert_eq!(
        record_run(&directory.run(), EVIDENCE, &report, &report_bytes),
        Err(JournalError::InvalidReport)
    );
    report.allocated_daily_budget_minor -= 1;
    let mut altered = report_bytes;
    altered.push(b' ');
    assert_eq!(
        record_run(&directory.run(), EVIDENCE, &report, &altered),
        Err(JournalError::InvalidReport)
    );
    assert!(!directory.run().exists());
}

#[test]
fn invalid_and_oversized_evidence_and_output_have_no_side_effects() {
    let directory = FixtureDirectory::new();
    let (report, report_bytes) = report(EVIDENCE);
    for (evidence, output, error) in [
        (
            b"{invalid".to_vec(),
            report_bytes.clone(),
            JournalError::InvalidEvidence,
        ),
        (
            vec![b' '; MAX_INPUT_BYTES + 1],
            report_bytes,
            JournalError::LimitExceeded,
        ),
        (
            EVIDENCE.to_vec(),
            vec![b' '; MAX_REPORT_BYTES + 1],
            JournalError::LimitExceeded,
        ),
    ] {
        assert_eq!(
            record_run(&directory.run(), &evidence, &report, &output),
            Err(error)
        );
        assert!(!directory.run().exists());
    }
}

#[test]
fn exact_four_mib_input_is_archived_without_relaxing_the_input_limit() {
    let directory = FixtureDirectory::new();
    let mut evidence = EVIDENCE.to_vec();
    evidence.resize(MAX_INPUT_BYTES, b' ');
    let (report, report_bytes) = report(&evidence);
    let manifest = record_run(&directory.run(), &evidence, &report, &report_bytes).unwrap();
    assert_eq!(manifest.evidence.size_bytes, MAX_INPUT_BYTES as u64);
    assert_eq!(
        fs::read(directory.run().join("evidence.json")).unwrap(),
        evidence
    );
}

#[test]
fn completed_or_tampered_existing_run_is_never_reused_or_overwritten() {
    let directory = FixtureDirectory::new();
    let (report, report_bytes) = report(EVIDENCE);
    record_run(&directory.run(), EVIDENCE, &report, &report_bytes).unwrap();
    let manifest = fs::read(directory.run().join("manifest.json")).unwrap();
    for tamper in [false, true] {
        if tamper {
            fs::write(directory.run().join("evidence.json"), b"tampered").unwrap();
        }
        assert_eq!(
            record_run(&directory.run(), EVIDENCE, &report, &report_bytes),
            Err(JournalError::DestinationExists)
        );
        assert_eq!(
            fs::read(directory.run().join("manifest.json")).unwrap(),
            manifest
        );
    }
    assert_eq!(
        fs::read(directory.run().join("evidence.json")).unwrap(),
        b"tampered"
    );
}

#[test]
fn concurrent_writers_cannot_share_the_same_run_directory() {
    let directory = FixtureDirectory::new();
    let (report, report_bytes) = report(EVIDENCE);
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let write = || {
            barrier.wait();
            record_run(&directory.run(), EVIDENCE, &report, &report_bytes)
        };
        let first = scope.spawn(write);
        let second = scope.spawn(write);
        [first.join().unwrap(), second.join().unwrap()]
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Err(JournalError::DestinationExists))
            .count(),
        1
    );
    assert_eq!(
        fs::read(directory.run().join("report.json")).unwrap(),
        report_bytes
    );
}

#[test]
fn existing_file_empty_directory_and_dangling_symlink_fence_destination() {
    let directory = FixtureDirectory::new();
    let (report, report_bytes) = report(EVIDENCE);
    fs::write(directory.0.join("file"), b"preserved").unwrap();
    fs::create_dir(directory.0.join("empty")).unwrap();
    symlink(directory.0.join("missing"), directory.0.join("link")).unwrap();
    for name in ["file", "empty", "link"] {
        assert_eq!(
            record_run(&directory.0.join(name), EVIDENCE, &report, &report_bytes),
            Err(JournalError::DestinationExists)
        );
    }
    assert_eq!(fs::read(directory.0.join("file")).unwrap(), b"preserved");
    assert!(!directory.0.join("missing").exists());
}

#[test]
fn missing_symlink_or_writable_parent_and_dot_segments_are_rejected() {
    let directory = FixtureDirectory::new();
    let (report, report_bytes) = report(EVIDENCE);
    symlink(&directory.0, directory.0.join("linked-parent")).unwrap();
    fs::create_dir(directory.0.join("writable")).unwrap();
    fs::set_permissions(
        directory.0.join("writable"),
        fs::Permissions::from_mode(0o777),
    )
    .unwrap();
    for path in [
        directory.0.join("missing/run"),
        directory.0.join("linked-parent/run"),
        directory.0.join("writable/run"),
    ] {
        assert_eq!(
            record_run(&path, EVIDENCE, &report, &report_bytes),
            Err(JournalError::UnsafeParent)
        );
    }
    for path in [
        directory.0.join("./run"),
        directory.0.join("../run"),
        PathBuf::from("/"),
    ] {
        assert_eq!(
            record_run(&path, EVIDENCE, &report, &report_bytes),
            Err(JournalError::InvalidPath)
        );
    }
    assert!(!directory.run().exists());
}

#[test]
fn failed_artifact_write_preserves_partial_run_without_completion_manifest() {
    fn fail_report(path: &Path, bytes: &[u8]) -> Result<(), JournalError> {
        if path.file_name().is_some_and(|name| name == "report.json") {
            // Model a partial disk write, not merely a validation failure.
            write_new_file(path, b"partial")?;
            return Err(JournalError::Unavailable);
        }
        write_new_file(path, bytes)
    }

    let directory = FixtureDirectory::new();
    DirBuilder::new()
        .mode(0o700)
        .create(directory.run())
        .unwrap();
    let (report, report_bytes) = report(EVIDENCE);
    assert_eq!(
        persist_artifacts(
            &directory.run(),
            EVIDENCE,
            &report_bytes,
            b"{}",
            fail_report
        ),
        Err(JournalError::Unavailable)
    );
    assert_eq!(
        fs::read(directory.run().join("evidence.json")).unwrap(),
        EVIDENCE
    );
    assert_eq!(
        fs::read(directory.run().join("report.json")).unwrap(),
        b"partial"
    );
    assert!(!directory.run().join("manifest.json").exists());
    assert_eq!(
        record_run(&directory.run(), EVIDENCE, &report, &report_bytes),
        Err(JournalError::DestinationExists)
    );
}

#[test]
fn storage_errors_do_not_include_evidence_or_destination_values() {
    let directory = FixtureDirectory::new();
    let (report, report_bytes) = report(EVIDENCE);
    let secret = "SECRET_MUST_NOT_APPEAR";
    let error = record_run(
        &directory.0.join(secret),
        secret.as_bytes(),
        &report,
        &report_bytes,
    )
    .unwrap_err();
    let text = error.to_string();
    assert!(!text.contains(secret));
    assert!(!text.contains(directory.0.to_str().unwrap()));
}
