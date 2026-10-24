//! Journal identities decide which collection a restarted operator resumes.
use super::{Arguments, arguments::parse_arguments, reports::collection_identity};

fn arguments(command: &str, extra: &[&str]) -> Arguments {
    let mut raw: Vec<String> = [
        command,
        "--registry",
        "/private/registry.json",
        "--actor",
        "finance",
        "--account",
        "ofk_region_wb",
        "--credentials-dir",
        "/private/credentials",
        "--state-dir",
        "/private/state",
        "--from",
        "2026-08-31",
        "--to",
        "2026-09-06",
    ]
    .map(str::to_owned)
    .to_vec();
    raw.extend(extra.iter().map(|value| (*value).to_owned()));
    parse_arguments(&raw).unwrap()
}

#[test]
fn official_report_journals_are_keyed_by_period_and_observation() {
    for (command, period) in [("list-reports-wb", "weekly"), ("sync-reports-wb", "daily")] {
        let parsed = arguments(
            command,
            &["--observation", "pilot_20260913", "--period", period],
        );
        assert_eq!(
            collection_identity(&parsed).unwrap(),
            format!("wb-official-v1:ofk_region_wb:2026-08-31:2026-09-06:{period}:pilot_20260913")
        );
    }
    let report = arguments(
        "publish-report-wb",
        &["--observation", "pilot_20260913", "--report-id", "42"],
    );
    // One observation journal covers every report selected from that list.
    assert_eq!(
        collection_identity(&report).unwrap(),
        "wb-official-v1:ofk_region_wb:2026-08-31:2026-09-06:weekly:pilot_20260913"
    );
}

#[test]
fn detailed_collection_journal_ignores_official_report_options() {
    assert_eq!(
        collection_identity(&arguments("collect-wb", &[])).unwrap(),
        "wb-detailed-v1:ofk_region_wb:2026-08-31:2026-09-06"
    );
}
