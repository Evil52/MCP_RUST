#![cfg(feature = "runtime-binaries")]

use std::{
    ffi::OsString,
    fs::{self, File},
    os::unix::ffi::OsStringExt as _,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

use mcp_ozon::reporting::ads_optimizer::MAX_INPUT_BYTES;
use serde_json::Value;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
const EXAMPLE: &str = include_str!("../config/ads-optimizer.example.json");

struct FixtureDirectory(PathBuf);

impl FixtureDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "ads-optimizer-cli-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.0.join(name);
        fs::write(&path, bytes).unwrap();
        path
    }
}

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn command(directory: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ads-optimizer"));
    command.current_dir(directory).env_clear();
    if let Some(profile_file) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile_file);
    }
    command
}

fn run(directory: &Path, input: &Path) -> Output {
    command(directory)
        .arg("recommend")
        .arg(input)
        .output()
        .unwrap()
}

#[test]
fn example_produces_reproducible_shadow_json_without_environment_or_file_writes() {
    let directory = FixtureDirectory::new();
    let evidence = directory.write("evidence.json", EXAMPLE.as_bytes());
    // A malformed ambient .env cannot affect this completely offline command.
    directory.write(".env", b"not valid dotenv \0 DO_NOT_LOAD");
    let output = run(&directory.0, &evidence);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["mode"], "shadow");
    assert_eq!(report["objective"]["kind"], "expected_economics");
    assert_eq!(report["account_id"], "SYNTHETIC_DEMO_NOT_A_LIVE_ACCOUNT");
    assert_eq!(report["allocated_daily_budget_minor"], 110_000);
    assert_eq!(report["unallocated_daily_budget_minor"], 0);
    assert_eq!(
        report["recommendations"][0]["action"],
        "test_budget_increase"
    );
    assert_eq!(
        report["recommendations"][0]["suggested_daily_budget_minor"],
        110_000
    );
    assert_eq!(report["recommendations"][0]["metrics"]["mature_days"], 14);
    assert_eq!(
        report["recommendations"][0]["metrics"]["advertising_allowance_per_order_minor"],
        15_000
    );
    assert_eq!(
        report["recommendations"][0]["metrics"]["economic_average_cpc_ceiling_minor"],
        600
    );
    assert_eq!(report["input_sha256"].as_str().unwrap().len(), 64);
    let repeated = run(&directory.0, &evidence);
    assert!(repeated.status.success());
    assert_eq!(repeated.stdout, output.stdout);
    assert_eq!(fs::read(&evidence).unwrap(), EXAMPLE.as_bytes());
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 2);
}

#[test]
fn malformed_evidence_is_rejected_without_echoing_contents_or_path() {
    let directory = FixtureDirectory::new();
    let secret = "SECRET_INPUT_MUST_NOT_APPEAR";
    let evidence = directory.write(secret, format!("{{\"secret\":\"{secret}\"").as_bytes());
    let output = run(&directory.0, &evidence);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("evidence contract"));
    assert!(!error.contains(secret));
    assert!(!error.contains(directory.0.to_str().unwrap()));
}

#[test]
fn oversized_input_is_rejected_before_parsing() {
    let directory = FixtureDirectory::new();
    let evidence = directory.0.join("too-large.json");
    File::create(&evidence)
        .unwrap()
        .set_len(MAX_INPUT_BYTES as u64 + 1)
        .unwrap();
    let output = run(&directory.0, &evidence);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("size limit")
    );
}

#[test]
fn directory_and_symlink_are_rejected_as_nonregular_inputs() {
    let directory = FixtureDirectory::new();
    let evidence = directory.write("evidence.json", EXAMPLE.as_bytes());
    let link = directory.0.join("linked.json");
    std::os::unix::fs::symlink(&evidence, &link).unwrap();
    for input in [&directory.0, &link] {
        let output = run(&directory.0, input);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("regular file")
        );
    }
}

#[test]
fn help_version_and_invalid_commands_need_no_configuration() {
    let directory = FixtureDirectory::new();
    for argument in ["--help", "-h", "--version"] {
        let output = command(&directory.0).arg(argument).output().unwrap();
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        let text = String::from_utf8(output.stdout).unwrap();
        if argument == "--version" {
            assert_eq!(
                text,
                format!("ads-optimizer {}\n", env!("CARGO_PKG_VERSION"))
            );
        } else {
            assert!(text.contains("usage: ads-optimizer recommend <evidence.json>"));
        }
    }
    for arguments in [
        vec![],
        vec!["recommend"],
        vec!["apply", "SECRET_INPUT_MUST_NOT_APPEAR"],
        vec!["recommend", "missing.json", "--execute"],
    ] {
        let output = command(&directory.0).args(arguments).output().unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let text = String::from_utf8(output.stderr).unwrap();
        assert!(text.contains("usage:"));
        assert!(!text.contains("SECRET_INPUT_MUST_NOT_APPEAR"));
    }
    let mut invalid_unicode = b"SECRET_INPUT_MUST_NOT_APPEAR".to_vec();
    invalid_unicode.push(0xff);
    let output = command(&directory.0)
        .arg("recommend")
        .arg(OsString::from_vec(invalid_unicode))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let text = String::from_utf8(output.stderr).unwrap();
    assert!(text.contains("arguments must be valid Unicode"));
    assert!(!text.contains("SECRET_INPUT_MUST_NOT_APPEAR"));
}

#[test]
fn advertising_drr_example_recommends_without_cost_data_or_configuration() {
    let directory = FixtureDirectory::new();
    let evidence = directory.write(
        "drr-evidence.json",
        include_bytes!("../config/ads-optimizer-drr.example.json"),
    );
    let output = run(&directory.0, &evidence);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["objective"]["kind"], "target_advertising_drr");
    assert_eq!(report["objective"]["max_drr_bps"], 1500);
    let recommendation = &report["recommendations"][0];
    assert_eq!(recommendation["action"], "test_budget_increase");
    assert_eq!(recommendation["suggested_daily_budget_minor"], 110_000);
    assert_eq!(recommendation["metrics"]["average_cpc_ceiling_minor"], 600);
    assert_eq!(
        recommendation["metrics"]["cpc_ceiling_basis"],
        "target_advertising_drr"
    );
    assert!(recommendation["metrics"]["economic_average_cpc_ceiling_minor"].is_null());
    assert!(
        recommendation["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .all(|reason| reason != "missing_economics")
    );
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);
}

#[test]
fn prepare_then_recommend_archives_exact_evidence_and_report_without_inventing_maturity() {
    let directory = FixtureDirectory::new();
    let bundle = directory.write(
        "bundle.json",
        include_bytes!("../config/ads-optimizer-prepare.example.json"),
    );
    let prepared = command(&directory.0)
        .arg("prepare")
        .arg(&bundle)
        .output()
        .unwrap();
    assert!(
        prepared.status.success(),
        "{}",
        String::from_utf8_lossy(&prepared.stderr)
    );
    let evidence: Value = serde_json::from_slice(&prepared.stdout).unwrap();
    assert_eq!(
        evidence["products"][0]["daily"][0]["observed_at"],
        "2026-09-25T03:00:00Z"
    );
    assert!(evidence["products"][0]["economics"].is_null());
    let input = directory.write("prepared.json", &prepared.stdout);
    let destination = directory.0.join("run-001");
    let output = command(&directory.0)
        .arg("recommend")
        .arg(&input)
        .arg("--journal-dir")
        .arg(&destination)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["recommendations"][0]["action"], "hold");
    assert_eq!(report["recommendations"][0]["metrics"]["mature_days"], 0);
    assert_eq!(
        fs::read(destination.join("evidence.json")).unwrap(),
        prepared.stdout
    );
    assert_eq!(
        fs::read(destination.join("report.json")).unwrap(),
        output.stdout
    );
    let manifest: Value =
        serde_json::from_slice(&fs::read(destination.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["input_sha256"], report["input_sha256"]);
    assert_eq!(manifest["as_of"], evidence["as_of"]);
    assert_eq!(fs::read_dir(&destination).unwrap().count(), 3);
    let retry = command(&directory.0)
        .arg("recommend")
        .arg(&input)
        .arg("--journal-dir")
        .arg(&destination)
        .output()
        .unwrap();
    assert!(!retry.status.success());
    assert!(retry.stdout.is_empty());
    assert_eq!(
        fs::read(destination.join("report.json")).unwrap(),
        output.stdout
    );
}

#[test]
fn reconcile_reports_arithmetic_without_claiming_equivalent_attribution() {
    let directory = FixtureDirectory::new();
    let input = directory.write(
        "exports.json",
        include_bytes!("../config/ads-optimizer-reconciliation.example.json"),
    );
    let output = command(&directory.0)
        .arg("reconcile")
        .arg(&input)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["mode"], "diagnostic_only");
    assert_eq!(report["semantic_equivalence_verified"], false);
    assert_eq!(report["rows"][0]["daily_minus_direct"]["orders"], "2");
    assert_eq!(
        report["rows"][0]["direct_plus_model_arithmetic"]["orders_match"],
        true
    );
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);
}

#[test]
fn rejected_prepare_provenance_and_reconciliation_inputs_do_not_echo_source_data() {
    let directory = FixtureDirectory::new();
    let mut value: Value =
        serde_json::from_str(include_str!("../config/ads-optimizer-prepare.example.json")).unwrap();
    value["advertising_pages"][0]["response"]["rows"][0]["account_id"] =
        Value::from("SECRET_FOREIGN_ACCOUNT");
    let input = directory.write("foreign.json", &serde_json::to_vec(&value).unwrap());
    let malformed = directory.write("malformed.json", b"SECRET_INPUT_MUST_NOT_APPEAR");
    for (verb, path) in [("prepare", &input), ("reconcile", &malformed)] {
        let output = command(&directory.0).arg(verb).arg(path).output().unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8(output.stderr).unwrap().contains("SECRET"));
    }
}
