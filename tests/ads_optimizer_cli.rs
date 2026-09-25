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
