#![cfg(feature = "runtime-binaries")]

use std::process::Command;

const RUNTIME_BINARY_NAMES: [&str; 7] = [
    "mcp-ozon",
    "mcp-ozon-control",
    "position-collector",
    "report-worker",
    "report-collector",
    "wb-automation",
    "ozon-campaign-guard",
];

fn assert_version_probe(path: &str, binary_name: &str) {
    let mut command = Command::new(path);
    command.arg("--version").env_clear();
    // Instrumented child processes must retain the one path used by LLVM's
    // profiling runtime. No application configuration or credentials are
    // inherited, and ordinary test/container runs do not set this variable.
    if let Some(profile_file) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile_file);
    }
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("failed to execute {binary_name}: {error}"));
    assert!(
        output.status.success(),
        "{binary_name} probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("{binary_name} {}\n", env!("CARGO_PKG_VERSION"))
    );
    assert!(output.stderr.is_empty());
}

macro_rules! runtime_probe {
    ($test_name:ident, $cargo_path:literal, $binary_name:literal) => {
        #[test]
        fn $test_name() {
            assert_version_probe(env!($cargo_path), $binary_name);
        }
    };
}

runtime_probe!(mcp_ozon_probe, "CARGO_BIN_EXE_mcp-ozon", "mcp-ozon");
runtime_probe!(
    mcp_ozon_control_probe,
    "CARGO_BIN_EXE_mcp-ozon-control",
    "mcp-ozon-control"
);
runtime_probe!(
    position_collector_probe,
    "CARGO_BIN_EXE_position-collector",
    "position-collector"
);
runtime_probe!(
    report_worker_probe,
    "CARGO_BIN_EXE_report-worker",
    "report-worker"
);
runtime_probe!(
    report_collector_probe,
    "CARGO_BIN_EXE_report-collector",
    "report-collector"
);
runtime_probe!(
    wb_automation_probe,
    "CARGO_BIN_EXE_wb-automation",
    "wb-automation"
);
runtime_probe!(
    ozon_campaign_guard_probe,
    "CARGO_BIN_EXE_ozon-campaign-guard",
    "ozon-campaign-guard"
);

#[test]
fn every_manifest_binary_has_an_explicit_probe() {
    assert!(
        include_str!("../Cargo.toml")
            .lines()
            .any(|line| line.trim() == "autobins = false"),
        "runtime binaries must remain explicit so a new src/bin target cannot bypass this probe list"
    );
    let mut in_binary_section = false;
    let mut manifest_binary_names = include_str!("../Cargo.toml")
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line == "[[bin]]" {
                in_binary_section = true;
                return None;
            }
            if line.starts_with('[') {
                in_binary_section = false;
                return None;
            }
            if !in_binary_section {
                return None;
            }
            line.strip_prefix("name = \"")
                .and_then(|name| name.strip_suffix('"'))
        })
        .collect::<Vec<_>>();
    manifest_binary_names.sort_unstable();

    let mut probed_binary_names = RUNTIME_BINARY_NAMES.to_vec();
    probed_binary_names.sort_unstable();

    assert_eq!(manifest_binary_names, probed_binary_names);
}
