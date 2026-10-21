#![cfg(feature = "runtime-binaries")]
//! The operator import binary validates trusted files before any database I/O.

use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::PathBuf,
    process::{Command, Output},
};

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "cost-import-cli-{label}-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let fixture = Self { root };
        fixture.write("policy.json", &policy(false), 0o600);
        fixture.write("registry.json", &registry(), 0o600);
        fixture.write("batch.json", &batch(), 0o600);
        fixture
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn write(&self, name: &str, value: &Value, mode: u32) {
        let path = self.path(name);
        fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
    }

    /// Runs with only the named operator variables, never the ambient environment.
    fn run(&self, arguments: &[&str], overrides: &[(&str, &str)]) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_cost-import"));
        command.args(arguments).current_dir(&self.root).env_clear();
        if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
            command.env("LLVM_PROFILE_FILE", profile);
        }
        let policy = self.path("policy.json");
        let registry = self.path("registry.json");
        let defaults = [
            ("COST_IMPORT_POLICY", policy.to_str().unwrap()),
            ("MCP_ACCESS_CONFIG", registry.to_str().unwrap()),
            ("COST_IMPORT_ACTOR_ID", "finance"),
        ];
        for (name, value) in defaults {
            if !overrides.iter().any(|(key, _)| *key == name) {
                command.env(name, value);
            }
        }
        for (name, value) in overrides {
            if !value.is_empty() {
                command.env(name, value);
            }
        }
        command.output().unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn policy(enabled: bool) -> Value {
    json!({"version":1,"enabled":enabled,"account_id":"wb","marketplace":"wildberries",
        "source_id":"one_c","allowed_skus":[1001]})
}

fn registry() -> Value {
    json!({"version":1,"actors":[
        {"id":"manager","name":"Manager","role":"manager","oidc":{"username":"manager"}},
        {"id":"finance","name":"Finance","role":"finance","account_ids":["wb"],
            "oidc":{"username":"finance"}}
    ],"accounts":[{"id":"wb","organization":"Fixture","marketplace":"wildberries",
        "seller_client_id":"1","manager_id":"manager",
        "wildberries":{"api_token_env":"WB_TOKEN"}}]})
}

fn batch() -> Value {
    json!({"version":1,"account_id":"wb","marketplace":"wildberries","source_id":"one_c",
        "export_id":"batch_1","exported_at":"2026-09-13T10:00:00Z",
        "rows":[{"source_row_id":"row_1","sku":1001,"amount_minor":12345,"currency":"RUB",
            "allocation":"per_unit","vat_treatment":"included","vat_rate_bps":2000,
            "effective_from":"2026-09-01","effective_to":"2026-09-30"}]})
}

fn stderr(output: &Output) -> String {
    assert!(!output.status.success(), "{output:?}");
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn validation_reports_the_batch_identity_without_importing() {
    let fixture = Fixture::new("validate");
    let output = fixture.run(&["validate", "batch.json"], &[]);
    assert!(output.status.success(), "{output:?}");
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "validated");
    assert_eq!(report["export_id"], "batch_1");
    assert_eq!(report["row_count"], 1);
    assert_eq!(report["imported"], false);
    assert_eq!(report["sha256"].as_str().unwrap().len(), 64);
}

#[test]
fn malformed_commands_and_missing_configuration_fail_before_reading_input() {
    let fixture = Fixture::new("configuration");
    for arguments in [&[][..], &["export", "batch.json"], &["validate"]] {
        assert!(
            stderr(&fixture.run(arguments, &[]))
                .contains("usage: cost-import <validate|import> <prepared-json-file>")
        );
    }
    for name in [
        "COST_IMPORT_POLICY",
        "MCP_ACCESS_CONFIG",
        "COST_IMPORT_ACTOR_ID",
    ] {
        assert!(
            stderr(&fixture.run(&["validate", "batch.json"], &[(name, "")])).contains(&format!(
                "required operator configuration is missing: {name}"
            ))
        );
    }
    assert!(
        stderr(&fixture.run(&["validate", "missing.json"], &[]))
            .contains("input file is unavailable")
    );
}

#[test]
fn trusted_files_must_be_private_regular_and_valid() {
    let fixture = Fixture::new("trust");
    fixture.write("policy.json", &policy(false), 0o664);
    assert!(
        stderr(&fixture.run(&["validate", "batch.json"], &[]))
            .contains("operator policy and registry must not be writable by group or others")
    );
    fixture.write("policy.json", &policy(false), 0o600);
    fixture.write("registry.json", &json!({"version":1}), 0o600);
    assert!(
        stderr(&fixture.run(&["validate", "batch.json"], &[]))
            .contains("access registry is invalid")
    );
    let mut unmanaged = registry();
    unmanaged["accounts"][0]["manager_id"] = json!("nobody");
    fixture.write("registry.json", &unmanaged, 0o600);
    assert!(
        stderr(&fixture.run(&["validate", "batch.json"], &[]))
            .contains("access registry is invalid")
    );
    fixture.write("registry.json", &registry(), 0o600);
    fixture.write("unreadable.json", &batch(), 0o000);
    // Root ignores file modes (e.g. containerized CI), so the case cannot exist there.
    if fs::File::open(fixture.path("unreadable.json")).is_err() {
        assert!(
            stderr(&fixture.run(&["validate", "unreadable.json"], &[]))
                .contains("input file is unavailable")
        );
    }
    std::os::unix::fs::symlink(fixture.path("batch.json"), fixture.path("linked.json")).unwrap();
    assert!(
        stderr(&fixture.run(&["validate", "linked.json"], &[]))
            .contains("input must be a regular file with one link")
    );
    fs::hard_link(fixture.path("batch.json"), fixture.path("second-link.json")).unwrap();
    assert!(
        stderr(&fixture.run(&["validate", "batch.json"], &[]))
            .contains("input must be a regular file with one link")
    );
}

#[test]
fn import_requires_activation_and_the_dedicated_database_role() {
    let fixture = Fixture::new("import");
    // An inactive policy may validate files but never authorizes a write.
    assert!(!fixture.run(&["import", "batch.json"], &[]).status.success());
    fixture.write("policy.json", &policy(true), 0o600);
    assert!(
        stderr(&fixture.run(&["import", "batch.json"], &[]))
            .contains("required operator configuration is missing: COST_IMPORT_DATABASE_URL")
    );
    for url in [
        "not a database url",
        "postgresql://report_collector:secret@127.0.0.1/ozon_positions",
        "postgresql://report_cost_importer@127.0.0.1/ozon_positions",
    ] {
        let message = stderr(&fixture.run(
            &["import", "batch.json"],
            &[("COST_IMPORT_DATABASE_URL", url)],
        ));
        assert!(message.contains("cost import"), "{url}: {message}");
        assert!(!message.contains("secret"), "{message}");
    }
    // Only a well-formed dedicated role reaches the connection attempt.
    let unreachable = fixture.run(
        &["import", "batch.json"],
        &[(
            "COST_IMPORT_DATABASE_URL",
            "postgresql://report_cost_importer:secret@127.0.0.1:1/ozon_positions",
        )],
    );
    let message = stderr(&unreachable);
    assert!(!message.contains("dedicated authenticated"), "{message}");
    assert!(!message.contains("secret"), "{message}");
}
