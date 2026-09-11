#![cfg(feature = "runtime-binaries")]

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::Path,
    process::{Command, Output},
};

fn run(arguments: &[&str], directory: &Path) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_wb-automation"));
    command.args(arguments).current_dir(directory).env_clear();
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile);
    }
    command.output().unwrap()
}

fn private_json(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn every_automation_command_fails_closed_on_missing_local_policy() {
    let root = std::env::temp_dir().join(format!("wb-cli-missing-{}", std::process::id()));
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    let mut commands = vec![
        vec![
            "observe-once",
            "missing-policy",
            "registry",
            "reader",
            "state",
            "false",
        ],
        vec![
            "shadow-once-pg",
            "missing-policy",
            "registry",
            "reader",
            "state",
            "false",
        ],
    ];
    for name in [
        "activate-protective-live-pg",
        "activate-bid-writes-pg",
        "activate-bounded-pacing-pg",
        "activate-traffic-frontier-v2-pg",
        "activate-traffic-frontier-v3-pg",
        "activate-traffic-frontier-v4-pg",
        "raise-traffic-frontier-limits-pg",
        "tighten-traffic-frontier-corridor-pg",
    ] {
        commands.push(vec![
            name,
            "missing-policy",
            "target-policy",
            "registry",
            "reader",
            "false",
        ]);
    }
    for name in ["execute-once", "auto-once", "execute-once-pg"] {
        commands.push(vec![
            name,
            "missing-policy",
            "registry",
            "reader",
            "writer",
            "state",
            "false",
            "http://127.0.0.1:9",
        ]);
    }
    commands.extend([
        vec![
            "explicit-exposure-increase-once-pg",
            "missing-policy",
            "registry",
            "reader",
            "writer",
            "state",
            "false",
            "http://127.0.0.1:9",
            "100",
            "--confirm-explicit-exposure-increase",
        ],
        vec![
            "explicit-quota-override-once-pg",
            "missing-policy",
            "registry",
            "reader",
            "writer",
            "state",
            "false",
            "http://127.0.0.1:9",
            "test/one-action",
            "--confirm-one-extra-audited-action",
        ],
        vec![
            "explicit-resume-after-daily-cap-once-pg",
            "missing-policy",
            "registry",
            "reader",
            "writer",
            "state",
            "false",
            "http://127.0.0.1:9",
            "--confirm-explicit-resume-after-daily-cap",
        ],
    ]);
    for arguments in commands {
        let output = run(&arguments, &root);
        assert!(!output.status.success(), "{arguments:?}");
        assert!(output.stdout.is_empty(), "{arguments:?}");
        let error = String::from_utf8(output.stderr).unwrap();
        assert!(
            error.contains("No such file") || error.contains("policy"),
            "{arguments:?}: {error}"
        );
        assert!(!error.contains("usage:"), "{arguments:?}");
    }
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    fs::remove_dir(&root).unwrap();
}

#[test]
fn campaign_launch_reconcile_prints_an_unconfirmed_outcome_without_writer_or_network() {
    let root = std::env::temp_dir().join(format!("wb-cli-reconcile-{}", std::process::id()));
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    let journal = root.join("ofk_region_wb-Nexus");
    fs::DirBuilder::new().mode(0o700).create(&journal).unwrap();
    let sid = "123e4567-e89b-42d3-a456-426614174000";
    let registry = root.join("registry.json");
    private_json(
        &registry,
        &json!({"version":1,"actors":[{"id":"admin","name":"Test","role":"admin","oidc":{"username":"admin"}}],"accounts":[{"id":"ofk_region_wb","organization":"Test","marketplace":"wildberries","seller_client_id":"test-seller","manager_id":"admin","wildberries":{"api_token_env":"UNUSED_TEST_TOKEN","seller_sid":sid}}]}),
    );
    let reader = root.join("reader.token");
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","typ":"JWT"}"#);
    let body = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&json!({"acc":3,"for":"self","t":false,"s":(1_u64<<6)|(1_u64<<30),"exp":(chrono::Utc::now()+chrono::Duration::hours(1)).timestamp(),"sid":sid})).unwrap());
    fs::write(
        &reader,
        format!("{header}.{body}.{}", URL_SAFE_NO_PAD.encode([0_u8; 64])),
    )
    .unwrap();
    fs::set_permissions(&reader, fs::Permissions::from_mode(0o600)).unwrap();
    let manifest = json!({"scope":"create_only","account_id":"ofk_region_wb","campaign_name":"Nexus","source_policy":"missing-policy","source_policy_sha256":"revoked","bids_kopecks":{"146312604":922,"207418966":922,"455101276":922,"461126890":922,"529996417":922},"budget_rubles":0,"funding_type":1,"actor_id":"admin","authorization_reference":"revoked/test","authorized_at":"2020-01-01T00:00:00Z","expires_at":"2020-01-02T00:00:00Z","registry":registry,"reader_token":reader,"writer_token":"missing-writer","reader_proxy":"http://127.0.0.1:9","writer_proxy":"http://127.0.0.1:9","allow_broad_reader":false,"journal_directory":root,"robot_policy":"missing-robot"});
    private_json(&root.join("manifest.json"), &manifest);
    private_json(&journal.join("manifest.json"), &manifest);
    let output = run(&["campaign-launch", "reconcile", "manifest.json"], &root);
    assert!(output.status.success(), "{output:?}");
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "no_confirmed_campaign_id");
    assert_eq!(result["automatic_retry_allowed"], false);
    assert_eq!(fs::read_dir(&journal).unwrap().count(), 1);
    fs::remove_dir_all(root).unwrap();
}
