use super::{
    bootstrap::IsolatedChild,
    static_adapter_fixture::*,
    static_driver::{acquire_executor, runtime},
    *,
};
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{Database, mock_reader, mock_writer},
    plan::CONTROL_DB_TEST_LOCK,
};
use std::process::{Command as ProcessCommand, Stdio};

const CHILD_MODE: &str = "MCP_OZON_DYNAMIC_GUARD_CHILD";
const CHILD_TEST: &str = "control::ozon::guard_runtime::tests::dynamic_bootstrap::postgres_dynamic_reader_bootstrap_requires_its_restricted_role";

async fn dynamic_child(mode: &str) {
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let mut fixture = StaticFixture::new();
    let mut config: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.config_path).unwrap()).unwrap();
    config["dynamic_bid_control"] = serde_json::json!({
        "position_store_id":"account", "position_region_name":"bootstrap fixture region",
        "bid_step_microrubles":1_000_000, "target_position":10,
        "cooldown_seconds":1800, "max_position_age_seconds":3600,
    });
    fs::write(&fixture.config_path, serde_json::to_vec(&config).unwrap()).unwrap();
    fixture.digest = load_static_guards(&fixture.config_path, "account")
        .unwrap()
        .1;
    fixture.initialize(&database).await;
    let before = fs::read(&fixture.state_path).unwrap();
    let lease = acquire_executor(&fixture).await;
    let (reader, reads) = mock_reader(vec![
        (200, campaign("CAMPAIGN_STATE_RUNNING")),
        (200, product(7_000_000)),
    ]);
    let (writer, requests) = mock_writer(vec![]);
    let result = runtime(
        &fixture,
        &database,
        Command::AuditStaticOnce,
        &lease,
        &reader,
        &writer,
    )
    .run(std::future::pending())
    .await;
    if mode == "reader" {
        result.unwrap();
        assert_eq!(reads.try_iter().count(), 3);
    } else {
        let error = result.unwrap_err();
        if mode == "missing" {
            assert!(error.to_string().contains("requires position database"));
        }
        assert_eq!(reads.try_iter().count(), 0);
    }
    assert_eq!(requests.try_iter().count(), 0);
    assert_eq!(fs::read(&fixture.state_path).unwrap(), before);
}

async fn run_dynamic_child(mode: &str) {
    let directory = TestDirectory::new();
    let output = directory.0.join("output");
    let output_file = File::create(&output).unwrap();
    let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            CHILD_TEST,
            "--include-ignored",
            "--test-threads=1",
        ])
        .env_clear()
        .env(CHILD_MODE, mode)
        .stdout(Stdio::from(output_file.try_clone().unwrap()))
        .stderr(Stdio::from(output_file));
    for key in [
        "OZON_CONTROL_TEST_DATABASE_URL",
        "OZON_EXECUTOR_TEST_DATABASE_URL",
        "POSITION_REPOSITORY_TEST_ADMIN_URL",
    ] {
        command.env(
            key,
            std::env::var_os(key).expect("isolated PostgreSQL fixture is configured"),
        );
    }
    if mode != "missing" {
        let source = if mode == "reader" {
            "POSITION_REPOSITORY_TEST_READER_URL"
        } else {
            "POSITION_REPOSITORY_TEST_ADMIN_URL"
        };
        command.env(POSITION_DATABASE_URL_ENV, std::env::var_os(source).unwrap());
    }
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile);
    }
    let mut child = IsolatedChild(command.spawn().unwrap());
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        status.is_ok_and(|status| status.success()),
        "dynamic bootstrap child {mode} failed: {}",
        fs::read_to_string(output).unwrap()
    );
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_dynamic_reader_bootstrap_requires_its_restricted_role() {
    if let Ok(mode) = std::env::var(CHILD_MODE) {
        dynamic_child(&mode).await;
        return;
    }
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    for mode in ["reader", "missing", "admin"] {
        run_dynamic_child(mode).await;
    }
}
