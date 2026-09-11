use super::{static_adapter_fixture::*, *};
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::Database, plan::CONTROL_DB_TEST_LOCK,
};
use std::{
    ffi::OsString,
    net::TcpListener,
    process::{Child, Command as ProcessCommand, Stdio},
};

const CHILD_MODE: &str = "MCP_OZON_GUARD_RUNTIME_BOOTSTRAP_CHILD";
const CHILD_TEST: &str = "control::ozon::guard_runtime::tests::bootstrap::subprocess_bootstrap_initializes_checks_and_serves_without_marketplace_io";

fn child_environment(
    fixture: &StaticFixture,
    database_url: &str,
    proxy_url: &str,
) -> BTreeMap<String, OsString> {
    let client_id = fixture.authorization.path.join("client-id");
    let client_secret = fixture.authorization.path.join("client-secret");
    for (path, value) in [
        (&client_id, "adapter-client"),
        (&client_secret, "local-fixture-secret"),
    ] {
        fs::write(path, value).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    let mut values = BTreeMap::from([
        ("CONTROL_MCP_AUTH_MODE".to_owned(), OsString::from("jwt")),
        (
            "CONTROL_MCP_ACCESS_CONFIG".to_owned(),
            fixture
                .authorization
                .path
                .join("registry.json")
                .into_os_string(),
        ),
        (
            "CONTROL_MCP_POLICY".to_owned(),
            fixture
                .authorization
                .path
                .join("policy.json")
                .into_os_string(),
        ),
        (
            "CONTROL_MCP_JWT_ISSUER".to_owned(),
            OsString::from("https://issuer.invalid"),
        ),
        (
            "CONTROL_MCP_PUBLIC_URL".to_owned(),
            OsString::from("https://control.invalid"),
        ),
        (
            "CONTROL_MCP_JWT_AUDIENCE".to_owned(),
            OsString::from("https://control.invalid"),
        ),
        (
            "CONTROL_MCP_OZON_EXECUTOR_DATABASE_URL".to_owned(),
            OsString::from(database_url),
        ),
        (
            "CONTROL_MCP_OZON_ACCOUNT_ID".to_owned(),
            OsString::from("account"),
        ),
        (
            "CONTROL_MCP_OZON_EXECUTOR_PERFORMANCE_CLIENT_ID_FILE".to_owned(),
            client_id.into_os_string(),
        ),
        (
            "CONTROL_MCP_OZON_EXECUTOR_PERFORMANCE_CLIENT_SECRET_FILE".to_owned(),
            client_secret.into_os_string(),
        ),
        (
            "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED".to_owned(),
            OsString::from("true"),
        ),
        (
            "CONTROL_MCP_OZON_PROXY".to_owned(),
            OsString::from(proxy_url),
        ),
        (
            "CONTROL_MCP_OZON_TIMEOUT_SECONDS".to_owned(),
            OsString::from("1"),
        ),
    ]);
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        values.insert("LLVM_PROFILE_FILE".to_owned(), profile);
    }
    values
}

struct IsolatedChild(Child);

impl Drop for IsolatedChild {
    fn drop(&mut self) {
        // A failing assertion or elapsed deadline must not leave a worker,
        // advisory lease, or open connection running after its fixture ends.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn run_child(mode: &str, environment: &BTreeMap<String, OsString>) {
    let registry = PathBuf::from(&environment["CONTROL_MCP_ACCESS_CONFIG"]);
    let output_path = registry.parent().unwrap().join(format!("child-{mode}.log"));
    let output_file = File::create(&output_path).unwrap();
    let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", CHILD_TEST, "--test-threads=1"])
        .env_clear()
        .env(CHILD_MODE, mode)
        .envs(environment)
        .stdout(Stdio::from(output_file.try_clone().unwrap()))
        .stderr(Stdio::from(output_file));
    let mut child = IsolatedChild(command.spawn().unwrap());
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        result.is_ok_and(|status| status.success()),
        "isolated runtime child {mode} failed or exceeded its deadline: {}",
        fs::read_to_string(output_path).unwrap(),
    );
}

async fn fixture_lease_is_held(database: &Database, fingerprint: &str) -> bool {
    let identity = format!("mcp-ozon/executor-identity/v1/{fingerprint}");
    database.admin.query_one(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND objsubid=1 AND classid::bigint=((hashtextextended($1::text,0)>>32)&4294967295) AND objid::bigint=(hashtextextended($1::text,0)&4294967295))",
        &[&identity],
    ).await.unwrap().get(0)
}

async fn child_run(mode: &str) {
    let arguments = match mode {
        "initialize" => vec![
            INITIALIZE_STATIC_STATE_COMMAND.to_owned(),
            INITIALIZE_STATIC_STATE_CONFIRMATION.to_owned(),
        ],
        "health" | "health_without_state" | "health_mismatch" => {
            vec![HEALTHCHECK_COMMAND.to_owned()]
        }
        "audit_without_config" => vec![AUDIT_COMMAND.to_owned()],
        "invalid_command" => vec!["invalid".to_owned()],
        _ => vec![],
    };
    let result = if mode == "serve" {
        // The production loop receives the same shutdown future for its entire
        // lifetime; this child ends deterministically after an idle DB tick.
        run_ozon_campaign_guard_with_shutdown(&arguments, async {
            tokio::time::sleep(Duration::from_millis(250)).await;
        })
        .await
    } else {
        run_ozon_campaign_guard(&arguments).await
    };
    let expected_error = match mode {
        "health_without_state" => Some("requires state file"),
        "health_mismatch" => Some("audit continuity failed"),
        "audit_without_config" => Some("requires static guard config"),
        "disabled_policy" => Some("enabled policy"),
        "invalid_command" => Some("usage:"),
        "unarmed" => Some("armed writer"),
        "runtime_missing" => Some("Ozon runtime"),
        _ => None,
    };
    if let Some(expected) = expected_error {
        assert!(result.unwrap_err().to_string().contains(expected));
    } else {
        result.unwrap();
    }
}

#[tokio::test]
async fn subprocess_bootstrap_initializes_checks_and_serves_without_marketplace_io() {
    if let Ok(mode) = std::env::var(CHILD_MODE) {
        child_run(&mode).await;
        return;
    }
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let fixture = StaticFixture::new();
        database.prepare(&fixture.authorization).await;
        // Bootstrap the actual consumer with an empty outbox so there is no
        // authorized marketplace operation for any worker to claim.
        database
            .admin
            .batch_execute("TRUNCATE control.ozon_campaign_plans CASCADE")
            .await
            .unwrap();
        let database_url = std::env::var("OZON_EXECUTOR_TEST_DATABASE_URL").unwrap();
        let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
        proxy.set_nonblocking(true).unwrap();
        let mut environment = child_environment(
            &fixture,
            &database_url,
            &format!("http://{}", proxy.local_addr().unwrap()),
        );
        environment.insert(
            STATIC_GUARDS_FILE_ENV.to_owned(),
            fixture.config_path.clone().into_os_string(),
        );
        environment.insert(
            STATIC_STATE_FILE_ENV.to_owned(),
            fixture.state_path.clone().into_os_string(),
        );
        run_child("initialize", &environment).await;
        let state = load_static_state(&fixture.state_path).unwrap();
        assert!(state.last_static_audit_event_id.is_some());
        assert_eq!(
            state.last_static_audit_event_id,
            database
                .executor
                .latest_static_guard_audit_event_id("account")
                .await
                .unwrap()
        );
        let executor_lease =
            OzonExecutorLease::acquire(&database_url.parse().unwrap(), &fixture.fingerprint)
                .await
                .unwrap();
        assert!(fixture_lease_is_held(&database, &fixture.fingerprint).await);
        run_child("health", &environment).await;
        let mut stale_state = state.clone();
        stale_state.last_static_audit_event_id = state.last_static_audit_event_id.map(|id| id + 1);
        persist_static_state(&fixture.state_path, &stale_state).unwrap();
        run_child("health_mismatch", &environment).await;
        persist_static_state(&fixture.state_path, &state).unwrap();
        environment.remove(STATIC_STATE_FILE_ENV);
        run_child("health_without_state", &environment).await;
        drop(executor_lease);
        // Observe the dropped owner's session lock without acquiring another
        // temporary lease whose asynchronous close could race the new worker.
        tokio::time::timeout(Duration::from_secs(2), async {
            while fixture_lease_is_held(&database, &fixture.fingerprint).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        environment.remove(STATIC_GUARDS_FILE_ENV);
        run_child("audit_without_config", &environment).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while fixture_lease_is_held(&database, &fixture.fingerprint).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        run_child("serve", &environment).await;
        for mode in ["invalid_command", "unarmed", "runtime_missing"] {
            let mut values = environment.clone();
            if mode == "unarmed" {
                values.insert(
                    "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED".to_owned(),
                    OsString::from("false"),
                );
            }
            if mode == "runtime_missing" {
                values.remove("CONTROL_MCP_OZON_ACCOUNT_ID");
            }
            run_child(mode, &values).await;
        }
        let policy_path = fixture.authorization.path.join("policy.json");
        let mut policy: serde_json::Value =
            serde_json::from_slice(&fs::read(&policy_path).unwrap()).unwrap();
        policy["mode"] = "plan_only".into();
        fs::write(policy_path, serde_json::to_vec(&policy).unwrap()).unwrap();
        run_child("disabled_policy", &environment).await;
        assert_eq!(
            proxy.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert_eq!(
            OzonExecutorLease::verify_held(&database_url.parse().unwrap(), &fixture.fingerprint)
                .await,
            Err(crate::control::ozon::executor_lease::OzonExecutorLeaseError::NotHeld),
        );
    }
}

#[test]
fn explicit_reconcile_command_requires_its_exact_confirmation() {
    assert_eq!(
        parse_command(&[
            RECONCILE_COMMAND.to_owned(),
            RECONCILE_CONFIRMATION.to_owned()
        ])
        .unwrap(),
        Command::ReconcileStaticOnce,
    );
    assert!(parse_command(&[RECONCILE_COMMAND.to_owned()]).is_err());
    assert!(parse_command(&[RECONCILE_COMMAND.to_owned(), "--confirm".to_owned()]).is_err());
}

#[test]
fn static_cycle_failures_reset_after_success_and_stop_at_the_exact_limit() {
    let mut failures = 0;
    for _ in 0..2 {
        record_static_cycle_result(Err(anyhow::anyhow!("unavailable")), &mut failures).unwrap();
    }
    assert_eq!(failures, 2);
    record_static_cycle_result(Ok(()), &mut failures).unwrap();
    assert_eq!(failures, 0);
    for expected in 1..=3 {
        let result = record_static_cycle_result(Err(anyhow::anyhow!("unavailable")), &mut failures);
        assert_eq!(failures, expected);
        assert_eq!(result.is_err(), expected == 3);
    }
}
