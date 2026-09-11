use super::*;

#[test]
fn executor_configuration_enforces_secret_separation_and_bounded_timeouts() {
    let fixtures = Fixtures::new();
    fixtures.configure_ozon("enabled");
    let client_id = TempCredential::new("executor-boundary-id", "executor-client");
    let secret = TempCredential::new("executor-boundary-secret", "runtime-secret");
    let valid = ozon_executor_runtime_values(&fixtures, &client_id, &secret);
    for (key, value, expected) in [
        (
            "CONTROL_MCP_OZON_EXECUTOR_PERFORMANCE_CLIENT_SECRET_FILE",
            client_id.display(),
            "разных credential files",
        ),
        (
            "CONTROL_MCP_OZON_TIMEOUT_SECONDS",
            "0".to_owned(),
            "от 1 до 30",
        ),
        (
            "CONTROL_MCP_OZON_TIMEOUT_SECONDS",
            "31".to_owned(),
            "от 1 до 30",
        ),
        (
            "CONTROL_MCP_OZON_TIMEOUT_SECONDS",
            "invalid".to_owned(),
            "целым числом",
        ),
        (
            "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED",
            "invalid".to_owned(),
            "true или false",
        ),
    ] {
        let mut values = valid.clone();
        values.insert(key.to_owned(), value);
        let error = ControlAppConfig::from_lookup_for_ozon_executor(|key| values.get(key).cloned())
            .unwrap_err();
        assert!(error.to_string().contains(expected));
    }
    let runtime = ControlAppConfig::from_lookup_for_ozon_executor(|key| valid.get(key).cloned())
        .unwrap()
        .ozon_runtime
        .unwrap();
    let marketplace = runtime.marketplace.unwrap();
    let debug = format!("{marketplace:?}");
    assert!(debug.contains("<redacted>"));
    assert!(debug.contains("store_one"));
    assert!(!debug.contains("executor-client"));
    assert!(!debug.contains("runtime-secret"));
}

#[test]
fn planner_refuses_an_explicit_marketplace_write_gate() {
    let fixtures = Fixtures::new();
    fixtures.configure_ozon("enabled");
    let id = TempCredential::new("planner-boundary-id", "planner-client");
    let secret = TempCredential::new("planner-boundary-secret", "runtime-secret");
    let mut values = ozon_runtime_values(&fixtures, &id, &secret);
    values.insert(
        "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED".to_owned(),
        "true".to_owned(),
    );
    assert!(
        from(&values)
            .unwrap_err()
            .to_string()
            .contains("cannot arm marketplace writes")
    );
}

#[test]
fn executor_loader_independently_validates_its_write_gate() {
    let fixtures = Fixtures::new();
    fixtures.configure_ozon("enabled");
    let client_id = TempCredential::new("executor-gate-id", "executor-client");
    let secret = TempCredential::new("executor-gate-secret", "runtime-secret");
    let mut values = ozon_executor_runtime_values(&fixtures, &client_id, &secret);
    let config =
        ControlAppConfig::from_lookup_for_ozon_executor(|key| values.get(key).cloned()).unwrap();
    values.insert(
        "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED".to_owned(),
        "yes".to_owned(),
    );
    let error = load_ozon_runtime(
        &mut |key| values.get(key).cloned(),
        &config.auth,
        &config.policy,
        &config.registry.load().unwrap(),
        OzonRuntimeIdentity::Executor,
    )
    .unwrap_err();
    assert!(error.to_string().contains("строго true или false"));
}

#[test]
fn runtime_loader_rejects_an_unbound_wb_target() {
    let fixtures = Fixtures::new();
    fixtures.configure_wb("plan_only");
    let values = jwt_values(&fixtures);
    let registry = crate::config::RegistrySource::new(&fixtures.registry)
        .unwrap()
        .load()
        .unwrap();
    let mut policy =
        crate::control::policy::ControlPolicy::load(&fixtures.policy, &registry).unwrap();
    policy.actors[0].wb_promotion_bid_targets[0].account_id = "another_wb_account".to_owned();
    let auth = ControlAuthConfig::Jwt(
        super::super::jwt::load_jwt_config(&mut |key| values.get(key).cloned()).unwrap(),
    );
    let mut values = values;
    values.insert("CONTROL_MCP_WB_ACCOUNT_ID".to_owned(), "wb_one".to_owned());
    let error = super::super::wb_runtime::load_wb_runtime(
        &mut |key| values.get(key).cloned(),
        &auth,
        &policy,
        &registry,
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("не имеет явных targets"));
}

#[test]
fn executor_environment_loader_reads_only_control_variables_in_a_child_process() {
    const CHILD: &str = "MCP_OZON_EXECUTOR_BOUNDARY_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let config = ControlAppConfig::from_ozon_executor_env().unwrap();
        assert!(config.ozon_runtime.is_none());
        assert!(config.wb_runtime.is_none());
        assert!(
            matches!(config.auth, ControlAuthConfig::Dev { actor_id } if actor_id == "manager")
        );
        return;
    }
    let fixtures = Fixtures::new();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", "control::config::tests::boundaries::executor_environment_loader_reads_only_control_variables_in_a_child_process", "--test-threads=1"])
        .env_clear().env(CHILD, "1").envs(fixtures.values());
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile);
    }
    let output = command.output().unwrap();
    assert!(output.status.success(), "{output:?}");
}
