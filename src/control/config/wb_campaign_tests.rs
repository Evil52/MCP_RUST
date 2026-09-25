use super::*;

#[test]
fn wb_campaign_runtime_rejects_dev_auth_before_reading_profile() {
    let fixtures = Fixtures::new();
    let mut values = fixtures.values();
    values.insert(
        "CONTROL_MCP_WB_CAMPAIGN_ROOT".to_owned(),
        "/must/not/be/read".to_owned(),
    );
    values.insert(
        "CONTROL_MCP_WB_CAMPAIGN_ACCOUNT_ID".to_owned(),
        "wb_one".to_owned(),
    );
    let error = ControlAppConfig::from_lookup(|key| values.get(key).cloned()).unwrap_err();
    assert!(error.to_string().contains("JWT authentication"));
}

#[test]
fn wb_campaign_runtime_binds_admin_account_profile_and_separate_writer() {
    use std::os::unix::fs::PermissionsExt;

    let fixtures = Fixtures::new();
    fixtures.configure_wb("enabled");
    let mut registry: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixtures.registry).unwrap()).unwrap();
    registry["actors"][0]["role"] = serde_json::json!("admin");
    fs::write(&fixtures.registry, serde_json::to_vec(&registry).unwrap()).unwrap();
    let future = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let reader = TempCredential::new(
        "campaign-reader",
        &wb_token(3, WB_PROMOTION_BIT | WB_READ_ONLY_BIT, future),
    );
    let writer = TempCredential::new("campaign-writer", &wb_token(3, WB_PROMOTION_BIT, future));
    let root = std::env::temp_dir().join(format!(
        "mcp-control-campaign-{}-{}",
        std::process::id(),
        FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    for name in ["journal", "campaigns"] {
        let path = root.join(name);
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let profile = serde_json::json!({
        "version":1,"account_id":"wb_one","actor_id":"manager",
        "registry":fixtures.registry.clone(),"reader_token":reader.0.clone(),"writer_token":writer.0.clone(),
        "reader_proxy":"http://ozon-egress:3128","writer_proxy":"http://write-egress:3130",
        "allow_broad_reader":false,"robot_template":root.join("robot-template.json"),
        "journal_directory":root.join("journal"),"campaigns_directory":root.join("campaigns"),
        "max_initial_budget_rubles":2000,
        "manual_control":{"approver_actor_ids":["approver"],"max_delta_percent":25,
            "action_limits":{"max_actions_per_hour":4,"max_actions_per_day":12,
                "cooldown_seconds":300,"max_cumulative_abs_delta_kopecks_per_day":5000}}
    });
    let profile_path = root.join("profile.json");
    fs::write(&profile_path, serde_json::to_vec(&profile).unwrap()).unwrap();
    fs::set_permissions(&profile_path, fs::Permissions::from_mode(0o600)).unwrap();
    let mut values = wb_runtime_values(&fixtures, &reader);
    values.insert(
        "CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE".to_owned(),
        writer.display(),
    );
    values.insert(
        "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED".to_owned(),
        "true".to_owned(),
    );
    values.insert(
        "CONTROL_MCP_WB_CAMPAIGN_ROOT".to_owned(),
        root.display().to_string(),
    );
    values.insert(
        "CONTROL_MCP_WB_CAMPAIGN_ACCOUNT_ID".to_owned(),
        "wb_one".to_owned(),
    );
    let config = ControlAppConfig::from_lookup(|key| values.get(key).cloned()).unwrap();
    let runtime = config.wb_campaign_runtime.unwrap();
    assert_eq!(runtime.account_id, "wb_one");
    assert_eq!(runtime.actor_id, "manager");
    assert!(runtime.writes_enabled);
    assert_eq!(runtime.root, root);
    fs::remove_dir_all(root).unwrap();
}
