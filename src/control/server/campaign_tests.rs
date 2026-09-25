use super::*;

#[tokio::test]
async fn authenticated_campaign_prepare_returns_fixed_handle_without_marketplace_write() {
    use std::os::unix::fs::PermissionsExt;

    let fixtures = Fixtures::new_wb(ControlMode::Enabled);
    let mut registry: Value =
        serde_json::from_slice(&fs::read(&fixtures.registry_path).unwrap()).unwrap();
    registry["actors"][0]["role"] = json!("admin");
    fs::write(
        &fixtures.registry_path,
        serde_json::to_vec(&registry).unwrap(),
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!(
        "mcp-wb-campaign-tool-{}-{}",
        std::process::id(),
        FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    for name in ["journal", "campaigns"] {
        let path = root.join(name);
        fs::create_dir(&path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let mut template: Value = serde_json::from_str(include_str!(
        "../../../config/wb-campaign-robot-template.example.json"
    ))
    .unwrap();
    template["account_id"] = json!("wb_one");
    let template_path = root.join("robot-template.json");
    fs::write(&template_path, serde_json::to_vec(&template).unwrap()).unwrap();
    fs::set_permissions(&template_path, fs::Permissions::from_mode(0o600)).unwrap();
    let profile = json!({
        "version":1,"account_id":"wb_one","actor_id":"manager",
        "registry":fixtures.registry_path.clone(),
        "reader_token":"/tmp/test-read-token","writer_token":"/tmp/test-write-token",
        "reader_proxy":"http://ozon-egress:3128","writer_proxy":"http://write-egress:3130",
        "allow_broad_reader":false,"robot_template":template_path,
        "journal_directory":root.join("journal"),"campaigns_directory":root.join("campaigns"),
        "max_initial_budget_rubles":2000,
        "manual_control":{"approver_actor_ids":["approver"],"max_delta_percent":25,
            "action_limits":{"max_actions_per_hour":4,"max_actions_per_day":12,
                "cooldown_seconds":300,"max_cumulative_abs_delta_kopecks_per_day":5000}}
    });
    let profile_path = root.join("profile.json");
    fs::write(&profile_path, serde_json::to_vec(&profile).unwrap()).unwrap();
    fs::set_permissions(&profile_path, fs::Permissions::from_mode(0o600)).unwrap();
    let server = fixtures.authenticated_server().with_wb_campaign_runtime(
        crate::control::ControlWbCampaignRuntimeConfig {
            account_id: "wb_one".to_owned(),
            actor_id: "manager".to_owned(),
            root: root.clone(),
            writes_enabled: true,
        },
    );
    let status = server
        .control_status(
            fixtures.identity("manager"),
            Parameters(EmptyInput::default()),
        )
        .await
        .unwrap();
    assert!(status.0.wb_campaign_creation_configured);
    let scope = server
        .control_scope(
            fixtures.identity("manager"),
            Parameters(EmptyInput::default()),
        )
        .await
        .unwrap();
    assert_eq!(scope.0.wb_campaign_account_id.as_deref(), Some("wb_one"));
    let now = Utc::now();
    let input = super::super::contract::PrepareWbCampaignInput {
        account_id: "wb_one".to_owned(),
        campaign_name: "MCP wire test".to_owned(),
        bids_kopecks: BTreeMap::from([(1001, 700)]),
        budget_rubles: 0,
        authorization_reference: "mcp_wire_test".to_owned(),
        expires_at: (now + ChronoDuration::minutes(10)).to_rfc3339(),
        robot_authorization_expires_at: (now + ChronoDuration::hours(2)).to_rfc3339(),
    };
    let result = server
        .prepare_campaign_request(&fixtures.identity("manager"), &input)
        .unwrap();
    assert_eq!(result.account_id, "wb_one");
    assert_eq!(result.campaign_handle.len(), 64);
    assert!(
        root.join("campaigns")
            .join(format!("campaign-{}", result.campaign_handle))
            .join("manifest.json")
            .is_file()
    );
    assert!(result.result.get("manifest").is_none());
    assert_eq!(result.result["marketplace_write_sent"], false);
    let found = server
        .campaign_lookup(
            &fixtures.identity("manager"),
            &super::super::contract::WbCampaignNameInput {
                account_id: "wb_one".to_owned(),
                campaign_name: "MCP wire test".to_owned(),
            },
        )
        .unwrap();
    assert_eq!(found.campaign_handle, result.campaign_handle);
    assert_eq!(found.result["outcome"], "found");
    let forged = root
        .join("campaigns")
        .join(format!("campaign-{}", "b".repeat(64)));
    fs::create_dir(&forged).unwrap();
    fs::set_permissions(&forged, fs::Permissions::from_mode(0o700)).unwrap();
    fs::copy(
        root.join("campaigns")
            .join(format!("campaign-{}", result.campaign_handle))
            .join("manifest.json"),
        forged.join("manifest.json"),
    )
    .unwrap();
    let error = crate::control::wb_launch::run_wb_campaign_launch_scoped(
        "create",
        &forged.join("manifest.json"),
        "wb_one",
        "manager",
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("handle and manifest identity differ")
    );
    fs::remove_dir_all(root).unwrap();
}
