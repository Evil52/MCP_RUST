use super::*;

use std::os::unix::fs::PermissionsExt;

use super::super::contract::{PrepareWbCampaignInput, WbCampaignHandleInput, WbCampaignNameInput};

/// An admin-run WB campaign runtime under a private temporary root.
fn campaign_runtime(mode: ControlMode, writes_enabled: bool) -> (Fixtures, ControlMcp, PathBuf) {
    let fixtures = Fixtures::new_wb(mode);
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
            writes_enabled,
        },
    );
    (fixtures, server, root)
}

fn prepare_input(campaign_name: &str) -> PrepareWbCampaignInput {
    let now = Utc::now();
    PrepareWbCampaignInput {
        account_id: "wb_one".to_owned(),
        campaign_name: campaign_name.to_owned(),
        bids_kopecks: BTreeMap::from([(1001, 700)]),
        budget_rubles: 0,
        authorization_reference: "mcp_wire_test".to_owned(),
        expires_at: (now + ChronoDuration::minutes(10)).to_rfc3339(),
        robot_authorization_expires_at: (now + ChronoDuration::hours(2)).to_rfc3339(),
    }
}

fn handle(campaign_handle: &str) -> WbCampaignHandleInput {
    WbCampaignHandleInput {
        account_id: "wb_one".to_owned(),
        campaign_handle: campaign_handle.to_owned(),
    }
}

/// The refusal text; tool results intentionally have no `Debug`.
fn refusal<T>(result: Result<T, String>) -> String {
    match result {
        Ok(_) => panic!("the campaign request must be refused"),
        Err(error) => error,
    }
}

#[tokio::test]
async fn authenticated_campaign_prepare_returns_fixed_handle_without_marketplace_write() {
    let (fixtures, server, root) = campaign_runtime(ControlMode::Enabled, true);
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
    let result = server
        .prepare_campaign_request(
            &fixtures.identity("manager"),
            &prepare_input("MCP wire test"),
        )
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
            &WbCampaignNameInput {
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

#[tokio::test]
async fn campaign_requests_fail_closed_outside_the_configured_runtime() {
    let (fixtures, server, root) = campaign_runtime(ControlMode::Enabled, false);
    let manager = fixtures.identity("manager");
    assert!(
        refusal(
            fixtures
                .authenticated_server()
                .prepare_campaign_request(&manager, &prepare_input("Scope"))
        )
        .starts_with("CONTROL_DISABLED: WB campaign runtime")
    );
    let mut missing = prepare_input("Scope");
    missing.account_id = "wb_missing".to_owned();
    assert_eq!(
        refusal(server.prepare_campaign_request(&manager, &missing)),
        format!("{ACCESS_DENIED}: WB account отсутствует")
    );
    assert_eq!(
        refusal(
            server
                .prepare_campaign_request(&fixtures.identity("approver"), &prepare_input("Scope"))
        ),
        format!("{ACCESS_DENIED}: WB campaign вне actor/runtime scope")
    );

    // Preparation is local; every marketplace stage still needs enabled writes.
    let prepared = server
        .prepare_campaign_request(&manager, &prepare_input("Writes off"))
        .unwrap();
    for stage in ["create", "bids", "fund", "start"] {
        assert_eq!(
            refusal(
                server
                    .campaign_stage(&manager, handle(&prepared.campaign_handle), stage, true)
                    .await
            ),
            "CONTROL_DISABLED: WB campaign marketplace writes выключены"
        );
    }
    assert_eq!(
        refusal(
            server
                .campaign_stage(&manager, handle("../campaigns"), "preflight", false)
                .await
        ),
        "CONTROL_WB_CAMPAIGN: недопустимый campaign_handle"
    );
    let unknown = "c".repeat(64);
    assert!(
        refusal(
            server
                .campaign_stage(&manager, handle(&unknown), "preflight", false)
                .await
        )
        .starts_with("CONTROL_WB_CAMPAIGN_preflight_FAILED: ")
    );
    fs::remove_dir_all(root).unwrap();

    let (fixtures, server, root) = campaign_runtime(ControlMode::Disabled, true);
    assert_eq!(
        refusal(
            server.prepare_campaign_request(&fixtures.identity("manager"), &prepare_input("Off"))
        ),
        "CONTROL_DISABLED: WB campaign policy выключена"
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn campaign_preparation_rejects_a_changed_profile_and_an_invalid_request() {
    let (fixtures, server, root) = campaign_runtime(ControlMode::Enabled, true);
    let manager = fixtures.identity("manager");
    let mut expired = prepare_input("Expired");
    expired.expires_at = (Utc::now() - ChronoDuration::minutes(1)).to_rfc3339();
    assert!(
        refusal(server.prepare_campaign_request(&manager, &expired))
            .starts_with("CONTROL_WB_CAMPAIGN_PREPARE_FAILED: ")
    );

    let profile_path = root.join("profile.json");
    let mut profile: Value = serde_json::from_slice(&fs::read(&profile_path).unwrap()).unwrap();
    profile["actor_id"] = json!("approver");
    fs::write(&profile_path, serde_json::to_vec(&profile).unwrap()).unwrap();
    assert_eq!(
        refusal(server.prepare_campaign_request(&manager, &prepare_input("Changed"))),
        "CONTROL_POLICY_CHANGED: WB campaign profile scope изменился"
    );
    fs::remove_file(&profile_path).unwrap();
    assert!(
        refusal(server.prepare_campaign_request(&manager, &prepare_input("Missing")))
            .starts_with("CONTROL_WB_CAMPAIGN_PROFILE: ")
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn campaign_lookup_and_export_stay_inside_the_runtime_manifest_scope() {
    let (fixtures, server, root) = campaign_runtime(ControlMode::Enabled, true);
    let manager = fixtures.identity("manager");
    let name = |campaign_name: &str| WbCampaignNameInput {
        account_id: "wb_one".to_owned(),
        campaign_name: campaign_name.to_owned(),
    };
    assert!(
        refusal(server.campaign_lookup(&manager, &name("Never prepared")))
            .starts_with("CONTROL_WB_CAMPAIGN_LOOKUP_FAILED: ")
    );
    assert!(
        refusal(server.campaign_export(&manager, handle(&"d".repeat(64))))
            .starts_with("CONTROL_WB_CAMPAIGN_SCOPE: ")
    );

    let prepared = server
        .prepare_campaign_request(&manager, &prepare_input("Export"))
        .unwrap();
    // Nothing was created on WB yet, so there is no campaign to export.
    assert!(
        refusal(server.campaign_export(&manager, handle(&prepared.campaign_handle)))
            .starts_with("CONTROL_WB_CAMPAIGN_EXPORT_FAILED: ")
    );
    let manifest_path = root
        .join("campaigns")
        .join(format!("campaign-{}", prepared.campaign_handle))
        .join("manifest.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["actor_id"] = json!("approver");
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert_eq!(
        refusal(server.campaign_export(&manager, handle(&prepared.campaign_handle))),
        format!("{ACCESS_DENIED}: WB campaign manifest вне actor/runtime scope")
    );
    assert_eq!(
        refusal(server.campaign_lookup(&manager, &name("Export"))),
        format!("{ACCESS_DENIED}: WB campaign вне actor/runtime scope")
    );
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn every_campaign_tool_delegates_to_its_scoped_stage() {
    let (fixtures, server, root) = campaign_runtime(ControlMode::Enabled, false);
    let manager = || fixtures.identity("manager");
    let unknown = || Parameters(handle(&"e".repeat(64)));
    let writes_off = "CONTROL_DISABLED: WB campaign marketplace writes выключены";
    assert_eq!(
        refusal(server.wb_campaign_bids(manager(), unknown()).await),
        writes_off
    );
    assert_eq!(
        refusal(server.wb_campaign_fund(manager(), unknown()).await),
        writes_off
    );
    assert_eq!(
        refusal(server.wb_campaign_start(manager(), unknown()).await),
        writes_off
    );
    for (stage, result) in [
        (
            "preflight",
            server.wb_campaign_preflight(manager(), unknown()).await,
        ),
        (
            "reconcile",
            server.wb_campaign_reconcile(manager(), unknown()).await,
        ),
    ] {
        assert!(refusal(result).starts_with(&format!("CONTROL_WB_CAMPAIGN_{stage}_FAILED: ")));
    }
    assert!(
        refusal(server.wb_campaign_export_tool(manager(), unknown()).await)
            .starts_with("CONTROL_WB_CAMPAIGN_SCOPE: ")
    );
    let missing = WbCampaignNameInput {
        account_id: "wb_one".to_owned(),
        campaign_name: "Never prepared".to_owned(),
    };
    assert!(
        refusal(
            server
                .wb_campaign_find(manager(), Parameters(missing))
                .await
        )
        .starts_with("CONTROL_WB_CAMPAIGN_LOOKUP_FAILED: ")
    );
    fs::remove_dir_all(root).unwrap();
}
