use super::*;

#[test]
fn reporting_only_never_looks_up_marketplace_credentials() {
    let mut registry = performance_registry();
    registry.accounts.push(MarketplaceAccount {
        id: "wb_shop".into(),
        organization: "WB Shop".into(),
        marketplace: Marketplace::Wildberries,
        seller_client_id: "42".into(),
        manager_id: "manager".into(),
        ozon: None,
        wildberries: Some(WildberriesAccount {
            api_token_env: "WB_TOKEN".into(),
            seller_sid: None,
        }),
    });
    let path = write_registry(&registry);
    let secret_names = [
        "SHOP_ID",
        "SHOP_KEY",
        "SHOP_PERFORMANCE_ID",
        "SHOP_PERFORMANCE_SECRET",
        "WB_TOKEN",
    ];
    let config = AppConfig::from_lookup(|key| {
        assert!(
            !secret_names.contains(&key),
            "credential lookup in reporting-only mode: {key}"
        );
        match key {
            "MCP_DATA_MODE" => Some("reporting_only".into()),
            "MCP_ACTOR_ID" => Some("admin".into()),
            "MCP_ACCESS_CONFIG" => Some(path.to_string_lossy().into_owned()),
            _ => None,
        }
    })
    .unwrap();
    assert_eq!(config.data_mode, McpDataMode::ReportingOnly);
    assert!(config.stores.is_empty());
    assert!(config.performance_stores.is_empty());
    assert!(config.wildberries_accounts.is_empty());
    assert_eq!(config.registry.load().unwrap().accounts.len(), 2);
}

#[test]
fn reporting_only_process_does_not_import_adjacent_dotenv() {
    const CHILD: &str = "MCP_REPORTING_ONLY_ENV_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let config = AppConfig::from_env().unwrap();
        assert_eq!(config.data_mode, McpDataMode::ReportingOnly);
        assert!(config.stores.is_empty());
        assert!(std::env::var_os("SHOP_KEY").is_none());
        assert!(std::env::var_os("DOTENV_IMPORT_MARKER").is_none());
        return;
    }
    let path = write_registry(&sample_registry());
    let directory = path.with_extension("reporting-only");
    std::fs::create_dir(&directory).unwrap();
    std::fs::write(
        directory.join(".env"),
        "SHOP_KEY=synthetic-key\nDOTENV_IMPORT_MARKER=present\n",
    )
    .unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "config::tests::data_mode::reporting_only_process_does_not_import_adjacent_dotenv",
            "--nocapture",
            "--test-threads=1",
        ])
        .current_dir(&directory)
        .env_clear()
        .env(CHILD, "1")
        .env("MCP_DATA_MODE", "reporting_only")
        .env("MCP_ACTOR_ID", "admin")
        .env("MCP_ACCESS_CONFIG", &path);
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile);
    }
    let output = command.output().unwrap();
    std::fs::remove_dir_all(directory).unwrap();
    assert!(
        output.status.success(),
        "environment child failed: {output:?}"
    );
}

#[test]
fn reporting_mode_typo_fails_closed_without_echoing_input() {
    for value in [
        "",
        "Reporting_Only",
        "reporting_only ",
        "synthetic-sensitive-input",
    ] {
        let error = AppConfig::from_lookup(|key| {
            assert_eq!(key, "MCP_DATA_MODE");
            Some(value.into())
        })
        .unwrap_err()
        .to_string();
        assert_eq!(
            error,
            "MCP_DATA_MODE должен быть live_analytics или reporting_only"
        );
    }
    assert_eq!(
        "live_analytics".parse::<McpDataMode>().unwrap(),
        McpDataMode::LiveAnalytics
    );
}
