use super::{
    BTreeMap, CREDENTIAL_DIRECTORY_ENV, CollectionClaim, Duration, MODE_ENV, POLICY_PATH_ENV,
    PathBuf, ReportCollectorConfig, ReportCollectorMode, StoreId, URL_SAFE_NO_PAD, Utc, claim,
    config, directory, entries, file, fs, personal_wb_token,
};
use base64::Engine as _;

fn recording_secret_lookup<'a>(
    requested: &'a std::cell::RefCell<Vec<String>>,
    values: &'a [(&'a str, String)],
) -> impl FnMut(&str) -> Option<String> + 'a {
    move |key| {
        requested.borrow_mut().push(key.to_owned());
        values
            .iter()
            .find_map(|(entry, value)| (*entry == key).then(|| value.clone()))
    }
}

#[test]
fn explicit_ozon_dry_run_resolves_only_the_policy_scoped_read_bindings() {
    let disabled = config(&entries()).unwrap();
    let rejected_keys = std::cell::RefCell::new(Vec::new());
    let mut no_secrets = recording_secret_lookup(&rejected_keys, &[]);
    let ozon_claim = claim("ozon", crate::reporting::snapshot::Marketplace::Ozon);
    assert!(
        disabled
            .resolve_ozon_dry_run(&ozon_claim, &mut no_secrets)
            .is_err()
    );

    let mut values = entries();
    values.push((MODE_ENV, "ozon_dry_run".to_owned()));
    let mut startup_keys = Vec::new();
    let dry_run = ReportCollectorConfig::from_lookup(&mut |key| {
        startup_keys.push(key.to_owned());
        values
            .iter()
            .find_map(|(entry, value)| (*entry == key).then(|| value.clone()))
    })
    .unwrap();
    assert_eq!(dry_run.mode(), ReportCollectorMode::OzonDryRun);
    assert!(
        ["ID", "KEY", "PERF_ID", "PERF_SECRET", "WB_TOKEN"]
            .into_iter()
            .all(|key| !startup_keys.iter().any(|requested| requested == key))
    );

    let secrets = [
        ("ID", "client-id".to_owned()),
        ("KEY", "api-key".to_owned()),
        ("PERF_ID", "performance-client-id".to_owned()),
        ("PERF_SECRET", "performance-client-secret".to_owned()),
        ("WB_TOKEN", "unrelated-wb-token".to_owned()),
    ];
    let resolved_keys = std::cell::RefCell::new(Vec::new());
    let (seller, performance, store_id) = dry_run
        .resolve_ozon_dry_run(
            &ozon_claim,
            &mut recording_secret_lookup(&resolved_keys, &secrets),
        )
        .unwrap();
    assert!(
        seller.is_configured(&StoreId::from("1")) && performance.is_configured(&StoreId::from("1"))
    );
    assert_eq!(store_id, StoreId::from("1"));
    assert_eq!(
        *resolved_keys.borrow(),
        ["ID", "KEY", "PERF_ID", "PERF_SECRET"]
    );
    assert!(
        dry_run
            .resolve_ozon_dry_run(
                &claim("wb", crate::reporting::snapshot::Marketplace::Wildberries),
                &mut no_secrets,
            )
            .is_err()
    );

    let mixed_policy = file(
        "mixed-policy",
        r#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"diana","account_ids":["ozon"]},{"actor_id":"wb","account_ids":["wb"]}]}]}"#,
    );
    values[2] = (POLICY_PATH_ENV, mixed_policy.display().to_string());
    let mixed = config(&values).unwrap();
    let mut requested = Vec::new();
    assert!(
        mixed
            .resolve_ozon_dry_run(&ozon_claim, &mut |key| {
                requested.push(key.to_owned());
                secrets
                    .iter()
                    .find_map(|(entry, value)| (*entry == key).then(|| value.clone()))
            })
            .is_ok()
    );
    assert!(!requested.iter().any(|key| key == "WB_TOKEN"));

    for missing_key in ["ID", "KEY", "PERF_ID", "PERF_SECRET"] {
        assert!(
            mixed
                .resolve_ozon_dry_run(&ozon_claim, &mut |key| {
                    (key != missing_key).then(|| "present".to_owned())
                })
                .is_err(),
            "missing {missing_key}"
        );
    }
    assert!(
        mixed
            .resolve_ozon_dry_run(&ozon_claim, &mut |_| Some(String::new()))
            .is_err()
    );
    let expired_claim = CollectionClaim::for_test(
        "ozon",
        crate::reporting::snapshot::Marketplace::Ozon,
        Utc::now() - Duration::from_secs(1),
    );
    assert!(
        mixed
            .resolve_ozon_dry_run(&expired_claim, &mut no_secrets)
            .is_err()
    );
    assert!(
        rejected_keys.borrow().is_empty(),
        "rejected claims and credential files must not read environment secrets"
    );
}

#[test]
fn report_collector_rejects_the_dedicated_control_performance_identity() {
    let mut values = entries();
    let registry_path = PathBuf::from(&values[1].1);
    let mut registry: serde_json::Value =
        serde_json::from_slice(&fs::read(&registry_path).unwrap()).unwrap();
    registry["accounts"][0]["ozon"]["performance"]["control_executor_client_id_sha256"] =
        crate::config::credential_sha256("dedicated-control-client").into();
    fs::write(
        &registry_path,
        serde_json::to_vec_pretty(&registry).unwrap(),
    )
    .unwrap();
    values.push((MODE_ENV, "ozon_dry_run".to_owned()));
    let collector = config(&values).unwrap();
    let claim = claim("ozon", crate::reporting::snapshot::Marketplace::Ozon);
    let secrets = BTreeMap::from([
        ("PERF_ID", "dedicated-control-client"),
        ("PERF_SECRET", "performance-secret"),
        ("ID", "seller-client"),
        ("KEY", "seller-secret"),
    ]);
    let error = collector
        .resolve_ozon_dry_run(&claim, &mut |key| {
            secrets.get(key).map(|value| (*value).to_owned())
        })
        .expect_err("dedicated Control Performance Client-Id must be rejected")
        .to_string();
    assert!(
        error.contains("выделенный Control Performance Client-Id"),
        "{error}"
    );
}

#[test]
fn report_collector_rejects_another_accounts_control_performance_identity() {
    let mut values = entries();
    let registry_path = PathBuf::from(&values[1].1);
    let mut registry: serde_json::Value =
        serde_json::from_slice(&fs::read(&registry_path).unwrap()).unwrap();
    registry["accounts"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "id": "second-ozon",
            "organization": "Second Ozon",
            "marketplace": "ozon",
            "seller_client_id": "3",
            "manager_id": "diana",
            "ozon": {
                "store_id": "3",
                "client_id_env": "SECOND_ID",
                "api_key_env": "SECOND_KEY",
                "performance": {
                    "client_id_env": "SECOND_PERF_ID",
                    "client_secret_env": "SECOND_PERF_SECRET",
                    "control_executor_client_id_sha256":
                        crate::config::credential_sha256("cross-account-control-client")
                }
            }
        }));
    fs::write(
        &registry_path,
        serde_json::to_vec_pretty(&registry).unwrap(),
    )
    .unwrap();
    values.push((MODE_ENV, "ozon_dry_run".to_owned()));
    let collector = config(&values).unwrap();
    let claim = claim("ozon", crate::reporting::snapshot::Marketplace::Ozon);
    let secrets = BTreeMap::from([
        ("PERF_ID", "cross-account-control-client"),
        ("PERF_SECRET", "performance-secret"),
        ("ID", "seller-client"),
        ("KEY", "seller-secret"),
    ]);
    let error = collector
        .resolve_ozon_dry_run(&claim, &mut |key| {
            secrets.get(key).map(|value| (*value).to_owned())
        })
        .expect_err("another account's Control Performance Client-Id must be rejected")
        .to_string();
    assert!(
        error.contains("выделенный Control Performance Client-Id"),
        "{error}"
    );
    for sensitive in ["cross-account-control-client", "performance-secret"] {
        assert!(!error.contains(sensitive), "{error}");
    }
}

#[test]
fn explicit_wb_dry_run_resolves_only_policy_scoped_personal_token() {
    let rejected_keys = std::cell::RefCell::new(Vec::new());
    let mut no_secrets = recording_secret_lookup(&rejected_keys, &[]);
    let mixed_policy = file(
        "wb-policy",
        r#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"diana","account_ids":["ozon"]},{"actor_id":"wb","account_ids":["wb"]}]}]}"#,
    );
    let mut values = entries();
    values[2] = (POLICY_PATH_ENV, mixed_policy.display().to_string());
    values.push((MODE_ENV, "wb_dry_run".to_owned()));
    let dry_run = config(&values).unwrap();
    assert_eq!(dry_run.mode(), ReportCollectorMode::WbDryRun);
    let token = personal_wb_token();
    let mut resolved_keys = Vec::new();
    let wb_claim = claim("wb", crate::reporting::snapshot::Marketplace::Wildberries);
    let (client, account_id) = dry_run
        .resolve_wb_dry_run(&wb_claim, &mut |key| {
            resolved_keys.push(key.to_owned());
            (key == "WB_TOKEN").then(|| token.clone())
        })
        .unwrap();
    assert!(client.is_configured("wb"));
    assert_eq!(account_id, "wb");
    assert_eq!(resolved_keys, ["WB_TOKEN"]);

    let ozon_claim = claim("ozon", crate::reporting::snapshot::Marketplace::Ozon);
    assert!(
        dry_run
            .resolve_wb_dry_run(&ozon_claim, &mut no_secrets)
            .is_err()
    );
    assert!(
        dry_run
            .resolve_wb_dry_run(&wb_claim, &mut |_| None)
            .is_err()
    );
    let wrong_type = {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","typ":"JWT"}"#);
        let claims = URL_SAFE_NO_PAD.encode(br#"{"acc":1}"#);
        let signature = URL_SAFE_NO_PAD.encode([0_u8; 64]);
        format!("{header}.{claims}.{signature}")
    };
    assert!(
        dry_run
            .resolve_wb_dry_run(&wb_claim, &mut |_| Some(wrong_type.clone()))
            .is_err()
    );

    let disabled = config(&entries()).unwrap();
    assert!(
        disabled
            .resolve_wb_dry_run(&wb_claim, &mut no_secrets)
            .is_err()
    );
    assert!(
        rejected_keys.borrow().is_empty(),
        "rejected claims and credential files must not read environment secrets"
    );
}

#[test]
fn dry_runs_can_resolve_only_the_claimed_account_from_a_credential_directory() {
    let rejected_keys = std::cell::RefCell::new(Vec::new());
    let mut no_secrets = recording_secret_lookup(&rejected_keys, &[]);
    let mixed_policy = file(
        "directory-canary-policy",
        r#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER","managers":[{"actor_id":"diana","account_ids":["ozon"]},{"actor_id":"wb","account_ids":["wb"]}]}]}"#,
    );
    let credential_directory = directory("canary-credentials");
    for (name, value) in [
        ("ID", "client-id".to_owned()),
        ("KEY", "api-key".to_owned()),
        ("PERF_ID", "performance-client-id".to_owned()),
        ("PERF_SECRET", "performance-client-secret".to_owned()),
        ("WB_TOKEN", personal_wb_token()),
    ] {
        fs::write(credential_directory.join(name), value).unwrap();
    }

    let mut values = entries();
    values[2] = (POLICY_PATH_ENV, mixed_policy.display().to_string());
    values.extend([
        (MODE_ENV, "ozon_dry_run".to_owned()),
        (
            CREDENTIAL_DIRECTORY_ENV,
            credential_directory.display().to_string(),
        ),
    ]);
    let ozon = config(&values).unwrap();
    let (seller, performance, store) = ozon
        .resolve_ozon_dry_run(
            &claim("ozon", crate::reporting::snapshot::Marketplace::Ozon),
            &mut no_secrets,
        )
        .unwrap();
    assert!(seller.is_configured(&store) && performance.is_configured(&store));

    values.retain(|(key, _)| *key != MODE_ENV);
    values.push((MODE_ENV, "wb_dry_run".to_owned()));
    let wb = config(&values).unwrap();
    let (client, account) = wb
        .resolve_wb_dry_run(
            &claim("wb", crate::reporting::snapshot::Marketplace::Wildberries),
            &mut no_secrets,
        )
        .unwrap();
    assert_eq!(account, "wb");
    assert!(client.is_configured("wb"));

    fs::remove_file(credential_directory.join("WB_TOKEN")).unwrap();
    assert!(
        wb.resolve_wb_dry_run(
            &claim("wb", crate::reporting::snapshot::Marketplace::Wildberries),
            &mut no_secrets,
        )
        .is_err()
    );
    assert!(
        rejected_keys.borrow().is_empty(),
        "rejected claims and credential files must not read environment secrets"
    );
}
