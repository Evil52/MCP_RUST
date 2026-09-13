use super::*;

#[test]
fn overlap_scan_uses_complete_bounded_nonfinished_campaign_ids() {
    let listing = json!({"adverts":[
        {"status":-1}, {"status":7}, {"status":8},
        {"status":9,"advert_list":[{"advertId":11},{"advertId":12},{"advertId":11}]},
        {"status":4,"advert_list":[{"advertId":13}]},
        {"status":11,"advert_list":[{"advertId":14}]}
    ]});
    assert_eq!(
        nonfinished_campaign_ids(&listing, Some(12)).unwrap(),
        BTreeSet::from([11, 13, 14])
    );
    assert_eq!(
        nonfinished_campaign_ids(&listing, None).unwrap(),
        BTreeSet::from([11, 12, 13, 14])
    );
    for listing in [
        json!({}),
        json!({"adverts":[{}]}),
        json!({"adverts":[{"status":9}]}),
        json!({"adverts":[{"status":9,"advert_list":[{}]}]}),
    ] {
        assert!(nonfinished_campaign_ids(&listing, None).is_err());
    }
    let listing = json!({"adverts":[{"status":9,"advert_list":(1..=501).map(|id| json!({"advertId":id})).collect::<Vec<_>>()}]});
    assert_eq!(
        nonfinished_campaign_ids(&listing, Some(501)).unwrap().len(),
        500
    );
    assert!(
        nonfinished_campaign_ids(&listing, None)
            .unwrap_err()
            .to_string()
            .contains("bounded scan")
    );
}

fn receipt_failure_responses(fixture: &Fixture, stage: &str) -> Vec<(u16, Value)> {
    let mut responses = if stage == "bids" {
        vec![
            (200, target(4, 500)),
            (200, json!({"total":0})),
            (200, minimums()),
            (200, target(4, 500)),
            (200, json!({"total":0})),
            (200, json!({})),
            (200, target(4, 922)),
        ]
    } else {
        preflight(fixture)
    };
    if stage == "fund" {
        responses.extend([
            (200, minimums()),
            (200, target(11, 922)),
            (200, json!({"total":0})),
            (200, json!({"balance":1000})),
            (200, json!({"total":1000})),
            (200, json!({"total":1000})),
            (200, target(11, 922)),
        ]);
    }
    responses
}

#[tokio::test]
async fn receipt_persistence_failure_never_repeats_a_completed_marketplace_write() {
    for stage in ["bids", "fund"] {
        let fixture = Fixture::new(LaunchScope::FundAndStart);
        let responses = receipt_failure_responses(&fixture, stage);
        let count = responses.len();
        let receipt_path = fixture
            .root
            .join("ofk_region_wb-Nexus")
            .join(format!("{stage}-receipt.json"));
        let (operator, receiver) = fixture.operator_with_hook(responses, move |index| {
            if index + 1 == count {
                fs::create_dir(&receipt_path).unwrap();
            }
        });
        let journal = fixture.journal();
        journal
            .receipt("create", &json!({"campaign_id":ID}))
            .unwrap();
        if stage == "fund" {
            journal.receipt("bids", &json!({})).unwrap();
        }
        let result = if stage == "bids" {
            operator.bids(&journal).await
        } else {
            operator.fund(&journal).await
        };
        assert!(result.is_err());
        assert!(journal.attempted(stage));
        assert!(journal.require_receipt(stage).is_err());
        let repeat = if stage == "bids" {
            operator.bids(&journal).await
        } else {
            operator.fund(&journal).await
        };
        assert!(
            repeat
                .unwrap_err()
                .to_string()
                .contains("already attempted")
        );
        let sent = requests(&receiver, count);
        assert_eq!(
            sent.iter()
                .filter(|request| request.starts_with("PATCH ")
                    || request.starts_with("POST /adv/v1/budget/deposit?"))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn cli_requires_a_safe_journal_before_write_or_readback() {
    let fixture = Fixture::new(LaunchScope::CreateOnly);
    assert!(
        run_wb_campaign_launch("reconcile", &fixture.path)
            .await
            .is_err()
    );
    fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o777)).unwrap();
    let error = run_wb_campaign_launch("create", &fixture.path)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("journal root"));
    assert!(!fixture.root.join("ofk_region_wb-Nexus").exists());
}

#[test]
fn loading_launch_rejects_invalid_read_and_write_proxy_configuration() {
    for reader in [true, false] {
        let mut fixture = Fixture::new(LaunchScope::CreateOnly);
        if reader {
            fixture.manifest.reader_proxy = "http://[".to_owned();
        } else {
            fixture.manifest.writer_proxy = "http://[".to_owned();
        }
        private_json(
            &fixture.path,
            &serde_json::to_value(&fixture.manifest).unwrap(),
        );
        assert!(Operator::load(&fixture.path, false).is_err());
    }
}

#[tokio::test]
async fn read_only_cli_reconcile_loads_only_the_reader_for_a_confirmed_id() {
    let fixture = Fixture::new(LaunchScope::CreateOnly);
    let journal = fixture.journal();
    journal
        .receipt("create", &json!({"campaign_id":ID}))
        .unwrap();
    drop(journal);
    fs::remove_file(&fixture.manifest.source_policy).unwrap();
    fs::remove_file(&fixture.manifest.writer_token).unwrap();
    let error = run_wb_campaign_launch("reconcile", &fixture.path)
        .await
        .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<crate::wb::WbError>()
            .map(crate::wb::WbError::kind),
        Some(crate::wb::WbErrorKind::Network)
    );
    let journal = Journal::open(
        &fixture.root,
        &serde_json::to_value(&fixture.manifest).unwrap(),
        false,
    )
    .unwrap();
    assert!(!journal.attempted("fund"));
    assert!(!journal.attempted("start"));
}

#[tokio::test]
async fn read_only_reconcile_rejects_an_invalid_proxy_before_any_request() {
    let mut fixture = Fixture::new(LaunchScope::CreateOnly);
    fixture.manifest.reader_proxy = "http://[".to_owned();
    private_json(
        &fixture.path,
        &serde_json::to_value(&fixture.manifest).unwrap(),
    );
    let journal = fixture.journal();
    journal
        .receipt("create", &json!({"campaign_id":ID}))
        .unwrap();
    drop(journal);
    let error = run_wb_campaign_launch("reconcile", &fixture.path)
        .await
        .unwrap_err();
    assert!(
        error
            .downcast_ref::<reqwest::Error>()
            .is_some_and(reqwest::Error::is_builder)
    );
    assert_eq!(fixture.journal().maybe_campaign_id().unwrap(), Some(ID));
}

#[tokio::test]
async fn cli_preflight_fails_at_its_explicit_local_proxy_without_journaling() {
    let fixture = Fixture::new(LaunchScope::CreateOnly);
    assert!(
        run_wb_campaign_launch("preflight", &fixture.path)
            .await
            .is_err()
    );
    assert!(!fixture.root.join("ofk_region_wb-Nexus").exists());
}

async fn assert_start_bootstrap_refuses_unready_dependencies(mode: &str) {
    let fixture = Fixture::new(LaunchScope::FundAndStart);
    let (operator, receiver) = fixture.operator(vec![(200, target(11, 922))]);
    let mut policy = operator.target_policy(ID);
    if mode == "policy-drift" {
        policy.campaign_name = "changed".to_owned();
    }
    private_json(
        &fixture.manifest.robot_policy,
        &serde_json::to_value(policy).unwrap(),
    );
    let journal = fixture.journal();
    journal
        .receipt("create", &json!({"campaign_id":ID}))
        .unwrap();
    journal.receipt("fund", &json!({})).unwrap();
    if mode == "missing-reader" {
        fs::remove_file(&fixture.manifest.reader_token).unwrap();
    }
    let error = operator.start(&journal).await.unwrap_err();
    match mode {
        "policy-drift" => assert!(error.to_string().contains("differs from reviewed copy")),
        "missing-reader" => assert!(error.to_string().contains("WB_AUTOMATION_READ_TOKEN_FILE")),
        "missing-db" => assert!(error.to_string().contains("PostgreSQL is required")),
        "invalid-db" => assert!(error.to_string().contains("invalid automation database")),
        _ => {
            assert_eq!(mode, "valid-db");
            assert_eq!(
                error
                    .downcast_ref::<crate::wb::WbError>()
                    .map(crate::wb::WbError::kind),
                Some(crate::wb::WbErrorKind::Network),
                "valid startup reaches only the explicit loopback proxy after its database checks"
            );
        }
    }
    assert!(!journal.attempted("start"));
    assert!(requests(&receiver, 1)[0].starts_with("GET /api/advert/v2/adverts?"));
}

#[tokio::test]
async fn startup_bootstrap_requires_policy_and_database_before_any_write() {
    const CHILD: &str = "WB_START_BOOTSTRAP_BOUNDARY_CHILD";
    if let Ok(mode) = std::env::var(CHILD) {
        assert_start_bootstrap_refuses_unready_dependencies(&mode).await;
        return;
    }
    let database_url = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL").ok();
    for (mode, url) in [
        ("policy-drift", None),
        ("missing-reader", None),
        ("missing-db", None),
        ("invalid-db", Some("invalid database url".to_owned())),
    ]
    .into_iter()
    .chain(database_url.map(|url| ("valid-db", Some(url))))
    {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.args(["--exact", "control::wb_launch::workflow_tests::boundaries::startup_bootstrap_requires_policy_and_database_before_any_write", "--test-threads=1"]).env_clear().env(CHILD, mode);
        if let Some(url) = url {
            command.env("WB_AUTOMATION_DATABASE_URL", url);
        }
        if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
            command.env("LLVM_PROFILE_FILE", profile);
        }
        let output = tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap();
        assert!(output.status.success(), "{output:?}");
    }
}
