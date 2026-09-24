use super::*;

fn reusable(name: &str, nms: &[u64]) -> Fixture {
    let mut fixture = Fixture::new(LaunchScope::CreateOnly);
    fixture.manifest.version = 2;
    fixture.manifest.account_id = "second_wb".to_owned();
    fixture.manifest.campaign_name = name.to_owned();
    fixture.manifest.bids_kopecks = nms.iter().map(|&nm| (nm, 700)).collect();
    let mut policy: WbAutomationPolicy = read_policy_json(&fixture.manifest.source_policy).unwrap();
    policy.account_id.clone_from(&fixture.manifest.account_id);
    policy.min_bid_kopecks = 700;
    policy.autonomous_pacing = crate::control::WbAutomationPacingMode::TrafficFrontierV4;
    policy.traffic_frontier_bid_kopecks = Some(700);
    policy.max_bid_kopecks = 1200;
    fixture.manifest.source_policy_sha256 = journal::digest(&serde_json::to_vec(&policy).unwrap());
    private_json(
        &fixture.manifest.source_policy,
        &serde_json::to_value(&policy).unwrap(),
    );
    let mut registry: Value = read_private_json(&fixture.manifest.registry).unwrap();
    registry["accounts"][0]["id"] = json!(fixture.manifest.account_id);
    registry["actors"].as_array_mut().unwrap().push(
        json!({"id":"reviewer","name":"Reviewer","role":"admin","oidc":{"username":"reviewer"}}),
    );
    private_json(&fixture.manifest.registry, &registry);
    fixture.manifest.manual_control = Some(setup::ManualControl {
        approver_actor_ids: vec!["reviewer".to_owned()],
        max_delta_percent: 25,
        action_limits: crate::control::WbActionLimits {
            max_actions_per_hour: 4,
            max_actions_per_day: 24,
            cooldown_seconds: 300,
            max_cumulative_abs_delta_kopecks_per_day: 10000,
        },
    });
    private_json(
        &fixture.path,
        &serde_json::to_value(&fixture.manifest).unwrap(),
    );
    fixture
}

fn reads(fixture: &Fixture) -> Vec<(u16, Value)> {
    let nms = fixture.manifest.nm_ids();
    let mut responses = nms
        .iter()
        .map(|nm| (200, json!({"cards":[{"nmID":nm,"subjectID":4263}]})))
        .collect::<Vec<_>>();
    responses.extend([
        (200,json!({"all":0,"adverts":[]})),
        (200,json!({"data":{"items":nms.iter().map(|nm| json!({"nmId":nm,"warehouseId":1,"quantity":25})).collect::<Vec<_>>()}})),
    ]);
    if fixture.manifest.budget_rubles > 0 {
        responses.push((200, json!({"net":fixture.manifest.budget_rubles})));
    }
    responses
}

#[tokio::test]
async fn different_accounts_names_products_create_without_source_campaign_and_initialize_bids() {
    for (name, nms, id) in [("Весна", vec![111, 222], 10001), ("Лето", vec![333], 10002)] {
        let fixture = reusable(name, &nms);
        let mut responses = reads(&fixture);
        responses.extend(reads(&fixture)); // Repeat under the final paced permit.
        responses.extend([
            (200,json!(id)),(200,details(id,name,&nms,4,102)),(200,json!({"total":0})),
            (200,details(id,name,&nms,4,102)),(200,json!({"total":0})),
            (200,details(id,name,&nms,4,102)),(200,json!({"total":0})),
            (200,json!({"bids":nms.iter().map(|nm| json!({"nm_id":nm,"bids":[{"type":"search","value":102}]})).collect::<Vec<_>>()})),
            (200,details(id,name,&nms,4,102)),(200,json!({"total":0})),
            (200,json!({})),(200,details(id,name,&nms,4,700)),
            (200,details(id,name,&nms,4,700)),(200,json!({"total":0})),
        ]);
        let count = responses.len();
        let (operator, receiver) = fixture.operator(responses);
        let journal = fixture.journal();
        assert_eq!(operator.create(&journal).await.unwrap()["campaign_id"], id);
        assert_eq!(
            operator.bids(&journal).await.unwrap()["bids_kopecks"][nms[0].to_string()],
            700
        );
        assert!(operator.create(&journal).await.is_err());
        assert!(operator.bids(&journal).await.is_err());
        let sent = requests(&receiver, count);
        let create = sent
            .iter()
            .find(|r| r.starts_with("POST /adv/v2/seacat/save-ad "))
            .unwrap();
        let body: Value = serde_json::from_str(create.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(body["name"], name);
        assert_eq!(body["nms"], json!(nms));
        assert_eq!(
            sent.iter()
                .filter(|r| r.starts_with("PATCH /api/advert/v1/bids "))
                .count(),
            1
        );
        assert!(!sent.iter().any(|r| r.contains(&SOURCE.to_string())
            || r.contains("budget/deposit")
            || r.contains("/adv/v0/start")));
        assert!(receiver.try_recv().is_err());
        drop(journal);
        let exported = export_wb_campaign(&fixture.path).unwrap();
        assert_eq!(exported["campaign_id"], id);
        assert_eq!(export_wb_campaign(&fixture.path).unwrap(), exported);
        let policy: WbAutomationPolicy = read_private_json(&fixture.manifest.robot_policy).unwrap();
        assert_eq!(
            (policy.min_bid_kopecks, policy.max_bid_kopecks),
            (700, 1200)
        );
        assert_eq!(policy.campaign_id, id);
        let initial: Value = read_private_json(&fixture.root.join("initial-state.json")).unwrap();
        assert_eq!(initial["campaign_id"], id);
        assert_eq!(
            initial["policy_sha256"],
            journal::digest(&serde_json::to_vec(&policy).unwrap())
        );
    }
}

#[tokio::test]
async fn uncertain_create_is_fenced_and_new_authorization_cannot_reset_it() {
    let mut fixture = reusable("Неопределённая", &[111]);
    let mut responses = reads(&fixture);
    responses.extend(reads(&fixture));
    responses.push((502, json!({})));
    let count = responses.len();
    let (operator, receiver) = fixture.operator(responses);
    let journal = fixture.journal();
    assert!(operator.create(&journal).await.is_err());
    assert!(operator.create(&journal).await.is_err());
    drop(journal);
    fixture.manifest.authorization_reference = "another/approval".to_owned();
    assert!(
        Journal::open(
            &fixture.root,
            &serde_json::to_value(&fixture.manifest).unwrap(),
            true
        )
        .is_err()
    );
    assert_eq!(
        requests(&receiver, count)
            .iter()
            .filter(|r| r.starts_with("POST /adv/v2/seacat/save-ad "))
            .count(),
        1
    );
}

#[test]
fn account_lock_serializes_different_names_and_legacy_identity_cannot_be_reset() {
    let fixture = reusable("First", &[111]);
    let journal = fixture.journal();
    let mut other = fixture.manifest.clone();
    other.campaign_name = "Second".to_owned();
    assert!(Journal::open(&fixture.root, &serde_json::to_value(&other).unwrap(), true).is_err());
    drop(journal);
    let other_journal =
        Journal::open(&fixture.root, &serde_json::to_value(&other).unwrap(), true).unwrap();
    drop(other_journal);
    let legacy = json!({"account_id":ACCOUNT,"campaign_name":NAME});
    let v2 = json!({"version":2,"account_id":ACCOUNT,"campaign_name":NAME});
    assert_eq!(
        journal::directory_name(&legacy).unwrap(),
        journal::directory_name(&v2).unwrap()
    );
}

#[test]
fn generic_manifest_rejects_legacy_recovery_wrong_account_and_out_of_range_bids() {
    let fixture = reusable("Valid", &[111]);
    let policy = read_policy_json(&fixture.manifest.source_policy).unwrap();
    fixture
        .manifest
        .validate(&policy, Utc::now(), false)
        .unwrap();
    for mutate in [
        |m: &mut Manifest| {
            m.version = 3;
        },
        |m: &mut Manifest| {
            m.account_id = "other".to_owned();
        },
        |m: &mut Manifest| {
            m.bids_kopecks.insert(111, 699);
        },
        |m: &mut Manifest| {
            m.bids_kopecks.insert(111, 1201);
        },
        |m: &mut Manifest| {
            m.campaign_name = " bad ".to_owned();
        },
        |m: &mut Manifest| {
            m.recreate = Some(RecreateApproval {
                previous_manifest_sha256: "a".repeat(64),
                previous_attempt_sha256: "b".repeat(64),
            });
        },
    ] {
        let mut changed = fixture.manifest.clone();
        mutate(&mut changed);
        assert!(changed.validate(&policy, Utc::now(), false).is_err());
    }
}

#[test]
fn one_profile_prepares_multiple_campaigns_without_credentials_and_never_overwrites() {
    let fixture = reusable("Template", &[111]);
    let profile = fixture.root.join("profile.json");
    private_json(
        &profile,
        &json!({"version":1,"account_id":fixture.manifest.account_id,
        "actor_id":fixture.manifest.actor_id,"registry":fixture.manifest.registry,
        "reader_token":fixture.manifest.reader_token,"writer_token":fixture.manifest.writer_token,
        "reader_proxy":fixture.manifest.reader_proxy,"writer_proxy":fixture.manifest.writer_proxy,
        "allow_broad_reader":false,"robot_template":fixture.manifest.source_policy,
        "journal_directory":fixture.root,"campaigns_directory":fixture.root,
        "max_initial_budget_rubles":2000,"manual_control":fixture.manifest.manual_control}),
    );
    fs::remove_file(&fixture.manifest.reader_token).unwrap();
    fs::remove_file(&fixture.manifest.writer_token).unwrap();
    let request_path = fixture.root.join("request.json");
    let mut request = json!({"campaign_name":"Prepared one","bids_kopecks":{"123":700},"budget_rubles":1500,
        "authorization_reference":"test/generic-approval","authorized_at":Utc::now()-chrono::Duration::minutes(1),
        "expires_at":Utc::now()+chrono::Duration::hours(1),"robot_authorization_expires_at":Utc::now()+chrono::Duration::days(10)});
    private_json(&request_path, &request);
    let first = prepare_wb_campaign(&profile, &request_path).unwrap();
    assert_eq!(first["marketplace_write_sent"], false);
    let manifest_path = Path::new(first["manifest"].as_str().unwrap());
    let bytes = fs::read(manifest_path).unwrap();
    assert!(prepare_wb_campaign(&profile, &request_path).is_err());
    assert_eq!(fs::read(manifest_path).unwrap(), bytes);
    request["campaign_name"] = json!("Prepared two");
    request["bids_kopecks"] = json!({"234":800});
    private_json(&request_path, &request);
    let second = prepare_wb_campaign(&profile, &request_path).unwrap();
    assert_ne!(first["manifest"], second["manifest"]);
    request["campaign_name"] = json!("Out of bounds");
    request["budget_rubles"] = json!(2001);
    private_json(&request_path, &request);
    assert!(prepare_wb_campaign(&profile, &request_path).is_err());
}

#[test]
fn enrollment_preserves_existing_policy_mode_and_targets_with_new_revision() {
    let fixture = reusable("Control campaign", &[111]);
    let policy: WbAutomationPolicy = read_policy_json(&fixture.manifest.source_policy).unwrap();
    let journal = fixture.journal();
    journal
        .receipt("create", &json!({"campaign_id":ID,"wb_http":200}))
        .unwrap();
    journal
        .receipt(
            "policy",
            &serde_json::to_value(fixture.manifest.target_policy(&policy, ID)).unwrap(),
        )
        .unwrap();
    journal
        .receipt(
            "bids",
            &json!({"campaign_id":ID,"bids_kopecks":fixture.manifest.bids_kopecks}),
        )
        .unwrap();
    drop(journal);
    export_wb_campaign(&fixture.path).unwrap();
    let current = fixture.root.join("current.json");
    let mut base: Value = read_private_json(&fixture.root.join("control-policy.json")).unwrap();
    base["revision"] = json!(9);
    base["mode"] = json!("disabled");
    base["actors"][0]["wb_promotion_bid_targets"][0]["advert_id"] = json!(42);
    private_json(&current, &base);
    let candidate = fixture.root.join("candidate.json");
    let result = enroll_wb_campaign(&fixture.path, &current, &candidate).unwrap();
    assert_eq!(result["policy_revision"], 10);
    assert_eq!(result["mode"], "disabled");
    assert_eq!(read_private_json::<Value>(&current).unwrap(), base);
    let next: Value = read_private_json(&candidate).unwrap();
    assert_eq!(
        next["actors"][0]["wb_promotion_bid_targets"][0],
        base["actors"][0]["wb_promotion_bid_targets"][0]
    );
    assert_eq!(
        next["actors"][0]["wb_promotion_bid_targets"][1]["advert_id"],
        ID
    );
    assert!(
        enroll_wb_campaign(
            &fixture.path,
            &candidate,
            &fixture.root.join("duplicate.json")
        )
        .is_err()
    );
    assert!(!fixture.root.join("duplicate.json").exists());
}

#[tokio::test]
async fn reusable_funding_uses_exact_configured_amount_once_and_does_not_start() {
    let mut fixture = reusable("Funded", &[111]);
    fixture.manifest.scope = LaunchScope::FundAndStart;
    fixture.manifest.budget_rubles = 1500;
    private_json(
        &fixture.path,
        &serde_json::to_value(&fixture.manifest).unwrap(),
    );
    let campaign = details(ID, "Funded", &[111], 4, 700);
    let mut responses = vec![(200, campaign.clone())];
    responses.extend(reads(&fixture));
    responses.extend([
        (
            200,
            json!({"bids":[{"nm_id":111,"bids":[{"type":"search","value":102}]}]}),
        ),
        (200, campaign.clone()),
        (200, json!({"total":0})),
        (200, json!({"net":1500})),
        (200, json!({"total":1500})),
        (200, json!({"total":1500})),
        (200, campaign.clone()),
        (200, campaign),
        (200, json!({"total":1500})),
    ]);
    let count = responses.len();
    let (operator, receiver) = fixture.operator(responses);
    private_json(
        &fixture.manifest.robot_policy,
        &serde_json::to_value(operator.target_policy(ID)).unwrap(),
    );
    let journal = fixture.journal();
    journal
        .receipt("create", &json!({"campaign_id":ID}))
        .unwrap();
    journal.receipt("bids", &json!({})).unwrap();
    assert_eq!(
        operator.fund(&journal).await.unwrap()["budget_rubles"],
        1500
    );
    assert!(operator.fund(&journal).await.is_err());
    let sent = requests(&receiver, count);
    let deposits = sent
        .iter()
        .filter(|r| r.starts_with("POST /adv/v1/budget/deposit?"))
        .collect::<Vec<_>>();
    assert_eq!(deposits.len(), 1);
    let body: Value = serde_json::from_str(deposits[0].split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(body, json!({"sum":1500,"type":1,"return":true}));
    assert!(!sent.iter().any(|r| r.contains("/adv/v0/start")));
}
