use super::*;
use crate::control::wb_launch::continuation::{Approval, CAMPAIGN_ID};

fn continued(status: i32, bid: u64) -> Value {
    details(CAMPAIGN_ID, NAME, &NMS, status, bid)
}

pub(super) fn fixture() -> Fixture {
    let mut fixture = recreate_workflow::fixture();
    let mut source: WbAutomationPolicy = read_policy_json(&fixture.manifest.source_policy).unwrap();
    source.hard_drr_basis_points = 1500;
    source.autonomous_pacing = crate::control::WbAutomationPacingMode::TrafficFrontierV4;
    fixture.manifest.source_policy_sha256 = journal::digest(&serde_json::to_vec(&source).unwrap());
    private_json(
        &fixture.manifest.source_policy,
        &serde_json::to_value(&source).unwrap(),
    );
    private_json(
        &fixture.path,
        &serde_json::to_value(&fixture.manifest).unwrap(),
    );
    let journal = fixture.journal();
    journal
        .attempt("create", &json!({"mock_preflight":true}))
        .unwrap();
    journal
        .receipt("create", &json!({"campaign_id":CAMPAIGN_ID,"wb_http":200}))
        .unwrap();
    journal
        .receipt(
            "policy",
            &serde_json::to_value(fixture.manifest.target_policy(&source, CAMPAIGN_ID)).unwrap(),
        )
        .unwrap();
    drop(journal);
    let anchor = |name| {
        let value: Value = read_private_json(
            &fixture
                .root
                .join(format!("ofk_region_wb-Nexus/recreate-{name}.json")),
        )
        .unwrap();
        journal::digest(&serde_json::to_vec(&value).unwrap())
    };
    let approval = Approval {
        campaign_id: CAMPAIGN_ID,
        previous_manifest_sha256: anchor("manifest"),
        previous_attempt_sha256: anchor("create-attempted"),
        previous_receipt_sha256: anchor("create-receipt"),
        previous_policy_sha256: anchor("policy-receipt"),
    };
    fixture.manifest.scope = LaunchScope::FundAndStart;
    fixture.manifest.budget_rubles = 1000;
    fixture.manifest.recreate = None;
    fixture.manifest.continue_created = Some(approval);
    fixture.manifest.authorized_at = Utc::now();
    fixture.manifest.expires_at = Utc::now() + chrono::Duration::hours(1);
    fixture.manifest.authorization_reference = "test/confirmed-continue-1000".into();
    private_json(
        &fixture.path,
        &serde_json::to_value(&fixture.manifest).unwrap(),
    );
    fixture
}

fn anchors(fixture: &Fixture) -> Vec<Vec<u8>> {
    [
        "manifest",
        "create-attempted",
        "recreate-manifest",
        "recreate-create-attempted",
        "recreate-create-receipt",
        "recreate-policy-receipt",
    ]
    .iter()
    .map(|name| {
        fs::read(
            fixture
                .root
                .join(format!("ofk_region_wb-Nexus/{name}.json")),
        )
        .unwrap()
    })
    .collect()
}

fn prepared_journal(fixture: &Fixture, operator: &Operator) -> Journal {
    let journal = fixture.journal();
    journal
        .receipt(
            "policy",
            &serde_json::to_value(operator.target_policy(CAMPAIGN_ID)).unwrap(),
        )
        .unwrap();
    journal
}

#[tokio::test]
async fn continuation_prepare_preserves_every_anchor_and_never_creates_or_funds() {
    let fixture = fixture();
    let before = anchors(&fixture);
    let mut responses = preflight(&fixture);
    // The already-created Nexus is excluded from its own overlap check.
    responses[6].1 = json!({"adverts":[{"status":9,"advert_list":[{"advertId":SOURCE}]},
        {"status":4,"advert_list":[{"advertId":CAMPAIGN_ID}]}]});
    responses.extend([
        (200, continued(4, 102)),
        (200, json!({"total":0})),
        (200, minimums()),
    ]);
    let count = responses.len();
    let (operator, receiver) = fixture.operator(responses);
    let journal = fixture.journal();
    assert!(operator.create(&journal).await.is_err());
    let result = operator.prepare_continuation(&journal).await.unwrap();
    assert_eq!(result["marketplace_write_sent"], false);
    assert_eq!(result["robot_installed"], false);
    assert_eq!(journal.campaign_id().unwrap(), CAMPAIGN_ID);
    assert_eq!(anchors(&fixture), before);
    assert!(!journal.attempted("bids"));
    assert!(!journal.attempted("fund"));
    assert!(!journal.attempted("start"));
    assert!(journal.attempt("create", &json!({})).is_err());
    assert!(
        requests(&receiver, count)
            .iter()
            .all(|request| !request.contains("budget/deposit")
                && !request.contains("/adv/v0/start")
                && !request.contains("/adv/v2/seacat/save-ad")
                && !request.starts_with("PATCH "))
    );
}

#[tokio::test]
async fn changed_continuation_evidence_fails_preflight_before_network() {
    let fixture = fixture();
    private_json(
        &fixture
            .root
            .join("ofk_region_wb-Nexus/recreate-create-receipt.json"),
        &json!({"campaign_id":CAMPAIGN_ID + 1,"wb_http":200}),
    );
    let (operator, receiver) = fixture.operator(vec![]);
    assert!(operator.preflight(None).await.is_err());
    assert!(receiver.try_recv().is_err());
    assert!(
        !fixture
            .root
            .join("ofk_region_wb-Nexus/continue-manifest.json")
            .exists()
    );
}

#[tokio::test]
async fn continuation_initial_bids_require_prepare_and_are_written_once() {
    let fixture = fixture();
    let mut responses = preflight(&fixture);
    responses.extend([
        (200, continued(4, 102)),
        (200, json!({"total":0})),
        (200, minimums()),
        (200, continued(4, 102)),
        (200, json!({"total":0})),
        (200, json!({})),
        (200, continued(4, 922)),
        (200, continued(4, 922)),
        (200, json!({"total":0})),
    ]);
    let count = responses.len();
    let (operator, receiver) = fixture.operator(responses);
    let journal = fixture.journal();
    assert!(operator.bids(&journal).await.is_err());
    assert!(receiver.try_recv().is_err());
    journal
        .receipt(
            "policy",
            &serde_json::to_value(operator.target_policy(CAMPAIGN_ID)).unwrap(),
        )
        .unwrap();
    operator.bids(&journal).await.unwrap();
    assert!(operator.bids(&journal).await.is_err());
    let sent = requests(&receiver, count);
    assert_eq!(
        sent.iter()
            .filter(|request| request.starts_with("PATCH "))
            .count(),
        1
    );
    assert!(!journal.attempted("fund"));
}

#[tokio::test]
async fn continuation_funding_preserves_once_only_fence_for_success_and_ambiguity() {
    for status in [200, 504] {
        let fixture = fixture();
        let before = anchors(&fixture);
        let mut responses = vec![(200, continued(4, 922))];
        responses.extend(preflight(&fixture));
        responses.extend([
            (200, minimums()),
            (200, continued(4, 922)),
            (200, json!({"total":0})),
            (200, json!({"balance":0,"net":1000})),
            (status, json!({"total":1000})),
        ]);
        if status == 200 {
            responses.extend([
                (200, json!({"total":1000})),
                (200, continued(4, 922)),
                (200, continued(4, 922)),
                (200, json!({"total":1000})),
            ]);
        }
        let count = responses.len();
        let (operator, receiver) = fixture.operator(responses);
        private_json(
            &fixture.manifest.robot_policy,
            &serde_json::to_value(operator.target_policy(CAMPAIGN_ID)).unwrap(),
        );
        let journal = prepared_journal(&fixture, &operator);
        journal
            .receipt(
                "bids",
                &json!({"campaign_id":CAMPAIGN_ID,"bids_kopecks":fixture.manifest.bids_kopecks}),
            )
            .unwrap();
        assert_eq!(operator.fund(&journal).await.is_ok(), status == 200);
        assert_eq!(journal.has_receipt("fund"), status == 200);
        if status == 200 {
            assert_eq!(
                journal.require_receipt("create").unwrap(),
                json!({"campaign_id":CAMPAIGN_ID,"wb_http":200})
            );
            assert_eq!(
                journal.require_receipt("fund-response").unwrap(),
                json!({"wb_http":200,"total":1000})
            );
            assert_eq!(journal.require_receipt("fund").unwrap()["type"], 1);
        }
        assert!(operator.fund(&journal).await.is_err());
        drop(journal);
        let journal = fixture.journal();
        assert!(operator.fund(&journal).await.is_err());
        assert_eq!(anchors(&fixture), before);
        let sent = requests(&receiver, count);
        let deposits = sent
            .iter()
            .filter(|r| r.starts_with("POST /adv/v1/budget/deposit?"))
            .collect::<Vec<_>>();
        assert_eq!(deposits.len(), 1);
        assert!(deposits[0].contains(&format!("id={CAMPAIGN_ID}")));
        let body: Value =
            serde_json::from_str(deposits[0].split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(body, json!({"sum":1000,"type":1,"return":true}));
        assert!(!sent.iter().any(|r| r.contains("/adv/v0/start")));
    }
}

#[tokio::test]
async fn netting_read_never_substitutes_prepaid_balance_or_bonuses() {
    for (data, expected) in [
        (json!({"balance":0,"net":549_814}), Some(549_814)),
        (json!({"balance":549_814,"net":0,"bonus":10_000}), Some(0)),
        (json!({"balance":549_814,"bonus":10_000}), None),
        (json!({"balance":549_814,"net":null}), None),
        (json!({"balance":549_814,"net":-1}), None),
    ] {
        let fixture = Fixture::new(LaunchScope::FundAndStart);
        let (operator, receiver) = fixture.operator(vec![(200, data)]);
        assert_eq!(operator.balance().await.ok(), expected);
        assert!(requests(&receiver, 1)[0].starts_with("GET /adv/v1/balance "));
    }
}

#[test]
fn continuation_cannot_expand_scope_recreate_or_renew_immutable_money_attempt() {
    let fixture = fixture();
    let source: WbAutomationPolicy = read_policy_json(&fixture.manifest.source_policy).unwrap();
    for key in [
        "campaign_id",
        "previous_manifest_sha256",
        "previous_attempt_sha256",
        "previous_receipt_sha256",
        "previous_policy_sha256",
    ] {
        let mut changed = serde_json::to_value(&fixture.manifest).unwrap();
        changed["continue_created"][key] = if key == "campaign_id" {
            json!(CAMPAIGN_ID + 1)
        } else {
            json!("0".repeat(64))
        };
        assert!(Journal::open(&fixture.root, &changed, true).is_err());
    }
    for (field, value) in [
        ("budget_rubles", json!(1001)),
        ("funding_type", json!(0)),
        ("funding_type", json!(3)),
        ("campaign_name", json!("Одуванчик")),
        ("scope", json!("create_only")),
        (
            "authorization_reference",
            json!("test/reviewed-replacement"),
        ),
        ("authorized_at", json!("2026-01-01T00:00:00Z")),
    ] {
        let mut changed = serde_json::to_value(&fixture.manifest).unwrap();
        changed[field] = value;
        assert!(Journal::open(&fixture.root, &changed, true).is_err());
    }
    let mut changed = fixture.manifest.clone();
    changed.bids_kopecks.insert(NMS[0], 923);
    assert!(changed.validate(&source, Utc::now(), false).is_err());
    let mut looser = source;
    looser.hard_drr_basis_points = 2500;
    let mut changed = fixture.manifest.clone();
    changed.source_policy_sha256 = journal::digest(&serde_json::to_vec(&looser).unwrap());
    assert!(changed.validate(&looser, Utc::now(), false).is_err());
    let journal = fixture.journal();
    assert!(
        Journal::open(
            &fixture.root,
            &serde_json::to_value(&fixture.manifest).unwrap(),
            true
        )
        .is_err()
    );
    journal
        .attempt(
            "fund",
            &json!({"sum":1000,"type":1,"campaign_id":CAMPAIGN_ID}),
        )
        .unwrap();
    drop(journal);
    let original: Value = read_private_json(
        &fixture
            .root
            .join("ofk_region_wb-Nexus/recreate-manifest.json"),
    )
    .unwrap();
    assert!(Journal::open(&fixture.root, &original, true).is_err());
    // Read-only historical reconciliation remains available.
    assert_eq!(
        Journal::open(&fixture.root, &original, false)
            .unwrap()
            .campaign_id()
            .unwrap(),
        CAMPAIGN_ID
    );
    let mut renewed = fixture.manifest.clone();
    renewed.authorization_reference = "test/new-approval-cannot-reset".into();
    assert!(Journal::open(&fixture.root, &serde_json::to_value(renewed).unwrap(), true).is_err());
    let journal = fixture.journal();
    assert!(journal.assert_not_attempted("fund").is_err());
    assert!(journal.attempt("fund", &json!({})).is_err());
}

#[test]
fn continuation_fences_prior_partial_or_dangling_trading_evidence() {
    for name in [
        "fund-attempted",
        "fund-response-receipt",
        "recreate-fund-attempted",
        "recreate-fund-receipt",
        "recreate-bids-attempted",
        "recreate-start-attempted",
    ] {
        let fixture = fixture();
        fs::write(
            fixture
                .root
                .join(format!("ofk_region_wb-Nexus/{name}.json")),
            b"{",
        )
        .unwrap();
        assert!(
            Journal::open(
                &fixture.root,
                &serde_json::to_value(&fixture.manifest).unwrap(),
                true
            )
            .is_err()
        );
    }
    let fixture = fixture();
    std::os::unix::fs::symlink(
        "missing",
        fixture
            .root
            .join("ofk_region_wb-Nexus/recreate-fund-attempted.json"),
    )
    .unwrap();
    assert!(
        Journal::open(
            &fixture.root,
            &serde_json::to_value(&fixture.manifest).unwrap(),
            true
        )
        .is_err()
    );
}

#[tokio::test]
async fn continuation_funding_still_requires_installed_protection() {
    let fixture = fixture();
    let (operator, receiver) = fixture.operator(vec![(200, continued(4, 922))]);
    let journal = prepared_journal(&fixture, &operator);
    journal
        .receipt(
            "bids",
            &json!({"campaign_id":CAMPAIGN_ID,"bids_kopecks":fixture.manifest.bids_kopecks}),
        )
        .unwrap();
    assert!(operator.fund(&journal).await.is_err());
    assert!(!journal.attempted("fund"));
    assert_eq!(requests(&receiver, 1).len(), 1);
}
