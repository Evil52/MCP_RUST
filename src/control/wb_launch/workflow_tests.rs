//! Every marketplace response below is a loopback mock; no production tokens
//! or account state are loaded by these end-to-end operator-stage tests.
use super::*;
use crate::test_support::mock_http;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use std::{
    fs,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    sync::mpsc,
};

const SID: &str = "123e4567-e89b-42d3-a456-426614174000";
const ID: u64 = 1_984_773_211;

#[path = "start_workflow_tests.rs"]
mod startup;

#[path = "authorization_workflow_tests.rs"]
mod authorization;

struct Fixture {
    root: PathBuf,
    manifest: Manifest,
    path: PathBuf,
}

impl Fixture {
    fn new(scope: LaunchScope) -> Self {
        let (mut manifest, policy) = tests::fixture();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("nexus-workflow-{}-{nonce}", std::process::id()));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        manifest.scope = scope;
        manifest.budget_rubles = if scope == LaunchScope::CreateOnly {
            0
        } else {
            1000
        };
        manifest.source_policy = root.join("source.json");
        manifest.registry = root.join("access.json");
        manifest.reader_token = root.join("reader.token");
        manifest.writer_token = root.join("writer.token");
        manifest.robot_policy = root.join("robot.json");
        manifest.journal_directory = root.clone();
        manifest.reader_proxy = "http://127.0.0.1:9".to_owned();
        manifest.writer_proxy = "http://127.0.0.1:9".to_owned();
        private_json(
            &manifest.source_policy,
            &serde_json::to_value(policy).unwrap(),
        );
        private_json(
            &manifest.registry,
            &json!({"version":1,
                "actors":[{"id":"admin","name":"Test admin","role":"admin","oidc":{"username":"admin"}}],
                "accounts":[{"id":ACCOUNT,"organization":"Test WB","marketplace":"wildberries",
                    "seller_client_id":"test-seller","manager_id":"admin",
                    "wildberries":{"api_token_env":"NEXUS_UNUSED_TEST_TOKEN","seller_sid":SID}}]
            }),
        );
        for (path, scope) in [
            (&manifest.reader_token, (1_u64 << 6) | (1_u64 << 30)),
            (&manifest.writer_token, 1_u64 << 6),
        ] {
            let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","typ":"JWT"}"#);
            let body = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&json!({
                "acc":3,"for":"self","t":false,"s":scope,"exp":(Utc::now()+chrono::Duration::hours(2)).timestamp(),"sid":SID
            })).unwrap());
            fs::write(
                path,
                format!("{header}.{body}.{}", URL_SAFE_NO_PAD.encode([0_u8; 64])),
            )
            .unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let path = root.join("manifest.json");
        private_json(&path, &serde_json::to_value(&manifest).unwrap());
        Self {
            root,
            manifest,
            path,
        }
    }

    fn operator(&self, responses: Vec<(u16, Value)>) -> (Operator, mpsc::Receiver<String>) {
        let (url, requests) = mock_http(
            responses
                .into_iter()
                .map(|(status, value)| (status, value.to_string()))
                .collect(),
        );
        let mut operator = Operator::load(&self.path, false).unwrap();
        operator.reader = WbClient::new_for_test(
            Duration::from_secs(2),
            BTreeMap::from([(
                ACCOUNT.to_owned(),
                WbCredentials {
                    token: "test-reader".to_owned(),
                },
            )]),
            &url,
            &url,
        );
        operator.writer =
            WbBidWriteClient::new_for_test(&url, "test-writer", Duration::from_secs(2));
        (operator, requests)
    }

    fn journal(&self) -> Journal {
        Journal::open(
            &self.root,
            &serde_json::to_value(&self.manifest).unwrap(),
            true,
        )
        .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

fn private_json(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn details(id: u64, name: &str, nms: &[u64], status: i32, bid: u64) -> Value {
    json!({"adverts":[{"id":id,"status":status,"bid_type":"manual",
        "settings":{"name":name,"payment_type":"cpc","placements":{"search":true,"recommendations":false}},
        "nm_settings":nms.iter().map(|nm| json!({"nm_id":nm,"bids_kopecks":{"search":bid,"recommendations":0}})).collect::<Vec<_>>()
    }]})
}

fn target(status: i32, bid: u64) -> Value {
    details(ID, NAME, &NMS, status, bid)
}
fn minimums() -> Value {
    json!({"bids":NMS.iter().map(|nm|json!({"nm_id":nm,"bids":[{"currency":"RUB","type":"search","value":500}]})).collect::<Vec<_>>()})
}

fn preflight(fixture: &Fixture) -> Vec<(u16, Value)> {
    let (_, policy) = tests::fixture();
    let mut responses = NMS
        .iter()
        .map(|nm| (200, json!({"cards":[{"nmID":nm,"subjectID":4263}]})))
        .collect::<Vec<_>>();
    responses.extend([
        (200, details(SOURCE,"Одуванчик",&policy.nm_ids,9,922)),
        (200, json!({"adverts":[{"status":9,"advert_list":[{"advertId":SOURCE}]}, {"status":7}]})),
        (200, details(SOURCE,"Одуванчик",&policy.nm_ids,9,922)),
        (200, json!({"data":{"items":NMS.iter().map(|nm| json!({"nmId":nm,"warehouseId":1,"quantity":25})).collect::<Vec<_>>()}})),
    ]);
    if fixture.manifest.scope == LaunchScope::FundAndStart {
        responses.push((200, json!({"balance":1000})));
    }
    responses
}

fn requests(receiver: &mpsc::Receiver<String>, count: usize) -> Vec<String> {
    (0..count)
        .map(|_| receiver.recv_timeout(Duration::from_secs(2)).unwrap())
        .collect()
}

#[tokio::test]
async fn create_only_creates_once_verifies_and_never_reads_finance() {
    let fixture = Fixture::new(LaunchScope::CreateOnly);
    let mut responses = preflight(&fixture);
    responses.extend([
        (200, json!(ID)),
        (200, target(4, 500)),
        (200, json!({"total":0})),
        (200, target(4, 500)),
        (200, json!({"total":0})),
    ]);
    let count = responses.len();
    let (operator, receiver) = fixture.operator(responses);
    let journal = fixture.journal();
    let result = operator.create(&journal).await.unwrap();
    assert_eq!(result["campaign_id"], ID);
    assert_eq!(result["budget_rubles"], 0);
    assert_eq!(journal.campaign_id().unwrap(), ID);
    assert!(journal.has_receipt("policy"));
    assert!(operator.create(&journal).await.is_err());
    assert!(operator.fund(&journal).await.is_err());
    assert!(operator.start(&journal).await.is_err());
    let sent = requests(&receiver, count);
    assert_eq!(
        sent.iter()
            .filter(|r| r.starts_with("POST /adv/v2/seacat/save-ad "))
            .count(),
        1
    );
    assert!(!sent.iter().any(|r| r.contains("/adv/v1/balance")
        || r.contains("budget/deposit")
        || r.contains("/adv/v0/start")));
}

#[tokio::test]
async fn rejected_create_is_fenced_and_reconcile_remains_read_only() {
    let fixture = Fixture::new(LaunchScope::CreateOnly);
    let mut responses = preflight(&fixture);
    responses.push((400, json!({"error":"mixed categories"})));
    let count = responses.len();
    let (operator, receiver) = fixture.operator(responses);
    let journal = fixture.journal();
    assert!(operator.create(&journal).await.is_err());
    assert!(operator.create(&journal).await.is_err());
    assert_eq!(
        operator.reconcile(&journal).await.unwrap()["outcome"],
        "no_confirmed_campaign_id"
    );
    drop(journal);
    assert_eq!(
        run_wb_campaign_launch("reconcile", &fixture.path)
            .await
            .unwrap()["automatic_retry_allowed"],
        false
    );
    assert_eq!(
        requests(&receiver, count)
            .iter()
            .filter(|r| r.starts_with("POST /adv/v2/seacat/save-ad "))
            .count(),
        1
    );
}

#[tokio::test]
async fn initial_bids_are_patched_once_then_read_back() {
    let fixture = Fixture::new(LaunchScope::CreateOnly);
    let responses = vec![
        (200, target(4, 500)),
        (200, json!({"total":0})),
        (200, minimums()),
        (200, target(4, 500)),
        (200, json!({"total":0})),
        (200, json!({})),
        (200, target(4, 922)),
        (200, target(4, 922)),
        (200, json!({"total":0})),
    ];
    let count = responses.len();
    let (operator, receiver) = fixture.operator(responses);
    let journal = fixture.journal();
    journal
        .receipt("create", &json!({"campaign_id":ID}))
        .unwrap();
    operator.bids(&journal).await.unwrap();
    assert!(operator.bids(&journal).await.is_err());
    let sent = requests(&receiver, count);
    assert_eq!(
        sent.iter()
            .filter(|r| r.starts_with("PATCH /api/advert/v1/bids "))
            .count(),
        1
    );
}

#[tokio::test]
async fn funding_requires_exact_receipt_and_is_never_repeated() {
    for total in [1000, 999] {
        let fixture = Fixture::new(LaunchScope::FundAndStart);
        let mut responses = preflight(&fixture);
        responses.extend([
            (200, minimums()),
            (200, target(11, 922)),
            (200, json!({"total":0})),
            (200, json!({"balance":1000})),
            (200, json!({"total":total})),
        ]);
        if total == 1000 {
            responses.extend([
                (200, json!({"total":1000})),
                (200, target(11, 922)),
                (200, target(11, 922)),
                (200, json!({"total":1000})),
            ]);
        }
        let count = responses.len();
        let (operator, receiver) = fixture.operator(responses);
        let journal = fixture.journal();
        journal
            .receipt("create", &json!({"campaign_id":ID}))
            .unwrap();
        journal.receipt("bids", &json!({})).unwrap();
        assert_eq!(operator.fund(&journal).await.is_ok(), total == 1000);
        assert!(operator.fund(&journal).await.is_err());
        assert_eq!(journal.has_receipt("fund"), total == 1000);
        let sent = requests(&receiver, count);
        let deposits = sent
            .iter()
            .filter(|r| r.starts_with("POST /adv/v1/budget/deposit?"))
            .collect::<Vec<_>>();
        assert_eq!(deposits.len(), 1);
        let body: Value =
            serde_json::from_str(deposits[0].split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(body, json!({"sum":1000,"type":1,"return":true}));
        assert!(!sent.iter().any(|r| r.contains("/adv/v0/start")));
    }
}

#[tokio::test]
async fn bid_drift_blocks_before_attempt_record_or_patch() {
    let fixture = Fixture::new(LaunchScope::CreateOnly);
    let (operator, receiver) = fixture.operator(vec![
        (200, target(4, 500)),
        (200, json!({"total":0})),
        (200, minimums()),
        (200, target(4, 600)),
    ]);
    let journal = fixture.journal();
    journal
        .receipt("create", &json!({"campaign_id":ID}))
        .unwrap();
    assert!(operator.bids(&journal).await.is_err());
    assert!(!journal.attempted("bids"));
    assert!(
        !requests(&receiver, 4)
            .iter()
            .any(|r| r.starts_with("PATCH "))
    );
}

#[tokio::test]
async fn entrypoint_and_authorization_fail_closed_before_network() {
    let fixture = Fixture::new(LaunchScope::CreateOnly);
    assert!(
        run_wb_campaign_launch("unknown", &fixture.path)
            .await
            .is_err()
    );
    assert!(run_wb_campaign_launch("fund", &fixture.path).await.is_err());
    let operator = Operator::load(&fixture.path, false).unwrap();
    let mut changed = fixture.manifest.clone();
    changed.authorization_reference = "revoked".to_owned();
    private_json(&fixture.path, &serde_json::to_value(changed).unwrap());
    assert!(operator.fresh_authorization().is_err());
}

#[tokio::test]
async fn independent_readback_returns_vendor_state_not_an_assumed_launch() {
    let fixture = Fixture::new(LaunchScope::FundAndStart);
    let (operator, receiver) =
        fixture.operator(vec![(200, target(11, 922)), (200, json!({"total":1000}))]);
    let journal = fixture.journal();
    journal
        .receipt("create", &json!({"campaign_id":ID}))
        .unwrap();
    let result = reconcile_campaign(&fixture.manifest, &operator.reader, &journal, ID)
        .await
        .unwrap();
    assert_eq!(result["status"], 11);
    assert_eq!(result["fund_confirmed_in_journal"], false);
    assert_eq!(result["start_confirmed_in_journal"], false);
    assert_eq!(result["budget"]["total"], 1000);
    assert!(requests(&receiver, 2).iter().all(|r| r.starts_with("GET ")));
}

#[tokio::test]
async fn new_status_four_is_rejected_before_observer_database_or_start() {
    let fixture = Fixture::new(LaunchScope::FundAndStart);
    let (operator, receiver) = fixture.operator(vec![(200, target(4, 922))]);
    let journal = fixture.journal();
    journal
        .receipt("create", &json!({"campaign_id":ID}))
        .unwrap();
    journal.receipt("fund", &json!({})).unwrap();
    assert!(
        operator
            .start(&journal)
            .await
            .unwrap_err()
            .to_string()
            .contains("status-4")
    );
    assert!(!journal.attempted("start"));
    assert!(requests(&receiver, 1)[0].starts_with("GET /api/advert/v2/adverts?"));
}

#[tokio::test]
async fn cli_write_dispatch_respects_existing_attempts_without_http() {
    let fixture = Fixture::new(LaunchScope::FundAndStart);
    let journal = fixture.journal();
    journal
        .receipt("create", &json!({"campaign_id":ID}))
        .unwrap();
    for stage in ["create", "bids", "fund", "start"] {
        journal.attempt(stage, &json!({})).unwrap();
    }
    drop(journal);
    for stage in ["create", "bids", "fund", "start"] {
        assert!(
            run_wb_campaign_launch(stage, &fixture.path)
                .await
                .unwrap_err()
                .to_string()
                .contains("already attempted")
        );
    }
}
