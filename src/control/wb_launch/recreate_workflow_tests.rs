use super::*;

pub(super) fn fixture() -> Fixture {
    let mut fixture = Fixture::new(LaunchScope::CreateOnly);
    let mut old = fixture.manifest.clone();
    old.bids_kopecks = [
        146_312_604,
        207_418_966,
        455_101_276,
        461_126_890,
        529_996_417,
    ]
    .into_iter()
    .map(|id| (id, 922))
    .collect();
    old.expires_at = Utc::now() - chrono::Duration::hours(1);
    let old = serde_json::to_value(old).unwrap();
    let journal = Journal::open(&fixture.root, &old, true).unwrap();
    drop(journal);
    let attempt = json!({"attempted_at":Utc::now()-chrono::Duration::hours(2),"evidence":{}});
    private_json(
        &fixture
            .root
            .join("ofk_region_wb-Nexus/create-attempted.json"),
        &attempt,
    );
    fixture.manifest.recreate = Some(RecreateApproval {
        previous_manifest_sha256: journal::digest(&serde_json::to_vec(&old).unwrap()),
        previous_attempt_sha256: journal::digest(&serde_json::to_vec(&attempt).unwrap()),
    });
    fixture.manifest.authorization_reference = "test/reviewed-replacement".into();
    private_json(
        &fixture.path,
        &serde_json::to_value(&fixture.manifest).unwrap(),
    );
    fixture
}

fn reads(fixture: &Fixture) -> Vec<(u16, Value)> {
    let mut responses = preflight(fixture);
    responses[6].1 = json!({"all":1,"adverts":[{"type":9,"status":9,"count":1,"advert_list":[{"advertId":SOURCE}]}]});
    responses
}

#[tokio::test]
async fn attempted_replacement_preflight_stops_before_network() {
    let fixture = fixture();
    let journal = fixture.journal();
    journal.attempt("create", &json!({})).unwrap();
    let (operator, receiver) = fixture.operator(vec![]);
    assert!(
        operator
            .preflight(None)
            .await
            .unwrap_err()
            .to_string()
            .contains("already attempted")
    );
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn explicitly_reauthorized_create_is_once_only_for_success_and_rejection() {
    for status in [200, 400] {
        let fixture = fixture();
        let mut responses = reads(&fixture);
        responses.push((
            status,
            if status == 200 {
                json!(ID)
            } else {
                json!({"error":"rejected"})
            },
        ));
        if status == 200 {
            responses.extend([
                (200, target(4, 500)),
                (200, json!({"total":0})),
                (200, target(4, 500)),
                (200, json!({"total":0})),
            ]);
        }
        let count = responses.len();
        let (operator, receiver) = fixture.operator(responses);
        let journal = fixture.journal();
        assert_eq!(operator.create(&journal).await.is_ok(), status == 200);
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
        assert!(
            !sent
                .iter()
                .any(|r| r.contains("budget/deposit") || r.contains("/adv/v0/start"))
        );
    }
}

#[tokio::test]
async fn completed_nexus_blocks_replacement_before_post() {
    let fixture = fixture();
    let mut responses = reads(&fixture);
    responses[6].1 = json!({"all":1,"adverts":[{"type":9,"status":7,"count":1,"advert_list":[{"advertId":ID}]}]});
    responses[7].1 = target(7, 500);
    responses.truncate(8);
    let count = responses.len();
    let (operator, receiver) = fixture.operator(responses);
    let journal = fixture.journal();
    assert!(
        operator
            .create(&journal)
            .await
            .unwrap_err()
            .to_string()
            .contains("another Nexus")
    );
    assert!(!journal.attempted("create"));
    assert!(
        !requests(&receiver, count)
            .iter()
            .any(|r| r.contains("/adv/v2/seacat/save-ad"))
    );
}
