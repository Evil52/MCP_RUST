use super::*;
use crate::test_support::mock_http_with_hook;

#[tokio::test]
async fn revoked_authorization_during_final_read_prevents_attempt_and_patch() {
    let fixture = Fixture::new(LaunchScope::CreateOnly);
    let path = fixture.path.clone();
    let mut revoked = fixture.manifest.clone();
    revoked.authorization_reference = "revoked/during-budget-read".to_owned();
    let (url, receiver) = mock_http_with_hook(
        [
            target(4, 500),
            json!({"total":0}),
            minimums(),
            target(4, 500),
            json!({"total":0}),
        ]
        .into_iter()
        .map(|value| (200, value.to_string()))
        .collect(),
        move |index| {
            if index == 4 {
                private_json(&path, &serde_json::to_value(&revoked).unwrap());
            }
        },
    );
    let mut operator = Operator::load(&fixture.path, false).unwrap();
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
    operator.writer = WbBidWriteClient::new_for_test(&url, "test-writer", Duration::from_secs(2));
    let journal = fixture.journal();
    journal
        .receipt("create", &json!({"campaign_id":ID}))
        .unwrap();

    let error = operator.bids(&journal).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("authorization changed or was revoked")
    );
    assert!(!journal.attempted("bids"));
    assert!(!journal.has_receipt("bids"));
    let sent = requests(&receiver, 5);
    assert!(sent[4].starts_with("GET /adv/v1/budget?"));
    assert!(sent.iter().all(|request| !request.starts_with("PATCH ")));
    assert!(receiver.try_recv().is_err());
}
