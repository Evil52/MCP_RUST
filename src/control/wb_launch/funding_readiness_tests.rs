//! Loopback-only money-boundary tests. Never load a live wallet or writer.
use super::*;

fn funding_journal(fixture: &Fixture) -> Journal {
    let journal = fixture.journal();
    journal
        .receipt("create", &json!({"campaign_id":ID}))
        .unwrap();
    journal.receipt("bids", &json!({})).unwrap();
    journal
}

fn install_protection(fixture: &Fixture, operator: &Operator) {
    private_json(
        &fixture.manifest.robot_policy,
        &serde_json::to_value(operator.target_policy(ID)).unwrap(),
    );
}

fn assert_no_money_attempt(journal: &Journal, receiver: &mpsc::Receiver<String>, count: usize) {
    assert!(!journal.attempted("fund"));
    assert!(!journal.has_receipt("fund"));
    assert!(!journal.attempted("start"));
    assert!(requests(receiver, count).iter().all(|request| {
        !request.contains("/adv/v1/budget/deposit")
            && !request.contains("/adv/v2/seacat/save-ad")
            && !request.starts_with("PATCH ")
            && !request.contains("/adv/v0/start")
    }));
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn new_campaign_is_not_funded_without_installed_protection() {
    let fixture = Fixture::new(LaunchScope::FundAndStart);
    let (operator, receiver) = fixture.operator(vec![(200, target(4, 922))]);
    let journal = funding_journal(&fixture);
    assert!(operator.fund(&journal).await.is_err());
    assert_no_money_attempt(&journal, &receiver, 1);
}

#[tokio::test]
async fn missing_or_different_protection_blocks_funding_before_preflight() {
    for changed in [false, true] {
        let fixture = Fixture::new(LaunchScope::FundAndStart);
        let (operator, receiver) = fixture.operator(vec![(200, target(11, 922))]);
        if changed {
            let mut policy = operator.target_policy(ID);
            policy.write_enabled = false;
            private_json(
                &fixture.manifest.robot_policy,
                &serde_json::to_value(policy).unwrap(),
            );
        }
        let journal = funding_journal(&fixture);
        assert!(operator.fund(&journal).await.is_err());
        assert_no_money_attempt(&journal, &receiver, 1);
    }
}

#[tokio::test]
async fn status_drift_during_preflight_blocks_funding_without_consuming_attempt() {
    let fixture = Fixture::new(LaunchScope::FundAndStart);
    let mut responses = vec![(200, target(11, 922))];
    responses.extend(preflight(&fixture));
    responses.extend([(200, minimums()), (200, target(4, 922))]);
    let count = responses.len();
    let (operator, receiver) = fixture.operator(responses);
    install_protection(&fixture, &operator);
    let journal = funding_journal(&fixture);
    let error = operator.fund(&journal).await.unwrap_err();
    assert!(error.to_string().contains("status drifted"));
    assert_no_money_attempt(&journal, &receiver, count);
}

#[tokio::test]
async fn protection_revoked_during_last_balance_read_prevents_deposit() {
    let fixture = Fixture::new(LaunchScope::FundAndStart);
    let mut responses = vec![(200, target(11, 922))];
    responses.extend(preflight(&fixture));
    responses.extend([
        (200, minimums()),
        (200, target(11, 922)),
        (200, json!({"total":0})),
        (200, json!({"balance":0,"net":1000})),
    ]);
    let count = responses.len();
    let policy_path = fixture.manifest.robot_policy.clone();
    let (operator, receiver) = fixture.operator_with_hook(responses, move |index| {
        if index + 1 == count {
            let mut revoked: WbAutomationPolicy = read_policy_json(&policy_path).unwrap();
            revoked.write_enabled = false;
            private_json(&policy_path, &serde_json::to_value(revoked).unwrap());
        }
    });
    install_protection(&fixture, &operator);
    let journal = funding_journal(&fixture);
    let error = operator.fund(&journal).await.unwrap_err();
    assert!(error.to_string().contains("differs from reviewed copy"));
    assert_no_money_attempt(&journal, &receiver, count);
}
