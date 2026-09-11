use super::*;
use crate::control::{
    WbAutomationLegacyStateSeed, WbAutomationObserver, WbAutomationPostgresStore,
    wb_automation_business_date,
};

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "release consumes the lease before the operator acquires it again"
)]
async fn guarded_start_requires_two_cycles_and_verifies_status_after_one_write() {
    let Ok(url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
        return;
    };
    let config: tokio_postgres::Config = url.parse().unwrap();
    let store = WbAutomationPostgresStore::connect(&config).await.unwrap();
    store.verify_runtime_contract().await.unwrap();
    let fixture = Fixture::new(LaunchScope::FundAndStart);
    let now = Utc::now();
    let date = wb_automation_business_date(now);
    let prior = date.pred_opt().unwrap();
    let stats = json!([{"advertId":ID,"stats":[
        {"date":prior.to_string(),"nm_id":NMS[0],"views":0,"clicks":0,"sum":0,"orders":0,"sumPrice":0},
        {"date":date.to_string(),"nm_id":NMS[0],"views":0,"clicks":0,"sum":0,"orders":0,"sumPrice":0}
    ]}]);
    let stock = json!({"data":{"items":NMS.iter().map(|nm|json!({"nmId":nm,"warehouseId":1,"quantity":25})).collect::<Vec<_>>()}});
    // Both the observer and writer use this one loopback server. No external
    // network or production DB URL is used by this test.
    let responses = vec![
        (200, target(11, 922)),
        (200, minimums()),
        (200, json!({"total":1000})),
        (200, stats),
        (200, stock),
        (200, target(11, 922)),
        (200, json!({})),
        (200, target(9, 922)),
        (200, json!({"total":1000})),
        (200, target(9, 922)),
        (200, json!({"total":1000})),
    ];
    let count = responses.len();
    let (operator, receiver) = fixture.operator(responses);
    let policy = operator.target_policy(ID);
    private_json(
        &fixture.manifest.robot_policy,
        &serde_json::to_value(&policy).unwrap(),
    );
    let mut observer = WbAutomationObserver::from_files(
        &fixture.manifest.robot_policy,
        &fixture.manifest.registry,
        &fixture.manifest.reader_token,
        true,
        Duration::from_secs(2),
        None,
    )
    .unwrap();
    observer.replace_client_for_test(operator.reader.clone());
    let digest = observer.policy_sha256();
    let mut lease = store
        .try_acquire_campaign(ACCOUNT, ID)
        .await
        .unwrap()
        .unwrap();
    lease
        .initialize_from_legacy(&WbAutomationLegacyStateSeed {
            policy_digest: digest.to_owned(),
            business_date: date,
            actions_today: 0,
            last_action_at: None,
            paused_for_daily_cap_on: None,
            incident_class: None,
            legacy_digest: journal::digest(fixture.root.to_string_lossy().as_bytes()),
        })
        .await
        .unwrap();
    assert!(!lease.verify_launch_cycles(digest, now).await.unwrap());
    for (index, at) in [(0, now - chrono::Duration::minutes(5)), (1, now)] {
        let key = journal::digest(format!("{}-{index}", fixture.root.display()).as_bytes());
        lease
            .persist_shadow_cycle(
                &key,
                digest,
                at,
                wb_automation_business_date(at),
                1,
                "{}",
                "{}",
            )
            .await
            .unwrap();
    }
    assert!(lease.verify_launch_cycles(digest, now).await.unwrap());
    assert!(
        !lease
            .verify_launch_cycles(&"a".repeat(64), now)
            .await
            .unwrap()
    );
    assert!(
        !lease
            .verify_launch_cycles(digest, now + chrono::Duration::minutes(2))
            .await
            .unwrap()
    );
    assert!(
        !lease
            .verify_launch_cycles(digest, now - chrono::Duration::seconds(1))
            .await
            .unwrap()
    );
    lease.release().await.unwrap();
    let journal = fixture.journal();
    journal
        .receipt("create", &json!({"campaign_id":ID}))
        .unwrap();
    journal
        .receipt("fund", &json!({"transferred_rubles":1000}))
        .unwrap();
    let result = operator
        .execute_start(&journal, ID, &policy, &observer, &store)
        .await
        .unwrap();
    assert_eq!(result["status"], 9);
    assert!(
        operator
            .execute_start(&journal, ID, &policy, &observer, &store)
            .await
            .is_err()
    );
    assert_eq!(
        requests(&receiver, count)
            .iter()
            .filter(|r| r.starts_with("GET /adv/v0/start?"))
            .count(),
        1
    );
}
