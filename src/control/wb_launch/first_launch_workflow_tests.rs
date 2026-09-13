use super::*;
use crate::control::{
    WbAutomationLegacyStateSeed, WbAutomationObserver, WbAutomationPostgresStore,
    WbAutomationStateView, wb_automation_business_date,
};

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "lease spans cycle evidence then is released for startup"
)]
async fn first_start_uses_real_readiness_cycles_and_never_retries_uncertain_write() {
    let Ok(url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
        return;
    };
    let config: tokio_postgres::Config = url.parse().unwrap();
    let store = WbAutomationPostgresStore::connect(&config).await.unwrap();
    store.verify_runtime_contract().await.unwrap();
    for (offset, http, readback_status) in [(2, 200, 9), (3, 500, 9), (4, 200, 11)] {
        let success = http == 200 && readback_status == 9;
        let id = ID + offset;
        let fixture = Fixture::new(LaunchScope::FundAndStart);
        let target = |status| details(id, NAME, &NMS, status, 922);
        let stock = json!({"data":{"items":NMS.iter().map(|nm|
            json!({"nmId":nm,"warehouseId":1,"quantity":25})).collect::<Vec<_>>()}});
        let mut responses = Vec::new();
        for _ in 0..3 {
            responses.extend([
                (200, target(4)),
                (200, minimums()),
                (200, json!({"total":1000})),
                (200, stock.clone()),
            ]);
        }
        responses.extend([(200, target(4)), (http, json!({}))]);
        if http == 200 {
            responses.push((200, target(readback_status)));
        }
        if success {
            responses.extend([
                (200, json!({"total":1000})),
                (200, target(9)),
                (200, json!({"total":1000})),
            ]);
        }
        let count = responses.len();
        let (operator, receiver) = fixture.operator(responses);
        let policy = operator.target_policy(id);
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
        let now = Utc::now();
        let mut lease = store
            .try_acquire_campaign(ACCOUNT, id)
            .await
            .unwrap()
            .unwrap();
        lease
            .initialize_from_legacy(&WbAutomationLegacyStateSeed {
                policy_digest: observer.policy_sha256().into(),
                business_date: wb_automation_business_date(now),
                actions_today: 0,
                last_action_at: None,
                paused_for_daily_cap_on: None,
                incident_class: None,
                legacy_digest: journal::digest(fixture.root.to_string_lossy().as_bytes()),
            })
            .await
            .unwrap();
        assert!(
            !lease
                .verify_first_launch_cycles(observer.policy_sha256())
                .await
                .unwrap()
        );
        for (index, at) in [(0, now - chrono::Duration::minutes(5)), (1, now)] {
            let snapshot = observer
                .observe(at, WbAutomationStateView::default())
                .await
                .unwrap();
            assert!(!snapshot.observation.daily_spend_complete);
            assert!(!snapshot.observation.attribution_complete);
            assert!(matches!(
                snapshot.decision.action,
                crate::control::WbAutomationAction::Hold { .. }
            ));
            let key = journal::digest(format!("{}-{index}", fixture.root.display()).as_bytes());
            lease
                .persist_shadow_cycle(
                    &key,
                    observer.policy_sha256(),
                    at,
                    wb_automation_business_date(at),
                    1,
                    &serde_json::to_string(&snapshot).unwrap(),
                    &serde_json::to_string(&snapshot.decision).unwrap(),
                )
                .await
                .unwrap();
        }
        assert!(
            lease
                .verify_first_launch_cycles(observer.policy_sha256())
                .await
                .unwrap()
        );
        lease.release().await.unwrap();
        let journal = fixture.journal();
        journal
            .receipt("create", &json!({"campaign_id":id,"wb_http":200}))
            .unwrap();
        journal
            .receipt(
                "bids",
                &json!({"campaign_id":id,"bids_kopecks":fixture.manifest.bids_kopecks}),
            )
            .unwrap();
        journal
            .receipt("fund-response", &json!({"wb_http":200,"total":1000}))
            .unwrap();
        journal
            .receipt(
                "fund",
                &json!({"campaign_id":id,"transferred_rubles":1000,"type":1,"budget_after":1000}),
            )
            .unwrap();
        let result = operator
            .execute_start(&journal, id, &policy, &observer, &store)
            .await;
        assert_eq!(
            result.is_ok(),
            success,
            "unexpected start outcome for mock HTTP {http}, readback status {readback_status}"
        );
        assert!(journal.attempted("start"));
        assert_eq!(journal.has_receipt("start"), success);
        assert!(
            operator
                .execute_start(&journal, id, &policy, &observer, &store)
                .await
                .is_err()
        );
        let sent = requests(&receiver, count);
        assert_eq!(
            sent.iter()
                .filter(|r| r.starts_with("GET /adv/v0/start?"))
                .count(),
            1
        );
        assert!(
            sent.iter()
                .all(|r| !r.contains("fullstats") && !r.contains("budget/deposit"))
        );
    }
}
