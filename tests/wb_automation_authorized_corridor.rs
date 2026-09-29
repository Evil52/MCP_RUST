use chrono::{Duration, Utc};
use mcp_ozon::control::{
    WbAutomationLegacyStateSeed, WbAutomationPolicy, WbAutomationPostgresError,
    WbAutomationPostgresStore,
};
use sha2::{Digest, Sha256};
use std::{fmt::Write as _, str::FromStr};
use tokio_postgres::Config;

fn digest(p: &WbAutomationPolicy) -> String {
    let mut result = String::new();
    for b in Sha256::digest(serde_json::to_vec(p).unwrap()) {
        write!(&mut result, "{b:02x}").unwrap();
    }
    result
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "campaign lease is explicitly released asynchronously"
)]
async fn authorized_corridor_preserves_state_and_rejects_incidents_and_pending_writes() {
    let Ok(url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
        return;
    };
    let config = Config::from_str(&url).unwrap();
    let store = WbAutomationPostgresStore::connect(&config).await.unwrap();
    let admin_config =
        Config::from_str(&std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").unwrap()).unwrap();
    let (admin, connection) = admin_config.connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(async move {
        connection.await.unwrap();
    });
    let now = Utc::now();
    let date = mcp_ozon::control::wb_automation_business_date(now);
    for (index, incident) in [None, Some("write_not_reconciled".to_owned()), None]
        .into_iter()
        .enumerate()
    {
        let mut source: WbAutomationPolicy =
            serde_json::from_str(include_str!("../config/wb-automation-oduvanchik.v4.json"))
                .unwrap();
        source.account_id = format!("renewal_{}", std::process::id());
        source.campaign_id = 9_900_000 + u64::try_from(index).unwrap();
        source.authorized_at = now - Duration::days(2);
        source.observe_until = now - Duration::days(1);
        source.authorization_expires_at = now - Duration::hours(1);
        let mut target = source.clone();
        target.authorization_reference = "test/authorized-corridor".into();
        target.authorized_at = now - Duration::minutes(2);
        target.observe_until = now - Duration::minutes(1);
        target.authorization_expires_at = now + Duration::days(30);
        target.min_bid_kopecks = 700;
        target.max_bid_kopecks = 1200;
        let source_hash = digest(&source);
        let target_hash = digest(&target);
        let cycle = format!("{:064x}", source.campaign_id);
        let mut lease = store
            .try_acquire_campaign(&source.account_id, source.campaign_id)
            .await
            .unwrap()
            .unwrap();
        lease
            .initialize_from_legacy(&WbAutomationLegacyStateSeed {
                policy_digest: source_hash.clone(),
                business_date: date,
                actions_today: 2,
                last_action_at: Some(now - Duration::hours(1)),
                paused_for_daily_cap_on: Some(date),
                incident_class: incident.clone(),
                legacy_digest: "c".repeat(64),
            })
            .await
            .unwrap();
        lease
            .persist_shadow_cycle(&cycle, &source_hash, now, date, 1, "{}", "{}")
            .await
            .unwrap();
        if index == 2 {
            lease
                .reserve_action(&mcp_ozon::control::WbAutomationActionReservation {
                    idempotency_key: format!("{:064x}", source.campaign_id + 100),
                    cycle_id: cycle.clone(),
                    policy_digest: source_hash.clone(),
                    request_digest: "e".repeat(64),
                    action_kind: mcp_ozon::control::WbAutomationDurableActionKind::ChangeBids,
                    request_json: serde_json::json!({"kind":"change_bids","changes":[{
                        "nm_id": source.nm_ids[0],"from_bid_kopecks":500,"to_bid_kopecks":550,
                        "reason":"low_exposure_exploration"
                    }]})
                    .to_string(),
                    business_date: date,
                    expected_state_revision: 1,
                    max_actions_per_day: 48,
                })
                .await
                .unwrap();
        }
        let before = lease.load_state().await.unwrap().unwrap();
        let result = lease.authorize_corridor_policy(&source, &target, now).await;
        if incident.is_some() || index == 2 {
            assert!(result.is_err());
            assert_eq!(lease.load_state().await.unwrap().unwrap(), before);
        } else {
            let receipt = result.unwrap();
            assert!(receipt.changed);
            assert!(
                !lease
                    .authorize_corridor_policy(&source, &target, now)
                    .await
                    .unwrap()
                    .changed
            );
            let after = lease.load_state().await.unwrap().unwrap();
            let mut expected = before;
            expected.policy_digest = target_hash;
            expected.revision += 1;
            assert_eq!(after, expected);
            let audit=admin.query_one("SELECT payload_json FROM wb_automation.audit_events WHERE account_id=$1 AND advert_id=$2 AND event_type='authorized_corridor_adjusted'", &[&source.account_id,&i64::try_from(source.campaign_id).unwrap()]).await.unwrap();
            let audit: serde_json::Value =
                serde_json::from_str(&audit.get::<_, String>(0)).unwrap();
            assert_eq!(
                audit["authorization_reference"],
                target.authorization_reference
            );
            assert_eq!(audit["from_min_bid_kopecks"], 500);
            assert_eq!(audit["to_min_bid_kopecks"], 700);
            let mut changed = target.clone();
            changed.daily_spend_cap_minor += 1;
            assert_eq!(
                lease
                    .authorize_corridor_policy(&source, &changed, now)
                    .await,
                Err(WbAutomationPostgresError::InvalidInput)
            );
        }
        lease.release().await.unwrap();
    }
    drop(admin);
    connection.await.unwrap();
}
