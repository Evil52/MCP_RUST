use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};

use chrono::{NaiveDate, TimeZone, Utc};
use mcp_ozon::control::{
    WbAutomationLegacyStateSeed, WbAutomationPostgresError, WbAutomationPostgresStore,
};
use tokio_postgres::{Config, NoTls};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

async fn admin(config: &Config) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let (client, connection) = config.connect(NoTls).await.expect("admin connects");
    (
        client,
        tokio::spawn(async move {
            let _ = connection.await;
        }),
    )
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "the lease is explicitly consumed by its async release at the end of the test"
)]
async fn v4_corridor_is_atomic_idempotent_and_audited() {
    let Ok(database_url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
        return;
    };
    let config = Config::from_str(&database_url).expect("test database URL parses");
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL")
        .expect("test wrapper provides the admin URL");
    let admin_config = Config::from_str(&admin_url).expect("admin URL parses");
    let (admin, admin_connection) = admin(&admin_config).await;
    let store = WbAutomationPostgresStore::connect(&config)
        .await
        .expect("store connects");
    let campaign_id = 9_000_000 + SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let campaign_id_i64 = i64::try_from(campaign_id).expect("campaign fits i64");
    let account_id = format!("v4_corridor_{}", std::process::id());
    let source_digest = "a".repeat(64);
    let target_digest = "b".repeat(64);
    let cycle_id = format!("{campaign_id:064x}");
    let date = NaiveDate::from_ymd_opt(2026, 9, 15).expect("date is valid");
    let observed_at = Utc.with_ymd_and_hms(2026, 9, 15, 5, 0, 0).single().unwrap();
    let mut lease = store
        .try_acquire_campaign(&account_id, campaign_id)
        .await
        .expect("lock query succeeds")
        .expect("lock is acquired");
    lease
        .initialize_from_legacy(&WbAutomationLegacyStateSeed {
            policy_digest: source_digest.clone(),
            business_date: date,
            actions_today: 2,
            last_action_at: None,
            paused_for_daily_cap_on: None,
            incident_class: None,
            legacy_digest: "c".repeat(64),
        })
        .await
        .expect("state is initialized");
    lease
        .persist_shadow_cycle(&cycle_id, &source_digest, observed_at, date, 1, "{}", "{}")
        .await
        .expect("source cycle is persisted");
    assert_eq!(
        lease
            .activate_traffic_frontier_v4_corridor_policy(
                &source_digest,
                &target_digest,
                701,
                700,
                1_050,
                1_200,
            )
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    let receipt = lease
        .activate_traffic_frontier_v4_corridor_policy(
            &source_digest,
            &target_digest,
            500,
            700,
            1_050,
            1_200,
        )
        .await
        .expect("v4 corridor is adjusted");
    assert!(receipt.changed);
    assert_eq!(receipt.state_revision, 2);
    let replay = lease
        .activate_traffic_frontier_v4_corridor_policy(
            &source_digest,
            &target_digest,
            500,
            700,
            1_050,
            1_200,
        )
        .await
        .expect("replay is idempotent");
    assert!(!replay.changed);
    let audit = admin
        .query_one(
            "SELECT payload_json FROM wb_automation.audit_events \
         WHERE account_id=$1 AND advert_id=$2 \
           AND event_type='traffic_frontier_v4_corridor_adjusted'",
            &[&account_id, &campaign_id_i64],
        )
        .await
        .expect("audit is readable");
    let payload: serde_json::Value = serde_json::from_str(&audit.get::<_, String>(0)).unwrap();
    assert_eq!(payload["from_min_bid_kopecks"], 500);
    assert_eq!(payload["to_min_bid_kopecks"], 700);
    assert_eq!(payload["from_max_bid_kopecks"], 1_050);
    assert_eq!(payload["to_max_bid_kopecks"], 1_200);
    assert_eq!(payload["autonomous_pacing"], "traffic_frontier_v4");
    lease.release().await.expect("lock is released");
    drop(admin);
    admin_connection.await.expect("admin connection ends");
}
