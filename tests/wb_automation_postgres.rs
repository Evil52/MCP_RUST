use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};

use chrono::{Days, Duration, NaiveDate, TimeZone, Utc};
use mcp_ozon::control::{
    WbAutomationLegacyStateSeed, WbAutomationPostgresError, WbAutomationPostgresStore,
};
use tokio_postgres::{Config, NoTls};

static CAMPAIGN_SEQUENCE: AtomicU64 = AtomicU64::new(1);

async fn raw_client(config: &Config) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let (client, connection) = config.connect(NoTls).await.expect("test database connects");
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, connection_task)
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "leases are consumed by explicit async release, which is the behavior under test"
)]
async fn automation_state_is_isolated_locked_and_idempotent() {
    let Ok(database_url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
        return;
    };
    let config = Config::from_str(&database_url).expect("test database URL parses");
    let campaign_id = 8_000_000_u64 + CAMPAIGN_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let account_id = format!("wb_automation_test_{}", std::process::id());
    let policy_digest = "a".repeat(64);
    let cycle_id = "b".repeat(64);
    let unique_conflict_cycle_id = "c".repeat(64);
    let invalid_date_cycle_id = "d".repeat(64);
    let stale_revision_cycle_id = "e".repeat(64);
    let business_date = NaiveDate::from_ymd_opt(2026, 8, 25).expect("valid date");
    let observed_at = Utc
        .with_ymd_and_hms(2026, 8, 25, 12, 0, 0)
        .single()
        .expect("valid timestamp");

    let first = WbAutomationPostgresStore::connect(&config)
        .await
        .expect("first store connects");
    let second = WbAutomationPostgresStore::connect(&config)
        .await
        .expect("second store connects");
    first
        .verify_runtime_contract()
        .await
        .expect("least-privilege contract holds");

    let mut lease = first
        .try_acquire_campaign(&account_id, campaign_id)
        .await
        .expect("lock query succeeds")
        .expect("first store wins the lock");
    let legacy_seed = WbAutomationLegacyStateSeed {
        policy_digest: policy_digest.clone(),
        business_date,
        actions_today: 0,
        last_action_at: None,
        paused_for_daily_cap_on: None,
        incident_class: None,
        legacy_digest: "f".repeat(64),
    };
    let mut invalid_seed = legacy_seed.clone();
    invalid_seed.actions_today = 501;
    assert_eq!(
        lease.initialize_from_legacy(&invalid_seed).await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    let mut invalid_incident_seed = legacy_seed.clone();
    invalid_incident_seed.incident_class = Some("not-an-error-class".to_owned());
    assert_eq!(
        lease.initialize_from_legacy(&invalid_incident_seed).await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    let mut invalid_pause_seed = legacy_seed.clone();
    invalid_pause_seed.paused_for_daily_cap_on = business_date.succ_opt();
    assert_eq!(
        lease.initialize_from_legacy(&invalid_pause_seed).await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    assert!(
        lease
            .initialize_from_legacy(&legacy_seed)
            .await
            .expect("legacy state is imported once")
    );
    assert!(
        !lease
            .initialize_from_legacy(&legacy_seed)
            .await
            .expect("identical legacy import is idempotent")
    );
    let mut conflicting_seed = legacy_seed.clone();
    conflicting_seed.actions_today = 1;
    assert_eq!(
        lease.initialize_from_legacy(&conflicting_seed).await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    let lease_debug = format!("{lease:?}");
    assert!(lease_debug.contains(&account_id));
    assert!(lease_debug.contains(&campaign_id.to_string()));
    assert!(
        second
            .try_acquire_campaign(&account_id, campaign_id)
            .await
            .expect("contending lock query succeeds")
            .is_none()
    );
    let state = lease
        .load_state()
        .await
        .expect("state query succeeds")
        .expect("state exists");
    assert_eq!(state.campaign_id, campaign_id);
    assert_eq!(state.policy_digest, policy_digest);
    assert_eq!(state.revision, 1);

    assert!(
        lease
            .persist_shadow_cycle(
                &cycle_id,
                &policy_digest,
                observed_at,
                business_date,
                1,
                "{\"observation\":\"complete\"}",
                "{\"action\":\"hold\"}",
            )
            .await
            .expect("first immutable cycle is inserted")
    );
    assert!(matches!(
        lease
            .persist_shadow_cycle(
                &unique_conflict_cycle_id,
                &policy_digest,
                observed_at,
                business_date,
                1,
                "{\"observation\":\"complete\"}",
                "{\"action\":\"hold\"}",
            )
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    ));
    assert!(matches!(
        lease
            .persist_shadow_cycle(
                &invalid_date_cycle_id,
                &policy_digest,
                observed_at + Duration::seconds(1),
                business_date
                    .checked_add_days(Days::new(1))
                    .expect("next business date is representable"),
                1,
                "{\"observation\":\"complete\"}",
                "{\"action\":\"hold\"}",
            )
            .await,
        Err(WbAutomationPostgresError::Unavailable)
    ));
    assert_eq!(
        lease
            .persist_shadow_cycle(
                &stale_revision_cycle_id,
                &policy_digest,
                observed_at + Duration::seconds(2),
                business_date,
                2,
                "{\"observation\":\"complete\"}",
                "{\"action\":\"hold\"}",
            )
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    assert!(
        !lease
            .persist_shadow_cycle(
                &cycle_id,
                &policy_digest,
                observed_at,
                business_date,
                1,
                "{\"observation\":\"complete\"}",
                "{\"action\":\"hold\"}",
            )
            .await
            .expect("identical cycle replay is idempotent")
    );
    assert_eq!(
        lease
            .persist_shadow_cycle(
                &cycle_id,
                &policy_digest,
                observed_at,
                business_date,
                1,
                "{\"observation\":\"complete\"}",
                "{\"action\":\"different\"}",
            )
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    lease.release().await.expect("lock is explicitly released");

    let next = second
        .try_acquire_campaign(&account_id, campaign_id)
        .await
        .expect("post-release lock query succeeds")
        .expect("second store acquires after release");
    next.release().await.expect("second lock is released");

    let abandoned = second
        .try_acquire_campaign(&account_id, campaign_id)
        .await
        .expect("abandonment lock query succeeds")
        .expect("second store reacquires before abnormal drop");
    drop(abandoned);

    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL")
        .expect("test wrapper provides the admin URL");
    let admin_config = Config::from_str(&admin_url).expect("admin URL parses");
    let (admin, admin_connection) = raw_client(&admin_config).await;
    admin
        .batch_execute(
            "REVOKE EXECUTE ON FUNCTION pg_catalog.hashtextextended(text, bigint) FROM PUBLIC",
        )
        .await
        .expect("temporary permission fault is installed");
    let failed_lock = first.try_acquire_campaign(&account_id, campaign_id).await;
    admin
        .batch_execute(
            "GRANT EXECUTE ON FUNCTION pg_catalog.hashtextextended(text, bigint) TO PUBLIC",
        )
        .await
        .expect("public function permission is restored");
    assert!(matches!(
        failed_lock,
        Err(WbAutomationPostgresError::Unavailable)
    ));
    drop(admin);
    admin_connection
        .await
        .expect("admin connection task shuts down");
}
