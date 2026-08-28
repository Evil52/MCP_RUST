use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};

use chrono::{Days, Duration, NaiveDate, TimeZone, Utc};
use mcp_ozon::control::{
    WbAutomationActionReservation, WbAutomationDurableActionKind, WbAutomationDurableActionStatus,
    WbAutomationLegacyStateSeed, WbAutomationPostgresError, WbAutomationPostgresStore,
};
use tokio_postgres::{Config, NoTls};

static CAMPAIGN_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static POSTGRES_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
    reason = "the campaign lease is consumed by the explicit async release under test"
)]
async fn protective_live_policy_activation_is_locked_audited_and_idempotent() {
    let Ok(database_url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
        return;
    };
    let _database_guard = POSTGRES_TEST_LOCK.lock().await;
    let config = Config::from_str(&database_url).expect("test database URL parses");
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL")
        .expect("test wrapper provides the admin URL");
    let admin_config = Config::from_str(&admin_url).expect("admin URL parses");
    let (admin, admin_connection) = raw_client(&admin_config).await;
    let store = WbAutomationPostgresStore::connect(&config)
        .await
        .expect("store connects");
    let campaign_id = 7_000_000_u64 + CAMPAIGN_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let campaign_id_i64 = i64::try_from(campaign_id).expect("campaign fits i64");
    let account_id = format!("wb_automation_activation_{}", std::process::id());
    let shadow_policy_digest = "1".repeat(64);
    let live_policy_digest = "2".repeat(64);
    let cycle_id = "3".repeat(64);
    let bid_policy_digest = "6".repeat(64);
    let paced_policy_digest = "7".repeat(64);
    let frontier_policy_digest = "8".repeat(64);
    let frontier_limits_policy_digest = "9".repeat(64);
    let frontier_corridor_policy_digest = "a".repeat(64);
    let frontier_v3_policy_digest = "b".repeat(64);
    let frontier_v4_policy_digest = "c".repeat(64);
    let live_cycle_id = format!("{campaign_id:064x}");
    let bid_cycle_id = format!("{:064x}", campaign_id + 1);
    let paced_cycle_id = format!("{:064x}", campaign_id + 2);
    let frontier_cycle_id = format!("{:064x}", campaign_id + 3);
    let frontier_limits_cycle_id = format!("{:064x}", campaign_id + 4);
    let frontier_corridor_cycle_id = format!("{:064x}", campaign_id + 5);
    let frontier_v3_cycle_id = format!("{:064x}", campaign_id + 6);
    let business_date = NaiveDate::from_ymd_opt(2026, 8, 26).expect("valid date");
    let observed_at = Utc
        .with_ymd_and_hms(2026, 8, 26, 12, 0, 0)
        .single()
        .expect("valid timestamp");
    let mut lease = store
        .try_acquire_campaign(&account_id, campaign_id)
        .await
        .expect("lock query succeeds")
        .expect("campaign lock is acquired");
    lease
        .initialize_from_legacy(&WbAutomationLegacyStateSeed {
            policy_digest: shadow_policy_digest.clone(),
            business_date,
            actions_today: 1,
            last_action_at: Some(observed_at - Duration::hours(1)),
            paused_for_daily_cap_on: None,
            incident_class: None,
            legacy_digest: "4".repeat(64),
        })
        .await
        .expect("shadow state is initialized");
    lease
        .persist_shadow_cycle(
            &cycle_id,
            &shadow_policy_digest,
            observed_at,
            business_date,
            1,
            "{}",
            "{}",
        )
        .await
        .expect("latest shadow evidence is persisted");

    assert_eq!(
        lease
            .activate_protective_live_policy(&shadow_policy_digest, &shadow_policy_digest)
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    assert_eq!(
        lease
            .activate_protective_live_policy(&"5".repeat(64), &live_policy_digest)
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    admin
        .batch_execute("REVOKE INSERT ON wb_automation.audit_events FROM wb_automation_writer")
        .await
        .expect("audit insert permission fault is installed");
    assert_eq!(
        lease
            .activate_protective_live_policy(&shadow_policy_digest, &live_policy_digest)
            .await,
        Err(WbAutomationPostgresError::Unavailable)
    );
    admin
        .batch_execute("GRANT INSERT ON wb_automation.audit_events TO wb_automation_writer")
        .await
        .expect("audit insert permission is restored");
    let activation = lease
        .activate_protective_live_policy(&shadow_policy_digest, &live_policy_digest)
        .await
        .expect("protective live policy is activated atomically");
    assert!(activation.changed);
    assert_eq!(activation.state_revision, 2);
    let state = lease
        .load_state()
        .await
        .expect("state query succeeds")
        .expect("state exists");
    assert_eq!(state.policy_digest, live_policy_digest);
    assert_eq!(state.revision, 2);

    let replay = lease
        .activate_protective_live_policy(&shadow_policy_digest, &live_policy_digest)
        .await
        .expect("activation replay is idempotent");
    assert!(!replay.changed);
    assert_eq!(replay.state_revision, 2);
    let audit = admin
        .query_one(
            "SELECT cycle_id, payload_json FROM wb_automation.audit_events \
             WHERE account_id=$1 AND advert_id=$2 AND event_type='protective_live_activated'",
            &[&account_id, &campaign_id_i64],
        )
        .await
        .expect("activation audit evidence is readable");
    assert_eq!(audit.get::<_, String>(0), cycle_id);
    let payload = serde_json::from_str::<serde_json::Value>(&audit.get::<_, String>(1))
        .expect("activation audit payload is valid JSON");
    assert_eq!(payload["from_policy_sha256"], shadow_policy_digest);
    assert_eq!(payload["to_policy_sha256"], live_policy_digest);
    assert_eq!(payload["bid_writes_enabled"], false);
    assert_eq!(payload["state_revision"], 2);

    lease
        .persist_shadow_cycle(
            &live_cycle_id,
            &live_policy_digest,
            observed_at + Duration::seconds(1),
            business_date,
            2,
            "{}",
            "{}",
        )
        .await
        .expect("latest protective live evidence is persisted");
    assert_eq!(
        lease
            .activate_bid_writes_policy(&live_policy_digest, &live_policy_digest)
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    assert_eq!(
        lease
            .activate_bid_writes_policy(&"8".repeat(64), &bid_policy_digest)
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    let bid_activation = lease
        .activate_bid_writes_policy(&live_policy_digest, &bid_policy_digest)
        .await
        .expect("bid writes policy is activated atomically");
    assert!(bid_activation.changed);
    assert_eq!(bid_activation.state_revision, 3);
    let bid_state = lease
        .load_state()
        .await
        .expect("bid live state query succeeds")
        .expect("bid live state exists");
    assert_eq!(bid_state.policy_digest, bid_policy_digest);
    assert_eq!(bid_state.revision, 3);
    let bid_replay = lease
        .activate_bid_writes_policy(&live_policy_digest, &bid_policy_digest)
        .await
        .expect("bid activation replay is idempotent");
    assert!(!bid_replay.changed);
    assert_eq!(bid_replay.state_revision, 3);
    let bid_audit = admin
        .query_one(
            "SELECT cycle_id, payload_json FROM wb_automation.audit_events \
             WHERE account_id=$1 AND advert_id=$2 AND event_type='bid_writes_activated'",
            &[&account_id, &campaign_id_i64],
        )
        .await
        .expect("bid activation audit evidence is readable");
    assert_eq!(bid_audit.get::<_, String>(0), live_cycle_id);
    let bid_payload = serde_json::from_str::<serde_json::Value>(&bid_audit.get::<_, String>(1))
        .expect("bid activation audit payload is valid JSON");
    assert_eq!(bid_payload["from_policy_sha256"], live_policy_digest);
    assert_eq!(bid_payload["to_policy_sha256"], bid_policy_digest);
    assert_eq!(bid_payload["mode"], "bid_live");
    assert_eq!(bid_payload["bid_writes_enabled"], true);
    assert_eq!(bid_payload["state_revision"], 3);

    lease
        .persist_shadow_cycle(
            &bid_cycle_id,
            &bid_policy_digest,
            observed_at + Duration::seconds(2),
            business_date,
            3,
            "{}",
            "{}",
        )
        .await
        .expect("latest bid-live evidence is persisted");
    assert_eq!(
        lease
            .activate_bounded_pacing_policy(
                &bid_policy_digest,
                &paced_policy_digest,
                500,
                600,
                5_000,
            )
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    let pacing = lease
        .activate_bounded_pacing_policy(&bid_policy_digest, &paced_policy_digest, 600, 500, 5_000)
        .await
        .expect("bounded pacing policy is activated atomically");
    assert!(pacing.changed);
    assert_eq!(pacing.state_revision, 4);
    let paced_state = lease
        .load_state()
        .await
        .expect("paced state query succeeds")
        .expect("paced state exists");
    assert_eq!(paced_state.policy_digest, paced_policy_digest);
    assert_eq!(paced_state.revision, 4);
    assert_eq!(paced_state.actions_today, 1);
    assert_eq!(
        paced_state.last_action_at,
        Some(observed_at - Duration::hours(1))
    );
    let pacing_replay = lease
        .activate_bounded_pacing_policy(&bid_policy_digest, &paced_policy_digest, 600, 500, 5_000)
        .await
        .expect("bounded pacing activation replay is idempotent");
    assert!(!pacing_replay.changed);
    assert_eq!(pacing_replay.state_revision, 4);
    let pacing_audit = admin
        .query_one(
            "SELECT cycle_id, payload_json FROM wb_automation.audit_events \
             WHERE account_id=$1 AND advert_id=$2 AND event_type='bounded_pacing_activated'",
            &[&account_id, &campaign_id_i64],
        )
        .await
        .expect("bounded pacing audit evidence is readable");
    assert_eq!(pacing_audit.get::<_, String>(0), bid_cycle_id);
    let pacing_payload =
        serde_json::from_str::<serde_json::Value>(&pacing_audit.get::<_, String>(1))
            .expect("bounded pacing audit payload is valid JSON");
    assert_eq!(pacing_payload["from_policy_sha256"], bid_policy_digest);
    assert_eq!(pacing_payload["to_policy_sha256"], paced_policy_digest);
    assert_eq!(pacing_payload["from_max_bid_kopecks"], 600);
    assert_eq!(pacing_payload["to_max_bid_kopecks"], 500);
    assert_eq!(pacing_payload["target_impressions_per_day"], 5_000);
    assert_eq!(pacing_payload["autonomous_pacing_enabled"], true);
    assert_eq!(pacing_payload["state_revision"], 4);

    lease
        .persist_shadow_cycle(
            &paced_cycle_id,
            &paced_policy_digest,
            observed_at + Duration::seconds(3),
            business_date,
            4,
            "{}",
            "{}",
        )
        .await
        .expect("latest bounded-pacing evidence is persisted");
    assert_eq!(
        lease
            .activate_traffic_frontier_v2_policy(
                &paced_policy_digest,
                &frontier_policy_digest,
                500,
                3_000,
                5_400,
                50,
                300,
                1_800,
            )
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    let frontier = lease
        .activate_traffic_frontier_v2_policy(
            &paced_policy_digest,
            &frontier_policy_digest,
            500,
            3_000,
            540,
            50,
            300,
            1_800,
        )
        .await
        .expect("traffic-frontier policy is activated atomically");
    assert!(frontier.changed);
    assert_eq!(frontier.state_revision, 5);
    let frontier_state = lease
        .load_state()
        .await
        .expect("traffic-frontier state query succeeds")
        .expect("traffic-frontier state exists");
    assert_eq!(frontier_state.policy_digest, frontier_policy_digest);
    assert_eq!(frontier_state.revision, 5);
    assert_eq!(frontier_state.actions_today, 1);
    let frontier_replay = lease
        .activate_traffic_frontier_v2_policy(
            &paced_policy_digest,
            &frontier_policy_digest,
            500,
            3_000,
            540,
            50,
            300,
            1_800,
        )
        .await
        .expect("traffic-frontier activation replay is idempotent");
    assert!(!frontier_replay.changed);
    assert_eq!(frontier_replay.state_revision, 5);
    let frontier_audit = admin
        .query_one(
            "SELECT cycle_id, payload_json FROM wb_automation.audit_events \
             WHERE account_id=$1 AND advert_id=$2 AND event_type='traffic_frontier_v2_activated'",
            &[&account_id, &campaign_id_i64],
        )
        .await
        .expect("traffic-frontier audit evidence is readable");
    assert_eq!(frontier_audit.get::<_, String>(0), paced_cycle_id);
    let frontier_payload =
        serde_json::from_str::<serde_json::Value>(&frontier_audit.get::<_, String>(1))
            .expect("traffic-frontier audit payload is valid JSON");
    assert_eq!(frontier_payload["from_policy_sha256"], paced_policy_digest);
    assert_eq!(frontier_payload["to_policy_sha256"], frontier_policy_digest);
    assert_eq!(frontier_payload["from_max_bid_kopecks"], 500);
    assert_eq!(frontier_payload["to_max_bid_kopecks"], 3_000);
    assert_eq!(frontier_payload["traffic_frontier_bid_kopecks"], 540);
    assert_eq!(frontier_payload["max_actions_per_day"], 50);
    assert_eq!(frontier_payload["cooldown_seconds"], 300);
    assert_eq!(frontier_payload["feedback_timeout_seconds"], 1_800);
    assert_eq!(frontier_payload["autonomous_pacing"], "traffic_frontier_v2");
    assert_eq!(frontier_payload["state_revision"], 5);

    lease
        .persist_shadow_cycle(
            &frontier_cycle_id,
            &frontier_policy_digest,
            observed_at + Duration::seconds(4),
            business_date,
            5,
            "{}",
            "{}",
        )
        .await
        .expect("latest traffic-frontier evidence is persisted");
    assert_eq!(
        lease
            .activate_traffic_frontier_limits_policy(
                &frontier_policy_digest,
                &frontier_limits_policy_digest,
                540,
                1_000,
                25_000,
                45_000,
                30_000,
                40_000,
            )
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    let limits = lease
        .activate_traffic_frontier_limits_policy(
            &frontier_policy_digest,
            &frontier_limits_policy_digest,
            540,
            1_000,
            25_000,
            45_000,
            30_000,
            50_000,
        )
        .await
        .expect("traffic-frontier limits are raised atomically");
    assert!(limits.changed);
    assert_eq!(limits.state_revision, 6);
    let limits_state = lease
        .load_state()
        .await
        .expect("traffic-frontier limits state query succeeds")
        .expect("traffic-frontier limits state exists");
    assert_eq!(limits_state.policy_digest, frontier_limits_policy_digest);
    assert_eq!(limits_state.revision, 6);
    assert_eq!(limits_state.actions_today, 1);
    let limits_replay = lease
        .activate_traffic_frontier_limits_policy(
            &frontier_policy_digest,
            &frontier_limits_policy_digest,
            540,
            1_000,
            25_000,
            45_000,
            30_000,
            50_000,
        )
        .await
        .expect("traffic-frontier limits replay is idempotent");
    assert!(!limits_replay.changed);
    assert_eq!(limits_replay.state_revision, 6);
    let limits_audit = admin
        .query_one(
            "SELECT cycle_id, payload_json FROM wb_automation.audit_events \
             WHERE account_id=$1 AND advert_id=$2 \
               AND event_type='traffic_frontier_limits_raised'",
            &[&account_id, &campaign_id_i64],
        )
        .await
        .expect("traffic-frontier limits audit evidence is readable");
    assert_eq!(limits_audit.get::<_, String>(0), frontier_cycle_id);
    let limits_payload =
        serde_json::from_str::<serde_json::Value>(&limits_audit.get::<_, String>(1))
            .expect("traffic-frontier limits audit payload is valid JSON");
    assert_eq!(limits_payload["from_policy_sha256"], frontier_policy_digest);
    assert_eq!(
        limits_payload["to_policy_sha256"],
        frontier_limits_policy_digest
    );
    assert_eq!(limits_payload["from_traffic_frontier_bid_kopecks"], 540);
    assert_eq!(limits_payload["to_traffic_frontier_bid_kopecks"], 1_000);
    assert_eq!(limits_payload["from_daily_pause_threshold_minor"], 25_000);
    assert_eq!(limits_payload["to_daily_pause_threshold_minor"], 45_000);
    assert_eq!(limits_payload["from_daily_spend_cap_minor"], 30_000);
    assert_eq!(limits_payload["to_daily_spend_cap_minor"], 50_000);
    assert_eq!(limits_payload["autonomous_pacing"], "traffic_frontier_v2");
    assert_eq!(limits_payload["state_revision"], 6);

    lease
        .persist_shadow_cycle(
            &frontier_limits_cycle_id,
            &frontier_limits_policy_digest,
            observed_at + Duration::seconds(5),
            business_date,
            6,
            "{}",
            "{}",
        )
        .await
        .expect("latest traffic-frontier limits evidence is persisted");
    assert_eq!(
        lease
            .activate_traffic_frontier_corridor_policy(
                &frontier_limits_policy_digest,
                &frontier_corridor_policy_digest,
                1_000,
                700,
                3_000,
                3_000,
            )
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    let corridor = lease
        .activate_traffic_frontier_corridor_policy(
            &frontier_limits_policy_digest,
            &frontier_corridor_policy_digest,
            1_000,
            700,
            3_000,
            1_200,
        )
        .await
        .expect("traffic-frontier corridor is tightened atomically");
    assert!(corridor.changed);
    assert_eq!(corridor.state_revision, 7);
    let corridor_replay = lease
        .activate_traffic_frontier_corridor_policy(
            &frontier_limits_policy_digest,
            &frontier_corridor_policy_digest,
            1_000,
            700,
            3_000,
            1_200,
        )
        .await
        .expect("traffic-frontier corridor replay is idempotent");
    assert!(!corridor_replay.changed);
    assert_eq!(corridor_replay.state_revision, 7);
    let corridor_audit = admin
        .query_one(
            "SELECT cycle_id, payload_json FROM wb_automation.audit_events \
             WHERE account_id=$1 AND advert_id=$2 \
               AND event_type='traffic_frontier_corridor_tightened'",
            &[&account_id, &campaign_id_i64],
        )
        .await
        .expect("traffic-frontier corridor audit evidence is readable");
    assert_eq!(corridor_audit.get::<_, String>(0), frontier_limits_cycle_id);
    let corridor_payload =
        serde_json::from_str::<serde_json::Value>(&corridor_audit.get::<_, String>(1))
            .expect("traffic-frontier corridor audit payload is valid JSON");
    assert_eq!(
        corridor_payload["from_policy_sha256"],
        frontier_limits_policy_digest
    );
    assert_eq!(
        corridor_payload["to_policy_sha256"],
        frontier_corridor_policy_digest
    );
    assert_eq!(corridor_payload["from_traffic_frontier_bid_kopecks"], 1_000);
    assert_eq!(corridor_payload["to_traffic_frontier_bid_kopecks"], 700);
    assert_eq!(corridor_payload["from_max_bid_kopecks"], 3_000);
    assert_eq!(corridor_payload["to_max_bid_kopecks"], 1_200);
    assert_eq!(corridor_payload["autonomous_pacing"], "traffic_frontier_v2");
    assert_eq!(corridor_payload["state_revision"], 7);

    lease
        .persist_shadow_cycle(
            &frontier_corridor_cycle_id,
            &frontier_corridor_policy_digest,
            observed_at + Duration::seconds(6),
            business_date,
            7,
            "{}",
            "{}",
        )
        .await
        .expect("latest traffic-frontier corridor evidence is persisted");
    assert_eq!(
        lease
            .activate_traffic_frontier_v3_policy(
                &frontier_corridor_policy_digest,
                &frontier_v3_policy_digest,
                1_500,
                3,
                13,
                1_800,
                3_600,
                200,
                10,
            )
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    let v3 = lease
        .activate_traffic_frontier_v3_policy(
            &frontier_corridor_policy_digest,
            &frontier_v3_policy_digest,
            1_500,
            3,
            12,
            1_800,
            3_600,
            200,
            10,
        )
        .await
        .expect("traffic-frontier v3 policy is activated atomically");
    assert!(v3.changed);
    assert_eq!(v3.state_revision, 8);
    let v3_state = lease
        .load_state()
        .await
        .expect("traffic-frontier v3 state query succeeds")
        .expect("traffic-frontier v3 state exists");
    assert_eq!(v3_state.policy_digest, frontier_v3_policy_digest);
    assert_eq!(v3_state.actions_today, 1);
    assert_eq!(
        v3_state.last_action_at,
        Some(observed_at - Duration::hours(1))
    );
    let v3_replay = lease
        .activate_traffic_frontier_v3_policy(
            &frontier_corridor_policy_digest,
            &frontier_v3_policy_digest,
            1_500,
            3,
            12,
            1_800,
            3_600,
            200,
            10,
        )
        .await
        .expect("traffic-frontier v3 activation replay is idempotent");
    assert!(!v3_replay.changed);
    assert_eq!(v3_replay.state_revision, 8);
    let v3_audit = admin
        .query_one(
            "SELECT cycle_id, payload_json FROM wb_automation.audit_events \
             WHERE account_id=$1 AND advert_id=$2 \
               AND event_type='traffic_frontier_v3_activated'",
            &[&account_id, &campaign_id_i64],
        )
        .await
        .expect("traffic-frontier v3 audit evidence is readable");
    assert_eq!(v3_audit.get::<_, String>(0), frontier_corridor_cycle_id);
    let v3_payload = serde_json::from_str::<serde_json::Value>(&v3_audit.get::<_, String>(1))
        .expect("traffic-frontier v3 audit payload is valid JSON");
    assert_eq!(
        v3_payload["from_policy_sha256"],
        frontier_corridor_policy_digest
    );
    assert_eq!(v3_payload["to_policy_sha256"], frontier_v3_policy_digest);
    assert_eq!(v3_payload["autonomous_pacing"], "traffic_frontier_v3");
    assert_eq!(v3_payload["target_impressions_per_day"], 1_500);
    assert_eq!(v3_payload["target_orders_per_day"], 3);
    assert_eq!(v3_payload["max_actions_per_day"], 12);
    assert_eq!(v3_payload["cooldown_seconds"], 1_800);
    assert_eq!(v3_payload["feedback_timeout_seconds"], 3_600);
    assert_eq!(v3_payload["min_feedback_impressions"], 200);
    assert_eq!(v3_payload["min_feedback_clicks"], 10);
    assert_eq!(v3_payload["state_revision"], 8);

    lease
        .persist_shadow_cycle(
            &frontier_v3_cycle_id,
            &frontier_v3_policy_digest,
            observed_at + Duration::seconds(7),
            business_date,
            8,
            "{}",
            "{}",
        )
        .await
        .expect("latest traffic-frontier v3 evidence is persisted");
    assert_eq!(
        lease
            .activate_traffic_frontier_v4_policy(
                &frontier_v3_policy_digest,
                &frontier_v4_policy_digest,
                1_500,
                2_500,
                700,
                10,
                1_500,
                3,
                48,
                1_800,
                1_800,
                200,
                10,
            )
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    let v4 = lease
        .activate_traffic_frontier_v4_policy(
            &frontier_v3_policy_digest,
            &frontier_v4_policy_digest,
            1_500,
            1_500,
            700,
            10,
            1_500,
            3,
            48,
            1_800,
            1_800,
            200,
            10,
        )
        .await
        .expect("traffic-frontier v4 policy is activated atomically");
    assert!(v4.changed);
    assert_eq!(v4.state_revision, 9);
    let v4_state = lease
        .load_state()
        .await
        .expect("traffic-frontier v4 state query succeeds")
        .expect("traffic-frontier v4 state exists");
    assert_eq!(v4_state.policy_digest, frontier_v4_policy_digest);
    assert_eq!(v4_state.actions_today, 1);
    let v4_replay = lease
        .activate_traffic_frontier_v4_policy(
            &frontier_v3_policy_digest,
            &frontier_v4_policy_digest,
            1_500,
            1_500,
            700,
            10,
            1_500,
            3,
            48,
            1_800,
            1_800,
            200,
            10,
        )
        .await
        .expect("traffic-frontier v4 activation replay is idempotent");
    assert!(!v4_replay.changed);
    assert_eq!(v4_replay.state_revision, 9);
    let v4_audit = admin
        .query_one(
            "SELECT cycle_id, payload_json FROM wb_automation.audit_events \
             WHERE account_id=$1 AND advert_id=$2 \
               AND event_type='traffic_frontier_v4_activated'",
            &[&account_id, &campaign_id_i64],
        )
        .await
        .expect("traffic-frontier v4 audit evidence is readable");
    assert_eq!(v4_audit.get::<_, String>(0), frontier_v3_cycle_id);
    let v4_payload = serde_json::from_str::<serde_json::Value>(&v4_audit.get::<_, String>(1))
        .expect("traffic-frontier v4 audit payload is valid JSON");
    assert_eq!(v4_payload["autonomous_pacing"], "traffic_frontier_v4");
    assert_eq!(v4_payload["target_drr_basis_points"], 1_500);
    assert_eq!(v4_payload["hard_drr_basis_points"], 1_500);
    assert_eq!(v4_payload["traffic_frontier_bid_kopecks"], 700);
    assert_eq!(v4_payload["bid_step_percent"], 10);
    assert_eq!(v4_payload["max_actions_per_day"], 48);
    assert_eq!(v4_payload["feedback_timeout_seconds"], 1_800);
    assert_eq!(v4_payload["zero_cost_probe_enabled"], true);
    assert_eq!(v4_payload["state_revision"], 9);

    lease.release().await.expect("campaign lock is released");
    drop(admin);
    admin_connection
        .await
        .expect("admin connection task shuts down");
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "leases are consumed by explicit async release, which is the behavior under test"
)]
async fn automation_state_is_isolated_locked_and_idempotent() {
    #[cfg(coverage)]
    mcp_ozon::control::exercise_coverage_only_database_mappings();
    let Ok(database_url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
        return;
    };
    let _database_guard = POSTGRES_TEST_LOCK.lock().await;
    let config = Config::from_str(&database_url).expect("test database URL parses");
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL")
        .expect("test wrapper provides the admin URL");
    let admin_config = Config::from_str(&admin_url).expect("admin URL parses");
    let (admin, admin_connection) = raw_client(&admin_config).await;
    let campaign_id = 8_000_000_u64 + CAMPAIGN_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let account_id = format!("wb_automation_test_{}", std::process::id());
    let policy_digest = "a".repeat(64);
    let cycle_id = "b".repeat(64);
    let unique_conflict_cycle_id = "c".repeat(64);
    let invalid_date_cycle_id = "d".repeat(64);
    let stale_revision_cycle_id = "e".repeat(64);
    let readback_cycle_id = "6".repeat(64);
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
    assert_eq!(
        lease
            .load_last_applied_snapshot_json()
            .await
            .expect("missing feedback baseline query succeeds"),
        None
    );

    let reservation = WbAutomationActionReservation {
        idempotency_key: "1".repeat(64),
        cycle_id: cycle_id.clone(),
        policy_digest: policy_digest.clone(),
        request_digest: "2".repeat(64),
        action_kind: WbAutomationDurableActionKind::ChangeBids,
        request_json:
            "{\"kind\":\"change_bids\",\"changes\":[{\"nm_id\":449627598,\"bid_kopecks\":117}]}"
                .to_owned(),
        business_date,
        expected_state_revision: 1,
        max_actions_per_day: 2,
    };
    let mut invalid_idempotency = reservation.clone();
    invalid_idempotency.idempotency_key = "short".to_owned();
    let mut invalid_cycle_digest = reservation.clone();
    invalid_cycle_digest.cycle_id = "short".to_owned();
    let mut invalid_policy_digest = reservation.clone();
    invalid_policy_digest.policy_digest = "short".to_owned();
    let mut invalid_request_digest = reservation.clone();
    invalid_request_digest.request_digest = "short".to_owned();
    let mut invalid_request_json = reservation.clone();
    invalid_request_json.request_json = "{".to_owned();
    for invalid_reservation in [
        invalid_idempotency,
        invalid_cycle_digest,
        invalid_policy_digest,
        invalid_request_digest,
        invalid_request_json,
    ] {
        assert_eq!(
            lease.reserve_action(&invalid_reservation).await,
            Err(WbAutomationPostgresError::InvalidInput)
        );
    }
    assert_eq!(
        lease
            .reserve_explicit_quota_override_action(&reservation, "")
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    assert_eq!(
        lease
            .reserve_explicit_quota_override_action(&reservation, "chat/2026-08-28/not-exhausted",)
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    assert_eq!(
        lease
            .cancel_reserved(&reservation.idempotency_key, 1, "Bad")
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    let mut missing_cycle_reservation = reservation.clone();
    missing_cycle_reservation.cycle_id = "f".repeat(64);
    assert_eq!(
        lease.reserve_action(&missing_cycle_reservation).await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    let mut old_date_reservation = reservation.clone();
    old_date_reservation.business_date = business_date
        .pred_opt()
        .expect("previous business date is representable");
    assert_eq!(
        lease.reserve_action(&old_date_reservation).await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    admin
        .batch_execute("REVOKE INSERT ON wb_automation.action_attempts FROM wb_automation_writer")
        .await
        .expect("reservation insert fault is installed");
    assert_eq!(
        lease.reserve_action(&reservation).await,
        Err(WbAutomationPostgresError::Unavailable)
    );
    admin
        .batch_execute("GRANT INSERT ON wb_automation.action_attempts TO wb_automation_writer")
        .await
        .expect("reservation insert permission is restored");
    admin
        .batch_execute("REVOKE INSERT ON wb_automation.audit_events FROM wb_automation_writer")
        .await
        .expect("reservation audit fault is installed");
    assert_eq!(
        lease.reserve_action(&reservation).await,
        Err(WbAutomationPostgresError::Unavailable)
    );
    admin
        .batch_execute("GRANT INSERT ON wb_automation.audit_events TO wb_automation_writer")
        .await
        .expect("reservation audit permission is restored");
    let reserved = lease
        .reserve_action(&reservation)
        .await
        .expect("first durable action is reserved");
    assert!(reserved.inserted);
    assert_eq!(reserved.state_revision, 2);
    assert_eq!(
        reserved.action.status,
        WbAutomationDurableActionStatus::Reserved
    );
    assert_eq!(
        lease.load_pending_action("short", 2).await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    assert_eq!(
        lease
            .load_pending_action(&reservation.idempotency_key, 1)
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    assert_eq!(
        lease
            .load_pending_action(&reservation.idempotency_key, 2)
            .await
            .expect("pending action is loaded by exact state revision"),
        reserved.action
    );
    admin
        .batch_execute("REVOKE SELECT ON wb_automation.action_attempts FROM wb_automation_writer")
        .await
        .expect("reservation replay read fault is installed");
    assert_eq!(
        lease.reserve_action(&reservation).await,
        Err(WbAutomationPostgresError::Unavailable)
    );
    assert_eq!(
        lease
            .load_pending_action(&reservation.idempotency_key, 2)
            .await,
        Err(WbAutomationPostgresError::Unavailable)
    );
    admin
        .batch_execute("GRANT SELECT ON wb_automation.action_attempts TO wb_automation_writer")
        .await
        .expect("reservation replay read permission is restored");
    let replayed = lease
        .reserve_action(&reservation)
        .await
        .expect("identical reservation replay is idempotent");
    assert!(!replayed.inserted);
    assert_eq!(replayed.state_revision, 2);
    let mut mismatched_replay = reservation.clone();
    mismatched_replay.request_digest = "4".repeat(64);
    assert_eq!(
        lease.reserve_action(&mismatched_replay).await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    let mut conflicting_reservation = reservation.clone();
    conflicting_reservation.idempotency_key = "3".repeat(64);
    conflicting_reservation.request_digest = "4".repeat(64);
    assert_eq!(
        lease.reserve_action(&conflicting_reservation).await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    assert_eq!(
        lease
            .mark_write_started(&reservation.idempotency_key, 1)
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    assert_eq!(
        lease
            .mark_applied(
                &reservation.idempotency_key,
                2,
                &cycle_id,
                Some(business_date),
            )
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    assert_eq!(
        lease
            .mark_applied(&reservation.idempotency_key, 2, &cycle_id, None)
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    admin
        .batch_execute("REVOKE INSERT ON wb_automation.audit_events FROM wb_automation_writer")
        .await
        .expect("write-start audit fault is installed");
    assert_eq!(
        lease
            .mark_write_started(&reservation.idempotency_key, 2)
            .await,
        Err(WbAutomationPostgresError::Unavailable)
    );
    admin
        .batch_execute("GRANT INSERT ON wb_automation.audit_events TO wb_automation_writer")
        .await
        .expect("write-start audit permission is restored");
    admin
        .batch_execute("REVOKE SELECT ON wb_automation.action_attempts FROM wb_automation_writer")
        .await
        .expect("write-start read fault is installed");
    assert_eq!(
        lease
            .mark_write_started(&reservation.idempotency_key, 2)
            .await,
        Err(WbAutomationPostgresError::Unavailable)
    );
    admin
        .batch_execute("GRANT SELECT ON wb_automation.action_attempts TO wb_automation_writer")
        .await
        .expect("write-start read permission is restored");
    assert!(
        lease
            .mark_write_started(&reservation.idempotency_key, 2)
            .await
            .expect("reserved action transitions to write-started")
    );
    assert!(
        !lease
            .mark_write_started(&reservation.idempotency_key, 2)
            .await
            .expect("write-started replay is idempotent")
    );
    assert!(
        lease
            .mark_awaiting_readback(&reservation.idempotency_key, 2)
            .await
            .expect("write-started action awaits readback")
    );
    assert!(
        !lease
            .mark_awaiting_readback(&reservation.idempotency_key, 2)
            .await
            .expect("awaiting-readback replay is idempotent")
    );
    assert_eq!(
        lease
            .mark_write_started(&reservation.idempotency_key, 2)
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    assert_eq!(
        lease
            .mark_applied(&reservation.idempotency_key, 2, &cycle_id, None)
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    assert!(
        lease
            .persist_shadow_cycle(
                &readback_cycle_id,
                &policy_digest,
                observed_at + Duration::seconds(3),
                business_date,
                2,
                "{\"observation\":\"readback_complete\"}",
                "{\"action\":\"hold\"}",
            )
            .await
            .expect("post-write readback cycle is persisted")
    );
    admin
        .batch_execute("REVOKE INSERT ON wb_automation.audit_events FROM wb_automation_writer")
        .await
        .expect("applied audit fault is installed");
    assert_eq!(
        lease
            .mark_applied(&reservation.idempotency_key, 2, &readback_cycle_id, None)
            .await,
        Err(WbAutomationPostgresError::Unavailable)
    );
    admin
        .batch_execute("GRANT INSERT ON wb_automation.audit_events TO wb_automation_writer")
        .await
        .expect("applied audit permission is restored");
    admin
        .batch_execute("REVOKE SELECT ON wb_automation.action_attempts FROM wb_automation_writer")
        .await
        .expect("applied read fault is installed");
    assert_eq!(
        lease
            .mark_applied(&reservation.idempotency_key, 2, &readback_cycle_id, None)
            .await,
        Err(WbAutomationPostgresError::Unavailable)
    );
    admin
        .batch_execute("GRANT SELECT ON wb_automation.action_attempts TO wb_automation_writer")
        .await
        .expect("applied read permission is restored");
    let applied = lease
        .mark_applied(&reservation.idempotency_key, 2, &readback_cycle_id, None)
        .await
        .expect("readback resolves the durable action");
    assert!(applied.changed);
    assert_eq!(applied.state_revision, 3);
    let applied_replay = lease
        .mark_applied(&reservation.idempotency_key, 2, &readback_cycle_id, None)
        .await
        .expect("applied transition replay is idempotent");
    assert!(!applied_replay.changed);
    assert_eq!(applied_replay.state_revision, 3);
    assert_eq!(
        lease
            .load_last_applied_snapshot_json()
            .await
            .expect("latest applied feedback baseline is readable")
            .as_deref(),
        Some("{\"observation\":\"complete\"}")
    );
    assert_eq!(
        lease
            .mark_applied(&reservation.idempotency_key, 2, &"a".repeat(64), None,)
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    let durable_state = lease
        .load_state()
        .await
        .expect("durable state remains readable")
        .expect("durable state exists");
    assert_eq!(durable_state.actions_today, 1);
    assert_eq!(durable_state.revision, 3);
    assert!(durable_state.pending_idempotency_key.is_none());

    let cancellation_cycle_id = "7".repeat(64);
    assert!(
        lease
            .persist_shadow_cycle(
                &cancellation_cycle_id,
                &policy_digest,
                observed_at + Duration::seconds(4),
                business_date,
                3,
                "{\"observation\":\"complete\"}",
                "{\"action\":\"change_bids\"}",
            )
            .await
            .expect("cancellation decision cycle is persisted")
    );
    let cancellation = WbAutomationActionReservation {
        idempotency_key: "8".repeat(64),
        cycle_id: cancellation_cycle_id,
        policy_digest: policy_digest.clone(),
        request_digest: "9".repeat(64),
        action_kind: WbAutomationDurableActionKind::ChangeBids,
        request_json:
            "{\"kind\":\"change_bids\",\"changes\":[{\"nm_id\":449627015,\"bid_kopecks\":118}]}"
                .to_owned(),
        business_date,
        expected_state_revision: 3,
        max_actions_per_day: 3,
    };
    assert_eq!(
        lease
            .reserve_action(&cancellation)
            .await
            .expect("cancellable action is reserved")
            .state_revision,
        4
    );
    assert_eq!(
        lease
            .cancel_reserved(&cancellation.idempotency_key, 3, "policy_changed")
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    admin
        .batch_execute("REVOKE INSERT ON wb_automation.audit_events FROM wb_automation_writer")
        .await
        .expect("cancellation audit fault is installed");
    assert_eq!(
        lease
            .cancel_reserved(&cancellation.idempotency_key, 4, "policy_changed")
            .await,
        Err(WbAutomationPostgresError::Unavailable)
    );
    admin
        .batch_execute("GRANT INSERT ON wb_automation.audit_events TO wb_automation_writer")
        .await
        .expect("cancellation audit permission is restored");
    admin
        .batch_execute("REVOKE SELECT ON wb_automation.action_attempts FROM wb_automation_writer")
        .await
        .expect("cancellation read fault is installed");
    assert_eq!(
        lease
            .cancel_reserved(&cancellation.idempotency_key, 4, "policy_changed")
            .await,
        Err(WbAutomationPostgresError::Unavailable)
    );
    admin
        .batch_execute("GRANT SELECT ON wb_automation.action_attempts TO wb_automation_writer")
        .await
        .expect("cancellation read permission is restored");
    let cancelled = lease
        .cancel_reserved(&cancellation.idempotency_key, 4, "policy_changed")
        .await
        .expect("unstarted write is cancelled durably");
    assert!(cancelled.changed);
    assert_eq!(cancelled.state_revision, 5);
    let cancelled_replay = lease
        .cancel_reserved(&cancellation.idempotency_key, 4, "policy_changed")
        .await
        .expect("cancellation replay is idempotent");
    assert!(!cancelled_replay.changed);
    assert_eq!(cancelled_replay.state_revision, 5);
    assert_eq!(
        lease
            .cancel_reserved(&cancellation.idempotency_key, 4, "operator_cancelled")
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );

    let pause_cycle_id = "0".repeat(64);
    assert!(
        lease
            .persist_shadow_cycle(
                &pause_cycle_id,
                &policy_digest,
                observed_at + Duration::seconds(5),
                business_date,
                5,
                "{\"observation\":\"daily_cap_reached\"}",
                "{\"action\":\"pause_campaign_for_daily_cap\"}",
            )
            .await
            .expect("protective-pause decision cycle is persisted")
    );
    let pause = WbAutomationActionReservation {
        idempotency_key: "5".repeat(64),
        cycle_id: pause_cycle_id,
        policy_digest: policy_digest.clone(),
        request_digest: "7".repeat(64),
        action_kind: WbAutomationDurableActionKind::PauseCampaignForDailyCap,
        request_json: "{\"kind\":\"pause_campaign_for_daily_cap\"}".to_owned(),
        business_date,
        expected_state_revision: 5,
        max_actions_per_day: 3,
    };
    let mut exhausted_quota = pause.clone();
    exhausted_quota.max_actions_per_day = 2;
    assert_eq!(
        lease.reserve_action(&exhausted_quota).await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    assert_eq!(
        lease
            .reserve_action(&pause)
            .await
            .expect("protective pause is reserved")
            .state_revision,
        6
    );
    assert!(
        lease
            .mark_write_started(&pause.idempotency_key, 6)
            .await
            .expect("protective pause write starts")
    );
    assert_eq!(
        lease
            .cancel_reserved(&pause.idempotency_key, 6, "policy_changed")
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    assert_eq!(
        lease
            .mark_applied(&pause.idempotency_key, 6, &pause.cycle_id, None)
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    assert!(
        lease
            .mark_awaiting_readback(&pause.idempotency_key, 6)
            .await
            .expect("protective pause awaits readback before reconciliation")
    );
    let reconciliation = lease
        .mark_reconciliation_required(&pause.idempotency_key, 6, "readback_unavailable")
        .await
        .expect("ambiguous write enters reconciliation");
    assert!(reconciliation.changed);
    assert_eq!(reconciliation.state_revision, 7);
    let reconciliation_replay = lease
        .mark_reconciliation_required(&pause.idempotency_key, 6, "readback_unavailable")
        .await
        .expect("reconciliation replay is idempotent");
    assert!(!reconciliation_replay.changed);
    assert_eq!(reconciliation_replay.state_revision, 7);
    assert_eq!(
        lease
            .mark_reconciliation_required(&pause.idempotency_key, 6, "write_timeout")
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    let pause_readback_cycle_id = "9".repeat(64);
    assert!(
        lease
            .persist_shadow_cycle(
                &pause_readback_cycle_id,
                &policy_digest,
                observed_at + Duration::seconds(6),
                business_date,
                7,
                "{\"observation\":\"campaign_paused\"}",
                "{\"action\":\"hold\"}",
            )
            .await
            .expect("reconciliation readback is persisted")
    );
    let pause_applied = lease
        .mark_applied(
            &pause.idempotency_key,
            7,
            &pause_readback_cycle_id,
            Some(business_date),
        )
        .await
        .expect("readback resolves reconciliation as applied");
    assert!(pause_applied.changed);
    assert_eq!(pause_applied.state_revision, 8);
    assert_eq!(
        lease
            .mark_applied(
                &pause.idempotency_key,
                7,
                &pause_readback_cycle_id,
                business_date.pred_opt(),
            )
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    let final_state = lease
        .load_state()
        .await
        .expect("final state remains readable")
        .expect("final state exists");
    assert_eq!(final_state.actions_today, 3);
    assert_eq!(final_state.revision, 8);
    assert!(final_state.pending_idempotency_key.is_none());
    assert_eq!(final_state.paused_for_daily_cap_on, Some(business_date));
    assert_eq!(
        final_state.incident_class.as_deref(),
        Some("readback_unavailable")
    );

    lease.release().await.expect("lock is explicitly released");

    let rollover_campaign_id = campaign_id + 1_000;
    let mut rollover = first
        .try_acquire_campaign(&account_id, rollover_campaign_id)
        .await
        .expect("rollover lock query succeeds")
        .expect("rollover campaign lock is acquired");
    assert!(
        rollover
            .initialize_from_legacy(&legacy_seed)
            .await
            .expect("rollover campaign state is initialized")
    );
    let next_business_date = business_date
        .succ_opt()
        .expect("next business date is representable");
    let next_date_cycle_id = "a".repeat(64);
    assert!(
        rollover
            .persist_shadow_cycle(
                &next_date_cycle_id,
                &policy_digest,
                observed_at + Duration::days(1),
                next_business_date,
                1,
                "{\"observation\":\"next_business_date\"}",
                "{\"action\":\"change_bids\"}",
            )
            .await
            .expect("next-date decision cycle is persisted")
    );
    let next_date_action = WbAutomationActionReservation {
        idempotency_key: "b".repeat(64),
        cycle_id: next_date_cycle_id,
        policy_digest: policy_digest.clone(),
        request_digest: "c".repeat(64),
        action_kind: WbAutomationDurableActionKind::ChangeBids,
        request_json:
            "{\"kind\":\"change_bids\",\"changes\":[{\"nm_id\":497424314,\"bid_kopecks\":119}]}"
                .to_owned(),
        business_date: next_business_date,
        expected_state_revision: 1,
        max_actions_per_day: 3,
    };
    let mut globally_conflicting_action = next_date_action.clone();
    globally_conflicting_action.idempotency_key = reservation.idempotency_key.clone();
    assert_eq!(
        rollover.reserve_action(&globally_conflicting_action).await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    assert_eq!(
        rollover
            .reserve_action(&next_date_action)
            .await
            .expect("next-date action resets the daily counter")
            .state_revision,
        2
    );
    assert!(
        rollover
            .mark_write_started(&next_date_action.idempotency_key, 2)
            .await
            .expect("next-date action starts its write")
    );
    assert_eq!(
        rollover
            .mark_reconciliation_required(&next_date_action.idempotency_key, 2, "write_ambiguous",)
            .await
            .expect("write-started action enters reconciliation")
            .state_revision,
        3
    );
    let rollover_readback_cycle_id = "d".repeat(64);
    assert!(
        rollover
            .persist_shadow_cycle(
                &rollover_readback_cycle_id,
                &policy_digest,
                observed_at + Duration::days(1) + Duration::seconds(1),
                next_business_date,
                3,
                "{\"observation\":\"next_business_date_readback\"}",
                "{\"action\":\"hold\"}",
            )
            .await
            .expect("next-date readback cycle is persisted")
    );
    assert_eq!(
        rollover
            .mark_applied(
                &next_date_action.idempotency_key,
                3,
                &rollover_readback_cycle_id,
                None,
            )
            .await
            .expect("next-date action is reconciled")
            .state_revision,
        4
    );
    rollover
        .release()
        .await
        .expect("rollover lock is explicitly released");

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

    let campaign_id_i64 = i64::try_from(campaign_id).expect("campaign fits i64");
    let action_counts = admin
        .query(
            "SELECT status, count(*) FROM wb_automation.action_attempts \
             WHERE account_id=$1 AND advert_id=$2 GROUP BY status ORDER BY status",
            &[&account_id, &campaign_id_i64],
        )
        .await
        .expect("durable action evidence is readable");
    let counts = action_counts
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(counts.get("applied"), Some(&2));
    assert_eq!(counts.get("cancelled"), Some(&1));
    let audit_count = admin
        .query_one(
            "SELECT count(*) FROM wb_automation.audit_events \
             WHERE account_id=$1 AND advert_id=$2",
            &[&account_id, &campaign_id_i64],
        )
        .await
        .expect("append-only audit evidence is readable")
        .get::<_, i64>(0);
    assert_eq!(audit_count, 11);
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

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "the campaign lease is consumed by the explicit async release under test"
)]
async fn incident_without_action_is_sticky_audited_and_resets_daily_quota() {
    let Ok(database_url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
        return;
    };
    let _database_guard = POSTGRES_TEST_LOCK.lock().await;
    let config = Config::from_str(&database_url).expect("test database URL parses");
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL")
        .expect("test wrapper provides the admin URL");
    let admin_config = Config::from_str(&admin_url).expect("admin URL parses");
    let (admin, admin_connection) = raw_client(&admin_config).await;
    let store = WbAutomationPostgresStore::connect(&config)
        .await
        .expect("store connects");
    let campaign_id = 9_000_000_u64 + CAMPAIGN_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let account_id = format!("wb_automation_incident_{}", std::process::id());
    let policy_digest = "7".repeat(64);
    let cycle_id = "8".repeat(64);
    let previous_date = NaiveDate::from_ymd_opt(2026, 8, 25).expect("valid date");
    let business_date = previous_date.succ_opt().expect("next date exists");
    let observed_at = Utc
        .with_ymd_and_hms(2026, 8, 26, 12, 0, 0)
        .single()
        .expect("valid timestamp");
    let mut lease = store
        .try_acquire_campaign(&account_id, campaign_id)
        .await
        .expect("lock query succeeds")
        .expect("campaign lock is acquired");
    lease
        .initialize_from_legacy(&WbAutomationLegacyStateSeed {
            policy_digest: policy_digest.clone(),
            business_date: previous_date,
            actions_today: 2,
            last_action_at: Some(observed_at - Duration::hours(1)),
            paused_for_daily_cap_on: None,
            incident_class: None,
            legacy_digest: "9".repeat(64),
        })
        .await
        .expect("legacy state is initialized");
    lease
        .persist_shadow_cycle(
            &cycle_id,
            &policy_digest,
            observed_at,
            business_date,
            1,
            "{}",
            "{}",
        )
        .await
        .expect("incident observation is persisted");
    assert_eq!(
        lease
            .mark_incident_without_action("short", 1, business_date, "manual_resume_required")
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    assert_eq!(
        lease
            .mark_incident_without_action(&cycle_id, 1, business_date, "Bad")
            .await,
        Err(WbAutomationPostgresError::InvalidInput)
    );
    assert_eq!(
        lease
            .mark_incident_without_action(&cycle_id, 2, business_date, "manual_resume_required",)
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    assert_eq!(
        lease
            .mark_incident_without_action(&cycle_id, 1, previous_date, "manual_resume_required",)
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    admin
        .batch_execute("REVOKE INSERT ON wb_automation.audit_events FROM wb_automation_writer")
        .await
        .expect("incident audit permission fault is installed");
    assert_eq!(
        lease
            .mark_incident_without_action(&cycle_id, 1, business_date, "manual_resume_required")
            .await,
        Err(WbAutomationPostgresError::Unavailable)
    );
    admin
        .batch_execute("GRANT INSERT ON wb_automation.audit_events TO wb_automation_writer")
        .await
        .expect("incident audit permission is restored");
    let transition = lease
        .mark_incident_without_action(&cycle_id, 1, business_date, "manual_resume_required")
        .await
        .expect("incident is locked atomically");
    assert!(transition.changed);
    assert_eq!(transition.state_revision, 2);
    let state = lease
        .load_state()
        .await
        .expect("state query succeeds")
        .expect("state exists");
    assert_eq!(state.business_date, business_date);
    assert_eq!(state.actions_today, 0);
    assert_eq!(
        state.incident_class.as_deref(),
        Some("manual_resume_required")
    );
    assert_eq!(
        lease
            .mark_incident_without_action(&cycle_id, 1, business_date, "manual_resume_required",)
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    lease.release().await.expect("campaign lock is released");
    drop(admin);
    admin_connection
        .await
        .expect("admin connection task shuts down");
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "the campaign lease is consumed by the explicit async release under test"
)]
async fn explicit_quota_override_is_counted_audited_and_single_use() {
    let Ok(database_url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
        return;
    };
    let _database_guard = POSTGRES_TEST_LOCK.lock().await;
    let config = Config::from_str(&database_url).expect("test database URL parses");
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL")
        .expect("test wrapper provides the admin URL");
    let admin_config = Config::from_str(&admin_url).expect("admin URL parses");
    let (admin, admin_connection) = raw_client(&admin_config).await;
    let store = WbAutomationPostgresStore::connect(&config)
        .await
        .expect("store connects");
    let campaign_id = 7_300_000_u64 + CAMPAIGN_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let campaign_id_i64 = i64::try_from(campaign_id).expect("campaign fits i64");
    let account_id = format!("wb_quota_override_{}", std::process::id());
    let policy_digest = "c".repeat(64);
    let cycle_id = format!("{campaign_id:064x}");
    let replay_cycle_id = format!("{:064x}", campaign_id + 1);
    let business_date = NaiveDate::from_ymd_opt(2026, 8, 28).expect("valid date");
    let observed_at = Utc
        .with_ymd_and_hms(2026, 8, 28, 9, 0, 0)
        .single()
        .expect("valid timestamp");
    let authorization_reference = "chat/2026-08-28/one-extra-audited-action";
    let mut lease = store
        .try_acquire_campaign(&account_id, campaign_id)
        .await
        .expect("lock query succeeds")
        .expect("campaign lock is acquired");
    lease
        .initialize_from_legacy(&WbAutomationLegacyStateSeed {
            policy_digest: policy_digest.clone(),
            business_date,
            actions_today: 12,
            last_action_at: None,
            paused_for_daily_cap_on: None,
            incident_class: None,
            legacy_digest: "d".repeat(64),
        })
        .await
        .expect("quota-exhausted state is initialized");
    lease
        .persist_shadow_cycle(
            &cycle_id,
            &policy_digest,
            observed_at,
            business_date,
            1,
            "{}",
            "{}",
        )
        .await
        .expect("override decision cycle is persisted");
    let reservation = WbAutomationActionReservation {
        idempotency_key: "e".repeat(64),
        cycle_id: cycle_id.clone(),
        policy_digest: policy_digest.clone(),
        request_digest: "f".repeat(64),
        action_kind: WbAutomationDurableActionKind::ChangeBids,
        request_json:
            "{\"kind\":\"change_bids\",\"changes\":[{\"nm_id\":449627598,\"bid_kopecks\":700}]}"
                .to_owned(),
        business_date,
        expected_state_revision: 1,
        max_actions_per_day: 12,
    };
    assert_eq!(
        lease.reserve_action(&reservation).await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    let reserved = lease
        .reserve_explicit_quota_override_action(&reservation, authorization_reference)
        .await
        .expect("one extra action is reserved without resetting the counter");
    assert_eq!(reserved.state_revision, 2);
    assert!(reserved.inserted);
    let cancelled = lease
        .cancel_reserved(&reservation.idempotency_key, 2, "operator_cancelled")
        .await
        .expect("test reservation is resolved without a marketplace write");
    assert_eq!(cancelled.state_revision, 3);

    let audit = admin
        .query_one(
            "SELECT payload_json FROM wb_automation.audit_events \
             WHERE account_id=$1 AND advert_id=$2 \
               AND event_type='explicit_quota_override_authorized'",
            &[&account_id, &campaign_id_i64],
        )
        .await
        .expect("one-time authorization audit exists");
    let payload = serde_json::from_str::<serde_json::Value>(audit.get(0))
        .expect("authorization audit payload parses");
    assert_eq!(payload["authorization_reference"], authorization_reference);
    assert_eq!(payload["actions_before"], 12);
    assert_eq!(payload["policy_max_actions_per_day"], 12);

    lease
        .persist_shadow_cycle(
            &replay_cycle_id,
            &policy_digest,
            observed_at + Duration::hours(1),
            business_date,
            3,
            "{}",
            "{}",
        )
        .await
        .expect("second decision cycle is persisted");
    let mut replay = reservation;
    replay.idempotency_key = "a".repeat(64);
    replay.cycle_id = replay_cycle_id;
    replay.request_digest = "b".repeat(64);
    replay.expected_state_revision = 3;
    assert_eq!(
        lease
            .reserve_explicit_quota_override_action(&replay, authorization_reference)
            .await,
        Err(WbAutomationPostgresError::StateChanged)
    );
    let state = lease
        .load_state()
        .await
        .expect("state query succeeds")
        .expect("state exists");
    assert_eq!(state.actions_today, 13);
    assert!(state.pending_idempotency_key.is_none());
    assert_eq!(state.revision, 3);

    lease.release().await.expect("campaign lock is released");
    drop(admin);
    admin_connection
        .await
        .expect("admin connection task shuts down");
}
