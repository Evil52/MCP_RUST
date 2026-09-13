use std::{str::FromStr, time::Duration};

use chrono::{DateTime, Utc};
use mcp_ozon::{
    control::WbAutomationPostgresStore,
    marketplace_quota::{QuotaError, QuotaKey, SharedQuota},
};
use tokio_postgres::{Client, Config, NoTls, error::SqlState};

async fn connect(variable: &str) -> Client {
    let url = std::env::var(variable).unwrap_or_else(|_| {
        panic!("{variable} is required; run through scripts/with-position-test-db.sh")
    });
    let (client, connection) = Config::from_str(&url)
        .unwrap()
        .connect(NoTls)
        .await
        .unwrap();
    std::mem::drop(tokio::spawn(async move {
        connection.await.unwrap();
    }));
    client
}

async fn acquire(client: &Client, key: &str, interval_millis: i64) -> i64 {
    client
        .query_one(
            "SELECT marketplace_quota.try_acquire($1, $2)",
            &[&key, &interval_millis],
        )
        .await
        .unwrap()
        .get(0)
}

async fn cooldown(client: &Client, key: &str, delay_millis: i64) {
    client
        .query_one(
            "SELECT marketplace_quota.extend_cooldown($1, $2)",
            &[&key, &delay_millis],
        )
        .await
        .unwrap();
}

async fn deadline(admin: &Client, key: &str) -> DateTime<Utc> {
    admin
        .query_one(
            "SELECT next_allowed_at FROM marketplace_quota.departures WHERE key = $1",
            &[&key],
        )
        .await
        .unwrap()
        .get(0)
}

async fn atomic_admission_and_durable_cooldown(admin: &Client, first: Client, second: &Client) {
    let key = "a".repeat(64);
    let (left, right) = tokio::join!(acquire(&first, &key, 60_000), acquire(second, &key, 60_000));
    assert_eq!(usize::from(left == 0) + usize::from(right == 0), 1);
    assert!(left.max(right) > 0 && left.max(right) <= 60_000);
    let reserved = deadline(admin, &key).await;
    assert!(acquire(second, &key, 86_400_000).await > 0);
    assert_eq!(deadline(admin, &key).await, reserved);

    // A shorter Retry-After cannot reduce an already reserved deadline.
    cooldown(&first, &key, 1).await;
    assert_eq!(deadline(admin, &key).await, reserved);
    cooldown(second, &key, 120_000).await;
    let extended = deadline(admin, &key).await;
    assert!(extended > reserved);

    // A new process/session must see the committed cooldown after the original
    // requester disappears; admission never relies on in-memory state.
    drop(first);
    let restarted = connect("REPORT_SNAPSHOT_TEST_COLLECTOR_URL").await;
    assert!(acquire(&restarted, &key, 1).await > 0);
    assert_eq!(deadline(admin, &key).await, extended);
    assert_eq!(acquire(second, &"b".repeat(64), 1).await, 0);
    assert_eq!(acquire(second, &"c".repeat(64), 86_400_000).await, 0);
    cooldown(second, &"d".repeat(64), 86_400_000).await;
    assert!(acquire(&restarted, &"d".repeat(64), 1).await > 0);
}

async fn uses_clock_after_lock(admin: &mut Client, collector: &Client) {
    let key = "e".repeat(64);
    cooldown(collector, &key, 60_000).await;
    let collector_pid: i32 = collector
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let observer = connect("POSITION_REPOSITORY_TEST_ADMIN_URL").await;
    let transaction = admin.transaction().await.unwrap();
    transaction
        .query_one(
            "SELECT key FROM marketplace_quota.departures WHERE key = $1 FOR UPDATE",
            &[&key],
        )
        .await
        .unwrap();

    let attempt = acquire(collector, &key, 60_000);
    let release = async {
        // Observe actual lock contention, then set the deadline in the past
        // relative to release. A timestamp sampled before waiting denies this.
        let mut waiting = false;
        for _ in 0..100 {
            waiting = observer
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity \
                     WHERE pid = $1 AND wait_event_type = 'Lock')",
                    &[&collector_pid],
                )
                .await
                .unwrap()
                .get(0);
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            waiting,
            "quota attempt must wait for the independent row lock"
        );
        transaction
            .execute(
                "UPDATE marketplace_quota.departures \
                 SET next_allowed_at = clock_timestamp() - interval '1 microsecond' \
                 WHERE key = $1",
                &[&key],
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    };
    let (wait_millis, ()) = tokio::join!(attempt, release);
    assert_eq!(wait_millis, 0);
}

async fn rejects_invalid_inputs(admin: &Client, collector: &Client) {
    let key = "f".repeat(64);
    for routine in ["try_acquire", "extend_cooldown"] {
        let sql = format!("SELECT marketplace_quota.{routine}($1, $2)");
        let mut invalid_delays = vec![None, Some(-1_i64), Some(0)];
        if routine == "try_acquire" {
            invalid_delays.extend([Some(86_400_001), Some(i64::MAX)]);
        }
        for invalid in invalid_delays {
            let error = collector
                .query_one(&sql, &[&key, &invalid])
                .await
                .unwrap_err();
            assert_eq!(error.code(), Some(&SqlState::INVALID_PARAMETER_VALUE));
        }
        for invalid in [
            None,
            Some(String::new()),
            Some("A".repeat(64)),
            Some("0".repeat(63)),
            Some("0".repeat(65)),
            Some("g".repeat(64)),
        ] {
            let error = collector
                .query_one(&sql, &[&invalid, &1_i64])
                .await
                .unwrap_err();
            assert_eq!(error.code(), Some(&SqlState::INVALID_PARAMETER_VALUE));
        }
    }
    let created: bool = admin
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM marketplace_quota.departures WHERE key = $1)",
            &[&key],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!created, "invalid requests must not create quota state");
}

async fn long_cooldowns_survive_and_outliers_fail_closed(admin: &Client, collector: &Client) {
    let two_days = "0".repeat(64);
    cooldown(collector, &two_days, 172_800_000).await;
    let wait = acquire(collector, &two_days, 1).await;
    assert!((172_790_000..=172_800_000).contains(&wait));
    let two_day_deadline = deadline(admin, &two_days).await;
    cooldown(collector, &two_days, 1).await;
    assert_eq!(deadline(admin, &two_days).await, two_day_deadline);

    let horizon = "1".repeat(64);
    cooldown(collector, &horizon, 31_622_400_000).await;
    assert!(acquire(collector, &horizon, 1).await > 31_622_390_000);

    for (key, delay) in [("2".repeat(64), 31_622_400_001), ("3".repeat(64), i64::MAX)] {
        cooldown(collector, &key, delay).await;
        let infinite: bool = admin
            .query_one(
                "SELECT next_allowed_at = 'infinity'::timestamptz \
                 FROM marketplace_quota.departures WHERE key = $1",
                &[&key],
            )
            .await
            .unwrap()
            .get(0);
        assert!(
            infinite,
            "outlier cooldown must block until administrator reconciliation"
        );
        let restarted = connect("REPORT_SNAPSHOT_TEST_COLLECTOR_URL").await;
        assert_eq!(acquire(&restarted, &key, 1).await, 86_400_000);
        cooldown(&restarted, &key, 1).await;
        assert_eq!(acquire(collector, &key, 1).await, 86_400_000);
    }
}

async fn enforces_restricted_roles(admin: &Client, collector: &Client) {
    for sql in [
        "SELECT * FROM marketplace_quota.departures",
        "DELETE FROM marketplace_quota.departures WHERE false",
        "UPDATE marketplace_quota.departures SET next_allowed_at = '-infinity' WHERE false",
    ] {
        let error = collector.batch_execute(sql).await.unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));
    }
    for variable in [
        "POSITION_REPOSITORY_TEST_READER_URL",
        "REPORT_OUTBOX_TEST_WORKER_URL",
    ] {
        let reader = connect(variable).await;
        // Prove ACL denial independently of the reader's read-only default.
        reader
            .batch_execute("SET default_transaction_read_only = off")
            .await
            .unwrap();
        for routine in ["try_acquire", "extend_cooldown"] {
            let error = reader
                .query_one(
                    &format!("SELECT marketplace_quota.{routine}($1, $2)"),
                    &[&"0".repeat(64), &1_i64],
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));
        }
    }
    let roles: bool = admin
        .query_one(
            "SELECT bool_and( \
                 has_schema_privilege(role_name, 'marketplace_quota', 'USAGE') \
                 AND NOT has_schema_privilege(role_name, 'marketplace_quota', 'CREATE') \
                 AND NOT has_table_privilege(role_name, 'marketplace_quota.departures', \
                     'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') \
                 AND has_function_privilege(role_name, \
                     'marketplace_quota.try_acquire(text,bigint)', 'EXECUTE') \
                 AND has_function_privilege(role_name, \
                     'marketplace_quota.extend_cooldown(text,bigint)', 'EXECUTE')) \
             FROM unnest(ARRAY['position_collector', 'report_collector', \
                 'report_refresh_requester', 'control_writer', 'ozon_control_planner', \
                 'ozon_control_executor', 'wb_automation_writer']::name[]) AS role_name",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(roles);
}

fn shared_quota(variable: &str) -> SharedQuota {
    SharedQuota::from_database_url(&std::env::var(variable).expect("quota test URL is required"))
}

fn retry_after(result: Result<(), QuotaError>) -> Duration {
    match result {
        Err(QuotaError::Limited { retry_after }) => retry_after,
        other => panic!("expected shared quota denial, got {other:?}"),
    }
}

async fn rust_gates_share_state_and_fail_closed() {
    let first = shared_quota("REPORT_SNAPSHOT_TEST_COLLECTOR_URL");
    let second = shared_quota("REPORT_REFRESH_TEST_REQUESTER_URL");
    first.preflight().await.unwrap();
    second.preflight().await.unwrap();
    let key = QuotaKey::ozon_seller("quota_test_seller", "sales").unwrap();
    let interval = Duration::from_secs(60);
    let (left, right) = tokio::join!(first.admit(&key, interval), second.admit(&key, interval));
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let mut remaining = retry_after(second.admit(&key, interval).await);
    for _ in 0..3 {
        let next = retry_after(first.admit(&key, interval).await);
        assert!(
            next <= remaining,
            "denial must not extend the shared deadline"
        );
        remaining = next;
    }

    let different_group = QuotaKey::ozon_seller("quota_test_seller", "stocks").unwrap();
    let different_account = QuotaKey::ozon_seller("another_test_seller", "sales").unwrap();
    first.admit(&different_group, interval).await.unwrap();
    second.admit(&different_account, interval).await.unwrap();
    first.defer(&key, Duration::from_secs(120)).await.unwrap();
    let long_delay = QuotaKey::ozon_seller("quota_test_seller", "long_delay").unwrap();
    first
        .defer(&long_delay, Duration::from_hours(48))
        .await
        .unwrap();
    let outlier = QuotaKey::ozon_seller("quota_test_seller", "outlier").unwrap();
    first
        .defer(&outlier, Duration::from_secs(31_622_401))
        .await
        .unwrap();
    drop(first);
    let restarted = shared_quota("REPORT_SNAPSHOT_TEST_COLLECTOR_URL");
    assert!(retry_after(restarted.admit(&key, interval).await) > interval);
    assert!(retry_after(restarted.admit(&long_delay, interval).await) > Duration::from_hours(24));
    assert_eq!(
        retry_after(restarted.admit(&outlier, interval).await),
        Duration::from_hours(24)
    );
    restarted
        .defer(&outlier, Duration::from_millis(1))
        .await
        .unwrap();
    assert_eq!(
        retry_after(second.admit(&outlier, interval).await),
        Duration::from_hours(24)
    );

    // The URL has a permitted role, but its local endpoint drops the database
    // handshake. Failure must never turn into an uncoordinated allowance.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unavailable = SharedQuota::from_database_url(&format!(
        "postgresql://report_collector:test@{}/unavailable?connect_timeout=1",
        listener.local_addr().unwrap()
    ));
    let disconnected = tokio::spawn(async move {
        let (connection, _) = listener.accept().await.unwrap();
        drop(connection);
    });
    assert_eq!(unavailable.preflight().await, Err(QuotaError::Unavailable));
    disconnected.await.unwrap();
    assert_eq!(
        unavailable.admit(&key, interval).await,
        Err(QuotaError::Unavailable)
    );
    assert_eq!(
        unavailable.defer(&key, interval).await,
        Err(QuotaError::Unavailable)
    );
}

async fn two_automation_runtimes_fit_the_bounded_connection_limit() {
    let url = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL")
        .expect("quota test requires the WB automation URL");
    let config = Config::from_str(&url).unwrap();
    let first_store = WbAutomationPostgresStore::connect(&config).await.unwrap();
    let second_store = WbAutomationPostgresStore::connect(&config).await.unwrap();
    first_store.verify_runtime_contract().await.unwrap();
    second_store.verify_runtime_contract().await.unwrap();
    let first_quota = SharedQuota::from_database_url(&url);
    let second_quota = SharedQuota::from_database_url(&url);
    first_quota.preflight().await.unwrap();
    second_quota.preflight().await.unwrap();
    let Err(extra_connection) = config.connect(NoTls).await else {
        panic!("WB automation must reject a fifth database connection");
    };
    assert_eq!(
        extra_connection.code(),
        Some(&SqlState::TOO_MANY_CONNECTIONS)
    );
    drop((first_store, second_store, first_quota, second_quota));
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run scripts/with-position-test-db.sh cargo test --test marketplace_quota_postgres -- --ignored"]
async fn shared_quota_is_atomic_durable_bounded_and_private() {
    let mut admin = connect("POSITION_REPOSITORY_TEST_ADMIN_URL").await;
    let first = connect("REPORT_SNAPSHOT_TEST_COLLECTOR_URL").await;
    let second = connect("REPORT_SNAPSHOT_TEST_COLLECTOR_URL").await;
    atomic_admission_and_durable_cooldown(&admin, first, &second).await;
    uses_clock_after_lock(&mut admin, &second).await;
    rejects_invalid_inputs(&admin, &second).await;
    long_cooldowns_survive_and_outliers_fail_closed(&admin, &second).await;
    enforces_restricted_roles(&admin, &second).await;
    drop(second);
    rust_gates_share_state_and_fail_closed().await;
    two_automation_runtimes_fit_the_bounded_connection_limit().await;
}
