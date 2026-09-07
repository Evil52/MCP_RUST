use std::{str::FromStr, time::Duration};

use mcp_ozon::{
    position_collector::{PostgresRepository, RepositoryError},
    postgres::{PostgresUnavailable, SupervisedClient},
    reporting::{
        postgres_collector::PostgresSnapshotWriter,
        postgres_outbox::{PostgresOutboxError, PostgresOutboxRepository},
        postgres_snapshot::PostgresSnapshotRepository,
    },
};
use tokio::time::sleep;
use tokio_postgres::{Client, Config, NoTls};

async fn connect(url: &str) -> Client {
    let (client, connection) = Config::from_str(url).unwrap().connect(NoTls).await.unwrap();
    std::mem::drop(tokio::spawn(async move {
        let _ = connection.await;
    }));
    client
}

async fn bounded_admin(url: &str) -> Client {
    let client = connect(url).await;
    client
        .batch_execute(
            "SET statement_timeout = '60s'; \
             SET idle_in_transaction_session_timeout = '30s'",
        )
        .await
        .unwrap();
    client
}

#[tokio::test]
async fn repositories_verify_injected_sessions_and_reject_a_bounded_wrong_role() {
    let (Ok(admin_url), Ok(collector_url), Ok(worker_url), Ok(report_collector_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("POSITION_REPOSITORY_TEST_COLLECTOR_URL"),
        std::env::var("REPORT_OUTBOX_TEST_WORKER_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };

    PostgresSnapshotWriter::from_client(connect(&report_collector_url).await)
        .verify_runtime_contract()
        .await
        .unwrap();
    PostgresOutboxRepository::from_client(connect(&worker_url).await)
        .verify_runtime_contract()
        .await
        .unwrap();
    PostgresSnapshotRepository::from_client(connect(&worker_url).await)
        .verify_runtime_contract()
        .await
        .unwrap();

    assert_eq!(
        PostgresRepository::from_client(bounded_admin(&admin_url).await)
            .verify_runtime_contract()
            .await,
        Err(RepositoryError::Unavailable)
    );
    assert_eq!(
        PostgresOutboxRepository::from_client(bounded_admin(&admin_url).await)
            .verify_runtime_contract()
            .await,
        Err(PostgresOutboxError::Unavailable)
    );

    // Keep the collector URL exercised as an independently bounded role; the
    // test must not accidentally pass only because the report roles work.
    PostgresRepository::from_client(connect(&collector_url).await)
        .verify_runtime_contract()
        .await
        .unwrap();
}

#[tokio::test]
async fn supervised_session_probe_recovers_after_the_backend_is_terminated() {
    let (Ok(admin_url), Ok(worker_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_OUTBOX_TEST_WORKER_URL"),
    ) else {
        return;
    };
    let config = Config::from_str(&worker_url).unwrap();
    let supervised = SupervisedClient::connect(&config, "postgres-hardening-integration")
        .await
        .unwrap();
    supervised.verify_session_bounds().await.unwrap();
    supervised.probe().await.unwrap();
    let backend_pid: i32 = supervised
        .acquire()
        .await
        .unwrap()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);

    let admin = connect(&admin_url).await;
    assert!(
        admin
            .query_one("SELECT pg_terminate_backend($1)", &[&backend_pid])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );

    // Driver shutdown and `Client::is_closed` are asynchronous. The first
    // failed probe is acceptable; the supervised client must reconnect within
    // a short bounded window and then remain usable.
    let mut recovered = false;
    for _ in 0..50 {
        if supervised.probe().await.is_ok() {
            recovered = true;
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        recovered,
        "the terminated PostgreSQL session must reconnect"
    );
    supervised.probe().await.unwrap();

    // The successful reconnect reserved the same cooldown used after a failed
    // attempt. If that replacement dies immediately, acquisition must not
    // start a third connection inside the five-second pacing window.
    let replacement_pid: i32 = supervised
        .acquire()
        .await
        .unwrap()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        admin
            .query_one("SELECT pg_terminate_backend($1)", &[&replacement_pid])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let mut cooldown_observed = false;
    for _ in 0..50 {
        if supervised.acquire().await.is_err() {
            cooldown_observed = true;
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        cooldown_observed,
        "a second terminated session must be paced by the reconnect cooldown"
    );

    // An acquired session can still fail its bounds query. Keep a transaction
    // deliberately aborted so the fixed pg_settings query is rejected after
    // acquisition, and prove that startup verification fails closed there too.
    let aborted_client = connect(&worker_url).await;
    aborted_client.batch_execute("BEGIN").await.unwrap();
    assert!(
        aborted_client
            .batch_execute("SELECT column_that_does_not_exist")
            .await
            .is_err()
    );
    let aborted =
        SupervisedClient::preconnected(aborted_client, "postgres-aborted-hardening-integration");
    assert_eq!(
        aborted.verify_session_bounds().await,
        Err(mcp_ozon::postgres::PostgresUnavailable)
    );

    // A caller-supplied session has deliberately no reconnect configuration.
    // Once it closes, even the health probe must fail closed at acquisition.
    let preconnected_client = connect(&worker_url).await;
    let preconnected_pid: i32 = preconnected_client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let preconnected = SupervisedClient::preconnected(
        preconnected_client,
        "postgres-preconnected-hardening-integration",
    );
    assert!(
        admin
            .query_one("SELECT pg_terminate_backend($1)", &[&preconnected_pid])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let mut closed = false;
    for _ in 0..50 {
        if preconnected.acquire().await.is_err() {
            closed = true;
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(closed, "the caller-supplied PostgreSQL session must close");
    assert_eq!(
        preconnected.probe().await,
        Err(mcp_ozon::postgres::PostgresUnavailable)
    );
    assert_eq!(
        preconnected.verify_session_bounds().await,
        Err(mcp_ozon::postgres::PostgresUnavailable)
    );
}

#[tokio::test]
async fn reconnection_refuses_changed_role_bounds_and_recovers_after_they_are_restored() {
    let (Ok(admin_url), Ok(worker_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_OUTBOX_TEST_WORKER_URL"),
    ) else {
        return;
    };
    // A separate role prevents these ALTER ROLE statements from weakening a
    // session opened by any other database-backed test running in parallel.
    let admin = bounded_admin(&admin_url).await;
    admin
        .batch_execute(
            "CREATE ROLE postgres_bounds_regression LOGIN PASSWORD 'bounds-test-only'; \
             DO $$ BEGIN EXECUTE format('GRANT CONNECT ON DATABASE %I TO postgres_bounds_regression', \
                 current_database()); END $$; \
             ALTER ROLE postgres_bounds_regression SET statement_timeout = '60s'; \
             ALTER ROLE postgres_bounds_regression SET idle_in_transaction_session_timeout = '30s'",
        )
        .await
        .unwrap();
    let mut config = Config::from_str(&worker_url).unwrap();
    config.user("postgres_bounds_regression");
    config.password("bounds-test-only");
    mcp_ozon::postgres::harden(&mut config, "postgres-bounds-regression");

    for disable_bound in [
        "ALTER ROLE postgres_bounds_regression SET statement_timeout = '0'",
        "ALTER ROLE postgres_bounds_regression SET idle_in_transaction_session_timeout = '0'",
    ] {
        let supervised = SupervisedClient::connect(&config, "postgres-bounds-regression")
            .await
            .unwrap();
        let initial = supervised.acquire().await.unwrap();
        let initial_pid: i32 = initial
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        admin.batch_execute(disable_bound).await.unwrap();
        assert!(
            admin
                .query_one("SELECT pg_terminate_backend($1)", &[&initial_pid])
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while !initial.is_closed() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the terminated backend must close before testing reconnection");
        drop(initial);

        // This is the first acquisition after closure, so it must validate a
        // real replacement connection rather than merely hit the cooldown.
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(6), supervised.acquire())
                .await
                .expect("reconnect must not recursively acquire the slot mutex")
                .err(),
            Some(PostgresUnavailable)
        );
        assert_eq!(supervised.probe().await, Err(PostgresUnavailable));
        assert_eq!(
            SupervisedClient::connect(&config, "postgres-unbounded-initial-session")
                .await
                .err(),
            Some(PostgresUnavailable)
        );

        admin
            .batch_execute(
                "ALTER ROLE postgres_bounds_regression SET statement_timeout = '60s'; \
                 ALTER ROLE postgres_bounds_regression SET idle_in_transaction_session_timeout = '30s'",
            )
            .await
            .unwrap();
        sleep(Duration::from_secs(5)).await;
        let recovered = tokio::time::timeout(Duration::from_secs(6), supervised.acquire())
            .await
            .expect("a corrected role must reconnect without deadlocking")
            .unwrap();
        let recovered_pid: i32 = recovered
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        assert_ne!(initial_pid, recovered_pid);
        drop(recovered);
        supervised.verify_session_bounds().await.unwrap();
        supervised.probe().await.unwrap();
    }
    admin
        .batch_execute(
            "DROP OWNED BY postgres_bounds_regression; DROP ROLE postgres_bounds_regression",
        )
        .await
        .unwrap();
}
