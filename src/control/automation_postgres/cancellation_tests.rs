use std::{str::FromStr, time::Duration};

use tokio::{task::JoinHandle, time::timeout};
use tokio_postgres::{Client, NoTls};

use super::*;

async fn raw_client(config: &Config) -> (Client, JoinHandle<()>) {
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, driver)
}

async fn backend_pid(store: &WbAutomationPostgresStore) -> i32 {
    store
        .client
        .acquire()
        .await
        .unwrap()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0)
}

async fn advisory_lock(client: &Client, key: &str, acquire: bool) -> bool {
    let query = if acquire {
        "SELECT pg_try_advisory_lock(hashtextextended($1, 0))"
    } else {
        "SELECT pg_advisory_unlock(hashtextextended($1, 0))"
    };
    client.query_one(query, &[&key]).await.unwrap().get(0)
}

#[tokio::test]
async fn cancelled_acquisition_discards_session_after_server_grants_lock() {
    let Ok(database_url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
        return;
    };
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").unwrap();
    let config = Config::from_str(&database_url).unwrap();
    let (admin, driver) = raw_client(&Config::from_str(&admin_url).unwrap()).await;
    let store = WbAutomationPostgresStore::connect(&config).await.unwrap();
    let pid = backend_pid(&store).await;
    let schema = format!("wb_cancel_lock_{}_{}", std::process::id(), pid);
    let account_id = format!("cancel_lock_{}", std::process::id());
    let lock_key = format!("wb/{account_id}/7000001");
    let gate_key = format!("{schema}/gate");
    // A real server-side gate makes the uncertain acquisition deterministic:
    // the campaign lock is granted, but SQL cannot return until we open it.
    admin
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; \
             GRANT USAGE ON SCHEMA {schema} TO PUBLIC; \
             CREATE FUNCTION {schema}.pg_try_advisory_lock(bigint) RETURNS bool \
             LANGUAGE plpgsql AS $$ \
             DECLARE locked bool; \
             BEGIN \
                 locked := pg_catalog.pg_try_advisory_lock($1); \
                 PERFORM pg_catalog.pg_advisory_xact_lock(\
                     pg_catalog.hashtextextended('{gate_key}', 0)); \
                 RETURN locked; \
             END $$; \
             GRANT EXECUTE ON FUNCTION {schema}.pg_try_advisory_lock(bigint) TO PUBLIC;"
        ))
        .await
        .unwrap();
    store
        .client
        .acquire()
        .await
        .unwrap()
        .batch_execute(&format!("SET search_path TO {schema}, pg_catalog"))
        .await
        .unwrap();
    {
        let client = store.client.acquire().await.unwrap();
        assert!(advisory_lock(&client, &lock_key, true).await);
        assert!(advisory_lock(&client, &lock_key, false).await);
    }
    assert!(advisory_lock(&admin, &gate_key, true).await);
    let acquiring_store = store.clone();
    let acquisition = tokio::spawn(async move {
        acquiring_store
            .try_acquire_campaign(&account_id, 7_000_001)
            .await
            .unwrap()
            .unwrap()
            .release()
            .await
            .unwrap();
    });
    timeout(Duration::from_secs(3), async {
        loop {
            let row = admin
                .query_one(
                    "SELECT count(*) FILTER (WHERE granted), \
                            count(*) FILTER (WHERE NOT granted) \
                     FROM pg_locks WHERE locktype='advisory' AND pid=$1",
                    &[&pid],
                )
                .await
                .unwrap();
            if row.get::<_, i64>(0) == 1 && row.get::<_, i64>(1) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("server grants the campaign lock while its result is pending");
    assert!(!acquisition.is_finished());
    assert!(!advisory_lock(&admin, &lock_key, true).await);
    acquisition.abort();
    assert!(acquisition.await.unwrap_err().is_cancelled());
    assert!(advisory_lock(&admin, &gate_key, false).await);
    timeout(Duration::from_secs(3), async {
        while !advisory_lock(&admin, &lock_key, true).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled acquisition closes its session and releases the lock");
    assert!(advisory_lock(&admin, &lock_key, false).await);
    let replacement_pid = timeout(Duration::from_secs(3), backend_pid(&store))
        .await
        .expect("cancellation releases the session mutex");
    assert_ne!(replacement_pid, pid);
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    drop(store);
    drop(admin);
    driver.await.unwrap();
}

#[tokio::test]
async fn confirmed_lock_contention_reuses_the_same_session() {
    let Ok(database_url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
        return;
    };
    let config = Config::from_str(&database_url).unwrap();
    let (peer, driver) = raw_client(&config).await;
    let store = WbAutomationPostgresStore::connect(&config).await.unwrap();
    let pid = backend_pid(&store).await;
    let account_id = format!("contended_lock_{}", std::process::id());
    let lock_key = format!("wb/{account_id}/7000002");
    assert!(advisory_lock(&peer, &lock_key, true).await);
    assert!(
        store
            .try_acquire_campaign(&account_id, 7_000_002)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(backend_pid(&store).await, pid);
    assert!(advisory_lock(&peer, &lock_key, false).await);
    let lease = store
        .try_acquire_campaign(&account_id, 7_000_002)
        .await
        .unwrap()
        .unwrap();
    lease.release().await.unwrap();
    assert_eq!(backend_pid(&store).await, pid);
    drop(store);
    drop(peer);
    driver.await.unwrap();
}
