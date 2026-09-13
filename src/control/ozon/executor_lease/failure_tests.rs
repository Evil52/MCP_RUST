use std::time::Duration;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};

use super::*;
use crate::control::plan::CONTROL_DB_TEST_LOCK;

#[tokio::test]
async fn startup_timeout_closes_a_peer_that_never_authenticates() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let mut database = Config::new();
    database
        .host("127.0.0.1")
        .port(listener.local_addr().unwrap().port())
        .user("test-executor")
        .ssl_mode(tokio_postgres::config::SslMode::Disable);
    let acquisition =
        tokio::spawn(async move { OzonExecutorLease::acquire(&database, &"a".repeat(64)).await });
    let (mut peer, _) = listener.accept().await.unwrap();
    let size = peer.read_u32().await.unwrap();
    let mut startup = vec![0; usize::try_from(size - 4).unwrap()];
    peer.read_exact(&mut startup).await.unwrap();
    assert_eq!(
        timeout(CONNECT_TIMEOUT + Duration::from_secs(1), acquisition)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err(),
        OzonExecutorLeaseError::Unavailable
    );
    let mut buffer = [0; 1];
    assert_eq!(
        timeout(Duration::from_secs(1), peer.read(&mut buffer))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

async fn raw_client(database_url: &str) -> (Client, JoinHandle<()>) {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await.unwrap();
    let task = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, task)
}

async fn advisory(client: &Client, key: &str, acquire: bool) -> bool {
    let sql = if acquire {
        "SELECT pg_try_advisory_lock(hashtextextended($1, 0))"
    } else {
        "SELECT pg_advisory_unlock(hashtextextended($1, 0))"
    };
    client.query_one(sql, &[&key]).await.unwrap().get(0)
}

#[tokio::test]
async fn cancelled_sql_acquisition_cannot_leave_an_executor_identity_locked() {
    let (Ok(database_url), Ok(admin_url)) = (
        std::env::var("OZON_EXECUTOR_TEST_DATABASE_URL"),
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
    ) else {
        return;
    };
    let _guard = CONTROL_DB_TEST_LOCK.lock().await;
    let (admin, driver) = raw_client(&admin_url).await;
    let schema = format!("executor_cancel_{}", std::process::id());
    let gate = format!("{schema}/gate");
    let fingerprint = "e".repeat(64);
    let key = format!("{LOCK_NAMESPACE}/{fingerprint}");
    admin
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; GRANT USAGE ON SCHEMA {schema} TO PUBLIC; \
         CREATE FUNCTION {schema}.pg_try_advisory_lock(bigint) RETURNS bool \
         LANGUAGE plpgsql AS $$ DECLARE locked bool; BEGIN \
         locked := pg_catalog.pg_try_advisory_lock($1); \
         PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('{gate}', 0)); \
         RETURN locked; END $$; \
         GRANT EXECUTE ON FUNCTION {schema}.pg_try_advisory_lock(bigint) TO PUBLIC;"
        ))
        .await
        .unwrap();
    let mut database = database_url.parse::<Config>().unwrap();
    database.options(format!("-c search_path={schema},pg_catalog"));
    assert!(advisory(&admin, &gate, true).await);
    let acquisition =
        tokio::spawn(async move { OzonExecutorLease::acquire(&database, &fingerprint).await });
    timeout(Duration::from_secs(3), async {
        loop {
            let waiting: bool = admin
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING(pid) \
                 WHERE l.locktype='advisory' AND NOT l.granted \
                 AND a.application_name='mcp-ozon-control-executor-lease')",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(!advisory(&admin, &key, true).await);
    acquisition.abort();
    assert!(acquisition.await.unwrap_err().is_cancelled());
    assert!(advisory(&admin, &gate, false).await);
    timeout(Duration::from_secs(3), async {
        while !advisory(&admin, &key, true).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled lease must close its PostgreSQL session");
    assert!(advisory(&admin, &key, false).await);
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    let database = database_url.parse::<Config>().unwrap();
    assert_eq!(
        OzonExecutorLease::verify_held(&database, &"e".repeat(64)).await,
        Err(OzonExecutorLeaseError::NotHeld)
    );
    assert_eq!(
        OzonExecutorLease::verify_held(&database, "invalid").await,
        Err(OzonExecutorLeaseError::InvalidFingerprint)
    );
    let lease = OzonExecutorLease::acquire(&database, &"f".repeat(64))
        .await
        .unwrap();
    let pid: i32 = lease
        .client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    // Poll lost() while healthy, then close the backend; both waiting and
    // repeated observation after loss must complete without a busy loop.
    let (lost, terminated) = tokio::join!(timeout(Duration::from_secs(3), lease.lost()), async {
        tokio::task::yield_now().await;
        admin
            .query_one("SELECT pg_terminate_backend($1)", &[&pid])
            .await
            .unwrap()
            .get::<_, bool>(0)
    });
    lost.unwrap();
    assert!(terminated);
    timeout(Duration::from_secs(1), lease.lost()).await.unwrap();
    drop(lease);
    drop(admin);
    driver.await.unwrap();
}

#[tokio::test]
async fn authentication_failure_is_unavailable_without_a_lease() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let mut database = Config::new();
    database
        .host("127.0.0.1")
        .port(listener.local_addr().unwrap().port())
        .user("test-executor")
        .ssl_mode(tokio_postgres::config::SslMode::Disable);
    let acquisition =
        tokio::spawn(async move { OzonExecutorLease::acquire(&database, &"a".repeat(64)).await });
    let (mut peer, _) = listener.accept().await.unwrap();
    let size = peer.read_u32().await.unwrap();
    let mut startup = vec![0; usize::try_from(size - 4).unwrap()];
    peer.read_exact(&mut startup).await.unwrap();
    peer.write_all(b"invalid PostgreSQL authentication response")
        .await
        .unwrap();
    drop(peer);
    assert_eq!(
        acquisition.await.unwrap().unwrap_err(),
        OzonExecutorLeaseError::Unavailable
    );
}
