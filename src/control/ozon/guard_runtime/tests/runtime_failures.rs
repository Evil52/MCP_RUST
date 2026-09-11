use super::{
    static_adapter_fixture::*,
    static_driver::{acquire_executor, runtime},
    static_safety::GuardLogs,
    *,
};
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{
        Database, TOKEN, credentials, mock_reader, mock_writer,
    },
    plan::CONTROL_DB_TEST_LOCK,
};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tracing::instrument::WithSubscriber as _;

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_static_runtime_stops_after_three_real_cycle_failures() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let fixture = StaticFixture::new();
    fixture.initialize(&database).await;
    let before = fs::read(&fixture.state_path).unwrap();
    let lease = acquire_executor(&fixture).await;
    let (reader, reads) = mock_reader(vec![(200, "{}".to_owned()); 3]);
    let (writer, requests) = mock_writer(vec![]);
    let logs = GuardLogs::default();
    let mut driver = runtime(
        &fixture,
        &database,
        Command::Serve,
        &lease,
        &reader,
        &writer,
    );
    driver.poll_interval = Duration::from_millis(1);
    let error = Box::pin(tokio::time::timeout(
        Duration::from_secs(3),
        driver
            .run(std::future::pending())
            .with_subscriber(logs.subscriber()),
    ))
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exceeded its consecutive cycle failure limit")
    );
    assert!(logs.contains("consecutive_cycle_failures=3"));
    assert_eq!(reads.try_iter().count(), 4);
    assert_eq!(requests.try_iter().count(), 0);
    assert_eq!(fs::read(&fixture.state_path).unwrap(), before);
    OzonStaticGuardStateLease::acquire(&fixture.state_path).unwrap();
}

async fn read_request(stream: TcpStream) -> (TcpStream, String) {
    let mut reader = BufReader::new(stream);
    let mut request = String::new();
    let mut body_len = 0;
    loop {
        let mut line = String::new();
        assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            body_len = value.trim().parse::<usize>().unwrap();
        }
        request.push_str(&line);
    }
    let mut body = vec![0; body_len];
    reader.read_exact(&mut body).await.unwrap();
    (reader.into_inner(), request)
}

struct PendingCampaignPeer {
    client: Arc<PerformanceClient>,
    requested: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

async fn pending_campaign_peer() -> PendingCampaignPeer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (requested, received) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut stream, request) = read_request(listener.accept().await.unwrap().0).await;
        assert!(request.starts_with("POST /api/client/token "));
        stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{TOKEN}", TOKEN.len()).as_bytes()).await.unwrap();
        drop(stream);
        let (stream, request) = read_request(listener.accept().await.unwrap().0).await;
        assert!(request.starts_with("GET /api/client/campaign"));
        requested.send(()).unwrap();
        let _ = released.await;
        drop(stream);
    });
    PendingCampaignPeer {
        client: Arc::new(PerformanceClient::new_for_test(
            url,
            Duration::from_secs(10),
            BTreeMap::from([(StoreId::from("store"), credentials())]),
        )),
        requested: received,
        release,
        task,
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_static_runtime_lease_loss_cancels_an_inflight_campaign_read() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let fixture = StaticFixture::new();
    fixture.initialize(&database).await;
    let before = fs::read(&fixture.state_path).unwrap();
    let lease = acquire_executor(&fixture).await;
    let PendingCampaignPeer {
        client,
        requested,
        release,
        task,
    } = pending_campaign_peer().await;
    let (writer, requests) = mock_writer(vec![]);
    let terminate = async {
        requested.await.unwrap();
        assert!(OzonStaticGuardStateLease::acquire(&fixture.state_path).is_err());
        let identity = format!("mcp-ozon/executor-identity/v1/{}", fixture.fingerprint);
        let rows = database.admin.query(
            "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype='advisory' AND granted AND objsubid=1 AND classid::bigint=((hashtextextended($1::text,0)>>32)&4294967295) AND objid::bigint=(hashtextextended($1::text,0)&4294967295)",
            &[&identity],
        ).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].get::<_, bool>(0));
    };
    let driver = runtime(
        &fixture,
        &database,
        Command::Serve,
        &lease,
        &client,
        &writer,
    );
    let (result, ()) = Box::pin(tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(driver.run(std::future::pending()), terminate)
    }))
    .await
    .unwrap();
    release.send(()).unwrap();
    task.await.unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("lease connection was lost")
    );
    assert_eq!(requests.try_iter().count(), 0);
    assert_eq!(fs::read(&fixture.state_path).unwrap(), before);
    OzonStaticGuardStateLease::acquire(&fixture.state_path).unwrap();
}
