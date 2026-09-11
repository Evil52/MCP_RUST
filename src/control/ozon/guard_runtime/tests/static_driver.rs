use super::{static_adapter_fixture::*, static_safety::GuardLogs, *};
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{Database, mock_reader, mock_writer},
    plan::CONTROL_DB_TEST_LOCK,
};
use tracing::instrument::WithSubscriber as _;

pub(super) fn runtime<'a>(
    fixture: &'a StaticFixture,
    database: &'a Database,
    command: Command,
    executor_lease: &'a OzonExecutorLease,
    reader: &'a Arc<PerformanceClient>,
    writer: &'a Arc<OzonAdsWriteClient>,
) -> super::super::static_runtime::StaticGuardRuntime<'a> {
    super::super::static_runtime::StaticGuardRuntime {
        command,
        state_lease: OzonStaticGuardStateLease::acquire(&fixture.state_path).unwrap(),
        state_path: &fixture.state_path,
        config: load_static_guards(&fixture.config_path, "account")
            .unwrap()
            .0,
        reader,
        writer,
        write_authorization: fixture.write_authorization(database),
        executor_lease,
    }
}

pub(super) async fn acquire_executor(fixture: &StaticFixture) -> OzonExecutorLease {
    let database = std::env::var("OZON_EXECUTOR_TEST_DATABASE_URL")
        .unwrap()
        .parse()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match OzonExecutorLease::acquire(&database, &fixture.fingerprint).await {
                Ok(lease) => break lease,
                Err(crate::control::ozon::executor_lease::OzonExecutorLeaseError::Busy) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("fixture lease acquisition failed: {error}"),
            }
        }
    })
    .await
    .unwrap()
}

async fn observe_log(logs: &GuardLogs, message: &str) {
    loop {
        if logs.contains(message) {
            return;
        }
        tokio::task::yield_now().await;
    }
}

async fn assert_audit(database: &Database, fixture: &StaticFixture, mismatch: bool) {
    let mut state = fixture.initialize(database).await;
    let database_cursor = state.last_static_audit_event_id;
    if mismatch {
        state.last_static_audit_event_id = state.last_static_audit_event_id.map(|value| value + 1);
        persist_static_state(&fixture.state_path, &state).unwrap();
    }
    let before = fs::read(&fixture.state_path).unwrap();
    let executor_lease = acquire_executor(fixture).await;
    let (reader, reads) = mock_reader(vec![
        (200, campaign("CAMPAIGN_STATE_RUNNING")),
        (200, product(7_000_000)),
    ]);
    let (writer, requests) = mock_writer(vec![]);
    let logs = GuardLogs::default();
    runtime(
        fixture,
        database,
        Command::AuditStaticOnce,
        &executor_lease,
        &reader,
        &writer,
    )
    .run(std::future::pending())
    .with_subscriber(logs.subscriber())
    .await
    .unwrap();
    assert_eq!(reads.try_iter().count(), 3);
    assert_eq!(requests.try_iter().count(), 0);
    assert_eq!(fs::read(&fixture.state_path).unwrap(), before);
    assert_eq!(
        database
            .executor
            .latest_static_guard_audit_event_id("account")
            .await
            .unwrap(),
        database_cursor
    );
    assert_eq!(logs.contains("running read-only audit only"), mismatch);
    // A completed command must release its file ownership even while its
    // bootstrap executor session remains alive in the caller.
    OzonStaticGuardStateLease::acquire(&fixture.state_path).unwrap();
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_static_driver_audits_matching_and_discontinuous_state_without_writes() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    for mismatch in [false, true] {
        let fixture = StaticFixture::new();
        assert_audit(&database, &fixture, mismatch).await;
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_static_driver_reconcile_clears_incident_only_after_exact_reads() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let fixture = StaticFixture::new();
    let mut state = fixture.initialize(&database).await;
    state.incident_campaign_ids.insert(21);
    state.incidents.insert(21, test_incident(&fixture.guard));
    persist_static_state(&fixture.state_path, &state).unwrap();
    let database_cursor = state.last_static_audit_event_id;
    let executor_lease = acquire_executor(&fixture).await;
    let (reader, reads) = mock_reader(vec![
        (200, product(7_000_000)),
        (200, campaign("CAMPAIGN_STATE_RUNNING")),
    ]);
    let (writer, requests) = mock_writer(vec![]);
    runtime(
        &fixture,
        &database,
        Command::ReconcileStaticOnce,
        &executor_lease,
        &reader,
        &writer,
    )
    .run(std::future::pending())
    .await
    .unwrap();
    assert_eq!(reads.try_iter().count(), 3);
    assert_eq!(requests.try_iter().count(), 0);
    let state = load_static_state(&fixture.state_path).unwrap();
    assert!(state.incident_campaign_ids.is_empty());
    assert!(state.incidents.is_empty());
    assert_eq!(state.last_static_audit_event_id, database_cursor);
    OzonStaticGuardStateLease::acquire(&fixture.state_path).unwrap();
}

async fn assert_serve_shutdown(database: &Database, fixture: &StaticFixture, fail: bool) {
    fixture.initialize(database).await;
    let before = fs::read(&fixture.state_path).unwrap();
    let executor_lease = acquire_executor(fixture).await;
    let response = if fail {
        "{}".to_owned()
    } else {
        campaign("CAMPAIGN_STATE_STOPPED")
    };
    let (reader, reads) = mock_reader(vec![(200, response)]);
    let (writer, requests) = mock_writer(vec![]);
    let logs = GuardLogs::default();
    let completion = if fail {
        "static Ozon guard cycle failed"
    } else {
        "static Ozon guard cycle ready"
    };
    tokio::time::timeout(
        Duration::from_secs(3),
        runtime(
            fixture,
            database,
            Command::Serve,
            &executor_lease,
            &reader,
            &writer,
        )
        .run(observe_log(&logs, completion))
        .with_subscriber(logs.subscriber()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(logs.contains(completion));
    assert_eq!(reads.try_iter().count(), 2);
    assert_eq!(requests.try_iter().count(), 0);
    assert_eq!(fs::read(&fixture.state_path).unwrap(), before);
    OzonStaticGuardStateLease::acquire(&fixture.state_path).unwrap();
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_static_driver_shutdown_ends_idle_and_failed_cycles_without_mutation() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    for fail in [false, true] {
        let fixture = StaticFixture::new();
        assert_serve_shutdown(&database, &fixture, fail).await;
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_static_driver_lease_loss_stops_worker_and_releases_state_ownership() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let fixture = StaticFixture::new();
    fixture.initialize(&database).await;
    let before = fs::read(&fixture.state_path).unwrap();
    let executor_lease = acquire_executor(&fixture).await;
    let (reader, _) = mock_reader(vec![(200, campaign("CAMPAIGN_STATE_STOPPED"))]);
    let (writer, requests) = mock_writer(vec![]);
    let logs = GuardLogs::default();
    let terminate_owner = async {
        observe_log(&logs, "static Ozon campaign guard armed").await;
        assert!(OzonStaticGuardStateLease::acquire(&fixture.state_path).is_err());
        let identity = format!("mcp-ozon/executor-identity/v1/{}", fixture.fingerprint);
        let rows = database.admin.query(
            "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype='advisory' AND granted AND objsubid=1 AND classid::bigint=((hashtextextended($1::text,0)>>32)&4294967295) AND objid::bigint=(hashtextextended($1::text,0)&4294967295)",
            &[&identity],
        ).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].get::<_, bool>(0));
    };
    let worker = runtime(
        &fixture,
        &database,
        Command::Serve,
        &executor_lease,
        &reader,
        &writer,
    )
    .run(std::future::pending())
    .with_subscriber(logs.subscriber());
    let (result, ()) = Box::pin(tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(worker, terminate_owner)
    }))
    .await
    .unwrap();
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
