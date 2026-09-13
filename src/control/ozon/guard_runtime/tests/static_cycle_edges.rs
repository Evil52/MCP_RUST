use super::{
    static_adapter_fixture::*,
    static_safety::{GuardLogs, dynamic_control_value, publish_position},
    *,
};
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{Database, TOKEN, mock_reader, mock_writer},
    plan::CONTROL_DB_TEST_LOCK,
};
use tracing::instrument::WithSubscriber as _;

fn dynamic_fixture() -> StaticFixture {
    let mut fixture = StaticFixture::new();
    let mut config: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.config_path).unwrap()).unwrap();
    config["dynamic_bid_control"] = dynamic_control_value();
    config["dynamic_bid_control"]["position_region_name"] = fixture
        .authorization
        .path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned()
        .into();
    fs::write(&fixture.config_path, serde_json::to_vec(&config).unwrap()).unwrap();
    fixture.digest = load_static_guards(&fixture.config_path, "account")
        .unwrap()
        .1;
    fixture
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_static_cycle_keeps_incident_campaign_locked_after_complete_telemetry() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let fixture = StaticFixture::new();
    let mut state = fixture.initialize(&database).await;
    state.incident_campaign_ids.insert(21);
    state.incidents.insert(21, test_incident(&fixture.guard));
    persist_static_state(&fixture.state_path, &state).unwrap();
    let original = state.clone();
    let (reader, reads) = mock_reader(vec![
        (200, campaign("CAMPAIGN_STATE_RUNNING")),
        (200, metrics("1.00", "100.00")),
    ]);
    let (writer, requests) = mock_writer(vec![]);
    guard_once_static(
        std::slice::from_ref(&fixture.guard),
        &mut state,
        &fixture.state_path,
        &reader,
        &writer,
        &fixture.store,
        fixture.write_authorization(&database),
        None,
        None,
        observed_at(),
    )
    .await
    .unwrap();
    assert_eq!(state, original);
    assert_eq!(reads.try_iter().count(), 3);
    assert_eq!(requests.try_iter().count(), 0);
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_static_cycle_holds_upward_bid_when_position_query_permission_is_lost() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let fixture = dynamic_fixture();
    let dynamic = load_static_guards(&fixture.config_path, "account")
        .unwrap()
        .0
        .dynamic_bid_control
        .unwrap();
    let mut state = fixture.initialize(&database).await;
    let original = state.clone();
    let position = OzonBidPositionReader::connect(
        &std::env::var("POSITION_REPOSITORY_TEST_READER_URL").unwrap(),
    )
    .await
    .unwrap();
    position.verify_runtime_contract().await.unwrap();
    database
        .admin
        .batch_execute(
            "REVOKE SELECT ON search_position.published_measurements FROM position_reader",
        )
        .await
        .unwrap();
    let (reader, reads) = mock_reader(vec![
        (200, campaign("CAMPAIGN_STATE_RUNNING")),
        (200, metrics("1.00", "100.00")),
        (200, product(7_000_000)),
    ]);
    let (writer, requests) = mock_writer(vec![]);
    let logs = GuardLogs::default();
    let result = guard_once_static(
        std::slice::from_ref(&fixture.guard),
        &mut state,
        &fixture.state_path,
        &reader,
        &writer,
        &fixture.store,
        fixture.write_authorization(&database),
        Some(&dynamic),
        Some(&position),
        observed_at(),
    )
    .with_subscriber(logs.subscriber())
    .await;
    database
        .admin
        .batch_execute("GRANT SELECT ON search_position.published_measurements TO position_reader")
        .await
        .unwrap();
    result.unwrap();
    assert!(logs.contains("position unavailable; upward bid changes are held"));
    assert!(logs.contains("dynamic Ozon bid hold"));
    assert!(logs.contains("position_unavailable"));
    assert_eq!(state, original);
    assert_eq!(reads.try_iter().count(), 4);
    assert_eq!(requests.try_iter().count(), 0);
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_static_cycle_rolls_back_failed_bid_marker_before_any_put() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let fixture = dynamic_fixture();
    let dynamic = load_static_guards(&fixture.config_path, "account")
        .unwrap()
        .0
        .dynamic_bid_control
        .unwrap();
    let mut state = fixture.initialize(&database).await;
    let original = state.clone();
    publish_position(
        &database,
        &dynamic.position_region_name,
        observed_at() - chrono::Duration::hours(1),
    )
    .await;
    let position = OzonBidPositionReader::connect(
        &std::env::var("POSITION_REPOSITORY_TEST_READER_URL").unwrap(),
    )
    .await
    .unwrap();
    position.verify_runtime_contract().await.unwrap();
    fs::remove_file(&fixture.state_path).unwrap();
    fs::create_dir(&fixture.state_path).unwrap();
    let (reader, reads) = mock_reader(vec![
        (200, campaign("CAMPAIGN_STATE_RUNNING")),
        (200, metrics("1.00", "100.00")),
        (200, product(7_000_000)),
    ]);
    let (writer, requests) = mock_writer(vec![(200, TOKEN.to_owned())]);
    let logs = GuardLogs::default();
    guard_once_static(
        std::slice::from_ref(&fixture.guard),
        &mut state,
        &fixture.state_path,
        &reader,
        &writer,
        &fixture.store,
        fixture.write_authorization(&database),
        Some(&dynamic),
        Some(&position),
        observed_at(),
    )
    .with_subscriber(logs.subscriber())
    .await
    .unwrap();
    assert!(logs.contains("dynamic Ozon bid change failed"));
    assert_eq!(state, original);
    assert_eq!(
        database
            .executor
            .latest_static_guard_audit_event_id("account")
            .await
            .unwrap(),
        state.last_static_audit_event_id
    );
    assert_eq!(reads.try_iter().count(), 4);
    assert_eq!(requests.try_iter().count(), 1);
    state
        .pending_bid_changes
        .insert(21, test_pending_bid(&fixture.guard, observed_at()));
    let error = change_static_campaign_bid(
        &mut state,
        &fixture.state_path,
        &reader,
        &writer,
        &fixture.store,
        fixture.write_authorization(&database),
        &fixture.guard,
        7_000_000,
        8_000_000,
        observed_at(),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("already has a pending bid mutation")
    );
    assert_eq!(requests.try_iter().count(), 0);
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_static_cycle_rejects_clock_rollback_before_any_bid_write() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let fixture = dynamic_fixture();
    let dynamic = load_static_guards(&fixture.config_path, "account")
        .unwrap()
        .0
        .dynamic_bid_control
        .unwrap();
    let cycle_time = observed_at() + chrono::Duration::minutes(30);
    let mut persisted = fixture.initialize(&database).await;
    persisted
        .last_bid_change_at
        .insert(21, cycle_time + chrono::Duration::seconds(1));
    persist_static_state(&fixture.state_path, &persisted).unwrap();
    let original_bytes = fs::read(&fixture.state_path).unwrap();
    let mut state = load_static_state(&fixture.state_path).unwrap();
    // Use a distinct publication slot from other cycle fixtures while keeping
    // this position five minutes old at the simulated rolled-back wall clock.
    publish_position(&database, &dynamic.position_region_name, observed_at()).await;
    let position = OzonBidPositionReader::connect(
        &std::env::var("POSITION_REPOSITORY_TEST_READER_URL").unwrap(),
    )
    .await
    .unwrap();
    position.verify_runtime_contract().await.unwrap();
    let (reader, reads) = mock_reader(vec![
        (200, campaign("CAMPAIGN_STATE_RUNNING")),
        (200, metrics("1.00", "100.00")),
        (200, product(7_000_000)),
    ]);
    let (writer, requests) = mock_writer(vec![]);
    let error = guard_once_static(
        std::slice::from_ref(&fixture.guard),
        &mut state,
        &fixture.state_path,
        &reader,
        &writer,
        &fixture.store,
        fixture.write_authorization(&database),
        Some(&dynamic),
        Some(&position),
        cycle_time,
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.downcast_ref::<crate::control::ozon::pacing::OzonBidPacingError>(),
        Some(&crate::control::ozon::pacing::OzonBidPacingError::InvalidObservation)
    );
    assert_eq!(reads.try_iter().count(), 4);
    assert_eq!(requests.try_iter().count(), 0);
    assert_eq!(state, persisted);
    assert_eq!(fs::read(&fixture.state_path).unwrap(), original_bytes);
    assert_eq!(
        database
            .executor
            .latest_static_guard_audit_event_id("account")
            .await
            .unwrap(),
        state.last_static_audit_event_id
    );
}
