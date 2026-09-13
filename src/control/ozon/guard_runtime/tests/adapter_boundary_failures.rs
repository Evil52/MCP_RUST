use super::static_safety::GuardLogs;
use super::{static_adapter_fixture::*, *};
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{
        Database, TOKEN, credentials, mock_reader, mock_writer,
    },
    plan::CONTROL_DB_TEST_LOCK,
};
use tracing::instrument::WithSubscriber as _;

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_reconcile_rejects_wrong_sku_and_bid_drift_before_activation() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let fixture = StaticFixture::new();
    for bid_drift in [false, true] {
        assert_reconcile_boundary(&database, &fixture, bid_drift).await;
    }
}

async fn assert_reconcile_boundary(database: &Database, fixture: &StaticFixture, bid_drift: bool) {
    let mut state = fixture.initialize(database).await;
    let previous = state.last_static_audit_event_id.unwrap();
    let (reader, reads) = mock_reader(if bid_drift {
        vec![
            (200, product(6_000_000)),
            (200, product(7_000_000)),
            (200, product(8_000_000)),
        ]
    } else {
        vec![(
            200,
            r#"{"products":[{"sku":999,"bid":7000000}]}"#.to_owned(),
        )]
    });
    let (writer, requests) = mock_writer(if bid_drift {
        vec![(200, TOKEN.to_owned()), (200, "{}".to_owned())]
    } else {
        vec![]
    });
    let error = reconcile_static_campaigns(
        std::slice::from_ref(&fixture.guard),
        &mut state,
        &fixture.state_path,
        &reader,
        &writer,
        &fixture.store,
        fixture.write_authorization(database),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains(if bid_drift {
        "bid readback differs"
    } else {
        "campaign SKU differs"
    }));
    let requests = requests.try_iter().collect::<Vec<_>>();
    assert!(
        requests
            .iter()
            .all(|request| !request.contains("/activate"))
    );
    assert_eq!(requests.len(), if bid_drift { 2 } else { 0 });
    assert_eq!(reads.try_iter().count(), if bid_drift { 4 } else { 2 });
    assert_eq!(
        state.last_static_audit_event_id,
        Some(previous + u64::from(bid_drift))
    );
    assert!(state.pending_bid_changes.is_empty());
    assert!(state.pending_campaign_mutations.is_empty());
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_bid_incident_storage_failure_retains_durable_marker_after_one_put() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let fixture = StaticFixture::new();
    let mut state = fixture.initialize(&database).await;
    let previous = state.last_static_audit_event_id.unwrap();
    let path = fixture.state_path.clone();
    let (url, reads) = crate::test_support::mock_http_with_hook(
        vec![(200, TOKEN.to_owned()), (200, product(7_000_000))],
        move |index| {
            if index == 1 {
                assert!(
                    load_static_state(&path)
                        .unwrap()
                        .pending_bid_changes
                        .contains_key(&21)
                );
                fs::remove_file(&path).unwrap();
                fs::create_dir(&path).unwrap();
            }
        },
    );
    let reader = Arc::new(PerformanceClient::new_for_test(
        url,
        Duration::from_secs(2),
        BTreeMap::from([(fixture.store.clone(), credentials())]),
    ));
    let (writer, requests) = mock_writer(vec![(200, TOKEN.to_owned()), (400, "{}".to_owned())]);
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
    assert!(matches!(
        error.downcast_ref::<crate::control::OzonStaticGuardStateError>(),
        Some(crate::control::OzonStaticGuardStateError::UnsafeFile)
    ));
    assert!(state.pending_bid_changes.contains_key(&21));
    assert_eq!(state.last_static_audit_event_id, Some(previous + 1));
    assert_eq!(
        database
            .executor
            .latest_static_guard_audit_event_id("account")
            .await
            .unwrap(),
        state.last_static_audit_event_id
    );
    let requests = requests.try_iter().collect::<Vec<_>>();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].starts_with("PUT /api/client/campaign/21/products "));
    assert_eq!(reads.try_iter().count(), 2);
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL roles"]
async fn postgres_pending_bid_mismatch_is_locked_before_cycle_observation() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    let database = Database::connect()
        .await
        .expect("isolated PostgreSQL roles are configured");
    let fixture = StaticFixture::new();
    let mut state = fixture.initialize(&database).await;
    let previous = state.last_static_audit_event_id;
    state
        .pending_bid_changes
        .insert(21, test_pending_bid(&fixture.guard, observed_at()));
    persist_static_state(&fixture.state_path, &state).unwrap();
    let (reader, reads) = mock_reader(vec![
        (200, product(7_000_000)),
        (200, campaign("CAMPAIGN_STATE_RUNNING")),
        (200, metrics("1.00", "100.00")),
    ]);
    let (writer, requests) = mock_writer(vec![]);
    let logs = GuardLogs::default();
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
    .with_subscriber(logs.subscriber())
    .await
    .unwrap();
    assert_eq!(
        state.incidents[&21].error_class,
        "pending_bid_readback_mismatch"
    );
    assert!(state.pending_bid_changes.contains_key(&21));
    assert_eq!(state.last_static_audit_event_id, previous);
    assert_eq!(reads.try_iter().count(), 4);
    assert_eq!(requests.try_iter().count(), 0);
    assert!(logs.contains("pending dynamic Ozon bid readback mismatch; campaign locked"));
    assert!(logs.contains("static Ozon guard cycle ready"));
    assert!(!logs.contains("campaign product read unavailable"));
    assert!(!logs.contains("static product guard passed"));
    assert_eq!(load_static_state(&fixture.state_path).unwrap(), state);
}
