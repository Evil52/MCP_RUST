use super::{static_adapter_fixture::*, *};
use crate::control::{
    ozon::launch_workflow::tests::adapter_fixture::{Database, TOKEN, mock_reader, mock_writer},
    plan::CONTROL_DB_TEST_LOCK,
};

use tracing::instrument::WithSubscriber as _;

#[derive(Clone, Default)]
pub(super) struct GuardLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for GuardLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl GuardLogs {
    pub(super) fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync + 'static {
        let output = self.clone();
        tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(move || output.clone())
            .finish()
    }
    pub(super) fn contains(&self, message: &str) -> bool {
        String::from_utf8_lossy(&self.0.lock().unwrap()).contains(message)
    }
}

fn dynamic_control_value() -> serde_json::Value {
    serde_json::json!({
        "position_store_id":"account", "position_region_name":"runtime fixture region",
        "bid_step_microrubles":1_000_000, "target_position":10,
        "cooldown_seconds":1800,"max_position_age_seconds":3600
    })
}

fn dynamic_control() -> OzonStaticDynamicBidControl {
    serde_json::from_value(dynamic_control_value()).unwrap()
}

#[derive(Clone, Copy)]
enum StopCase {
    Telemetry,
    Spend,
    Drr,
    Product,
}

impl StopCase {
    const fn reason(self) -> &'static str {
        match self {
            Self::Telemetry => "telemetry_unavailable",
            Self::Spend => "spend_cap_reached",
            Self::Drr => "drr_cap_exceeded",
            Self::Product => "product_guard_failed",
        }
    }

    fn metrics(self) -> (u16, String) {
        match self {
            Self::Telemetry => (400, "{}".to_owned()),
            Self::Spend => (200, metrics("2000.00", "10000.00")),
            Self::Drr => (200, metrics("20.00", "100.00")),
            Self::Product => (200, metrics("1.00", "100.00")),
        }
    }
}

async fn assert_stop_case(
    database: &Database,
    fixture: &StaticFixture,
    case: StopCase,
    revoked: bool,
) {
    let mut state = fixture.initialize(database).await;
    let old_cursor = state.last_static_audit_event_id;
    if revoked {
        database
            .admin
            .execute(
                "UPDATE control.ozon_runtime_gates SET enabled=false WHERE gate_key='global'",
                &[],
            )
            .await
            .unwrap();
    }
    let mut responses = vec![(200, campaign("CAMPAIGN_STATE_RUNNING")), case.metrics()];
    if matches!(case, StopCase::Product) {
        responses.push((
            200,
            r#"{"products":[{"sku":999,"bid":7000000}]}"#.to_owned(),
        ));
    }
    if !revoked {
        responses.push((200, campaign("CAMPAIGN_STATE_STOPPED")));
    }
    let expected_reads = responses.len() + 1;
    let (reader, reads) = mock_reader(responses);
    let (writer, requests) = mock_writer(if revoked {
        vec![(200, TOKEN.to_owned())]
    } else {
        vec![(200, TOKEN.to_owned()), (200, "{}".to_owned())]
    });
    let logs = GuardLogs::default();
    let result = guard_once_static(
        std::slice::from_ref(&fixture.guard),
        &mut state,
        &fixture.state_path,
        &reader,
        &writer,
        &fixture.store,
        fixture.write_authorization(database),
        Some(&dynamic_control()),
        None,
        observed_at(),
    )
    .with_subscriber(logs.subscriber())
    .await;
    assert_eq!(result.is_err(), matches!(case, StopCase::Telemetry));
    assert_eq!(reads.try_iter().count(), expected_reads);
    let requests = requests.try_iter().collect::<Vec<_>>();
    assert_eq!(requests.len(), if revoked { 1 } else { 2 });
    assert!(requests.iter().all(|request| !request.starts_with("PUT ")));
    if revoked {
        assert_eq!(state.last_static_audit_event_id, old_cursor);
    } else {
        assert!(requests[1].starts_with("POST /api/client/campaign/21/deactivate "));
        assert!(state.last_static_audit_event_id > old_cursor);
        assert!(logs.contains(case.reason()));
    }
    assert!(state.pending_bid_changes.is_empty());
    assert!(state.pending_campaign_mutations.is_empty());
    assert!(state.incident_campaign_ids.is_empty());
}

#[tokio::test]
async fn postgres_static_financial_stops_precede_optional_reads_and_bid_changes() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let fixture = StaticFixture::new();
        for (case, revoked) in [
            (StopCase::Telemetry, false),
            (StopCase::Spend, false),
            (StopCase::Drr, false),
            (StopCase::Product, false),
            (StopCase::Spend, true),
            (StopCase::Drr, true),
        ] {
            assert_stop_case(&database, &fixture, case, revoked).await;
        }
    }
}

async fn publish_position(database: &Database) {
    let slot: DateTime<Utc> = "2026-09-01T11:30:00Z".parse().unwrap();
    let measured = observed_at() - chrono::Duration::minutes(5);
    let monitor:i64=database.admin.query_one(
        "INSERT INTO search_position.monitors(store_id,product_id,search_phrase,region_code,region_name,interval_minutes,max_position,active) VALUES('account','1001','runtime fixture query','fixture','runtime fixture region',30,100,true) RETURNING id", &[],
    ).await.unwrap().get(0);
    let run:i64=database.admin.query_one(
        "INSERT INTO search_position.collection_runs(source,scheduled_for,started_at,status,monitors_planned,queries_planned,collector_version,payload_digest) VALUES('ozon_public_search',$1,$2,'running',1,1,'runtime-adapter-test',repeat('f',64)) RETURNING id", &[&slot,&measured],
    ).await.unwrap().get(0);
    database.admin.execute("INSERT INTO search_position.measurements(run_id,monitor_id,observed_at,outcome,overall_position,placement) VALUES($1,$2,$3,'found',44,'unknown')", &[&run,&monitor,&measured]).await.unwrap();
    database.admin.execute("UPDATE search_position.collection_runs SET finished_at=$2,status='succeeded',monitors_attempted=1,monitors_succeeded=1,queries_attempted=1,queries_succeeded=1 WHERE id=$1", &[&run,&(measured+chrono::Duration::seconds(1))]).await.unwrap();
}

#[tokio::test]
async fn postgres_static_dynamic_bid_uses_published_position_and_exact_put_readback() {
    let _lock = CONTROL_DB_TEST_LOCK.lock().await;
    if let Some(database) = Database::connect().await {
        let mut fixture = StaticFixture::new();
        let dynamic = dynamic_control();
        let mut config: serde_json::Value =
            serde_json::from_slice(&fs::read(&fixture.config_path).unwrap()).unwrap();
        config["dynamic_bid_control"] = dynamic_control_value();
        fs::write(&fixture.config_path, serde_json::to_vec(&config).unwrap()).unwrap();
        fixture.digest = load_static_guards(&fixture.config_path, "account")
            .unwrap()
            .1;
        let mut state = fixture.initialize(&database).await;
        publish_position(&database).await;
        let position_reader = OzonBidPositionReader::connect(
            &std::env::var("POSITION_REPOSITORY_TEST_READER_URL").unwrap(),
        )
        .await
        .unwrap();
        position_reader.verify_runtime_contract().await.unwrap();
        let (reader, reads) = mock_reader(vec![
            (200, campaign("CAMPAIGN_STATE_RUNNING")),
            (200, metrics("1.00", "100.00")),
            (200, product(7_000_000)),
            (200, product(8_000_000)),
        ]);
        let (writer, requests) = mock_writer(vec![(200, TOKEN.to_owned()), (200, "{}".to_owned())]);
        guard_once_static(
            std::slice::from_ref(&fixture.guard),
            &mut state,
            &fixture.state_path,
            &reader,
            &writer,
            &fixture.store,
            fixture.write_authorization(&database),
            Some(&dynamic),
            Some(&position_reader),
            observed_at(),
        )
        .await
        .unwrap();
        assert_eq!(reads.try_iter().count(), 5);
        let requests = requests.try_iter().collect::<Vec<_>>();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with("PUT /api/client/campaign/21/products "));
        assert!(requests[1].contains("8000000"));
        assert_eq!(state.last_bid_change_at.get(&21), Some(&observed_at()));
        assert!(state.pending_bid_changes.is_empty());
        assert!(state.incident_campaign_ids.is_empty());
        assert_eq!(load_static_state(&fixture.state_path).unwrap(), state);
    }
}
