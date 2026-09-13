use super::*;
use std::{io::Write, sync::Arc};

#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl Write for CapturedLog {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn with_logs(work: impl Future<Output = ()>) -> String {
    let log = CapturedLog::default();
    let sink = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .with_writer(move || sink.clone())
        .finish();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tracing::subscriber::with_default(subscriber, || runtime.block_on(work));
    let bytes = log.0.lock().unwrap().clone();
    String::from_utf8(bytes).unwrap()
}

async fn production_cycle(
    repository: &FakeRepository,
    reader: &FakeReader,
    writer: &FakeWriter,
) -> Result<(), OzonGuardWorkflowError> {
    run_durable_ozon_guard_cycle(
        repository,
        reader,
        writer,
        &clock(),
        &NoOzonGuardFailpoints,
        OzonGuardRunContext {
            account_id: "account",
            worker_id: "worker",
            write_boundary: Duration::from_secs(2),
        },
    )
    .await
}

#[test]
fn stop_observability_preserves_write_outcome_and_missing_telemetry() {
    for (write, class) in [
        (Ok(()), "ok"),
        (Err(OzonGuardWriteFailure::Permit), "permit"),
        (
            Err(OzonGuardWriteFailure::MarkerUncertain),
            "marker_uncertain",
        ),
        (Err(OzonGuardWriteFailure::Ambiguous), "ambiguous"),
    ] {
        let log = with_logs(async {
            let repository = FakeRepository::default();
            repository.0.lock().unwrap().recoveries.push_back(lease(
                durable_guard(42),
                "telemetry_unavailable",
                None,
            ));
            let reader = FakeReader::default();
            reader.running.lock().unwrap().extend([Ok(true), Ok(false)]);
            let writer = FakeWriter::default();
            writer.results.lock().unwrap().push_back(write);
            production_cycle(&repository, &reader, &writer)
                .await
                .unwrap();
            assert_eq!(repository.0.lock().unwrap().finishes, vec![(42, None)]);
            assert_eq!(*writer.calls.lock().unwrap(), vec![42]);
        });
        assert!(log.contains("stop confirmed by persisted readback"));
        assert!(log.contains(&format!("write_result=\"{class}\"")), "{log}");
        assert!(log.contains("campaign_id=42"));
        assert!(log.contains("evidence=None"));
    }
}

#[test]
fn post_write_readback_failure_retains_stop_and_continues_other_active_guards() {
    let log = with_logs(async {
        let repository = FakeRepository::default();
        repository
            .0
            .lock()
            .unwrap()
            .active
            .extend([durable_guard(1), durable_guard(2)]);
        let reader = FakeReader::default();
        let over_cap = OzonGuardMetrics {
            spend_minor: 100_000,
            attributed_revenue_minor: 0,
        };
        let healthy = OzonGuardMetrics {
            spend_minor: 1,
            attributed_revenue_minor: 100,
        };
        reader
            .metrics
            .lock()
            .unwrap()
            .extend([Ok(over_cap), Ok(healthy)]);
        reader
            .running
            .lock()
            .unwrap()
            .extend([Ok(true), Err(OzonGuardReadFailure::CampaignState)]);
        let writer = FakeWriter::default();
        production_cycle(&repository, &reader, &writer)
            .await
            .unwrap();
        let state = repository.0.lock().unwrap();
        assert_eq!(state.observations, vec![(2, healthy)]);
        assert_eq!(
            state.readbacks,
            vec![
                (1, OzonGuardStopReadback::Running),
                (1, OzonGuardStopReadback::Unavailable)
            ]
        );
        assert!(state.finishes.is_empty());
        assert!(state.incidents.is_empty());
        assert_eq!(state.recoveries.len(), 1);
        drop(state);
        assert_eq!(*writer.calls.lock().unwrap(), vec![1]);
    });
    assert!(log.contains("post-write readback unavailable; marker retained"));
    assert!(log.contains("active_read_failures=1"));
    assert!(log.contains("write_result=\"ok\""));
}

#[tokio::test]
async fn inconsistent_recovery_metrics_are_rejected_without_reading_or_writing() {
    let repository = FakeRepository::default();
    let mut inconsistent = lease(durable_guard(42), "spend_cap_reached", None);
    inconsistent.spend_minor = Some(100_000);
    repository
        .0
        .lock()
        .unwrap()
        .recoveries
        .push_back(inconsistent);
    let reader = FakeReader::default();
    let writer = FakeWriter::default();
    assert_eq!(
        production_cycle(&repository, &reader, &writer).await,
        Err(OzonPlanStoreError::Unavailable.into())
    );
    assert!(writer.calls.lock().unwrap().is_empty());
    assert!(repository.0.lock().unwrap().finishes.is_empty());
}

#[test]
fn guard_planning_rejects_invalid_limits_and_reversed_date_windows() {
    let mut invalid = durable_guard(42);
    invalid.target_drr_percent = 9;
    assert_eq!(
        plan_static_guard_first_step(
            &invalid,
            OzonGuardMetrics {
                spend_minor: 1,
                attributed_revenue_minor: 100
            }
        ),
        Err(OzonGuardEvaluationError::InvalidLimit)
    );
    assert_eq!(
        aggregate_complete_guard_metrics(
            &BTreeSet::from([42]),
            metrics_date(),
            metrics_date().pred_opt().unwrap(),
            []
        ),
        Err(OzonGuardTelemetryError::InvalidDateWindow)
    );
}

#[test]
fn unavailable_recovery_readback_reports_the_existing_marker_without_writing() {
    for write_started_at in [None, Some(DateTime::UNIX_EPOCH)] {
        let log = with_logs(async {
            let repository = FakeRepository::default();
            let pending = OzonGuardStopLease {
                write_started_at,
                ..lease(durable_guard(42), "telemetry_unavailable", None)
            };
            repository.0.lock().unwrap().recoveries.push_back(pending);
            let reader = FakeReader::default();
            reader
                .running
                .lock()
                .unwrap()
                .push_back(Err(OzonGuardReadFailure::CampaignState));
            let writer = FakeWriter::default();
            production_cycle(&repository, &reader, &writer)
                .await
                .unwrap();
            assert!(writer.calls.lock().unwrap().is_empty());
            let state = repository.0.lock().unwrap();
            assert_eq!(
                state.readbacks,
                vec![(42, OzonGuardStopReadback::Unavailable)]
            );
            assert!(state.finishes.is_empty());
            assert!(state.incidents.is_empty());
            drop(state);
        });
        assert!(log.contains("stop readback unavailable; stopping intent retained"));
        assert!(log.contains(&format!("write_started={}", write_started_at.is_some())));
        assert!(log.contains("recovery_read_failures=1"));
    }
}
