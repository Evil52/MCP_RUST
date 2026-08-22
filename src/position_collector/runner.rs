use std::{
    future::{Future, ready},
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use thiserror::Error;

use super::{
    model::{BatchPlan, Observation, QueryPlan, QueryRequest, QueryScan, ValidationError},
    schedule::{COLLECTION_INTERVAL_MINUTES, EXECUTION_OFFSET_MINUTES},
};

pub const PAGE_TIMEOUT_SECONDS: u64 = 90;
pub const BATCH_TIMEOUT_SECONDS: u64 = 20 * 60;
pub const MIN_QUERY_INTERVAL_SECONDS: u64 = 10;
pub const MAX_START_DELAY_SECONDS: u64 = 2 * 60;
pub const MAX_CONSECUTIVE_FAILURES: usize = 3;

/// Provider boundary for one public `(region, phrase)` scan.
///
/// The core never retries this method. A future browser/API implementation is
/// responsible only for one bounded observation and must not contain seller
/// credentials or marketplace write operations.
pub trait PositionSource: Send + Sync {
    fn scan(
        &self,
        request: QueryRequest,
    ) -> Pin<Box<dyn Future<Output = Result<QueryScan, SourceError>> + Send + '_>>;
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SourceError {
    #[error("public search source is disabled")]
    Disabled,
    #[error("public search returned CAPTCHA")]
    Captcha,
    #[error("public search returned HTTP 403")]
    HttpForbidden,
    #[error("public search returned HTTP 429")]
    RateLimited,
    #[error("public search timed out")]
    Timeout,
    #[error("public search provider is unavailable")]
    Unavailable,
    #[error("public search markup is unsupported")]
    MarkupChanged,
    #[error("public search result failed validation: {0}")]
    InvalidObservation(ValidationError),
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum CollectError {
    #[error("the preceding collection slot is still running")]
    OverlappingRun,
    #[error("collection is paused after a protective response")]
    CircuitOpen,
    #[error("actual start is outside the bounded window for the planned slot")]
    OutsideStartWindow,
}

/// Offline source used by the scaffold until a reviewed provider is enabled.
#[derive(Clone, Copy, Debug, Default)]
pub struct DisabledSource;

impl PositionSource for DisabledSource {
    fn scan(
        &self,
        _request: QueryRequest,
    ) -> Pin<Box<dyn Future<Output = Result<QueryScan, SourceError>> + Send + '_>> {
        Box::pin(ready(Err(SourceError::Disabled)))
    }
}

impl SourceError {
    #[must_use]
    pub const fn is_protective(&self) -> bool {
        matches!(
            self,
            Self::Captcha | Self::HttpForbidden | Self::RateLimited
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchStatus {
    Succeeded,
    Partial,
    Failed,
    Blocked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchStopReason {
    Completed,
    SafetyCircuit,
    ConsecutiveFailures,
    Deadline,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryResult {
    Succeeded {
        request: QueryRequest,
        observations: Vec<Observation>,
    },
    Failed {
        request: QueryRequest,
        monitor_ids: Vec<i64>,
        error: SourceError,
    },
    DeadlineExceeded {
        request: QueryRequest,
        monitor_ids: Vec<i64>,
    },
}

enum QueryScanError {
    Source(SourceError),
    BatchDeadline,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchResult {
    status: BatchStatus,
    planned_queries: usize,
    attempted_queries: usize,
    succeeded_queries: usize,
    stopped_early: bool,
    stop_reason: BatchStopReason,
    circuit_break: bool,
    results: Vec<QueryResult>,
}

impl BatchResult {
    #[must_use]
    pub const fn status(&self) -> BatchStatus {
        self.status
    }

    #[must_use]
    pub const fn planned_queries(&self) -> usize {
        self.planned_queries
    }

    #[must_use]
    pub const fn attempted_queries(&self) -> usize {
        self.attempted_queries
    }

    #[must_use]
    pub const fn succeeded_queries(&self) -> usize {
        self.succeeded_queries
    }

    #[must_use]
    pub const fn stopped_early(&self) -> bool {
        self.stopped_early
    }

    #[must_use]
    pub const fn stop_reason(&self) -> BatchStopReason {
        self.stop_reason
    }

    #[must_use]
    pub fn results(&self) -> &[QueryResult] {
        &self.results
    }

    #[cfg(test)]
    pub(super) fn fixture(
        status: BatchStatus,
        planned_queries: usize,
        attempted_queries: usize,
        succeeded_queries: usize,
        results: Vec<QueryResult>,
    ) -> Self {
        Self {
            status,
            planned_queries,
            attempted_queries,
            succeeded_queries,
            stopped_early: attempted_queries < planned_queries,
            stop_reason: BatchStopReason::Completed,
            circuit_break: status == BatchStatus::Blocked,
            results,
        }
    }
}

/// Executes a deterministic batch once, sequentially, with no retry.
///
/// Sequential execution is intentional for the first scaffold. It produces
/// less bursty public traffic and is ample for 14 managers after queries are
/// coalesced by `(region, phrase)`. Any CAPTCHA, 403, or 429 stops the rest of
/// the batch immediately.
async fn collect_batch(
    source: &dyn PositionSource,
    plan: &BatchPlan,
    batch_deadline: tokio::time::Instant,
) -> BatchResult {
    let planned_queries = plan.queries().len();
    let mut attempted_queries = 0;
    let mut succeeded_queries = 0;
    let mut results = Vec::with_capacity(planned_queries);
    let mut blocked = false;
    let mut circuit_break = false;
    let mut consecutive_failures = 0;
    let mut stop_reason = BatchStopReason::Completed;
    let query_interval = Duration::from_secs(MIN_QUERY_INTERVAL_SECONDS);
    let mut next_query_at = tokio::time::Instant::now();

    for query in plan.queries() {
        if tokio::time::timeout_at(batch_deadline, tokio::time::sleep_until(next_query_at))
            .await
            .is_err()
        {
            stop_reason = BatchStopReason::Deadline;
            break;
        }
        next_query_at = tokio::time::Instant::now() + query_interval;
        attempted_queries += 1;
        let request = query.request().clone();
        let monitor_ids = query
            .targets()
            .iter()
            .map(super::model::MonitorTarget::monitor_id)
            .collect::<Vec<_>>();
        match scan_query(source, query, &request, batch_deadline).await {
            Ok(observations) => {
                succeeded_queries += 1;
                consecutive_failures = 0;
                results.push(QueryResult::Succeeded {
                    request,
                    observations,
                });
            }
            Err(QueryScanError::BatchDeadline) => {
                results.push(QueryResult::DeadlineExceeded {
                    request,
                    monitor_ids,
                });
                stop_reason = BatchStopReason::Deadline;
                break;
            }
            Err(QueryScanError::Source(error)) => {
                circuit_break = error.is_protective()
                    || matches!(
                        &error,
                        SourceError::MarkupChanged | SourceError::InvalidObservation(_)
                    );
                blocked = circuit_break;
                consecutive_failures += 1;
                results.push(QueryResult::Failed {
                    request,
                    monitor_ids,
                    error,
                });
                if circuit_break {
                    stop_reason = BatchStopReason::SafetyCircuit;
                    break;
                }
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    stop_reason = BatchStopReason::ConsecutiveFailures;
                    break;
                }
            }
        }
    }

    let status = batch_status(blocked, succeeded_queries, planned_queries);

    BatchResult {
        status,
        planned_queries,
        attempted_queries,
        succeeded_queries,
        stopped_early: attempted_queries < planned_queries,
        stop_reason,
        circuit_break,
        results,
    }
}

const fn batch_status(
    blocked: bool,
    succeeded_queries: usize,
    planned_queries: usize,
) -> BatchStatus {
    if blocked {
        BatchStatus::Blocked
    } else if succeeded_queries == planned_queries {
        BatchStatus::Succeeded
    } else if succeeded_queries == 0 {
        BatchStatus::Failed
    } else {
        BatchStatus::Partial
    }
}

async fn scan_query(
    source: &dyn PositionSource,
    query: &QueryPlan,
    request: &QueryRequest,
    batch_deadline: tokio::time::Instant,
) -> Result<Vec<Observation>, QueryScanError> {
    let page_deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(PAGE_TIMEOUT_SECONDS))
        .expect("fixed page timeout fits Tokio Instant");
    let effective_deadline = page_deadline.min(batch_deadline);
    match tokio::time::timeout_at(effective_deadline, source.scan(request.clone())).await {
        Ok(Ok(scan)) => scan
            .into_observations(query)
            .map_err(SourceError::InvalidObservation)
            .map_err(QueryScanError::Source),
        Ok(Err(error)) => Err(QueryScanError::Source(error)),
        Err(_) if effective_deadline == batch_deadline => Err(QueryScanError::BatchDeadline),
        Err(_) => Err(QueryScanError::Source(SourceError::Timeout)),
    }
}

/// Safe entry point that enforces one in-process batch at a time.
pub struct Collector {
    source: Box<dyn PositionSource>,
    gate: SingleFlight,
    circuit_open: AtomicBool,
}

impl Collector {
    pub fn new(source: impl PositionSource + 'static) -> Self {
        Self {
            source: Box::new(source),
            gate: SingleFlight::default(),
            circuit_open: AtomicBool::new(false),
        }
    }

    /// Runs a batch only inside the bounded start window for its logical slot.
    ///
    /// `started_at` is the scheduler's actual UTC wake time. The scheduler must
    /// keep the planned boundary separately; this check permits at most a small
    /// wake delay and prevents an old slot from being replayed after restart.
    ///
    /// # Panics
    ///
    /// Panics if the bounded batch budget cannot be represented by Tokio's
    /// monotonic clock.
    pub async fn collect_at(
        &self,
        plan: &BatchPlan,
        started_at: DateTime<Utc>,
    ) -> Result<BatchResult, CollectError> {
        if self.circuit_open.load(Ordering::Acquire) {
            return Err(CollectError::CircuitOpen);
        }
        let budget = batch_budget(plan, started_at)?;
        let _lease = self.gate.try_start().ok_or(CollectError::OverlappingRun)?;
        let deadline = tokio::time::Instant::now()
            .checked_add(budget)
            .expect("bounded batch budget fits Tokio Instant");
        let result = collect_batch(self.source.as_ref(), plan, deadline).await;
        if result.circuit_break {
            self.circuit_open.store(true, Ordering::Release);
        }
        Ok(result)
    }

    pub fn is_running(&self) -> bool {
        self.gate.is_running()
    }

    pub fn is_circuit_open(&self) -> bool {
        self.circuit_open.load(Ordering::Acquire)
    }

    /// Explicit operator action after the blocking condition has been reviewed.
    pub fn reset_circuit(&self) {
        self.circuit_open.store(false, Ordering::Release);
    }
}

impl std::fmt::Debug for Collector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Collector")
            .field("running", &self.is_running())
            .field("circuit_open", &self.is_circuit_open())
            .finish_non_exhaustive()
    }
}

fn batch_budget(plan: &BatchPlan, started_at: DateTime<Utc>) -> Result<Duration, CollectError> {
    let planned_start = plan
        .slot()
        .checked_add_signed(ChronoDuration::minutes(i64::from(EXECUTION_OFFSET_MINUTES)))
        .ok_or(CollectError::OutsideStartWindow)?;
    let latest_start = planned_start
        .checked_add_signed(ChronoDuration::seconds(
            i64::try_from(MAX_START_DELAY_SECONDS).expect("fixed delay fits i64"),
        ))
        .ok_or(CollectError::OutsideStartWindow)?;
    if started_at < planned_start || started_at > latest_start {
        return Err(CollectError::OutsideStartWindow);
    }

    // Reject a slot whose own interval is not representable: the fixed budget
    // below is only meaningful inside a slot that has an end.
    plan.slot()
        .checked_add_signed(ChronoDuration::minutes(i64::from(
            COLLECTION_INTERVAL_MINUTES,
        )))
        .ok_or(CollectError::OutsideStartWindow)?;
    // The latest accepted start is slot + 7 minutes, so the fixed 20-minute
    // budget always ends no later than slot + 27 minutes, comfortably inside
    // the next COLLECTION_INTERVAL_MINUTES boundary.
    Ok(Duration::from_secs(BATCH_TIMEOUT_SECONDS))
}

/// In-process overlap guard. Distributed slot idempotency remains a database
/// invariant; this guard prevents two local scheduler ticks from overlapping.
#[derive(Debug, Default)]
struct SingleFlight {
    running: AtomicBool,
}

impl SingleFlight {
    fn try_start(&self) -> Option<SingleFlightLease<'_>> {
        self.running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| SingleFlightLease { owner: self })
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub struct SingleFlightLease<'a> {
    owner: &'a SingleFlight,
}

impl Drop for SingleFlightLease<'_> {
    fn drop(&mut self) {
        self.owner.running.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, future::ready, pin::Pin, sync::Mutex, time::Duration};

    use chrono::{TimeZone, Utc};

    use super::{
        BatchStatus, BatchStopReason, CollectError, Collector, DisabledSource, PositionSource,
        QueryResult, SingleFlight, SourceError, batch_budget, collect_batch,
    };
    use crate::position_collector::{
        BatchPlan, MonitorTarget, PlacementKind, QueryRequest, QueryScan, SearchHit,
        ValidationError,
    };

    struct FixtureSource {
        responses: Mutex<VecDeque<Result<QueryScan, SourceError>>>,
        requests: Mutex<Vec<QueryRequest>>,
    }

    impl FixtureSource {
        fn new(responses: Vec<Result<QueryScan, SourceError>>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn request_count(&self) -> usize {
            self.requests.lock().unwrap().len()
        }
    }

    impl PositionSource for FixtureSource {
        fn scan(
            &self,
            request: QueryRequest,
        ) -> Pin<Box<dyn Future<Output = Result<QueryScan, SourceError>> + Send + '_>> {
            self.requests.lock().unwrap().push(request);
            Box::pin(ready(self.responses.lock().unwrap().pop_front().unwrap()))
        }
    }

    fn slot() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 16, 7, 30, 0).unwrap()
    }

    fn planned_start() -> chrono::DateTime<Utc> {
        slot() + chrono::Duration::minutes(5)
    }

    fn plan(phrases: &[&str]) -> BatchPlan {
        let targets = phrases
            .iter()
            .enumerate()
            .map(|(index, phrase)| {
                MonitorTarget::new(
                    i64::try_from(index + 1).unwrap(),
                    "store",
                    format!("100{index}"),
                    *phrase,
                    "moscow",
                    "Москва",
                    100,
                )
                .unwrap()
            })
            .collect();
        BatchPlan::new(slot(), targets).unwrap()
    }

    fn successful_scan(product_id: &str) -> QueryScan {
        QueryScan::new(
            slot(),
            "moscow",
            true,
            true,
            vec![SearchHit::new(product_id, 21, PlacementKind::Unknown).unwrap()],
        )
    }

    async fn run_batch(source: &dyn PositionSource, plan: &BatchPlan) -> super::BatchResult {
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(super::BATCH_TIMEOUT_SECONDS);
        collect_batch(source, plan, deadline).await
    }

    #[tokio::test(start_paused = true)]
    async fn successful_batch_calls_each_coalesced_query_once() {
        let plan = plan(&["one", "two"]);
        let source = FixtureSource::new(vec![
            Ok(successful_scan("1000")),
            Ok(successful_scan("1001")),
        ]);

        let result = run_batch(&source, &plan).await;

        assert_eq!(result.status(), BatchStatus::Succeeded);
        assert_eq!(result.planned_queries(), 2);
        assert_eq!(result.attempted_queries(), 2);
        assert_eq!(result.succeeded_queries(), 2);
        assert_eq!(result.stop_reason(), BatchStopReason::Completed);
        assert!(!result.stopped_early());
        assert_eq!(result.results().len(), 2);
        assert_eq!(source.request_count(), 2);
        assert!(matches!(
            &result.results()[0],
            QueryResult::Succeeded { observations, request }
                if observations.len() == 1 && request.search_phrase() == "one"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn ordinary_failure_is_not_retried_and_later_queries_continue() {
        let plan = plan(&["one", "two"]);
        let source =
            FixtureSource::new(vec![Err(SourceError::Timeout), Ok(successful_scan("1001"))]);

        let result = run_batch(&source, &plan).await;

        assert_eq!(result.status(), BatchStatus::Partial);
        assert_eq!(result.attempted_queries(), 2);
        assert_eq!(result.succeeded_queries(), 1);
        assert_eq!(source.request_count(), 2);
        assert!(matches!(
            &result.results()[0],
            QueryResult::Failed {
                error: SourceError::Timeout,
                monitor_ids,
                ..
            } if monitor_ids == &[1]
        ));
    }

    #[tokio::test]
    async fn all_failed_batch_is_failed() {
        let plan = plan(&["one"]);
        let source = FixtureSource::new(vec![Err(SourceError::Unavailable)]);
        let result = run_batch(&source, &plan).await;
        assert_eq!(result.status(), BatchStatus::Failed);
        assert_eq!(result.succeeded_queries(), 0);
    }

    #[test]
    fn start_window_and_absolute_batch_deadline_are_fail_closed() {
        let plan = plan(&["one"]);
        assert_eq!(
            batch_budget(&plan, planned_start() - chrono::Duration::seconds(1)),
            Err(CollectError::OutsideStartWindow)
        );
        assert_eq!(
            batch_budget(&plan, planned_start() + chrono::Duration::seconds(121)),
            Err(CollectError::OutsideStartWindow)
        );
        assert_eq!(
            batch_budget(&plan, planned_start()).unwrap(),
            Duration::from_secs(super::BATCH_TIMEOUT_SECONDS)
        );

        let last_day = chrono::DateTime::<Utc>::MAX_UTC.date_naive();
        let last_slot = last_day.and_hms_opt(23, 30, 0).unwrap().and_utc();
        let last_plan = BatchPlan::new(
            last_slot,
            vec![MonitorTarget::new(1, "store", "1", "p", "r", "R", 100).unwrap()],
        )
        .unwrap();
        assert_eq!(
            batch_budget(&last_plan, last_slot + chrono::Duration::minutes(5)),
            Err(CollectError::OutsideStartWindow)
        );
    }

    #[tokio::test]
    async fn disabled_source_never_creates_business_observations() {
        let plan = plan(&["one"]);
        let collector = Collector::new(DisabledSource);
        assert!(!collector.is_running());
        let result = collector.collect_at(&plan, planned_start()).await.unwrap();
        assert!(!collector.is_running());
        assert_eq!(result.status(), BatchStatus::Failed);
        assert!(matches!(
            &result.results()[0],
            QueryResult::Failed {
                error: SourceError::Disabled,
                ..
            }
        ));
    }

    #[test]
    fn collector_debug_reports_state_without_source_internals() {
        let rendered = format!("{:?}", Collector::new(DisabledSource));

        assert!(rendered.contains("running: false"));
        assert!(rendered.contains("circuit_open: false"));
        assert!(!rendered.contains("source"));
    }

    #[tokio::test]
    async fn protective_response_stops_batch_without_retry() {
        for protective in [
            SourceError::Captcha,
            SourceError::HttpForbidden,
            SourceError::RateLimited,
        ] {
            let plan = plan(&["one", "two", "three"]);
            let source =
                FixtureSource::new(vec![Err(protective.clone()), Ok(successful_scan("1001"))]);

            let result = run_batch(&source, &plan).await;

            assert_eq!(result.status(), BatchStatus::Blocked);
            assert_eq!(result.planned_queries(), 3);
            assert_eq!(result.attempted_queries(), 1);
            assert_eq!(result.succeeded_queries(), 0);
            assert_eq!(result.stop_reason(), BatchStopReason::SafetyCircuit);
            assert!(result.stopped_early());
            assert_eq!(source.request_count(), 1);
        }
    }

    #[tokio::test]
    async fn invalid_provider_result_is_not_published() {
        let plan = plan(&["one"]);
        let collector = Collector::new(FixtureSource::new(vec![Ok(QueryScan::new(
            slot(),
            "other",
            true,
            true,
            vec![],
        ))]));
        let result = collector.collect_at(&plan, planned_start()).await.unwrap();

        assert_eq!(result.status(), BatchStatus::Blocked);
        assert!(collector.is_circuit_open());
        assert!(matches!(
            &result.results()[0],
            QueryResult::Failed {
                error: SourceError::InvalidObservation(ValidationError::RegionMismatch),
                ..
            }
        ));
    }

    #[test]
    fn source_error_classification_only_stops_on_protection() {
        for error in [
            SourceError::Disabled,
            SourceError::Timeout,
            SourceError::Unavailable,
            SourceError::MarkupChanged,
            SourceError::InvalidObservation(ValidationError::IncompleteScan),
        ] {
            assert!(!error.is_protective());
        }
    }

    #[test]
    fn single_flight_releases_on_drop() {
        let gate = SingleFlight::default();
        assert!(!gate.is_running());
        let lease = gate.try_start().unwrap();
        assert!(gate.is_running());
        assert!(gate.try_start().is_none());
        drop(lease);
        assert!(!gate.is_running());
        assert!(gate.try_start().is_some());
    }

    struct PendingSource {
        entered: std::sync::Arc<tokio::sync::Notify>,
    }

    impl PositionSource for PendingSource {
        fn scan(
            &self,
            _request: QueryRequest,
        ) -> Pin<Box<dyn Future<Output = Result<QueryScan, SourceError>> + Send + '_>> {
            self.entered.notify_one();
            Box::pin(std::future::pending())
        }
    }

    #[tokio::test]
    async fn collector_rejects_overlap_and_cancellation_releases_lease() {
        let entered = std::sync::Arc::new(tokio::sync::Notify::new());
        let collector = std::sync::Arc::new(Collector::new(PendingSource {
            entered: entered.clone(),
        }));
        let plan = std::sync::Arc::new(plan(&["one"]));
        let task = {
            let collector = collector.clone();
            let plan = plan.clone();
            tokio::spawn(async move { collector.collect_at(&plan, planned_start()).await })
        };
        entered.notified().await;

        assert!(collector.is_running());
        assert_eq!(
            collector.collect_at(&plan, planned_start()).await,
            Err(CollectError::OverlappingRun)
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!collector.is_running());
    }

    #[tokio::test(start_paused = true)]
    async fn pending_source_is_cancelled_at_the_fixed_page_timeout() {
        let entered = std::sync::Arc::new(tokio::sync::Notify::new());
        let collector = Collector::new(PendingSource { entered });
        let result = collector
            .collect_at(&plan(&["one"]), planned_start())
            .await
            .unwrap();

        assert_eq!(result.status(), BatchStatus::Failed);
        assert!(matches!(
            &result.results()[0],
            QueryResult::Failed {
                error: SourceError::Timeout,
                ..
            }
        ));
        assert!(!collector.is_running());
    }

    #[tokio::test]
    async fn protective_response_latches_circuit_until_operator_reset() {
        let collector = Collector::new(FixtureSource::new(vec![
            Err(SourceError::RateLimited),
            Ok(successful_scan("1000")),
        ]));
        let plan = plan(&["one"]);

        assert_eq!(
            collector
                .collect_at(&plan, planned_start())
                .await
                .unwrap()
                .status(),
            BatchStatus::Blocked
        );
        assert!(collector.is_circuit_open());
        assert_eq!(
            collector.collect_at(&plan, planned_start()).await,
            Err(CollectError::CircuitOpen)
        );
        collector.reset_circuit();
        assert!(!collector.is_circuit_open());
        assert_eq!(
            collector
                .collect_at(&plan, planned_start())
                .await
                .unwrap()
                .status(),
            BatchStatus::Succeeded
        );
    }

    #[tokio::test(start_paused = true)]
    async fn markup_change_latches_circuit_and_three_transport_failures_stop_batch() {
        let markup_collector =
            Collector::new(FixtureSource::new(vec![Err(SourceError::MarkupChanged)]));
        assert_eq!(
            markup_collector
                .collect_at(&plan(&["one"]), planned_start())
                .await
                .unwrap()
                .status(),
            BatchStatus::Blocked
        );
        assert!(markup_collector.is_circuit_open());

        let plan = plan(&["one", "two", "three", "four"]);
        let source = FixtureSource::new(vec![
            Err(SourceError::Unavailable),
            Err(SourceError::Timeout),
            Err(SourceError::Unavailable),
            Ok(successful_scan("1003")),
        ]);
        let result = run_batch(&source, &plan).await;
        assert_eq!(result.status(), BatchStatus::Failed);
        assert_eq!(result.attempted_queries(), 3);
        assert_eq!(result.stop_reason(), BatchStopReason::ConsecutiveFailures);
        assert!(result.stopped_early());
        assert_eq!(source.request_count(), 3);
    }

    #[derive(Clone)]
    struct TimedSource {
        starts: std::sync::Arc<Mutex<Vec<tokio::time::Instant>>>,
    }

    impl PositionSource for TimedSource {
        fn scan(
            &self,
            request: QueryRequest,
        ) -> Pin<Box<dyn Future<Output = Result<QueryScan, SourceError>> + Send + '_>> {
            self.starts
                .lock()
                .unwrap()
                .push(tokio::time::Instant::now());
            let product_id = request.product_ids()[0].clone();
            Box::pin(ready(Ok(QueryScan::new(
                request.slot(),
                request.region_code(),
                true,
                true,
                vec![SearchHit::new(product_id, 21, PlacementKind::Unknown).unwrap()],
            ))))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn query_starts_are_paced_even_when_responses_are_immediate() {
        let starts = std::sync::Arc::new(Mutex::new(Vec::new()));
        let collector = Collector::new(TimedSource {
            starts: starts.clone(),
        });

        let result = collector
            .collect_at(&plan(&["one", "two"]), planned_start())
            .await
            .unwrap();

        assert_eq!(result.status(), BatchStatus::Succeeded);
        let starts = starts.lock().unwrap();
        assert_eq!(starts.len(), 2);
        assert!(
            starts[1].duration_since(starts[0])
                >= Duration::from_secs(super::MIN_QUERY_INTERVAL_SECONDS)
        );
        drop(starts);
    }

    #[tokio::test(start_paused = true)]
    async fn batch_deadline_can_stop_during_the_inter_query_pacing_wait() {
        let plan = plan(&["one", "two"]);
        let source = FixtureSource::new(vec![Ok(successful_scan("1000"))]);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

        let result = collect_batch(&source, &plan, deadline).await;

        assert_eq!(result.status(), BatchStatus::Partial);
        assert_eq!(result.stop_reason(), BatchStopReason::Deadline);
        assert_eq!(result.attempted_queries(), 1);
        assert_eq!(result.succeeded_queries(), 1);
        assert!(result.stopped_early());
        assert_eq!(source.request_count(), 1);
    }

    struct SlowSuccessSource;

    impl PositionSource for SlowSuccessSource {
        fn scan(
            &self,
            request: QueryRequest,
        ) -> Pin<Box<dyn Future<Output = Result<QueryScan, SourceError>> + Send + '_>> {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(super::PAGE_TIMEOUT_SECONDS - 1)).await;
                let product_id = request.product_ids()[0].clone();
                Ok(QueryScan::new(
                    request.slot(),
                    request.region_code(),
                    true,
                    true,
                    vec![SearchHit::new(product_id, 21, PlacementKind::Unknown).unwrap()],
                ))
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn total_batch_deadline_bounds_many_slow_queries() {
        let collector = Collector::new(SlowSuccessSource);
        let plan = plan(&[
            "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
            "eleven", "twelve", "thirteen", "fourteen",
        ]);
        let expected_deadline_monitor_ids = plan.queries()[13]
            .targets()
            .iter()
            .map(super::super::model::MonitorTarget::monitor_id)
            .collect::<Vec<_>>();

        let result = collector.collect_at(&plan, planned_start()).await.unwrap();
        assert_eq!(result.status(), BatchStatus::Partial);
        assert_eq!(result.stop_reason(), BatchStopReason::Deadline);
        assert_eq!(result.planned_queries(), 14);
        assert_eq!(result.attempted_queries(), 14);
        assert_eq!(result.succeeded_queries(), 13);
        assert!(matches!(
            result.results().last(),
            Some(QueryResult::DeadlineExceeded { monitor_ids, .. })
                if monitor_ids == &expected_deadline_monitor_ids
        ));
        assert!(!collector.is_running());
    }
}
