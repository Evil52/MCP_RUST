use std::{future::Future, pin::Pin};

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

use super::{
    BatchPlan, BatchResult, BatchStatus, ObservationOutcome, PlacementKind, QueryResult,
    SourceError, runner::MAX_START_DELAY_SECONDS, schedule::EXECUTION_OFFSET_MINUTES,
};

const MAX_COLLECTOR_VERSION_BYTES: usize = 64;

/// Fully validated, database-independent payload for one atomic run commit.
///
/// A PostgreSQL adapter must insert the run as `running`, append all
/// measurements, optionally open the durable circuit, and only then move the
/// run to its terminal status in one transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistenceBatch {
    scheduled_for: DateTime<Utc>,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    collector_version: String,
    status: BatchStatus,
    monitors_planned: usize,
    monitors_attempted: usize,
    monitors_succeeded: usize,
    queries_planned: usize,
    queries_attempted: usize,
    queries_succeeded: usize,
    error_class: Option<ErrorClass>,
    http_status: Option<u16>,
    circuit_reason: Option<CircuitReason>,
    measurements: Vec<MeasurementRecord>,
}

impl PersistenceBatch {
    pub fn from_result(
        plan: &BatchPlan,
        result: &BatchResult,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        collector_version: impl Into<String>,
    ) -> Result<Self, PersistenceError> {
        validate_times(plan, started_at, finished_at)?;
        let collector_version = validate_collector_version(collector_version.into())?;
        validate_result_shape(plan, result)?;

        let mut monitors_attempted = 0;
        let mut monitors_succeeded = 0;
        let mut measurements = Vec::new();
        let mut first_error = None;

        for query_result in result.results() {
            match query_result {
                QueryResult::Succeeded { observations, .. } => {
                    monitors_attempted += observations.len();
                    monitors_succeeded += observations.len();
                    measurements.extend(observations.iter().map(MeasurementRecord::from));
                }
                QueryResult::Failed {
                    monitor_ids, error, ..
                } => {
                    monitors_attempted += monitor_ids.len();
                    let error_class = ErrorClass::from_source(error);
                    if query_result.circuit_reason().is_some() {
                        first_error = Some(error_class);
                    } else {
                        first_error.get_or_insert(error_class);
                    }
                }
                QueryResult::DeadlineExceeded { monitor_ids, .. } => {
                    monitors_attempted += monitor_ids.len();
                    first_error = Some(ErrorClass::DeadlineExceeded);
                }
            }
        }

        let error_class = first_error;
        let http_status = error_class.and_then(ErrorClass::http_status);
        let circuit_reason = result
            .results()
            .iter()
            .find_map(QueryResult::circuit_reason);

        Ok(Self {
            scheduled_for: plan.slot(),
            started_at,
            finished_at,
            collector_version,
            status: result.status(),
            monitors_planned: plan.target_count(),
            monitors_attempted,
            monitors_succeeded,
            queries_planned: result.planned_queries(),
            queries_attempted: result.attempted_queries(),
            queries_succeeded: result.succeeded_queries(),
            error_class,
            http_status,
            circuit_reason,
            measurements,
        })
    }

    #[must_use]
    pub fn scheduled_for(&self) -> DateTime<Utc> {
        self.scheduled_for
    }

    #[must_use]
    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }

    #[must_use]
    pub fn finished_at(&self) -> DateTime<Utc> {
        self.finished_at
    }

    #[must_use]
    pub fn collector_version(&self) -> &str {
        &self.collector_version
    }

    #[must_use]
    pub fn status(&self) -> BatchStatus {
        self.status
    }

    #[must_use]
    pub fn monitors_planned(&self) -> usize {
        self.monitors_planned
    }

    #[must_use]
    pub fn monitors_attempted(&self) -> usize {
        self.monitors_attempted
    }

    #[must_use]
    pub fn monitors_succeeded(&self) -> usize {
        self.monitors_succeeded
    }

    #[must_use]
    pub fn queries_planned(&self) -> usize {
        self.queries_planned
    }

    #[must_use]
    pub fn queries_attempted(&self) -> usize {
        self.queries_attempted
    }

    #[must_use]
    pub fn queries_succeeded(&self) -> usize {
        self.queries_succeeded
    }

    #[must_use]
    pub fn error_class(&self) -> Option<ErrorClass> {
        self.error_class
    }

    #[must_use]
    pub fn http_status(&self) -> Option<u16> {
        self.http_status
    }

    #[must_use]
    pub fn circuit_reason(&self) -> Option<CircuitReason> {
        self.circuit_reason
    }

    #[must_use]
    pub fn measurements(&self) -> &[MeasurementRecord] {
        &self.measurements
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorClass {
    SourceDisabled,
    Captcha,
    HttpForbidden,
    RateLimited,
    Timeout,
    Unavailable,
    MarkupChanged,
    InvalidObservation,
    DeadlineExceeded,
}

impl ErrorClass {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SourceDisabled => "source_disabled",
            Self::Captcha => "captcha",
            Self::HttpForbidden => "http_forbidden",
            Self::RateLimited => "rate_limited",
            Self::Timeout => "timeout",
            Self::Unavailable => "unavailable",
            Self::MarkupChanged => "markup_changed",
            Self::InvalidObservation => "invalid_observation",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }

    #[must_use]
    pub fn http_status(self) -> Option<u16> {
        match self {
            Self::HttpForbidden => Some(403),
            Self::RateLimited => Some(429),
            _ => None,
        }
    }

    fn from_source(error: &SourceError) -> Self {
        match error {
            SourceError::Disabled => Self::SourceDisabled,
            SourceError::Captcha => Self::Captcha,
            SourceError::HttpForbidden => Self::HttpForbidden,
            SourceError::RateLimited => Self::RateLimited,
            SourceError::Timeout => Self::Timeout,
            SourceError::Unavailable => Self::Unavailable,
            SourceError::MarkupChanged => Self::MarkupChanged,
            SourceError::InvalidObservation(_) => Self::InvalidObservation,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitReason {
    Captcha,
    HttpForbidden,
    RateLimited,
    MarkupChanged,
    InvalidObservation,
}

impl CircuitReason {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Captcha => "captcha",
            Self::HttpForbidden => "http_forbidden",
            Self::RateLimited => "rate_limited",
            Self::MarkupChanged => "markup_changed",
            Self::InvalidObservation => "invalid_observation",
        }
    }
}

impl QueryResult {
    fn circuit_reason(&self) -> Option<CircuitReason> {
        let QueryResult::Failed { error, .. } = self else {
            return None;
        };
        match error {
            SourceError::Captcha => Some(CircuitReason::Captcha),
            SourceError::HttpForbidden => Some(CircuitReason::HttpForbidden),
            SourceError::RateLimited => Some(CircuitReason::RateLimited),
            SourceError::MarkupChanged => Some(CircuitReason::MarkupChanged),
            SourceError::InvalidObservation(_) => Some(CircuitReason::InvalidObservation),
            SourceError::Disabled | SourceError::Timeout | SourceError::Unavailable => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistedOutcome {
    Found,
    NotFound,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MeasurementRecord {
    monitor_id: i64,
    observed_at: DateTime<Utc>,
    outcome: PersistedOutcome,
    overall_position: Option<u16>,
    placement: Option<PlacementKind>,
    organic_position: Option<u16>,
    sponsored_position: Option<u16>,
}

impl MeasurementRecord {
    #[must_use]
    pub fn monitor_id(&self) -> i64 {
        self.monitor_id
    }

    #[must_use]
    pub fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }

    #[must_use]
    pub fn outcome(&self) -> PersistedOutcome {
        self.outcome
    }

    #[must_use]
    pub fn overall_position(&self) -> Option<u16> {
        self.overall_position
    }

    #[must_use]
    pub fn placement(&self) -> Option<PlacementKind> {
        self.placement
    }

    #[must_use]
    pub fn organic_position(&self) -> Option<u16> {
        self.organic_position
    }

    #[must_use]
    pub fn sponsored_position(&self) -> Option<u16> {
        self.sponsored_position
    }
}

impl From<&super::Observation> for MeasurementRecord {
    fn from(observation: &super::Observation) -> Self {
        let (outcome, overall_position, placement, organic_position, sponsored_position) =
            match observation.outcome() {
                ObservationOutcome::Found {
                    overall_position,
                    placement,
                    organic_position,
                    sponsored_position,
                } => (
                    PersistedOutcome::Found,
                    Some(overall_position),
                    Some(placement),
                    organic_position,
                    sponsored_position,
                ),
                ObservationOutcome::NotFound => {
                    (PersistedOutcome::NotFound, None, None, None, None)
                }
            };
        Self {
            monitor_id: observation.monitor_id(),
            observed_at: observation.observed_at(),
            outcome,
            overall_position,
            placement,
            organic_position,
            sponsored_position,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistOutcome {
    Inserted,
    AlreadyExists,
}

/// Atomic, slot-idempotent persistence boundary.
///
/// Repeating an identical committed slot returns `AlreadyExists`; a different
/// payload for the same slot must fail with `SlotConflict` instead of replacing
/// history.
pub trait PositionRepository: Send + Sync {
    fn persist<'a>(
        &'a self,
        batch: &'a PersistenceBatch,
    ) -> Pin<Box<dyn Future<Output = Result<PersistOutcome, RepositoryError>> + Send + 'a>>;
}

/// Offline repository used until the reviewed PostgreSQL adapter is wired.
#[derive(Clone, Copy, Debug, Default)]
pub struct DisabledRepository;

impl PositionRepository for DisabledRepository {
    fn persist<'a>(
        &'a self,
        _batch: &'a PersistenceBatch,
    ) -> Pin<Box<dyn Future<Output = Result<PersistOutcome, RepositoryError>> + Send + 'a>> {
        Box::pin(std::future::ready(Err(RepositoryError::Disabled)))
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum RepositoryError {
    #[error("position repository is disabled")]
    Disabled,
    #[error("a different payload already exists for this collection slot")]
    SlotConflict,
    #[error("manual canary requires exactly one active monitor")]
    CanaryTargetCount,
    #[error("the active canary monitor violates the fixed 30-minute top-100 contract")]
    InvalidCanaryTarget,
    #[error("position repository is unavailable")]
    Unavailable,
}

impl From<tokio_postgres::Error> for RepositoryError {
    fn from(_error: tokio_postgres::Error) -> Self {
        Self::Unavailable
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceError {
    #[error("collector_version must contain 1..=64 non-whitespace ASCII bytes")]
    InvalidCollectorVersion,
    #[error("run timestamps are outside the logical collection slot")]
    InvalidRunTime,
    #[error("batch result does not belong to the supplied plan")]
    ResultPlanMismatch,
}

fn validate_collector_version(value: String) -> Result<String, PersistenceError> {
    if value.is_empty()
        || value.len() > MAX_COLLECTOR_VERSION_BYTES
        || !value.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(PersistenceError::InvalidCollectorVersion);
    }
    Ok(value)
}

fn validate_times(
    plan: &BatchPlan,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
) -> Result<(), PersistenceError> {
    let earliest_start = plan
        .slot()
        .checked_add_signed(Duration::minutes(i64::from(EXECUTION_OFFSET_MINUTES)))
        .ok_or(PersistenceError::InvalidRunTime)?;
    let latest_start = earliest_start
        .checked_add_signed(Duration::seconds(
            i64::try_from(MAX_START_DELAY_SECONDS).expect("fixed start delay fits i64"),
        ))
        .ok_or(PersistenceError::InvalidRunTime)?;
    let slot_end = plan
        .slot()
        .checked_add_signed(Duration::minutes(30))
        .ok_or(PersistenceError::InvalidRunTime)?;
    if started_at < earliest_start
        || started_at > latest_start
        || finished_at < started_at
        || finished_at >= slot_end
    {
        return Err(PersistenceError::InvalidRunTime);
    }
    Ok(())
}

fn validate_result_shape(plan: &BatchPlan, result: &BatchResult) -> Result<(), PersistenceError> {
    if result.planned_queries() != plan.queries().len()
        || result.attempted_queries() != result.results().len()
        || result.succeeded_queries()
            != result
                .results()
                .iter()
                .filter(|item| matches!(item, QueryResult::Succeeded { .. }))
                .count()
    {
        return Err(PersistenceError::ResultPlanMismatch);
    }

    for (query_result, query_plan) in result.results().iter().zip(plan.queries()) {
        let (request, monitor_ids) = match query_result {
            QueryResult::Succeeded {
                request,
                observations,
            } => (
                request,
                observations
                    .iter()
                    .map(super::Observation::monitor_id)
                    .collect::<Vec<_>>(),
            ),
            QueryResult::Failed {
                request,
                monitor_ids,
                ..
            }
            | QueryResult::DeadlineExceeded {
                request,
                monitor_ids,
            } => (request, monitor_ids.clone()),
        };
        let expected_ids = query_plan
            .targets()
            .iter()
            .map(super::MonitorTarget::monitor_id)
            .collect::<Vec<_>>();
        if request != query_plan.request() || monitor_ids != expected_ids {
            return Err(PersistenceError::ResultPlanMismatch);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{future::ready, pin::Pin};

    use chrono::{TimeZone, Utc};

    use super::{
        CircuitReason, DisabledRepository, ErrorClass, PersistOutcome, PersistedOutcome,
        PersistenceBatch, PersistenceError, PositionRepository, RepositoryError,
    };
    use crate::position_collector::{
        BatchPlan, BatchResult, BatchStatus, MonitorTarget, PlacementKind, PositionSource,
        QueryRequest, QueryResult, QueryScan, SearchHit, SourceError, ValidationError,
    };

    fn slot() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 16, 7, 30, 0).unwrap()
    }

    fn target(monitor_id: i64, product_id: &str) -> MonitorTarget {
        MonitorTarget::new(
            monitor_id,
            "store-1",
            product_id,
            "ручка кнопка",
            "moscow",
            "Москва",
            100,
        )
        .unwrap()
    }

    fn batch_plan(targets: Vec<MonitorTarget>) -> BatchPlan {
        BatchPlan::new(slot(), targets).unwrap()
    }

    fn failed_result(plan: &BatchPlan, error: SourceError) -> BatchResult {
        let query = &plan.queries()[0];
        BatchResult::fixture(
            if error.is_protective()
                || matches!(
                    error,
                    SourceError::MarkupChanged | SourceError::InvalidObservation(_)
                )
            {
                BatchStatus::Blocked
            } else {
                BatchStatus::Failed
            },
            1,
            1,
            0,
            vec![QueryResult::Failed {
                request: query.request().clone(),
                monitor_ids: query
                    .targets()
                    .iter()
                    .map(MonitorTarget::monitor_id)
                    .collect(),
                error,
            }],
        )
    }

    fn persist(
        plan: &BatchPlan,
        result: &BatchResult,
    ) -> Result<PersistenceBatch, PersistenceError> {
        PersistenceBatch::from_result(
            plan,
            result,
            slot() + chrono::Duration::minutes(5),
            slot() + chrono::Duration::minutes(6),
            "test-1.0",
        )
    }

    #[test]
    fn successful_payload_preserves_found_unknown_and_not_found_observations() {
        let plan = batch_plan(vec![target(1, "1001"), target(2, "1002")]);
        let query = &plan.queries()[0];
        let observations = QueryScan::new(
            slot() + chrono::Duration::minutes(5),
            "moscow",
            true,
            true,
            vec![
                SearchHit::new("1001", 9, PlacementKind::Unknown).unwrap(),
                SearchHit::new("1001", 14, PlacementKind::Organic).unwrap(),
                SearchHit::new("1001", 6, PlacementKind::Sponsored).unwrap(),
            ],
        )
        .into_observations(query)
        .unwrap();
        let result = BatchResult::fixture(
            BatchStatus::Succeeded,
            1,
            1,
            1,
            vec![QueryResult::Succeeded {
                request: query.request().clone(),
                observations,
            }],
        );

        let payload = persist(&plan, &result).unwrap();
        assert_eq!(payload.scheduled_for(), slot());
        assert_eq!(payload.started_at(), slot() + chrono::Duration::minutes(5));
        assert_eq!(payload.finished_at(), slot() + chrono::Duration::minutes(6));
        assert_eq!(payload.collector_version(), "test-1.0");
        assert_eq!(payload.status(), BatchStatus::Succeeded);
        assert_eq!(payload.monitors_planned(), 2);
        assert_eq!(payload.monitors_attempted(), 2);
        assert_eq!(payload.monitors_succeeded(), 2);
        assert_eq!(payload.queries_planned(), 1);
        assert_eq!(payload.queries_attempted(), 1);
        assert_eq!(payload.queries_succeeded(), 1);
        assert_eq!(payload.error_class(), None);
        assert_eq!(payload.http_status(), None);
        assert_eq!(payload.circuit_reason(), None);

        let found = &payload.measurements()[0];
        assert_eq!(found.monitor_id(), 1);
        assert_eq!(found.observed_at(), slot() + chrono::Duration::minutes(5));
        assert_eq!(found.outcome(), PersistedOutcome::Found);
        assert_eq!(found.overall_position(), Some(6));
        assert_eq!(found.placement(), Some(PlacementKind::Sponsored));
        assert_eq!(found.organic_position(), Some(14));
        assert_eq!(found.sponsored_position(), Some(6));

        let missing = &payload.measurements()[1];
        assert_eq!(missing.monitor_id(), 2);
        assert_eq!(missing.outcome(), PersistedOutcome::NotFound);
        assert_eq!(missing.overall_position(), None);
        assert_eq!(missing.placement(), None);
        assert_eq!(missing.organic_position(), None);
        assert_eq!(missing.sponsored_position(), None);
    }

    #[test]
    fn source_failures_are_payload_free_and_map_to_stable_classes() {
        let plan = batch_plan(vec![target(1, "1001")]);
        let cases = [
            (
                SourceError::Disabled,
                ErrorClass::SourceDisabled,
                None,
                None,
            ),
            (
                SourceError::Captcha,
                ErrorClass::Captcha,
                None,
                Some(CircuitReason::Captcha),
            ),
            (
                SourceError::HttpForbidden,
                ErrorClass::HttpForbidden,
                Some(403),
                Some(CircuitReason::HttpForbidden),
            ),
            (
                SourceError::RateLimited,
                ErrorClass::RateLimited,
                Some(429),
                Some(CircuitReason::RateLimited),
            ),
            (SourceError::Timeout, ErrorClass::Timeout, None, None),
            (
                SourceError::Unavailable,
                ErrorClass::Unavailable,
                None,
                None,
            ),
            (
                SourceError::MarkupChanged,
                ErrorClass::MarkupChanged,
                None,
                Some(CircuitReason::MarkupChanged),
            ),
            (
                SourceError::InvalidObservation(ValidationError::RegionMismatch),
                ErrorClass::InvalidObservation,
                None,
                Some(CircuitReason::InvalidObservation),
            ),
        ];

        for (source, expected_class, expected_http, expected_circuit) in cases {
            let payload = persist(&plan, &failed_result(&plan, source)).unwrap();
            assert_eq!(payload.error_class(), Some(expected_class));
            assert_eq!(payload.http_status(), expected_http);
            assert_eq!(payload.circuit_reason(), expected_circuit);
            assert!(payload.measurements().is_empty());
            assert_eq!(payload.monitors_attempted(), 1);
            assert_eq!(payload.monitors_succeeded(), 0);
        }

        let classes = [
            (ErrorClass::SourceDisabled, "source_disabled"),
            (ErrorClass::Captcha, "captcha"),
            (ErrorClass::HttpForbidden, "http_forbidden"),
            (ErrorClass::RateLimited, "rate_limited"),
            (ErrorClass::Timeout, "timeout"),
            (ErrorClass::Unavailable, "unavailable"),
            (ErrorClass::MarkupChanged, "markup_changed"),
            (ErrorClass::InvalidObservation, "invalid_observation"),
            (ErrorClass::DeadlineExceeded, "deadline_exceeded"),
        ];
        for (class, expected) in classes {
            assert_eq!(class.as_str(), expected);
        }
        for (reason, expected) in [
            (CircuitReason::Captcha, "captcha"),
            (CircuitReason::HttpForbidden, "http_forbidden"),
            (CircuitReason::RateLimited, "rate_limited"),
            (CircuitReason::MarkupChanged, "markup_changed"),
            (CircuitReason::InvalidObservation, "invalid_observation"),
        ] {
            assert_eq!(reason.as_str(), expected);
        }
    }

    #[test]
    fn deadline_result_maps_without_fabricating_measurements() {
        let plan = batch_plan(vec![target(1, "1001")]);
        let query = &plan.queries()[0];
        let result = BatchResult::fixture(
            BatchStatus::Failed,
            1,
            1,
            0,
            vec![QueryResult::DeadlineExceeded {
                request: query.request().clone(),
                monitor_ids: vec![1],
            }],
        );
        let payload = persist(&plan, &result).unwrap();
        assert_eq!(payload.error_class(), Some(ErrorClass::DeadlineExceeded));
        assert!(payload.measurements().is_empty());
    }

    #[test]
    fn terminal_safety_error_takes_precedence_over_an_ordinary_failure() {
        let plan = BatchPlan::new(
            slot(),
            vec![
                MonitorTarget::new(1, "store-1", "1001", "alpha", "moscow", "Москва", 100).unwrap(),
                MonitorTarget::new(2, "store-1", "1002", "beta", "moscow", "Москва", 100).unwrap(),
            ],
        )
        .unwrap();
        let result = BatchResult::fixture(
            BatchStatus::Blocked,
            2,
            2,
            0,
            vec![
                QueryResult::Failed {
                    request: plan.queries()[0].request().clone(),
                    monitor_ids: vec![1],
                    error: SourceError::Timeout,
                },
                QueryResult::Failed {
                    request: plan.queries()[1].request().clone(),
                    monitor_ids: vec![2],
                    error: SourceError::RateLimited,
                },
            ],
        );
        let payload = persist(&plan, &result).unwrap();
        assert_eq!(payload.error_class(), Some(ErrorClass::RateLimited));
        assert_eq!(payload.http_status(), Some(429));
        assert_eq!(payload.circuit_reason(), Some(CircuitReason::RateLimited));
    }

    #[test]
    fn persistence_payload_rejects_invalid_version_time_and_foreign_result() {
        let plan = batch_plan(vec![target(1, "1001")]);
        let result = failed_result(&plan, SourceError::Disabled);
        for version in ["", "bad version", &"x".repeat(65)] {
            assert_eq!(
                PersistenceBatch::from_result(
                    &plan,
                    &result,
                    slot() + chrono::Duration::minutes(5),
                    slot() + chrono::Duration::minutes(6),
                    version,
                ),
                Err(PersistenceError::InvalidCollectorVersion)
            );
        }
        for (started_at, finished_at) in [
            (slot() - chrono::Duration::seconds(1), slot()),
            (
                slot() + chrono::Duration::minutes(7) + chrono::Duration::seconds(1),
                slot() + chrono::Duration::minutes(8),
            ),
            (
                slot() + chrono::Duration::minutes(6),
                slot() + chrono::Duration::minutes(5),
            ),
            (
                slot() + chrono::Duration::minutes(5),
                slot() + chrono::Duration::minutes(30),
            ),
        ] {
            assert_eq!(
                PersistenceBatch::from_result(&plan, &result, started_at, finished_at, "test",),
                Err(PersistenceError::InvalidRunTime)
            );
        }

        let foreign_plan = batch_plan(vec![target(2, "2002")]);
        assert_eq!(
            persist(&foreign_plan, &result),
            Err(PersistenceError::ResultPlanMismatch)
        );

        let malformed = BatchResult::fixture(BatchStatus::Failed, 2, 0, 1, Vec::new());
        assert_eq!(
            persist(&plan, &malformed),
            Err(PersistenceError::ResultPlanMismatch)
        );

        let same_request_different_monitor = batch_plan(vec![target(9, "1001")]);
        let wrong_observation = BatchResult::fixture(
            BatchStatus::Succeeded,
            1,
            1,
            1,
            vec![QueryResult::Succeeded {
                request: plan.queries()[0].request().clone(),
                observations: QueryScan::new(
                    slot() + chrono::Duration::minutes(5),
                    "moscow",
                    true,
                    true,
                    Vec::new(),
                )
                .into_observations(&same_request_different_monitor.queries()[0])
                .unwrap(),
            }],
        );
        assert_eq!(
            persist(&plan, &wrong_observation),
            Err(PersistenceError::ResultPlanMismatch)
        );
    }

    struct NeverUsedSource;

    impl PositionSource for NeverUsedSource {
        fn scan(
            &self,
            _request: QueryRequest,
        ) -> Pin<Box<dyn Future<Output = Result<QueryScan, SourceError>> + Send + '_>> {
            Box::pin(ready(Err(SourceError::Disabled)))
        }
    }

    #[tokio::test]
    async fn disabled_repository_performs_no_io_and_exposes_stable_result_contract() {
        let plan = batch_plan(vec![target(1, "1001")]);
        let payload = persist(&plan, &failed_result(&plan, SourceError::Disabled)).unwrap();
        assert_eq!(
            DisabledRepository.persist(&payload).await,
            Err(RepositoryError::Disabled)
        );
        assert_eq!(PersistOutcome::Inserted, PersistOutcome::Inserted);
        assert_ne!(PersistOutcome::Inserted, PersistOutcome::AlreadyExists);
        assert_eq!(
            RepositoryError::SlotConflict.to_string(),
            "a different payload already exists for this collection slot"
        );
        assert_eq!(
            RepositoryError::Unavailable.to_string(),
            "position repository is unavailable"
        );
        let source = NeverUsedSource;
        assert_eq!(
            source.scan(plan.queries()[0].request().clone()).await,
            Err(SourceError::Disabled)
        );
    }
}
