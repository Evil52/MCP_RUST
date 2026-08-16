//! Safe, provider-independent core for scheduled public search observations.
//!
//! This module deliberately contains no browser, HTTP, marketplace credential,
//! or database implementation. It defines the deterministic contract those
//! adapters must satisfy before a real collector is enabled.

mod model;
mod postgres_repository;
mod repository;
mod runner;
mod schedule;

pub use model::{
    BatchPlan, MonitorTarget, Observation, ObservationOutcome, PlacementKind, QueryPlan,
    QueryRequest, QueryScan, SearchHit, ValidationError,
};
pub use postgres_repository::PostgresRepository;
pub use repository::{
    CircuitReason, DisabledRepository, ErrorClass, MeasurementRecord, PersistOutcome,
    PersistedOutcome, PersistenceBatch, PersistenceError, PositionRepository, RepositoryError,
};
pub use runner::{
    BATCH_TIMEOUT_SECONDS, BatchResult, BatchStatus, BatchStopReason, CollectError, Collector,
    DisabledSource, MAX_CONSECUTIVE_FAILURES, MAX_START_DELAY_SECONDS, MIN_QUERY_INTERVAL_SECONDS,
    PAGE_TIMEOUT_SECONDS, PositionSource, QueryResult, SourceError,
};
pub use schedule::{
    COLLECTION_INTERVAL_MINUTES, EXECUTION_OFFSET_MINUTES, ScheduleError, next_execution_after,
    slot_for_planned_execution,
};
