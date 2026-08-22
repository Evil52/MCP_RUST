//! PostgreSQL-backed preflight for one daily snapshot collection occurrence.
//!
//! This layer deliberately stops before credential resolution or marketplace
//! I/O. It combines the deterministic time window with the exact set of
//! already-published account/cutoff identities and returns only missing work.

use chrono::{DateTime, Utc};
use thiserror::Error;

use super::{
    collector_plan::CollectionTarget,
    collector_schedule::{CollectionScheduleError, ScheduledCollection, due_collection},
    postgres_collector::{PostgresCollectorError, PostgresSnapshotWriter},
};

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum CollectionOrchestrationError {
    #[error(transparent)]
    Schedule(#[from] CollectionScheduleError),
    #[error("snapshot publication state is unavailable")]
    Repository,
}

/// Returns missing targets for the one collection window that is open now.
///
/// No database query is made outside a collection window. Inside one, the
/// exact cutoff is checked before any caller may resolve marketplace
/// credentials. A complete published target is never returned again.
pub async fn plan_due_collection(
    writer: &PostgresSnapshotWriter,
    now: DateTime<Utc>,
    targets: &[CollectionTarget],
) -> Result<Option<ScheduledCollection>, CollectionOrchestrationError> {
    let Some(occurrence) = due_collection(now)? else {
        return Ok(None);
    };
    let published = writer
        .published_targets(occurrence.cutoff_at, targets)
        .await
        .map_err(map_repository_error)?;
    let targets = targets
        .iter()
        .filter(|target| {
            !published.iter().any(|(account_id, marketplace)| {
                account_id == &target.account_id && *marketplace == target.marketplace
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    Ok((!targets.is_empty()).then_some(ScheduledCollection {
        occurrence,
        targets,
    }))
}

const fn map_repository_error(_: PostgresCollectorError) -> CollectionOrchestrationError {
    CollectionOrchestrationError::Repository
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_errors_are_sanitized() {
        assert_eq!(
            map_repository_error(PostgresCollectorError::Conflict),
            CollectionOrchestrationError::Repository
        );
    }
}
