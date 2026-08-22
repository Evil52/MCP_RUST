use chrono::{DateTime, Duration, Timelike, Utc};
use thiserror::Error;

/// Logical search slots are aligned to `:00` and `:30` UTC.
pub const COLLECTION_INTERVAL_MINUTES: u32 = 30;

/// The collector starts five minutes after the logical slot boundary.
pub const EXECUTION_OFFSET_MINUTES: u32 = 5;

const INTERVAL_SECONDS: i64 = COLLECTION_INTERVAL_MINUTES as i64 * 60;
const OFFSET_SECONDS: i64 = EXECUTION_OFFSET_MINUTES as i64 * 60;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleError {
    #[error("planned execution time must be an exact HH:05 or HH:35 UTC boundary")]
    InvalidExecutionBoundary,
    #[error("date-time is outside the supported range")]
    OutOfRange,
}

/// Returns the next future `HH:05`/`HH:35` execution boundary.
///
/// The calculation is wall-clock based. It never replays missed ticks and
/// therefore cannot create a catch-up burst after a process pause.
pub fn next_execution_after(now: DateTime<Utc>) -> Result<DateTime<Utc>, ScheduleError> {
    let remainder = (now.timestamp() - OFFSET_SECONDS).rem_euclid(INTERVAL_SECONDS);
    let seconds = if remainder == 0 {
        INTERVAL_SECONDS
    } else {
        INTERVAL_SECONDS - remainder
    };

    let next = now
        .checked_add_signed(Duration::seconds(seconds))
        .ok_or(ScheduleError::OutOfRange)?;
    next.with_nanosecond(0)
        .and_then(|value| value.with_second(0))
        .ok_or(ScheduleError::OutOfRange)
}

/// Converts a saved, planned execution boundary to its logical half-hour slot.
///
/// Callers must pass the exact value previously returned by
/// [`next_execution_after`], not the actual wall-clock wake time. A scheduler
/// can wake a little late without changing the logical slot; after a restart it
/// must compute a new future boundary instead of reconstructing a missed one.
pub fn slot_for_planned_execution(
    planned_execution_at: DateTime<Utc>,
) -> Result<DateTime<Utc>, ScheduleError> {
    if planned_execution_at.second() != 0
        || planned_execution_at.nanosecond() != 0
        || planned_execution_at.minute() % COLLECTION_INTERVAL_MINUTES != EXECUTION_OFFSET_MINUTES
    {
        return Err(ScheduleError::InvalidExecutionBoundary);
    }

    Ok(planned_execution_at - Duration::minutes(i64::from(EXECUTION_OFFSET_MINUTES)))
}

#[expect(
    clippy::redundant_pub_crate,
    reason = "crate-only intent remains explicit if the parent module becomes public"
)]
pub(crate) fn is_aligned_slot(slot: DateTime<Utc>) -> bool {
    slot.minute().is_multiple_of(COLLECTION_INTERVAL_MINUTES)
        && slot.second() == 0
        && slot.nanosecond() == 0
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Timelike, Utc};

    use super::{ScheduleError, is_aligned_slot, next_execution_after, slot_for_planned_execution};

    #[test]
    fn next_execution_is_strictly_future_and_does_not_replay_ticks() {
        let before = Utc.with_ymd_and_hms(2026, 8, 16, 7, 4, 59).unwrap();
        let boundary = Utc.with_ymd_and_hms(2026, 8, 16, 7, 5, 0).unwrap();
        let after = boundary.with_nanosecond(1).unwrap();

        assert_eq!(next_execution_after(before).unwrap(), boundary);
        assert_eq!(
            next_execution_after(boundary).unwrap(),
            Utc.with_ymd_and_hms(2026, 8, 16, 7, 35, 0).unwrap()
        );
        assert_eq!(
            next_execution_after(after).unwrap(),
            Utc.with_ymd_and_hms(2026, 8, 16, 7, 35, 0).unwrap()
        );
    }

    #[test]
    fn schedule_crosses_midnight_without_drift() {
        let now = Utc.with_ymd_and_hms(2026, 8, 16, 23, 35, 1).unwrap();
        assert_eq!(
            next_execution_after(now).unwrap(),
            Utc.with_ymd_and_hms(2026, 8, 17, 0, 5, 0).unwrap()
        );
    }

    #[test]
    fn execution_boundary_maps_to_exact_slot() {
        let execution = Utc.with_ymd_and_hms(2026, 8, 16, 7, 35, 0).unwrap();
        let slot = Utc.with_ymd_and_hms(2026, 8, 16, 7, 30, 0).unwrap();

        assert_eq!(slot_for_planned_execution(execution).unwrap(), slot);
        assert!(is_aligned_slot(slot));
        assert!(!is_aligned_slot(execution));
    }

    #[test]
    fn invalid_execution_boundary_is_rejected() {
        let wrong_minute = Utc.with_ymd_and_hms(2026, 8, 16, 7, 6, 0).unwrap();
        let wrong_second = Utc.with_ymd_and_hms(2026, 8, 16, 7, 5, 1).unwrap();
        let wrong_nanos = Utc
            .with_ymd_and_hms(2026, 8, 16, 7, 5, 0)
            .unwrap()
            .with_nanosecond(1)
            .unwrap();

        for value in [wrong_minute, wrong_second, wrong_nanos] {
            assert_eq!(
                slot_for_planned_execution(value),
                Err(ScheduleError::InvalidExecutionBoundary)
            );
        }
    }

    #[test]
    fn date_time_overflow_is_reported() {
        assert_eq!(
            next_execution_after(chrono::DateTime::<Utc>::MAX_UTC),
            Err(ScheduleError::OutOfRange)
        );
    }
}
