//! Pure scheduling contract for daily marketplace snapshot collection.
//!
//! The runtime will poll this contract and use PostgreSQL uniqueness to claim
//! one account/cutoff. This module deliberately performs no database or
//! marketplace I/O.

use chrono::{DateTime, Duration, NaiveDate, Utc};
use thiserror::Error;

use super::{
    ReportKey, ReportKind, ReportScheduleError, business_date, collector_plan::CollectionTarget,
    report_cutoff, reporting_interval,
};

/// A source observation may finish at most this long after its logical
/// report cutoff. The same bound is enforced by the snapshot model and SQL.
pub const COLLECTION_COMPLETION_WINDOW: Duration = Duration::minutes(30);

/// One immutable occurrence that may be collected now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueCollection {
    pub local_date: NaiveDate,
    pub kind: ReportKind,
    pub cutoff_at: DateTime<Utc>,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
    pub complete_by: DateTime<Utc>,
    pub delayed: bool,
}

/// Missing account targets for one open immutable occurrence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledCollection {
    pub occurrence: DueCollection,
    pub targets: Vec<CollectionTarget>,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum CollectionScheduleError {
    #[error(transparent)]
    Report(#[from] ReportScheduleError),
}

/// Returns the single collection occurrence whose completion window is open.
///
/// A restart inside the thirty-minute window returns the same immutable
/// occurrence with `delayed=true`; repository uniqueness prevents duplicate
/// publication. Once the window closes, no older occurrence is returned:
/// collecting current state and backdating it would corrupt report history.
pub fn due_collection(
    now: DateTime<Utc>,
) -> Result<Option<DueCollection>, CollectionScheduleError> {
    let local_date = business_date(now);
    for kind in [ReportKind::Evening, ReportKind::Morning] {
        let key = internal_key(local_date, kind);
        let cutoff_at = report_cutoff(&key)?;
        let complete_by = cutoff_at
            .checked_add_signed(COLLECTION_COMPLETION_WINDOW)
            .ok_or(ReportScheduleError::OutOfRange)?;
        if now >= cutoff_at && now <= complete_by {
            let (period_start, period_end) = reporting_interval(&key)?;
            return Ok(Some(DueCollection {
                local_date,
                kind,
                cutoff_at,
                period_start,
                period_end,
                complete_by,
                delayed: now > cutoff_at,
            }));
        }
    }
    Ok(None)
}

/// Plans only account/cutoff pairs that have not already been published.
///
/// The callback is the future repository boundary. It must return `true` only
/// when all four mandatory sources for the exact account, marketplace and
/// cutoff are terminal and published. No collection is planned outside the
/// bounded completion window.
pub fn due_for_plan(
    now: DateTime<Utc>,
    targets: &[CollectionTarget],
    published: &mut dyn FnMut(&CollectionTarget, DateTime<Utc>) -> bool,
) -> Result<Option<ScheduledCollection>, CollectionScheduleError> {
    let Some(occurrence) = due_collection(now)? else {
        return Ok(None);
    };
    let targets = targets
        .iter()
        .filter(|target| !published(target, occurrence.cutoff_at))
        .cloned()
        .collect::<Vec<_>>();
    Ok((!targets.is_empty()).then_some(ScheduledCollection {
        occurrence,
        targets,
    }))
}

fn internal_key(local_date: NaiveDate, kind: ReportKind) -> ReportKey {
    ReportKey {
        local_date,
        kind,
        recipient_id: "collector".to_owned(),
        report_version: 1,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{COLLECTION_COMPLETION_WINDOW, due_collection, due_for_plan};
    use crate::reporting::{
        ReportKind,
        collector_plan::CollectionTarget,
        snapshot::{Marketplace, SnapshotSource},
    };

    fn utc(day: u32, hour: u32, minute: u32, second: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, day, hour, minute, second)
            .unwrap()
    }

    fn target(account_id: &str, marketplace: Marketplace) -> CollectionTarget {
        CollectionTarget {
            account_id: account_id.to_owned(),
            marketplace,
            sources: [
                SnapshotSource::Sales,
                SnapshotSource::Advertising,
                SnapshotSource::Stocks,
                SnapshotSource::Prices,
            ],
        }
    }

    #[test]
    fn morning_window_uses_the_preceding_complete_yekaterinburg_day() {
        assert!(due_collection(utc(16, 2, 59, 59)).unwrap().is_none());
        let due = due_collection(utc(16, 3, 0, 0)).unwrap().unwrap();
        assert_eq!(due.kind, ReportKind::Morning);
        assert_eq!(due.local_date, utc(16, 0, 0, 0).date_naive());
        assert_eq!(due.cutoff_at, utc(16, 3, 0, 0));
        assert_eq!(due.period_start, utc(14, 19, 0, 0));
        assert_eq!(due.period_end, utc(15, 19, 0, 0));
        assert_eq!(due.complete_by, utc(16, 3, 30, 0));
        assert!(!due.delayed);
    }

    #[test]
    fn restart_inside_window_recovers_but_closed_window_never_backdates() {
        let delayed = due_collection(utc(16, 3, 30, 0)).unwrap().unwrap();
        assert!(delayed.delayed);
        assert_eq!(
            delayed.complete_by - delayed.cutoff_at,
            COLLECTION_COMPLETION_WINDOW
        );
        assert!(due_collection(utc(16, 3, 30, 1)).unwrap().is_none());
        assert!(due_collection(utc(16, 11, 59, 59)).unwrap().is_none());
    }

    #[test]
    fn evening_window_is_preliminary_and_ends_at_its_cutoff() {
        let due = due_collection(utc(16, 12, 20, 0)).unwrap().unwrap();
        assert_eq!(due.kind, ReportKind::Evening);
        assert_eq!(due.cutoff_at, utc(16, 12, 0, 0));
        assert_eq!(due.period_start, utc(15, 19, 0, 0));
        assert_eq!(due.period_end, due.cutoff_at);
        assert_eq!(due.complete_by, utc(16, 12, 30, 0));
        assert!(due.delayed);
        assert!(due_collection(utc(16, 12, 30, 1)).unwrap().is_none());
    }

    #[test]
    fn planner_retries_only_missing_accounts_for_the_exact_cutoff() {
        let targets = [
            target("diana", Marketplace::Ozon),
            target("vahrusheva", Marketplace::Wildberries),
        ];
        let expected_cutoff = utc(16, 3, 0, 0);
        let mut inspected = Vec::new();
        let plan = due_for_plan(utc(16, 3, 12, 0), &targets, &mut |target, cutoff| {
            inspected.push((target.account_id.clone(), cutoff));
            target.account_id == "diana"
        })
        .unwrap()
        .unwrap();
        assert_eq!(
            inspected,
            vec![
                ("diana".to_owned(), expected_cutoff),
                ("vahrusheva".to_owned(), expected_cutoff),
            ]
        );
        assert_eq!(plan.targets, vec![targets[1].clone()]);
        assert!(plan.occurrence.delayed);

        assert!(
            due_for_plan(utc(16, 3, 13, 0), &targets, &mut |_, _| true)
                .unwrap()
                .is_none()
        );
        assert!(
            due_for_plan(utc(16, 4, 0, 0), &targets, &mut |_, _| false)
                .unwrap()
                .is_none()
        );
    }
}
