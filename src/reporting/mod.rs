//! Deterministic scheduling primitives for server-generated daily reports.
//!
//! This module does not collect marketplace data and does not send email. It
//! defines stable report identities and catch-up decisions which a future
//! PostgreSQL outbox worker can persist and deliver exactly once.

use std::collections::BTreeSet;

use chrono::{DateTime, Duration, FixedOffset, NaiveDate, TimeZone, Utc};
use thiserror::Error;

pub mod html;
pub mod kpi;
pub mod outbox;
pub mod policy;
pub mod postgres_outbox;
pub mod postgres_snapshot;
pub mod rules;
pub mod snapshot;
pub mod xlsx;

const YEKATERINBURG_OFFSET_SECONDS: i32 = 5 * 60 * 60;
const MORNING_HOUR: u32 = 8;
const MORNING_DEADLINE_HOUR: u32 = 14;
const EVENING_HOUR: u32 = 17;
const EVENING_DEADLINE_HOUR: u32 = 23;

/// The two immutable daily report identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReportKind {
    Morning,
    Evening,
}

/// Idempotency key used by the future PostgreSQL outbox.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReportKey {
    pub local_date: NaiveDate,
    pub kind: ReportKind,
    pub recipient_id: String,
    pub report_version: u32,
}

/// A report that may be delivered now.
///
/// `covered_keys` normally contains one key. When the server recovers after
/// 17:00 and neither report was sent, one consolidated delivery covers both
/// keys so the recipient does not receive two near-identical messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDelivery {
    pub covered_keys: Vec<ReportKey>,
    pub scheduled_for: DateTime<Utc>,
    pub delayed: bool,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ReportScheduleError {
    #[error("report version must be positive")]
    InvalidReportVersion,
    #[error("recipient id must be a non-empty bounded identifier")]
    InvalidRecipientId,
    #[error("date-time is outside the supported range")]
    OutOfRange,
}

/// Returns due report deliveries for the current Yekaterinburg business day.
///
/// Already sent report keys are never returned. Missed morning and evening
/// reports are consolidated after 17:00. Reports are not delivered after the
/// evening 23:00 deadline; the persistence layer records such keys as expired.
pub fn due_deliveries(
    now: DateTime<Utc>,
    recipient_id: &str,
    report_version: u32,
    sent: &BTreeSet<ReportKey>,
) -> Result<Vec<PendingDelivery>, ReportScheduleError> {
    validate_identity(recipient_id, report_version)?;

    let offset = yekaterinburg_offset();
    let local_now = now.with_timezone(&offset);
    let date = local_now.date_naive();
    let morning = report_time(date, MORNING_HOUR)?;
    let morning_deadline = report_time(date, MORNING_DEADLINE_HOUR)?;
    let evening = report_time(date, EVENING_HOUR)?;
    let evening_deadline = report_time(date, EVENING_DEADLINE_HOUR)?;

    let morning_key = report_key(date, ReportKind::Morning, recipient_id, report_version);
    let evening_key = report_key(date, ReportKind::Evening, recipient_id, report_version);
    let morning_missing = !sent.contains(&morning_key);
    let evening_missing = !sent.contains(&evening_key);

    if local_now >= evening && local_now <= evening_deadline {
        let mut covered_keys = Vec::with_capacity(2);
        if morning_missing {
            covered_keys.push(morning_key);
        }
        if evening_missing {
            covered_keys.push(evening_key);
        }
        return Ok((!covered_keys.is_empty())
            .then(|| PendingDelivery {
                covered_keys,
                scheduled_for: evening.with_timezone(&Utc),
                delayed: local_now > evening,
            })
            .into_iter()
            .collect());
    }

    if local_now >= morning && local_now <= morning_deadline && morning_missing {
        return Ok(vec![PendingDelivery {
            covered_keys: vec![morning_key],
            scheduled_for: morning.with_timezone(&Utc),
            delayed: local_now > morning,
        }]);
    }

    Ok(Vec::new())
}

fn validate_identity(recipient_id: &str, report_version: u32) -> Result<(), ReportScheduleError> {
    if report_version == 0 {
        return Err(ReportScheduleError::InvalidReportVersion);
    }
    if recipient_id.is_empty()
        || recipient_id.len() > 128
        || !recipient_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(ReportScheduleError::InvalidRecipientId);
    }
    Ok(())
}

fn report_key(
    local_date: NaiveDate,
    kind: ReportKind,
    recipient_id: &str,
    report_version: u32,
) -> ReportKey {
    ReportKey {
        local_date,
        kind,
        recipient_id: recipient_id.to_owned(),
        report_version,
    }
}

fn report_time(date: NaiveDate, hour: u32) -> Result<DateTime<FixedOffset>, ReportScheduleError> {
    let local = date
        .and_hms_opt(hour, 0, 0)
        .ok_or(ReportScheduleError::OutOfRange)?;
    yekaterinburg_offset()
        .from_local_datetime(&local)
        .single()
        .ok_or(ReportScheduleError::OutOfRange)
}

fn yekaterinburg_offset() -> FixedOffset {
    FixedOffset::east_opt(YEKATERINBURG_OFFSET_SECONDS)
        .expect("the fixed Yekaterinburg UTC offset is valid")
}

/// Deadline after which a scheduled report must not be sent automatically.
pub fn delivery_deadline(key: &ReportKey) -> Result<DateTime<Utc>, ReportScheduleError> {
    validate_identity(&key.recipient_id, key.report_version)?;
    let hour = match key.kind {
        ReportKind::Morning => MORNING_DEADLINE_HOUR,
        ReportKind::Evening => EVENING_DEADLINE_HOUR,
    };
    Ok(report_time(key.local_date, hour)?.with_timezone(&Utc))
}

/// Returns the business data interval represented by a report.
///
/// Morning reports cover the complete preceding local day. Evening reports
/// cover the current local day from midnight through the 17:00 cutoff and are
/// explicitly preliminary.
pub fn reporting_interval(
    key: &ReportKey,
) -> Result<(DateTime<Utc>, DateTime<Utc>), ReportScheduleError> {
    validate_identity(&key.recipient_id, key.report_version)?;
    let date = match key.kind {
        ReportKind::Morning => key
            .local_date
            .checked_sub_signed(Duration::days(1))
            .ok_or(ReportScheduleError::OutOfRange)?,
        ReportKind::Evening => key.local_date,
    };
    let start = report_time(date, 0)?;
    let end = match key.kind {
        ReportKind::Morning => report_time(key.local_date, 0)?,
        ReportKind::Evening => report_time(key.local_date, EVENING_HOUR)?,
    };
    Ok((start.with_timezone(&Utc), end.with_timezone(&Utc)))
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone, Utc};

    use super::{
        ReportKey, ReportKind, ReportScheduleError, delivery_deadline, due_deliveries,
        reporting_interval,
    };

    fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .unwrap()
    }

    fn key(kind: ReportKind) -> ReportKey {
        ReportKey {
            local_date: NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
            kind,
            recipient_id: "pilot_owner".to_owned(),
            report_version: 1,
        }
    }

    #[test]
    fn schedule_uses_yekaterinburg_time_and_marks_late_delivery() {
        assert!(
            due_deliveries(
                utc(2026, 8, 16, 2, 59),
                "pilot_owner",
                1,
                &Default::default()
            )
            .unwrap()
            .is_empty()
        );

        let on_time = due_deliveries(
            utc(2026, 8, 16, 3, 0),
            "pilot_owner",
            1,
            &Default::default(),
        )
        .unwrap();
        assert_eq!(on_time.len(), 1);
        assert!(!on_time[0].delayed);
        assert_eq!(on_time[0].covered_keys, vec![key(ReportKind::Morning)]);

        let late = due_deliveries(
            utc(2026, 8, 16, 5, 0),
            "pilot_owner",
            1,
            &Default::default(),
        )
        .unwrap();
        assert!(late[0].delayed);
    }

    #[test]
    fn recovery_after_evening_cutoff_consolidates_missing_reports() {
        let deliveries = due_deliveries(
            utc(2026, 8, 16, 13, 30),
            "pilot_owner",
            1,
            &Default::default(),
        )
        .unwrap();

        assert_eq!(deliveries.len(), 1);
        assert!(deliveries[0].delayed);
        assert_eq!(
            deliveries[0].covered_keys,
            vec![key(ReportKind::Morning), key(ReportKind::Evening)]
        );
        assert_eq!(deliveries[0].scheduled_for, utc(2026, 8, 16, 12, 0));
    }

    #[test]
    fn sent_keys_prevent_duplicates_and_allow_the_missing_companion() {
        let sent = [key(ReportKind::Morning)].into_iter().collect();
        let deliveries = due_deliveries(utc(2026, 8, 16, 12, 0), "pilot_owner", 1, &sent).unwrap();

        assert_eq!(deliveries[0].covered_keys, vec![key(ReportKind::Evening)]);
        assert!(
            due_deliveries(
                utc(2026, 8, 16, 12, 0),
                "pilot_owner",
                1,
                &[key(ReportKind::Morning), key(ReportKind::Evening)]
                    .into_iter()
                    .collect(),
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn expired_windows_are_not_automatically_sent() {
        assert!(
            due_deliveries(
                utc(2026, 8, 16, 10, 0),
                "pilot_owner",
                1,
                &Default::default()
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            due_deliveries(
                utc(2026, 8, 16, 18, 1),
                "pilot_owner",
                1,
                &Default::default()
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn deadlines_and_data_intervals_are_exact() {
        assert_eq!(
            delivery_deadline(&key(ReportKind::Morning)).unwrap(),
            utc(2026, 8, 16, 9, 0)
        );
        assert_eq!(
            delivery_deadline(&key(ReportKind::Evening)).unwrap(),
            utc(2026, 8, 16, 18, 0)
        );
        assert_eq!(
            reporting_interval(&key(ReportKind::Morning)).unwrap(),
            (utc(2026, 8, 14, 19, 0), utc(2026, 8, 15, 19, 0))
        );
        assert_eq!(
            reporting_interval(&key(ReportKind::Evening)).unwrap(),
            (utc(2026, 8, 15, 19, 0), utc(2026, 8, 16, 12, 0))
        );
    }

    #[test]
    fn invalid_identities_and_unrepresentable_dates_fail_closed() {
        for recipient in ["", "bad recipient", "почта", &"x".repeat(129)] {
            assert_eq!(
                due_deliveries(utc(2026, 8, 16, 3, 0), recipient, 1, &Default::default()),
                Err(ReportScheduleError::InvalidRecipientId)
            );
        }
        assert_eq!(
            due_deliveries(
                utc(2026, 8, 16, 3, 0),
                "pilot_owner",
                0,
                &Default::default()
            ),
            Err(ReportScheduleError::InvalidReportVersion)
        );

        let invalid = ReportKey {
            local_date: NaiveDate::MIN,
            kind: ReportKind::Morning,
            recipient_id: "pilot_owner".to_owned(),
            report_version: 1,
        };
        assert_eq!(
            reporting_interval(&invalid),
            Err(ReportScheduleError::OutOfRange)
        );

        let invalid_identity = ReportKey {
            recipient_id: "bad recipient".to_owned(),
            ..key(ReportKind::Morning)
        };
        assert_eq!(
            delivery_deadline(&invalid_identity),
            Err(ReportScheduleError::InvalidRecipientId)
        );
        assert_eq!(
            reporting_interval(&invalid_identity),
            Err(ReportScheduleError::InvalidRecipientId)
        );
    }
}
