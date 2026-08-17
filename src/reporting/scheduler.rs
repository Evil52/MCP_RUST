//! Idempotent planning of daily-report occurrences.
//!
//! This module persists only report identities in the transactional outbox.
//! It never collects marketplace data, renders an artifact, uploads to object
//! storage, or contacts an email provider.

use chrono::{DateTime, Utc};
use thiserror::Error;

use super::{
    PendingDelivery, ReportScheduleError, business_date, due_deliveries, policy::DailyReportPolicy,
};

/// A delivery plan ready to be persisted for one policy audience.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledDelivery {
    pub recipient_id: String,
    pub delivery: PendingDelivery,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerError {
    #[error(transparent)]
    Schedule(#[from] ReportScheduleError),
}

/// Computes every missing morning/evening occurrence for an audience.
///
/// `covered_keys` must include every persisted delivery occurrence for the
/// current business date, regardless of the batch state. A planned occurrence
/// is already reserved and must not be planned a second time after a restart.
pub fn due_for_audience(
    now: DateTime<Utc>,
    recipient_id: &str,
    report_version: u32,
    covered_keys: &std::collections::BTreeSet<super::ReportKey>,
) -> Result<Vec<ScheduledDelivery>, SchedulerError> {
    Ok(
        due_deliveries(now, recipient_id, report_version, covered_keys)?
            .into_iter()
            .map(|delivery| ScheduledDelivery {
                recipient_id: recipient_id.to_owned(),
                delivery,
            })
            .collect(),
    )
}

/// Computes plans for every audience in a validated daily-report policy.
///
/// The caller supplies coverage separately for each audience. This keeps the
/// pure scheduling policy independent of PostgreSQL and makes recovery logic
/// testable without a clock, mail account, or marketplace credentials.
pub fn due_for_policy(
    now: DateTime<Utc>,
    policy: &DailyReportPolicy,
    covered_for: &mut dyn FnMut(
        &str,
        chrono::NaiveDate,
        u32,
    ) -> std::collections::BTreeSet<super::ReportKey>,
) -> Result<Vec<ScheduledDelivery>, SchedulerError> {
    let date = business_date(now);
    let mut planned = Vec::new();
    for audience in &policy.audiences {
        planned.extend(due_for_audience(
            now,
            &audience.id,
            policy.version,
            &covered_for(&audience.id, date, policy.version),
        )?);
    }
    Ok(planned)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::{TimeZone, Utc};

    use crate::reporting::{
        ReportKey, ReportKind,
        policy::{AudiencePolicy, DailyReportPolicy, ManagerScope},
    };

    use super::{business_date, due_for_audience, due_for_policy};

    fn utc(hour: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 16, hour, 0, 0).unwrap()
    }

    #[test]
    fn planned_occurrences_are_idempotent_and_use_yekaterinburg_dates() {
        assert_eq!(
            business_date(utc(19)),
            chrono::NaiveDate::from_ymd_opt(2026, 8, 17).unwrap()
        );
        let first = due_for_audience(utc(3), "pilot_owner", 1, &BTreeSet::new()).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].delivery.covered_keys[0].kind, ReportKind::Morning);

        let covered = first[0].delivery.covered_keys.iter().cloned().collect();
        assert!(
            due_for_audience(utc(5), "pilot_owner", 1, &covered)
                .unwrap()
                .is_empty()
        );

        let evening = due_for_audience(utc(12), "pilot_owner", 1, &covered).unwrap();
        assert_eq!(evening.len(), 1);
        assert_eq!(
            evening[0].delivery.covered_keys[0].kind,
            ReportKind::Evening
        );
    }

    #[test]
    fn recovery_after_downtime_consolidates_only_uncovered_occurrences() {
        let morning = ReportKey {
            local_date: chrono::NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
            kind: ReportKind::Morning,
            recipient_id: "pilot_owner".to_owned(),
            report_version: 1,
        };
        let plans =
            due_for_audience(utc(13), "pilot_owner", 1, &[morning].into_iter().collect()).unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].delivery.covered_keys.len(), 1);
        assert_eq!(plans[0].delivery.covered_keys[0].kind, ReportKind::Evening);
    }

    #[test]
    fn policy_planning_keeps_audiences_separate() {
        let policy = DailyReportPolicy {
            version: 1,
            enabled: false,
            timezone: "Asia/Yekaterinburg".to_owned(),
            sender_email_env: "DAILY_REPORT_SENDER_EMAIL".to_owned(),
            audiences: vec![
                AudiencePolicy {
                    id: "diana".to_owned(),
                    email_env: "DIANA_EMAIL".to_owned(),
                    managers: vec![ManagerScope {
                        actor_id: "diana_serafimovich".to_owned(),
                        account_ids: ["furnitura_dlya_doma".to_owned()].into_iter().collect(),
                    }],
                },
                AudiencePolicy {
                    id: "owner".to_owned(),
                    email_env: "OWNER_EMAIL".to_owned(),
                    managers: vec![ManagerScope {
                        actor_id: "anna_agzamova".to_owned(),
                        account_ids: ["ofk_region_wb".to_owned()].into_iter().collect(),
                    }],
                },
            ],
        };
        let plans = due_for_policy(utc(3), &policy, &mut |_, _, _| BTreeSet::new()).unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].recipient_id, "diana");
        assert_eq!(plans[1].recipient_id, "owner");
    }

    #[test]
    fn invalid_policy_audience_cannot_be_scheduled() {
        let policy = DailyReportPolicy {
            version: 1,
            enabled: false,
            timezone: "Asia/Yekaterinburg".to_owned(),
            sender_email_env: "DAILY_REPORT_SENDER_EMAIL".to_owned(),
            audiences: vec![AudiencePolicy {
                id: "not a recipient id".to_owned(),
                email_env: "OWNER_EMAIL".to_owned(),
                managers: vec![ManagerScope {
                    actor_id: "anna_agzamova".to_owned(),
                    account_ids: ["ofk_region_wb".to_owned()].into_iter().collect(),
                }],
            }],
        };
        assert!(due_for_policy(utc(3), &policy, &mut |_, _, _| BTreeSet::new()).is_err());
    }
}
