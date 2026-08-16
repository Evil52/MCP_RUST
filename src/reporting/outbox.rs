use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

use super::{PendingDelivery, ReportKey, delivery_deadline};

const MAX_DELIVERY_ATTEMPTS: u8 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    Planned,
    Generating,
    Ready,
    Sending,
    Sent,
    Expired,
    PermanentFailure,
}

impl DeliveryStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Sent | Self::Expired | Self::PermanentFailure)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryErrorClass {
    Authentication,
    InvalidRecipient,
    RateLimited,
    ProviderUnavailable,
    Transport,
}

impl DeliveryErrorClass {
    fn is_transient(self) -> bool {
        matches!(
            self,
            Self::RateLimited | Self::ProviderUnavailable | Self::Transport
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactIdentity {
    pub object_key: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryRecord {
    covered_keys: Vec<ReportKey>,
    scheduled_for: DateTime<Utc>,
    status: DeliveryStatus,
    delayed: bool,
    attempts: u8,
    artifact: Option<ArtifactIdentity>,
    next_attempt_at: Option<DateTime<Utc>>,
    provider_message_id: Option<String>,
    last_error: Option<DeliveryErrorClass>,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OutboxError {
    #[error("delivery transition is not allowed from the current state")]
    InvalidTransition,
    #[error("delivery has passed its automatic-delivery deadline")]
    DeadlineExceeded,
    #[error("artifact identity is invalid")]
    InvalidArtifact,
    #[error("provider message id is invalid")]
    InvalidProviderMessageId,
    #[error("transient retry time must move forward and remain before the deadline")]
    InvalidRetryTime,
    #[error("report schedule is invalid")]
    InvalidSchedule,
}

impl DeliveryRecord {
    pub fn planned(delivery: PendingDelivery) -> Result<Self, OutboxError> {
        if delivery.covered_keys.is_empty() || delivery.covered_keys.len() > 2 {
            return Err(OutboxError::InvalidSchedule);
        }
        let recipient = &delivery.covered_keys[0].recipient_id;
        let version = delivery.covered_keys[0].report_version;
        let local_date = delivery.covered_keys[0].local_date;
        if delivery.covered_keys.iter().any(|key| {
            key.recipient_id != *recipient
                || key.report_version != version
                || key.local_date != local_date
        }) {
            return Err(OutboxError::InvalidSchedule);
        }
        let unique_keys: BTreeSet<_> = delivery.covered_keys.iter().collect();
        if unique_keys.len() != delivery.covered_keys.len() {
            return Err(OutboxError::InvalidSchedule);
        }
        let deadline = deadline(&delivery.covered_keys)?;
        if delivery.scheduled_for != deadline - Duration::hours(6) {
            return Err(OutboxError::InvalidSchedule);
        }
        Ok(Self {
            covered_keys: delivery.covered_keys,
            scheduled_for: delivery.scheduled_for,
            status: DeliveryStatus::Planned,
            delayed: delivery.delayed,
            attempts: 0,
            artifact: None,
            next_attempt_at: None,
            provider_message_id: None,
            last_error: None,
        })
    }

    pub fn covered_keys(&self) -> &[ReportKey] {
        &self.covered_keys
    }

    pub fn scheduled_for(&self) -> DateTime<Utc> {
        self.scheduled_for
    }

    pub fn status(&self) -> DeliveryStatus {
        self.status
    }

    pub fn delayed(&self) -> bool {
        self.delayed
    }

    pub fn attempts(&self) -> u8 {
        self.attempts
    }

    pub fn artifact(&self) -> Option<&ArtifactIdentity> {
        self.artifact.as_ref()
    }

    pub fn next_attempt_at(&self) -> Option<DateTime<Utc>> {
        self.next_attempt_at
    }

    pub fn provider_message_id(&self) -> Option<&str> {
        self.provider_message_id.as_deref()
    }

    pub fn last_error(&self) -> Option<DeliveryErrorClass> {
        self.last_error
    }

    pub fn start_generation(&mut self, now: DateTime<Utc>) -> Result<(), OutboxError> {
        self.require_before_deadline(now)?;
        if self.status != DeliveryStatus::Planned {
            return Err(OutboxError::InvalidTransition);
        }
        self.status = DeliveryStatus::Generating;
        Ok(())
    }

    pub fn mark_ready(
        &mut self,
        now: DateTime<Utc>,
        artifact: ArtifactIdentity,
    ) -> Result<(), OutboxError> {
        self.require_before_deadline(now)?;
        if self.status != DeliveryStatus::Generating {
            return Err(OutboxError::InvalidTransition);
        }
        validate_artifact(&artifact)?;
        self.artifact = Some(artifact);
        self.status = DeliveryStatus::Ready;
        Ok(())
    }

    pub fn claim_send(&mut self, now: DateTime<Utc>) -> Result<(), OutboxError> {
        self.require_before_deadline(now)?;
        if self.status != DeliveryStatus::Ready
            || self.next_attempt_at.is_some_and(|retry| retry > now)
        {
            return Err(OutboxError::InvalidTransition);
        }
        self.attempts += 1;
        self.next_attempt_at = None;
        self.status = DeliveryStatus::Sending;
        Ok(())
    }

    pub fn record_sent(&mut self, provider_message_id: String) -> Result<(), OutboxError> {
        if self.status != DeliveryStatus::Sending {
            return Err(OutboxError::InvalidTransition);
        }
        if provider_message_id.is_empty()
            || provider_message_id.len() > 512
            || !provider_message_id.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':' | b'@')
            })
        {
            return Err(OutboxError::InvalidProviderMessageId);
        }
        self.provider_message_id = Some(provider_message_id);
        self.last_error = None;
        self.status = DeliveryStatus::Sent;
        Ok(())
    }

    pub fn record_failure(
        &mut self,
        now: DateTime<Utc>,
        class: DeliveryErrorClass,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<(), OutboxError> {
        if self.status != DeliveryStatus::Sending {
            return Err(OutboxError::InvalidTransition);
        }
        if !class.is_transient() || self.attempts >= MAX_DELIVERY_ATTEMPTS {
            self.last_error = Some(class);
            self.status = DeliveryStatus::PermanentFailure;
            return Ok(());
        }
        let retry_at = retry_at.ok_or(OutboxError::InvalidRetryTime)?;
        if retry_at <= now || retry_at > deadline(&self.covered_keys)? {
            return Err(OutboxError::InvalidRetryTime);
        }
        self.last_error = Some(class);
        self.next_attempt_at = Some(retry_at);
        self.status = DeliveryStatus::Ready;
        Ok(())
    }

    pub fn expire(&mut self, now: DateTime<Utc>) -> Result<(), OutboxError> {
        if self.status.is_terminal() {
            return Err(OutboxError::InvalidTransition);
        }
        if now <= deadline(&self.covered_keys)? {
            return Err(OutboxError::DeadlineExceeded);
        }
        self.status = DeliveryStatus::Expired;
        self.next_attempt_at = None;
        Ok(())
    }

    fn require_before_deadline(&self, now: DateTime<Utc>) -> Result<(), OutboxError> {
        if now > deadline(&self.covered_keys)? {
            return Err(OutboxError::DeadlineExceeded);
        }
        Ok(())
    }
}

fn deadline(keys: &[ReportKey]) -> Result<DateTime<Utc>, OutboxError> {
    let mut maximum = None;
    for key in keys {
        let candidate = delivery_deadline(key).map_err(|_| OutboxError::InvalidSchedule)?;
        maximum = Some(maximum.map_or(candidate, |current: DateTime<Utc>| current.max(candidate)));
    }
    maximum.ok_or(OutboxError::InvalidSchedule)
}

fn validate_artifact(artifact: &ArtifactIdentity) -> Result<(), OutboxError> {
    if artifact.object_key.is_empty()
        || artifact.object_key.len() > 512
        || artifact.sha256.len() != 64
        || !artifact.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(OutboxError::InvalidArtifact);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::{TimeZone, Utc};

    use super::{
        ArtifactIdentity, DeliveryErrorClass, DeliveryRecord, DeliveryStatus, OutboxError,
    };
    use crate::reporting::due_deliveries;

    fn utc(hour: u32, minute: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 16, hour, minute, 0).unwrap()
    }

    fn planned(now: chrono::DateTime<Utc>) -> DeliveryRecord {
        let delivery = due_deliveries(now, "pilot_owner", 1, &BTreeSet::new())
            .unwrap()
            .remove(0);
        DeliveryRecord::planned(delivery).unwrap()
    }

    fn artifact() -> ArtifactIdentity {
        ArtifactIdentity {
            object_key: "reports/2026-08-16/pilot_owner.xlsx".to_owned(),
            sha256: "a".repeat(64),
        }
    }

    fn ready() -> DeliveryRecord {
        let mut record = planned(utc(3, 0));
        record.start_generation(utc(3, 0)).unwrap();
        record.mark_ready(utc(3, 1), artifact()).unwrap();
        record
    }

    #[test]
    fn happy_path_is_exactly_once_and_keeps_audit_identity() {
        let mut record = ready();
        assert_eq!(record.status(), DeliveryStatus::Ready);
        assert_eq!(record.scheduled_for(), utc(3, 0));
        assert!(!record.delayed());
        assert_eq!(record.covered_keys().len(), 1);
        assert_eq!(record.artifact(), Some(&artifact()));

        record.claim_send(utc(3, 2)).unwrap();
        assert_eq!(record.attempts(), 1);
        record.record_sent("gmail-message-1".to_owned()).unwrap();
        assert_eq!(record.status(), DeliveryStatus::Sent);
        assert_eq!(record.provider_message_id(), Some("gmail-message-1"));
        assert_eq!(record.last_error(), None);
        assert!(record.status().is_terminal());
        assert_eq!(
            record.claim_send(utc(3, 3)),
            Err(OutboxError::InvalidTransition)
        );
    }

    #[test]
    fn transient_failure_retries_only_at_a_future_bounded_time() {
        let mut record = ready();
        record.claim_send(utc(3, 2)).unwrap();
        record
            .record_failure(utc(3, 3), DeliveryErrorClass::RateLimited, Some(utc(3, 10)))
            .unwrap();
        assert_eq!(record.status(), DeliveryStatus::Ready);
        assert_eq!(record.next_attempt_at(), Some(utc(3, 10)));
        assert_eq!(record.last_error(), Some(DeliveryErrorClass::RateLimited));
        assert_eq!(
            record.claim_send(utc(3, 9)),
            Err(OutboxError::InvalidTransition)
        );
        record.claim_send(utc(3, 10)).unwrap();
        record
            .record_failure(
                utc(3, 11),
                DeliveryErrorClass::ProviderUnavailable,
                Some(utc(3, 20)),
            )
            .unwrap();
    }

    #[test]
    fn invalid_retry_and_permanent_classes_fail_closed() {
        for retry in [None, Some(utc(3, 3)), Some(utc(9, 1))] {
            let mut record = ready();
            record.claim_send(utc(3, 3)).unwrap();
            assert_eq!(
                record.record_failure(utc(3, 3), DeliveryErrorClass::Transport, retry),
                Err(OutboxError::InvalidRetryTime)
            );
        }
        for class in [
            DeliveryErrorClass::Authentication,
            DeliveryErrorClass::InvalidRecipient,
        ] {
            let mut record = ready();
            record.claim_send(utc(3, 3)).unwrap();
            record.record_failure(utc(3, 4), class, None).unwrap();
            assert_eq!(record.status(), DeliveryStatus::PermanentFailure);
        }
    }

    #[test]
    fn retry_budget_is_hard_capped() {
        let mut record = ready();
        for minute in 0..4 {
            let now = utc(3, 2 + minute * 2);
            record.claim_send(now).unwrap();
            record
                .record_failure(
                    now,
                    DeliveryErrorClass::Transport,
                    Some(now + chrono::Duration::minutes(1)),
                )
                .unwrap();
        }
        record.claim_send(utc(3, 10)).unwrap();
        record
            .record_failure(utc(3, 11), DeliveryErrorClass::Transport, Some(utc(3, 12)))
            .unwrap();
        assert_eq!(record.attempts(), 5);
        assert_eq!(record.status(), DeliveryStatus::PermanentFailure);
    }

    #[test]
    fn generation_artifact_and_provider_transitions_are_strict() {
        let invalid_artifacts = [
            ArtifactIdentity {
                object_key: String::new(),
                sha256: "a".repeat(64),
            },
            ArtifactIdentity {
                object_key: "x".repeat(513),
                sha256: "a".repeat(64),
            },
            ArtifactIdentity {
                object_key: "x".to_owned(),
                sha256: "g".repeat(64),
            },
            ArtifactIdentity {
                object_key: "x".to_owned(),
                sha256: "a".repeat(63),
            },
        ];
        for invalid in invalid_artifacts {
            let mut record = planned(utc(3, 0));
            record.start_generation(utc(3, 0)).unwrap();
            assert_eq!(
                record.mark_ready(utc(3, 1), invalid),
                Err(OutboxError::InvalidArtifact)
            );
        }

        let mut record = planned(utc(3, 0));
        assert_eq!(
            record.mark_ready(utc(3, 0), artifact()),
            Err(OutboxError::InvalidTransition)
        );
        record.start_generation(utc(3, 0)).unwrap();
        assert_eq!(
            record.start_generation(utc(3, 0)),
            Err(OutboxError::InvalidTransition)
        );
        record.mark_ready(utc(3, 1), artifact()).unwrap();
        assert_eq!(
            record.record_sent("not-sending".to_owned()),
            Err(OutboxError::InvalidTransition)
        );
        record.claim_send(utc(3, 2)).unwrap();
        for invalid in [
            String::new(),
            "x".repeat(513),
            "contains whitespace".to_owned(),
        ] {
            assert_eq!(
                record.record_sent(invalid),
                Err(OutboxError::InvalidProviderMessageId)
            );
        }
    }

    #[test]
    fn deadlines_expire_nonterminal_work_and_consolidated_uses_evening_deadline() {
        let mut morning = planned(utc(3, 0));
        assert_eq!(
            morning.expire(utc(9, 0)),
            Err(OutboxError::DeadlineExceeded)
        );
        morning.expire(utc(9, 1)).unwrap();
        assert_eq!(morning.status(), DeliveryStatus::Expired);
        assert_eq!(
            morning.expire(utc(9, 2)),
            Err(OutboxError::InvalidTransition)
        );

        let mut consolidated = planned(utc(13, 30));
        assert!(consolidated.delayed());
        assert_eq!(consolidated.covered_keys().len(), 2);
        consolidated.start_generation(utc(13, 30)).unwrap();
        consolidated.mark_ready(utc(13, 31), artifact()).unwrap();
        consolidated.claim_send(utc(17, 59)).unwrap();

        let mut late = planned(utc(3, 0));
        assert_eq!(
            late.start_generation(utc(9, 1)),
            Err(OutboxError::DeadlineExceeded)
        );
    }

    #[test]
    fn malformed_schedule_cannot_enter_outbox() {
        let valid = due_deliveries(utc(13, 0), "pilot_owner", 1, &BTreeSet::new())
            .unwrap()
            .remove(0);
        let mut empty = valid.clone();
        empty.covered_keys.clear();
        assert_eq!(
            DeliveryRecord::planned(empty),
            Err(OutboxError::InvalidSchedule)
        );

        let mut mixed = valid;
        mixed.covered_keys[1].recipient_id = "other".to_owned();
        assert_eq!(
            DeliveryRecord::planned(mixed),
            Err(OutboxError::InvalidSchedule)
        );

        let valid = due_deliveries(utc(13, 0), "pilot_owner", 1, &BTreeSet::new())
            .unwrap()
            .remove(0);
        for malformed in [
            {
                let mut value = valid.clone();
                value.covered_keys.push(value.covered_keys[0].clone());
                value
            },
            {
                let mut value = valid.clone();
                value.covered_keys[1] = value.covered_keys[0].clone();
                value
            },
            {
                let mut value = valid.clone();
                value.covered_keys[1].local_date =
                    value.covered_keys[1].local_date.succ_opt().unwrap();
                value
            },
            {
                let mut value = valid;
                value.scheduled_for += chrono::Duration::minutes(1);
                value
            },
        ] {
            assert_eq!(
                DeliveryRecord::planned(malformed),
                Err(OutboxError::InvalidSchedule)
            );
        }
    }

    #[test]
    fn failure_recording_requires_an_active_send() {
        let mut record = ready();
        assert_eq!(
            record.record_failure(utc(3, 2), DeliveryErrorClass::Transport, Some(utc(3, 3))),
            Err(OutboxError::InvalidTransition)
        );
    }
}
