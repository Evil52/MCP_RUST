//! Operator closure of ambiguous `sending` attempts.

use chrono::{DateTime, Utc};

use super::{
    MAX_DELIVERY_ATTEMPTS, PostgresOutboxError, PostgresOutboxRepository, ReconciliationDecision,
    ReconciliationOutcome, exactly_one, validate_identity, validate_provider_message_id,
};

impl PostgresOutboxRepository {
    /// Closes one ambiguous `sending` attempt after an operator checks Gmail.
    ///
    /// `ConfirmedSent` requires the provider message ID. `SuppressedUnknown`
    /// permanently blocks resend when the provider outcome cannot be proven.
    /// The append-only reconciliation row and terminal batch transition commit
    /// together. Repeating the exact decision is idempotent; changing it is a
    /// conflict.
    pub async fn reconcile_sending(
        &self,
        batch_id: i64,
        attempt_no: u8,
        recipient_id: &str,
        report_version: u32,
        reconciled_at: DateTime<Utc>,
        decision: &ReconciliationDecision,
    ) -> Result<ReconciliationOutcome, PostgresOutboxError> {
        validate_identity(recipient_id, report_version)
            .map_err(|_| PostgresOutboxError::InvalidDelivery)?;
        if batch_id <= 0 || !(1..=MAX_DELIVERY_ATTEMPTS).contains(&attempt_no) {
            return Err(PostgresOutboxError::InvalidDelivery);
        }
        let report_version =
            i32::try_from(report_version).map_err(|_| PostgresOutboxError::InvalidDelivery)?;
        let (decision_text, provider_message_id, terminal_status, error_class, sent_at) =
            match decision {
                ReconciliationDecision::ConfirmedSent {
                    provider_message_id,
                } => {
                    validate_provider_message_id(provider_message_id)
                        .map_err(|_| PostgresOutboxError::InvalidDelivery)?;
                    (
                        "confirmed_sent",
                        Some(provider_message_id.as_str()),
                        "sent",
                        None,
                        Some(reconciled_at),
                    )
                }
                ReconciliationDecision::SuppressedUnknown => (
                    "suppressed_unknown",
                    None,
                    "permanent_failure",
                    Some("operator_reconciled_unknown"),
                    None,
                ),
            };
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let batch = transaction
            .query_opt(
                "SELECT status, attempts, provider_message_id, last_error_class \
                 FROM daily_reporting.delivery_batches \
                 WHERE id = $1 AND recipient_id = $2 AND report_version = $3 FOR UPDATE",
                &[&batch_id, &recipient_id, &report_version],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?
            .ok_or(PostgresOutboxError::Conflict)?;
        let existing = transaction
            .query_opt(
                "SELECT decision, provider_message_id \
                 FROM daily_reporting.delivery_reconciliations \
                 WHERE batch_id = $1 AND attempt_no = $2",
                &[&batch_id, &i16::from(attempt_no)],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        if let Some(existing) = existing {
            let exact_reconciliation = existing.get::<_, &str>(0) == decision_text
                && existing.get::<_, Option<&str>>(1) == provider_message_id;
            let exact_terminal = batch.get::<_, &str>(0) == terminal_status
                && batch.get::<_, Option<&str>>(2) == provider_message_id
                && batch.get::<_, Option<&str>>(3) == error_class;
            if exact_reconciliation && exact_terminal {
                transaction
                    .commit()
                    .await
                    .map_err(|_| PostgresOutboxError::Unavailable)?;
                return Ok(ReconciliationOutcome::Existing);
            }
            return Err(PostgresOutboxError::Conflict);
        }
        if batch.get::<_, &str>(0) != "sending" || batch.get::<_, i16>(1) != i16::from(attempt_no) {
            return Err(PostgresOutboxError::Conflict);
        }
        transaction
            .execute(
                "INSERT INTO daily_reporting.delivery_reconciliations ( \
                    batch_id, attempt_no, reconciled_at, decision, provider_message_id \
                 ) VALUES ($1, $2, $3, $4, $5)",
                &[
                    &batch_id,
                    &i16::from(attempt_no),
                    &reconciled_at,
                    &decision_text,
                    &provider_message_id,
                ],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        let changed = transaction
            .execute(
                "UPDATE daily_reporting.delivery_batches \
                 SET status = $2, next_attempt_at = NULL, provider_message_id = $3, \
                     last_error_class = $4, sent_at = $5, \
                     updated_at = greatest(clock_timestamp(), updated_at + interval '1 microsecond') \
                 WHERE id = $1 AND status = 'sending' AND attempts = $6",
                &[
                    &batch_id,
                    &terminal_status,
                    &provider_message_id,
                    &error_class,
                    &sent_at,
                    &i16::from(attempt_no),
                ],
            )
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        exactly_one(changed)?;
        transaction
            .commit()
            .await
            .map_err(|_| PostgresOutboxError::Unavailable)?;
        Ok(ReconciliationOutcome::Applied)
    }
}
