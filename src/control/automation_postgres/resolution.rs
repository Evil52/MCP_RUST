use super::{
    AuditEvent, ResolutionKind, WbAutomationCampaignLease, WbAutomationDurableActionStatus,
    WbAutomationPostgresError, WbAutomationStateTransitionReceipt, audit_event_key, ensure_one_row,
    insert_audit_event, load_action_in_transaction, load_locked_state_summary, status_database,
    to_i64, validate_digest, validate_error_class,
};

impl WbAutomationCampaignLease<'_> {
    /// Cancels only a live call that proved no HTTP request was dispatched.
    /// Never use this after a timeout, cancellation, crash or HTTP response.
    pub async fn cancel_not_sent(
        &mut self,
        idempotency_key: &str,
        expected_state_revision: u64,
    ) -> Result<WbAutomationStateTransitionReceipt, WbAutomationPostgresError> {
        self.resolve_without_write(
            idempotency_key,
            expected_state_revision,
            "write_not_sent",
            ResolutionKind::NotSent,
        )
        .await
    }

    pub(super) async fn resolve_without_write(
        &mut self,
        idempotency_key: &str,
        expected_state_revision: u64,
        error_class: &str,
        resolution: ResolutionKind,
    ) -> Result<WbAutomationStateTransitionReceipt, WbAutomationPostgresError> {
        validate_digest(idempotency_key)?;
        if !validate_error_class(error_class) {
            return Err(WbAutomationPostgresError::InvalidInput);
        }
        let target_status = resolution.status();
        let event_type = resolution.event_type();
        let expected_revision = to_i64(expected_state_revision)?;
        let client = self
            .client
            .as_mut()
            .ok_or(WbAutomationPostgresError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        let state =
            load_locked_state_summary(&transaction, &self.account_id, self.campaign_id).await?;
        let action = match load_action_in_transaction(
            &transaction,
            &self.account_id,
            self.campaign_id,
            idempotency_key,
        )
        .await
        {
            Ok(action) => action,
            Err(error) => return Err(error),
        };
        if action.status == target_status {
            let replay_revision = expected_revision
                .checked_add(1)
                .ok_or(WbAutomationPostgresError::InvalidInput)?;
            let replay_matches = action.last_error_class.as_deref() == Some(error_class)
                && state.revision == replay_revision
                && match resolution {
                    ResolutionKind::Cancelled | ResolutionKind::NotSent => {
                        state.pending_idempotency_key.is_none()
                    }
                    ResolutionKind::ReconciliationRequired => {
                        state.pending_idempotency_key.as_deref() == Some(idempotency_key)
                            && state.incident_class.as_deref() == Some(error_class)
                    }
                };
            if !replay_matches {
                return Err(WbAutomationPostgresError::StateChanged);
            }
            transaction
                .commit()
                .await
                .map_err(|_| WbAutomationPostgresError::Unavailable)?;
            return Ok(WbAutomationStateTransitionReceipt {
                changed: false,
                state_revision: u64::try_from(replay_revision)
                    .map_err(|_| WbAutomationPostgresError::Unavailable)?,
            });
        }
        if state.revision != expected_revision
            || state.pending_idempotency_key.as_deref() != Some(idempotency_key)
        {
            return Err(WbAutomationPostgresError::StateChanged);
        }
        let valid_source = match resolution {
            ResolutionKind::Cancelled => action.status == WbAutomationDurableActionStatus::Reserved,
            ResolutionKind::NotSent => [
                WbAutomationDurableActionStatus::Reserved,
                WbAutomationDurableActionStatus::WriteStarted,
            ]
            .contains(&action.status),
            ResolutionKind::ReconciliationRequired => [
                WbAutomationDurableActionStatus::WriteStarted,
                WbAutomationDurableActionStatus::AwaitingReadback,
            ]
            .contains(&action.status),
        };
        if !valid_source {
            return Err(WbAutomationPostgresError::StateChanged);
        }
        let updated = transaction
            .execute(
                "UPDATE wb_automation.action_attempts \
                 SET status=$4, last_error_class=$5 \
                 WHERE idempotency_key=$1 AND account_id=$2 AND advert_id=$3 AND status=$6",
                &[
                    &idempotency_key,
                    &self.account_id,
                    &self.campaign_id,
                    &status_database(target_status),
                    &error_class,
                    &status_database(action.status),
                ],
            )
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        ensure_one_row(updated)?;
        let new_revision = expected_revision
            .checked_add(1)
            .ok_or(WbAutomationPostgresError::InvalidInput)?;
        let state_updated = match resolution {
            ResolutionKind::Cancelled | ResolutionKind::NotSent => {
                transaction
                    .execute(
                        "UPDATE wb_automation.execution_state \
                         SET pending_idempotency_key=NULL, revision=$3 \
                         WHERE account_id=$1 AND advert_id=$2 AND revision=$4",
                        &[
                            &self.account_id,
                            &self.campaign_id,
                            &new_revision,
                            &expected_revision,
                        ],
                    )
                    .await
            }
            ResolutionKind::ReconciliationRequired => {
                transaction
                    .execute(
                        "UPDATE wb_automation.execution_state \
                         SET incident_class=$3, revision=$4 \
                         WHERE account_id=$1 AND advert_id=$2 AND revision=$5",
                        &[
                            &self.account_id,
                            &self.campaign_id,
                            &error_class,
                            &new_revision,
                            &expected_revision,
                        ],
                    )
                    .await
            }
        }
        .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        ensure_one_row(state_updated)?;
        let payload = serde_json::to_string(&serde_json::json!({
            "error_class": error_class,
        }))
        .map_err(|_| WbAutomationPostgresError::InvalidInput)?;
        insert_audit_event(
            &transaction,
            &AuditEvent {
                event_key: &audit_event_key(idempotency_key, event_type, error_class),
                cycle_id: &action.cycle_id,
                account_id: &self.account_id,
                campaign_id: self.campaign_id,
                event_type,
                idempotency_key: Some(idempotency_key),
                payload_json: &payload,
            },
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| WbAutomationPostgresError::Unavailable)?;
        Ok(WbAutomationStateTransitionReceipt {
            changed: true,
            state_revision: u64::try_from(new_revision)
                .map_err(|_| WbAutomationPostgresError::Unavailable)?,
        })
    }
}
