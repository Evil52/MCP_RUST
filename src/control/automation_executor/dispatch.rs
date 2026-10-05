use anyhow::Result;

use super::{
    PendingAction, PendingActionKind, PostgresWriteResult, WbAutomationCampaignLease,
    WbAutomationExecutor, WbBidPlacement, WbGuardedWriteError, WbPreparedBidChange,
    verify_pending_permit,
};

impl WbAutomationExecutor {
    pub(super) async fn send_pending(
        &self,
        pending: &PendingAction,
    ) -> Result<PostgresWriteResult> {
        let expected = pending.clone();
        let state_path = self.state_directory.join("execution-state.json");
        let permit = move || async move { verify_pending_permit(&state_path, &expected) };
        let result = match &pending.kind {
            PendingActionKind::ChangeBids { changes } => {
                let prepared = changes
                    .iter()
                    .map(|change| WbPreparedBidChange {
                        nm_id: change.nm_id,
                        placement: WbBidPlacement::Search,
                        before_bid_kopecks: change.from_bid_kopecks,
                        bid_kopecks: change.to_bid_kopecks,
                    })
                    .collect::<Vec<_>>();
                self.writer
                    .change_bids_with_permit(self.observer.policy().campaign_id, &prepared, permit)
                    .await
            }
            PendingActionKind::PauseCampaignForDailyCap => {
                self.writer
                    .pause_campaign_with_permit(self.observer.policy().campaign_id, permit)
                    .await
            }
            PendingActionKind::ResumeCampaignAfterDailyCap => {
                self.writer
                    .start_campaign_with_permit(self.observer.policy().campaign_id, permit)
                    .await
            }
        };
        match result {
            Ok(_) => Ok(PostgresWriteResult::Sent),
            Err(WbGuardedWriteError::Write(error))
                if error.outcome_kind()
                    == super::super::wb::WbWriteOutcomeKind::DefiniteFailure =>
            {
                Ok(PostgresWriteResult::NotSent)
            }
            Err(_) => Err(anyhow::anyhow!(
                "WB automation write требует readback reconciliation"
            )),
        }
    }

    pub(super) async fn send_pending_postgres(
        &self,
        pending: &PendingAction,
        lease: &mut WbAutomationCampaignLease<'_>,
        idempotency_key: &str,
        state_revision: u64,
    ) -> Result<
        (),
        WbGuardedWriteError<crate::control::automation_postgres::WbAutomationPostgresError>,
    > {
        let permit = move || async move {
            lease
                .mark_write_started(idempotency_key, state_revision)
                .await
                .map(|_| ())
        };
        match &pending.kind {
            PendingActionKind::ChangeBids { changes } => {
                let prepared = changes
                    .iter()
                    .map(|change| WbPreparedBidChange {
                        nm_id: change.nm_id,
                        placement: WbBidPlacement::Search,
                        before_bid_kopecks: change.from_bid_kopecks,
                        bid_kopecks: change.to_bid_kopecks,
                    })
                    .collect::<Vec<_>>();
                self.writer
                    .change_bids_with_permit(self.observer.policy().campaign_id, &prepared, permit)
                    .await
                    .map(|_| ())
            }
            PendingActionKind::PauseCampaignForDailyCap => self
                .writer
                .pause_campaign_with_permit(self.observer.policy().campaign_id, permit)
                .await
                .map(|_| ()),
            PendingActionKind::ResumeCampaignAfterDailyCap => self
                .writer
                .start_campaign_with_permit(self.observer.policy().campaign_id, permit)
                .await
                .map(|_| ()),
        }
    }
}
