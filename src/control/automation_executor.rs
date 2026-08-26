use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    config::{Marketplace, RegistrySource},
    control::policy::WbBidPlacement,
};

use super::{
    automation::{
        WbAutomationAction, WbAutomationBidChange, WbAutomationDecision,
        wb_automation_business_date,
    },
    automation_observer::{
        WbAutomationObserver, WbAutomationSnapshot, WbAutomationStateView,
        persist_wb_automation_snapshot,
    },
    automation_postgres::{
        WbAutomationActionReservation, WbAutomationCampaignLease, WbAutomationDurableAction,
        WbAutomationDurableActionKind, WbAutomationDurableActionStatus,
        WbAutomationLegacyStateSeed, WbAutomationPostgresStore,
    },
    config::{read_control_token, validate_wb_writer_token},
    wb::{WbBidWriteClient, WbGuardedWriteError, WbPreparedBidChange},
};

const STATE_SCHEMA_VERSION: u32 = 1;
const MAX_STATE_BYTES: u64 = 256 * 1024;
const READBACK_GRACE: ChronoDuration = ChronoDuration::minutes(5);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WbAutomationExecutionOutcome {
    Observed,
    ReservationCancelled,
    WriteSentReconciliationRequired,
    AwaitingReadback,
    Reconciled,
    IncidentLocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WbAutomationExecutionReceipt {
    pub observed_at: DateTime<Utc>,
    pub decision: WbAutomationDecision,
    pub outcome: WbAutomationExecutionOutcome,
    pub snapshot_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WbAutomationPostgresExecutionReceipt {
    pub observed_at: DateTime<Utc>,
    pub decision: WbAutomationDecision,
    pub outcome: WbAutomationExecutionOutcome,
    pub cycle_id: String,
    pub cycle_inserted: bool,
    pub legacy_imported: bool,
    pub state_revision: u64,
}

pub struct WbAutomationExecutor {
    observer: WbAutomationObserver,
    writer: WbBidWriteClient,
    state_directory: PathBuf,
}

impl std::fmt::Debug for WbAutomationExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WbAutomationExecutor")
            .field("observer", &self.observer)
            .field("writer", &self.writer)
            .field("state_directory", &self.state_directory)
            .finish()
    }
}

impl WbAutomationExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn from_files(
        policy_path: &Path,
        registry_path: &Path,
        reader_token_path: &Path,
        writer_token_path: &Path,
        state_directory: &Path,
        allow_broad_reader: bool,
        timeout: Duration,
        reader_proxy_url: Option<&str>,
        writer_proxy_url: &str,
    ) -> Result<Self> {
        ensure!(
            !timeout.is_zero() && timeout <= Duration::from_secs(30),
            "WB automation executor timeout должен быть от 1 до 30 секунд"
        );
        validate_private_directory(state_directory)?;
        let observer = WbAutomationObserver::from_files(
            policy_path,
            registry_path,
            reader_token_path,
            allow_broad_reader,
            timeout,
            reader_proxy_url,
        )?;
        let registry = RegistrySource::new(registry_path)
            .context("WB automation executor registry path неверен")?
            .load()
            .context("WB automation executor registry недоступен")?;
        let account = registry
            .accounts
            .iter()
            .find(|account| account.id == observer.policy().account_id)
            .context("WB automation executor account отсутствует")?;
        ensure!(
            account.marketplace == Marketplace::Wildberries,
            "WB automation executor account должен быть Wildberries"
        );
        let seller_sid = account
            .wildberries
            .as_ref()
            .and_then(|binding| binding.seller_sid.as_deref())
            .context("WB automation executor требует reviewed seller_sid")?;
        let writer_token = read_control_token(writer_token_path, "WB_AUTOMATION_WRITE_TOKEN_FILE")?;
        validate_wb_writer_token(&writer_token, seller_sid)?;
        let writer = WbBidWriteClient::new(timeout, &writer_token, writer_proxy_url)?;
        Ok(Self {
            observer,
            writer,
            state_directory: state_directory.to_owned(),
        })
    }

    pub async fn run_once(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<WbAutomationExecutionReceipt> {
        let business_date = wb_automation_business_date(observed_at);
        let mut state = load_execution_state(
            &self.state_directory,
            self.observer.policy_sha256(),
            self.observer.policy().account_id.as_str(),
            self.observer.policy().campaign_id,
            business_date,
            !self.observer.policy().write_enabled,
        )?;
        if state.business_date != business_date {
            state.business_date = business_date;
            state.actions_today = 0;
        }
        let state_view = WbAutomationStateView {
            paused_by_automation: state
                .paused_for_daily_cap_on
                .is_some_and(|paused_on| paused_on < business_date),
            actions_today: state.actions_today,
            last_action_at: state.last_action_at,
        };
        let snapshot = self.observer.observe(observed_at, state_view).await?;
        let snapshot_path = persist_wb_automation_snapshot(&self.state_directory, &snapshot)?;
        let decision = snapshot.decision.clone();

        if state.incident_class.is_some() {
            save_execution_state(&self.state_directory, &state)?;
            return Ok(receipt(
                snapshot_path,
                decision,
                WbAutomationExecutionOutcome::IncidentLocked,
            ));
        }
        if let Some(pending) = state.pending.clone() {
            let outcome = reconcile_pending(&snapshot.observation, &mut state, &pending);
            save_execution_state(&self.state_directory, &state)?;
            return Ok(receipt(snapshot_path, decision, outcome));
        }
        // `daily_pause_threshold_minor` is validated to sit at or below
        // `daily_spend_cap_minor`, so an unpaused run that has already reached
        // the cap means the soft pause did not hold: it was never sent, WB did
        // not apply it, or spend outran the observe-write-reconcile cycle.
        // Nothing is in flight to correct it at this point, so stop automating
        // and leave the campaign for an operator rather than issuing further
        // spend-affecting writes.
        if snapshot.observation.daily_spend_minor >= self.observer.policy().daily_spend_cap_minor {
            state.incident_class = Some("daily_spend_cap_breached".to_owned());
            save_execution_state(&self.state_directory, &state)?;
            return Ok(receipt(
                snapshot_path,
                decision,
                WbAutomationExecutionOutcome::IncidentLocked,
            ));
        }

        let Some(pending) = pending_from_decision(
            &decision.action,
            &snapshot.observation,
            self.observer.policy().min_bid_kopecks,
            observed_at,
        ) else {
            save_execution_state(&self.state_directory, &state)?;
            return Ok(receipt(
                snapshot_path,
                decision,
                WbAutomationExecutionOutcome::Observed,
            ));
        };
        state.actions_today = state
            .actions_today
            .checked_add(1)
            .context("WB automation action counter overflow")?;
        state.last_action_at = Some(observed_at);
        state.pending = Some(pending.clone());
        save_execution_state(&self.state_directory, &state)?;
        self.send_pending(&pending).await?;
        Ok(receipt(
            snapshot_path,
            decision,
            WbAutomationExecutionOutcome::WriteSentReconciliationRequired,
        ))
    }

    #[must_use]
    pub const fn policy(&self) -> &super::automation::WbAutomationPolicy {
        self.observer.policy()
    }

    #[must_use]
    pub fn policy_sha256(&self) -> &str {
        self.observer.policy_sha256()
    }

    /// Executes one write-capable cycle using PostgreSQL as the only mutable
    /// source of truth. `None` means the campaign advisory lock is owned by a
    /// different runtime or operator and no observation or write was attempted.
    pub async fn run_once_postgres(
        &self,
        store: &WbAutomationPostgresStore,
        legacy: &WbAutomationLegacyStateSeed,
        observed_at: DateTime<Utc>,
    ) -> Result<Option<WbAutomationPostgresExecutionReceipt>> {
        self.run_once_postgres_with_intent(
            store,
            legacy,
            observed_at,
            PostgresExecutionIntent::Automatic,
        )
        .await
    }

    /// Executes exactly one explicitly requested low-exposure increase through
    /// the same durable reservation, final permit and read-back protocol as an
    /// automatic action. It does not make aggregate campaign metrics look like
    /// per-SKU attribution and therefore cannot weaken later scheduled cycles.
    pub async fn run_explicit_exposure_increase_once_postgres(
        &self,
        store: &WbAutomationPostgresStore,
        legacy: &WbAutomationLegacyStateSeed,
        observed_at: DateTime<Utc>,
        target_impressions: u64,
    ) -> Result<Option<WbAutomationPostgresExecutionReceipt>> {
        self.run_once_postgres_with_intent(
            store,
            legacy,
            observed_at,
            PostgresExecutionIntent::ExplicitExposureTarget(target_impressions),
        )
        .await
    }

    async fn run_once_postgres_with_intent(
        &self,
        store: &WbAutomationPostgresStore,
        legacy: &WbAutomationLegacyStateSeed,
        observed_at: DateTime<Utc>,
        intent: PostgresExecutionIntent,
    ) -> Result<Option<WbAutomationPostgresExecutionReceipt>> {
        let policy = self.observer.policy();
        ensure!(
            policy.write_enabled,
            "PostgreSQL executor refuses a shadow-only policy"
        );
        let Some(mut lease) = store
            .try_acquire_campaign(&policy.account_id, policy.campaign_id)
            .await?
        else {
            return Ok(None);
        };
        let existing = lease.load_state().await?;
        let legacy_imported = if let Some(existing) = existing.as_ref() {
            ensure!(
                existing.policy_digest == self.observer.policy_sha256()
                    && existing.imported_legacy_digest.as_deref()
                        == Some(legacy.legacy_digest.as_str()),
                "WB automation PostgreSQL state does not match policy or legacy seed"
            );
            false
        } else {
            lease.initialize_from_legacy(legacy).await?
        };
        let state = match existing {
            Some(state) => state,
            None => lease
                .load_state()
                .await?
                .context("WB automation PostgreSQL state is unavailable")?,
        };
        let business_date = wb_automation_business_date(observed_at);
        if matches!(intent, PostgresExecutionIntent::ExplicitExposureTarget(_)) {
            ensure!(
                state.pending_idempotency_key.is_none() && state.incident_class.is_none(),
                "WB explicit exposure increase requires clean durable state"
            );
        }
        let state_view = WbAutomationStateView {
            paused_by_automation: state
                .paused_for_daily_cap_on
                .is_some_and(|paused_on| paused_on < business_date),
            actions_today: if state.business_date == business_date {
                state.actions_today
            } else {
                0
            },
            last_action_at: state.last_action_at,
        };
        let mut snapshot = self.observer.observe(observed_at, state_view).await?;
        match intent {
            PostgresExecutionIntent::ExplicitExposureTarget(target_impressions) => {
                snapshot.decision =
                    explicit_exposure_increase_decision(policy, &snapshot, target_impressions)?;
            }
            PostgresExecutionIntent::Automatic if policy.autonomous_pacing.is_enabled() => {
                if let Some(decision) = autonomous_exposure_pacing_decision(policy, &snapshot)? {
                    snapshot.decision = decision;
                }
            }
            PostgresExecutionIntent::Automatic => {}
        }
        let snapshot_json = serde_json::to_string(&snapshot)?;
        let decision_json = serde_json::to_string(&snapshot.decision)?;
        let cycle_id = sha256_domain("wb-automation-cycle-v1", snapshot_json.as_bytes());
        let cycle_inserted = lease
            .persist_shadow_cycle(
                &cycle_id,
                self.observer.policy_sha256(),
                snapshot.observation.observed_at,
                business_date,
                state.revision,
                &snapshot_json,
                &decision_json,
            )
            .await?;
        let decision = snapshot.decision.clone();

        if let Some(idempotency_key) = state.pending_idempotency_key.as_deref() {
            let action = lease
                .load_pending_action(idempotency_key, state.revision)
                .await?;
            let (outcome, state_revision) = reconcile_postgres_pending(
                &snapshot.observation,
                &mut lease,
                &action,
                state.revision,
                &cycle_id,
            )
            .await?;
            lease.release().await?;
            return Ok(Some(postgres_receipt(
                decision,
                outcome,
                cycle_id,
                cycle_inserted,
                legacy_imported,
                state_revision,
            )));
        }
        if state.incident_class.is_some() {
            lease.release().await?;
            return Ok(Some(postgres_receipt(
                decision,
                WbAutomationExecutionOutcome::IncidentLocked,
                cycle_id,
                cycle_inserted,
                legacy_imported,
                state.revision,
            )));
        }
        if snapshot.observation.daily_spend_minor >= policy.daily_spend_cap_minor {
            let transition = lease
                .mark_incident_without_action(
                    &cycle_id,
                    state.revision,
                    business_date,
                    "daily_spend_cap_breached",
                )
                .await?;
            lease.release().await?;
            return Ok(Some(postgres_receipt(
                decision,
                WbAutomationExecutionOutcome::IncidentLocked,
                cycle_id,
                cycle_inserted,
                legacy_imported,
                transition.state_revision,
            )));
        }

        let Some(pending) = pending_from_decision(
            &decision.action,
            &snapshot.observation,
            policy.min_bid_kopecks,
            observed_at,
        ) else {
            lease.release().await?;
            return Ok(Some(postgres_receipt(
                decision,
                WbAutomationExecutionOutcome::Observed,
                cycle_id,
                cycle_inserted,
                legacy_imported,
                state.revision,
            )));
        };
        let request_json = serde_json::to_string(&pending.kind)?;
        let request_digest = sha256_domain("wb-automation-request-v1", request_json.as_bytes());
        let idempotency_material = format!("{cycle_id}:{request_digest}");
        let idempotency_key =
            sha256_domain("wb-automation-action-v1", idempotency_material.as_bytes());
        let reservation = WbAutomationActionReservation {
            idempotency_key: idempotency_key.clone(),
            cycle_id: cycle_id.clone(),
            policy_digest: self.observer.policy_sha256().to_owned(),
            request_digest,
            action_kind: durable_action_kind(&pending.kind)
                .context("WB automation PostgreSQL action kind is unsupported")?,
            request_json,
            business_date,
            expected_state_revision: state.revision,
            max_actions_per_day: policy.max_actions_per_day,
        };
        let reservation = lease.reserve_action(&reservation).await?;
        let write_result = self
            .send_pending_postgres(
                &pending,
                &mut lease,
                &idempotency_key,
                reservation.state_revision,
            )
            .await;
        let (outcome, state_revision) = match classify_postgres_write(&write_result)? {
            PostgresWriteResult::Sent => {
                lease
                    .mark_awaiting_readback(&idempotency_key, reservation.state_revision)
                    .await?;
                (
                    WbAutomationExecutionOutcome::WriteSentReconciliationRequired,
                    reservation.state_revision,
                )
            }
            PostgresWriteResult::Ambiguous => {
                let transition = lease
                    .mark_reconciliation_required(
                        &idempotency_key,
                        reservation.state_revision,
                        "write_result_ambiguous",
                    )
                    .await?;
                (
                    WbAutomationExecutionOutcome::WriteSentReconciliationRequired,
                    transition.state_revision,
                )
            }
        };
        lease.release().await?;
        Ok(Some(postgres_receipt(
            decision,
            outcome,
            cycle_id,
            cycle_inserted,
            legacy_imported,
            state_revision,
        )))
    }

    async fn send_pending(&self, pending: &PendingAction) -> Result<()> {
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
        result
            .map(|_| ())
            .map_err(|_| anyhow::anyhow!("WB automation write требует readback reconciliation"))
    }

    async fn send_pending_postgres(
        &self,
        pending: &PendingAction,
        lease: &mut WbAutomationCampaignLease<'_>,
        idempotency_key: &str,
        state_revision: u64,
    ) -> Result<(), WbGuardedWriteError<super::automation_postgres::WbAutomationPostgresError>>
    {
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
            PendingActionKind::ResumeCampaignAfterDailyCap => Err(WbGuardedWriteError::Permit(
                super::automation_postgres::WbAutomationPostgresError::InvalidInput,
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostgresExecutionIntent {
    Automatic,
    ExplicitExposureTarget(u64),
}

fn explicit_exposure_increase_decision(
    policy: &super::automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
    target_impressions: u64,
) -> Result<WbAutomationDecision> {
    ensure!(
        target_impressions == policy.target_impressions_per_day,
        "WB explicit exposure target does not match policy"
    );
    ensure!(
        matches!(
            snapshot.decision.action,
            WbAutomationAction::Hold {
                reason: super::automation::WbAutomationHoldReason::AttributionIncomplete
            }
        ),
        "WB explicit exposure increase is blocked by a stronger decision guard"
    );
    let metrics = snapshot
        .observation
        .campaign_level_metrics
        .as_ref()
        .context("WB explicit exposure increase requires campaign delivery metrics")?;
    ensure!(
        metrics.impressions < target_impressions
            && metrics.impressions < policy.low_exposure_max_impressions
            && metrics.clicks < policy.low_exposure_max_clicks,
        "WB explicit exposure increase requires verified low exposure"
    );
    let sku = policy
        .nm_ids
        .iter()
        .filter_map(|nm_id| {
            snapshot
                .observation
                .skus
                .iter()
                .find(|sku| sku.nm_id == *nm_id)
        })
        .filter(|sku| {
            sku.sellable_stock > policy.min_sellable_stock
                && sku.current_bid_kopecks < policy.max_bid_kopecks
        })
        .min_by_key(|sku| sku.current_bid_kopecks)
        .context("WB explicit exposure increase has no safe SKU candidate")?;
    let to_bid_kopecks = super::automation::increase_bid(policy, sku.current_bid_kopecks)
        .context("WB explicit exposure bid increase is invalid")?;
    ensure!(
        to_bid_kopecks > sku.current_bid_kopecks,
        "WB explicit exposure bid cannot increase"
    );
    Ok(WbAutomationDecision {
        account_id: policy.account_id.clone(),
        campaign_id: policy.campaign_id,
        observed_at: snapshot.observation.observed_at,
        action: WbAutomationAction::ChangeBids {
            changes: vec![WbAutomationBidChange {
                nm_id: sku.nm_id,
                from_bid_kopecks: sku.current_bid_kopecks,
                to_bid_kopecks,
                reason: super::automation::WbAutomationBidReason::ExplicitExposureTarget,
            }],
        },
        unresolved_stops: Vec::new(),
    })
}

/// Turns an otherwise fail-closed campaign-level attribution hold into one
/// bounded pacing step. Aggregate metrics may authorize more campaign
/// exposure, but never pretend to identify SKU economics: the least-bid safe
/// in-stock SKU is selected deterministically, one at a time.
fn autonomous_exposure_pacing_decision(
    policy: &super::automation::WbAutomationPolicy,
    snapshot: &WbAutomationSnapshot,
) -> Result<Option<WbAutomationDecision>> {
    if !matches!(
        snapshot.decision.action,
        WbAutomationAction::Hold {
            reason: super::automation::WbAutomationHoldReason::AttributionIncomplete
        }
    ) {
        return Ok(None);
    }
    let Some(metrics) = snapshot.observation.campaign_level_metrics.as_ref() else {
        return Ok(None);
    };
    let delivery_below_target = metrics.impressions < policy.target_impressions_per_day;
    let no_order_signal_is_safe = if metrics.attributed_orders == 0 {
        metrics.attributed_revenue_minor == 0 && metrics.clicks < policy.no_order_reduce_clicks
    } else {
        metrics.attributed_revenue_minor > 0
            && u128::from(metrics.spend_minor) * 10_000
                <= u128::from(metrics.attributed_revenue_minor)
                    * u128::from(policy.target_drr_basis_points)
    };
    if !delivery_below_target || !no_order_signal_is_safe {
        return Ok(None);
    }
    let Some(sku) = policy
        .nm_ids
        .iter()
        .filter_map(|nm_id| {
            snapshot
                .observation
                .skus
                .iter()
                .find(|sku| sku.nm_id == *nm_id)
        })
        .filter(|sku| {
            sku.sellable_stock > policy.min_sellable_stock
                && sku.current_bid_kopecks < policy.max_bid_kopecks
        })
        .min_by_key(|sku| sku.current_bid_kopecks)
    else {
        return Ok(None);
    };
    let to_bid_kopecks = super::automation::increase_bid(policy, sku.current_bid_kopecks)
        .context("WB autonomous exposure pacing bid increase is invalid")?;
    ensure!(
        to_bid_kopecks > sku.current_bid_kopecks,
        "WB autonomous exposure pacing bid cannot increase"
    );
    Ok(Some(WbAutomationDecision {
        account_id: policy.account_id.clone(),
        campaign_id: policy.campaign_id,
        observed_at: snapshot.observation.observed_at,
        action: WbAutomationAction::ChangeBids {
            changes: vec![WbAutomationBidChange {
                nm_id: sku.nm_id,
                from_bid_kopecks: sku.current_bid_kopecks,
                to_bid_kopecks,
                reason: super::automation::WbAutomationBidReason::AutonomousExposurePacing,
            }],
        },
        unresolved_stops: Vec::new(),
    }))
}

const fn receipt(
    snapshot_path: PathBuf,
    decision: WbAutomationDecision,
    outcome: WbAutomationExecutionOutcome,
) -> WbAutomationExecutionReceipt {
    WbAutomationExecutionReceipt {
        observed_at: decision.observed_at,
        decision,
        outcome,
        snapshot_path,
    }
}

const fn postgres_receipt(
    decision: WbAutomationDecision,
    outcome: WbAutomationExecutionOutcome,
    cycle_id: String,
    cycle_inserted: bool,
    legacy_imported: bool,
    state_revision: u64,
) -> WbAutomationPostgresExecutionReceipt {
    WbAutomationPostgresExecutionReceipt {
        observed_at: decision.observed_at,
        decision,
        outcome,
        cycle_id,
        cycle_inserted,
        legacy_imported,
        state_revision,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostgresWriteResult {
    Sent,
    Ambiguous,
}

fn classify_postgres_write(
    result: &Result<(), WbGuardedWriteError<super::automation_postgres::WbAutomationPostgresError>>,
) -> Result<PostgresWriteResult> {
    match result {
        Ok(()) => Ok(PostgresWriteResult::Sent),
        Err(WbGuardedWriteError::Permit(error)) => Err(anyhow::Error::new(*error)
            .context("WB automation final PostgreSQL permit is unavailable")),
        Err(WbGuardedWriteError::Write(_)) => Ok(PostgresWriteResult::Ambiguous),
    }
}

async fn reconcile_postgres_pending(
    observation: &super::automation::WbAutomationObservation,
    lease: &mut WbAutomationCampaignLease<'_>,
    action: &WbAutomationDurableAction,
    state_revision: u64,
    readback_cycle_id: &str,
) -> Result<(WbAutomationExecutionOutcome, u64)> {
    let pending = pending_from_durable_action(action)?;
    if action.status == WbAutomationDurableActionStatus::Reserved {
        let transition = lease
            .cancel_reserved(&action.idempotency_key, state_revision, "write_not_started")
            .await?;
        return Ok((
            WbAutomationExecutionOutcome::ReservationCancelled,
            transition.state_revision,
        ));
    }
    ensure!(
        !matches!(
            action.status,
            WbAutomationDurableActionStatus::Applied | WbAutomationDurableActionStatus::Cancelled
        ),
        "WB automation PostgreSQL pending action is already resolved"
    );
    if pending_is_visible(observation, &pending) {
        let paused_on = durable_pause_date(&pending)?;
        let transition = lease
            .mark_applied(
                &action.idempotency_key,
                state_revision,
                readback_cycle_id,
                paused_on,
            )
            .await?;
        return Ok((
            WbAutomationExecutionOutcome::Reconciled,
            transition.state_revision,
        ));
    }
    if action.status == WbAutomationDurableActionStatus::ReconciliationRequired {
        return Ok((WbAutomationExecutionOutcome::IncidentLocked, state_revision));
    }
    let write_started_at = action
        .write_started_at
        .context("WB automation PostgreSQL write timestamp is unavailable")?;
    if observation.observed_at < write_started_at + READBACK_GRACE {
        if action.status == WbAutomationDurableActionStatus::WriteStarted {
            lease
                .mark_awaiting_readback(&action.idempotency_key, state_revision)
                .await?;
        }
        return Ok((
            WbAutomationExecutionOutcome::AwaitingReadback,
            state_revision,
        ));
    }
    let transition = lease
        .mark_reconciliation_required(
            &action.idempotency_key,
            state_revision,
            "write_not_reconciled",
        )
        .await?;
    Ok((
        WbAutomationExecutionOutcome::IncidentLocked,
        transition.state_revision,
    ))
}

fn pending_from_durable_action(action: &WbAutomationDurableAction) -> Result<PendingAction> {
    let kind = serde_json::from_str::<PendingActionKind>(&action.request_json)
        .context("WB automation PostgreSQL action payload is invalid")?;
    ensure!(
        durable_action_kind(&kind) == Some(action.action_kind),
        "WB automation PostgreSQL action payload does not match its kind"
    );
    Ok(PendingAction {
        reserved_at: action.reserved_at,
        kind,
    })
}

fn pending_is_visible(
    observation: &super::automation::WbAutomationObservation,
    pending: &PendingAction,
) -> bool {
    match &pending.kind {
        PendingActionKind::ChangeBids { changes } => changes
            .iter()
            .all(|change| bid_change_is_visible(observation, change)),
        PendingActionKind::PauseCampaignForDailyCap => observation.campaign_status == 11,
        PendingActionKind::ResumeCampaignAfterDailyCap => observation.campaign_status == 9,
    }
}

fn durable_pause_date(pending: &PendingAction) -> Result<Option<NaiveDate>> {
    match pending.kind {
        PendingActionKind::PauseCampaignForDailyCap => {
            Ok(Some(wb_automation_business_date(pending.reserved_at)))
        }
        PendingActionKind::ChangeBids { .. } => Ok(None),
        PendingActionKind::ResumeCampaignAfterDailyCap => {
            anyhow::bail!("PostgreSQL durable actions never contain automatic resume")
        }
    }
}

const fn durable_action_kind(kind: &PendingActionKind) -> Option<WbAutomationDurableActionKind> {
    match kind {
        PendingActionKind::ChangeBids { .. } => Some(WbAutomationDurableActionKind::ChangeBids),
        PendingActionKind::PauseCampaignForDailyCap => {
            Some(WbAutomationDurableActionKind::PauseCampaignForDailyCap)
        }
        PendingActionKind::ResumeCampaignAfterDailyCap => None,
    }
}

fn sha256_domain(domain: &str, bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain.as_bytes());
    digest.update([0]);
    digest.update(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest.finalize() {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionState {
    schema_version: u32,
    policy_sha256: String,
    account_id: String,
    campaign_id: u64,
    business_date: NaiveDate,
    actions_today: u32,
    last_action_at: Option<DateTime<Utc>>,
    paused_for_daily_cap_on: Option<NaiveDate>,
    pending: Option<PendingAction>,
    incident_class: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingAction {
    reserved_at: DateTime<Utc>,
    kind: PendingActionKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PendingActionKind {
    ChangeBids { changes: Vec<WbAutomationBidChange> },
    PauseCampaignForDailyCap,
    ResumeCampaignAfterDailyCap,
}

fn pending_from_decision(
    action: &WbAutomationAction,
    observation: &super::automation::WbAutomationObservation,
    min_bid_kopecks: u64,
    reserved_at: DateTime<Utc>,
) -> Option<PendingAction> {
    let kind = match action {
        WbAutomationAction::Hold { .. } => return None,
        WbAutomationAction::ChangeBids { changes } => {
            if changes.len() != 1 {
                return None;
            }
            PendingActionKind::ChangeBids {
                changes: changes.clone(),
            }
        }
        WbAutomationAction::DisableSku { nm_id, reason } => {
            let current = observation
                .skus
                .iter()
                .find(|sku| sku.nm_id == *nm_id)?
                .current_bid_kopecks;
            if current == min_bid_kopecks {
                return None;
            }
            PendingActionKind::ChangeBids {
                changes: vec![WbAutomationBidChange {
                    nm_id: *nm_id,
                    from_bid_kopecks: current,
                    to_bid_kopecks: min_bid_kopecks,
                    reason: match reason {
                        super::automation::WbAutomationDisableReason::LowStock => {
                            super::automation::WbAutomationBidReason::LowStockGuard
                        }
                        super::automation::WbAutomationDisableReason::NoOrdersHardStop => {
                            super::automation::WbAutomationBidReason::NoOrdersHardStop
                        }
                        super::automation::WbAutomationDisableReason::HardDrrExceeded => {
                            super::automation::WbAutomationBidReason::HardDrrExceeded
                        }
                    },
                }],
            }
        }
        WbAutomationAction::PauseCampaignForDailyCap => PendingActionKind::PauseCampaignForDailyCap,
        WbAutomationAction::ResumeCampaignAfterDailyCap => {
            PendingActionKind::ResumeCampaignAfterDailyCap
        }
    };
    Some(PendingAction { reserved_at, kind })
}

fn reconcile_pending(
    observation: &super::automation::WbAutomationObservation,
    state: &mut ExecutionState,
    pending: &PendingAction,
) -> WbAutomationExecutionOutcome {
    let applied = match &pending.kind {
        PendingActionKind::ChangeBids { changes } => changes
            .iter()
            .all(|change| bid_change_is_visible(observation, change)),
        PendingActionKind::PauseCampaignForDailyCap => observation.campaign_status == 11,
        PendingActionKind::ResumeCampaignAfterDailyCap => observation.campaign_status == 9,
    };
    if applied {
        match pending.kind {
            PendingActionKind::PauseCampaignForDailyCap => {
                // The pause belongs to the business date it was reserved on.
                // A reconciliation that lands after the Yekaterinburg rollover
                // sees the next business date, and recording that instead would
                // keep `paused_by_automation` false for the whole new day and
                // hold the campaign paused one day longer than the cap requires.
                state.paused_for_daily_cap_on =
                    Some(wb_automation_business_date(pending.reserved_at));
            }
            PendingActionKind::ResumeCampaignAfterDailyCap => {
                state.paused_for_daily_cap_on = None;
            }
            PendingActionKind::ChangeBids { .. } => {}
        }
        state.pending = None;
        return WbAutomationExecutionOutcome::Reconciled;
    }
    if observation.observed_at < pending.reserved_at + READBACK_GRACE {
        return WbAutomationExecutionOutcome::AwaitingReadback;
    }
    state.incident_class = Some("write_not_reconciled".to_owned());
    WbAutomationExecutionOutcome::IncidentLocked
}

fn bid_change_is_visible(
    observation: &super::automation::WbAutomationObservation,
    change: &WbAutomationBidChange,
) -> bool {
    observation
        .skus
        .iter()
        .find(|sku| sku.nm_id == change.nm_id)
        .is_some_and(|sku| sku.current_bid_kopecks == change.to_bid_kopecks)
}

fn verify_pending_permit(path: &Path, expected: &PendingAction) -> Result<()> {
    let state = read_state_file(path)?.context("WB automation execution state исчез")?;
    ensure!(
        state.incident_class.is_none() && state.pending.as_ref() == Some(expected),
        "WB automation final permit отозван"
    );
    Ok(())
}

fn load_execution_state(
    directory: &Path,
    policy_sha256: &str,
    account_id: &str,
    campaign_id: u64,
    business_date: NaiveDate,
    allow_shadow_policy_migration: bool,
) -> Result<ExecutionState> {
    let path = directory.join("execution-state.json");
    let Some(mut state) = read_state_file(&path)? else {
        return Ok(ExecutionState {
            schema_version: STATE_SCHEMA_VERSION,
            policy_sha256: policy_sha256.to_owned(),
            account_id: account_id.to_owned(),
            campaign_id,
            business_date,
            actions_today: 0,
            last_action_at: None,
            paused_for_daily_cap_on: None,
            pending: None,
            incident_class: None,
        });
    };
    ensure!(
        state.schema_version == STATE_SCHEMA_VERSION
            && state.account_id == account_id
            && state.campaign_id == campaign_id,
        "WB automation execution state не соответствует policy"
    );
    if state.policy_sha256 != policy_sha256 {
        ensure!(
            allow_shadow_policy_migration,
            "WB automation execution state не соответствует policy"
        );
        // A shadow policy cannot emit writes. Updating only its digest keeps
        // pending/cooldown/pause/incident state intact so a policy rollout does
        // not erase an in-flight reconciliation or create a fresh action slot.
        policy_sha256.clone_into(&mut state.policy_sha256);
    }
    Ok(state)
}

fn read_state_file(path: &Path) -> Result<Option<ExecutionState>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("WB automation execution state недоступен"),
    };
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.len() <= MAX_STATE_BYTES
            && metadata.permissions().mode().is_multiple_of(0o100),
        "WB automation execution state небезопасен"
    );
    let bytes = fs::read(path).context("WB automation execution state нельзя прочитать")?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .context("WB automation execution state повреждён")
}

fn save_execution_state(directory: &Path, state: &ExecutionState) -> Result<()> {
    validate_private_directory(directory)?;
    let bytes = serde_json::to_vec_pretty(state)
        .context("WB automation execution state нельзя сериализовать")?;
    ensure!(
        bytes.len() as u64 <= MAX_STATE_BYTES,
        "WB automation execution state слишком велик"
    );
    save_execution_state_bytes(directory, &bytes, write_state)
}

fn save_execution_state_bytes(
    directory: &Path,
    bytes: &[u8],
    write: fn(&mut File, &[u8]) -> Result<()>,
) -> Result<()> {
    let temporary = directory.join(format!(".execution-state-{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .context("WB automation temporary execution state недоступен")?;
    if let Err(error) = write(&mut file, bytes) {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    fs::rename(&temporary, directory.join("execution-state.json"))
        .context("WB automation execution state нельзя опубликовать")?;
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .context("WB automation execution state directory нельзя синхронизировать")
}

fn write_state(file: &mut File, bytes: &[u8]) -> Result<()> {
    file.write_all(bytes)
        .context("WB automation execution state нельзя записать")?;
    file.write_all(b"\n")
        .context("WB automation execution state нельзя завершить")?;
    file.sync_all()
        .context("WB automation execution state нельзя синхронизировать")
}

fn validate_private_directory(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).context("WB automation state directory недоступен")?;
    ensure!(
        metadata.is_dir()
            && !metadata.file_type().is_symlink()
            && metadata.permissions().mode().is_multiple_of(0o100),
        "WB automation state directory должен быть доступен только владельцу"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use chrono::{TimeZone, Utc};
    use tokio_postgres::Config;

    use super::*;
    use crate::{
        control::automation::{
            WbAutomationBidReason, WbAutomationDisableReason, WbAutomationHoldReason,
            WbAutomationObservation, WbAutomationPacingMode, WbAutomationPolicy,
            WbAutomationSkuObservation,
        },
        test_support::mock_http,
        wb::{WbClient, WbCredentials},
    };

    const TEST_SELLER_SID: &str = "123e4567-e89b-42d3-a456-426614174000";
    static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
    static POSTGRES_EXECUTOR_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct Fixture {
        root: PathBuf,
        policy: PathBuf,
        registry: PathBuf,
        reader_token: PathBuf,
        writer_token: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            Self::new_for_campaign(39_682_633)
        }

        fn new_for_campaign(campaign_id: u64) -> Self {
            let id = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "mcp-wb-automation-executor-{}-{id}",
                std::process::id()
            ));
            fs::create_dir(&root).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            let policy = root.join("policy.json");
            let registry = root.join("access.json");
            let reader_token = root.join("reader.token");
            let writer_token = root.join("writer.token");
            let mut test_policy = serde_json::from_slice::<WbAutomationPolicy>(include_bytes!(
                "../../config/wb-automation-robot.json"
            ))
            .unwrap();
            test_policy.write_enabled = true;
            test_policy.bid_writes_enabled = true;
            test_policy.campaign_id = campaign_id;
            fs::write(&policy, serde_json::to_vec_pretty(&test_policy).unwrap()).unwrap();
            fs::write(
                &registry,
                serde_json::to_vec(&serde_json::json!({
                    "version": 1,
                    "actors": [{
                        "id": "manager",
                        "name": "Manager",
                        "role": "manager",
                        "oidc": {"username": "manager"}
                    }],
                    "accounts": [{
                        "id": "ip_domnyshev_wb",
                        "organization": "Test WB",
                        "marketplace": "wildberries",
                        "seller_client_id": "seller",
                        "manager_id": "manager",
                        "wildberries": {
                            "api_token_env": "UNUSED_WB_TOKEN",
                            "seller_sid": TEST_SELLER_SID
                        }
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
            fs::write(&reader_token, wb_token((1_u64 << 6) | (1_u64 << 30))).unwrap();
            fs::write(&writer_token, wb_token(1_u64 << 6)).unwrap();
            fs::set_permissions(&reader_token, fs::Permissions::from_mode(0o600)).unwrap();
            fs::set_permissions(&writer_token, fs::Permissions::from_mode(0o600)).unwrap();
            Self {
                root,
                policy,
                registry,
                reader_token,
                writer_token,
            }
        }

        fn executor(&self, reader_base_url: &str, writer_base_url: &str) -> WbAutomationExecutor {
            let mut executor = WbAutomationExecutor::from_files(
                &self.policy,
                &self.registry,
                &self.reader_token,
                &self.writer_token,
                &self.root,
                false,
                Duration::from_secs(2),
                None,
                "http://127.0.0.1:3128",
            )
            .unwrap();
            let account_id = executor.observer.policy().account_id.clone();
            executor
                .observer
                .replace_client_for_test(WbClient::new_for_test(
                    Duration::from_secs(2),
                    BTreeMap::from([(
                        account_id,
                        WbCredentials {
                            token: "test-reader".to_owned(),
                        },
                    )]),
                    reader_base_url,
                    reader_base_url,
                ));
            executor.writer = WbBidWriteClient::new_for_test(
                writer_base_url,
                "test-writer",
                Duration::from_secs(2),
            );
            executor
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn wb_token(scope: u64) -> String {
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3_600;
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","typ":"JWT"}"#);
        let claims = serde_json::json!({
            "acc": 3,
            "for": "self",
            "t": false,
            "s": scope,
            "exp": expires,
            "sid": TEST_SELLER_SID
        });
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
        let signature = URL_SAFE_NO_PAD.encode([0_u8; 64]);
        format!("{header}.{payload}.{signature}")
    }

    fn campaign_response_for(campaign_id: u64, status: i32, bid: u64) -> serde_json::Value {
        campaign_response_with_bids(campaign_id, status, [bid; 3])
    }

    fn campaign_response_with_bids(
        campaign_id: u64,
        status: i32,
        bids: [u64; 3],
    ) -> serde_json::Value {
        serde_json::json!({
            "adverts": [{
                "id": campaign_id,
                "status": status,
                "bid_type": "manual",
                "settings": {
                    "name": "Робот",
                    "payment_type": "cpc",
                    "placements": {"search": true, "recommendations": false}
                },
                "nm_settings": [
                    {"nm_id": 449_627_598_u64, "bids_kopecks": {"search": bids[0], "recommendations": 0}},
                    {"nm_id": 449_627_015_u64, "bids_kopecks": {"search": bids[1], "recommendations": 0}},
                    {"nm_id": 497_424_314_u64, "bids_kopecks": {"search": bids[2], "recommendations": 0}}
                ]
            }]
        })
    }

    fn minimum_bids_response() -> serde_json::Value {
        serde_json::json!({
            "bids": [
                {"nm_id": 449_627_598_u64, "bids": [{"currency": "RUB", "type": "search", "value": 102}]},
                {"nm_id": 449_627_015_u64, "bids": [{"currency": "RUB", "type": "search", "value": 102}]},
                {"nm_id": 497_424_314_u64, "bids": [{"currency": "RUB", "type": "search", "value": 102}]}
            ]
        })
    }

    fn reader_server(
        status: i32,
        bid: u64,
        current_date: &str,
        daily_spend_rubles: Option<u64>,
        stock: u64,
    ) -> (String, std::sync::mpsc::Receiver<String>) {
        reader_server_for(
            39_682_633,
            status,
            bid,
            current_date,
            daily_spend_rubles,
            stock,
        )
    }

    fn reader_server_for(
        campaign_id: u64,
        status: i32,
        bid: u64,
        current_date: &str,
        daily_spend_rubles: Option<u64>,
        stock: u64,
    ) -> (String, std::sync::mpsc::Receiver<String>) {
        // The previous business date must carry at least one per-SKU row.
        // Without it the observation has no attribution evidence at all and
        // holds fail-closed, so every write path below would stop being
        // exercised. The row is deliberately low-exposure (few views, no
        // orders) so the engine still reaches its bid-exploration branch.
        let previous_date = current_date
            .parse::<NaiveDate>()
            .expect("fixture current_date")
            .pred_opt()
            .expect("fixture previous_date")
            .format("%Y-%m-%d")
            .to_string();
        let mut stat_rows = vec![serde_json::json!({
            "date": previous_date,
            "nm_id": 449_627_598_u64,
            "views": 10,
            "clicks": 1,
            "sum": 1,
            "orders": 0,
            "sumPrice": 0
        })];
        if let Some(spend) = daily_spend_rubles {
            stat_rows.push(serde_json::json!({
                "date": current_date,
                "nm_id": 449_627_598_u64,
                "views": 10,
                "clicks": 1,
                "sum": spend,
                "orders": 0,
                "sumPrice": 0
            }));
        }
        let advertising_payload = serde_json::json!([{
            "advertId": campaign_id,
            "stats": stat_rows
        }]);
        mock_http(vec![
            (
                200,
                campaign_response_for(campaign_id, status, bid).to_string(),
            ),
            (200, minimum_bids_response().to_string()),
            (200, serde_json::json!({"total": 1_000}).to_string()),
            (200, advertising_payload.to_string()),
            (
                200,
                serde_json::json!({"data": {"items": [
                    {"nmId": 449_627_598_u64, "warehouseId": 1, "quantity": stock},
                    {"nmId": 449_627_015_u64, "warehouseId": 1, "quantity": stock},
                    {"nmId": 497_424_314_u64, "warehouseId": 1, "quantity": stock}
                ]}})
                .to_string(),
            ),
        ])
    }

    fn campaign_level_reader_server_for(
        campaign_id: u64,
        status: i32,
        bids: [u64; 3],
        current_date: &str,
        daily_spend_rubles: u64,
        stock: u64,
    ) -> (String, std::sync::mpsc::Receiver<String>) {
        let previous_date = current_date
            .parse::<NaiveDate>()
            .expect("fixture current_date")
            .pred_opt()
            .expect("fixture previous_date")
            .format("%Y-%m-%d")
            .to_string();
        let advertising_payload = serde_json::json!([{
            "advertId": campaign_id,
            "stats": [
                {
                    "date": previous_date,
                    "views": 44,
                    "clicks": 2,
                    "sum": 2.19,
                    "orders": 0,
                    "sumPrice": 0
                },
                {
                    "date": current_date,
                    "views": 1,
                    "clicks": 0,
                    "sum": daily_spend_rubles,
                    "orders": 0,
                    "sumPrice": 0
                }
            ]
        }]);
        mock_http(vec![
            (
                200,
                campaign_response_with_bids(campaign_id, status, bids).to_string(),
            ),
            (200, minimum_bids_response().to_string()),
            (200, serde_json::json!({"total": 1_000}).to_string()),
            (200, advertising_payload.to_string()),
            (
                200,
                serde_json::json!({"data": {"items": [
                    {"nmId": 449_627_598_u64, "warehouseId": 1, "quantity": stock},
                    {"nmId": 449_627_015_u64, "warehouseId": 1, "quantity": stock},
                    {"nmId": 497_424_314_u64, "warehouseId": 1, "quantity": stock}
                ]}})
                .to_string(),
            ),
        ])
    }

    fn explicit_exposure_snapshot() -> (WbAutomationPolicy, WbAutomationSnapshot) {
        let mut policy = serde_json::from_slice::<WbAutomationPolicy>(include_bytes!(
            "../../config/wb-automation-robot.json"
        ))
        .unwrap();
        policy.write_enabled = true;
        policy.bid_writes_enabled = true;
        let observation = WbAutomationObservation {
            observed_at: now(),
            campaign_status: 9,
            paused_by_automation: false,
            budget_remaining_minor: 100_000,
            daily_spend_minor: 234,
            daily_spend_complete: true,
            actions_today: 0,
            last_action_at: None,
            attribution_complete: false,
            campaign_level_metrics: Some(crate::control::automation::WbAutomationCampaignMetrics {
                impressions: 44,
                clicks: 2,
                spend_minor: 219,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            }),
            skus: policy
                .nm_ids
                .iter()
                .copied()
                .map(|nm_id| WbAutomationSkuObservation {
                    nm_id,
                    current_bid_kopecks: 117,
                    sellable_stock: 10,
                    impressions: 0,
                    clicks: 0,
                    spend_minor: 0,
                    attributed_orders: 0,
                    attributed_revenue_minor: 0,
                })
                .collect(),
        };
        let decision = WbAutomationDecision {
            account_id: policy.account_id.clone(),
            campaign_id: policy.campaign_id,
            observed_at: observation.observed_at,
            action: WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AttributionIncomplete,
            },
            unresolved_stops: Vec::new(),
        };
        (
            policy,
            WbAutomationSnapshot {
                schema_version: 4,
                policy_sha256: "a".repeat(64),
                previous_business_date: now().date_naive().pred_opt().unwrap(),
                observation,
                decision,
            },
        )
    }

    fn execution_state(business_date: NaiveDate) -> ExecutionState {
        ExecutionState {
            schema_version: STATE_SCHEMA_VERSION,
            policy_sha256: String::new(),
            account_id: "ip_domnyshev_wb".to_owned(),
            campaign_id: 39_682_633,
            business_date,
            actions_today: 0,
            last_action_at: None,
            paused_for_daily_cap_on: None,
            pending: None,
            incident_class: None,
        }
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, 0).unwrap()
    }

    fn observation(status: i32, bid: u64) -> WbAutomationObservation {
        WbAutomationObservation {
            observed_at: now(),
            campaign_status: status,
            paused_by_automation: false,
            budget_remaining_minor: 100_000,
            daily_spend_minor: 0,
            daily_spend_complete: true,
            actions_today: 0,
            last_action_at: None,
            attribution_complete: true,
            campaign_level_metrics: None,
            skus: vec![WbAutomationSkuObservation {
                nm_id: 1,
                current_bid_kopecks: bid,
                sellable_stock: 10,
                impressions: 0,
                clicks: 0,
                spend_minor: 0,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            }],
        }
    }

    fn state(pending: PendingAction) -> ExecutionState {
        ExecutionState {
            schema_version: STATE_SCHEMA_VERSION,
            policy_sha256: "a".repeat(64),
            account_id: "account".to_owned(),
            campaign_id: 1,
            business_date: now().date_naive(),
            actions_today: 1,
            last_action_at: Some(now()),
            paused_for_daily_cap_on: None,
            pending: Some(pending),
            incident_class: None,
        }
    }

    #[test]
    fn durable_payloads_are_typed_visible_and_domain_separated() {
        let change = WbAutomationBidChange {
            nm_id: 1,
            from_bid_kopecks: 100,
            to_bid_kopecks: 115,
            reason: WbAutomationBidReason::LowExposureExploration,
        };
        let request = PendingActionKind::ChangeBids {
            changes: vec![change],
        };
        let mut action = WbAutomationDurableAction {
            idempotency_key: "1".repeat(64),
            cycle_id: "2".repeat(64),
            policy_digest: "3".repeat(64),
            request_digest: "4".repeat(64),
            action_kind: WbAutomationDurableActionKind::ChangeBids,
            request_json: serde_json::to_string(&request).unwrap(),
            status: WbAutomationDurableActionStatus::AwaitingReadback,
            reserved_at: now(),
            write_started_at: Some(now()),
            resolved_at: None,
            readback_cycle_id: None,
            last_error_class: None,
        };
        let pending = pending_from_durable_action(&action).unwrap();
        assert_eq!(pending.kind, request);
        assert!(pending_is_visible(&observation(9, 115), &pending));
        assert!(!pending_is_visible(&observation(9, 114), &pending));
        assert_eq!(
            durable_action_kind(&pending.kind),
            Some(WbAutomationDurableActionKind::ChangeBids)
        );

        let pause = PendingAction {
            reserved_at: now(),
            kind: PendingActionKind::PauseCampaignForDailyCap,
        };
        assert!(pending_is_visible(&observation(11, 100), &pause));
        assert!(!pending_is_visible(&observation(9, 100), &pause));
        assert_eq!(
            durable_action_kind(&pause.kind),
            Some(WbAutomationDurableActionKind::PauseCampaignForDailyCap)
        );
        assert_eq!(
            durable_pause_date(&pause).unwrap(),
            Some(wb_automation_business_date(now()))
        );
        let resume = PendingAction {
            reserved_at: now(),
            kind: PendingActionKind::ResumeCampaignAfterDailyCap,
        };
        assert!(pending_is_visible(&observation(9, 100), &resume));
        assert_eq!(durable_action_kind(&resume.kind), None);
        assert!(durable_pause_date(&resume).is_err());
        assert_eq!(durable_pause_date(&pending).unwrap(), None);
        assert_eq!(
            classify_postgres_write(&Ok(())).unwrap(),
            PostgresWriteResult::Sent
        );
        assert!(
            classify_postgres_write(&Err(WbGuardedWriteError::Permit(
                crate::control::automation_postgres::WbAutomationPostgresError::StateChanged,
            )))
            .is_err()
        );
        assert_eq!(
            classify_postgres_write(&Err(WbGuardedWriteError::Write(
                crate::control::wb::WbWriteError::InvalidRequest("coverage"),
            )))
            .unwrap(),
            PostgresWriteResult::Ambiguous
        );

        action.action_kind = WbAutomationDurableActionKind::PauseCampaignForDailyCap;
        assert!(pending_from_durable_action(&action).is_err());
        action.request_json = "{".to_owned();
        assert!(pending_from_durable_action(&action).is_err());
        assert_ne!(
            sha256_domain("wb-automation-cycle-v1", b"same"),
            sha256_domain("wb-automation-action-v1", b"same")
        );

        let decision = WbAutomationDecision {
            account_id: "account".to_owned(),
            campaign_id: 1,
            observed_at: now(),
            action: WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::NoMaterialChange,
            },
            unresolved_stops: Vec::new(),
        };
        let receipt = postgres_receipt(
            decision,
            WbAutomationExecutionOutcome::Observed,
            "5".repeat(64),
            true,
            false,
            7,
        );
        assert_eq!(receipt.state_revision, 7);
        assert!(receipt.cycle_inserted);
    }

    #[test]
    fn explicit_exposure_increase_is_single_step_and_preserves_stronger_guards() {
        let (policy, snapshot) = explicit_exposure_snapshot();
        let decision = explicit_exposure_increase_decision(
            &policy,
            &snapshot,
            policy.target_impressions_per_day,
        )
        .unwrap();
        assert!(matches!(
            decision.action,
            WbAutomationAction::ChangeBids { ref changes }
                if changes == &[WbAutomationBidChange {
                    nm_id: 449_627_598,
                    from_bid_kopecks: 117,
                    to_bid_kopecks: 134,
                    reason: WbAutomationBidReason::ExplicitExposureTarget,
                }]
        ));

        assert!(
            explicit_exposure_increase_decision(
                &policy,
                &snapshot,
                policy.target_impressions_per_day - 1,
            )
            .is_err()
        );

        let mut guarded = snapshot.clone();
        guarded.decision.action = WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::CooldownActive,
        };
        assert!(
            explicit_exposure_increase_decision(
                &policy,
                &guarded,
                policy.target_impressions_per_day,
            )
            .is_err()
        );

        let mut delivered = snapshot;
        delivered
            .observation
            .campaign_level_metrics
            .as_mut()
            .unwrap()
            .impressions = policy.low_exposure_max_impressions;
        assert!(
            explicit_exposure_increase_decision(
                &policy,
                &delivered,
                policy.target_impressions_per_day,
            )
            .is_err()
        );
    }

    #[test]
    fn autonomous_pacing_uses_target_and_campaign_efficiency_guards() {
        let (policy, snapshot) = explicit_exposure_snapshot();
        let decision = autonomous_exposure_pacing_decision(&policy, &snapshot)
            .unwrap()
            .expect("campaign below target has a bounded pacing step");
        assert!(matches!(
            decision.action,
            WbAutomationAction::ChangeBids { ref changes }
                if changes == &[WbAutomationBidChange {
                    nm_id: 449_627_598,
                    from_bid_kopecks: 117,
                    to_bid_kopecks: 134,
                    reason: WbAutomationBidReason::AutonomousExposurePacing,
                }]
        ));

        let mut delivered = snapshot.clone();
        delivered
            .observation
            .campaign_level_metrics
            .as_mut()
            .unwrap()
            .impressions = policy.target_impressions_per_day;
        assert_eq!(
            autonomous_exposure_pacing_decision(&policy, &delivered).unwrap(),
            None
        );

        let mut no_orders = snapshot.clone();
        no_orders
            .observation
            .campaign_level_metrics
            .as_mut()
            .unwrap()
            .clicks = policy.no_order_reduce_clicks;
        assert_eq!(
            autonomous_exposure_pacing_decision(&policy, &no_orders).unwrap(),
            None
        );

        let mut expensive = snapshot.clone();
        let metrics = expensive
            .observation
            .campaign_level_metrics
            .as_mut()
            .unwrap();
        metrics.attributed_orders = 1;
        metrics.attributed_revenue_minor = 1_000;
        metrics.spend_minor = 200;
        assert_eq!(
            autonomous_exposure_pacing_decision(&policy, &expensive).unwrap(),
            None
        );

        let mut guarded = snapshot;
        guarded.decision.action = WbAutomationAction::Hold {
            reason: WbAutomationHoldReason::CooldownActive,
        };
        assert_eq!(
            autonomous_exposure_pacing_decision(&policy, &guarded).unwrap(),
            None
        );

        let (policy, mut missing_metrics) = explicit_exposure_snapshot();
        missing_metrics.observation.campaign_level_metrics = None;
        assert_eq!(
            autonomous_exposure_pacing_decision(&policy, &missing_metrics).unwrap(),
            None
        );

        let (policy, mut no_candidate) = explicit_exposure_snapshot();
        for sku in &mut no_candidate.observation.skus {
            sku.current_bid_kopecks = policy.max_bid_kopecks;
        }
        assert_eq!(
            autonomous_exposure_pacing_decision(&policy, &no_candidate).unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn automatic_pacing_executes_only_when_typed_policy_enables_it() {
        #[cfg(coverage)]
        let database_url = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL").unwrap();
        #[cfg(not(coverage))]
        let Ok(database_url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
            return;
        };
        let _serial = POSTGRES_EXECUTOR_TEST_LOCK.lock().await;
        let config = Config::from_str(&database_url).unwrap();
        let store = WbAutomationPostgresStore::connect(&config).await.unwrap();
        store.verify_runtime_contract().await.unwrap();
        let observed_at = Utc::now();
        let current_date = wb_automation_business_date(observed_at)
            .format("%Y-%m-%d")
            .to_string();

        let enabled_campaign = 39_682_721;
        let enabled_fixture = Fixture::new_for_campaign(enabled_campaign);
        let (enabled_reader, _) = campaign_level_reader_server_for(
            enabled_campaign,
            9,
            [117, 117, 117],
            &current_date,
            2,
            10,
        );
        let (enabled_writer, enabled_requests) = mock_http(vec![(200, "{}".to_owned())]);
        let enabled = enabled_fixture.executor(&enabled_reader, &enabled_writer);
        let enabled_legacy = WbAutomationLegacyStateSeed {
            policy_digest: enabled.policy_sha256().to_owned(),
            business_date: wb_automation_business_date(observed_at),
            actions_today: 0,
            last_action_at: None,
            paused_for_daily_cap_on: None,
            incident_class: None,
            legacy_digest: "c".repeat(64),
        };
        let paced = enabled
            .run_once_postgres(&store, &enabled_legacy, observed_at)
            .await
            .unwrap()
            .expect("enabled campaign lock is available");
        assert_eq!(
            paced.outcome,
            WbAutomationExecutionOutcome::WriteSentReconciliationRequired
        );
        assert!(matches!(
            paced.decision.action,
            WbAutomationAction::ChangeBids { ref changes }
                if changes.len() == 1
                    && changes[0].reason == WbAutomationBidReason::AutonomousExposurePacing
        ));
        assert!(
            enabled_requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with("PATCH /api/advert/v1/bids")
        );

        let disabled_campaign = 39_682_722;
        let disabled_fixture = Fixture::new_for_campaign(disabled_campaign);
        let mut disabled_policy = serde_json::from_slice::<WbAutomationPolicy>(
            &fs::read(&disabled_fixture.policy).unwrap(),
        )
        .unwrap();
        disabled_policy.autonomous_pacing = WbAutomationPacingMode::Disabled;
        fs::write(
            &disabled_fixture.policy,
            serde_json::to_vec_pretty(&disabled_policy).unwrap(),
        )
        .unwrap();
        let (disabled_reader, _) = campaign_level_reader_server_for(
            disabled_campaign,
            9,
            [117, 117, 117],
            &current_date,
            2,
            10,
        );
        let (disabled_writer, disabled_requests) = mock_http(Vec::new());
        let disabled = disabled_fixture.executor(&disabled_reader, &disabled_writer);
        let disabled_legacy = WbAutomationLegacyStateSeed {
            policy_digest: disabled.policy_sha256().to_owned(),
            business_date: wb_automation_business_date(observed_at),
            actions_today: 0,
            last_action_at: None,
            paused_for_daily_cap_on: None,
            incident_class: None,
            legacy_digest: "d".repeat(64),
        };
        let held = disabled
            .run_once_postgres(&store, &disabled_legacy, observed_at)
            .await
            .unwrap()
            .expect("disabled campaign lock is available");
        assert_eq!(held.outcome, WbAutomationExecutionOutcome::Observed);
        assert!(matches!(
            held.decision.action,
            WbAutomationAction::Hold {
                reason: WbAutomationHoldReason::AttributionIncomplete
            }
        ));
        assert!(disabled_requests.try_recv().is_err());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the PostgreSQL campaign lease is consumed by explicit async release"
    )]
    async fn postgres_executor_reserves_writes_and_reconciles_from_readback() {
        #[cfg(coverage)]
        let database_url = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL").unwrap();
        #[cfg(not(coverage))]
        let Ok(database_url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
            return;
        };
        let _serial = POSTGRES_EXECUTOR_TEST_LOCK.lock().await;
        let fixture = Fixture::new();
        let observed_at = Utc::now();
        let current_date = wb_automation_business_date(observed_at)
            .format("%Y-%m-%d")
            .to_string();
        let (reader_url, _) = reader_server(9, 102, &current_date, Some(0), 10);
        let (writer_url, writer_requests) = mock_http(vec![(200, "{}".to_owned())]);
        let mut executor = fixture.executor(&reader_url, &writer_url);
        let config = Config::from_str(&database_url).unwrap();
        let store = WbAutomationPostgresStore::connect(&config).await.unwrap();
        store.verify_runtime_contract().await.unwrap();
        let legacy = WbAutomationLegacyStateSeed {
            policy_digest: executor.policy_sha256().to_owned(),
            business_date: wb_automation_business_date(observed_at),
            actions_today: 0,
            last_action_at: None,
            paused_for_daily_cap_on: None,
            incident_class: None,
            legacy_digest: "f".repeat(64),
        };

        let first = executor
            .run_once_postgres(&store, &legacy, observed_at)
            .await
            .unwrap()
            .expect("campaign lock is available");
        assert_eq!(
            first.outcome,
            WbAutomationExecutionOutcome::WriteSentReconciliationRequired
        );
        assert_eq!(first.state_revision, 2);
        assert!(first.cycle_inserted);
        assert!(first.legacy_imported);
        assert!(
            writer_requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with("PATCH /api/advert/v1/bids")
        );

        let (readback_url, _) = reader_server(9, 117, &current_date, None, 10);
        executor
            .observer
            .replace_client_for_test(WbClient::new_for_test(
                Duration::from_secs(2),
                BTreeMap::from([(
                    "ip_domnyshev_wb".to_owned(),
                    WbCredentials {
                        token: "test-reader".to_owned(),
                    },
                )]),
                &readback_url,
                &readback_url,
            ));
        let reconciled = executor
            .run_once_postgres(&store, &legacy, observed_at + ChronoDuration::minutes(1))
            .await
            .unwrap()
            .expect("campaign lock is available");
        assert_eq!(reconciled.outcome, WbAutomationExecutionOutcome::Reconciled);
        assert_eq!(reconciled.state_revision, 3);
        assert!(!reconciled.legacy_imported);

        let lease = store
            .try_acquire_campaign("ip_domnyshev_wb", 39_682_633)
            .await
            .unwrap()
            .expect("executor released its campaign lock");
        let state = lease.load_state().await.unwrap().unwrap();
        assert_eq!(state.actions_today, 1);
        assert!(state.pending_idempotency_key.is_none());
        assert_eq!(state.revision, 3);
        lease.release().await.unwrap();
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the PostgreSQL campaign lease is consumed by explicit async release"
    )]
    async fn explicit_exposure_increase_uses_durable_write_and_exact_readback() {
        #[cfg(coverage)]
        let database_url = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL").unwrap();
        #[cfg(not(coverage))]
        let Ok(database_url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
            return;
        };
        let _serial = POSTGRES_EXECUTOR_TEST_LOCK.lock().await;
        let campaign_id = 39_682_720;
        let fixture = Fixture::new_for_campaign(campaign_id);
        let observed_at = Utc::now();
        let current_date = wb_automation_business_date(observed_at)
            .format("%Y-%m-%d")
            .to_string();
        let (reader_url, _) =
            campaign_level_reader_server_for(campaign_id, 9, [117, 117, 117], &current_date, 2, 10);
        let (writer_url, writer_requests) = mock_http(vec![(200, "{}".to_owned())]);
        let mut executor = fixture.executor(&reader_url, &writer_url);
        let config = Config::from_str(&database_url).unwrap();
        let store = WbAutomationPostgresStore::connect(&config).await.unwrap();
        store.verify_runtime_contract().await.unwrap();
        let legacy = WbAutomationLegacyStateSeed {
            policy_digest: executor.policy_sha256().to_owned(),
            business_date: wb_automation_business_date(observed_at),
            actions_today: 0,
            last_action_at: None,
            paused_for_daily_cap_on: None,
            incident_class: None,
            legacy_digest: "e".repeat(64),
        };

        let first = executor
            .run_explicit_exposure_increase_once_postgres(&store, &legacy, observed_at, 5_000)
            .await
            .unwrap()
            .expect("campaign lock is available");
        assert_eq!(
            first.outcome,
            WbAutomationExecutionOutcome::WriteSentReconciliationRequired
        );
        assert!(matches!(
            first.decision.action,
            WbAutomationAction::ChangeBids { ref changes }
                if changes == &[WbAutomationBidChange {
                    nm_id: 449_627_598,
                    from_bid_kopecks: 117,
                    to_bid_kopecks: 134,
                    reason: WbAutomationBidReason::ExplicitExposureTarget,
                }]
        ));
        let write_request = writer_requests
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(write_request.starts_with("PATCH /api/advert/v1/bids"));
        assert!(write_request.contains("\"nm_id\":449627598"));
        assert!(write_request.contains("\"bid_kopecks\":134"));

        let (readback_url, _) =
            campaign_level_reader_server_for(campaign_id, 9, [134, 117, 117], &current_date, 2, 10);
        executor
            .observer
            .replace_client_for_test(WbClient::new_for_test(
                Duration::from_secs(2),
                BTreeMap::from([(
                    "ip_domnyshev_wb".to_owned(),
                    WbCredentials {
                        token: "test-reader".to_owned(),
                    },
                )]),
                &readback_url,
                &readback_url,
            ));
        let reconciled = executor
            .run_once_postgres(&store, &legacy, observed_at + ChronoDuration::minutes(1))
            .await
            .unwrap()
            .expect("campaign lock is available");
        assert_eq!(reconciled.outcome, WbAutomationExecutionOutcome::Reconciled);

        let lease = store
            .try_acquire_campaign("ip_domnyshev_wb", campaign_id)
            .await
            .unwrap()
            .expect("executor released its campaign lock");
        let state = lease.load_state().await.unwrap().unwrap();
        assert_eq!(state.actions_today, 1);
        assert!(state.pending_idempotency_key.is_none());
        assert_eq!(state.revision, 3);
        lease.release().await.unwrap();
    }

    #[tokio::test]
    async fn postgres_executor_covers_holds_caps_pauses_and_ambiguous_readback() {
        #[cfg(coverage)]
        let database_url = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL").unwrap();
        #[cfg(not(coverage))]
        let Ok(database_url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
            return;
        };
        let _serial = POSTGRES_EXECUTOR_TEST_LOCK.lock().await;
        let config = Config::from_str(&database_url).unwrap();
        let store = WbAutomationPostgresStore::connect(&config).await.unwrap();
        let observed_at = Utc::now();
        let current_date = wb_automation_business_date(observed_at)
            .format("%Y-%m-%d")
            .to_string();

        let observed_campaign = 39_682_700;
        let observed_fixture = Fixture::new_for_campaign(observed_campaign);
        let (reader_url, _) = reader_server_for(observed_campaign, 9, 102, &current_date, None, 3);
        let observed_executor = observed_fixture.executor(&reader_url, "http://127.0.0.1:1");
        assert_eq!(observed_executor.policy().campaign_id, observed_campaign);
        let observed_legacy = postgres_legacy(&observed_executor, observed_at, None);
        assert_eq!(
            observed_executor
                .run_once_postgres(&store, &observed_legacy, observed_at)
                .await
                .unwrap()
                .unwrap()
                .outcome,
            WbAutomationExecutionOutcome::Observed
        );

        let cap_campaign = 39_682_701;
        let cap_fixture = Fixture::new_for_campaign(cap_campaign);
        let (cap_url, _) = reader_server_for(cap_campaign, 9, 102, &current_date, Some(300), 10);
        let mut cap_executor = cap_fixture.executor(&cap_url, "http://127.0.0.1:1");
        let cap_legacy = postgres_legacy(&cap_executor, observed_at, None);
        let capped = cap_executor
            .run_once_postgres(&store, &cap_legacy, observed_at)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(capped.outcome, WbAutomationExecutionOutcome::IncidentLocked);
        assert_eq!(capped.state_revision, 2);
        let (clean_url, _) = reader_server_for(cap_campaign, 9, 102, &current_date, None, 10);
        cap_executor
            .observer
            .replace_client_for_test(test_reader(&clean_url));
        assert_eq!(
            cap_executor
                .run_once_postgres(
                    &store,
                    &cap_legacy,
                    observed_at + ChronoDuration::minutes(1),
                )
                .await
                .unwrap()
                .unwrap()
                .outcome,
            WbAutomationExecutionOutcome::IncidentLocked
        );

        let pause_campaign = 39_682_702;
        let pause_fixture = Fixture::new_for_campaign(pause_campaign);
        let (pause_url, _) =
            reader_server_for(pause_campaign, 9, 102, &current_date, Some(250), 10);
        let (pause_writer_url, pause_requests) = mock_http(vec![(200, "{}".to_owned())]);
        let mut pause_executor = pause_fixture.executor(&pause_url, &pause_writer_url);
        let pause_legacy = postgres_legacy(&pause_executor, observed_at, None);
        pause_executor
            .run_once_postgres(&store, &pause_legacy, observed_at)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pause_requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with("GET /adv/v0/pause")
        );
        let (pause_readback_url, _) =
            reader_server_for(pause_campaign, 11, 102, &current_date, None, 10);
        pause_executor
            .observer
            .replace_client_for_test(test_reader(&pause_readback_url));
        assert_eq!(
            pause_executor
                .run_once_postgres(
                    &store,
                    &pause_legacy,
                    observed_at + ChronoDuration::minutes(1),
                )
                .await
                .unwrap()
                .unwrap()
                .outcome,
            WbAutomationExecutionOutcome::Reconciled
        );
        let next_day = observed_at + ChronoDuration::days(1);
        let next_date = wb_automation_business_date(next_day)
            .format("%Y-%m-%d")
            .to_string();
        let (resume_url, _) = reader_server_for(pause_campaign, 11, 102, &next_date, None, 10);
        pause_executor
            .observer
            .replace_client_for_test(test_reader(&resume_url));
        let resume = pause_executor
            .run_once_postgres(&store, &pause_legacy, next_day)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resume.outcome, WbAutomationExecutionOutcome::Observed);
        assert_eq!(resume.state_revision, 3);

        let ambiguous_campaign = 39_682_703;
        let ambiguous_fixture = Fixture::new_for_campaign(ambiguous_campaign);
        let (ambiguous_url, _) =
            reader_server_for(ambiguous_campaign, 9, 102, &current_date, Some(0), 10);
        let (ambiguous_writer_url, _) = mock_http(vec![(500, "{}".to_owned())]);
        let ambiguous_executor = ambiguous_fixture.executor(&ambiguous_url, &ambiguous_writer_url);
        let ambiguous_legacy = postgres_legacy(&ambiguous_executor, observed_at, None);
        let ambiguous = ambiguous_executor
            .run_once_postgres(&store, &ambiguous_legacy, observed_at)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            ambiguous.outcome,
            WbAutomationExecutionOutcome::WriteSentReconciliationRequired
        );
        assert_eq!(ambiguous.state_revision, 3);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "campaign leases are deliberately held across lock-contention assertions"
    )]
    async fn postgres_executor_recovers_each_preexisting_pending_state_without_retry() {
        #[cfg(coverage)]
        let database_url = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL").unwrap();
        #[cfg(not(coverage))]
        let Ok(database_url) = std::env::var("WB_AUTOMATION_TEST_DATABASE_URL") else {
            return;
        };
        let _serial = POSTGRES_EXECUTOR_TEST_LOCK.lock().await;
        let config = Config::from_str(&database_url).unwrap();
        let store = WbAutomationPostgresStore::connect(&config).await.unwrap();
        let observed_at = Utc::now();
        let current_date = wb_automation_business_date(observed_at)
            .format("%Y-%m-%d")
            .to_string();

        let locked_campaign = 39_682_710;
        let locked_fixture = Fixture::new_for_campaign(locked_campaign);
        let (locked_url, _) = reader_server_for(locked_campaign, 9, 102, &current_date, None, 3);
        let locked_executor = locked_fixture.executor(&locked_url, "http://127.0.0.1:1");
        let locked_legacy = postgres_legacy(&locked_executor, observed_at, None);
        let lock_owner = WbAutomationPostgresStore::connect(&config).await.unwrap();
        let held = lock_owner
            .try_acquire_campaign("ip_domnyshev_wb", locked_campaign)
            .await
            .unwrap()
            .unwrap();
        assert!(
            locked_executor
                .run_once_postgres(&store, &locked_legacy, observed_at)
                .await
                .unwrap()
                .is_none()
        );
        held.release().await.unwrap();
        let mut resume_lease = lock_owner
            .try_acquire_campaign("ip_domnyshev_wb", locked_campaign)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            locked_executor
                .send_pending_postgres(
                    &PendingAction {
                        reserved_at: observed_at,
                        kind: PendingActionKind::ResumeCampaignAfterDailyCap,
                    },
                    &mut resume_lease,
                    &"0".repeat(64),
                    1,
                )
                .await,
            Err(WbGuardedWriteError::Permit(
                crate::control::automation_postgres::WbAutomationPostgresError::InvalidInput
            ))
        ));
        resume_lease.release().await.unwrap();

        let rollover_campaign = 39_682_711;
        let rollover_fixture = Fixture::new_for_campaign(rollover_campaign);
        let (rollover_url, _) =
            reader_server_for(rollover_campaign, 9, 102, &current_date, None, 3);
        let rollover_executor = rollover_fixture.executor(&rollover_url, "http://127.0.0.1:1");
        let mut rollover_legacy = postgres_legacy(&rollover_executor, observed_at, None);
        rollover_legacy.business_date = rollover_legacy.business_date.pred_opt().unwrap();
        rollover_legacy.actions_today = 2;
        rollover_legacy.last_action_at = Some(observed_at - ChronoDuration::hours(1));
        assert_eq!(
            rollover_executor
                .run_once_postgres(&store, &rollover_legacy, observed_at)
                .await
                .unwrap()
                .unwrap()
                .outcome,
            WbAutomationExecutionOutcome::Observed
        );

        let cancelled_campaign = 39_682_712;
        let cancelled_fixture = Fixture::new_for_campaign(cancelled_campaign);
        let (cancelled_url, _) =
            reader_server_for(cancelled_campaign, 9, 102, &current_date, None, 10);
        let cancelled_executor = cancelled_fixture.executor(&cancelled_url, "http://127.0.0.1:1");
        let cancelled_legacy = postgres_legacy(&cancelled_executor, observed_at, None);
        seed_postgres_bid_action(
            &store,
            &cancelled_executor,
            &cancelled_legacy,
            observed_at,
            false,
        )
        .await;
        let cancelled = cancelled_executor
            .run_once_postgres(
                &store,
                &cancelled_legacy,
                observed_at + ChronoDuration::minutes(1),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            cancelled.outcome,
            WbAutomationExecutionOutcome::ReservationCancelled
        );
        assert_eq!(cancelled.state_revision, 3);

        let pending_campaign = 39_682_713;
        let pending_fixture = Fixture::new_for_campaign(pending_campaign);
        let (pending_url, _) = reader_server_for(pending_campaign, 9, 102, &current_date, None, 10);
        let mut pending_executor = pending_fixture.executor(&pending_url, "http://127.0.0.1:1");
        let pending_legacy = postgres_legacy(&pending_executor, observed_at, None);
        seed_postgres_bid_action(
            &store,
            &pending_executor,
            &pending_legacy,
            observed_at,
            true,
        )
        .await;
        let awaiting = pending_executor
            .run_once_postgres(
                &store,
                &pending_legacy,
                observed_at + ChronoDuration::minutes(1),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            awaiting.outcome,
            WbAutomationExecutionOutcome::AwaitingReadback
        );
        assert_eq!(awaiting.state_revision, 2);
        let (still_waiting_url, _) =
            reader_server_for(pending_campaign, 9, 102, &current_date, None, 10);
        pending_executor
            .observer
            .replace_client_for_test(test_reader(&still_waiting_url));
        assert_eq!(
            pending_executor
                .run_once_postgres(
                    &store,
                    &pending_legacy,
                    observed_at + ChronoDuration::minutes(2),
                )
                .await
                .unwrap()
                .unwrap()
                .outcome,
            WbAutomationExecutionOutcome::AwaitingReadback
        );
        let (stale_url, _) = reader_server_for(pending_campaign, 9, 102, &current_date, None, 10);
        pending_executor
            .observer
            .replace_client_for_test(test_reader(&stale_url));
        let incident = pending_executor
            .run_once_postgres(
                &store,
                &pending_legacy,
                observed_at + ChronoDuration::minutes(10),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            incident.outcome,
            WbAutomationExecutionOutcome::IncidentLocked
        );
        assert_eq!(incident.state_revision, 3);
        let (locked_readback_url, _) =
            reader_server_for(pending_campaign, 9, 102, &current_date, None, 10);
        pending_executor
            .observer
            .replace_client_for_test(test_reader(&locked_readback_url));
        assert_eq!(
            pending_executor
                .run_once_postgres(
                    &store,
                    &pending_legacy,
                    observed_at + ChronoDuration::seconds(630),
                )
                .await
                .unwrap()
                .unwrap()
                .outcome,
            WbAutomationExecutionOutcome::IncidentLocked
        );
        let (late_readback_url, _) =
            reader_server_for(pending_campaign, 9, 117, &current_date, None, 10);
        pending_executor
            .observer
            .replace_client_for_test(test_reader(&late_readback_url));
        let recovered = pending_executor
            .run_once_postgres(
                &store,
                &pending_legacy,
                observed_at + ChronoDuration::minutes(11),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.outcome, WbAutomationExecutionOutcome::Reconciled);
        assert_eq!(recovered.state_revision, 4);

        let mut wrong_legacy = pending_legacy;
        wrong_legacy.legacy_digest = "0".repeat(64);
        assert!(
            pending_executor
                .run_once_postgres(
                    &store,
                    &wrong_legacy,
                    observed_at + ChronoDuration::minutes(12),
                )
                .await
                .is_err()
        );
    }

    fn postgres_legacy(
        executor: &WbAutomationExecutor,
        observed_at: DateTime<Utc>,
        incident_class: Option<&str>,
    ) -> WbAutomationLegacyStateSeed {
        WbAutomationLegacyStateSeed {
            policy_digest: executor.policy_sha256().to_owned(),
            business_date: wb_automation_business_date(observed_at),
            actions_today: 0,
            last_action_at: None,
            paused_for_daily_cap_on: None,
            incident_class: incident_class.map(str::to_owned),
            legacy_digest: sha256_domain(
                "wb-automation-test-legacy-v1",
                executor.policy_sha256().as_bytes(),
            ),
        }
    }

    fn test_reader(base_url: &str) -> WbClient {
        WbClient::new_for_test(
            Duration::from_secs(2),
            BTreeMap::from([(
                "ip_domnyshev_wb".to_owned(),
                WbCredentials {
                    token: "test-reader".to_owned(),
                },
            )]),
            base_url,
            base_url,
        )
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the helper consumes its campaign lease through explicit async release"
    )]
    async fn seed_postgres_bid_action(
        store: &WbAutomationPostgresStore,
        executor: &WbAutomationExecutor,
        legacy: &WbAutomationLegacyStateSeed,
        observed_at: DateTime<Utc>,
        write_started: bool,
    ) {
        let campaign_id = executor.policy().campaign_id;
        let mut lease = store
            .try_acquire_campaign("ip_domnyshev_wb", campaign_id)
            .await
            .unwrap()
            .unwrap();
        lease.initialize_from_legacy(legacy).await.unwrap();
        let cycle_id = sha256_domain(
            "wb-automation-test-cycle-v1",
            campaign_id.to_string().as_bytes(),
        );
        lease
            .persist_shadow_cycle(
                &cycle_id,
                executor.policy_sha256(),
                observed_at - ChronoDuration::minutes(1),
                wb_automation_business_date(observed_at),
                1,
                "{}",
                "{}",
            )
            .await
            .unwrap();
        let request = PendingActionKind::ChangeBids {
            changes: vec![WbAutomationBidChange {
                nm_id: 449_627_598,
                from_bid_kopecks: 102,
                to_bid_kopecks: 117,
                reason: WbAutomationBidReason::LowExposureExploration,
            }],
        };
        let request_json = serde_json::to_string(&request).unwrap();
        let idempotency_key = sha256_domain(
            "wb-automation-test-action-v1",
            campaign_id.to_string().as_bytes(),
        );
        lease
            .reserve_action(&WbAutomationActionReservation {
                idempotency_key: idempotency_key.clone(),
                cycle_id,
                policy_digest: executor.policy_sha256().to_owned(),
                request_digest: sha256_domain(
                    "wb-automation-test-request-v1",
                    request_json.as_bytes(),
                ),
                action_kind: WbAutomationDurableActionKind::ChangeBids,
                request_json,
                business_date: wb_automation_business_date(observed_at),
                expected_state_revision: 1,
                max_actions_per_day: 2,
            })
            .await
            .unwrap();
        if write_started {
            lease.mark_write_started(&idempotency_key, 2).await.unwrap();
        }
        lease.release().await.unwrap();
    }

    #[test]
    fn readback_reconciles_exact_bid_and_daily_pause_states() {
        let pending = PendingAction {
            reserved_at: now() - ChronoDuration::minutes(10),
            kind: PendingActionKind::ChangeBids {
                changes: vec![WbAutomationBidChange {
                    nm_id: 1,
                    from_bid_kopecks: 100,
                    to_bid_kopecks: 115,
                    reason: WbAutomationBidReason::LowExposureExploration,
                }],
            },
        };
        let mut applied = state(pending.clone());
        assert_eq!(
            reconcile_pending(&observation(9, 115), &mut applied, &pending),
            WbAutomationExecutionOutcome::Reconciled
        );
        assert!(applied.pending.is_none());

        let pause = PendingAction {
            reserved_at: now() - ChronoDuration::minutes(10),
            kind: PendingActionKind::PauseCampaignForDailyCap,
        };
        let mut paused = state(pause.clone());
        assert_eq!(
            reconcile_pending(&observation(11, 115), &mut paused, &pause),
            WbAutomationExecutionOutcome::Reconciled
        );
        assert_eq!(paused.paused_for_daily_cap_on, Some(now().date_naive()));
    }

    /// A pause reserved just before the Yekaterinburg business-date rollover is
    /// reconciled by the next run, which already sees the following business
    /// date. Recording the reconciliation date rather than the reservation date
    /// would make `paused_by_automation` (`paused_on < business_date`) false for
    /// the whole of the new day, so the campaign would stay paused an extra day.
    #[test]
    fn daily_pause_reconciled_after_rollover_records_the_reservation_date() {
        // 18:50 UTC is 23:50 in Yekaterinburg: still business date 2026-08-25.
        let reserved_at = Utc.with_ymd_and_hms(2026, 8, 25, 18, 50, 0).unwrap();
        // 19:30 UTC is 00:30 the next day: business date 2026-08-26.
        let reconciled_at = Utc.with_ymd_and_hms(2026, 8, 25, 19, 30, 0).unwrap();
        let reserved_date = crate::reporting::business_date(reserved_at);
        let reconciled_date = crate::reporting::business_date(reconciled_at);
        assert_eq!(reserved_date.succ_opt().unwrap(), reconciled_date);

        let pause = PendingAction {
            reserved_at,
            kind: PendingActionKind::PauseCampaignForDailyCap,
        };
        let mut paused = state(pause.clone());
        // `run_once` rolls the stored business date forward before reconciling.
        paused.business_date = reconciled_date;
        assert_eq!(
            reconcile_pending(&observation(11, 115), &mut paused, &pause),
            WbAutomationExecutionOutcome::Reconciled
        );

        assert_eq!(
            paused.paused_for_daily_cap_on,
            Some(reserved_date),
            "the pause belongs to the business date it was reserved on"
        );
        assert!(
            paused
                .paused_for_daily_cap_on
                .is_some_and(|paused_on| paused_on < reconciled_date),
            "the campaign must be resumable on the new business date, not the day after"
        );
    }

    #[test]
    fn ambiguous_write_waits_then_locks_without_retry() {
        let pending = PendingAction {
            reserved_at: now() - ChronoDuration::minutes(1),
            kind: PendingActionKind::ResumeCampaignAfterDailyCap,
        };
        let mut waiting = state(pending.clone());
        assert_eq!(
            reconcile_pending(&observation(11, 100), &mut waiting, &pending),
            WbAutomationExecutionOutcome::AwaitingReadback
        );
        waiting.pending.as_mut().unwrap().reserved_at = now() - ChronoDuration::minutes(10);
        let stale = waiting.pending.clone().unwrap();
        assert_eq!(
            reconcile_pending(&observation(11, 100), &mut waiting, &stale),
            WbAutomationExecutionOutcome::IncidentLocked
        );
        assert_eq!(
            waiting.incident_class.as_deref(),
            Some("write_not_reconciled")
        );
    }

    #[test]
    fn decisions_reserve_exact_pending_actions_or_hold() {
        let observed = observation(9, 200);
        let at = now();
        assert!(
            pending_from_decision(
                &WbAutomationAction::Hold {
                    reason: WbAutomationHoldReason::NoMaterialChange
                },
                &observed,
                102,
                at
            )
            .is_none()
        );

        let change = WbAutomationBidChange {
            nm_id: 1,
            from_bid_kopecks: 200,
            to_bid_kopecks: 170,
            reason: WbAutomationBidReason::TargetDrrExceeded,
        };
        assert!(matches!(
            pending_from_decision(
                &WbAutomationAction::ChangeBids {
                    changes: vec![change.clone()]
                },
                &observed,
                102,
                at
            )
            .unwrap()
            .kind,
            PendingActionKind::ChangeBids { .. }
        ));
        assert!(
            pending_from_decision(
                &WbAutomationAction::ChangeBids {
                    changes: vec![change.clone(), change],
                },
                &observed,
                102,
                at,
            )
            .is_none(),
            "the executor independently refuses multi-SKU decisions"
        );
        // Defence in depth. The decision engine no longer emits a stop for a
        // SKU already at the floor, so this guard is unreachable through
        // `run_once`; it stays because reserving a write that changes nothing
        // would burn an action from the daily quota for no effect.
        assert!(
            pending_from_decision(
                &WbAutomationAction::DisableSku {
                    nm_id: 1,
                    reason: WbAutomationDisableReason::LowStock,
                },
                &observation(9, 102),
                102,
                at,
            )
            .is_none(),
            "a stop on an already floored SKU reserves no write"
        );

        for (reason, expected) in [
            (
                WbAutomationDisableReason::LowStock,
                WbAutomationBidReason::LowStockGuard,
            ),
            (
                WbAutomationDisableReason::NoOrdersHardStop,
                WbAutomationBidReason::NoOrdersHardStop,
            ),
            (
                WbAutomationDisableReason::HardDrrExceeded,
                WbAutomationBidReason::HardDrrExceeded,
            ),
        ] {
            let pending = pending_from_decision(
                &WbAutomationAction::DisableSku { nm_id: 1, reason },
                &observed,
                102,
                at,
            )
            .unwrap();
            assert!(matches!(
                pending.kind,
                PendingActionKind::ChangeBids { ref changes }
                    if changes[0].reason == expected && changes[0].to_bid_kopecks == 102
            ));
        }
        assert!(
            pending_from_decision(
                &WbAutomationAction::DisableSku {
                    nm_id: 1,
                    reason: WbAutomationDisableReason::LowStock
                },
                &observation(9, 102),
                102,
                at
            )
            .is_none()
        );
        assert!(
            pending_from_decision(
                &WbAutomationAction::DisableSku {
                    nm_id: 2,
                    reason: WbAutomationDisableReason::LowStock
                },
                &observed,
                102,
                at
            )
            .is_none()
        );
        assert!(matches!(
            pending_from_decision(
                &WbAutomationAction::PauseCampaignForDailyCap,
                &observed,
                102,
                at
            )
            .unwrap()
            .kind,
            PendingActionKind::PauseCampaignForDailyCap
        ));
        assert!(matches!(
            pending_from_decision(
                &WbAutomationAction::ResumeCampaignAfterDailyCap,
                &observed,
                102,
                at
            )
            .unwrap()
            .kind,
            PendingActionKind::ResumeCampaignAfterDailyCap
        ));
    }

    #[test]
    fn state_files_are_private_bounded_and_policy_bound() {
        let fixture = Fixture::new();
        let business_date = now().date_naive();
        assert!(
            read_state_file(&fixture.root.join("execution-state.json"))
                .unwrap()
                .is_none()
        );
        let initial = load_execution_state(
            &fixture.root,
            "a",
            "ip_domnyshev_wb",
            39_682_633,
            business_date,
            false,
        )
        .unwrap();
        assert_eq!(initial.schema_version, STATE_SCHEMA_VERSION);

        let mut stored = execution_state(business_date);
        stored.policy_sha256 = "a".to_owned();
        let pending = PendingAction {
            reserved_at: now(),
            kind: PendingActionKind::PauseCampaignForDailyCap,
        };
        stored.pending = Some(pending.clone());
        save_execution_state(&fixture.root, &stored).unwrap();
        assert_eq!(
            load_execution_state(
                &fixture.root,
                "a",
                "ip_domnyshev_wb",
                39_682_633,
                business_date,
                false
            )
            .unwrap(),
            stored
        );
        verify_pending_permit(&fixture.root.join("execution-state.json"), &pending).unwrap();
        let different = PendingAction {
            reserved_at: now(),
            kind: PendingActionKind::ResumeCampaignAfterDailyCap,
        };
        assert!(
            verify_pending_permit(&fixture.root.join("execution-state.json"), &different).is_err()
        );
        assert!(
            load_execution_state(
                &fixture.root,
                "wrong",
                "ip_domnyshev_wb",
                39_682_633,
                business_date,
                false
            )
            .is_err()
        );
        let migrated = load_execution_state(
            &fixture.root,
            "shadow-policy",
            "ip_domnyshev_wb",
            39_682_633,
            business_date,
            true,
        )
        .unwrap();
        assert_eq!(migrated.policy_sha256, "shadow-policy");
        assert_eq!(migrated.pending.as_ref(), Some(&pending));
        assert_eq!(migrated.actions_today, stored.actions_today);

        fs::write(fixture.root.join("execution-state.json"), b"not-json").unwrap();
        assert!(read_state_file(&fixture.root.join("execution-state.json")).is_err());

        let regular_parent = fixture.root.join("regular-parent");
        fs::write(&regular_parent, b"not a directory").unwrap();
        assert!(read_state_file(&regular_parent.join("child")).is_err());

        assert!(
            save_execution_state_bytes(&fixture.root, b"{}", |_, _| Err(anyhow::anyhow!(
                "injected write failure"
            )))
            .is_err()
        );
        fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(validate_private_directory(&fixture.root).is_err());
        fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn resume_readback_clears_daily_pause_and_missing_sku_is_not_visible() {
        let pending = PendingAction {
            reserved_at: now() - ChronoDuration::minutes(10),
            kind: PendingActionKind::ResumeCampaignAfterDailyCap,
        };
        let mut resumed = state(pending.clone());
        resumed.paused_for_daily_cap_on = Some(now().date_naive());
        assert_eq!(
            reconcile_pending(&observation(9, 100), &mut resumed, &pending),
            WbAutomationExecutionOutcome::Reconciled
        );
        assert!(resumed.paused_for_daily_cap_on.is_none());
        assert!(!bid_change_is_visible(
            &observation(9, 100),
            &WbAutomationBidChange {
                nm_id: 2,
                from_bid_kopecks: 100,
                to_bid_kopecks: 115,
                reason: WbAutomationBidReason::LowExposureExploration,
            }
        ));
    }

    #[tokio::test]
    async fn executor_writes_once_reconciles_and_locks_incidents() {
        let fixture = Fixture::new();
        let (reader_url, _) = reader_server(9, 102, "2026-08-25", Some(0), 10);
        let (writer_url, writer_requests) = mock_http(vec![(200, "{}".to_owned())]);
        let mut executor = fixture.executor(&reader_url, &writer_url);
        assert!(format!("{executor:?}").contains("ip_domnyshev_wb"));

        let first = executor.run_once(now()).await.unwrap();
        assert_eq!(
            first.outcome,
            WbAutomationExecutionOutcome::WriteSentReconciliationRequired
        );
        assert!(
            writer_requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with("PATCH /api/advert/v1/bids")
        );

        let (reconcile_url, _) = reader_server(9, 117, "2026-08-25", None, 10);
        executor
            .observer
            .replace_client_for_test(WbClient::new_for_test(
                Duration::from_secs(2),
                BTreeMap::from([(
                    "ip_domnyshev_wb".to_owned(),
                    WbCredentials {
                        token: "test-reader".to_owned(),
                    },
                )]),
                &reconcile_url,
                &reconcile_url,
            ));
        let reconciled = executor
            .run_once(now() + ChronoDuration::minutes(1))
            .await
            .unwrap();
        assert_eq!(reconciled.outcome, WbAutomationExecutionOutcome::Reconciled);

        let mut locked = read_state_file(&fixture.root.join("execution-state.json"))
            .unwrap()
            .unwrap();
        locked.incident_class = Some("manual_test_lock".to_owned());
        save_execution_state(&fixture.root, &locked).unwrap();
        let (locked_url, _) = reader_server(9, 117, "2026-08-25", None, 10);
        executor
            .observer
            .replace_client_for_test(WbClient::new_for_test(
                Duration::from_secs(2),
                BTreeMap::from([(
                    "ip_domnyshev_wb".to_owned(),
                    WbCredentials {
                        token: "test-reader".to_owned(),
                    },
                )]),
                &locked_url,
                &locked_url,
            ));
        assert_eq!(
            executor
                .run_once(now() + ChronoDuration::minutes(2))
                .await
                .unwrap()
                .outcome,
            WbAutomationExecutionOutcome::IncidentLocked
        );
    }

    #[tokio::test]
    async fn low_stock_at_minimum_is_observed_without_a_write() {
        let fixture = Fixture::new();
        let (reader_url, _) = reader_server(9, 102, "2026-08-25", None, 3);
        let executor = fixture.executor(&reader_url, "http://127.0.0.1:1");
        assert_eq!(
            executor.run_once(now()).await.unwrap().outcome,
            WbAutomationExecutionOutcome::Observed
        );
    }

    #[tokio::test]
    async fn pause_and_resume_pending_actions_use_guarded_status_writes() {
        let fixture = Fixture::new();
        let mut stored = execution_state(now().date_naive());
        stored.policy_sha256 = "unused".to_owned();

        for (kind, expected_path) in [
            (
                PendingActionKind::PauseCampaignForDailyCap,
                "GET /adv/v0/pause?id=39682633",
            ),
            (
                PendingActionKind::ResumeCampaignAfterDailyCap,
                "GET /adv/v0/start?id=39682633",
            ),
        ] {
            let pending = PendingAction {
                reserved_at: now(),
                kind,
            };
            stored.pending = Some(pending.clone());
            save_execution_state(&fixture.root, &stored).unwrap();
            let (writer_url, requests) = mock_http(vec![(200, "{}".to_owned())]);
            let executor = fixture.executor("http://127.0.0.1:1", &writer_url);
            executor.send_pending(&pending).await.unwrap();
            assert!(
                requests
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .starts_with(expected_path)
            );
        }

        let pending = PendingAction {
            reserved_at: now(),
            kind: PendingActionKind::PauseCampaignForDailyCap,
        };
        stored.pending = Some(pending.clone());
        save_execution_state(&fixture.root, &stored).unwrap();
        let executor = fixture.executor("http://127.0.0.1:1", "http://127.0.0.1:1");
        assert!(executor.send_pending(&pending).await.is_err());
    }

    /// `daily_pause_threshold_minor` (250 RUB) is the soft pause and
    /// `daily_spend_cap_minor` (300 RUB) is the ceiling that pause exists to
    /// defend. Observing spend at or above the ceiling with nothing in flight
    /// means the pause did not hold, so the executor stops automating and
    /// leaves the campaign to an operator instead of issuing further
    /// spend-affecting writes.
    #[tokio::test]
    async fn reaching_the_daily_spend_cap_locks_an_incident_without_writing() {
        let fixture = Fixture::new();
        let (reader_url, _) = reader_server(9, 102, "2026-08-25", Some(300), 10);
        // An unroutable writer proves no write is attempted on this path.
        let executor = fixture.executor(&reader_url, "http://127.0.0.1:1");

        let receipt = executor.run_once(now()).await.unwrap();

        assert_eq!(
            receipt.outcome,
            WbAutomationExecutionOutcome::IncidentLocked
        );
        let state = read_state_file(&fixture.root.join("execution-state.json"))
            .unwrap()
            .unwrap();
        assert_eq!(
            state.incident_class.as_deref(),
            Some("daily_spend_cap_breached")
        );
        assert!(
            state.pending.is_none(),
            "a breach must not reserve a further write"
        );

        // The lock is sticky: a later run reports the incident and still does
        // not act, even though the reader now serves a clean observation.
        let (clean_url, _) = reader_server(9, 102, "2026-08-25", None, 10);
        let relocked = fixture.executor(&clean_url, "http://127.0.0.1:1");
        assert_eq!(
            relocked
                .run_once(now() + ChronoDuration::minutes(1))
                .await
                .unwrap()
                .outcome,
            WbAutomationExecutionOutcome::IncidentLocked
        );
    }

    /// Spend below the ceiling still follows the ordinary soft-pause path.
    #[tokio::test]
    async fn spend_below_the_daily_cap_still_pauses_rather_than_locking() {
        let fixture = Fixture::new();
        let (reader_url, _) = reader_server(9, 102, "2026-08-25", Some(250), 10);
        let (writer_url, writer_requests) = mock_http(vec![(200, "{}".to_owned())]);
        let executor = fixture.executor(&reader_url, &writer_url);

        let receipt = executor.run_once(now()).await.unwrap();

        assert_eq!(
            receipt.outcome,
            WbAutomationExecutionOutcome::WriteSentReconciliationRequired
        );
        assert!(
            writer_requests
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .starts_with("GET /adv/v0/pause")
        );
        let state = read_state_file(&fixture.root.join("execution-state.json"))
            .unwrap()
            .unwrap();
        assert!(state.incident_class.is_none());
    }

    #[tokio::test]
    async fn action_counter_overflow_fails_before_any_write() {
        let fixture = Fixture::new();
        let (reader_url, _) = reader_server(9, 102, "2026-08-25", Some(250), 10);
        let executor = fixture.executor(&reader_url, "http://127.0.0.1:1");
        let mut stored = execution_state(now().date_naive());
        stored.policy_sha256 = executor.observer.policy_sha256().to_owned();
        stored.actions_today = u32::MAX;
        save_execution_state(&fixture.root, &stored).unwrap();
        assert!(executor.run_once(now()).await.is_err());
    }

    #[tokio::test]
    async fn executor_rejects_invalid_inputs_and_resets_a_previous_business_day() {
        let invalid_fixture = Fixture::new();
        let mut invalid_policy = serde_json::from_slice::<serde_json::Value>(include_bytes!(
            "../../config/wb-automation-robot.json"
        ))
        .unwrap();
        invalid_policy["allow_budget_top_up"] = serde_json::Value::Bool(true);
        fs::write(
            &invalid_fixture.policy,
            serde_json::to_vec(&invalid_policy).unwrap(),
        )
        .unwrap();
        assert!(
            WbAutomationExecutor::from_files(
                &invalid_fixture.policy,
                &invalid_fixture.registry,
                &invalid_fixture.reader_token,
                &invalid_fixture.writer_token,
                &invalid_fixture.root,
                false,
                Duration::from_secs(2),
                None,
                "http://127.0.0.1:3128",
            )
            .is_err()
        );

        let mismatch_fixture = Fixture::new();
        let (unused_reader, _) = reader_server(9, 102, "2026-08-25", None, 3);
        let mismatch_executor = mismatch_fixture.executor(&unused_reader, "http://127.0.0.1:1");
        let mut mismatch = execution_state(now().date_naive());
        mismatch.policy_sha256 = "wrong".to_owned();
        save_execution_state(&mismatch_fixture.root, &mismatch).unwrap();
        assert!(mismatch_executor.run_once(now()).await.is_err());

        let reset_fixture = Fixture::new();
        let (reader_url, _) = reader_server(9, 102, "2026-08-25", None, 3);
        let reset_executor = reset_fixture.executor(&reader_url, "http://127.0.0.1:1");
        let mut previous = execution_state(now().date_naive().pred_opt().unwrap());
        previous.policy_sha256 = reset_executor.observer.policy_sha256().to_owned();
        previous.actions_today = u32::MAX;
        previous.paused_for_daily_cap_on = Some(previous.business_date);
        save_execution_state(&reset_fixture.root, &previous).unwrap();
        assert_eq!(
            reset_executor.run_once(now()).await.unwrap().outcome,
            WbAutomationExecutionOutcome::Observed
        );
        let reset = read_state_file(&reset_fixture.root.join("execution-state.json"))
            .unwrap()
            .unwrap();
        assert_eq!(reset.business_date, now().date_naive());
        assert_eq!(reset.actions_today, 0);
    }
}
