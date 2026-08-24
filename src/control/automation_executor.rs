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

use crate::{
    config::{Marketplace, RegistrySource},
    control::policy::WbBidPlacement,
};

use super::{
    automation::{WbAutomationAction, WbAutomationBidChange, WbAutomationDecision},
    automation_observer::{
        WbAutomationObserver, WbAutomationStateView, persist_wb_automation_snapshot,
    },
    config::{read_control_token, validate_wb_writer_token},
    wb::{WbBidWriteClient, WbPreparedBidChange},
};

const STATE_SCHEMA_VERSION: u32 = 1;
const MAX_STATE_BYTES: u64 = 256 * 1024;
const READBACK_GRACE: ChronoDuration = ChronoDuration::minutes(5);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WbAutomationExecutionOutcome {
    Observed,
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
        let business_date = crate::reporting::business_date(observed_at);
        let mut state = load_execution_state(
            &self.state_directory,
            self.observer.policy_sha256(),
            self.observer.policy().account_id.as_str(),
            self.observer.policy().campaign_id,
            business_date,
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
        WbAutomationAction::ChangeBids { changes } => PendingActionKind::ChangeBids {
            changes: changes.clone(),
        },
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
                state.paused_for_daily_cap_on = Some(state.business_date);
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
) -> Result<ExecutionState> {
    let path = directory.join("execution-state.json");
    let Some(state) = read_state_file(&path)? else {
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
            && state.policy_sha256 == policy_sha256
            && state.account_id == account_id
            && state.campaign_id == campaign_id,
        "WB automation execution state не соответствует policy"
    );
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
    let temporary = directory.join(format!(".execution-state-{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .context("WB automation temporary execution state недоступен")?;
    if let Err(error) = write_state(&mut file, &bytes) {
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
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::control::automation::{
        WbAutomationBidReason, WbAutomationObservation, WbAutomationSkuObservation,
    };

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
            actions_today: 0,
            last_action_at: None,
            attribution_complete: true,
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
}
