//! Private execution-state files and pending-action reconciliation.

use super::{
    DateTime, Deserialize, File, MAX_STATE_BYTES, NaiveDate, OpenOptions, Path, READBACK_GRACE,
    Result, STATE_SCHEMA_VERSION, Serialize, Sha256, Utc, WbAutomationAction,
    WbAutomationBidChange, WbAutomationExecutionOutcome, Write, ensure,
    wb_automation_business_date,
};
use anyhow::Context;
use sha2::Digest;
use std::fs;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

pub(super) fn sha256_domain(domain: &str, bytes: &[u8]) -> String {
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
pub(super) struct ExecutionState {
    pub(super) schema_version: u32,
    pub(super) policy_sha256: String,
    pub(super) account_id: String,
    pub(super) campaign_id: u64,
    pub(super) business_date: NaiveDate,
    pub(super) actions_today: u32,
    pub(super) last_action_at: Option<DateTime<Utc>>,
    pub(super) paused_for_daily_cap_on: Option<NaiveDate>,
    pub(super) pending: Option<PendingAction>,
    pub(super) incident_class: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PendingAction {
    pub(super) reserved_at: DateTime<Utc>,
    pub(super) kind: PendingActionKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum PendingActionKind {
    ChangeBids { changes: Vec<WbAutomationBidChange> },
    PauseCampaignForDailyCap,
    ResumeCampaignAfterDailyCap,
}

pub(super) fn pending_from_decision(
    action: &WbAutomationAction,
    observation: &super::super::automation::WbAutomationObservation,
    policy_min_bid_kopecks: u64,
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
            let sku = observation.skus.iter().find(|sku| sku.nm_id == *nm_id)?;
            let current = sku.current_bid_kopecks;
            let effective_minimum = policy_min_bid_kopecks.max(sku.minimum_bid_kopecks);
            if current <= effective_minimum {
                return None;
            }
            PendingActionKind::ChangeBids {
                changes: vec![WbAutomationBidChange {
                    nm_id: *nm_id,
                    from_bid_kopecks: current,
                    to_bid_kopecks: effective_minimum,
                    reason: match reason {
                        super::super::automation::WbAutomationDisableReason::LowStock => {
                            super::super::automation::WbAutomationBidReason::LowStockGuard
                        }
                        super::super::automation::WbAutomationDisableReason::NoOrdersHardStop => {
                            super::super::automation::WbAutomationBidReason::NoOrdersHardStop
                        }
                        super::super::automation::WbAutomationDisableReason::HardDrrExceeded => {
                            super::super::automation::WbAutomationBidReason::HardDrrExceeded
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

pub(super) fn reconcile_pending(
    observation: &super::super::automation::WbAutomationObservation,
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

pub(super) fn bid_change_is_visible(
    observation: &super::super::automation::WbAutomationObservation,
    change: &WbAutomationBidChange,
) -> bool {
    observation
        .skus
        .iter()
        .find(|sku| sku.nm_id == change.nm_id)
        .is_some_and(|sku| sku.current_bid_kopecks == change.to_bid_kopecks)
}

pub(super) fn verify_pending_permit(path: &Path, expected: &PendingAction) -> Result<()> {
    let state = read_state_file(path)?.context("WB automation execution state исчез")?;
    ensure!(
        state.incident_class.is_none() && state.pending.as_ref() == Some(expected),
        "WB automation final permit отозван"
    );
    Ok(())
}

pub(super) fn load_execution_state(
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

pub(super) fn read_state_file(path: &Path) -> Result<Option<ExecutionState>> {
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

pub(super) fn save_execution_state(directory: &Path, state: &ExecutionState) -> Result<()> {
    validate_private_directory(directory)?;
    let bytes = serde_json::to_vec_pretty(state)
        .context("WB automation execution state нельзя сериализовать")?;
    ensure!(
        bytes.len() as u64 <= MAX_STATE_BYTES,
        "WB automation execution state слишком велик"
    );
    save_execution_state_bytes(directory, &bytes, write_state)
}

pub(super) fn save_execution_state_bytes(
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

pub(super) fn write_state(file: &mut File, bytes: &[u8]) -> Result<()> {
    file.write_all(bytes)
        .context("WB automation execution state нельзя записать")?;
    file.write_all(b"\n")
        .context("WB automation execution state нельзя завершить")?;
    file.sync_all()
        .context("WB automation execution state нельзя синхронизировать")
}

pub(super) fn validate_private_directory(path: &Path) -> Result<()> {
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
