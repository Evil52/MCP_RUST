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
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::{
        control::automation::{
            WbAutomationBidReason, WbAutomationDisableReason, WbAutomationHoldReason,
            WbAutomationObservation, WbAutomationSkuObservation,
        },
        test_support::mock_http,
        wb::{WbClient, WbCredentials},
    };

    const TEST_SELLER_SID: &str = "123e4567-e89b-42d3-a456-426614174000";
    static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    struct Fixture {
        root: PathBuf,
        policy: PathBuf,
        registry: PathBuf,
        reader_token: PathBuf,
        writer_token: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
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
            fs::write(
                &policy,
                include_bytes!("../../config/wb-automation-robot.json"),
            )
            .unwrap();
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

    fn campaign_response(status: i32, bid: u64) -> serde_json::Value {
        serde_json::json!({
            "adverts": [{
                "id": 39_682_633,
                "status": status,
                "bid_type": "manual",
                "settings": {
                    "name": "Робот",
                    "payment_type": "cpc",
                    "placements": {"search": true, "recommendations": false}
                },
                "nm_settings": [
                    {"nm_id": 449_627_598_u64, "bids_kopecks": {"search": bid, "recommendations": 0}},
                    {"nm_id": 449_627_015_u64, "bids_kopecks": {"search": bid, "recommendations": 0}},
                    {"nm_id": 497_424_314_u64, "bids_kopecks": {"search": bid, "recommendations": 0}}
                ]
            }]
        })
    }

    fn reader_server(
        status: i32,
        bid: u64,
        current_date: &str,
        daily_spend_rubles: Option<u64>,
        stock: u64,
    ) -> (String, std::sync::mpsc::Receiver<String>) {
        let advertising_payload = daily_spend_rubles.map_or(serde_json::Value::Null, |spend| {
            serde_json::json!([{
                "advertId": 39_682_633,
                "stats": [{
                    "date": current_date,
                    "nm_id": 449_627_598_u64,
                    "views": 10,
                    "clicks": 1,
                    "sum": spend,
                    "orders": 0,
                    "sumPrice": 0
                }]
            }])
        });
        mock_http(vec![
            (200, campaign_response(status, bid).to_string()),
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
                    changes: vec![change]
                },
                &observed,
                102,
                at
            )
            .unwrap()
            .kind,
            PendingActionKind::ChangeBids { .. }
        ));
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
                business_date
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
                business_date
            )
            .is_err()
        );

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
        let (reader_url, _) = reader_server(9, 102, "2026-08-25", None, 10);
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
