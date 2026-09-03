#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use mcp_ozon::{
    control::{
        ControlAppConfig, ControlMode, OzonAdsWriteClient, OzonBidPacingAction,
        OzonBidPacingObservation, OzonBidPacingPolicy, OzonBidPositionReader, OzonCampaignGuard,
        OzonCampaignProduct, OzonCampaignProductsRequest, OzonCampaignStrategy, OzonPlanRepository,
        OzonWriteErrorKind, evaluate_ozon_bid_pacing, evaluate_ozon_campaign_guard,
        validate_ozon_campaign_product_guard,
    },
    ozon_performance::{CampaignProductsQuery, CampaignsQuery, PerformanceClient, StatisticsQuery},
    reporting::{business_date, ozon_adapter::parse_performance_daily_campaigns},
};
use serde::{Deserialize, Serialize};
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const POLL_INTERVAL: Duration = Duration::from_secs(60);
/// Read and write clients have independent pacing gates, while Ozon applies
/// one quota to both. Keep an explicit boundary around every marketplace
/// mutation so a preceding read or following readback cannot burst the shared
/// Performance API quota.
const WRITE_BOUNDARY_INTERVAL: Duration = Duration::from_secs(2);
const STATIC_GUARDS_FILE_ENV: &str = "CONTROL_MCP_OZON_STATIC_GUARDS_FILE";
const STATIC_STATE_FILE_ENV: &str = "CONTROL_MCP_OZON_STATIC_GUARD_STATE_FILE";
const POSITION_DATABASE_URL_ENV: &str = "CONTROL_MCP_OZON_POSITION_DATABASE_URL";
const MAX_STATIC_GUARDS: usize = 50;
const RECONCILE_COMMAND: &str = "reconcile-static-once";
const RECONCILE_CONFIRMATION: &str = "--confirm-static-bid-corridor-and-activation";
const AUDIT_COMMAND: &str = "audit-static-once";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Command {
    Serve,
    AuditStaticOnce,
    ReconcileStaticOnce,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticGuardFile {
    account_id: String,
    #[serde(default)]
    dynamic_bid_control: Option<StaticDynamicBidControl>,
    guards: Vec<StaticGuard>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticDynamicBidControl {
    position_store_id: String,
    position_region_name: String,
    target_position: u16,
    bid_step_microrubles: u64,
    cooldown_seconds: u64,
    max_position_age_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticGuard {
    campaign_id: u64,
    sku: u64,
    date_from: String,
    spend_cap_microrubles: u64,
    target_drr_percent: u8,
    #[serde(default = "default_min_cpc_bid_microrubles")]
    min_cpc_bid_microrubles: u64,
    #[serde(default = "default_max_cpc_bid_microrubles")]
    max_cpc_bid_microrubles: u64,
}

const fn default_min_cpc_bid_microrubles() -> u64 {
    7_000_000
}

const fn default_max_cpc_bid_microrubles() -> u64 {
    12_000_000
}

#[derive(Debug)]
struct StaticCampaignGuard {
    guard: OzonCampaignGuard,
    min_cpc_bid_microrubles: u64,
    max_cpc_bid_microrubles: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PendingStaticBidChange {
    from_microrubles: u64,
    to_microrubles: u64,
    started_at: DateTime<Utc>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StaticGuardState {
    incident_campaign_ids: BTreeSet<u64>,
    #[serde(default)]
    last_bid_change_at: BTreeMap<u64, DateTime<Utc>>,
    #[serde(default)]
    pending_bid_changes: BTreeMap<u64, PendingStaticBidChange>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ozon_campaign_guard=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    let command = match env::args().skip(1).collect::<Vec<_>>().as_slice() {
        [] => Command::Serve,
        [command] if command == AUDIT_COMMAND => Command::AuditStaticOnce,
        [command, confirmation]
            if command == RECONCILE_COMMAND && confirmation == RECONCILE_CONFIRMATION =>
        {
            Command::ReconcileStaticOnce
        }
        _ => bail!(
            "usage: ozon-campaign-guard [{AUDIT_COMMAND}|{RECONCILE_COMMAND} {RECONCILE_CONFIRMATION}]"
        ),
    };

    let config = ControlAppConfig::from_env()?;
    if config.policy.mode != ControlMode::Enabled {
        bail!("Ozon campaign guard требует enabled policy");
    }
    let runtime = config
        .ozon_runtime
        .context("Ozon campaign guard требует Ozon runtime")?;
    if !runtime.writer_enabled {
        bail!("Ozon campaign guard требует armed writer");
    }
    let credentials = BTreeMap::from([(runtime.store_id.clone(), runtime.credentials.clone())]);
    let reader = Arc::new(PerformanceClient::new_with_https_proxy(
        runtime.request_timeout,
        credentials,
        &runtime.proxy_url,
    )?);
    let writer = Arc::new(OzonAdsWriteClient::new(
        runtime.request_timeout,
        runtime.credentials,
        &runtime.proxy_url,
    )?);

    if let Some(static_guards_path) = env::var_os(STATIC_GUARDS_FILE_ENV) {
        let state_path = env::var_os(STATIC_STATE_FILE_ENV)
            .map(PathBuf::from)
            .context("static Ozon guard требует state file")?;
        let (static_guards, dynamic_bid_control) =
            load_static_guards(Path::new(&static_guards_path), &runtime.account_id)?;
        let position_reader = if dynamic_bid_control.is_some() {
            let database_url = env::var(POSITION_DATABASE_URL_ENV)
                .context("dynamic Ozon bid control requires position database")?;
            let reader = Arc::new(OzonBidPositionReader::connect(&database_url).await?);
            reader.verify_runtime_contract().await?;
            Some(reader)
        } else {
            None
        };
        let mut state = load_static_state(&state_path)?;
        tracing::info!(
            account_id=%runtime.account_id,
            guards=static_guards.len(),
            dynamic_bid_control=dynamic_bid_control.is_some(),
            "static Ozon campaign guard armed"
        );
        if command == Command::AuditStaticOnce {
            audit_static_campaigns(&static_guards, &reader, &runtime.store_id).await?;
            return Ok(());
        }
        if command == Command::ReconcileStaticOnce {
            reconcile_static_campaigns(&static_guards, &reader, &writer, &runtime.store_id).await?;
            return Ok(());
        }
        loop {
            if let Err(error) = guard_once_static(
                &static_guards,
                &mut state,
                &state_path,
                &reader,
                &writer,
                &runtime.store_id,
                dynamic_bid_control.as_ref(),
                position_reader.as_deref(),
            )
            .await
            {
                tracing::error!(%error, "static Ozon guard cycle failed");
            }
            tokio::select! {
                () = tokio::time::sleep(POLL_INTERVAL) => {}
                () = shutdown_signal() => break,
            }
        }
        return Ok(());
    }

    if command != Command::Serve {
        bail!("static reconcile command requires static guard config");
    }

    let database = &config
        .policy_database
        .context("Ozon campaign guard требует plan database")?
        .database;
    let plans = Arc::new(OzonPlanRepository::connect(database).await?);
    plans.verify_runtime_contract().await?;
    tracing::info!(account_id=%runtime.account_id, "Ozon campaign guard started");
    loop {
        if let Err(error) = guard_once(&plans, &reader, &writer, &runtime.store_id).await {
            tracing::error!(%error, "Ozon campaign guard cycle failed");
        }
        tokio::select! {
            () = tokio::time::sleep(POLL_INTERVAL) => {}
            () = shutdown_signal() => break,
        }
    }
    Ok(())
}

fn load_static_guards(
    path: &Path,
    expected_account_id: &str,
) -> Result<(Vec<StaticCampaignGuard>, Option<StaticDynamicBidControl>)> {
    let bytes = fs::read(path).context("static Ozon guard config недоступен")?;
    if bytes.is_empty() || bytes.len() > 64 * 1024 {
        bail!("static Ozon guard config имеет недопустимый размер");
    }
    let config: StaticGuardFile =
        serde_json::from_slice(&bytes).context("static Ozon guard config имеет неверный формат")?;
    if config.account_id != expected_account_id
        || config.guards.is_empty()
        || config.guards.len() > MAX_STATIC_GUARDS
    {
        bail!("static Ozon guard config не совпадает с runtime scope");
    }
    if let Some(dynamic) = config.dynamic_bid_control.as_ref()
        && (dynamic.position_store_id != expected_account_id
            || dynamic.position_region_name.is_empty()
            || dynamic.position_region_name.trim() != dynamic.position_region_name
            || dynamic.position_region_name.len() > 128
            || dynamic.position_region_name.chars().any(char::is_control)
            || dynamic.target_position == 0
            || dynamic.bid_step_microrubles == 0
            || !dynamic.bid_step_microrubles.is_multiple_of(1_000_000)
            || dynamic.cooldown_seconds != 1_800
            || dynamic.max_position_age_seconds < dynamic.cooldown_seconds)
    {
        bail!("static Ozon dynamic bid control has invalid bounds");
    }
    let dynamic_bid_control = config.dynamic_bid_control;
    let mut campaigns = BTreeSet::new();
    let mut skus = BTreeSet::new();
    let guards = config
        .guards
        .into_iter()
        .map(|guard| {
            if guard.campaign_id == 0
                || guard.sku == 0
                || !campaigns.insert(guard.campaign_id)
                || !skus.insert(guard.sku)
                || chrono::NaiveDate::parse_from_str(&guard.date_from, "%Y-%m-%d").is_err()
                || evaluate_ozon_campaign_guard(
                    0,
                    0,
                    guard.spend_cap_microrubles,
                    guard.target_drr_percent,
                )
                .is_err()
                || guard.min_cpc_bid_microrubles == 0
                || guard.min_cpc_bid_microrubles > guard.max_cpc_bid_microrubles
                || !guard.min_cpc_bid_microrubles.is_multiple_of(1_000_000)
                || !guard.max_cpc_bid_microrubles.is_multiple_of(1_000_000)
            {
                bail!("static Ozon guard item имеет недопустимые данные");
            }
            Ok(StaticCampaignGuard {
                guard: OzonCampaignGuard {
                    plan_id: format!("static-{}", guard.campaign_id),
                    account_id: expected_account_id.to_owned(),
                    sku: guard.sku,
                    campaign_id: guard.campaign_id,
                    date_from: guard.date_from,
                    spend_cap_microrubles: guard.spend_cap_microrubles,
                    target_drr_percent: guard.target_drr_percent,
                },
                min_cpc_bid_microrubles: guard.min_cpc_bid_microrubles,
                max_cpc_bid_microrubles: guard.max_cpc_bid_microrubles,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((guards, dynamic_bid_control))
}

fn load_static_state(path: &Path) -> Result<StaticGuardState> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).context("static Ozon guard state повреждён"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(StaticGuardState::default())
        }
        Err(error) => Err(error).context("static Ozon guard state недоступен"),
    }
}

fn persist_static_state(path: &Path, state: &StaticGuardState) -> Result<()> {
    let parent = path
        .parent()
        .context("static Ozon guard state path invalid")?;
    fs::create_dir_all(parent).context("static Ozon guard state directory unavailable")?;
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, serde_json::to_vec(state)?)
        .context("static Ozon guard state write failed")?;
    fs::rename(&temporary, path).context("static Ozon guard state commit failed")
}

#[allow(clippy::too_many_arguments)]
async fn guard_once_static(
    guards: &[StaticCampaignGuard],
    state: &mut StaticGuardState,
    state_path: &Path,
    reader: &Arc<PerformanceClient>,
    writer: &Arc<OzonAdsWriteClient>,
    store: &mcp_ozon::config::StoreId,
    dynamic_bid_control: Option<&StaticDynamicBidControl>,
    position_reader: Option<&OzonBidPositionReader>,
) -> Result<()> {
    let running = running_static_campaigns(reader, store, guards).await?;
    tracing::info!(
        running = running.len(),
        guards = guards.len(),
        "static Ozon guard cycle ready"
    );
    if running.is_empty() {
        return Ok(());
    }
    let metrics = match static_guard_metrics(reader, store, guards, &running).await {
        Ok(metrics) => metrics,
        Err(error) => {
            tracing::warn!(
                running = running.len(),
                %error,
                "statistics unavailable; no-write hold"
            );
            return Ok(());
        }
    };
    for static_guard in guards {
        let guard = &static_guard.guard;
        if state.incident_campaign_ids.contains(&guard.campaign_id)
            || !running.contains(&guard.campaign_id)
        {
            continue;
        }
        let product_snapshot =
            match campaign_product_snapshot(reader, store, guard.campaign_id).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    tracing::warn!(
                        campaign_id = guard.campaign_id,
                        sku = guard.sku,
                        %error,
                        "campaign product read unavailable; no-write hold"
                    );
                    continue;
                }
            };
        let product_guard = validate_ozon_campaign_product_guard(
            &product_snapshot,
            guard.sku,
            static_guard.min_cpc_bid_microrubles,
            static_guard.max_cpc_bid_microrubles,
        );
        let (spend_minor, revenue_minor) =
            metrics.get(&guard.campaign_id).copied().unwrap_or_default();
        let current_bid_microrubles = match product_guard {
            Err(error) => {
                tracing::warn!(campaign_id=guard.campaign_id,sku=guard.sku,%error,"product or bid corridor invalid; fail-closed stop requested");
                if let Err(error) = guard_campaign_static(
                    state,
                    state_path,
                    reader,
                    writer,
                    store,
                    guard,
                    0,
                    0,
                    Some("product_guard_failed"),
                )
                .await
                {
                    tracing::error!(campaign_id=guard.campaign_id,sku=guard.sku,%error,"static Ozon guard item failed");
                }
                tokio::time::sleep(Duration::from_secs(7)).await;
                continue;
            }
            Ok(bid_microrubles) => {
                tracing::debug!(
                    campaign_id = guard.campaign_id,
                    sku = guard.sku,
                    bid_microrubles,
                    "static product guard passed"
                );
                bid_microrubles
            }
        };

        reconcile_pending_static_bid(
            state,
            state_path,
            guard.campaign_id,
            current_bid_microrubles,
        )?;
        if state.incident_campaign_ids.contains(&guard.campaign_id) {
            continue;
        }

        if let Some(dynamic) = dynamic_bid_control {
            let position = match position_reader {
                Some(position_reader) => position_reader
                    .latest_position(
                        &dynamic.position_store_id,
                        guard.sku,
                        &dynamic.position_region_name,
                    )
                    .await
                    .inspect_err(|error| {
                        tracing::warn!(
                            campaign_id = guard.campaign_id,
                            sku = guard.sku,
                            %error,
                            "position unavailable; upward bid changes are held"
                        );
                    })
                    .ok()
                    .flatten(),
                None => None,
            };
            let observed_at = Utc::now();
            let action = evaluate_ozon_bid_pacing(
                OzonBidPacingPolicy {
                    min_bid_microrubles: static_guard.min_cpc_bid_microrubles,
                    max_bid_microrubles: static_guard.max_cpc_bid_microrubles,
                    bid_step_microrubles: dynamic.bid_step_microrubles,
                    spend_cap_microrubles: guard.spend_cap_microrubles,
                    target_drr_percent: guard.target_drr_percent,
                    target_position: dynamic.target_position,
                    cooldown_seconds: dynamic.cooldown_seconds,
                    max_position_age_seconds: dynamic.max_position_age_seconds,
                },
                OzonBidPacingObservation {
                    observed_at,
                    current_bid_microrubles,
                    spend_minor,
                    attributed_revenue_minor: revenue_minor,
                    position,
                    last_bid_change_at: state.last_bid_change_at.get(&guard.campaign_id).copied(),
                },
            )?;
            match action {
                OzonBidPacingAction::Hold(reason) => {
                    tracing::info!(
                        campaign_id = guard.campaign_id,
                        sku = guard.sku,
                        spend_minor,
                        revenue_minor,
                        reason = reason.as_str(),
                        "dynamic Ozon bid hold"
                    );
                }
                OzonBidPacingAction::ChangeBid {
                    from_microrubles,
                    to_microrubles,
                } => {
                    if let Err(error) = change_static_campaign_bid(
                        state,
                        state_path,
                        reader,
                        writer,
                        store,
                        guard,
                        from_microrubles,
                        to_microrubles,
                        observed_at,
                    )
                    .await
                    {
                        tracing::error!(campaign_id=guard.campaign_id,sku=guard.sku,%error,"dynamic Ozon bid change failed");
                    }
                }
                OzonBidPacingAction::Pause(reason) => {
                    if let Err(error) = guard_campaign_static(
                        state,
                        state_path,
                        reader,
                        writer,
                        store,
                        guard,
                        spend_minor,
                        revenue_minor,
                        Some(reason.as_str()),
                    )
                    .await
                    {
                        tracing::error!(campaign_id=guard.campaign_id,sku=guard.sku,%error,"dynamic Ozon campaign pause failed");
                    }
                    tokio::time::sleep(Duration::from_secs(7)).await;
                }
            }
            continue;
        }

        let stop_reason = evaluate_ozon_campaign_guard(
            spend_minor,
            revenue_minor,
            guard.spend_cap_microrubles,
            guard.target_drr_percent,
        )?
        .map(mcp_ozon::control::OzonGuardStopReason::as_str);
        let stop_requested = stop_reason.is_some();
        if let Err(error) = guard_campaign_static(
            state,
            state_path,
            reader,
            writer,
            store,
            guard,
            spend_minor,
            revenue_minor,
            stop_reason,
        )
        .await
        {
            tracing::error!(campaign_id=guard.campaign_id,sku=guard.sku,%error,"static Ozon guard item failed");
        }
        if stop_requested {
            tokio::time::sleep(Duration::from_secs(7)).await;
        }
    }
    Ok(())
}

async fn running_static_campaigns(
    reader: &PerformanceClient,
    store: &mcp_ozon::config::StoreId,
    guards: &[StaticCampaignGuard],
) -> Result<BTreeSet<u64>> {
    let response = reader
        .campaigns(
            store,
            CampaignsQuery {
                campaign_ids: guards.iter().map(|guard| guard.guard.campaign_id).collect(),
                adv_object_type: Some("SKU"),
                state: None,
                page: 1,
                page_size: u32::try_from(guards.len()).context("too many static guards")?,
            },
        )
        .await?;
    let rows = response
        .get("list")
        .and_then(serde_json::Value::as_array)
        .context("invalid static campaign readback")?;
    Ok(rows
        .iter()
        .filter(|row| {
            row.get("state").and_then(serde_json::Value::as_str) == Some("CAMPAIGN_STATE_RUNNING")
        })
        .filter_map(|row| {
            row.get("id").and_then(|value| match value {
                serde_json::Value::String(value) => value.parse().ok(),
                serde_json::Value::Number(value) => value.as_u64(),
                _ => None,
            })
        })
        .collect())
}

async fn campaign_product_snapshot(
    reader: &PerformanceClient,
    store: &mcp_ozon::config::StoreId,
    campaign_id: u64,
) -> Result<serde_json::Value> {
    reader
        .campaign_products(
            store,
            campaign_id,
            CampaignProductsQuery {
                page: 1,
                page_size: 2,
            },
        )
        .await
        .map_err(Into::into)
}

fn reconcile_pending_static_bid(
    state: &mut StaticGuardState,
    state_path: &Path,
    campaign_id: u64,
    current_bid_microrubles: u64,
) -> Result<()> {
    let Some(pending) = state.pending_bid_changes.get(&campaign_id).cloned() else {
        return Ok(());
    };
    if current_bid_microrubles == pending.to_microrubles {
        state.pending_bid_changes.remove(&campaign_id);
        state
            .last_bid_change_at
            .insert(campaign_id, pending.started_at);
        persist_static_state(state_path, state)?;
        tracing::info!(
            campaign_id,
            from_bid_microrubles = pending.from_microrubles,
            to_bid_microrubles = pending.to_microrubles,
            "pending dynamic Ozon bid change reconciled"
        );
    } else {
        state.incident_campaign_ids.insert(campaign_id);
        persist_static_state(state_path, state)?;
        tracing::error!(
            campaign_id,
            expected_bid_microrubles = pending.to_microrubles,
            current_bid_microrubles,
            "pending dynamic Ozon bid readback mismatch; campaign locked"
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn change_static_campaign_bid(
    state: &mut StaticGuardState,
    state_path: &Path,
    reader: &Arc<PerformanceClient>,
    writer: &Arc<OzonAdsWriteClient>,
    store: &mcp_ozon::config::StoreId,
    guard: &OzonCampaignGuard,
    from_microrubles: u64,
    to_microrubles: u64,
    started_at: DateTime<Utc>,
) -> Result<()> {
    state.pending_bid_changes.insert(
        guard.campaign_id,
        PendingStaticBidChange {
            from_microrubles,
            to_microrubles,
            started_at,
        },
    );
    persist_static_state(state_path, state)?;

    let request = OzonCampaignProductsRequest {
        bids: vec![OzonCampaignProduct {
            sku: guard.sku,
            bid: Some(to_microrubles),
            target_cir: None,
            top_position: None,
        }],
    };
    tokio::time::sleep(WRITE_BOUNDARY_INTERVAL).await;
    let write = writer
        .update_products_with_permit(
            guard.campaign_id,
            OzonCampaignStrategy::TargetBids,
            &request,
            || async { Ok::<(), std::convert::Infallible>(()) },
        )
        .await;
    tokio::time::sleep(WRITE_BOUNDARY_INTERVAL).await;
    let readback = campaign_product_snapshot(reader, store, guard.campaign_id)
        .await
        .and_then(|snapshot| exact_static_product_bid(&snapshot, guard.sku));
    if matches!(readback, Ok(actual) if actual == to_microrubles) {
        state.pending_bid_changes.remove(&guard.campaign_id);
        state
            .last_bid_change_at
            .insert(guard.campaign_id, started_at);
        persist_static_state(state_path, state)?;
        tracing::info!(
            campaign_id = guard.campaign_id,
            sku = guard.sku,
            from_bid_microrubles = from_microrubles,
            to_bid_microrubles = to_microrubles,
            write_reported_error = write.is_err(),
            "dynamic Ozon bid changed and read back"
        );
        return Ok(());
    }

    state.incident_campaign_ids.insert(guard.campaign_id);
    persist_static_state(state_path, state)?;
    match write {
        Ok(()) => bail!("dynamic bid readback mismatch; campaign locked"),
        Err(error) => bail!("dynamic bid write failed and readback did not confirm it: {error}"),
    }
}

async fn validate_static_campaign_product(
    reader: &PerformanceClient,
    store: &mcp_ozon::config::StoreId,
    static_guard: &StaticCampaignGuard,
) -> Result<u64> {
    let snapshot = campaign_product_snapshot(reader, store, static_guard.guard.campaign_id).await?;
    validate_ozon_campaign_product_guard(
        &snapshot,
        static_guard.guard.sku,
        static_guard.min_cpc_bid_microrubles,
        static_guard.max_cpc_bid_microrubles,
    )
    .map_err(Into::into)
}

async fn reconcile_static_campaigns(
    guards: &[StaticCampaignGuard],
    reader: &Arc<PerformanceClient>,
    writer: &Arc<OzonAdsWriteClient>,
    store: &mcp_ozon::config::StoreId,
) -> Result<()> {
    for static_guard in guards {
        let guard = &static_guard.guard;
        let snapshot = campaign_product_snapshot(reader, store, guard.campaign_id).await?;
        let current_bid = exact_static_product_bid(&snapshot, guard.sku)?;
        let desired_bid = current_bid.clamp(
            static_guard.min_cpc_bid_microrubles,
            static_guard.max_cpc_bid_microrubles,
        );
        if desired_bid == current_bid {
            validate_ozon_campaign_product_guard(
                &snapshot,
                guard.sku,
                static_guard.min_cpc_bid_microrubles,
                static_guard.max_cpc_bid_microrubles,
            )?;
        } else {
            let request = OzonCampaignProductsRequest {
                bids: vec![OzonCampaignProduct {
                    sku: guard.sku,
                    bid: Some(desired_bid),
                    target_cir: None,
                    top_position: None,
                }],
            };
            tokio::time::sleep(WRITE_BOUNDARY_INTERVAL).await;
            let write = writer
                .update_products_with_permit(
                    guard.campaign_id,
                    OzonCampaignStrategy::TargetBids,
                    &request,
                    || async { Ok::<(), std::convert::Infallible>(()) },
                )
                .await;
            tokio::time::sleep(WRITE_BOUNDARY_INTERVAL).await;
            if let Err(error) = write {
                let readback = validate_static_campaign_product(reader, store, static_guard).await;
                if !matches!(readback, Ok(value) if value == desired_bid) {
                    bail!(
                        "campaign {} bid update became ambiguous: {error}",
                        guard.campaign_id
                    );
                }
            }
            let readback = validate_static_campaign_product(reader, store, static_guard).await?;
            if readback != desired_bid {
                bail!("campaign {} bid readback differs", guard.campaign_id);
            }
            tracing::info!(
                campaign_id = guard.campaign_id,
                sku = guard.sku,
                from_bid_microrubles = current_bid,
                to_bid_microrubles = desired_bid,
                "static Ozon campaign bid reconciled"
            );
        }

        if !campaign_is_running(reader, store, guard.campaign_id).await? {
            tokio::time::sleep(WRITE_BOUNDARY_INTERVAL).await;
            let activation = writer
                .activate_campaign_with_permit(guard.campaign_id, || async {
                    Ok::<(), std::convert::Infallible>(())
                })
                .await;
            tokio::time::sleep(WRITE_BOUNDARY_INTERVAL).await;
            if let Err(error) = activation
                && !matches!(
                    campaign_is_running(reader, store, guard.campaign_id).await,
                    Ok(true)
                )
            {
                bail!(
                    "campaign {} activation became ambiguous: {error}",
                    guard.campaign_id
                );
            }
            if !campaign_is_running(reader, store, guard.campaign_id).await? {
                bail!("campaign {} activation readback failed", guard.campaign_id);
            }
            tracing::info!(
                campaign_id = guard.campaign_id,
                sku = guard.sku,
                "static Ozon campaign activated"
            );
        }
    }
    Ok(())
}

async fn audit_static_campaigns(
    guards: &[StaticCampaignGuard],
    reader: &PerformanceClient,
    store: &mcp_ozon::config::StoreId,
) -> Result<()> {
    let running = running_static_campaigns(reader, store, guards).await?;
    for static_guard in guards {
        let guard = &static_guard.guard;
        let snapshot = campaign_product_snapshot(reader, store, guard.campaign_id).await?;
        let (actual_sku, actual_bid_microrubles) = exact_static_product(&snapshot)?;
        tracing::info!(
            campaign_id = guard.campaign_id,
            expected_sku = guard.sku,
            actual_sku,
            actual_bid_microrubles,
            running = running.contains(&guard.campaign_id),
            "static Ozon campaign audit"
        );
    }
    Ok(())
}

fn exact_static_product_bid(snapshot: &serde_json::Value, expected_sku: u64) -> Result<u64> {
    let (sku, bid) = exact_static_product(snapshot)?;
    if sku != expected_sku {
        bail!("campaign SKU differs from static guard: expected {expected_sku}, actual {sku}");
    }
    Ok(bid)
}

fn exact_static_product(snapshot: &serde_json::Value) -> Result<(u64, u64)> {
    let products = snapshot
        .get("products")
        .and_then(serde_json::Value::as_array)
        .filter(|products| products.len() == 1)
        .context("campaign must contain exactly one product")?;
    let product = &products[0];
    let sku = canonical_wire_u64(product.get("sku")).context("campaign SKU is invalid")?;
    let bid = canonical_wire_u64(product.get("bid")).context("campaign bid is invalid")?;
    Ok((sku, bid))
}

fn canonical_wire_u64(value: Option<&serde_json::Value>) -> Option<u64> {
    match value? {
        serde_json::Value::Number(value) => value.as_u64(),
        serde_json::Value::String(value) => value
            .parse::<u64>()
            .ok()
            .filter(|parsed| parsed.to_string() == *value),
        _ => None,
    }
}

async fn static_guard_metrics(
    reader: &PerformanceClient,
    store: &mcp_ozon::config::StoreId,
    guards: &[StaticCampaignGuard],
    running: &BTreeSet<u64>,
) -> Result<BTreeMap<u64, (u64, u64)>> {
    let date_from = guards
        .iter()
        .filter(|guard| running.contains(&guard.guard.campaign_id))
        .map(|guard| guard.guard.date_from.as_str())
        .min()
        .context("running static guard has no date")?
        .to_owned();
    let date_to = business_date(Utc::now()).format("%Y-%m-%d").to_string();
    let response = reader
        .daily_statistics(
            store,
            StatisticsQuery {
                campaign_ids: running.iter().copied().collect(),
                date_from,
                date_to,
            },
        )
        .await?;
    let mut metrics = BTreeMap::<u64, (u64, u64)>::new();
    for row in parse_performance_daily_campaigns(&response)
        .map_err(|error| anyhow::anyhow!("statistics parse failed: {error}"))?
    {
        let entry = metrics.entry(row.campaign_id).or_default();
        entry.0 = entry
            .0
            .checked_add(row.spend_minor)
            .context("spend overflow")?;
        entry.1 = entry
            .1
            .checked_add(row.attributed_revenue_minor)
            .context("revenue overflow")?;
    }
    Ok(metrics)
}

#[allow(clippy::too_many_arguments)]
async fn guard_campaign_static(
    state: &mut StaticGuardState,
    state_path: &Path,
    reader: &Arc<PerformanceClient>,
    writer: &Arc<OzonAdsWriteClient>,
    store: &mcp_ozon::config::StoreId,
    guard: &OzonCampaignGuard,
    spend_minor: u64,
    revenue_minor: u64,
    stop_reason: Option<&'static str>,
) -> Result<()> {
    let Some(stop_reason) = stop_reason else {
        tracing::info!(
            campaign_id = guard.campaign_id,
            sku = guard.sku,
            spend_minor,
            revenue_minor,
            "static Ozon guard observation"
        );
        return Ok(());
    };
    tokio::time::sleep(WRITE_BOUNDARY_INTERVAL).await;
    let result = writer
        .deactivate_campaign_with_permit(guard.campaign_id, || async {
            Ok::<(), std::convert::Infallible>(())
        })
        .await;
    tokio::time::sleep(WRITE_BOUNDARY_INTERVAL).await;
    if let Err(error) = result {
        let stopped_after_ambiguous = matches!(
            &error,
            mcp_ozon::control::OzonGuardedWriteError::Write(write)
                if write.kind() == OzonWriteErrorKind::Ambiguous
        ) && matches!(
            campaign_is_running(reader, store, guard.campaign_id).await,
            Ok(false)
        );
        if !stopped_after_ambiguous {
            state.incident_campaign_ids.insert(guard.campaign_id);
            persist_static_state(state_path, state)?;
            bail!("static campaign stop became incident: {error}");
        }
    }
    if campaign_is_running(reader, store, guard.campaign_id).await? {
        state.incident_campaign_ids.insert(guard.campaign_id);
        persist_static_state(state_path, state)?;
        bail!("static deactivate readback still reports running");
    }
    tracing::info!(
        campaign_id = guard.campaign_id,
        sku = guard.sku,
        stop_reason,
        "static Ozon campaign stopped"
    );
    Ok(())
}

async fn guard_once(
    plans: &Arc<OzonPlanRepository>,
    reader: &Arc<PerformanceClient>,
    writer: &Arc<OzonAdsWriteClient>,
    store: &mcp_ozon::config::StoreId,
) -> Result<()> {
    for guard in plans.active_guards().await? {
        if let Err(error) = guard_campaign(plans, reader, writer, store, &guard).await {
            tracing::error!(
                plan_id=%guard.plan_id,
                campaign_id=guard.campaign_id,
                sku=guard.sku,
                %error,
                "Ozon campaign guard item failed"
            );
        }
    }
    Ok(())
}

async fn guard_campaign(
    plans: &Arc<OzonPlanRepository>,
    reader: &Arc<PerformanceClient>,
    writer: &Arc<OzonAdsWriteClient>,
    store: &mcp_ozon::config::StoreId,
    guard: &OzonCampaignGuard,
) -> Result<()> {
    let (spend_minor, revenue_minor, stop_reason) =
        evaluate_live_guard(reader, store, guard).await?;
    let Some(stop_reason) = stop_reason else {
        plans
            .record_guard_observation(&guard.plan_id, spend_minor, revenue_minor)
            .await?;
        return Ok(());
    };

    plans
        .claim_guard_stop(
            &guard.plan_id,
            guard.campaign_id,
            stop_reason,
            spend_minor,
            revenue_minor,
        )
        .await?;
    tokio::time::sleep(WRITE_BOUNDARY_INTERVAL).await;
    let permit_plans = Arc::clone(plans);
    let plan_id = guard.plan_id.clone();
    let campaign_id = guard.campaign_id;
    let result = writer
        .deactivate_campaign_with_permit(campaign_id, move || async move {
            permit_plans
                .revalidate_stop_permit(&plan_id, campaign_id)
                .await
        })
        .await;
    tokio::time::sleep(WRITE_BOUNDARY_INTERVAL).await;
    if let Err(error) = result {
        let error_class = match error {
            mcp_ozon::control::OzonGuardedWriteError::Permit(_) => "stop_permit_failed",
            mcp_ozon::control::OzonGuardedWriteError::Write(error) => match error.kind() {
                OzonWriteErrorKind::Definite => "stop_write_rejected",
                OzonWriteErrorKind::Ambiguous => "stop_write_ambiguous",
            },
        };
        plans
            .mark_guard_incident(&guard.plan_id, error_class)
            .await?;
        bail!("campaign stop became incident: {error_class}");
    }
    match campaign_is_running(reader, store, guard.campaign_id).await {
        Ok(false) => {}
        Ok(true) => {
            plans
                .mark_guard_incident(&guard.plan_id, "stop_readback_mismatch")
                .await?;
            bail!("deactivate readback still reports running");
        }
        Err(error) => {
            plans
                .mark_guard_incident(&guard.plan_id, "stop_readback_unavailable")
                .await?;
            return Err(error.context("deactivate readback unavailable"));
        }
    }
    plans
        .finish_guard(
            &guard.plan_id,
            guard.campaign_id,
            stop_reason,
            spend_minor,
            revenue_minor,
        )
        .await?;
    tracing::info!(
        campaign_id = guard.campaign_id,
        sku = guard.sku,
        stop_reason,
        "Ozon campaign stopped"
    );
    Ok(())
}

async fn evaluate_live_guard(
    reader: &PerformanceClient,
    store: &mcp_ozon::config::StoreId,
    guard: &OzonCampaignGuard,
) -> Result<(u64, u64, Option<&'static str>)> {
    let date_to = business_date(Utc::now()).format("%Y-%m-%d").to_string();
    let metrics_response = reader
        .daily_statistics(
            store,
            StatisticsQuery {
                campaign_ids: vec![guard.campaign_id],
                date_from: guard.date_from.clone(),
                date_to,
            },
        )
        .await
        .context("statistics request failed; no-write hold")?;
    let rows = parse_performance_daily_campaigns(&metrics_response)
        .map_err(|error| anyhow::anyhow!("statistics parse failed; no-write hold: {error}"))?;
    let mut spend_minor = 0_u64;
    let mut revenue_minor = 0_u64;
    for row in rows {
        if row.campaign_id == guard.campaign_id {
            spend_minor = spend_minor
                .checked_add(row.spend_minor)
                .context("spend overflow")?;
            revenue_minor = revenue_minor
                .checked_add(row.attributed_revenue_minor)
                .context("revenue overflow")?;
        }
    }
    let stop_reason = evaluate_ozon_campaign_guard(
        spend_minor,
        revenue_minor,
        guard.spend_cap_microrubles,
        guard.target_drr_percent,
    )?
    .map(mcp_ozon::control::OzonGuardStopReason::as_str);
    Ok((spend_minor, revenue_minor, stop_reason))
}

async fn campaign_is_running(
    reader: &PerformanceClient,
    store: &mcp_ozon::config::StoreId,
    campaign_id: u64,
) -> Result<bool> {
    let response = reader
        .campaigns(
            store,
            CampaignsQuery {
                campaign_ids: vec![campaign_id],
                adv_object_type: Some("SKU"),
                state: None,
                page: 1,
                page_size: 10,
            },
        )
        .await?;
    let rows = response
        .get("list")
        .and_then(serde_json::Value::as_array)
        .context("invalid campaign readback")?;
    let campaign = rows
        .iter()
        .find(|row| {
            row.get("id").and_then(|value| match value {
                serde_json::Value::String(value) => value.parse().ok(),
                serde_json::Value::Number(value) => value.as_u64(),
                _ => None,
            }) == Some(campaign_id)
        })
        .context("campaign missing from readback")?;
    Ok(campaign.get("state").and_then(serde_json::Value::as_str) == Some("CAMPAIGN_STATE_RUNNING"))
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let ctrl_c = async {
            if signal::ctrl_c().await.is_err() {
                std::future::pending::<()>().await;
            }
        };
        let terminate = async {
            match signal::unix::signal(signal::unix::SignalKind::terminate()) {
                Ok(mut stream) => {
                    let _ = stream.recv().await;
                }
                Err(_) => std::future::pending::<()>().await,
            }
        };
        tokio::select! { () = ctrl_c => {}, () = terminate => {} }
    }
}
