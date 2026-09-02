#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use mcp_ozon::{
    control::{
        ControlAppConfig, ControlMode, OzonAdsWriteClient, OzonCampaignGuard, OzonPlanRepository,
        OzonWriteErrorKind, evaluate_ozon_campaign_guard,
    },
    ozon_performance::{CampaignsQuery, PerformanceClient, StatisticsQuery},
    reporting::{business_date, ozon_adapter::parse_performance_daily_campaigns},
};
use serde::{Deserialize, Serialize};
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const POLL_INTERVAL: Duration = Duration::from_secs(60);
const STATIC_GUARDS_FILE_ENV: &str = "CONTROL_MCP_OZON_STATIC_GUARDS_FILE";
const STATIC_STATE_FILE_ENV: &str = "CONTROL_MCP_OZON_STATIC_GUARD_STATE_FILE";
const MAX_STATIC_GUARDS: usize = 50;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticGuardFile {
    account_id: String,
    guards: Vec<StaticGuard>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticGuard {
    campaign_id: u64,
    sku: u64,
    date_from: String,
    spend_cap_microrubles: u64,
    target_drr_percent: u8,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StaticGuardState {
    incident_campaign_ids: BTreeSet<u64>,
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
        let static_guards =
            load_static_guards(Path::new(&static_guards_path), &runtime.account_id)?;
        let mut state = load_static_state(&state_path)?;
        tracing::info!(
            account_id=%runtime.account_id,
            guards=static_guards.len(),
            "static Ozon campaign guard armed"
        );
        loop {
            if let Err(error) = guard_once_static(
                &static_guards,
                &mut state,
                &state_path,
                &reader,
                &writer,
                &runtime.store_id,
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

fn load_static_guards(path: &Path, expected_account_id: &str) -> Result<Vec<OzonCampaignGuard>> {
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
    let mut campaigns = BTreeSet::new();
    let mut skus = BTreeSet::new();
    config
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
            {
                bail!("static Ozon guard item имеет недопустимые данные");
            }
            Ok(OzonCampaignGuard {
                plan_id: format!("static-{}", guard.campaign_id),
                account_id: expected_account_id.to_owned(),
                sku: guard.sku,
                campaign_id: guard.campaign_id,
                date_from: guard.date_from,
                spend_cap_microrubles: guard.spend_cap_microrubles,
                target_drr_percent: guard.target_drr_percent,
            })
        })
        .collect()
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

async fn guard_once_static(
    guards: &[OzonCampaignGuard],
    state: &mut StaticGuardState,
    state_path: &Path,
    reader: &Arc<PerformanceClient>,
    writer: &Arc<OzonAdsWriteClient>,
    store: &mcp_ozon::config::StoreId,
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
    let metrics = static_guard_metrics(reader, store, guards, &running).await;
    for guard in guards {
        if state.incident_campaign_ids.contains(&guard.campaign_id)
            || !running.contains(&guard.campaign_id)
        {
            continue;
        }
        let (spend_minor, revenue_minor, stop_reason) = match &metrics {
            Ok(metrics) => {
                let (spend_minor, revenue_minor) =
                    metrics.get(&guard.campaign_id).copied().unwrap_or_default();
                let stop_reason = evaluate_ozon_campaign_guard(
                    spend_minor,
                    revenue_minor,
                    guard.spend_cap_microrubles,
                    guard.target_drr_percent,
                )?
                .map(mcp_ozon::control::OzonGuardStopReason::as_str);
                (spend_minor, revenue_minor, stop_reason)
            }
            Err(error) => {
                tracing::warn!(campaign_id=guard.campaign_id,%error,"statistics unavailable; fail-closed stop requested");
                (0, 0, Some("statistics_unavailable"))
            }
        };
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
    guards: &[OzonCampaignGuard],
) -> Result<BTreeSet<u64>> {
    let response = reader
        .campaigns(
            store,
            CampaignsQuery {
                campaign_ids: guards.iter().map(|guard| guard.campaign_id).collect(),
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

async fn static_guard_metrics(
    reader: &PerformanceClient,
    store: &mcp_ozon::config::StoreId,
    guards: &[OzonCampaignGuard],
    running: &BTreeSet<u64>,
) -> Result<BTreeMap<u64, (u64, u64)>> {
    let date_from = guards
        .iter()
        .filter(|guard| running.contains(&guard.campaign_id))
        .map(|guard| guard.date_from.as_str())
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
    let result = writer
        .deactivate_campaign_with_permit(guard.campaign_id, || async {
            Ok::<(), std::convert::Infallible>(())
        })
        .await;
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
        .await;
    let parsed_metrics = match metrics_response {
        Ok(response) => parse_performance_daily_campaigns(&response)
            .map_err(|error| format!("statistics parse failed: {error}")),
        Err(error) => Err(format!("statistics request failed: {error}")),
    };
    let (spend_minor, revenue_minor, stop_reason) = match parsed_metrics {
        Ok(rows) => {
            let mut spend = 0_u64;
            let mut revenue = 0_u64;
            for row in rows {
                if row.campaign_id == guard.campaign_id {
                    spend = spend
                        .checked_add(row.spend_minor)
                        .context("spend overflow")?;
                    revenue = revenue
                        .checked_add(row.attributed_revenue_minor)
                        .context("revenue overflow")?;
                }
            }
            let reason = evaluate_ozon_campaign_guard(
                spend,
                revenue,
                guard.spend_cap_microrubles,
                guard.target_drr_percent,
            )?
            .map(mcp_ozon::control::OzonGuardStopReason::as_str);
            (spend, revenue, reason)
        }
        Err(error) => {
            tracing::warn!(campaign_id=guard.campaign_id,%error,"statistics unavailable; fail-closed stop requested");
            (0, 0, Some("statistics_unavailable"))
        }
    };
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
