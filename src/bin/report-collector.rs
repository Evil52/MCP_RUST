#![forbid(unsafe_code)]

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, NaiveDate, Utc};
use mcp_ozon::reporting::{
    ReportKey, ReportKind, business_date,
    collector_orchestrator::plan_due_collection,
    collector_plan::CollectionTarget,
    collector_schedule::DueCollection,
    collector_service::{ReportCollectorConfig, ReportCollectorMode},
    ozon_performance_source::{OzonPerformanceReportSource, PerformanceClientReportTransport},
    ozon_source::{OzonClientReportTransport, collect_complete_snapshots},
    postgres_collector::PostgresSnapshotWriter,
    report_cutoff, reporting_interval,
    snapshot::Marketplace,
    wb_source::{WbClientReportTransport, WbReportSource},
};
use tokio::signal;
use tokio::time::{Duration, timeout};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// The individual Seller request limit is configured by the report collector.
/// This bounds the entire manual account run, including pagination and the
/// transactional snapshot publication, so a slow upstream cannot hold an
/// operator invocation forever.
const REPORT_TARGET_TOTAL_DEADLINE: Duration = Duration::from_secs(12 * 60);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mcp_ozon::reporting=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let command = parse_command(&arguments)?;
    let config = ReportCollectorConfig::from_lookup(&mut |key| std::env::var(key).ok())?;
    let writer = PostgresSnapshotWriter::connect(config.database_config())
        .await
        .context("daily report snapshot writer is unavailable")?;
    writer.verify_runtime_contract().await?;
    let targets = config.collection_plan();
    match command {
        Command::Healthcheck => {
            tracing::info!(targets = targets.len(), "report collector preflight passed");
            return Ok(());
        }
        Command::OzonDryRun { account_id, date } => {
            run_ozon_dry_run(&config, &writer, &account_id, date).await?;
            return Ok(());
        }
        Command::WbDryRun { account_id, date } => {
            run_wb_dry_run(&config, &writer, &account_id, date).await?;
            return Ok(());
        }
        Command::CollectDue => {
            run_due_collection(&config, &writer, Utc::now()).await?;
            return Ok(());
        }
        Command::ServeDisabled => {}
    }
    if config.mode() != ReportCollectorMode::Disabled || config.policy().enabled {
        bail!("automatic report collection runtime is unavailable");
    }
    tracing::warn!(
        targets = targets.len(),
        "report collector is disabled; no marketplace request or snapshot write is performed"
    );
    shutdown_signal().await;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Command {
    ServeDisabled,
    Healthcheck,
    OzonDryRun { account_id: String, date: NaiveDate },
    WbDryRun { account_id: String, date: NaiveDate },
    CollectDue,
}

fn parse_command(arguments: &[String]) -> Result<Command> {
    match arguments {
        [] => Ok(Command::ServeDisabled),
        [argument] if argument == "healthcheck" => Ok(Command::Healthcheck),
        [argument] if argument == "collect-due" => Ok(Command::CollectDue),
        [command, account_id, date]
            if matches!(command.as_str(), "ozon-dry-run" | "wb-dry-run") =>
        {
            ensure!(
                !account_id.is_empty()
                    && account_id.len() <= 128
                    && account_id.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
                    }),
                "account id is invalid"
            );
            let date = NaiveDate::parse_from_str(date, "%Y-%m-%d")
                .context("dry-run date must use YYYY-MM-DD")?;
            Ok(if command == "ozon-dry-run" {
                Command::OzonDryRun {
                    account_id: account_id.clone(),
                    date,
                }
            } else {
                Command::WbDryRun {
                    account_id: account_id.clone(),
                    date,
                }
            })
        }
        _ => {
            bail!(
                "usage: report-collector [healthcheck | collect-due | ozon-dry-run <account-id> <YYYY-MM-DD> | wb-dry-run <account-id> <YYYY-MM-DD>]"
            )
        }
    }
}

async fn run_due_collection(
    config: &ReportCollectorConfig,
    writer: &PostgresSnapshotWriter,
    now: DateTime<Utc>,
) -> Result<()> {
    ensure!(
        config.mode() == ReportCollectorMode::Scheduled,
        "collect-due requires REPORT_COLLECTOR_MODE=scheduled"
    );
    ensure!(
        config.policy().enabled,
        "collect-due requires an enabled daily report policy"
    );
    let Some(plan) = plan_due_collection(writer, now, config.collection_plan()).await? else {
        tracing::info!("no daily report collection occurrence is due");
        return Ok(());
    };
    let owner_id = claim_owner("scheduled");
    let mut completed = 0_usize;
    let mut busy = 0_usize;
    let mut failed = 0_usize;
    let total = plan.targets.len();
    for target in plan.targets {
        let remaining = (plan.occurrence.complete_by - Utc::now())
            .to_std()
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            let remaining_targets = total.saturating_sub(completed + busy + failed);
            failed += remaining_targets;
            tracing::warn!(
                remaining_targets,
                "daily report collection window closed before all targets were attempted"
            );
            break;
        }
        let Some(claim) = writer
            .claim_target(&target, plan.occurrence.cutoff_at, &owner_id)
            .await?
        else {
            busy += 1;
            continue;
        };
        let deadline = remaining.min(REPORT_TARGET_TOTAL_DEADLINE);
        let outcome = timeout(
            deadline,
            collect_scheduled_target(config, writer, &claim, &target, &plan.occurrence),
        )
        .await;
        match outcome {
            Ok(Ok(snapshot_ids)) => {
                completed += 1;
                tracing::info!(
                    account_id = target.account_id,
                    marketplace = ?target.marketplace,
                    snapshots = snapshot_ids.len(),
                    cutoff_at = %plan.occurrence.cutoff_at,
                    "scheduled report snapshots were atomically published"
                );
            }
            Ok(Err(error)) => {
                failed += 1;
                let released = writer.release_claim(&claim).await.unwrap_or(false);
                tracing::warn!(
                    account_id = target.account_id,
                    marketplace = ?target.marketplace,
                    released,
                    error = %error,
                    "scheduled report target failed without publishing partial snapshots"
                );
            }
            Err(_) => {
                failed += 1;
                let released = writer.release_claim(&claim).await.unwrap_or(false);
                tracing::warn!(
                    account_id = target.account_id,
                    marketplace = ?target.marketplace,
                    released,
                    "scheduled report target exceeded its bounded collection deadline"
                );
            }
        }
    }
    tracing::info!(total, completed, busy, failed, "collect-due finished");
    ensure!(
        failed == 0,
        "scheduled collection finished with {failed} failed target(s)"
    );
    Ok(())
}

async fn collect_scheduled_target(
    config: &ReportCollectorConfig,
    writer: &PostgresSnapshotWriter,
    claim: &mcp_ozon::reporting::postgres_collector::CollectionClaim,
    target: &CollectionTarget,
    occurrence: &DueCollection,
) -> Result<Vec<i64>> {
    let date = business_date(occurrence.period_start);
    match target.marketplace {
        Marketplace::Ozon => {
            let (client, performance, store) =
                config.resolve_ozon_scheduled(claim, &mut |key| std::env::var(key).ok())?;
            let performance_store = store.clone();
            let transport = OzonClientReportTransport::new(client, store);
            let performance_source = OzonPerformanceReportSource::new(
                PerformanceClientReportTransport::new(performance, performance_store),
            );
            let advertising = performance_source
                .collect(date)
                .await
                .map_err(|error| anyhow::anyhow!("performance_{}", error.code()))?;
            let snapshots = collect_complete_snapshots(
                &transport,
                advertising,
                target.account_id.clone(),
                occurrence.cutoff_at,
                Utc::now,
                occurrence.period_start,
                occurrence.period_end,
                env!("CARGO_PKG_VERSION").to_owned(),
            )
            .await
            .map_err(anyhow::Error::from)?;
            writer
                .persist_claimed_batch(claim, &snapshots)
                .await
                .map_err(|_| anyhow::anyhow!("snapshot_persistence_failed"))
        }
        Marketplace::Wildberries => {
            let (client, account) =
                config.resolve_wb_scheduled(claim, &mut |key| std::env::var(key).ok())?;
            let source = WbReportSource::new(WbClientReportTransport::new(client, account));
            let facts = source
                .collect(date)
                .await
                .map_err(|error| anyhow::anyhow!("wb_{}", error.code()))?;
            let snapshots = facts
                .into_snapshots(
                    target.account_id.clone(),
                    occurrence.cutoff_at,
                    Utc::now(),
                    occurrence.period_start,
                    occurrence.period_end,
                    env!("CARGO_PKG_VERSION").to_owned(),
                )
                .map_err(|_| anyhow::anyhow!("invalid_wb_snapshot_input"))?;
            writer
                .persist_claimed_batch(claim, &snapshots)
                .await
                .map_err(|_| anyhow::anyhow!("snapshot_persistence_failed"))
        }
    }
}

async fn run_wb_dry_run(
    config: &ReportCollectorConfig,
    writer: &PostgresSnapshotWriter,
    account_id: &str,
    date: NaiveDate,
) -> Result<()> {
    ensure!(
        config.mode() == ReportCollectorMode::WbDryRun,
        "wb-dry-run requires REPORT_COLLECTOR_MODE=wb_dry_run"
    );
    ensure!(
        !config.policy().enabled,
        "wb-dry-run refuses an enabled daily report policy"
    );
    let (period_start, period_end, cutoff_at) = morning_report_window(date, Utc::now())?;
    let target = dry_run_target(config, account_id, Marketplace::Wildberries)?;
    let owner_id = claim_owner("wb");
    let claim = writer
        .claim_target(&target, cutoff_at, &owner_id)
        .await?
        .context("WB dry-run target is already claimed or complete")?;
    let outcome = timeout(REPORT_TARGET_TOTAL_DEADLINE, async {
        let (client, account) =
            config.resolve_wb_dry_run(&claim, &mut |key| std::env::var(key).ok())?;
        let source = WbReportSource::new(WbClientReportTransport::new(client, account));
        let facts = source
            .collect(date)
            .await
            .map_err(|error| anyhow::anyhow!("wb_{}", error.code()))?;
        let source_as_of = Utc::now();
        let snapshots = facts
            .into_snapshots(
                account_id.to_owned(),
                cutoff_at,
                source_as_of,
                period_start,
                period_end,
                env!("CARGO_PKG_VERSION").to_owned(),
            )
            .map_err(|_| anyhow::anyhow!("invalid_wb_snapshot_input"))?;
        writer
            .persist_claimed_batch(&claim, &snapshots)
            .await
            .map_err(|_| anyhow::anyhow!("snapshot_persistence_failed"))
    })
    .await;
    let snapshot_ids = finish_dry_run(writer, &claim, "WB", outcome).await?;
    tracing::info!(
        account_id,
        snapshots = snapshot_ids.len(),
        "WB dry-run completed and atomically published its complete snapshot set"
    );
    Ok(())
}

async fn run_ozon_dry_run(
    config: &ReportCollectorConfig,
    writer: &mcp_ozon::reporting::postgres_collector::PostgresSnapshotWriter,
    account_id: &str,
    date: NaiveDate,
) -> Result<()> {
    ensure!(
        config.mode() == ReportCollectorMode::OzonDryRun,
        "ozon-dry-run requires REPORT_COLLECTOR_MODE=ozon_dry_run"
    );
    ensure!(
        !config.policy().enabled,
        "ozon-dry-run refuses an enabled daily report policy"
    );
    let (period_start, period_end, cutoff_at) = morning_report_window(date, Utc::now())?;
    let target = dry_run_target(config, account_id, Marketplace::Ozon)?;
    let owner_id = claim_owner("ozon");
    let claim = writer
        .claim_target(&target, cutoff_at, &owner_id)
        .await?
        .context("Ozon dry-run target is already claimed or complete")?;
    let outcome = timeout(REPORT_TARGET_TOTAL_DEADLINE, async {
        let (client, performance, store) =
            config.resolve_ozon_dry_run(&claim, &mut |key| std::env::var(key).ok())?;
        let performance_store = store.clone();
        let transport = OzonClientReportTransport::new(client, store);
        let performance_source = OzonPerformanceReportSource::new(
            PerformanceClientReportTransport::new(performance, performance_store),
        );
        let advertising = performance_source
            .collect(date)
            .await
            .map_err(|error| anyhow::anyhow!("performance_{}", error.code()))?;
        let snapshots = collect_complete_snapshots(
            &transport,
            advertising,
            account_id.to_owned(),
            cutoff_at,
            Utc::now,
            period_start,
            period_end,
            env!("CARGO_PKG_VERSION").to_owned(),
        )
        .await
        .map_err(anyhow::Error::from)?;
        writer
            .persist_claimed_batch(&claim, &snapshots)
            .await
            .map_err(|_| anyhow::anyhow!("snapshot_persistence_failed"))
    })
    .await;
    let snapshot_ids = finish_dry_run(writer, &claim, "Ozon", outcome).await?;
    tracing::info!(
        account_id,
        snapshots = snapshot_ids.len(),
        "Ozon dry-run completed and atomically published its complete seller snapshot set"
    );
    Ok(())
}

fn dry_run_target(
    config: &ReportCollectorConfig,
    account_id: &str,
    marketplace: Marketplace,
) -> Result<CollectionTarget> {
    config
        .collection_plan()
        .iter()
        .find(|target| target.account_id == account_id && target.marketplace == marketplace)
        .cloned()
        .context("dry-run account is outside the validated collection plan")
}

fn claim_owner(marketplace: &str) -> String {
    format!(
        "dry-run-{marketplace}-{}-{}",
        std::process::id(),
        Utc::now().timestamp_micros()
    )
}

async fn finish_dry_run<T>(
    writer: &PostgresSnapshotWriter,
    claim: &mcp_ozon::reporting::postgres_collector::CollectionClaim,
    marketplace: &str,
    outcome: Result<Result<T>, tokio::time::error::Elapsed>,
) -> Result<T> {
    match outcome {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => {
            let _ = writer.release_claim(claim).await;
            Err(anyhow::anyhow!(
                "{marketplace} dry-run failed ({error:#}); no partial report snapshots were published"
            ))
        }
        Err(_) => {
            let _ = writer.release_claim(claim).await;
            Err(anyhow::anyhow!(
                "{marketplace} dry-run failed (collection_deadline); no partial report snapshots were published"
            ))
        }
    }
}

fn morning_report_window(
    date: NaiveDate,
    now: DateTime<Utc>,
) -> Result<(DateTime<Utc>, DateTime<Utc>, DateTime<Utc>)> {
    let report_date = date.succ_opt().context("dry-run date is out of range")?;
    let key = ReportKey {
        local_date: report_date,
        kind: ReportKind::Morning,
        recipient_id: "collector".to_owned(),
        report_version: 1,
    };
    let (start, end) = reporting_interval(&key)?;
    let cutoff = report_cutoff(&key)?;
    ensure!(
        cutoff <= now,
        "dry-run requires the 08:00 EKB morning cutoff to have passed"
    );
    ensure!(
        now <= cutoff + chrono::Duration::minutes(30),
        "dry-run must start no later than 08:30 EKB for the requested business day"
    );
    Ok((start, end, cutoff))
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
        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate => {}
        }
    }
    #[cfg(not(unix))]
    if signal::ctrl_c().await.is_err() {
        std::future::pending::<()>().await;
    }
}
