#![forbid(unsafe_code)]

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone, Timelike, Utc};
use mcp_ozon::reporting::{
    ReportKey, ReportKind, business_date,
    collector_orchestrator::plan_due_collection,
    collector_plan::CollectionTarget,
    collector_schedule::{COLLECTION_COMPLETION_WINDOW, DueCollection},
    collector_service::{ReportCollectorConfig, ReportCollectorMode},
    credential_bootstrap::bootstrap_report_credentials,
    ozon_performance_source::{OzonPerformanceReportSource, PerformanceClientReportTransport},
    ozon_source::{OzonClientReportTransport, collect_complete_snapshots_extended},
    postgres_collector::{CollectionClaim, PostgresSnapshotWriter, SalesRefreshClaim},
    report_cutoff, reporting_interval,
    snapshot::Marketplace,
    wb_source::{WbClientReportTransport, WbReportSource},
};
use mcp_ozon::runtime::print_runtime_version_if_requested;
use std::path::PathBuf;
use tokio::signal;
use tokio::time::{Duration, MissedTickBehavior, timeout};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// The individual Seller request limit is configured by the report collector.
/// This bounds the entire manual account run, including pagination and the
/// transactional snapshot publication, so a slow upstream cannot hold an
/// operator invocation forever.
const REPORT_TARGET_TOTAL_DEADLINE: Duration = Duration::from_mins(12);
/// Keep the same-account Seller API quiet after a scheduled collection. This
/// matches the analytics endpoint's minimum pacing interval and avoids a fresh
/// client instance immediately following the last scheduled request.
const REFRESH_AFTER_SCHEDULE_SAFETY: Duration = Duration::from_secs(65);
const SCHEDULED_TICK: Duration = Duration::from_secs(60);
const MAX_CONSECUTIVE_SCHEDULER_FAILURES: u32 = 5;

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if print_runtime_version_if_requested("report-collector", &arguments)? {
        return Ok(());
    }
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mcp_ozon::reporting=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();
    let command = parse_command(&arguments)?;
    if let Command::BootstrapCredentials {
        registry,
        policy,
        dotenv,
        output,
    } = &command
    {
        let summary = bootstrap_report_credentials(registry, policy, dotenv, output)?;
        tracing::info!(
            accounts = summary.account_count,
            credentials = summary.credential_count,
            "created the policy-scoped report credential directory"
        );
        return Ok(());
    }
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
        Command::CollectionPreflight => {
            ensure!(
                config.mode() == ReportCollectorMode::Scheduled && config.policy().enabled,
                "collection-preflight requires scheduled mode and an enabled daily report policy"
            );
            let receipt = writer
                .verify_collection_activation(targets, Utc::now())
                .await?;
            tracing::info!(
                cutoff_at = %receipt.cutoff_at,
                targets = receipt.target_count,
                "scheduled collection activation preflight passed"
            );
            return Ok(());
        }
        Command::OzonDryRun {
            account_id,
            date,
            kind,
        } => {
            run_ozon_dry_run(&config, &writer, &account_id, date, kind).await?;
            return Ok(());
        }
        Command::WbDryRun {
            account_id,
            date,
            kind,
        } => {
            run_wb_dry_run(&config, &writer, &account_id, date, kind).await?;
            return Ok(());
        }
        Command::CollectDue => {
            run_collect_due_command(&config, &writer).await?;
            return Ok(());
        }
        Command::RefreshOnce => {
            ensure!(
                config.mode() == ReportCollectorMode::Scheduled && config.policy().enabled,
                "refresh-once requires scheduled mode and an enabled daily report policy"
            );
            run_one_sales_refresh(&config, &writer).await?;
            return Ok(());
        }
        Command::RunScheduler => {
            run_scheduler_command(&config, &writer).await?;
            return Ok(());
        }
        Command::BootstrapCredentials { .. } => {
            unreachable!("bootstrap exits before runtime configuration")
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
    CollectionPreflight,
    OzonDryRun {
        account_id: String,
        date: NaiveDate,
        kind: ReportKind,
    },
    WbDryRun {
        account_id: String,
        date: NaiveDate,
        kind: ReportKind,
    },
    CollectDue,
    RefreshOnce,
    RunScheduler,
    BootstrapCredentials {
        registry: PathBuf,
        policy: PathBuf,
        dotenv: PathBuf,
        output: PathBuf,
    },
}

fn parse_command(arguments: &[String]) -> Result<Command> {
    match arguments {
        [] => Ok(Command::ServeDisabled),
        [argument] if argument == "healthcheck" => Ok(Command::Healthcheck),
        [argument] if argument == "collection-preflight" => Ok(Command::CollectionPreflight),
        [argument] if argument == "collect-due" => Ok(Command::CollectDue),
        [argument] if argument == "refresh-once" => Ok(Command::RefreshOnce),
        [argument] if argument == "run-scheduler" => Ok(Command::RunScheduler),
        [command, registry, policy, dotenv, output] if command == "bootstrap-credentials" => {
            Ok(Command::BootstrapCredentials {
                registry: registry.into(),
                policy: policy.into(),
                dotenv: dotenv.into(),
                output: output.into(),
            })
        }
        [command, account_id, date]
            if matches!(command.as_str(), "ozon-dry-run" | "wb-dry-run") =>
        {
            parse_dry_run_command(command, account_id, date, "morning")
        }
        [command, account_id, date, kind]
            if matches!(command.as_str(), "ozon-dry-run" | "wb-dry-run") =>
        {
            parse_dry_run_command(command, account_id, date, kind)
        }
        _ => {
            bail!(
                "usage: report-collector [healthcheck | collection-preflight | collect-due | refresh-once | run-scheduler | ozon-dry-run <account-id> <YYYY-MM-DD> [morning|evening] | wb-dry-run <account-id> <YYYY-MM-DD> [morning|evening] | bootstrap-credentials <access.json> <policy.json> <source.env> <new-output-directory>]"
            )
        }
    }
}

fn parse_dry_run_command(
    command: &str,
    account_id: &str,
    date: &str,
    kind: &str,
) -> Result<Command> {
    ensure!(
        !account_id.is_empty()
            && account_id.len() <= 128
            && account_id
                .bytes()
                .all(|byte| { byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-') }),
        "account id is invalid"
    );
    let date =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").context("dry-run date must use YYYY-MM-DD")?;
    let kind = match kind {
        "morning" => ReportKind::Morning,
        "evening" => ReportKind::Evening,
        _ => bail!("dry-run kind must be morning or evening"),
    };
    Ok(if command == "ozon-dry-run" {
        Command::OzonDryRun {
            account_id: account_id.to_owned(),
            date,
            kind,
        }
    } else {
        Command::WbDryRun {
            account_id: account_id.to_owned(),
            date,
            kind,
        }
    })
}

async fn run_collect_due_command(
    config: &ReportCollectorConfig,
    writer: &PostgresSnapshotWriter,
) -> Result<()> {
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    let signal_task = tokio::spawn(async move {
        shutdown_signal().await;
        signal_cancellation.cancel();
    });
    let result = run_due_collection(config, writer, Utc::now(), &cancellation).await;
    signal_task.abort();
    result
}

async fn run_scheduler_command(
    config: &ReportCollectorConfig,
    writer: &PostgresSnapshotWriter,
) -> Result<()> {
    ensure!(
        config.mode() == ReportCollectorMode::Scheduled && config.policy().enabled,
        "run-scheduler requires scheduled mode and an enabled daily report policy"
    );
    let receipt = writer
        .verify_collection_activation(config.collection_plan(), Utc::now())
        .await?;
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    let signal_task = tokio::spawn(async move {
        shutdown_signal().await;
        signal_cancellation.cancel();
    });
    tracing::info!(
        targets = config.collection_plan().len(),
        activation_cutoff_at = %receipt.cutoff_at,
        tick_seconds = SCHEDULED_TICK.as_secs(),
        "daily report collection scheduler started"
    );
    let result = run_collection_scheduler(config, writer, &cancellation).await;
    signal_task.abort();
    result
}

async fn run_collection_scheduler(
    config: &ReportCollectorConfig,
    writer: &PostgresSnapshotWriter,
    cancellation: &CancellationToken,
) -> Result<()> {
    let mut timer = tokio::time::interval(SCHEDULED_TICK);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut consecutive_failures = 0_u32;
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(()),
            _ = timer.tick() => {}
        }
        let tick_at = Utc::now();
        let result = run_due_collection(config, writer, tick_at, cancellation).await;
        let result = if result.is_ok() && !cancellation.is_cancelled() {
            run_one_sales_refresh(config, writer).await
        } else {
            result
        };
        match result {
            Ok(()) => consecutive_failures = 0,
            Err(error) => {
                consecutive_failures += 1;
                ensure!(
                    consecutive_failures < MAX_CONSECUTIVE_SCHEDULER_FAILURES,
                    "daily report collector failed {consecutive_failures} consecutive ticks; \
                     exiting so the supervisor can restart it"
                );
                tracing::warn!(
                    consecutive_failures,
                    error = %error,
                    "daily report collection tick failed; retrying on the next tick"
                );
            }
        }
    }
}

/// Claims and processes one manager refresh. The scheduler calls this only
/// outside the fixed report windows, so scheduled report snapshots keep
/// priority. PostgreSQL returns at most one globally fenced job.
async fn run_one_sales_refresh(
    config: &ReportCollectorConfig,
    writer: &PostgresSnapshotWriter,
) -> Result<()> {
    if !refresh_window_is_open(Utc::now()) {
        return Ok(());
    }
    let owner_id = claim_owner("refresh");
    let Some(refresh_claim) = writer.claim_sales_refresh(&owner_id).await? else {
        return Ok(());
    };
    let Some(target) = config
        .collection_plan()
        .iter()
        .find(|target| {
            target.account_id == refresh_claim.account_id()
                && target.marketplace == refresh_claim.marketplace()
        })
        .cloned()
    else {
        finish_failed_refresh(writer, &refresh_claim, "account_not_in_policy").await?;
        return Ok(());
    };
    let Some(collection_claim) = writer
        .claim_target(&target, refresh_claim.cutoff_at(), &owner_id)
        .await?
    else {
        finish_failed_refresh(writer, &refresh_claim, "snapshot_claim_busy").await?;
        return Ok(());
    };
    let remaining = (refresh_claim.lease_until() - Utc::now())
        .to_std()
        .unwrap_or(Duration::ZERO);
    if remaining.is_zero() {
        let _ = writer.release_claim(&collection_claim).await;
        finish_failed_refresh(writer, &refresh_claim, "refresh_lease_expired").await?;
        return Ok(());
    }
    let outcome = timeout(
        remaining.min(REPORT_TARGET_TOTAL_DEADLINE),
        collect_sales_refresh_target(config, writer, &collection_claim, &refresh_claim, &target),
    )
    .await;
    match outcome {
        Ok(Ok(snapshot_ids)) => {
            tracing::info!(
                account_id = refresh_claim.account_id(),
                marketplace = ?refresh_claim.marketplace(),
                snapshots = snapshot_ids.len(),
                cutoff_at = %refresh_claim.cutoff_at(),
                "manager-requested marketplace snapshot refresh completed"
            );
        }
        Ok(Err(error)) => {
            let released = writer
                .release_claim(&collection_claim)
                .await
                .unwrap_or(false);
            let error_class = refresh_error_class(&error);
            finish_failed_refresh(writer, &refresh_claim, error_class).await?;
            tracing::warn!(
                account_id = refresh_claim.account_id(),
                marketplace = ?refresh_claim.marketplace(),
                released,
                error_class,
                "manager-requested marketplace snapshot refresh failed"
            );
        }
        Err(_) => {
            let released = writer
                .release_claim(&collection_claim)
                .await
                .unwrap_or(false);
            finish_failed_refresh(writer, &refresh_claim, "collection_deadline").await?;
            tracing::warn!(
                account_id = refresh_claim.account_id(),
                marketplace = ?refresh_claim.marketplace(),
                released,
                "manager-requested marketplace snapshot refresh reached its deadline"
            );
        }
    }
    Ok(())
}

/// Manual refreshes cannot start close enough to a fixed report window to
/// overlap it. The pre-window reserve is the complete refresh deadline; the
/// post-window reserve is the Seller analytics pacing interval.
fn refresh_window_is_open(now: DateTime<Utc>) -> bool {
    let offset = FixedOffset::east_opt(5 * 60 * 60).expect("EKB offset must be valid");
    let local_seconds = now
        .with_timezone(&offset)
        .time()
        .num_seconds_from_midnight();
    let pre_window = u32::try_from(REPORT_TARGET_TOTAL_DEADLINE.as_secs())
        .expect("refresh deadline must fit in one day");
    let collection_window = u32::try_from(COLLECTION_COMPLETION_WINDOW.num_seconds())
        .expect("collection window must be positive and fit in one day");
    let post_window = u32::try_from(REFRESH_AFTER_SCHEDULE_SAFETY.as_secs())
        .expect("refresh safety interval must fit in one day");

    [8 * 60 * 60, 17 * 60 * 60].into_iter().all(|cutoff| {
        let reserved_start = cutoff - pre_window;
        let reserved_end = cutoff + collection_window + post_window;
        !(reserved_start..=reserved_end).contains(&local_seconds)
    })
}

async fn collect_sales_refresh_target(
    config: &ReportCollectorConfig,
    writer: &PostgresSnapshotWriter,
    collection_claim: &CollectionClaim,
    refresh_claim: &SalesRefreshClaim,
    target: &CollectionTarget,
) -> Result<Vec<i64>> {
    if let Some(staged) = writer.load_staged_batch(collection_claim).await? {
        tracing::info!(
            account_id = collection_claim.account_id(),
            marketplace = ?collection_claim.marketplace(),
            snapshots = staged.len(),
            "resuming marketplace refresh from normalized staging readback"
        );
        return writer
            .persist_refresh_claimed_batch(collection_claim, refresh_claim, &staged)
            .await
            .map_err(|_| anyhow::anyhow!("snapshot_persistence_failed"));
    }
    let period_start = refresh_period_start(refresh_claim.business_date())?;
    ensure!(
        refresh_claim.cutoff_at() > period_start,
        "refresh cutoff must be after the EKB business-day start"
    );
    let snapshots = match refresh_claim.marketplace() {
        Marketplace::Ozon => {
            let (client, performance, store) = config.resolve_ozon_scheduled(collection_claim)?;
            let performance_store = store.clone();
            let transport = OzonClientReportTransport::new(client, store);
            let performance_source = OzonPerformanceReportSource::new(
                PerformanceClientReportTransport::new(performance, performance_store),
            );
            let performance_facts = performance_source
                .collect_extended(refresh_claim.business_date())
                .await
                .map_err(|_| anyhow::anyhow!("performance_collection_failed"))?;
            collect_complete_snapshots_extended(
                &transport,
                performance_facts.advertising,
                performance_facts.expenses,
                target.account_id.clone(),
                refresh_claim.cutoff_at(),
                Utc::now,
                period_start,
                refresh_claim.cutoff_at(),
                env!("CARGO_PKG_VERSION").to_owned(),
            )
            .await
            .map_err(|_| anyhow::anyhow!("seller_collection_failed"))?
        }
        Marketplace::Wildberries => {
            let (client, account) = config.resolve_wb_scheduled(collection_claim)?;
            let source = WbReportSource::new(WbClientReportTransport::new(client, account));
            let facts = source
                .collect(refresh_claim.business_date())
                .await
                .map_err(|_| anyhow::anyhow!("wb_collection_failed"))?;
            facts
                .into_snapshots(
                    &target.account_id,
                    refresh_claim.cutoff_at(),
                    Utc::now(),
                    period_start,
                    refresh_claim.cutoff_at(),
                    env!("CARGO_PKG_VERSION"),
                )
                .map_err(|_| anyhow::anyhow!("invalid_wb_snapshot_input"))?
        }
    };
    writer
        .stage_claimed_batch(collection_claim, &snapshots)
        .await?;
    let staged = writer
        .load_staged_batch(collection_claim)
        .await?
        .context("staged marketplace refresh readback is incomplete")?;
    writer
        .persist_refresh_claimed_batch(collection_claim, refresh_claim, &staged)
        .await
        .map_err(|_| anyhow::anyhow!("snapshot_persistence_failed"))
}

fn refresh_period_start(date: NaiveDate) -> Result<DateTime<Utc>> {
    let offset = FixedOffset::east_opt(5 * 60 * 60).context("EKB offset is invalid")?;
    let local = date
        .and_hms_opt(0, 0, 0)
        .context("refresh business date is invalid")?;
    offset
        .from_local_datetime(&local)
        .single()
        .map(|value| value.with_timezone(&Utc))
        .context("refresh business-day start is out of range")
}

async fn finish_failed_refresh(
    writer: &PostgresSnapshotWriter,
    claim: &SalesRefreshClaim,
    error_class: &str,
) -> Result<()> {
    ensure!(
        writer.fail_sales_refresh(claim, error_class).await?,
        "refresh queue claim was lost before failure could be recorded"
    );
    Ok(())
}

fn refresh_error_class(error: &anyhow::Error) -> &'static str {
    let message = error.to_string();
    if message.contains("performance_collection_failed") {
        "performance_collection_failed"
    } else if message.contains("seller_collection_failed") {
        "seller_collection_failed"
    } else if message.contains("wb_collection_failed") {
        "wb_collection_failed"
    } else if message.contains("invalid_wb_snapshot_input") {
        "invalid_wb_snapshot_input"
    } else if message.contains("snapshot_persistence_failed") {
        "snapshot_persistence_failed"
    } else {
        "refresh_collection_failed"
    }
}

async fn run_due_collection(
    config: &ReportCollectorConfig,
    writer: &PostgresSnapshotWriter,
    now: DateTime<Utc>,
    cancellation: &CancellationToken,
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
        if cancellation.is_cancelled() {
            tracing::info!("daily report collection cancelled before the next target");
            return Ok(());
        }
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
        let outcome = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                let released = writer.release_claim(&claim).await.unwrap_or(false);
                tracing::info!(
                    account_id = target.account_id,
                    marketplace = ?target.marketplace,
                    released,
                    "scheduled report collection cancelled and its live claim was released"
                );
                return Ok(());
            }
            outcome = timeout(
                deadline,
                collect_scheduled_target(config, writer, &claim, &target, &plan.occurrence),
            ) => outcome
        };
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
    if let Some(staged) = writer.load_staged_batch(claim).await? {
        tracing::info!(
            account_id = claim.account_id(),
            marketplace = ?claim.marketplace(),
            snapshots = staged.len(),
            "resuming scheduled collection from normalized staging readback"
        );
        return writer
            .persist_claimed_batch(claim, &staged)
            .await
            .map_err(|_| anyhow::anyhow!("snapshot_persistence_failed"));
    }
    let date = business_date(occurrence.period_start);
    let snapshots = match target.marketplace {
        Marketplace::Ozon => {
            let (client, performance, store) = config.resolve_ozon_scheduled(claim)?;
            let performance_store = store.clone();
            let transport = OzonClientReportTransport::new(client, store);
            let performance_source = OzonPerformanceReportSource::new(
                PerformanceClientReportTransport::new(performance, performance_store),
            );
            let performance_facts = performance_source
                .collect_extended(date)
                .await
                .map_err(|error| anyhow::anyhow!("performance_{}", error.code()))?;
            collect_complete_snapshots_extended(
                &transport,
                performance_facts.advertising,
                performance_facts.expenses,
                target.account_id.clone(),
                occurrence.cutoff_at,
                Utc::now,
                occurrence.period_start,
                occurrence.period_end,
                env!("CARGO_PKG_VERSION").to_owned(),
            )
            .await
            .map_err(anyhow::Error::from)?
        }
        Marketplace::Wildberries => {
            let (client, account) = config.resolve_wb_scheduled(claim)?;
            let source = WbReportSource::new(WbClientReportTransport::new(client, account));
            let facts = source
                .collect(date)
                .await
                .map_err(|error| anyhow::anyhow!("wb_{}", error.code()))?;
            facts
                .into_snapshots(
                    &target.account_id,
                    occurrence.cutoff_at,
                    Utc::now(),
                    occurrence.period_start,
                    occurrence.period_end,
                    env!("CARGO_PKG_VERSION"),
                )
                .map_err(|_| anyhow::anyhow!("invalid_wb_snapshot_input"))?
        }
    };
    writer.stage_claimed_batch(claim, &snapshots).await?;
    let staged = writer
        .load_staged_batch(claim)
        .await?
        .context("staged scheduled collection readback is incomplete")?;
    writer
        .persist_claimed_batch(claim, &staged)
        .await
        .map_err(|_| anyhow::anyhow!("snapshot_persistence_failed"))
}

async fn run_wb_dry_run(
    config: &ReportCollectorConfig,
    writer: &PostgresSnapshotWriter,
    account_id: &str,
    date: NaiveDate,
    kind: ReportKind,
) -> Result<()> {
    ensure!(
        config.mode() == ReportCollectorMode::WbDryRun,
        "wb-dry-run requires REPORT_COLLECTOR_MODE=wb_dry_run"
    );
    ensure!(
        !config.policy().enabled,
        "wb-dry-run refuses an enabled daily report policy"
    );
    let (period_start, period_end, cutoff_at) = dry_run_report_window(date, kind, Utc::now())?;
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
                account_id,
                cutoff_at,
                source_as_of,
                period_start,
                period_end,
                env!("CARGO_PKG_VERSION"),
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
    kind: ReportKind,
) -> Result<()> {
    ensure!(
        config.mode() == ReportCollectorMode::OzonDryRun,
        "ozon-dry-run requires REPORT_COLLECTOR_MODE=ozon_dry_run"
    );
    ensure!(
        !config.policy().enabled,
        "ozon-dry-run refuses an enabled daily report policy"
    );
    let (period_start, period_end, cutoff_at) = dry_run_report_window(date, kind, Utc::now())?;
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
        let performance_facts = performance_source
            .collect_extended(date)
            .await
            .map_err(|error| anyhow::anyhow!("performance_{}", error.code()))?;
        let snapshots = collect_complete_snapshots_extended(
            &transport,
            performance_facts.advertising,
            performance_facts.expenses,
            account_id.to_owned(),
            cutoff_at,
            Utc::now,
            period_start,
            period_end,
            env!("CARGO_PKG_VERSION").to_owned(),
        )
        .await
        .map_err(|error| anyhow::anyhow!("seller_{}", error.code()))?;
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

fn dry_run_report_window(
    date: NaiveDate,
    kind: ReportKind,
    now: DateTime<Utc>,
) -> Result<(DateTime<Utc>, DateTime<Utc>, DateTime<Utc>)> {
    let report_date = match kind {
        ReportKind::Morning => date.succ_opt().context("dry-run date is out of range")?,
        ReportKind::Evening => date,
    };
    let key = ReportKey {
        local_date: report_date,
        kind,
        recipient_id: "collector".to_owned(),
        report_version: 1,
    };
    let (start, end) = reporting_interval(&key)?;
    let cutoff = report_cutoff(&key)?;
    ensure!(
        cutoff <= now,
        "dry-run requires its EKB report cutoff to have passed"
    );
    ensure!(
        now <= cutoff + chrono::Duration::hours(24),
        "dry-run must start within 24 hours after its EKB report cutoff"
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
            () = ctrl_c => {}
            () = terminate => {}
        }
    }
    #[cfg(not(unix))]
    if signal::ctrl_c().await.is_err() {
        std::future::pending::<()>().await;
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Timelike};

    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn dry_run_cli_defaults_to_morning_and_accepts_only_explicit_report_kinds() {
        assert_eq!(
            parse_command(&arguments(&["collection-preflight"])).unwrap(),
            Command::CollectionPreflight
        );
        assert_eq!(
            parse_command(&arguments(&["refresh-once"])).unwrap(),
            Command::RefreshOnce
        );
        assert!(matches!(
            parse_command(&arguments(&["ozon-dry-run", "ozon", "2026-08-18"])).unwrap(),
            Command::OzonDryRun {
                kind: ReportKind::Morning,
                ..
            }
        ));
        assert!(matches!(
            parse_command(&arguments(&["wb-dry-run", "wb", "2026-08-19", "evening"])).unwrap(),
            Command::WbDryRun {
                kind: ReportKind::Evening,
                ..
            }
        ));
        for invalid in [
            arguments(&["ozon-dry-run", "bad/account", "2026-08-18"]),
            arguments(&["ozon-dry-run", "ozon", "18-08-2026"]),
            arguments(&["ozon-dry-run", "ozon", "2026-08-18", "night"]),
        ] {
            assert!(parse_command(&invalid).is_err());
        }
    }

    #[test]
    fn manager_refresh_reserves_scheduled_windows_and_api_pacing_tail() {
        // UTC 03:00 and 12:00 are 08:00 and 17:00 in Yekaterinburg.
        assert!(refresh_window_is_open(utc(2, 47, 59)));
        assert!(!refresh_window_is_open(utc(2, 48, 0)));
        assert!(!refresh_window_is_open(utc(3, 31, 5)));
        assert!(refresh_window_is_open(utc(3, 31, 6)));
        assert!(refresh_window_is_open(utc(11, 47, 59)));
        assert!(!refresh_window_is_open(utc(11, 48, 0)));
        assert!(!refresh_window_is_open(utc(12, 31, 5)));
        assert!(refresh_window_is_open(utc(12, 31, 6)));
    }

    fn utc(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 18, hour, minute, second)
            .unwrap()
    }

    #[test]
    fn dry_run_windows_match_the_exact_morning_and_evening_cutoffs() {
        let requested_date = NaiveDate::from_ymd_opt(2026, 8, 18).unwrap();
        let morning_now = Utc.with_ymd_and_hms(2026, 8, 19, 3, 10, 0).unwrap();
        let (morning_start, morning_end, morning_cutoff) =
            dry_run_report_window(requested_date, ReportKind::Morning, morning_now).unwrap();
        assert_eq!(business_date(morning_start), requested_date);
        assert_eq!(
            business_date(morning_end),
            NaiveDate::from_ymd_opt(2026, 8, 19).unwrap()
        );
        assert_eq!(morning_cutoff.hour(), 3);

        let evening_now = Utc.with_ymd_and_hms(2026, 8, 18, 12, 20, 0).unwrap();
        let (evening_start, evening_end, evening_cutoff) =
            dry_run_report_window(requested_date, ReportKind::Evening, evening_now).unwrap();
        assert_eq!(business_date(evening_start), requested_date);
        assert_eq!(evening_end, evening_cutoff);
        assert_eq!(evening_cutoff.hour(), 12);

        assert!(
            dry_run_report_window(
                requested_date,
                ReportKind::Evening,
                Utc.with_ymd_and_hms(2026, 8, 18, 11, 59, 59).unwrap(),
            )
            .is_err()
        );
        assert!(
            dry_run_report_window(
                requested_date,
                ReportKind::Evening,
                Utc.with_ymd_and_hms(2026, 8, 19, 11, 59, 59).unwrap(),
            )
            .is_ok()
        );
        assert!(
            dry_run_report_window(
                requested_date,
                ReportKind::Evening,
                Utc.with_ymd_and_hms(2026, 8, 19, 12, 0, 1).unwrap(),
            )
            .is_err()
        );
    }
}
