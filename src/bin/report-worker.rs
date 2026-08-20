#![forbid(unsafe_code)]

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, NaiveDate, Utc};
use mcp_ozon::reporting::{
    ReportKey, ReportKind,
    artifact_store::persist_and_mark_ready,
    gmail_outbox::GmailOutboxWorker,
    postgres_outbox::{
        GenerationErrorClass, GenerationStatus, PostgresOutboxRepository, ReconciliationDecision,
    },
    postgres_snapshot::PostgresSnapshotRepository,
    preview::render_published_preview,
    report_cutoff,
    service::{ReportPreviewScope, ReportWorkerConfig, ReportWorkerMode},
};
use tokio::{
    signal,
    time::{MissedTickBehavior, timeout},
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const DRY_RUN_TICK: std::time::Duration = std::time::Duration::from_secs(60);
const MAX_GENERATIONS_PER_TICK: u16 = 16;
/// Bounds the queue reads of one tick, so a stalled scheduler still
/// advances its failure budget instead of hanging forever.
const TICK_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
/// Bounds one report generation, including rendering and artifact commit.
const GENERATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
/// Bounds one complete claim/load/OAuth/Gmail/persist canary attempt. Any
/// timeout after claiming leaves the row `sending` for reconciliation.
const DELIVERY_CANARY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
/// Consecutive failing ticks tolerated before the process exits for restart.
const MAX_CONSECUTIVE_TICK_FAILURES: u32 = 5;

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
    let healthcheck = matches!(arguments.as_slice(), [argument] if argument == "healthcheck");
    let config = ReportWorkerConfig::from_lookup(|key| std::env::var(key).ok())?;
    config.artifact_store().verify_writable()?;
    let (outbox, snapshots) = config.connect().await?;
    outbox.verify_runtime_contract().await?;
    snapshots.verify_runtime_contract().await?;
    let targets = config.collection_plan()?;
    if healthcheck {
        tracing::info!(targets = targets.len(), "report source preflight passed");
        return Ok(());
    }
    if matches!(arguments.as_slice(), [command] if command == "deliver-one") {
        ensure!(
            config.mode() == ReportWorkerMode::DeliveryCanary && config.policy().enabled,
            "deliver-one requires explicit delivery_canary mode and enabled policy"
        );
        let worker = config.delivery_worker(outbox)?;
        let outcome = timeout(DELIVERY_CANARY_TIMEOUT, worker.deliver_one())
            .await
            .context(
                "Gmail canary exceeded 60 seconds; any claimed row remains sending for reconciliation",
            )??;
        tracing::info!(?outcome, "single Gmail canary attempt finished");
        return Ok(());
    }
    if let [command, audience_id] = arguments.as_slice()
        && command == "mail-preflight"
    {
        ensure!(
            config.mode() == ReportWorkerMode::ScheduledDelivery && config.policy().enabled,
            "mail-preflight requires explicit scheduled_delivery mode and enabled policy"
        );
        ensure!(
            config
                .policy()
                .audiences
                .iter()
                .any(|audience| audience.id == *audience_id),
            "mail-preflight audience is outside the validated policy"
        );
        let receipt = outbox
            .verify_mail_activation(audience_id, config.policy().version, Utc::now())
            .await?;
        tracing::info!(
            audience_id,
            canary_sent_at = %receipt.canary_sent_at,
            "scheduled mail activation preflight passed"
        );
        return Ok(());
    }
    if let [
        command,
        audience_id,
        batch_id,
        attempt_no,
        provider_message_id,
        confirmation,
    ] = arguments.as_slice()
        && command == "reconcile-sent"
    {
        ensure!(
            confirmation == "--confirm-gmail-sent",
            "reconcile-sent requires --confirm-gmail-sent"
        );
        ensure_reconciliation_mode(&config, audience_id)?;
        let outcome = outbox
            .reconcile_sending(
                parse_positive_i64(batch_id, "batch id")?,
                parse_attempt_no(attempt_no)?,
                audience_id,
                config.policy().version,
                Utc::now(),
                &ReconciliationDecision::ConfirmedSent {
                    provider_message_id: provider_message_id.clone(),
                },
            )
            .await?;
        tracing::info!(
            audience_id,
            ?outcome,
            "ambiguous Gmail send reconciled as sent"
        );
        return Ok(());
    }
    if let [command, audience_id, batch_id, attempt_no, confirmation] = arguments.as_slice()
        && command == "reconcile-suppress"
    {
        ensure!(
            confirmation == "--confirm-provider-outcome-unknown",
            "reconcile-suppress requires --confirm-provider-outcome-unknown"
        );
        ensure_reconciliation_mode(&config, audience_id)?;
        let outcome = outbox
            .reconcile_sending(
                parse_positive_i64(batch_id, "batch id")?,
                parse_attempt_no(attempt_no)?,
                audience_id,
                config.policy().version,
                Utc::now(),
                &ReconciliationDecision::SuppressedUnknown,
            )
            .await?;
        tracing::info!(
            audience_id,
            ?outcome,
            "ambiguous Gmail send suppressed without retry"
        );
        return Ok(());
    }
    if let [
        command,
        audience_id,
        actor_id,
        local_date,
        kind,
        cutoff,
        output_dir,
    ] = arguments.as_slice()
    {
        if command != "preview" {
            return usage();
        }
        ensure!(
            config.mode() == ReportWorkerMode::Disabled && !config.policy().enabled,
            "manual preview requires disabled delivery mode"
        );
        let scope = config.preview_scope(audience_id, actor_id)?;
        let key = ReportKey {
            local_date: NaiveDate::parse_from_str(local_date, "%Y-%m-%d")
                .context("preview date must use YYYY-MM-DD")?,
            kind: parse_kind(kind)?,
            recipient_id: scope.actor_id.clone(),
            report_version: config.policy().version,
        };
        let cutoff = DateTime::parse_from_rfc3339(cutoff)
            .context("preview cutoff must be RFC3339")?
            .with_timezone(&Utc);
        let manifest = snapshots
            .load_manifest(cutoff, scope.accounts.clone())
            .await?;
        let facts = snapshots.load_report_facts(&manifest).await?;
        let preview =
            render_published_preview(&key, &scope.manager_name, Utc::now(), &manifest, facts)?;
        let (html_path, xlsx_path) = write_preview(Path::new(output_dir), &scope, &key, &preview)?;
        tracing::info!(
            audience_id = scope.audience_id,
            actor_id = scope.actor_id,
            html = %html_path.display(),
            xlsx = %xlsx_path.display(),
            xlsx_bytes = preview.receipt.size_bytes,
            sha256 = preview.receipt.artifact.sha256,
            "manual report preview generated without delivery"
        );
        return Ok(());
    }
    if let [command, batch_id] = arguments.as_slice() {
        if command != "generate" {
            return usage();
        }
        ensure!(
            matches!(
                (config.mode(), config.policy().enabled),
                (ReportWorkerMode::Disabled, false) | (ReportWorkerMode::DryRun, true)
            ),
            "artifact generation requires a consistent non-delivery mode"
        );
        let batch_id = parse_positive_i64(batch_id, "generation batch id")?;
        generate_batch(&config, &outbox, &snapshots, batch_id, Utc::now()).await?;
        return Ok(());
    }
    if !arguments.is_empty() {
        return usage();
    }
    match (config.mode(), config.policy().enabled) {
        (ReportWorkerMode::Disabled, false) => {
            tracing::warn!(
                targets = targets.len(),
                "report worker is disabled; no snapshots, artifacts, or email are generated"
            );
            shutdown_signal().await;
        }
        (ReportWorkerMode::DryRun, true) => {
            tracing::info!(
                targets = targets.len(),
                tick_seconds = DRY_RUN_TICK.as_secs(),
                "report dry-run scheduler started; email delivery is unavailable"
            );
            tokio::select! {
                result = run_dry_scheduler(&config, &outbox, &snapshots) => result?,
                _ = shutdown_signal() => {}
            }
        }
        (ReportWorkerMode::DeliveryCanary, true) => {
            bail!("delivery_canary mode requires the explicit deliver-one command")
        }
        (ReportWorkerMode::ScheduledDelivery, true) => {
            let activation_audience_id = config
                .activation_audience_id()
                .context("scheduled mail activation audience is unavailable")?;
            // The gate is deliberately hard: scheduled Gmail delivery never
            // starts without recent provider-backed proof. It also trips when
            // every delivery has failed for a day, and `restart: unless-stopped`
            // then restarts the container into the same refusal, so the message
            // has to name the operator action instead of only the condition.
            let receipt = outbox
                .verify_mail_activation(activation_audience_id, config.policy().version, Utc::now())
                .await
                .with_context(|| {
                    format!(
                        "scheduled mail activation refused for audience {activation_audience_id}: \
                         no successful Gmail send in the last 24 hours, or an unreconciled \
                         sending row remains. Restarting will not clear this. Reconcile any \
                         ambiguous attempt with `report-worker reconcile-sent`/`reconcile-suppress`, \
                         then re-run the `delivery_canary` `deliver-one` command before scheduling"
                    )
                })?;
            let delivery = config.delivery_worker(outbox.clone())?;
            tracing::info!(
                targets = targets.len(),
                tick_seconds = DRY_RUN_TICK.as_secs(),
                activation_audience_id,
                canary_sent_at = %receipt.canary_sent_at,
                "scheduled report generation and Gmail delivery started"
            );
            tokio::select! {
                result = run_delivery_scheduler(&config, &outbox, &snapshots, &delivery) => result?,
                _ = shutdown_signal() => {}
            }
        }
        _ => bail!("report-worker mode and policy enabled flag are inconsistent"),
    }
    Ok(())
}

/// Runs scheduled planning and generation until shutdown or sustained failure.
///
/// A tick that cannot read its work queue is transient by default: the
/// database may be restarting or failing over, and the next tick re-reads from
/// authoritative state. Only a sustained inability to make progress ends the
/// loop, which exits the process so the supervisor restarts it with a fresh
/// session instead of leaving a live container that silently does nothing.
async fn run_dry_scheduler(
    config: &ReportWorkerConfig,
    outbox: &PostgresOutboxRepository,
    snapshots: &PostgresSnapshotRepository,
) -> Result<()> {
    let mut timer = tokio::time::interval(DRY_RUN_TICK);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut consecutive_failures = 0_u32;
    loop {
        timer.tick().await;
        match run_scheduler_tick(config, outbox, snapshots, Utc::now()).await {
            Ok(()) => consecutive_failures = 0,
            Err(error) => {
                consecutive_failures += 1;
                ensure!(
                    consecutive_failures < MAX_CONSECUTIVE_TICK_FAILURES,
                    "daily report scheduler failed {consecutive_failures} consecutive ticks; \
                     exiting so the supervisor can restart it"
                );
                tracing::warn!(
                    consecutive_failures,
                    error = %error,
                    "daily report scheduler tick failed; retrying on the next tick"
                );
            }
        }
    }
}

/// Runs generation and a bounded delivery drain on the same minute cadence.
///
/// Planning remains authoritative in PostgreSQL, so a restart inside a report
/// deadline catches up ready work without creating a second occurrence. Each
/// send is still delegated to the one-attempt coordinator: an ambiguous send
/// stays `sending` and can never be claimed by a later tick.
async fn run_delivery_scheduler(
    config: &ReportWorkerConfig,
    outbox: &PostgresOutboxRepository,
    snapshots: &PostgresSnapshotRepository,
    delivery: &GmailOutboxWorker,
) -> Result<()> {
    let mut timer = tokio::time::interval(DRY_RUN_TICK);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut consecutive_failures = 0_u32;
    loop {
        timer.tick().await;
        let result = async {
            run_scheduler_tick(config, outbox, snapshots, Utc::now()).await?;
            run_delivery_tick(delivery).await
        }
        .await;
        match result {
            Ok(()) => consecutive_failures = 0,
            Err(error) => {
                consecutive_failures += 1;
                ensure!(
                    consecutive_failures < MAX_CONSECUTIVE_TICK_FAILURES,
                    "daily report delivery scheduler failed {consecutive_failures} consecutive ticks; \
                     exiting so the supervisor can restart it"
                );
                tracing::warn!(
                    consecutive_failures,
                    error = %error,
                    "daily report delivery tick failed; retrying on the next tick"
                );
            }
        }
    }
}

async fn run_delivery_tick(delivery: &GmailOutboxWorker) -> Result<()> {
    let outcome = delivery.deliver_ready().await?;
    tracing::info!(
        attempts = outcome.attempts,
        queue_drained = outcome.queue_drained,
        "bounded Gmail delivery pass finished"
    );
    Ok(())
}

/// Executes exactly one planning and generation pass.
///
/// The queue reads are bounded so a stalled tick cannot hold the loop open
/// past the failure budget, and each generation is bounded separately so one
/// slow report cannot consume the whole pass.
async fn run_scheduler_tick(
    config: &ReportWorkerConfig,
    outbox: &PostgresOutboxRepository,
    snapshots: &PostgresSnapshotRepository,
    now: DateTime<Utc>,
) -> Result<()> {
    let (planned, batch_ids) = timeout(TICK_QUERY_TIMEOUT, async {
        let planned = outbox.plan_due(now, config.policy()).await?;
        let batch_ids = outbox
            .pending_generation_ids(now, MAX_GENERATIONS_PER_TICK)
            .await?;
        anyhow::Ok((planned, batch_ids))
    })
    .await
    .context("daily report scheduler queue read timed out")??;
    tracing::info!(
        planned = planned.len(),
        candidates = batch_ids.len(),
        "daily report dry-run tick"
    );
    for batch_id in batch_ids {
        // A single unrenderable report must never stop the pass: the rest of
        // the queue is independent of it.
        let failure = match timeout(
            GENERATION_TIMEOUT,
            generate_batch(config, outbox, snapshots, batch_id, now),
        )
        .await
        {
            Ok(Ok(())) => continue,
            Ok(Err(error)) => {
                tracing::warn!(
                    batch_id,
                    error = %error,
                    "daily report generation deferred"
                );
                GenerationErrorClass::Failed
            }
            Err(_) => {
                tracing::warn!(
                    batch_id,
                    timeout_seconds = GENERATION_TIMEOUT.as_secs(),
                    "daily report generation exceeded its budget"
                );
                GenerationErrorClass::Timeout
            }
        };
        // Recording the failure is what holds this batch back from the next
        // scans. If even that cannot be written the batch simply retries next
        // tick, which is the pre-existing behaviour rather than a new risk.
        if outbox
            .record_generation_failure(batch_id, now, failure)
            .await
            .is_err()
        {
            tracing::warn!(
                batch_id,
                "daily report generation failure could not be recorded; \
                 the batch stays eligible for the next tick"
            );
        }
    }
    Ok(())
}

async fn generate_batch(
    config: &ReportWorkerConfig,
    outbox: &PostgresOutboxRepository,
    snapshots: &PostgresSnapshotRepository,
    batch_id: i64,
    now: DateTime<Utc>,
) -> Result<()> {
    let candidate = outbox.generation_candidate(batch_id, now).await?;
    let scope = config.generation_scope(&candidate.key)?;
    let cutoff = report_cutoff(&candidate.key)?;
    let manifest = snapshots
        .load_manifest(cutoff, scope.accounts.clone())
        .await?;
    let facts = snapshots.load_report_facts(&manifest).await?;
    let report = render_published_preview(
        &candidate.key,
        &scope.report_name,
        candidate.generated_at,
        &manifest,
        facts,
    )?;
    if candidate.status == GenerationStatus::Planned {
        outbox.start_generation(candidate.batch_id).await?;
    }
    let receipt = persist_and_mark_ready(
        config.artifact_store(),
        outbox,
        candidate.batch_id,
        &report.bundle,
    )
    .await?;
    tracing::info!(
        batch_id = candidate.batch_id,
        audience_id = scope.audience_id,
        object_key = receipt.artifact.object_key,
        xlsx_bytes = receipt.xlsx_size_bytes,
        html_bytes = receipt.html_size_bytes,
        "daily report artifact generated"
    );
    Ok(())
}

fn usage() -> Result<()> {
    bail!(
        "usage: report-worker [healthcheck | deliver-one | mail-preflight <audience-id> | reconcile-sent <audience-id> <batch-id> <attempt-no> <provider-message-id> --confirm-gmail-sent | reconcile-suppress <audience-id> <batch-id> <attempt-no> --confirm-provider-outcome-unknown | generate <batch-id> | preview <audience-id> <actor-id> <YYYY-MM-DD> <morning|evening> <cutoff-rfc3339> <existing-output-dir>]"
    )
}

fn ensure_reconciliation_mode(config: &ReportWorkerConfig, audience_id: &str) -> Result<()> {
    ensure!(
        config.mode() == ReportWorkerMode::DryRun && config.policy().enabled,
        "mail reconciliation requires dry_run mode and an enabled policy"
    );
    ensure!(
        config
            .policy()
            .audiences
            .iter()
            .any(|audience| audience.id == audience_id),
        "mail reconciliation audience is outside the validated policy"
    );
    Ok(())
}

fn parse_positive_i64(value: &str, name: &str) -> Result<i64> {
    let value = value
        .parse::<i64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    ensure!(value > 0, "{name} must be a positive integer");
    Ok(value)
}

fn parse_attempt_no(value: &str) -> Result<u8> {
    let value = value
        .parse::<u8>()
        .context("attempt number must be between 1 and 5")?;
    ensure!(
        (1..=5).contains(&value),
        "attempt number must be between 1 and 5"
    );
    Ok(value)
}

fn parse_kind(value: &str) -> Result<ReportKind> {
    match value {
        "morning" => Ok(ReportKind::Morning),
        "evening" => Ok(ReportKind::Evening),
        _ => bail!("preview kind must be morning or evening"),
    }
}

fn write_preview(
    output_dir: &Path,
    scope: &ReportPreviewScope,
    key: &ReportKey,
    preview: &mcp_ozon::reporting::preview::ReportPreview,
) -> Result<(PathBuf, PathBuf)> {
    ensure!(
        output_dir
            .metadata()
            .is_ok_and(|metadata| metadata.is_dir()),
        "preview output directory must already exist"
    );
    let kind = match key.kind {
        ReportKind::Morning => "morning",
        ReportKind::Evening => "evening",
    };
    let stem = format!(
        "daily-report-{}-{}-{}",
        scope.actor_id, key.local_date, kind
    );
    let html_path = output_dir.join(format!("{stem}.html"));
    let xlsx_path = output_dir.join(format!("{stem}.xlsx"));
    write_new(&html_path, preview.bundle.html.as_bytes())?;
    if let Err(error) = write_new(&xlsx_path, &preview.bundle.xlsx) {
        let _ = fs::remove_file(&html_path);
        return Err(error);
    }
    Ok((html_path, xlsx_path))
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("preview output {} cannot be created", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("preview output {} cannot be written", path.display()))?;
    file.sync_all()
        .with_context(|| format!("preview output {} cannot be synchronized", path.display()))
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
