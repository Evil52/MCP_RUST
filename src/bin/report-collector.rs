#![forbid(unsafe_code)]

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone, Utc};
use mcp_ozon::reporting::{
    collector_service::{ReportCollectorConfig, ReportCollectorMode},
    ozon_source::{OzonClientReportTransport, OzonReportSource, collect_and_persist},
};
use tokio::signal;
use tokio::time::{Duration, timeout};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// The individual Seller request limit is configured by the report collector.
/// This bounds the entire manual account run, including pagination and the
/// transactional snapshot publication, so a slow upstream cannot hold an
/// operator invocation forever.
const OZON_DRY_RUN_TOTAL_DEADLINE: Duration = Duration::from_secs(10 * 60);

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
    let config = ReportCollectorConfig::from_lookup(|key| std::env::var(key).ok())?;
    let writer = config.connect_writer().await?;
    writer.verify_runtime_contract().await?;
    let targets = config.collection_plan()?;
    if matches!(&command, Command::Healthcheck) {
        tracing::info!(targets = targets.len(), "report collector preflight passed");
        return Ok(());
    }
    if let Command::OzonDryRun { account_id, date } = command {
        run_ozon_dry_run(&config, &writer, &account_id, date).await?;
        return Ok(());
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
}

fn parse_command(arguments: &[String]) -> Result<Command> {
    match arguments {
        [] => Ok(Command::ServeDisabled),
        [argument] if argument == "healthcheck" => Ok(Command::Healthcheck),
        [command, account_id, date] if command == "ozon-dry-run" => {
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
            Ok(Command::OzonDryRun {
                account_id: account_id.clone(),
                date,
            })
        }
        _ => {
            bail!("usage: report-collector [healthcheck | ozon-dry-run <account-id> <YYYY-MM-DD>]")
        }
    }
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
    let (period_start, period_end) = complete_yekaterinburg_day(date, Utc::now())?;
    let store = config.ozon_dry_run_store(account_id)?;
    let client = config.ozon_dry_run_client()?;
    let source = OzonReportSource::new(OzonClientReportTransport::new(client, store));
    let cutoff_at = Utc::now();
    let snapshot_ids = timeout(
        OZON_DRY_RUN_TOTAL_DEADLINE,
        collect_and_persist(
            &source,
            writer,
            account_id.to_owned(),
            cutoff_at,
            cutoff_at,
            period_start,
            period_end,
            env!("CARGO_PKG_VERSION").to_owned(),
        ),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "Ozon dry-run failed (collection_deadline); no partial report snapshots were published"
        )
    })?
    .map_err(|error| {
        anyhow::anyhow!(
            "Ozon dry-run failed ({}); no partial report snapshots were published",
            error.code()
        )
    })?;
    tracing::info!(
        account_id,
        snapshots = snapshot_ids.len(),
        "Ozon dry-run completed and atomically published its complete seller snapshot set"
    );
    Ok(())
}

fn complete_yekaterinburg_day(
    date: NaiveDate,
    now: DateTime<Utc>,
) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let offset = FixedOffset::east_opt(5 * 60 * 60).expect("valid UTC+05:00 offset");
    let start = offset
        .from_local_datetime(
            &date
                .and_hms_opt(0, 0, 0)
                .context("dry-run date is out of range")?,
        )
        .single()
        .context("dry-run start is out of range")?
        .with_timezone(&Utc);
    let end_date = date.succ_opt().context("dry-run end is out of range")?;
    let end = offset
        .from_local_datetime(
            &end_date
                .and_hms_opt(0, 0, 0)
                .context("dry-run end is out of range")?,
        )
        .single()
        .context("dry-run end is out of range")?
        .with_timezone(&Utc);
    ensure!(end <= now, "dry-run requires a completed Yekaterinburg day");
    Ok((start, end))
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
