#![forbid(unsafe_code)]

use anyhow::{Result, bail};
use mcp_ozon::reporting::service::{ReportWorkerConfig, ReportWorkerMode};
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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
    if !healthcheck && !arguments.is_empty() {
        bail!("usage: report-worker [healthcheck]");
    }
    let config = ReportWorkerConfig::from_lookup(|key| std::env::var(key).ok())?;
    let (outbox, snapshots) = config.connect().await?;
    outbox.verify_runtime_contract().await?;
    snapshots.verify_runtime_contract().await?;
    if healthcheck {
        return Ok(());
    }
    if config.mode() != ReportWorkerMode::Disabled || config.policy().enabled {
        bail!("report delivery runtime is unavailable");
    }
    tracing::warn!("report worker is disabled; no snapshots, artifacts, or email are generated");
    shutdown_signal().await;
    Ok(())
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
