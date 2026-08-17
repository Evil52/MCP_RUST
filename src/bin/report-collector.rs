#![forbid(unsafe_code)]

use anyhow::{Result, bail};
use mcp_ozon::reporting::collector_service::{ReportCollectorConfig, ReportCollectorMode};
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
        bail!("usage: report-collector [healthcheck]");
    }
    let config = ReportCollectorConfig::from_lookup(|key| std::env::var(key).ok())?;
    let writer = config.connect_writer().await?;
    writer.verify_runtime_contract().await?;
    let targets = config.collection_plan()?;
    if healthcheck {
        tracing::info!(targets = targets.len(), "report collector preflight passed");
        return Ok(());
    }
    if config.mode() != ReportCollectorMode::Disabled || config.policy().enabled {
        bail!("report collection runtime is unavailable");
    }
    tracing::warn!(
        targets = targets.len(),
        "report collector is disabled; no marketplace request or snapshot write is performed"
    );
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
