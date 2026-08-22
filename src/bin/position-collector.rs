#![forbid(unsafe_code)]

use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, FixedOffset, Utc};
use mcp_ozon::position_collector::{CollectorRuntimeConfig, CollectorRuntimeMode};
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mcp_ozon::position_collector=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    enum Command {
        Serve,
        Healthcheck,
        CanaryPlan(DateTime<FixedOffset>),
    }
    let command = match std::env::args().skip(1).collect::<Vec<_>>().as_slice() {
        [] => Command::Serve,
        [argument] if argument == "healthcheck" => Command::Healthcheck,
        [argument, slot] if argument == "canary-plan" => Command::CanaryPlan(
            slot.parse()
                .map_err(|_| anyhow!("canary slot must be RFC3339 UTC"))?,
        ),
        _ => bail!("usage: position-collector [healthcheck|canary-plan <UTC-slot>]"),
    };
    let config = CollectorRuntimeConfig::from_env()?;
    let repository = config.connect_repository().await?;
    repository.verify_runtime_contract().await?;
    match command {
        Command::Healthcheck => return Ok(()),
        Command::CanaryPlan(slot) => {
            if slot.offset().local_minus_utc() != 0 {
                bail!("canary slot must use UTC offset Z");
            }
            let slot = slot.with_timezone(&Utc);
            let plan = repository.load_canary_plan(slot).await?;
            tracing::info!(
                slot = %plan.slot(),
                targets = plan.target_count(),
                queries = plan.queries().len(),
                "manual canary plan is valid; no marketplace request was made"
            );
            return Ok(());
        }
        Command::Serve => {}
    }
    if config.mode() != CollectorRuntimeMode::Disabled {
        return Err(anyhow!("collector runtime mode is unavailable"));
    }
    tracing::warn!(
        "position collector runtime is disabled; no scheduler, browser or marketplace request will run"
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
            () = ctrl_c => {}
            () = terminate => {}
        }
    }

    #[cfg(not(unix))]
    if signal::ctrl_c().await.is_err() {
        std::future::pending::<()>().await;
    }
}
