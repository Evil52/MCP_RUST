#![forbid(unsafe_code)]

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use anyhow::Result;
use mcp_ozon::{
    auth::JwtAuthenticator,
    config::{AppConfig, AuthConfig, TransportMode},
    http::build_router_with_cancellation_and_session_idle_timeout,
    ozon::OzonClient,
    ozon_performance::PerformanceClient,
    reporting::mcp_read::ReportingReader,
    runtime::{
        HTTP_CANCELLED_DRAIN_TIMEOUT, HTTP_HEADER_READ_TIMEOUT, HTTP_MAX_CONNECTIONS,
        HTTP_NATURAL_DRAIN_TIMEOUT, run_http_until_bounded_shutdown, serve_hardened_http,
    },
    server::OzonMcp,
    wb::WbClient,
};
use rmcp::ServiceExt;
use tokio::{signal, sync::Semaphore};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mcp_ozon=info,rmcp=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    let config = AppConfig::from_env()?;
    let reporting_database_url = match std::env::var("MCP_REPORTING_DATABASE_URL") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            anyhow::bail!("MCP_REPORTING_DATABASE_URL содержит недопустимую кодировку")
        }
    };
    let reporting_reader =
        ReportingReader::connect_optional(reporting_database_url.as_deref()).await?;
    let client = OzonClient::new(
        config.ozon_api_base_url.clone(),
        config.request_timeout,
        config.stores,
    )?;
    let wb_client = WbClient::new(config.request_timeout, config.wildberries_accounts);
    let performance_client =
        PerformanceClient::new(config.request_timeout, config.performance_stores)?;
    let registry = config.registry.clone();
    let server = match &config.auth {
        AuthConfig::Dev { actor_id } => OzonMcp::new(client, actor_id.clone(), registry),
        AuthConfig::Jwt(jwt_config) => {
            let authenticator = JwtAuthenticator::new(jwt_config.clone(), registry.clone())?;
            OzonMcp::new_authenticated(client, registry, authenticator)
        }
    }
    .with_wildberries_client(wb_client)
    .with_performance_client(performance_client)
    .with_reporting_reader(reporting_reader)
    .with_preview_features(
        config.ozon_postings_vnext,
        config.ozon_finance_accruals_preview,
    );

    match config.transport {
        TransportMode::Http => {
            serve_http(
                config.bind,
                config.max_sessions,
                config.session_idle_timeout,
                server,
            )
            .await
        }
        TransportMode::Stdio => {
            if matches!(config.auth, AuthConfig::Jwt(_)) {
                anyhow::bail!("MCP_AUTH_MODE=jwt поддерживается только с MCP_TRANSPORT=http");
            }
            serve_stdio(server).await
        }
    }
}

async fn serve_http(
    bind: std::net::SocketAddr,
    max_sessions: NonZeroUsize,
    session_idle_timeout: Duration,
    server: OzonMcp,
) -> Result<()> {
    let cancellation_token = CancellationToken::new();
    let router = build_router_with_cancellation_and_session_idle_timeout(
        server,
        max_sessions,
        session_idle_timeout,
        cancellation_token.clone(),
    );
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, endpoint = %format!("http://{bind}/mcp"), "MCP Ozon запущен");
    let (graceful_tx, graceful_rx) = tokio::sync::oneshot::channel();
    let serve = serve_hardened_http(
        listener,
        router,
        graceful_rx,
        Arc::new(Semaphore::new(HTTP_MAX_CONNECTIONS)),
        HTTP_HEADER_READ_TIMEOUT,
    );
    let result = run_http_until_bounded_shutdown(
        Box::pin(serve),
        Box::pin(shutdown_signal()),
        graceful_tx,
        cancellation_token,
        HTTP_NATURAL_DRAIN_TIMEOUT,
        HTTP_CANCELLED_DRAIN_TIMEOUT,
    )
    .await;
    if let Some(result) = result {
        result?;
    } else {
        tracing::warn!("HTTP shutdown deadline reached; remaining futures were dropped");
    }
    Ok(())
}

async fn serve_stdio(server: OzonMcp) -> Result<()> {
    tracing::info!("MCP Ozon запущен через stdio");
    let service = server.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;
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
