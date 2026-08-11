#![forbid(unsafe_code)]

use std::{future::Future, num::NonZeroUsize, time::Duration};

use anyhow::Result;
use mcp_ozon::{
    auth::JwtAuthenticator,
    config::{AppConfig, AuthConfig, TransportMode},
    http::build_router_with_cancellation,
    ozon::OzonClient,
    ozon_performance::PerformanceClient,
    server::OzonMcp,
    wb::WbClient,
};
use rmcp::ServiceExt;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const HTTP_NATURAL_DRAIN_TIMEOUT: Duration = Duration::from_secs(55);
const HTTP_CANCELLED_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

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
    .with_preview_features(
        config.ozon_postings_vnext,
        config.ozon_finance_accruals_preview,
    );

    match config.transport {
        TransportMode::Http => serve_http(config.bind, config.max_sessions, server).await,
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
    server: OzonMcp,
) -> Result<()> {
    let cancellation_token = CancellationToken::new();
    let router = build_router_with_cancellation(server, max_sessions, cancellation_token.clone());
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, endpoint = %format!("http://{bind}/mcp"), "MCP Ozon запущен");

    let (graceful_tx, graceful_rx) = tokio::sync::oneshot::channel();
    let serve = axum::serve(listener, router).with_graceful_shutdown(async move {
        let _ = graceful_rx.await;
    });
    let serve = async move { serve.await };
    let result = run_http_until_bounded_shutdown(
        serve,
        shutdown_signal(),
        graceful_tx,
        cancellation_token,
        HTTP_NATURAL_DRAIN_TIMEOUT,
        HTTP_CANCELLED_DRAIN_TIMEOUT,
    )
    .await;
    if let Some(result) = result {
        result?;
    } else {
        tracing::warn!(
            max_shutdown_seconds =
                (HTTP_NATURAL_DRAIN_TIMEOUT + HTTP_CANCELLED_DRAIN_TIMEOUT).as_secs(),
            "HTTP shutdown deadline reached; remaining connection futures were dropped"
        );
    }
    Ok(())
}

/// Stops accepting immediately after `shutdown`, then bounds both the natural
/// drain and the post-cancellation drain. Returning `None` means the owned
/// server future was dropped at the hard deadline.
async fn run_http_until_bounded_shutdown<Server, Shutdown>(
    server: Server,
    shutdown: Shutdown,
    graceful_tx: tokio::sync::oneshot::Sender<()>,
    cancellation_token: CancellationToken,
    natural_drain_timeout: Duration,
    cancelled_drain_timeout: Duration,
) -> Option<Server::Output>
where
    Server: Future,
    Shutdown: Future<Output = ()>,
{
    tokio::pin!(server);
    tokio::pin!(shutdown);

    tokio::select! {
        output = &mut server => return Some(output),
        () = &mut shutdown => {}
    }

    let _ = graceful_tx.send(());
    let natural_deadline = tokio::time::Instant::now() + natural_drain_timeout;
    if let Ok(output) = tokio::time::timeout_at(natural_deadline, &mut server).await {
        return Some(output);
    }

    tracing::warn!(
        drain_seconds = natural_drain_timeout.as_secs(),
        "HTTP requests did not drain naturally; cancelling MCP sessions and calls"
    );
    cancellation_token.cancel();

    let hard_deadline = natural_deadline + cancelled_drain_timeout;
    tokio::time::timeout_at(hard_deadline, &mut server)
        .await
        .ok()
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
            if let Err(error) = signal::ctrl_c().await {
                tracing::error!(%error, "не удалось ожидать Ctrl-C");
                std::future::pending::<()>().await;
            }
        };
        let terminate = async {
            match signal::unix::signal(signal::unix::SignalKind::terminate()) {
                Ok(mut stream) => {
                    let _ = stream.recv().await;
                }
                Err(error) => {
                    tracing::error!(%error, "не удалось ожидать SIGTERM");
                    std::future::pending::<()>().await;
                }
            }
        };
        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate => {}
        }
    }

    #[cfg(not(unix))]
    if let Err(error) = signal::ctrl_c().await {
        tracing::error!(%error, "не удалось ожидать Ctrl-C");
        std::future::pending::<()>().await;
    }

    tracing::info!("получен сигнал завершения; новые соединения больше не принимаются");
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll},
    };

    use super::*;

    struct PendingUntilDropped {
        dropped: Arc<AtomicBool>,
    }

    impl Future for PendingUntilDropped {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingUntilDropped {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn server_completion_before_signal_does_not_cancel() {
        let (graceful_tx, _graceful_rx) = tokio::sync::oneshot::channel();
        let cancellation_token = CancellationToken::new();
        let result = run_http_until_bounded_shutdown(
            std::future::ready(7_u8),
            std::future::pending(),
            graceful_tx,
            cancellation_token.clone(),
            Duration::from_millis(10),
            Duration::from_millis(10),
        )
        .await;

        assert_eq!(result, Some(7));
        assert!(!cancellation_token.is_cancelled());
    }

    #[tokio::test]
    async fn shutdown_allows_a_natural_drain_before_cancelling() {
        let (graceful_tx, graceful_rx) = tokio::sync::oneshot::channel();
        let cancellation_token = CancellationToken::new();
        let server = async move {
            graceful_rx.await.expect("graceful trigger");
            tokio::time::sleep(Duration::from_millis(5)).await;
            "drained"
        };
        let result = run_http_until_bounded_shutdown(
            server,
            std::future::ready(()),
            graceful_tx,
            cancellation_token.clone(),
            Duration::from_millis(50),
            Duration::from_millis(20),
        )
        .await;

        assert_eq!(result, Some("drained"));
        assert!(!cancellation_token.is_cancelled());
    }

    #[tokio::test]
    async fn natural_deadline_cancels_mcp_and_allows_a_short_final_drain() {
        let (graceful_tx, graceful_rx) = tokio::sync::oneshot::channel();
        let cancellation_token = CancellationToken::new();
        let observed_cancellation = cancellation_token.clone();
        let server = async move {
            graceful_rx.await.expect("graceful trigger");
            observed_cancellation.cancelled().await;
            "cancelled"
        };
        let result = run_http_until_bounded_shutdown(
            server,
            std::future::ready(()),
            graceful_tx,
            cancellation_token.clone(),
            Duration::from_millis(5),
            Duration::from_millis(50),
        )
        .await;

        assert_eq!(result, Some("cancelled"));
        assert!(cancellation_token.is_cancelled());
    }

    #[tokio::test]
    async fn hard_deadline_drops_the_server_future() {
        let (graceful_tx, _graceful_rx) = tokio::sync::oneshot::channel();
        let cancellation_token = CancellationToken::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let result = run_http_until_bounded_shutdown(
            PendingUntilDropped {
                dropped: Arc::clone(&dropped),
            },
            std::future::ready(()),
            graceful_tx,
            cancellation_token.clone(),
            Duration::from_millis(5),
            Duration::from_millis(5),
        )
        .await;

        assert_eq!(result, None);
        assert!(cancellation_token.is_cancelled());
        assert!(dropped.load(Ordering::SeqCst));
    }
}
