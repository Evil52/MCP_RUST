#![forbid(unsafe_code)]

use std::{fmt::Display, future::Future, num::NonZeroUsize, sync::Arc, time::Duration};

use anyhow::Result;
use hyper::server::conn::http1::Builder as HttpConnectionBuilder;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use mcp_ozon::{
    auth::JwtAuthenticator,
    config::{AppConfig, AuthConfig, TransportMode},
    http::build_router_with_cancellation_and_session_idle_timeout,
    ozon::OzonClient,
    ozon_performance::PerformanceClient,
    server::OzonMcp,
    wb::WbClient,
};
use rmcp::ServiceExt;
use tokio::{signal, sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const HTTP_MAX_CONNECTIONS: usize = 128;
const HTTP_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_ACCEPT_ERROR_BACKOFF: Duration = Duration::from_secs(1);
const HTTP_NATURAL_DRAIN_TIMEOUT: Duration = Duration::from_secs(55);
const HTTP_CANCELLED_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct ObserveFirstRequest<S> {
    inner: S,
    observed: Arc<tokio::sync::Notify>,
}

impl<S, Request> hyper::service::Service<Request> for ObserveFirstRequest<S>
where
    S: hyper::service::Service<Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn call(&self, request: Request) -> Self::Future {
        self.observed.notify_one();
        self.inner.call(request)
    }
}

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

/// Serves Axum through a configured HTTP/1.1 Hyper accept loop.
///
/// `axum::serve` intentionally exposes no connection-level configuration. Its
/// pinned implementation also creates Hyper's builder without a timer, which
/// disables Hyper's nominal HTTP/1 header timeout, and spawns one task for every
/// accepted socket. This loop supplies the timer explicitly and bounds accepted
/// sockets before handing them to Hyper.
///
/// The application listener is deliberately HTTP/1.1-only. Production TLS and
/// HTTP/2 terminate at the reverse proxy, whose idle-connection policy protects
/// this bounded internal listener from indefinitely retained HTTP/2 sockets.
async fn serve_hardened_http(
    listener: tokio::net::TcpListener,
    router: axum::Router,
    mut graceful_rx: tokio::sync::oneshot::Receiver<()>,
    connection_permits: Arc<Semaphore>,
    header_read_timeout: Duration,
) -> std::io::Result<()> {
    let mut http = HttpConnectionBuilder::new();
    http.timer(TokioTimer::new())
        .header_read_timeout(header_read_timeout);
    let http = Arc::new(http);

    let (connection_shutdown_tx, _) = tokio::sync::watch::channel(());
    let mut connections = JoinSet::new();

    'accept: loop {
        // Acquire before accept: sockets beyond the cap remain in the kernel
        // backlog and consume no userspace connection task or parser buffer.
        let permit = loop {
            tokio::select! {
                biased;
                _ = &mut graceful_rx => break 'accept,
                completed = connections.join_next(), if !connections.is_empty() => {
                    if let Some(completed) = completed {
                        log_connection_completion(completed);
                    }
                }
                permit = Arc::clone(&connection_permits).acquire_owned() => {
                    break permit.expect("the private HTTP connection semaphore is never closed");
                }
            }
        };

        let accepted = tokio::select! {
            biased;
            _ = &mut graceful_rx => {
                drop(permit);
                break 'accept;
            }
            accepted = listener.accept() => accepted,
        };
        let (stream, peer_addr) = match accepted {
            Ok(accepted) => accepted,
            Err(error) => {
                drop(permit);
                tracing::warn!(%error, "HTTP accept failed; retrying after backoff");
                tokio::select! {
                    biased;
                    _ = &mut graceful_rx => break 'accept,
                    () = tokio::time::sleep(HTTP_ACCEPT_ERROR_BACKOFF) => {}
                }
                continue;
            }
        };
        if let Err(error) = stream.set_nodelay(true) {
            drop(permit);
            tracing::warn!(%peer_addr, %error, "failed to enable TCP_NODELAY; closing connection");
            continue;
        }

        let http = Arc::clone(&http);
        let mut shutdown_rx = connection_shutdown_tx.subscribe();
        let service = TowerToHyperService::new(router.clone());
        connections.spawn(async move {
            let first_request = Arc::new(tokio::sync::Notify::new());
            let service = ObserveFirstRequest {
                inner: service,
                observed: Arc::clone(&first_request),
            };
            let io = TokioIo::new(stream);
            let mut connection = std::pin::pin!(http.serve_connection(io, service));
            let mut first_request_deadline =
                std::pin::pin!(tokio::time::sleep(header_read_timeout));
            let mut first_request_seen = false;
            let mut graceful_started = false;

            let result = loop {
                tokio::select! {
                    biased;
                    result = connection.as_mut() => break result,
                    () = first_request.notified(), if !first_request_seen => {
                        first_request_seen = true;
                    }
                    _ = shutdown_rx.changed(), if !graceful_started => {
                        if !first_request_seen {
                            // No request can be in flight, so an immediate close
                            // is both graceful and resistant to pre-header stalls.
                            break Ok(());
                        }
                        graceful_started = true;
                        connection.as_mut().graceful_shutdown();
                    }
                    () = &mut first_request_deadline, if !first_request_seen => {
                        tracing::debug!(%peer_addr, "closing connection that did not produce headers before the deadline");
                        break Ok(());
                    }
                }
            };
            drop(permit);
            result
        });
    }

    drop(listener);
    let _ = connection_shutdown_tx.send(());
    while let Some(completed) = connections.join_next().await {
        log_connection_completion(completed);
    }
    Ok(())
}

fn log_connection_completion<E>(completed: Result<Result<(), E>, tokio::task::JoinError>)
where
    E: Display,
{
    match completed {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, "HTTP connection closed with a protocol error"),
        Err(error) if error.is_cancelled() => {}
        Err(error) => tracing::warn!(%error, "HTTP connection task failed"),
    }
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
        net::SocketAddr,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll},
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
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

    async fn hardened_test_server(
        max_connections: usize,
        header_read_timeout: Duration,
    ) -> (
        SocketAddr,
        Arc<Semaphore>,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<std::io::Result<()>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener binds");
        let address = listener.local_addr().expect("listener has an address");
        let router = axum::Router::new().route("/health", axum::routing::get(|| async { "ok" }));
        let permits = Arc::new(Semaphore::new(max_connections));
        let (graceful_tx, graceful_rx) = tokio::sync::oneshot::channel();
        let server_permits = Arc::clone(&permits);
        let task = tokio::spawn(serve_hardened_http(
            listener,
            router,
            graceful_rx,
            server_permits,
            header_read_timeout,
        ));
        (address, permits, graceful_tx, task)
    }

    async fn wait_for_available_permits(permits: &Semaphore, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if permits.available_permits() == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("connection permit count reaches the expected value");
    }

    async fn read_until_closed(stream: &mut TcpStream) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut received = Vec::new();
            let mut chunk = [0_u8; 512];
            loop {
                match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => return received,
                    Ok(read) => received.extend_from_slice(&chunk[..read]),
                }
            }
        })
        .await
        .expect("connection closes before the test deadline")
    }

    async fn read_until_contains(stream: &mut TcpStream, expected: &[u8]) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut received = Vec::new();
            let mut chunk = [0_u8; 512];
            loop {
                let read = stream
                    .read(&mut chunk)
                    .await
                    .expect("test response is readable");
                assert_ne!(read, 0, "connection closed before the complete response");
                received.extend_from_slice(&chunk[..read]);
                if received
                    .windows(expected.len())
                    .any(|window| window == expected)
                {
                    return received;
                }
            }
        })
        .await
        .expect("response arrives before the test deadline")
    }

    #[tokio::test]
    async fn accepted_connection_count_is_hard_capped() {
        let (address, permits, graceful_tx, server_task) =
            hardened_test_server(2, Duration::from_secs(5)).await;

        let mut first = TcpStream::connect(address)
            .await
            .expect("first client connects");
        first.write_all(b"G").await.expect("first client writes");
        let mut second = TcpStream::connect(address)
            .await
            .expect("second client connects");
        second.write_all(b"G").await.expect("second client writes");
        wait_for_available_permits(&permits, 0).await;

        let mut queued = TcpStream::connect(address)
            .await
            .expect("queued client reaches the kernel backlog");
        queued
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("queued client writes a complete request");
        let mut probe = [0_u8; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(200), queued.read(&mut probe))
                .await
                .is_err(),
            "a connection beyond the cap must not reach the HTTP service"
        );

        drop(first);
        let response = read_until_closed(&mut queued).await;
        assert!(
            response
                .windows(b"200 OK".len())
                .any(|part| part == b"200 OK"),
            "queued request was not admitted after capacity recovered: {}",
            String::from_utf8_lossy(&response)
        );

        drop(second);
        drop(queued);
        let _ = graceful_tx.send(());
        server_task
            .await
            .expect("server task joins")
            .expect("server shuts down cleanly");
    }

    #[tokio::test]
    async fn initial_and_keep_alive_header_deadlines_release_capacity() {
        let header_timeout = Duration::from_millis(150);
        let (address, permits, graceful_tx, server_task) =
            hardened_test_server(1, header_timeout).await;

        let mut idle = TcpStream::connect(address)
            .await
            .expect("idle client connects");
        wait_for_available_permits(&permits, 0).await;
        assert!(read_until_closed(&mut idle).await.is_empty());

        let mut keep_alive = TcpStream::connect(address)
            .await
            .expect("keep-alive client connects");
        keep_alive
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("first request writes");
        let response = read_until_contains(&mut keep_alive, b"\r\n\r\nok").await;
        assert!(
            response
                .windows(b"200 OK".len())
                .any(|part| part == b"200 OK"),
            "first request failed: {}",
            String::from_utf8_lossy(&response)
        );

        keep_alive
            .write_all(b"GET /health HTTP/1.1\r\nHost: local")
            .await
            .expect("partial second header writes");
        let _ = read_until_closed(&mut keep_alive).await;

        let _ = graceful_tx.send(());
        server_task
            .await
            .expect("server task joins")
            .expect("server shuts down cleanly");
    }

    #[tokio::test]
    async fn prior_knowledge_http2_is_rejected_and_releases_capacity() {
        let (address, permits, graceful_tx, server_task) =
            hardened_test_server(1, Duration::from_millis(500)).await;

        let mut h2 = TcpStream::connect(address)
            .await
            .expect("HTTP/2 probe connects");
        wait_for_available_permits(&permits, 0).await;
        h2.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .expect("HTTP/2 prior-knowledge preface writes");
        let rejected = read_until_closed(&mut h2).await;
        assert!(
            !rejected
                .windows(b"200 OK".len())
                .any(|part| part == b"200 OK"),
            "the plaintext application listener must not negotiate HTTP/2: {}",
            String::from_utf8_lossy(&rejected)
        );
        let mut h1 = TcpStream::connect(address)
            .await
            .expect("HTTP/1 client connects after rejection");
        h1.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("HTTP/1 request writes");
        let response = read_until_closed(&mut h1).await;
        assert!(
            response
                .windows(b"200 OK".len())
                .any(|part| part == b"200 OK"),
            "HTTP/1 must remain available after an HTTP/2 rejection: {}",
            String::from_utf8_lossy(&response)
        );

        let _ = graceful_tx.send(());
        server_task
            .await
            .expect("server task joins")
            .expect("server shuts down cleanly");
        assert_eq!(permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn graceful_shutdown_immediately_closes_pre_header_connections() {
        let (address, permits, graceful_tx, server_task) =
            hardened_test_server(1, Duration::from_secs(30)).await;
        let mut idle = TcpStream::connect(address)
            .await
            .expect("idle client connects");
        wait_for_available_permits(&permits, 0).await;

        graceful_tx.send(()).expect("graceful receiver is alive");
        tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .expect("pre-header connection cannot stall graceful shutdown")
            .expect("server task joins")
            .expect("server shuts down cleanly");
        assert!(read_until_closed(&mut idle).await.is_empty());
        assert_eq!(permits.available_permits(), 1);
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
