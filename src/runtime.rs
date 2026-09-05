//! Shared bounded HTTP runtime and process probes for isolated binaries.

use std::{
    future::Future,
    io::{self, Write as _},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use hyper::server::conn::http1::Builder as HttpConnectionBuilder;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;

pub const HTTP_MAX_CONNECTIONS: usize = 128;
pub const HTTP_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_ACCEPT_ERROR_BACKOFF: Duration = Duration::from_secs(1);
pub const HTTP_NATURAL_DRAIN_TIMEOUT: Duration = Duration::from_secs(55);
pub const HTTP_CANCELLED_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Handles the side-effect-free probe shared by every shipped runtime binary.
///
/// The probe intentionally runs before logging, configuration, credentials,
/// database connections, or marketplace clients are initialized. CI can
/// therefore execute the optimized binary copied into each final image without
/// granting that image secrets or network access.
pub fn print_runtime_version_if_requested(
    binary_name: &str,
    arguments: &[String],
) -> io::Result<bool> {
    if !matches!(arguments, [argument] if argument == "--version") {
        return Ok(false);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{binary_name} {}", env!("CARGO_PKG_VERSION"))?;
    Ok(true)
}

struct AcceptedTcpStream {
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    nodelay_result: std::io::Result<()>,
}

trait TcpAcceptor: Send + Sync + 'static {
    fn accept(
        &self,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<AcceptedTcpStream>> + Send + '_>>;
}

impl TcpAcceptor for tokio::net::TcpListener {
    fn accept(
        &self,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<AcceptedTcpStream>> + Send + '_>> {
        Box::pin(async move {
            let (stream, peer_addr) = Self::accept(self).await?;
            let nodelay_result = stream.set_nodelay(true);
            Ok(AcceptedTcpStream {
                stream,
                peer_addr,
                nodelay_result,
            })
        })
    }
}

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

/// Serves Axum through a configured, bounded HTTP/1.1 Hyper accept loop.
pub async fn serve_hardened_http(
    listener: tokio::net::TcpListener,
    router: axum::Router,
    graceful_rx: tokio::sync::oneshot::Receiver<()>,
    connection_permits: Arc<Semaphore>,
    header_read_timeout: Duration,
) -> std::io::Result<()> {
    serve_hardened_http_with_acceptor(
        Box::new(listener),
        router,
        graceful_rx,
        connection_permits,
        header_read_timeout,
    )
    .await
}

async fn serve_hardened_http_with_acceptor(
    acceptor: Box<dyn TcpAcceptor>,
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
        let permit = loop {
            tokio::select! {
                biased;
                _ = &mut graceful_rx => break 'accept,
                completed = connections.join_next(), if !connections.is_empty() => {
                    let completed = completed
                        .expect("a non-empty private connection set always yields a task");
                    log_connection_completion(completed);
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
            accepted = acceptor.accept() => accepted,
        };
        let AcceptedTcpStream {
            stream,
            peer_addr,
            nodelay_result,
        } = match accepted {
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
        if let Err(error) = nodelay_result {
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
                    () = first_request.notified(), if !first_request_seen => {
                        first_request_seen = true;
                    }
                    () = &mut first_request_deadline, if !first_request_seen => {
                        tracing::debug!(%peer_addr, "closing connection that did not produce headers before the deadline");
                        break Ok(());
                    }
                    _ = shutdown_rx.changed(), if !graceful_started => {
                        if !first_request_seen {
                            break Ok(());
                        }
                        graceful_started = true;
                        connection.as_mut().graceful_shutdown();
                    }
                    result = connection.as_mut() => break result,
                }
            };
            drop(permit);
            result.map_err(|error| error.to_string())
        });
    }

    drop(acceptor);
    let _ = connection_shutdown_tx.send(());
    while let Some(completed) = connections.join_next().await {
        log_connection_completion(completed);
    }
    Ok(())
}

fn log_connection_completion(completed: Result<Result<(), String>, tokio::task::JoinError>) {
    match completed {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, "HTTP connection closed with a protocol error"),
        Err(error) if error.is_cancelled() => {}
        Err(error) => tracing::warn!(%error, "HTTP connection task failed"),
    }
}

pub type HttpServerFuture = Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + 'static>>;
pub type ShutdownFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Stops accepting immediately and bounds both natural and cancelled drains.
pub async fn run_http_until_bounded_shutdown(
    mut server: HttpServerFuture,
    mut shutdown: ShutdownFuture,
    graceful_tx: tokio::sync::oneshot::Sender<()>,
    cancellation_token: CancellationToken,
    natural_drain_timeout: Duration,
    cancelled_drain_timeout: Duration,
) -> Option<std::io::Result<()>> {
    tokio::select! {
        output = &mut server => return Some(output),
        () = &mut shutdown => {}
    }

    let _ = graceful_tx.send(());
    let natural_deadline = tokio::time::Instant::now() + natural_drain_timeout;
    if let Ok(output) = tokio::time::timeout_at(natural_deadline, &mut server).await {
        return Some(output);
    }

    let drain_seconds = natural_drain_timeout.as_secs();
    tracing::warn!(
        drain_seconds,
        "HTTP requests did not drain naturally; cancelling MCP sessions and calls"
    );
    cancellation_token.cancel();

    let hard_deadline = natural_deadline + cancelled_drain_timeout;
    tokio::time::timeout_at(hard_deadline, &mut server)
        .await
        .ok()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        net::SocketAddr,
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll},
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    use super::*;

    #[test]
    fn optimized_runtime_probe_is_exact_and_side_effect_free() {
        assert!(
            print_runtime_version_if_requested("test-runtime", &["--version".to_owned()])
                .expect("stdout accepts the runtime version")
        );
        assert!(
            !print_runtime_version_if_requested("test-runtime", &[])
                .expect("a non-probe does not touch stdout")
        );
        assert!(
            !print_runtime_version_if_requested(
                "test-runtime",
                &["--version".to_owned(), "unexpected".to_owned()],
            )
            .expect("the existing command parser owns non-exact arguments")
        );
    }

    struct PendingUntilDropped {
        dropped: Arc<AtomicBool>,
    }

    struct ScriptedAcceptor {
        outcomes: Mutex<VecDeque<std::io::Result<AcceptedTcpStream>>>,
        attempts: tokio::sync::mpsc::UnboundedSender<()>,
    }

    impl TcpAcceptor for ScriptedAcceptor {
        fn accept(
            &self,
        ) -> Pin<Box<dyn Future<Output = std::io::Result<AcceptedTcpStream>> + Send + '_>> {
            Box::pin(async move {
                self.attempts
                    .send(())
                    .expect("scripted accept observer remains alive");
                let outcome = self
                    .outcomes
                    .lock()
                    .expect("scripted acceptor mutex is not poisoned")
                    .pop_front();
                match outcome {
                    Some(outcome) => outcome,
                    None => std::future::pending().await,
                }
            })
        }
    }

    impl Future for PendingUntilDropped {
        type Output = std::io::Result<()>;

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
                let read = stream.read(&mut chunk).await.expect("response is readable");
                assert_ne!(read, 0, "connection closed before complete response");
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
        .expect("response arrives before deadline")
    }

    #[tokio::test]
    async fn accepted_connection_count_is_hard_capped() {
        let (address, permits, graceful_tx, server_task) =
            hardened_test_server(2, Duration::from_secs(5)).await;
        let first = TcpStream::connect(address).await.unwrap();
        let second = TcpStream::connect(address).await.unwrap();
        wait_for_available_permits(&permits, 0).await;
        let mut queued = TcpStream::connect(address).await.unwrap();
        queued
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut probe = [0_u8; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(100), queued.read(&mut probe))
                .await
                .is_err()
        );
        drop(first);
        assert!(
            read_until_closed(&mut queued)
                .await
                .windows(6)
                .any(|part| part == b"200 OK")
        );
        drop(second);
        let _ = graceful_tx.send(());
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn header_deadline_and_keep_alive_release_capacity() {
        let (address, _permits, graceful_tx, server_task) =
            hardened_test_server(1, Duration::from_millis(150)).await;
        let mut idle = TcpStream::connect(address).await.unwrap();
        assert!(read_until_closed(&mut idle).await.is_empty());
        let mut keep_alive = TcpStream::connect(address).await.unwrap();
        keep_alive
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        assert!(
            read_until_contains(&mut keep_alive, b"\r\n\r\nok")
                .await
                .windows(6)
                .any(|part| part == b"200 OK")
        );
        keep_alive
            .write_all(b"GET /health HTTP/1.1\r\nHost: local")
            .await
            .unwrap();
        let _ = read_until_closed(&mut keep_alive).await;
        let _ = graceful_tx.send(());
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_a_connection_after_its_first_request() {
        let (address, _permits, graceful_tx, server_task) =
            hardened_test_server(1, Duration::from_secs(5)).await;
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        assert!(
            read_until_contains(&mut stream, b"\r\n\r\nok")
                .await
                .windows(6)
                .any(|part| part == b"200 OK")
        );
        graceful_tx.send(()).unwrap();
        assert!(read_until_closed(&mut stream).await.is_empty());
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn accept_and_nodelay_failures_release_capacity_and_honor_shutdown() {
        let socket_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket_listener.local_addr().unwrap();
        let client = tokio::spawn(TcpStream::connect(address));
        let (stream, peer_addr) = socket_listener.accept().await.unwrap();
        let client = client.await.unwrap().unwrap();
        drop(socket_listener);

        let (attempts_tx, mut attempts_rx) = tokio::sync::mpsc::unbounded_channel();
        let acceptor = ScriptedAcceptor {
            attempts: attempts_tx,
            outcomes: Mutex::new(VecDeque::from([
                Err(std::io::Error::other("synthetic accept failure")),
                Ok(AcceptedTcpStream {
                    stream,
                    peer_addr,
                    nodelay_result: Err(std::io::Error::other("synthetic nodelay failure")),
                }),
            ])),
        };
        let router = axum::Router::new();
        let permits = Arc::new(Semaphore::new(1));
        let (graceful_tx, graceful_rx) = tokio::sync::oneshot::channel();
        let server_permits = Arc::clone(&permits);
        let task = tokio::spawn(serve_hardened_http_with_acceptor(
            Box::new(acceptor),
            router,
            graceful_rx,
            server_permits,
            Duration::from_secs(5),
        ));

        attempts_rx.recv().await.unwrap();
        tokio::time::advance(HTTP_ACCEPT_ERROR_BACKOFF).await;
        attempts_rx.recv().await.unwrap();
        attempts_rx.recv().await.unwrap();
        drop(client);
        graceful_tx.send(()).unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn connection_completion_logging_handles_protocol_cancel_and_panic() {
        let protocol_error =
            tokio::spawn(async { Err::<(), _>("protocol failure".to_owned()) }).await;
        log_connection_completion(protocol_error);

        let cancelled = tokio::spawn(std::future::pending::<Result<(), String>>());
        tokio::task::yield_now().await;
        cancelled.abort();
        log_connection_completion(cancelled.await);

        let panicked = tokio::spawn(async {
            panic!("synthetic connection task panic");
            #[allow(unreachable_code)]
            Ok::<(), String>(())
        })
        .await;
        log_connection_completion(panicked);
    }

    #[tokio::test]
    async fn test_helpers_observe_delayed_permits_and_fragmented_responses() {
        let permits = Arc::new(Semaphore::new(0));
        let released = Arc::clone(&permits);
        let release = tokio::spawn(async move {
            tokio::task::yield_now().await;
            released.add_permits(1);
        });
        wait_for_available_permits(&permits, 1).await;
        release.await.unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let writer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(&vec![b'a'; 700]).await.unwrap();
            tokio::task::yield_now().await;
            stream.write_all(b"needle").await.unwrap();
        });
        let mut reader = TcpStream::connect(address).await.unwrap();
        assert!(
            read_until_contains(&mut reader, b"needle")
                .await
                .ends_with(b"needle")
        );
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn prior_knowledge_http2_is_rejected_and_shutdown_closes_idle_socket() {
        let (address, permits, graceful_tx, server_task) =
            hardened_test_server(1, Duration::from_secs(30)).await;
        let mut h2 = TcpStream::connect(address).await.unwrap();
        h2.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        let response = read_until_closed(&mut h2).await;
        assert!(!String::from_utf8_lossy(&response).contains("200 OK"));
        let mut idle = TcpStream::connect(address).await.unwrap();
        wait_for_available_permits(&permits, 0).await;
        graceful_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(read_until_closed(&mut idle).await.is_empty());
    }

    #[tokio::test]
    async fn bounded_shutdown_covers_completion_natural_cancelled_and_hard_deadline() {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let token = CancellationToken::new();
        assert!(matches!(
            run_http_until_bounded_shutdown(
                Box::pin(std::future::ready(Ok(()))),
                Box::pin(std::future::pending()),
                tx,
                token.clone(),
                Duration::from_millis(10),
                Duration::from_millis(10),
            )
            .await,
            Some(Ok(()))
        ));
        assert!(!token.is_cancelled());

        let (tx, rx) = tokio::sync::oneshot::channel();
        let token = CancellationToken::new();
        let natural = async move {
            rx.await.unwrap();
            Ok(())
        };
        assert!(matches!(
            run_http_until_bounded_shutdown(
                Box::pin(natural),
                Box::pin(std::future::ready(())),
                tx,
                token.clone(),
                Duration::from_millis(20),
                Duration::from_millis(20),
            )
            .await,
            Some(Ok(()))
        ));
        assert!(!token.is_cancelled());

        let (tx, rx) = tokio::sync::oneshot::channel();
        let token = CancellationToken::new();
        let observed = token.clone();
        let cancelled = async move {
            rx.await.unwrap();
            observed.cancelled().await;
            Ok(())
        };
        assert!(matches!(
            run_http_until_bounded_shutdown(
                Box::pin(cancelled),
                Box::pin(std::future::ready(())),
                tx,
                token.clone(),
                Duration::from_millis(1),
                Duration::from_millis(20),
            )
            .await,
            Some(Ok(()))
        ));
        assert!(token.is_cancelled());

        let (tx, _rx) = tokio::sync::oneshot::channel();
        let token = CancellationToken::new();
        let dropped = Arc::new(AtomicBool::new(false));
        assert!(
            run_http_until_bounded_shutdown(
                Box::pin(PendingUntilDropped {
                    dropped: Arc::clone(&dropped),
                }),
                Box::pin(std::future::ready(())),
                tx,
                token,
                Duration::from_millis(1),
                Duration::from_millis(1),
            )
            .await
            .is_none()
        );
        assert!(dropped.load(Ordering::SeqCst));
    }
}
