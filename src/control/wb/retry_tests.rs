//! A protocol NACK says the peer did not process the request. That makes it
//! safe for reqwest to retry by default, but still violates our stricter
//! contract that each fresh write permit authorizes one network attempt.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinSet,
};

use super::{WbBidWriteClient, WbGuardedWriteError, WbWriteError, client::write_http_builder};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy)]
enum ProtocolNack {
    RefusedStream,
    GoAway,
}

async fn write_frame(socket: &mut TcpStream, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    let length = u32::try_from(payload.len()).expect("fixture frame length fits u32");
    assert!(length <= 16_384);
    let mut header = [0_u8; 9];
    header[..3].copy_from_slice(&length.to_be_bytes()[1..]);
    header[3] = kind;
    header[4] = flags;
    header[5..].copy_from_slice(&stream.to_be_bytes());
    socket.write_all(&header).await.expect("send frame header");
    socket.write_all(payload).await.expect("send frame payload");
}

async fn serve_nacks(mut socket: TcpStream, nack: ProtocolNack, attempts: Arc<AtomicUsize>) {
    let mut preface = [0_u8; 24];
    socket
        .read_exact(&mut preface)
        .await
        .expect("HTTP/2 preface");
    assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
    write_frame(&mut socket, 4, 0, 0, &[]).await; // SETTINGS
    loop {
        let mut header = [0_u8; 9];
        if socket.read_exact(&mut header).await.is_err() {
            return;
        }
        let length =
            usize::from(header[0]) * 65_536 + usize::from(header[1]) * 256 + usize::from(header[2]);
        assert!(
            length <= 16_384,
            "fixture only expects small write requests"
        );
        let stream = u32::from_be_bytes(header[5..].try_into().expect("four-byte stream id"));
        let mut payload = vec![0; length];
        socket
            .read_exact(&mut payload)
            .await
            .expect("frame payload");
        match header[3] {
            1 => {
                // Each new request is a HEADERS frame on a distinct stream.
                // This fixture refuses it before any application processing.
                assert_ne!(stream, 0);
                assert_eq!(header[4] & 4, 4, "request headers fit one frame");
                attempts.fetch_add(1, Ordering::SeqCst);
                match nack {
                    ProtocolNack::RefusedStream => {
                        // RST_STREAM with REFUSED_STREAM (0x7).
                        write_frame(&mut socket, 3, 0, stream, &7_u32.to_be_bytes()).await;
                    }
                    ProtocolNack::GoAway => {
                        // NO_ERROR with last processed stream 0 explicitly
                        // excludes this request, allowing a default retry.
                        write_frame(&mut socket, 7, 0, 0, &[0; 8]).await;
                        socket
                            .shutdown()
                            .await
                            .expect("flush GOAWAY before closing");
                        // Drain in-flight DATA so an unread request body
                        // cannot turn the close into a TCP reset that masks
                        // the GOAWAY error we intend to exercise.
                        let mut buffer = [0_u8; 1024];
                        while let Ok(read) = socket.read(&mut buffer).await {
                            if read == 0 {
                                break;
                            }
                        }
                        return;
                    }
                }
            }
            4 if header[4] & 1 == 0 => write_frame(&mut socket, 4, 1, 0, &[]).await,
            6 if header[4] & 1 == 0 => write_frame(&mut socket, 6, 1, 0, &payload).await,
            7 => return,
            _ => {}
        }
    }
}

async fn assert_attempts(nack: ProtocolNack, use_write_policy: bool, expected_attempts: usize) {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("loopback listener");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let attempts = Arc::new(AtomicUsize::new(0));
    let server_attempts = attempts.clone();
    let (shutdown, mut shutdown_received) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (socket, _) = accepted.expect("accept client connection");
                    connections.spawn(serve_nacks(socket, nack, server_attempts.clone()));
                }
                result = connections.join_next(), if !connections.is_empty() => {
                    result.expect("connection task exists").expect("protocol peer completes");
                }
                _ = &mut shutdown_received => break,
            }
        }
        connections.abort_all();
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                assert!(error.is_cancelled(), "protocol peer failed: {error}");
            }
        }
    });
    let builder = if use_write_policy {
        // The same builder configures the production HTTPS/proxy client.
        write_http_builder(TEST_TIMEOUT)
    } else {
        // Control: prove the fixture actually exercises reqwest's implicit
        // retry classifier, rather than only returning an arbitrary error.
        reqwest::Client::builder().no_proxy().timeout(TEST_TIMEOUT)
    };
    let http = builder
        .http2_prior_knowledge()
        .build()
        .expect("HTTP/2 client");
    let client =
        WbBidWriteClient::from_parts(http, &base_url, "test-token", TEST_TIMEOUT, Duration::ZERO)
            .expect("write client");
    let permits = AtomicUsize::new(0);
    let result = client
        .deposit_once_with_permit(42, || async {
            permits.fetch_add(1, Ordering::SeqCst);
            Ok::<_, ()>(())
        })
        .await;
    shutdown.send(()).expect("server is listening");
    server.await.expect("server completes");
    assert!(matches!(
        result,
        Err(WbGuardedWriteError::Write(WbWriteError::Ambiguous {
            reason: "network_error",
            ..
        }))
    ));
    assert_eq!(permits.load(Ordering::SeqCst), 1);
    assert_eq!(attempts.load(Ordering::SeqCst), expected_attempts);
}

#[tokio::test]
async fn refused_stream_does_not_reuse_a_write_permit_for_another_attempt() {
    assert_attempts(ProtocolNack::RefusedStream, true, 1).await;
}

#[tokio::test]
async fn goaway_does_not_reuse_a_write_permit_on_a_new_connection() {
    assert_attempts(ProtocolNack::GoAway, true, 1).await;
}

#[tokio::test]
async fn default_reqwest_control_retries_refused_stream_with_one_permit() {
    assert_attempts(ProtocolNack::RefusedStream, false, 3).await;
}

#[tokio::test]
async fn default_reqwest_control_retries_goaway_with_one_permit() {
    assert_attempts(ProtocolNack::GoAway, false, 3).await;
}
