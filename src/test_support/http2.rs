//! Minimal loopback HTTP/2 peer for guarded-write retry regression tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinSet,
};

#[derive(Clone, Copy)]
pub(crate) enum ProtocolNack {
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

pub(crate) struct NackPeer {
    pub(crate) base_url: String,
    attempts: Arc<AtomicUsize>,
    shutdown: oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
}

impl NackPeer {
    pub(crate) async fn start(nack: ProtocolNack) -> Self {
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
        Self {
            base_url,
            attempts,
            shutdown,
            server,
        }
    }

    pub(crate) async fn finish(self) -> usize {
        self.shutdown.send(()).expect("server is listening");
        self.server.await.expect("server completes");
        self.attempts.load(Ordering::SeqCst)
    }
}
