use std::sync::atomic::{AtomicUsize, Ordering};

use axum::body::Bytes;
use axum::{Router, body::Body, routing::post};
use http_body::{Body as HttpBody, Frame};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use super::*;

struct ResponseFrames(mpsc::Receiver<Result<Frame<Bytes>, std::io::Error>>);

impl HttpBody for ResponseFrames {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.get_mut().0.poll_recv(context)
    }
}

async fn exercise_body(body: Body, token_phase: bool) -> OzonWriteError {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(AtomicUsize::new(0));
    let response_body = Arc::new(Mutex::new(Some(body)));
    let request_count = Arc::clone(&requests);
    let path = if token_phase {
        TOKEN_PATH
    } else {
        "/api/client/campaign/42/activate"
    };
    let router = Router::new().route(
        path,
        post(move || {
            let response_body = Arc::clone(&response_body);
            let request_count = Arc::clone(&request_count);
            async move {
                request_count.fetch_add(1, Ordering::SeqCst);
                response_body.lock().await.take().expect("one response")
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = OzonAdsWriteClient::new_for_test(
        &base_url,
        PerformanceCredentials {
            client_id: "body-test-client".to_owned(),
            client_secret: "body-test-secret".to_owned(),
        },
        Duration::from_secs(2),
    );
    if !token_phase {
        client.token.lock().await.cached = Some(CachedToken {
            value: "test-access-token".to_owned(),
            refresh_at: Instant::now() + Duration::from_secs(60),
        });
    }
    let permits = AtomicUsize::new(0);
    let outcome = client
        .activate_campaign_with_permit(42, || async {
            permits.fetch_add(1, Ordering::SeqCst);
            Ok::<_, ()>(())
        })
        .await;
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert_eq!(permits.load(Ordering::SeqCst), usize::from(!token_phase));
    let OzonGuardedWriteError::Write(error) = outcome.unwrap_err() else {
        panic!("the local permit always accepts the mutation");
    };
    error
}

#[tokio::test]
async fn truncated_stream_is_definite_before_permit_and_ambiguous_after_it() {
    for token_phase in [true, false] {
        // Flush successful headers and a body prefix before the peer fails.
        // This checks the response-reading boundary, after send() succeeds.
        let (sender, receiver) = mpsc::channel(2);
        sender
            .send(Ok(Frame::data(Bytes::from_static(b"{"))))
            .await
            .unwrap();
        let failed_body = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            sender
                .send(Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)))
                .await
                .unwrap();
        });
        let error = exercise_body(Body::new(ResponseFrames(receiver)), token_phase).await;
        failed_body.await.unwrap();
        if token_phase {
            assert!(matches!(error, OzonWriteError::TokenTransport));
            assert_eq!(error.kind(), OzonWriteErrorKind::Definite);
        } else {
            assert!(matches!(error, OzonWriteError::AmbiguousTransport));
            assert_eq!(error.kind(), OzonWriteErrorKind::Ambiguous);
        }
    }
}

#[tokio::test]
async fn streamed_body_limits_hold_without_a_content_length_header() {
    for token_phase in [true, false] {
        let limit = if token_phase {
            MAX_TOKEN_BYTES
        } else {
            MAX_RESPONSE_BYTES
        };
        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(Ok(Frame::data(Bytes::from(vec![b'x'; limit + 1]))))
            .await
            .unwrap();
        drop(sender);
        let error = exercise_body(Body::new(ResponseFrames(receiver)), token_phase).await;
        if token_phase {
            assert!(matches!(error, OzonWriteError::TokenResponseTooLarge));
            assert_eq!(error.kind(), OzonWriteErrorKind::Definite);
        } else {
            assert!(matches!(error, OzonWriteError::ResponseTooLarge));
            assert_eq!(error.kind(), OzonWriteErrorKind::Ambiguous);
        }
    }
}
