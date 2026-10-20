use super::*;
use axum::{Router, http::Uri, response::IntoResponse as _};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Notify, mpsc as async_mpsc};

async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (url, server)
}

fn assert_local_cooldown(error: &PerformanceError, minimum: Duration) {
    assert!(matches!(
        error,
        PerformanceError::RateLimited {
            request_id: None,
            retry_after: Some(delay),
        } if *delay > minimum
    ));
}

#[tokio::test]
async fn vendor_cooldown_blocks_clones_but_not_other_accounts_without_shared_quota() {
    for (status, retry_after, minimum_seconds) in [
        (StatusCode::TOO_MANY_REQUESTS, Some("81"), 75),
        (StatusCode::SERVICE_UNAVAILABLE, Some("81"), 75),
        (StatusCode::TOO_MANY_REQUESTS, None, 25),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let router = Router::new().fallback(move || {
            let observed = Arc::clone(&observed);
            async move {
                observed.fetch_add(1, Ordering::SeqCst);
                let mut response = (status, "{}").into_response();
                if let Some(delay) = retry_after {
                    response
                        .headers_mut()
                        .insert("retry-after", delay.parse().unwrap());
                }
                response
            }
        });
        let (url, server) = serve(router).await;
        let client = PerformanceClient::new_for_test(
            url,
            Duration::from_secs(3),
            credentials_for(&["limited", "quiet"]),
        );
        assert!(!client.shared_quota.is_enabled());
        let limited = StoreId::from("limited");
        let quiet = StoreId::from("quiet");
        cache_access_token(&client, &limited, "limited-token").await;
        cache_access_token(&client, &quiet, "quiet-token").await;

        let first = client
            .get(&limited, CAMPAIGNS_PATH, Vec::new())
            .await
            .unwrap_err();
        assert_eq!(
            first.kind(),
            if status == StatusCode::TOO_MANY_REQUESTS {
                PerformanceErrorKind::RateLimited
            } else {
                PerformanceErrorKind::Http
            }
        );
        let clone = client.clone();
        let blocked = tokio::time::timeout(
            Duration::from_millis(100),
            clone.get(&limited, LIMITS_PATH, Vec::new()),
        )
        .await
        .expect("a vendor cooldown must fail fast instead of consuming the logical timeout")
        .unwrap_err();
        assert_local_cooldown(&blocked, Duration::from_secs(minimum_seconds));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            client.global_in_flight.available_permits(),
            MAX_GLOBAL_IN_FLIGHT
        );

        client
            .get(&quiet, CAMPAIGNS_PATH, Vec::new())
            .await
            .unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }
}

#[tokio::test]
async fn oauth_vendor_cooldown_outlives_the_fallback_without_shared_quota() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let (url, server) = serve(Router::new().fallback(move |uri: Uri| {
        let observed = Arc::clone(&observed);
        async move {
            assert_eq!(uri.path(), TOKEN_PATH);
            observed.fetch_add(1, Ordering::SeqCst);
            (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "81")], "{}")
        }
    }))
    .await;
    let client = PerformanceClient::new_for_test(url, Duration::from_secs(3), credentials());
    let store = StoreId::from("shop");
    let first = client
        .get(&store, CAMPAIGNS_PATH, Vec::new())
        .await
        .unwrap_err();
    assert_eq!(first.kind(), PerformanceErrorKind::RateLimited);
    client.accounts[&store]
        .token
        .lock()
        .await
        .cooldown
        .as_mut()
        .unwrap()
        .until = Instant::now();
    let blocked = client
        .clone()
        .get(&store, CAMPAIGNS_PATH, Vec::new())
        .await
        .unwrap_err();
    assert_local_cooldown(&blocked, Duration::from_secs(75));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn oauth_concurrency_is_bounded_without_consuming_business_capacity() {
    let release = Arc::new(Notify::new());
    let handler_release = Arc::clone(&release);
    let (started, mut starts) = async_mpsc::unbounded_channel();
    let (url, server) = serve(Router::new().fallback(move |uri: Uri| {
        let release = Arc::clone(&handler_release);
        let started = started.clone();
        async move {
            if uri.path() == TOKEN_PATH {
                let ready = release.notified();
                tokio::pin!(ready);
                ready.as_mut().enable();
                started.send(()).unwrap();
                ready.await;
                (StatusCode::OK, token(1_800)).into_response()
            } else {
                (StatusCode::OK, "{}").into_response()
            }
        }
    }))
    .await;
    let client = PerformanceClient::new_for_test(
        url,
        Duration::from_secs(3),
        credentials_for(&["first", "second", "third", "cached"]),
    );
    let mut requests = Vec::new();
    for name in ["first", "second"] {
        let clone = client.clone();
        requests.push(tokio::spawn(async move {
            clone
                .get(&StoreId::from(name), CAMPAIGNS_PATH, Vec::new())
                .await
        }));
        tokio::time::timeout(Duration::from_secs(1), starts.recv())
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(client.oauth_in_flight.available_permits(), 0);
    assert_eq!(
        client.global_in_flight.available_permits(),
        MAX_GLOBAL_IN_FLIGHT
    );
    let third = StoreId::from("third");
    let overloaded = client
        .clone()
        .get(&third, CAMPAIGNS_PATH, Vec::new())
        .await
        .unwrap_err();
    assert_eq!(overloaded.kind(), PerformanceErrorKind::Overloaded);
    assert!(
        client.accounts[&third]
            .token
            .lock()
            .await
            .cooldown
            .is_none()
    );
    assert!(starts.try_recv().is_err());

    let cached = StoreId::from("cached");
    cache_access_token(&client, &cached, "cached-token").await;
    assert!(
        client
            .get(&cached, CAMPAIGNS_PATH, Vec::new())
            .await
            .is_ok()
    );
    release.notify_waiters();
    for request in requests {
        request.await.unwrap().unwrap();
    }
    assert_eq!(
        client.oauth_in_flight.available_permits(),
        MAX_OAUTH_IN_FLIGHT
    );
    server.abort();
}

#[tokio::test]
async fn cancelled_or_timed_out_oauth_refresh_releases_capacity_and_suppresses_repeat() {
    for cancel in [false, true] {
        let (started, mut starts) = async_mpsc::unbounded_channel();
        let (url, server) = serve(Router::new().fallback(move || {
            let started = started.clone();
            async move {
                started.send(()).unwrap();
                std::future::pending::<axum::response::Response>().await
            }
        }))
        .await;
        let client =
            PerformanceClient::new_for_test(url, Duration::from_millis(200), credentials());
        let clone = client.clone();
        let request = tokio::spawn(async move {
            clone
                .get(&StoreId::from("shop"), CAMPAIGNS_PATH, Vec::new())
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), starts.recv())
            .await
            .unwrap()
            .unwrap();
        if cancel {
            request.abort();
            assert!(request.await.unwrap_err().is_cancelled());
        } else {
            assert_eq!(
                request.await.unwrap().unwrap_err().kind(),
                PerformanceErrorKind::Timeout
            );
        }
        assert_eq!(
            client.oauth_in_flight.available_permits(),
            MAX_OAUTH_IN_FLIGHT
        );
        let blocked = client
            .get(&StoreId::from("shop"), CAMPAIGNS_PATH, Vec::new())
            .await
            .unwrap_err();
        assert!(matches!(
            blocked,
            PerformanceError::TokenUnavailable {
                previous: "timeout"
            }
        ));
        assert!(starts.try_recv().is_err());
        server.abort();
    }
}
