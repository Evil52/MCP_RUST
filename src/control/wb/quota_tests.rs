use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};

use super::*;
use crate::marketplace_quota::{QuotaError, SharedQuota};

#[tokio::test]
async fn unavailable_shared_quota_blocks_all_writes_before_dispatch_permit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let token = format!(
        "e30.{}.signature",
        URL_SAFE_NO_PAD.encode(br#"{"sid":"123e4567-e89b-42d3-a456-426614174000"}"#)
    );
    let client = WbBidWriteClient::new_for_test(&base, &token, Duration::from_secs(1))
        .with_shared_quota(SharedQuota::from_database_url(
            "invalid quota configuration",
        ));
    let permits = AtomicUsize::new(0);
    let permit = || async {
        permits.fetch_add(1, Ordering::SeqCst);
        Ok::<(), ()>(())
    };
    let change = WbPreparedBidChange {
        nm_id: 1,
        placement: WbBidPlacement::Search,
        before_bid_kopecks: 100,
        bid_kopecks: 110,
    };
    let request = WbCreateCampaignRequest {
        name: "quota test".to_owned(),
        nm_ids: vec![1],
        bid_type: WbCampaignBidType::Manual,
        payment_type: WbCampaignPaymentType::Cpc,
        placement_types: vec![WbBidPlacement::Search],
    };
    let errors = [
        client
            .change_bids_with_permit(1, &[change], permit)
            .await
            .unwrap_err(),
        client
            .pause_campaign_with_permit(1, permit)
            .await
            .unwrap_err(),
        client
            .start_campaign_with_permit(1, permit)
            .await
            .unwrap_err(),
    ];
    for error in errors {
        assert_not_dispatched(error);
    }
    assert_not_dispatched(
        client
            .create_campaign_with_permit(&request, permit)
            .await
            .unwrap_err(),
    );
    assert_not_dispatched(
        client
            .deposit_once_with_permit(1, permit)
            .await
            .unwrap_err(),
    );
    assert_eq!(permits.load(Ordering::SeqCst), 0);
    assert!(
        timeout(Duration::from_millis(25), listener.accept())
            .await
            .is_err()
    );
}

fn assert_not_dispatched(error: WbGuardedWriteError<()>) {
    let WbGuardedWriteError::Write(error) = error else {
        panic!("permit must not run")
    };
    assert!(matches!(
        error,
        WbWriteError::SharedQuota(QuotaError::Unavailable)
    ));
    assert_eq!(error.outcome_kind(), WbWriteOutcomeKind::DefiniteFailure);
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn competing_departure_during_permit_is_rechecked_before_write_bytes() {
    let url = std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL")
        .expect("requires isolated PostgreSQL collector URL");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let token = format!(
        "e30.{}.signature",
        URL_SAFE_NO_PAD.encode(br#"{"sid":"d2000000-0000-4000-8000-000000000001"}"#)
    );
    let client = WbBidWriteClient::new_for_test(&base, &token, Duration::from_secs(1))
        .with_shared_quota(SharedQuota::from_database_url(&url));
    let competitor = SharedQuota::from_database_url(&url);
    let key = crate::marketplace_quota::QuotaKey::wb(&token, "promotion_pause").unwrap();
    let permits = AtomicUsize::new(0);
    let error = client
        .pause_campaign_with_permit(1, || async {
            permits.fetch_add(1, Ordering::SeqCst);
            competitor
                .admit(&key, Duration::from_secs(30))
                .await
                .unwrap();
            Ok::<(), ()>(())
        })
        .await
        .unwrap_err();
    let WbGuardedWriteError::Write(error) = error else {
        panic!("permit succeeded")
    };
    assert!(matches!(
        error,
        WbWriteError::SharedQuota(QuotaError::Limited { .. })
    ));
    assert_eq!(error.outcome_kind(), WbWriteOutcomeKind::DefiniteFailure);
    assert_eq!(permits.load(Ordering::SeqCst), 1);
    assert!(
        timeout(Duration::from_millis(25), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn write_429_persists_cooldown_for_rotated_client_without_repeating_write() {
    let url = std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL")
        .expect("requires isolated PostgreSQL collector URL");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let token = format!(
        "e30.{}.signature",
        URL_SAFE_NO_PAD.encode(br#"{"sid":"d2000000-0000-4000-8000-000000000002"}"#)
    );
    let first = WbBidWriteClient::new_for_test(&base, &token, Duration::from_secs(1))
        .with_shared_quota(SharedQuota::from_database_url(&url));
    let second = WbBidWriteClient::new_for_test(
        &base,
        &token.replace("signature", "rotated"),
        Duration::from_secs(1),
    )
    .with_shared_quota(SharedQuota::from_database_url(&url));
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let mut buffer = [0; 1024];
            let read = socket.read(&mut buffer).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&buffer[..read]);
        }
        socket.write_all(b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 2\r\nConnection: close\r\nX-Ratelimit-Retry: 120\r\n\r\n{}").await.unwrap();
        listener
    });
    let permit = || async { Ok::<(), ()>(()) };
    assert!(matches!(
        first.pause_campaign_with_permit(1, permit).await,
        Err(WbGuardedWriteError::Write(WbWriteError::HttpStatus {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            ..
        }))
    ));
    assert!(matches!(second.pause_campaign_with_permit(1, permit).await,
        Err(WbGuardedWriteError::Write(WbWriteError::SharedQuota(QuotaError::Limited { retry_after })))
            if retry_after > Duration::from_secs(100)));
    let listener = server.await.unwrap();
    assert!(
        timeout(Duration::from_millis(25), listener.accept())
            .await
            .is_err()
    );
}
