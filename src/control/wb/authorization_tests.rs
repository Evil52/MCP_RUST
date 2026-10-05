use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use tokio::{net::TcpListener, time::timeout};

use super::{
    WbBidPlacement, WbBidWriteClient, WbCampaignBidType, WbCampaignPaymentType,
    WbCreateCampaignRequest, WbGuardedWriteError, WbPreparedBidChange, WbWriteError,
    WbWriteOutcomeKind,
};

fn at(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).unwrap()
}

#[tokio::test]
async fn expired_authorization_never_runs_durable_permit_or_sends() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = WbBidWriteClient::new_for_test(
        &format!("http://{}", listener.local_addr().unwrap()),
        "test-token",
        Duration::from_secs(1),
    )
    .with_authorization_window(at(100), at(200))
    .with_authorization_clock(Arc::new(|| at(200)));
    let permits = AtomicUsize::new(0);
    for start in [false, true] {
        let permit = || async {
            permits.fetch_add(1, Ordering::SeqCst);
            Ok::<(), ()>(())
        };
        let result = if start {
            client.start_campaign_with_permit(42, permit).await
        } else {
            client.pause_campaign_with_permit(42, permit).await
        };
        assert_expired(result.unwrap_err());
    }
    assert_eq!(permits.load(Ordering::SeqCst), 0);
    assert!(
        timeout(Duration::from_millis(25), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn authorization_expiring_during_permit_cannot_dispatch_any_write() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let clock = Arc::new(AtomicI64::new(150));
    let read_clock = Arc::clone(&clock);
    let client = WbBidWriteClient::new_for_test(
        &format!("http://{}", listener.local_addr().unwrap()),
        "test-token",
        Duration::from_secs(1),
    )
    .with_authorization_window(at(100), at(200))
    .with_authorization_clock(Arc::new(move || at(read_clock.load(Ordering::SeqCst))));
    let permits = AtomicUsize::new(0);
    let change = WbPreparedBidChange {
        nm_id: 1,
        placement: WbBidPlacement::Search,
        before_bid_kopecks: 100,
        bid_kopecks: 110,
    };
    let request = WbCreateCampaignRequest {
        name: "authorization test".to_owned(),
        nm_ids: vec![1],
        bid_type: WbCampaignBidType::Manual,
        payment_type: WbCampaignPaymentType::Cpc,
        placement_types: vec![WbBidPlacement::Search],
    };
    for kind in 0..5 {
        clock.store(150, Ordering::SeqCst);
        let permit = || async {
            // Model a DB permit that completes precisely at authorization expiry.
            permits.fetch_add(1, Ordering::SeqCst);
            clock.store(200, Ordering::SeqCst);
            Ok::<(), ()>(())
        };
        let result = match kind {
            0 => {
                client
                    .change_bids_with_permit(42, std::slice::from_ref(&change), permit)
                    .await
            }
            1 => client.pause_campaign_with_permit(42, permit).await,
            2 => client.start_campaign_with_permit(42, permit).await,
            3 => client
                .create_campaign_with_permit(&request, permit)
                .await
                .map(|_| serde_json::Value::Null),
            _ => client
                .deposit_once_with_permit(42, permit)
                .await
                .map(|_| serde_json::Value::Null),
        };
        assert_expired(result.unwrap_err());
    }
    assert_eq!(permits.load(Ordering::SeqCst), 5);
    assert!(
        timeout(Duration::from_millis(25), listener.accept())
            .await
            .is_err()
    );
}

fn assert_expired(error: WbGuardedWriteError<()>) {
    let WbGuardedWriteError::Write(error) = error else {
        panic!("unexpected permit failure")
    };
    assert!(matches!(error, WbWriteError::AuthorizationUnavailable));
    assert_eq!(error.outcome_kind(), WbWriteOutcomeKind::DefiniteFailure);
}
