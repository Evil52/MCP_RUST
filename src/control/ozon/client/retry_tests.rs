use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::test_support::http2::{NackPeer, ProtocolNack};

async fn assert_single_attempt(nack: ProtocolNack) {
    let peer = NackPeer::start(nack).await;
    let http = write_http_builder(Duration::from_secs(2))
        .http2_prior_knowledge()
        .build()
        .unwrap();
    let client = OzonAdsWriteClient::from_parts(
        http,
        &peer.base_url,
        PerformanceCredentials {
            client_id: "test-client".to_owned(),
            client_secret: "test-secret".to_owned(),
        },
        PerformanceRequestPacer::new(),
        Duration::ZERO,
    );
    // Prime a valid cached token so the counted frames are marketplace
    // mutations. OAuth itself precedes the durable permit.
    client.token.lock().await.cached = Some(CachedToken {
        value: "test-access-token".to_owned(),
        refresh_at: Instant::now() + Duration::from_secs(60),
    });
    let permits = AtomicUsize::new(0);
    let result = client
        .activate_campaign_with_permit(42, || async {
            permits.fetch_add(1, Ordering::SeqCst);
            Ok::<_, ()>(())
        })
        .await;
    assert!(matches!(
        result,
        Err(OzonGuardedWriteError::Write(
            OzonWriteError::AmbiguousTransport
        ))
    ));
    assert_eq!(permits.load(Ordering::SeqCst), 1);
    assert_eq!(peer.finish().await, 1);
}

#[tokio::test]
async fn refused_stream_keeps_ozon_mutation_to_one_attempt_per_permit() {
    assert_single_attempt(ProtocolNack::RefusedStream).await;
}

#[tokio::test]
async fn goaway_keeps_ozon_mutation_to_one_attempt_per_permit() {
    assert_single_attempt(ProtocolNack::GoAway).await;
}
