//! A protocol NACK says the peer did not process the request. That makes it
//! safe for reqwest to retry by default, but still violates our stricter
//! contract that each fresh write permit authorizes one network attempt.

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use super::{WbBidWriteClient, WbGuardedWriteError, WbWriteError, client::write_http_builder};
use crate::test_support::http2::{NackPeer, ProtocolNack};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

async fn assert_attempts(nack: ProtocolNack, use_write_policy: bool, expected_attempts: usize) {
    let peer = NackPeer::start(nack).await;
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
    let client = WbBidWriteClient::from_parts(
        http,
        &peer.base_url,
        "test-token",
        TEST_TIMEOUT,
        Duration::ZERO,
    )
    .expect("write client");
    let permits = AtomicUsize::new(0);
    let result = client
        .deposit_once_with_permit(42, || async {
            permits.fetch_add(1, Ordering::SeqCst);
            Ok::<_, ()>(())
        })
        .await;
    assert!(matches!(
        result,
        Err(WbGuardedWriteError::Write(WbWriteError::Ambiguous {
            reason: "network_error",
            ..
        }))
    ));
    assert_eq!(permits.load(Ordering::SeqCst), 1);
    assert_eq!(peer.finish().await, expected_attempts);
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
