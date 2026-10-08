//! Shutdown and OAuth-timing boundaries of the delivery pass.

use std::sync::{Arc, atomic::AtomicUsize, atomic::Ordering};

use tokio_util::sync::CancellationToken;

use super::{
    AuthorizationFuture, ClaimedDelivery, DeliveryFuture, DeliveryPassOutcome, GmailAccessToken,
    GmailOutboxError, MailDelivery, Recorded, ReportKind, StoredReportBundle, at, claim,
    fake_delivery, queued_worker, receipt,
};

/// An OAuth exchange that never completes, as on a hung token endpoint.
struct PendingAuthorization;

impl MailDelivery for PendingAuthorization {
    fn authorize(&self) -> AuthorizationFuture<'_> {
        Box::pin(std::future::pending())
    }

    fn deliver<'a>(
        &'a self,
        _claim: &'a ClaimedDelivery,
        _bundle: StoredReportBundle,
        _token: &'a GmailAccessToken,
    ) -> DeliveryFuture<'a> {
        unreachable!("a pending authorization never reaches the send")
    }
}

/// A send during which the process is asked to shut down.
struct ShutdownDuringSend {
    stop: CancellationToken,
    calls: AtomicUsize,
}

impl MailDelivery for ShutdownDuringSend {
    fn authorize(&self) -> AuthorizationFuture<'_> {
        Box::pin(std::future::ready(Ok(GmailAccessToken::for_test(
            "access-token",
        ))))
    }

    fn deliver<'a>(
        &'a self,
        _claim: &'a ClaimedDelivery,
        _bundle: StoredReportBundle,
        _token: &'a GmailAccessToken,
    ) -> DeliveryFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.stop.cancel();
            receipt()
        })
    }
}

#[tokio::test(start_paused = true)]
async fn a_hung_oauth_exchange_times_out_before_any_row_is_claimed() {
    let (worker, outbox, artifacts) = queued_worker(
        vec![Ok(Some(claim(1, ReportKind::Evening)))],
        vec![at(12, 0)],
        Arc::new(PendingAuthorization),
    );
    assert_eq!(
        worker.deliver_ready(&CancellationToken::new()).await,
        Err(GmailOutboxError::AttemptTimedOut)
    );
    assert_eq!(
        outbox.claims.lock().unwrap().len(),
        1,
        "nothing was claimed"
    );
    assert_eq!(artifacts.calls.load(Ordering::Relaxed), 0);
    assert!(outbox.recorded.lock().unwrap().is_empty());
}

#[tokio::test]
async fn shutdown_finishes_the_inflight_attempt_and_claims_nothing_new() {
    let stop = CancellationToken::new();
    let delivery = Arc::new(ShutdownDuringSend {
        stop: stop.clone(),
        calls: AtomicUsize::new(0),
    });
    let (worker, outbox, _) = queued_worker(
        vec![
            Ok(Some(claim(1, ReportKind::Evening))),
            Ok(Some(claim(1, ReportKind::Evening))),
        ],
        (0..4).map(|minute| at(12, minute)).collect(),
        delivery.clone(),
    );
    assert_eq!(
        worker.deliver_ready(&stop).await.unwrap(),
        DeliveryPassOutcome {
            attempts: 1,
            queue_drained: false,
        }
    );
    assert_eq!(delivery.calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        outbox.recorded.lock().unwrap().as_slice(),
        &[Recorded::Sent],
        "the attempt in flight reaches its recorded outcome"
    );
    assert_eq!(outbox.claims.lock().unwrap().len(), 1);

    let (stopped, outbox, artifacts) = queued_worker(
        vec![Ok(Some(claim(1, ReportKind::Evening)))],
        Vec::new(),
        fake_delivery(receipt()),
    );
    assert_eq!(
        stopped.deliver_ready(&stop).await.unwrap(),
        DeliveryPassOutcome {
            attempts: 0,
            queue_drained: false,
        }
    );
    assert_eq!(outbox.claims.lock().unwrap().len(), 1);
    assert_eq!(artifacts.calls.load(Ordering::Relaxed), 0);
}
