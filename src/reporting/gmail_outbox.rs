//! Exactly-once-oriented bridge between the report outbox and Gmail.
//!
//! This module does not own a background loop. One call probes for ready work,
//! refreshes OAuth, and only then claims at most one ready row, verifies its
//! immutable artifact, performs one provider attempt, and records only an
//! outcome whose safety is known. Between the claim and the Gmail request only
//! local work remains, so a slow token exchange, an OAuth outage or a shutdown
//! during refresh can no longer strand a row in `sending`. A bounded pass may
//! invoke that primitive repeatedly and stops claiming once asked to shut
//! down. An ambiguous provider outcome or a post-claim persistence failure
//! still leaves the row `sending`, so another worker cannot resend it.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use tokio_util::sync::CancellationToken;

use super::{
    artifact_store::{ArtifactStoreError, LocalArtifactStore, StoredReportBundle},
    gmail_delivery::{GmailDeliveryError, GmailDeliveryService},
    gmail_oauth::{GmailAccessToken, GmailOAuthCredentials},
    mail_routing::MailRouting,
    outbox::{ArtifactIdentity, DeliveryErrorClass},
    postgres_outbox::{ClaimedDelivery, PostgresOutboxError, PostgresOutboxRepository},
};

const RETRY_BASE_SECONDS: i64 = 60;
const RETRY_MAX_SECONDS: i64 = 15 * 60;
const DELIVERY_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
const MAX_DELIVERIES_PER_PASS: u8 = 16;

type ProbeFuture<'a> = Pin<Box<dyn Future<Output = Result<bool, PostgresOutboxError>> + Send + 'a>>;
type ClaimFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ClaimedDelivery>, PostgresOutboxError>> + Send + 'a>>;
type CompletionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), PostgresOutboxError>> + Send + 'a>>;
type ArtifactFuture<'a> =
    Pin<Box<dyn Future<Output = Result<StoredReportBundle, ArtifactStoreError>> + Send + 'a>>;
type AuthorizationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GmailAccessToken, GmailDeliveryError>> + Send + 'a>>;
type DeliveryFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<super::gmail::GmailSendReceipt, GmailDeliveryError>> + Send + 'a,
    >,
>;

trait DeliveryOutbox: Send + Sync {
    fn has_ready(&self, now: DateTime<Utc>) -> ProbeFuture<'_>;

    fn claim(&self, now: DateTime<Utc>) -> ClaimFuture<'_>;

    fn sent<'a>(
        &'a self,
        claim: &'a ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        provider_message_id: &'a str,
    ) -> CompletionFuture<'a>;

    fn transient<'a>(
        &'a self,
        claim: &'a ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        class: DeliveryErrorClass,
        retry_at: DateTime<Utc>,
    ) -> CompletionFuture<'a>;

    fn exhausted<'a>(
        &'a self,
        claim: &'a ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        class: DeliveryErrorClass,
    ) -> CompletionFuture<'a>;

    fn permanent<'a>(
        &'a self,
        claim: &'a ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        class: DeliveryErrorClass,
    ) -> CompletionFuture<'a>;
}

impl DeliveryOutbox for PostgresOutboxRepository {
    fn has_ready(&self, now: DateTime<Utc>) -> ProbeFuture<'_> {
        Box::pin(async move { self.has_ready_delivery(now).await })
    }

    fn claim(&self, now: DateTime<Utc>) -> ClaimFuture<'_> {
        Box::pin(async move { self.claim_ready(now).await })
    }

    fn sent<'a>(
        &'a self,
        claim: &'a ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        provider_message_id: &'a str,
    ) -> CompletionFuture<'a> {
        Box::pin(async move {
            self.record_sent(claim, started_at, finished_at, provider_message_id)
                .await
        })
    }

    fn transient<'a>(
        &'a self,
        claim: &'a ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        class: DeliveryErrorClass,
        retry_at: DateTime<Utc>,
    ) -> CompletionFuture<'a> {
        Box::pin(async move {
            self.record_transient_failure(claim, started_at, finished_at, class, retry_at)
                .await
        })
    }

    fn exhausted<'a>(
        &'a self,
        claim: &'a ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        class: DeliveryErrorClass,
    ) -> CompletionFuture<'a> {
        Box::pin(async move {
            self.record_exhausted_failure(claim, started_at, finished_at, class)
                .await
        })
    }

    fn permanent<'a>(
        &'a self,
        claim: &'a ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        class: DeliveryErrorClass,
    ) -> CompletionFuture<'a> {
        Box::pin(async move {
            self.record_permanent_failure(claim, started_at, finished_at, class)
                .await
        })
    }
}

trait ArtifactLoader: Send + Sync {
    fn load(&self, artifact: ArtifactIdentity) -> ArtifactFuture<'_>;
}

impl ArtifactLoader for LocalArtifactStore {
    fn load(&self, artifact: ArtifactIdentity) -> ArtifactFuture<'_> {
        let store = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || Self::load(&store, &artifact))
                .await
                .map_err(|_| ArtifactStoreError::Unavailable)?
        })
    }
}

trait MailDelivery: Send + Sync {
    fn authorize(&self) -> AuthorizationFuture<'_>;

    fn deliver<'a>(
        &'a self,
        claim: &'a ClaimedDelivery,
        bundle: StoredReportBundle,
        token: &'a GmailAccessToken,
    ) -> DeliveryFuture<'a>;
}

#[derive(Clone)]
pub struct GmailProvider {
    service: GmailDeliveryService,
    routing: MailRouting,
    credentials: GmailOAuthCredentials,
}

impl fmt::Debug for GmailProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GmailProvider")
            .field("transport", &"fixed-mail-egress")
            .field("routing", &"<redacted>")
            .field("credentials", &"<redacted>")
            .finish()
    }
}

impl GmailProvider {
    #[must_use]
    pub const fn new(
        service: GmailDeliveryService,
        routing: MailRouting,
        credentials: GmailOAuthCredentials,
    ) -> Self {
        Self {
            service,
            routing,
            credentials,
        }
    }
}

impl MailDelivery for GmailProvider {
    fn authorize(&self) -> AuthorizationFuture<'_> {
        Box::pin(async move { self.service.authorize(&self.credentials).await })
    }

    fn deliver<'a>(
        &'a self,
        claim: &'a ClaimedDelivery,
        bundle: StoredReportBundle,
        token: &'a GmailAccessToken,
    ) -> DeliveryFuture<'a> {
        Box::pin(async move { self.service.send(&self.routing, claim, bundle, token).await })
    }
}

trait DeliveryClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

struct SystemClock;

impl DeliveryClock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryTickOutcome {
    Idle,
    Sent { batch_id: i64, attempt_no: u8 },
    RetryScheduled { batch_id: i64, attempt_no: u8 },
    RetryExhausted { batch_id: i64, attempt_no: u8 },
    PermanentFailure { batch_id: i64, attempt_no: u8 },
    Ambiguous { batch_id: i64, attempt_no: u8 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryPassOutcome {
    pub attempts: u8,
    pub queue_drained: bool,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum GmailOutboxError {
    #[error("daily report outbox is unavailable before a delivery claim")]
    ClaimUnavailable,
    #[error("daily report outcome could not be persisted; the claim remains sending")]
    CompletionUncertain,
    #[error("daily report delivery attempt timed out; any claim remains sending")]
    AttemptTimedOut,
}

#[derive(Clone)]
pub struct GmailOutboxWorker {
    outbox: Arc<dyn DeliveryOutbox>,
    artifacts: Arc<dyn ArtifactLoader>,
    delivery: Arc<dyn MailDelivery>,
    clock: Arc<dyn DeliveryClock>,
}

impl fmt::Debug for GmailOutboxWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GmailOutboxWorker")
            .field("delivery", &"single-attempt")
            .finish()
    }
}

impl GmailOutboxWorker {
    #[must_use]
    pub fn new(
        outbox: PostgresOutboxRepository,
        artifacts: LocalArtifactStore,
        delivery: GmailProvider,
    ) -> Self {
        Self {
            outbox: Arc::new(outbox),
            artifacts: Arc::new(artifacts),
            delivery: Arc::new(delivery),
            clock: Arc::new(SystemClock),
        }
    }

    #[cfg(test)]
    fn for_test(
        outbox: Arc<dyn DeliveryOutbox>,
        artifacts: Arc<dyn ArtifactLoader>,
        delivery: Arc<dyn MailDelivery>,
        clock: Arc<dyn DeliveryClock>,
    ) -> Self {
        Self {
            outbox,
            artifacts,
            delivery,
            clock,
        }
    }

    pub async fn deliver_one(&self) -> Result<DeliveryTickOutcome, GmailOutboxError> {
        let started_at = self.clock.now();
        // Probe first so an idle tick never calls the OAuth provider.
        if !self
            .outbox
            .has_ready(started_at)
            .await
            .map_err(|_| GmailOutboxError::ClaimUnavailable)?
        {
            return Ok(DeliveryTickOutcome::Idle);
        }
        // Refresh before any row becomes `sending`. A failed refresh is still
        // recorded against the claimed row below as the known, pre-send outcome
        // it is; a slow or interrupted one leaves nothing claimed.
        let authorization = self.delivery.authorize().await;
        let Some(claim) = self
            .outbox
            .claim(started_at)
            .await
            .map_err(|_| GmailOutboxError::ClaimUnavailable)?
        else {
            return Ok(DeliveryTickOutcome::Idle);
        };

        let Ok(bundle) = self.artifacts.load(claim.artifact.clone()).await else {
            let finished_at = self.clock.now();
            self.outbox
                .permanent(
                    &claim,
                    started_at,
                    finished_at,
                    DeliveryErrorClass::InvalidArtifact,
                )
                .await
                .map_err(|_| GmailOutboxError::CompletionUncertain)?;
            return Ok(permanent_outcome(&claim));
        };

        let result = match &authorization {
            Ok(token) => self.delivery.deliver(&claim, bundle, token).await,
            Err(error) => Err(*error),
        };
        let finished_at = self.clock.now();
        match result {
            Ok(receipt) => {
                self.outbox
                    .sent(
                        &claim,
                        started_at,
                        finished_at,
                        &receipt.provider_message_id,
                    )
                    .await
                    .map_err(|_| GmailOutboxError::CompletionUncertain)?;
                Ok(DeliveryTickOutcome::Sent {
                    batch_id: claim.batch_id,
                    attempt_no: claim.attempt_no,
                })
            }
            Err(GmailDeliveryError::Ambiguous) => Ok(DeliveryTickOutcome::Ambiguous {
                batch_id: claim.batch_id,
                attempt_no: claim.attempt_no,
            }),
            Err(error) => {
                self.record_known_failure(&claim, started_at, finished_at, error)
                    .await
            }
        }
    }

    /// Drains a bounded number of ready rows for one scheduler pass.
    ///
    /// Every row still gets one provider attempt. Observing an empty queue ends
    /// the pass early; otherwise the hard cap leaves remaining work for the
    /// next minute tick. A timed-out attempt is never converted into a retry:
    /// if it had already claimed a row, that row stays `sending`.
    ///
    /// Once `stop` is cancelled the pass claims nothing new, but an attempt
    /// already in flight runs to its recorded outcome first. A routine restart
    /// therefore never abandons a claimed row in `sending`.
    pub async fn deliver_ready(
        &self,
        stop: &CancellationToken,
    ) -> Result<DeliveryPassOutcome, GmailOutboxError> {
        let mut attempts = 0_u8;
        while attempts < MAX_DELIVERIES_PER_PASS {
            if stop.is_cancelled() {
                return Ok(DeliveryPassOutcome {
                    attempts,
                    queue_drained: false,
                });
            }
            let outcome = tokio::time::timeout(DELIVERY_ATTEMPT_TIMEOUT, self.deliver_one())
                .await
                .map_err(|_| GmailOutboxError::AttemptTimedOut)??;
            if outcome == DeliveryTickOutcome::Idle {
                return Ok(DeliveryPassOutcome {
                    attempts,
                    queue_drained: true,
                });
            }
            attempts += 1;
        }
        Ok(DeliveryPassOutcome {
            attempts,
            queue_drained: false,
        })
    }

    async fn record_known_failure(
        &self,
        claim: &ClaimedDelivery,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        error: GmailDeliveryError,
    ) -> Result<DeliveryTickOutcome, GmailOutboxError> {
        if let Some(class) = permanent_class(error) {
            self.outbox
                .permanent(claim, started_at, finished_at, class)
                .await
                .map_err(|_| GmailOutboxError::CompletionUncertain)?;
            return Ok(permanent_outcome(claim));
        }

        let class = transient_class(error).ok_or(GmailOutboxError::CompletionUncertain)?;
        let retry_at = finished_at + retry_delay(claim.attempt_no);
        // The claim carries the deadline persisted with its coverage. Deriving
        // it from the report kind would give a recovered morning occurrence the
        // 14:00 window it was explicitly moved out of, so its first transient
        // failure would exhaust a budget that still had hours left.
        if retry_at <= claim.deadline_at {
            self.outbox
                .transient(claim, started_at, finished_at, class, retry_at)
                .await
                .map_err(|_| GmailOutboxError::CompletionUncertain)?;
            Ok(DeliveryTickOutcome::RetryScheduled {
                batch_id: claim.batch_id,
                attempt_no: claim.attempt_no,
            })
        } else {
            self.outbox
                .exhausted(claim, started_at, finished_at, class)
                .await
                .map_err(|_| GmailOutboxError::CompletionUncertain)?;
            Ok(DeliveryTickOutcome::RetryExhausted {
                batch_id: claim.batch_id,
                attempt_no: claim.attempt_no,
            })
        }
    }
}

const fn permanent_outcome(claim: &ClaimedDelivery) -> DeliveryTickOutcome {
    DeliveryTickOutcome::PermanentFailure {
        batch_id: claim.batch_id,
        attempt_no: claim.attempt_no,
    }
}

const fn permanent_class(error: GmailDeliveryError) -> Option<DeliveryErrorClass> {
    match error {
        GmailDeliveryError::Routing => Some(DeliveryErrorClass::InvalidRouting),
        GmailDeliveryError::Message => Some(DeliveryErrorClass::InvalidArtifact),
        GmailDeliveryError::Authentication => Some(DeliveryErrorClass::Authentication),
        GmailDeliveryError::ProviderRejected => Some(DeliveryErrorClass::ProviderRejected),
        GmailDeliveryError::OAuthRateLimited
        | GmailDeliveryError::OAuthUnavailable
        | GmailDeliveryError::OAuthInvalidResponse
        | GmailDeliveryError::ProviderRateLimited
        | GmailDeliveryError::Ambiguous => None,
    }
}

const fn transient_class(error: GmailDeliveryError) -> Option<DeliveryErrorClass> {
    match error {
        GmailDeliveryError::OAuthRateLimited | GmailDeliveryError::ProviderRateLimited => {
            Some(DeliveryErrorClass::RateLimited)
        }
        GmailDeliveryError::OAuthUnavailable | GmailDeliveryError::OAuthInvalidResponse => {
            Some(DeliveryErrorClass::ProviderUnavailable)
        }
        GmailDeliveryError::Routing
        | GmailDeliveryError::Message
        | GmailDeliveryError::Authentication
        | GmailDeliveryError::ProviderRejected
        | GmailDeliveryError::Ambiguous => None,
    }
}

fn retry_delay(attempt_no: u8) -> Duration {
    let exponent = u32::from(attempt_no.saturating_sub(1)).min(4);
    Duration::seconds((RETRY_BASE_SECONDS * 2_i64.pow(exponent)).min(RETRY_MAX_SECONDS))
}

#[cfg(test)]
mod tests;
