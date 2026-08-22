//! Exactly-once-oriented bridge between the report outbox and Gmail.
//!
//! This module does not own a background loop. One call claims at most one
//! ready row, verifies its immutable artifact, performs one provider attempt,
//! and records only an outcome whose safety is known. A bounded pass may invoke
//! that primitive repeatedly, but an ambiguous provider outcome or a post-claim
//! persistence failure leaves the row `sending`, so another worker cannot
//! resend it automatically.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use chrono::{DateTime, Duration, Utc};

use super::{
    artifact_store::{ArtifactStoreError, LocalArtifactStore, StoredReportBundle},
    gmail_delivery::{GmailDeliveryError, GmailDeliveryService},
    gmail_oauth::GmailOAuthCredentials,
    mail_routing::MailRouting,
    outbox::{ArtifactIdentity, DeliveryErrorClass},
    postgres_outbox::{ClaimedDelivery, PostgresOutboxError, PostgresOutboxRepository},
};

const RETRY_BASE_SECONDS: i64 = 60;
const RETRY_MAX_SECONDS: i64 = 15 * 60;
const DELIVERY_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
const MAX_DELIVERIES_PER_PASS: u8 = 16;

type ClaimFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ClaimedDelivery>, PostgresOutboxError>> + Send + 'a>>;
type CompletionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), PostgresOutboxError>> + Send + 'a>>;
type ArtifactFuture<'a> =
    Pin<Box<dyn Future<Output = Result<StoredReportBundle, ArtifactStoreError>> + Send + 'a>>;
type DeliveryFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<super::gmail::GmailSendReceipt, GmailDeliveryError>> + Send + 'a,
    >,
>;

trait DeliveryOutbox: Send + Sync {
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
            tokio::task::spawn_blocking(move || LocalArtifactStore::load(&store, &artifact))
                .await
                .map_err(|_| ArtifactStoreError::Unavailable)?
        })
    }
}

trait MailDelivery: Send + Sync {
    fn deliver<'a>(
        &'a self,
        claim: &'a ClaimedDelivery,
        bundle: StoredReportBundle,
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
    pub fn new(
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
    fn deliver<'a>(
        &'a self,
        claim: &'a ClaimedDelivery,
        bundle: StoredReportBundle,
    ) -> DeliveryFuture<'a> {
        Box::pin(async move {
            self.service
                .deliver(&self.routing, &self.credentials, claim, bundle)
                .await
        })
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

        let result = self.delivery.deliver(&claim, bundle).await;
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
    pub async fn deliver_ready(&self) -> Result<DeliveryPassOutcome, GmailOutboxError> {
        let mut attempts = 0_u8;
        while attempts < MAX_DELIVERIES_PER_PASS {
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

fn permanent_outcome(claim: &ClaimedDelivery) -> DeliveryTickOutcome {
    DeliveryTickOutcome::PermanentFailure {
        batch_id: claim.batch_id,
        attempt_no: claim.attempt_no,
    }
}

fn permanent_class(error: GmailDeliveryError) -> Option<DeliveryErrorClass> {
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

fn transient_class(error: GmailDeliveryError) -> Option<DeliveryErrorClass> {
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
mod tests {
    use std::{
        collections::VecDeque,
        fs,
        future::IntoFuture,
        path::Path,
        str::FromStr,
        sync::{
            Mutex,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
    };

    use axum::{Router, http::StatusCode, routing::post};
    use chrono::{NaiveDate, TimeZone};
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use tokio::{net::TcpListener, task::JoinHandle};
    use tokio_postgres::Config;

    use super::*;
    use crate::config::AccessRegistry;
    use crate::reporting::{
        ReportKey, ReportKind,
        artifact_store::persist_and_mark_ready,
        bundle::ReportBundle,
        due_deliveries,
        gmail::GmailSendReceipt,
        postgres_outbox::{CreateOutcome, PostgresOutboxError},
    };

    static NEXT_DIR: AtomicU64 = AtomicU64::new(1);

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Recorded {
        Sent,
        Transient(DeliveryErrorClass),
        Exhausted(DeliveryErrorClass),
        Permanent(DeliveryErrorClass),
    }

    struct FakeOutbox {
        claims: Mutex<VecDeque<Result<Option<ClaimedDelivery>, PostgresOutboxError>>>,
        completion_error: bool,
        recorded: Mutex<Vec<Recorded>>,
    }

    impl DeliveryOutbox for FakeOutbox {
        fn claim(&self, _now: DateTime<Utc>) -> ClaimFuture<'_> {
            Box::pin(async move { self.claims.lock().unwrap().pop_front().unwrap_or(Ok(None)) })
        }

        fn sent<'a>(
            &'a self,
            _claim: &'a ClaimedDelivery,
            _started_at: DateTime<Utc>,
            _finished_at: DateTime<Utc>,
            _provider_message_id: &'a str,
        ) -> CompletionFuture<'a> {
            self.complete(Recorded::Sent)
        }

        fn transient<'a>(
            &'a self,
            _claim: &'a ClaimedDelivery,
            _started_at: DateTime<Utc>,
            _finished_at: DateTime<Utc>,
            class: DeliveryErrorClass,
            _retry_at: DateTime<Utc>,
        ) -> CompletionFuture<'a> {
            self.complete(Recorded::Transient(class))
        }

        fn exhausted<'a>(
            &'a self,
            _claim: &'a ClaimedDelivery,
            _started_at: DateTime<Utc>,
            _finished_at: DateTime<Utc>,
            class: DeliveryErrorClass,
        ) -> CompletionFuture<'a> {
            self.complete(Recorded::Exhausted(class))
        }

        fn permanent<'a>(
            &'a self,
            _claim: &'a ClaimedDelivery,
            _started_at: DateTime<Utc>,
            _finished_at: DateTime<Utc>,
            class: DeliveryErrorClass,
        ) -> CompletionFuture<'a> {
            self.complete(Recorded::Permanent(class))
        }
    }

    impl FakeOutbox {
        fn complete(&self, outcome: Recorded) -> CompletionFuture<'_> {
            Box::pin(async move {
                self.recorded.lock().unwrap().push(outcome);
                if self.completion_error {
                    Err(PostgresOutboxError::Unavailable)
                } else {
                    Ok(())
                }
            })
        }
    }

    struct FakeArtifacts {
        fail: bool,
        calls: AtomicUsize,
    }

    impl ArtifactLoader for FakeArtifacts {
        fn load(&self, _artifact: ArtifactIdentity) -> ArtifactFuture<'_> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::Relaxed);
                if self.fail {
                    Err(ArtifactStoreError::Integrity)
                } else {
                    Ok(bundle())
                }
            })
        }
    }

    struct FakeDelivery {
        result: Mutex<Result<GmailSendReceipt, GmailDeliveryError>>,
        calls: AtomicUsize,
    }

    impl MailDelivery for FakeDelivery {
        fn deliver<'a>(
            &'a self,
            _claim: &'a ClaimedDelivery,
            _bundle: StoredReportBundle,
        ) -> DeliveryFuture<'a> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::Relaxed);
                self.result.lock().unwrap().clone()
            })
        }
    }

    struct PendingDelivery;

    impl MailDelivery for PendingDelivery {
        fn deliver<'a>(
            &'a self,
            _claim: &'a ClaimedDelivery,
            _bundle: StoredReportBundle,
        ) -> DeliveryFuture<'a> {
            Box::pin(std::future::pending())
        }
    }

    struct FakeClock {
        times: Mutex<VecDeque<DateTime<Utc>>>,
    }

    impl DeliveryClock for FakeClock {
        fn now(&self) -> DateTime<Utc> {
            self.times.lock().unwrap().pop_front().unwrap()
        }
    }

    fn at(hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2098, 8, 19, hour, minute, 0).unwrap()
    }

    /// Claim carrying the standard window for its kind: 14:00 EKB (09:00 UTC)
    /// for morning, 23:00 EKB (18:00 UTC) for evening.
    fn claim(attempt_no: u8, kind: ReportKind) -> ClaimedDelivery {
        let deadline_at = match kind {
            ReportKind::Morning => at(9, 0),
            ReportKind::Evening => at(18, 0),
        };
        claim_with_deadline(attempt_no, kind, deadline_at)
    }

    /// Claim carrying an explicit persisted deadline, as `claim_ready` builds
    /// it from `delivery_coverage`.
    fn claim_with_deadline(
        attempt_no: u8,
        kind: ReportKind,
        deadline_at: DateTime<Utc>,
    ) -> ClaimedDelivery {
        ClaimedDelivery {
            batch_id: 7,
            recipient_id: "owner".to_owned(),
            report_version: 1,
            attempt_no,
            artifact: ArtifactIdentity {
                object_key: "daily-reports/2098/08/19/owner/v1/evening.xlsx".to_owned(),
                sha256: "a".repeat(64),
                html_sha256: "b".repeat(64),
            },
            covered_keys: vec![ReportKey {
                local_date: NaiveDate::from_ymd_opt(2098, 8, 19).unwrap(),
                kind,
                recipient_id: "owner".to_owned(),
                report_version: 1,
            }],
            deadline_at,
        }
    }

    fn bundle() -> StoredReportBundle {
        StoredReportBundle {
            html: "<html>report</html>".to_owned(),
            xlsx: vec![1, 2, 3],
        }
    }

    fn worker(
        claim_result: Result<Option<ClaimedDelivery>, PostgresOutboxError>,
        artifact_fail: bool,
        delivery_result: Result<GmailSendReceipt, GmailDeliveryError>,
        completion_error: bool,
        times: Vec<DateTime<Utc>>,
    ) -> (
        GmailOutboxWorker,
        Arc<FakeOutbox>,
        Arc<FakeArtifacts>,
        Arc<FakeDelivery>,
    ) {
        let outbox = Arc::new(FakeOutbox {
            claims: Mutex::new(VecDeque::from([claim_result])),
            completion_error,
            recorded: Mutex::new(Vec::new()),
        });
        let artifacts = Arc::new(FakeArtifacts {
            fail: artifact_fail,
            calls: AtomicUsize::new(0),
        });
        let delivery = Arc::new(FakeDelivery {
            result: Mutex::new(delivery_result),
            calls: AtomicUsize::new(0),
        });
        let clock = Arc::new(FakeClock {
            times: Mutex::new(times.into()),
        });
        (
            GmailOutboxWorker::for_test(outbox.clone(), artifacts.clone(), delivery.clone(), clock),
            outbox,
            artifacts,
            delivery,
        )
    }

    // FakeDelivery consumes the production Result shape; keeping that shape in
    // the success fixture makes every call site explicit and symmetric with
    // failure fixtures.
    #[allow(clippy::unnecessary_wraps)]
    fn receipt() -> Result<GmailSendReceipt, GmailDeliveryError> {
        Ok(GmailSendReceipt {
            provider_message_id: "message-1".to_owned(),
        })
    }

    fn queued_worker(
        claims: Vec<Result<Option<ClaimedDelivery>, PostgresOutboxError>>,
        times: Vec<DateTime<Utc>>,
        delivery: Arc<dyn MailDelivery>,
    ) -> (GmailOutboxWorker, Arc<FakeOutbox>, Arc<FakeArtifacts>) {
        let outbox = Arc::new(FakeOutbox {
            claims: Mutex::new(claims.into()),
            completion_error: false,
            recorded: Mutex::new(Vec::new()),
        });
        let artifacts = Arc::new(FakeArtifacts {
            fail: false,
            calls: AtomicUsize::new(0),
        });
        let clock = Arc::new(FakeClock {
            times: Mutex::new(times.into()),
        });
        (
            GmailOutboxWorker::for_test(outbox.clone(), artifacts.clone(), delivery, clock),
            outbox,
            artifacts,
        )
    }

    fn policy(audience_id: &str) -> super::super::policy::DailyReportPolicy {
        let registry: AccessRegistry = serde_json::from_value(json!({
            "version": 1,
            "actors": [
                {"id":"diana","name":"Diana","role":"manager","oidc":{"username":"diana"}}
            ],
            "accounts": [
                {"id":"ozon","organization":"Ozon","marketplace":"ozon","seller_client_id":"1","manager_id":"diana","ozon":{"store_id":"1","client_id_env":"OZON_ID","api_key_env":"OZON_KEY"}}
            ]
        }))
        .unwrap();
        let bytes = serde_json::to_vec(&json!({
            "version": 1,
            "enabled": false,
            "timezone": "Asia/Yekaterinburg",
            "sender_email_env": "SENDER",
            "audiences": [{
                "id": audience_id,
                "email_env": "RECIPIENT",
                "managers": [{"actor_id":"diana","account_ids":["ozon"]}]
            }]
        }))
        .unwrap();
        super::super::policy::DailyReportPolicy::from_slice(&bytes, &registry).unwrap()
    }

    fn credential_directory() -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "mcp-ozon-gmail-outbox-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        set_mode(&directory, 0o700);
        for (name, value) in [
            ("client_id", "client-id.apps.googleusercontent.com\n"),
            ("client_secret", "client-secret\n"),
            ("refresh_token", "refresh-token\n"),
        ] {
            let path = directory.join(name);
            fs::write(&path, value).unwrap();
            set_mode(&path, 0o600);
        }
        directory
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt as _;

        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    #[cfg(not(unix))]
    fn set_mode(_path: &Path, _mode: u32) {}

    async fn local_mail_server() -> (String, JoinHandle<std::io::Result<()>>) {
        async fn token() -> (StatusCode, &'static str) {
            (
                StatusCode::OK,
                r#"{"access_token":"access-token","token_type":"Bearer","expires_in":3600,"scope":"https://www.googleapis.com/auth/gmail.send"}"#,
            )
        }

        async fn send() -> (StatusCode, &'static str) {
            (StatusCode::OK, r#"{"id":"gmail-e2e-message"}"#)
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/token", post(token))
            .route("/gmail/v1/users/me/messages/send", post(send));
        let task = tokio::spawn(axum::serve(listener, app).into_future());
        (format!("http://{address}"), task)
    }

    fn report_bundle(recipient: &str) -> ReportBundle {
        let html = "<html><body>local Gmail outbox report</body></html>".to_owned();
        let xlsx = b"local-gmail-outbox-xlsx".to_vec();
        let sha256 = hex_sha256(&xlsx);
        let html_sha256 = hex_sha256(html.as_bytes());
        ReportBundle {
            artifact: ArtifactIdentity {
                object_key: format!("daily-reports/2098/08/19/{recipient}/v1/evening.xlsx"),
                sha256,
                html_sha256,
            },
            attachment_name: "daily-report-2098-08-19-evening.xlsx".to_owned(),
            html,
            xlsx,
        }
    }

    fn hex_sha256(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .fold(String::with_capacity(64), |mut output, byte| {
                use std::fmt::Write as _;
                write!(output, "{byte:02x}").expect("writing to String cannot fail");
                output
            })
    }

    fn create_outcome_id(outcome: CreateOutcome) -> i64 {
        match outcome {
            CreateOutcome::Inserted(batch_id) | CreateOutcome::Existing(batch_id) => batch_id,
        }
    }

    #[tokio::test]
    async fn idle_claim_error_and_success_have_exact_side_effects() {
        let (idle, _, artifacts, delivery) =
            worker(Ok(None), false, receipt(), false, vec![at(12, 0)]);
        assert_eq!(idle.deliver_one().await.unwrap(), DeliveryTickOutcome::Idle);
        assert_eq!(artifacts.calls.load(Ordering::Relaxed), 0);
        assert_eq!(delivery.calls.load(Ordering::Relaxed), 0);

        let (failed, _, _, _) = worker(
            Err(PostgresOutboxError::Unavailable),
            false,
            receipt(),
            false,
            vec![at(12, 0)],
        );
        assert_eq!(
            failed.deliver_one().await,
            Err(GmailOutboxError::ClaimUnavailable)
        );

        let (success, outbox, _, _) = worker(
            Ok(Some(claim(1, ReportKind::Evening))),
            false,
            receipt(),
            false,
            vec![at(12, 0), at(12, 1)],
        );
        assert_eq!(
            success.deliver_one().await.unwrap(),
            DeliveryTickOutcome::Sent {
                batch_id: 7,
                attempt_no: 1
            }
        );
        assert_eq!(
            outbox.recorded.lock().unwrap().as_slice(),
            &[Recorded::Sent]
        );
        assert_eq!(
            format!("{success:?}"),
            "GmailOutboxWorker { delivery: \"single-attempt\" }"
        );
    }

    #[tokio::test]
    async fn delivery_pass_stops_on_idle_and_hard_caps_a_nonempty_queue() {
        let delivery = Arc::new(FakeDelivery {
            result: Mutex::new(receipt()),
            calls: AtomicUsize::new(0),
        });
        let (drained, outbox, artifacts) = queued_worker(
            vec![
                Ok(Some(claim(1, ReportKind::Evening))),
                Ok(Some(claim(1, ReportKind::Evening))),
                Ok(None),
            ],
            (0..5).map(|minute| at(12, minute)).collect(),
            delivery.clone(),
        );
        assert_eq!(
            drained.deliver_ready().await.unwrap(),
            DeliveryPassOutcome {
                attempts: 2,
                queue_drained: true,
            }
        );
        assert_eq!(delivery.calls.load(Ordering::Relaxed), 2);
        assert_eq!(artifacts.calls.load(Ordering::Relaxed), 2);
        assert_eq!(outbox.recorded.lock().unwrap().len(), 2);

        let delivery = Arc::new(FakeDelivery {
            result: Mutex::new(receipt()),
            calls: AtomicUsize::new(0),
        });
        let (bounded, outbox, artifacts) = queued_worker(
            (0..=MAX_DELIVERIES_PER_PASS)
                .map(|_| Ok(Some(claim(1, ReportKind::Evening))))
                .collect(),
            (0..(MAX_DELIVERIES_PER_PASS * 2))
                .map(|second| at(13, 0) + Duration::seconds(i64::from(second)))
                .collect(),
            delivery.clone(),
        );
        assert_eq!(
            bounded.deliver_ready().await.unwrap(),
            DeliveryPassOutcome {
                attempts: MAX_DELIVERIES_PER_PASS,
                queue_drained: false,
            }
        );
        assert_eq!(
            delivery.calls.load(Ordering::Relaxed),
            usize::from(MAX_DELIVERIES_PER_PASS)
        );
        assert_eq!(
            artifacts.calls.load(Ordering::Relaxed),
            usize::from(MAX_DELIVERIES_PER_PASS)
        );
        assert_eq!(
            outbox.recorded.lock().unwrap().len(),
            usize::from(MAX_DELIVERIES_PER_PASS)
        );
        assert_eq!(outbox.claims.lock().unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn delivery_pass_timeout_leaves_a_claim_unclassified() {
        let (worker, outbox, artifacts) = queued_worker(
            vec![Ok(Some(claim(1, ReportKind::Evening)))],
            vec![at(12, 0)],
            Arc::new(PendingDelivery),
        );
        assert_eq!(
            worker.deliver_ready().await,
            Err(GmailOutboxError::AttemptTimedOut)
        );
        assert_eq!(artifacts.calls.load(Ordering::Relaxed), 1);
        assert!(outbox.recorded.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn artifact_and_every_known_permanent_failure_are_recorded_once() {
        let (artifact, outbox, _, delivery) = worker(
            Ok(Some(claim(1, ReportKind::Evening))),
            true,
            receipt(),
            false,
            vec![at(12, 0), at(12, 1)],
        );
        assert!(matches!(
            artifact.deliver_one().await.unwrap(),
            DeliveryTickOutcome::PermanentFailure { .. }
        ));
        assert_eq!(delivery.calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            outbox.recorded.lock().unwrap().as_slice(),
            &[Recorded::Permanent(DeliveryErrorClass::InvalidArtifact)]
        );

        for (error, class) in [
            (
                GmailDeliveryError::Routing,
                DeliveryErrorClass::InvalidRouting,
            ),
            (
                GmailDeliveryError::Message,
                DeliveryErrorClass::InvalidArtifact,
            ),
            (
                GmailDeliveryError::Authentication,
                DeliveryErrorClass::Authentication,
            ),
            (
                GmailDeliveryError::ProviderRejected,
                DeliveryErrorClass::ProviderRejected,
            ),
        ] {
            let (worker, outbox, _, _) = worker(
                Ok(Some(claim(1, ReportKind::Evening))),
                false,
                Err(error),
                false,
                vec![at(12, 0), at(12, 1)],
            );
            assert!(matches!(
                worker.deliver_one().await.unwrap(),
                DeliveryTickOutcome::PermanentFailure { .. }
            ));
            assert_eq!(
                outbox.recorded.lock().unwrap().as_slice(),
                &[Recorded::Permanent(class)]
            );
        }
    }

    #[tokio::test]
    async fn retryable_failures_schedule_or_exhaust_without_internal_send_retry() {
        for (error, class) in [
            (
                GmailDeliveryError::OAuthRateLimited,
                DeliveryErrorClass::RateLimited,
            ),
            (
                GmailDeliveryError::ProviderRateLimited,
                DeliveryErrorClass::RateLimited,
            ),
            (
                GmailDeliveryError::OAuthUnavailable,
                DeliveryErrorClass::ProviderUnavailable,
            ),
            (
                GmailDeliveryError::OAuthInvalidResponse,
                DeliveryErrorClass::ProviderUnavailable,
            ),
        ] {
            let (worker, outbox, _, delivery) = worker(
                Ok(Some(claim(1, ReportKind::Evening))),
                false,
                Err(error),
                false,
                vec![at(12, 0), at(12, 1)],
            );
            assert!(matches!(
                worker.deliver_one().await.unwrap(),
                DeliveryTickOutcome::RetryScheduled { .. }
            ));
            assert_eq!(delivery.calls.load(Ordering::Relaxed), 1);
            assert_eq!(
                outbox.recorded.lock().unwrap().as_slice(),
                &[Recorded::Transient(class)]
            );
        }

        let (exhausted, outbox, _, _) = worker(
            Ok(Some(claim(4, ReportKind::Morning))),
            false,
            Err(GmailDeliveryError::ProviderRateLimited),
            false,
            vec![at(8, 59), at(9, 0)],
        );
        assert!(matches!(
            exhausted.deliver_one().await.unwrap(),
            DeliveryTickOutcome::RetryExhausted { .. }
        ));
        assert_eq!(
            outbox.recorded.lock().unwrap().as_slice(),
            &[Recorded::Exhausted(DeliveryErrorClass::RateLimited)]
        );
        assert_eq!(retry_delay(1), Duration::seconds(60));
        assert_eq!(retry_delay(5), Duration::seconds(15 * 60));
        assert_eq!(retry_delay(u8::MAX), Duration::seconds(15 * 60));
    }

    #[tokio::test]
    async fn recovered_morning_keeps_the_evening_retry_window_it_was_scheduled_into() {
        // A morning occurrence recovered after 17:00 EKB is scheduled at the
        // evening boundary and persists the 23:00 EKB (18:00 UTC) deadline.
        // Deriving the window from the report kind would collapse it back to
        // 14:00 EKB and make the first transient failure terminal.
        let (worker, outbox, _, _) = worker(
            Ok(Some(claim_with_deadline(1, ReportKind::Morning, at(18, 0)))),
            false,
            Err(GmailDeliveryError::ProviderRateLimited),
            false,
            vec![at(12, 5), at(12, 6)],
        );
        assert!(matches!(
            worker.deliver_one().await.unwrap(),
            DeliveryTickOutcome::RetryScheduled { .. }
        ));
        assert_eq!(
            outbox.recorded.lock().unwrap().as_slice(),
            &[Recorded::Transient(DeliveryErrorClass::RateLimited)]
        );
    }

    #[tokio::test]
    async fn ambiguous_send_and_completion_failure_never_become_a_retry() {
        let (ambiguous, outbox, _, delivery) = worker(
            Ok(Some(claim(1, ReportKind::Evening))),
            false,
            Err(GmailDeliveryError::Ambiguous),
            false,
            vec![at(12, 0), at(12, 1)],
        );
        assert!(matches!(
            ambiguous.deliver_one().await.unwrap(),
            DeliveryTickOutcome::Ambiguous { .. }
        ));
        assert_eq!(delivery.calls.load(Ordering::Relaxed), 1);
        assert!(outbox.recorded.lock().unwrap().is_empty());

        let (uncertain, outbox, _, _) = worker(
            Ok(Some(claim(1, ReportKind::Evening))),
            false,
            receipt(),
            true,
            vec![at(12, 0), at(12, 1)],
        );
        assert_eq!(
            uncertain.deliver_one().await,
            Err(GmailOutboxError::CompletionUncertain)
        );
        assert_eq!(
            outbox.recorded.lock().unwrap().as_slice(),
            &[Recorded::Sent]
        );

        for error in [
            GmailDeliveryError::Routing,
            GmailDeliveryError::Message,
            GmailDeliveryError::Authentication,
            GmailDeliveryError::ProviderRejected,
            GmailDeliveryError::Ambiguous,
        ] {
            assert_eq!(transient_class(error), None);
        }
        assert_eq!(permanent_class(GmailDeliveryError::Ambiguous), None);
    }

    #[test]
    fn concrete_constructors_and_redacted_provider_debug_are_available_for_runtime_wiring() {
        let service = GmailDeliveryService::through_mail_egress().unwrap();
        let policy = policy("owner");
        let routing = MailRouting::from_slice(
            br#"{"version":1,"routes":[{"name":"SENDER","address":"sender@example.test"},{"name":"RECIPIENT","address":"recipient@example.test"}]}"#,
            &policy,
        )
        .unwrap();
        let directory = credential_directory();
        let credentials = GmailOAuthCredentials::load(&directory).unwrap();
        let provider = GmailProvider::new(service, routing, credentials);
        assert!(!format!("{provider:?}").contains("example.test"));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg_attr(
        not(coverage),
        ignore = "requires the isolated report-worker PostgreSQL role"
    )]
    #[tokio::test]
    async fn concrete_outbox_artifact_oauth_and_gmail_adapters_complete_one_local_delivery() {
        let database_url = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL")
            .expect("coverage wrapper must provide the report-worker database URL");
        exercise_concrete_delivery(&database_url).await;
    }

    async fn exercise_concrete_delivery(database_url: &str) {
        let recipient = format!("gmail_e2e_{}", std::process::id());
        let config = Config::from_str(database_url).unwrap();
        let setup = PostgresOutboxRepository::connect(&config).await.unwrap();
        let covered_morning = [ReportKey {
            local_date: NaiveDate::from_ymd_opt(2098, 8, 19).unwrap(),
            kind: ReportKind::Morning,
            recipient_id: recipient.clone(),
            report_version: 1,
        }]
        .into_iter()
        .collect();
        let delivery = due_deliveries(at(12, 0), &recipient, 1, &covered_morning)
            .unwrap()
            .remove(0);
        let batch_id = create_outcome_id(setup.create_planned(delivery.clone()).await.unwrap());
        assert_eq!(
            create_outcome_id(setup.create_planned(delivery).await.unwrap()),
            batch_id
        );
        setup.start_generation(batch_id).await.unwrap();

        let root = std::env::temp_dir().join(format!(
            "mcp-ozon-gmail-outbox-artifacts-{}",
            std::process::id()
        ));
        fs::create_dir(&root).unwrap();
        let store = LocalArtifactStore::open(&root).unwrap();
        persist_and_mark_ready(&store, &setup, batch_id, &report_bundle(&recipient))
            .await
            .unwrap();

        let (base_url, server) = local_mail_server().await;
        let service = GmailDeliveryService::for_test_endpoints(
            &format!("{base_url}/token"),
            &format!("{base_url}/gmail/v1/users/me/messages/send"),
        );
        let routing_document = serde_json::to_vec(&json!({
            "version": 1,
            "routes": [
                {"name":"SENDER","address":"sender@example.test"},
                {"name":"RECIPIENT","address":"recipient@example.test"}
            ]
        }))
        .unwrap();
        let routing = MailRouting::from_slice(&routing_document, &policy(&recipient)).unwrap();
        let credential_root = credential_directory();
        let credentials = GmailOAuthCredentials::load(&credential_root).unwrap();
        let provider = GmailProvider::new(service, routing, credentials);
        let outbox = PostgresOutboxRepository::connect(&config).await.unwrap();
        let worker = GmailOutboxWorker::for_test(
            Arc::new(outbox),
            Arc::new(store.clone()),
            Arc::new(provider.clone()),
            Arc::new(FakeClock {
                times: Mutex::new(vec![at(12, 1), at(12, 2)].into()),
            }),
        );
        assert_eq!(
            worker.deliver_one().await.unwrap(),
            DeliveryTickOutcome::Sent {
                batch_id,
                attempt_no: 1
            }
        );

        let constructor_repository = PostgresOutboxRepository::connect(&config).await.unwrap();
        let mut missing_claim = claim(1, ReportKind::Evening);
        missing_claim.batch_id = i64::MAX;
        assert_eq!(
            DeliveryOutbox::transient(
                &constructor_repository,
                &missing_claim,
                at(12, 1),
                at(12, 2),
                DeliveryErrorClass::RateLimited,
                at(12, 3),
            )
            .await,
            Err(PostgresOutboxError::Unavailable)
        );
        assert_eq!(
            DeliveryOutbox::exhausted(
                &constructor_repository,
                &missing_claim,
                at(12, 1),
                at(12, 2),
                DeliveryErrorClass::RateLimited,
            )
            .await,
            Err(PostgresOutboxError::Unavailable)
        );
        assert_eq!(
            DeliveryOutbox::permanent(
                &constructor_repository,
                &missing_claim,
                at(12, 1),
                at(12, 2),
                DeliveryErrorClass::Authentication,
            )
            .await,
            Err(PostgresOutboxError::Unavailable)
        );
        let concrete = GmailOutboxWorker::new(constructor_repository, store, provider);
        assert!(format!("{concrete:?}").contains("single-attempt"));
        assert!(SystemClock.now() <= Utc::now());
        server.abort();
        fs::remove_dir_all(root).unwrap();
        fs::remove_dir_all(credential_root).unwrap();
    }
}
