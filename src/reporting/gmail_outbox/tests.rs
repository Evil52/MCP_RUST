mod shutdown;

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
use tokio_util::sync::CancellationToken;

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
    probe_error: bool,
    completion_error: bool,
    recorded: Mutex<Vec<Recorded>>,
}

impl DeliveryOutbox for FakeOutbox {
    fn has_ready(&self, _now: DateTime<Utc>) -> ProbeFuture<'_> {
        Box::pin(async move {
            if self.probe_error {
                return Err(PostgresOutboxError::Unavailable);
            }
            let claims = self.claims.lock().unwrap();
            Ok(!matches!(claims.front(), None | Some(Ok(None))))
        })
    }

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
    authorization: Mutex<Result<GmailAccessToken, GmailDeliveryError>>,
    authorizations: AtomicUsize,
    result: Mutex<Result<GmailSendReceipt, GmailDeliveryError>>,
    calls: AtomicUsize,
}

impl MailDelivery for FakeDelivery {
    fn authorize(&self) -> AuthorizationFuture<'_> {
        Box::pin(async move {
            self.authorizations.fetch_add(1, Ordering::Relaxed);
            self.authorization.lock().unwrap().clone()
        })
    }

    fn deliver<'a>(
        &'a self,
        _claim: &'a ClaimedDelivery,
        _bundle: StoredReportBundle,
        token: &'a GmailAccessToken,
    ) -> DeliveryFuture<'a> {
        Box::pin(async move {
            assert_eq!(token.as_str(), "access-token");
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.result.lock().unwrap().clone()
        })
    }
}

fn fake_delivery(result: Result<GmailSendReceipt, GmailDeliveryError>) -> Arc<FakeDelivery> {
    Arc::new(FakeDelivery {
        authorization: Mutex::new(Ok(GmailAccessToken::for_test("access-token"))),
        authorizations: AtomicUsize::new(0),
        result: Mutex::new(result),
        calls: AtomicUsize::new(0),
    })
}

struct PendingDelivery;

impl MailDelivery for PendingDelivery {
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
        probe_error: false,
        completion_error,
        recorded: Mutex::new(Vec::new()),
    });
    let artifacts = Arc::new(FakeArtifacts {
        fail: artifact_fail,
        calls: AtomicUsize::new(0),
    });
    let delivery = fake_delivery(delivery_result);
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
        probe_error: false,
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
    let (idle, _, artifacts, delivery) = worker(Ok(None), false, receipt(), false, vec![at(12, 0)]);
    assert_eq!(idle.deliver_one().await.unwrap(), DeliveryTickOutcome::Idle);
    assert_eq!(artifacts.calls.load(Ordering::Relaxed), 0);
    assert_eq!(delivery.authorizations.load(Ordering::Relaxed), 0);
    assert_eq!(delivery.calls.load(Ordering::Relaxed), 0);

    let probe_failure = Arc::new(FakeOutbox {
        claims: Mutex::new(VecDeque::from([Ok(Some(claim(1, ReportKind::Evening)))])),
        probe_error: true,
        completion_error: false,
        recorded: Mutex::new(Vec::new()),
    });
    let delivery = fake_delivery(receipt());
    let unprobed = GmailOutboxWorker::for_test(
        probe_failure.clone(),
        artifacts.clone(),
        delivery.clone(),
        Arc::new(FakeClock {
            times: Mutex::new(vec![at(12, 0)].into()),
        }),
    );
    assert_eq!(
        unprobed.deliver_one().await,
        Err(GmailOutboxError::ClaimUnavailable)
    );
    assert_eq!(delivery.authorizations.load(Ordering::Relaxed), 0);
    assert_eq!(probe_failure.claims.lock().unwrap().len(), 1);

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
    let delivery = fake_delivery(receipt());
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
        drained
            .deliver_ready(&CancellationToken::new())
            .await
            .unwrap(),
        DeliveryPassOutcome {
            attempts: 2,
            queue_drained: true,
        }
    );
    assert_eq!(delivery.calls.load(Ordering::Relaxed), 2);
    assert_eq!(artifacts.calls.load(Ordering::Relaxed), 2);
    assert_eq!(outbox.recorded.lock().unwrap().len(), 2);

    let delivery = fake_delivery(receipt());
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
        bounded
            .deliver_ready(&CancellationToken::new())
            .await
            .unwrap(),
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
        worker.deliver_ready(&CancellationToken::new()).await,
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
async fn an_oauth_failure_before_the_claim_is_recorded_without_a_send() {
    for (error, recorded) in [
        (
            GmailDeliveryError::OAuthUnavailable,
            Recorded::Transient(DeliveryErrorClass::ProviderUnavailable),
        ),
        (
            GmailDeliveryError::Authentication,
            Recorded::Permanent(DeliveryErrorClass::Authentication),
        ),
    ] {
        let (worker, outbox, artifacts, delivery) = worker(
            Ok(Some(claim(1, ReportKind::Evening))),
            false,
            receipt(),
            false,
            vec![at(12, 0), at(12, 1)],
        );
        *delivery.authorization.lock().unwrap() = Err(error);
        worker.deliver_one().await.unwrap();
        assert_eq!(delivery.authorizations.load(Ordering::Relaxed), 1);
        assert_eq!(delivery.calls.load(Ordering::Relaxed), 0);
        assert_eq!(artifacts.calls.load(Ordering::Relaxed), 1);
        assert_eq!(outbox.recorded.lock().unwrap().as_slice(), &[recorded]);
    }
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

#[ignore = "requires the isolated report-worker PostgreSQL role"]
#[tokio::test]
async fn concrete_outbox_artifact_oauth_and_gmail_adapters_complete_one_local_delivery() {
    let database_url = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL")
        .expect("isolated test fixture must provide REPORT_OUTBOX_TEST_WORKER_URL");
    exercise_concrete_delivery(&database_url).await;
}

async fn exercise_concrete_delivery(database_url: &str) {
    let recipient = format!("gmail_e2e_{}", std::process::id());
    let config = Config::from_str(database_url).unwrap();
    let setup = PostgresOutboxRepository::connect(&config).await.unwrap();
    let covered_morning = std::iter::once(ReportKey {
        local_date: NaiveDate::from_ymd_opt(2098, 8, 19).unwrap(),
        kind: ReportKind::Morning,
        recipient_id: recipient.clone(),
        report_version: 1,
    })
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
