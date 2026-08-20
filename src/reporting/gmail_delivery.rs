//! Single-attempt Gmail delivery orchestration.
//!
//! Routing and artifact validation happen before OAuth. OAuth refresh happens
//! before the sole Gmail send attempt. This layer never retries internally:
//! callers may schedule a later attempt only for errors explicitly marked
//! retry-safe, while an ambiguous send outcome must remain `sending` until an
//! operator reconciles it.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use super::{
    artifact_store::StoredReportBundle,
    gmail::{GmailClient, GmailClientBuildError, GmailSendError, GmailSendReceipt},
    gmail_oauth::{
        GmailAccessToken, GmailOAuthClient, GmailOAuthClientBuildError, GmailOAuthCredentials,
        GmailOAuthError,
    },
    mail::{ReportEmail, build_report_email},
    mail_routing::MailRouting,
    postgres_outbox::ClaimedDelivery,
};

type OAuthFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GmailAccessToken, GmailOAuthError>> + Send + 'a>>;
type SendFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GmailSendReceipt, GmailSendError>> + Send + 'a>>;

trait OAuthProvider: Send + Sync {
    fn refresh<'a>(&'a self, credentials: &'a GmailOAuthCredentials) -> OAuthFuture<'a>;
}

impl OAuthProvider for GmailOAuthClient {
    fn refresh<'a>(&'a self, credentials: &'a GmailOAuthCredentials) -> OAuthFuture<'a> {
        Box::pin(async move { GmailOAuthClient::refresh(self, credentials).await })
    }
}

trait MessageProvider: Send + Sync {
    fn send<'a>(&'a self, access_token: &'a str, email: &'a ReportEmail) -> SendFuture<'a>;
}

impl MessageProvider for GmailClient {
    fn send<'a>(&'a self, access_token: &'a str, email: &'a ReportEmail) -> SendFuture<'a> {
        Box::pin(async move { GmailClient::send(self, access_token, email).await })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GmailDeliveryBuildError {
    #[error("fixed Google OAuth delivery transport is invalid")]
    OAuth(#[source] GmailOAuthClientBuildError),
    #[error("fixed Gmail message transport is invalid")]
    Gmail(#[source] GmailClientBuildError),
}

/// Payload-free result classification for one orchestration attempt.
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum GmailDeliveryError {
    #[error("daily report address routing is invalid")]
    Routing,
    #[error("daily report email artifact or scope is invalid")]
    Message,
    #[error("Gmail OAuth credentials were rejected")]
    Authentication,
    #[error("Gmail OAuth refresh was rate limited")]
    OAuthRateLimited,
    #[error("Gmail OAuth refresh is temporarily unavailable")]
    OAuthUnavailable,
    #[error("Gmail OAuth returned an invalid response")]
    OAuthInvalidResponse,
    #[error("Gmail delivery was rate limited before acceptance")]
    ProviderRateLimited,
    #[error("Gmail permanently rejected the message")]
    ProviderRejected,
    #[error("Gmail delivery outcome is ambiguous")]
    Ambiguous,
}

impl GmailDeliveryError {
    /// Safe means a new outbox attempt may be scheduled later. This method
    /// never performs that retry and never marks an ambiguous send as safe.
    pub const fn retry_safe(self) -> bool {
        matches!(
            self,
            Self::OAuthRateLimited
                | Self::OAuthUnavailable
                | Self::OAuthInvalidResponse
                | Self::ProviderRateLimited
        )
    }

    pub const fn is_ambiguous(self) -> bool {
        matches!(self, Self::Ambiguous)
    }
}

#[derive(Clone)]
pub struct GmailDeliveryService {
    oauth: Arc<dyn OAuthProvider>,
    messages: Arc<dyn MessageProvider>,
}

impl fmt::Debug for GmailDeliveryService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GmailDeliveryService")
            .field("transport", &"fixed-mail-egress")
            .finish()
    }
}

impl GmailDeliveryService {
    /// Builds the only production service: fixed OAuth and Gmail endpoints
    /// through the dedicated mail-egress proxy.
    pub fn through_mail_egress() -> Result<Self, GmailDeliveryBuildError> {
        let oauth =
            GmailOAuthClient::through_mail_egress().map_err(GmailDeliveryBuildError::OAuth)?;
        let messages =
            GmailClient::through_mail_egress().map_err(GmailDeliveryBuildError::Gmail)?;
        Ok(Self {
            oauth: Arc::new(oauth),
            messages: Arc::new(messages),
        })
    }

    #[cfg(test)]
    fn for_test(oauth: Arc<dyn OAuthProvider>, messages: Arc<dyn MessageProvider>) -> Self {
        Self { oauth, messages }
    }

    #[cfg(test)]
    pub(super) fn for_test_endpoints(token_url: String, send_url: String) -> Self {
        Self {
            oauth: Arc::new(GmailOAuthClient::for_test(token_url)),
            messages: Arc::new(GmailClient::for_test(send_url)),
        }
    }

    /// Resolves, validates and sends one claimed report exactly once.
    pub async fn deliver(
        &self,
        routing: &MailRouting,
        credentials: &GmailOAuthCredentials,
        claim: &ClaimedDelivery,
        bundle: StoredReportBundle,
    ) -> Result<GmailSendReceipt, GmailDeliveryError> {
        let route = routing
            .resolve(&claim.recipient_id)
            .map_err(|_| GmailDeliveryError::Routing)?;
        let email = build_report_email(route.sender(), route.recipient(), claim, bundle)
            .map_err(|_| GmailDeliveryError::Message)?;
        let token = self
            .oauth
            .refresh(credentials)
            .await
            .map_err(map_oauth_error)?;
        self.messages
            .send(token.as_str(), &email)
            .await
            .map_err(map_send_error)
    }
}

fn map_oauth_error(error: GmailOAuthError) -> GmailDeliveryError {
    match error {
        GmailOAuthError::Rejected => GmailDeliveryError::Authentication,
        GmailOAuthError::RateLimited => GmailDeliveryError::OAuthRateLimited,
        GmailOAuthError::Unavailable => GmailDeliveryError::OAuthUnavailable,
        GmailOAuthError::InvalidResponse => GmailDeliveryError::OAuthInvalidResponse,
    }
}

fn map_send_error(error: GmailSendError) -> GmailDeliveryError {
    match error {
        GmailSendError::InvalidCredential | GmailSendError::Authentication => {
            GmailDeliveryError::Authentication
        }
        GmailSendError::RateLimited => GmailDeliveryError::ProviderRateLimited,
        GmailSendError::Rejected => GmailDeliveryError::ProviderRejected,
        GmailSendError::Ambiguous => GmailDeliveryError::Ambiguous,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        fs,
        future::IntoFuture,
        path::{Path, PathBuf},
        sync::{
            Mutex,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
    };

    use axum::{Router, http::StatusCode, routing::post};
    use chrono::{NaiveDate, TimeZone, Utc};
    use serde_json::json;
    use tokio::{net::TcpListener, task::JoinHandle};

    use crate::{
        config::AccessRegistry,
        reporting::{ReportKey, ReportKind, outbox::ArtifactIdentity, policy::DailyReportPolicy},
    };

    use super::*;

    static NEXT_CREDENTIAL_DIRECTORY: AtomicU64 = AtomicU64::new(1);

    struct FakeOAuth {
        results: Mutex<VecDeque<Result<GmailAccessToken, GmailOAuthError>>>,
        calls: AtomicUsize,
    }

    impl OAuthProvider for FakeOAuth {
        fn refresh<'a>(&'a self, _credentials: &'a GmailOAuthCredentials) -> OAuthFuture<'a> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::Relaxed);
                self.results.lock().unwrap().pop_front().unwrap()
            })
        }
    }

    struct FakeMessages {
        results: Mutex<VecDeque<Result<GmailSendReceipt, GmailSendError>>>,
        calls: AtomicUsize,
        addresses: Mutex<Vec<(String, String)>>,
    }

    impl MessageProvider for FakeMessages {
        fn send<'a>(&'a self, access_token: &'a str, email: &'a ReportEmail) -> SendFuture<'a> {
            Box::pin(async move {
                assert_eq!(access_token, "access-token");
                self.calls.fetch_add(1, Ordering::Relaxed);
                self.addresses
                    .lock()
                    .unwrap()
                    .push((email.sender().to_owned(), email.recipient().to_owned()));
                self.results.lock().unwrap().pop_front().unwrap()
            })
        }
    }

    fn oauth(result: Result<GmailAccessToken, GmailOAuthError>) -> Arc<FakeOAuth> {
        Arc::new(FakeOAuth {
            results: Mutex::new(VecDeque::from([result])),
            calls: AtomicUsize::new(0),
        })
    }

    fn messages(result: Result<GmailSendReceipt, GmailSendError>) -> Arc<FakeMessages> {
        Arc::new(FakeMessages {
            results: Mutex::new(VecDeque::from([result])),
            calls: AtomicUsize::new(0),
            addresses: Mutex::new(Vec::new()),
        })
    }

    fn policy() -> DailyReportPolicy {
        let registry: AccessRegistry = serde_json::from_value(json!({
            "version":1,
            "actors":[{"id":"diana","name":"Diana","role":"manager","oidc":{"username":"diana"}}],
            "accounts":[{"id":"ozon","organization":"Ozon","marketplace":"ozon","seller_client_id":"1","manager_id":"diana","ozon":{"store_id":"1","client_id_env":"OZON_ID","api_key_env":"OZON_KEY"}}]
        }))
        .unwrap();
        DailyReportPolicy::from_slice(
            br#"{"version":1,"enabled":false,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"owner","email_env":"OWNER_EMAIL","managers":[{"actor_id":"diana","account_ids":["ozon"]}]}]}"#,
            &registry,
        )
        .unwrap()
    }

    fn routing() -> MailRouting {
        MailRouting::from_slice(
            br#"{"version":1,"routes":[{"name":"SENDER","address":"reports@example.test"},{"name":"OWNER_EMAIL","address":"owner@example.test"}]}"#,
            &policy(),
        )
        .unwrap()
    }

    fn credentials() -> (PathBuf, GmailOAuthCredentials) {
        let directory = std::env::temp_dir().join(format!(
            "mcp-ozon-delivery-oauth-{}-{}",
            std::process::id(),
            NEXT_CREDENTIAL_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        set_mode(&directory, 0o700);
        for (name, value) in [
            ("client_id", "client-id"),
            ("client_secret", "client-secret"),
            ("refresh_token", "refresh-token"),
        ] {
            let path = directory.join(name);
            fs::write(&path, value).unwrap();
            set_mode(&path, 0o600);
        }
        let credentials = GmailOAuthCredentials::load(&directory).unwrap();
        (directory, credentials)
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt as _;

        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    #[cfg(not(unix))]
    fn set_mode(_path: &Path, _mode: u32) {}

    fn claim(recipient_id: &str) -> ClaimedDelivery {
        ClaimedDelivery {
            batch_id: 1,
            recipient_id: recipient_id.to_owned(),
            report_version: 1,
            attempt_no: 1,
            artifact: ArtifactIdentity {
                object_key: "daily-reports/2026/08/19/owner/v1/morning.xlsx".to_owned(),
                sha256: "a".repeat(64),
                html_sha256: "b".repeat(64),
            },
            covered_keys: vec![ReportKey {
                local_date: NaiveDate::from_ymd_opt(2026, 8, 19).unwrap(),
                kind: ReportKind::Morning,
                recipient_id: recipient_id.to_owned(),
                report_version: 1,
            }],
            deadline_at: Utc.with_ymd_and_hms(2026, 8, 19, 9, 0, 0).unwrap(),
        }
    }

    fn bundle() -> StoredReportBundle {
        StoredReportBundle {
            html: "<html>report</html>".to_owned(),
            xlsx: vec![1, 2, 3],
        }
    }

    async fn response_server(
        path: &'static str,
        body: &'static str,
    ) -> (String, JoinHandle<std::io::Result<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(path, post(move || async move { (StatusCode::OK, body) }));
        let task = tokio::spawn(axum::serve(listener, app).into_future());
        (format!("http://{address}{path}"), task)
    }

    #[tokio::test]
    async fn one_route_refresh_and_send_produce_one_receipt_without_leaking_addresses() {
        let oauth = oauth(Ok(GmailAccessToken::for_test("access-token")));
        let messages = messages(Ok(GmailSendReceipt {
            provider_message_id: "message-1".to_owned(),
        }));
        let service = GmailDeliveryService::for_test(oauth.clone(), messages.clone());
        let (directory, credentials) = credentials();
        let receipt = service
            .deliver(&routing(), &credentials, &claim("owner"), bundle())
            .await
            .unwrap();
        assert_eq!(receipt.provider_message_id, "message-1");
        assert_eq!(oauth.calls.load(Ordering::Relaxed), 1);
        assert_eq!(messages.calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            messages.addresses.lock().unwrap().as_slice(),
            &[(
                "reports@example.test".to_owned(),
                "owner@example.test".to_owned()
            )]
        );
        assert_eq!(
            format!("{service:?}"),
            "GmailDeliveryService { transport: \"fixed-mail-egress\" }"
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn concrete_oauth_and_message_adapters_complete_one_local_wire_delivery() {
        let (token_url, token_task) = response_server(
            "/token",
            r#"{"access_token":"access-token","expires_in":3600,"token_type":"Bearer","scope":"https://www.googleapis.com/auth/gmail.send"}"#,
        )
        .await;
        let (send_url, send_task) = response_server(
            "/gmail/v1/users/me/messages/send",
            r#"{"id":"local-message-1"}"#,
        )
        .await;
        let service = GmailDeliveryService {
            oauth: Arc::new(GmailOAuthClient::for_test(token_url)),
            messages: Arc::new(GmailClient::for_test(send_url)),
        };
        let (directory, credentials) = credentials();
        let receipt = service
            .deliver(&routing(), &credentials, &claim("owner"), bundle())
            .await
            .unwrap();
        assert_eq!(receipt.provider_message_id, "local-message-1");
        token_task.abort();
        send_task.abort();
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn routing_and_message_validation_stop_before_oauth_or_send() {
        for (claim, bundle, expected) in [
            (claim("foreign"), bundle(), GmailDeliveryError::Routing),
            (
                claim("owner"),
                StoredReportBundle {
                    html: String::new(),
                    xlsx: vec![1],
                },
                GmailDeliveryError::Message,
            ),
        ] {
            let oauth = oauth(Ok(GmailAccessToken::for_test("access-token")));
            let messages = messages(Ok(GmailSendReceipt {
                provider_message_id: "unused".to_owned(),
            }));
            let service = GmailDeliveryService::for_test(oauth.clone(), messages.clone());
            let (directory, credentials) = credentials();
            assert_eq!(
                service
                    .deliver(&routing(), &credentials, &claim, bundle)
                    .await,
                Err(expected)
            );
            assert_eq!(oauth.calls.load(Ordering::Relaxed), 0);
            assert_eq!(messages.calls.load(Ordering::Relaxed), 0);
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test]
    async fn oauth_failures_are_exact_and_never_start_a_send() {
        for (source, expected, retry_safe) in [
            (
                GmailOAuthError::Rejected,
                GmailDeliveryError::Authentication,
                false,
            ),
            (
                GmailOAuthError::RateLimited,
                GmailDeliveryError::OAuthRateLimited,
                true,
            ),
            (
                GmailOAuthError::Unavailable,
                GmailDeliveryError::OAuthUnavailable,
                true,
            ),
            (
                GmailOAuthError::InvalidResponse,
                GmailDeliveryError::OAuthInvalidResponse,
                true,
            ),
        ] {
            let oauth = oauth(Err(source));
            let messages = messages(Ok(GmailSendReceipt {
                provider_message_id: "unused".to_owned(),
            }));
            let service = GmailDeliveryService::for_test(oauth, messages.clone());
            let (directory, credentials) = credentials();
            let error = service
                .deliver(&routing(), &credentials, &claim("owner"), bundle())
                .await
                .unwrap_err();
            assert_eq!(error, expected);
            assert_eq!(error.retry_safe(), retry_safe);
            assert!(!error.is_ambiguous());
            assert_eq!(messages.calls.load(Ordering::Relaxed), 0);
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test]
    async fn provider_failures_distinguish_retryable_permanent_and_ambiguous() {
        for (source, expected, retry_safe, ambiguous) in [
            (
                GmailSendError::InvalidCredential,
                GmailDeliveryError::Authentication,
                false,
                false,
            ),
            (
                GmailSendError::Authentication,
                GmailDeliveryError::Authentication,
                false,
                false,
            ),
            (
                GmailSendError::RateLimited,
                GmailDeliveryError::ProviderRateLimited,
                true,
                false,
            ),
            (
                GmailSendError::Rejected,
                GmailDeliveryError::ProviderRejected,
                false,
                false,
            ),
            (
                GmailSendError::Ambiguous,
                GmailDeliveryError::Ambiguous,
                false,
                true,
            ),
        ] {
            let oauth = oauth(Ok(GmailAccessToken::for_test("access-token")));
            let messages = messages(Err(source));
            let service = GmailDeliveryService::for_test(oauth, messages.clone());
            let (directory, credentials) = credentials();
            let error = service
                .deliver(&routing(), &credentials, &claim("owner"), bundle())
                .await
                .unwrap_err();
            assert_eq!(error, expected);
            assert_eq!(error.retry_safe(), retry_safe);
            assert_eq!(error.is_ambiguous(), ambiguous);
            assert_eq!(messages.calls.load(Ordering::Relaxed), 1);
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn production_service_has_only_fixed_mail_egress_transports() {
        GmailDeliveryService::through_mail_egress().unwrap();
    }
}
