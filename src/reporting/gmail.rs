//! Bounded Gmail API transport for daily-report delivery.
//!
//! The production constructor always uses the fixed Gmail endpoint through a
//! fixed, separately hardened mail-egress proxy. It never reads credentials,
//! follows redirects, or inherits ambient proxy variables. OAuth refresh and
//! outbox state transitions remain outside this module.

use std::time::Duration;

use reqwest::{
    Client, Proxy, StatusCode,
    header::{AUTHORIZATION, HeaderValue},
    redirect::Policy,
};
use serde::Serialize;

use super::mail::ReportEmail;

const GMAIL_SEND_URL: &str = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send";
const MAIL_EGRESS_PROXY_URL: &str = "http://mail-egress:3129";
const MAX_GMAIL_RESPONSE_BYTES: usize = 4 * 1024;
const MAX_ACCESS_TOKEN_BYTES: usize = 4 * 1024;
const SEND_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
pub enum GmailClientBuildError {
    #[error("fixed Gmail transport configuration is invalid")]
    InvalidTransport(#[source] reqwest::Error),
}

/// Payload-free delivery classification. Provider response bodies and OAuth
/// credentials are intentionally never included in errors or logs.
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum GmailSendError {
    #[error("Gmail OAuth credential is invalid")]
    InvalidCredential,
    #[error("Gmail rejected the OAuth credential")]
    Authentication,
    #[error("Gmail rate limit prevented delivery")]
    RateLimited,
    #[error("Gmail permanently rejected the message")]
    Rejected,
    #[error("Gmail delivery outcome is ambiguous")]
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GmailSendReceipt {
    pub provider_message_id: String,
}

#[derive(Debug, Clone)]
pub struct GmailClient {
    http: Client,
    send_url: String,
}

#[derive(Serialize)]
struct GmailSendRequest<'a> {
    raw: &'a str,
}

impl GmailClient {
    /// Builds the only production transport: Gmail through the dedicated
    /// mail-egress proxy. There is intentionally no public arbitrary endpoint
    /// or direct-Internet constructor.
    pub fn through_mail_egress() -> Result<Self, GmailClientBuildError> {
        Self::build(GMAIL_SEND_URL, Some(MAIL_EGRESS_PROXY_URL))
    }

    fn build(send_url: &str, proxy_url: Option<&str>) -> Result<Self, GmailClientBuildError> {
        let http = Client::builder()
            .timeout(SEND_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .redirect(Policy::none())
            .no_proxy()
            .user_agent("mcp-ozon-report-worker/0.2")
            .pool_max_idle_per_host(1);
        let http = match proxy_url {
            Some(proxy_url) => http
                .proxy(Proxy::https(proxy_url).map_err(GmailClientBuildError::InvalidTransport)?),
            None => http,
        }
        .build()
        .map_err(GmailClientBuildError::InvalidTransport)?;
        Ok(Self {
            http,
            send_url: send_url.to_owned(),
        })
    }

    #[cfg(test)]
    fn for_test(send_url: String) -> Self {
        Self::build(&send_url, None).expect("local test Gmail transport is valid")
    }

    /// Sends exactly once. Transport failures, 5xx responses, redirects, and
    /// malformed success receipts are ambiguous and must never be retried
    /// automatically by the caller.
    pub async fn send(
        &self,
        access_token: &str,
        email: &ReportEmail,
    ) -> Result<GmailSendReceipt, GmailSendError> {
        let authorization = bearer_authorization(access_token)?;
        let raw = email.gmail_raw();
        let response = self
            .http
            .post(&self.send_url)
            .header(AUTHORIZATION, authorization)
            .json(&GmailSendRequest { raw: &raw })
            .send()
            .await
            .map_err(|_| GmailSendError::Ambiguous)?;
        match response.status() {
            StatusCode::OK => parse_success_response(response).await,
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(GmailSendError::Authentication),
            StatusCode::TOO_MANY_REQUESTS => Err(GmailSendError::RateLimited),
            status if status.is_client_error() => Err(GmailSendError::Rejected),
            _ => Err(GmailSendError::Ambiguous),
        }
    }
}

fn bearer_authorization(access_token: &str) -> Result<HeaderValue, GmailSendError> {
    if !access_token_is_valid(access_token) {
        return Err(GmailSendError::InvalidCredential);
    }
    let mut value = HeaderValue::from_str(&format!("Bearer {access_token}"))
        .map_err(|_| GmailSendError::InvalidCredential)?;
    value.set_sensitive(true);
    Ok(value)
}

pub(super) fn access_token_is_valid(access_token: &str) -> bool {
    !access_token.is_empty()
        && access_token.len() <= MAX_ACCESS_TOKEN_BYTES
        && !access_token
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
}

async fn parse_success_response(
    mut response: reqwest::Response,
) -> Result<GmailSendReceipt, GmailSendError> {
    let mut body = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or(0)
            .min(MAX_GMAIL_RESPONSE_BYTES as u64) as usize,
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| GmailSendError::Ambiguous)?
    {
        if chunk.len() > MAX_GMAIL_RESPONSE_BYTES.saturating_sub(body.len()) {
            return Err(GmailSendError::Ambiguous);
        }
        body.extend_from_slice(&chunk);
    }
    let value: serde_json::Value =
        serde_json::from_slice(&body).map_err(|_| GmailSendError::Ambiguous)?;
    let message_id = value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| valid_message_id(value))
        .ok_or(GmailSendError::Ambiguous)?;
    Ok(GmailSendReceipt {
        provider_message_id: message_id.to_owned(),
    })
}

fn valid_message_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[cfg(test)]
mod tests {
    use std::{
        future::IntoFuture,
        sync::{Arc, Mutex},
    };

    use axum::{
        Router,
        body::Bytes,
        extract::State,
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::post,
    };
    use chrono::NaiveDate;
    use tokio::{net::TcpListener, task::JoinHandle};

    use super::*;
    use crate::reporting::{
        ReportKey, ReportKind, artifact_store::StoredReportBundle, mail::build_report_email,
        outbox::ArtifactIdentity, postgres_outbox::ClaimedDelivery,
    };

    type SeenRequests = Arc<Mutex<Vec<(HeaderMap, Vec<u8>)>>>;

    #[derive(Clone)]
    struct MockState {
        status: StatusCode,
        body: Vec<u8>,
        seen: SeenRequests,
    }

    async fn gmail_handler(
        State(state): State<MockState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        state.seen.lock().unwrap().push((headers, body.to_vec()));
        (state.status, state.body)
    }

    async fn server(
        status: StatusCode,
        body: impl Into<Vec<u8>>,
    ) -> (String, SeenRequests, JoinHandle<std::io::Result<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = MockState {
            status,
            body: body.into(),
            seen: seen.clone(),
        };
        let app = Router::new()
            .route("/gmail/v1/users/me/messages/send", post(gmail_handler))
            .with_state(state);
        let task = tokio::spawn(axum::serve(listener, app).into_future());
        (
            format!("http://{address}/gmail/v1/users/me/messages/send"),
            seen,
            task,
        )
    }

    fn email() -> ReportEmail {
        let claim = ClaimedDelivery {
            batch_id: 7,
            recipient_id: "pilot_owner".to_owned(),
            report_version: 1,
            attempt_no: 1,
            artifact: ArtifactIdentity {
                object_key: "daily-reports/example.xlsx".to_owned(),
                sha256: "a".repeat(64),
                html_sha256: "b".repeat(64),
            },
            covered_keys: vec![ReportKey {
                local_date: NaiveDate::from_ymd_opt(2026, 8, 18).unwrap(),
                kind: ReportKind::Morning,
                recipient_id: "pilot_owner".to_owned(),
                report_version: 1,
            }],
        };
        build_report_email(
            "reports@example.test",
            "owner@example.test",
            &claim,
            StoredReportBundle {
                html: "<html>report</html>".to_owned(),
                xlsx: vec![1, 2, 3],
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn sends_one_bounded_gmail_request_and_returns_provider_id() {
        let (url, seen, task) =
            server(StatusCode::OK, br#"{"id":"18f-message_1","threadId":"t"}"#).await;
        let receipt = GmailClient::for_test(url)
            .send("access-token", &email())
            .await
            .unwrap();
        assert_eq!(receipt.provider_message_id, "18f-message_1");
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0[AUTHORIZATION], "Bearer access-token");
        let request: serde_json::Value = serde_json::from_slice(&seen[0].1).unwrap();
        assert!(request["raw"].as_str().unwrap().len() > 100);
        assert_eq!(request.as_object().unwrap().len(), 1);
        task.abort();
    }

    #[tokio::test]
    async fn provider_statuses_are_classified_without_response_payloads() {
        for (status, expected) in [
            (StatusCode::UNAUTHORIZED, GmailSendError::Authentication),
            (StatusCode::FORBIDDEN, GmailSendError::Authentication),
            (StatusCode::TOO_MANY_REQUESTS, GmailSendError::RateLimited),
            (StatusCode::BAD_REQUEST, GmailSendError::Rejected),
            (StatusCode::FOUND, GmailSendError::Ambiguous),
            (StatusCode::INTERNAL_SERVER_ERROR, GmailSendError::Ambiguous),
        ] {
            let (url, _, task) = server(status, b"provider-secret".to_vec()).await;
            let error = GmailClient::for_test(url)
                .send("access-token", &email())
                .await
                .unwrap_err();
            assert_eq!(error, expected);
            assert!(!error.to_string().contains("provider-secret"));
            task.abort();
        }
    }

    #[tokio::test]
    async fn malformed_or_oversized_success_is_ambiguous_and_never_retryable() {
        for body in [
            b"not-json".to_vec(),
            br#"{"threadId":"missing-id"}"#.to_vec(),
            br#"{"id":"bad id"}"#.to_vec(),
            format!(r#"{{"id":"{}"}}"#, "x".repeat(257)).into_bytes(),
            vec![b'x'; MAX_GMAIL_RESPONSE_BYTES + 1],
        ] {
            let (url, _, task) = server(StatusCode::OK, body).await;
            assert_eq!(
                GmailClient::for_test(url)
                    .send("access-token", &email())
                    .await,
                Err(GmailSendError::Ambiguous)
            );
            task.abort();
        }
    }

    #[tokio::test]
    async fn invalid_tokens_and_transport_failure_do_not_leak_credentials() {
        for token in ["", "has space", "line\nbreak"] {
            let error = GmailClient::for_test("http://127.0.0.1:1/send".to_owned())
                .send(token, &email())
                .await
                .unwrap_err();
            assert_eq!(error, GmailSendError::InvalidCredential);
            if !token.is_empty() {
                assert!(!error.to_string().contains(token));
            }
        }
        let oversized = "x".repeat(MAX_ACCESS_TOKEN_BYTES + 1);
        assert_eq!(
            GmailClient::for_test("http://127.0.0.1:1/send".to_owned())
                .send(&oversized, &email())
                .await,
            Err(GmailSendError::InvalidCredential)
        );
        assert_eq!(
            GmailClient::for_test("http://127.0.0.1:1/send".to_owned())
                .send("access-token", &email())
                .await,
            Err(GmailSendError::Ambiguous)
        );
    }

    #[test]
    fn production_client_has_only_the_fixed_mail_egress_route() {
        GmailClient::through_mail_egress().unwrap();
        assert!(valid_message_id("a-B_9.id"));
        for value in ["", "bad id", "bad/id", &"x".repeat(257)] {
            assert!(!valid_message_id(value));
        }
    }
}
