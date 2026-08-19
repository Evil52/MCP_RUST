//! Credential-isolated Google OAuth refresh for the Gmail report sender.
//!
//! Production accepts exactly three private files and one fixed token endpoint
//! through the dedicated mail-egress proxy. It never accepts a password,
//! authorization endpoint, scope, token endpoint, or proxy from runtime input.

use std::{
    collections::BTreeSet,
    fmt, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use reqwest::{Client, Proxy, StatusCode, redirect::Policy};

use super::gmail::access_token_is_valid;

const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const MAIL_EGRESS_PROXY_URL: &str = "http://mail-egress:3129";
const GMAIL_SEND_SCOPE: &str = "https://www.googleapis.com/auth/gmail.send";
const CLIENT_ID_FILE: &str = "client_id";
const CLIENT_SECRET_FILE: &str = "client_secret";
const REFRESH_TOKEN_FILE: &str = "refresh_token";
const MAX_CLIENT_ID_BYTES: usize = 512;
const MAX_CLIENT_SECRET_BYTES: usize = 1024;
const MAX_REFRESH_TOKEN_BYTES: usize = 4096;
const MAX_TOKEN_RESPONSE_BYTES: usize = 8 * 1024;
const MIN_ACCESS_TOKEN_SECONDS: u64 = 60;
const MAX_ACCESS_TOKEN_SECONDS: u64 = 2 * 60 * 60;
const TOKEN_TIMEOUT: Duration = Duration::from_secs(20);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum GmailCredentialError {
    #[error("Gmail OAuth credential directory is invalid")]
    InvalidDirectory,
    #[error("Gmail OAuth credential file is invalid")]
    InvalidFile,
    #[error("Gmail OAuth credential value is invalid")]
    InvalidValue,
}

#[derive(Clone, PartialEq, Eq)]
pub struct GmailOAuthCredentials {
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

impl fmt::Debug for GmailOAuthCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GmailOAuthCredentials")
            .field("client_id", &"<redacted>")
            .field("client_secret", &"<redacted>")
            .field("refresh_token", &"<redacted>")
            .finish()
    }
}

impl GmailOAuthCredentials {
    pub fn load(directory: impl AsRef<Path>) -> Result<Self, GmailCredentialError> {
        let directory = directory.as_ref();
        let metadata =
            fs::symlink_metadata(directory).map_err(|_| GmailCredentialError::InvalidDirectory)?;
        if !metadata.file_type().is_dir() || !private_permissions(&metadata) {
            return Err(GmailCredentialError::InvalidDirectory);
        }
        let expected = BTreeSet::from([
            CLIENT_ID_FILE.to_owned(),
            CLIENT_SECRET_FILE.to_owned(),
            REFRESH_TOKEN_FILE.to_owned(),
        ]);
        let actual = fs::read_dir(directory)
            .map_err(|_| GmailCredentialError::InvalidDirectory)?
            .map(|entry| {
                entry
                    .map_err(|_| GmailCredentialError::InvalidDirectory)?
                    .file_name()
                    .into_string()
                    .map_err(|_| GmailCredentialError::InvalidFile)
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        if actual != expected {
            return Err(GmailCredentialError::InvalidDirectory);
        }
        Ok(Self {
            client_id: read_private_value(directory.join(CLIENT_ID_FILE), MAX_CLIENT_ID_BYTES)?,
            client_secret: read_private_value(
                directory.join(CLIENT_SECRET_FILE),
                MAX_CLIENT_SECRET_BYTES,
            )?,
            refresh_token: read_private_value(
                directory.join(REFRESH_TOKEN_FILE),
                MAX_REFRESH_TOKEN_BYTES,
            )?,
        })
    }
}

fn read_private_value(path: PathBuf, limit: usize) -> Result<String, GmailCredentialError> {
    let metadata = fs::symlink_metadata(&path).map_err(|_| GmailCredentialError::InvalidFile)?;
    if !metadata.file_type().is_file()
        || !private_permissions(&metadata)
        || metadata.len() == 0
        || metadata.len() > (limit + 2) as u64
    {
        return Err(GmailCredentialError::InvalidFile);
    }
    let bytes = fs::read(path).map_err(|_| GmailCredentialError::InvalidFile)?;
    let value = std::str::from_utf8(&bytes).map_err(|_| GmailCredentialError::InvalidValue)?;
    let value = value
        .strip_suffix("\r\n")
        .or_else(|| value.strip_suffix('\n'))
        .unwrap_or(value);
    if value.is_empty()
        || value.len() > limit
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(GmailCredentialError::InvalidValue);
    }
    Ok(value.to_owned())
}

#[cfg(unix)]
fn private_permissions(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    metadata.permissions().mode() & 0o077 == 0
}

#[cfg(not(unix))]
fn private_permissions(_metadata: &fs::Metadata) -> bool {
    true
}

#[derive(Debug, thiserror::Error)]
pub enum GmailOAuthClientBuildError {
    #[error("fixed Google OAuth transport configuration is invalid")]
    InvalidTransport(#[source] reqwest::Error),
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum GmailOAuthError {
    #[error("Google rejected the Gmail OAuth credentials")]
    Rejected,
    #[error("Google OAuth rate limit prevented refresh")]
    RateLimited,
    #[error("Google OAuth is temporarily unavailable")]
    Unavailable,
    #[error("Google OAuth returned an invalid token response")]
    InvalidResponse,
}

#[derive(Clone, PartialEq, Eq)]
pub struct GmailAccessToken {
    value: String,
    expires_in_seconds: u64,
}

impl fmt::Debug for GmailAccessToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GmailAccessToken")
            .field("value", &"<redacted>")
            .field("expires_in_seconds", &self.expires_in_seconds)
            .finish()
    }
}

impl GmailAccessToken {
    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn expires_in_seconds(&self) -> u64 {
        self.expires_in_seconds
    }

    #[cfg(test)]
    pub(super) fn for_test(value: &str) -> Self {
        assert!(access_token_is_valid(value));
        Self {
            value: value.to_owned(),
            expires_in_seconds: MIN_ACCESS_TOKEN_SECONDS,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GmailOAuthClient {
    http: Client,
    token_url: String,
}

impl GmailOAuthClient {
    pub fn through_mail_egress() -> Result<Self, GmailOAuthClientBuildError> {
        Self::build(GOOGLE_TOKEN_URL, Some(MAIL_EGRESS_PROXY_URL))
    }

    fn build(token_url: &str, proxy_url: Option<&str>) -> Result<Self, GmailOAuthClientBuildError> {
        let http = Client::builder()
            .timeout(TOKEN_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .redirect(Policy::none())
            .no_proxy()
            .user_agent("mcp-ozon-report-worker/0.2")
            .pool_max_idle_per_host(1);
        let http = match proxy_url {
            Some(proxy_url) => http.proxy(
                Proxy::https(proxy_url).map_err(GmailOAuthClientBuildError::InvalidTransport)?,
            ),
            None => http,
        }
        .build()
        .map_err(GmailOAuthClientBuildError::InvalidTransport)?;
        Ok(Self {
            http,
            token_url: token_url.to_owned(),
        })
    }

    #[cfg(test)]
    pub(super) fn for_test(token_url: String) -> Self {
        Self::build(&token_url, None).expect("local test OAuth transport is valid")
    }

    pub async fn refresh(
        &self,
        credentials: &GmailOAuthCredentials,
    ) -> Result<GmailAccessToken, GmailOAuthError> {
        let response = self
            .http
            .post(&self.token_url)
            .form(&[
                ("client_id", credentials.client_id.as_str()),
                ("client_secret", credentials.client_secret.as_str()),
                ("refresh_token", credentials.refresh_token.as_str()),
                ("grant_type", "refresh_token"),
            ])
            .send()
            .await
            .map_err(|_| GmailOAuthError::Unavailable)?;
        match response.status() {
            StatusCode::OK => parse_token_response(response).await,
            StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                Err(GmailOAuthError::Rejected)
            }
            StatusCode::TOO_MANY_REQUESTS => Err(GmailOAuthError::RateLimited),
            _ => Err(GmailOAuthError::Unavailable),
        }
    }
}

async fn parse_token_response(
    mut response: reqwest::Response,
) -> Result<GmailAccessToken, GmailOAuthError> {
    let mut body = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or(0)
            .min(MAX_TOKEN_RESPONSE_BYTES as u64) as usize,
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| GmailOAuthError::InvalidResponse)?
    {
        if chunk.len() > MAX_TOKEN_RESPONSE_BYTES.saturating_sub(body.len()) {
            return Err(GmailOAuthError::InvalidResponse);
        }
        body.extend_from_slice(&chunk);
    }
    let value: serde_json::Value =
        serde_json::from_slice(&body).map_err(|_| GmailOAuthError::InvalidResponse)?;
    let access_token = value
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .filter(|value| access_token_is_valid(value))
        .ok_or(GmailOAuthError::InvalidResponse)?;
    let token_type = value
        .get("token_type")
        .and_then(serde_json::Value::as_str)
        .ok_or(GmailOAuthError::InvalidResponse)?;
    let expires_in_seconds = value
        .get("expires_in")
        .and_then(serde_json::Value::as_u64)
        .filter(|seconds| (MIN_ACCESS_TOKEN_SECONDS..=MAX_ACCESS_TOKEN_SECONDS).contains(seconds))
        .ok_or(GmailOAuthError::InvalidResponse)?;
    if token_type != "Bearer" {
        return Err(GmailOAuthError::InvalidResponse);
    }
    if let Some(scope) = value.get("scope") {
        let scope = scope.as_str().ok_or(GmailOAuthError::InvalidResponse)?;
        if scope.split_ascii_whitespace().collect::<Vec<_>>() != [GMAIL_SEND_SCOPE] {
            return Err(GmailOAuthError::InvalidResponse);
        }
    }
    Ok(GmailAccessToken {
        value: access_token.to_owned(),
        expires_in_seconds,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        future::IntoFuture,
        sync::atomic::{AtomicU64, Ordering},
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
    use tokio::{net::TcpListener, task::JoinHandle};

    use super::*;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(1);
    type SeenRequests = Arc<Mutex<Vec<(HeaderMap, Vec<u8>)>>>;

    fn credential_directory() -> PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "mcp-ozon-gmail-oauth-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        set_mode(&directory, 0o700);
        for (name, value) in [
            (CLIENT_ID_FILE, "client+id.apps.googleusercontent.com\n"),
            (CLIENT_SECRET_FILE, "client/secret\r\n"),
            (REFRESH_TOKEN_FILE, "refresh_token-value\n"),
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

    #[derive(Clone)]
    struct MockState {
        status: StatusCode,
        body: Vec<u8>,
        seen: SeenRequests,
    }

    async fn token_handler(
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
        let app = Router::new()
            .route("/token", post(token_handler))
            .with_state(MockState {
                status,
                body: body.into(),
                seen: seen.clone(),
            });
        let task = tokio::spawn(axum::serve(listener, app).into_future());
        (format!("http://{address}/token"), seen, task)
    }

    #[test]
    fn credentials_load_from_exact_private_files_and_debug_is_redacted() {
        let directory = credential_directory();
        let credentials = GmailOAuthCredentials::load(&directory).unwrap();
        assert_eq!(
            credentials.client_id,
            "client+id.apps.googleusercontent.com"
        );
        assert_eq!(credentials.client_secret, "client/secret");
        assert_eq!(credentials.refresh_token, "refresh_token-value");
        let debug = format!("{credentials:?}");
        for secret in ["client+id", "client/secret", "refresh_token-value"] {
            assert!(!debug.contains(secret));
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn credential_directory_and_values_fail_closed() {
        enum Mutation {
            ExtraFile,
            MissingFile,
            EmptyValue,
            Whitespace,
            NonUtf8,
            Oversized,
            PublicDirectory,
            PublicFile,
        }

        let missing = std::env::temp_dir().join("mcp-ozon-gmail-oauth-missing");
        assert_eq!(
            GmailOAuthCredentials::load(missing),
            Err(GmailCredentialError::InvalidDirectory)
        );
        for mutation in [
            Mutation::ExtraFile,
            Mutation::MissingFile,
            Mutation::EmptyValue,
            Mutation::Whitespace,
            Mutation::NonUtf8,
            Mutation::Oversized,
            Mutation::PublicDirectory,
            Mutation::PublicFile,
        ] {
            let directory = credential_directory();
            match mutation {
                Mutation::ExtraFile => fs::write(directory.join("extra"), "x").unwrap(),
                Mutation::MissingFile => fs::remove_file(directory.join(CLIENT_ID_FILE)).unwrap(),
                Mutation::EmptyValue => fs::write(directory.join(CLIENT_SECRET_FILE), "").unwrap(),
                Mutation::Whitespace => {
                    fs::write(directory.join(REFRESH_TOKEN_FILE), "has space").unwrap()
                }
                Mutation::NonUtf8 => fs::write(directory.join(CLIENT_ID_FILE), [0xff]).unwrap(),
                Mutation::Oversized => fs::write(
                    directory.join(CLIENT_SECRET_FILE),
                    "x".repeat(MAX_CLIENT_SECRET_BYTES + 1),
                )
                .unwrap(),
                Mutation::PublicDirectory => {
                    set_mode(&directory, 0o755);
                }
                Mutation::PublicFile => {
                    set_mode(&directory.join(REFRESH_TOKEN_FILE), 0o644);
                }
            }
            assert!(GmailOAuthCredentials::load(&directory).is_err());
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test]
    async fn refresh_uses_exact_form_and_returns_only_a_bounded_bearer_token() {
        let directory = credential_directory();
        let credentials = GmailOAuthCredentials::load(&directory).unwrap();
        let (url, seen, task) = server(
            StatusCode::OK,
            format!(
                r#"{{"access_token":"short-access-token","expires_in":3599,"token_type":"Bearer","scope":"{GMAIL_SEND_SCOPE}"}}"#
            ),
        )
        .await;
        let token = GmailOAuthClient::for_test(url)
            .refresh(&credentials)
            .await
            .unwrap();
        assert_eq!(token.as_str(), "short-access-token");
        assert_eq!(token.expires_in_seconds(), 3599);
        assert!(!format!("{token:?}").contains("short-access-token"));
        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 1);
            assert_eq!(
                seen[0].0[reqwest::header::CONTENT_TYPE],
                "application/x-www-form-urlencoded"
            );
            let body = std::str::from_utf8(&seen[0].1).unwrap();
            assert!(body.contains("client_id=client%2Bid.apps.googleusercontent.com"));
            assert!(body.contains("client_secret=client%2Fsecret"));
            assert!(body.contains("refresh_token=refresh_token-value"));
            assert!(body.contains("grant_type=refresh_token"));
        }
        task.abort();

        let (url, _, task) = server(
            StatusCode::OK,
            br#"{"access_token":"token-without-scope","expires_in":60,"token_type":"Bearer"}"#
                .to_vec(),
        )
        .await;
        assert_eq!(
            GmailOAuthClient::for_test(url)
                .refresh(&credentials)
                .await
                .unwrap()
                .as_str(),
            "token-without-scope"
        );
        task.abort();
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn oauth_statuses_are_payload_free_and_transport_is_bounded() {
        let directory = credential_directory();
        let credentials = GmailOAuthCredentials::load(&directory).unwrap();
        for (status, expected) in [
            (StatusCode::BAD_REQUEST, GmailOAuthError::Rejected),
            (StatusCode::UNAUTHORIZED, GmailOAuthError::Rejected),
            (StatusCode::FORBIDDEN, GmailOAuthError::Rejected),
            (StatusCode::TOO_MANY_REQUESTS, GmailOAuthError::RateLimited),
            (StatusCode::FOUND, GmailOAuthError::Unavailable),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                GmailOAuthError::Unavailable,
            ),
        ] {
            let (url, _, task) = server(status, b"oauth-secret-diagnostic".to_vec()).await;
            let error = GmailOAuthClient::for_test(url)
                .refresh(&credentials)
                .await
                .unwrap_err();
            assert_eq!(error, expected);
            assert!(!error.to_string().contains("oauth-secret-diagnostic"));
            task.abort();
        }
        assert_eq!(
            GmailOAuthClient::for_test("http://127.0.0.1:1/token".to_owned())
                .refresh(&credentials)
                .await,
            Err(GmailOAuthError::Unavailable)
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn malformed_success_responses_are_rejected_without_exposing_tokens() {
        let directory = credential_directory();
        let credentials = GmailOAuthCredentials::load(&directory).unwrap();
        for body in [
            b"not-json".to_vec(),
            br#"{"expires_in":3599,"token_type":"Bearer"}"#.to_vec(),
            br#"{"access_token":"token","expires_in":59,"token_type":"Bearer"}"#.to_vec(),
            br#"{"access_token":"token","expires_in":7201,"token_type":"Bearer"}"#.to_vec(),
            br#"{"access_token":"token","expires_in":3599,"token_type":"bearer"}"#.to_vec(),
            br#"{"access_token":"token","expires_in":3599,"token_type":"Bearer","scope":7}"#.to_vec(),
            br#"{"access_token":"token","expires_in":3599,"token_type":"Bearer","scope":"https://mail.google.com/"}"#.to_vec(),
            vec![b'x'; MAX_TOKEN_RESPONSE_BYTES + 1],
        ] {
            let (url, _, task) = server(StatusCode::OK, body).await;
            assert_eq!(
                GmailOAuthClient::for_test(url)
                    .refresh(&credentials)
                    .await,
                Err(GmailOAuthError::InvalidResponse)
            );
            task.abort();
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn production_oauth_client_has_only_the_fixed_mail_egress_route() {
        GmailOAuthClient::through_mail_egress().unwrap();
    }
}
