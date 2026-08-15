use std::{
    collections::BTreeSet,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use axum::http::HeaderMap;
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    errors::ErrorKind as JwtErrorKind,
    jwk::{AlgorithmParameters, JwkSet},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::config::{AccessRegistry, JwtConfig, RegistrySource};

#[derive(Debug, Clone)]
pub struct AuthenticatedActor {
    pub actor_id: String,
}

#[derive(Debug)]
pub(crate) struct AuthenticatedAccess {
    pub(crate) actor: AuthenticatedActor,
    pub(crate) registry: Arc<AccessRegistry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JwtAuthenticationFailure {
    MissingCredentials,
    InvalidToken,
    ExpiredToken,
    WrongAudience,
    InsufficientScope,
    AccessDenied,
    VerifierUnavailable,
}

impl JwtAuthenticationFailure {
    fn oauth_error(self) -> Option<(&'static str, &'static str)> {
        match self {
            Self::MissingCredentials | Self::AccessDenied | Self::VerifierUnavailable => None,
            Self::InvalidToken => Some(("invalid_token", "The access token is invalid")),
            Self::ExpiredToken => Some(("invalid_token", "The access token has expired")),
            Self::WrongAudience => Some(("invalid_token", "The access token audience is invalid")),
            Self::InsufficientScope => Some((
                "insufficient_scope",
                "The access token lacks a required scope",
            )),
        }
    }

    pub fn public_message(self) -> &'static str {
        match self {
            Self::MissingCredentials => "Требуется авторизация: access token не передан.",
            Self::InvalidToken => "Требуется повторная авторизация: access token недействителен.",
            Self::ExpiredToken => "Требуется повторная авторизация: access token истёк.",
            Self::WrongAudience => {
                "Требуется повторная авторизация: access token выпущен для другого ресурса."
            }
            Self::InsufficientScope => {
                "Требуется повторная авторизация с необходимыми разрешениями."
            }
            Self::AccessDenied => "Доступ для подтверждённой учётной записи не разрешён.",
            Self::VerifierUnavailable => "Сервис проверки access token временно недоступен.",
        }
    }
}

impl fmt::Display for JwtAuthenticationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MissingCredentials => "missing bearer credentials",
            Self::InvalidToken => "invalid access token",
            Self::ExpiredToken => "expired access token",
            Self::WrongAudience => "invalid access token audience",
            Self::InsufficientScope => "insufficient OAuth scope",
            Self::AccessDenied => "authenticated identity is not authorized",
            Self::VerifierUnavailable => "JWT verifier is temporarily unavailable",
        })
    }
}

impl std::error::Error for JwtAuthenticationFailure {}

#[derive(Debug, Clone, Deserialize)]
struct AccessTokenClaims {
    sub: String,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    preferred_username: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    email_verified: Option<bool>,
}

#[derive(Debug)]
struct CachedJwks {
    fetched_at: Instant,
    keys: JwkSet,
}

#[derive(Debug, Default)]
struct JwksCacheState {
    cache: Option<CachedJwks>,
    last_unknown_kid_refresh_at: Option<Instant>,
    last_failed_refresh_at: Option<Instant>,
}

const UNKNOWN_KID_REFRESH_COOLDOWN: Duration = Duration::from_secs(30);
const FAILED_REFRESH_COOLDOWN: Duration = Duration::from_secs(5);
/// Explicitly retained clock-skew allowance for `exp` and `nbf` validation.
///
/// `jsonwebtoken` currently defaults to the same value, but relying on that
/// implicit default would let a dependency update silently change the token
/// acceptance boundary.
const JWT_CLOCK_SKEW_LEEWAY_SECONDS: u64 = 60;
const MAX_JWKS_BODY_BYTES: usize = 1024 * 1024;
const MAX_JWKS_KEYS: usize = 64;
const MAX_JWK_STRING_BYTES: usize = 16 * 1024;

fn jwks_strings_are_bounded(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(value) => value.len() <= MAX_JWK_STRING_BYTES,
        serde_json::Value::Array(values) => values.iter().all(jwks_strings_are_bounded),
        serde_json::Value::Object(values) => values.iter().all(|(name, value)| {
            name.len() <= MAX_JWK_STRING_BYTES && jwks_strings_are_bounded(value)
        }),
        _ => true,
    }
}

fn parse_bounded_jwks(body: &[u8]) -> std::result::Result<JwkSet, JwtAuthenticationFailure> {
    let value = serde_json::from_slice::<serde_json::Value>(body)
        .map_err(|_| JwtAuthenticationFailure::VerifierUnavailable)?;
    let keys = value
        .get("keys")
        .and_then(serde_json::Value::as_array)
        .ok_or(JwtAuthenticationFailure::VerifierUnavailable)?;
    if keys.is_empty() || keys.len() > MAX_JWKS_KEYS || !jwks_strings_are_bounded(&value) {
        return Err(JwtAuthenticationFailure::VerifierUnavailable);
    }
    let jwks = serde_json::from_value::<JwkSet>(value)
        .map_err(|_| JwtAuthenticationFailure::VerifierUnavailable)?;
    if jwks
        .keys
        .iter()
        .any(|jwk| matches!(&jwk.algorithm, AlgorithmParameters::Other(_)))
    {
        return Err(JwtAuthenticationFailure::VerifierUnavailable);
    }
    Ok(jwks)
}

#[derive(Debug, Clone)]
pub struct JwtAuthenticator {
    config: JwtConfig,
    registry: RegistrySource,
    client: reqwest::Client,
    cache: Arc<RwLock<JwksCacheState>>,
    refresh_gate: Arc<Mutex<()>>,
}

impl JwtAuthenticator {
    pub fn new(config: JwtConfig, registry: RegistrySource) -> Result<Self> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            // JWKS is an authentication trust anchor. Fetch it directly so a
            // process-wide HTTP(S)_PROXY cannot observe or rewrite signing keys.
            .no_proxy()
            .build()?;
        Ok(Self {
            config,
            registry,
            client,
            cache: Arc::new(RwLock::new(JwksCacheState::default())),
            refresh_gate: Arc::new(Mutex::new(())),
        })
    }

    pub fn protected_resource_metadata(&self) -> ProtectedResourceMetadata {
        ProtectedResourceMetadata {
            resource: self.config.resource_url.clone(),
            authorization_servers: vec![self.config.issuer.clone()],
            bearer_methods_supported: vec!["header"],
            scopes_supported: self.config.required_scopes.clone(),
        }
    }

    pub fn required_scopes(&self) -> &[String] {
        &self.config.required_scopes
    }

    pub fn resource_metadata_url(&self) -> &str {
        &self.config.resource_metadata_url
    }

    pub fn challenge(&self, failure: &JwtAuthenticationFailure) -> Option<String> {
        let base = format!(
            "Bearer resource_metadata=\"{}\", scope=\"{}\"",
            self.resource_metadata_url(),
            self.required_scopes().join(" ")
        );
        match failure {
            JwtAuthenticationFailure::MissingCredentials => Some(base),
            _ => self.oauth_challenge_with_error(base, *failure),
        }
    }

    fn oauth_challenge_with_error(
        &self,
        base: String,
        failure: JwtAuthenticationFailure,
    ) -> Option<String> {
        let (error, error_description) = failure.oauth_error()?;
        Some(format!(
            "{base}, error=\"{error}\", error_description=\"{error_description}\""
        ))
    }

    fn validate_required_scopes(
        &self,
        scope_claim: Option<&str>,
    ) -> std::result::Result<(), JwtAuthenticationFailure> {
        let Some(scope_claim) = scope_claim else {
            return Err(JwtAuthenticationFailure::InsufficientScope);
        };
        if scope_claim
            .bytes()
            .any(|byte| byte != b' ' && !matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
        {
            return Err(JwtAuthenticationFailure::InsufficientScope);
        }
        let granted = scope_claim
            .split(' ')
            .filter(|scope| !scope.is_empty())
            .collect::<BTreeSet<_>>();
        let missing = self
            .config
            .required_scopes
            .iter()
            .filter(|scope| !granted.contains(scope.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(JwtAuthenticationFailure::InsufficientScope);
        }
        Ok(())
    }

    async fn fetch_jwks(&self) -> std::result::Result<JwkSet, JwtAuthenticationFailure> {
        let mut response = self
            .client
            .get(&self.config.jwks_url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|_| JwtAuthenticationFailure::VerifierUnavailable)?;
        if !response.status().is_success() {
            return Err(JwtAuthenticationFailure::VerifierUnavailable);
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_JWKS_BODY_BYTES as u64)
        {
            return Err(JwtAuthenticationFailure::VerifierUnavailable);
        }

        let mut body = Vec::with_capacity(
            response
                .content_length()
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or(8 * 1024)
                .min(MAX_JWKS_BODY_BYTES),
        );
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| JwtAuthenticationFailure::VerifierUnavailable)?
        {
            if chunk.len() > MAX_JWKS_BODY_BYTES.saturating_sub(body.len()) {
                return Err(JwtAuthenticationFailure::VerifierUnavailable);
            }
            body.extend_from_slice(&chunk);
        }
        parse_bounded_jwks(&body)
    }

    fn cached_decoding_key(
        &self,
        state: &JwksCacheState,
        kid: &str,
    ) -> std::result::Result<Option<DecodingKey>, JwtAuthenticationFailure> {
        let fresh_cache = state
            .cache
            .as_ref()
            .filter(|cache| cache.fetched_at.elapsed() < self.config.jwks_cache_ttl);
        if let Some(jwk) = fresh_cache.and_then(|cache| cache.keys.find(kid)) {
            return DecodingKey::from_jwk(jwk)
                .map(Some)
                .map_err(|_| JwtAuthenticationFailure::VerifierUnavailable);
        }
        if state
            .last_failed_refresh_at
            .is_some_and(|at| at.elapsed() < FAILED_REFRESH_COOLDOWN)
        {
            return Err(JwtAuthenticationFailure::VerifierUnavailable);
        }
        if fresh_cache.is_some()
            && state
                .last_unknown_kid_refresh_at
                .is_some_and(|at| at.elapsed() < UNKNOWN_KID_REFRESH_COOLDOWN)
        {
            return Err(JwtAuthenticationFailure::InvalidToken);
        }
        Ok(None)
    }

    async fn decoding_key(
        &self,
        kid: &str,
    ) -> std::result::Result<DecodingKey, JwtAuthenticationFailure> {
        let cached_key = {
            let state = self.cache.read().await;
            self.cached_decoding_key(&state, kid)?
        };
        if let Some(key) = cached_key {
            return Ok(key);
        }

        // Only one task may fetch JWKS. Every waiter re-checks the cache after
        // acquiring the gate, so concurrent misses are coalesced into one fetch.
        let _refresh_guard = self.refresh_gate.lock().await;
        let cached_key = {
            let state = self.cache.read().await;
            self.cached_decoding_key(&state, kid)?
        };
        if let Some(key) = cached_key {
            return Ok(key);
        }

        let refresh_is_for_unknown_kid =
            self.cache.read().await.cache.as_ref().is_some_and(|cache| {
                cache.fetched_at.elapsed() < self.config.jwks_cache_ttl
                    && cache.keys.find(kid).is_none()
            });
        let keys = match self.fetch_jwks().await {
            Ok(keys) => keys,
            Err(error) => {
                let failed_at = Instant::now();
                let mut state = self.cache.write().await;
                state.last_failed_refresh_at = Some(failed_at);
                if refresh_is_for_unknown_kid {
                    state.last_unknown_kid_refresh_at = Some(failed_at);
                }
                return Err(error);
            }
        };
        let fetched_at = Instant::now();
        let key = keys.find(kid).map(DecodingKey::from_jwk).transpose();
        let missing_after_refresh = matches!(key, Ok(None));
        let mut state = self.cache.write().await;
        state.cache = Some(CachedJwks { fetched_at, keys });
        state.last_failed_refresh_at = None;
        state.last_unknown_kid_refresh_at =
            (refresh_is_for_unknown_kid || missing_after_refresh).then_some(fetched_at);
        key.map_err(|_| JwtAuthenticationFailure::VerifierUnavailable)?
            .ok_or(JwtAuthenticationFailure::InvalidToken)
    }

    pub async fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> std::result::Result<AuthenticatedActor, JwtAuthenticationFailure> {
        Ok(self.authenticate_with_registry(headers).await?.actor)
    }

    pub(crate) async fn authenticate_with_registry(
        &self,
        headers: &HeaderMap,
    ) -> std::result::Result<AuthenticatedAccess, JwtAuthenticationFailure> {
        let value = headers
            .get(axum::http::header::AUTHORIZATION)
            .ok_or(JwtAuthenticationFailure::MissingCredentials)?
            .to_str()
            .map_err(|_| JwtAuthenticationFailure::InvalidToken)?;
        let token = value
            .strip_prefix("Bearer ")
            .filter(|token| !token.trim().is_empty())
            .ok_or(JwtAuthenticationFailure::InvalidToken)?;
        let header = decode_header(token).map_err(|_| JwtAuthenticationFailure::InvalidToken)?;
        if header.alg != Algorithm::RS256 {
            return Err(JwtAuthenticationFailure::InvalidToken);
        }
        let kid = header.kid.ok_or(JwtAuthenticationFailure::InvalidToken)?;
        let key = self.decoding_key(&kid).await?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[self.config.issuer.as_str()]);
        validation.set_audience(&[self.config.audience.as_str()]);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.leeway = JWT_CLOCK_SKEW_LEEWAY_SECONDS;
        validation.required_spec_claims = ["exp", "iss", "aud", "sub"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let claims = decode::<AccessTokenClaims>(token, &key, &validation)
            .map_err(|error| match error.kind() {
                JwtErrorKind::ExpiredSignature => JwtAuthenticationFailure::ExpiredToken,
                JwtErrorKind::InvalidAudience => JwtAuthenticationFailure::WrongAudience,
                JwtErrorKind::MissingRequiredClaim(claim) if claim == "aud" => {
                    JwtAuthenticationFailure::WrongAudience
                }
                _ => JwtAuthenticationFailure::InvalidToken,
            })?
            .claims;
        self.validate_required_scopes(claims.scope.as_deref())?;
        let registry = self
            .registry
            .load_async()
            .await
            .map_err(|_| JwtAuthenticationFailure::VerifierUnavailable)?;
        let verified_email = if claims.email_verified == Some(true) {
            claims.email.as_deref()
        } else {
            None
        };
        let actor = registry
            .actor_for_oidc(
                &claims.sub,
                claims.preferred_username.as_deref(),
                verified_email,
            )
            .map_err(|_| JwtAuthenticationFailure::AccessDenied)?;
        let actor_id = actor.id.clone();
        Ok(AuthenticatedAccess {
            actor: AuthenticatedActor { actor_id },
            registry,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProtectedResourceMetadata {
    pub resource: String,
    pub authorization_servers: Vec<String>,
    pub bearer_methods_supported: Vec<&'static str>,
    pub scopes_supported: Vec<String>,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        num::NonZeroUsize,
        process::{Command, Stdio},
        sync::{
            OnceLock,
            atomic::{AtomicU64, Ordering},
            mpsc,
        },
        time::Duration,
    };

    use axum::{
        Router,
        body::Body,
        http::{HeaderValue, Request, StatusCode, header::AUTHORIZATION},
    };
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde::Serialize;
    use serde_json::json;
    use tower::ServiceExt;

    use super::*;
    use crate::{http::build_router, ozon::OzonClient, server::OzonMcp, test_support::mock_http};
    use rmcp::transport::{
        StreamableHttpServerConfig, StreamableHttpService,
        streamable_http_server::session::local::LocalSessionManager,
    };

    const KID: &str = "test-key";

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    static TEST_KEY: OnceLock<TestKey> = OnceLock::new();

    struct TestKey {
        encoding: EncodingKey,
        modulus: String,
        exponent: String,
    }

    fn openssl(args: &[&str], input: &[u8]) -> Vec<u8> {
        let mut child = Command::new("openssl")
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("openssl is required to generate ephemeral JWT test keys");
        child.stdin.take().unwrap().write_all(input).unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        output.stdout
    }

    fn test_key() -> &'static TestKey {
        TEST_KEY.get_or_init(|| {
            let private_pem = openssl(
                &[
                    "genpkey",
                    "-algorithm",
                    "RSA",
                    "-pkeyopt",
                    "rsa_keygen_bits:2048",
                    "-pkeyopt",
                    "rsa_keygen_pubexp:65537",
                ],
                &[],
            );
            let modulus_output = openssl(&["rsa", "-noout", "-modulus"], &private_pem);
            let modulus_hex = std::str::from_utf8(&modulus_output)
                .unwrap()
                .trim()
                .strip_prefix("Modulus=")
                .unwrap();
            let modulus = modulus_hex
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect::<Vec<_>>();
            TestKey {
                encoding: EncodingKey::from_rsa_pem(&private_pem).unwrap(),
                modulus: URL_SAFE_NO_PAD.encode(modulus),
                exponent: URL_SAFE_NO_PAD.encode([1, 0, 1]),
            }
        })
    }

    #[derive(Serialize)]
    struct TestClaims<'a> {
        iss: &'a str,
        aud: &'a str,
        sub: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        scope: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        preferred_username: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        email: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        email_verified: Option<bool>,
        exp: i64,
        nbf: i64,
    }

    fn registry() -> RegistrySource {
        registry_with_actors(json!([{
            "id": "admin",
            "name": "Administrator",
            "role": "admin",
            "oidc": {"username": "admin"}
        }]))
    }

    fn registry_with_actors(actors: serde_json::Value) -> RegistrySource {
        let id = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("mcp-ozon-auth-{}-{id}.json", std::process::id()));
        fs::write(
            &path,
            serde_json::to_vec_pretty(&json!({
                "version": 1,
                "actors": actors,
                "accounts": [],
            }))
            .unwrap(),
        )
        .unwrap();
        RegistrySource::new(path).unwrap()
    }

    fn jwks() -> String {
        jwks_with_kid(KID)
    }

    fn jwks_with_kid(kid: &str) -> String {
        let key = test_key();
        json!({
            "keys": [{
                "kty": "RSA",
                "kid": kid,
                "use": "sig",
                "alg": "RS256",
                "n": key.modulus,
                "e": key.exponent
            }]
        })
        .to_string()
    }

    fn blocking_jwks_http(
        body: String,
    ) -> (String, tokio::sync::oneshot::Receiver<()>, mpsc::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            assert!(stream.read(&mut request).unwrap() > 0);
            started_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        (
            format!("http://{address}"),
            started_receiver,
            release_sender,
        )
    }

    fn write_jwks_http_response(
        stream: &mut TcpStream,
        status: u16,
        extra_headers: &[(String, String)],
        body: &[u8],
        chunked: bool,
    ) -> std::io::Result<()> {
        let reason = if status == 200 { "OK" } else { "Redirect" };
        let mut headers =
            format!("HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n");
        for (name, value) in extra_headers {
            headers.push_str(&format!("{name}: {value}\r\n"));
        }
        if chunked {
            headers.push_str("Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
        } else {
            headers.push_str(&format!(
                "Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            ));
        }

        stream.write_all(headers.as_bytes())?;
        if chunked {
            for chunk in body.chunks(16 * 1024) {
                write!(stream, "{:x}\r\n", chunk.len())?;
                stream.write_all(chunk)?;
                stream.write_all(b"\r\n")?;
            }
            stream.write_all(b"0\r\n\r\n")?;
        } else {
            stream.write_all(body)?;
        }
        Ok(())
    }

    fn one_shot_jwks_http(
        status: u16,
        extra_headers: Vec<(String, String)>,
        body: Vec<u8>,
        chunked: bool,
    ) -> (String, mpsc::Receiver<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (request_sender, request_receiver) = mpsc::channel();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            assert!(stream.read(&mut request).unwrap() > 0);
            request_sender.send(()).unwrap();
            let _ = write_jwks_http_response(&mut stream, status, &extra_headers, &body, chunked);
        });
        (format!("http://{address}"), request_receiver)
    }

    fn config(jwks_url: String) -> JwtConfig {
        JwtConfig {
            issuer: "http://issuer.test/realms/ofk".to_owned(),
            audience: "ozonofk-mcp".to_owned(),
            jwks_url,
            resource_url: "http://localhost:8788/mcp".to_owned(),
            resource_metadata_url: "http://localhost:8788/.well-known/oauth-protected-resource"
                .to_owned(),
            required_scopes: vec!["mcp:tools".to_owned()],
            jwks_cache_ttl: Duration::from_secs(300),
        }
    }

    fn token(kid: Option<&str>, audience: &str, username: &str) -> String {
        token_with_scope(
            kid,
            audience,
            username,
            Some("openid profile email mcp:tools"),
        )
    }

    fn token_with_scope(
        kid: Option<&str>,
        audience: &str,
        username: &str,
        scope: Option<&str>,
    ) -> String {
        token_with_identity(
            kid,
            audience,
            "subject-1",
            Some(username),
            Some("admin@example.test"),
            Some(true),
            scope,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn token_with_identity(
        kid: Option<&str>,
        audience: &str,
        subject: &str,
        username: Option<&str>,
        email: Option<&str>,
        email_verified: Option<bool>,
        scope: Option<&str>,
    ) -> String {
        let now = chrono::Utc::now().timestamp();
        token_with_identity_and_times(
            kid,
            audience,
            subject,
            username,
            email,
            email_verified,
            scope,
            now + 3_600,
            now - 1,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn token_with_identity_and_times(
        kid: Option<&str>,
        audience: &str,
        subject: &str,
        username: Option<&str>,
        email: Option<&str>,
        email_verified: Option<bool>,
        scope: Option<&str>,
        exp: i64,
        nbf: i64,
    ) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = kid.map(str::to_owned);
        encode(
            &header,
            &TestClaims {
                iss: "http://issuer.test/realms/ofk",
                aud: audience,
                sub: subject,
                scope,
                preferred_username: username,
                email,
                email_verified,
                exp,
                nbf,
            },
            &test_key().encoding,
        )
        .unwrap()
    }

    /// Mints an otherwise valid token under a caller-chosen `iss`.
    fn token_from_issuer(issuer: &str) -> String {
        let now = chrono::Utc::now().timestamp();
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(KID.to_owned());
        encode(
            &header,
            &TestClaims {
                iss: issuer,
                aud: "ozonofk-mcp",
                sub: "subject-1",
                scope: Some("openid profile email mcp:tools"),
                preferred_username: Some("admin"),
                email: Some("admin@example.test"),
                email_verified: Some(true),
                exp: now + 3_600,
                nbf: now - 1,
            },
            &test_key().encoding,
        )
        .unwrap()
    }

    fn bearer(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    }

    /// A client for the loopback servers these tests spawn.
    ///
    /// `.no_proxy()` is not cosmetic: `reqwest` honours `HTTP_PROXY`/`ALL_PROXY`
    /// even for `127.0.0.1`, so a developer with a proxy exported in their shell
    /// — or a sibling test that sets one — would otherwise divert this request
    /// away from the server under test.
    fn loopback_client() -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("a loopback client always builds")
    }

    async fn call_mcp_tool(
        endpoint: &str,
        bearer: Option<&str>,
        name: &str,
        arguments: serde_json::Value,
    ) -> serde_json::Value {
        let mut request = loopback_client()
            .post(endpoint)
            .header("accept", "application/json, text/event-stream")
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments}
            }));
        if let Some(token) = bearer {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.unwrap();
        assert!(response.status().is_success());
        response.json().await.unwrap()
    }

    #[tokio::test]
    async fn mcp_tool_authentication_is_checked_per_http_request_before_dispatch() {
        let registry = registry();
        let (jwks_url, _) = mock_http(vec![(200, jwks())]);
        let authenticator = JwtAuthenticator::new(config(jwks_url), registry.clone()).unwrap();
        let client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
            BTreeMap::new(),
        )
        .unwrap();
        let server = Arc::new(OzonMcp::new_authenticated(
            client,
            registry.clone(),
            authenticator,
        ));
        let service: StreamableHttpService<OzonMcp, LocalSessionManager> =
            StreamableHttpService::new(
                move || Ok((*server).clone()),
                Default::default(),
                StreamableHttpServerConfig::default()
                    .with_legacy_session_mode(false)
                    .with_json_response(true),
            );
        let router = Router::new().nest_service("/mcp", service);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        assert_eq!(registry.load_count(), 0);
        let missing = call_mcp_tool(&endpoint, None, "ozon_analytics", json!({})).await;
        assert_eq!(missing["result"]["isError"], true);
        assert!(missing["result"]["_meta"]["mcp/www_authenticate"].is_array());
        assert_eq!(
            missing["result"]["content"][0]["text"],
            "Требуется авторизация: access token не передан."
        );
        assert_eq!(registry.load_count(), 0);

        let valid_token = token(Some(KID), "ozonofk-mcp", "admin");
        let valid = call_mcp_tool(&endpoint, Some(&valid_token), "list_members", json!({})).await;
        assert_ne!(valid["result"]["isError"], true);
        assert_eq!(valid["result"]["structuredContent"]["actor"]["id"], "admin");
        assert_eq!(
            registry.load_count(),
            1,
            "JWT subject mapping and tool RBAC must use one registry snapshot"
        );

        let unknown_token = token(Some(KID), "ozonofk-mcp", "not-provisioned");
        let unknown =
            call_mcp_tool(&endpoint, Some(&unknown_token), "list_members", json!({})).await;
        assert_eq!(unknown["result"]["isError"], true);
        assert!(unknown["result"].get("_meta").is_none());
        assert_eq!(
            unknown["result"]["content"][0]["text"],
            "Доступ для подтверждённой учётной записи не разрешён."
        );
        assert_eq!(registry.load_count(), 2);

        task.abort();
    }

    #[tokio::test]
    async fn production_router_installs_verified_actor_and_registry_before_mcp_dispatch() {
        let registry = registry();
        let (jwks_url, requests) = mock_http(vec![(200, jwks())]);
        let authenticator = JwtAuthenticator::new(config(jwks_url), registry.clone()).unwrap();
        let client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
            BTreeMap::new(),
        )
        .unwrap();
        let router = build_router(
            OzonMcp::new_authenticated(client, registry.clone(), authenticator),
            NonZeroUsize::new(1).unwrap(),
        );
        let valid_token = token(Some(KID), "ozonofk-mcp", "admin");

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("host", "localhost")
                    .header("accept", "application/json, text/event-stream")
                    .header("content-type", "application/json")
                    .header(AUTHORIZATION, format!("Bearer {valid_token}"))
                    .body(Body::from(
                        json!({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "initialize",
                            "params": {
                                "protocolVersion": "2025-06-18",
                                "capabilities": {},
                                "clientInfo": {"name": "transport-auth-test", "version": "0"}
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("mcp-session-id").is_some());
        assert_eq!(registry.load_count(), 1);
        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn authenticates_rs256_tokens_and_caches_jwks() {
        let (base_url, requests) = mock_http(vec![(200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        let metadata = auth.protected_resource_metadata();
        assert_eq!(metadata.resource, "http://localhost:8788/mcp");
        assert_eq!(metadata.authorization_servers.len(), 1);
        assert_eq!(metadata.bearer_methods_supported, ["header"]);
        assert_eq!(metadata.scopes_supported, ["mcp:tools"]);
        assert_eq!(auth.required_scopes(), ["mcp:tools"]);
        assert_eq!(
            auth.resource_metadata_url(),
            "http://localhost:8788/.well-known/oauth-protected-resource"
        );

        let headers = bearer(&token(Some(KID), "ozonofk-mcp", "admin"));
        for _ in 0..2 {
            assert_eq!(auth.authenticate(&headers).await.unwrap().actor_id, "admin");
        }
        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn jwks_fetch_is_direct_does_not_follow_redirects_and_supports_local_issuers() {
        let (target_url, target_requests) = mock_http(vec![(200, jwks())]);
        let (redirect_url, redirect_requests) = one_shot_jwks_http(
            302,
            vec![("Location".to_owned(), target_url)],
            jwks().into_bytes(),
            false,
        );
        let redirected = JwtAuthenticator::new(config(redirect_url), registry()).unwrap();
        assert_eq!(
            redirected.fetch_jwks().await.unwrap_err(),
            JwtAuthenticationFailure::VerifierUnavailable
        );
        assert!(
            redirect_requests
                .recv_timeout(Duration::from_secs(1))
                .is_ok()
        );
        assert!(
            target_requests
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "the JWKS client must not follow redirects"
        );

        let (local_url, local_requests) = mock_http(vec![(200, jwks())]);
        let direct = JwtAuthenticator::new(config(local_url), registry()).unwrap();
        assert_eq!(direct.fetch_jwks().await.unwrap().keys.len(), 1);
        assert!(local_requests.recv_timeout(Duration::from_secs(1)).is_ok());
    }

    #[tokio::test]
    async fn rejects_jwks_bodies_over_one_mib_when_sized_chunked_or_compressed() {
        let oversized = vec![b' '; MAX_JWKS_BODY_BYTES + 1];
        for chunked in [false, true] {
            let (url, requests) = one_shot_jwks_http(200, Vec::new(), oversized.clone(), chunked);
            let auth = JwtAuthenticator::new(config(url), registry()).unwrap();
            assert_eq!(
                auth.fetch_jwks().await.unwrap_err(),
                JwtAuthenticationFailure::VerifierUnavailable
            );
            assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
        }

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&oversized).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(compressed.len() < MAX_JWKS_BODY_BYTES);
        let (url, requests) = one_shot_jwks_http(
            200,
            vec![("Content-Encoding".to_owned(), "gzip".to_owned())],
            compressed,
            false,
        );
        let auth = JwtAuthenticator::new(config(url), registry()).unwrap();
        assert_eq!(
            auth.fetch_jwks().await.unwrap_err(),
            JwtAuthenticationFailure::VerifierUnavailable
        );
        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn rejects_excessive_jwk_counts_and_string_sizes_before_typed_parsing() {
        let valid_key = json!({
            "kty": "RSA",
            "kid": KID,
            "use": "sig",
            "alg": "RS256",
            "n": test_key().modulus,
            "e": test_key().exponent,
            "ignored_primitives": [null, true, 7]
        });
        assert_eq!(
            parse_bounded_jwks(
                serde_json::to_string(&json!({"keys": [valid_key.clone()]}))
                    .unwrap()
                    .as_bytes()
            )
            .unwrap()
            .keys
            .len(),
            1
        );

        let too_many = vec![valid_key.clone(); MAX_JWKS_KEYS + 1];
        for invalid in [
            json!({"keys": too_many}),
            json!({"keys": [{"kty": "RSA", "kid": "x".repeat(MAX_JWK_STRING_BYTES + 1)}]}),
            json!({"keys": [{"x".repeat(MAX_JWK_STRING_BYTES + 1): "value"}]}),
            json!({"keys": [valid_key], "ignored": "x".repeat(MAX_JWK_STRING_BYTES + 1)}),
            json!({"keys": [{"kty": 7}]}),
        ] {
            assert_eq!(
                parse_bounded_jwks(serde_json::to_string(&invalid).unwrap().as_bytes())
                    .unwrap_err(),
                JwtAuthenticationFailure::VerifierUnavailable
            );
        }
    }

    #[tokio::test]
    async fn concurrent_same_valid_kid_waiters_reuse_one_empty_cache_refresh() {
        const CALLS: usize = 16;
        let (base_url, request_started, release_response) = blocking_jwks_http(jwks());
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(CALLS + 1));
        let mut tasks = Vec::new();
        for _ in 0..CALLS {
            let auth = auth.clone();
            let barrier = barrier.clone();
            let token = token(Some(KID), "ozonofk-mcp", "admin");
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                auth.authenticate(&bearer(&token)).await
            }));
        }

        barrier.wait().await;
        tokio::time::timeout(Duration::from_secs(1), request_started)
            .await
            .expect("the single JWKS request must start")
            .unwrap();
        for _ in 0..CALLS {
            tokio::task::yield_now().await;
        }
        release_response.send(()).unwrap();

        for task in tasks {
            assert_eq!(task.await.unwrap().unwrap().actor_id, "admin");
        }
    }

    #[tokio::test]
    async fn concurrent_random_unknown_kids_are_singleflight_and_bounded() {
        let (base_url, requests) = mock_http(vec![(200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        let mut tasks = Vec::new();
        for index in 0..24 {
            let auth = auth.clone();
            let token = token(
                Some(&format!("attacker-controlled-kid-{}", index % 11)),
                "ozonofk-mcp",
                "admin",
            );
            tasks.push(tokio::spawn(async move {
                auth.authenticate(&bearer(&token)).await
            }));
        }
        for task in tasks {
            assert_eq!(
                task.await.unwrap().unwrap_err(),
                JwtAuthenticationFailure::InvalidToken
            );
        }

        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(requests.try_recv().is_err());

        // A different attacker-controlled kid within the global cooldown must
        // not allocate per-kid state or trigger another network refresh.
        assert_eq!(
            auth.authenticate(&bearer(&token(
                Some("another-random-kid"),
                "ozonofk-mcp",
                "admin"
            )))
            .await
            .unwrap_err(),
            JwtAuthenticationFailure::InvalidToken
        );
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn concurrent_failed_empty_cache_refresh_is_negative_cached_and_retriable() {
        let (base_url, requests) = mock_http(vec![(500, "{}".to_owned()), (200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        let concurrent_failures = async {
            let mut tasks = Vec::new();
            for index in 0..24 {
                let auth = auth.clone();
                let token = token(
                    Some(&format!("random-kid-during-outage-{index}")),
                    "ozonofk-mcp",
                    "admin",
                );
                tasks.push(tokio::spawn(async move {
                    auth.authenticate(&bearer(&token)).await
                }));
            }
            for task in tasks {
                assert_eq!(
                    task.await.unwrap().unwrap_err(),
                    JwtAuthenticationFailure::VerifierUnavailable
                );
            }
        };
        tokio::time::timeout(Duration::from_secs(1), concurrent_failures)
            .await
            .expect("JWKS failure waiters must reuse the bounded negative-cache result");

        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(requests.try_recv().is_err());

        // Avoid a real five-second sleep while proving that a later request can
        // retry an empty cache after the global failure cooldown expires.
        auth.cache.write().await.last_failed_refresh_at =
            Some(std::time::Instant::now() - FAILED_REFRESH_COOLDOWN - Duration::from_millis(1));
        assert_eq!(
            auth.authenticate(&bearer(&token(Some(KID), "ozonofk-mcp", "admin")))
                .await
                .unwrap()
                .actor_id,
            "admin"
        );
        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn failed_unknown_kid_refresh_sets_both_global_cooldowns() {
        let (base_url, requests) = mock_http(vec![(200, jwks()), (500, "{}".to_owned())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();

        auth.authenticate(&bearer(&token(Some(KID), "ozonofk-mcp", "admin")))
            .await
            .unwrap();
        assert_eq!(
            auth.authenticate(&bearer(&token(
                Some("unknown-during-outage"),
                "ozonofk-mcp",
                "admin"
            )))
            .await
            .unwrap_err(),
            JwtAuthenticationFailure::VerifierUnavailable
        );
        assert_eq!(
            auth.authenticate(&bearer(&token(
                Some("another-unknown-during-outage"),
                "ozonofk-mcp",
                "admin"
            )))
            .await
            .unwrap_err(),
            JwtAuthenticationFailure::VerifierUnavailable
        );

        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn fresh_unknown_kid_refreshes_once_for_normal_key_rotation() {
        const ROTATED_KID: &str = "rotated-test-key";
        let (base_url, requests) =
            mock_http(vec![(200, jwks()), (200, jwks_with_kid(ROTATED_KID))]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();

        assert_eq!(
            auth.authenticate(&bearer(&token(Some(KID), "ozonofk-mcp", "admin")))
                .await
                .unwrap()
                .actor_id,
            "admin"
        );
        assert_eq!(
            auth.authenticate(&bearer(&token(Some(ROTATED_KID), "ozonofk-mcp", "admin")))
                .await
                .unwrap()
                .actor_id,
            "admin"
        );

        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(requests.try_recv().is_err());
    }

    #[test]
    fn builds_safe_oauth_challenges_only_for_recoverable_authentication_failures() {
        let auth =
            JwtAuthenticator::new(config("http://127.0.0.1:1".to_owned()), registry()).unwrap();
        let missing = auth
            .challenge(&JwtAuthenticationFailure::MissingCredentials)
            .unwrap();
        assert!(missing.starts_with("Bearer "));
        assert!(missing.contains(
            "resource_metadata=\"http://localhost:8788/.well-known/oauth-protected-resource\""
        ));
        assert!(missing.contains("scope=\"mcp:tools\""));
        assert!(!missing.contains("error="));
        assert!(!missing.contains("error_description="));
        assert!(
            !JwtAuthenticationFailure::MissingCredentials
                .to_string()
                .is_empty()
        );

        for (failure, oauth_error, description) in [
            (
                JwtAuthenticationFailure::InvalidToken,
                "invalid_token",
                "The access token is invalid",
            ),
            (
                JwtAuthenticationFailure::ExpiredToken,
                "invalid_token",
                "The access token has expired",
            ),
            (
                JwtAuthenticationFailure::WrongAudience,
                "invalid_token",
                "The access token audience is invalid",
            ),
            (
                JwtAuthenticationFailure::InsufficientScope,
                "insufficient_scope",
                "The access token lacks a required scope",
            ),
        ] {
            let challenge = auth.challenge(&failure).unwrap();
            assert!(challenge.is_ascii());
            assert!(challenge.contains(
                "resource_metadata=\"http://localhost:8788/.well-known/oauth-protected-resource\""
            ));
            assert!(challenge.contains("scope=\"mcp:tools\""));
            assert!(challenge.contains(&format!("error=\"{oauth_error}\"")));
            assert!(challenge.contains(&format!("error_description=\"{description}\"")));
            assert!(!failure.public_message().is_empty());
            assert!(!failure.to_string().is_empty());
        }

        for failure in [
            JwtAuthenticationFailure::AccessDenied,
            JwtAuthenticationFailure::VerifierUnavailable,
        ] {
            assert_eq!(auth.challenge(&failure), None);
            assert!(!failure.public_message().is_empty());
            assert!(!failure.to_string().is_empty());
        }
    }

    #[tokio::test]
    async fn subject_pinned_actor_rejects_fallback_claims_for_a_mismatched_sub() {
        let registry = registry_with_actors(json!([{
            "id": "pinned",
            "name": "Pinned Actor",
            "role": "admin",
            "oidc": {
                "subject": "expected-subject",
                "username": "matching-user",
                "email": "matching@example.test"
            }
        }]));
        let (base_url, _) = mock_http(vec![(200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry).unwrap();

        let mismatched = token_with_identity(
            Some(KID),
            "ozonofk-mcp",
            "wrong-subject",
            Some("matching-user"),
            Some("matching@example.test"),
            Some(true),
            Some("mcp:tools"),
        );
        assert_eq!(
            auth.authenticate(&bearer(&mismatched)).await.unwrap_err(),
            JwtAuthenticationFailure::AccessDenied
        );

        let exact_subject = token_with_identity(
            Some(KID),
            "ozonofk-mcp",
            "expected-subject",
            Some("different-user"),
            Some("different@example.test"),
            Some(false),
            Some("mcp:tools"),
        );
        assert_eq!(
            auth.authenticate(&bearer(&exact_subject))
                .await
                .unwrap()
                .actor_id,
            "pinned"
        );
    }

    #[tokio::test]
    async fn email_fallback_requires_email_verified_true() {
        let registry = registry_with_actors(json!([{
            "id": "email-fallback",
            "name": "Email Fallback",
            "role": "manager",
            "oidc": {"email": "verified@example.test"}
        }]));
        let (base_url, _) = mock_http(vec![(200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry).unwrap();

        for email_verified in [None, Some(false)] {
            let token = token_with_identity(
                Some(KID),
                "ozonofk-mcp",
                "unrelated-subject",
                None,
                Some("verified@example.test"),
                email_verified,
                Some("mcp:tools"),
            );
            assert_eq!(
                auth.authenticate(&bearer(&token)).await.unwrap_err(),
                JwtAuthenticationFailure::AccessDenied
            );
        }

        let verified = token_with_identity(
            Some(KID),
            "ozonofk-mcp",
            "unrelated-subject",
            None,
            Some("verified@example.test"),
            Some(true),
            Some("mcp:tools"),
        );
        assert_eq!(
            auth.authenticate(&bearer(&verified))
                .await
                .unwrap()
                .actor_id,
            "email-fallback"
        );
    }

    #[tokio::test]
    async fn ambiguous_username_and_verified_email_fallback_is_rejected() {
        let registry = registry_with_actors(json!([
            {
                "id": "username-fallback",
                "name": "Username Fallback",
                "role": "manager",
                "oidc": {"username": "shared-user"}
            },
            {
                "id": "email-fallback",
                "name": "Email Fallback",
                "role": "manager",
                "oidc": {"email": "shared@example.test"}
            }
        ]));
        let (base_url, _) = mock_http(vec![(200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry).unwrap();
        let token = token_with_identity(
            Some(KID),
            "ozonofk-mcp",
            "unrelated-subject",
            Some("shared-user"),
            Some("shared@example.test"),
            Some(true),
            Some("mcp:tools"),
        );
        let error = auth.authenticate(&bearer(&token)).await.unwrap_err();
        assert_eq!(error, JwtAuthenticationFailure::AccessDenied);
    }

    #[tokio::test]
    async fn requires_every_configured_oauth_scope() {
        let (base_url, _) = mock_http(vec![(200, jwks())]);
        let mut jwt_config = config(base_url);
        jwt_config.required_scopes = vec!["mcp:tools".to_owned(), "analytics:read".to_owned()];
        let auth = JwtAuthenticator::new(jwt_config, registry()).unwrap();

        for scope in [
            None,
            Some("mcp:tools"),
            Some("analytics:read"),
            Some("mcp:tools\tanalytics:read"),
        ] {
            let token = token_with_scope(Some(KID), "ozonofk-mcp", "admin", scope);
            assert_eq!(
                auth.authenticate(&bearer(&token)).await.unwrap_err(),
                JwtAuthenticationFailure::InsufficientScope
            );
        }

        let token = token_with_scope(
            Some(KID),
            "ozonofk-mcp",
            "admin",
            Some("openid analytics:read mcp:tools"),
        );
        assert_eq!(
            auth.authenticate(&bearer(&token)).await.unwrap().actor_id,
            "admin"
        );
    }

    #[tokio::test]
    async fn rejects_bad_headers_algorithms_claims_and_unknown_users() {
        let auth =
            JwtAuthenticator::new(config("http://127.0.0.1:1".to_owned()), registry()).unwrap();
        assert_eq!(
            auth.authenticate(&HeaderMap::new()).await.unwrap_err(),
            JwtAuthenticationFailure::MissingCredentials
        );

        for value in ["Basic abc", "Bearer ", "Bearer not-a-jwt"] {
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, HeaderValue::from_str(value).unwrap());
            assert_eq!(
                auth.authenticate(&headers).await.unwrap_err(),
                JwtAuthenticationFailure::InvalidToken
            );
        }
        let mut invalid_text = HeaderMap::new();
        invalid_text.insert(
            AUTHORIZATION,
            HeaderValue::from_bytes(b"Bearer \xff").unwrap(),
        );
        assert_eq!(
            auth.authenticate(&invalid_text).await.unwrap_err(),
            JwtAuthenticationFailure::InvalidToken
        );

        let hs_token = encode(
            &Header::new(Algorithm::HS256),
            &json!({"sub": "subject-1"}),
            &EncodingKey::from_secret(b"test"),
        )
        .unwrap();
        assert_eq!(
            auth.authenticate(&bearer(&hs_token)).await.unwrap_err(),
            JwtAuthenticationFailure::InvalidToken
        );
        assert_eq!(
            auth.authenticate(&bearer(&token(None, "ozonofk-mcp", "admin")))
                .await
                .unwrap_err(),
            JwtAuthenticationFailure::InvalidToken
        );

        let (base_url, _) = mock_http(vec![(200, jwks()), (200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        let signed = token(Some(KID), "ozonofk-mcp", "admin");
        let mut parts = signed.split('.').map(str::to_owned).collect::<Vec<_>>();
        let mut signature = URL_SAFE_NO_PAD.decode(&parts[2]).unwrap();
        signature[0] ^= 1;
        parts[2] = URL_SAFE_NO_PAD.encode(signature);
        assert_eq!(
            auth.authenticate(&bearer(&parts.join(".")))
                .await
                .unwrap_err(),
            JwtAuthenticationFailure::InvalidToken
        );
        assert_eq!(
            auth.authenticate(&bearer(&token(Some(KID), "wrong", "admin")))
                .await
                .unwrap_err(),
            JwtAuthenticationFailure::WrongAudience
        );
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(KID.to_owned());
        let now = chrono::Utc::now().timestamp();
        let missing_audience = encode(
            &header,
            &json!({
                "iss": "http://issuer.test/realms/ofk",
                "sub": "subject-1",
                "scope": "mcp:tools",
                "preferred_username": "admin",
                "exp": now + 3_600,
                "nbf": now - 1
            }),
            &test_key().encoding,
        )
        .unwrap();
        assert_eq!(
            auth.authenticate(&bearer(&missing_audience))
                .await
                .unwrap_err(),
            JwtAuthenticationFailure::WrongAudience
        );
        assert_eq!(
            auth.authenticate(&bearer(&token(Some(KID), "ozonofk-mcp", "unknown")))
                .await
                .unwrap_err(),
            JwtAuthenticationFailure::AccessDenied
        );
    }

    #[tokio::test]
    async fn rejects_malformed_numeric_time_claims() {
        let (base_url, _) = mock_http(vec![(200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        let now = chrono::Utc::now().timestamp();
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(KID.to_owned());

        for claims in [
            json!({
                "iss": "http://issuer.test/realms/ofk",
                "aud": "ozonofk-mcp",
                "sub": "subject-1",
                "scope": "mcp:tools",
                "preferred_username": "admin",
                "exp": "never",
                "nbf": now - 1
            }),
            json!({
                "iss": "http://issuer.test/realms/ofk",
                "aud": "ozonofk-mcp",
                "sub": "subject-1",
                "scope": "mcp:tools",
                "preferred_username": "admin",
                "exp": now + 3_600,
                "nbf": "99999999999"
            }),
        ] {
            let token = encode(&header, &claims, &test_key().encoding).unwrap();
            assert_eq!(
                auth.authenticate(&bearer(&token)).await.unwrap_err(),
                JwtAuthenticationFailure::InvalidToken
            );
        }
    }

    #[tokio::test]
    async fn classifies_expired_tokens_after_signature_verification() {
        let (base_url, _) = mock_http(vec![(200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        let now = chrono::Utc::now().timestamp();
        let expired = token_with_identity_and_times(
            Some(KID),
            "ozonofk-mcp",
            "subject-1",
            Some("admin"),
            Some("admin@example.test"),
            Some(true),
            Some("mcp:tools"),
            now - 120,
            now - 3_600,
        );

        assert_eq!(
            auth.authenticate(&bearer(&expired)).await.unwrap_err(),
            JwtAuthenticationFailure::ExpiredToken
        );
    }

    #[tokio::test]
    async fn accepts_only_the_explicit_clock_skew_window() {
        let (base_url, _) = mock_http(vec![(200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        let now = chrono::Utc::now().timestamp();
        let within_skew = token_with_identity_and_times(
            Some(KID),
            "ozonofk-mcp",
            "subject-1",
            Some("admin"),
            Some("admin@example.test"),
            Some(true),
            Some("mcp:tools"),
            now - 30,
            now + 30,
        );

        assert_eq!(
            auth.authenticate(&bearer(&within_skew))
                .await
                .unwrap()
                .actor_id,
            "admin"
        );

        let beyond_skew = token_with_identity_and_times(
            Some(KID),
            "ozonofk-mcp",
            "subject-1",
            Some("admin"),
            Some("admin@example.test"),
            Some(true),
            Some("mcp:tools"),
            now + 3_600,
            now + 90,
        );
        assert_eq!(
            auth.authenticate(&bearer(&beyond_skew)).await.unwrap_err(),
            JwtAuthenticationFailure::InvalidToken
        );
    }

    #[tokio::test]
    async fn reports_jwks_failures_and_refreshes_unknown_or_expired_keys() {
        for (status, body) in [
            (500, "{}".to_owned()),
            (200, "{".to_owned()),
            (200, json!({"keys": []}).to_string()),
        ] {
            let (base_url, _) = mock_http(vec![(status, body)]);
            let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
            assert_eq!(
                auth.authenticate(&bearer(&token(Some(KID), "ozonofk-mcp", "admin")))
                    .await
                    .unwrap_err(),
                JwtAuthenticationFailure::VerifierUnavailable
            );
        }

        let auth =
            JwtAuthenticator::new(config("http://127.0.0.1:1".to_owned()), registry()).unwrap();
        assert_eq!(
            auth.authenticate(&bearer(&token(Some(KID), "ozonofk-mcp", "admin")))
                .await
                .unwrap_err(),
            JwtAuthenticationFailure::VerifierUnavailable
        );

        let (base_url, _) = mock_http(vec![(200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        assert_eq!(
            auth.authenticate(&bearer(&token(Some("unknown"), "ozonofk-mcp", "admin")))
                .await
                .unwrap_err(),
            JwtAuthenticationFailure::InvalidToken
        );

        let (base_url, requests) = mock_http(vec![(200, jwks()), (200, jwks())]);
        let mut jwt_config = config(base_url);
        jwt_config.jwks_cache_ttl = Duration::ZERO;
        let auth = JwtAuthenticator::new(jwt_config, registry()).unwrap();
        let headers = bearer(&token(Some(KID), "ozonofk-mcp", "admin"));
        auth.authenticate(&headers).await.unwrap();
        auth.authenticate(&headers).await.unwrap();
        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
    }

    /// `SECURITY.md` lists the issuer among the claims every `tools/call`
    /// validates, and `authenticate` pins it with `set_issuer`. Nothing else in
    /// the suite exercised that pin: a token bearing the right audience, scope,
    /// subject and signing key but a foreign `iss` was accepted.
    ///
    /// The signature still has to verify against the configured JWKS, so this is
    /// defence in depth rather than a standalone bypass today. It stops being
    /// defence in depth the moment a deployment shares signing keys across
    /// realms or fronts several issuers behind one JWKS URL, which is exactly
    /// when a silently dropped `set_issuer` would matter.
    #[tokio::test]
    async fn a_token_from_a_foreign_issuer_is_refused_even_with_a_valid_signature() {
        let (base_url, _) = mock_http(vec![(200, jwks()), (200, jwks()), (200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();

        // The configured issuer authenticates, so the fixture is known-good and
        // a later failure cannot be blamed on the rest of the claim set.
        assert_eq!(
            auth.authenticate(&bearer(&token_from_issuer("http://issuer.test/realms/ofk")))
                .await
                .unwrap()
                .actor_id,
            "admin"
        );

        for foreign in [
            "http://attacker.test/realms/ofk",
            // Same host, different realm.
            "http://issuer.test/realms/other",
            // Prefix and suffix of the configured issuer, so a `starts_with` or
            // `contains` comparison would not be enough.
            "http://issuer.test/realms",
            "http://issuer.test/realms/ofk/",
            "http://issuer.test/realms/ofk.attacker.test",
            "",
        ] {
            assert_eq!(
                auth.authenticate(&bearer(&token_from_issuer(foreign)))
                    .await
                    .unwrap_err(),
                JwtAuthenticationFailure::InvalidToken,
                "a token issued by {foreign:?} must be refused"
            );
        }
    }

    /// A JWKS document whose `keys` member is absent or is not an array must
    /// leave the verifier unavailable. These shapes reach `parse_bounded_jwks`
    /// before any typed parsing, so getting them wrong would either accept a
    /// key-less trust anchor or panic on the unwrapped array.
    #[test]
    fn jwks_documents_without_a_usable_key_array_are_refused() {
        let valid_key = json!({
            "kty": "RSA",
            "kid": KID,
            "use": "sig",
            "alg": "RS256",
            "n": test_key().modulus,
            "e": test_key().exponent
        });
        for malformed in [
            json!({}),
            json!({"keys": null}),
            json!({"keys": {}}),
            json!({"keys": "not-an-array"}),
            json!({"keys": 1}),
            // An IdP mid-rotation can publish an empty set; it is not a usable
            // trust anchor and must not be cached as one.
            json!({"keys": []}),
            // JSON member names are case sensitive: `Keys` is not `keys`.
            json!({"Keys": [valid_key.clone()]}),
            // A top-level array is not a JWKS document.
            json!([valid_key]),
        ] {
            assert_eq!(
                parse_bounded_jwks(malformed.to_string().as_bytes()).unwrap_err(),
                JwtAuthenticationFailure::VerifierUnavailable,
                "malformed JWKS must be refused: {malformed}"
            );
        }

        // Not JSON at all — an HTML error page served with a 200 status.
        assert_eq!(
            parse_bounded_jwks(b"<html>502 Bad Gateway</html>").unwrap_err(),
            JwtAuthenticationFailure::VerifierUnavailable
        );
    }

    /// The IdP publishes a key under the exact `kid` the token names, but the
    /// key material itself cannot be turned into an RS256 decoding key. That is
    /// an operator-side outage, not an invalid token: reporting it as
    /// `InvalidToken` would tell the client to re-authorize forever, and
    /// unwrapping it would take the process down.
    #[tokio::test]
    async fn a_matching_kid_with_unusable_key_material_reports_the_verifier_unavailable() {
        let unusable = json!({
            "keys": [{
                "kty": "RSA",
                "kid": KID,
                "use": "sig",
                "alg": "RS256",
                "n": "!!! not base64url !!!",
                "e": test_key().exponent
            }]
        })
        .to_string();

        // The document itself must survive bounded parsing: the failure has to
        // come from the key material, not from the envelope.
        assert_eq!(
            parse_bounded_jwks(unusable.as_bytes()).unwrap().keys.len(),
            1
        );

        let (base_url, requests) = mock_http(vec![(200, unusable.clone())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        let headers = bearer(&token(Some(KID), "ozonofk-mcp", "admin"));
        assert_eq!(
            auth.authenticate(&headers).await.unwrap_err(),
            JwtAuthenticationFailure::VerifierUnavailable
        );
        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());

        // The same failure must be reported from the cached path, without
        // another fetch, rather than being retried on every request.
        assert_eq!(
            auth.authenticate(&headers).await.unwrap_err(),
            JwtAuthenticationFailure::VerifierUnavailable
        );
    }

    /// The JWKS response is cut off mid-body. The prefix that does arrive is
    /// deliberately a *complete, parseable* JWKS document, so a reader that
    /// swallowed the truncation error would parse it and authenticate against a
    /// half-delivered trust anchor. Only the transport error distinguishes the
    /// two outcomes, which is exactly why it must not be ignored.
    #[tokio::test]
    async fn a_truncated_jwks_response_is_refused_even_though_its_prefix_parses() {
        let prefix = jwks();
        // Same document plus trailing whitespace: valid JSON either way, so the
        // declared length is the only signal that bytes are missing.
        let declared_length = prefix.len() + 64;
        assert!(parse_bounded_jwks(prefix.as_bytes()).is_ok());

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            assert!(stream.read(&mut request).unwrap() > 0);
            // Promise the padded document, then hang up after the prefix.
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {declared_length}\r\nConnection: close\r\n\r\n{prefix}"
            );
        });

        let auth = JwtAuthenticator::new(config(format!("http://{address}")), registry()).unwrap();
        assert_eq!(
            auth.authenticate(&bearer(&token(Some(KID), "ozonofk-mcp", "admin")))
                .await
                .unwrap_err(),
            JwtAuthenticationFailure::VerifierUnavailable,
            "a truncated JWKS must not authenticate a token, even when its prefix parses"
        );
    }

    /// A perfectly valid, correctly signed token must still be refused when the
    /// access registry cannot be read, because the registry is what maps the
    /// verified subject onto an actor. Failing open here would authenticate a
    /// subject with no authorization record at all.
    #[tokio::test]
    async fn an_unreadable_registry_refuses_an_otherwise_valid_token() {
        let (base_url, _) = mock_http(vec![(200, jwks()), (200, jwks())]);
        let registry = registry();
        let auth = JwtAuthenticator::new(config(base_url), registry.clone()).unwrap();
        let headers = bearer(&token(Some(KID), "ozonofk-mcp", "admin"));

        // Same token, same JWKS: only the registry changes between the calls.
        assert_eq!(auth.authenticate(&headers).await.unwrap().actor_id, "admin");

        fs::remove_file(registry.path()).unwrap();
        assert_eq!(
            auth.authenticate(&headers).await.unwrap_err(),
            JwtAuthenticationFailure::VerifierUnavailable
        );

        // A registry that is present but no longer valid JSON must fail the
        // same way instead of being served from the parsed cache.
        fs::write(registry.path(), b"{\"version\": 1, \"actors\": [").unwrap();
        assert_eq!(
            auth.authenticate(&headers).await.unwrap_err(),
            JwtAuthenticationFailure::VerifierUnavailable
        );
    }
}
