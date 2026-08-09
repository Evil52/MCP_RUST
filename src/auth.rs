use std::{collections::BTreeSet, sync::Arc, time::Instant};

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Json,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::config::{JwtConfig, RegistrySource};

#[derive(Debug, Clone)]
pub struct AuthenticatedActor {
    pub actor_id: String,
}

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

#[derive(Debug, Clone)]
pub struct JwtAuthenticator {
    config: JwtConfig,
    registry: RegistrySource,
    client: reqwest::Client,
    cache: Arc<RwLock<Option<CachedJwks>>>,
}

impl JwtAuthenticator {
    pub fn new(config: JwtConfig, registry: RegistrySource) -> Result<Self> {
        let client = reqwest::Client::new();
        Ok(Self {
            config,
            registry,
            client,
            cache: Arc::new(RwLock::new(None)),
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

    fn challenge(&self) -> HeaderValue {
        HeaderValue::from_str(&format!(
            "Bearer resource_metadata=\"{}\", scope=\"{}\"",
            self.config.resource_metadata_url,
            self.config.required_scopes.join(" ")
        ))
        .expect("resource metadata URL was validated at startup")
    }

    fn validate_required_scopes(&self, scope_claim: Option<&str>) -> Result<()> {
        let scope_claim = scope_claim.context("JWT не содержит claim scope")?;
        if scope_claim
            .bytes()
            .any(|byte| byte != b' ' && !matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
        {
            bail!("JWT содержит некорректный OAuth scope");
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
            bail!(
                "JWT не содержит обязательные OAuth scopes: {}",
                missing.join(" ")
            );
        }
        Ok(())
    }

    async fn refresh_jwks(&self) -> Result<()> {
        let keys = self
            .client
            .get(&self.config.jwks_url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .context("не удалось получить JWKS")?
            .error_for_status()
            .context("Keycloak вернул ошибку при запросе JWKS")?
            .json::<JwkSet>()
            .await
            .context("Keycloak вернул некорректный JWKS")?;
        if keys.keys.is_empty() {
            bail!("JWKS не содержит ключей");
        }
        *self.cache.write().await = Some(CachedJwks {
            fetched_at: Instant::now(),
            keys,
        });
        Ok(())
    }

    async fn decoding_key(&self, kid: &str) -> Result<DecodingKey> {
        let needs_refresh = self.cache.read().await.as_ref().is_none_or(|cache| {
            cache.fetched_at.elapsed() >= self.config.jwks_cache_ttl
                || cache.keys.find(kid).is_none()
        });
        if needs_refresh {
            self.refresh_jwks().await?;
        }
        let cache = self.cache.read().await;
        let jwk = cache
            .as_ref()
            .and_then(|cache| cache.keys.find(kid))
            .ok_or_else(|| anyhow!("JWT подписан неизвестным ключом kid={kid:?}"))?;
        DecodingKey::from_jwk(jwk).context("неподдерживаемый ключ JWKS")
    }

    pub async fn authenticate(&self, headers: &HeaderMap) -> Result<AuthenticatedActor> {
        let value = headers
            .get(axum::http::header::AUTHORIZATION)
            .context("отсутствует Authorization: Bearer")?
            .to_str()
            .context("некорректный заголовок Authorization")?;
        let token = value
            .strip_prefix("Bearer ")
            .filter(|token| !token.trim().is_empty())
            .context("ожидается Authorization: Bearer <token>")?;
        let header = decode_header(token).context("некорректный JWT header")?;
        if header.alg != Algorithm::RS256 {
            bail!("разрешён только алгоритм RS256");
        }
        let kid = header.kid.context("JWT header не содержит kid")?;
        let key = self.decoding_key(&kid).await?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[self.config.issuer.as_str()]);
        validation.set_audience(&[self.config.audience.as_str()]);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.required_spec_claims = ["exp", "iss", "aud", "sub"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let claims = decode::<AccessTokenClaims>(token, &key, &validation)
            .context("JWT не прошёл проверку подписи или claims")?
            .claims;
        self.validate_required_scopes(claims.scope.as_deref())?;
        let registry = self.registry.load()?;
        let verified_email = if claims.email_verified == Some(true) {
            claims.email.as_deref()
        } else {
            None
        };
        let actor = registry.actor_for_oidc(
            &claims.sub,
            claims.preferred_username.as_deref(),
            verified_email,
        )?;
        Ok(AuthenticatedActor {
            actor_id: actor.id.clone(),
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

#[derive(Debug, Serialize)]
struct AuthErrorBody {
    error: &'static str,
    message: &'static str,
}

pub async fn require_jwt(
    State(auth): State<JwtAuthenticator>,
    mut request: Request,
    next: Next,
) -> Response {
    match auth.authenticate(request.headers()).await {
        Ok(actor) => {
            request.extensions_mut().insert(actor);
            next.run(request).await
        }
        Err(error) => {
            tracing::warn!(reason = %error, "MCP JWT отклонён");
            let mut response = (
                StatusCode::UNAUTHORIZED,
                Json(AuthErrorBody {
                    error: "unauthorized",
                    message: "Требуется действительный access token",
                }),
            )
                .into_response();
            response
                .headers_mut()
                .insert(axum::http::header::WWW_AUTHENTICATE, auth.challenge());
            response
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::Write,
        process::{Command, Stdio},
        sync::{
            OnceLock,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };

    use axum::{
        Extension, Router,
        body::Body,
        http::{Request as HttpRequest, header::AUTHORIZATION},
        middleware,
        routing::get,
    };
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde::Serialize;
    use serde_json::json;
    use tower::ServiceExt;

    use super::*;
    use crate::test_support::mock_http;

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

    async fn no_content() -> StatusCode {
        StatusCode::NO_CONTENT
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
        let key = test_key();
        json!({
            "keys": [{
                "kty": "RSA",
                "kid": KID,
                "use": "sig",
                "alg": "RS256",
                "n": key.modulus,
                "e": key.exponent
            }]
        })
        .to_string()
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
        let mut header = Header::new(Algorithm::RS256);
        header.kid = kid.map(str::to_owned);
        let now = chrono::Utc::now().timestamp();
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

    #[tokio::test]
    async fn authenticates_rs256_tokens_and_caches_jwks() {
        let (base_url, requests) = mock_http(vec![(200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        let metadata = auth.protected_resource_metadata();
        assert_eq!(metadata.resource, "http://localhost:8788/mcp");
        assert_eq!(metadata.authorization_servers.len(), 1);
        assert_eq!(metadata.bearer_methods_supported, ["header"]);
        assert_eq!(metadata.scopes_supported, ["mcp:tools"]);

        let headers = bearer(&token(Some(KID), "ozonofk-mcp", "admin"));
        for _ in 0..2 {
            assert_eq!(auth.authenticate(&headers).await.unwrap().actor_id, "admin");
        }
        assert!(requests.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(requests.try_recv().is_err());
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
        assert!(auth.authenticate(&bearer(&mismatched)).await.is_err());

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
            assert!(auth.authenticate(&bearer(&token)).await.is_err());
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
        assert!(error.to_string().contains("неоднозначно"), "{error}");
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
            assert!(auth.authenticate(&bearer(&token)).await.is_err());
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
        assert!(auth.authenticate(&HeaderMap::new()).await.is_err());

        for value in ["Basic abc", "Bearer ", "Bearer not-a-jwt"] {
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, HeaderValue::from_str(value).unwrap());
            assert!(auth.authenticate(&headers).await.is_err());
        }
        let mut invalid_text = HeaderMap::new();
        invalid_text.insert(
            AUTHORIZATION,
            HeaderValue::from_bytes(b"Bearer \xff").unwrap(),
        );
        assert!(auth.authenticate(&invalid_text).await.is_err());

        let hs_token = encode(
            &Header::new(Algorithm::HS256),
            &json!({"sub": "subject-1"}),
            &EncodingKey::from_secret(b"test"),
        )
        .unwrap();
        assert!(auth.authenticate(&bearer(&hs_token)).await.is_err());
        assert!(
            auth.authenticate(&bearer(&token(None, "ozonofk-mcp", "admin")))
                .await
                .is_err()
        );

        let (base_url, _) = mock_http(vec![(200, jwks()), (200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        assert!(
            auth.authenticate(&bearer(&token(Some(KID), "wrong", "admin")))
                .await
                .is_err()
        );
        assert!(
            auth.authenticate(&bearer(&token(Some(KID), "ozonofk-mcp", "unknown")))
                .await
                .is_err()
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
            assert!(
                auth.authenticate(&bearer(&token(Some(KID), "ozonofk-mcp", "admin")))
                    .await
                    .is_err()
            );
        }

        let auth =
            JwtAuthenticator::new(config("http://127.0.0.1:1".to_owned()), registry()).unwrap();
        assert!(
            auth.authenticate(&bearer(&token(Some(KID), "ozonofk-mcp", "admin")))
                .await
                .is_err()
        );

        let (base_url, _) = mock_http(vec![(200, jwks())]);
        let auth = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        assert!(
            auth.authenticate(&bearer(&token(Some("unknown"), "ozonofk-mcp", "admin")))
                .await
                .is_err()
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

    #[tokio::test]
    async fn middleware_rejects_missing_token_and_forwards_verified_identity() {
        let unauthenticated =
            JwtAuthenticator::new(config("http://127.0.0.1:1".to_owned()), registry()).unwrap();
        let app = Router::new()
            .route("/", get(no_content))
            .route_layer(middleware::from_fn_with_state(unauthenticated, require_jwt));
        let response = app
            .oneshot(HttpRequest::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(no_content().await, StatusCode::NO_CONTENT);
        assert!(
            response
                .headers()
                .get(axum::http::header::WWW_AUTHENTICATE)
                .unwrap()
                .to_str()
                .unwrap()
                .contains("oauth-protected-resource")
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::WWW_AUTHENTICATE)
                .unwrap(),
            "Bearer resource_metadata=\"http://localhost:8788/.well-known/oauth-protected-resource\", scope=\"mcp:tools\""
        );

        let (base_url, _) = mock_http(vec![(200, jwks())]);
        let authenticated = JwtAuthenticator::new(config(base_url), registry()).unwrap();
        let app = Router::new()
            .route(
                "/",
                get(
                    |Extension(actor): Extension<AuthenticatedActor>| async move { actor.actor_id },
                ),
            )
            .route_layer(middleware::from_fn_with_state(authenticated, require_jwt));
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/")
                    .header(
                        AUTHORIZATION,
                        format!("Bearer {}", token(Some(KID), "ozonofk-mcp", "admin")),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
