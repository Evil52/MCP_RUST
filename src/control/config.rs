use std::{
    fs::File,
    io::Read,
    net::SocketAddr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use jsonwebtoken::dangerous::insecure_decode;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use serde::Deserialize;
use tokio_postgres::Config as PostgresConfig;

use crate::{
    config::{AuthMode, JwtConfig, RegistrySource, TransportMode, is_canonical_uuid},
    control::{
        plan::validate_control_database_url,
        policy::{ControlMode, ControlPolicy},
    },
};

const DEFAULT_CONTROL_ACCESS_CONFIG: &str = "config/access.json";
const DEFAULT_CONTROL_POLICY: &str = "config/control-policy.json";
const CONTROL_REQUIRED_SCOPE: &str = "mcp:ads-control";
const CONTROL_INTERNAL_JWKS_URL: &str = "http://control-auth-egress:8080/jwks";
const MAX_CONTROL_CREDENTIAL_BYTES: u64 = 16_384;
const WB_PROMOTION_BIT: u64 = 1 << 6;
const WB_READ_ONLY_BIT: u64 = 1 << 30;

#[derive(Debug, Clone)]
pub enum ControlAuthConfig {
    Dev { actor_id: String },
    Jwt(JwtConfig),
}

#[derive(Debug, Clone)]
pub struct ControlAppConfig {
    pub bind: SocketAddr,
    pub max_sessions: NonZeroUsize,
    pub session_idle_timeout: Duration,
    pub transport: TransportMode,
    pub auth: ControlAuthConfig,
    pub registry: RegistrySource,
    pub policy: ControlPolicy,
    /// Optional restricted store used to persist even a disabled policy
    /// revision as a rollback-prevention tombstone. It contains no WB secret.
    pub policy_database: Option<ControlPolicyDatabaseConfig>,
    pub wb_runtime: Option<ControlWbRuntimeConfig>,
}

#[derive(Clone)]
pub struct ControlPolicyDatabaseConfig {
    pub database: PostgresConfig,
}

impl std::fmt::Debug for ControlPolicyDatabaseConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlPolicyDatabaseConfig")
            .field("database", &"<redacted>")
            .finish()
    }
}

#[derive(Clone)]
pub struct ControlWbRuntimeConfig {
    pub account_id: String,
    pub seller_sid: String,
    /// Dedicated Personal production token with only Promotion read access.
    pub reader_token: String,
    /// Dedicated Personal production token with only Promotion read/write access.
    /// It is absent in `plan_only`, so that process cannot construct a writer.
    pub writer_token: Option<String>,
    pub database: PostgresConfig,
    pub proxy_url: String,
    pub request_timeout: Duration,
}

impl std::fmt::Debug for ControlWbRuntimeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlWbRuntimeConfig")
            .field("account_id", &self.account_id)
            .field("seller_sid", &self.seller_sid)
            .field("reader_token", &"<redacted>")
            .field("writer_token_loaded", &self.writer_token.is_some())
            .field("database", &"<redacted>")
            .field("proxy_url", &self.proxy_url)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

impl ControlAppConfig {
    /// Loads only `CONTROL_MCP_*` variables.
    ///
    /// This function intentionally does not call `dotenvy::dotenv` and never
    /// resolves credential env names from `access.json`. The disabled scaffold
    /// therefore cannot inherit Seller, Performance, or WB keys from the
    /// analytics process environment.
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let bind = value_or(&mut lookup, "CONTROL_MCP_BIND", "127.0.0.1:8790")
            .parse::<SocketAddr>()
            .context("CONTROL_MCP_BIND должен иметь формат IP:PORT")?;
        let transport =
            value_or(&mut lookup, "CONTROL_MCP_TRANSPORT", "http").parse::<TransportMode>()?;
        let max_sessions = parse_max_sessions(lookup("CONTROL_MCP_MAX_SESSIONS").as_deref())?;
        let session_idle_timeout = parse_session_idle_timeout(
            lookup("CONTROL_MCP_SESSION_IDLE_TIMEOUT_SECONDS").as_deref(),
        )?;
        let registry_path = value_or(
            &mut lookup,
            "CONTROL_MCP_ACCESS_CONFIG",
            DEFAULT_CONTROL_ACCESS_CONFIG,
        );
        let registry = RegistrySource::new(registry_path)?;
        let snapshot = registry.load()?;
        let policy_path = value_or(&mut lookup, "CONTROL_MCP_POLICY", DEFAULT_CONTROL_POLICY);
        let policy = ControlPolicy::load(PathBuf::from(policy_path), &snapshot)?;

        let auth_mode =
            value_or(&mut lookup, "CONTROL_MCP_AUTH_MODE", "dev").parse::<AuthMode>()?;
        let auth = match auth_mode {
            AuthMode::Dev => {
                let actor_id = lookup("CONTROL_MCP_ACTOR_ID")
                    .context("CONTROL_MCP_ACTOR_ID обязателен в dev-режиме")?;
                snapshot.actor(&actor_id)?;
                let allow_non_loopback = parse_strict_bool(
                    &value_or(&mut lookup, "CONTROL_MCP_DEV_ALLOW_NON_LOOPBACK", "false"),
                    "CONTROL_MCP_DEV_ALLOW_NON_LOOPBACK",
                )?;
                if transport == TransportMode::Http
                    && !bind.ip().is_loopback()
                    && !allow_non_loopback
                {
                    bail!(
                        "dev Control MCP может слушать non-loopback только при явном CONTROL_MCP_DEV_ALLOW_NON_LOOPBACK=true"
                    );
                }
                ControlAuthConfig::Dev { actor_id }
            }
            AuthMode::Jwt => {
                if transport != TransportMode::Http {
                    bail!("JWT для Control MCP поддерживается только через HTTP");
                }
                ControlAuthConfig::Jwt(load_jwt_config(&mut lookup)?)
            }
        };

        let policy_database = load_policy_database(&mut lookup)?;
        if policy_database.is_some() && !matches!(auth, ControlAuthConfig::Jwt(_)) {
            bail!("Control policy store разрешён только в JWT-режиме");
        }
        let wb_runtime = load_wb_runtime(
            &mut lookup,
            &auth,
            &policy,
            &snapshot,
            policy_database.as_ref(),
        )?;

        Ok(Self {
            bind,
            max_sessions,
            session_idle_timeout,
            transport,
            auth,
            registry,
            policy,
            policy_database,
            wb_runtime,
        })
    }
}

#[derive(Deserialize)]
struct WbControlTokenClaims {
    acc: u8,
    #[serde(rename = "for")]
    token_for: Option<String>,
    t: Option<bool>,
    s: u64,
    exp: u64,
    sid: String,
}

fn load_wb_runtime(
    lookup: &mut dyn FnMut(&str) -> Option<String>,
    auth: &ControlAuthConfig,
    policy: &ControlPolicy,
    registry: &crate::config::AccessRegistry,
    policy_database: Option<&ControlPolicyDatabaseConfig>,
) -> Result<Option<ControlWbRuntimeConfig>> {
    let writes_enabled = parse_strict_bool(
        &value_or(lookup, "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED", "false"),
        "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED",
    )?;
    if policy.mode == ControlMode::Disabled {
        return Ok(None);
    }
    if !matches!(auth, ControlAuthConfig::Jwt(_)) {
        bail!("WB Control runtime разрешён только в JWT-режиме Control MCP");
    }
    let account_id = required_nonempty(lookup, "CONTROL_MCP_WB_ACCOUNT_ID")?;
    let account = registry
        .accounts
        .iter()
        .find(|account| account.id == account_id)
        .context("CONTROL_MCP_WB_ACCOUNT_ID отсутствует в access registry")?;
    if !matches!(account.marketplace, crate::config::Marketplace::Wildberries)
        || account.wildberries.is_none()
    {
        bail!("CONTROL_MCP_WB_ACCOUNT_ID должен ссылаться на Wildberries account");
    }
    let expected_seller_sid = account
        .wildberries
        .as_ref()
        .and_then(|wildberries| wildberries.seller_sid.as_deref())
        .context("WB Control требует reviewed wildberries.seller_sid в access registry")?;
    if !policy.actors.iter().any(|actor| {
        actor
            .wb_promotion_bid_targets
            .iter()
            .any(|target| target.account_id == account_id)
    }) {
        bail!("CONTROL_MCP_WB_ACCOUNT_ID не имеет явных targets в control policy");
    }
    let reader_token_path = PathBuf::from(required_nonempty(
        lookup,
        "CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE",
    )?);
    let reader_token = read_control_token(
        &reader_token_path,
        "CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE",
    )?;
    validate_wb_reader_token(&reader_token, expected_seller_sid)?;
    let writer_token = if policy.mode == ControlMode::Enabled && writes_enabled {
        let writer_token_path = PathBuf::from(required_nonempty(
            lookup,
            "CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE",
        )?);
        let token = read_control_token(
            &writer_token_path,
            "CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE",
        )?;
        validate_wb_writer_token(&token, expected_seller_sid)?;
        Some(token)
    } else {
        None
    };
    let database = policy_database
        .context("CONTROL_MCP_DATABASE_URL обязателен для WB Control runtime")?
        .database
        .clone();
    let proxy_url = required_nonempty(lookup, "CONTROL_MCP_WB_PROXY")?;
    validate_proxy_url(&proxy_url)?;
    let timeout_seconds = value_or(lookup, "CONTROL_MCP_WB_TIMEOUT_SECONDS", "20")
        .parse::<u64>()
        .context("CONTROL_MCP_WB_TIMEOUT_SECONDS должен быть целым числом")?;
    if !(1..=30).contains(&timeout_seconds) {
        bail!("CONTROL_MCP_WB_TIMEOUT_SECONDS должен быть от 1 до 30");
    }
    Ok(Some(ControlWbRuntimeConfig {
        account_id,
        seller_sid: expected_seller_sid.to_owned(),
        reader_token,
        writer_token,
        database,
        proxy_url,
        request_timeout: Duration::from_secs(timeout_seconds),
    }))
}

fn load_policy_database(
    lookup: &mut dyn FnMut(&str) -> Option<String>,
) -> Result<Option<ControlPolicyDatabaseConfig>> {
    let Some(database_url) = lookup("CONTROL_MCP_DATABASE_URL") else {
        return Ok(None);
    };
    if database_url.is_empty() || database_url.trim() != database_url {
        bail!("CONTROL_MCP_DATABASE_URL должен быть непустым URL без внешнего whitespace");
    }
    let database = validate_control_database_url(&database_url).map_err(|_| {
        anyhow::anyhow!(
            "CONTROL_MCP_DATABASE_URL должен использовать restricted role control_writer и один TCP host"
        )
    })?;
    Ok(Some(ControlPolicyDatabaseConfig { database }))
}

fn required_nonempty(lookup: &mut dyn FnMut(&str) -> Option<String>, key: &str) -> Result<String> {
    lookup(key)
        .filter(|value| !value.is_empty() && value.trim() == value)
        .with_context(|| format!("{key} обязателен для WB Control runtime"))
}

fn read_control_token(path: &Path, variable_name: &str) -> Result<String> {
    let file = File::open(path)
        .with_context(|| format!("не удалось прочитать {variable_name} {}", path.display()))?;
    let metadata = file
        .metadata()
        .context("не удалось проверить WB token file")?;
    if !metadata.is_file() || metadata.len() > MAX_CONTROL_CREDENTIAL_BYTES {
        bail!("WB token file должен быть обычным файлом безопасного размера");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0 {
            bail!("WB token file не должен быть доступен group/other (ожидается chmod 600/400)");
        }
    }
    let mut bytes = Vec::new();
    file.take(MAX_CONTROL_CREDENTIAL_BYTES + 1)
        .read_to_end(&mut bytes)
        .context("не удалось прочитать WB token file")?;
    normalize_control_token_bytes(bytes)
}

fn normalize_control_token_bytes(mut bytes: Vec<u8>) -> Result<String> {
    if bytes.len() as u64 > MAX_CONTROL_CREDENTIAL_BYTES {
        bail!("WB token file превышает безопасный лимит");
    }
    if bytes.ends_with(b"\r\n") {
        bytes.truncate(bytes.len() - 2);
    } else if bytes.ends_with(b"\n") {
        bytes.pop();
    }
    let token = String::from_utf8(bytes).context("WB token file должен быть UTF-8")?;
    if token.is_empty()
        || !token.is_ascii()
        || token
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        bail!("WB promotion token имеет недопустимый формат");
    }
    Ok(token)
}

fn decode_wb_control_token(token: &str, purpose: &str) -> Result<WbControlTokenClaims> {
    let claims = insecure_decode::<WbControlTokenClaims>(token)
        .with_context(|| format!("WB promotion {purpose} token должен быть корректным JWT"))?
        .claims;
    if claims.acc != 3 || claims.token_for.as_deref() != Some("self") || claims.t != Some(false) {
        bail!("WB promotion {purpose} token должен быть Personal production token");
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("системное время находится до Unix epoch")?
        .as_secs();
    if claims.exp <= now.saturating_add(300) {
        bail!("WB promotion {purpose} token истёк или истекает менее чем через 5 минут");
    }
    Ok(claims)
}

fn validate_wb_reader_token(token: &str, expected_seller_sid: &str) -> Result<()> {
    let claims = decode_wb_control_token(token, "read")?;
    validate_wb_token_seller(&claims, expected_seller_sid, "read")?;
    if claims.s != (WB_PROMOTION_BIT | WB_READ_ONLY_BIT) {
        bail!(
            "WB promotion read token должен быть узким: только категория Продвижение в режиме чтения"
        );
    }
    Ok(())
}

fn validate_wb_writer_token(token: &str, expected_seller_sid: &str) -> Result<()> {
    let claims = decode_wb_control_token(token, "write")?;
    validate_wb_token_seller(&claims, expected_seller_sid, "write")?;
    if claims.s != WB_PROMOTION_BIT {
        bail!(
            "WB promotion write token должен быть узким: только категория Продвижение с чтением и записью"
        );
    }
    Ok(())
}

fn validate_wb_token_seller(
    claims: &WbControlTokenClaims,
    expected_seller_sid: &str,
    purpose: &str,
) -> Result<()> {
    if !is_canonical_uuid(&claims.sid)
        || !is_canonical_uuid(expected_seller_sid)
        || claims.sid != expected_seller_sid
    {
        bail!("WB promotion {purpose} token принадлежит другому seller sid");
    }
    Ok(())
}

fn validate_proxy_url(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value).context("CONTROL_MCP_WB_PROXY должен быть URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        bail!("CONTROL_MCP_WB_PROXY должен быть origin URL без credentials/path/query/fragment");
    }
    Ok(())
}

fn value_or(lookup: &mut dyn FnMut(&str) -> Option<String>, key: &str, default: &str) -> String {
    lookup(key).unwrap_or_else(|| default.to_owned())
}

fn parse_max_sessions(value: Option<&str>) -> Result<NonZeroUsize> {
    let Some(value) = value else {
        return Ok(LocalSessionManager::DEFAULT_MAX_SESSIONS);
    };
    let parsed = value
        .parse::<NonZeroUsize>()
        .context("CONTROL_MCP_MAX_SESSIONS должен быть положительным целым числом")?;
    if parsed > LocalSessionManager::DEFAULT_MAX_SESSIONS {
        bail!("CONTROL_MCP_MAX_SESSIONS не может превышать встроенный лимит");
    }
    Ok(parsed)
}

fn parse_session_idle_timeout(value: Option<&str>) -> Result<Duration> {
    let seconds = value
        .unwrap_or("120")
        .parse::<u64>()
        .context("CONTROL_MCP_SESSION_IDLE_TIMEOUT_SECONDS должен быть целым числом")?;
    if !(90..=300).contains(&seconds) {
        bail!("CONTROL_MCP_SESSION_IDLE_TIMEOUT_SECONDS должен быть от 90 до 300");
    }
    Ok(Duration::from_secs(seconds))
}

fn parse_strict_bool(value: &str, name: &str) -> Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => bail!("{name} должен быть строго true или false"),
    }
}

fn load_jwt_config(lookup: &mut dyn FnMut(&str) -> Option<String>) -> Result<JwtConfig> {
    let issuer = required(lookup, "CONTROL_MCP_JWT_ISSUER")?
        .trim_end_matches('/')
        .to_owned();
    validate_https_url("CONTROL_MCP_JWT_ISSUER", &issuer)?;
    let resource_url = normalize_url(
        "CONTROL_MCP_PUBLIC_URL",
        &required(lookup, "CONTROL_MCP_PUBLIC_URL")?,
    )?;
    let audience = normalize_url(
        "CONTROL_MCP_JWT_AUDIENCE",
        &required(lookup, "CONTROL_MCP_JWT_AUDIENCE")?,
    )?;
    if audience != resource_url {
        bail!("CONTROL_MCP_JWT_AUDIENCE должен точно совпадать с CONTROL_MCP_PUBLIC_URL");
    }
    let jwks_url = lookup("CONTROL_MCP_JWT_JWKS_URL")
        .unwrap_or_else(|| format!("{issuer}/protocol/openid-connect/certs"));
    validate_jwks_url(&jwks_url)?;
    let scopes = value_or(
        lookup,
        "CONTROL_MCP_JWT_REQUIRED_SCOPES",
        CONTROL_REQUIRED_SCOPE,
    );
    if scopes != CONTROL_REQUIRED_SCOPE {
        bail!(
            "CONTROL_MCP_JWT_REQUIRED_SCOPES должен быть ровно {CONTROL_REQUIRED_SCOPE}; analytics scope не подходит"
        );
    }
    let ttl = value_or(lookup, "CONTROL_MCP_JWKS_CACHE_TTL_SECONDS", "300")
        .parse::<u64>()
        .context("CONTROL_MCP_JWKS_CACHE_TTL_SECONDS должен быть целым числом")?;
    if !(30..=86_400).contains(&ttl) {
        bail!("CONTROL_MCP_JWKS_CACHE_TTL_SECONDS должен быть от 30 до 86400");
    }
    let mut metadata_url =
        reqwest::Url::parse(&resource_url).expect("normalized control public URL remains valid");
    metadata_url.set_path("/.well-known/oauth-protected-resource");
    metadata_url.set_query(None);
    metadata_url.set_fragment(None);
    Ok(JwtConfig {
        issuer,
        audience,
        jwks_url,
        resource_url,
        resource_metadata_url: metadata_url.to_string(),
        required_scopes: vec![CONTROL_REQUIRED_SCOPE.to_owned()],
        jwks_cache_ttl: Duration::from_secs(ttl),
    })
}

fn required(lookup: &mut dyn FnMut(&str) -> Option<String>, key: &str) -> Result<String> {
    lookup(key).with_context(|| format!("{key} обязателен в JWT-режиме"))
}

fn validate_https_url(name: &str, value: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(value).with_context(|| format!("{name} должен быть URL"))?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        bail!("{name} должен быть абсолютным HTTPS URL без credentials/query/fragment");
    }
    Ok(())
}

fn validate_jwks_url(value: &str) -> Result<()> {
    if value == CONTROL_INTERNAL_JWKS_URL {
        return Ok(());
    }
    validate_https_url("CONTROL_MCP_JWT_JWKS_URL", value).with_context(|| {
        format!(
            "CONTROL_MCP_JWT_JWKS_URL должен использовать HTTPS или точный внутренний адрес {CONTROL_INTERNAL_JWKS_URL}"
        )
    })
}

fn normalize_url(name: &str, value: &str) -> Result<String> {
    validate_https_url(name, value)?;
    Ok(reqwest::Url::parse(value)
        .expect("validated URL parses")
        .to_string())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        fs,
        sync::{
            Mutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

    use super::*;

    static FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
    const TEST_WB_SELLER_SID: &str = "123e4567-e89b-42d3-a456-426614174000";
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct ProcessEnvGuard {
        previous: Vec<(&'static str, Option<OsString>)>,
    }

    impl ProcessEnvGuard {
        fn isolated(keys: &[&'static str]) -> Self {
            let previous = keys
                .iter()
                .map(|key| (*key, std::env::var_os(key)))
                .collect();
            for key in keys {
                // SAFETY: this module serializes its process-environment test
                // with `ENV_LOCK`; production code only reads these variables.
                unsafe { std::env::remove_var(key) };
            }
            Self { previous }
        }

        fn set(&self, key: &'static str, value: impl AsRef<std::ffi::OsStr>) {
            assert!(self.previous.iter().any(|(known, _)| *known == key));
            // SAFETY: see `isolated`; the guard restores every touched value.
            unsafe { std::env::set_var(key, value) };
        }
    }

    impl Drop for ProcessEnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.previous.drain(..) {
                // SAFETY: see `isolated`; restoration also runs during unwind.
                unsafe {
                    if let Some(value) = value {
                        std::env::set_var(key, value);
                    } else {
                        std::env::remove_var(key);
                    }
                }
            }
        }
    }

    struct Fixtures {
        registry: PathBuf,
        policy: PathBuf,
    }

    impl Fixtures {
        fn new() -> Self {
            let id = FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir();
            let registry = root.join(format!("mcp-control-registry-{id}.json"));
            let policy = root.join(format!("mcp-control-policy-{id}.json"));
            fs::write(
                &registry,
                serde_json::to_vec(&serde_json::json!({
                    "version": 1,
                    "actors": [{
                        "id": "manager",
                        "name": "Manager",
                        "role": "manager",
                        "oidc": { "username": "manager" }
                    }, {
                        "id": "approver",
                        "name": "Approver",
                        "role": "finance",
                        "oidc": { "username": "approver" }
                    }],
                    "accounts": [{
                        "id": "ozon_one",
                        "organization": "Example",
                        "marketplace": "ozon",
                        "seller_client_id": "seller",
                        "manager_id": "manager",
                        "ozon": {
                            "store_id": "store_one",
                            "client_id_env": "UNUSED_CLIENT_ID",
                            "api_key_env": "UNUSED_API_KEY",
                            "performance": {
                                "client_id_env": "UNUSED_PERF_ID",
                                "client_secret_env": "UNUSED_PERF_SECRET"
                            }
                        }
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
            fs::write(
                &policy,
                serde_json::to_vec(&serde_json::json!({
                    "version": 1,
                    "revision": 1,
                    "mode": "disabled",
                    "actors": [{ "actor_id": "manager", "targets": [] }]
                }))
                .unwrap(),
            )
            .unwrap();
            Self { registry, policy }
        }

        fn values(&self) -> BTreeMap<String, String> {
            BTreeMap::from([
                (
                    "CONTROL_MCP_ACCESS_CONFIG".to_owned(),
                    self.registry.display().to_string(),
                ),
                (
                    "CONTROL_MCP_POLICY".to_owned(),
                    self.policy.display().to_string(),
                ),
                ("CONTROL_MCP_ACTOR_ID".to_owned(), "manager".to_owned()),
            ])
        }

        fn configure_wb(&self, mode: &str) {
            fs::write(
                &self.registry,
                serde_json::to_vec(&serde_json::json!({
                    "version": 1,
                    "actors": [{
                        "id": "manager",
                        "name": "Manager",
                        "role": "manager",
                        "oidc": { "username": "manager" }
                    }, {
                        "id": "approver",
                        "name": "Approver",
                        "role": "finance",
                        "account_ids": ["wb_one"],
                        "oidc": { "username": "approver" }
                    }],
                    "accounts": [{
                        "id": "wb_one",
                        "organization": "Example",
                        "marketplace": "wildberries",
                        "seller_client_id": "seller",
                        "manager_id": "manager",
                        "wildberries": {
                            "api_token_env": "UNUSED_WB_TOKEN",
                            "seller_sid": TEST_WB_SELLER_SID
                        }
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
            fs::write(
                &self.policy,
                serde_json::to_vec(&serde_json::json!({
                    "version": 1,
                    "revision": 1,
                    "mode": mode,
                    "actors": [{
                        "actor_id": "manager",
                        "targets": [],
                        "wb_promotion_bid_targets": [{
                            "account_id": "wb_one",
                            "seller_sid": TEST_WB_SELLER_SID,
                            "advert_id": 42,
                            "nm_ids": [1001],
                            "placements": ["search"],
                            "bid_limits_kopecks": {
                                "min_minor": 100,
                                "max_minor": 5000,
                                "max_delta_percent": 5
                            },
                            "approver_actor_ids": ["approver"],
                            "action_limits": {
                                "max_actions_per_hour": 4,
                                "max_actions_per_day": 12,
                                "cooldown_seconds": 900,
                                "max_cumulative_abs_delta_kopecks_per_day": 5000
                            }
                        }]
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
        }
    }

    impl Drop for Fixtures {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.registry);
            let _ = fs::remove_file(&self.policy);
        }
    }

    struct TempCredential(PathBuf);

    impl TempCredential {
        fn new(label: &str, token: &str) -> Self {
            let id = FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("mcp-control-{label}-{id}.token"));
            fs::write(&path, token).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            }
            Self(path)
        }

        fn display(&self) -> String {
            self.0.display().to_string()
        }
    }

    impl Drop for TempCredential {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn from(values: &BTreeMap<String, String>) -> Result<ControlAppConfig> {
        ControlAppConfig::from_lookup(|key| values.get(key).cloned())
    }

    #[test]
    fn defaults_are_disabled_and_do_not_request_marketplace_credentials() {
        let fixtures = Fixtures::new();
        let values = fixtures.values();
        let mut requested = Vec::new();
        let config = ControlAppConfig::from_lookup(|key| {
            requested.push(key.to_owned());
            values.get(key).cloned()
        })
        .expect("disabled control config");

        assert_eq!(config.bind, "127.0.0.1:8790".parse().unwrap());
        assert_eq!(config.policy.mode, crate::control::ControlMode::Disabled);
        assert!(matches!(config.auth, ControlAuthConfig::Dev { .. }));
        assert!(requested.iter().all(|key| {
            key.starts_with("CONTROL_MCP_")
                && !key.contains("API_KEY")
                && !key.contains("CLIENT_SECRET")
                && !key.contains("API_TOKEN")
        }));
    }

    #[test]
    fn process_environment_loader_reads_only_the_control_namespace() {
        let _lock = ENV_LOCK.lock().unwrap();
        let fixtures = Fixtures::new();
        let guard = ProcessEnvGuard::isolated(&[
            "CONTROL_MCP_ACCESS_CONFIG",
            "CONTROL_MCP_POLICY",
            "CONTROL_MCP_ACTOR_ID",
            "CONTROL_MCP_BIND",
            "CONTROL_MCP_TRANSPORT",
            "CONTROL_MCP_MAX_SESSIONS",
            "CONTROL_MCP_SESSION_IDLE_TIMEOUT_SECONDS",
            "CONTROL_MCP_AUTH_MODE",
            "CONTROL_MCP_DEV_ALLOW_NON_LOOPBACK",
            "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED",
            "CONTROL_MCP_WB_ACCOUNT_ID",
            "CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE",
            "CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE",
            "CONTROL_MCP_DATABASE_URL",
            "CONTROL_MCP_WB_PROXY",
            "CONTROL_MCP_WB_TIMEOUT_SECONDS",
        ]);
        guard.set("CONTROL_MCP_ACCESS_CONFIG", &fixtures.registry);
        guard.set("CONTROL_MCP_POLICY", &fixtures.policy);
        guard.set("CONTROL_MCP_ACTOR_ID", "manager");

        let config = ControlAppConfig::from_env().expect("process control environment");
        assert!(matches!(
            config.auth,
            ControlAuthConfig::Dev { ref actor_id } if actor_id == "manager"
        ));
        assert_eq!(config.policy.mode, crate::control::ControlMode::Disabled);
    }

    #[test]
    fn process_environment_guard_restores_a_preexisting_value() {
        let _lock = ENV_LOCK.lock().unwrap();
        let outer = ProcessEnvGuard::isolated(&["CONTROL_MCP_BIND"]);
        // SAFETY: both guards run while the process-environment test lock is held,
        // and `outer` restores the original value at the end of the test.
        unsafe { std::env::set_var("CONTROL_MCP_BIND", "127.0.0.1:8799") };
        {
            let inner = ProcessEnvGuard::isolated(&["CONTROL_MCP_BIND"]);
            inner.set("CONTROL_MCP_BIND", "127.0.0.1:8790");
        }
        assert_eq!(
            std::env::var("CONTROL_MCP_BIND").as_deref(),
            Ok("127.0.0.1:8799")
        );
        drop(outer);
    }

    #[test]
    fn dev_non_loopback_requires_explicit_opt_in_and_bounds_sessions() {
        let fixtures = Fixtures::new();
        let mut values = fixtures.values();
        values.insert("CONTROL_MCP_BIND".to_owned(), "0.0.0.0:8790".to_owned());
        assert!(from(&values).is_err());
        values.insert(
            "CONTROL_MCP_DEV_ALLOW_NON_LOOPBACK".to_owned(),
            "true".to_owned(),
        );
        assert!(from(&values).is_ok());
        values.insert("CONTROL_MCP_MAX_SESSIONS".to_owned(), "999999".to_owned());
        assert!(from(&values).is_err());
    }

    #[test]
    fn explicit_session_bounds_and_strict_boolean_are_enforced() {
        let fixtures = Fixtures::new();
        let mut values = fixtures.values();
        values.insert("CONTROL_MCP_MAX_SESSIONS".to_owned(), "1".to_owned());
        values.insert(
            "CONTROL_MCP_SESSION_IDLE_TIMEOUT_SECONDS".to_owned(),
            "90".to_owned(),
        );
        let config = from(&values).expect("lower session bounds are valid");
        assert_eq!(config.max_sessions.get(), 1);
        assert_eq!(config.session_idle_timeout, Duration::from_secs(90));

        for timeout in ["89", "301", "not-a-number"] {
            values.insert(
                "CONTROL_MCP_SESSION_IDLE_TIMEOUT_SECONDS".to_owned(),
                timeout.to_owned(),
            );
            assert!(from(&values).is_err(), "timeout {timeout} must fail");
        }

        values.insert(
            "CONTROL_MCP_SESSION_IDLE_TIMEOUT_SECONDS".to_owned(),
            "120".to_owned(),
        );
        values.insert(
            "CONTROL_MCP_DEV_ALLOW_NON_LOOPBACK".to_owned(),
            "TRUE".to_owned(),
        );
        assert!(from(&values).is_err());
    }

    #[test]
    fn jwt_uses_a_separate_exact_scope_and_http_transport() {
        let fixtures = Fixtures::new();
        let mut values = fixtures.values();
        values.extend([
            ("CONTROL_MCP_AUTH_MODE".to_owned(), "jwt".to_owned()),
            (
                "CONTROL_MCP_JWT_ISSUER".to_owned(),
                "https://auth.example.test/realms/ofk".to_owned(),
            ),
            (
                "CONTROL_MCP_PUBLIC_URL".to_owned(),
                "https://control.example.test/mcp".to_owned(),
            ),
            (
                "CONTROL_MCP_JWT_AUDIENCE".to_owned(),
                "https://control.example.test/mcp".to_owned(),
            ),
        ]);
        let config = from(&values).expect("valid control JWT config");
        assert!(matches!(
            config.auth,
            ControlAuthConfig::Jwt(JwtConfig { ref required_scopes, .. })
                if required_scopes == &[CONTROL_REQUIRED_SCOPE]
        ));

        values.insert(
            "CONTROL_MCP_JWT_REQUIRED_SCOPES".to_owned(),
            "mcp:tools".to_owned(),
        );
        assert!(from(&values).is_err());
        values.insert(
            "CONTROL_MCP_JWT_REQUIRED_SCOPES".to_owned(),
            CONTROL_REQUIRED_SCOPE.to_owned(),
        );
        values.insert("CONTROL_MCP_TRANSPORT".to_owned(), "stdio".to_owned());
        assert!(from(&values).is_err());
    }

    fn jwt_values(fixtures: &Fixtures) -> BTreeMap<String, String> {
        let mut values = fixtures.values();
        values.extend([
            ("CONTROL_MCP_AUTH_MODE".to_owned(), "jwt".to_owned()),
            (
                "CONTROL_MCP_JWT_ISSUER".to_owned(),
                "https://auth.example.test/realms/ofk".to_owned(),
            ),
            (
                "CONTROL_MCP_PUBLIC_URL".to_owned(),
                "https://control.example.test/mcp".to_owned(),
            ),
            (
                "CONTROL_MCP_JWT_AUDIENCE".to_owned(),
                "https://control.example.test/mcp".to_owned(),
            ),
        ]);
        values
    }

    #[test]
    fn jwt_requires_both_resource_urls_and_an_exact_audience() {
        let fixtures = Fixtures::new();

        let mut missing_public = jwt_values(&fixtures);
        missing_public.remove("CONTROL_MCP_PUBLIC_URL");
        assert!(from(&missing_public).is_err());

        let mut missing_audience = jwt_values(&fixtures);
        missing_audience.remove("CONTROL_MCP_JWT_AUDIENCE");
        assert!(from(&missing_audience).is_err());

        let mut mismatched = jwt_values(&fixtures);
        mismatched.insert(
            "CONTROL_MCP_JWT_AUDIENCE".to_owned(),
            "https://other.example.test/mcp".to_owned(),
        );
        assert!(from(&mismatched).is_err());
    }

    #[test]
    fn disabled_jwt_runtime_keeps_restricted_policy_store_without_loading_wb_tokens() {
        let fixtures = Fixtures::new();
        let mut values = jwt_values(&fixtures);
        values.insert(
            "CONTROL_MCP_DATABASE_URL".to_owned(),
            "postgresql://control_writer:secret@position-db:5432/ozon_positions".to_owned(),
        );
        let config = from(&values).expect("disabled JWT policy store");
        assert!(config.policy_database.is_some());
        assert!(config.wb_runtime.is_none());

        let mut dev_values = fixtures.values();
        dev_values.insert(
            "CONTROL_MCP_DATABASE_URL".to_owned(),
            "postgresql://control_writer:secret@position-db:5432/ozon_positions".to_owned(),
        );
        assert!(from(&dev_values).is_err());
    }

    #[test]
    fn jwt_cache_ttl_and_all_http_urls_fail_closed() {
        let fixtures = Fixtures::new();

        for ttl in ["29", "86401", "not-a-number"] {
            let mut values = jwt_values(&fixtures);
            values.insert(
                "CONTROL_MCP_JWKS_CACHE_TTL_SECONDS".to_owned(),
                ttl.to_owned(),
            );
            assert!(from(&values).is_err(), "TTL {ttl} must fail");
        }

        for invalid_url in [
            "ftp://auth.example.test/keys",
            "https://user@auth.example.test/keys",
            "https://user:password@auth.example.test/keys",
            "https://auth.example.test/keys#fragment",
            "not a URL",
        ] {
            let mut values = jwt_values(&fixtures);
            values.insert(
                "CONTROL_MCP_JWT_JWKS_URL".to_owned(),
                invalid_url.to_owned(),
            );
            assert!(from(&values).is_err(), "URL {invalid_url} must fail");
        }

        for key in ["CONTROL_MCP_PUBLIC_URL", "CONTROL_MCP_JWT_AUDIENCE"] {
            let mut values = jwt_values(&fixtures);
            values.insert(key.to_owned(), "not a URL".to_owned());
            assert!(from(&values).is_err(), "{key} must reject an invalid URL");
        }
    }

    #[test]
    fn jwt_trust_urls_require_tls_or_the_exact_internal_jwks_proxy() {
        let fixtures = Fixtures::new();

        let mut internal = jwt_values(&fixtures);
        internal.insert(
            "CONTROL_MCP_JWT_JWKS_URL".to_owned(),
            CONTROL_INTERNAL_JWKS_URL.to_owned(),
        );
        let config = from(&internal).expect("exact internal TLS-verifying JWKS proxy");
        assert!(matches!(
            config.auth,
            ControlAuthConfig::Jwt(JwtConfig { ref jwks_url, .. })
                if jwks_url == CONTROL_INTERNAL_JWKS_URL
        ));

        let mut plaintext_issuer = jwt_values(&fixtures);
        plaintext_issuer.insert(
            "CONTROL_MCP_JWT_ISSUER".to_owned(),
            "http://auth.example.test/realms/ofk".to_owned(),
        );
        assert!(from(&plaintext_issuer).is_err());

        let mut plaintext_resource = jwt_values(&fixtures);
        for key in ["CONTROL_MCP_PUBLIC_URL", "CONTROL_MCP_JWT_AUDIENCE"] {
            plaintext_resource.insert(key.to_owned(), "http://control.example.test/mcp".to_owned());
        }
        assert!(from(&plaintext_resource).is_err());

        for lookalike in [
            "http://control-auth-egress/jwks",
            "http://control-auth-egress:8081/jwks",
            "http://control-auth-egress.example.test:8080/jwks",
            "http://control-auth-egress:8080/jwks/",
            "http://control-auth-egress:8080/jwks?next=https://evil.example",
            "http://user@control-auth-egress:8080/jwks",
            "http://127.0.0.1:8080/jwks",
        ] {
            let mut values = jwt_values(&fixtures);
            values.insert("CONTROL_MCP_JWT_JWKS_URL".to_owned(), lookalike.to_owned());
            assert!(
                from(&values).is_err(),
                "JWKS lookalike {lookalike} must fail"
            );
        }

        for (key, value) in [
            (
                "CONTROL_MCP_JWT_ISSUER",
                "https://auth.example.test/realms/ofk?tenant=other",
            ),
            (
                "CONTROL_MCP_JWT_JWKS_URL",
                "https://auth.example.test/keys?tenant=other",
            ),
        ] {
            let mut values = jwt_values(&fixtures);
            values.insert(key.to_owned(), value.to_owned());
            assert!(from(&values).is_err(), "{key} query must fail");
        }
    }

    fn wb_token(acc: u8, mask: u64, exp: u64) -> String {
        wb_token_with_sid(acc, mask, exp, serde_json::json!(TEST_WB_SELLER_SID))
    }

    fn wb_token_with_sid(acc: u8, mask: u64, exp: u64, sid: serde_json::Value) -> String {
        wb_token_from_claims(serde_json::json!({
            "acc": acc,
            "for": "self",
            "t": false,
            "s": mask,
            "exp": exp,
            "sid": sid
        }))
    }

    fn wb_token_from_claims(claims: serde_json::Value) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
        let signature = URL_SAFE_NO_PAD.encode([0_u8; 64]);
        format!("{header}.{payload}.{signature}")
    }

    fn wb_token_without_sid(mask: u64, exp: u64) -> String {
        wb_token_from_claims(serde_json::json!({
            "acc": 3,
            "for": "self",
            "t": false,
            "s": mask,
            "exp": exp
        }))
    }

    fn wb_runtime_values(
        fixtures: &Fixtures,
        reader_token: &TempCredential,
    ) -> BTreeMap<String, String> {
        let mut values = jwt_values(fixtures);
        values.extend([
            ("CONTROL_MCP_WB_ACCOUNT_ID".to_owned(), "wb_one".to_owned()),
            (
                "CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE".to_owned(),
                reader_token.display(),
            ),
            (
                "CONTROL_MCP_DATABASE_URL".to_owned(),
                "postgresql://control_writer:secret@position-db:5432/ozon_positions".to_owned(),
            ),
            (
                "CONTROL_MCP_WB_PROXY".to_owned(),
                "http://wb-control-egress:3128".to_owned(),
            ),
        ]);
        values
    }

    #[test]
    fn plan_only_loads_exact_read_token_without_reading_or_loading_writer_secret() {
        let fixtures = Fixtures::new();
        fixtures.configure_wb("plan_only");
        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let reader = TempCredential::new(
            "reader",
            &wb_token(3, WB_PROMOTION_BIT | WB_READ_ONLY_BIT, future),
        );
        let mut values = wb_runtime_values(&fixtures, &reader);
        // A stale deployment may still set the old write gate and a write path.
        // Plan-only must not even look up the write credential.
        values.insert(
            "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED".to_owned(),
            "true".to_owned(),
        );
        values.insert(
            "CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE".to_owned(),
            "/must/not/be/read".to_owned(),
        );
        let mut requested = Vec::new();
        let config = ControlAppConfig::from_lookup(|key| {
            requested.push(key.to_owned());
            values.get(key).cloned()
        })
        .expect("plan-only WB runtime");

        let runtime = config.wb_runtime.expect("WB planner runtime");
        assert!(runtime.writer_token.is_none());
        assert_eq!(runtime.reader_token, fs::read_to_string(&reader.0).unwrap());
        assert!(
            !requested
                .iter()
                .any(|key| key == "CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE")
        );
        assert!(
            !requested
                .iter()
                .any(|key| key == "CONTROL_MCP_WB_PROMOTION_TOKEN_FILE")
        );
    }

    #[test]
    fn enabled_executor_requires_a_distinct_exact_write_token() {
        let fixtures = Fixtures::new();
        fixtures.configure_wb("enabled");
        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let reader = TempCredential::new(
            "reader",
            &wb_token(3, WB_PROMOTION_BIT | WB_READ_ONLY_BIT, future),
        );
        let writer = TempCredential::new("writer", &wb_token(3, WB_PROMOTION_BIT, future));
        let mut values = wb_runtime_values(&fixtures, &reader);
        values.insert(
            "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED".to_owned(),
            "true".to_owned(),
        );

        assert!(
            from(&values).is_err(),
            "missing write token must fail closed"
        );
        values.insert(
            "CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE".to_owned(),
            reader.display(),
        );
        assert!(
            from(&values).is_err(),
            "read-only token cannot become a writer"
        );
        values.insert(
            "CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE".to_owned(),
            writer.display(),
        );
        let config = from(&values).expect("enabled executor with split credentials");
        let runtime = config.wb_runtime.expect("WB executor runtime");
        assert_eq!(runtime.reader_token, fs::read_to_string(&reader.0).unwrap());
        assert_eq!(
            runtime.writer_token.as_deref(),
            Some(fs::read_to_string(&writer.0).unwrap().as_str())
        );
    }

    #[test]
    fn enabled_policy_without_write_gate_keeps_reader_but_has_no_writer() {
        let fixtures = Fixtures::new();
        fixtures.configure_wb("enabled");
        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let reader = TempCredential::new(
            "reader",
            &wb_token(3, WB_PROMOTION_BIT | WB_READ_ONLY_BIT, future),
        );
        let values = wb_runtime_values(&fixtures, &reader);
        let runtime = from(&values)
            .expect("read-only runtime")
            .wb_runtime
            .expect("WB reader runtime");
        assert!(runtime.writer_token.is_none());
    }

    #[test]
    fn wb_runtime_requires_reviewed_registry_seller_sid() {
        let fixtures = Fixtures::new();
        fixtures.configure_wb("plan_only");
        let mut registry: serde_json::Value =
            serde_json::from_slice(&fs::read(&fixtures.registry).unwrap()).unwrap();
        registry["accounts"][0]["wildberries"]
            .as_object_mut()
            .unwrap()
            .remove("seller_sid");
        fs::write(&fixtures.registry, serde_json::to_vec(&registry).unwrap()).unwrap();
        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let reader = TempCredential::new(
            "reader-missing-sid",
            &wb_token(3, WB_PROMOTION_BIT | WB_READ_ONLY_BIT, future),
        );
        let error = from(&wb_runtime_values(&fixtures, &reader))
            .unwrap_err()
            .to_string();
        assert!(error.contains("seller_sid"), "{error}");
    }

    #[test]
    fn wb_tokens_must_be_personal_narrow_and_have_exact_access_mode() {
        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let reader = wb_token(3, WB_PROMOTION_BIT | WB_READ_ONLY_BIT, future);
        let writer = wb_token(3, WB_PROMOTION_BIT, future);
        assert!(validate_wb_reader_token(&reader, TEST_WB_SELLER_SID).is_ok());
        assert!(validate_wb_writer_token(&writer, TEST_WB_SELLER_SID).is_ok());
        assert!(validate_wb_reader_token(&writer, TEST_WB_SELLER_SID).is_err());
        assert!(validate_wb_writer_token(&reader, TEST_WB_SELLER_SID).is_err());
        assert!(
            validate_wb_reader_token(
                &wb_token(3, WB_PROMOTION_BIT | WB_READ_ONLY_BIT | (1 << 3), future),
                TEST_WB_SELLER_SID
            )
            .is_err()
        );
        assert!(
            validate_wb_writer_token(
                &wb_token(3, WB_PROMOTION_BIT | (1 << 3), future),
                TEST_WB_SELLER_SID
            )
            .is_err()
        );
        assert!(
            validate_wb_writer_token(
                &wb_token(3, WB_PROMOTION_BIT | (1 << 63), future),
                TEST_WB_SELLER_SID
            )
            .is_err()
        );
        assert!(
            validate_wb_writer_token(&wb_token(1, WB_PROMOTION_BIT, future), TEST_WB_SELLER_SID)
                .is_err()
        );
        let other_seller = wb_token_with_sid(
            3,
            WB_PROMOTION_BIT,
            future,
            serde_json::json!("123e4567-e89b-42d3-a456-426614174001"),
        );
        assert!(validate_wb_writer_token(&other_seller, TEST_WB_SELLER_SID).is_err());
        assert!(
            validate_wb_writer_token(
                &wb_token_without_sid(WB_PROMOTION_BIT, future),
                TEST_WB_SELLER_SID
            )
            .is_err()
        );
        let missing_seller =
            wb_token_with_sid(3, WB_PROMOTION_BIT, future, serde_json::Value::Null);
        assert!(validate_wb_writer_token(&missing_seller, TEST_WB_SELLER_SID).is_err());
        let ill_typed_seller =
            wb_token_with_sid(3, WB_PROMOTION_BIT, future, serde_json::json!(4_389_764));
        assert!(validate_wb_writer_token(&ill_typed_seller, TEST_WB_SELLER_SID).is_err());
        assert!(
            validate_wb_writer_token(&wb_token(3, WB_PROMOTION_BIT, 1), TEST_WB_SELLER_SID)
                .is_err()
        );
        for invalid_identity_claims in [
            serde_json::json!({
                "acc": 3, "for": "organization", "t": false,
                "s": WB_PROMOTION_BIT, "exp": future, "sid": TEST_WB_SELLER_SID
            }),
            serde_json::json!({
                "acc": 3, "t": false,
                "s": WB_PROMOTION_BIT, "exp": future, "sid": TEST_WB_SELLER_SID
            }),
            serde_json::json!({
                "acc": 3, "for": "self", "t": true,
                "s": WB_PROMOTION_BIT, "exp": future, "sid": TEST_WB_SELLER_SID
            }),
            serde_json::json!({
                "acc": 3, "for": "self",
                "s": WB_PROMOTION_BIT, "exp": future, "sid": TEST_WB_SELLER_SID
            }),
        ] {
            assert!(
                validate_wb_writer_token(
                    &wb_token_from_claims(invalid_identity_claims),
                    TEST_WB_SELLER_SID,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn control_runtime_debug_output_redacts_every_credential() {
        let database = "postgresql://control_writer:secret@position-db:5432/ozon_positions"
            .parse::<PostgresConfig>()
            .unwrap();
        let policy_database = ControlPolicyDatabaseConfig {
            database: database.clone(),
        };
        let policy_debug = format!("{policy_database:?}");
        assert!(policy_debug.contains("<redacted>"));
        assert!(!policy_debug.contains("secret"));

        let runtime = ControlWbRuntimeConfig {
            account_id: "wb_one".to_owned(),
            seller_sid: TEST_WB_SELLER_SID.to_owned(),
            reader_token: "reader-secret".to_owned(),
            writer_token: Some("writer-secret".to_owned()),
            database,
            proxy_url: "http://wb-control-egress:3128".to_owned(),
            request_timeout: Duration::from_secs(20),
        };
        let runtime_debug = format!("{runtime:?}");
        assert!(runtime_debug.contains("writer_token_loaded: true"));
        assert!(runtime_debug.contains(TEST_WB_SELLER_SID));
        assert!(!runtime_debug.contains("reader-secret"));
        assert!(!runtime_debug.contains("writer-secret"));
        assert!(!runtime_debug.contains("secret@"));
    }

    #[test]
    fn token_file_normalization_is_bounded_and_exact() {
        assert_eq!(
            normalize_control_token_bytes(b"token\r\n".to_vec()).unwrap(),
            "token"
        );
        assert_eq!(
            normalize_control_token_bytes(b"token\n".to_vec()).unwrap(),
            "token"
        );
        for invalid in [Vec::new(), b"two tokens".to_vec(), vec![0xff]] {
            assert!(normalize_control_token_bytes(invalid).is_err());
        }
        assert!(
            normalize_control_token_bytes(vec![
                b'a';
                usize::try_from(MAX_CONTROL_CREDENTIAL_BYTES)
                    .expect("credential limit fits usize")
                    + 1
            ])
            .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn token_files_must_be_regular_private_and_readable() {
        use std::os::unix::fs::PermissionsExt;

        let id = FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!("mcp-control-token-dir-{id}"));
        fs::create_dir(&directory).unwrap();
        assert!(read_control_token(&directory, "TEST_TOKEN").is_err());
        fs::remove_dir(&directory).unwrap();

        let public = TempCredential::new("public", "token");
        fs::set_permissions(&public.0, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_control_token(&public.0, "TEST_TOKEN").is_err());

        let oversized = TempCredential::new(
            "oversized",
            &"a".repeat(
                usize::try_from(MAX_CONTROL_CREDENTIAL_BYTES).expect("credential limit fits usize")
                    + 1,
            ),
        );
        assert!(read_control_token(&oversized.0, "TEST_TOKEN").is_err());

        let missing = std::env::temp_dir().join(format!("mcp-control-missing-token-{id}"));
        assert!(read_control_token(&missing, "TEST_TOKEN").is_err());
    }

    #[test]
    fn database_and_proxy_inputs_fail_closed_on_every_unsafe_shape() {
        for database_url in [
            " postgresql://control_writer:secret@position-db:5432/ozon_positions",
            "postgresql://wrong_role:secret@position-db:5432/ozon_positions",
        ] {
            let mut lookup =
                |key: &str| (key == "CONTROL_MCP_DATABASE_URL").then(|| database_url.to_owned());
            assert!(load_policy_database(&mut lookup).is_err());
        }

        for proxy in [
            "not a URL",
            "ftp://wb-control-egress:3128",
            "http://user@wb-control-egress:3128",
            "http://:password@wb-control-egress:3128",
            "http://wb-control-egress:3128/path",
            "http://wb-control-egress:3128/?query=1",
            "http://wb-control-egress:3128/#fragment",
        ] {
            assert!(
                validate_proxy_url(proxy).is_err(),
                "proxy {proxy} must fail"
            );
        }
    }

    #[test]
    fn wb_runtime_refuses_dev_auth_invalid_gate_and_missing_scope() {
        let fixtures = Fixtures::new();
        fixtures.configure_wb("plan_only");

        let mut invalid_gate = jwt_values(&fixtures);
        invalid_gate.insert(
            "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED".to_owned(),
            "TRUE".to_owned(),
        );
        assert!(from(&invalid_gate).is_err());

        assert!(
            from(&fixtures.values()).is_err(),
            "non-disabled WB runtime must require JWT auth"
        );

        let mut policy: serde_json::Value =
            serde_json::from_slice(&fs::read(&fixtures.policy).unwrap()).unwrap();
        policy["actors"][0]["wb_promotion_bid_targets"] = serde_json::json!([]);
        fs::write(&fixtures.policy, serde_json::to_vec(&policy).unwrap()).unwrap();
        let mut no_scope = jwt_values(&fixtures);
        no_scope.insert("CONTROL_MCP_WB_ACCOUNT_ID".to_owned(), "wb_one".to_owned());
        assert!(from(&no_scope).is_err());
    }

    #[test]
    fn wb_runtime_refuses_wrong_account_token_paths_and_timeout() {
        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;

        let wrong_account_fixtures = Fixtures::new();
        wrong_account_fixtures.configure_wb("plan_only");
        let mut registry: serde_json::Value =
            serde_json::from_slice(&fs::read(&wrong_account_fixtures.registry).unwrap()).unwrap();
        registry["accounts"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "id": "ozon_one",
                "organization": "Example Ozon",
                "marketplace": "ozon",
                "seller_client_id": "seller",
                "manager_id": "manager",
                "ozon": {
                    "store_id": "store_one",
                    "client_id_env": "UNUSED_CLIENT_ID",
                    "api_key_env": "UNUSED_API_KEY"
                }
            }));
        fs::write(
            &wrong_account_fixtures.registry,
            serde_json::to_vec(&registry).unwrap(),
        )
        .unwrap();
        let mut wrong_account = jwt_values(&wrong_account_fixtures);
        wrong_account.insert(
            "CONTROL_MCP_WB_ACCOUNT_ID".to_owned(),
            "ozon_one".to_owned(),
        );
        assert!(from(&wrong_account).is_err());

        let fixtures = Fixtures::new();
        fixtures.configure_wb("plan_only");
        let reader = TempCredential::new(
            "reader-errors",
            &wb_token(3, WB_PROMOTION_BIT | WB_READ_ONLY_BIT, future),
        );
        let mut missing_read = wb_runtime_values(&fixtures, &reader);
        missing_read.remove("CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE");
        assert!(from(&missing_read).is_err());

        let mut values = wb_runtime_values(&fixtures, &reader);
        values.insert(
            "CONTROL_MCP_WB_PROMOTION_READ_TOKEN_FILE".to_owned(),
            "/definitely/missing/control-reader".to_owned(),
        );
        assert!(from(&values).is_err());

        for timeout in ["0", "31", "not-a-number"] {
            let mut invalid_timeout = wb_runtime_values(&fixtures, &reader);
            invalid_timeout.insert(
                "CONTROL_MCP_WB_TIMEOUT_SECONDS".to_owned(),
                timeout.to_owned(),
            );
            assert!(
                from(&invalid_timeout).is_err(),
                "timeout {timeout} must fail"
            );
        }

        let enabled = Fixtures::new();
        enabled.configure_wb("enabled");
        let enabled_reader = TempCredential::new(
            "enabled-reader-errors",
            &wb_token(3, WB_PROMOTION_BIT | WB_READ_ONLY_BIT, future),
        );
        let mut missing_writer = wb_runtime_values(&enabled, &enabled_reader);
        missing_writer.insert(
            "CONTROL_MCP_MARKETPLACE_WRITES_ENABLED".to_owned(),
            "true".to_owned(),
        );
        missing_writer.insert(
            "CONTROL_MCP_WB_PROMOTION_WRITE_TOKEN_FILE".to_owned(),
            "/definitely/missing/control-writer".to_owned(),
        );
        assert!(from(&missing_writer).is_err());
    }
}
