use std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;

use crate::{
    config::{AuthMode, JwtConfig, RegistrySource, TransportMode},
    control::policy::ControlPolicy,
};

const DEFAULT_CONTROL_ACCESS_CONFIG: &str = "config/access.json";
const DEFAULT_CONTROL_POLICY: &str = "config/control-policy.json";
const CONTROL_REQUIRED_SCOPE: &str = "mcp:ads-control";

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

        Ok(Self {
            bind,
            max_sessions,
            session_idle_timeout,
            transport,
            auth,
            registry,
            policy,
        })
    }
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
    validate_http_url("CONTROL_MCP_JWT_ISSUER", &issuer)?;
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
    validate_http_url("CONTROL_MCP_JWT_JWKS_URL", &jwks_url)?;
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

fn validate_http_url(name: &str, value: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(value).with_context(|| format!("{name} должен быть URL"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        bail!("{name} должен быть безопасным абсолютным HTTP(S) URL без credentials/fragment");
    }
    Ok(())
}

fn normalize_url(name: &str, value: &str) -> Result<String> {
    validate_http_url(name, value)?;
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

    use super::*;

    static FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
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
    }

    impl Drop for Fixtures {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.registry);
            let _ = fs::remove_file(&self.policy);
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
}
