use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const DEFAULT_OZON_API_BASE_URL: &str = "https://api-seller.ozon.ru";
pub const DEFAULT_ACCESS_CONFIG_PATH: &str = "config/access.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Manager,
    Analyst,
    Finance,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    Dev,
    Jwt,
}

impl FromStr for AuthMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "dev" | "trusted" => Ok(Self::Dev),
            "jwt" | "oidc" | "keycloak" => Ok(Self::Jwt),
            other => bail!("неизвестный MCP_AUTH_MODE={other:?}; используйте dev или jwt"),
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Manager => "manager",
            Self::Analyst => "analyst",
            Self::Finance => "finance",
            Self::Admin => "admin",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct StoreId(pub String);

impl Default for StoreId {
    fn default() -> Self {
        Self::from("ofk")
    }
}

impl StoreId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for StoreId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<&str> for StoreId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Marketplace {
    Ozon,
    Wildberries,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Actor {
    pub id: String,
    pub name: String,
    pub role: Role,
    #[serde(default)]
    pub account_ids: BTreeSet<String>,
    #[serde(default)]
    pub oidc: Option<OidcIdentity>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OidcIdentity {
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
}

impl Actor {
    pub fn can_access_account(&self, account: &MarketplaceAccount) -> bool {
        self.role == Role::Admin
            || account.manager_id == self.id
            || self.account_ids.contains(&account.id)
    }

    pub fn can_access_store(&self, store: &StoreId, registry: &AccessRegistry) -> bool {
        registry
            .account_for_store(store)
            .is_some_and(|account| self.can_access_account(account))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketplaceAccount {
    pub id: String,
    pub organization: String,
    pub marketplace: Marketplace,
    pub seller_client_id: String,
    pub manager_id: String,
    #[serde(default)]
    pub ozon: Option<OzonAccount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OzonAccount {
    pub store_id: StoreId,
    pub client_id_env: String,
    pub api_key_env: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessRegistry {
    pub version: u32,
    pub actors: Vec<Actor>,
    pub accounts: Vec<MarketplaceAccount>,
}

impl AccessRegistry {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("не удалось прочитать реестр доступа {}", path.display()))?;
        let registry: Self = serde_json::from_str(&contents)
            .with_context(|| format!("неверный JSON реестра доступа {}", path.display()))?;
        registry.validate()?;
        Ok(registry)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            bail!("неподдерживаемая версия реестра доступа: {}", self.version);
        }
        let actor_ids: BTreeSet<_> = self.actors.iter().map(|actor| actor.id.as_str()).collect();
        if actor_ids.len() != self.actors.len() {
            bail!("идентификаторы actors должны быть уникальными");
        }
        let account_ids: BTreeSet<_> = self
            .accounts
            .iter()
            .map(|account| account.id.as_str())
            .collect();
        if account_ids.len() != self.accounts.len() {
            bail!("идентификаторы accounts должны быть уникальными");
        }
        let mut store_ids = BTreeSet::new();
        for account in &self.accounts {
            if !actor_ids.contains(account.manager_id.as_str()) {
                bail!(
                    "для кабинета {} указан неизвестный manager_id={}",
                    account.id,
                    account.manager_id
                );
            }
            if let Some(ozon) = &account.ozon {
                if account.marketplace != Marketplace::Ozon {
                    bail!(
                        "Ozon-настройки допустимы только для Ozon-кабинета {}",
                        account.id
                    );
                }
                if ozon.store_id.0.trim().is_empty()
                    || ozon.client_id_env.trim().is_empty()
                    || ozon.api_key_env.trim().is_empty()
                {
                    bail!(
                        "Ozon-настройки кабинета {} не могут быть пустыми",
                        account.id
                    );
                }
                if !store_ids.insert(ozon.store_id.clone()) {
                    bail!("store_id={} должен быть уникальным", ozon.store_id);
                }
            }
        }
        for actor in &self.actors {
            for account_id in &actor.account_ids {
                if !account_ids.contains(account_id.as_str()) {
                    return Err(anyhow::anyhow!(
                        "actor {} ссылается на неизвестный account_id={account_id}",
                        actor.id
                    ));
                }
            }
        }
        let mut subjects = BTreeSet::new();
        let mut usernames = BTreeSet::new();
        let mut emails = BTreeSet::new();
        for actor in &self.actors {
            let Some(identity) = &actor.oidc else {
                continue;
            };
            for (field, value, values) in [
                ("subject", identity.subject.as_deref(), &mut subjects),
                ("username", identity.username.as_deref(), &mut usernames),
                ("email", identity.email.as_deref(), &mut emails),
            ] {
                let Some(value) = value else {
                    continue;
                };
                if value.trim().is_empty() {
                    bail!(
                        "OIDC {field} пользователя {} не может быть пустым",
                        actor.id
                    );
                }
                if !values.insert(value.to_owned()) {
                    bail!("OIDC {field}={value:?} должен быть уникальным");
                }
            }
        }
        Ok(())
    }

    pub fn actor(&self, id: &str) -> Result<&Actor> {
        self.actors
            .iter()
            .find(|actor| actor.id == id)
            .with_context(|| format!("неизвестный MCP_ACTOR_ID={id:?}"))
    }

    pub fn account_for_store(&self, store: &StoreId) -> Option<&MarketplaceAccount> {
        self.accounts.iter().find(|account| {
            account
                .ozon
                .as_ref()
                .is_some_and(|ozon| ozon.store_id == *store)
        })
    }

    pub fn actor_for_oidc(
        &self,
        subject: &str,
        username: Option<&str>,
        email: Option<&str>,
    ) -> Result<&Actor> {
        self.actors
            .iter()
            .find(|actor| {
                actor.oidc.as_ref().is_some_and(|identity| {
                    identity.subject.as_deref() == Some(subject)
                        || username.is_some_and(|value| identity.username.as_deref() == Some(value))
                        || email.is_some_and(|value| identity.email.as_deref() == Some(value))
                })
            })
            .with_context(|| "OIDC-пользователь не зарегистрирован в реестре доступа")
    }
}

#[derive(Debug, Clone)]
pub struct RegistrySource {
    path: Arc<PathBuf>,
}

impl RegistrySource {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let source = Self {
            path: Arc::new(path.into()),
        };
        source.load()?;
        Ok(source)
    }

    pub fn load(&self) -> Result<AccessRegistry> {
        AccessRegistry::load(&self.path)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Clone)]
pub struct StoreCredentials {
    pub client_id: String,
    pub api_key: String,
}

impl fmt::Debug for StoreCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoreCredentials")
            .field("client_id", &"<redacted>")
            .field("api_key", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMode {
    Http,
    Stdio,
}

impl FromStr for TransportMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "http" | "streamable-http" => Ok(Self::Http),
            "stdio" => Ok(Self::Stdio),
            other => bail!("неизвестный MCP_TRANSPORT={other:?}; используйте http или stdio"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub bind: SocketAddr,
    pub transport: TransportMode,
    pub ozon_api_base_url: String,
    pub request_timeout: Duration,
    pub stores: BTreeMap<StoreId, StoreCredentials>,
    pub auth: AuthConfig,
    pub registry: RegistrySource,
}

#[derive(Debug, Clone)]
pub enum AuthConfig {
    Dev { actor_id: String },
    Jwt(JwtConfig),
}

#[derive(Debug, Clone)]
pub struct JwtConfig {
    pub issuer: String,
    pub audience: String,
    pub jwks_url: String,
    pub resource_url: String,
    pub resource_metadata_url: String,
    pub jwks_cache_ttl: Duration,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        Self::from_lookup_inner(&mut lookup)
    }

    fn from_lookup_inner(lookup: &mut dyn FnMut(&str) -> Option<String>) -> Result<Self> {
        let value = |lookup: &mut dyn FnMut(&str) -> Option<String>, key: &str, default: &str| {
            lookup(key).unwrap_or_else(|| default.to_owned())
        };
        let bind = value(lookup, "MCP_BIND", "127.0.0.1:8787")
            .parse()
            .context("MCP_BIND должен иметь формат IP:PORT")?;
        let transport = value(lookup, "MCP_TRANSPORT", "http").parse()?;
        let ozon_api_base_url = value(lookup, "OZON_API_BASE_URL", DEFAULT_OZON_API_BASE_URL)
            .trim_end_matches('/')
            .to_owned();
        let timeout_seconds = value(lookup, "OZON_REQUEST_TIMEOUT_SECONDS", "30")
            .parse::<u64>()
            .context("OZON_REQUEST_TIMEOUT_SECONDS должен быть целым числом")?;
        if !(1..=300).contains(&timeout_seconds) {
            bail!("OZON_REQUEST_TIMEOUT_SECONDS должен быть от 1 до 300");
        }
        let registry_path = value(lookup, "MCP_ACCESS_CONFIG", DEFAULT_ACCESS_CONFIG_PATH);
        let registry = RegistrySource {
            path: Arc::new(registry_path.into()),
        };
        let snapshot = registry.load()?;
        let auth_mode: AuthMode = value(lookup, "MCP_AUTH_MODE", "dev").parse()?;
        let auth = match auth_mode {
            AuthMode::Dev => {
                let actor_id = lookup("MCP_ACTOR_ID")
                    .context("MCP_ACTOR_ID обязателен при MCP_AUTH_MODE=dev")?;
                snapshot.actor(&actor_id)?;
                AuthConfig::Dev { actor_id }
            }
            AuthMode::Jwt => {
                let issuer = lookup("MCP_JWT_ISSUER")
                    .context("MCP_JWT_ISSUER обязателен при MCP_AUTH_MODE=jwt")?
                    .trim_end_matches('/')
                    .to_owned();
                let audience = lookup("MCP_JWT_AUDIENCE")
                    .context("MCP_JWT_AUDIENCE обязателен при MCP_AUTH_MODE=jwt")?;
                let jwks_url = lookup("MCP_JWT_JWKS_URL")
                    .unwrap_or_else(|| format!("{issuer}/protocol/openid-connect/certs"));
                let resource_url = lookup("MCP_PUBLIC_URL")
                    .context("MCP_PUBLIC_URL обязателен при MCP_AUTH_MODE=jwt")?;
                let mut parsed_resource = reqwest::Url::parse(&resource_url)
                    .context("MCP_PUBLIC_URL должен быть абсолютным URL")?;
                if !matches!(parsed_resource.scheme(), "http" | "https") {
                    bail!("MCP_PUBLIC_URL должен использовать http или https");
                }
                parsed_resource.set_path("/.well-known/oauth-protected-resource");
                parsed_resource.set_query(None);
                parsed_resource.set_fragment(None);
                let resource_metadata_url = parsed_resource.to_string();
                let ttl = value(lookup, "MCP_JWKS_CACHE_TTL_SECONDS", "300")
                    .parse::<u64>()
                    .context("MCP_JWKS_CACHE_TTL_SECONDS должен быть целым числом")?;
                if !(30..=86_400).contains(&ttl) {
                    bail!("MCP_JWKS_CACHE_TTL_SECONDS должен быть от 30 до 86400");
                }
                AuthConfig::Jwt(JwtConfig {
                    issuer,
                    audience,
                    jwks_url,
                    resource_url,
                    resource_metadata_url,
                    jwks_cache_ttl: Duration::from_secs(ttl),
                })
            }
        };
        let stores = snapshot
            .accounts
            .iter()
            .filter_map(|account| account.ozon.as_ref())
            .filter_map(|ozon| {
                let client_id = lookup(&ozon.client_id_env).unwrap_or_default();
                let api_key = lookup(&ozon.api_key_env).unwrap_or_default();
                (!client_id.trim().is_empty() && !api_key.trim().is_empty()).then(|| {
                    (
                        ozon.store_id.clone(),
                        StoreCredentials { client_id, api_key },
                    )
                })
            })
            .collect();
        Ok(Self {
            bind,
            transport,
            ozon_api_base_url,
            request_timeout: Duration::from_secs(timeout_seconds),
            stores,
            auth,
            registry,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    };

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn write_registry(registry: &AccessRegistry) -> PathBuf {
        let id = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("mcp-ozon-config-{}-{id}.json", std::process::id()));
        std::fs::write(&path, serde_json::to_vec_pretty(registry).unwrap()).unwrap();
        path
    }

    fn sample_registry() -> AccessRegistry {
        AccessRegistry {
            version: 1,
            actors: vec![
                Actor {
                    id: "admin".into(),
                    name: "Admin".into(),
                    role: Role::Admin,
                    account_ids: BTreeSet::from(["shop".into()]),
                    oidc: Some(OidcIdentity {
                        username: Some("admin-user".into()),
                        ..OidcIdentity::default()
                    }),
                },
                Actor {
                    id: "manager".into(),
                    name: "Manager".into(),
                    role: Role::Manager,
                    account_ids: BTreeSet::new(),
                    oidc: None,
                },
            ],
            accounts: vec![MarketplaceAccount {
                id: "shop".into(),
                organization: "Shop".into(),
                marketplace: Marketplace::Ozon,
                seller_client_id: "123".into(),
                manager_id: "manager".into(),
                ozon: Some(OzonAccount {
                    store_id: StoreId::from("shop"),
                    client_id_env: "SHOP_ID".into(),
                    api_key_env: "SHOP_KEY".into(),
                }),
            }],
        }
    }

    #[test]
    fn registry_validates_and_enforces_access() {
        let registry = sample_registry();
        registry.validate().unwrap();
        assert_eq!(Role::Manager.to_string(), "manager");
        assert_eq!(Role::Analyst.to_string(), "analyst");
        assert_eq!(Role::Finance.to_string(), "finance");
        assert_eq!(Role::Admin.to_string(), "admin");
        let store = StoreId::from("shop");
        assert!(
            registry
                .actor("admin")
                .unwrap()
                .can_access_store(&store, &registry)
        );
        assert!(
            registry
                .actor("manager")
                .unwrap()
                .can_access_store(&store, &registry)
        );
        assert_eq!(
            registry
                .actor_for_oidc("unrelated-subject", Some("admin-user"), None)
                .unwrap()
                .id,
            "admin"
        );
        assert!(
            registry
                .actor_for_oidc("unknown", Some("unknown"), None)
                .is_err()
        );
    }

    #[test]
    fn registry_rejects_unknown_manager() {
        let mut registry = sample_registry();
        registry.accounts[0].manager_id = "missing".into();
        assert!(
            registry
                .validate()
                .unwrap_err()
                .to_string()
                .contains("неизвестный manager_id")
        );
    }

    #[test]
    fn credentials_are_redacted() {
        let value = format!(
            "{:?}",
            StoreCredentials {
                client_id: "id".into(),
                api_key: "secret".into()
            }
        );
        assert!(!value.contains("secret"));
        assert!(!value.contains("\"id\""));
    }

    #[test]
    fn transport_aliases_are_supported() {
        assert_eq!(
            "http".parse::<TransportMode>().unwrap(),
            TransportMode::Http
        );
        assert_eq!(
            "streamable-http".parse::<TransportMode>().unwrap(),
            TransportMode::Http
        );
        assert_eq!(
            "stdio".parse::<TransportMode>().unwrap(),
            TransportMode::Stdio
        );
        assert!("invalid".parse::<TransportMode>().is_err());
    }

    #[test]
    fn registry_load_and_validation_failures_are_explained() {
        let missing = std::env::temp_dir().join("mcp-ozon-definitely-missing.json");
        assert!(
            AccessRegistry::load(&missing)
                .unwrap_err()
                .to_string()
                .contains("прочитать")
        );
        assert!(RegistrySource::new(&missing).is_err());

        let invalid_json =
            std::env::temp_dir().join(format!("mcp-ozon-invalid-{}.json", std::process::id()));
        std::fs::write(&invalid_json, "{").unwrap();
        assert!(
            AccessRegistry::load(&invalid_json)
                .unwrap_err()
                .to_string()
                .contains("неверный JSON")
        );

        let mut invalid_registry = sample_registry();
        invalid_registry.version = 2;
        assert!(AccessRegistry::load(&write_registry(&invalid_registry)).is_err());

        let mut cases = Vec::new();
        let mut value = sample_registry();
        value.version = 2;
        cases.push(value);
        let mut value = sample_registry();
        value.actors.push(value.actors[0].clone());
        cases.push(value);
        let mut value = sample_registry();
        value.accounts.push(value.accounts[0].clone());
        cases.push(value);
        let mut value = sample_registry();
        value.accounts[0].marketplace = Marketplace::Wildberries;
        cases.push(value);
        let mut value = sample_registry();
        value.accounts[0].ozon.as_mut().unwrap().store_id = StoreId::new("");
        cases.push(value);
        let mut value = sample_registry();
        value.accounts.push(MarketplaceAccount {
            id: "second".into(),
            ..value.accounts[0].clone()
        });
        cases.push(value);
        let mut value = sample_registry();
        value.actors[0].account_ids.insert("missing".into());
        cases.push(value);
        let mut value = sample_registry();
        value.actors[0].oidc.as_mut().unwrap().username = Some(" ".into());
        cases.push(value);
        let mut value = sample_registry();
        value.actors[1].oidc = Some(OidcIdentity {
            username: Some("admin-user".into()),
            ..OidcIdentity::default()
        });
        cases.push(value);
        for registry in cases {
            assert!(registry.validate().is_err());
        }
        assert!(sample_registry().actor("missing").is_err());
    }

    #[test]
    fn app_config_loads_registry_and_only_complete_credentials() {
        let path = write_registry(&sample_registry());
        let values = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("MCP_BIND", "0.0.0.0:9999"),
            ("MCP_TRANSPORT", "stdio"),
            ("OZON_API_BASE_URL", "https://example.invalid/"),
            ("OZON_REQUEST_TIMEOUT_SECONDS", "5"),
            ("SHOP_ID", "client"),
            ("SHOP_KEY", "secret"),
        ]);
        let config =
            AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned())).unwrap();
        assert_eq!(config.bind, "0.0.0.0:9999".parse().unwrap());
        assert_eq!(config.transport, TransportMode::Stdio);
        assert_eq!(config.ozon_api_base_url, "https://example.invalid");
        assert_eq!(config.request_timeout, Duration::from_secs(5));
        assert!(matches!(
            config.auth,
            AuthConfig::Dev { ref actor_id } if actor_id == "admin"
        ));
        assert_eq!(config.registry.path(), path);
        assert!(config.stores.contains_key(&StoreId::from("shop")));

        let defaults = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
        ]);
        let config =
            AppConfig::from_lookup(|key| defaults.get(key).map(|value| (*value).to_owned()))
                .unwrap();
        assert_eq!(config.bind, "127.0.0.1:8787".parse().unwrap());
        assert_eq!(config.transport, TransportMode::Http);
        assert!(config.stores.is_empty());
    }

    #[test]
    fn app_config_rejects_invalid_environment() {
        let path = write_registry(&sample_registry());
        let base = [
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
        ];
        let result = |extra: Option<(&str, &str)>| {
            AppConfig::from_lookup(|key| {
                extra
                    .filter(|(name, _)| *name == key)
                    .map(|(_, value)| value.to_owned())
                    .or_else(|| {
                        base.iter()
                            .find_map(|(name, value)| (*name == key).then(|| (*value).to_owned()))
                    })
            })
        };
        assert!(result(Some(("MCP_BIND", "bad"))).is_err());
        assert!(result(Some(("MCP_TRANSPORT", "bad"))).is_err());
        assert!(result(Some(("MCP_AUTH_MODE", "bad"))).is_err());
        assert!(result(Some(("OZON_REQUEST_TIMEOUT_SECONDS", "bad"))).is_err());
        assert!(result(Some(("OZON_REQUEST_TIMEOUT_SECONDS", "0"))).is_err());
        assert!(AppConfig::from_lookup(|_| None).is_err());

        let missing_registry = std::env::temp_dir().join(format!(
            "mcp-ozon-missing-config-{}.json",
            std::process::id()
        ));
        let missing_registry_values = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", missing_registry.to_str().unwrap()),
        ]);
        assert!(
            AppConfig::from_lookup(|key| missing_registry_values
                .get(key)
                .map(|value| (*value).to_owned()))
            .is_err()
        );

        for value in ["file:///tmp/mcp", "not-a-url"] {
            let jwt = BTreeMap::from([
                ("MCP_AUTH_MODE", "jwt"),
                ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
                ("MCP_JWT_ISSUER", "http://issuer.test/realms/ofk"),
                ("MCP_JWT_AUDIENCE", "ozonofk-mcp"),
                ("MCP_PUBLIC_URL", value),
            ]);
            assert!(
                AppConfig::from_lookup(|key| jwt.get(key).map(|value| (*value).to_owned()))
                    .is_err()
            );
        }

        for value in ["bad", "1", "86401"] {
            let jwt = BTreeMap::from([
                ("MCP_AUTH_MODE", "jwt"),
                ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
                ("MCP_JWT_ISSUER", "http://issuer.test/realms/ofk"),
                ("MCP_JWT_AUDIENCE", "ozonofk-mcp"),
                ("MCP_PUBLIC_URL", "http://localhost:8788/mcp"),
                ("MCP_JWKS_CACHE_TTL_SECONDS", value),
            ]);
            assert!(
                AppConfig::from_lookup(|key| jwt.get(key).map(|value| (*value).to_owned()))
                    .is_err()
            );
        }

        let unknown = [
            ("MCP_ACTOR_ID", "unknown"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
        ];
        assert!(
            AppConfig::from_lookup(|key| unknown
                .iter()
                .find_map(|(name, value)| (*name == key).then(|| (*value).to_owned())))
            .is_err()
        );

        let required_jwt_values = [
            ("MCP_AUTH_MODE", "jwt"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("MCP_JWT_ISSUER", "http://issuer.test/realms/ofk"),
            ("MCP_JWT_AUDIENCE", "ozonofk-mcp"),
            ("MCP_PUBLIC_URL", "http://localhost:8788/mcp"),
        ];
        for omitted in ["MCP_JWT_ISSUER", "MCP_JWT_AUDIENCE", "MCP_PUBLIC_URL"] {
            assert!(
                AppConfig::from_lookup(|key| required_jwt_values.iter().find_map(
                    |(name, value)| {
                        (*name == key && *name != omitted).then(|| (*value).to_owned())
                    }
                ))
                .is_err()
            );
        }
    }

    #[test]
    fn app_config_loads_jwt_mode_without_trusted_actor() {
        let path = write_registry(&sample_registry());
        let values = BTreeMap::from([
            ("MCP_AUTH_MODE", "jwt"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("MCP_JWT_ISSUER", "http://localhost:8180/realms/ofk/"),
            ("MCP_JWT_AUDIENCE", "ozonofk-mcp"),
            (
                "MCP_JWT_JWKS_URL",
                "http://keycloak:8080/realms/ofk/protocol/openid-connect/certs",
            ),
            ("MCP_PUBLIC_URL", "http://localhost:8788/mcp"),
            ("MCP_JWKS_CACHE_TTL_SECONDS", "600"),
        ]);
        let config =
            AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned())).unwrap();
        let rendered = format!("{:?}", config.auth);
        assert!(rendered.contains("http://localhost:8180/realms/ofk"));
        assert!(rendered.contains("ozonofk-mcp"));
        assert!(rendered.contains("http://localhost:8788/mcp"));
        assert!(rendered.contains("http://localhost:8788/.well-known/oauth-protected-resource"));
        assert!(rendered.contains("600s"));
    }

    #[test]
    fn app_config_can_load_from_process_environment() {
        let _guard = ENV_LOCK.lock().unwrap();
        let path = write_registry(&sample_registry());
        unsafe {
            std::env::set_var("MCP_ACTOR_ID", "admin");
            std::env::set_var("MCP_ACCESS_CONFIG", &path);
        }
        let config = AppConfig::from_env().unwrap();
        assert!(matches!(
            config.auth,
            AuthConfig::Dev { ref actor_id } if actor_id == "admin"
        ));
        unsafe {
            std::env::remove_var("MCP_ACTOR_ID");
            std::env::remove_var("MCP_ACCESS_CONFIG");
        }
    }
}
