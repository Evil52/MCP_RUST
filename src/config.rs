use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fs::File,
    io::Read,
    net::SocketAddr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use jsonwebtoken::dangerous::insecure_decode;
use rmcp::{
    schemars::JsonSchema, transport::streamable_http_server::session::local::LocalSessionManager,
};
use serde::{Deserialize, Serialize};

use crate::wb::WbCredentials;

pub const DEFAULT_OZON_API_BASE_URL: &str = "https://api-seller.ozon.ru";
pub const DEFAULT_ACCESS_CONFIG_PATH: &str = "config/access.json";
pub const DEFAULT_JWT_REQUIRED_SCOPES: &str = "mcp:tools";
const MAX_ACCESS_REGISTRY_BYTES: u64 = 1_048_576;
const MAX_ENV_NAME_BYTES: usize = 128;
const MAX_CREDENTIAL_BYTES: usize = 16_384;

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
            "jwt" | "oidc" => Ok(Self::Jwt),
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
    #[must_use]
    pub fn can_access_account(&self, account: &MarketplaceAccount) -> bool {
        self.role == Role::Admin
            || account.manager_id == self.id
            || self.account_ids.contains(&account.id)
    }

    #[must_use]
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
    #[serde(default)]
    pub wildberries: Option<WildberriesAccount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OzonAccount {
    pub store_id: StoreId,
    pub client_id_env: String,
    pub api_key_env: String,
    #[serde(default)]
    pub performance: Option<OzonPerformanceAccount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OzonPerformanceAccount {
    pub client_id_env: String,
    pub client_secret_env: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WildberriesAccount {
    pub api_token_env: String,
    /// Stable WB seller UUID (`sid` JWT claim). Optional for analytics legacy
    /// bindings, but mandatory for any Control runtime.
    #[serde(default)]
    pub seller_sid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessRegistry {
    pub version: u32,
    pub actors: Vec<Actor>,
    pub accounts: Vec<MarketplaceAccount>,
}

fn read_registry_bytes(path: &Path) -> Result<Vec<u8>> {
    let file = File::open(path)
        .with_context(|| format!("не удалось прочитать реестр доступа {}", path.display()))?;
    let mut contents = Vec::new();
    file.take(MAX_ACCESS_REGISTRY_BYTES + 1)
        .read_to_end(&mut contents)
        .with_context(|| format!("не удалось прочитать реестр доступа {}", path.display()))?;
    if contents.len() as u64 > MAX_ACCESS_REGISTRY_BYTES {
        bail!(
            "реестр доступа {} превышает безопасный лимит {} байт",
            path.display(),
            MAX_ACCESS_REGISTRY_BYTES
        );
    }
    Ok(contents)
}

fn validate_env_name(value: &str, field: &str) -> Result<()> {
    let mut bytes = value.bytes();
    let first = bytes.next();
    let valid = value.len() <= MAX_ENV_NAME_BYTES
        && matches!(first, Some(b'A'..=b'Z' | b'_'))
        && bytes.all(|byte| matches!(byte, b'A'..=b'Z' | b'0'..=b'9' | b'_'));
    if !valid {
        bail!(
            "{field} должен быть безопасным именем переменной окружения [A-Z_][A-Z0-9_]{{0,{}}}",
            MAX_ENV_NAME_BYTES - 1
        );
    }
    Ok(())
}

pub(crate) fn is_canonical_uuid(value: &str) -> bool {
    value.len() == 36
        && value != "00000000-0000-0000-0000-000000000000"
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => matches!(byte, b'0'..=b'9' | b'a'..=b'f'),
        })
}

fn validate_credential(value: &str, env_name: &str) -> Result<()> {
    if value.len() > MAX_CREDENTIAL_BYTES || value.bytes().any(|byte| !matches!(byte, 0x21..=0x7e))
    {
        bail!(
            "credential из {env_name} содержит пробельные/управляющие/non-ASCII символы или превышает безопасный лимит"
        );
    }
    Ok(())
}

#[derive(Deserialize)]
struct WbTokenTypeClaims {
    acc: u8,
}

fn decoded_base64url_len(encoded_len: usize) -> Option<usize> {
    let remainder = encoded_len % 4;
    // A non-padded base64url value can never have one trailing symbol.
    if remainder == 1 {
        None
    } else {
        Some(encoded_len / 4 * 3 + remainder.saturating_sub(1))
    }
}

fn is_unpadded_base64url(segment: &str) -> bool {
    !segment.is_empty()
        && segment.len() % 4 != 1
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn invalid_wb_token_type(env_name: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "credential из {env_name} должен быть корректным JWT-токеном Wildberries типа Personal"
    )
}

/// Classifies a WB token for capability/quota selection only.
///
/// Signature verification is deliberately not attempted here because WB does
/// not publish a key contract for local API-token authentication. Authenticity
/// is still verified by WB on every request. The signed `acc` payload is decoded
/// locally only to ensure this client's Personal quota matrix is never
/// used for Base, Test or Service tokens. Service tokens additionally require
/// an `X-Client-Secret` and `asid` binding that this owner-operated client does
/// not currently configure.
pub(crate) fn validate_wb_token_type(token: &str, env_name: &str) -> Result<()> {
    let mut segments = token.split('.');
    let (Some(header), Some(payload), Some(signature), None) = (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) else {
        return Err(invalid_wb_token_type(env_name));
    };
    // Bound the decoded payload before handing it to the JWT parser. Do this
    // before the alphabet check so the impossible one-symbol base64url tail is
    // rejected explicitly rather than hidden behind a later invariant.
    let decoded_payload_len =
        decoded_base64url_len(payload.len()).ok_or_else(|| invalid_wb_token_type(env_name))?;
    if !is_unpadded_base64url(header)
        || !is_unpadded_base64url(payload)
        || !is_unpadded_base64url(signature)
    {
        return Err(invalid_wb_token_type(env_name));
    }

    if decoded_payload_len > MAX_CREDENTIAL_BYTES {
        return Err(invalid_wb_token_type(env_name));
    }

    let claims =
        insecure_decode::<WbTokenTypeClaims>(token).map_err(|_| invalid_wb_token_type(env_name))?;
    if claims.claims.acc != 3 {
        return Err(invalid_wb_token_type(env_name));
    }
    Ok(())
}

impl AccessRegistry {
    pub fn load(path: &Path) -> Result<Self> {
        Self::from_slice(&read_registry_bytes(path)?, path)
    }

    fn from_slice(contents: &[u8], path: &Path) -> Result<Self> {
        let registry: Self = serde_json::from_slice(contents)
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
        self.validate_accounts(&actor_ids)?;
        self.validate_actor_account_ids(&account_ids)?;
        self.validate_oidc_identities()?;
        Ok(())
    }

    fn validate_accounts(&self, actor_ids: &BTreeSet<&str>) -> Result<()> {
        let mut store_ids = BTreeSet::new();
        let mut wb_seller_sids = BTreeSet::new();
        let mut selectors = self.account_selectors()?;
        for account in &self.accounts {
            Self::validate_account(account, actor_ids, &mut store_ids, &mut selectors)?;
            if let Some(seller_sid) = account
                .wildberries
                .as_ref()
                .and_then(|wildberries| wildberries.seller_sid.as_deref())
                && !wb_seller_sids.insert(seller_sid)
            {
                bail!("wildberries.seller_sid должен быть уникальным между account bindings");
            }
        }
        Ok(())
    }

    fn account_selectors(&self) -> Result<BTreeMap<String, String>> {
        let mut selectors = BTreeMap::new();
        for account in &self.accounts {
            if account.id.trim().is_empty() {
                bail!("идентификатор кабинета не может быть пустым");
            }
            selectors.insert(account.id.clone(), account.id.clone());
        }
        Ok(selectors)
    }

    fn validate_account(
        account: &MarketplaceAccount,
        actor_ids: &BTreeSet<&str>,
        store_ids: &mut BTreeSet<StoreId>,
        selectors: &mut BTreeMap<String, String>,
    ) -> Result<()> {
        if !actor_ids.contains(account.manager_id.as_str()) {
            bail!(
                "для кабинета {} указан неизвестный manager_id={}",
                account.id,
                account.manager_id
            );
        }
        if account.ozon.is_some() && account.wildberries.is_some() {
            bail!(
                "кабинет {} не может одновременно содержать Ozon и Wildberries credentials",
                account.id
            );
        }
        if let Some(ozon) = &account.ozon {
            Self::validate_ozon_account(account, ozon, store_ids, selectors)?;
        }
        if let Some(wildberries) = &account.wildberries {
            Self::validate_wildberries_account(account, wildberries)?;
        }
        Ok(())
    }

    fn validate_ozon_account(
        account: &MarketplaceAccount,
        ozon: &OzonAccount,
        store_ids: &mut BTreeSet<StoreId>,
        selectors: &mut BTreeMap<String, String>,
    ) -> Result<()> {
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
        validate_env_name(&ozon.client_id_env, "client_id_env")?;
        validate_env_name(&ozon.api_key_env, "api_key_env")?;
        if let Some(performance) = &ozon.performance {
            if performance.client_id_env.trim().is_empty()
                || performance.client_secret_env.trim().is_empty()
            {
                bail!(
                    "Performance-настройки кабинета {} не могут быть пустыми",
                    account.id
                );
            }
            validate_env_name(&performance.client_id_env, "performance.client_id_env")?;
            validate_env_name(
                &performance.client_secret_env,
                "performance.client_secret_env",
            )?;
        }
        if !store_ids.insert(ozon.store_id.clone()) {
            bail!("store_id={} должен быть уникальным", ozon.store_id);
        }
        if let Some(owner) = selectors.insert(ozon.store_id.0.clone(), account.id.clone())
            && owner != account.id
        {
            bail!(
                "selector магазина {:?} неоднозначен между кабинетами {} и {}",
                ozon.store_id.0,
                owner,
                account.id
            );
        }
        Ok(())
    }

    fn validate_wildberries_account(
        account: &MarketplaceAccount,
        wildberries: &WildberriesAccount,
    ) -> Result<()> {
        if account.marketplace != Marketplace::Wildberries {
            bail!(
                "Wildberries-настройки допустимы только для WB-кабинета {}",
                account.id
            );
        }
        if wildberries.api_token_env.trim().is_empty() {
            bail!(
                "Wildberries-настройки кабинета {} не могут быть пустыми",
                account.id
            );
        }
        validate_env_name(&wildberries.api_token_env, "api_token_env")?;
        if let Some(seller_sid) = &wildberries.seller_sid
            && !is_canonical_uuid(seller_sid)
        {
            bail!("wildberries.seller_sid должен быть canonical UUID");
        }
        Ok(())
    }

    fn validate_actor_account_ids(&self, account_ids: &BTreeSet<&str>) -> Result<()> {
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
        Ok(())
    }

    fn validate_oidc_identities(&self) -> Result<()> {
        let mut subjects = BTreeSet::new();
        let mut usernames = BTreeSet::new();
        let mut emails = BTreeSet::new();
        for actor in &self.actors {
            let Some(identity) = &actor.oidc else {
                continue;
            };
            if identity.subject.is_none() && identity.username.is_none() && identity.email.is_none()
            {
                bail!(
                    "OIDC identity пользователя {} должен содержать subject, username или email",
                    actor.id
                );
            }
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

    #[must_use]
    pub fn account_for_store(&self, store: &StoreId) -> Option<&MarketplaceAccount> {
        self.accounts.iter().find(|account| {
            account
                .ozon
                .as_ref()
                .is_some_and(|ozon| ozon.store_id == *store)
        })
    }

    #[must_use]
    pub fn account_for_store_selector(&self, selector: &StoreId) -> Option<&MarketplaceAccount> {
        self.accounts.iter().find(|account| {
            account
                .ozon
                .as_ref()
                .is_some_and(|ozon| account.id == selector.0 || ozon.store_id == *selector)
        })
    }

    pub fn actor_for_oidc(
        &self,
        subject: &str,
        username: Option<&str>,
        email: Option<&str>,
    ) -> Result<&Actor> {
        let mut matches = self.actors.iter().filter(|actor| {
            actor.oidc.as_ref().is_some_and(|identity| {
                if let Some(expected_subject) = identity.subject.as_deref() {
                    expected_subject == subject
                } else {
                    username.is_some_and(|value| identity.username.as_deref() == Some(value))
                        || email.is_some_and(|value| identity.email.as_deref() == Some(value))
                }
            })
        });
        let actor = matches
            .next()
            .context("OIDC-пользователь не зарегистрирован в реестре доступа")?;
        if matches.next().is_some() {
            bail!("OIDC identity неоднозначно соответствует нескольким пользователям");
        }
        Ok(actor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct OzonCredentialBinding {
    account_id: String,
    seller_client_id: String,
    store_id: StoreId,
    client_id_env: String,
    api_key_env: String,
    performance_client_id_env: Option<String>,
    performance_client_secret_env: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct WbCredentialBinding {
    account_id: String,
    seller_client_id: String,
    api_token_env: String,
    seller_sid: Option<String>,
}

impl WbCredentialBinding {
    fn snapshot(registry: &AccessRegistry) -> BTreeSet<Self> {
        registry
            .accounts
            .iter()
            .filter_map(|account| {
                account.wildberries.as_ref().map(|wildberries| Self {
                    account_id: account.id.clone(),
                    seller_client_id: account.seller_client_id.clone(),
                    api_token_env: wildberries.api_token_env.clone(),
                    seller_sid: wildberries.seller_sid.clone(),
                })
            })
            .collect()
    }
}

impl OzonCredentialBinding {
    fn snapshot(registry: &AccessRegistry) -> BTreeSet<Self> {
        registry
            .accounts
            .iter()
            .filter_map(|account| {
                account.ozon.as_ref().map(|ozon| Self {
                    account_id: account.id.clone(),
                    seller_client_id: account.seller_client_id.clone(),
                    store_id: ozon.store_id.clone(),
                    client_id_env: ozon.client_id_env.clone(),
                    api_key_env: ozon.api_key_env.clone(),
                    performance_client_id_env: ozon
                        .performance
                        .as_ref()
                        .map(|performance| performance.client_id_env.clone()),
                    performance_client_secret_env: ozon
                        .performance
                        .as_ref()
                        .map(|performance| performance.client_secret_env.clone()),
                })
            })
            .collect()
    }
}

/// The parsed registry together with the exact bytes it was parsed from.
///
/// Keying the cache on the file contents — rather than on a modification
/// timestamp — keeps hot reload exactly as eager as it was before: any edit
/// changes the bytes, and any byte change re-parses and re-validates.
#[derive(Debug)]
struct CachedRegistry {
    raw: Vec<u8>,
    registry: Arc<AccessRegistry>,
}

#[derive(Debug, Clone)]
pub struct RegistrySource {
    path: Arc<PathBuf>,
    credential_bindings: Arc<BTreeSet<OzonCredentialBinding>>,
    wb_credential_bindings: Arc<BTreeSet<WbCredentialBinding>>,
    cache: Arc<RwLock<Option<CachedRegistry>>>,
    #[cfg(test)]
    load_count: Arc<std::sync::atomic::AtomicU64>,
    #[cfg(test)]
    last_load_thread: Arc<std::sync::Mutex<Option<std::thread::ThreadId>>>,
    #[cfg(test)]
    panic_next_load: Arc<std::sync::atomic::AtomicBool>,
}

impl RegistrySource {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = Arc::new(path.into());
        let raw = read_registry_bytes(&path)?;
        let registry = AccessRegistry::from_slice(&raw, &path)?;
        let source = Self {
            path,
            credential_bindings: Arc::new(OzonCredentialBinding::snapshot(&registry)),
            wb_credential_bindings: Arc::new(WbCredentialBinding::snapshot(&registry)),
            cache: Arc::new(RwLock::new(Some(CachedRegistry {
                raw,
                registry: Arc::new(registry),
            }))),
            #[cfg(test)]
            load_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(test)]
            last_load_thread: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            panic_next_load: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        Ok(source)
    }

    /// Returns the current access registry, re-parsing it only when the file
    /// contents actually changed.
    ///
    /// Every tool call needs the registry to resolve the caller's identity and
    /// stores, so the unchanged-file path avoids a full JSON parse, a full
    /// validation pass and a deep clone per call.
    pub fn load(&self) -> Result<Arc<AccessRegistry>> {
        #[cfg(test)]
        {
            use std::sync::atomic::Ordering;

            self.load_count.fetch_add(1, Ordering::Relaxed);
            *self
                .last_load_thread
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(std::thread::current().id());
            assert!(
                !self.panic_next_load.swap(false, Ordering::Relaxed),
                "injected registry load panic"
            );
        }

        let raw = read_registry_bytes(&self.path)?;
        if let Some(cached) = self.cached(&raw) {
            return Ok(cached);
        }
        let registry = AccessRegistry::from_slice(&raw, &self.path)?;
        if OzonCredentialBinding::snapshot(&registry) != *self.credential_bindings {
            bail!(
                "MCP_ACCESS_CONFIG_RESTART_REQUIRED: привязки Ozon credentials изменились; перезапустите MCP, чтобы атомарно перечитать реестр и ключи"
            );
        }
        if WbCredentialBinding::snapshot(&registry) != *self.wb_credential_bindings {
            bail!(
                "MCP_ACCESS_CONFIG_RESTART_REQUIRED: привязки Wildberries credentials изменились; перезапустите MCP, чтобы атомарно перечитать реестр и ключи"
            );
        }
        let registry = Arc::new(registry);
        *self.write_cache() = Some(CachedRegistry {
            raw,
            registry: Arc::clone(&registry),
        });
        Ok(registry)
    }

    /// Loads and validates the hot-reloadable registry without blocking a
    /// Tokio runtime worker on filesystem I/O or JSON validation.
    pub(crate) async fn load_async(&self) -> Result<Arc<AccessRegistry>> {
        let source = self.clone();
        tokio::task::spawn_blocking(move || source.load())
            .await
            .map_err(|_| {
                anyhow::anyhow!("не удалось безопасно выполнить фоновую загрузку реестра доступа")
            })?
    }

    fn cached(&self, raw: &[u8]) -> Option<Arc<AccessRegistry>> {
        let cache = self.read_cache();
        let cached = cache.as_ref()?;
        (cached.raw == raw).then(|| Arc::clone(&cached.registry))
    }

    // Nothing that can panic runs while either guard is held, so a poisoned
    // lock is unreachable; recovering the value keeps it that way for good.
    fn read_cache(&self) -> RwLockReadGuard<'_, Option<CachedRegistry>> {
        self.cache.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_cache(&self) -> RwLockWriteGuard<'_, Option<CachedRegistry>> {
        self.cache.write().unwrap_or_else(PoisonError::into_inner)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(test)]
    pub(crate) fn load_count(&self) -> u64 {
        use std::sync::atomic::Ordering;

        self.load_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn last_load_thread(&self) -> Option<std::thread::ThreadId> {
        *self
            .last_load_thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    fn panic_on_next_load(&self) {
        use std::sync::atomic::Ordering;

        self.panic_next_load.store(true, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct StoreCredentials {
    pub client_id: String,
    pub api_key: String,
}

#[derive(Clone)]
pub struct PerformanceCredentials {
    pub client_id: String,
    pub client_secret: String,
}

impl fmt::Debug for PerformanceCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PerformanceCredentials")
            .field("client_id", &"<redacted>")
            .field("client_secret", &"<redacted>")
            .finish()
    }
}

fn validate_unique_performance_client_ids(
    stores: &BTreeMap<StoreId, PerformanceCredentials>,
) -> Result<()> {
    let mut first_by_client_id: BTreeMap<&str, &StoreId> = BTreeMap::new();
    for (store, credentials) in stores {
        if let Some(first_store) = first_by_client_id.get(credentials.client_id.as_str()) {
            bail!(
                "Performance client_id нельзя совместно использовать для разных магазинов: {first_store} и {store}"
            );
        }
        first_by_client_id.insert(credentials.client_id.as_str(), store);
    }
    Ok(())
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
    pub max_sessions: NonZeroUsize,
    pub session_idle_timeout: Duration,
    pub transport: TransportMode,
    pub ozon_api_base_url: String,
    pub request_timeout: Duration,
    pub ozon_postings_vnext: bool,
    pub ozon_finance_accruals_preview: bool,
    pub stores: BTreeMap<StoreId, StoreCredentials>,
    pub performance_stores: BTreeMap<StoreId, PerformanceCredentials>,
    pub wildberries_accounts: BTreeMap<String, WbCredentials>,
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
    pub required_scopes: Vec<String>,
    pub jwks_cache_ttl: Duration,
}

fn is_safe_oauth_scope_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
}

fn parse_required_scopes(value: &str) -> Result<Vec<String>> {
    if value
        .bytes()
        .any(|byte| byte != b' ' && !matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
    {
        bail!(
            "MCP_JWT_REQUIRED_SCOPES должен содержать только безопасные OAuth scope-токены, разделённые пробелами"
        );
    }

    let mut seen = BTreeSet::new();
    let scopes = value
        .split(' ')
        .filter(|scope| !scope.is_empty())
        .filter(|scope| is_safe_oauth_scope_token(scope))
        .filter(|scope| seen.insert((*scope).to_owned()))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if scopes.is_empty() {
        bail!("MCP_JWT_REQUIRED_SCOPES должен содержать хотя бы один OAuth scope");
    }
    Ok(scopes)
}

/// Parses `MCP_MAX_SESSIONS`, which may only *lower* the built-in bound.
///
/// Raising it past the vendored default would let a single deployment opt back
/// into the unbounded-session memory exhaustion the limit exists to prevent, so
/// an out-of-range value fails startup instead of being silently clamped.
fn parse_max_sessions(value: Option<&str>) -> Result<NonZeroUsize> {
    let Some(value) = value else {
        return Ok(LocalSessionManager::DEFAULT_MAX_SESSIONS);
    };
    let parsed = value
        .trim()
        .parse::<NonZeroUsize>()
        .context("MCP_MAX_SESSIONS должен быть положительным целым числом")?;
    if parsed > LocalSessionManager::DEFAULT_MAX_SESSIONS {
        bail!(
            "MCP_MAX_SESSIONS может только уменьшать лимит: максимум {}",
            LocalSessionManager::DEFAULT_MAX_SESSIONS
        );
    }
    Ok(parsed)
}

/// Parses the deploy-level session idle lifetime. The lower bound avoids
/// pathological reconnect churn while the upper bound ensures abandoned
/// public handshakes cannot retain the bounded registry indefinitely.
fn parse_session_idle_timeout(value: Option<&str>) -> Result<Duration> {
    const DEFAULT_SECONDS: u64 = 120;
    const MIN_SECONDS: u64 = 90;
    const MAX_SECONDS: u64 = 300;

    let seconds = match value {
        Some(value) => value
            .parse::<u64>()
            .context("MCP_SESSION_IDLE_TIMEOUT_SECONDS должен быть целым числом")?,
        None => DEFAULT_SECONDS,
    };
    if !(MIN_SECONDS..=MAX_SECONDS).contains(&seconds) {
        bail!("MCP_SESSION_IDLE_TIMEOUT_SECONDS должен быть от {MIN_SECONDS} до {MAX_SECONDS}");
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

fn validate_ozon_api_base_url(value: &str) -> Result<String> {
    let parsed = reqwest::Url::parse(value).map_err(|_| {
        anyhow::anyhow!(
            "OZON_API_BASE_URL должен указывать на официальный HTTPS endpoint Ozon Seller API"
        )
    })?;
    let is_official_endpoint = parsed.scheme() == "https"
        && parsed.host_str() == Some("api-seller.ozon.ru")
        && matches!(parsed.port(), None | Some(443))
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.path() == "/"
        && parsed.query().is_none()
        && parsed.fragment().is_none();
    if !is_official_endpoint {
        bail!(
            "OZON_API_BASE_URL разрешён только как https://api-seller.ozon.ru без credentials, query, fragment и дополнительного path"
        );
    }
    Ok(DEFAULT_OZON_API_BASE_URL.to_owned())
}

fn lookup_value(
    lookup: &mut dyn FnMut(&str) -> Option<String>,
    key: &str,
    default: &str,
) -> String {
    lookup(key).unwrap_or_else(|| default.to_owned())
}

fn load_jwt_config(lookup: &mut dyn FnMut(&str) -> Option<String>) -> Result<JwtConfig> {
    let issuer = lookup("MCP_JWT_ISSUER")
        .context("MCP_JWT_ISSUER обязателен при MCP_AUTH_MODE=jwt")?
        .trim_end_matches('/')
        .to_owned();
    let audience =
        lookup("MCP_JWT_AUDIENCE").context("MCP_JWT_AUDIENCE обязателен при MCP_AUTH_MODE=jwt")?;
    let jwks_url = lookup("MCP_JWT_JWKS_URL")
        .unwrap_or_else(|| format!("{issuer}/protocol/openid-connect/certs"));
    let resource_url =
        lookup("MCP_PUBLIC_URL").context("MCP_PUBLIC_URL обязателен при MCP_AUTH_MODE=jwt")?;
    let mut parsed_resource =
        reqwest::Url::parse(&resource_url).context("MCP_PUBLIC_URL должен быть абсолютным URL")?;
    if !matches!(parsed_resource.scheme(), "http" | "https") {
        bail!("MCP_PUBLIC_URL должен использовать http или https");
    }
    let parsed_audience = reqwest::Url::parse(&audience)
        .context("MCP_JWT_AUDIENCE должен быть абсолютным URL ресурса MCP_PUBLIC_URL")?;
    if !matches!(parsed_audience.scheme(), "http" | "https") {
        bail!("MCP_JWT_AUDIENCE должен использовать http или https");
    }
    let resource_url = parsed_resource.to_string();
    let audience = parsed_audience.to_string();
    if audience != resource_url {
        bail!(
            "MCP_JWT_AUDIENCE должен точно совпадать с нормализованным URL ресурса MCP_PUBLIC_URL"
        );
    }
    parsed_resource.set_path("/.well-known/oauth-protected-resource");
    parsed_resource.set_query(None);
    parsed_resource.set_fragment(None);
    let resource_metadata_url = parsed_resource.to_string();
    let required_scopes = parse_required_scopes(&lookup_value(
        lookup,
        "MCP_JWT_REQUIRED_SCOPES",
        DEFAULT_JWT_REQUIRED_SCOPES,
    ))?;
    let ttl = lookup_value(lookup, "MCP_JWKS_CACHE_TTL_SECONDS", "300")
        .parse::<u64>()
        .context("MCP_JWKS_CACHE_TTL_SECONDS должен быть целым числом")?;
    if !(30..=86_400).contains(&ttl) {
        bail!("MCP_JWKS_CACHE_TTL_SECONDS должен быть от 30 до 86400");
    }
    Ok(JwtConfig {
        issuer,
        audience,
        jwks_url,
        resource_url,
        resource_metadata_url,
        required_scopes,
        jwks_cache_ttl: Duration::from_secs(ttl),
    })
}

fn load_auth_config(
    lookup: &mut dyn FnMut(&str) -> Option<String>,
    snapshot: &AccessRegistry,
) -> Result<AuthConfig> {
    let auth_mode: AuthMode = lookup_value(lookup, "MCP_AUTH_MODE", "dev").parse()?;
    match auth_mode {
        AuthMode::Dev => {
            let actor_id =
                lookup("MCP_ACTOR_ID").context("MCP_ACTOR_ID обязателен при MCP_AUTH_MODE=dev")?;
            snapshot.actor(&actor_id)?;
            Ok(AuthConfig::Dev { actor_id })
        }
        AuthMode::Jwt => Ok(AuthConfig::Jwt(load_jwt_config(lookup)?)),
    }
}

fn load_credential_pair(
    lookup: &mut dyn FnMut(&str) -> Option<String>,
    first_env: &str,
    second_env: &str,
    incomplete_message: String,
) -> Result<Option<(String, String)>> {
    let first = lookup(first_env).unwrap_or_default();
    let second = lookup(second_env).unwrap_or_default();
    match (first.is_empty(), second.is_empty()) {
        (true, true) => Ok(None),
        (false, false) => {
            validate_credential(&first, first_env)?;
            validate_credential(&second, second_env)?;
            Ok(Some((first, second)))
        }
        _ => bail!(incomplete_message),
    }
}

fn load_ozon_credentials(
    account: &MarketplaceAccount,
    lookup: &mut dyn FnMut(&str) -> Option<String>,
    stores: &mut BTreeMap<StoreId, StoreCredentials>,
    performance_stores: &mut BTreeMap<StoreId, PerformanceCredentials>,
) -> Result<()> {
    let Some(ozon) = &account.ozon else {
        return Ok(());
    };
    if let Some((client_id, api_key)) = load_credential_pair(
        lookup,
        &ozon.client_id_env,
        &ozon.api_key_env,
        format!(
            "для магазина {} должны быть одновременно заданы {} и {}",
            ozon.store_id, ozon.client_id_env, ozon.api_key_env
        ),
    )? {
        stores.insert(
            ozon.store_id.clone(),
            StoreCredentials { client_id, api_key },
        );
    }
    if let Some(performance) = &ozon.performance
        && let Some((client_id, client_secret)) = load_credential_pair(
            lookup,
            &performance.client_id_env,
            &performance.client_secret_env,
            format!(
                "для Performance API магазина {} должны быть одновременно заданы {} и {}",
                ozon.store_id, performance.client_id_env, performance.client_secret_env
            ),
        )?
    {
        performance_stores.insert(
            ozon.store_id.clone(),
            PerformanceCredentials {
                client_id,
                client_secret,
            },
        );
    }
    Ok(())
}

fn load_wildberries_credentials(
    account: &MarketplaceAccount,
    lookup: &mut dyn FnMut(&str) -> Option<String>,
    wildberries_accounts: &mut BTreeMap<String, WbCredentials>,
) -> Result<()> {
    let Some(wildberries) = &account.wildberries else {
        return Ok(());
    };
    let token = lookup(&wildberries.api_token_env).unwrap_or_default();
    if token.is_empty() {
        return Ok(());
    }
    validate_credential(&token, &wildberries.api_token_env)?;
    validate_wb_token_type(&token, &wildberries.api_token_env)?;
    wildberries_accounts.insert(account.id.clone(), WbCredentials { token });
    Ok(())
}

struct MarketplaceCredentials {
    stores: BTreeMap<StoreId, StoreCredentials>,
    performance_stores: BTreeMap<StoreId, PerformanceCredentials>,
    wildberries_accounts: BTreeMap<String, WbCredentials>,
}

fn load_marketplace_credentials(
    snapshot: &AccessRegistry,
    lookup: &mut dyn FnMut(&str) -> Option<String>,
) -> Result<MarketplaceCredentials> {
    let mut credentials = MarketplaceCredentials {
        stores: BTreeMap::new(),
        performance_stores: BTreeMap::new(),
        wildberries_accounts: BTreeMap::new(),
    };
    for account in &snapshot.accounts {
        load_ozon_credentials(
            account,
            lookup,
            &mut credentials.stores,
            &mut credentials.performance_stores,
        )?;
        load_wildberries_credentials(account, lookup, &mut credentials.wildberries_accounts)?;
    }
    validate_unique_performance_client_ids(&credentials.performance_stores)?;
    Ok(credentials)
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        finish_optional_dotenv_load(dotenvy::dotenv())?;
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        Self::from_lookup_inner(&mut lookup)
    }

    fn from_lookup_inner(lookup: &mut dyn FnMut(&str) -> Option<String>) -> Result<Self> {
        let bind: SocketAddr = lookup_value(lookup, "MCP_BIND", "127.0.0.1:8787")
            .parse()
            .context("MCP_BIND должен иметь формат IP:PORT")?;
        let max_sessions = parse_max_sessions(lookup("MCP_MAX_SESSIONS").as_deref())?;
        let session_idle_timeout =
            parse_session_idle_timeout(lookup("MCP_SESSION_IDLE_TIMEOUT_SECONDS").as_deref())?;
        let transport = lookup_value(lookup, "MCP_TRANSPORT", "http").parse()?;
        let dev_allow_non_loopback = parse_strict_bool(
            &lookup_value(lookup, "MCP_DEV_ALLOW_NON_LOOPBACK", "false"),
            "MCP_DEV_ALLOW_NON_LOOPBACK",
        )?;
        let ozon_api_base_url = validate_ozon_api_base_url(&lookup_value(
            lookup,
            "OZON_API_BASE_URL",
            DEFAULT_OZON_API_BASE_URL,
        ))?;
        let timeout_seconds = lookup_value(lookup, "OZON_REQUEST_TIMEOUT_SECONDS", "30")
            .parse::<u64>()
            .context("OZON_REQUEST_TIMEOUT_SECONDS должен быть целым числом")?;
        if !(1..=300).contains(&timeout_seconds) {
            bail!("OZON_REQUEST_TIMEOUT_SECONDS должен быть от 1 до 300");
        }
        let ozon_postings_vnext = parse_strict_bool(
            &lookup_value(lookup, "OZON_POSTINGS_VNEXT", "false"),
            "OZON_POSTINGS_VNEXT",
        )?;
        let ozon_finance_accruals_preview = parse_strict_bool(
            &lookup_value(lookup, "OZON_FINANCE_ACCRUALS_PREVIEW", "false"),
            "OZON_FINANCE_ACCRUALS_PREVIEW",
        )?;
        let registry_path = lookup_value(lookup, "MCP_ACCESS_CONFIG", DEFAULT_ACCESS_CONFIG_PATH);
        let registry = RegistrySource::new(registry_path)?;
        let snapshot = registry.load()?;
        let auth = load_auth_config(lookup, &snapshot)?;
        if transport == TransportMode::Http
            && matches!(auth, AuthConfig::Dev { .. })
            && !bind.ip().is_loopback()
            && !dev_allow_non_loopback
        {
            bail!(
                "MCP_AUTH_MODE=dev с MCP_TRANSPORT=http разрешён только на loopback; для изолированного контейнера задайте MCP_DEV_ALLOW_NON_LOOPBACK=true явно"
            );
        }
        let credentials = load_marketplace_credentials(&snapshot, lookup)?;
        Ok(Self {
            bind,
            max_sessions,
            session_idle_timeout,
            transport,
            ozon_api_base_url,
            request_timeout: Duration::from_secs(timeout_seconds),
            ozon_postings_vnext,
            ozon_finance_accruals_preview,
            stores: credentials.stores,
            performance_stores: credentials.performance_stores,
            wildberries_accounts: credentials.wildberries_accounts,
            auth,
            registry,
        })
    }
}

fn finish_optional_dotenv_load(result: dotenvy::Result<PathBuf>) -> Result<()> {
    match result {
        Ok(_) => Ok(()),
        Err(error) if error.not_found() => Ok(()),
        Err(_) => bail!(
            "не удалось безопасно загрузить .env: файл недоступен или содержит некорректное значение"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use std::sync::{
        Barrier, Mutex,
        atomic::{AtomicU64, Ordering},
    };

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn wb_token_with_payload(payload: &[u8]) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","typ":"JWT"}"#);
        let claims = URL_SAFE_NO_PAD.encode(payload);
        let signature = URL_SAFE_NO_PAD.encode([0_u8; 64]);
        format!("{header}.{claims}.{signature}")
    }

    fn wb_token_with_claims(claims: serde_json::Value) -> String {
        wb_token_with_payload(&serde_json::to_vec(&claims).unwrap())
    }

    fn personal_wb_token() -> String {
        wb_token_with_claims(serde_json::json!({"acc": 3}))
    }

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
                    performance: None,
                }),
                wildberries: None,
            }],
        }
    }

    fn performance_registry() -> AccessRegistry {
        let mut registry = sample_registry();
        registry.accounts[0].ozon.as_mut().unwrap().performance = Some(OzonPerformanceAccount {
            client_id_env: "SHOP_PERFORMANCE_ID".into(),
            client_secret_env: "SHOP_PERFORMANCE_SECRET".into(),
        });
        registry
    }

    fn two_store_performance_registry() -> AccessRegistry {
        let mut registry = performance_registry();
        registry.accounts.push(MarketplaceAccount {
            id: "second_shop".into(),
            organization: "Second shop".into(),
            marketplace: Marketplace::Ozon,
            seller_client_id: "456".into(),
            manager_id: "manager".into(),
            ozon: Some(OzonAccount {
                store_id: StoreId::from("second_shop"),
                client_id_env: "SECOND_SHOP_ID".into(),
                api_key_env: "SECOND_SHOP_KEY".into(),
                performance: Some(OzonPerformanceAccount {
                    client_id_env: "SECOND_PERFORMANCE_ID".into(),
                    client_secret_env: "SECOND_PERFORMANCE_SECRET".into(),
                }),
            }),
            wildberries: None,
        });
        registry
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
    fn oidc_subject_pinning_has_precedence_over_username_and_email_fallback() {
        let mut registry = sample_registry();
        registry.actors[0].oidc = Some(OidcIdentity {
            subject: Some("pinned-subject".into()),
            username: Some("admin-user".into()),
            email: Some("admin@example.test".into()),
        });
        registry.actors[1].oidc = Some(OidcIdentity {
            subject: None,
            username: Some("fallback-user".into()),
            email: Some("fallback@example.test".into()),
        });
        registry.validate().unwrap();

        assert!(
            registry
                .actor_for_oidc(
                    "unrelated-subject",
                    Some("admin-user"),
                    Some("admin@example.test")
                )
                .is_err()
        );
        assert_eq!(
            registry
                .actor_for_oidc("pinned-subject", Some("different-user"), None)
                .unwrap()
                .id,
            "admin"
        );
        assert_eq!(
            registry
                .actor_for_oidc("unrelated-subject", Some("fallback-user"), None)
                .unwrap()
                .id,
            "manager"
        );
        assert_eq!(
            registry
                .actor_for_oidc(
                    "another-unrelated-subject",
                    None,
                    Some("fallback@example.test")
                )
                .unwrap()
                .id,
            "manager"
        );
    }

    #[test]
    fn an_unchanged_registry_file_is_served_from_cache_and_edits_still_hot_reload() {
        let path = write_registry(&sample_registry());
        let source = RegistrySource::new(&path).unwrap();

        // Identical bytes must not be parsed or validated again.
        let first = source.load().unwrap();
        let second = source.load().unwrap();
        assert!(Arc::ptr_eq(&first, &second));

        let mut edited = sample_registry();
        edited.actors[1].name = "Renamed manager".to_owned();
        std::fs::write(&path, serde_json::to_vec_pretty(&edited).unwrap()).unwrap();
        let reloaded = source.load().unwrap();
        assert!(!Arc::ptr_eq(&first, &reloaded));
        assert_eq!(reloaded.actor("manager").unwrap().name, "Renamed manager");

        // Rewriting byte-identical content is not an edit.
        std::fs::write(&path, serde_json::to_vec_pretty(&edited).unwrap()).unwrap();
        assert!(Arc::ptr_eq(&reloaded, &source.load().unwrap()));

        // A clone shares the cache rather than starting a cold one.
        assert!(Arc::ptr_eq(&reloaded, &source.clone().load().unwrap()));

        // A file that becomes invalid keeps failing instead of serving the
        // last good parse out of the cache.
        std::fs::write(&path, b"{").unwrap();
        assert!(source.load().is_err());
        std::fs::remove_file(&path).unwrap();
        assert!(
            source
                .load()
                .unwrap_err()
                .to_string()
                .contains("не удалось прочитать реестр доступа")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_registry_load_runs_off_the_runtime_thread_and_sanitizes_join_failures() {
        let path = write_registry(&sample_registry());
        let source = RegistrySource::new(&path).unwrap();
        let runtime_thread = std::thread::current().id();

        let first = source.load_async().await.unwrap();
        let second = source.load_async().await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(source.load_count(), 2);
        assert_ne!(source.last_load_thread(), Some(runtime_thread));

        source.panic_on_next_load();
        let error = source.load_async().await.unwrap_err().to_string();
        assert!(
            error.contains("фоновую загрузку реестра доступа"),
            "{error}"
        );
        assert!(!error.contains("injected registry load panic"), "{error}");
        assert_eq!(source.load_count(), 3);
    }

    #[test]
    fn env_names_and_credentials_are_accepted_exactly_at_their_limits() {
        // Boundary pairs: the largest accepted value and the smallest rejected
        // one, so an off-by-one in either bound is caught.
        let longest = format!("A{}", "B".repeat(MAX_ENV_NAME_BYTES - 1));
        assert_eq!(longest.len(), MAX_ENV_NAME_BYTES);
        validate_env_name(&longest, "client_id_env").unwrap();
        assert!(validate_env_name(&format!("{longest}C"), "client_id_env").is_err());

        for accepted in ["A", "_", "_A0", "OZON_CLIENT_ID", "A0_9"] {
            validate_env_name(accepted, "client_id_env").unwrap();
        }
        for rejected in [
            "",
            "0LEADING_DIGIT",
            "lower_case",
            "MiXeD",
            "WITH-DASH",
            "WITH SPACE",
            "WITH.DOT",
            "WITH$DOLLAR",
            "ПЕРЕМЕННАЯ",
            "TRAILING\n",
        ] {
            let error = validate_env_name(rejected, "client_id_env")
                .unwrap_err()
                .to_string();
            assert!(error.contains("client_id_env"), "{rejected:?}: {error}");
        }

        let longest = "k".repeat(MAX_CREDENTIAL_BYTES);
        validate_credential(&longest, "OZON_API_KEY").unwrap();
        assert!(validate_credential(&format!("{longest}k"), "OZON_API_KEY").is_err());

        // An empty credential is not rejected here: absent keys are filtered
        // out before validation, so emptiness is not this function's contract.
        validate_credential("", "OZON_API_KEY").unwrap();
        for accepted in ["!", "~", "a-b_c.d:e/f", "0123456789"] {
            validate_credential(accepted, "OZON_API_KEY").unwrap();
        }
        // Header-splitting and whitespace payloads must never reach a client.
        for rejected in [
            " leading",
            "trailing ",
            "with space",
            "with\ttab",
            "with\nnewline",
            "with\r\nsplit",
            "with\0nul",
            "ключ",
            "with\u{7f}del",
        ] {
            let error = validate_credential(rejected, "OZON_API_KEY")
                .unwrap_err()
                .to_string();
            assert!(error.contains("OZON_API_KEY"), "{rejected:?}: {error}");
        }
    }

    #[test]
    fn an_oversized_registry_file_is_refused_at_the_exact_limit() {
        let path = std::env::temp_dir().join(format!(
            "mcp-ozon-oversize-{}-{}.json",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));

        // Padding a valid registry with whitespace keeps it parseable, so the
        // only thing under test is the size guard.
        let mut document = serde_json::to_vec(&sample_registry()).unwrap();
        let padding = MAX_ACCESS_REGISTRY_BYTES as usize - document.len();
        document.extend(std::iter::repeat_n(b' ', padding));
        assert_eq!(document.len() as u64, MAX_ACCESS_REGISTRY_BYTES);
        std::fs::write(&path, &document).unwrap();
        RegistrySource::new(&path).expect("a registry exactly at the limit is accepted");

        document.push(b' ');
        std::fs::write(&path, &document).unwrap();
        let error = RegistrySource::new(&path).unwrap_err().to_string();
        assert!(error.contains("превышает безопасный лимит"), "{error}");
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn concurrent_readers_never_observe_a_torn_registry_while_it_is_rewritten() {
        // Every tool call loads the registry, so reads run concurrently with an
        // operator editing the file. Each observation must be one whole
        // generation — never a mix — and no reader may panic or deadlock.
        let path = write_registry(&sample_registry());
        let mut renamed = sample_registry();
        renamed.actors[1].name = "Renamed manager".to_owned();
        let generations = [
            serde_json::to_vec_pretty(&sample_registry()).unwrap(),
            serde_json::to_vec_pretty(&renamed).unwrap(),
        ];
        let source = RegistrySource::new(&path).unwrap();
        let writes_finished = Arc::new(Barrier::new(5));

        std::thread::scope(|scope| {
            let writer_path = path.clone();
            let writer_barrier = Arc::clone(&writes_finished);
            let writer_thread = scope.spawn(move || {
                for round in 0..200 {
                    std::fs::write(&writer_path, &generations[round % 2]).unwrap();
                }
                writer_barrier.wait();
            });
            let readers: Vec<_> = (0..4)
                .map(|_| {
                    let source = source.clone();
                    let writes_finished = Arc::clone(&writes_finished);
                    scope.spawn(move || {
                        let mut observed = 0_usize;
                        for _ in 0..200 {
                            // A partially written file is a legitimate transient
                            // error; a wrong-but-parsed registry is not.
                            if let Ok(registry) = source.load() {
                                let name = registry.actor("manager").unwrap().name.clone();
                                assert!(
                                    matches!(name.as_str(), "Manager" | "Renamed manager"),
                                    "torn registry generation: {name:?}"
                                );
                                assert_eq!(registry.actors.len(), 2);
                                assert_eq!(registry.accounts.len(), 1);
                                observed += 1;
                            }
                        }
                        // Coverage instrumentation can let the writer finish
                        // before any reader gets scheduled. Synchronize on the
                        // completed final write, then require one whole
                        // generation so the assertion cannot pass vacuously.
                        writes_finished.wait();
                        let registry = source
                            .load()
                            .expect("the completed final registry is readable");
                        assert_eq!(registry.actors.len(), 2);
                        assert_eq!(registry.accounts.len(), 1);
                        observed += 1;
                        observed
                    })
                })
                .collect();
            writer_thread.join().unwrap();
            let observed: usize = readers
                .into_iter()
                .map(|reader| reader.join().unwrap())
                .sum();
            // Guards against a vacuous pass where every read happened to fail.
            assert!(observed > 0, "no reader ever observed a whole registry");
        });

        std::fs::remove_file(&path).unwrap();
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
    fn registry_rejects_blank_account_id() {
        let mut registry = sample_registry();
        registry.accounts[0].id = " \t".into();
        let error = registry.validate().unwrap_err().to_string();
        assert!(error.contains("идентификатор кабинета не может быть пустым"));
    }

    #[test]
    fn registry_rejects_ambiguous_account_and_store_selectors() {
        let mut registry = sample_registry();
        registry.accounts.push(MarketplaceAccount {
            id: "other".into(),
            organization: "Other".into(),
            marketplace: Marketplace::Ozon,
            seller_client_id: "456".into(),
            manager_id: "manager".into(),
            ozon: Some(OzonAccount {
                store_id: StoreId::from("shop"),
                client_id_env: "OTHER_ID".into(),
                api_key_env: "OTHER_KEY".into(),
                performance: None,
            }),
            wildberries: None,
        });
        assert!(registry.validate().is_err());

        let mut registry = sample_registry();
        registry.accounts[0].ozon.as_mut().unwrap().store_id = StoreId::from("other");
        registry.accounts.push(MarketplaceAccount {
            id: "other".into(),
            organization: "Directory".into(),
            marketplace: Marketplace::Wildberries,
            seller_client_id: "789".into(),
            manager_id: "manager".into(),
            ozon: None,
            wildberries: None,
        });
        let error = registry.validate().unwrap_err().to_string();
        assert!(error.contains("selector магазина"), "{error}");
        assert!(error.contains("неоднозначен"), "{error}");
    }

    #[test]
    fn registry_validates_wildberries_bindings_and_loads_only_present_tokens() {
        let wb_account = MarketplaceAccount {
            id: "wb_shop".into(),
            organization: "WB Shop".into(),
            marketplace: Marketplace::Wildberries,
            seller_client_id: "42".into(),
            manager_id: "manager".into(),
            ozon: None,
            wildberries: Some(WildberriesAccount {
                api_token_env: "WB_TOKEN".into(),
                seller_sid: None,
            }),
        };
        let mut registry = sample_registry();
        registry.accounts.push(wb_account.clone());
        registry.validate().unwrap();
        let path = std::env::temp_dir().join(format!("mcp-ozon-bench-{}.json", std::process::id()));
        std::fs::write(&path, serde_json::to_vec_pretty(&registry).unwrap()).unwrap();
        let wb_token = personal_wb_token();
        let values = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("WB_TOKEN", wb_token.as_str()),
        ]);
        let config =
            AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned())).unwrap();
        assert!(config.wildberries_accounts.contains_key("wb_shop"));
        assert_eq!(
            format!("{:?}", config.wildberries_accounts["wb_shop"]),
            "WbCredentials { token: \"<redacted>\" }"
        );

        let mut wrong_marketplace = sample_registry();
        let mut invalid = wb_account.clone();
        invalid.marketplace = Marketplace::Ozon;
        wrong_marketplace.accounts.push(invalid);
        assert!(wrong_marketplace.validate().is_err());

        let mut blank_binding = sample_registry();
        let mut invalid = wb_account.clone();
        invalid.wildberries.as_mut().unwrap().api_token_env = " ".into();
        blank_binding.accounts.push(invalid);
        assert!(blank_binding.validate().is_err());

        let mut nil_sid = sample_registry();
        let mut invalid = wb_account.clone();
        invalid.wildberries.as_mut().unwrap().seller_sid =
            Some("00000000-0000-0000-0000-000000000000".into());
        nil_sid.accounts.push(invalid);
        assert!(nil_sid.validate().is_err());

        let mut mixed = sample_registry();
        let mut invalid = wb_account;
        invalid.ozon = mixed.accounts[0].ozon.clone();
        mixed.accounts.push(invalid);
        assert!(mixed.validate().is_err());

        let source = RegistrySource::new(&path).unwrap();
        registry.accounts[1]
            .wildberries
            .as_mut()
            .unwrap()
            .api_token_env = "WB_TOKEN_ROTATED".into();
        std::fs::write(&path, serde_json::to_vec(&registry).unwrap()).unwrap();
        assert!(
            source
                .load()
                .unwrap_err()
                .to_string()
                .contains("Wildberries credentials")
        );
    }

    #[test]
    fn wildberries_seller_sid_change_requires_restart() {
        let mut registry = sample_registry();
        registry.accounts.push(MarketplaceAccount {
            id: "wb_shop".into(),
            organization: "WB Shop".into(),
            marketplace: Marketplace::Wildberries,
            seller_client_id: "42".into(),
            manager_id: "manager".into(),
            ozon: None,
            wildberries: Some(WildberriesAccount {
                api_token_env: "WB_TOKEN".into(),
                seller_sid: Some("11111111-1111-4111-8111-111111111111".into()),
            }),
        });
        let path = write_registry(&registry);
        let source = RegistrySource::new(&path).unwrap();

        registry.accounts[1]
            .wildberries
            .as_mut()
            .unwrap()
            .seller_sid = Some("22222222-2222-4222-8222-222222222222".into());
        std::fs::write(&path, serde_json::to_vec_pretty(&registry).unwrap()).unwrap();

        let error = source.load().unwrap_err().to_string();
        assert!(
            error.starts_with("MCP_ACCESS_CONFIG_RESTART_REQUIRED:"),
            "{error}"
        );
        assert!(error.contains("Wildberries credentials"), "{error}");
    }

    #[test]
    fn duplicate_wildberries_seller_sid_fails_registry_hot_reload() {
        let mut registry = sample_registry();
        for (id, seller_sid) in [
            ("wb_one", "11111111-1111-4111-8111-111111111111"),
            ("wb_two", "22222222-2222-4222-8222-222222222222"),
        ] {
            registry.accounts.push(MarketplaceAccount {
                id: id.into(),
                organization: id.into(),
                marketplace: Marketplace::Wildberries,
                seller_client_id: id.into(),
                manager_id: "manager".into(),
                ozon: None,
                wildberries: Some(WildberriesAccount {
                    api_token_env: format!("{}_TOKEN", id.to_ascii_uppercase()),
                    seller_sid: Some(seller_sid.into()),
                }),
            });
        }
        let path = write_registry(&registry);
        let source = RegistrySource::new(&path).unwrap();

        registry.accounts[2]
            .wildberries
            .as_mut()
            .unwrap()
            .seller_sid = registry.accounts[1]
            .wildberries
            .as_ref()
            .unwrap()
            .seller_sid
            .clone();
        std::fs::write(&path, serde_json::to_vec_pretty(&registry).unwrap()).unwrap();

        let error = source.load().unwrap_err().to_string();
        assert!(error.contains("seller_sid"), "{error}");
        assert!(error.contains("уникальным"), "{error}");
    }

    #[test]
    fn app_config_inserts_only_wildberries_accounts_with_non_empty_tokens() {
        let mut registry = sample_registry();
        for (id, token_env) in [
            ("wb_configured", "WB_CONFIGURED_TOKEN"),
            ("wb_without_token", "WB_MISSING_TOKEN"),
        ] {
            registry.accounts.push(MarketplaceAccount {
                id: id.into(),
                organization: id.into(),
                marketplace: Marketplace::Wildberries,
                seller_client_id: id.into(),
                manager_id: "manager".into(),
                ozon: None,
                wildberries: Some(WildberriesAccount {
                    api_token_env: token_env.into(),
                    seller_sid: None,
                }),
            });
        }
        let path = std::env::temp_dir().join(format!("mcp-ozon-bench-{}.json", std::process::id()));
        std::fs::write(&path, serde_json::to_vec_pretty(&registry).unwrap()).unwrap();
        let wb_token = personal_wb_token();
        let values = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("WB_CONFIGURED_TOKEN", wb_token.as_str()),
        ]);

        let config =
            AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned())).unwrap();

        assert_eq!(config.wildberries_accounts.len(), 1);
        assert_eq!(config.wildberries_accounts["wb_configured"].token, wb_token);
        assert!(!config.wildberries_accounts.contains_key("wb_without_token"));
    }

    #[test]
    fn production_config_accepts_only_personal_wb_jwt_tokens() {
        assert_eq!(decoded_base64url_len(0), Some(0));
        assert_eq!(decoded_base64url_len(1), None);
        assert_eq!(decoded_base64url_len(2), Some(1));
        assert_eq!(decoded_base64url_len(3), Some(2));
        assert_eq!(decoded_base64url_len(4), Some(3));

        let personal = personal_wb_token();
        validate_wb_token_type(&personal, "WB_TOKEN").unwrap();

        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(br#"{"acc":3}"#);
        let signature = URL_SAFE_NO_PAD.encode([0_u8; 64]);
        let rejected = [
            wb_token_with_claims(serde_json::json!({"acc": 1})),
            wb_token_with_claims(serde_json::json!({"acc": 2})),
            wb_token_with_claims(serde_json::json!({"acc": 4})),
            wb_token_with_claims(serde_json::json!({"acc": 0})),
            wb_token_with_claims(serde_json::json!({"acc": 255})),
            wb_token_with_claims(serde_json::json!({"acc": "3"})),
            wb_token_with_claims(serde_json::json!({})),
            wb_token_with_payload(br#"{"acc":"super-secret"}"#),
            "not-a-jwt".to_owned(),
            format!("{header}.{payload}.{signature}.extra"),
            format!("{header}..{signature}"),
            format!("{header}.A.{signature}"),
            format!("AA.{payload}.{signature}"),
            format!("{header}.***.{signature}"),
            format!("{header}.{payload}.!"),
            format!("{header}.{payload}.A"),
        ];
        for token in rejected {
            let error = validate_wb_token_type(&token, "WB_TOKEN")
                .unwrap_err()
                .to_string();
            assert!(error.contains("WB_TOKEN"), "{error}");
            assert!(error.contains("Personal"), "{error}");
            assert!(!error.contains("super-secret"), "{error}");
            assert!(!error.contains(&token), "{error}");
        }

        let oversized_payload = "A".repeat((MAX_CREDENTIAL_BYTES / 3 + 1) * 4);
        let oversized = format!("{header}.{oversized_payload}.{signature}");
        assert!(validate_wb_token_type(&oversized, "WB_TOKEN").is_err());

        let mut registry = sample_registry();
        registry.accounts.push(MarketplaceAccount {
            id: "wb_shop".into(),
            organization: "WB Shop".into(),
            marketplace: Marketplace::Wildberries,
            seller_client_id: "42".into(),
            manager_id: "manager".into(),
            ozon: None,
            wildberries: Some(WildberriesAccount {
                api_token_env: "WB_TOKEN".into(),
                seller_sid: None,
            }),
        });
        let path = write_registry(&registry);
        for acc in [1, 2, 4, 5] {
            let token = wb_token_with_claims(serde_json::json!({"acc": acc}));
            let values = BTreeMap::from([
                ("MCP_ACTOR_ID", "admin"),
                ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
                ("WB_TOKEN", token.as_str()),
            ]);
            let error =
                AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned()))
                    .unwrap_err()
                    .to_string();
            assert!(error.contains("WB_TOKEN"), "acc={acc}: {error}");
            assert!(!error.contains(&token), "acc={acc}: {error}");
        }
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
    fn complete_performance_pair_loads_and_partial_pairs_fail_closed() {
        let path = write_registry(&performance_registry());
        let complete = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("SHOP_PERFORMANCE_ID", "performance-client"),
            ("SHOP_PERFORMANCE_SECRET", "performance-secret"),
        ]);
        let config =
            AppConfig::from_lookup(|key| complete.get(key).map(|value| (*value).to_owned()))
                .unwrap();
        let credentials = &config.performance_stores[&StoreId::from("shop")];
        assert_eq!(credentials.client_id, "performance-client");
        assert_eq!(credentials.client_secret, "performance-secret");

        for (name, value) in [
            ("SHOP_PERFORMANCE_ID", "performance-client"),
            ("SHOP_PERFORMANCE_SECRET", "performance-secret"),
        ] {
            let partial = BTreeMap::from([
                ("MCP_ACTOR_ID", "admin"),
                ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
                (name, value),
            ]);
            let error =
                AppConfig::from_lookup(|key| partial.get(key).map(|value| (*value).to_owned()))
                    .unwrap_err()
                    .to_string();
            assert!(error.contains("должны быть одновременно заданы"), "{error}");
            assert!(!error.contains(value), "{error}");
        }

        for performance in [
            OzonPerformanceAccount {
                client_id_env: " ".into(),
                client_secret_env: "SHOP_PERFORMANCE_SECRET".into(),
            },
            OzonPerformanceAccount {
                client_id_env: "SHOP_PERFORMANCE_ID".into(),
                client_secret_env: " ".into(),
            },
        ] {
            let mut registry = sample_registry();
            registry.accounts[0].ozon.as_mut().unwrap().performance = Some(performance);
            assert!(registry.validate().is_err());
        }
    }

    #[test]
    fn invalid_performance_secret_env_name_is_rejected() {
        let mut registry = performance_registry();
        registry.accounts[0]
            .ozon
            .as_mut()
            .unwrap()
            .performance
            .as_mut()
            .unwrap()
            .client_secret_env = "INVALID-PERFORMANCE-SECRET".into();

        let error = registry.validate().unwrap_err().to_string();
        assert!(error.contains("performance.client_secret_env"), "{error}");
    }

    #[test]
    fn omitted_performance_pair_keeps_store_unconfigured() {
        let path = write_registry(&performance_registry());
        let values = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
        ]);

        let config =
            AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned())).unwrap();

        assert!(config.performance_stores.is_empty());
    }

    #[test]
    fn duplicate_performance_client_id_is_rejected_even_with_the_same_secret() {
        let path = write_registry(&two_store_performance_registry());
        let shared = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("SHOP_PERFORMANCE_ID", "shared-client"),
            ("SHOP_PERFORMANCE_SECRET", "shared-secret"),
            ("SECOND_PERFORMANCE_ID", "shared-client"),
            ("SECOND_PERFORMANCE_SECRET", "shared-secret"),
        ]);
        let error = AppConfig::from_lookup(|key| shared.get(key).map(|value| (*value).to_owned()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("нельзя совместно использовать"), "{error}");
        for sensitive in ["shared-client", "shared-secret"] {
            assert!(!error.contains(sensitive), "{error}");
        }

        let mut unique = shared.clone();
        unique.insert("SECOND_PERFORMANCE_ID", "second-client");
        let config =
            AppConfig::from_lookup(|key| unique.get(key).map(|value| (*value).to_owned())).unwrap();
        assert_eq!(config.performance_stores.len(), 2);

        let mut conflicting = shared;
        conflicting.insert("SECOND_PERFORMANCE_SECRET", "different-secret");
        let error =
            AppConfig::from_lookup(|key| conflicting.get(key).map(|value| (*value).to_owned()))
                .unwrap_err()
                .to_string();
        assert!(error.contains("нельзя совместно использовать"), "{error}");
        for sensitive in ["shared-client", "shared-secret", "different-secret"] {
            assert!(!error.contains(sensitive), "{error}");
        }
    }

    #[test]
    fn performance_credentials_debug_is_redacted() {
        let value = format!(
            "{:?}",
            PerformanceCredentials {
                client_id: "performance-client".into(),
                client_secret: "performance-secret".into(),
            }
        );
        assert_eq!(
            value,
            "PerformanceCredentials { client_id: \"<redacted>\", client_secret: \"<redacted>\" }"
        );
        assert!(!value.contains("performance-client"));
        assert!(!value.contains("performance-secret"));
    }

    #[test]
    fn performance_credential_binding_edit_requires_restart() {
        let path = write_registry(&performance_registry());
        let source = RegistrySource::new(&path).unwrap();
        let mut edited = performance_registry();
        edited.accounts[0]
            .ozon
            .as_mut()
            .unwrap()
            .performance
            .as_mut()
            .unwrap()
            .client_secret_env = "SHOP_PERFORMANCE_SECRET_ROTATED".into();
        std::fs::write(&path, serde_json::to_vec_pretty(&edited).unwrap()).unwrap();
        let error = source.load().unwrap_err().to_string();
        assert!(
            error.starts_with("MCP_ACCESS_CONFIG_RESTART_REQUIRED:"),
            "{error}"
        );
        assert!(error.contains("Ozon credentials"), "{error}");
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

        let unreadable_as_file = std::env::temp_dir();
        assert!(
            AccessRegistry::load(&unreadable_as_file)
                .unwrap_err()
                .to_string()
                .contains("прочитать")
        );

        let invalid_json =
            std::env::temp_dir().join(format!("mcp-ozon-invalid-{}.json", std::process::id()));
        std::fs::write(&invalid_json, "{").unwrap();
        assert!(
            AccessRegistry::load(&invalid_json)
                .unwrap_err()
                .to_string()
                .contains("неверный JSON")
        );

        let oversized =
            std::env::temp_dir().join(format!("mcp-ozon-oversized-{}.json", std::process::id()));
        std::fs::write(
            &oversized,
            vec![b' '; usize::try_from(MAX_ACCESS_REGISTRY_BYTES).unwrap() + 1],
        )
        .unwrap();
        assert!(
            AccessRegistry::load(&oversized)
                .unwrap_err()
                .to_string()
                .contains("безопасный лимит")
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
        let mut value = sample_registry();
        value.actors[1].oidc = Some(OidcIdentity::default());
        cases.push(value);
        let mut value = sample_registry();
        value.accounts[0].ozon.as_mut().unwrap().client_id_env = "lowercase".into();
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
            ("OZON_API_BASE_URL", "https://api-seller.ozon.ru:443/"),
            ("OZON_REQUEST_TIMEOUT_SECONDS", "5"),
            ("SHOP_ID", "client"),
            ("SHOP_KEY", "secret"),
        ]);
        let config =
            AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned())).unwrap();
        assert_eq!(config.bind, "0.0.0.0:9999".parse().unwrap());
        assert_eq!(config.transport, TransportMode::Stdio);
        assert_eq!(config.ozon_api_base_url, DEFAULT_OZON_API_BASE_URL);
        assert_eq!(config.request_timeout, Duration::from_secs(5));
        assert!(!config.ozon_postings_vnext);
        assert!(!config.ozon_finance_accruals_preview);
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
        assert_eq!(config.session_idle_timeout, Duration::from_secs(120));
        assert_eq!(config.transport, TransportMode::Http);
        assert!(!config.ozon_postings_vnext);
        assert!(!config.ozon_finance_accruals_preview);
        assert!(config.stores.is_empty());

        let preview = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("OZON_POSTINGS_VNEXT", "true"),
            ("OZON_FINANCE_ACCRUALS_PREVIEW", "true"),
        ]);
        let config =
            AppConfig::from_lookup(|key| preview.get(key).map(|value| (*value).to_owned()))
                .unwrap();
        assert!(config.ozon_postings_vnext);
        assert!(config.ozon_finance_accruals_preview);

        let partial = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("SHOP_ID", "client"),
        ]);
        assert!(
            AppConfig::from_lookup(|key| partial.get(key).map(|value| (*value).to_owned()))
                .unwrap_err()
                .to_string()
                .contains("одновременно заданы")
        );

        for unsafe_value in [" leading-space", "contains\nnewline"] {
            let invalid = BTreeMap::from([
                ("MCP_ACTOR_ID", "admin"),
                ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
                ("SHOP_ID", "client"),
                ("SHOP_KEY", unsafe_value),
            ]);
            let error =
                AppConfig::from_lookup(|key| invalid.get(key).map(|value| (*value).to_owned()))
                    .unwrap_err()
                    .to_string();
            assert!(error.contains("SHOP_KEY"));
            assert!(!error.contains(unsafe_value));
        }
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

        // MCP_MAX_SESSIONS may only lower the vendored bound, so a raised or
        // unparsable value must stop startup instead of being clamped.
        assert_eq!(
            result(None).unwrap().max_sessions,
            LocalSessionManager::DEFAULT_MAX_SESSIONS
        );
        assert_eq!(
            result(Some(("MCP_MAX_SESSIONS", " 32 ")))
                .unwrap()
                .max_sessions
                .get(),
            32
        );
        assert_eq!(
            result(Some((
                "MCP_MAX_SESSIONS",
                &LocalSessionManager::DEFAULT_MAX_SESSIONS.to_string()
            )))
            .unwrap()
            .max_sessions,
            LocalSessionManager::DEFAULT_MAX_SESSIONS
        );
        for value in ["0", "-1", "", "bad", "1.5", "18446744073709551616"] {
            assert!(
                result(Some(("MCP_MAX_SESSIONS", value))).is_err(),
                "MCP_MAX_SESSIONS={value:?}"
            );
        }
        let raised = (LocalSessionManager::DEFAULT_MAX_SESSIONS.get() + 1).to_string();
        let error = result(Some(("MCP_MAX_SESSIONS", &raised)))
            .unwrap_err()
            .to_string();
        assert!(error.contains("может только уменьшать лимит"), "{error}");
        assert_eq!(
            result(Some(("MCP_SESSION_IDLE_TIMEOUT_SECONDS", "90")))
                .unwrap()
                .session_idle_timeout,
            Duration::from_secs(90)
        );
        assert_eq!(
            result(Some(("MCP_SESSION_IDLE_TIMEOUT_SECONDS", "300")))
                .unwrap()
                .session_idle_timeout,
            Duration::from_secs(300)
        );
        for value in ["", "89", "301", "-1", "bad", " 120 "] {
            assert!(
                result(Some(("MCP_SESSION_IDLE_TIMEOUT_SECONDS", value))).is_err(),
                "MCP_SESSION_IDLE_TIMEOUT_SECONDS={value:?}"
            );
        }
        assert!(result(Some(("MCP_AUTH_MODE", "bad"))).is_err());
        assert!(result(Some(("OZON_REQUEST_TIMEOUT_SECONDS", "bad"))).is_err());
        assert!(result(Some(("OZON_REQUEST_TIMEOUT_SECONDS", "0"))).is_err());
        for name in [
            "MCP_DEV_ALLOW_NON_LOOPBACK",
            "OZON_POSTINGS_VNEXT",
            "OZON_FINANCE_ACCRUALS_PREVIEW",
        ] {
            for value in ["", "1", "yes", "TRUE", " false"] {
                assert!(result(Some((name, value))).is_err(), "{name}={value:?}");
            }
        }
        for value in [
            "not-a-url",
            "http://api-seller.ozon.ru",
            "https://api-seller.ozon.ru.evil.example",
            "https://api-seller.ozon.ru:444",
            "https://api-seller.ozon.ru/v1",
            "https://api-seller.ozon.ru?redirect=evil",
            "https://api-seller.ozon.ru#fragment",
            "https://user:secret@api-seller.ozon.ru",
        ] {
            let error = result(Some(("OZON_API_BASE_URL", value)))
                .unwrap_err()
                .to_string();
            assert!(error.contains("OZON_API_BASE_URL"));
            assert!(!error.contains("secret"));
        }
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
                ("MCP_JWT_AUDIENCE", "http://localhost:8788/mcp"),
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
                ("MCP_JWT_AUDIENCE", "http://localhost:8788/mcp"),
                ("MCP_PUBLIC_URL", "http://localhost:8788/mcp"),
                ("MCP_JWKS_CACHE_TTL_SECONDS", value),
            ]);
            assert!(
                AppConfig::from_lookup(|key| jwt.get(key).map(|value| (*value).to_owned()))
                    .is_err()
            );
        }

        for value in [
            "",
            "mcp:tools\tanalytics:read",
            "mcp:to\"ols",
            "mcp:\\tools",
            "mcp:инструменты",
        ] {
            let jwt = BTreeMap::from([
                ("MCP_AUTH_MODE", "jwt"),
                ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
                ("MCP_JWT_ISSUER", "http://issuer.test/realms/ofk"),
                ("MCP_JWT_AUDIENCE", "http://localhost:8788/mcp"),
                ("MCP_PUBLIC_URL", "http://localhost:8788/mcp"),
                ("MCP_JWT_REQUIRED_SCOPES", value),
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
            ("MCP_JWT_AUDIENCE", "http://localhost:8788/mcp"),
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
    fn dev_http_requires_loopback_or_an_explicit_container_opt_in() {
        let path = write_registry(&sample_registry());
        let load = |values: &BTreeMap<&str, &str>| {
            AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned()))
        };

        for bind in ["127.0.0.1:8787", "127.42.0.1:8787", "[::1]:8787"] {
            let values = BTreeMap::from([
                ("MCP_ACTOR_ID", "admin"),
                ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
                ("MCP_BIND", bind),
            ]);
            assert!(load(&values).is_ok(), "bind={bind}");
        }

        for bind in ["0.0.0.0:8787", "[::]:8787", "192.0.2.10:8787"] {
            let values = BTreeMap::from([
                ("MCP_ACTOR_ID", "admin"),
                ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
                ("MCP_BIND", bind),
            ]);
            let error = load(&values).unwrap_err().to_string();
            assert!(error.contains("MCP_DEV_ALLOW_NON_LOOPBACK=true"), "{error}");
        }

        let opted_in = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("MCP_BIND", "0.0.0.0:8787"),
            ("MCP_DEV_ALLOW_NON_LOOPBACK", "true"),
        ]);
        assert_eq!(
            load(&opted_in).unwrap().bind,
            "0.0.0.0:8787".parse().unwrap()
        );

        let stdio = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("MCP_BIND", "0.0.0.0:8787"),
            ("MCP_TRANSPORT", "stdio"),
        ]);
        assert_eq!(load(&stdio).unwrap().transport, TransportMode::Stdio);

        let stdio_typo = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("MCP_TRANSPORT", "stdio"),
            ("MCP_DEV_ALLOW_NON_LOOPBACK", "TRUE"),
        ]);
        assert!(load(&stdio_typo).is_err());

        let jwt_typo = BTreeMap::from([
            ("MCP_AUTH_MODE", "jwt"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("MCP_JWT_ISSUER", "http://issuer.test/realms/ofk"),
            ("MCP_JWT_AUDIENCE", "https://mcp.example/mcp"),
            ("MCP_PUBLIC_URL", "https://mcp.example/mcp"),
            ("MCP_DEV_ALLOW_NON_LOOPBACK", "yes"),
        ]);
        assert!(load(&jwt_typo).is_err());
    }

    #[test]
    fn app_config_loads_jwt_mode_without_trusted_actor() {
        let path = write_registry(&sample_registry());
        let values = BTreeMap::from([
            ("MCP_AUTH_MODE", "jwt"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("MCP_JWT_ISSUER", "https://issuer.example.com/"),
            ("MCP_JWT_AUDIENCE", "https://mcp.example.com/mcp"),
            (
                "MCP_JWT_JWKS_URL",
                "https://issuer.example.com/.well-known/jwks.json",
            ),
            ("MCP_PUBLIC_URL", "https://mcp.example.com/mcp"),
            ("MCP_JWKS_CACHE_TTL_SECONDS", "600"),
        ]);
        let config =
            AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned())).unwrap();
        let rendered = format!("{:?}", config.auth);
        assert!(rendered.contains("https://issuer.example.com"));
        assert!(rendered.contains("audience: \"https://mcp.example.com/mcp\""));
        assert!(rendered.contains("https://mcp.example.com/mcp"));
        assert!(rendered.contains("https://mcp.example.com/.well-known/oauth-protected-resource"));
        assert!(rendered.contains("mcp:tools"));
        assert!(rendered.contains("600s"));

        assert!(matches!(
            config.auth,
            AuthConfig::Jwt(JwtConfig {
                ref required_scopes,
                ..
            }) if required_scopes == &["mcp:tools"]
        ));

        let custom_values = BTreeMap::from([
            ("MCP_AUTH_MODE", "jwt"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("MCP_JWT_ISSUER", "https://issuer.example.com"),
            ("MCP_JWT_AUDIENCE", "https://mcp.example.com/mcp"),
            ("MCP_PUBLIC_URL", "https://mcp.example.com/mcp"),
            (
                "MCP_JWT_REQUIRED_SCOPES",
                " mcp:tools analytics:read mcp:tools ",
            ),
        ]);
        let custom =
            AppConfig::from_lookup(|key| custom_values.get(key).map(|value| (*value).to_owned()))
                .unwrap();
        assert!(matches!(
            custom.auth,
            AuthConfig::Jwt(JwtConfig {
                ref required_scopes,
                ..
            }) if required_scopes == &["mcp:tools", "analytics:read"]
        ));
    }

    #[test]
    fn jwt_audience_must_be_the_normalized_public_resource_url() {
        let path = write_registry(&sample_registry());
        let config_from = |audience: &str, public_url: &str| {
            let values = BTreeMap::from([
                ("MCP_AUTH_MODE", "jwt"),
                ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
                ("MCP_JWT_ISSUER", "http://localhost:8180/realms/ofk"),
                ("MCP_JWT_AUDIENCE", audience),
                ("MCP_PUBLIC_URL", public_url),
            ]);
            AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned()))
        };

        let config = config_from("http://localhost/mcp", "HTTP://LOCALHOST:80/mcp").unwrap();
        assert!(matches!(
            config.auth,
            AuthConfig::Jwt(JwtConfig {
                ref audience,
                ref resource_url,
                ..
            }) if audience == "http://localhost/mcp" && resource_url == audience
        ));

        for audience in [
            "ozonofk-mcp",
            "ftp://localhost:8788/mcp",
            "http://localhost:8788/mcp/",
            "http://localhost:8788/other",
        ] {
            let error = config_from(audience, "http://localhost:8788/mcp")
                .unwrap_err()
                .to_string();
            assert!(error.contains("MCP_JWT_AUDIENCE"), "{error}");
        }
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

    #[test]
    fn optional_dotenv_ignores_only_absence_and_redacts_invalid_lines() {
        assert!(finish_optional_dotenv_load(Ok(PathBuf::from(".env"))).is_ok());
        assert!(
            finish_optional_dotenv_load(Err(dotenvy::Error::Io(
                std::io::ErrorKind::NotFound.into()
            )))
            .is_ok()
        );

        let parse_error = finish_optional_dotenv_load(Err(dotenvy::Error::LineParse(
            "OZON_API_KEY=super-secret value".to_owned(),
            26,
        )))
        .unwrap_err()
        .to_string();
        assert!(parse_error.contains(".env"));
        assert!(!parse_error.contains("super-secret"));

        assert!(
            finish_optional_dotenv_load(Err(dotenvy::Error::Io(
                std::io::ErrorKind::PermissionDenied.into()
            )))
            .is_err()
        );
    }

    /// Every environment-variable name the registry can name must be validated
    /// at the registry boundary — not just the first field of each pair. A field
    /// whose validation was dropped would let a registry edit point the process
    /// at an arbitrary environment variable, so each one is corrupted
    /// individually and the error must name that exact field.
    #[test]
    fn every_env_name_field_in_the_registry_is_validated_individually() {
        type Corrupt = fn(&mut AccessRegistry);

        let ozon_fields: Vec<(&str, Corrupt)> = vec![
            ("client_id_env", |registry| {
                registry.accounts[0].ozon.as_mut().unwrap().client_id_env = "BAD-NAME".into();
            }),
            ("api_key_env", |registry| {
                registry.accounts[0].ozon.as_mut().unwrap().api_key_env = "BAD-NAME".into();
            }),
            ("performance.client_id_env", |registry| {
                registry.accounts[0]
                    .ozon
                    .as_mut()
                    .unwrap()
                    .performance
                    .as_mut()
                    .unwrap()
                    .client_id_env = "BAD-NAME".into();
            }),
            ("performance.client_secret_env", |registry| {
                registry.accounts[0]
                    .ozon
                    .as_mut()
                    .unwrap()
                    .performance
                    .as_mut()
                    .unwrap()
                    .client_secret_env = "BAD-NAME".into();
            }),
        ];
        for (field, corrupt) in ozon_fields {
            let mut registry = performance_registry();
            corrupt(&mut registry);
            let error = registry.validate().unwrap_err().to_string();
            // `starts_with` rather than `contains`, so a broken
            // `performance.client_id_env` cannot be mistaken for `client_id_env`.
            assert!(
                error.starts_with(field),
                "{field} must be validated at the registry boundary, got: {error}"
            );
        }

        let mut wildberries = sample_registry();
        wildberries.accounts.push(MarketplaceAccount {
            id: "wb_shop".into(),
            organization: "WB Shop".into(),
            marketplace: Marketplace::Wildberries,
            seller_client_id: "42".into(),
            manager_id: "manager".into(),
            ozon: None,
            wildberries: Some(WildberriesAccount {
                api_token_env: "WB_TOKEN".into(),
                seller_sid: None,
            }),
        });
        wildberries.validate().unwrap();
        wildberries.accounts[1]
            .wildberries
            .as_mut()
            .unwrap()
            .api_token_env = "BAD-NAME".into();
        let error = wildberries.validate().unwrap_err().to_string();
        assert!(
            error.starts_with("api_token_env"),
            "api_token_env must be validated at the registry boundary, got: {error}"
        );
    }

    /// Credential *values* read out of the environment must be rejected when
    /// they contain whitespace, control bytes or non-ASCII, at every position
    /// they are loaded from. These values become outbound HTTP header values,
    /// so a newline that slipped through is a header-injection primitive.
    #[test]
    fn credential_values_are_validated_at_every_load_position() {
        // A bare CR-LF payload that would split an outbound request header.
        const INJECTION: &str = "value\r\nX-Injected: 1";

        for (variable, expected) in [
            ("SHOP_ID", "SHOP_ID"),
            ("SHOP_KEY", "SHOP_KEY"),
            ("SHOP_PERFORMANCE_ID", "SHOP_PERFORMANCE_ID"),
            ("SHOP_PERFORMANCE_SECRET", "SHOP_PERFORMANCE_SECRET"),
        ] {
            let path = write_registry(&performance_registry());
            let mut values = BTreeMap::from([
                ("MCP_ACTOR_ID", "admin"),
                ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
                ("SHOP_ID", "client"),
                ("SHOP_KEY", "key"),
                ("SHOP_PERFORMANCE_ID", "performance-client"),
                ("SHOP_PERFORMANCE_SECRET", "performance-secret"),
            ]);
            values.insert(variable, INJECTION);
            let error =
                AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned()))
                    .unwrap_err()
                    .to_string();
            assert!(
                error.contains(expected),
                "a poisoned {variable} must be rejected and named, got: {error}"
            );
        }

        let mut registry = sample_registry();
        registry.accounts.push(MarketplaceAccount {
            id: "wb_shop".into(),
            organization: "WB Shop".into(),
            marketplace: Marketplace::Wildberries,
            seller_client_id: "42".into(),
            manager_id: "manager".into(),
            ozon: None,
            wildberries: Some(WildberriesAccount {
                api_token_env: "WB_TOKEN".into(),
                seller_sid: None,
            }),
        });
        let path = write_registry(&registry);
        let values = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
            ("WB_TOKEN", INJECTION),
        ]);
        let error = AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned()))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("WB_TOKEN"),
            "a poisoned WB_TOKEN must be rejected and named, got: {error}"
        );
    }

    /// `AppConfig::from_env` reads the registry through `RegistrySource`, which
    /// both parses it up front and re-reads it on demand. A registry that is
    /// unreadable or invalid must abort startup rather than yield a config with
    /// an empty access model.
    #[test]
    fn startup_refuses_a_missing_or_invalid_registry() {
        let missing = std::env::temp_dir().join("mcp-ozon-config-does-not-exist.json");
        let _ = std::fs::remove_file(&missing);
        let values = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", missing.to_str().unwrap()),
        ]);
        let error = AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("реестр доступа"), "{error}");

        // Structurally valid JSON that violates the registry contract: an
        // account whose manager is not a known actor.
        let mut orphaned = sample_registry();
        orphaned.accounts[0].manager_id = "nobody".into();
        let path = write_registry(&orphaned);
        let values = BTreeMap::from([
            ("MCP_ACTOR_ID", "admin"),
            ("MCP_ACCESS_CONFIG", path.to_str().unwrap()),
        ]);
        let error = AppConfig::from_lookup(|key| values.get(key).map(|value| (*value).to_owned()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("nobody"), "{error}");
    }
}
