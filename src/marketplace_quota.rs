//! Cross-process marketplace departure and cooldown coordination.
//!
//! The database contains only opaque quota keys and deadlines. A reservation is
//! spent immediately before a wire attempt and is never refunded on cancellation.
//! This module schedules traffic; it never authorizes a marketplace operation.

use std::{
    fmt::{self, Write as _},
    str::FromStr,
    sync::{Arc, LazyLock},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use mcp_storage::SupervisedClient;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::{
    sync::{Mutex, OnceCell},
    time::{Instant, timeout},
};
use tokio_postgres::Config;

const DATABASE_ENV: &str = "MCP_MARKETPLACE_QUOTA_DATABASE_URL";
const REQUIRED_ENV: &str = "MCP_MARKETPLACE_QUOTA_REQUIRED";
const OPERATION_TIMEOUT: Duration = Duration::from_secs(6);
const CONNECT_COOLDOWN: Duration = Duration::from_secs(5);
const MAX_DELAY_MILLIS: u128 = 86_400_000;
// One supervised quota session per process, regardless of account/client count.
// Environment configuration, like marketplace credentials, requires a restart.
static ENV_QUOTA: LazyLock<SharedQuota> = LazyLock::new(|| {
    SharedQuota::from_settings(std::env::var(DATABASE_ENV), &std::env::var(REQUIRED_ENV))
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum QuotaError {
    #[error("shared marketplace quota is unavailable")]
    Unavailable,
    #[error("shared marketplace quota is cooling down")]
    Limited { retry_after: Duration },
    #[error("shared marketplace quota identity is invalid")]
    InvalidIdentity,
}

/// An opaque identity scoped to a vendor account and a method group.
#[derive(Clone, PartialEq, Eq)]
pub struct QuotaKey(String);

impl fmt::Debug for QuotaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("QuotaKey(<opaque>)")
    }
}

impl QuotaKey {
    pub fn ozon_seller(client_id: &str, bucket: &str) -> Result<Self, QuotaError> {
        Self::new("ozon_seller", client_id, bucket)
    }

    pub fn ozon_performance(client_id: &str, bucket: &str) -> Result<Self, QuotaError> {
        Self::new("ozon_performance", client_id, bucket)
    }

    /// Uses the stable seller SID to share a quota across token rotations and
    /// distinct tokens for one seller. The JWT is not verified here: credentials
    /// and write permissions are independently validated by their existing
    /// boundaries. This decoded claim can only group/throttle requests.
    pub fn wb(token: &str, bucket: &str) -> Result<Self, QuotaError> {
        #[derive(Deserialize)]
        struct Claims {
            sid: String,
        }

        if token.len() > 16_384 {
            return Err(QuotaError::InvalidIdentity);
        }
        let mut segments = token.split('.');
        let (Some(header), Some(payload), Some(signature)) =
            (segments.next(), segments.next(), segments.next())
        else {
            return Err(QuotaError::InvalidIdentity);
        };
        if header.is_empty() || signature.is_empty() || segments.next().is_some() {
            return Err(QuotaError::InvalidIdentity);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| QuotaError::InvalidIdentity)?;
        let claims: Claims =
            serde_json::from_slice(&bytes).map_err(|_| QuotaError::InvalidIdentity)?;
        if !canonical_sid(&claims.sid) {
            return Err(QuotaError::InvalidIdentity);
        }
        Self::new("wildberries", &claims.sid, bucket)
    }

    fn new(vendor: &str, identity: &str, bucket: &str) -> Result<Self, QuotaError> {
        if identity.is_empty()
            || identity.len() > 256
            || !identity
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'@'))
            || bucket.is_empty()
            || bucket.len() > 64
            || !bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
        {
            return Err(QuotaError::InvalidIdentity);
        }
        let mut hash = Sha256::new();
        for part in [vendor, identity, bucket] {
            hash.update(part.as_bytes());
            hash.update([0]);
        }
        let mut encoded = String::with_capacity(64);
        for byte in hash.finalize() {
            let _ = write!(&mut encoded, "{byte:02x}");
        }
        Ok(Self(encoded))
    }
}

fn canonical_sid(sid: &str) -> bool {
    sid.len() == 36
        && sid != "00000000-0000-0000-0000-000000000000"
        && sid.bytes().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
}

struct DatabaseQuota {
    config: Config,
    connection: OnceCell<SupervisedClient>,
    next_connect: Mutex<Instant>,
}

#[derive(Clone)]
enum Backend {
    Disabled,
    Invalid,
    Postgres(Arc<DatabaseQuota>),
}

/// Optional only for legacy/offline runtimes. A configured but broken backend
/// always fails closed; it never falls back to a process-local allowance.
#[derive(Clone)]
pub struct SharedQuota {
    backend: Backend,
}

impl fmt::Debug for SharedQuota {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedQuota")
            .field("enabled", &self.is_enabled())
            .finish_non_exhaustive()
    }
}

impl SharedQuota {
    #[must_use]
    pub fn from_env() -> Self {
        ENV_QUOTA.clone()
    }

    fn from_settings(
        database: Result<String, std::env::VarError>,
        required: &Result<String, std::env::VarError>,
    ) -> Self {
        let required = match required.as_deref() {
            Ok("true") => true,
            Ok("false") | Err(std::env::VarError::NotPresent) => false,
            _ => {
                return Self {
                    backend: Backend::Invalid,
                };
            }
        };
        match database {
            Ok(url) => Self::from_database_url(&url),
            Err(std::env::VarError::NotPresent) if !required => Self {
                backend: Backend::Disabled,
            },
            Err(_) => Self {
                backend: Backend::Invalid,
            },
        }
    }

    #[must_use]
    pub fn from_database_url(url: &str) -> Self {
        let config = if url.starts_with("postgresql://") || url.starts_with("postgres://") {
            Config::from_str(url).ok()
        } else {
            None
        };
        let Some(config) = config.filter(|config| {
            matches!(
                config.get_user(),
                Some(
                    "report_collector"
                        | "report_refresh_requester"
                        | "control_writer"
                        | "ozon_control_planner"
                        | "ozon_control_executor"
                        | "wb_automation_writer"
                        | "position_collector"
                )
            )
        }) else {
            return Self {
                backend: Backend::Invalid,
            };
        };
        Self {
            backend: Backend::Postgres(Arc::new(DatabaseQuota {
                config,
                connection: OnceCell::new(),
                next_connect: Mutex::new(Instant::now()),
            })),
        }
    }

    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        !matches!(self.backend, Backend::Disabled)
    }

    /// A read-only schema/privilege probe; consumes no vendor allowance.
    pub async fn preflight(&self) -> Result<(), QuotaError> {
        let Some(database) = self.database()? else {
            return Ok(());
        };
        timeout(OPERATION_TIMEOUT, Box::pin(async {
            let client = database.connection().await?.acquire().await.map_err(|_| QuotaError::Unavailable)?;
            let row = client.query_one("SELECT has_function_privilege(current_user, 'marketplace_quota.try_acquire(text,bigint)', 'EXECUTE') AND has_function_privilege(current_user, 'marketplace_quota.extend_cooldown(text,bigint)', 'EXECUTE')", &[]).await.map_err(|_| QuotaError::Unavailable)?;
            drop(client);
            if row.get::<_, bool>(0) { Ok(()) } else { Err(QuotaError::Unavailable) }
        })).await.map_err(|_| QuotaError::Unavailable)?
    }

    pub async fn admit(&self, key: &QuotaKey, interval: Duration) -> Result<(), QuotaError> {
        let Some(database) = self.database()? else {
            return Ok(());
        };
        let millis = bounded_millis(interval)?;
        timeout(
            OPERATION_TIMEOUT,
            Box::pin(async {
                let client = database
                    .connection()
                    .await?
                    .acquire()
                    .await
                    .map_err(|_| QuotaError::Unavailable)?;
                let row = client
                    .query_one(
                        "SELECT marketplace_quota.try_acquire($1, $2)",
                        &[&key.0, &millis],
                    )
                    .await
                    .map_err(|_| QuotaError::Unavailable)?;
                drop(client);
                match row
                    .try_get::<_, i64>(0)
                    .map_err(|_| QuotaError::Unavailable)?
                {
                    0 => Ok(()),
                    wait if wait > 0 => Err(QuotaError::Limited {
                        retry_after: Duration::from_millis(wait.unsigned_abs()),
                    }),
                    _ => Err(QuotaError::Unavailable),
                }
            }),
        )
        .await
        .map_err(|_| QuotaError::Unavailable)?
    }

    /// Extends the shared deadline after a vendor throttle response. It cannot
    /// undo a wire attempt; callers must retain the original write outcome.
    pub async fn defer(&self, key: &QuotaKey, delay: Duration) -> Result<(), QuotaError> {
        let Some(database) = self.database()? else {
            return Ok(());
        };
        let millis = cooldown_millis(delay);
        timeout(
            OPERATION_TIMEOUT,
            Box::pin(async {
                let client = database
                    .connection()
                    .await?
                    .acquire()
                    .await
                    .map_err(|_| QuotaError::Unavailable)?;
                client
                    .query_one(
                        "SELECT marketplace_quota.extend_cooldown($1, $2)",
                        &[&key.0, &millis],
                    )
                    .await
                    .map_err(|_| QuotaError::Unavailable)?;
                drop(client);
                Ok(())
            }),
        )
        .await
        .map_err(|_| QuotaError::Unavailable)?
    }

    fn database(&self) -> Result<Option<&DatabaseQuota>, QuotaError> {
        match &self.backend {
            Backend::Disabled => Ok(None),
            Backend::Invalid => Err(QuotaError::Unavailable),
            Backend::Postgres(database) => Ok(Some(database)),
        }
    }
}

impl DatabaseQuota {
    async fn connection(&self) -> Result<&SupervisedClient, QuotaError> {
        self.connection
            .get_or_try_init(|| async {
                let mut next_connect = self.next_connect.lock().await;
                if *next_connect > Instant::now() {
                    return Err(QuotaError::Unavailable);
                }
                *next_connect = Instant::now() + CONNECT_COOLDOWN;
                drop(next_connect);
                SupervisedClient::connect(&self.config, "marketplace-quota")
                    .await
                    .map_err(|_| QuotaError::Unavailable)
            })
            .await
    }
}

fn bounded_millis(duration: Duration) -> Result<i64, QuotaError> {
    // Round up, so even a sub-millisecond residual cooldown cannot be bypassed.
    let millis = duration.as_nanos().div_ceil(1_000_000);
    if !(1..=MAX_DELAY_MILLIS).contains(&millis) {
        return Err(QuotaError::Unavailable);
    }
    i64::try_from(millis).map_err(|_| QuotaError::Unavailable)
}

fn cooldown_millis(duration: Duration) -> i64 {
    let millis = duration.as_nanos().div_ceil(1_000_000).max(1);
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests;
