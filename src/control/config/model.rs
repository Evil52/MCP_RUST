use std::{net::SocketAddr, num::NonZeroUsize, time::Duration};

use tokio_postgres::Config as PostgresConfig;

use crate::{
    config::{JwtConfig, PerformanceCredentials, RegistrySource, StoreId, TransportMode},
    control::policy::ControlPolicy,
};

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
    pub ozon_runtime: Option<ControlOzonRuntimeConfig>,
    pub wb_runtime: Option<ControlWbRuntimeConfig>,
}

#[derive(Clone)]
pub struct ControlOzonRuntimeConfig {
    pub account_id: String,
    /// Role-specific PostgreSQL identity. The HTTP planner and the durable
    /// executor deliberately cannot share database credentials or grants.
    pub database: PostgresConfig,
    /// Always false for the credentialless planner. The executor sets it only
    /// when policy and the explicit process gate both allow marketplace writes.
    pub writer_enabled: bool,
    /// Marketplace identity and egress exist only in the executor process. The
    /// HTTP planner is intentionally incapable of bypassing the DB outbox.
    pub marketplace: Option<ControlOzonMarketplaceRuntimeConfig>,
}

#[derive(Clone)]
pub struct ControlOzonMarketplaceRuntimeConfig {
    pub store_id: StoreId,
    pub credentials: PerformanceCredentials,
    pub proxy_url: String,
    pub request_timeout: Duration,
}

impl std::fmt::Debug for ControlOzonRuntimeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlOzonRuntimeConfig")
            .field("account_id", &self.account_id)
            .field("database", &"<redacted>")
            .field("writer_enabled", &self.writer_enabled)
            .field("marketplace_configured", &self.marketplace.is_some())
            .finish()
    }
}

impl std::fmt::Debug for ControlOzonMarketplaceRuntimeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlOzonMarketplaceRuntimeConfig")
            .field("store_id", &self.store_id)
            .field("credentials", &"<redacted>")
            .field("proxy_url", &self.proxy_url)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
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
    /// Personal production read-only token with Promotion access. Additional
    /// read categories require an explicit runtime opt-in.
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
