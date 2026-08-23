use std::{net::SocketAddr, num::NonZeroUsize, time::Duration};

use tokio_postgres::Config as PostgresConfig;

use crate::{
    config::{JwtConfig, RegistrySource, TransportMode},
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
