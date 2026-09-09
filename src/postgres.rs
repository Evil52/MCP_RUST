//! Compatibility facade for the isolated PostgreSQL infrastructure crate.

pub use mcp_storage::{
    CONNECT_TIMEOUT, ClientGuard, MAX_IDLE_IN_TRANSACTION_MILLIS, MAX_STATEMENT_TIMEOUT_MILLIS,
    PostgresUnavailable, SessionMetricsSnapshot, SupervisedClient, harden, prometheus_metrics,
};
