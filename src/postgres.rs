//! Supervised PostgreSQL connectivity shared by the isolated worker binaries.
//!
//! Every worker owns exactly one logical database session. Three properties
//! are enforced here, once, so no repository has to remember them:
//!
//! 1. The driver task is supervised. A terminated connection is logged instead
//!    of vanishing with a dropped `JoinHandle`.
//! 2. A dead session is replaced on demand. Losing the connection to a
//!    restarted or failed-over server degrades one operation, not the process.
//! 3. The transport is bounded, and the server's own bounds are audited. The
//!    schema bootstrap already gives each role a `statement_timeout` and an
//!    `idle_in_transaction_session_timeout`, so this module does not restate
//!    them — overriding could only weaken them. It verifies they are present
//!    and adds what a server-side timeout cannot provide: detection of a peer
//!    that vanished without closing the socket, which would otherwise leave
//!    this process waiting for a reply that is never coming, holding the one
//!    session every other database operation needs.

use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use tokio::{
    sync::{Mutex, MutexGuard},
    time::Instant,
};
use tokio_postgres::{Client, Config, NoTls};

/// Ceiling accepted for the server-applied `statement_timeout`.
///
/// The value itself belongs to the database role, which is the authority a
/// DBA can audit and change without a redeploy. This process only refuses to
/// run against a session whose bound is missing or uselessly large.
pub const MAX_STATEMENT_TIMEOUT_MILLIS: i64 = 120_000;

/// Ceiling accepted for the server-applied `idle_in_transaction_session_timeout`.
pub const MAX_IDLE_IN_TRANSACTION_MILLIS: i64 = 120_000;

/// TCP-level connection establishment budget.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Keepalive probing detects a peer that vanished without a FIN, which is the
/// exact failure a server-side timeout cannot observe.
const KEEPALIVES_IDLE: Duration = Duration::from_secs(30);
const KEEPALIVES_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVES_RETRIES: u32 = 3;

/// Bounds how long unacknowledged data may stay outstanding before the kernel
/// tears the connection down, so a black-holed session cannot outlive it.
const TCP_USER_TIMEOUT: Duration = Duration::from_secs(60);

/// Minimum spacing between reconnection attempts. A database that is down must
/// not turn a caller's retry loop into a connection flood.
const RECONNECT_COOLDOWN: Duration = Duration::from_secs(5);

/// The database session is not usable right now.
///
/// Carries no detail on purpose: the connection string and server diagnostics
/// must never reach a caller that renders errors into a report or a tool reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresUnavailable;

/// Applies the transport-level bounds a database role cannot express.
///
/// Statement and transaction timeouts are deliberately left alone: they are
/// already set per role in the schema bootstrap, and a client that overrode
/// them could only ever weaken a bound the DBA chose. What no server-side
/// timeout can cover is a peer that disappears without closing the socket —
/// the server may well cancel its own query, but this process would wait for
/// a reply that is never coming. That gap is what these settings close.
pub fn harden(config: &mut Config, application_name: &str) {
    config.connect_timeout(CONNECT_TIMEOUT);
    config.application_name(application_name);
    config.keepalives(true);
    config.keepalives_idle(KEEPALIVES_IDLE);
    config.keepalives_interval(KEEPALIVES_INTERVAL);
    config.keepalives_retries(KEEPALIVES_RETRIES);
    config.tcp_user_timeout(TCP_USER_TIMEOUT);
}

struct ConnectionSlot {
    client: Option<Client>,
    next_attempt_at: Instant,
}

/// One supervised, self-healing PostgreSQL session.
///
/// Serializing access through a mutex keeps the least-privilege single-session
/// model the database roles are built around. Reconnection happens under that
/// same mutex, so a recovering worker opens one replacement session rather than
/// one per waiting caller.
pub struct SupervisedClient {
    component: &'static str,
    /// `None` for a caller-supplied client, which has no configuration to
    /// reconnect with and therefore fails closed once its session ends.
    config: Option<Config>,
    slot: Mutex<ConnectionSlot>,
}

impl SupervisedClient {
    /// Connects and supervises the driver task.
    pub async fn connect(
        config: &Config,
        component: &'static str,
    ) -> Result<Self, PostgresUnavailable> {
        let client = connect_supervised(config, component).await?;
        Ok(Self {
            component,
            config: Some(config.clone()),
            slot: Mutex::new(ConnectionSlot {
                client: Some(client),
                next_attempt_at: Instant::now(),
            }),
        })
    }

    /// Adopts an already-established client, as integration tests do.
    ///
    /// Such a session is never reconnected: the caller owns its lifecycle and
    /// silently substituting a fresh one could cross a transactional boundary
    /// the test is asserting on.
    #[must_use]
    pub fn preconnected(client: Client, component: &'static str) -> Self {
        Self {
            component,
            config: None,
            slot: Mutex::new(ConnectionSlot {
                client: Some(client),
                next_attempt_at: Instant::now(),
            }),
        }
    }

    /// Borrows the live session, replacing a terminated one when possible.
    pub async fn acquire(&self) -> Result<ClientGuard<'_>, PostgresUnavailable> {
        let mut slot = self.slot.lock().await;
        if slot
            .client
            .as_ref()
            .is_some_and(tokio_postgres::Client::is_closed)
        {
            tracing::warn!(
                component = self.component,
                "PostgreSQL session ended; it will be re-established on demand"
            );
            slot.client = None;
        }
        if slot.client.is_none() {
            let config = self.config.as_ref().ok_or(PostgresUnavailable)?;
            let now = Instant::now();
            if slot.next_attempt_at > now {
                return Err(PostgresUnavailable);
            }
            // Reserve the cooldown before attempting, so a failed attempt
            // paces the next one even though this guard is released early.
            slot.next_attempt_at = now + RECONNECT_COOLDOWN;
            slot.client = Some(connect_supervised(config, self.component).await?);
            tracing::info!(
                component = self.component,
                "PostgreSQL session re-established"
            );
        }
        Ok(ClientGuard { slot })
    }

    /// Confirms the session can still complete a round trip.
    ///
    /// Used by container health checks, which must observe this process's own
    /// session rather than proving that some new connection would succeed.
    pub async fn probe(&self) -> Result<(), PostgresUnavailable> {
        let client = self.acquire().await?;
        client
            .query_one("SELECT 1", &[])
            .await
            .map(std::mem::drop)
            .map_err(|_| PostgresUnavailable)
    }

    /// Refuses a session the server left without statement or transaction
    /// bounds.
    ///
    /// Those bounds live on the database role rather than in this process, so
    /// they can be audited and retuned without a redeploy. The cost of that
    /// choice is that dropping an `ALTER ROLE` would silently unbound every
    /// worker; checking the effective values at startup turns that into a
    /// refusal to start instead.
    pub async fn verify_session_bounds(&self) -> Result<(), PostgresUnavailable> {
        let client = self.acquire().await?;
        // `pg_settings.setting` reports the effective value in the parameter's
        // base unit — milliseconds here — so no unit string has to be parsed.
        let row = client
            .query_one(
                "SELECT \
                    (SELECT setting::bigint FROM pg_settings \
                       WHERE name = 'statement_timeout'), \
                    (SELECT setting::bigint FROM pg_settings \
                       WHERE name = 'idle_in_transaction_session_timeout')",
                &[],
            )
            .await
            .ok()
            .ok_or(PostgresUnavailable)?;
        let statement_timeout: i64 = row.get(0);
        let idle_in_transaction: i64 = row.get(1);
        let bounded = |value: i64, ceiling: i64| value > 0 && value <= ceiling;
        if bounded(statement_timeout, MAX_STATEMENT_TIMEOUT_MILLIS)
            && bounded(idle_in_transaction, MAX_IDLE_IN_TRANSACTION_MILLIS)
        {
            return Ok(());
        }
        tracing::error!(
            component = self.component,
            statement_timeout_millis = statement_timeout,
            idle_in_transaction_millis = idle_in_transaction,
            "PostgreSQL role is missing a bounded statement or transaction timeout"
        );
        Err(PostgresUnavailable)
    }
}

async fn connect_supervised(
    config: &Config,
    component: &'static str,
) -> Result<Client, PostgresUnavailable> {
    let (client, connection) = config
        .connect(NoTls)
        .await
        .map_err(|_| PostgresUnavailable)?;
    // Supervised rather than detached: the driver future owns the socket, and
    // its termination is the only place the reason for a lost session exists.
    std::mem::drop(tokio::spawn(async move {
        match connection.await {
            Ok(()) => tracing::info!(component, "PostgreSQL connection closed cleanly"),
            Err(error) => {
                tracing::warn!(component, %error, "PostgreSQL connection terminated");
            }
        }
    }));
    Ok(client)
}

/// An exclusive borrow of the live session.
pub struct ClientGuard<'a> {
    slot: MutexGuard<'a, ConnectionSlot>,
}

impl Deref for ClientGuard<'_> {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.slot
            .client
            .as_ref()
            .expect("an acquired guard always holds a live session")
    }
}

impl DerefMut for ClientGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.slot
            .client
            .as_mut()
            .expect("an acquired guard always holds a live session")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hardening_applies_transport_bounds_and_pins_the_application_name() {
        let mut config: Config = "postgresql://report_worker:secret@db/reports\
             ?application_name=spoofed"
            .parse()
            .expect("the fixture URL parses");
        harden(&mut config, "mcp-ozon-report-worker");
        assert_eq!(
            config.get_application_name(),
            Some("mcp-ozon-report-worker")
        );
        assert_eq!(config.get_connect_timeout(), Some(&CONNECT_TIMEOUT));
        assert_eq!(config.get_keepalives_idle(), KEEPALIVES_IDLE);
        assert_eq!(config.get_keepalives_interval(), Some(KEEPALIVES_INTERVAL));
        assert_eq!(config.get_keepalives_retries(), Some(KEEPALIVES_RETRIES));
        assert_eq!(config.get_tcp_user_timeout(), Some(&TCP_USER_TIMEOUT));
    }

    #[test]
    fn hardening_leaves_server_side_session_bounds_to_the_database_role() {
        // Overriding these could only ever weaken the role's own values, so a
        // URL that carries them is left exactly as supplied and audited by
        // `verify_session_bounds` instead.
        let mut config: Config = "postgresql://report_worker:secret@db/reports\
             ?options=-c%20statement_timeout%3D9000"
            .parse()
            .expect("the fixture URL parses");
        harden(&mut config, "mcp-ozon-report-worker");
        assert_eq!(config.get_options(), Some("-c statement_timeout=9000"));
    }

    /// The schema bootstrap sets 60s and 30s; a compliant deployment must
    /// never be refused by the ceilings this module accepts.
    const _: () = {
        assert!(MAX_STATEMENT_TIMEOUT_MILLIS >= 60_000);
        assert!(MAX_IDLE_IN_TRANSACTION_MILLIS >= 30_000);
    };

    #[tokio::test]
    async fn a_preconnected_session_is_never_silently_replaced() {
        // A caller-supplied client has no configuration, so the reconnect path
        // must fail closed instead of inventing a new session.
        let config: Config = "postgresql://report_worker:secret@127.0.0.1:1/reports"
            .parse()
            .expect("the fixture URL parses");
        let supervised = SupervisedClient {
            component: "test",
            config: None,
            slot: Mutex::new(ConnectionSlot {
                client: None,
                next_attempt_at: Instant::now(),
            }),
        };
        assert_eq!(supervised.acquire().await.err(), Some(PostgresUnavailable));
        std::mem::drop(config);
    }

    #[tokio::test]
    async fn a_failed_reconnect_is_paced_by_the_cooldown() {
        let mut config: Config = "postgresql://report_worker:secret@127.0.0.1:1/reports"
            .parse()
            .expect("the fixture URL parses");
        harden(&mut config, "test");
        let supervised = SupervisedClient {
            component: "test",
            config: Some(config),
            slot: Mutex::new(ConnectionSlot {
                client: None,
                next_attempt_at: Instant::now(),
            }),
        };
        // The first attempt reaches the closed port and reserves the cooldown.
        assert_eq!(supervised.acquire().await.err(), Some(PostgresUnavailable));
        let reserved = supervised.slot.lock().await.next_attempt_at;
        assert!(reserved > Instant::now());
        // The second attempt is refused by the cooldown without a syscall,
        // leaving the reservation untouched.
        assert_eq!(supervised.acquire().await.err(), Some(PostgresUnavailable));
        assert_eq!(supervised.slot.lock().await.next_attempt_at, reserved);
    }
}
