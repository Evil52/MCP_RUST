//! Supervised PostgreSQL connectivity shared by the isolated worker binaries.
//!
//! Each `SupervisedClient` owns one logical database session. Three properties
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

mod metrics;

use metrics::{SessionHold, SessionMetrics, SessionWait};
pub use metrics::{SessionMetricsSnapshot, prometheus_metrics};

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

/// Budget for TCP establishment and the complete PostgreSQL startup/auth exchange.
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
    metrics: SessionMetrics,
}

impl SupervisedClient {
    /// Applies transport bounds, verifies server-side bounds, and supervises the driver.
    ///
    /// Retains the hardened configuration for every replacement session. The
    /// caller's configuration and role-owned statement/transaction bounds are
    /// left unchanged.
    pub async fn connect(
        config: &Config,
        component: &'static str,
    ) -> Result<Self, PostgresUnavailable> {
        let mut config = config.clone();
        harden(&mut config, component);
        let client = connect_supervised(&config, component).await?;
        Ok(Self {
            component,
            config: Some(config),
            metrics: SessionMetrics::default(),
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
            metrics: SessionMetrics::default(),
            slot: Mutex::new(ConnectionSlot {
                client: Some(client),
                next_attempt_at: Instant::now(),
            }),
        }
    }

    /// Borrows the live session, replacing a terminated one when possible.
    pub async fn acquire(&self) -> Result<ClientGuard<'_>, PostgresUnavailable> {
        let wait = SessionWait::new(&self.metrics);
        let mut slot = self.slot.lock().await;
        wait.acquired();
        let hold = SessionHold::new(&self.metrics);
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
            let connected = connect_supervised(config, self.component).await;
            if connected.is_err() {
                // A slow failure can consume the initial reservation. Leave
                // a full cooldown for callers waiting behind this attempt.
                slot.next_attempt_at = Instant::now() + RECONNECT_COOLDOWN;
            }
            slot.client = Some(connected?);
            tracing::info!(
                component = self.component,
                "PostgreSQL session re-established"
            );
        }
        Ok(ClientGuard { slot, _hold: hold })
    }

    /// Reads this session's counters without acquiring its database mutex.
    #[must_use]
    pub fn session_metrics(&self) -> SessionMetricsSnapshot {
        self.metrics.snapshot()
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
    /// worker. New sessions are checked before publication, including after
    /// reconnection; this method also audits caller-supplied sessions.
    pub async fn verify_session_bounds(&self) -> Result<(), PostgresUnavailable> {
        let client = self.acquire().await?;
        verify_client_session_bounds(&client, self.component).await
    }
}

async fn verify_client_session_bounds(
    client: &Client,
    component: &'static str,
) -> Result<(), PostgresUnavailable> {
    // Inspect the client directly: reconnect already holds the slot mutex,
    // so calling `acquire` here would deadlock. The query has its own deadline
    // because the server's statement bound has not yet been established.
    // `pg_settings.setting` reports milliseconds without unit-string parsing.
    let row = tokio::time::timeout(
        CONNECT_TIMEOUT,
        client.query_one(
            "SELECT \
                (SELECT setting::bigint FROM pg_settings \
                   WHERE name = 'statement_timeout'), \
                (SELECT setting::bigint FROM pg_settings \
                   WHERE name = 'idle_in_transaction_session_timeout')",
            &[],
        ),
    )
    .await
    .map_err(|_| PostgresUnavailable)?
    .map_err(|_| PostgresUnavailable)?;
    let statement_timeout: i64 = row.get(0);
    let idle_in_transaction: i64 = row.get(1);
    let bounded = |value: i64, ceiling: i64| value > 0 && value <= ceiling;
    if bounded(statement_timeout, MAX_STATEMENT_TIMEOUT_MILLIS)
        && bounded(idle_in_transaction, MAX_IDLE_IN_TRANSACTION_MILLIS)
    {
        return Ok(());
    }
    tracing::error!(
        component,
        statement_timeout_millis = statement_timeout,
        idle_in_transaction_millis = idle_in_transaction,
        "PostgreSQL role is missing a bounded statement or transaction timeout"
    );
    Err(PostgresUnavailable)
}

async fn connect_supervised(
    config: &Config,
    component: &'static str,
) -> Result<Client, PostgresUnavailable> {
    // Config::connect_timeout only bounds opening each socket. A TCP peer can
    // accept it and then stall forever during PostgreSQL startup or auth, so
    // bound the whole exchange before publishing or spawning its driver.
    // This cold-path handshake is large; keep it off every caller's future
    // stack while retaining cancellation of the owned connection attempt.
    let (client, connection) =
        tokio::time::timeout(CONNECT_TIMEOUT, Box::pin(config.connect(NoTls)))
            .await
            .map_err(|_| PostgresUnavailable)?
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
    // Keep the new client private until its effective server-side bounds are
    // accepted. On failure it is dropped and never becomes an acquirable slot.
    verify_client_session_bounds(&client, component).await?;
    Ok(client)
}

/// An exclusive borrow of the live session.
pub struct ClientGuard<'a> {
    slot: MutexGuard<'a, ConnectionSlot>,
    _hold: SessionHold<'a>,
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

impl ClientGuard<'_> {
    /// Removes the current session from the supervised slot instead of
    /// returning it to the next caller.
    ///
    /// A session-level advisory-lock guard uses this on every abnormal drop:
    /// PostgreSQL releases the lock when the connection closes, while reusing
    /// an uncertain session could leave a marketplace campaign locked forever.
    pub fn discard(mut self) {
        self.slot.client.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{io::AsyncReadExt, net::TcpListener, sync::oneshot, task::JoinHandle};

    async fn stalled_postgres_peer() -> (Config, oneshot::Receiver<Vec<u8>>, JoinHandle<()>) {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("the loopback listener binds");
        let mut config = Config::new();
        config
            .host("127.0.0.1")
            .port(
                listener
                    .local_addr()
                    .expect("listener has an address")
                    .port(),
            )
            .user("startup-test")
            .ssl_mode(tokio_postgres::config::SslMode::Disable);
        let (started, startup_received) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("client connects");
            let mut header = [0_u8; 8];
            socket
                .read_exact(&mut header)
                .await
                .expect("startup arrives");
            assert_eq!(&header[4..], &196_608_u32.to_be_bytes());
            let length = u32::from_be_bytes(header[..4].try_into().expect("length has four bytes"));
            assert!((8..=1024).contains(&length));
            let mut startup = vec![0; usize::try_from(length).expect("length fits usize") - 8];
            socket
                .read_exact(&mut startup)
                .await
                .expect("startup parameters arrive");
            started.send(startup).expect("test waits for startup");
            // Keep the TCP connection alive but never answer the startup
            // packet. A cancelled connect must close the socket itself.
            let mut buffer = [0_u8; 1024];
            assert_eq!(
                socket
                    .read(&mut buffer)
                    .await
                    .expect("socket closes cleanly"),
                0,
                "cancelled startup must close without sending more protocol bytes"
            );
        });
        (config, startup_received, peer)
    }

    async fn wait_for_startup(startup_received: oneshot::Receiver<Vec<u8>>) -> Vec<u8> {
        let startup = tokio::time::timeout(Duration::from_secs(2), startup_received)
            .await
            .expect("loopback startup completes promptly")
            .expect("the peer received startup");
        // Pause only after real I/O has completed, so Tokio cannot advance
        // the deadline while the OS is still establishing the socket.
        tokio::time::pause();
        startup
    }

    async fn assert_peer_closed(peer: JoinHandle<()>) {
        // Real I/O must be allowed to make progress before a test timer fires.
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(2), peer)
            .await
            .expect("dropping the connect closes the peer socket")
            .expect("the peer task completes");
    }

    #[tokio::test]
    async fn connect_hardens_its_own_config_and_preserves_role_options() {
        const OPTIONS: &str =
            "-c statement_timeout=9000 -c idle_in_transaction_session_timeout=9000";
        let (mut config, startup_received, peer) = stalled_postgres_peer().await;
        config
            .application_name("spoofed")
            .keepalives(false)
            .options(OPTIONS);
        assert_eq!(config.get_connect_timeout(), None);
        let caller_config = std::sync::Arc::new(config);
        let config = caller_config.clone();
        let connect = tokio::spawn(async move {
            SupervisedClient::connect(&config, "mcp-ozon-wb-automation").await
        });
        let startup = wait_for_startup(startup_received).await;
        let application_name = b"application_name\0mcp-ozon-wb-automation\0";
        assert!(
            startup
                .windows(application_name.len())
                .any(|part| part == application_name)
        );
        let options = format!("options\0{OPTIONS}\0");
        assert!(
            startup
                .windows(options.len())
                .any(|part| part == options.as_bytes())
        );
        assert_eq!(caller_config.get_application_name(), Some("spoofed"));
        assert_eq!(caller_config.get_connect_timeout(), None);
        assert!(!caller_config.get_keepalives());
        assert_eq!(caller_config.get_options(), Some(OPTIONS));
        connect.abort();
        assert!(
            connect
                .await
                .err()
                .expect("connect was cancelled")
                .is_cancelled()
        );
        assert_peer_closed(peer).await;
    }

    #[tokio::test]
    async fn startup_is_bounded_after_the_tcp_connection_succeeds() {
        let (config, startup_received, peer) = stalled_postgres_peer().await;
        let connect = tokio::spawn(async move { SupervisedClient::connect(&config, "test").await });
        wait_for_startup(startup_received).await;
        tokio::time::advance(CONNECT_TIMEOUT).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(1), connect)
                .await
                .expect("startup has its own deadline")
                .expect("connect task completes")
                .err(),
            Some(PostgresUnavailable)
        );
        assert_peer_closed(peer).await;
    }

    #[tokio::test]
    async fn timed_out_reconnect_releases_the_mutex_and_preserves_a_full_cooldown() {
        let (config, startup_received, peer) = stalled_postgres_peer().await;
        let supervised = std::sync::Arc::new(SupervisedClient {
            component: "test",
            config: Some(config),
            metrics: SessionMetrics::default(),
            slot: Mutex::new(ConnectionSlot {
                client: None,
                next_attempt_at: Instant::now(),
            }),
        });
        let reconnect = {
            let supervised = supervised.clone();
            tokio::spawn(async move { supervised.acquire().await.err() })
        };
        wait_for_startup(startup_received).await;
        tokio::time::advance(CONNECT_TIMEOUT).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(1), reconnect)
                .await
                .expect("reconnect startup has a deadline")
                .expect("reconnect task completes"),
            Some(PostgresUnavailable)
        );
        let reserved = tokio::time::timeout(Duration::from_millis(1), supervised.slot.lock())
            .await
            .expect("the timed out reconnect releases the mutex")
            .next_attempt_at;
        assert_eq!(reserved, Instant::now() + RECONNECT_COOLDOWN);
        assert_eq!(supervised.acquire().await.err(), Some(PostgresUnavailable));
        assert_eq!(supervised.slot.lock().await.next_attempt_at, reserved);
        assert_eq!(supervised.session_metrics().held, 0);
        assert_peer_closed(peer).await;
    }

    #[tokio::test]
    async fn cancelling_reconnect_closes_the_socket_and_keeps_its_cooldown() {
        let (config, startup_received, peer) = stalled_postgres_peer().await;
        let supervised = std::sync::Arc::new(SupervisedClient {
            component: "test",
            config: Some(config),
            metrics: SessionMetrics::default(),
            slot: Mutex::new(ConnectionSlot {
                client: None,
                next_attempt_at: Instant::now(),
            }),
        });
        let reconnect = {
            let supervised = supervised.clone();
            tokio::spawn(async move { supervised.acquire().await.err() })
        };
        wait_for_startup(startup_received).await;
        reconnect.abort();
        assert!(
            reconnect
                .await
                .expect_err("reconnect was cancelled")
                .is_cancelled()
        );
        let reserved = tokio::time::timeout(Duration::from_millis(1), supervised.slot.lock())
            .await
            .expect("cancelling reconnect releases the mutex")
            .next_attempt_at;
        assert!(reserved > Instant::now());
        assert_eq!(supervised.acquire().await.err(), Some(PostgresUnavailable));
        assert_eq!(supervised.slot.lock().await.next_attempt_at, reserved);
        assert_eq!(supervised.session_metrics().held, 0);
        assert_peer_closed(peer).await;
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_an_actual_acquire_records_the_wait_without_leaking_a_gauge() {
        let supervised = SupervisedClient {
            component: "metrics-test",
            config: None,
            metrics: SessionMetrics::default(),
            slot: Mutex::new(ConnectionSlot {
                client: None,
                next_attempt_at: Instant::now(),
            }),
        };
        let occupied = supervised.slot.lock().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), supervised.acquire())
                .await
                .is_err()
        );
        let cancelled = supervised.session_metrics();
        assert_eq!(cancelled.waiting, 0);
        assert_eq!(cancelled.wait_count, 1);
        assert_eq!(cancelled.wait_micros, 25_000);
        assert_eq!(cancelled.cancelled_waits, 1);
        assert_eq!(cancelled.held, 0);
        assert_eq!(cancelled.hold_count, 0);
        drop(occupied);
        assert_eq!(supervised.acquire().await.err(), Some(PostgresUnavailable));
        let failed = supervised.session_metrics();
        assert_eq!(failed.wait_count, 2);
        assert_eq!(failed.cancelled_waits, 1);
        assert_eq!(failed.hold_count, 1);
        assert_eq!(failed.held, 0);
    }

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
            metrics: SessionMetrics::default(),
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
            metrics: SessionMetrics::default(),
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
