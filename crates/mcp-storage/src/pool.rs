//! A small fixed set of supervised sessions for one database role.
//!
//! One `SupervisedClient` serializes every operation of a component on a
//! single session, so one slow statement delays every other caller. A pool
//! hands out the first idle member, preferring one whose session is already
//! live, and opens further sessions only on demand: it never holds more
//! connections than its peak concurrency. Every member keeps the supervision,
//! reconnection cooldown and server-bound verification of `SupervisedClient`.

use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
};

use tokio::sync::Semaphore;
use tokio_postgres::Config;

use super::{
    ClientGuard, PostgresUnavailable, SessionHold, SessionMetrics, SessionMetricsSnapshot,
    SessionWait, SupervisedClient, harden, verify_client_session_bounds,
};

pub struct SupervisedPool {
    component: &'static str,
    members: Box<[SupervisedClient]>,
    available: Semaphore,
    next: AtomicUsize,
    metrics: SessionMetrics,
}

impl SupervisedPool {
    /// Connects the first member immediately, so an unusable database or a
    /// role without statement bounds still fails startup, and creates the
    /// remaining members lazily.
    pub async fn connect(
        config: &Config,
        component: &'static str,
        size: NonZeroUsize,
    ) -> Result<Self, PostgresUnavailable> {
        let first = SupervisedClient::connect(config, component).await?;
        let mut hardened = config.clone();
        harden(&mut hardened, component);
        let mut members = Vec::with_capacity(size.get());
        members.push(first);
        members
            .extend((1..size.get()).map(|_| SupervisedClient::lazy(hardened.clone(), component)));
        Ok(Self::from_members(component, members))
    }

    /// Adopts one caller-supplied client, as integration tests do. Like
    /// [`SupervisedClient::preconnected`] it is never reconnected.
    #[must_use]
    pub fn preconnected(client: tokio_postgres::Client, component: &'static str) -> Self {
        Self::from_members(
            component,
            vec![SupervisedClient::preconnected(client, component)],
        )
    }

    fn from_members(component: &'static str, members: Vec<SupervisedClient>) -> Self {
        let size = members.len();
        Self {
            component,
            members: members.into_boxed_slice(),
            available: Semaphore::new(size),
            next: AtomicUsize::new(0),
            metrics: SessionMetrics::default(),
        }
    }

    /// Borrows an idle session, waiting only while every member is in use.
    pub async fn acquire(&self) -> Result<ClientGuard<'_>, PostgresUnavailable> {
        let wait = SessionWait::new(&self.metrics);
        // The pool owns its semaphore and never closes it.
        let permit = self
            .available
            .acquire()
            .await
            .map_err(|_| PostgresUnavailable)?;
        wait.acquired();
        let hold = SessionHold::new(&self.metrics);
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let count = self.members.len();
        loop {
            // A permit guarantees an idle member exists, but another permit
            // holder may be probing the one this scan reaches first.
            let mut fallback = None;
            for offset in 0..count {
                let member = &self.members[(start + offset) % count];
                let Ok(slot) = member.slot.try_lock() else {
                    continue;
                };
                if slot
                    .client
                    .as_ref()
                    .is_some_and(|client| !client.is_closed())
                {
                    drop(fallback);
                    return member.ready(slot, hold, Some(permit)).await;
                }
                if fallback.is_none() {
                    fallback = Some((member, slot));
                }
            }
            if let Some((member, slot)) = fallback {
                return member.ready(slot, hold, Some(permit)).await;
            }
            tokio::task::yield_now().await;
        }
    }

    /// Confirms an idle member can complete a round trip.
    pub async fn probe(&self) -> Result<(), PostgresUnavailable> {
        let client = self.acquire().await?;
        client
            .query_one("SELECT 1", &[])
            .await
            .map(std::mem::drop)
            .map_err(|_| PostgresUnavailable)
    }

    /// Audits the server-applied statement and transaction bounds of the
    /// session it borrows. Every newly opened member is verified on connect.
    pub async fn verify_session_bounds(&self) -> Result<(), PostgresUnavailable> {
        let client = self.acquire().await?;
        verify_client_session_bounds(&client, self.component).await
    }

    /// Pool-wide contention counters; waiting means every member was busy.
    #[must_use]
    pub fn session_metrics(&self) -> SessionMetricsSnapshot {
        self.metrics.snapshot()
    }

    #[must_use]
    pub fn size(&self) -> usize {
        self.members.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use super::*;

    fn closed_port_config() -> Config {
        let mut config =
            Config::from_str("postgresql://report_worker:secret@127.0.0.1:1/reports").unwrap();
        harden(&mut config, "pool-test");
        config
    }

    #[tokio::test]
    async fn an_unreachable_database_fails_closed_and_is_paced_per_member() {
        let pool = SupervisedPool::from_members(
            "pool-test",
            (0..2)
                .map(|_| SupervisedClient::lazy(closed_port_config(), "pool-test"))
                .collect(),
        );
        assert_eq!(pool.size(), 2);
        assert_eq!(pool.acquire().await.err(), Some(PostgresUnavailable));
        assert_eq!(pool.acquire().await.err(), Some(PostgresUnavailable));
        // Both members have now reserved their reconnect cooldown, so a third
        // attempt is refused without another connection attempt.
        assert!(
            pool.members
                .iter()
                .all(|member| member.slot.try_lock().unwrap().next_attempt_at
                    > tokio::time::Instant::now())
        );
        assert_eq!(pool.probe().await, Err(PostgresUnavailable));
        assert_eq!(pool.verify_session_bounds().await, Err(PostgresUnavailable));
        let metrics = pool.session_metrics();
        assert_eq!(metrics.waiting, 0);
        assert_eq!(metrics.held, 0);
        assert_eq!(metrics.hold_count, 4);
        assert_eq!(
            SupervisedPool::connect(&closed_port_config(), "pool-test", NonZeroUsize::MIN)
                .await
                .err(),
            Some(PostgresUnavailable)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_for_a_busy_pool_is_bounded_by_the_caller_and_counted() {
        let pool = SupervisedPool::from_members(
            "pool-test",
            vec![SupervisedClient::lazy(closed_port_config(), "pool-test")],
        );
        let permit = pool.available.acquire().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(25), pool.acquire())
                .await
                .is_err()
        );
        let metrics = pool.session_metrics();
        assert_eq!(metrics.waiting, 0);
        assert_eq!(metrics.cancelled_waits, 1);
        assert_eq!(metrics.wait_micros, 25_000);
        drop(permit);
    }

    #[tokio::test]
    async fn a_member_locked_by_another_scan_is_skipped() {
        let pool = SupervisedPool::from_members(
            "pool-test",
            (0..2)
                .map(|_| SupervisedClient::lazy(closed_port_config(), "pool-test"))
                .collect(),
        );
        let held = pool.members[0].slot.lock().await;
        let untouched = held.next_attempt_at;
        // The scan skips the locked first member and reaches the second one,
        // which fails closed against the unreachable endpoint.
        assert_eq!(pool.acquire().await.err(), Some(PostgresUnavailable));
        drop(held);
        assert!(pool.members[1].slot.try_lock().unwrap().next_attempt_at > untouched);
        assert_eq!(
            pool.members[0].slot.try_lock().unwrap().next_attempt_at,
            untouched
        );
    }

    async fn backend_pid(client: &tokio_postgres::Client) -> i32 {
        client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0)
    }

    /// Requires `POSITION_REPOSITORY_TEST_READER_URL` from the disposable
    /// PostgreSQL fixture.
    #[tokio::test]
    #[ignore = "requires the disposable PostgreSQL fixture"]
    async fn concurrent_callers_receive_distinct_sessions_and_reuse_them() {
        let url = std::env::var("POSITION_REPOSITORY_TEST_READER_URL").unwrap();
        let config = Config::from_str(&url).unwrap();
        let pool = SupervisedPool::connect(&config, "pool-test", NonZeroUsize::new(2).unwrap())
            .await
            .unwrap();
        pool.verify_session_bounds().await.unwrap();
        let first = pool.acquire().await.unwrap();
        let second = pool.acquire().await.unwrap();
        let (first_pid, second_pid) = (backend_pid(&first).await, backend_pid(&second).await);
        assert_ne!(first_pid, second_pid);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), pool.acquire())
                .await
                .is_err(),
            "a full pool waits instead of opening a third session"
        );
        drop(first);
        let reused = pool.acquire().await.unwrap();
        assert_eq!(backend_pid(&reused).await, first_pid);
        drop((reused, second));
        pool.probe().await.unwrap();
        assert!(pool.session_metrics().wait_count >= 4);
    }
}
