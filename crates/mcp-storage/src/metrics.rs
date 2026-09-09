//! Bounded, payload-free session contention measurements.

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::Instant;

// Fixed process totals: no registry, identity labels or retained client handles.
static PROCESS: SessionMetrics = SessionMetrics::new();

#[derive(Debug, Default)]
pub struct SessionMetrics {
    waiting: AtomicU64,
    held: AtomicU64,
    wait_count: AtomicU64,
    wait_micros: AtomicU64,
    wait_max_micros: AtomicU64,
    cancelled_waits: AtomicU64,
    hold_count: AtomicU64,
    hold_micros: AtomicU64,
    hold_max_micros: AtomicU64,
}

/// Approximate concurrent snapshot of session contention.
///
/// Durations include completed observations;
/// cancelled mutex waits are included in wait totals and counted separately.
/// Holding time includes session verification/reconnection under the mutex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionMetricsSnapshot {
    pub waiting: u64,
    pub held: u64,
    pub wait_count: u64,
    pub wait_micros: u64,
    pub wait_max_micros: u64,
    pub cancelled_waits: u64,
    pub hold_count: u64,
    pub hold_micros: u64,
    pub hold_max_micros: u64,
}

impl SessionMetrics {
    const fn new() -> Self {
        Self {
            waiting: AtomicU64::new(0),
            held: AtomicU64::new(0),
            wait_count: AtomicU64::new(0),
            wait_micros: AtomicU64::new(0),
            wait_max_micros: AtomicU64::new(0),
            cancelled_waits: AtomicU64::new(0),
            hold_count: AtomicU64::new(0),
            hold_micros: AtomicU64::new(0),
            hold_max_micros: AtomicU64::new(0),
        }
    }

    pub(super) fn snapshot(&self) -> SessionMetricsSnapshot {
        SessionMetricsSnapshot {
            waiting: self.waiting.load(Ordering::Relaxed),
            held: self.held.load(Ordering::Relaxed),
            wait_count: self.wait_count.load(Ordering::Relaxed),
            wait_micros: self.wait_micros.load(Ordering::Relaxed),
            wait_max_micros: self.wait_max_micros.load(Ordering::Relaxed),
            cancelled_waits: self.cancelled_waits.load(Ordering::Relaxed),
            hold_count: self.hold_count.load(Ordering::Relaxed),
            hold_micros: self.hold_micros.load(Ordering::Relaxed),
            hold_max_micros: self.hold_max_micros.load(Ordering::Relaxed),
        }
    }

    fn finish_wait(&self, elapsed: u64, cancelled: bool) {
        self.waiting.fetch_sub(1, Ordering::Relaxed);
        add(&self.wait_count, 1);
        add(&self.wait_micros, elapsed);
        self.wait_max_micros.fetch_max(elapsed, Ordering::Relaxed);
        if cancelled {
            add(&self.cancelled_waits, 1);
        }
    }

    fn finish_hold(&self, elapsed: u64) {
        self.held.fetch_sub(1, Ordering::Relaxed);
        add(&self.hold_count, 1);
        add(&self.hold_micros, elapsed);
        self.hold_max_micros.fetch_max(elapsed, Ordering::Relaxed);
    }
}

fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

pub struct SessionWait<'a> {
    metrics: &'a SessionMetrics,
    started: Instant,
    cancelled: bool,
}

impl<'a> SessionWait<'a> {
    pub(super) fn new(metrics: &'a SessionMetrics) -> Self {
        metrics.waiting.fetch_add(1, Ordering::Relaxed);
        PROCESS.waiting.fetch_add(1, Ordering::Relaxed);
        Self {
            metrics,
            started: Instant::now(),
            cancelled: true,
        }
    }

    pub(super) fn acquired(mut self) {
        self.cancelled = false;
    }
}

impl Drop for SessionWait<'_> {
    fn drop(&mut self) {
        let elapsed = elapsed_micros(self.started);
        self.metrics.finish_wait(elapsed, self.cancelled);
        PROCESS.finish_wait(elapsed, self.cancelled);
    }
}

pub struct SessionHold<'a> {
    metrics: &'a SessionMetrics,
    started: Instant,
}

impl<'a> SessionHold<'a> {
    pub(super) fn new(metrics: &'a SessionMetrics) -> Self {
        metrics.held.fetch_add(1, Ordering::Relaxed);
        PROCESS.held.fetch_add(1, Ordering::Relaxed);
        Self {
            metrics,
            started: Instant::now(),
        }
    }
}

impl Drop for SessionHold<'_> {
    fn drop(&mut self) {
        let elapsed = elapsed_micros(self.started);
        self.metrics.finish_hold(elapsed);
        PROCESS.finish_hold(elapsed);
    }
}

// Preserve integer precision without casting an ever-growing counter to f64.
fn seconds(micros: u64) -> String {
    format!("{}.{:06}", micros / 1_000_000, micros % 1_000_000)
}

/// Process-local cumulative session metrics for the existing HTTP metrics
/// surface. Reading these never acquires a database session or contacts it.
#[must_use]
pub fn prometheus_metrics() -> String {
    render(PROCESS.snapshot())
}

fn render(snapshot: SessionMetricsSnapshot) -> String {
    format!(
        concat!(
            "# TYPE mcp_postgres_session_waiters gauge\n",
            "mcp_postgres_session_waiters {}\n",
            "# TYPE mcp_postgres_sessions_held gauge\n",
            "mcp_postgres_sessions_held {}\n",
            "# TYPE mcp_postgres_session_wait_seconds summary\n",
            "mcp_postgres_session_wait_seconds_count {}\n",
            "mcp_postgres_session_wait_seconds_sum {}\n",
            "# TYPE mcp_postgres_session_wait_max_seconds gauge\n",
            "mcp_postgres_session_wait_max_seconds {}\n",
            "# TYPE mcp_postgres_session_cancelled_waits_total counter\n",
            "mcp_postgres_session_cancelled_waits_total {}\n",
            "# TYPE mcp_postgres_session_hold_seconds summary\n",
            "mcp_postgres_session_hold_seconds_count {}\n",
            "mcp_postgres_session_hold_seconds_sum {}\n",
            "# TYPE mcp_postgres_session_hold_max_seconds gauge\n",
            "mcp_postgres_session_hold_max_seconds {}\n",
        ),
        snapshot.waiting,
        snapshot.held,
        snapshot.wait_count,
        seconds(snapshot.wait_micros),
        seconds(snapshot.wait_max_micros),
        snapshot.cancelled_waits,
        snapshot.hold_count,
        seconds(snapshot.hold_micros),
        seconds(snapshot.hold_max_micros),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn cancelled_waits_and_released_sessions_do_not_leave_stuck_gauges() {
        let metrics = SessionMetrics::default();
        let first = SessionWait::new(&metrics);
        let cancelled = SessionWait::new(&metrics);
        assert_eq!(metrics.snapshot().waiting, 2);
        tokio::time::advance(Duration::from_millis(25)).await;
        first.acquired();
        let held = SessionHold::new(&metrics);
        tokio::time::advance(Duration::from_millis(50)).await;
        drop(cancelled);
        assert_eq!(metrics.snapshot().waiting, 0);
        assert_eq!(metrics.snapshot().held, 1);
        drop(held);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.wait_count, 2);
        assert_eq!(snapshot.wait_micros, 100_000);
        assert_eq!(snapshot.wait_max_micros, 75_000);
        assert_eq!(snapshot.cancelled_waits, 1);
        assert_eq!(snapshot.hold_count, 1);
        assert_eq!(snapshot.hold_micros, 50_000);
        assert_eq!(snapshot.hold_max_micros, 50_000);
        assert_eq!(snapshot.held, 0);
        let text = render(snapshot);
        assert!(text.contains("mcp_postgres_session_wait_seconds_sum 0.100000\n"));
        assert!(text.contains("mcp_postgres_session_hold_seconds_sum 0.050000\n"));
        assert!(text.contains("mcp_postgres_session_cancelled_waits_total 1\n"));
        assert!(!text.contains('{'));
    }

    #[test]
    fn cumulative_counters_saturate_and_decimal_seconds_keep_precision() {
        let counter = AtomicU64::new(u64::MAX - 1);
        add(&counter, 10);
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(seconds(1), "0.000001");
        assert_eq!(seconds(u64::MAX), "18446744073709.551615");
        assert!(prometheus_metrics().contains("mcp_postgres_session_wait_seconds_count"));
    }
}
