#![allow(
    clippy::unused_async_trait_impl,
    reason = "in-memory async port fakes intentionally complete without suspension"
)]

use std::{
    collections::VecDeque,
    os::unix::fs::{PermissionsExt as _, symlink},
    sync::{
        Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use super::*;

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "mcp-ozon-guard-runtime-{}-{}",
            std::process::id(),
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[derive(Default)]
struct BlockingGuardTasks {
    launch_calls: AtomicUsize,
    guard_calls: AtomicUsize,
}

impl OzonWorkflowTasks for BlockingGuardTasks {
    async fn drain_launch_once(&self) -> bool {
        self.launch_calls.fetch_add(1, Ordering::Relaxed);
        true
    }

    async fn run_guard_once(&self) -> bool {
        self.guard_calls.fetch_add(1, Ordering::Relaxed);
        std::future::pending::<bool>().await
    }
}

#[derive(Default)]
struct FailingWorkflowTasks {
    launch_calls: AtomicUsize,
}

impl OzonWorkflowTasks for FailingWorkflowTasks {
    async fn drain_launch_once(&self) -> bool {
        self.launch_calls.fetch_add(1, Ordering::Relaxed);
        false
    }

    async fn run_guard_once(&self) -> bool {
        true
    }
}

#[derive(Default)]
struct FakeStaticStopIo {
    writes: Mutex<VecDeque<Result<(), String>>>,
    activations: Mutex<VecDeque<Result<(), String>>>,
    pre_marker_failures: Mutex<VecDeque<String>>,
    readbacks: Mutex<VecDeque<Result<bool, String>>>,
    write_calls: AtomicUsize,
    activation_calls: AtomicUsize,
    read_calls: AtomicUsize,
    audit_event_sequence: AtomicU64,
}

impl OzonStaticCampaignIo for FakeStaticStopIo {
    async fn deactivate_with_final_marker<P, MarkerFuture>(
        &self,
        _static_guard: &OzonStaticCampaignGuard,
        _expected_prior_event_id: Option<u64>,
        marker: P,
    ) -> Result<(), String>
    where
        P: FnOnce(u64) -> MarkerFuture,
        MarkerFuture: Future<Output = Result<(), String>>,
    {
        let pre_marker_failure = self.pre_marker_failures.lock().unwrap().pop_front();
        if let Some(error) = pre_marker_failure {
            return Err(error);
        }
        let event_id = self.audit_event_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        marker(event_id).await?;
        self.write_calls.fetch_add(1, Ordering::Relaxed);
        self.writes
            .lock()
            .unwrap()
            .pop_front()
            .expect("a static stop write was configured")
    }

    async fn activate_with_final_marker<P, MarkerFuture>(
        &self,
        _static_guard: &OzonStaticCampaignGuard,
        _expected_prior_event_id: Option<u64>,
        marker: P,
    ) -> Result<(), String>
    where
        P: FnOnce(u64) -> MarkerFuture,
        MarkerFuture: Future<Output = Result<(), String>>,
    {
        let pre_marker_failure = self.pre_marker_failures.lock().unwrap().pop_front();
        if let Some(error) = pre_marker_failure {
            return Err(error);
        }
        let event_id = self.audit_event_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        marker(event_id).await?;
        self.activation_calls.fetch_add(1, Ordering::Relaxed);
        self.activations
            .lock()
            .unwrap()
            .pop_front()
            .expect("a static activation write was configured")
    }

    async fn campaign_is_running(&self, _campaign_id: u64) -> Result<bool, String> {
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        self.readbacks
            .lock()
            .unwrap()
            .pop_front()
            .expect("a static stop readback was configured")
    }
}

#[derive(Default)]
struct RecordingClock(Mutex<Vec<Duration>>);

impl OzonGuardClock for RecordingClock {
    fn now(&self) -> DateTime<Utc> {
        DateTime::UNIX_EPOCH
    }

    async fn sleep(&self, duration: Duration) {
        self.0.lock().unwrap().push(duration);
    }
}

#[derive(Default)]
struct StaticFailpoints(BTreeSet<OzonStaticMutationFailpoint>);

impl OzonStaticMutationFailpoints for StaticFailpoints {
    fn is_enabled(&self, point: OzonStaticMutationFailpoint) -> bool {
        self.0.contains(&point)
    }
}

fn test_static_guard(campaign_id: u64, max_bid: u64) -> OzonStaticCampaignGuard {
    OzonStaticCampaignGuard {
        guard: OzonCampaignGuard {
            plan_id: format!("static-{campaign_id}"),
            account_id: "account".to_owned(),
            sku: campaign_id + 100,
            campaign_id,
            date_from: "2026-09-01".to_owned(),
            spend_cap_microrubles: 2_000_000_000,
            target_drr_percent: 15,
            status: super::super::model::OzonCampaignGuardStatus::Active,
            stop_reason: None,
            incident_error_class: None,
        },
        min_cpc_bid_microrubles: 7_000_000,
        max_cpc_bid_microrubles: max_bid,
    }
}

fn test_pending_bid(
    guard: &OzonStaticCampaignGuard,
    started_at: DateTime<Utc>,
) -> PendingStaticBidChange {
    PendingStaticBidChange {
        account_id: Some(guard.guard.account_id.clone()),
        sku: Some(guard.guard.sku),
        min_cpc_bid_microrubles: Some(guard.min_cpc_bid_microrubles),
        max_cpc_bid_microrubles: Some(guard.max_cpc_bid_microrubles),
        date_from: Some(guard.guard.date_from.clone()),
        spend_cap_microrubles: Some(guard.guard.spend_cap_microrubles),
        target_drr_percent: Some(guard.guard.target_drr_percent),
        from_microrubles: 7_000_000,
        to_microrubles: 8_000_000,
        started_at,
    }
}

fn test_incident(guard: &OzonStaticCampaignGuard) -> OzonStaticGuardIncident {
    OzonStaticGuardIncident {
        account_id: Some(guard.guard.account_id.clone()),
        sku: Some(guard.guard.sku),
        min_cpc_bid_microrubles: Some(guard.min_cpc_bid_microrubles),
        max_cpc_bid_microrubles: Some(guard.max_cpc_bid_microrubles),
        date_from: Some(guard.guard.date_from.clone()),
        spend_cap_microrubles: Some(guard.guard.spend_cap_microrubles),
        target_drr_percent: Some(guard.guard.target_drr_percent),
        stop_reason: Some("telemetry_unavailable".to_owned()),
        error_class: "readback_unavailable".to_owned(),
        spend_minor: None,
        revenue_minor: None,
        occurred_at: DateTime::UNIX_EPOCH,
    }
}

mod http_reads;
mod regression_files;
mod regression_mutations;

mod static_adapter_fixture;
mod static_authorization;
mod static_writes;

mod bootstrap;
mod durable_adapter;

mod static_safety;
