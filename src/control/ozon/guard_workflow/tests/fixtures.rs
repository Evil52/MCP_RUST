use super::*;

pub(super) fn guard(campaign_id: u64, date_from: &str) -> OzonStaticCampaignGuard {
    OzonStaticCampaignGuard {
        guard: OzonCampaignGuard {
            plan_id: format!("static-{campaign_id}"),
            account_id: "account".to_owned(),
            sku: campaign_id + 100,
            campaign_id,
            date_from: date_from.to_owned(),
            spend_cap_microrubles: 2_000_000_000,
            target_drr_percent: 15,
            status: OzonCampaignGuardStatus::Active,
            stop_reason: None,
            incident_error_class: None,
        },
        min_cpc_bid_microrubles: 7_000_000,
        max_cpc_bid_microrubles: 10_000_000,
    }
}

pub(super) fn durable_guard(campaign_id: u64) -> OzonCampaignGuard {
    OzonCampaignGuard {
        plan_id: format!("{campaign_id:064x}"),
        account_id: "account".to_owned(),
        sku: campaign_id + 100,
        campaign_id,
        date_from: "2026-09-01".to_owned(),
        spend_cap_microrubles: 1_000_000_000,
        target_drr_percent: 15,
        status: OzonCampaignGuardStatus::Active,
        stop_reason: None,
        incident_error_class: None,
    }
}

pub(super) fn lease(
    guard: OzonCampaignGuard,
    reason: &str,
    evidence: OzonGuardEvidence,
) -> OzonGuardStopLease {
    OzonGuardStopLease {
        guard,
        stop_reason: reason.to_owned(),
        spend_minor: evidence.map(|metrics| metrics.spend_minor),
        revenue_minor: evidence.map(|metrics| metrics.attributed_revenue_minor),
        generation: 1,
        owner_id: "worker".to_owned(),
        lease_token: "a".repeat(64),
        lease_expires_at: DateTime::UNIX_EPOCH + chrono::Duration::hours(1),
        write_started_at: None,
    }
}

pub(super) fn marked_lease(
    guard: OzonCampaignGuard,
    reason: &str,
    evidence: OzonGuardEvidence,
) -> OzonGuardStopLease {
    OzonGuardStopLease {
        write_started_at: Some(DateTime::UNIX_EPOCH + chrono::Duration::minutes(1)),
        ..lease(guard, reason, evidence)
    }
}

#[derive(Default)]
pub(super) struct FakeRepositoryState {
    pub(super) active: Vec<OzonCampaignGuard>,
    pub(super) recoveries: VecDeque<OzonGuardStopLease>,
    pub(super) claims: Vec<(u64, String, OzonGuardEvidence)>,
    pub(super) observations: Vec<(u64, OzonGuardMetrics)>,
    pub(super) readbacks: Vec<(u64, OzonGuardStopReadback)>,
    pub(super) finishes: Vec<(u64, OzonGuardEvidence)>,
    pub(super) incidents: Vec<(u64, String, OzonGuardEvidence)>,
}

#[derive(Default)]
pub(super) struct FakeRepository(pub(super) Mutex<FakeRepositoryState>);

impl OzonGuardRepositoryPort for FakeRepository {
    async fn active_guards(
        &self,
        account_id: &str,
    ) -> Result<Vec<OzonCampaignGuard>, OzonPlanStoreError> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .active
            .iter()
            .filter(|guard| guard.account_id == account_id)
            .cloned()
            .collect())
    }

    async fn claim_stop_recovery(
        &self,
        account_id: &str,
        _worker_id: &str,
    ) -> Result<Option<OzonGuardStopLease>, OzonPlanStoreError> {
        let mut state = self.0.lock().unwrap();
        let Some(index) = state
            .recoveries
            .iter()
            .position(|lease| lease.guard.account_id == account_id)
        else {
            return Ok(None);
        };
        Ok(state.recoveries.remove(index))
    }

    async fn claim_stop(
        &self,
        guard: &OzonCampaignGuard,
        reason: &str,
        evidence: OzonGuardEvidence,
        _worker_id: &str,
    ) -> Result<OzonGuardStopLease, OzonPlanStoreError> {
        let mut state = self.0.lock().unwrap();
        state
            .active
            .retain(|candidate| candidate.plan_id != guard.plan_id);
        state
            .claims
            .push((guard.campaign_id, reason.to_owned(), evidence));
        let lease = lease(guard.clone(), reason, evidence);
        state.recoveries.push_back(lease.clone());
        drop(state);
        Ok(lease)
    }

    async fn record_observation(
        &self,
        guard: &OzonCampaignGuard,
        metrics: OzonGuardMetrics,
    ) -> Result<(), OzonPlanStoreError> {
        self.0
            .lock()
            .unwrap()
            .observations
            .push((guard.campaign_id, metrics));
        Ok(())
    }

    async fn finish_stop(
        &self,
        lease: &OzonGuardStopLease,
        evidence: OzonGuardEvidence,
    ) -> Result<(), OzonPlanStoreError> {
        let mut state = self.0.lock().unwrap();
        state
            .recoveries
            .retain(|candidate| candidate.guard.plan_id != lease.guard.plan_id);
        state.finishes.push((lease.guard.campaign_id, evidence));
        drop(state);
        Ok(())
    }

    async fn mark_incident(
        &self,
        lease: &OzonGuardStopLease,
        error_class: &str,
        evidence: OzonGuardEvidence,
    ) -> Result<(), OzonPlanStoreError> {
        let mut state = self.0.lock().unwrap();
        state
            .recoveries
            .retain(|candidate| candidate.guard.plan_id != lease.guard.plan_id);
        state
            .incidents
            .push((lease.guard.campaign_id, error_class.to_owned(), evidence));
        drop(state);
        Ok(())
    }

    async fn record_readback(
        &self,
        lease: &OzonGuardStopLease,
        observation: OzonGuardStopReadback,
    ) -> Result<(), OzonPlanStoreError> {
        self.0
            .lock()
            .unwrap()
            .readbacks
            .push((lease.guard.campaign_id, observation));
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct FakeReader {
    pub(super) metrics: Mutex<VecDeque<Result<OzonGuardMetrics, OzonGuardReadFailure>>>,
    pub(super) running: Mutex<VecDeque<Result<bool, OzonGuardReadFailure>>>,
    pub(super) observed_at: Mutex<Vec<DateTime<Utc>>>,
}

impl OzonGuardReaderPort for FakeReader {
    async fn metrics(
        &self,
        _guard: &OzonCampaignGuard,
        observed_at: DateTime<Utc>,
    ) -> Result<OzonGuardMetrics, OzonGuardReadFailure> {
        self.observed_at.lock().unwrap().push(observed_at);
        self.metrics
            .lock()
            .unwrap()
            .pop_front()
            .expect("a metric result was configured")
    }

    async fn campaign_is_running(&self, _campaign_id: u64) -> Result<bool, OzonGuardReadFailure> {
        self.running
            .lock()
            .unwrap()
            .pop_front()
            .expect("a readback result was configured")
    }
}

#[derive(Default)]
pub(super) struct FakeWriter {
    pub(super) results: Mutex<VecDeque<Result<(), OzonGuardWriteFailure>>>,
    pub(super) calls: Mutex<Vec<u64>>,
}

impl OzonGuardWriterPort for FakeWriter {
    async fn deactivate_with_final_permit(
        &self,
        lease: &OzonGuardStopLease,
    ) -> Result<(), OzonGuardWriteFailure> {
        self.calls.lock().unwrap().push(lease.guard.campaign_id);
        self.results.lock().unwrap().pop_front().unwrap_or(Ok(()))
    }
}

pub(super) struct FakeClock {
    pub(super) now: DateTime<Utc>,
    pub(super) sleeps: Mutex<Vec<Duration>>,
}

impl OzonGuardClock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        self.now
    }

    async fn sleep(&self, duration: Duration) {
        self.sleeps.lock().unwrap().push(duration);
    }
}

#[derive(Default)]
pub(super) struct FakeFailpoints(pub(super) BTreeSet<OzonGuardFailpoint>);

impl OzonGuardFailpoints for FakeFailpoints {
    fn is_enabled(&self, point: OzonGuardFailpoint) -> bool {
        self.0.contains(&point)
    }
}

pub(super) fn clock() -> FakeClock {
    FakeClock {
        now: DateTime::UNIX_EPOCH + chrono::Duration::days(20_000),
        sleeps: Mutex::new(Vec::new()),
    }
}

pub(super) async fn cycle(
    repository: &FakeRepository,
    reader: &FakeReader,
    writer: &FakeWriter,
    clock: &FakeClock,
    failpoints: &FakeFailpoints,
) -> Result<(), OzonGuardWorkflowError> {
    run_durable_ozon_guard_cycle(
        repository,
        reader,
        writer,
        clock,
        failpoints,
        OzonGuardRunContext {
            account_id: "account",
            worker_id: "worker",
            write_boundary: Duration::from_secs(2),
        },
    )
    .await
}

pub(super) fn metrics_date() -> chrono::NaiveDate {
    chrono::NaiveDate::from_ymd_opt(2026, 9, 2).unwrap()
}

pub(super) fn aggregate_test_metrics(
    expected: &BTreeSet<u64>,
    rows: impl IntoIterator<Item = OzonGuardMetricRow>,
) -> Result<BTreeMap<u64, OzonGuardMetrics>, OzonGuardTelemetryError> {
    aggregate_complete_guard_metrics(
        expected,
        metrics_date() - chrono::Duration::days(1),
        metrics_date(),
        rows,
    )
}
