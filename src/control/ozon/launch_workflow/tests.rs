use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use chrono::{Duration as ChronoDuration, TimeZone, Utc};

use super::*;
use crate::control::ozon::{OzonCampaignLaunchSpec, prepare_campaign_launch_manifest};

use super::super::model::OzonPlanApproval;

#[derive(Default)]
struct RepositoryState {
    recoveries: VecDeque<OzonLaunchLease>,
    executions: VecDeque<OzonLaunchLease>,
    events: Vec<String>,
    fail_claim: Option<OzonPlanStoreError>,
    fail_complete: Option<OzonPlanStoreError>,
    ambiguous_readbacks: Vec<Option<Value>>,
}

#[derive(Default)]
struct TestRepository {
    state: Mutex<RepositoryState>,
}

impl TestRepository {
    fn with_leases(
        recoveries: impl IntoIterator<Item = OzonLaunchLease>,
        executions: impl IntoIterator<Item = OzonLaunchLease>,
    ) -> Self {
        Self {
            state: Mutex::new(RepositoryState {
                recoveries: recoveries.into_iter().collect(),
                executions: executions.into_iter().collect(),
                events: Vec::new(),
                fail_claim: None,
                fail_complete: None,
                ambiguous_readbacks: Vec::new(),
            }),
        }
    }

    fn events(&self) -> Vec<String> {
        self.state.lock().unwrap().events.clone()
    }

    fn push_recovery(&self, lease: OzonLaunchLease) {
        self.state.lock().unwrap().recoveries.push_back(lease);
    }
}

impl OzonLaunchRepositoryPort for TestRepository {
    async fn claim_recovery(
        &self,
        account_id: &str,
        worker_id: &str,
    ) -> Result<Option<OzonLaunchLease>, OzonPlanStoreError> {
        assert_eq!(account_id, "account");
        assert_eq!(worker_id, "worker");
        let mut state = self.state.lock().unwrap();
        state.events.push("claim_recovery".to_owned());
        if let Some(error) = state.fail_claim.take() {
            return Err(error);
        }
        Ok(state.recoveries.pop_front())
    }

    async fn claim_execution(
        &self,
        account_id: &str,
        worker_id: &str,
    ) -> Result<Option<OzonLaunchLease>, OzonPlanStoreError> {
        assert_eq!(account_id, "account");
        assert_eq!(worker_id, "worker");
        let mut state = self.state.lock().unwrap();
        state.events.push("claim_execution".to_owned());
        Ok(state.executions.pop_front())
    }

    async fn complete(
        &self,
        lease: &OzonLaunchLease,
        campaign_id: Option<u64>,
        readback: Option<&Value>,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        let mut state = self.state.lock().unwrap();
        state.events.push(format!(
            "complete:{}:{}:{}",
            lease.action.as_db(),
            campaign_id.unwrap_or_default(),
            readback.is_some()
        ));
        if let Some(error) = state.fail_complete.take() {
            return Err(error);
        }
        drop(state);
        Ok(plan_at(lease, lease.action.completed_status(), campaign_id))
    }

    async fn confirm_applied(
        &self,
        lease: &OzonLaunchLease,
        campaign_id: u64,
        _readback: &Value,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        self.state
            .lock()
            .unwrap()
            .events
            .push(format!("confirm:{}:{campaign_id}", lease.action.as_db()));
        Ok(plan_at(lease, OzonLaunchStatus::Applied, Some(campaign_id)))
    }

    async fn mark_ambiguous(
        &self,
        lease: &OzonLaunchLease,
        error_class: &str,
        campaign_id: Option<u64>,
        readback: Option<&Value>,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        let mut state = self.state.lock().unwrap();
        state.events.push(format!(
            "ambiguous:{}:{error_class}:{}:{}",
            lease.action.as_db(),
            campaign_id.unwrap_or_default(),
            readback.is_some()
        ));
        state.ambiguous_readbacks.push(readback.cloned());
        drop(state);
        Ok(plan_at(lease, OzonLaunchStatus::Ambiguous, campaign_id))
    }

    async fn fail(
        &self,
        lease: &OzonLaunchLease,
        error_class: &str,
        campaign_id: Option<u64>,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        self.state.lock().unwrap().events.push(format!(
            "failed:{}:{error_class}:{}",
            lease.action.as_db(),
            campaign_id.unwrap_or_default()
        ));
        Ok(plan_at(lease, OzonLaunchStatus::Failed, campaign_id))
    }

    async fn release(
        &self,
        lease: &OzonLaunchLease,
        error_class: &str,
    ) -> Result<(), OzonPlanStoreError> {
        self.state
            .lock()
            .unwrap()
            .events
            .push(format!("release:{}:{error_class}", lease.action.as_db()));
        Ok(())
    }
}

#[derive(Default)]
struct TestIo {
    writes: Mutex<VecDeque<Result<OzonLaunchWriteReceipt, OzonLaunchWriteFailure>>>,
    readbacks: Mutex<VecDeque<Result<OzonLaunchObservation, String>>>,
    execute_count: AtomicUsize,
}

impl TestIo {
    fn new(
        writes: impl IntoIterator<Item = Result<OzonLaunchWriteReceipt, OzonLaunchWriteFailure>>,
        readbacks: impl IntoIterator<Item = Result<OzonLaunchObservation, String>>,
    ) -> Self {
        Self {
            writes: Mutex::new(writes.into_iter().collect()),
            readbacks: Mutex::new(readbacks.into_iter().collect()),
            execute_count: AtomicUsize::new(0),
        }
    }
}

impl OzonLaunchIoPort for TestIo {
    async fn execute<F>(
        &self,
        lease: &OzonLaunchLease,
        failpoints: &F,
    ) -> Result<OzonLaunchWriteReceipt, OzonLaunchWriteFailure>
    where
        F: OzonLaunchFailpoints + Sync,
    {
        self.execute_count.fetch_add(1, Ordering::AcqRel);
        if failpoints
            .hit(OzonLaunchFailpoint::AfterWriteStarted)
            .is_err()
        {
            return Err(OzonLaunchWriteFailure::Ambiguous(
                ambiguous_write_error_class(lease.action),
            ));
        }
        let result = self.writes.lock().unwrap().pop_front().unwrap_or_else(|| {
            Err(OzonLaunchWriteFailure::NotStarted(
                "missing test write".to_owned(),
            ))
        });
        if result.is_ok() && failpoints.hit(OzonLaunchFailpoint::AfterWrite).is_err() {
            return Err(OzonLaunchWriteFailure::Ambiguous(
                ambiguous_write_error_class(lease.action),
            ));
        }
        result
    }

    async fn readback(&self, _lease: &OzonLaunchLease) -> Result<OzonLaunchObservation, String> {
        self.readbacks
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err("missing test readback".to_owned()))
    }
}

#[derive(Default)]
struct OneShotFailpoint {
    selected: Option<OzonLaunchFailpoint>,
    fired: AtomicBool,
}

impl OneShotFailpoint {
    const fn new(selected: OzonLaunchFailpoint) -> Self {
        Self {
            selected: Some(selected),
            fired: AtomicBool::new(false),
        }
    }
}

impl OzonLaunchFailpoints for OneShotFailpoint {
    fn hit(&self, point: OzonLaunchFailpoint) -> Result<(), OzonLaunchWorkflowError> {
        if self.selected == Some(point) && !self.fired.swap(true, Ordering::AcqRel) {
            Err(OzonLaunchWorkflowError::Failpoint(point))
        } else {
            Ok(())
        }
    }
}

fn manifest() -> super::super::OzonCampaignLaunchManifest {
    let spec = OzonCampaignLaunchSpec {
        account_id: "account".to_owned(),
        title: "Durable workflow test".to_owned(),
        from_date: "2026-09-04".to_owned(),
        to_date: "2026-09-10".to_owned(),
        skus: vec![1001],
        weekly_budget_microrubles: 2_000_000_000,
        per_sku_spend_cap_microrubles: 2_000_000_000,
        initial_cpc_bid_microrubles: 7_000_000,
        max_cpc_bid_microrubles: 12_000_000,
        target_drr_percent: 15,
        target_position: 10,
    };
    prepare_campaign_launch_manifest(
        "actor",
        1,
        7,
        &"a".repeat(64),
        "account",
        &[1001],
        2_000_000_000,
        2_000_000_000,
        7_000_000,
        12_000_000,
        15,
        10,
        spec,
    )
    .unwrap()
}

fn lease(
    action: OzonLaunchAction,
    mode: OzonLaunchClaimMode,
    status: OzonLaunchStatus,
) -> OzonLaunchLease {
    let now = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();
    OzonLaunchLease {
        plan: OzonCampaignPlan {
            plan_id: "b".repeat(64),
            plan_digest: "c".repeat(64),
            actor_id: "actor".to_owned(),
            account_id: "account".to_owned(),
            sku: 1001,
            schema_version: 1,
            policy_revision: 7,
            policy_digest: "a".repeat(64),
            manifest: manifest(),
            status,
            approval: Some(OzonPlanApproval {
                approval_id: "d".repeat(64),
                approver_id: "approver".to_owned(),
                reference: "test".to_owned(),
                approved_at: now,
                expires_at: now + ChronoDuration::minutes(3),
            }),
            campaign_id: (action != OzonLaunchAction::CreateCampaign).then_some(42),
            created_at: now,
            expires_at: now + ChronoDuration::minutes(15),
            operation_started_at: (status != OzonLaunchStatus::Approved).then_some(now),
            finished_at: None,
            last_error_class: None,
            readback: None,
            execution_requested_at: Some(now),
            current_action: action,
            workflow_generation: 1,
            workflow_lease_expires_at: Some(now + ChronoDuration::minutes(5)),
            workflow_write_started_at: (mode == OzonLaunchClaimMode::Reconcile).then_some(now),
        },
        action,
        mode,
        generation: 1,
        owner_id: "worker".to_owned(),
        lease_token: "e".repeat(64),
    }
}

fn plan_at(
    lease: &OzonLaunchLease,
    status: OzonLaunchStatus,
    campaign_id: Option<u64>,
) -> OzonCampaignPlan {
    let mut plan = lease.plan.clone();
    plan.status = status;
    plan.campaign_id = campaign_id.or(plan.campaign_id);
    plan.current_action = lease.action.next().unwrap_or(lease.action);
    plan
}

fn create_stage(campaign_id: u64) -> OzonLaunchObservation {
    OzonLaunchObservation::Stage {
        campaign_id,
        readback: serde_json::json!({
            "campaign_id": campaign_id,
            "title": "Durable workflow test",
            "action": "create_campaign",
            "verified": true,
        }),
    }
}

fn product_stage(campaign_id: u64) -> OzonLaunchObservation {
    OzonLaunchObservation::Stage {
        campaign_id,
        readback: serde_json::json!({
            "campaign_id": campaign_id,
            "sku": 1001,
            "title": "Durable workflow test",
            "bid_microrubles": 7_000_000,
            "state": "CAMPAIGN_STATE_INACTIVE",
            "action": "add_products",
            "verified": true,
        }),
    }
}

fn applied(campaign_id: u64) -> OzonLaunchObservation {
    OzonLaunchObservation::Applied {
        campaign_id,
        readback: serde_json::json!({
            "campaign_id": campaign_id,
            "sku": 1001,
            "title": "Durable workflow test",
            "bid_microrubles": 7_000_000,
            "state": "CAMPAIGN_STATE_RUNNING",
        }),
    }
}

#[tokio::test]
async fn recovery_is_prioritized_and_never_executes_a_mutation() {
    let recovery = lease(
        OzonLaunchAction::AddProducts,
        OzonLaunchClaimMode::Reconcile,
        OzonLaunchStatus::AddingProducts,
    );
    let execution = lease(
        OzonLaunchAction::CreateCampaign,
        OzonLaunchClaimMode::Execute,
        OzonLaunchStatus::Approved,
    );
    let repository = TestRepository::with_leases([recovery], [execution]);
    let io = TestIo::new([], [Ok(product_stage(42))]);

    let outcome = drain_ozon_launch_workflow_once(
        &repository,
        &io,
        &NoOzonLaunchFailpoints,
        "account",
        "worker",
    )
    .await
    .unwrap();

    assert!(matches!(
        outcome,
        OzonLaunchDrainOutcome::Reconciled {
            status: OzonLaunchStatus::ProductsAdded,
            ..
        }
    ));
    assert_eq!(io.execute_count.load(Ordering::Acquire), 0);
    assert_eq!(
        repository.events(),
        ["claim_recovery", "complete:add_products:42:true"]
    );
}

#[tokio::test]
async fn bounded_batch_runs_all_three_stages_without_an_outer_poll_sleep() {
    let executions = [
        lease(
            OzonLaunchAction::CreateCampaign,
            OzonLaunchClaimMode::Execute,
            OzonLaunchStatus::Approved,
        ),
        lease(
            OzonLaunchAction::AddProducts,
            OzonLaunchClaimMode::Execute,
            OzonLaunchStatus::Created,
        ),
        lease(
            OzonLaunchAction::ActivateCampaign,
            OzonLaunchClaimMode::Execute,
            OzonLaunchStatus::ProductsAdded,
        ),
    ];
    let repository = TestRepository::with_leases([], executions);
    let io = TestIo::new(
        [
            Ok(OzonLaunchWriteReceipt::Created(42)),
            Ok(OzonLaunchWriteReceipt::Mutated(42)),
            Ok(OzonLaunchWriteReceipt::Mutated(42)),
        ],
        [Ok(create_stage(42)), Ok(product_stage(42)), Ok(applied(42))],
    );

    let outcome = drain_ozon_launch_workflow_batch(
        &repository,
        &io,
        &NoOzonLaunchFailpoints,
        "account",
        "worker",
    )
    .await
    .unwrap();

    assert_eq!(outcome.processed, 3);
    assert_eq!(outcome.persisted_failures, 0);
    assert!(!outcome.saturated);
    assert_eq!(io.execute_count.load(Ordering::Acquire), 3);
    assert!(
        repository
            .events()
            .contains(&"complete:create_campaign:42:true".to_owned())
    );
    assert!(
        repository
            .events()
            .contains(&"complete:add_products:42:true".to_owned())
    );
    assert!(
        repository
            .events()
            .contains(&"complete:activate_campaign:42:true".to_owned())
    );
}

#[tokio::test]
async fn definite_ambiguous_and_prewrite_failures_take_distinct_durable_paths() {
    for (failure, expected_event, expected_error) in [
        (
            OzonLaunchWriteFailure::NotStarted("oauth".to_owned()),
            "release:create_campaign:ozon_create_not_started",
            "not_started",
        ),
        (
            OzonLaunchWriteFailure::Definite("ozon_create_precondition_conflict"),
            "failed:create_campaign:ozon_create_precondition_conflict:0",
            "write",
        ),
        (
            OzonLaunchWriteFailure::Ambiguous("ozon_create_ambiguous"),
            "ambiguous:create_campaign:ozon_create_ambiguous:0:false",
            "write",
        ),
    ] {
        let repository = TestRepository::with_leases(
            [],
            [lease(
                OzonLaunchAction::CreateCampaign,
                OzonLaunchClaimMode::Execute,
                OzonLaunchStatus::Approved,
            )],
        );
        let io = TestIo::new([Err(failure)], []);
        let error = drain_ozon_launch_workflow_once(
            &repository,
            &io,
            &NoOzonLaunchFailpoints,
            "account",
            "worker",
        )
        .await
        .unwrap_err();
        assert!(
            repository
                .events()
                .iter()
                .any(|event| event == expected_event)
        );
        assert_eq!(
            match error {
                OzonLaunchWorkflowError::WriteNotStarted(_) => "not_started",
                OzonLaunchWorkflowError::Write(_) => "write",
                _ => "unexpected",
            },
            expected_error
        );
    }
}

#[tokio::test]
async fn every_crash_boundary_recovers_by_readback_without_a_second_post() {
    for point in [
        OzonLaunchFailpoint::AfterWriteStarted,
        OzonLaunchFailpoint::AfterWrite,
        OzonLaunchFailpoint::AfterReadback,
    ] {
        let execute_lease = lease(
            OzonLaunchAction::CreateCampaign,
            OzonLaunchClaimMode::Execute,
            OzonLaunchStatus::Approved,
        );
        let repository = TestRepository::with_leases([], [execute_lease]);
        let io = TestIo::new(
            [Ok(OzonLaunchWriteReceipt::Created(42))],
            [Ok(create_stage(42)), Ok(create_stage(42))],
        );
        let failpoint = OneShotFailpoint::new(point);
        let first =
            drain_ozon_launch_workflow_once(&repository, &io, &failpoint, "account", "worker")
                .await;
        assert!(first.is_err());

        repository.push_recovery(lease(
            OzonLaunchAction::CreateCampaign,
            OzonLaunchClaimMode::Reconcile,
            OzonLaunchStatus::Creating,
        ));
        let recovered =
            drain_ozon_launch_workflow_once(&repository, &io, &failpoint, "account", "worker")
                .await
                .unwrap();
        assert!(matches!(
            recovered,
            OzonLaunchDrainOutcome::Reconciled {
                status: OzonLaunchStatus::Created,
                ..
            }
        ));
        assert_eq!(io.execute_count.load(Ordering::Acquire), 1);
    }

    let repository = TestRepository::with_leases(
        [],
        [lease(
            OzonLaunchAction::CreateCampaign,
            OzonLaunchClaimMode::Execute,
            OzonLaunchStatus::Approved,
        )],
    );
    let io = TestIo::default();
    let error = drain_ozon_launch_workflow_once(
        &repository,
        &io,
        &OneShotFailpoint::new(OzonLaunchFailpoint::AfterClaim),
        "account",
        "worker",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error,
        OzonLaunchWorkflowError::Failpoint(OzonLaunchFailpoint::AfterClaim)
    );
    assert_eq!(io.execute_count.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn readback_failure_is_backed_off_and_does_not_block_the_next_row() {
    let repository = TestRepository::with_leases(
        [],
        [
            lease(
                OzonLaunchAction::AddProducts,
                OzonLaunchClaimMode::Execute,
                OzonLaunchStatus::Created,
            ),
            lease(
                OzonLaunchAction::CreateCampaign,
                OzonLaunchClaimMode::Execute,
                OzonLaunchStatus::Approved,
            ),
        ],
    );
    let io = TestIo::new(
        [
            Ok(OzonLaunchWriteReceipt::Mutated(42)),
            Ok(OzonLaunchWriteReceipt::Created(43)),
        ],
        [Err("provider unavailable".to_owned()), Ok(create_stage(43))],
    );
    let outcome = drain_ozon_launch_workflow_batch(
        &repository,
        &io,
        &NoOzonLaunchFailpoints,
        "account",
        "worker",
    )
    .await
    .unwrap();
    assert_eq!(outcome.processed, 2);
    assert_eq!(outcome.persisted_failures, 1);
    assert!(!outcome.saturated);
    assert!(repository.events().iter().any(|event| {
        event == "ambiguous:add_products:ozon_products_readback_unavailable:42:false"
    }));
    assert_eq!(io.execute_count.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn recovery_can_prove_applied_and_execution_mismatches_fail_closed() {
    let recovery = lease(
        OzonLaunchAction::ActivateCampaign,
        OzonLaunchClaimMode::Reconcile,
        OzonLaunchStatus::Ambiguous,
    );
    let repository = TestRepository::with_leases([recovery], []);
    let io = TestIo::new([], [Ok(applied(42))]);
    let outcome = drain_ozon_launch_workflow_once(
        &repository,
        &io,
        &NoOzonLaunchFailpoints,
        "account",
        "worker",
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        OzonLaunchDrainOutcome::Reconciled {
            status: OzonLaunchStatus::Applied,
            ..
        }
    ));
    assert!(
        repository
            .events()
            .contains(&"confirm:activate_campaign:42".to_owned())
    );

    let wrong_mode = lease(
        OzonLaunchAction::CreateCampaign,
        OzonLaunchClaimMode::Reconcile,
        OzonLaunchStatus::Creating,
    );
    let repository = TestRepository::with_leases([], [wrong_mode]);
    let error = drain_ozon_launch_workflow_once(
        &repository,
        &TestIo::default(),
        &NoOzonLaunchFailpoints,
        "account",
        "worker",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error,
        OzonLaunchWorkflowError::Repository(OzonPlanStoreError::InvalidState)
    );
}

mod regression;

#[test]
fn parser_error_classes_and_budget_constants_are_exact() {
    assert_eq!(positive_json_u64(Some(&serde_json::json!(1))), Some(1));
    assert_eq!(positive_json_u64(Some(&serde_json::json!("1"))), Some(1));
    assert_eq!(positive_json_u64(Some(&serde_json::json!("01"))), None);
    assert_eq!(positive_json_u64(Some(&serde_json::json!(0))), None);
    assert_eq!(positive_json_u64(Some(&serde_json::json!(-1))), None);
    assert_eq!(positive_json_u64(Some(&serde_json::json!(true))), None);
    assert_eq!(positive_json_u64(None), None);
    assert_eq!(DEFAULT_FINAL_PERMIT_DEADLINE, Duration::from_secs(60));
    assert_eq!(DEFAULT_READBACK_DEADLINE, Duration::from_secs(60));
    assert_eq!(PERFORMANCE_CROSS_CLIENT_BOUNDARY, Duration::from_secs(2));
    assert_eq!(MAX_OZON_LAUNCH_ACTIONS_PER_CYCLE, 16);
    for (action, expected) in [
        (OzonLaunchAction::CreateCampaign, "ozon_create_ambiguous"),
        (OzonLaunchAction::AddProducts, "ozon_products_ambiguous"),
        (
            OzonLaunchAction::ActivateCampaign,
            "ozon_activate_ambiguous",
        ),
    ] {
        assert_eq!(ambiguous_write_error_class(action), expected);
    }
    for (action, expected) in [
        (
            OzonLaunchAction::CreateCampaign,
            "ozon_create_readback_unavailable",
        ),
        (
            OzonLaunchAction::AddProducts,
            "ozon_products_readback_unavailable",
        ),
        (
            OzonLaunchAction::ActivateCampaign,
            "ozon_activate_readback_unavailable",
        ),
    ] {
        assert_eq!(readback_error_class(action), expected);
    }
    for error in [
        OzonWriteError::Http {
            status: reqwest::StatusCode::BAD_REQUEST,
        },
        OzonWriteError::Unauthorized,
        OzonWriteError::Forbidden,
        OzonWriteError::Http {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
        },
    ] {
        assert!(matches!(
            classify_provider_write_failure(OzonLaunchAction::CreateCampaign, true, &error),
            OzonLaunchWriteFailure::Ambiguous("ozon_create_ambiguous")
        ));
    }
    assert!(matches!(
        classify_provider_write_failure(
            OzonLaunchAction::CreateCampaign,
            false,
            &OzonWriteError::TokenHttp {
                status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
            },
        ),
        OzonLaunchWriteFailure::NotStarted(_)
    ));
}

pub(in crate::control::ozon) mod adapter_fixture;
mod adapter_postgres;
mod adapter_reads;
