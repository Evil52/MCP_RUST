//! Durable outbox consumer for Ozon campaign launches.
//!
//! Approval and execution are deliberately separate capabilities. The MCP
//! apply tool only records the explicit outbox request; this module is the
//! sole owner of marketplace launch mutations and always reconciles an
//! uncertain action by readback before considering any later action.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use serde_json::Value;
use thiserror::Error;

use crate::{
    config::{AccessRegistry, RegistrySource, StoreId},
    control::policy::{ControlMode, ControlPolicy},
    ozon_performance::{CampaignProductsQuery, CampaignsQuery, PerformanceClient},
};

use super::{
    client::{OzonAdsWriteClient, OzonGuardedWriteError, OzonWriteError},
    model::{
        OzonCampaignPlan, OzonLaunchAction, OzonLaunchClaimMode, OzonLaunchLease, OzonLaunchStatus,
        OzonPlanStoreError,
    },
    repository::{OzonPlanRepository, create_identity_preflight_digest_for},
};

const DEFAULT_FINAL_PERMIT_DEADLINE: Duration = Duration::from_secs(60);
const DEFAULT_READBACK_DEADLINE: Duration = Duration::from_secs(60);
const PERFORMANCE_CROSS_CLIENT_BOUNDARY: Duration = Duration::from_secs(2);
pub(in crate::control) const MAX_OZON_LAUNCH_ACTIONS_PER_CYCLE: usize = 16;

#[allow(
    clippy::enum_variant_names,
    reason = "the names document exact crash boundaries in traces and tests"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control) enum OzonLaunchFailpoint {
    AfterClaim,
    AfterWriteStarted,
    AfterWrite,
    AfterReadback,
}

pub(in crate::control) trait OzonLaunchFailpoints {
    fn hit(&self, point: OzonLaunchFailpoint) -> Result<(), OzonLaunchWorkflowError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub(in crate::control) struct NoOzonLaunchFailpoints;

impl OzonLaunchFailpoints for NoOzonLaunchFailpoints {
    fn hit(&self, _point: OzonLaunchFailpoint) -> Result<(), OzonLaunchWorkflowError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control) struct OzonLaunchDeadlineExceeded;

#[allow(async_fn_in_trait)]
pub(in crate::control) trait OzonLaunchClock {
    async fn timeout<T, F>(
        &self,
        duration: Duration,
        future: F,
    ) -> Result<T, OzonLaunchDeadlineExceeded>
    where
        T: Send,
        F: Future<Output = T> + Send;

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send;
}

#[derive(Debug, Default, Clone, Copy)]
pub(in crate::control) struct TokioOzonLaunchClock;

impl OzonLaunchClock for TokioOzonLaunchClock {
    async fn timeout<T, F>(
        &self,
        duration: Duration,
        future: F,
    ) -> Result<T, OzonLaunchDeadlineExceeded>
    where
        T: Send,
        F: Future<Output = T> + Send,
    {
        tokio::time::timeout(duration, future)
            .await
            .map_err(|_| OzonLaunchDeadlineExceeded)
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(duration)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::control) enum OzonLaunchDrainOutcome {
    Idle,
    Executed {
        plan_id: String,
        status: OzonLaunchStatus,
    },
    Reconciled {
        plan_id: String,
        status: OzonLaunchStatus,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control) struct OzonLaunchBatchOutcome {
    pub processed: usize,
    pub persisted_failures: usize,
    pub saturated: bool,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(in crate::control) enum OzonLaunchWorkflowError {
    #[error("durable Ozon launch repository failed: {0}")]
    Repository(OzonPlanStoreError),
    #[error("durable Ozon launch mutation was not started: {0}")]
    WriteNotStarted(String),
    #[error("durable Ozon launch mutation failed: {0}")]
    Write(String),
    #[error("durable Ozon launch readback failed: {0}")]
    Readback(String),
    #[allow(
        dead_code,
        reason = "constructed by injected deterministic test failpoints"
    )]
    #[error("durable Ozon launch failpoint reached: {0:?}")]
    Failpoint(OzonLaunchFailpoint),
}

impl From<OzonPlanStoreError> for OzonLaunchWorkflowError {
    fn from(error: OzonPlanStoreError) -> Self {
        Self::Repository(error)
    }
}

#[derive(Debug)]
pub(in crate::control) enum OzonLaunchWriteReceipt {
    Created(u64),
    Mutated(u64),
}

#[derive(Debug)]
pub(in crate::control) enum OzonLaunchWriteFailure {
    NotStarted(String),
    Definite(&'static str),
    Ambiguous(&'static str),
}

#[derive(Debug)]
enum OzonFinalPermitError {
    Transient(String),
    Conflict(&'static str),
}

#[derive(Debug)]
pub(in crate::control) enum OzonLaunchObservation {
    Stage { campaign_id: u64, readback: Value },
    Applied { campaign_id: u64, readback: Value },
}

#[allow(async_fn_in_trait)]
pub(in crate::control) trait OzonLaunchRepositoryPort {
    async fn claim_recovery(
        &self,
        account_id: &str,
        worker_id: &str,
    ) -> Result<Option<OzonLaunchLease>, OzonPlanStoreError>;

    async fn claim_execution(
        &self,
        account_id: &str,
        worker_id: &str,
    ) -> Result<Option<OzonLaunchLease>, OzonPlanStoreError>;

    async fn complete(
        &self,
        lease: &OzonLaunchLease,
        campaign_id: Option<u64>,
        readback: Option<&Value>,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError>;

    async fn confirm_applied(
        &self,
        lease: &OzonLaunchLease,
        campaign_id: u64,
        readback: &Value,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError>;

    async fn mark_ambiguous(
        &self,
        lease: &OzonLaunchLease,
        error_class: &str,
        campaign_id: Option<u64>,
        readback: Option<&Value>,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError>;

    async fn fail(
        &self,
        lease: &OzonLaunchLease,
        error_class: &str,
        campaign_id: Option<u64>,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError>;

    async fn release(
        &self,
        lease: &OzonLaunchLease,
        error_class: &str,
    ) -> Result<(), OzonPlanStoreError>;
}

impl OzonLaunchRepositoryPort for OzonPlanRepository {
    async fn claim_recovery(
        &self,
        account_id: &str,
        worker_id: &str,
    ) -> Result<Option<OzonLaunchLease>, OzonPlanStoreError> {
        self.claim_launch_recovery(account_id, worker_id).await
    }

    async fn claim_execution(
        &self,
        account_id: &str,
        worker_id: &str,
    ) -> Result<Option<OzonLaunchLease>, OzonPlanStoreError> {
        self.claim_next_launch_action(account_id, worker_id).await
    }

    async fn complete(
        &self,
        lease: &OzonLaunchLease,
        campaign_id: Option<u64>,
        readback: Option<&Value>,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        self.complete_launch_action(lease, campaign_id, readback)
            .await
    }

    async fn confirm_applied(
        &self,
        lease: &OzonLaunchLease,
        campaign_id: u64,
        readback: &Value,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        self.confirm_launch_applied(lease, campaign_id, readback)
            .await
    }

    async fn mark_ambiguous(
        &self,
        lease: &OzonLaunchLease,
        error_class: &str,
        campaign_id: Option<u64>,
        readback: Option<&Value>,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        self.mark_launch_ambiguous(lease, error_class, campaign_id, readback)
            .await
    }

    async fn fail(
        &self,
        lease: &OzonLaunchLease,
        error_class: &str,
        campaign_id: Option<u64>,
    ) -> Result<OzonCampaignPlan, OzonPlanStoreError> {
        self.fail_launch_action(lease, error_class, campaign_id)
            .await
    }

    async fn release(
        &self,
        lease: &OzonLaunchLease,
        error_class: &str,
    ) -> Result<(), OzonPlanStoreError> {
        self.release_launch_lease(lease, error_class).await
    }
}

#[allow(async_fn_in_trait)]
pub(in crate::control) trait OzonLaunchIoPort {
    async fn execute<F>(
        &self,
        lease: &OzonLaunchLease,
        failpoints: &F,
    ) -> Result<OzonLaunchWriteReceipt, OzonLaunchWriteFailure>
    where
        F: OzonLaunchFailpoints + Sync;

    async fn readback(&self, lease: &OzonLaunchLease) -> Result<OzonLaunchObservation, String>;
}

/// Processes at most one durable launch lease. Recovery is always selected
/// before fresh execution, keeping the call bounded and preventing starvation
/// of uncertain writes after a restart.
#[allow(
    clippy::future_not_send,
    reason = "the injected async port traits intentionally support deterministic local test doubles"
)]
pub(in crate::control) async fn drain_ozon_launch_workflow_once<R, I, F>(
    repository: &R,
    io: &I,
    failpoints: &F,
    account_id: &str,
    worker_id: &str,
) -> Result<OzonLaunchDrainOutcome, OzonLaunchWorkflowError>
where
    R: OzonLaunchRepositoryPort,
    I: OzonLaunchIoPort,
    F: OzonLaunchFailpoints + Sync,
{
    if let Some(lease) = repository.claim_recovery(account_id, worker_id).await? {
        if lease.mode != OzonLaunchClaimMode::Reconcile {
            return Err(OzonPlanStoreError::InvalidState.into());
        }
        failpoints.hit(OzonLaunchFailpoint::AfterClaim)?;
        return reconcile(repository, io, failpoints, &lease).await;
    }
    let Some(lease) = repository.claim_execution(account_id, worker_id).await? else {
        return Ok(OzonLaunchDrainOutcome::Idle);
    };
    if lease.mode != OzonLaunchClaimMode::Execute {
        return Err(OzonPlanStoreError::InvalidState.into());
    }
    failpoints.hit(OzonLaunchFailpoint::AfterClaim)?;
    execute(repository, io, failpoints, &lease).await
}

/// Drains a bounded launch backlog without inserting the guard runtime's outer
/// polling sleep between the three stages of one launch. Persisted provider or
/// readback failures are isolated to their row, while repository/fencing and
/// deterministic crash failures stop the batch immediately.
#[allow(
    clippy::future_not_send,
    reason = "the injected async port traits intentionally support deterministic local test doubles"
)]
pub(in crate::control) async fn drain_ozon_launch_workflow_batch<R, I, F>(
    repository: &R,
    io: &I,
    failpoints: &F,
    account_id: &str,
    worker_id: &str,
) -> Result<OzonLaunchBatchOutcome, OzonLaunchWorkflowError>
where
    R: OzonLaunchRepositoryPort,
    I: OzonLaunchIoPort,
    F: OzonLaunchFailpoints + Sync,
{
    let mut processed = 0_usize;
    let mut persisted_failures = 0_usize;
    for _ in 0..MAX_OZON_LAUNCH_ACTIONS_PER_CYCLE {
        match drain_ozon_launch_workflow_once(repository, io, failpoints, account_id, worker_id)
            .await
        {
            Ok(OzonLaunchDrainOutcome::Idle) => {
                return Ok(OzonLaunchBatchOutcome {
                    processed,
                    persisted_failures,
                    saturated: false,
                });
            }
            Ok(
                OzonLaunchDrainOutcome::Executed { .. } | OzonLaunchDrainOutcome::Reconciled { .. },
            ) => {
                processed = processed.saturating_add(1);
            }
            Err(
                OzonLaunchWorkflowError::WriteNotStarted(_)
                | OzonLaunchWorkflowError::Write(_)
                | OzonLaunchWorkflowError::Readback(_),
            ) => {
                processed = processed.saturating_add(1);
                persisted_failures = persisted_failures.saturating_add(1);
            }
            Err(
                error @ (OzonLaunchWorkflowError::Repository(_)
                | OzonLaunchWorkflowError::Failpoint(_)),
            ) => return Err(error),
        }
    }
    Ok(OzonLaunchBatchOutcome {
        processed,
        persisted_failures,
        saturated: true,
    })
}

#[allow(
    clippy::future_not_send,
    reason = "the injected async port traits intentionally support deterministic local test doubles"
)]
async fn execute<R, I, F>(
    repository: &R,
    io: &I,
    failpoints: &F,
    lease: &OzonLaunchLease,
) -> Result<OzonLaunchDrainOutcome, OzonLaunchWorkflowError>
where
    R: OzonLaunchRepositoryPort,
    I: OzonLaunchIoPort,
    F: OzonLaunchFailpoints + Sync,
{
    let receipt = match io.execute(lease, failpoints).await {
        Ok(receipt) => receipt,
        Err(OzonLaunchWriteFailure::NotStarted(error)) => {
            repository
                .release(lease, not_started_error_class(lease.action))
                .await?;
            return Err(OzonLaunchWorkflowError::WriteNotStarted(error));
        }
        Err(OzonLaunchWriteFailure::Definite(error_class)) => {
            let plan = repository
                .fail(lease, error_class, lease.plan.campaign_id)
                .await?;
            return Err(OzonLaunchWorkflowError::Write(format!(
                "{} ({})",
                error_class,
                plan.status.as_db()
            )));
        }
        Err(OzonLaunchWriteFailure::Ambiguous(error_class)) => {
            repository
                .mark_ambiguous(lease, error_class, lease.plan.campaign_id, None)
                .await?;
            return Err(OzonLaunchWorkflowError::Write(error_class.to_owned()));
        }
    };

    match (lease.action, receipt) {
        (OzonLaunchAction::CreateCampaign, OzonLaunchWriteReceipt::Created(campaign_id)) => {
            let observation = match io.readback(lease).await {
                Ok(observation) => observation,
                Err(error) => {
                    repository
                        .mark_ambiguous(
                            lease,
                            readback_error_class(lease.action),
                            Some(campaign_id),
                            None,
                        )
                        .await?;
                    return Err(OzonLaunchWorkflowError::Readback(error));
                }
            };
            failpoints.hit(OzonLaunchFailpoint::AfterReadback)?;
            let (observed_campaign_id, readback, applied) = match observation {
                OzonLaunchObservation::Stage {
                    campaign_id,
                    readback,
                } => (campaign_id, readback, false),
                OzonLaunchObservation::Applied {
                    campaign_id,
                    readback,
                } => (campaign_id, readback, true),
            };
            if observed_campaign_id != campaign_id {
                repository
                    .mark_ambiguous(
                        lease,
                        "ozon_create_readback_mismatch",
                        Some(campaign_id),
                        Some(&readback),
                    )
                    .await?;
                return Err(OzonLaunchWorkflowError::Readback(
                    "create response and exact readback campaign ids differ".to_owned(),
                ));
            }
            let plan = if applied {
                repository
                    .confirm_applied(lease, campaign_id, &readback)
                    .await?
            } else {
                repository
                    .complete(lease, Some(campaign_id), Some(&readback))
                    .await?
            };
            Ok(OzonLaunchDrainOutcome::Executed {
                plan_id: plan.plan_id,
                status: plan.status,
            })
        }
        (
            OzonLaunchAction::AddProducts | OzonLaunchAction::ActivateCampaign,
            OzonLaunchWriteReceipt::Mutated(campaign_id),
        ) => {
            let observation = match io.readback(lease).await {
                Ok(observation) => observation,
                Err(error) => {
                    repository
                        .mark_ambiguous(
                            lease,
                            readback_error_class(lease.action),
                            Some(campaign_id),
                            None,
                        )
                        .await?;
                    return Err(OzonLaunchWorkflowError::Readback(error));
                }
            };
            failpoints.hit(OzonLaunchFailpoint::AfterReadback)?;
            let plan = match (lease.action, observation) {
                (
                    OzonLaunchAction::AddProducts,
                    OzonLaunchObservation::Stage {
                        campaign_id,
                        readback,
                    },
                )
                | (
                    OzonLaunchAction::ActivateCampaign,
                    OzonLaunchObservation::Applied {
                        campaign_id,
                        readback,
                    },
                ) => {
                    repository
                        .complete(lease, Some(campaign_id), Some(&readback))
                        .await?
                }
                (
                    OzonLaunchAction::AddProducts,
                    OzonLaunchObservation::Applied {
                        campaign_id,
                        readback,
                    },
                ) => {
                    repository
                        .confirm_applied(lease, campaign_id, &readback)
                        .await?
                }
                (OzonLaunchAction::ActivateCampaign, OzonLaunchObservation::Stage { .. }) => {
                    repository
                        .mark_ambiguous(
                            lease,
                            "ozon_activate_readback_mismatch",
                            Some(campaign_id),
                            None,
                        )
                        .await?;
                    return Err(OzonLaunchWorkflowError::Readback(
                        "activation did not produce exact running state".to_owned(),
                    ));
                }
                _ => return Err(OzonPlanStoreError::InvalidState.into()),
            };
            Ok(OzonLaunchDrainOutcome::Executed {
                plan_id: plan.plan_id,
                status: plan.status,
            })
        }
        _ => Err(OzonPlanStoreError::InvalidState.into()),
    }
}

#[allow(
    clippy::future_not_send,
    reason = "the injected async port traits intentionally support deterministic local test doubles"
)]
async fn reconcile<R, I, F>(
    repository: &R,
    io: &I,
    failpoints: &F,
    lease: &OzonLaunchLease,
) -> Result<OzonLaunchDrainOutcome, OzonLaunchWorkflowError>
where
    R: OzonLaunchRepositoryPort,
    I: OzonLaunchIoPort,
    F: OzonLaunchFailpoints + Sync,
{
    let observation = match io.readback(lease).await {
        Ok(observation) => observation,
        Err(error) => {
            repository
                .mark_ambiguous(
                    lease,
                    readback_error_class(lease.action),
                    lease.plan.campaign_id,
                    None,
                )
                .await?;
            return Err(OzonLaunchWorkflowError::Readback(error));
        }
    };
    failpoints.hit(OzonLaunchFailpoint::AfterReadback)?;
    let plan = match observation {
        OzonLaunchObservation::Stage {
            campaign_id,
            readback,
        } => {
            repository
                .complete(lease, Some(campaign_id), Some(&readback))
                .await?
        }
        OzonLaunchObservation::Applied {
            campaign_id,
            readback,
        } => {
            repository
                .confirm_applied(lease, campaign_id, &readback)
                .await?
        }
    };
    Ok(OzonLaunchDrainOutcome::Reconciled {
        plan_id: plan.plan_id,
        status: plan.status,
    })
}

const fn readback_error_class(action: OzonLaunchAction) -> &'static str {
    match action {
        OzonLaunchAction::CreateCampaign => "ozon_create_readback_unavailable",
        OzonLaunchAction::AddProducts => "ozon_products_readback_unavailable",
        OzonLaunchAction::ActivateCampaign => "ozon_activate_readback_unavailable",
    }
}

const fn not_started_error_class(action: OzonLaunchAction) -> &'static str {
    match action {
        OzonLaunchAction::CreateCampaign => "ozon_create_not_started",
        OzonLaunchAction::AddProducts => "ozon_products_not_started",
        OzonLaunchAction::ActivateCampaign => "ozon_activate_not_started",
    }
}

/// Production adapter. All dependencies are constructor-injected; tests use
/// the ports above and deterministic failpoints without network or wall time.
pub(in crate::control) struct PerformanceOzonLaunchIo<C = TokioOzonLaunchClock> {
    repository: Arc<OzonPlanRepository>,
    reader: Arc<PerformanceClient>,
    writer: Arc<OzonAdsWriteClient>,
    registry: RegistrySource,
    policy: Arc<ControlPolicy>,
    account_id: String,
    store_id: StoreId,
    clock: C,
    final_permit_deadline: Duration,
}

impl PerformanceOzonLaunchIo<TokioOzonLaunchClock> {
    pub(in crate::control) const fn new(
        repository: Arc<OzonPlanRepository>,
        reader: Arc<PerformanceClient>,
        writer: Arc<OzonAdsWriteClient>,
        registry: RegistrySource,
        policy: Arc<ControlPolicy>,
        account_id: String,
        store_id: StoreId,
    ) -> Self {
        Self {
            repository,
            reader,
            writer,
            registry,
            policy,
            account_id,
            store_id,
            clock: TokioOzonLaunchClock,
            final_permit_deadline: DEFAULT_FINAL_PERMIT_DEADLINE,
        }
    }
}

impl<C> PerformanceOzonLaunchIo<C>
where
    C: OzonLaunchClock + Sync,
{
    async fn preflight(&self, lease: &OzonLaunchLease) -> Result<(), OzonFinalPermitError> {
        self.clock
            .timeout(self.final_permit_deadline, async {
                match lease.action {
                    OzonLaunchAction::CreateCampaign => {
                        ensure_ozon_sku_not_running(
                            self.reader.as_ref(),
                            &self.store_id,
                            lease.plan.sku,
                        )
                        .await
                        .map_err(classify_create_preflight_error)?;
                        ensure_ozon_campaign_title_absent(
                            self.reader.as_ref(),
                            &self.store_id,
                            &lease.plan.manifest.create_request.title,
                        )
                        .await
                        .map_err(classify_create_preflight_error)?;
                    }
                    OzonLaunchAction::AddProducts => {
                        ensure_add_products_precondition(
                            self.reader.as_ref(),
                            &self.store_id,
                            &lease.plan,
                        )
                        .await?;
                    }
                    OzonLaunchAction::ActivateCampaign => {
                        ensure_activate_precondition(
                            self.reader.as_ref(),
                            &self.store_id,
                            &lease.plan,
                        )
                        .await?;
                    }
                }
                // Preserve the documented read/write pacing boundary. The
                // writer then reserves the shared account pacer, which closes
                // any race with reads from another workflow.
                self.clock.sleep(PERFORMANCE_CROSS_CLIENT_BOUNDARY).await;
                Ok(())
            })
            .await
            .map_err(|_| {
                OzonFinalPermitError::Transient("launch preflight deadline exceeded".to_owned())
            })?
    }

    async fn final_permit<F>(
        &self,
        lease: &OzonLaunchLease,
        failpoints: &F,
        write_started: &AtomicBool,
    ) -> Result<(), OzonFinalPermitError>
    where
        F: OzonLaunchFailpoints + Sync,
    {
        self.clock
            .timeout(self.final_permit_deadline, async {
                let registry = self.registry.load_async().await.map_err(|error| {
                    OzonFinalPermitError::Transient(format!("registry reload failed: {error}"))
                })?;
                authorize_launch_plan(&self.policy, &registry, &self.account_id, &lease.plan)
                    .map_err(OzonFinalPermitError::Transient)?;
                let create_identity_digest = (lease.action == OzonLaunchAction::CreateCampaign)
                    .then(|| create_identity_preflight_digest_for(&lease.plan));
                self.repository
                    .start_launch_write(lease, create_identity_digest.as_deref(), || {
                        write_started.store(true, Ordering::Release);
                    })
                    .await
                    .map_err(|error| OzonFinalPermitError::Transient(error.to_string()))?;
                failpoints
                    .hit(OzonLaunchFailpoint::AfterWriteStarted)
                    .map_err(|error| OzonFinalPermitError::Transient(error.to_string()))
            })
            .await
            .map_err(|_| {
                OzonFinalPermitError::Transient("final permit deadline exceeded".to_owned())
            })?
    }
}

impl<C> OzonLaunchIoPort for PerformanceOzonLaunchIo<C>
where
    C: OzonLaunchClock + Sync,
{
    async fn execute<F>(
        &self,
        lease: &OzonLaunchLease,
        failpoints: &F,
    ) -> Result<OzonLaunchWriteReceipt, OzonLaunchWriteFailure>
    where
        F: OzonLaunchFailpoints + Sync,
    {
        let write_started = Arc::new(AtomicBool::new(false));
        if let Err(error) = self.preflight(lease).await {
            return match error {
                OzonFinalPermitError::Conflict(error_class) => {
                    Err(OzonLaunchWriteFailure::Definite(error_class))
                }
                OzonFinalPermitError::Transient(error) => {
                    Err(OzonLaunchWriteFailure::NotStarted(error))
                }
            };
        }
        let result = match lease.action {
            OzonLaunchAction::CreateCampaign => {
                let write_started_for_permit = Arc::clone(&write_started);
                self.writer
                    .create_campaign_with_permit(&lease.plan.manifest.create_request, || async {
                        self.final_permit(lease, failpoints, write_started_for_permit.as_ref())
                            .await
                    })
                    .await
                    .map(OzonLaunchWriteReceipt::Created)
            }
            OzonLaunchAction::AddProducts => {
                let campaign_id = lease
                    .plan
                    .campaign_id
                    .ok_or(OzonLaunchWriteFailure::Definite("ozon_campaign_id_missing"))?;
                let write_started_for_permit = Arc::clone(&write_started);
                self.writer
                    .add_products_with_permit(
                        campaign_id,
                        lease
                            .plan
                            .manifest
                            .create_request
                            .product_autopilot_strategy,
                        &lease.plan.manifest.products_request,
                        || async {
                            self.final_permit(lease, failpoints, write_started_for_permit.as_ref())
                                .await
                        },
                    )
                    .await
                    .map(|()| OzonLaunchWriteReceipt::Mutated(campaign_id))
            }
            OzonLaunchAction::ActivateCampaign => {
                let campaign_id = lease
                    .plan
                    .campaign_id
                    .ok_or(OzonLaunchWriteFailure::Definite("ozon_campaign_id_missing"))?;
                let write_started_for_permit = Arc::clone(&write_started);
                self.writer
                    .activate_campaign_with_permit(campaign_id, || async {
                        self.final_permit(lease, failpoints, write_started_for_permit.as_ref())
                            .await
                    })
                    .await
                    .map(|()| OzonLaunchWriteReceipt::Mutated(campaign_id))
            }
        };
        match result {
            Ok(receipt) => {
                failpoints
                    .hit(OzonLaunchFailpoint::AfterWrite)
                    .map_err(|_| {
                        OzonLaunchWriteFailure::Ambiguous(ambiguous_write_error_class(lease.action))
                    })?;
                Ok(receipt)
            }
            Err(OzonGuardedWriteError::Permit(OzonFinalPermitError::Conflict(error_class))) => {
                Err(OzonLaunchWriteFailure::Definite(error_class))
            }
            Err(OzonGuardedWriteError::Permit(OzonFinalPermitError::Transient(error))) => {
                if write_started.load(Ordering::Acquire) {
                    Err(OzonLaunchWriteFailure::Ambiguous(
                        ambiguous_write_error_class(lease.action),
                    ))
                } else {
                    Err(OzonLaunchWriteFailure::NotStarted(error))
                }
            }
            Err(OzonGuardedWriteError::Write(error)) => Err(classify_provider_write_failure(
                lease.action,
                write_started.load(Ordering::Acquire),
                &error,
            )),
        }
    }

    async fn readback(&self, lease: &OzonLaunchLease) -> Result<OzonLaunchObservation, String> {
        self.clock
            .timeout(DEFAULT_READBACK_DEADLINE, async {
                self.clock.sleep(PERFORMANCE_CROSS_CLIENT_BOUNDARY).await;
                match lease.action {
                    OzonLaunchAction::CreateCampaign => {
                        let (campaign_id, state) = find_ozon_campaign_identity_by_title(
                            self.reader.as_ref(),
                            &self.store_id,
                            &lease.plan.manifest.create_request.title,
                        )
                        .await?;
                        if state == "CAMPAIGN_STATE_RUNNING" {
                            exact_ozon_launch_readback(
                                self.reader.as_ref(),
                                &self.store_id,
                                campaign_id,
                                lease.plan.sku,
                                &lease.plan.manifest.create_request.title,
                                lease.plan.manifest.spec.initial_cpc_bid_microrubles,
                            )
                            .await
                            .map(|readback| {
                                OzonLaunchObservation::Applied {
                                    campaign_id,
                                    readback,
                                }
                            })
                        } else {
                            Ok(OzonLaunchObservation::Stage {
                                campaign_id,
                                readback: serde_json::json!({
                                    "campaign_id": campaign_id,
                                    "title": lease.plan.manifest.create_request.title,
                                    "state": state,
                                    "action": "create_campaign",
                                    "verified": true,
                                }),
                            })
                        }
                    }
                    OzonLaunchAction::AddProducts => {
                        let campaign_id = lease
                            .plan
                            .campaign_id
                            .ok_or_else(|| "campaign id missing".to_owned())?;
                        exact_ozon_products_stage_readback(
                            self.reader.as_ref(),
                            &self.store_id,
                            campaign_id,
                            lease.plan.sku,
                            &lease.plan.manifest.create_request.title,
                            lease.plan.manifest.spec.initial_cpc_bid_microrubles,
                        )
                        .await
                    }
                    OzonLaunchAction::ActivateCampaign => {
                        let campaign_id = lease
                            .plan
                            .campaign_id
                            .ok_or_else(|| "campaign id missing".to_owned())?;
                        exact_ozon_launch_readback(
                            self.reader.as_ref(),
                            &self.store_id,
                            campaign_id,
                            lease.plan.sku,
                            &lease.plan.manifest.create_request.title,
                            lease.plan.manifest.spec.initial_cpc_bid_microrubles,
                        )
                        .await
                        .map(|readback| OzonLaunchObservation::Applied {
                            campaign_id,
                            readback,
                        })
                    }
                }
            })
            .await
            .map_err(|_| "launch readback deadline exceeded".to_owned())?
    }
}

const fn ambiguous_write_error_class(action: OzonLaunchAction) -> &'static str {
    match action {
        OzonLaunchAction::CreateCampaign => "ozon_create_ambiguous",
        OzonLaunchAction::AddProducts => "ozon_products_ambiguous",
        OzonLaunchAction::ActivateCampaign => "ozon_activate_ambiguous",
    }
}

fn classify_provider_write_failure(
    action: OzonLaunchAction,
    write_started: bool,
    error: &OzonWriteError,
) -> OzonLaunchWriteFailure {
    if write_started {
        // A status code (including 4xx) is not proof that the provider made no
        // state change after the durable marker. Only exact readback resolves
        // this boundary.
        OzonLaunchWriteFailure::Ambiguous(ambiguous_write_error_class(action))
    } else {
        // OAuth and local request construction happen before the permit, so a
        // failure with no marker is a definite no-send and may be retried.
        OzonLaunchWriteFailure::NotStarted(error.to_string())
    }
}

fn authorize_launch_plan(
    policy: &ControlPolicy,
    registry: &AccessRegistry,
    runtime_account_id: &str,
    plan: &OzonCampaignPlan,
) -> Result<(), String> {
    if policy.mode != ControlMode::Enabled
        || plan.schema_version != policy.version
        || plan.policy_revision != policy.revision
        || plan.policy_digest != policy.digest()
        || runtime_account_id != plan.account_id
    {
        return Err("policy/runtime binding changed".to_owned());
    }
    let actor = registry
        .actor(&plan.actor_id)
        .map_err(|_| "plan actor missing".to_owned())?;
    let account = registry
        .accounts
        .iter()
        .find(|account| account.id == plan.account_id)
        .ok_or_else(|| "runtime account missing".to_owned())?;
    if !actor.can_access_account(account) {
        return Err("plan actor account access revoked".to_owned());
    }
    let approval = plan
        .approval
        .as_ref()
        .ok_or_else(|| "approval missing".to_owned())?;
    let approver = registry
        .actor(&approval.approver_id)
        .map_err(|_| "approver missing".to_owned())?;
    if !approver.can_access_account(account) || approver.id == actor.id {
        return Err("approver account access revoked".to_owned());
    }
    let delegated = policy
        .actor_policy(&plan.actor_id)
        .into_iter()
        .flat_map(|actor_policy| &actor_policy.ozon_campaign_launch_targets)
        .find(|target| {
            target.account_id == plan.account_id
                && target.skus.as_slice() == [plan.sku]
                && target.weekly_budget_microrubles == plan.manifest.spec.weekly_budget_microrubles
                && target.per_sku_spend_cap_microrubles
                    == plan.manifest.spec.per_sku_spend_cap_microrubles
                && target.initial_cpc_bid_microrubles
                    == plan.manifest.spec.initial_cpc_bid_microrubles
                && target.max_cpc_bid_microrubles == plan.manifest.spec.max_cpc_bid_microrubles
                && target.target_drr_percent == plan.manifest.spec.target_drr_percent
                && target.target_position == plan.manifest.spec.target_position
        })
        .is_some_and(|target| {
            target
                .approver_actor_ids
                .iter()
                .any(|approver_id| approver_id == &approver.id)
        });
    delegated
        .then_some(())
        .ok_or_else(|| "launch delegation changed".to_owned())
}

pub(in crate::control) async fn ensure_ozon_sku_not_running(
    reader: &PerformanceClient,
    store: &StoreId,
    sku: u64,
) -> Result<(), String> {
    let mut page = 1_u32;
    let mut visited = 0_usize;
    loop {
        let response = reader
            .campaigns(
                store,
                CampaignsQuery {
                    campaign_ids: Vec::new(),
                    adv_object_type: Some("SKU"),
                    state: Some("CAMPAIGN_STATE_RUNNING"),
                    page,
                    page_size: 100,
                },
            )
            .await
            .map_err(|error| format!("SKU preflight failed: {error}"))?;
        let campaigns = response
            .get("list")
            .and_then(Value::as_array)
            .ok_or_else(|| "SKU preflight campaign list is invalid".to_owned())?;
        for campaign in campaigns {
            let (campaign_id, _, state) =
                campaign_identity(campaign).map_err(|error| format!("SKU preflight {error}"))?;
            if state != "CAMPAIGN_STATE_RUNNING" {
                return Err("SKU preflight campaign state is not running".to_owned());
            }
            visited = visited.saturating_add(1);
            if visited > 1_000 {
                return Err("SKU preflight campaign bound exceeded".to_owned());
            }
            let products = reader
                .campaign_products(
                    store,
                    campaign_id,
                    CampaignProductsQuery {
                        page: 1,
                        page_size: 100,
                    },
                )
                .await
                .map_err(|error| format!("SKU preflight failed: {error}"))?;
            let rows = products
                .get("products")
                .and_then(Value::as_array)
                .ok_or_else(|| "SKU preflight products list is invalid".to_owned())?;
            let mut contains_sku = false;
            for product in rows {
                let observed_sku = positive_json_u64(product.get("sku"))
                    .ok_or_else(|| "SKU preflight product SKU is invalid".to_owned())?;
                contains_sku |= observed_sku == sku;
            }
            if contains_sku {
                return Err(format!(
                    "SKU {sku} already belongs to running campaign {campaign_id}"
                ));
            }
            if rows.len() == 100 {
                return Err("SKU preflight products pagination is incomplete".to_owned());
            }
        }
        if campaigns.len() < 100 {
            return Ok(());
        }
        page = page
            .checked_add(1)
            .ok_or_else(|| "SKU preflight page overflow".to_owned())?;
    }
}

#[cfg(test)]
pub(in crate::control) async fn find_ozon_campaign_by_title(
    reader: &PerformanceClient,
    store: &StoreId,
    title: &str,
) -> Result<u64, String> {
    find_ozon_campaign_identity_by_title(reader, store, title)
        .await
        .map(|(campaign_id, _)| campaign_id)
}

async fn find_ozon_campaign_identity_by_title(
    reader: &PerformanceClient,
    store: &StoreId,
    title: &str,
) -> Result<(u64, String), String> {
    let mut matches = Vec::new();
    let mut complete_listing = false;
    for page in 1..=100_u32 {
        let response = reader
            .campaigns(
                store,
                CampaignsQuery {
                    campaign_ids: Vec::new(),
                    adv_object_type: Some("SKU"),
                    state: None,
                    page,
                    page_size: 100,
                },
            )
            .await
            .map_err(|error| format!("campaign readback failed: {error}"))?;
        let campaigns = response
            .get("list")
            .and_then(Value::as_array)
            .ok_or_else(|| "campaign readback list is invalid".to_owned())?;
        for campaign in campaigns {
            let (campaign_id, observed_title, state) = campaign_identity(campaign)
                .map_err(|error| format!("campaign readback {error}"))?;
            if observed_title == title {
                matches.push((campaign_id, state.to_owned()));
            }
        }
        if campaigns.len() < 100 {
            complete_listing = true;
            break;
        }
    }
    if !complete_listing {
        return Err("campaign readback listing bound exceeded".to_owned());
    }
    match matches.as_slice() {
        [campaign] => Ok(campaign.clone()),
        [] => Err("campaign readback not found".to_owned()),
        _ => Err("campaign readback title is not unique".to_owned()),
    }
}

async fn ensure_ozon_campaign_title_absent(
    reader: &PerformanceClient,
    store: &StoreId,
    title: &str,
) -> Result<(), String> {
    for page in 1..=100_u32 {
        let response = reader
            .campaigns(
                store,
                CampaignsQuery {
                    campaign_ids: Vec::new(),
                    adv_object_type: Some("SKU"),
                    state: None,
                    page,
                    page_size: 100,
                },
            )
            .await
            .map_err(|error| format!("title preflight failed: {error}"))?;
        let campaigns = response
            .get("list")
            .and_then(Value::as_array)
            .ok_or_else(|| "title preflight campaign list is invalid".to_owned())?;
        for campaign in campaigns {
            let (_, observed_title, _) =
                campaign_identity(campaign).map_err(|error| format!("title preflight {error}"))?;
            if observed_title == title {
                return Err("campaign title already exists".to_owned());
            }
        }
        if campaigns.len() < 100 {
            return Ok(());
        }
    }
    Err("title preflight campaign listing bound exceeded".to_owned())
}

fn classify_create_preflight_error(error: String) -> OzonFinalPermitError {
    if error == "campaign title already exists"
        || (error.starts_with("SKU ") && error.contains(" already belongs "))
    {
        OzonFinalPermitError::Conflict("ozon_create_precondition_conflict")
    } else {
        OzonFinalPermitError::Transient(error)
    }
}

async fn ensure_add_products_precondition(
    reader: &PerformanceClient,
    store: &StoreId,
    plan: &OzonCampaignPlan,
) -> Result<(), OzonFinalPermitError> {
    let campaign_id = plan.campaign_id.ok_or(OzonFinalPermitError::Conflict(
        "ozon_products_precondition_conflict",
    ))?;
    let (state, _) = exact_campaign(
        reader,
        store,
        campaign_id,
        &plan.manifest.create_request.title,
    )
    .await
    .map_err(OzonFinalPermitError::Transient)?;
    let products = reader
        .campaign_products(
            store,
            campaign_id,
            CampaignProductsQuery {
                page: 1,
                page_size: 100,
            },
        )
        .await
        .map_err(|error| {
            OzonFinalPermitError::Transient(format!("add-products preflight failed: {error}"))
        })?;
    let rows = products
        .get("products")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            OzonFinalPermitError::Transient(
                "add-products preflight product list is invalid".to_owned(),
            )
        })?;
    if !is_mutable_non_running_state(&state) || !rows.is_empty() {
        return Err(OzonFinalPermitError::Conflict(
            "ozon_products_precondition_conflict",
        ));
    }
    Ok(())
}

async fn ensure_activate_precondition(
    reader: &PerformanceClient,
    store: &StoreId,
    plan: &OzonCampaignPlan,
) -> Result<(), OzonFinalPermitError> {
    let campaign_id = plan.campaign_id.ok_or(OzonFinalPermitError::Conflict(
        "ozon_activate_precondition_conflict",
    ))?;
    let (state, _) = exact_campaign(
        reader,
        store,
        campaign_id,
        &plan.manifest.create_request.title,
    )
    .await
    .map_err(OzonFinalPermitError::Transient)?;
    let products = reader
        .campaign_products(
            store,
            campaign_id,
            CampaignProductsQuery {
                page: 1,
                page_size: 100,
            },
        )
        .await
        .map_err(|error| {
            OzonFinalPermitError::Transient(format!("activation preflight failed: {error}"))
        })?;
    let rows = products
        .get("products")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            OzonFinalPermitError::Transient(
                "activation preflight product list is invalid".to_owned(),
            )
        })?;
    let exact_product = rows.len() == 1
        && positive_json_u64(rows[0].get("sku")) == Some(plan.sku)
        && positive_json_u64(rows[0].get("bid"))
            == Some(plan.manifest.spec.initial_cpc_bid_microrubles);
    if !is_mutable_non_running_state(&state) || !exact_product {
        return Err(OzonFinalPermitError::Conflict(
            "ozon_activate_precondition_conflict",
        ));
    }
    Ok(())
}

pub(in crate::control) async fn exact_ozon_launch_readback(
    reader: &PerformanceClient,
    store: &StoreId,
    campaign_id: u64,
    sku: u64,
    expected_title: &str,
    expected_bid: u64,
) -> Result<Value, String> {
    let (state, title) = exact_campaign(reader, store, campaign_id, expected_title).await?;
    if state != "CAMPAIGN_STATE_RUNNING" {
        return Err("campaign is not running".to_owned());
    }
    exact_product(reader, store, campaign_id, sku, expected_bid).await?;
    Ok(serde_json::json!({
        "campaign_id": campaign_id,
        "sku": sku,
        "state": state,
        "title": title,
        "bid_microrubles": expected_bid,
    }))
}

async fn exact_ozon_products_stage_readback(
    reader: &PerformanceClient,
    store: &StoreId,
    campaign_id: u64,
    sku: u64,
    expected_title: &str,
    expected_bid: u64,
) -> Result<OzonLaunchObservation, String> {
    let (state, title) = exact_campaign(reader, store, campaign_id, expected_title).await?;
    exact_product(reader, store, campaign_id, sku, expected_bid).await?;
    if state != "CAMPAIGN_STATE_RUNNING" && !is_mutable_non_running_state(&state) {
        return Err("campaign state is not mutable for product attachment".to_owned());
    }
    let readback = serde_json::json!({
        "campaign_id": campaign_id,
        "sku": sku,
        "state": state,
        "title": title,
        "bid_microrubles": expected_bid,
        "action": "add_products",
        "verified": true,
    });
    Ok(if state == "CAMPAIGN_STATE_RUNNING" {
        OzonLaunchObservation::Applied {
            campaign_id,
            readback,
        }
    } else {
        OzonLaunchObservation::Stage {
            campaign_id,
            readback,
        }
    })
}

async fn exact_campaign(
    reader: &PerformanceClient,
    store: &StoreId,
    campaign_id: u64,
    expected_title: &str,
) -> Result<(String, String), String> {
    let response = reader
        .campaigns(
            store,
            CampaignsQuery {
                campaign_ids: vec![campaign_id],
                adv_object_type: Some("SKU"),
                state: None,
                page: 1,
                page_size: 10,
            },
        )
        .await
        .map_err(|error| format!("campaign readback failed: {error}"))?;
    let campaigns = response
        .get("list")
        .and_then(Value::as_array)
        .ok_or_else(|| "campaign readback list is invalid".to_owned())?;
    if campaigns.len() != 1 {
        return Err("campaign readback is incomplete".to_owned());
    }
    let (observed_campaign_id, title, state) = campaign_identity(&campaigns[0])?;
    if observed_campaign_id != campaign_id {
        return Err("campaign readback id mismatch".to_owned());
    }
    if title != expected_title {
        return Err("campaign readback metadata is unsupported".to_owned());
    }
    Ok((state.to_owned(), title.to_owned()))
}

fn is_supported_campaign_state(state: &str) -> bool {
    matches!(
        state,
        "CAMPAIGN_STATE_RUNNING"
            | "CAMPAIGN_STATE_STOPPED"
            | "CAMPAIGN_STATE_INACTIVE"
            | "CAMPAIGN_STATE_FINISHED"
            | "CAMPAIGN_STATE_ARCHIVED"
            | "CAMPAIGN_STATE_PLANNED"
            | "CAMPAIGN_STATE_MODERATION_DRAFT"
            | "CAMPAIGN_STATE_MODERATION_FAILED"
            | "CAMPAIGN_STATE_MODERATION_IN_PROGRESS"
    )
}

fn is_mutable_non_running_state(state: &str) -> bool {
    matches!(
        state,
        "CAMPAIGN_STATE_INACTIVE" | "CAMPAIGN_STATE_STOPPED" | "CAMPAIGN_STATE_PLANNED"
    )
}

fn campaign_identity(campaign: &Value) -> Result<(u64, &str, &str), String> {
    let campaign_id =
        positive_json_u64(campaign.get("id")).ok_or_else(|| "campaign id is invalid".to_owned())?;
    let title = campaign
        .get("title")
        .and_then(Value::as_str)
        .filter(|title| !title.is_empty())
        .ok_or_else(|| "campaign title is invalid".to_owned())?;
    let state = campaign
        .get("state")
        .and_then(Value::as_str)
        .ok_or_else(|| "campaign state is invalid".to_owned())?;
    if !is_supported_campaign_state(state) {
        return Err("campaign state is unsupported".to_owned());
    }
    Ok((campaign_id, title, state))
}

async fn exact_product(
    reader: &PerformanceClient,
    store: &StoreId,
    campaign_id: u64,
    sku: u64,
    expected_bid: u64,
) -> Result<(), String> {
    let products = reader
        .campaign_products(
            store,
            campaign_id,
            CampaignProductsQuery {
                page: 1,
                page_size: 100,
            },
        )
        .await
        .map_err(|error| format!("product readback failed: {error}"))?;
    let rows = products
        .get("products")
        .and_then(Value::as_array)
        .ok_or_else(|| "product readback list is invalid".to_owned())?;
    if rows.len() != 1
        || positive_json_u64(rows[0].get("sku")) != Some(sku)
        || positive_json_u64(rows[0].get("bid")) != Some(expected_bid)
    {
        return Err("exact SKU/bid readback is not proven".to_owned());
    }
    Ok(())
}

pub(in crate::control) fn positive_json_u64(value: Option<&Value>) -> Option<u64> {
    let value = value?;
    let parsed = if let Some(number) = value.as_u64() {
        number
    } else {
        let text = value.as_str()?;
        let number = text.parse::<u64>().ok()?;
        if number.to_string() != text {
            return None;
        }
        number
    };
    (parsed > 0).then_some(parsed)
}

#[cfg(test)]
#[allow(
    clippy::future_not_send,
    reason = "deterministic test ports use synchronous mutexes and are never spawned"
)]
#[allow(
    clippy::unused_async_trait_impl,
    reason = "test doubles implement production async traits without adding artificial suspension points"
)]
mod tests {
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
            self.state.lock().unwrap().events.push(format!(
                "complete:{}:{}:{}",
                lease.action.as_db(),
                campaign_id.unwrap_or_default(),
                readback.is_some()
            ));
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
            self.state.lock().unwrap().events.push(format!(
                "ambiguous:{}:{error_class}:{}:{}",
                lease.action.as_db(),
                campaign_id.unwrap_or_default(),
                readback.is_some()
            ));
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

        async fn readback(
            &self,
            _lease: &OzonLaunchLease,
        ) -> Result<OzonLaunchObservation, String> {
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
        assert!(matches!(
            classify_create_preflight_error("campaign title already exists".to_owned()),
            OzonFinalPermitError::Conflict("ozon_create_precondition_conflict")
        ));
        assert!(matches!(
            classify_create_preflight_error("network".to_owned()),
            OzonFinalPermitError::Transient(error) if error == "network"
        ));
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
}
