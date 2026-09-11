#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, File},
    future::Future,
    io::Read as _,
    os::unix::fs::MetadataExt as _,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::{
    config::{AccessRegistry, RegistrySource, StoreId, credential_sha256},
    control::{
        ControlAppConfig, ControlMode, ControlPolicy, MAX_OZON_STATIC_GUARD_FILE_BYTES,
        OzonAdsWriteClient, OzonBidPacingAction, OzonBidPacingObservation, OzonBidPacingPolicy,
        OzonBidPositionReader, OzonCampaignGuard, OzonCampaignProduct, OzonCampaignProductsRequest,
        OzonCampaignStrategy, OzonGuardMetricRow, OzonGuardStopReason, OzonGuardedWriteError,
        OzonPlanRepository, OzonStaticCampaignGuard, OzonStaticCampaignMutationKind,
        OzonStaticDynamicBidControl, OzonStaticGuardConfig, OzonStaticGuardFirstStep,
        OzonStaticGuardIncident, OzonStaticGuardState as StaticGuardState,
        OzonStaticGuardStateLease, OzonStaticPendingBidChange as PendingStaticBidChange,
        OzonStaticPendingCampaignMutation as PendingStaticCampaignMutation,
        aggregate_complete_guard_metrics, evaluate_ozon_bid_pacing, evaluate_ozon_campaign_guard,
        group_static_guard_metric_windows, load_ozon_static_guard_state as load_static_state,
        parse_complete_running_campaigns, parse_ozon_campaign_product,
        parse_ozon_static_guard_config, persist_ozon_static_guard_state as persist_static_state,
        plan_static_guard_first_step, validate_ozon_campaign_product_guard,
        validate_ozon_static_guard_policy, validate_ozon_static_guard_state_scope,
    },
    ozon_performance::{
        CampaignProductsQuery, CampaignsQuery, PerformanceClient, PerformanceRequestPacer,
        StatisticsQuery,
    },
    reporting::{business_date, ozon_adapter::parse_performance_daily_campaigns},
};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use sha2::{Digest as _, Sha256};
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use super::{
    OzonExecutorLease, OzonStaticGuardMutation, OzonStaticGuardWriteIntent,
    guard_workflow::{
        NoOzonGuardFailpoints, OzonGuardClock, OzonGuardEvidence, OzonGuardMetrics,
        OzonGuardReadFailure, OzonGuardReaderPort, OzonGuardRepositoryPort, OzonGuardRunContext,
        OzonGuardWriteFailure, OzonGuardWriterPort, TokioOzonGuardClock,
        run_durable_ozon_guard_cycle,
    },
    launch_workflow::{
        NoOzonLaunchFailpoints, PerformanceOzonLaunchIo, drain_ozon_launch_workflow_batch,
    },
    model::{OzonGuardStopLease, OzonGuardStopReadback, OzonPlanStoreError},
};

mod static_cycle;
use static_cycle::guard_once_static;

const LAUNCH_POLL_INTERVAL: Duration = Duration::from_secs(5);
const GUARD_POLL_INTERVAL: Duration = Duration::from_secs(60);
const MAX_CONSECUTIVE_WORKFLOW_FAILURES: usize = 3;
const STATIC_PENDING_HEALTH_GRACE: chrono::Duration = chrono::Duration::seconds(180);
/// The shared Client-Id pacer excludes read starts across the final write
/// permit and provider response. This additional boundary preserves the
/// reviewed spacing before and after each marketplace mutation.
const WRITE_BOUNDARY_INTERVAL: Duration = Duration::from_secs(2);
const STATIC_GUARDS_FILE_ENV: &str = "CONTROL_MCP_OZON_STATIC_GUARDS_FILE";
const STATIC_STATE_FILE_ENV: &str = "CONTROL_MCP_OZON_STATIC_GUARD_STATE_FILE";
const POSITION_DATABASE_URL_ENV: &str = "CONTROL_MCP_OZON_POSITION_DATABASE_URL";
const RECONCILE_COMMAND: &str = "reconcile-static-once";
const RECONCILE_CONFIRMATION: &str = "--confirm-static-bid-corridor-and-activation";
const INITIALIZE_STATIC_STATE_COMMAND: &str = "initialize-static-state";
const INITIALIZE_STATIC_STATE_CONFIRMATION: &str = "--confirm-static-state-baseline";
const AUDIT_COMMAND: &str = "audit-static-once";
const HEALTHCHECK_COMMAND: &str = "healthcheck";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Command {
    Serve,
    AuditStaticOnce,
    InitializeStaticState,
    ReconcileStaticOnce,
    Healthcheck,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OzonWorkflowLoopFailure {
    Launch,
    Guard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaticAuditContinuity {
    Matched,
    InitializeState,
    ReadOnlyAudit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum OzonStaticMutationFailpoint {
    BeforeMarker,
    AfterMarker,
    AfterWrite,
}

trait OzonStaticMutationFailpoints {
    fn is_enabled(&self, point: OzonStaticMutationFailpoint) -> bool;
}

#[derive(Debug, Default, Clone, Copy)]
struct NoOzonStaticMutationFailpoints;

impl OzonStaticMutationFailpoints for NoOzonStaticMutationFailpoints {
    fn is_enabled(&self, _point: OzonStaticMutationFailpoint) -> bool {
        false
    }
}

#[derive(Clone, Copy)]
struct StaticGuardWriteAuthorization<'a> {
    repository: &'a OzonPlanRepository,
    policy: &'a ControlPolicy,
    registry: &'a RegistrySource,
    config_path: &'a Path,
    runtime_account_id: &'a str,
    store: &'a StoreId,
    executor_fingerprint: &'a str,
    worker_id: &'a str,
    config_digest: &'a str,
}

impl StaticGuardWriteAuthorization<'_> {
    async fn persist_marker<P, MarkerFuture>(
        self,
        static_guard: &OzonStaticCampaignGuard,
        mutation: OzonStaticGuardMutation,
        target_bid_microrubles: Option<u64>,
        expected_prior_event_id: Option<u64>,
        marker: P,
    ) -> Result<(), String>
    where
        P: FnOnce(u64) -> MarkerFuture,
        MarkerFuture: Future<Output = Result<(), String>>,
    {
        let registry = self
            .registry
            .load_async()
            .await
            .map_err(|error| format!("registry reload failed: {error}"))?;
        validate_static_runtime_registry(
            &registry,
            self.runtime_account_id,
            self.store,
            self.executor_fingerprint,
        )?;
        let config_path = self.config_path.to_path_buf();
        let runtime_account_id = self.runtime_account_id.to_owned();
        let (current_config, current_digest) = tokio::task::spawn_blocking(move || {
            load_static_guards(&config_path, &runtime_account_id)
        })
        .await
        .map_err(|_| "static authorization reload task failed".to_owned())?
        .map_err(|error| format!("static authorization reload failed: {error}"))?;
        validate_reloaded_static_guard(
            &current_config,
            &current_digest,
            self.config_digest,
            self.policy,
            static_guard,
            mutation,
            target_bid_microrubles,
        )?;
        let intent = OzonStaticGuardWriteIntent {
            account_id: static_guard.guard.account_id.clone(),
            sku: static_guard.guard.sku,
            campaign_id: static_guard.guard.campaign_id,
            mutation,
            target_bid_microrubles,
            config_digest: self.config_digest.to_owned(),
        };
        self.repository
            .authorize_static_guard_write(
                self.policy.version,
                self.policy.revision,
                self.policy.digest(),
                &intent,
                self.worker_id,
                expected_prior_event_id,
                |event_id| async move {
                    marker(event_id)
                        .await
                        .map_err(|_| OzonPlanStoreError::Unavailable)
                },
            )
            .await
            .map_err(|error| error.to_string())
    }
}

fn validate_reloaded_static_guard(
    current_config: &OzonStaticGuardConfig,
    current_digest: &str,
    startup_digest: &str,
    policy: &ControlPolicy,
    static_guard: &OzonStaticCampaignGuard,
    mutation: OzonStaticGuardMutation,
    target_bid_microrubles: Option<u64>,
) -> Result<(), String> {
    validate_ozon_static_guard_policy(current_config, policy)
        .map_err(|error| format!("static guard policy changed: {error}"))?;
    if current_digest != startup_digest
        || !current_config
            .guards
            .iter()
            .any(|candidate| candidate == static_guard)
    {
        return Err("static guard config changed after startup".to_owned());
    }
    match (mutation, target_bid_microrubles) {
        (OzonStaticGuardMutation::SetBid, Some(target_bid))
            if (static_guard.min_cpc_bid_microrubles..=static_guard.max_cpc_bid_microrubles)
                .contains(&target_bid) =>
        {
            Ok(())
        }
        (OzonStaticGuardMutation::Activate | OzonStaticGuardMutation::Deactivate, None) => Ok(()),
        _ => Err("static guard mutation is outside the reviewed corridor".to_owned()),
    }
}

fn hit_static_mutation_failpoint<F: OzonStaticMutationFailpoints>(
    failpoints: &F,
    point: OzonStaticMutationFailpoint,
) -> Result<()> {
    if failpoints.is_enabled(point) {
        bail!("static Ozon mutation failpoint reached: {point:?}");
    }
    Ok(())
}

fn parse_command(arguments: &[String]) -> Result<Command> {
    match arguments {
        [] => Ok(Command::Serve),
        [command] if command == AUDIT_COMMAND => Ok(Command::AuditStaticOnce),
        [command] if command == HEALTHCHECK_COMMAND => Ok(Command::Healthcheck),
        [command, confirmation]
            if command == INITIALIZE_STATIC_STATE_COMMAND
                && confirmation == INITIALIZE_STATIC_STATE_CONFIRMATION =>
        {
            Ok(Command::InitializeStaticState)
        }
        [command, confirmation]
            if command == RECONCILE_COMMAND && confirmation == RECONCILE_CONFIRMATION =>
        {
            Ok(Command::ReconcileStaticOnce)
        }
        _ => bail!(
            "usage: ozon-campaign-guard [{AUDIT_COMMAND}|{INITIALIZE_STATIC_STATE_COMMAND} {INITIALIZE_STATIC_STATE_CONFIRMATION}|{RECONCILE_COMMAND} {RECONCILE_CONFIRMATION}|{HEALTHCHECK_COMMAND}]"
        ),
    }
}

/// Runs the Ozon campaign guard process after the binary has handled its
/// side-effect-free release identity probe.
pub async fn run_ozon_campaign_guard(arguments: &[String]) -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mcp_ozon::control::ozon=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    let command = parse_command(arguments)?;

    let config = ControlAppConfig::from_ozon_executor_env()?;
    // The executor does not build an HTTP authenticator, but it still reloads
    // the shared registry at final-write boundaries. Pin those reloads to the
    // same immutable OIDC-subject contract as the JWT ingress before any
    // database lease or marketplace client is constructed.
    config.registry.require_jwt_oidc_bindings()?;
    if config.policy.mode != ControlMode::Enabled {
        bail!("Ozon campaign guard требует enabled policy");
    }
    let runtime = config
        .ozon_runtime
        .context("Ozon campaign guard требует Ozon runtime")?;
    if !runtime.writer_enabled {
        bail!("Ozon campaign guard требует armed writer");
    }
    let marketplace = runtime
        .marketplace
        .as_ref()
        .context("Ozon campaign guard требует executor marketplace identity")?;
    let executor_fingerprint = credential_sha256(&marketplace.credentials.client_id);
    if command == Command::Healthcheck {
        verify_executor_health(
            &runtime.database,
            &runtime.account_id,
            &config.policy,
            &executor_fingerprint,
        )
        .await?;
        return Ok(());
    }
    let executor_lease = OzonExecutorLease::acquire(&runtime.database, &executor_fingerprint)
        .await
        .context("Ozon campaign guard не получил exclusive executor lease")?;
    tracing::info!(account_id=%runtime.account_id, "exclusive Ozon executor identity lease acquired");
    let plans = Arc::new(OzonPlanRepository::connect(&runtime.database).await?);
    plans.verify_runtime_contract().await?;
    let worker_id = format!("ozon-guard-{}", std::process::id());
    let credentials = BTreeMap::from([(
        marketplace.store_id.clone(),
        marketplace.credentials.clone(),
    )]);
    let performance_pacer = PerformanceRequestPacer::new();
    let reader = Arc::new(PerformanceClient::new_with_https_proxy_and_pacer(
        marketplace.request_timeout,
        credentials,
        &marketplace.proxy_url,
        &performance_pacer,
    )?);
    let writer = Arc::new(OzonAdsWriteClient::new_with_pacer(
        marketplace.request_timeout,
        marketplace.credentials.clone(),
        &marketplace.proxy_url,
        performance_pacer,
    )?);

    if let Some(static_guards_path) = env::var_os(STATIC_GUARDS_FILE_ENV) {
        let state_path = env::var_os(STATIC_STATE_FILE_ENV)
            .map(PathBuf::from)
            .context("static Ozon guard требует state file")?;
        let _state_lease = OzonStaticGuardStateLease::acquire(&state_path)?;
        let (static_guard_config, static_guard_config_digest) =
            load_static_guards(Path::new(&static_guards_path), &runtime.account_id)?;
        let write_authorization = StaticGuardWriteAuthorization {
            repository: plans.as_ref(),
            policy: &config.policy,
            registry: &config.registry,
            config_path: Path::new(&static_guards_path),
            runtime_account_id: &runtime.account_id,
            store: &marketplace.store_id,
            executor_fingerprint: &executor_fingerprint,
            worker_id: &worker_id,
            config_digest: &static_guard_config_digest,
        };
        validate_ozon_static_guard_policy(&static_guard_config, &config.policy)?;
        let static_guards = static_guard_config.guards;
        let dynamic_bid_control = static_guard_config.dynamic_bid_control;
        let mut state = load_static_state(&state_path)?;
        let allowed_campaign_ids = static_guards
            .iter()
            .map(|guard| guard.guard.campaign_id)
            .collect::<BTreeSet<_>>();
        validate_ozon_static_guard_state_scope(&state, &allowed_campaign_ids)?;
        let latest_static_audit_event_id = plans
            .latest_static_guard_audit_event_id(&runtime.account_id)
            .await?;
        match validate_static_command_audit_continuity(
            command,
            &state,
            latest_static_audit_event_id,
        )? {
            StaticAuditContinuity::InitializeState => {
                let initialization = plans.initialize_static_guard_state(
                    config.policy.version,
                    config.policy.revision,
                    config.policy.digest(),
                    &runtime.account_id,
                    &static_guard_config_digest,
                    &worker_id,
                    state.last_static_audit_event_id,
                    |event_id| {
                        let state = &mut state;
                        async move {
                            persist_static_initialization_cursor(state, &state_path, event_id)
                                .map_err(|_| OzonPlanStoreError::Unavailable)
                        }
                    },
                );
                tokio::select! {
                    result = initialization => result?,
                    () = executor_lease.lost() => bail!("Ozon executor lease connection was lost"),
                }
                tracing::info!(
                    account_id = %runtime.account_id,
                    audit_event_id = ?state.last_static_audit_event_id,
                    "static Ozon guard state genesis recorded"
                );
                return Ok(());
            }
            StaticAuditContinuity::ReadOnlyAudit => {
                tracing::warn!(
                    local_event_id = ?state.last_static_audit_event_id,
                    database_event_id = ?latest_static_audit_event_id,
                    "static state audit watermark mismatch; running read-only audit only"
                );
                tokio::select! {
                    result = audit_static_campaigns(&static_guards, &reader, &marketplace.store_id) => result?,
                    () = executor_lease.lost() => bail!("Ozon executor lease connection was lost"),
                }
                return Ok(());
            }
            StaticAuditContinuity::Matched => {}
        }
        let position_reader = if dynamic_bid_control.is_some() {
            let database_url = env::var(POSITION_DATABASE_URL_ENV)
                .context("dynamic Ozon bid control requires position database")?;
            let reader = Arc::new(OzonBidPositionReader::connect(&database_url).await?);
            reader.verify_runtime_contract().await?;
            Some(reader)
        } else {
            None
        };
        tokio::select! {
            result = recover_pending_static_campaign_mutations(
                &mut state,
                &state_path,
                reader.as_ref(),
                writer.as_ref(),
                &marketplace.store_id,
                &static_guards,
                write_authorization,
            ) => result?,
            () = executor_lease.lost() => bail!("Ozon executor lease connection was lost"),
        }
        tokio::select! {
            result = recover_pending_static_bids(
                &mut state,
                &state_path,
                reader.as_ref(),
                &marketplace.store_id,
                &static_guards,
            ) => result?,
            () = executor_lease.lost() => bail!("Ozon executor lease connection was lost"),
        }
        tracing::info!(
            account_id=%runtime.account_id,
            guards=static_guards.len(),
            dynamic_bid_control=dynamic_bid_control.is_some(),
            "static Ozon campaign guard armed"
        );
        if command == Command::AuditStaticOnce {
            tokio::select! {
                result = audit_static_campaigns(&static_guards, &reader, &marketplace.store_id) => result?,
                () = executor_lease.lost() => bail!("Ozon executor lease connection was lost"),
            }
            return Ok(());
        }
        if command == Command::ReconcileStaticOnce {
            tokio::select! {
                result = reconcile_static_campaigns(
                    &static_guards,
                    &mut state,
                    &state_path,
                    &reader,
                    &writer,
                    &marketplace.store_id,
                    write_authorization,
                ) => result?,
                () = executor_lease.lost() => bail!("Ozon executor lease connection was lost"),
            }
            return Ok(());
        }
        let mut consecutive_cycle_failures = 0_usize;
        loop {
            let cycle = guard_once_static(
                &static_guards,
                &mut state,
                &state_path,
                &reader,
                &writer,
                &marketplace.store_id,
                write_authorization,
                dynamic_bid_control.as_ref(),
                position_reader.as_deref(),
                Utc::now(),
            );
            tokio::select! {
                result = cycle => {
                    match result {
                        Ok(()) => consecutive_cycle_failures = 0,
                        Err(error) => {
                            let failure_limit_reached = record_cycle_outcome(
                                &mut consecutive_cycle_failures,
                                false,
                            );
                            tracing::error!(
                                %error,
                                consecutive_cycle_failures,
                                failure_limit = MAX_CONSECUTIVE_WORKFLOW_FAILURES,
                                "static Ozon guard cycle failed"
                            );
                            if failure_limit_reached {
                                bail!("static Ozon guard exceeded its consecutive cycle failure limit");
                            }
                        }
                    }
                }
                () = shutdown_signal() => break,
                () = executor_lease.lost() => {
                    bail!("Ozon executor lease connection was lost");
                }
            }
            tokio::select! {
                () = tokio::time::sleep(GUARD_POLL_INTERVAL) => {}
                () = shutdown_signal() => break,
                () = executor_lease.lost() => {
                    bail!("Ozon executor lease connection was lost");
                }
            }
        }
        return Ok(());
    }

    if command != Command::Serve {
        bail!("static reconcile command requires static guard config");
    }

    let durable_reader = PerformanceGuardReader {
        client: reader.as_ref(),
        store: &marketplace.store_id,
    };
    let durable_writer = PerformanceGuardWriter {
        client: writer.as_ref(),
        repository: plans.as_ref(),
    };
    let clock = TokioOzonGuardClock;
    let failpoints = NoOzonGuardFailpoints;
    let launch_io = PerformanceOzonLaunchIo::new(
        Arc::clone(&plans),
        Arc::clone(&reader),
        Arc::clone(&writer),
        config.registry.clone(),
        Arc::new(config.policy.clone()),
        runtime.account_id.clone(),
        marketplace.store_id.clone(),
    );
    let launch_failpoints = NoOzonLaunchFailpoints;
    let tasks = DurableOzonWorkflowTasks {
        repository: plans.as_ref(),
        launch_io: &launch_io,
        launch_failpoints: &launch_failpoints,
        guard_reader: &durable_reader,
        guard_writer: &durable_writer,
        guard_clock: &clock,
        guard_failpoints: &failpoints,
        account_id: &runtime.account_id,
        worker_id: &worker_id,
    };
    tracing::info!(
        account_id=%runtime.account_id,
        %worker_id,
        launch_poll_seconds=LAUNCH_POLL_INTERVAL.as_secs(),
        guard_poll_seconds=GUARD_POLL_INTERVAL.as_secs(),
        "independent durable Ozon launch and guard workflows started"
    );
    tokio::select! {
        result = run_independent_workflow_loops(
            &tasks,
            LAUNCH_POLL_INTERVAL,
            GUARD_POLL_INTERVAL,
            shutdown_signal(),
        ) => {
            if let Err(failure) = result {
                bail!("durable Ozon {failure:?} workflow exceeded its consecutive failure limit");
            }
        }
        () = executor_lease.lost() => bail!("Ozon executor lease connection was lost"),
    }
    Ok(())
}

async fn verify_executor_health(
    database: &tokio_postgres::Config,
    account_id: &str,
    policy: &ControlPolicy,
    executor_fingerprint: &str,
) -> Result<()> {
    OzonExecutorLease::verify_held(database, executor_fingerprint)
        .await
        .context("Ozon executor identity lease is not held")?;
    let plans = OzonPlanRepository::connect(database).await?;
    plans.verify_runtime_contract().await?;
    if let Some(static_guards_path) = env::var_os(STATIC_GUARDS_FILE_ENV) {
        let state_path = env::var_os(STATIC_STATE_FILE_ENV)
            .map(PathBuf::from)
            .context("static Ozon guard healthcheck requires state file")?;
        let (static_guard_config, static_guard_config_digest) =
            load_static_guards(Path::new(&static_guards_path), account_id)?;
        validate_ozon_static_guard_policy(&static_guard_config, policy)?;
        let state = load_static_state(&state_path)?;
        let allowed_campaign_ids = static_guard_config
            .guards
            .iter()
            .map(|guard| guard.guard.campaign_id)
            .collect::<BTreeSet<_>>();
        validate_ozon_static_guard_state_scope(&state, &allowed_campaign_ids)?;
        let latest_static_audit_event_id =
            plans.latest_static_guard_audit_event_id(account_id).await?;
        validate_static_audit_continuity(&state, latest_static_audit_event_id).with_context(
            || {
                format!(
                    "static state audit continuity failed for config {static_guard_config_digest}"
                )
            },
        )?;
        validate_static_state_health(&state, Utc::now())?;
    }
    Ok(())
}

#[allow(async_fn_in_trait)]
trait OzonWorkflowTasks {
    async fn drain_launch_once(&self) -> bool;
    async fn run_guard_once(&self) -> bool;
}

struct DurableOzonWorkflowTasks<'a> {
    repository: &'a OzonPlanRepository,
    launch_io: &'a PerformanceOzonLaunchIo,
    launch_failpoints: &'a NoOzonLaunchFailpoints,
    guard_reader: &'a PerformanceGuardReader<'a>,
    guard_writer: &'a PerformanceGuardWriter<'a>,
    guard_clock: &'a TokioOzonGuardClock,
    guard_failpoints: &'a NoOzonGuardFailpoints,
    account_id: &'a str,
    worker_id: &'a str,
}

impl OzonWorkflowTasks for DurableOzonWorkflowTasks<'_> {
    async fn drain_launch_once(&self) -> bool {
        match drain_ozon_launch_workflow_batch(
            self.repository,
            self.launch_io,
            self.launch_failpoints,
            self.account_id,
            self.worker_id,
        )
        .await
        {
            Ok(outcome) if outcome.persisted_failures != 0 || outcome.saturated => {
                tracing::warn!(
                    processed = outcome.processed,
                    persisted_failures = outcome.persisted_failures,
                    saturated = outcome.saturated,
                    "durable Ozon launch batch requires continued recovery"
                );
                true
            }
            Ok(outcome) if outcome.processed != 0 => {
                tracing::info!(
                    processed = outcome.processed,
                    "durable Ozon launch batch completed"
                );
                true
            }
            Ok(outcome) => {
                tracing::debug!(?outcome, "durable Ozon launch queue is idle");
                true
            }
            Err(error) => {
                tracing::error!(%error, "durable Ozon launch drain failed");
                false
            }
        }
    }

    async fn run_guard_once(&self) -> bool {
        match run_durable_ozon_guard_cycle(
            self.repository,
            self.guard_reader,
            self.guard_writer,
            self.guard_clock,
            self.guard_failpoints,
            OzonGuardRunContext {
                account_id: self.account_id,
                worker_id: self.worker_id,
                write_boundary: WRITE_BOUNDARY_INTERVAL,
            },
        )
        .await
        {
            Ok(()) => true,
            Err(error) => {
                tracing::error!(%error, "Ozon campaign guard cycle failed");
                false
            }
        }
    }
}

async fn run_independent_workflow_loops<T, S>(
    tasks: &T,
    launch_poll_interval: Duration,
    guard_poll_interval: Duration,
    shutdown: S,
) -> Result<(), OzonWorkflowLoopFailure>
where
    T: OzonWorkflowTasks + Sync,
    S: Future<Output = ()>,
{
    tokio::select! {
        failure = poll_launch_workflow(tasks, launch_poll_interval) => Err(failure),
        failure = poll_guard_workflow(tasks, guard_poll_interval) => Err(failure),
        () = shutdown => Ok(()),
    }
}

const fn record_cycle_outcome(consecutive_failures: &mut usize, succeeded: bool) -> bool {
    if succeeded {
        *consecutive_failures = 0;
        false
    } else {
        *consecutive_failures = consecutive_failures.saturating_add(1);
        *consecutive_failures >= MAX_CONSECUTIVE_WORKFLOW_FAILURES
    }
}

async fn poll_launch_workflow<T: OzonWorkflowTasks + Sync>(
    tasks: &T,
    interval: Duration,
) -> OzonWorkflowLoopFailure {
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut consecutive_failures = 0_usize;
    loop {
        ticks.tick().await;
        let succeeded = tasks.drain_launch_once().await;
        if record_cycle_outcome(&mut consecutive_failures, succeeded) {
            return OzonWorkflowLoopFailure::Launch;
        }
    }
}

async fn poll_guard_workflow<T: OzonWorkflowTasks + Sync>(
    tasks: &T,
    interval: Duration,
) -> OzonWorkflowLoopFailure {
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut consecutive_failures = 0_usize;
    loop {
        ticks.tick().await;
        let succeeded = tasks.run_guard_once().await;
        if record_cycle_outcome(&mut consecutive_failures, succeeded) {
            return OzonWorkflowLoopFailure::Guard;
        }
    }
}

/// Reads the operator-authored guard file and validates it against the account
/// this process is bound to.
///
/// Only the file access lives here. Every rule that decides whether a campaign
/// may be acted on is `parse_ozon_static_guard_config`, in the library, so it is
/// covered by the same lint and coverage gates as the guard it authorises.
fn load_static_guards(
    path: &Path,
    expected_account_id: &str,
) -> Result<(OzonStaticGuardConfig, String)> {
    let bytes = read_bounded_regular_file(
        path,
        MAX_OZON_STATIC_GUARD_FILE_BYTES,
        "static Ozon guard config",
    )?;
    let config = parse_ozon_static_guard_config(&bytes, expected_account_id)?;
    let config_digest =
        Sha256::digest(&bytes)
            .iter()
            .fold(String::with_capacity(64), |mut output, byte| {
                use std::fmt::Write as _;
                write!(output, "{byte:02x}").expect("writing to String cannot fail");
                output
            });
    Ok((config, config_digest))
}

fn read_bounded_regular_file(path: &Path, max_bytes: usize, label: &str) -> Result<Vec<u8>> {
    let path_metadata =
        fs::symlink_metadata(path).with_context(|| format!("{label} metadata недоступна"))?;
    if !path_metadata.file_type().is_file() {
        bail!("{label} must be a regular non-symlink file");
    }
    let file = File::open(path).with_context(|| format!("{label} недоступен"))?;
    let opened_metadata = file
        .metadata()
        .with_context(|| format!("{label} metadata недоступна"))?;
    if !opened_metadata.is_file()
        || opened_metadata.dev() != path_metadata.dev()
        || opened_metadata.ino() != path_metadata.ino()
    {
        bail!("{label} changed while opening");
    }
    let byte_limit = u64::try_from(max_bytes).expect("authorization file limit fits u64");
    let mut bytes = Vec::new();
    file.take(byte_limit + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("{label} read failed"))?;
    if bytes.len() > max_bytes {
        bail!("{label} exceeds its byte limit");
    }
    Ok(bytes)
}

fn validate_static_runtime_registry(
    registry: &AccessRegistry,
    runtime_account_id: &str,
    store: &StoreId,
    executor_fingerprint: &str,
) -> Result<(), String> {
    let account = registry
        .accounts
        .iter()
        .find(|account| account.id == runtime_account_id)
        .ok_or_else(|| "static guard runtime account disappeared from registry".to_owned())?;
    let ozon = account
        .ozon
        .as_ref()
        .ok_or_else(|| "static guard runtime account is no longer Ozon-bound".to_owned())?;
    let bound_fingerprint = ozon
        .performance
        .as_ref()
        .and_then(|performance| performance.control_executor_client_id_sha256.as_deref());
    if ozon.store_id != *store || bound_fingerprint != Some(executor_fingerprint) {
        return Err("static guard registry account/store/executor binding changed".to_owned());
    }
    Ok(())
}

fn validate_static_audit_continuity(
    state: &StaticGuardState,
    latest_database_event_id: Option<u64>,
) -> Result<()> {
    if state.last_static_audit_event_id.is_none() || latest_database_event_id.is_none() {
        bail!(
            "static guard state has no PostgreSQL initialization sentinel; run the explicit confirmed initialization command after reviewing the local state"
        );
    }
    if state.last_static_audit_event_id != latest_database_event_id {
        bail!(
            "static guard state audit watermark differs from PostgreSQL; exact state restore or reviewed offline repair is required"
        );
    }
    Ok(())
}

fn validate_static_command_audit_continuity(
    command: Command,
    state: &StaticGuardState,
    latest_database_event_id: Option<u64>,
) -> Result<StaticAuditContinuity> {
    if command == Command::InitializeStaticState {
        if state.last_static_audit_event_id.is_none() && latest_database_event_id.is_none() {
            return Ok(StaticAuditContinuity::InitializeState);
        }
        bail!(
            "static guard state initialization requires both the reviewed local state and PostgreSQL audit history to have no prior sentinel"
        );
    }
    match validate_static_audit_continuity(state, latest_database_event_id) {
        Ok(()) => Ok(StaticAuditContinuity::Matched),
        Err(_) if command == Command::AuditStaticOnce => Ok(StaticAuditContinuity::ReadOnlyAudit),
        Err(error) => Err(error),
    }
}

fn validate_static_state_health(state: &StaticGuardState, now: DateTime<Utc>) -> Result<()> {
    if !state.incident_campaign_ids.is_empty() || !state.incidents.is_empty() {
        bail!("static Ozon guard has unresolved incidents");
    }
    let pending_started_at = state
        .pending_bid_changes
        .values()
        .map(|pending| pending.started_at)
        .chain(
            state
                .pending_campaign_mutations
                .values()
                .map(|pending| pending.started_at),
        );
    if pending_started_at.into_iter().any(|started_at| {
        let age = now.signed_duration_since(started_at);
        age < chrono::Duration::zero() || age > STATIC_PENDING_HEALTH_GRACE
    }) {
        bail!("static Ozon guard has stale or future-dated pending mutations");
    }
    Ok(())
}

fn advance_static_audit_watermark(
    state: &mut StaticGuardState,
    event_id: u64,
) -> Result<Option<u64>, String> {
    if event_id == 0
        || state
            .last_static_audit_event_id
            .is_some_and(|previous| event_id <= previous)
    {
        return Err("static guard audit event is not strictly monotonic".to_owned());
    }
    let previous = state.last_static_audit_event_id;
    state.last_static_audit_event_id = Some(event_id);
    Ok(previous)
}

fn persist_static_initialization_cursor(
    state: &mut StaticGuardState,
    state_path: &Path,
    event_id: u64,
) -> Result<(), String> {
    let previous_event_id = advance_static_audit_watermark(state, event_id)?;
    if let Err(error) = persist_static_state(state_path, state) {
        state.last_static_audit_event_id = previous_event_id;
        return Err(error.to_string());
    }
    Ok(())
}

async fn running_static_campaigns(
    reader: &PerformanceClient,
    store: &StoreId,
    guards: &[OzonStaticCampaignGuard],
) -> Result<BTreeSet<u64>> {
    let expected = guards
        .iter()
        .map(|guard| guard.guard.campaign_id)
        .collect::<BTreeSet<_>>();
    let response = reader
        .campaigns(
            store,
            CampaignsQuery {
                campaign_ids: expected.iter().copied().collect(),
                adv_object_type: Some("SKU"),
                state: None,
                page: 1,
                page_size: u32::try_from(guards.len()).context("too many static guards")?,
            },
        )
        .await?;
    parse_complete_running_campaigns(&response, &expected).map_err(Into::into)
}

async fn campaign_product_snapshot(
    reader: &PerformanceClient,
    store: &StoreId,
    campaign_id: u64,
) -> Result<serde_json::Value> {
    reader
        .campaign_products(
            store,
            campaign_id,
            CampaignProductsQuery {
                page: 1,
                page_size: 2,
            },
        )
        .await
        .map_err(Into::into)
}

/// Reconciles every persisted campaign-state mutation before the normal
/// running-campaign filter. The marker is an irreversible local boundary: no
/// recovery path below is allowed to call activate/deactivate again.
async fn recover_pending_static_campaign_mutations(
    state: &mut StaticGuardState,
    state_path: &Path,
    reader: &PerformanceClient,
    writer: &OzonAdsWriteClient,
    store: &StoreId,
    guards: &[OzonStaticCampaignGuard],
    write_authorization: StaticGuardWriteAuthorization<'_>,
) -> Result<()> {
    let io = PerformanceStaticCampaignIo {
        reader,
        writer,
        store,
        write_authorization,
    };
    recover_pending_static_campaign_mutations_with_io(state, state_path, &io, guards).await
}

#[allow(
    clippy::future_not_send,
    reason = "static recovery test ports are intentionally single-task"
)]
async fn recover_pending_static_campaign_mutations_with_io<I: OzonStaticCampaignIo>(
    state: &mut StaticGuardState,
    state_path: &Path,
    io: &I,
    guards: &[OzonStaticCampaignGuard],
) -> Result<()> {
    let campaign_ids = state
        .pending_campaign_mutations
        .keys()
        .copied()
        .collect::<Vec<_>>();
    for campaign_id in campaign_ids {
        let static_guard = guards
            .iter()
            .find(|candidate| candidate.guard.campaign_id == campaign_id)
            .context("pending static campaign mutation is outside configured scope")?;
        let pending = state
            .pending_campaign_mutations
            .get(&campaign_id)
            .cloned()
            .context("pending static campaign mutation disappeared during recovery")?;
        if !pending_campaign_mutation_matches_guard(&pending, static_guard) {
            record_static_guard_incident(
                state,
                state_path,
                static_guard,
                pending.stop_reason.as_deref(),
                "pending_mutation_config_mismatch",
                pending.spend_minor,
                pending.revenue_minor,
                Utc::now(),
            )?;
            continue;
        }

        match io.campaign_is_running(campaign_id).await {
            Ok(is_running)
                if is_running
                    == matches!(pending.kind, OzonStaticCampaignMutationKind::Activate) =>
            {
                state.pending_campaign_mutations.remove(&campaign_id);
                persist_static_state(state_path, state)?;
                tracing::info!(
                    campaign_id,
                    mutation = ?pending.kind,
                    "pending static campaign mutation confirmed by readback"
                );
            }
            Ok(_) => {
                let error_class = match pending.kind {
                    OzonStaticCampaignMutationKind::Deactivate => {
                        "pending_deactivate_readback_mismatch"
                    }
                    OzonStaticCampaignMutationKind::Activate => {
                        "pending_activate_readback_mismatch"
                    }
                };
                record_static_guard_incident(
                    state,
                    state_path,
                    static_guard,
                    pending.stop_reason.as_deref(),
                    error_class,
                    pending.spend_minor,
                    pending.revenue_minor,
                    Utc::now(),
                )?;
            }
            Err(error) => {
                record_static_guard_incident(
                    state,
                    state_path,
                    static_guard,
                    pending.stop_reason.as_deref(),
                    "pending_campaign_readback_unavailable",
                    pending.spend_minor,
                    pending.revenue_minor,
                    Utc::now(),
                )?;
                tracing::error!(campaign_id, %error, "pending static campaign readback failed");
            }
        }
    }
    Ok(())
}

fn pending_campaign_mutation_matches_guard(
    pending: &PendingStaticCampaignMutation,
    static_guard: &OzonStaticCampaignGuard,
) -> bool {
    pending.account_id == static_guard.guard.account_id
        && pending.sku == static_guard.guard.sku
        && pending.min_cpc_bid_microrubles == static_guard.min_cpc_bid_microrubles
        && pending.max_cpc_bid_microrubles == static_guard.max_cpc_bid_microrubles
        && pending.date_from == static_guard.guard.date_from
        && pending.spend_cap_microrubles == static_guard.guard.spend_cap_microrubles
        && pending.target_drr_percent == static_guard.guard.target_drr_percent
}

#[allow(clippy::too_many_arguments)]
fn record_static_guard_incident(
    state: &mut StaticGuardState,
    state_path: &Path,
    static_guard: &OzonStaticCampaignGuard,
    stop_reason: Option<&str>,
    error_class: &str,
    spend_minor: Option<u64>,
    revenue_minor: Option<u64>,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let campaign_id = static_guard.guard.campaign_id;
    state.incident_campaign_ids.insert(campaign_id);
    state
        .incidents
        .entry(campaign_id)
        .or_insert_with(|| OzonStaticGuardIncident {
            account_id: Some(static_guard.guard.account_id.clone()),
            sku: Some(static_guard.guard.sku),
            min_cpc_bid_microrubles: Some(static_guard.min_cpc_bid_microrubles),
            max_cpc_bid_microrubles: Some(static_guard.max_cpc_bid_microrubles),
            date_from: Some(static_guard.guard.date_from.clone()),
            spend_cap_microrubles: Some(static_guard.guard.spend_cap_microrubles),
            target_drr_percent: Some(static_guard.guard.target_drr_percent),
            stop_reason: stop_reason.map(str::to_owned),
            error_class: error_class.to_owned(),
            spend_minor,
            revenue_minor,
            occurred_at,
        });
    persist_static_state(state_path, state).map_err(Into::into)
}

/// Reconciles every durable bid intent before campaign-state filtering. A
/// campaign that stopped or disappeared after a crash must not leave an
/// unexamined pending mutation behind.
async fn recover_pending_static_bids(
    state: &mut StaticGuardState,
    state_path: &Path,
    reader: &PerformanceClient,
    store: &StoreId,
    guards: &[OzonStaticCampaignGuard],
) -> Result<()> {
    let pending_campaign_ids = state
        .pending_bid_changes
        .keys()
        .copied()
        .collect::<Vec<_>>();
    for campaign_id in pending_campaign_ids {
        let static_guard = guards
            .iter()
            .find(|guard| guard.guard.campaign_id == campaign_id)
            .context("pending static bid is outside configured scope")?;
        if !pending_static_bid_matches_guard(state, static_guard) {
            record_static_guard_incident(
                state,
                state_path,
                static_guard,
                None,
                "pending_bid_config_mismatch",
                None,
                None,
                Utc::now(),
            )?;
            tracing::error!(
                campaign_id,
                sku = static_guard.guard.sku,
                "pending static bid no longer matches reviewed guard config; campaign locked"
            );
            continue;
        }
        match campaign_product_snapshot(reader, store, campaign_id)
            .await
            .and_then(|snapshot| exact_static_product_bid(&snapshot, static_guard.guard.sku))
        {
            Ok(current_bid_microrubles) => {
                reconcile_pending_static_bid(
                    state,
                    state_path,
                    static_guard,
                    current_bid_microrubles,
                )?;
            }
            Err(error) => {
                record_static_guard_incident(
                    state,
                    state_path,
                    static_guard,
                    None,
                    "pending_bid_readback_unavailable",
                    None,
                    None,
                    Utc::now(),
                )?;
                tracing::error!(
                    campaign_id,
                    sku = static_guard.guard.sku,
                    %error,
                    "pending static bid readback unavailable; campaign locked"
                );
            }
        }
    }
    Ok(())
}

fn reconcile_pending_static_bid(
    state: &mut StaticGuardState,
    state_path: &Path,
    static_guard: &OzonStaticCampaignGuard,
    current_bid_microrubles: u64,
) -> Result<()> {
    let campaign_id = static_guard.guard.campaign_id;
    let Some(pending) = state.pending_bid_changes.get(&campaign_id).cloned() else {
        return Ok(());
    };
    if pending_matches_static_guard(&pending, static_guard)
        && current_bid_microrubles == pending.to_microrubles
    {
        state.pending_bid_changes.remove(&campaign_id);
        state
            .last_bid_change_at
            .insert(campaign_id, pending.started_at);
        persist_static_state(state_path, state)?;
        tracing::info!(
            campaign_id,
            from_bid_microrubles = pending.from_microrubles,
            to_bid_microrubles = pending.to_microrubles,
            "pending dynamic Ozon bid change reconciled"
        );
    } else {
        record_static_guard_incident(
            state,
            state_path,
            static_guard,
            None,
            "pending_bid_readback_mismatch",
            None,
            None,
            Utc::now(),
        )?;
        tracing::error!(
            campaign_id,
            expected_bid_microrubles = pending.to_microrubles,
            current_bid_microrubles,
            "pending dynamic Ozon bid readback mismatch; campaign locked"
        );
    }
    Ok(())
}

fn pending_static_bid_matches_guard(
    state: &StaticGuardState,
    static_guard: &OzonStaticCampaignGuard,
) -> bool {
    state
        .pending_bid_changes
        .get(&static_guard.guard.campaign_id)
        .is_none_or(|pending| pending_matches_static_guard(pending, static_guard))
}

fn pending_matches_static_guard(
    pending: &PendingStaticBidChange,
    static_guard: &OzonStaticCampaignGuard,
) -> bool {
    let guard = &static_guard.guard;
    pending.account_id.as_deref() == Some(guard.account_id.as_str())
        && pending.sku == Some(guard.sku)
        && pending.min_cpc_bid_microrubles == Some(static_guard.min_cpc_bid_microrubles)
        && pending.max_cpc_bid_microrubles == Some(static_guard.max_cpc_bid_microrubles)
        && pending.date_from.as_deref() == Some(guard.date_from.as_str())
        && pending.spend_cap_microrubles == Some(guard.spend_cap_microrubles)
        && pending.target_drr_percent == Some(guard.target_drr_percent)
        && (static_guard.min_cpc_bid_microrubles..=static_guard.max_cpc_bid_microrubles)
            .contains(&pending.from_microrubles)
        && (static_guard.min_cpc_bid_microrubles..=static_guard.max_cpc_bid_microrubles)
            .contains(&pending.to_microrubles)
}

#[allow(clippy::too_many_arguments)]
async fn change_static_campaign_bid(
    state: &mut StaticGuardState,
    state_path: &Path,
    reader: &Arc<PerformanceClient>,
    writer: &Arc<OzonAdsWriteClient>,
    store: &StoreId,
    write_authorization: StaticGuardWriteAuthorization<'_>,
    static_guard: &OzonStaticCampaignGuard,
    from_microrubles: u64,
    to_microrubles: u64,
    started_at: DateTime<Utc>,
) -> Result<()> {
    let guard = &static_guard.guard;
    if state.pending_bid_changes.contains_key(&guard.campaign_id) {
        bail!("static campaign already has a pending bid mutation");
    }
    let pending = PendingStaticBidChange {
        account_id: Some(guard.account_id.clone()),
        sku: Some(guard.sku),
        min_cpc_bid_microrubles: Some(static_guard.min_cpc_bid_microrubles),
        max_cpc_bid_microrubles: Some(static_guard.max_cpc_bid_microrubles),
        date_from: Some(guard.date_from.clone()),
        spend_cap_microrubles: Some(guard.spend_cap_microrubles),
        target_drr_percent: Some(guard.target_drr_percent),
        from_microrubles,
        to_microrubles,
        started_at,
    };

    let request = OzonCampaignProductsRequest {
        bids: vec![OzonCampaignProduct {
            sku: guard.sku,
            bid: Some(to_microrubles),
            target_cir: None,
            top_position: None,
        }],
    };
    let expected_prior_event_id = state.last_static_audit_event_id;
    tokio::time::sleep(WRITE_BOUNDARY_INTERVAL).await;
    let write = writer
        .update_products_with_permit(
            guard.campaign_id,
            OzonCampaignStrategy::TargetBids,
            &request,
            || async {
                write_authorization
                    .persist_marker(
                        static_guard,
                        OzonStaticGuardMutation::SetBid,
                        Some(to_microrubles),
                        expected_prior_event_id,
                        |event_id| {
                            let state = &mut *state;
                            async move {
                                let previous_event_id =
                                    advance_static_audit_watermark(state, event_id)?;
                                state.pending_bid_changes.insert(guard.campaign_id, pending);
                                if let Err(error) = persist_static_state(state_path, state) {
                                    state.pending_bid_changes.remove(&guard.campaign_id);
                                    state.last_static_audit_event_id = previous_event_id;
                                    return Err(error.to_string());
                                }
                                Ok(())
                            }
                        },
                    )
                    .await
            },
        )
        .await;
    if !state.pending_bid_changes.contains_key(&guard.campaign_id) {
        if let Err(error) = write {
            bail!("dynamic bid write failed before durable marker: {error}");
        }
        bail!("dynamic bid write completed without its durable marker");
    }
    tokio::time::sleep(WRITE_BOUNDARY_INTERVAL).await;
    let readback = campaign_product_snapshot(reader, store, guard.campaign_id)
        .await
        .and_then(|snapshot| exact_static_product_bid(&snapshot, guard.sku));
    if matches!(readback, Ok(actual) if actual == to_microrubles) {
        state.pending_bid_changes.remove(&guard.campaign_id);
        state
            .last_bid_change_at
            .insert(guard.campaign_id, started_at);
        persist_static_state(state_path, state)?;
        tracing::info!(
            campaign_id = guard.campaign_id,
            sku = guard.sku,
            from_bid_microrubles = from_microrubles,
            to_bid_microrubles = to_microrubles,
            write_reported_error = write.is_err(),
            "dynamic Ozon bid changed and read back"
        );
        return Ok(());
    }

    record_static_guard_incident(
        state,
        state_path,
        static_guard,
        None,
        "bid_write_unconfirmed",
        None,
        None,
        Utc::now(),
    )?;
    match write {
        Ok(()) => bail!("dynamic bid readback mismatch; campaign locked"),
        Err(error) => bail!("dynamic bid write failed and readback did not confirm it: {error}"),
    }
}

async fn validate_static_campaign_product(
    reader: &PerformanceClient,
    store: &StoreId,
    static_guard: &OzonStaticCampaignGuard,
) -> Result<u64> {
    let snapshot = campaign_product_snapshot(reader, store, static_guard.guard.campaign_id).await?;
    validate_ozon_campaign_product_guard(
        &snapshot,
        static_guard.guard.sku,
        static_guard.min_cpc_bid_microrubles,
        static_guard.max_cpc_bid_microrubles,
    )
    .map_err(Into::into)
}

async fn activate_static_campaign(
    state: &mut StaticGuardState,
    state_path: &Path,
    reader: &PerformanceClient,
    writer: &OzonAdsWriteClient,
    store: &StoreId,
    write_authorization: StaticGuardWriteAuthorization<'_>,
    static_guard: &OzonStaticCampaignGuard,
) -> Result<()> {
    let io = PerformanceStaticCampaignIo {
        reader,
        writer,
        store,
        write_authorization,
    };
    activate_static_campaign_with_io(
        state,
        state_path,
        &io,
        &TokioOzonGuardClock,
        &NoOzonStaticMutationFailpoints,
        static_guard,
        WRITE_BOUNDARY_INTERVAL,
    )
    .await
}

#[allow(
    clippy::future_not_send,
    reason = "static test ports are intentionally single-task and fully injected"
)]
async fn activate_static_campaign_with_io<I, C, F>(
    state: &mut StaticGuardState,
    state_path: &Path,
    io: &I,
    clock: &C,
    failpoints: &F,
    static_guard: &OzonStaticCampaignGuard,
    write_boundary: Duration,
) -> Result<()>
where
    I: OzonStaticCampaignIo,
    C: OzonGuardClock,
    F: OzonStaticMutationFailpoints,
{
    let guard = &static_guard.guard;
    if state
        .pending_campaign_mutations
        .contains_key(&guard.campaign_id)
    {
        bail!("static campaign already has a pending state mutation");
    }
    let pending = PendingStaticCampaignMutation {
        account_id: guard.account_id.clone(),
        sku: guard.sku,
        min_cpc_bid_microrubles: static_guard.min_cpc_bid_microrubles,
        max_cpc_bid_microrubles: static_guard.max_cpc_bid_microrubles,
        date_from: guard.date_from.clone(),
        spend_cap_microrubles: guard.spend_cap_microrubles,
        target_drr_percent: guard.target_drr_percent,
        kind: OzonStaticCampaignMutationKind::Activate,
        stop_reason: None,
        spend_minor: None,
        revenue_minor: None,
        started_at: clock.now(),
    };
    let expected_prior_event_id = state.last_static_audit_event_id;
    clock.sleep(write_boundary).await;
    let write = io
        .activate_with_final_marker(static_guard, expected_prior_event_id, |event_id| {
            let state = &mut *state;
            async move {
                hit_static_mutation_failpoint(
                    failpoints,
                    OzonStaticMutationFailpoint::BeforeMarker,
                )
                .map_err(|error| error.to_string())?;
                let previous_event_id = advance_static_audit_watermark(state, event_id)?;
                state
                    .pending_campaign_mutations
                    .insert(guard.campaign_id, pending);
                if let Err(error) = persist_static_state(state_path, state) {
                    state.pending_campaign_mutations.remove(&guard.campaign_id);
                    state.last_static_audit_event_id = previous_event_id;
                    return Err(error.to_string());
                }
                hit_static_mutation_failpoint(failpoints, OzonStaticMutationFailpoint::AfterMarker)
                    .map_err(|error| error.to_string())
            }
        })
        .await;
    if !state
        .pending_campaign_mutations
        .contains_key(&guard.campaign_id)
    {
        if let Err(error) = write {
            bail!("static activate failed before durable marker: {error}");
        }
        bail!("static activate completed without its durable marker");
    }
    if failpoints.is_enabled(OzonStaticMutationFailpoint::AfterMarker) {
        bail!("static Ozon mutation failpoint reached: AfterMarker");
    }
    hit_static_mutation_failpoint(failpoints, OzonStaticMutationFailpoint::AfterWrite)?;
    clock.sleep(write_boundary).await;
    match io.campaign_is_running(guard.campaign_id).await {
        Ok(true) => {
            state.pending_campaign_mutations.remove(&guard.campaign_id);
            persist_static_state(state_path, state)?;
            tracing::info!(
                campaign_id = guard.campaign_id,
                write_reported_error = write.is_err(),
                "static Ozon campaign activation confirmed by exact readback"
            );
        }
        Ok(false) => {
            record_static_guard_incident(
                state,
                state_path,
                static_guard,
                None,
                if write.is_err() {
                    "activate_write_failed"
                } else {
                    "activate_readback_mismatch"
                },
                None,
                None,
                clock.now(),
            )?;
            if let Err(error) = write {
                bail!(
                    "campaign {} activation failed and readback is inactive: {error}",
                    guard.campaign_id
                );
            }
            bail!(
                "campaign {} activation readback is inactive",
                guard.campaign_id
            );
        }
        Err(readback_error) => {
            record_static_guard_incident(
                state,
                state_path,
                static_guard,
                None,
                "activate_readback_unavailable",
                None,
                None,
                clock.now(),
            )?;
            if let Err(write_error) = write {
                bail!(
                    "campaign {} activation and readback are uncertain: write={write_error}; readback={readback_error}",
                    guard.campaign_id
                );
            }
            bail!(
                "campaign {} activation readback unavailable: {readback_error}",
                guard.campaign_id
            );
        }
    }
    Ok(())
}

async fn reconcile_static_campaigns(
    guards: &[OzonStaticCampaignGuard],
    state: &mut StaticGuardState,
    state_path: &Path,
    reader: &Arc<PerformanceClient>,
    writer: &Arc<OzonAdsWriteClient>,
    store: &StoreId,
    write_authorization: StaticGuardWriteAuthorization<'_>,
) -> Result<()> {
    for static_guard in guards {
        let guard = &static_guard.guard;
        let snapshot = campaign_product_snapshot(reader, store, guard.campaign_id).await?;
        let current_bid = exact_static_product_bid(&snapshot, guard.sku)?;
        let desired_bid = current_bid.clamp(
            static_guard.min_cpc_bid_microrubles,
            static_guard.max_cpc_bid_microrubles,
        );
        if desired_bid == current_bid {
            validate_ozon_campaign_product_guard(
                &snapshot,
                guard.sku,
                static_guard.min_cpc_bid_microrubles,
                static_guard.max_cpc_bid_microrubles,
            )?;
        } else {
            change_static_campaign_bid(
                state,
                state_path,
                reader,
                writer,
                store,
                write_authorization,
                static_guard,
                current_bid,
                desired_bid,
                Utc::now(),
            )
            .await?;
            let readback = validate_static_campaign_product(reader, store, static_guard).await?;
            if readback != desired_bid {
                bail!("campaign {} bid readback differs", guard.campaign_id);
            }
            tracing::info!(
                campaign_id = guard.campaign_id,
                sku = guard.sku,
                from_bid_microrubles = current_bid,
                to_bid_microrubles = desired_bid,
                "static Ozon campaign bid reconciled"
            );
        }

        if !campaign_is_running(reader, store, guard.campaign_id).await? {
            activate_static_campaign(
                state,
                state_path,
                reader,
                writer,
                store,
                write_authorization,
                static_guard,
            )
            .await?;
            tracing::info!(
                campaign_id = guard.campaign_id,
                sku = guard.sku,
                "static Ozon campaign activated"
            );
        }
        clear_reconciled_static_campaign_state(state, state_path, guard.campaign_id)?;
    }
    Ok(())
}

fn clear_reconciled_static_campaign_state(
    state: &mut StaticGuardState,
    state_path: &Path,
    campaign_id: u64,
) -> Result<()> {
    let incident_removed = state.incident_campaign_ids.remove(&campaign_id);
    let incident_evidence_removed = state.incidents.remove(&campaign_id).is_some();
    let pending_removed = state.pending_bid_changes.remove(&campaign_id).is_some();
    let campaign_mutation_removed = state
        .pending_campaign_mutations
        .remove(&campaign_id)
        .is_some();
    if incident_removed || incident_evidence_removed || pending_removed || campaign_mutation_removed
    {
        persist_static_state(state_path, state)?;
    }
    Ok(())
}

async fn audit_static_campaigns(
    guards: &[OzonStaticCampaignGuard],
    reader: &PerformanceClient,
    store: &StoreId,
) -> Result<()> {
    let running = running_static_campaigns(reader, store, guards).await?;
    for static_guard in guards {
        let guard = &static_guard.guard;
        let snapshot = campaign_product_snapshot(reader, store, guard.campaign_id).await?;
        let (actual_sku, actual_bid_microrubles) = parse_ozon_campaign_product(&snapshot)?;
        tracing::info!(
            campaign_id = guard.campaign_id,
            expected_sku = guard.sku,
            actual_sku,
            actual_bid_microrubles,
            running = running.contains(&guard.campaign_id),
            "static Ozon campaign audit"
        );
    }
    Ok(())
}

/// Reads the current bid of the one product a static guard owns.
///
/// The corridor is checked separately by `validate_ozon_campaign_product_guard`
/// once the desired bid is known. Both paths share the library parser, so this
/// process cannot canonicalise a marketplace bid differently from the guard
/// that authorises writing one.
fn exact_static_product_bid(snapshot: &serde_json::Value, expected_sku: u64) -> Result<u64> {
    let (sku, bid) = parse_ozon_campaign_product(snapshot)?;
    if sku != expected_sku {
        bail!("campaign SKU differs from static guard: expected {expected_sku}, actual {sku}");
    }
    Ok(bid)
}

async fn static_guard_metrics(
    reader: &PerformanceClient,
    store: &StoreId,
    guards: &[OzonStaticCampaignGuard],
    running: &BTreeSet<u64>,
    observed_at: DateTime<Utc>,
) -> Result<BTreeMap<u64, (u64, u64)>> {
    let date_to = business_date(observed_at).format("%Y-%m-%d").to_string();
    let date_to_value = chrono::NaiveDate::parse_from_str(&date_to, "%Y-%m-%d")
        .context("invalid guard telemetry end date")?;
    let windows = group_static_guard_metric_windows(guards, running, &date_to)?;
    let mut metrics = BTreeMap::<u64, (u64, u64)>::new();
    for (date_from, campaign_ids) in windows {
        let date_from_value = chrono::NaiveDate::parse_from_str(&date_from, "%Y-%m-%d")
            .context("invalid guard telemetry start date")?;
        let response = reader
            .daily_statistics(
                store,
                StatisticsQuery {
                    campaign_ids: campaign_ids.iter().copied().collect(),
                    date_from,
                    date_to: date_to.clone(),
                },
            )
            .await?;
        let rows = parse_performance_daily_campaigns(&response)
            .map_err(|error| anyhow::anyhow!("statistics parse failed: {error}"))?
            .into_iter()
            .map(|row| OzonGuardMetricRow {
                business_date: row.business_date,
                campaign_id: row.campaign_id,
                spend_minor: row.spend_minor,
                attributed_revenue_minor: row.attributed_revenue_minor,
            });
        for (campaign_id, aggregate) in
            aggregate_complete_guard_metrics(&campaign_ids, date_from_value, date_to_value, rows)?
        {
            metrics.insert(
                campaign_id,
                (aggregate.spend_minor, aggregate.attributed_revenue_minor),
            );
        }
    }
    Ok(metrics)
}

#[allow(clippy::too_many_arguments)]
async fn guard_campaign_static(
    state: &mut StaticGuardState,
    state_path: &Path,
    reader: &Arc<PerformanceClient>,
    writer: &Arc<OzonAdsWriteClient>,
    store: &StoreId,
    write_authorization: StaticGuardWriteAuthorization<'_>,
    static_guard: &OzonStaticCampaignGuard,
    spend_minor: Option<u64>,
    revenue_minor: Option<u64>,
    stop_reason: Option<&'static str>,
) -> Result<()> {
    let io = PerformanceStaticCampaignIo {
        reader: reader.as_ref(),
        writer: writer.as_ref(),
        store,
        write_authorization,
    };
    guard_campaign_static_with_io(
        state,
        state_path,
        &io,
        &TokioOzonGuardClock,
        &NoOzonStaticMutationFailpoints,
        static_guard,
        spend_minor,
        revenue_minor,
        stop_reason,
        WRITE_BOUNDARY_INTERVAL,
    )
    .await
}

#[allow(async_fn_in_trait)]
trait OzonStaticCampaignIo {
    async fn deactivate_with_final_marker<P, MarkerFuture>(
        &self,
        static_guard: &OzonStaticCampaignGuard,
        expected_prior_event_id: Option<u64>,
        marker: P,
    ) -> Result<(), String>
    where
        P: FnOnce(u64) -> MarkerFuture,
        MarkerFuture: Future<Output = Result<(), String>>;

    async fn activate_with_final_marker<P, MarkerFuture>(
        &self,
        static_guard: &OzonStaticCampaignGuard,
        expected_prior_event_id: Option<u64>,
        marker: P,
    ) -> Result<(), String>
    where
        P: FnOnce(u64) -> MarkerFuture,
        MarkerFuture: Future<Output = Result<(), String>>;

    async fn campaign_is_running(&self, campaign_id: u64) -> Result<bool, String>;
}

struct PerformanceStaticCampaignIo<'a> {
    reader: &'a PerformanceClient,
    writer: &'a OzonAdsWriteClient,
    store: &'a StoreId,
    write_authorization: StaticGuardWriteAuthorization<'a>,
}

impl OzonStaticCampaignIo for PerformanceStaticCampaignIo<'_> {
    async fn deactivate_with_final_marker<P, MarkerFuture>(
        &self,
        static_guard: &OzonStaticCampaignGuard,
        expected_prior_event_id: Option<u64>,
        marker: P,
    ) -> Result<(), String>
    where
        P: FnOnce(u64) -> MarkerFuture,
        MarkerFuture: Future<Output = Result<(), String>>,
    {
        self.writer
            .deactivate_campaign_with_permit(static_guard.guard.campaign_id, || async {
                self.write_authorization
                    .persist_marker(
                        static_guard,
                        OzonStaticGuardMutation::Deactivate,
                        None,
                        expected_prior_event_id,
                        marker,
                    )
                    .await
            })
            .await
            .map_err(|error| error.to_string())
    }

    async fn activate_with_final_marker<P, MarkerFuture>(
        &self,
        static_guard: &OzonStaticCampaignGuard,
        expected_prior_event_id: Option<u64>,
        marker: P,
    ) -> Result<(), String>
    where
        P: FnOnce(u64) -> MarkerFuture,
        MarkerFuture: Future<Output = Result<(), String>>,
    {
        self.writer
            .activate_campaign_with_permit(static_guard.guard.campaign_id, || async {
                self.write_authorization
                    .persist_marker(
                        static_guard,
                        OzonStaticGuardMutation::Activate,
                        None,
                        expected_prior_event_id,
                        marker,
                    )
                    .await
            })
            .await
            .map_err(|error| error.to_string())
    }

    async fn campaign_is_running(&self, campaign_id: u64) -> Result<bool, String> {
        campaign_is_running(self.reader, self.store, campaign_id)
            .await
            .map_err(|error| error.to_string())
    }
}

#[allow(
    clippy::future_not_send,
    clippy::too_many_arguments,
    reason = "static test ports are intentionally single-task and fully injected"
)]
async fn guard_campaign_static_with_io<I, C, F>(
    state: &mut StaticGuardState,
    state_path: &Path,
    io: &I,
    clock: &C,
    failpoints: &F,
    static_guard: &OzonStaticCampaignGuard,
    spend_minor: Option<u64>,
    revenue_minor: Option<u64>,
    stop_reason: Option<&'static str>,
    write_boundary: Duration,
) -> Result<()>
where
    I: OzonStaticCampaignIo,
    C: OzonGuardClock,
    F: OzonStaticMutationFailpoints,
{
    let guard = &static_guard.guard;
    let Some(stop_reason) = stop_reason else {
        tracing::info!(
            campaign_id = guard.campaign_id,
            sku = guard.sku,
            ?spend_minor,
            ?revenue_minor,
            "static Ozon guard observation"
        );
        return Ok(());
    };
    if state
        .pending_campaign_mutations
        .contains_key(&guard.campaign_id)
    {
        bail!("static campaign already has a pending state mutation");
    }
    let pending = PendingStaticCampaignMutation {
        account_id: guard.account_id.clone(),
        sku: guard.sku,
        min_cpc_bid_microrubles: static_guard.min_cpc_bid_microrubles,
        max_cpc_bid_microrubles: static_guard.max_cpc_bid_microrubles,
        date_from: guard.date_from.clone(),
        spend_cap_microrubles: guard.spend_cap_microrubles,
        target_drr_percent: guard.target_drr_percent,
        kind: OzonStaticCampaignMutationKind::Deactivate,
        stop_reason: Some(stop_reason.to_owned()),
        spend_minor,
        revenue_minor,
        started_at: clock.now(),
    };
    let expected_prior_event_id = state.last_static_audit_event_id;
    clock.sleep(write_boundary).await;
    let write = io
        .deactivate_with_final_marker(static_guard, expected_prior_event_id, |event_id| {
            let state = &mut *state;
            async move {
                hit_static_mutation_failpoint(
                    failpoints,
                    OzonStaticMutationFailpoint::BeforeMarker,
                )
                .map_err(|error| error.to_string())?;
                let previous_event_id = advance_static_audit_watermark(state, event_id)?;
                state
                    .pending_campaign_mutations
                    .insert(guard.campaign_id, pending);
                if let Err(error) = persist_static_state(state_path, state) {
                    state.pending_campaign_mutations.remove(&guard.campaign_id);
                    state.last_static_audit_event_id = previous_event_id;
                    return Err(error.to_string());
                }
                hit_static_mutation_failpoint(failpoints, OzonStaticMutationFailpoint::AfterMarker)
                    .map_err(|error| error.to_string())
            }
        })
        .await;
    if !state
        .pending_campaign_mutations
        .contains_key(&guard.campaign_id)
    {
        if let Err(error) = write {
            bail!("static deactivate failed before durable marker: {error}");
        }
        bail!("static deactivate completed without its durable marker");
    }
    if failpoints.is_enabled(OzonStaticMutationFailpoint::AfterMarker) {
        bail!("static Ozon mutation failpoint reached: AfterMarker");
    }
    hit_static_mutation_failpoint(failpoints, OzonStaticMutationFailpoint::AfterWrite)?;
    clock.sleep(write_boundary).await;
    match io.campaign_is_running(guard.campaign_id).await {
        Ok(false) => {
            state.pending_campaign_mutations.remove(&guard.campaign_id);
            persist_static_state(state_path, state)?;
            tracing::info!(
                campaign_id = guard.campaign_id,
                write_reported_error = write.is_err(),
                "static Ozon campaign stop confirmed by exact readback"
            );
        }
        Ok(true) => {
            record_static_guard_incident(
                state,
                state_path,
                static_guard,
                Some(stop_reason),
                if write.is_err() {
                    "deactivate_write_failed"
                } else {
                    "deactivate_readback_mismatch"
                },
                spend_minor,
                revenue_minor,
                clock.now(),
            )?;
            if let Err(error) = write {
                bail!("static deactivate failed and readback still reports running: {error}");
            }
            bail!("static deactivate readback still reports running");
        }
        Err(readback_error) => {
            record_static_guard_incident(
                state,
                state_path,
                static_guard,
                Some(stop_reason),
                "deactivate_readback_unavailable",
                spend_minor,
                revenue_minor,
                clock.now(),
            )?;
            if let Err(write_error) = write {
                bail!(
                    "static deactivate write and readback are uncertain: write={write_error}; readback={readback_error}"
                );
            }
            bail!("static deactivate readback unavailable: {readback_error}");
        }
    }
    tracing::info!(
        campaign_id = guard.campaign_id,
        sku = guard.sku,
        stop_reason,
        "static Ozon campaign stopped"
    );
    Ok(())
}

impl OzonGuardRepositoryPort for OzonPlanRepository {
    async fn active_guards(
        &self,
        account_id: &str,
    ) -> Result<Vec<OzonCampaignGuard>, OzonPlanStoreError> {
        self.active_guards_for_account(account_id).await
    }

    async fn claim_stop_recovery(
        &self,
        account_id: &str,
        worker_id: &str,
    ) -> Result<Option<OzonGuardStopLease>, OzonPlanStoreError> {
        self.claim_guard_stop_recovery(account_id, worker_id).await
    }

    async fn claim_stop(
        &self,
        guard: &OzonCampaignGuard,
        reason: &str,
        evidence: OzonGuardEvidence,
        worker_id: &str,
    ) -> Result<OzonGuardStopLease, OzonPlanStoreError> {
        self.claim_guard_stop_leased(
            guard,
            reason,
            evidence.map(|metrics| metrics.spend_minor),
            evidence.map(|metrics| metrics.attributed_revenue_minor),
            worker_id,
        )
        .await
    }

    async fn record_observation(
        &self,
        guard: &OzonCampaignGuard,
        metrics: OzonGuardMetrics,
    ) -> Result<(), OzonPlanStoreError> {
        self.record_guard_observation(guard, metrics.spend_minor, metrics.attributed_revenue_minor)
            .await
    }

    async fn finish_stop(
        &self,
        lease: &OzonGuardStopLease,
        evidence: OzonGuardEvidence,
    ) -> Result<(), OzonPlanStoreError> {
        self.finish_guard_leased(
            lease,
            evidence.map(|metrics| metrics.spend_minor),
            evidence.map(|metrics| metrics.attributed_revenue_minor),
        )
        .await
    }

    async fn mark_incident(
        &self,
        lease: &OzonGuardStopLease,
        error_class: &str,
        evidence: OzonGuardEvidence,
    ) -> Result<(), OzonPlanStoreError> {
        self.mark_guard_incident_leased(
            lease,
            error_class,
            evidence.map(|metrics| metrics.spend_minor),
            evidence.map(|metrics| metrics.attributed_revenue_minor),
        )
        .await
    }

    async fn record_readback(
        &self,
        lease: &OzonGuardStopLease,
        observation: OzonGuardStopReadback,
    ) -> Result<(), OzonPlanStoreError> {
        self.record_guard_stop_readback(lease, observation).await
    }
}

struct PerformanceGuardReader<'a> {
    client: &'a PerformanceClient,
    store: &'a crate::config::StoreId,
}

impl OzonGuardReaderPort for PerformanceGuardReader<'_> {
    async fn metrics(
        &self,
        guard: &OzonCampaignGuard,
        observed_at: DateTime<Utc>,
    ) -> Result<OzonGuardMetrics, OzonGuardReadFailure> {
        let (spend_minor, attributed_revenue_minor, _) =
            evaluate_live_guard(self.client, self.store, guard, observed_at)
                .await
                .map_err(|_| OzonGuardReadFailure::Telemetry)?;
        Ok(OzonGuardMetrics {
            spend_minor,
            attributed_revenue_minor,
        })
    }

    async fn campaign_is_running(&self, campaign_id: u64) -> Result<bool, OzonGuardReadFailure> {
        campaign_is_running(self.client, self.store, campaign_id)
            .await
            .map_err(|_| OzonGuardReadFailure::CampaignState)
    }
}

struct PerformanceGuardWriter<'a> {
    client: &'a OzonAdsWriteClient,
    repository: &'a OzonPlanRepository,
}

const fn classify_guard_stop_write_failure(
    error: &OzonGuardedWriteError<OzonPlanStoreError>,
    marker_attempted: bool,
    write_started: bool,
) -> OzonGuardWriteFailure {
    match error {
        OzonGuardedWriteError::Permit(_) => {
            if marker_attempted {
                OzonGuardWriteFailure::MarkerUncertain
            } else {
                OzonGuardWriteFailure::Permit
            }
        }
        OzonGuardedWriteError::Write(_) if !write_started => OzonGuardWriteFailure::Permit,
        // Once the durable mutation boundary has been crossed, no provider
        // response (including a 4xx) proves that the campaign state did not
        // change. Exact readback is the only terminal evidence.
        OzonGuardedWriteError::Write(_) => OzonGuardWriteFailure::Ambiguous,
    }
}

impl OzonGuardWriterPort for PerformanceGuardWriter<'_> {
    async fn deactivate_with_final_permit(
        &self,
        lease: &OzonGuardStopLease,
    ) -> Result<(), OzonGuardWriteFailure> {
        let marker_attempted = AtomicBool::new(false);
        let write_started = AtomicBool::new(false);
        let result = self
            .client
            .deactivate_campaign_with_permit(lease.guard.campaign_id, || {
                let write_started = &write_started;
                let marker_attempted = &marker_attempted;
                async move {
                    marker_attempted.store(true, Ordering::Release);
                    self.repository.start_guard_stop_write(lease).await?;
                    write_started.store(true, Ordering::Release);
                    Ok::<(), OzonPlanStoreError>(())
                }
            })
            .await;
        result.map_err(|error| {
            classify_guard_stop_write_failure(
                &error,
                marker_attempted.load(Ordering::Acquire),
                write_started.load(Ordering::Acquire),
            )
        })
    }
}

async fn evaluate_live_guard(
    reader: &PerformanceClient,
    store: &StoreId,
    guard: &OzonCampaignGuard,
    observed_at: DateTime<Utc>,
) -> Result<(u64, u64, Option<&'static str>)> {
    let date_to = business_date(observed_at).format("%Y-%m-%d").to_string();
    let metrics_response = reader
        .daily_statistics(
            store,
            StatisticsQuery {
                campaign_ids: vec![guard.campaign_id],
                date_from: guard.date_from.clone(),
                date_to,
            },
        )
        .await
        .context("statistics request failed")?;
    let rows = parse_performance_daily_campaigns(&metrics_response)
        .map_err(|error| anyhow::anyhow!("statistics parse failed: {error}"))?
        .into_iter()
        .map(|row| OzonGuardMetricRow {
            business_date: row.business_date,
            campaign_id: row.campaign_id,
            spend_minor: row.spend_minor,
            attributed_revenue_minor: row.attributed_revenue_minor,
        });
    let expected = BTreeSet::from([guard.campaign_id]);
    let date_from = chrono::NaiveDate::parse_from_str(&guard.date_from, "%Y-%m-%d")
        .context("invalid guard telemetry start date")?;
    let date_to = business_date(observed_at);
    let metrics = aggregate_complete_guard_metrics(&expected, date_from, date_to, rows)?;
    let metrics = metrics
        .get(&guard.campaign_id)
        .context("complete guard telemetry lost the requested campaign")?;
    let spend_minor = metrics.spend_minor;
    let revenue_minor = metrics.attributed_revenue_minor;
    let stop_reason = evaluate_ozon_campaign_guard(
        spend_minor,
        revenue_minor,
        guard.spend_cap_microrubles,
        guard.target_drr_percent,
    )?
    .map(OzonGuardStopReason::as_str);
    Ok((spend_minor, revenue_minor, stop_reason))
}

async fn campaign_is_running(
    reader: &PerformanceClient,
    store: &StoreId,
    campaign_id: u64,
) -> Result<bool> {
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
        .await?;
    let expected = BTreeSet::from([campaign_id]);
    Ok(parse_complete_running_campaigns(&response, &expected)?.contains(&campaign_id))
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let ctrl_c = async {
            if signal::ctrl_c().await.is_err() {
                std::future::pending::<()>().await;
            }
        };
        let terminate = async {
            match signal::unix::signal(signal::unix::SignalKind::terminate()) {
                Ok(mut stream) => {
                    let _ = stream.recv().await;
                }
                Err(_) => std::future::pending::<()>().await,
            }
        };
        tokio::select! { () = ctrl_c => {}, () = terminate => {} }
    }
}

#[cfg(test)]
mod tests;
