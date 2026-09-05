//! Pure planning and telemetry completeness rules for Ozon guard runtimes.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde_json::Value;
use thiserror::Error;

use super::{
    guard::{OzonGuardEvaluationError, OzonGuardStopReason, evaluate_ozon_campaign_guard},
    model::{OzonCampaignGuard, OzonGuardStopLease, OzonGuardStopReadback, OzonPlanStoreError},
    static_guard::OzonStaticCampaignGuard,
};

/// Maximum number of expired stop leases recovered in one polling cycle.
pub const MAX_OZON_GUARD_RECOVERIES_PER_CYCLE: usize = 50;

/// Failure class returned by the marketplace mutation adapter.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonGuardWriteFailure {
    #[error("final stop permit was rejected")]
    Permit,
    #[error("durable stop marker acknowledgement is uncertain")]
    MarkerUncertain,
    #[error("campaign stop result is ambiguous")]
    Ambiguous,
}

/// Read-side failure. Telemetry and state readback deliberately remain
/// distinct because telemetry failure requests a stop, while an unavailable
/// readback must never authorize a second blind write.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonGuardReadFailure {
    #[error("guard telemetry is unavailable or incomplete")]
    Telemetry,
    #[error("campaign state readback is unavailable or incomplete")]
    CampaignState,
}

/// Deterministic crash boundaries exercised by workflow tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OzonGuardFailpoint {
    AfterStopClaim,
    BeforeDeactivate,
    AfterDeactivate,
}

/// A deterministic failpoint trigger.
pub trait OzonGuardFailpoints {
    fn is_enabled(&self, point: OzonGuardFailpoint) -> bool;
}

/// Production failpoint implementation.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOzonGuardFailpoints;

impl OzonGuardFailpoints for NoOzonGuardFailpoints {
    fn is_enabled(&self, _point: OzonGuardFailpoint) -> bool {
        false
    }
}

/// Clock and pacing boundary used by the durable workflow.
#[allow(async_fn_in_trait)]
pub trait OzonGuardClock {
    fn now(&self) -> DateTime<Utc>;
    async fn sleep(&self, duration: Duration);
}

/// Production UTC/Tokio clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioOzonGuardClock;

impl OzonGuardClock for TokioOzonGuardClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// Repository boundary for the durable stop workflow.
#[allow(async_fn_in_trait)]
pub trait OzonGuardRepositoryPort {
    async fn active_guards(
        &self,
        account_id: &str,
    ) -> Result<Vec<OzonCampaignGuard>, OzonPlanStoreError>;

    async fn claim_stop_recovery(
        &self,
        account_id: &str,
        worker_id: &str,
    ) -> Result<Option<OzonGuardStopLease>, OzonPlanStoreError>;

    async fn claim_stop(
        &self,
        guard: &OzonCampaignGuard,
        reason: &str,
        evidence: OzonGuardEvidence,
        worker_id: &str,
    ) -> Result<OzonGuardStopLease, OzonPlanStoreError>;

    async fn record_observation(
        &self,
        guard: &OzonCampaignGuard,
        metrics: OzonGuardMetrics,
    ) -> Result<(), OzonPlanStoreError>;

    async fn finish_stop(
        &self,
        lease: &OzonGuardStopLease,
        evidence: OzonGuardEvidence,
    ) -> Result<(), OzonPlanStoreError>;

    async fn mark_incident(
        &self,
        lease: &OzonGuardStopLease,
        error_class: &str,
        evidence: OzonGuardEvidence,
    ) -> Result<(), OzonPlanStoreError>;

    async fn record_readback(
        &self,
        lease: &OzonGuardStopLease,
        observation: OzonGuardStopReadback,
    ) -> Result<(), OzonPlanStoreError>;
}

/// Read adapter boundary for complete metrics and exact campaign state.
#[allow(async_fn_in_trait)]
pub trait OzonGuardReaderPort {
    async fn metrics(
        &self,
        guard: &OzonCampaignGuard,
        observed_at: DateTime<Utc>,
    ) -> Result<OzonGuardMetrics, OzonGuardReadFailure>;

    async fn campaign_is_running(&self, campaign_id: u64) -> Result<bool, OzonGuardReadFailure>;
}

/// Write adapter boundary. The implementation atomically persists the final,
/// fenced write-start marker immediately before the HTTP mutation. A returned
/// write result other than `Permit` therefore means recovery must be
/// readback-only even when the marketplace outcome is ambiguous.
#[allow(async_fn_in_trait)]
pub trait OzonGuardWriterPort {
    async fn deactivate_with_final_permit(
        &self,
        lease: &OzonGuardStopLease,
    ) -> Result<(), OzonGuardWriteFailure>;
}

/// Durable guard orchestration failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OzonGuardWorkflowError {
    #[error("durable Ozon guard repository failed: {0}")]
    Repository(OzonPlanStoreError),
    #[error("durable Ozon guard read failed: {0}")]
    Read(OzonGuardReadFailure),
    #[error("durable Ozon guard failpoint reached: {0:?}")]
    Failpoint(OzonGuardFailpoint),
}

impl From<OzonPlanStoreError> for OzonGuardWorkflowError {
    fn from(error: OzonPlanStoreError) -> Self {
        Self::Repository(error)
    }
}

/// Runs one bounded durable guard cycle: expired `stopping` records are
/// reconciled before fresh `active` work is observed.
#[derive(Debug, Clone, Copy)]
pub(super) struct OzonGuardRunContext<'a> {
    pub account_id: &'a str,
    pub worker_id: &'a str,
    pub write_boundary: Duration,
}

struct OzonGuardWorkflowPorts<'a, R, D, W, C, F> {
    repository: &'a R,
    reader: &'a D,
    writer: &'a W,
    clock: &'a C,
    failpoints: &'a F,
}

#[allow(
    clippy::future_not_send,
    reason = "injected async ports are polled in one cancellation-aware runtime task"
)]
pub(super) async fn run_durable_ozon_guard_cycle<R, D, W, C, F>(
    repository: &R,
    reader: &D,
    writer: &W,
    clock: &C,
    failpoints: &F,
    context: OzonGuardRunContext<'_>,
) -> Result<(), OzonGuardWorkflowError>
where
    R: OzonGuardRepositoryPort,
    D: OzonGuardReaderPort,
    W: OzonGuardWriterPort,
    C: OzonGuardClock,
    F: OzonGuardFailpoints,
{
    let ports = OzonGuardWorkflowPorts {
        repository,
        reader,
        writer,
        clock,
        failpoints,
    };
    let mut recovered = 0_usize;
    let mut recovery_read_failures = 0_usize;
    for _ in 0..MAX_OZON_GUARD_RECOVERIES_PER_CYCLE {
        let Some(lease) = repository
            .claim_stop_recovery(context.account_id, context.worker_id)
            .await?
        else {
            break;
        };
        let result = reconcile_leased_stop(
            &ports,
            &lease,
            match (lease.spend_minor, lease.revenue_minor) {
                (Some(spend_minor), Some(attributed_revenue_minor)) => Some(OzonGuardMetrics {
                    spend_minor,
                    attributed_revenue_minor,
                }),
                (None, None) => None,
                _ => {
                    return Err(OzonGuardWorkflowError::Repository(
                        OzonPlanStoreError::Unavailable,
                    ));
                }
            },
            context.write_boundary,
        )
        .await;
        recovered = recovered.saturating_add(1);
        match result {
            Ok(()) => {}
            Err(OzonGuardWorkflowError::Read(error)) => {
                recovery_read_failures = recovery_read_failures.saturating_add(1);
                tracing::warn!(
                    account_id = context.account_id,
                    plan_id = lease.guard.plan_id,
                    campaign_id = lease.guard.campaign_id,
                    generation = lease.generation,
                    %error,
                    "durable Ozon recovery read failed; continuing bounded recovery drain"
                );
            }
            Err(error) => return Err(error),
        }
    }
    // Recovery gets the first bounded share of every cycle, but saturation
    // must not starve active spend-cap enforcement forever. Stopping rows are
    // excluded from `active_guards`, so the scan below cannot repeat them.
    if recovered == MAX_OZON_GUARD_RECOVERIES_PER_CYCLE {
        tracing::warn!(
            account_id = context.account_id,
            recovered,
            recovery_read_failures,
            recovery_limit = MAX_OZON_GUARD_RECOVERIES_PER_CYCLE,
            "durable Ozon guard recovery batch saturated; continuing active hard-stop scan"
        );
    }
    if recovered != 0 {
        tracing::info!(
            account_id = context.account_id,
            recovered,
            recovery_read_failures,
            "durable Ozon guard recoveries reconciled before fresh work"
        );
    }

    let observed_at = clock.now();
    let mut active_read_failures = 0_usize;
    for guard in repository.active_guards(context.account_id).await? {
        let (evidence, stop_reason) = match reader.metrics(&guard, observed_at).await {
            Ok(metrics) => {
                let reason = evaluate_ozon_campaign_guard(
                    metrics.spend_minor,
                    metrics.attributed_revenue_minor,
                    guard.spend_cap_microrubles,
                    guard.target_drr_percent,
                )
                .map_or(Some("guard_evaluation_failed"), |reason| {
                    reason.map(super::guard::OzonGuardStopReason::as_str)
                });
                (Some(metrics), reason)
            }
            Err(_) => (None, Some("telemetry_unavailable")),
        };
        let Some(stop_reason) = stop_reason else {
            let metrics = evidence.ok_or(OzonGuardWorkflowError::Read(
                OzonGuardReadFailure::Telemetry,
            ))?;
            repository.record_observation(&guard, metrics).await?;
            continue;
        };
        let lease = repository
            .claim_stop(&guard, stop_reason, evidence, context.worker_id)
            .await?;
        hit_failpoint(failpoints, OzonGuardFailpoint::AfterStopClaim)?;
        match reconcile_leased_stop(&ports, &lease, evidence, context.write_boundary).await {
            Ok(()) => {}
            Err(OzonGuardWorkflowError::Read(error)) => {
                active_read_failures = active_read_failures.saturating_add(1);
                tracing::warn!(
                    account_id = context.account_id,
                    plan_id = guard.plan_id,
                    campaign_id = guard.campaign_id,
                    %error,
                    "durable Ozon active guard read failed after stop claim; continuing active scan"
                );
            }
            Err(error) => return Err(error),
        }
    }
    if active_read_failures != 0 {
        tracing::warn!(
            account_id = context.account_id,
            active_read_failures,
            "durable Ozon guard cycle completed with isolated read failures"
        );
    }
    Ok(())
}

#[allow(
    clippy::future_not_send,
    reason = "injected async ports are polled in one cancellation-aware runtime task"
)]
async fn reconcile_leased_stop<R, D, W, C, F>(
    ports: &OzonGuardWorkflowPorts<'_, R, D, W, C, F>,
    lease: &OzonGuardStopLease,
    evidence: OzonGuardEvidence,
    write_boundary: Duration,
) -> Result<(), OzonGuardWorkflowError>
where
    R: OzonGuardRepositoryPort,
    D: OzonGuardReaderPort,
    W: OzonGuardWriterPort,
    C: OzonGuardClock,
    F: OzonGuardFailpoints,
{
    match ports
        .reader
        .campaign_is_running(lease.guard.campaign_id)
        .await
    {
        Ok(false) => {
            ports
                .repository
                .record_readback(lease, OzonGuardStopReadback::Stopped)
                .await?;
            ports.repository.finish_stop(lease, evidence).await?;
            tracing::info!(
                account_id = lease.guard.account_id,
                plan_id = lease.guard.plan_id,
                campaign_id = lease.guard.campaign_id,
                generation = lease.generation,
                stop_reason = lease.stop_reason,
                ?evidence,
                "durable Ozon campaign stop confirmed by pre-write readback"
            );
            return Ok(());
        }
        Ok(true) if lease.write_started_at.is_some() => {
            ports
                .repository
                .record_readback(lease, OzonGuardStopReadback::Running)
                .await?;
            ports
                .repository
                .mark_incident(lease, "stop_write_unconfirmed", evidence)
                .await?;
            tracing::warn!(
                account_id = lease.guard.account_id,
                plan_id = lease.guard.plan_id,
                campaign_id = lease.guard.campaign_id,
                generation = lease.generation,
                stop_reason = lease.stop_reason,
                incident_error_class = "stop_write_unconfirmed",
                ?evidence,
                "durable Ozon stop marker read back as running; write remains unconfirmed and was not repeated"
            );
            return Ok(());
        }
        Ok(true) => {
            ports
                .repository
                .record_readback(lease, OzonGuardStopReadback::Running)
                .await?;
        }
        Err(error) => {
            ports
                .repository
                .record_readback(lease, OzonGuardStopReadback::Unavailable)
                .await?;
            // Keep `stopping` intact. Once a marker exists, only a future
            // exact readback may resolve it; before the marker, a later owner
            // may safely retry from the same read-before-write boundary.
            tracing::warn!(
                account_id = lease.guard.account_id,
                plan_id = lease.guard.plan_id,
                campaign_id = lease.guard.campaign_id,
                generation = lease.generation,
                write_started = lease.write_started_at.is_some(),
                stop_reason = lease.stop_reason,
                "durable Ozon stop readback unavailable; stopping intent retained"
            );
            return Err(OzonGuardWorkflowError::Read(error));
        }
    }

    hit_failpoint(ports.failpoints, OzonGuardFailpoint::BeforeDeactivate)?;
    ports.clock.sleep(write_boundary).await;
    let write = ports.writer.deactivate_with_final_permit(lease).await;
    ports.clock.sleep(write_boundary).await;
    hit_failpoint(ports.failpoints, OzonGuardFailpoint::AfterDeactivate)?;

    match ports
        .reader
        .campaign_is_running(lease.guard.campaign_id)
        .await
    {
        Ok(false) => {
            ports
                .repository
                .record_readback(lease, OzonGuardStopReadback::Stopped)
                .await?;
            ports.repository.finish_stop(lease, evidence).await?;
            tracing::info!(
                account_id = lease.guard.account_id,
                plan_id = lease.guard.plan_id,
                campaign_id = lease.guard.campaign_id,
                generation = lease.generation,
                stop_reason = lease.stop_reason,
                write_result = write_result_class(write),
                ?evidence,
                "durable Ozon campaign stop confirmed by persisted readback"
            );
        }
        Ok(true) => {
            ports
                .repository
                .record_readback(lease, OzonGuardStopReadback::Running)
                .await?;
            let error_class = match write {
                Ok(()) => "stop_readback_mismatch",
                // The permit failed before the durable mutation boundary was
                // established. Leave the stop recoverable instead of forging
                // a terminal incident that the schema correctly rejects.
                Err(OzonGuardWriteFailure::Permit) => {
                    tracing::warn!(
                        account_id = lease.guard.account_id,
                        plan_id = lease.guard.plan_id,
                        campaign_id = lease.guard.campaign_id,
                        generation = lease.generation,
                        stop_reason = lease.stop_reason,
                        "durable Ozon final permit failed before write marker; stopping intent retained"
                    );
                    return Err(OzonGuardWorkflowError::Repository(
                        OzonPlanStoreError::LeaseLost,
                    ));
                }
                Err(OzonGuardWriteFailure::MarkerUncertain) => {
                    tracing::warn!(
                        account_id = lease.guard.account_id,
                        plan_id = lease.guard.plan_id,
                        campaign_id = lease.guard.campaign_id,
                        generation = lease.generation,
                        stop_reason = lease.stop_reason,
                        "durable Ozon write marker acknowledgement uncertain; stopping intent retained"
                    );
                    return Err(OzonGuardWorkflowError::Repository(
                        OzonPlanStoreError::Unavailable,
                    ));
                }
                Err(OzonGuardWriteFailure::Ambiguous) => "stop_write_ambiguous",
            };
            ports
                .repository
                .mark_incident(lease, error_class, evidence)
                .await?;
            tracing::warn!(
                account_id = lease.guard.account_id,
                plan_id = lease.guard.plan_id,
                campaign_id = lease.guard.campaign_id,
                generation = lease.generation,
                stop_reason = lease.stop_reason,
                incident_error_class = error_class,
                ?evidence,
                "durable Ozon campaign stop could not be confirmed"
            );
        }
        Err(error) => {
            ports
                .repository
                .record_readback(lease, OzonGuardStopReadback::Unavailable)
                .await?;
            // The persisted marker makes a second POST forbidden. Keep the
            // row in `stopping` so the next lease owner only reads back.
            tracing::warn!(
                account_id = lease.guard.account_id,
                plan_id = lease.guard.plan_id,
                campaign_id = lease.guard.campaign_id,
                generation = lease.generation,
                stop_reason = lease.stop_reason,
                write_result = write_result_class(write),
                "durable Ozon post-write readback unavailable; marker retained for readback-only recovery"
            );
            return Err(OzonGuardWorkflowError::Read(error));
        }
    }
    Ok(())
}

const fn write_result_class(write: Result<(), OzonGuardWriteFailure>) -> &'static str {
    match write {
        Ok(()) => "ok",
        Err(OzonGuardWriteFailure::Permit) => "permit",
        Err(OzonGuardWriteFailure::MarkerUncertain) => "marker_uncertain",
        Err(OzonGuardWriteFailure::Ambiguous) => "ambiguous",
    }
}

fn hit_failpoint<F: OzonGuardFailpoints>(
    failpoints: &F,
    point: OzonGuardFailpoint,
) -> Result<(), OzonGuardWorkflowError> {
    if failpoints.is_enabled(point) {
        Err(OzonGuardWorkflowError::Failpoint(point))
    } else {
        Ok(())
    }
}

/// One normalized campaign metric row supplied by the Performance adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OzonGuardMetricRow {
    pub business_date: chrono::NaiveDate,
    pub campaign_id: u64,
    pub spend_minor: u64,
    pub attributed_revenue_minor: u64,
}

/// A complete accumulated guard observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OzonGuardMetrics {
    pub spend_minor: u64,
    pub attributed_revenue_minor: u64,
}

/// Metrics evidence attached to a stop. `None` means the stop was requested
/// specifically because telemetry was unavailable; it is never represented by
/// fabricated numeric zeroes.
pub type OzonGuardEvidence = Option<OzonGuardMetrics>;

/// First decision in a static guard item. Optional product/position reads are
/// reachable only after complete spend telemetry proves no hard stop is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OzonStaticGuardFirstStep {
    Stop(OzonGuardStopReason),
    InspectProduct,
}

pub fn plan_static_guard_first_step(
    guard: &OzonCampaignGuard,
    metrics: OzonGuardMetrics,
) -> Result<OzonStaticGuardFirstStep, OzonGuardEvaluationError> {
    Ok(evaluate_ozon_campaign_guard(
        metrics.spend_minor,
        metrics.attributed_revenue_minor,
        guard.spend_cap_microrubles,
        guard.target_drr_percent,
    )?
    .map_or(
        OzonStaticGuardFirstStep::InspectProduct,
        OzonStaticGuardFirstStep::Stop,
    ))
}

/// A telemetry response that cannot safely drive a guard decision.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonGuardTelemetryError {
    #[error("guard telemetry contains an unrequested campaign")]
    UnexpectedCampaign,
    #[error("guard telemetry omits a requested campaign")]
    MissingCampaign,
    #[error("guard telemetry has no current-day row for a requested campaign")]
    MissingCurrentDay,
    #[error("guard telemetry omits a requested campaign-day row")]
    MissingCampaignDay,
    #[error("guard telemetry aggregate overflows")]
    Overflow,
    #[error("guard telemetry window is invalid")]
    InvalidDateWindow,
    #[error("guard telemetry contains a row outside the requested window")]
    RowOutsideDateWindow,
    #[error("guard telemetry repeats a campaign-day row")]
    DuplicateMetricRow,
    #[error("campaign state readback has an unsupported shape or value")]
    InvalidCampaignSnapshot,
    #[error("campaign state readback repeats a campaign")]
    DuplicateCampaign,
}

/// Parses an exact campaign-state readback and returns the running subset.
///
/// Missing, duplicate, malformed, or unrequested rows are never interpreted as
/// a stopped campaign: all of them make the readback unusable.
pub fn parse_complete_running_campaigns(
    snapshot: &Value,
    expected_campaign_ids: &BTreeSet<u64>,
) -> Result<BTreeSet<u64>, OzonGuardTelemetryError> {
    let rows = snapshot
        .get("list")
        .and_then(Value::as_array)
        .ok_or(OzonGuardTelemetryError::InvalidCampaignSnapshot)?;
    let mut observed = BTreeSet::new();
    let mut running = BTreeSet::new();
    for row in rows {
        let campaign_id = canonical_campaign_id(row.get("id"))
            .ok_or(OzonGuardTelemetryError::InvalidCampaignSnapshot)?;
        let state = row
            .get("state")
            .and_then(Value::as_str)
            .ok_or(OzonGuardTelemetryError::InvalidCampaignSnapshot)?;
        if !expected_campaign_ids.contains(&campaign_id) {
            return Err(OzonGuardTelemetryError::UnexpectedCampaign);
        }
        if !observed.insert(campaign_id) {
            return Err(OzonGuardTelemetryError::DuplicateCampaign);
        }
        match state {
            "CAMPAIGN_STATE_RUNNING" => {
                running.insert(campaign_id);
            }
            "CAMPAIGN_STATE_STOPPED"
            | "CAMPAIGN_STATE_INACTIVE"
            | "CAMPAIGN_STATE_FINISHED"
            | "CAMPAIGN_STATE_ARCHIVED"
            | "CAMPAIGN_STATE_PLANNED"
            | "CAMPAIGN_STATE_MODERATION_DRAFT"
            | "CAMPAIGN_STATE_MODERATION_FAILED"
            | "CAMPAIGN_STATE_MODERATION_IN_PROGRESS" => {}
            _ => return Err(OzonGuardTelemetryError::InvalidCampaignSnapshot),
        }
    }
    if observed != *expected_campaign_ids {
        return Err(OzonGuardTelemetryError::MissingCampaign);
    }
    Ok(running)
}

fn canonical_campaign_id(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Number(value) => value.as_u64().filter(|value| *value != 0),
        Value::String(value) => value
            .parse::<u64>()
            .ok()
            .filter(|parsed| *parsed != 0 && parsed.to_string() == *value),
        _ => None,
    }
}

/// Groups running static campaigns by their exact approved statistics window.
///
/// Combining campaigns with different `date_from` values under the oldest date
/// silently over-counts newer campaigns, so each distinct window becomes a
/// separate bounded Performance request.
pub fn group_static_guard_metric_windows(
    guards: &[OzonStaticCampaignGuard],
    running_campaign_ids: &BTreeSet<u64>,
    date_to: &str,
) -> Result<BTreeMap<String, BTreeSet<u64>>, OzonGuardTelemetryError> {
    let date_to = chrono::NaiveDate::parse_from_str(date_to, "%Y-%m-%d")
        .map_err(|_| OzonGuardTelemetryError::InvalidDateWindow)?;
    let configured = guards
        .iter()
        .map(|guard| guard.guard.campaign_id)
        .collect::<BTreeSet<_>>();
    if !running_campaign_ids.is_subset(&configured) {
        return Err(OzonGuardTelemetryError::UnexpectedCampaign);
    }
    let mut windows = BTreeMap::<String, BTreeSet<u64>>::new();
    for guard in guards {
        if running_campaign_ids.contains(&guard.guard.campaign_id) {
            let date_from = chrono::NaiveDate::parse_from_str(&guard.guard.date_from, "%Y-%m-%d")
                .map_err(|_| OzonGuardTelemetryError::InvalidDateWindow)?;
            if date_from > date_to {
                return Err(OzonGuardTelemetryError::InvalidDateWindow);
            }
            windows
                .entry(guard.guard.date_from.clone())
                .or_default()
                .insert(guard.guard.campaign_id);
        }
    }
    Ok(windows)
}

/// Accumulates rows only when every requested campaign is represented.
///
/// A present row containing zeroes is a valid observation. Every requested
/// campaign-day in the inclusive window must be represented; an old-only row
/// or a hole inside the window is incomplete telemetry and can never authorize
/// a bid or activation decision.
pub fn aggregate_complete_guard_metrics(
    expected_campaign_ids: &BTreeSet<u64>,
    date_from: chrono::NaiveDate,
    date_to: chrono::NaiveDate,
    rows: impl IntoIterator<Item = OzonGuardMetricRow>,
) -> Result<BTreeMap<u64, OzonGuardMetrics>, OzonGuardTelemetryError> {
    if date_from > date_to {
        return Err(OzonGuardTelemetryError::InvalidDateWindow);
    }
    let inclusive_days = date_to
        .signed_duration_since(date_from)
        .num_days()
        .checked_add(1)
        .and_then(|days| usize::try_from(days).ok())
        .ok_or(OzonGuardTelemetryError::InvalidDateWindow)?;
    let expected_campaign_days = inclusive_days
        .checked_mul(expected_campaign_ids.len())
        .ok_or(OzonGuardTelemetryError::Overflow)?;
    let mut metrics = BTreeMap::<u64, OzonGuardMetrics>::new();
    let mut observed_days = BTreeSet::new();
    let mut current_day_campaigns = BTreeSet::new();
    for row in rows {
        if !(date_from..=date_to).contains(&row.business_date) {
            return Err(OzonGuardTelemetryError::RowOutsideDateWindow);
        }
        if !expected_campaign_ids.contains(&row.campaign_id) {
            return Err(OzonGuardTelemetryError::UnexpectedCampaign);
        }
        if !observed_days.insert((row.campaign_id, row.business_date)) {
            return Err(OzonGuardTelemetryError::DuplicateMetricRow);
        }
        if row.business_date == date_to {
            current_day_campaigns.insert(row.campaign_id);
        }
        let entry = metrics.entry(row.campaign_id).or_insert(OzonGuardMetrics {
            spend_minor: 0,
            attributed_revenue_minor: 0,
        });
        entry.spend_minor = entry
            .spend_minor
            .checked_add(row.spend_minor)
            .ok_or(OzonGuardTelemetryError::Overflow)?;
        entry.attributed_revenue_minor = entry
            .attributed_revenue_minor
            .checked_add(row.attributed_revenue_minor)
            .ok_or(OzonGuardTelemetryError::Overflow)?;
    }
    if metrics.keys().copied().collect::<BTreeSet<_>>() != *expected_campaign_ids {
        return Err(OzonGuardTelemetryError::MissingCampaign);
    }
    if current_day_campaigns != *expected_campaign_ids {
        return Err(OzonGuardTelemetryError::MissingCurrentDay);
    }
    if observed_days.len() != expected_campaign_days {
        return Err(OzonGuardTelemetryError::MissingCampaignDay);
    }
    Ok(metrics)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unused_async_trait_impl,
        reason = "in-memory async port fakes intentionally complete without suspension"
    )]

    use std::{collections::VecDeque, sync::Mutex};

    use super::*;
    use crate::control::ozon::model::{OzonCampaignGuard, OzonCampaignGuardStatus};

    fn guard(campaign_id: u64, date_from: &str) -> OzonStaticCampaignGuard {
        OzonStaticCampaignGuard {
            guard: OzonCampaignGuard {
                plan_id: format!("static-{campaign_id}"),
                account_id: "account".to_owned(),
                sku: campaign_id + 100,
                campaign_id,
                date_from: date_from.to_owned(),
                spend_cap_microrubles: 2_000_000_000,
                target_drr_percent: 15,
                status: super::super::model::OzonCampaignGuardStatus::Active,
                stop_reason: None,
                incident_error_class: None,
            },
            min_cpc_bid_microrubles: 7_000_000,
            max_cpc_bid_microrubles: 10_000_000,
        }
    }

    fn durable_guard(campaign_id: u64) -> OzonCampaignGuard {
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

    fn lease(
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

    fn marked_lease(
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
    struct FakeRepositoryState {
        active: Vec<OzonCampaignGuard>,
        recoveries: VecDeque<OzonGuardStopLease>,
        claims: Vec<(u64, String, OzonGuardEvidence)>,
        observations: Vec<(u64, OzonGuardMetrics)>,
        readbacks: Vec<(u64, OzonGuardStopReadback)>,
        finishes: Vec<(u64, OzonGuardEvidence)>,
        incidents: Vec<(u64, String, OzonGuardEvidence)>,
    }

    #[derive(Default)]
    struct FakeRepository(Mutex<FakeRepositoryState>);

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
    struct FakeReader {
        metrics: Mutex<VecDeque<Result<OzonGuardMetrics, OzonGuardReadFailure>>>,
        running: Mutex<VecDeque<Result<bool, OzonGuardReadFailure>>>,
        observed_at: Mutex<Vec<DateTime<Utc>>>,
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

        async fn campaign_is_running(
            &self,
            _campaign_id: u64,
        ) -> Result<bool, OzonGuardReadFailure> {
            self.running
                .lock()
                .unwrap()
                .pop_front()
                .expect("a readback result was configured")
        }
    }

    #[derive(Default)]
    struct FakeWriter {
        results: Mutex<VecDeque<Result<(), OzonGuardWriteFailure>>>,
        calls: Mutex<Vec<u64>>,
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

    struct FakeClock {
        now: DateTime<Utc>,
        sleeps: Mutex<Vec<Duration>>,
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
    struct FakeFailpoints(BTreeSet<OzonGuardFailpoint>);

    impl OzonGuardFailpoints for FakeFailpoints {
        fn is_enabled(&self, point: OzonGuardFailpoint) -> bool {
            self.0.contains(&point)
        }
    }

    fn clock() -> FakeClock {
        FakeClock {
            now: DateTime::UNIX_EPOCH + chrono::Duration::days(20_000),
            sleeps: Mutex::new(Vec::new()),
        }
    }

    async fn cycle(
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

    fn metrics_date() -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(2026, 9, 2).unwrap()
    }

    fn aggregate_test_metrics(
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

    #[tokio::test]
    async fn complete_non_breaching_observation_is_recorded_without_a_write() {
        let repository = FakeRepository::default();
        repository.0.lock().unwrap().active.push(durable_guard(1));
        let metrics = OzonGuardMetrics {
            spend_minor: 10_000,
            attributed_revenue_minor: 100_000,
        };
        let reader = FakeReader::default();
        reader.metrics.lock().unwrap().push_back(Ok(metrics));
        let writer = FakeWriter::default();
        let clock = clock();

        cycle(
            &repository,
            &reader,
            &writer,
            &clock,
            &FakeFailpoints::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            repository.0.lock().unwrap().observations,
            vec![(1, metrics)]
        );
        assert!(writer.calls.lock().unwrap().is_empty());
        assert_eq!(reader.observed_at.lock().unwrap().as_slice(), &[clock.now]);
    }

    #[tokio::test]
    async fn incomplete_telemetry_requests_a_fenced_fail_closed_stop() {
        let repository = FakeRepository::default();
        repository.0.lock().unwrap().active.push(durable_guard(2));
        let reader = FakeReader::default();
        reader
            .metrics
            .lock()
            .unwrap()
            .push_back(Err(OzonGuardReadFailure::Telemetry));
        reader.running.lock().unwrap().extend([Ok(true), Ok(false)]);
        let writer = FakeWriter::default();
        let clock = clock();

        cycle(
            &repository,
            &reader,
            &writer,
            &clock,
            &FakeFailpoints::default(),
        )
        .await
        .unwrap();

        let state = repository.0.lock().unwrap();
        assert_eq!(state.claims[0].1, "telemetry_unavailable");
        assert_eq!(state.claims[0].2, None);
        assert_eq!(state.finishes[0].1, None);
        assert!(state.incidents.is_empty());
        assert_eq!(
            state.readbacks,
            vec![
                (2, OzonGuardStopReadback::Running),
                (2, OzonGuardStopReadback::Stopped),
            ]
        );
        assert_eq!(writer.calls.lock().unwrap().as_slice(), &[2]);
        assert_eq!(
            clock.sleeps.lock().unwrap().as_slice(),
            &[Duration::from_secs(2); 2]
        );
    }

    #[tokio::test]
    async fn invalid_guard_limits_fail_closed_with_real_metrics_evidence() {
        let repository = FakeRepository::default();
        let mut guard = durable_guard(20);
        guard.target_drr_percent = 9;
        repository.0.lock().unwrap().active.push(guard);
        let metrics = OzonGuardMetrics {
            spend_minor: 10,
            attributed_revenue_minor: 100,
        };
        let reader = FakeReader::default();
        reader.metrics.lock().unwrap().push_back(Ok(metrics));
        reader.running.lock().unwrap().extend([Ok(true), Ok(false)]);

        cycle(
            &repository,
            &reader,
            &FakeWriter::default(),
            &clock(),
            &FakeFailpoints::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            repository.0.lock().unwrap().claims,
            vec![(20, "guard_evaluation_failed".to_owned(), Some(metrics))]
        );
    }

    #[tokio::test]
    async fn crash_after_claim_is_recovered_by_readback_without_a_second_write() {
        let repository = FakeRepository::default();
        repository.0.lock().unwrap().active.push(durable_guard(3));
        let metrics = OzonGuardMetrics {
            spend_minor: 100_000,
            attributed_revenue_minor: 0,
        };
        let reader = FakeReader::default();
        reader.metrics.lock().unwrap().push_back(Ok(metrics));
        let writer = FakeWriter::default();
        let clock = clock();
        let crash = FakeFailpoints(BTreeSet::from([OzonGuardFailpoint::AfterStopClaim]));

        assert_eq!(
            cycle(&repository, &reader, &writer, &clock, &crash).await,
            Err(OzonGuardWorkflowError::Failpoint(
                OzonGuardFailpoint::AfterStopClaim
            ))
        );
        assert!(writer.calls.lock().unwrap().is_empty());

        reader.running.lock().unwrap().push_back(Ok(false));
        cycle(
            &repository,
            &reader,
            &writer,
            &clock,
            &FakeFailpoints::default(),
        )
        .await
        .unwrap();
        let state = repository.0.lock().unwrap();
        assert_eq!(state.finishes, vec![(3, Some(metrics))]);
        assert!(state.incidents.is_empty());
        drop(state);
        assert!(writer.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn ambiguous_recovery_uses_one_write_and_finishes_when_readback_is_stopped() {
        let metrics = OzonGuardMetrics {
            spend_minor: 100_000,
            attributed_revenue_minor: 50_000,
        };
        let repository = FakeRepository::default();
        repository.0.lock().unwrap().recoveries.push_back(lease(
            durable_guard(4),
            "spend_cap_reached",
            Some(metrics),
        ));
        let reader = FakeReader::default();
        reader.running.lock().unwrap().extend([Ok(true), Ok(false)]);
        let writer = FakeWriter::default();
        writer
            .results
            .lock()
            .unwrap()
            .push_back(Err(OzonGuardWriteFailure::Ambiguous));

        cycle(
            &repository,
            &reader,
            &writer,
            &clock(),
            &FakeFailpoints::default(),
        )
        .await
        .unwrap();

        let state = repository.0.lock().unwrap();
        assert_eq!(state.finishes, vec![(4, Some(metrics))]);
        assert!(state.incidents.is_empty());
        drop(state);
        assert_eq!(writer.calls.lock().unwrap().as_slice(), &[4]);
    }

    #[tokio::test]
    async fn saturated_recovery_batch_still_scans_active_hard_stops() {
        let repository = FakeRepository::default();
        let over_cap = OzonGuardMetrics {
            spend_minor: 2_000_000_000,
            attributed_revenue_minor: 0,
        };
        {
            let mut state = repository.0.lock().unwrap();
            for campaign_id in 1..=MAX_OZON_GUARD_RECOVERIES_PER_CYCLE as u64 {
                state.recoveries.push_back(marked_lease(
                    durable_guard(campaign_id),
                    "spend_cap_reached",
                    Some(over_cap),
                ));
            }
            state.active.push(durable_guard(100));
        }
        let reader = FakeReader::default();
        reader.running.lock().unwrap().extend(
            std::iter::repeat_n(
                Err(OzonGuardReadFailure::CampaignState),
                MAX_OZON_GUARD_RECOVERIES_PER_CYCLE,
            )
            .chain([Ok(true), Ok(false)]),
        );
        reader.metrics.lock().unwrap().push_back(Ok(over_cap));
        let writer = FakeWriter::default();

        cycle(
            &repository,
            &reader,
            &writer,
            &clock(),
            &FakeFailpoints::default(),
        )
        .await
        .unwrap();
        let state = repository.0.lock().unwrap();
        assert!(state.recoveries.is_empty());
        assert_eq!(state.claims[0].0, 100);
        assert_eq!(state.finishes, vec![(100, Some(over_cap))]);
        assert_eq!(*writer.calls.lock().unwrap(), vec![100]);
    }

    #[tokio::test]
    async fn unmarked_recovery_writes_once_and_incidents_a_running_readback() {
        let metrics = OzonGuardMetrics {
            spend_minor: 100_000,
            attributed_revenue_minor: 0,
        };
        for (write, expected_error_class) in [
            (Ok(()), "stop_readback_mismatch"),
            (
                Err(OzonGuardWriteFailure::Ambiguous),
                "stop_write_ambiguous",
            ),
        ] {
            let repository = FakeRepository::default();
            repository.0.lock().unwrap().recoveries.push_back(lease(
                durable_guard(5),
                "spend_cap_reached",
                Some(metrics),
            ));
            let reader = FakeReader::default();
            reader.running.lock().unwrap().extend([Ok(true), Ok(true)]);
            let writer = FakeWriter::default();
            writer.results.lock().unwrap().push_back(write);

            cycle(
                &repository,
                &reader,
                &writer,
                &clock(),
                &FakeFailpoints::default(),
            )
            .await
            .unwrap();

            let state = repository.0.lock().unwrap();
            assert_eq!(
                state.incidents,
                vec![(5, expected_error_class.to_owned(), Some(metrics))]
            );
            assert_eq!(writer.calls.lock().unwrap().as_slice(), &[5]);
        }
    }

    #[tokio::test]
    async fn marker_present_recovery_is_readback_only() {
        let metrics = OzonGuardMetrics {
            spend_minor: 100_000,
            attributed_revenue_minor: 0,
        };
        let repository = FakeRepository::default();
        repository
            .0
            .lock()
            .unwrap()
            .recoveries
            .push_back(marked_lease(
                durable_guard(6),
                "spend_cap_reached",
                Some(metrics),
            ));
        let reader = FakeReader::default();
        reader.running.lock().unwrap().push_back(Ok(true));
        let writer = FakeWriter::default();

        cycle(
            &repository,
            &reader,
            &writer,
            &clock(),
            &FakeFailpoints::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            repository.0.lock().unwrap().incidents,
            vec![(6, "stop_write_unconfirmed".to_owned(), Some(metrics))]
        );
        assert!(writer.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unavailable_prewrite_readback_leaves_stop_recoverable_without_a_write() {
        let metrics = OzonGuardMetrics {
            spend_minor: 100_000,
            attributed_revenue_minor: 0,
        };
        let repository = FakeRepository::default();
        repository.0.lock().unwrap().recoveries.push_back(lease(
            durable_guard(6),
            "spend_cap_reached",
            Some(metrics),
        ));
        let reader = FakeReader::default();
        reader
            .running
            .lock()
            .unwrap()
            .push_back(Err(OzonGuardReadFailure::CampaignState));
        let writer = FakeWriter::default();

        cycle(
            &repository,
            &reader,
            &writer,
            &clock(),
            &FakeFailpoints::default(),
        )
        .await
        .unwrap();
        let state = repository.0.lock().unwrap();
        assert!(state.incidents.is_empty());
        assert_eq!(
            state.readbacks,
            vec![(6, OzonGuardStopReadback::Unavailable)]
        );
        assert!(writer.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn marked_recovery_readback_outage_never_repeats_or_terminalizes_the_write() {
        let metrics = OzonGuardMetrics {
            spend_minor: 100_000,
            attributed_revenue_minor: 0,
        };
        let repository = FakeRepository::default();
        repository
            .0
            .lock()
            .unwrap()
            .recoveries
            .push_back(marked_lease(
                durable_guard(16),
                "spend_cap_reached",
                Some(metrics),
            ));
        let reader = FakeReader::default();
        reader
            .running
            .lock()
            .unwrap()
            .push_back(Err(OzonGuardReadFailure::CampaignState));
        let writer = FakeWriter::default();

        cycle(
            &repository,
            &reader,
            &writer,
            &clock(),
            &FakeFailpoints::default(),
        )
        .await
        .unwrap();
        let state = repository.0.lock().unwrap();
        assert!(state.incidents.is_empty());
        assert_eq!(
            state.readbacks,
            vec![(16, OzonGuardStopReadback::Unavailable)]
        );
        assert!(writer.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn persistent_recovery_outage_does_not_starve_active_over_cap_guards() {
        let repository = FakeRepository::default();
        let reader = FakeReader::default();
        let writer = FakeWriter::default();
        let over_cap = OzonGuardMetrics {
            spend_minor: 2_000_000_000,
            attributed_revenue_minor: 0,
        };

        for active_campaign_id in [30, 31] {
            {
                let mut state = repository.0.lock().unwrap();
                state.recoveries.push_back(marked_lease(
                    durable_guard(16),
                    "spend_cap_reached",
                    Some(over_cap),
                ));
                state.active.push(durable_guard(active_campaign_id));
            }
            reader.metrics.lock().unwrap().push_back(Ok(over_cap));
            reader.running.lock().unwrap().extend([
                Err(OzonGuardReadFailure::CampaignState),
                Ok(true),
                Ok(false),
            ]);

            cycle(
                &repository,
                &reader,
                &writer,
                &clock(),
                &FakeFailpoints::default(),
            )
            .await
            .unwrap();
        }

        let state = repository.0.lock().unwrap();
        assert_eq!(
            state
                .claims
                .iter()
                .map(|(campaign_id, _, _)| *campaign_id)
                .collect::<Vec<_>>(),
            vec![30, 31]
        );
        assert_eq!(
            state
                .finishes
                .iter()
                .map(|(campaign_id, _)| *campaign_id)
                .collect::<Vec<_>>(),
            vec![30, 31]
        );
        assert_eq!(*writer.calls.lock().unwrap(), vec![30, 31]);
        assert_eq!(
            state
                .readbacks
                .iter()
                .filter(|(campaign_id, observation)| {
                    *campaign_id == 16 && *observation == OzonGuardStopReadback::Unavailable
                })
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn permit_failure_before_marker_leaves_stop_recoverable() {
        let metrics = OzonGuardMetrics {
            spend_minor: 100_000,
            attributed_revenue_minor: 0,
        };
        let repository = FakeRepository::default();
        repository.0.lock().unwrap().recoveries.push_back(lease(
            durable_guard(17),
            "spend_cap_reached",
            Some(metrics),
        ));
        let reader = FakeReader::default();
        reader.running.lock().unwrap().extend([Ok(true), Ok(true)]);
        let writer = FakeWriter::default();
        writer
            .results
            .lock()
            .unwrap()
            .push_back(Err(OzonGuardWriteFailure::Permit));

        assert_eq!(
            cycle(
                &repository,
                &reader,
                &writer,
                &clock(),
                &FakeFailpoints::default(),
            )
            .await,
            Err(OzonGuardWorkflowError::Repository(
                OzonPlanStoreError::LeaseLost
            ))
        );
        assert!(repository.0.lock().unwrap().incidents.is_empty());
        assert_eq!(writer.calls.lock().unwrap().as_slice(), &[17]);
    }

    #[tokio::test]
    async fn lost_marker_ack_is_not_misreported_as_an_unattempted_permit() {
        let metrics = OzonGuardMetrics {
            spend_minor: 100_000,
            attributed_revenue_minor: 0,
        };
        let repository = FakeRepository::default();
        repository.0.lock().unwrap().recoveries.push_back(lease(
            durable_guard(18),
            "spend_cap_reached",
            Some(metrics),
        ));
        let reader = FakeReader::default();
        reader.running.lock().unwrap().extend([Ok(true), Ok(true)]);
        let writer = FakeWriter::default();
        writer
            .results
            .lock()
            .unwrap()
            .push_back(Err(OzonGuardWriteFailure::MarkerUncertain));

        assert_eq!(
            cycle(
                &repository,
                &reader,
                &writer,
                &clock(),
                &FakeFailpoints::default(),
            )
            .await,
            Err(OzonGuardWorkflowError::Repository(
                OzonPlanStoreError::Unavailable
            ))
        );
        assert!(repository.0.lock().unwrap().incidents.is_empty());
        assert_eq!(writer.calls.lock().unwrap().as_slice(), &[18]);
    }

    #[tokio::test]
    async fn crash_after_write_is_finished_from_recovery_readback() {
        let repository = FakeRepository::default();
        repository.0.lock().unwrap().active.push(durable_guard(7));
        let metrics = OzonGuardMetrics {
            spend_minor: 100_000,
            attributed_revenue_minor: 0,
        };
        let reader = FakeReader::default();
        reader.metrics.lock().unwrap().push_back(Ok(metrics));
        reader.running.lock().unwrap().push_back(Ok(true));
        let writer = FakeWriter::default();
        let crash = FakeFailpoints(BTreeSet::from([OzonGuardFailpoint::AfterDeactivate]));

        assert_eq!(
            cycle(&repository, &reader, &writer, &clock(), &crash).await,
            Err(OzonGuardWorkflowError::Failpoint(
                OzonGuardFailpoint::AfterDeactivate
            ))
        );
        assert_eq!(writer.calls.lock().unwrap().as_slice(), &[7]);

        repository
            .0
            .lock()
            .unwrap()
            .recoveries
            .front_mut()
            .unwrap()
            .write_started_at = Some(clock().now);
        reader.running.lock().unwrap().push_back(Ok(false));
        cycle(
            &repository,
            &reader,
            &writer,
            &clock(),
            &FakeFailpoints::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            repository.0.lock().unwrap().finishes,
            vec![(7, Some(metrics))]
        );
        assert_eq!(writer.calls.lock().unwrap().as_slice(), &[7]);
    }

    #[test]
    fn static_hard_stop_is_planned_before_optional_product_inputs() {
        let guard = durable_guard(8);
        assert_eq!(
            plan_static_guard_first_step(
                &guard,
                OzonGuardMetrics {
                    spend_minor: 100_000,
                    attributed_revenue_minor: 0,
                },
            ),
            Ok(OzonStaticGuardFirstStep::Stop(
                OzonGuardStopReason::SpendCapReached
            ))
        );
        assert_eq!(
            plan_static_guard_first_step(
                &guard,
                OzonGuardMetrics {
                    spend_minor: 10_000,
                    attributed_revenue_minor: 100_000,
                },
            ),
            Ok(OzonStaticGuardFirstStep::InspectProduct)
        );
    }

    #[test]
    fn metric_windows_preserve_each_campaign_start_date() {
        let guards = [
            guard(1, "2026-09-01"),
            guard(2, "2026-09-02"),
            guard(3, "2026-09-02"),
        ];
        let running = BTreeSet::from([1, 2, 3]);
        assert_eq!(
            group_static_guard_metric_windows(&guards, &running, "2026-09-03").unwrap(),
            BTreeMap::from([
                ("2026-09-01".to_owned(), BTreeSet::from([1])),
                ("2026-09-02".to_owned(), BTreeSet::from([2, 3])),
            ])
        );
        assert_eq!(
            group_static_guard_metric_windows(&guards, &BTreeSet::from([4]), "2026-09-03"),
            Err(OzonGuardTelemetryError::UnexpectedCampaign)
        );
        assert!(
            group_static_guard_metric_windows(&guards, &BTreeSet::new(), "2026-09-03")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            group_static_guard_metric_windows(&guards, &running, "2026-08-31"),
            Err(OzonGuardTelemetryError::InvalidDateWindow)
        );
        assert_eq!(
            group_static_guard_metric_windows(&guards, &running, "not-a-date"),
            Err(OzonGuardTelemetryError::InvalidDateWindow)
        );
    }

    #[test]
    fn telemetry_is_complete_scoped_and_overflow_checked() {
        let expected = BTreeSet::from([1, 2]);
        assert_eq!(
            aggregate_test_metrics(
                &expected,
                [
                    OzonGuardMetricRow {
                        business_date: metrics_date() - chrono::Duration::days(1),
                        campaign_id: 1,
                        spend_minor: 10,
                        attributed_revenue_minor: 100,
                    },
                    OzonGuardMetricRow {
                        business_date: metrics_date(),
                        campaign_id: 1,
                        spend_minor: 5,
                        attributed_revenue_minor: 50,
                    },
                    OzonGuardMetricRow {
                        business_date: metrics_date() - chrono::Duration::days(1),
                        campaign_id: 2,
                        spend_minor: 0,
                        attributed_revenue_minor: 0,
                    },
                    OzonGuardMetricRow {
                        business_date: metrics_date(),
                        campaign_id: 2,
                        spend_minor: 0,
                        attributed_revenue_minor: 0,
                    },
                ]
            )
            .unwrap(),
            BTreeMap::from([
                (
                    1,
                    OzonGuardMetrics {
                        spend_minor: 15,
                        attributed_revenue_minor: 150,
                    },
                ),
                (
                    2,
                    OzonGuardMetrics {
                        spend_minor: 0,
                        attributed_revenue_minor: 0,
                    },
                ),
            ])
        );
        assert_eq!(
            aggregate_test_metrics(
                &expected,
                [OzonGuardMetricRow {
                    business_date: metrics_date(),
                    campaign_id: 1,
                    spend_minor: 0,
                    attributed_revenue_minor: 0,
                }]
            ),
            Err(OzonGuardTelemetryError::MissingCampaign)
        );
        assert_eq!(
            aggregate_test_metrics(
                &expected,
                [OzonGuardMetricRow {
                    business_date: metrics_date(),
                    campaign_id: 3,
                    spend_minor: 0,
                    attributed_revenue_minor: 0,
                }]
            ),
            Err(OzonGuardTelemetryError::UnexpectedCampaign)
        );
        assert_eq!(
            aggregate_test_metrics(
                &BTreeSet::from([1]),
                [OzonGuardMetricRow {
                    business_date: metrics_date() - chrono::Duration::days(1),
                    campaign_id: 1,
                    spend_minor: 10,
                    attributed_revenue_minor: 100,
                }]
            ),
            Err(OzonGuardTelemetryError::MissingCurrentDay)
        );
        assert_eq!(
            aggregate_complete_guard_metrics(
                &BTreeSet::from([1]),
                metrics_date() - chrono::Duration::days(2),
                metrics_date(),
                [
                    OzonGuardMetricRow {
                        business_date: metrics_date() - chrono::Duration::days(2),
                        campaign_id: 1,
                        spend_minor: 10,
                        attributed_revenue_minor: 100,
                    },
                    OzonGuardMetricRow {
                        business_date: metrics_date(),
                        campaign_id: 1,
                        spend_minor: 0,
                        attributed_revenue_minor: 0,
                    },
                ],
            ),
            Err(OzonGuardTelemetryError::MissingCampaignDay)
        );

        for row in [
            OzonGuardMetricRow {
                business_date: metrics_date(),
                campaign_id: 1,
                spend_minor: 1,
                attributed_revenue_minor: 0,
            },
            OzonGuardMetricRow {
                business_date: metrics_date(),
                campaign_id: 1,
                spend_minor: 0,
                attributed_revenue_minor: 1,
            },
        ] {
            let first = OzonGuardMetricRow {
                business_date: metrics_date() - chrono::Duration::days(1),
                campaign_id: 1,
                spend_minor: if row.spend_minor == 0 { 0 } else { u64::MAX },
                attributed_revenue_minor: if row.attributed_revenue_minor == 0 {
                    0
                } else {
                    u64::MAX
                },
            };
            assert_eq!(
                aggregate_test_metrics(&BTreeSet::from([1]), [first, row]),
                Err(OzonGuardTelemetryError::Overflow)
            );
        }
        assert!(
            aggregate_test_metrics(&BTreeSet::new(), [])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            aggregate_complete_guard_metrics(
                &BTreeSet::from([1]),
                metrics_date(),
                metrics_date(),
                [OzonGuardMetricRow {
                    business_date: metrics_date() + chrono::Duration::days(1),
                    campaign_id: 1,
                    spend_minor: 0,
                    attributed_revenue_minor: 0,
                }],
            ),
            Err(OzonGuardTelemetryError::RowOutsideDateWindow)
        );
        let duplicate = OzonGuardMetricRow {
            business_date: metrics_date(),
            campaign_id: 1,
            spend_minor: 0,
            attributed_revenue_minor: 0,
        };
        assert_eq!(
            aggregate_test_metrics(&BTreeSet::from([1]), [duplicate, duplicate]),
            Err(OzonGuardTelemetryError::DuplicateMetricRow)
        );
    }

    #[test]
    fn campaign_state_readback_is_exact_before_stopped_is_inferred() {
        let expected = BTreeSet::from([1, 2]);
        assert_eq!(
            parse_complete_running_campaigns(
                &serde_json::json!({"list": [
                    {"id": "1", "state": "CAMPAIGN_STATE_RUNNING"},
                    {"id": 2, "state": "CAMPAIGN_STATE_STOPPED"}
                ]}),
                &expected,
            )
            .unwrap(),
            BTreeSet::from([1])
        );
        for (snapshot, error) in [
            (
                serde_json::json!({}),
                OzonGuardTelemetryError::InvalidCampaignSnapshot,
            ),
            (
                serde_json::json!({"list": [{"id": "01", "state": "CAMPAIGN_STATE_STOPPED"}, {"id": 2, "state": "CAMPAIGN_STATE_STOPPED"}]}),
                OzonGuardTelemetryError::InvalidCampaignSnapshot,
            ),
            (
                serde_json::json!({"list": [{"id": 0, "state": "CAMPAIGN_STATE_STOPPED"}, {"id": 2, "state": "CAMPAIGN_STATE_STOPPED"}]}),
                OzonGuardTelemetryError::InvalidCampaignSnapshot,
            ),
            (
                serde_json::json!({"list": [{"id": true, "state": "CAMPAIGN_STATE_STOPPED"}, {"id": 2, "state": "CAMPAIGN_STATE_STOPPED"}]}),
                OzonGuardTelemetryError::InvalidCampaignSnapshot,
            ),
            (
                serde_json::json!({"list": [{"id": 1}, {"id": 2, "state": "CAMPAIGN_STATE_STOPPED"}]}),
                OzonGuardTelemetryError::InvalidCampaignSnapshot,
            ),
            (
                serde_json::json!({"list": [
                    {"id": 1, "state": "CAMPAIGN_STATE_UNKNOWN"},
                    {"id": 2, "state": "CAMPAIGN_STATE_STOPPED"}
                ]}),
                OzonGuardTelemetryError::InvalidCampaignSnapshot,
            ),
            (
                serde_json::json!({"list": [{"id": 1, "state": "CAMPAIGN_STATE_STOPPED"}]}),
                OzonGuardTelemetryError::MissingCampaign,
            ),
            (
                serde_json::json!({"list": [
                    {"id": 1, "state": "CAMPAIGN_STATE_STOPPED"},
                    {"id": 1, "state": "CAMPAIGN_STATE_STOPPED"}
                ]}),
                OzonGuardTelemetryError::DuplicateCampaign,
            ),
            (
                serde_json::json!({"list": [
                    {"id": 1, "state": "CAMPAIGN_STATE_STOPPED"},
                    {"id": 3, "state": "CAMPAIGN_STATE_STOPPED"}
                ]}),
                OzonGuardTelemetryError::UnexpectedCampaign,
            ),
        ] {
            assert_eq!(
                parse_complete_running_campaigns(&snapshot, &expected),
                Err(error)
            );
        }
    }
}
