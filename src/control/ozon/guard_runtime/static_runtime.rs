use super::{
    Command, OzonAdsWriteClient, OzonBidPositionReader, OzonExecutorLease, OzonPlanStoreError,
    OzonStaticGuardConfig, OzonStaticGuardStateLease, POSITION_DATABASE_URL_ENV, PerformanceClient,
    StaticAuditContinuity, StaticGuardWriteAuthorization, audit_static_campaigns,
    guard_once_static, load_static_state, persist_static_initialization_cursor,
    reconcile_static_campaigns, record_static_cycle_result, recover_pending_static_bids,
    recover_pending_static_campaign_mutations, validate_ozon_static_guard_policy,
    validate_ozon_static_guard_state_scope, validate_static_command_audit_continuity,
};
use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use std::{collections::BTreeSet, env, future::Future, path::Path, sync::Arc, time::Duration};

/// Static command execution after bootstrap has validated identity and acquired
/// both executor ownership and the private state-file lease. Supplying clients
/// here separates construction from command semantics without changing either.
pub(super) struct StaticGuardRuntime<'a> {
    pub(super) command: Command,
    pub(super) poll_interval: Duration,
    pub(super) state_lease: OzonStaticGuardStateLease,
    pub(super) state_path: &'a Path,
    pub(super) config: OzonStaticGuardConfig,
    pub(super) reader: &'a Arc<PerformanceClient>,
    pub(super) writer: &'a Arc<OzonAdsWriteClient>,
    pub(super) write_authorization: StaticGuardWriteAuthorization<'a>,
    pub(super) executor_lease: &'a OzonExecutorLease,
}

impl StaticGuardRuntime<'_> {
    pub(super) async fn run<S: Future<Output = ()>>(self, shutdown: S) -> Result<()> {
        tokio::pin!(shutdown);
        let Self {
            command,
            poll_interval,
            state_lease: _state_lease,
            state_path,
            config: static_guard_config,
            reader,
            writer,
            write_authorization,
            executor_lease,
        } = self;
        let plans = write_authorization.repository;
        validate_ozon_static_guard_policy(&static_guard_config, write_authorization.policy)?;
        let static_guards = static_guard_config.guards;
        let dynamic_bid_control = static_guard_config.dynamic_bid_control;
        let mut state = load_static_state(state_path)?;
        let allowed_campaign_ids = static_guards
            .iter()
            .map(|guard| guard.guard.campaign_id)
            .collect::<BTreeSet<_>>();
        validate_ozon_static_guard_state_scope(&state, &allowed_campaign_ids)?;
        let latest_static_audit_event_id = plans
            .latest_static_guard_audit_event_id(write_authorization.runtime_account_id)
            .await?;
        match validate_static_command_audit_continuity(
            command,
            &state,
            latest_static_audit_event_id,
        )? {
            StaticAuditContinuity::InitializeState => {
                let initialization = plans.initialize_static_guard_state(
                    write_authorization.policy.version,
                    write_authorization.policy.revision,
                    write_authorization.policy.digest(),
                    write_authorization.runtime_account_id,
                    write_authorization.config_digest,
                    write_authorization.worker_id,
                    state.last_static_audit_event_id,
                    |event_id| {
                        let state = &mut state;
                        async move {
                            persist_static_initialization_cursor(state, state_path, event_id)
                                .map_err(|_| OzonPlanStoreError::Unavailable)
                        }
                    },
                );
                tokio::select! {
                    result = initialization => result?,
                    () = executor_lease.lost() => bail!("Ozon executor lease connection was lost"),
                }
                tracing::info!(
                    account_id = %write_authorization.runtime_account_id,
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
                    result = audit_static_campaigns(&static_guards, reader, write_authorization.store) => result?,
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
                state_path,
                reader.as_ref(),
                writer.as_ref(),
                write_authorization.store,
                &static_guards,
                write_authorization,
            ) => result?,
            () = executor_lease.lost() => bail!("Ozon executor lease connection was lost"),
        }
        tokio::select! {
            result = recover_pending_static_bids(
                &mut state,
                state_path,
                reader.as_ref(),
                write_authorization.store,
                &static_guards,
            ) => result?,
            () = executor_lease.lost() => bail!("Ozon executor lease connection was lost"),
        }
        tracing::info!(
            account_id=%write_authorization.runtime_account_id,
            guards=static_guards.len(),
            dynamic_bid_control=dynamic_bid_control.is_some(),
            "static Ozon campaign guard armed"
        );
        if command == Command::AuditStaticOnce {
            tokio::select! {
                result = audit_static_campaigns(&static_guards, reader, write_authorization.store) => result?,
                () = executor_lease.lost() => bail!("Ozon executor lease connection was lost"),
            }
            return Ok(());
        }
        if command == Command::ReconcileStaticOnce {
            tokio::select! {
                result = reconcile_static_campaigns(
                    &static_guards,
                    &mut state,
                    state_path,
                    reader,
                    writer,
                    write_authorization.store,
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
                state_path,
                reader,
                writer,
                write_authorization.store,
                write_authorization,
                dynamic_bid_control.as_ref(),
                position_reader.as_deref(),
                Utc::now(),
            );
            tokio::select! {
                result = cycle => record_static_cycle_result(
                    result,
                    &mut consecutive_cycle_failures,
                )?,
                () = &mut shutdown => break,
                () = executor_lease.lost() => {
                    bail!("Ozon executor lease connection was lost");
                }
            }
            tokio::select! {
                () = tokio::time::sleep(poll_interval) => {}
                () = &mut shutdown => break,
                () = executor_lease.lost() => {
                    bail!("Ozon executor lease connection was lost");
                }
            }
        }
        Ok(())
    }
}
