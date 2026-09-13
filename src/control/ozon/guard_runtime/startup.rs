use super::{
    ControlPolicy, OzonExecutorLease, OzonPlanRepository, STATIC_GUARDS_FILE_ENV,
    STATIC_STATE_FILE_ENV, load_static_guards, load_static_state,
    validate_ozon_static_guard_policy, validate_ozon_static_guard_state_scope,
    validate_static_audit_continuity, validate_static_state_health,
};
use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use std::{
    collections::BTreeSet,
    env,
    path::{Path, PathBuf},
};

pub(super) async fn verify_executor_health(
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

pub(super) fn record_static_cycle_result(
    result: Result<()>,
    consecutive_failures: &mut usize,
) -> Result<()> {
    match result {
        Ok(()) => *consecutive_failures = 0,
        Err(error) => {
            let failure_limit_reached = super::record_cycle_outcome(consecutive_failures, false);
            tracing::error!(
                %error,
                consecutive_cycle_failures = *consecutive_failures,
                failure_limit = super::MAX_CONSECUTIVE_WORKFLOW_FAILURES,
                "static Ozon guard cycle failed"
            );
            if failure_limit_reached {
                bail!("static Ozon guard exceeded its consecutive cycle failure limit");
            }
        }
    }
    Ok(())
}
