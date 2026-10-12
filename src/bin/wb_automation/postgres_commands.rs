//! PostgreSQL command execution and explicit maintenance confirmations.
use crate::{
    ExplicitActionOptions, ExplicitExposureIncreaseOptions, ExplicitQuotaOverrideOptions,
    PostgresExecuteOptions, REQUEST_TIMEOUT, connect_state_store, load_legacy_state,
};
use anyhow::{Context as _, Result, ensure};
use chrono::Utc;
use mcp_ozon::control::WbAutomationExecutor;

pub async fn execute_postgres_once(options: PostgresExecuteOptions) -> Result<()> {
    execute_postgres_with_intent(options, PostgresCommandIntent::Automatic, None).await
}

pub async fn explicit_exposure_increase_postgres_once(
    options: ExplicitExposureIncreaseOptions,
) -> Result<()> {
    ensure!(
        options.confirmation == "--confirm-explicit-exposure-increase",
        "WB explicit exposure increase confirmation is invalid"
    );
    execute_postgres_with_intent(
        options.execute,
        PostgresCommandIntent::ExplicitExposureTarget(options.target_impressions),
        None,
    )
    .await
}

pub async fn explicit_quota_override_postgres_once(
    options: ExplicitQuotaOverrideOptions,
) -> Result<()> {
    ensure!(
        options.confirmation == "--confirm-one-extra-audited-action",
        "WB explicit quota override confirmation is invalid"
    );
    execute_postgres_with_intent(
        options.execute,
        PostgresCommandIntent::ExplicitQuotaOverride,
        Some(&options.authorization_reference),
    )
    .await
}

pub async fn explicit_resume_after_daily_cap_postgres_once(
    options: ExplicitActionOptions,
) -> Result<()> {
    ensure!(
        options.confirmation == "--confirm-explicit-resume-after-daily-cap",
        "WB explicit resume confirmation is invalid"
    );
    execute_postgres_with_intent(
        options.execute,
        PostgresCommandIntent::ExplicitResumeAfterDailyCap,
        None,
    )
    .await
}

pub async fn explicit_bid_cap_repair_postgres_once(options: ExplicitActionOptions) -> Result<()> {
    ensure!(
        options.confirmation == "--confirm-explicit-bid-cap-repair",
        "WB explicit bid cap repair confirmation is invalid"
    );
    execute_postgres_with_intent(
        options.execute,
        PostgresCommandIntent::ExplicitBidCapRepair,
        None,
    )
    .await
}

#[derive(Clone, Copy)]
enum PostgresCommandIntent {
    Automatic,
    ExplicitExposureTarget(u64),
    ExplicitQuotaOverride,
    ExplicitResumeAfterDailyCap,
    ExplicitBidCapRepair,
}

async fn execute_postgres_with_intent(
    options: PostgresExecuteOptions,
    intent: PostgresCommandIntent,
    quota_override_authorization: Option<&str>,
) -> Result<()> {
    let state_directory = options
        .legacy_state
        .parent()
        .context("WB automation legacy state parent is unavailable")?;
    let executor = WbAutomationExecutor::from_files(
        &options.execute.policy,
        &options.execute.registry,
        &options.execute.reader_token,
        &options.execute.writer_token,
        state_directory,
        options.execute.allow_broad_reader,
        REQUEST_TIMEOUT,
        options.execute.reader_proxy_url.as_deref(),
        &options.execute.writer_proxy_url,
    )?;
    let legacy = load_legacy_state(
        &options.legacy_state,
        &executor.policy().account_id,
        executor.policy().campaign_id,
        executor.policy_sha256(),
    )?;
    let store = connect_state_store().await?;
    let receipt = match intent {
        PostgresCommandIntent::ExplicitExposureTarget(target) => {
            executor
                .run_explicit_exposure_increase_once_postgres(&store, &legacy, Utc::now(), target)
                .await?
        }
        PostgresCommandIntent::ExplicitQuotaOverride => {
            executor
                .run_explicit_quota_override_once_postgres(
                    &store,
                    &legacy,
                    Utc::now(),
                    quota_override_authorization
                        .context("WB explicit quota override authorization is unavailable")?,
                )
                .await?
        }
        PostgresCommandIntent::ExplicitResumeAfterDailyCap => {
            executor
                .run_explicit_resume_after_daily_cap_once_postgres(&store, &legacy, Utc::now())
                .await?
        }
        PostgresCommandIntent::ExplicitBidCapRepair => {
            executor
                .run_explicit_bid_cap_repair_once_postgres(&store, &legacy, Utc::now())
                .await?
        }
        PostgresCommandIntent::Automatic => {
            executor
                .run_once_postgres(&store, &legacy, Utc::now())
                .await?
        }
    };
    match receipt {
        Some(receipt) => println!("{}", serde_json::to_string(&receipt)?),
        None => println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "account_id": executor.policy().account_id,
                "campaign_id": executor.policy().campaign_id,
                "outcome": "lock_contended",
            }))?
        ),
    }
    Ok(())
}
