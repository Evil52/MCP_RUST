use super::{
    Command, activate_bid_writes_postgres, activate_bounded_pacing_postgres,
    activate_protective_live_postgres, activate_traffic_frontier_v2_postgres,
    activate_traffic_frontier_v3_postgres, activate_traffic_frontier_v4_postgres, auto_once,
    execute_once, execute_postgres_once, explicit_exposure_increase_postgres_once,
    explicit_quota_override_postgres_once, explicit_resume_after_daily_cap_postgres_once,
    observe_once, parse_command, raise_traffic_frontier_limits_postgres, shadow_postgres_once,
    tighten_traffic_frontier_corridor_postgres,
};
use anyhow::{Result, ensure};
use mcp_ozon::runtime::print_runtime_version_if_requested;
use std::path::Path;

pub async fn run() -> Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if print_runtime_version_if_requested("wb-automation", &arguments)? {
        return Ok(());
    }
    if arguments
        .first()
        .is_some_and(|arg| arg == "campaign-launch")
    {
        ensure!(
            arguments.len() == 3,
            "usage: wb-automation campaign-launch preflight|create|bids|fund|start|reconcile MANIFEST.json"
        );
        let result =
            mcp_ozon::control::run_wb_campaign_launch(&arguments[1], Path::new(&arguments[2]))
                .await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }
    match parse_command(&arguments)? {
        Command::Observe(options) => observe_once(options).await,
        Command::ShadowPostgres(options) => shadow_postgres_once(options).await,
        Command::ActivateProtectiveLivePostgres(options) => {
            activate_protective_live_postgres(options).await
        }
        Command::ActivateBidWritesPostgres(options) => activate_bid_writes_postgres(options).await,
        Command::ActivateBoundedPacingPostgres(options) => {
            activate_bounded_pacing_postgres(options).await
        }
        Command::ActivateTrafficFrontierV2Postgres(options) => {
            activate_traffic_frontier_v2_postgres(options).await
        }
        Command::ActivateTrafficFrontierV3Postgres(options) => {
            activate_traffic_frontier_v3_postgres(options).await
        }
        Command::ActivateTrafficFrontierV4Postgres(options) => {
            activate_traffic_frontier_v4_postgres(options).await
        }
        Command::RaiseTrafficFrontierLimitsPostgres(options) => {
            raise_traffic_frontier_limits_postgres(options).await
        }
        Command::TightenTrafficFrontierCorridorPostgres(options) => {
            tighten_traffic_frontier_corridor_postgres(options).await
        }
        Command::ExecutePostgres(options) => execute_postgres_once(options).await,
        Command::ExplicitExposureIncreasePostgres(options) => {
            explicit_exposure_increase_postgres_once(options).await
        }
        Command::ExplicitQuotaOverridePostgres(options) => {
            explicit_quota_override_postgres_once(options).await
        }
        Command::ExplicitResumeAfterDailyCapPostgres(options) => {
            explicit_resume_after_daily_cap_postgres_once(options).await
        }
        Command::Execute(options) => execute_once(options).await,
        Command::Auto(options) => auto_once(options).await,
    }
}
