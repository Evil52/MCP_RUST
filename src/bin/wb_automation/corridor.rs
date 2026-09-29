use super::super::{
    Config, ObserveOptions, PathBuf, Result, Utc, WbAutomationPostgresStore, WbAutomationStateView,
    build_observer,
};
use anyhow::{Context, ensure};
use mcp_ozon::control::validate_wb_automation_corridor_update;
use std::str::FromStr;

pub async fn run(args: &[String]) -> Result<()> {
    ensure!(
        (args.len() == 7 || args.len() == 8)
            && args.last().map(String::as_str) == Some("--confirm-authorized-corridor"),
        "usage: wb-automation authorize-corridor-pg SOURCE.json TARGET.json ACCESS.json READ_TOKEN true|false [READER_PROXY] --confirm-authorized-corridor"
    );
    let allow_broad_reader = match args[5].as_str() {
        "true" => true,
        "false" => false,
        _ => anyhow::bail!("allow-broad-reader must be true or false"),
    };
    let mut options = ObserveOptions {
        policy: PathBuf::from(&args[1]),
        registry: PathBuf::from(&args[3]),
        reader_token: PathBuf::from(&args[4]),
        state_directory: PathBuf::new(),
        allow_broad_reader,
        reader_proxy_url: (args.len() == 8).then(|| args[6].clone()),
    };
    let source = build_observer(&options)?;
    options.policy = PathBuf::from(&args[2]);
    let target = build_observer(&options)?;
    validate_wb_automation_corridor_update(source.policy(), target.policy(), Utc::now())?;
    let database_url = std::env::var(super::super::DATABASE_URL_ENV)
        .context("WB automation PostgreSQL URL is unavailable")?;
    let config =
        Config::from_str(&database_url).context("WB automation PostgreSQL URL is invalid")?;
    let store = WbAutomationPostgresStore::connect(&config).await?;
    store.verify_runtime_contract().await?;
    let mut lease = store
        .try_acquire_campaign(&target.policy().account_id, target.policy().campaign_id)
        .await?
        .context("WB authorized corridor campaign lock is contended")?;
    // Pure preflight; the later scheduled write loads real durable guards and
    // re-observes WB through the normal executor.
    let snapshot = target
        .observe(Utc::now(), WbAutomationStateView::default())
        .await
        .context("WB authorized corridor read-only preflight failed")?;
    let receipt = lease
        .authorize_corridor_policy(source.policy(), target.policy(), Utc::now())
        .await?;
    lease.release().await?;
    println!(
        "{}",
        serde_json::json!({
            "account_id": target.policy().account_id, "campaign_id": target.policy().campaign_id,
            "outcome": if receipt.changed { "authorized_corridor_adjusted" } else { "authorized_corridor_already_active" },
            "state_revision": receipt.state_revision,
            "source_policy_sha256": source.policy_sha256(), "target_policy_sha256": target.policy_sha256(),
            "authorization_reference": target.policy().authorization_reference,
            "authorization_expires_at": target.policy().authorization_expires_at,
            "min_bid_kopecks": target.policy().min_bid_kopecks, "max_bid_kopecks": target.policy().max_bid_kopecks,
            "preflight_decision": snapshot.decision,
        })
    );
    Ok(())
}
