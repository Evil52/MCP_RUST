use super::super::{
    ActivatePolicyOptions, Config, PathBuf, Result, Utc, WbAutomationPolicy,
    WbAutomationPostgresStore, WbAutomationStateView, bail, build_observer, ensure,
};
use anyhow::Context;
use mcp_ozon::control::WbAutomationPacingMode;
use std::str::FromStr;

pub async fn adjust(options: ActivatePolicyOptions) -> Result<()> {
    let source = build_observer(&options.source)?;
    let target = build_observer(&super::super::ObserveOptions {
        policy: options.target_policy,
        registry: options.source.registry.clone(),
        reader_token: options.source.reader_token.clone(),
        state_directory: PathBuf::new(),
        allow_broad_reader: options.source.allow_broad_reader,
        reader_proxy_url: options.source.reader_proxy_url.clone(),
    })?;
    validate(source.policy(), target.policy())?;
    let now = Utc::now();
    ensure!(
        now >= target.policy().authorized_at && now < target.policy().authorization_expires_at,
        "WB traffic-frontier v4 corridor authorization is not active"
    );
    target
        .observe(now, WbAutomationStateView::default())
        .await
        .context("WB traffic-frontier v4 corridor read-only preflight failed")?;
    let database_url = std::env::var(super::super::DATABASE_URL_ENV)
        .context("WB automation PostgreSQL URL is unavailable")?;
    let database_config =
        Config::from_str(&database_url).context("WB automation PostgreSQL URL is invalid")?;
    let store = WbAutomationPostgresStore::connect(&database_config).await?;
    store.verify_runtime_contract().await?;
    let Some(mut lease) = store
        .try_acquire_campaign(
            target.policy().account_id.as_str(),
            target.policy().campaign_id,
        )
        .await?
    else {
        bail!("WB traffic-frontier v4 corridor campaign lock is contended");
    };
    let receipt = lease
        .activate_traffic_frontier_v4_corridor_policy(
            source.policy_sha256(),
            target.policy_sha256(),
            source.policy().min_bid_kopecks,
            target.policy().min_bid_kopecks,
            source.policy().max_bid_kopecks,
            target.policy().max_bid_kopecks,
        )
        .await?;
    lease.release().await?;
    println!(
        "{}",
        serde_json::json!({
            "account_id": target.policy().account_id,
            "campaign_id": target.policy().campaign_id,
            "outcome": if receipt.changed {
                "traffic_frontier_v4_corridor_adjusted"
            } else {
                "traffic_frontier_v4_corridor_already_active"
            },
            "state_revision": receipt.state_revision,
            "min_bid_kopecks": target.policy().min_bid_kopecks,
            "max_bid_kopecks": target.policy().max_bid_kopecks,
            "bid_writes_enabled": true,
        })
    );
    Ok(())
}

pub fn validate(source: &WbAutomationPolicy, target: &WbAutomationPolicy) -> Result<()> {
    let reviewed_source = matches!(
        (
            source.campaign_id,
            source.campaign_name.as_str(),
            source.authorization_reference.as_str(),
        ),
        (
            39_807_762,
            "Одуванчик",
            "chat/2026-08-28/oduvanchik-traffic-frontier-v4-drr-15",
        ) | (
            40_141_836,
            "Nexus",
            "chat/2026-09-14/nexus-funded-bids-and-automation-like-oduvanchik",
        )
    );
    ensure!(
        reviewed_source
            && source.account_id == "ofk_region_wb"
            && source.write_enabled
            && source.bid_writes_enabled
            && source.autonomous_pacing == WbAutomationPacingMode::TrafficFrontierV4
            && source.traffic_frontier_bid_kopecks == Some(700)
            && source.min_bid_kopecks <= 700
            && source.max_bid_kopecks == 1_050
            && target.authorization_reference
                == "chat/2026-09-15/oduvanchik-nexus-traffic-frontier-v4-7-12"
            && target.min_bid_kopecks == 700
            && target.max_bid_kopecks == 1_200,
        "WB traffic-frontier v4 7-12 corridor transition is outside the reviewed authorization"
    );
    let mut expected = source.clone();
    expected
        .authorization_reference
        .clone_from(&target.authorization_reference);
    expected.authorized_at = target.authorized_at;
    expected.authorization_expires_at = target.authorization_expires_at;
    expected.observe_until = target.observe_until;
    expected.min_bid_kopecks = 700;
    expected.max_bid_kopecks = 1_200;
    ensure!(
        target == &expected,
        "WB traffic-frontier v4 corridor transition changes an unapproved policy field"
    );
    Ok(())
}
