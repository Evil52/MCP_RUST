use super::{claim_owner, shutdown_signal};
use anyhow::{Result, ensure};
use chrono::Utc;
use mcp_ozon::reporting::{
    collector_service::{ReportCollectorConfig, ReportCollectorMode},
    postgres_collector::PostgresSnapshotWriter,
    snapshot::Marketplace,
};
use std::sync::Arc;
use tokio::time::{Duration, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

pub async fn run_independent_sources(
    config: &ReportCollectorConfig,
    writer: &Arc<PostgresSnapshotWriter>,
) -> Result<()> {
    let cancellation = CancellationToken::new();
    let signal = cancellation.clone();
    let task = tokio::spawn(async move {
        shutdown_signal().await;
        signal.cancel();
    });
    let owner = claim_owner("source");
    let mut planned_at = Utc::now() - chrono::Duration::minutes(1);
    let mut timer = tokio::time::interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! { biased; ()=cancellation.cancelled()=>break, _=timer.tick()=>{} }
        let now = Utc::now();
        if now - planned_at >= chrono::Duration::seconds(60) {
            mcp_ozon::reporting::source_collection::enqueue_recent(config, writer, now).await?;
            planned_at = now;
        }
        // Cancellation drops the in-flight read; its short fenced lease can be
        // reclaimed after restart. Saved pages are unaffected.
        tokio::select! {
            biased;
            ()=cancellation.cancelled()=>break,
            result=mcp_ozon::reporting::source_collection::run_quantum(config,writer,&owner)=>{
                if let Err(error)=result {tracing::warn!(error=%error,"source collection quantum failed");}
            }
        }
    }
    task.abort();
    Ok(())
}

pub async fn run_history(config: &ReportCollectorConfig, preflight: bool) -> Result<()> {
    ensure!(
        config.mode() == ReportCollectorMode::Scheduled && config.policy().enabled,
        "history requires enabled scheduled collection"
    );
    ensure!(
        config
            .collection_plan()
            .iter()
            .any(|t| t.marketplace == Marketplace::Wildberries),
        "history policy has no WB accounts"
    );
    if preflight {
        mcp_ozon::reporting::advertising_history::HistoryRepository::connect_collector(
            config.database_config(),
        )
        .await?;
        return Ok(());
    }
    let cancellation = CancellationToken::new();
    let signal = cancellation.clone();
    let task = tokio::spawn(async move {
        shutdown_signal().await;
        signal.cancel();
    });
    let result = mcp_ozon::reporting::advertising_history::worker::run(config, cancellation).await;
    task.abort();
    result
}
