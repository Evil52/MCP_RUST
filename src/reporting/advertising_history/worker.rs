//! One fenced historical request per quantum. The existing `WbClient` owns all
//! endpoint/token quotas; archive work also yields to the daily source queue.
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::{MissedTickBehavior, timeout};
use tokio_util::sync::CancellationToken;

use super::{
    HistoryRepository, last_closed_day, normalize_details, normalize_inventory,
    normalize_statistics,
};
use crate::{
    reporting::{
        collector_service::{ReportCollectorConfig, ReportCollectorMode},
        snapshot::Marketplace,
    },
    wb::WbError,
};

#[derive(Debug)]
pub struct HistoryClaim(ClaimFields);

impl HistoryClaim {
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.0.account_id
    }
    #[must_use]
    pub const fn lease_until(&self) -> DateTime<Utc> {
        self.0.lease_until
    }
}

#[derive(Debug, Deserialize)]
struct ClaimFields {
    job_id: i64,
    generation: i64,
    account_id: String,
    lease_until: DateTime<Utc>,
    task_id: Option<i64>,
    kind: Option<String>,
    date_from: Option<NaiveDate>,
    date_to: Option<NaiveDate>,
    campaign_ids: Option<Vec<u64>>,
}

pub async fn run(config: &ReportCollectorConfig, cancellation: CancellationToken) -> Result<()> {
    ensure!(
        config.mode() == ReportCollectorMode::Scheduled && config.policy().enabled,
        "history requires enabled scheduled collection"
    );
    let repository = HistoryRepository::connect_collector(config.database_config()).await?;
    let scope = config
        .collection_plan()
        .iter()
        .filter(|t| t.marketplace == Marketplace::Wildberries)
        .map(|t| t.account_id.clone())
        .collect::<Vec<_>>();
    ensure!(!scope.is_empty(), "history policy has no WB accounts");
    let owner = format!("wb-history-{}", std::process::id());
    let mut refreshed = None;
    let mut timer = tokio::time::interval(std::time::Duration::from_secs(5));
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! { biased; ()=cancellation.cancelled()=>break, _=timer.tick()=>{} }
        let closed = last_closed_day();
        if refreshed != Some(closed) {
            refresh_initialized(&repository, &scope, closed).await?;
            refreshed = Some(closed);
        }
        tokio::select! { biased; ()=cancellation.cancelled()=>break,
            result=run_quantum(config,&repository,&scope,&owner)=>{
                if let Err(error)=result { tracing::warn!(error=%error,"WB advertising history quantum failed"); }
            }
        }
    }
    Ok(())
}

async fn refresh_initialized(
    repository: &HistoryRepository,
    scope: &[String],
    date: NaiveDate,
) -> Result<()> {
    for account in scope {
        repository
            .execute(
                "SELECT daily_reporting.wb_history_refresh_recent($1,$2)",
                &[&account, &date],
            )
            .await?;
    }
    Ok(())
}

pub async fn run_quantum(
    config: &ReportCollectorConfig,
    repository: &HistoryRepository,
    scope: &[String],
    owner: &str,
) -> Result<()> {
    ensure!(
        scope.iter().all(|account| config
            .collection_plan()
            .iter()
            .any(|target| target.account_id == *account
                && target.marketplace == Marketplace::Wildberries)),
        "history worker scope outside collection policy"
    );
    let value = repository
        .json_query(
            "SELECT daily_reporting.wb_history_claim($1,$2)::text",
            &[&scope, &owner],
        )
        .await?;
    if value.is_null() {
        return Ok(());
    }
    let claim = HistoryClaim(serde_json::from_value(value).context("invalid history claim")?);
    let result = timeout(std::time::Duration::from_secs(90), fetch(config, &claim)).await;
    let (raw, normalized) = match result {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            let (code, delay, terminal) = failure(&error);
            repository
                .execute(
                    "SELECT daily_reporting.wb_history_defer($1,$2,$3,$4,$5,$6)",
                    &[
                        &claim.0.job_id,
                        &claim.0.generation,
                        &owner,
                        &code,
                        &delay,
                        &terminal,
                    ],
                )
                .await?;
            return Ok(());
        }
        Err(_) => {
            repository
                .execute(
                    "SELECT daily_reporting.wb_history_defer($1,$2,$3,'timeout',120,false)",
                    &[&claim.0.job_id, &claim.0.generation, &owner],
                )
                .await?;
            return Ok(());
        }
    };
    if let Err(error) = publish(repository, &claim, owner, &raw, &normalized).await {
        // A database outage/lost lease can also prevent deferral; in that case
        // the fenced lease remains reclaimable. Bound recoverable publication
        // failures instead of issuing WB reads indefinitely for invalid data.
        let _deferred = repository
            .execute(
                "SELECT daily_reporting.wb_history_defer($1,$2,$3,'publication_failed',120,false)",
                &[&claim.0.job_id, &claim.0.generation, &owner],
            )
            .await;
        return Err(error);
    }
    Ok(())
}

async fn publish(
    repository: &HistoryRepository,
    claim: &HistoryClaim,
    owner: &str,
    raw: &Value,
    normalized: &Value,
) -> Result<()> {
    if let Some(task) = claim.0.task_id {
        if claim.0.kind.as_deref() == Some("details") {
            repository
                .execute(
                    "SELECT daily_reporting.wb_history_details($1,$2,$3,$4,$5::text::jsonb)",
                    &[
                        &claim.0.job_id,
                        &claim.0.generation,
                        &owner,
                        &task,
                        &normalized.to_string(),
                    ],
                )
                .await?;
        } else {
            repository.execute("SELECT daily_reporting.wb_history_publish($1,$2,$3,$4,$5::text::jsonb,$6::text::jsonb)",
            &[&claim.0.job_id,&claim.0.generation,&owner,&task,&raw.to_string(),&normalized.to_string()]).await?;
        }
    } else {
        repository
            .execute(
                "SELECT daily_reporting.wb_history_inventory($1,$2,$3,$4::text::jsonb)",
                &[
                    &claim.0.job_id,
                    &claim.0.generation,
                    &owner,
                    &normalized.to_string(),
                ],
            )
            .await?;
    }
    Ok(())
}

async fn fetch(config: &ReportCollectorConfig, claim: &HistoryClaim) -> Result<(Value, Value)> {
    let client = config.resolve_wb_history(claim)?;
    if claim.0.task_id.is_none() {
        let raw = client.promotion_campaigns(&claim.0.account_id).await?;
        let normalized = normalize_inventory(&raw)?;
        Ok((raw, normalized))
    } else {
        let from = claim.0.date_from.context("missing history range")?;
        let to = claim.0.date_to.context("missing history range")?;
        let ids = claim
            .0
            .campaign_ids
            .as_ref()
            .context("missing history campaigns")?;
        let raw = if claim.0.kind.as_deref() == Some("details") {
            client
                .promotion_campaign_details(&claim.0.account_id, ids.clone(), Vec::new(), None)
                .await?
        } else {
            client
                .promotion_stats(
                    &claim.0.account_id,
                    ids.clone(),
                    from.to_string(),
                    to.to_string(),
                )
                .await?
        };
        let normalized = if claim.0.kind.as_deref() == Some("details") {
            normalize_details(&raw, ids)?
        } else {
            normalize_statistics(&raw, ids, from, to)?
        };
        Ok((raw, normalized))
    }
}

fn failure(error: &anyhow::Error) -> (&'static str, i32, bool) {
    error
        .downcast_ref::<WbError>()
        .map_or(("invalid_history_input", 120, true), |error| {
            let delay = match error {
                WbError::LocalRateLimited { retry_after }
                | WbError::RateLimited {
                    retry_after: Some(retry_after),
                    ..
                } => i32::try_from(
                    retry_after
                        .as_secs()
                        .saturating_add(1)
                        .clamp(20, 2_147_483_647),
                )
                .unwrap_or(i32::MAX),
                _ => 120,
            };
            let terminal = matches!(
                error,
                WbError::Unauthorized { .. }
                    | WbError::Forbidden { .. }
                    | WbError::MissingCredentials(_)
                    | WbError::EndpointNotAllowed { .. }
                    | WbError::InvalidArguments { .. }
            );
            (error.kind().code(), delay, terminal)
        })
}
