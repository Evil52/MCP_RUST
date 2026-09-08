//! One fair, resumable page quantum followed by independent source publication.
use anyhow::{Result, anyhow};
use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;
use tokio::time::timeout;

use super::{
    ReportKey, ReportKind, business_date,
    collector_service::ReportCollectorConfig,
    ozon_performance_source::{OzonPerformanceReportSource, PerformanceClientReportTransport},
    ozon_source::{OzonClientReportTransport, OzonReportSource},
    postgres_collector::{
        CollectedAdvertisingExpenseFact, CollectedFacts, PostgresSnapshotWriter, SourceJobClaim,
    },
    report_cutoff, reporting_interval,
    snapshot::{Marketplace, SnapshotSource},
    wb_source::{WbClientReportTransport, WbReportSource},
};

#[derive(Debug, Clone, Copy)]
pub struct SourceFailure {
    pub code: &'static str,
    pub retry_after: Option<u64>,
}
impl From<&'static str> for SourceFailure {
    fn from(code: &'static str) -> Self {
        Self {
            code,
            retry_after: None,
        }
    }
}

/// Reconstruct only occurrences within the existing truthful 24-hour recovery
/// contract. First/last observation times remain actual collection times.
pub async fn enqueue_recent(
    config: &ReportCollectorConfig,
    writer: &PostgresSnapshotWriter,
    now: DateTime<Utc>,
) -> Result<()> {
    writer
        .dispatch_source_refreshes(config.collection_plan())
        .await?;
    for local_date in [business_date(now), business_date(now - Duration::days(1))] {
        for kind in [ReportKind::Morning, ReportKind::Evening] {
            let key = ReportKey {
                local_date,
                kind,
                recipient_id: "collector".to_owned(),
                report_version: 1,
            };
            let cutoff = report_cutoff(&key)?;
            if cutoff <= now && now < cutoff + Duration::hours(24) {
                let (start, end) = reporting_interval(&key)?;
                writer
                    .enqueue_source_jobs(config.collection_plan(), cutoff, start, end)
                    .await?;
            }
        }
    }
    Ok(())
}

pub async fn run_quantum(
    config: &ReportCollectorConfig,
    writer: &Arc<PostgresSnapshotWriter>,
    owner: &str,
) -> Result<bool> {
    let Some(claim) = writer
        .claim_source_job(config.collection_plan(), owner)
        .await?
    else {
        return Ok(false);
    };
    let outcome = timeout(
        std::time::Duration::from_secs(100),
        collect(config, writer, &claim),
    )
    .await
    .unwrap_or_else(|_| Err(SourceFailure::from("timeout")));
    complete_quantum(writer, &claim, outcome).await?;
    Ok(true)
}

type SourceFacts = (CollectedFacts, Vec<CollectedAdvertisingExpenseFact>);

async fn complete_quantum(
    writer: &PostgresSnapshotWriter,
    claim: &SourceJobClaim,
    outcome: Result<SourceFacts, SourceFailure>,
) -> Result<()> {
    match outcome {
        Ok((facts, expenses)) => {
            match writer
                .publish_source_job(claim, facts, expenses, env!("CARGO_PKG_VERSION"))
                .await
            {
                Ok(id) => {
                    tracing::info!(account_id=claim.account_id(),source=?claim.source,snapshot_id=id,"independent source snapshot published");
                }
                Err(super::postgres_collector::PostgresCollectorError::ClaimLost) => {
                    return Ok(());
                }
                Err(error) => {
                    let terminal =
                        error != super::postgres_collector::PostgresCollectorError::Unavailable;
                    writer
                        .defer_source_job(
                            claim,
                            Some(if terminal {
                                "invalid_source_publication"
                            } else {
                                "database_unavailable"
                            }),
                            65,
                            terminal,
                        )
                        .await?;
                    tracing::warn!(account_id=claim.account_id(),source=?claim.source,terminal,"source publication deferred");
                }
            }
        }
        Err(SourceFailure {
            code: "checkpoint_deferred",
            ..
        }) => writer.defer_source_job(claim, None, 1, false).await?,
        Err(failure) => {
            let code = failure.code;
            let retry = retryable(code) && failure.retry_after.is_none_or(|s| s <= 86400);
            let delay = 65_u32
                .saturating_mul(1 << claim.consecutive_failures.min(4))
                .min(900)
                .max(
                    u32::try_from(failure.retry_after.unwrap_or(0))
                        .unwrap_or(i32::MAX as u32)
                        .min(i32::MAX as u32),
                );
            writer
                .defer_source_job(claim, Some(code), delay, !retry)
                .await?;
            tracing::warn!(account_id=claim.account_id(),source=?claim.source,error_class=code,retry,"source deferred; other sources remain available");
        }
    }
    Ok(())
}

fn retryable(code: &str) -> bool {
    matches!(
        code,
        "timeout"
            | "rate_limited"
            | "network_error"
            | "transport_error"
            | "local_overloaded"
            | "token_endpoint_cooldown"
            | "upstream_http_error"
            | "upstream_server_error"
            | "checkpoint_unavailable"
    )
}

async fn collect(
    config: &ReportCollectorConfig,
    writer: &Arc<PostgresSnapshotWriter>,
    claim: &SourceJobClaim,
) -> Result<(CollectedFacts, Vec<CollectedAdvertisingExpenseFact>), SourceFailure> {
    let checkpoints = writer.source_checkpoints(claim);
    let from = business_date(claim.period_start);
    let to = business_date(claim.period_end - Duration::microseconds(1));
    let facts = match claim.marketplace() {
        Marketplace::Ozon if claim.source == SnapshotSource::Advertising => {
            let (client, store) = config
                .source_performance(claim)
                .map_err(|_| "credentials_unavailable")?;
            let source = OzonPerformanceReportSource::new(PerformanceClientReportTransport::new(
                client, store,
            ))
            .with_checkpoints(checkpoints);
            let facts = source
                .collect_extended(from)
                .await
                .map_err(|e| e.failure())?;
            return Ok((
                CollectedFacts::Advertising(facts.advertising),
                facts.expenses,
            ));
        }
        Marketplace::Ozon => {
            let (client, store) = config
                .source_seller(claim)
                .map_err(|_| "credentials_unavailable")?;
            let source = OzonReportSource::new(
                OzonClientReportTransport::new(client, store).with_durable_retry(),
            )
            .with_checkpoints(checkpoints);
            match claim.source {
                SnapshotSource::Sales => CollectedFacts::Sales(
                    source
                        .collect_sales_pages(from, to)
                        .await
                        .map_err(|e| e.failure())?,
                ),
                SnapshotSource::Stocks => CollectedFacts::Stocks(
                    source
                        .collect_stock_pages()
                        .await
                        .map_err(|e| e.failure())?,
                ),
                SnapshotSource::Prices => CollectedFacts::Prices(
                    source
                        .collect_price_pages()
                        .await
                        .map_err(|e| e.failure())?,
                ),
                SnapshotSource::Finance => CollectedFacts::Finance(
                    source
                        .collect_finance_pages(from, to)
                        .await
                        .map_err(|e| e.failure())?,
                ),
                SnapshotSource::Advertising => return Err("source_invalid".into()),
            }
        }
        Marketplace::Wildberries => {
            let (client, account) = config
                .resolve_wb_scheduled(claim.credential_claim())
                .map_err(|_| "credentials_unavailable")?;
            let source = WbReportSource::new(WbClientReportTransport::new(client, account))
                .with_checkpoints(checkpoints);
            match claim.source {
                SnapshotSource::Sales => CollectedFacts::Sales(
                    source
                        .collect_sales_pages(from)
                        .await
                        .map_err(|e| e.failure())?,
                ),
                SnapshotSource::Stocks => CollectedFacts::Stocks(
                    source
                        .collect_stock_pages()
                        .await
                        .map_err(|e| e.failure())?,
                ),
                SnapshotSource::Prices => CollectedFacts::Prices(
                    source
                        .collect_price_pages()
                        .await
                        .map_err(|e| e.failure())?,
                ),
                SnapshotSource::Advertising => CollectedFacts::Advertising(
                    source
                        .collect_advertising(from)
                        .await
                        .map_err(|e| e.failure())?,
                ),
                SnapshotSource::Finance => return Err("source_invalid".into()),
            }
        }
    };
    Ok((facts, Vec::new()))
}

pub async fn require_enabled(
    config: &ReportCollectorConfig,
    writer: &PostgresSnapshotWriter,
) -> Result<()> {
    if config.mode() != super::collector_service::ReportCollectorMode::Scheduled
        || !config.policy().enabled
    {
        return Err(anyhow!(
            "independent sources require scheduled mode and an explicitly enabled policy"
        ));
    }
    writer.verify_source_job_contract().await?;
    Ok(())
}

#[cfg(test)]
mod tests;
