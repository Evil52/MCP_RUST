use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use serde_json::{Value, json};

use super::{
    CollectedAdvertisingExpenseFact, CollectedFacts, CollectedSnapshot, CollectionClaim,
    CollectionTarget, DateTime, Duration, Marketplace, PostgresCollectorError,
    PostgresSnapshotWriter, SnapshotSource, SnapshotStatus, Utc, map_snapshot_insert_error,
    marketplace_name, parse_marketplace, persist_snapshot_contents, require_claim_completed,
    snapshot_source_name, validate_coverage_targets, validate_error_class, validate_owner_id,
};
use crate::reporting::checkpoint::{CheckpointError, Checkpoints, JournalFuture, PageJournal};

#[derive(Debug, Clone)]
pub struct SourceJobClaim {
    lease: CollectionClaim,
    pub source: SnapshotSource,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
    pub consecutive_failures: u32,
}

impl SourceJobClaim {
    #[cfg(test)]
    #[must_use]
    pub(crate) fn for_test(lease: CollectionClaim, source: SnapshotSource) -> Self {
        Self {
            period_start: lease.cutoff_at - Duration::days(1),
            period_end: lease.cutoff_at,
            lease,
            source,
            consecutive_failures: 0,
        }
    }

    /// Credential resolution accepts this fenced lease; publication uses the
    /// source-job methods, never the legacy account-batch persistence method.
    #[must_use]
    pub const fn credential_claim(&self) -> &CollectionClaim {
        &self.lease
    }
    #[must_use]
    pub fn account_id(&self) -> &str {
        self.lease.account_id()
    }
    #[must_use]
    pub const fn marketplace(&self) -> Marketplace {
        self.lease.marketplace()
    }
    #[must_use]
    pub const fn cutoff_at(&self) -> DateTime<Utc> {
        self.lease.cutoff_at
    }
}

impl PostgresSnapshotWriter {
    pub async fn dispatch_source_refreshes(
        &self,
        targets: &[CollectionTarget],
    ) -> Result<(), PostgresCollectorError> {
        validate_coverage_targets(targets)?;
        if targets.is_empty() {
            return Ok(());
        }
        let scope=Value::Array(targets.iter().map(|t| json!({"account_id":t.account_id,"marketplace":marketplace_name(t.marketplace)})).collect());
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        client
            .execute(
                "SELECT daily_reporting.dispatch_source_refreshes($1::text::jsonb)",
                &[&scope.to_string()],
            )
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        Ok(())
    }

    pub async fn verify_source_job_contract(&self) -> Result<(), PostgresCollectorError> {
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        client.prepare("SELECT id, source, period_start, period_end FROM daily_reporting.source_collection_jobs LIMIT 0")
            .await.map_err(|_| PostgresCollectorError::Unavailable)?;
        client.prepare("SELECT payload FROM daily_reporting.source_collection_pages WHERE job_id=$1 AND request_key=$2")
            .await.map_err(|_| PostgresCollectorError::Unavailable)?;
        let row=client.query_one("SELECT has_function_privilege(current_user, 'daily_reporting.claim_source_collection(jsonb,text)', 'EXECUTE') AND NOT has_table_privilege(current_user, 'daily_reporting.source_collection_jobs','UPDATE')", &[])
            .await.map_err(|_| PostgresCollectorError::Unavailable)?;
        if row.get::<_, bool>(0) {
            Ok(())
        } else {
            Err(PostgresCollectorError::Unavailable)
        }
    }

    pub async fn enqueue_source_jobs(
        &self,
        targets: &[CollectionTarget],
        cutoff: DateTime<Utc>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<(), PostgresCollectorError> {
        validate_coverage_targets(targets)?;
        if start >= end || start > cutoff || cutoff > end + Duration::days(2) {
            return Err(PostgresCollectorError::InvalidInput);
        }
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        for target in targets {
            for source in &target.sources {
                tx.execute(
                    "SELECT daily_reporting.enqueue_source_collection($1,$2,$3,$4,$5,$6)",
                    &[
                        &target.account_id,
                        &marketplace_name(target.marketplace),
                        &snapshot_source_name(*source),
                        &cutoff,
                        &start,
                        &end,
                    ],
                )
                .await
                .map_err(|_| PostgresCollectorError::Unavailable)?;
            }
        }
        tx.commit()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)
    }

    pub async fn claim_source_job(
        &self,
        targets: &[CollectionTarget],
        owner: &str,
    ) -> Result<Option<SourceJobClaim>, PostgresCollectorError> {
        validate_coverage_targets(targets)?;
        validate_owner_id(owner)?;
        if targets.is_empty() {
            return Ok(None);
        }
        let scope=Value::Array(targets.iter().map(|t| json!({"account_id":t.account_id,"marketplace":marketplace_name(t.marketplace)})).collect());
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let row = client
            .query_opt(
                "SELECT * FROM daily_reporting.claim_source_collection($1::text::jsonb,$2)",
                &[&scope.to_string(), &owner],
            )
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        row.map(|row| {
            Ok(SourceJobClaim {
                lease: CollectionClaim {
                    id: row.get("id"),
                    generation: row.get("generation"),
                    account_id: row.get("account_id"),
                    marketplace: parse_marketplace(row.get("marketplace"))?,
                    cutoff_at: row.get("cutoff_at"),
                    owner_id: row.get("owner_id"),
                    lease_until: row.get("lease_until"),
                },
                source: match row.get::<_, &str>("source") {
                    "sales" => SnapshotSource::Sales,
                    "stocks" => SnapshotSource::Stocks,
                    "prices" => SnapshotSource::Prices,
                    "advertising" => SnapshotSource::Advertising,
                    "finance" => SnapshotSource::Finance,
                    _ => return Err(PostgresCollectorError::Unavailable),
                },
                period_start: row.get("period_start"),
                period_end: row.get("period_end"),
                consecutive_failures: u32::try_from(row.get::<_, i32>("consecutive_failures"))
                    .map_err(|_| PostgresCollectorError::Unavailable)?,
            })
        })
        .transpose()
    }

    pub fn source_checkpoints(self: &Arc<Self>, claim: &SourceJobClaim) -> Checkpoints {
        Some(Arc::new(PostgresPageJournal {
            writer: Arc::clone(self),
            claim: claim.clone(),
            admitted: AtomicBool::new(false),
        }))
    }

    pub async fn defer_source_job(
        &self,
        claim: &SourceJobClaim,
        error: Option<&str>,
        delay_seconds: u32,
        terminal: bool,
    ) -> Result<(), PostgresCollectorError> {
        if let Some(error) = error {
            validate_error_class(error)?;
        }
        let delay =
            i32::try_from(delay_seconds).map_err(|_| PostgresCollectorError::InvalidInput)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let c = &claim.lease;
        let row = client
            .query_one(
                "SELECT daily_reporting.defer_source_collection($1,$2,$3,$4,$5,$6)",
                &[&c.id, &c.generation, &c.owner_id, &error, &delay, &terminal],
            )
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        require_claim_completed(row.get(0))
    }

    pub async fn publish_source_job(
        &self,
        claim: &SourceJobClaim,
        facts: CollectedFacts,
        expenses: Vec<CollectedAdvertisingExpenseFact>,
        version: &str,
    ) -> Result<i64, PostgresCollectorError> {
        if facts.source() != claim.source {
            return Err(super::sales_validation::reject_metadata(
                claim.source,
                "source_mismatch",
            ));
        }
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let tx = client
            .transaction()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let c = &claim.lease;
        let row = tx
            .query_opt(
                "SELECT * FROM daily_reporting.lock_source_publication($1,$2,$3)",
                &[&c.id, &c.generation, &c.owner_id],
            )
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?
            .ok_or(PostgresCollectorError::ClaimLost)?;
        let first: DateTime<Utc> = row.get::<_, Option<_>>(0).ok_or_else(|| {
            super::sales_validation::reject_metadata(claim.source, "missing_first_observation")
        })?;
        let observed: DateTime<Utc> = row.get::<_, Option<_>>(1).ok_or_else(|| {
            super::sales_validation::reject_metadata(claim.source, "missing_last_observation")
        })?;
        let period = matches!(
            claim.source,
            SnapshotSource::Sales | SnapshotSource::Advertising | SnapshotSource::Finance
        );
        let mut snapshot = CollectedSnapshot::new(
            c.account_id.clone(),
            c.marketplace,
            c.cutoff_at,
            observed,
            if period { claim.period_start } else { observed },
            if period { claim.period_end } else { observed },
            SnapshotStatus::Succeeded,
            true,
            version.to_owned(),
            facts,
        )?;
        if !expenses.is_empty() {
            snapshot = snapshot.with_advertising_expenses(expenses)?;
        }
        let row=tx.query_one("INSERT INTO daily_reporting.source_snapshots(account_id,marketplace,source,cutoff_at,source_as_of,period_start,period_end,collector_version,source_job_id,source_job_generation,started_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id",
            &[&c.account_id,&marketplace_name(c.marketplace),&snapshot_source_name(claim.source),&c.cutoff_at,&observed,&snapshot.period_start,&snapshot.period_end,&version,&c.id,&c.generation,&first])
            .await.map_err(|error| map_snapshot_insert_error(&error))?;
        let id = persist_snapshot_contents(&tx, row.get(0), &snapshot).await?;
        let done = tx
            .query_one(
                "SELECT daily_reporting.finish_source_collection($1,$2,$3)",
                &[&c.id, &c.generation, &c.owner_id],
            )
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        require_claim_completed(done.get(0))?;
        tx.commit()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        Ok(id)
    }
}

struct PostgresPageJournal {
    writer: Arc<PostgresSnapshotWriter>,
    claim: SourceJobClaim,
    admitted: AtomicBool,
}
impl PageJournal for PostgresPageJournal {
    fn load<'a>(&'a self, key: &'a str) -> JournalFuture<'a, Option<Value>> {
        Box::pin(async move {
            let client = self
                .writer
                .client
                .acquire()
                .await
                .map_err(|_| CheckpointError::Unavailable)?;
            let c = &self.claim.lease;
            let row=client.query_opt("SELECT p.payload::text FROM daily_reporting.source_collection_jobs j LEFT JOIN daily_reporting.source_collection_pages p ON p.job_id=j.id AND p.request_key=$4 WHERE j.id=$1 AND j.generation=$2 AND j.owner_id=$3 AND j.status='running' AND j.lease_until>clock_timestamp() AND j.deadline_at>clock_timestamp()", &[&c.id,&c.generation,&c.owner_id,&key])
                .await.map_err(|_| CheckpointError::Unavailable)?.ok_or(CheckpointError::Unavailable)?;
            row.get::<_, Option<String>>(0)
                .map(|value| serde_json::from_str(&value).map_err(|_| CheckpointError::Invalid))
                .transpose()
        })
    }
    fn admit(&self) -> JournalFuture<'_, ()> {
        Box::pin(async move {
            if self.admitted.swap(true, Ordering::SeqCst) {
                return Err(CheckpointError::Deferred);
            }
            let client = self
                .writer
                .client
                .acquire()
                .await
                .map_err(|_| CheckpointError::Unavailable)?;
            let c = &self.claim.lease;
            let row = client
                .query_one(
                    "SELECT daily_reporting.admit_source_page($1,$2,$3)",
                    &[&c.id, &c.generation, &c.owner_id],
                )
                .await
                .map_err(|_| CheckpointError::Unavailable)?;
            if row.get::<_, bool>(0) {
                Ok(())
            } else {
                Err(CheckpointError::Deferred)
            }
        })
    }
    fn save<'a>(&'a self, key: &'a str, page: Value) -> JournalFuture<'a, ()> {
        Box::pin(async move {
            let client = self
                .writer
                .client
                .acquire()
                .await
                .map_err(|_| CheckpointError::Unavailable)?;
            let c = &self.claim.lease;
            client
                .execute(
                    "SELECT daily_reporting.save_source_page($1,$2,$3,$4,$5::text::jsonb)",
                    &[&c.id, &c.generation, &c.owner_id, &key, &page.to_string()],
                )
                .await
                .map_err(|error| {
                    if error.code()
                        == Some(&tokio_postgres::error::SqlState::PROGRAM_LIMIT_EXCEEDED)
                    {
                        CheckpointError::Invalid
                    } else {
                        CheckpointError::Unavailable
                    }
                })?;
            Ok(())
        })
    }
}
