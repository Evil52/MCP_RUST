use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Config, Transaction, types::ToSql};

use crate::postgres::SupervisedClient;

use super::{
    BatchPlan, BatchStatus, CircuitReason, MeasurementRecord, MonitorTarget, PersistOutcome,
    PersistedOutcome, PersistenceBatch, PlacementKind, PositionRepository, RepositoryError,
};

const SOURCE: &str = "ozon_public_search";

/// Transactional PostgreSQL implementation of the collector persistence boundary.
///
/// The connection is deliberately supplied as a parsed `Config`, so this type
/// never logs or returns a database URL containing credentials.
pub struct PostgresRepository {
    client: SupervisedClient,
}

impl PostgresRepository {
    pub async fn connect(config: &Config) -> Result<Self, RepositoryError> {
        let client = SupervisedClient::connect(config, "mcp-ozon-position-collector")
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        Ok(Self { client })
    }

    pub fn from_client(client: Client) -> Self {
        Self {
            client: SupervisedClient::preconnected(client, "mcp-ozon-position-collector"),
        }
    }

    /// Verifies the exact least-privilege database contract required by the
    /// disabled runtime without reading marketplace history.
    pub async fn verify_runtime_contract(&self) -> Result<(), RepositoryError> {
        // Checked before the guard is taken: the session mutex is not
        // reentrant, and this helper acquires it in its own right.
        self.client
            .verify_session_bounds()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        let row = client
            .query_one(
                "SELECT current_user = 'position_collector', \
                        has_table_privilege(current_user, \
                            'search_position.monitors', 'SELECT'), \
                        has_table_privilege(current_user, \
                            'search_position.collection_runs', 'INSERT'), \
                        NOT has_table_privilege(current_user, \
                            'search_position.measurements', 'SELECT'), \
                        EXISTS ( \
                            SELECT 1 FROM information_schema.columns \
                            WHERE table_schema = 'search_position' \
                              AND table_name = 'collection_runs' \
                              AND column_name = 'payload_digest' \
                        )",
                &[],
            )
            .await?;
        let valid = (0..5).all(|index| row.get::<_, bool>(index));
        if valid {
            Ok(())
        } else {
            Err(RepositoryError::Unavailable)
        }
    }

    /// Loads the single active monitor allowed during the manual canary phase.
    ///
    /// This method performs no marketplace request and returns no raw database
    /// row. More than one active monitor fails closed so a canary cannot
    /// accidentally expand into a scheduled batch.
    pub async fn load_canary_plan(
        &self,
        slot: DateTime<Utc>,
    ) -> Result<BatchPlan, RepositoryError> {
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        let rows = client
            .query(
                "SELECT id, store_id, product_id, search_phrase, region_code, \
                        region_name, interval_minutes, max_position \
                 FROM search_position.monitors \
                 WHERE active \
                 ORDER BY id \
                 LIMIT 2",
                &[],
            )
            .await?;
        if rows.len() != 1 {
            return Err(RepositoryError::CanaryTargetCount);
        }
        let row = &rows[0];
        build_canary_plan(
            slot,
            row.get::<_, i64>(0),
            row.get::<_, String>(1),
            row.get::<_, String>(2),
            row.get::<_, String>(3),
            row.get::<_, String>(4),
            row.get::<_, String>(5),
            row.get::<_, i16>(6),
            row.get::<_, i16>(7),
        )
    }

    async fn persist_inner(
        &self,
        batch: &PersistenceBatch,
    ) -> Result<PersistOutcome, RepositoryError> {
        let digest = payload_digest(batch);
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        let transaction = client.transaction().await?;

        let inserted = transaction
            .query_opt(
                "INSERT INTO search_position.collection_runs (\
                    source, scheduled_for, started_at, status, monitors_planned, \
                    queries_planned, collector_version, payload_digest\
                 ) VALUES ($1, $2, $3, 'running', $4, $5, $6, $7) \
                 ON CONFLICT (source, scheduled_for) DO NOTHING \
                 RETURNING id",
                &[
                    &SOURCE,
                    &batch.scheduled_for(),
                    &batch.started_at(),
                    &to_i32(batch.monitors_planned())?,
                    &to_i32(batch.queries_planned())?,
                    &batch.collector_version(),
                    &digest,
                ],
            )
            .await?;

        let Some(row) = inserted else {
            return finish_existing(transaction, batch.scheduled_for(), &digest).await;
        };
        let run_id: i64 = row.get(0);

        insert_measurements(&transaction, run_id, batch.measurements()).await?;
        if let Some(reason) = batch.circuit_reason() {
            transaction
                .execute(
                    "SELECT search_position.open_ozon_collector_circuit($1, $2)",
                    &[&run_id, &reason.as_str()],
                )
                .await?;
        }
        transaction
            .execute(
                "UPDATE search_position.collection_runs SET \
                    finished_at = $2, status = $3, monitors_attempted = $4, \
                    monitors_succeeded = $5, queries_attempted = $6, \
                    queries_succeeded = $7, error_class = $8, http_status = $9 \
                 WHERE id = $1",
                &[
                    &run_id,
                    &batch.finished_at(),
                    &status_text(batch.status()),
                    &to_i32(batch.monitors_attempted())?,
                    &to_i32(batch.monitors_succeeded())?,
                    &to_i32(batch.queries_attempted())?,
                    &to_i32(batch.queries_succeeded())?,
                    &batch.error_class().map(|value| value.as_str()),
                    &batch.http_status().map(|value| {
                        i16::try_from(value).expect("HTTP status fits PostgreSQL int2")
                    }),
                ],
            )
            .await?;
        transaction.commit().await?;
        Ok(PersistOutcome::Inserted)
    }
}

#[allow(clippy::too_many_arguments)]
fn build_canary_plan(
    slot: DateTime<Utc>,
    monitor_id: i64,
    store_id: String,
    product_id: String,
    search_phrase: String,
    region_code: String,
    region_name: String,
    interval_minutes: i16,
    max_position: i16,
) -> Result<BatchPlan, RepositoryError> {
    if interval_minutes != 30 {
        return Err(RepositoryError::InvalidCanaryTarget);
    }
    let max_position =
        u16::try_from(max_position).map_err(|_| RepositoryError::InvalidCanaryTarget)?;
    let target = MonitorTarget::new(
        monitor_id,
        store_id,
        product_id,
        search_phrase,
        region_code,
        region_name,
        max_position,
    )
    .map_err(|_| RepositoryError::InvalidCanaryTarget)?;
    BatchPlan::new(slot, vec![target]).map_err(|_| RepositoryError::InvalidCanaryTarget)
}

impl PositionRepository for PostgresRepository {
    fn persist<'a>(
        &'a self,
        batch: &'a PersistenceBatch,
    ) -> Pin<Box<dyn Future<Output = Result<PersistOutcome, RepositoryError>> + Send + 'a>> {
        Box::pin(self.persist_inner(batch))
    }
}

async fn finish_existing(
    transaction: Transaction<'_>,
    scheduled_for: DateTime<Utc>,
    expected_digest: &str,
) -> Result<PersistOutcome, RepositoryError> {
    let existing = transaction
        .query_opt(
            "SELECT payload_digest FROM search_position.collection_runs \
             WHERE source = $1 AND scheduled_for = $2",
            &[&SOURCE, &scheduled_for],
        )
        .await?;
    let matches = existing
        .as_ref()
        .is_some_and(|row| row.get::<_, &str>(0) == expected_digest);
    transaction.commit().await?;
    if matches {
        Ok(PersistOutcome::AlreadyExists)
    } else {
        Err(RepositoryError::SlotConflict)
    }
}

async fn insert_measurements(
    transaction: &Transaction<'_>,
    run_id: i64,
    measurements: &[MeasurementRecord],
) -> Result<(), RepositoryError> {
    let statement = transaction
        .prepare(
            "INSERT INTO search_position.measurements (\
                run_id, monitor_id, observed_at, outcome, overall_position, \
                placement, organic_position, sponsored_position\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .await?;
    for measurement in measurements {
        let values: [&(dyn ToSql + Sync); 8] = [
            &run_id,
            &measurement.monitor_id(),
            &measurement.observed_at(),
            &outcome_text(measurement.outcome()),
            &measurement.overall_position().map(i32::from),
            &measurement.placement().map(placement_text),
            &measurement.organic_position().map(i32::from),
            &measurement.sponsored_position().map(i32::from),
        ];
        transaction.execute(&statement, &values).await?;
    }
    Ok(())
}

fn status_text(status: BatchStatus) -> &'static str {
    match status {
        BatchStatus::Succeeded => "succeeded",
        BatchStatus::Partial => "partial",
        BatchStatus::Failed => "failed",
        BatchStatus::Blocked => "blocked",
    }
}

fn outcome_text(outcome: PersistedOutcome) -> &'static str {
    match outcome {
        PersistedOutcome::Found => "found",
        PersistedOutcome::NotFound => "not_found",
    }
}

fn placement_text(placement: PlacementKind) -> &'static str {
    match placement {
        PlacementKind::Organic => "organic",
        PlacementKind::Sponsored => "sponsored",
        PlacementKind::Unknown => "unknown",
    }
}

fn to_i32(value: usize) -> Result<i32, RepositoryError> {
    i32::try_from(value).map_err(|_| RepositoryError::Unavailable)
}

fn payload_digest(batch: &PersistenceBatch) -> String {
    let mut digest = Sha256::new();
    digest.update(b"mcp-ozon-position-persistence-v1\0");
    hash_i64(&mut digest, batch.scheduled_for().timestamp_micros());
    hash_i64(&mut digest, batch.started_at().timestamp_micros());
    hash_i64(&mut digest, batch.finished_at().timestamp_micros());
    hash_text(&mut digest, batch.collector_version());
    hash_text(&mut digest, status_text(batch.status()));
    for value in [
        batch.monitors_planned(),
        batch.monitors_attempted(),
        batch.monitors_succeeded(),
        batch.queries_planned(),
        batch.queries_attempted(),
        batch.queries_succeeded(),
    ] {
        hash_u64(
            &mut digest,
            u64::try_from(value).expect("usize always fits u64"),
        );
    }
    hash_optional_text(&mut digest, batch.error_class().map(|value| value.as_str()));
    hash_optional_u16(&mut digest, batch.http_status());
    hash_optional_text(
        &mut digest,
        batch.circuit_reason().map(CircuitReason::as_str),
    );

    let mut measurements = batch.measurements().iter().collect::<Vec<_>>();
    measurements.sort_unstable_by_key(|item| item.monitor_id());
    hash_u64(
        &mut digest,
        u64::try_from(measurements.len()).expect("usize always fits u64"),
    );
    for measurement in measurements {
        hash_i64(&mut digest, measurement.monitor_id());
        hash_i64(&mut digest, measurement.observed_at().timestamp_micros());
        hash_text(&mut digest, outcome_text(measurement.outcome()));
        hash_optional_u16(&mut digest, measurement.overall_position());
        hash_optional_text(&mut digest, measurement.placement().map(placement_text));
        hash_optional_u16(&mut digest, measurement.organic_position());
        hash_optional_u16(&mut digest, measurement.sponsored_position());
    }
    let bytes = digest.finalize();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn hash_text(digest: &mut Sha256, value: &str) {
    hash_u64(
        digest,
        u64::try_from(value.len()).expect("usize always fits u64"),
    );
    digest.update(value.as_bytes());
}

fn hash_optional_text(digest: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            digest.update([1]);
            hash_text(digest, value);
        }
        None => digest.update([0]),
    }
}

fn hash_optional_u16(digest: &mut Sha256, value: Option<u16>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        None => digest.update([0]),
    }
}

fn hash_i64(digest: &mut Sha256, value: i64) {
    digest.update(value.to_be_bytes());
}

fn hash_u64(digest: &mut Sha256, value: u64) {
    digest.update(value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{build_canary_plan, outcome_text, placement_text, status_text, to_i32};
    use crate::position_collector::{
        BatchStatus, PersistedOutcome, PlacementKind, RepositoryError,
    };

    #[test]
    fn database_enums_and_integer_bounds_are_exact() {
        assert_eq!(status_text(BatchStatus::Succeeded), "succeeded");
        assert_eq!(status_text(BatchStatus::Partial), "partial");
        assert_eq!(status_text(BatchStatus::Failed), "failed");
        assert_eq!(status_text(BatchStatus::Blocked), "blocked");
        assert_eq!(outcome_text(PersistedOutcome::Found), "found");
        assert_eq!(outcome_text(PersistedOutcome::NotFound), "not_found");
        assert_eq!(placement_text(PlacementKind::Organic), "organic");
        assert_eq!(placement_text(PlacementKind::Sponsored), "sponsored");
        assert_eq!(placement_text(PlacementKind::Unknown), "unknown");
        assert_eq!(to_i32(i32::MAX as usize), Ok(i32::MAX));
        assert!(to_i32(i32::MAX as usize + 1).is_err());
    }

    #[test]
    fn canary_row_conversion_enforces_the_fixed_contract() {
        let slot = Utc.with_ymd_and_hms(2026, 8, 16, 7, 30, 0).unwrap();
        let build = |interval, max_position, phrase: &str| {
            build_canary_plan(
                slot,
                1,
                "store-1".to_owned(),
                "3411079879".to_owned(),
                phrase.to_owned(),
                "moscow".to_owned(),
                "Москва".to_owned(),
                interval,
                max_position,
            )
        };
        assert_eq!(build(30, 100, "ручка кнопка").unwrap().target_count(), 1);
        assert_eq!(
            build(15, 100, "ручка кнопка"),
            Err(RepositoryError::InvalidCanaryTarget)
        );
        assert_eq!(
            build(30, -1, "ручка кнопка"),
            Err(RepositoryError::InvalidCanaryTarget)
        );
        assert_eq!(
            build(30, 100, "bad\nphrase"),
            Err(RepositoryError::InvalidCanaryTarget)
        );
    }
}
