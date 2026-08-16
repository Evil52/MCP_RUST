use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio_postgres::{Client, Config, NoTls, Transaction, types::ToSql};

use super::{
    BatchStatus, CircuitReason, MeasurementRecord, PersistOutcome, PersistedOutcome,
    PersistenceBatch, PlacementKind, PositionRepository, RepositoryError,
};

const SOURCE: &str = "ozon_public_search";

/// Transactional PostgreSQL implementation of the collector persistence boundary.
///
/// The connection is deliberately supplied as a parsed `Config`, so this type
/// never logs or returns a database URL containing credentials.
pub struct PostgresRepository {
    client: Mutex<Client>,
}

impl PostgresRepository {
    pub async fn connect(config: &Config) -> Result<Self, RepositoryError> {
        let (client, connection) = config.connect(NoTls).await?;
        std::mem::drop(tokio::spawn(connection));
        Ok(Self {
            client: Mutex::new(client),
        })
    }

    pub fn from_client(client: Client) -> Self {
        Self {
            client: Mutex::new(client),
        }
    }

    async fn persist_inner(
        &self,
        batch: &PersistenceBatch,
    ) -> Result<PersistOutcome, RepositoryError> {
        let digest = payload_digest(batch);
        let mut client = self.client.lock().await;
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
    use super::{outcome_text, placement_text, status_text, to_i32};
    use crate::position_collector::{BatchStatus, PersistedOutcome, PlacementKind};

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
}
