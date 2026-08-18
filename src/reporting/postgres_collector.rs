use std::collections::BTreeSet;

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Config, Transaction};

use crate::postgres::SupervisedClient;

use super::snapshot::{Marketplace, SnapshotDescriptor, SnapshotSource, SnapshotStatus};

const MAX_FACT_ROWS: usize = 25_000;
const MAX_COLLECTOR_VERSION_BYTES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollectedSalesFact {
    pub business_date: NaiveDate,
    pub sku: u64,
    pub ordered_units: u64,
    pub operational_gmv_minor: u64,
    /// `None` means the upstream Seller account did not grant this metric.
    pub cancelled_units: Option<u64>,
    /// `None` means the upstream Seller account did not grant this metric.
    pub returned_units: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollectedAdvertisingFact {
    pub business_date: NaiveDate,
    pub campaign_id: u64,
    pub sku: u64,
    pub impressions: u64,
    pub clicks: u64,
    pub spend_minor: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollectedStockFact {
    pub sku: u64,
    pub warehouse_id: String,
    pub sellable_units: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollectedPriceFact {
    pub sku: u64,
    pub price_minor: u64,
    pub old_price_minor: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "source", content = "facts", rename_all = "snake_case")]
pub enum CollectedFacts {
    Sales(Vec<CollectedSalesFact>),
    Advertising(Vec<CollectedAdvertisingFact>),
    Stocks(Vec<CollectedStockFact>),
    Prices(Vec<CollectedPriceFact>),
}

impl CollectedFacts {
    fn source(&self) -> SnapshotSource {
        match self {
            Self::Sales(_) => SnapshotSource::Sales,
            Self::Advertising(_) => SnapshotSource::Advertising,
            Self::Stocks(_) => SnapshotSource::Stocks,
            Self::Prices(_) => SnapshotSource::Prices,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Sales(facts) => facts.len(),
            Self::Advertising(facts) => facts.len(),
            Self::Stocks(facts) => facts.len(),
            Self::Prices(facts) => facts.len(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollectedSnapshot {
    account_id: String,
    marketplace: Marketplace,
    cutoff_at: DateTime<Utc>,
    source_as_of: DateTime<Utc>,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
    status: SnapshotStatus,
    pagination_complete: bool,
    collector_version: String,
    facts: CollectedFacts,
}

impl CollectedSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        account_id: String,
        marketplace: Marketplace,
        cutoff_at: DateTime<Utc>,
        source_as_of: DateTime<Utc>,
        period_start: DateTime<Utc>,
        period_end: DateTime<Utc>,
        status: SnapshotStatus,
        pagination_complete: bool,
        collector_version: String,
        facts: CollectedFacts,
    ) -> Result<Self, PostgresCollectorError> {
        let row_count =
            u32::try_from(facts.len()).map_err(|_| PostgresCollectorError::InvalidInput)?;
        if facts.len() > MAX_FACT_ROWS
            || collector_version.is_empty()
            || collector_version.len() > MAX_COLLECTOR_VERSION_BYTES
            || !collector_version
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
            || (status == SnapshotStatus::Succeeded && !pagination_complete)
        {
            return Err(PostgresCollectorError::InvalidInput);
        }
        SnapshotDescriptor::new(
            1,
            account_id.clone(),
            marketplace,
            facts.source(),
            cutoff_at,
            source_as_of,
            period_start,
            period_end,
            row_count,
            pagination_complete,
            status,
        )
        .map_err(|_| PostgresCollectorError::InvalidInput)?;
        validate_facts(&facts)?;
        Ok(Self {
            account_id,
            marketplace,
            cutoff_at,
            source_as_of,
            period_start,
            period_end,
            status,
            pagination_complete,
            collector_version,
            facts,
        })
    }
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum PostgresCollectorError {
    #[error("report snapshot input is invalid")]
    InvalidInput,
    #[error("report collector database is unavailable")]
    Unavailable,
    #[error("a report snapshot already exists for this account/source/cutoff")]
    Conflict,
}

pub struct PostgresSnapshotWriter {
    client: SupervisedClient,
}

impl PostgresSnapshotWriter {
    pub async fn connect(config: &Config) -> Result<Self, PostgresCollectorError> {
        let client = SupervisedClient::connect(config, "mcp-ozon-report-collector")
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        Ok(Self { client })
    }

    pub fn from_client(client: Client) -> Self {
        Self {
            client: SupervisedClient::preconnected(client, "mcp-ozon-report-collector"),
        }
    }

    pub async fn verify_runtime_contract(&self) -> Result<(), PostgresCollectorError> {
        // Checked before the guard is taken: the session mutex is not
        // reentrant, and this helper acquires it in its own right.
        self.client
            .verify_session_bounds()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let row = client
            .query_one(
                "SELECT current_user = 'report_collector' \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.source_snapshots', 'SELECT,INSERT') \
                    AND has_column_privilege(current_user, \
                        'daily_reporting.source_snapshots', 'status', 'UPDATE') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.sales_facts', 'INSERT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.advertising_facts', 'INSERT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.stock_facts', 'INSERT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.price_facts', 'INSERT') \
                    AND NOT has_table_privilege(current_user, \
                        'daily_reporting.delivery_batches', 'SELECT')",
                &[],
            )
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        row.get::<_, bool>(0)
            .then_some(())
            .ok_or(PostgresCollectorError::Unavailable)
    }

    /// Atomically appends facts and publishes their immutable snapshot.
    ///
    /// Any failure rolls back both the snapshot row and all facts. A duplicate
    /// account/source/cutoff identity fails closed instead of overwriting or
    /// silently reusing data from a previous collection attempt.
    pub async fn persist(
        &self,
        snapshot: &CollectedSnapshot,
    ) -> Result<i64, PostgresCollectorError> {
        Ok(self
            .persist_batch(std::slice::from_ref(snapshot))
            .await?
            .remove(0))
    }

    /// Persists a related group of report snapshots as one database unit.
    /// A failed source can therefore never publish only sales, stocks, or
    /// prices for one logical report cutoff.
    pub async fn persist_batch(
        &self,
        snapshots: &[CollectedSnapshot],
    ) -> Result<Vec<i64>, PostgresCollectorError> {
        if snapshots.is_empty() {
            return Err(PostgresCollectorError::InvalidInput);
        }
        let mut client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let mut snapshot_ids = Vec::with_capacity(snapshots.len());
        for snapshot in snapshots {
            snapshot_ids.push(persist_in_transaction(&transaction, snapshot).await?);
        }
        transaction
            .commit()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        Ok(snapshot_ids)
    }
}

async fn persist_in_transaction(
    transaction: &Transaction<'_>,
    snapshot: &CollectedSnapshot,
) -> Result<i64, PostgresCollectorError> {
    let payload = serde_json::to_vec(snapshot).map_err(|_| PostgresCollectorError::InvalidInput)?;
    let payload_sha256 = sha256(&payload);
    let row_count =
        i32::try_from(snapshot.facts.len()).map_err(|_| PostgresCollectorError::InvalidInput)?;
    let snapshot_id = insert_snapshot(transaction, snapshot).await?;
    insert_facts(transaction, snapshot_id, &snapshot.facts).await?;
    let status = match snapshot.status {
        SnapshotStatus::Succeeded => "succeeded",
        SnapshotStatus::Partial => "partial",
    };
    transaction
        .query_one(
            "UPDATE daily_reporting.source_snapshots \
                 SET status = $2, pagination_complete = $3, row_count = $4, \
                     payload_sha256 = $5, finished_at = clock_timestamp() \
                 WHERE id = $1 AND status = 'running' \
                 RETURNING id",
            &[
                &snapshot_id,
                &status,
                &snapshot.pagination_complete,
                &row_count,
                &payload_sha256,
            ],
        )
        .await
        .map_err(|_| PostgresCollectorError::Unavailable)?;
    Ok(snapshot_id)
}

async fn insert_snapshot(
    transaction: &Transaction<'_>,
    snapshot: &CollectedSnapshot,
) -> Result<i64, PostgresCollectorError> {
    let marketplace = match snapshot.marketplace {
        Marketplace::Ozon => "ozon",
        Marketplace::Wildberries => "wildberries",
    };
    let source = match snapshot.facts.source() {
        SnapshotSource::Sales => "sales",
        SnapshotSource::Advertising => "advertising",
        SnapshotSource::Stocks => "stocks",
        SnapshotSource::Prices => "prices",
    };
    let row = transaction
        .query_opt(
            "INSERT INTO daily_reporting.source_snapshots \
                (account_id, marketplace, source, cutoff_at, source_as_of, \
                 period_start, period_end, collector_version) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (account_id, marketplace, source, cutoff_at) DO NOTHING \
             RETURNING id",
            &[
                &snapshot.account_id,
                &marketplace,
                &source,
                &snapshot.cutoff_at,
                &snapshot.source_as_of,
                &snapshot.period_start,
                &snapshot.period_end,
                &snapshot.collector_version,
            ],
        )
        .await
        .map_err(|_| PostgresCollectorError::Unavailable)?;
    row.map(|row| row.get(0))
        .ok_or(PostgresCollectorError::Conflict)
}

async fn insert_facts(
    transaction: &Transaction<'_>,
    snapshot_id: i64,
    facts: &CollectedFacts,
) -> Result<(), PostgresCollectorError> {
    match facts {
        CollectedFacts::Sales(facts) => {
            for fact in facts {
                let cancelled_units = fact.cancelled_units.map(as_i32).transpose()?;
                let returned_units = fact.returned_units.map(as_i32).transpose()?;
                transaction
                    .execute(
                        "INSERT INTO daily_reporting.sales_facts \
                         (snapshot_id, business_date, sku, ordered_units, \
                          operational_gmv_minor, cancelled_units, returned_units) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7)",
                        &[
                            &snapshot_id,
                            &fact.business_date,
                            &as_i64(fact.sku)?,
                            &as_i32(fact.ordered_units)?,
                            &as_i64(fact.operational_gmv_minor)?,
                            &cancelled_units,
                            &returned_units,
                        ],
                    )
                    .await
                    .map_err(|_| PostgresCollectorError::Unavailable)?;
            }
        }
        CollectedFacts::Advertising(facts) => {
            for fact in facts {
                transaction
                    .execute(
                        "INSERT INTO daily_reporting.advertising_facts \
                         (snapshot_id, business_date, campaign_id, sku, impressions, clicks, \
                          spend_minor, attributed_orders, attributed_revenue_minor) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                        &[
                            &snapshot_id,
                            &fact.business_date,
                            &as_i64(fact.campaign_id)?,
                            &as_i64(fact.sku)?,
                            &as_i64(fact.impressions)?,
                            &as_i64(fact.clicks)?,
                            &as_i64(fact.spend_minor)?,
                            &as_i32(fact.attributed_orders)?,
                            &as_i64(fact.attributed_revenue_minor)?,
                        ],
                    )
                    .await
                    .map_err(|_| PostgresCollectorError::Unavailable)?;
            }
        }
        CollectedFacts::Stocks(facts) => {
            for fact in facts {
                transaction
                    .execute(
                        "INSERT INTO daily_reporting.stock_facts \
                         (snapshot_id, sku, warehouse_id, sellable_units) \
                         VALUES ($1, $2, $3, $4)",
                        &[
                            &snapshot_id,
                            &as_i64(fact.sku)?,
                            &fact.warehouse_id,
                            &as_i32(fact.sellable_units)?,
                        ],
                    )
                    .await
                    .map_err(|_| PostgresCollectorError::Unavailable)?;
            }
        }
        CollectedFacts::Prices(facts) => {
            for fact in facts {
                let old_price_minor = fact.old_price_minor.map(as_i64).transpose()?;
                transaction
                    .execute(
                        "INSERT INTO daily_reporting.price_facts \
                         (snapshot_id, sku, price_minor, old_price_minor) \
                         VALUES ($1, $2, $3, $4)",
                        &[
                            &snapshot_id,
                            &as_i64(fact.sku)?,
                            &as_i64(fact.price_minor)?,
                            &old_price_minor,
                        ],
                    )
                    .await
                    .map_err(|_| PostgresCollectorError::Unavailable)?;
            }
        }
    }
    Ok(())
}

fn validate_facts(facts: &CollectedFacts) -> Result<(), PostgresCollectorError> {
    match facts {
        CollectedFacts::Sales(facts) => ensure_unique(
            facts,
            |fact| (fact.business_date, fact.sku),
            |fact| {
                fact.sku > 0
                    && fits_i32(fact.ordered_units)
                    && fits_i64(fact.operational_gmv_minor)
                    && fact.cancelled_units.is_none_or(fits_i32)
                    && fact.returned_units.is_none_or(fits_i32)
            },
        ),
        CollectedFacts::Advertising(facts) => ensure_unique(
            facts,
            |fact| (fact.business_date, fact.campaign_id, fact.sku),
            |fact| {
                fact.campaign_id > 0
                    && fits_i64(fact.campaign_id)
                    && fits_i64(fact.sku)
                    && fits_i64(fact.impressions)
                    && fits_i64(fact.clicks)
                    && fact.clicks <= fact.impressions
                    && fits_i64(fact.spend_minor)
                    && fits_i32(fact.attributed_orders)
                    && fits_i64(fact.attributed_revenue_minor)
            },
        ),
        CollectedFacts::Stocks(facts) => ensure_unique(
            facts,
            |fact| (fact.sku, fact.warehouse_id.clone()),
            |fact| {
                fact.sku > 0
                    && fits_i64(fact.sku)
                    && fits_i32(fact.sellable_units)
                    && !fact.warehouse_id.is_empty()
                    && fact.warehouse_id.len() <= 128
                    && fact.warehouse_id.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
                    })
            },
        ),
        CollectedFacts::Prices(facts) => ensure_unique(
            facts,
            |fact| fact.sku,
            |fact| {
                fact.sku > 0
                    && fits_i64(fact.sku)
                    && fits_i64(fact.price_minor)
                    && fact
                        .old_price_minor
                        .is_none_or(|old| old >= fact.price_minor && fits_i64(old))
            },
        ),
    }
}

fn ensure_unique<T, K: Ord>(
    facts: &[T],
    key: impl Fn(&T) -> K,
    valid: impl Fn(&T) -> bool,
) -> Result<(), PostgresCollectorError> {
    let mut seen = BTreeSet::new();
    if facts
        .iter()
        .any(|fact| !valid(fact) || !seen.insert(key(fact)))
    {
        Err(PostgresCollectorError::InvalidInput)
    } else {
        Ok(())
    }
}

fn fits_i32(value: u64) -> bool {
    i32::try_from(value).is_ok()
}

fn fits_i64(value: u64) -> bool {
    i64::try_from(value).is_ok()
}

fn as_i32(value: u64) -> Result<i32, PostgresCollectorError> {
    i32::try_from(value).map_err(|_| PostgresCollectorError::InvalidInput)
}

fn as_i64(value: u64) -> Result<i64, PostgresCollectorError> {
    i64::try_from(value).map_err(|_| PostgresCollectorError::InvalidInput)
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::*;

    fn cutoff() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 17, 3, 0, 0).unwrap()
    }

    fn snapshot(facts: CollectedFacts) -> Result<CollectedSnapshot, PostgresCollectorError> {
        let source_as_of = cutoff() - Duration::minutes(10);
        let (period_start, period_end) = match facts.source() {
            SnapshotSource::Sales | SnapshotSource::Advertising => {
                (cutoff() - Duration::days(1), cutoff())
            }
            SnapshotSource::Stocks | SnapshotSource::Prices => (source_as_of, source_as_of),
        };
        CollectedSnapshot::new(
            "pilot".to_owned(),
            Marketplace::Ozon,
            cutoff(),
            source_as_of,
            period_start,
            period_end,
            SnapshotStatus::Succeeded,
            true,
            "test-1.0".to_owned(),
            facts,
        )
    }

    fn sales() -> CollectedSalesFact {
        CollectedSalesFact {
            business_date: cutoff().date_naive(),
            sku: 1,
            ordered_units: 2,
            operational_gmv_minor: 300,
            cancelled_units: Some(0),
            returned_units: Some(0),
        }
    }

    fn advertising() -> CollectedAdvertisingFact {
        CollectedAdvertisingFact {
            business_date: cutoff().date_naive(),
            campaign_id: 2,
            sku: 1,
            impressions: 100,
            clicks: 5,
            spend_minor: 20,
            attributed_orders: 1,
            attributed_revenue_minor: 80,
        }
    }

    #[test]
    fn every_fact_shape_and_partial_snapshot_validate() {
        assert!(snapshot(CollectedFacts::Sales(vec![sales()])).is_ok());
        assert!(snapshot(CollectedFacts::Advertising(vec![advertising()])).is_ok());
        assert!(
            snapshot(CollectedFacts::Stocks(vec![CollectedStockFact {
                sku: 1,
                warehouse_id: "fbo-msk:1".to_owned(),
                sellable_units: 3,
            }]))
            .is_ok()
        );
        assert!(
            snapshot(CollectedFacts::Prices(vec![CollectedPriceFact {
                sku: 1,
                price_minor: 100,
                old_price_minor: Some(120),
            }]))
            .is_ok()
        );
        let source_as_of = cutoff() - Duration::minutes(10);
        assert!(
            CollectedSnapshot::new(
                "wb-pilot".to_owned(),
                Marketplace::Wildberries,
                cutoff(),
                source_as_of,
                source_as_of,
                source_as_of,
                SnapshotStatus::Partial,
                false,
                "test".to_owned(),
                CollectedFacts::Stocks(Vec::new()),
            )
            .is_ok()
        );
    }

    #[test]
    fn snapshot_metadata_and_fact_bounds_fail_closed() {
        let mut duplicate = sales();
        duplicate.ordered_units = 3;
        assert_eq!(
            snapshot(CollectedFacts::Sales(vec![sales(), duplicate])),
            Err(PostgresCollectorError::InvalidInput)
        );

        let mut bad_ad = advertising();
        bad_ad.clicks = bad_ad.impressions + 1;
        assert!(snapshot(CollectedFacts::Advertising(vec![bad_ad])).is_err());

        for warehouse_id in ["", "bad warehouse"] {
            assert!(
                snapshot(CollectedFacts::Stocks(vec![CollectedStockFact {
                    sku: 1,
                    warehouse_id: warehouse_id.to_owned(),
                    sellable_units: 1,
                }]))
                .is_err()
            );
        }

        assert!(
            snapshot(CollectedFacts::Prices(vec![CollectedPriceFact {
                sku: 1,
                price_minor: 200,
                old_price_minor: Some(100),
            }]))
            .is_err()
        );

        let source_as_of = cutoff() - Duration::minutes(10);
        for (account, version, complete) in [
            ("bad account", "test", true),
            ("pilot", "bad version!", true),
            ("pilot", "test", false),
        ] {
            assert!(
                CollectedSnapshot::new(
                    account.to_owned(),
                    Marketplace::Ozon,
                    cutoff(),
                    source_as_of,
                    source_as_of,
                    source_as_of,
                    SnapshotStatus::Succeeded,
                    complete,
                    version.to_owned(),
                    CollectedFacts::Stocks(Vec::new()),
                )
                .is_err()
            );
        }
    }

    #[test]
    fn integer_conversion_helpers_reject_overflow() {
        assert_eq!(as_i32(i32::MAX as u64).unwrap(), i32::MAX);
        assert_eq!(as_i64(i64::MAX as u64).unwrap(), i64::MAX);
        assert_eq!(
            as_i32(i32::MAX as u64 + 1),
            Err(PostgresCollectorError::InvalidInput)
        );
        assert_eq!(
            as_i64(i64::MAX as u64 + 1),
            Err(PostgresCollectorError::InvalidInput)
        );
    }
}
