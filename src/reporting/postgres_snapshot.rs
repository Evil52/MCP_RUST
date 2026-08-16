use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tokio_postgres::{Client, Config, NoTls};

use super::snapshot::{
    AccountScope, FrozenSnapshotManifest, Marketplace, SnapshotDescriptor, SnapshotSource,
    SnapshotStatus,
};

const MAX_MANIFEST_ACCOUNTS: usize = 64;

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum PostgresSnapshotError {
    #[error("published report snapshots are unavailable")]
    Unavailable,
    #[error("published report snapshots violate the frozen manifest contract")]
    InvalidManifest,
}

pub struct PostgresSnapshotRepository {
    client: Mutex<Client>,
}

impl PostgresSnapshotRepository {
    pub async fn connect(config: &Config) -> Result<Self, PostgresSnapshotError> {
        let (client, connection) = config
            .connect(NoTls)
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        std::mem::drop(tokio::spawn(connection));
        Ok(Self::from_client(client))
    }

    pub fn from_client(client: Client) -> Self {
        Self {
            client: Mutex::new(client),
        }
    }

    pub async fn verify_runtime_contract(&self) -> Result<(), PostgresSnapshotError> {
        let client = self.client.lock().await;
        let row = client
            .query_one(
                "SELECT current_user = 'report_worker' \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.published_source_snapshots', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.published_sales_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.published_advertising_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.published_stock_facts', 'SELECT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.published_price_facts', 'SELECT') \
                    AND NOT has_table_privilege(current_user, \
                        'daily_reporting.source_snapshots', 'SELECT')",
                &[],
            )
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        row.get::<_, bool>(0)
            .then_some(())
            .ok_or(PostgresSnapshotError::Unavailable)
    }

    pub async fn load_manifest(
        &self,
        cutoff_at: DateTime<Utc>,
        accounts: Vec<AccountScope>,
    ) -> Result<FrozenSnapshotManifest, PostgresSnapshotError> {
        if accounts.is_empty() || accounts.len() > MAX_MANIFEST_ACCOUNTS {
            return Err(PostgresSnapshotError::InvalidManifest);
        }
        let account_ids = accounts
            .iter()
            .map(|account| account.account_id().to_owned())
            .collect::<Vec<_>>();
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT id, account_id, marketplace, source, cutoff_at, source_as_of, \
                        period_start, period_end, row_count, pagination_complete, status \
                 FROM daily_reporting.published_source_snapshots \
                 WHERE cutoff_at = $1 AND account_id::text = ANY($2::text[]) \
                 ORDER BY account_id, source",
                &[&cutoff_at, &account_ids],
            )
            .await
            .map_err(|_| PostgresSnapshotError::Unavailable)?;
        let snapshots = rows
            .into_iter()
            .map(|row| {
                let row_count = u32::try_from(row.get::<_, i32>(8))
                    .map_err(|_| PostgresSnapshotError::InvalidManifest)?;
                SnapshotDescriptor::new(
                    row.get(0),
                    row.get(1),
                    parse_marketplace(row.get(2))?,
                    parse_source(row.get(3))?,
                    row.get(4),
                    row.get(5),
                    row.get(6),
                    row.get(7),
                    row_count,
                    row.get(9),
                    parse_status(row.get(10))?,
                )
                .map_err(|_| PostgresSnapshotError::InvalidManifest)
            })
            .collect::<Result<Vec<_>, _>>()?;
        FrozenSnapshotManifest::new(cutoff_at, accounts, snapshots)
            .map_err(|_| PostgresSnapshotError::InvalidManifest)
    }
}

fn parse_marketplace(value: &str) -> Result<Marketplace, PostgresSnapshotError> {
    match value {
        "ozon" => Ok(Marketplace::Ozon),
        "wildberries" => Ok(Marketplace::Wildberries),
        _ => Err(PostgresSnapshotError::InvalidManifest),
    }
}

fn parse_source(value: &str) -> Result<SnapshotSource, PostgresSnapshotError> {
    match value {
        "sales" => Ok(SnapshotSource::Sales),
        "advertising" => Ok(SnapshotSource::Advertising),
        "stocks" => Ok(SnapshotSource::Stocks),
        "prices" => Ok(SnapshotSource::Prices),
        _ => Err(PostgresSnapshotError::InvalidManifest),
    }
}

fn parse_status(value: &str) -> Result<SnapshotStatus, PostgresSnapshotError> {
    match value {
        "succeeded" => Ok(SnapshotStatus::Succeeded),
        "partial" => Ok(SnapshotStatus::Partial),
        _ => Err(PostgresSnapshotError::InvalidManifest),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Marketplace, PostgresSnapshotError, SnapshotSource, SnapshotStatus, parse_marketplace,
        parse_source, parse_status,
    };

    #[test]
    fn database_text_mappings_are_exact_and_fail_closed() {
        assert_eq!(parse_marketplace("ozon"), Ok(Marketplace::Ozon));
        assert_eq!(
            parse_marketplace("wildberries"),
            Ok(Marketplace::Wildberries)
        );
        assert_eq!(parse_source("sales"), Ok(SnapshotSource::Sales));
        assert_eq!(parse_source("advertising"), Ok(SnapshotSource::Advertising));
        assert_eq!(parse_source("stocks"), Ok(SnapshotSource::Stocks));
        assert_eq!(parse_source("prices"), Ok(SnapshotSource::Prices));
        assert_eq!(parse_status("succeeded"), Ok(SnapshotStatus::Succeeded));
        assert_eq!(parse_status("partial"), Ok(SnapshotStatus::Partial));
        for invalid in ["Ozon", "orders", "running"] {
            let error = match invalid {
                "Ozon" => parse_marketplace(invalid).map(|_| ()),
                "orders" => parse_source(invalid).map(|_| ()),
                _ => parse_status(invalid).map(|_| ()),
            };
            assert_eq!(error, Err(PostgresSnapshotError::InvalidManifest));
        }
    }
}
