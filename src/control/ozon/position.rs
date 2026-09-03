use std::str::FromStr;

use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio_postgres::{Client, Config, config::Host};

use crate::postgres::SupervisedClient;

use super::OzonPositionSignal;

const COMPONENT: &str = "mcp-ozon-ozon-bid-position-reader";

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonBidPositionReadError {
    #[error("Ozon bid position reader configuration is invalid")]
    InvalidConfiguration,
    #[error("Ozon bid position reader database is unavailable")]
    Unavailable,
    #[error("Ozon bid position monitor selection is ambiguous")]
    AmbiguousTarget,
    #[error("Ozon bid position snapshot is invalid")]
    InvalidSnapshot,
}

pub struct OzonBidPositionReader {
    client: SupervisedClient,
}

impl OzonBidPositionReader {
    pub async fn connect(database_url: &str) -> Result<Self, OzonBidPositionReadError> {
        let mut config = Config::from_str(database_url)
            .map_err(|_| OzonBidPositionReadError::InvalidConfiguration)?;
        validate_database_config(&config)?;
        crate::postgres::harden(&mut config, COMPONENT);
        let client = SupervisedClient::connect(&config, COMPONENT)
            .await
            .map_err(|_| OzonBidPositionReadError::Unavailable)?;
        Ok(Self { client })
    }

    #[must_use]
    pub fn from_client(client: Client) -> Self {
        Self {
            client: SupervisedClient::preconnected(client, COMPONENT),
        }
    }

    pub async fn verify_runtime_contract(&self) -> Result<(), OzonBidPositionReadError> {
        self.client
            .verify_session_bounds()
            .await
            .map_err(|_| OzonBidPositionReadError::Unavailable)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonBidPositionReadError::Unavailable)?;
        let row = client
            .query_one(
                "SELECT current_user = 'position_reader', \
                        current_setting('default_transaction_read_only') = 'on', \
                        has_table_privilege(current_user, \
                            'search_position.monitors', 'SELECT'), \
                        has_table_privilege(current_user, \
                            'search_position.latest_measurements', 'SELECT'), \
                        NOT has_table_privilege(current_user, \
                            'search_position.measurements', 'SELECT')",
                &[],
            )
            .await
            .map_err(|_| OzonBidPositionReadError::Unavailable)?;
        drop(client);
        if (0..5).all(|index| row.get::<_, bool>(index)) {
            Ok(())
        } else {
            Err(OzonBidPositionReadError::Unavailable)
        }
    }

    pub async fn latest_position(
        &self,
        store_id: &str,
        sku: u64,
        region_name: &str,
    ) -> Result<Option<OzonPositionSignal>, OzonBidPositionReadError> {
        validate_lookup(store_id, sku, region_name)?;
        let product_id = sku.to_string();
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| OzonBidPositionReadError::Unavailable)?;
        let rows = client
            .query(
                "SELECT latest.observed_at, latest.outcome, latest.overall_position, \
                        monitor.max_position, latest.run_status, latest.is_partial \
                 FROM search_position.monitors AS monitor \
                 LEFT JOIN search_position.latest_measurements AS latest \
                   ON latest.monitor_id = monitor.id \
                 WHERE monitor.active \
                   AND monitor.store_id = $1 \
                   AND monitor.product_id = $2 \
                   AND monitor.region_name = $3 \
                 ORDER BY monitor.id \
                 LIMIT 2",
                &[&store_id, &product_id, &region_name],
            )
            .await
            .map_err(|_| OzonBidPositionReadError::Unavailable)?;
        drop(client);
        match rows.as_slice() {
            [] => Ok(None),
            [_first, _second] => Err(OzonBidPositionReadError::AmbiguousTarget),
            [row] => position_from_row(row),
            _ => unreachable!("query is bounded to two rows"),
        }
    }
}

fn position_from_row(
    row: &tokio_postgres::Row,
) -> Result<Option<OzonPositionSignal>, OzonBidPositionReadError> {
    let Some(observed_at) = row.get::<_, Option<DateTime<Utc>>>(0) else {
        return Ok(None);
    };
    let outcome = row
        .get::<_, Option<String>>(1)
        .ok_or(OzonBidPositionReadError::InvalidSnapshot)?;
    let overall_position = row.get::<_, Option<i32>>(2);
    let max_position = row.get::<_, i16>(3);
    let run_status = row
        .get::<_, Option<String>>(4)
        .ok_or(OzonBidPositionReadError::InvalidSnapshot)?;
    let is_partial = row
        .get::<_, Option<bool>>(5)
        .ok_or(OzonBidPositionReadError::InvalidSnapshot)?;
    if run_status != "succeeded" || is_partial || max_position <= 0 {
        return Err(OzonBidPositionReadError::InvalidSnapshot);
    }
    let position = match (outcome.as_str(), overall_position) {
        ("found", Some(position)) if position > 0 && position <= i32::from(max_position) => {
            u16::try_from(position).map_err(|_| OzonBidPositionReadError::InvalidSnapshot)?
        }
        ("not_found", None) => u16::try_from(max_position)
            .ok()
            .and_then(|position| position.checked_add(1))
            .ok_or(OzonBidPositionReadError::InvalidSnapshot)?,
        _ => return Err(OzonBidPositionReadError::InvalidSnapshot),
    };
    Ok(Some(OzonPositionSignal {
        observed_at,
        position,
    }))
}

fn validate_database_config(config: &Config) -> Result<(), OzonBidPositionReadError> {
    if config.get_user() != Some("position_reader")
        || config.get_password().is_none_or(<[u8]>::is_empty)
        || config.get_dbname().is_none_or(str::is_empty)
        || config.get_hosts().len() != 1
        || !matches!(config.get_hosts(), [Host::Tcp(host)] if !host.trim().is_empty())
        || config.get_options().is_some()
    {
        return Err(OzonBidPositionReadError::InvalidConfiguration);
    }
    Ok(())
}

fn validate_lookup(
    store_id: &str,
    sku: u64,
    region_name: &str,
) -> Result<(), OzonBidPositionReadError> {
    let valid_text = |value: &str, maximum: usize| {
        !value.is_empty()
            && value.len() <= maximum
            && value.trim() == value
            && !value.chars().any(char::is_control)
    };
    if sku == 0 || !valid_text(store_id, 128) || !valid_text(region_name, 128) {
        return Err(OzonBidPositionReadError::InvalidConfiguration);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_url_is_restricted_to_the_read_only_position_role() {
        assert!(
            validate_database_config(
                &Config::from_str("postgresql://position_reader:secret@position-db/ozon_positions")
                    .unwrap()
            )
            .is_ok()
        );
        for raw in [
            "postgresql://position_admin:secret@position-db/ozon_positions",
            "postgresql://position_reader@position-db/ozon_positions",
            "host=/tmp user=position_reader password=secret dbname=ozon_positions",
            "postgresql://position_reader:secret@one,two/ozon_positions",
        ] {
            let config = Config::from_str(raw).unwrap();
            assert_eq!(
                validate_database_config(&config),
                Err(OzonBidPositionReadError::InvalidConfiguration)
            );
        }
    }

    #[test]
    fn position_lookup_identity_is_bounded() {
        assert!(validate_lookup("furnitura_dlya_doma", 1, "Москва").is_ok());
        for (store, sku, region) in [
            ("", 1, "Москва"),
            ("furnitura_dlya_doma", 0, "Москва"),
            ("furnitura_dlya_doma", 1, " Москва"),
            ("furnitura_dlya_doma\n", 1, "Москва"),
        ] {
            assert_eq!(
                validate_lookup(store, sku, region),
                Err(OzonBidPositionReadError::InvalidConfiguration)
            );
        }
    }
}
