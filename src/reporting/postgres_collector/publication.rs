//! Publication proofs for the legacy claimed-batch scheduler.
//!
//! A target is published for a cutoff only when every source its marketplace
//! requires has a succeeded, complete snapshot there. Snapshots are joined on
//! the exact required source: an optional source recorded at the same cutoff,
//! such as the WB seller-warehouse stocks the independent collector adds,
//! neither stands in for a missing required source nor makes a complete
//! target look over-counted.

use std::collections::BTreeSet;

use super::{
    COLLECTION_CANARY_MAX_AGE, CollectionActivationReceipt, CollectionTarget, DateTime,
    Marketplace, PostgresCollectorError, PostgresSnapshotWriter, Utc, marketplace_name,
    parse_marketplace, snapshot_source_name, validate_coverage_targets,
};

/// Aligned columns with one row per required (account, marketplace, source).
/// `required` repeats the owning target's number of required sources.
#[derive(Debug, Default, PartialEq, Eq)]
struct RequiredSources {
    account_ids: Vec<String>,
    marketplaces: Vec<String>,
    sources: Vec<String>,
    required: Vec<i64>,
}

impl RequiredSources {
    fn of(targets: &[CollectionTarget]) -> Result<Self, PostgresCollectorError> {
        let mut columns = Self::default();
        for target in targets {
            let required = i64::try_from(target.sources.len())
                .map_err(|_| PostgresCollectorError::InvalidInput)?;
            for source in &target.sources {
                columns.account_ids.push(target.account_id.clone());
                columns
                    .marketplaces
                    .push(marketplace_name(target.marketplace).to_owned());
                columns
                    .sources
                    .push(snapshot_source_name(*source).to_owned());
                columns.required.push(required);
            }
        }
        Ok(columns)
    }

    fn total(&self) -> Result<i64, PostgresCollectorError> {
        i64::try_from(self.sources.len()).map_err(|_| PostgresCollectorError::InvalidInput)
    }
}

impl PostgresSnapshotWriter {
    /// Returns policy targets whose exact cutoff already has every required
    /// terminal published source snapshot for its marketplace.
    ///
    /// The bounded result is used before marketplace I/O. A partial account
    /// set is not considered published, so the scheduler can fail closed on
    /// the existing uniqueness conflict instead of silently treating an
    /// incomplete report as complete.
    pub async fn published_targets(
        &self,
        cutoff_at: DateTime<Utc>,
        targets: &[CollectionTarget],
    ) -> Result<BTreeSet<(String, Marketplace)>, PostgresCollectorError> {
        validate_coverage_targets(targets)?;
        if targets.is_empty() {
            return Ok(BTreeSet::new());
        }
        let required = RequiredSources::of(targets)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let rows = client
            .query(
                "WITH required(account_id, marketplace, source, required_count) AS ( \
                     SELECT * FROM unnest($2::text[], $3::text[], $4::text[], $5::bigint[]) \
                 ) \
                 SELECT snapshot.account_id, snapshot.marketplace \
                 FROM daily_reporting.source_snapshots AS snapshot \
                 JOIN required \
                   ON required.account_id = snapshot.account_id::text \
                  AND required.marketplace = snapshot.marketplace::text \
                  AND required.source = snapshot.source::text \
                 WHERE snapshot.cutoff_at = $1 \
                   AND snapshot.status = 'succeeded' \
                   AND snapshot.pagination_complete \
                 GROUP BY snapshot.account_id, snapshot.marketplace \
                 HAVING count(*) = max(required.required_count) \
                    AND count(DISTINCT snapshot.source) = max(required.required_count) \
                 ORDER BY snapshot.account_id, snapshot.marketplace",
                &[
                    &cutoff_at,
                    &required.account_ids,
                    &required.marketplaces,
                    &required.sources,
                    &required.required,
                ],
            )
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        rows.into_iter()
            .map(|row| {
                let marketplace = parse_marketplace(row.get(1))?;
                Ok((row.get(0), marketplace))
            })
            .collect()
    }

    /// Requires one recent common complete-source cutoff for every policy target.
    ///
    /// A live policy is expanded only after each account has completed the
    /// same operator-reviewed occurrence. The proof contains no credentials
    /// and performs no marketplace I/O. Normal scheduled publications can
    /// subsequently serve as the same bounded restart proof.
    pub async fn verify_collection_activation(
        &self,
        targets: &[CollectionTarget],
        now: DateTime<Utc>,
    ) -> Result<CollectionActivationReceipt, PostgresCollectorError> {
        validate_coverage_targets(targets)?;
        if targets.is_empty() {
            return Err(PostgresCollectorError::InvalidInput);
        }
        let oldest_allowed = now
            .checked_sub_signed(COLLECTION_CANARY_MAX_AGE)
            .ok_or(PostgresCollectorError::InvalidInput)?;
        let required = RequiredSources::of(targets)?;
        let required_rows = required.total()?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let row = client
            .query_opt(
                "WITH required(account_id, marketplace, source) AS ( \
                     SELECT * FROM unnest($3::text[], $4::text[], $5::text[]) \
                 ) \
                 SELECT snapshot.cutoff_at \
                 FROM daily_reporting.source_snapshots AS snapshot \
                 JOIN required \
                   ON required.account_id = snapshot.account_id::text \
                  AND required.marketplace = snapshot.marketplace::text \
                  AND required.source = snapshot.source::text \
                 WHERE snapshot.cutoff_at BETWEEN $1 AND $2 \
                   AND snapshot.status = 'succeeded' \
                   AND snapshot.pagination_complete \
                 GROUP BY snapshot.cutoff_at \
                 HAVING count(*) = $6 \
                    AND count(DISTINCT (snapshot.account_id, snapshot.marketplace, snapshot.source)) = $6 \
                 ORDER BY snapshot.cutoff_at DESC \
                 LIMIT 1",
                &[
                    &oldest_allowed,
                    &now,
                    &required.account_ids,
                    &required.marketplaces,
                    &required.sources,
                    &required_rows,
                ],
            )
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let cutoff_at = row
            .map(|row| row.get(0))
            .ok_or(PostgresCollectorError::CanaryMissing)?;
        Ok(CollectionActivationReceipt {
            cutoff_at,
            target_count: u16::try_from(targets.len())
                .map_err(|_| PostgresCollectorError::InvalidInput)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reporting::snapshot::SnapshotSource;

    fn target(account_id: &str, marketplace: Marketplace) -> CollectionTarget {
        CollectionTarget {
            account_id: account_id.to_owned(),
            marketplace,
            sources: SnapshotSource::required_for(marketplace).to_vec(),
        }
    }

    #[test]
    fn required_sources_are_exact_aligned_triples_without_optional_sources() {
        let columns = RequiredSources::of(&[
            target("wb", Marketplace::Wildberries),
            target("ozon", Marketplace::Ozon),
        ])
        .unwrap();
        assert_eq!(
            columns.sources,
            [
                "sales",
                "advertising",
                "stocks",
                "prices",
                "sales",
                "advertising",
                "finance",
                "stocks",
                "prices",
            ]
        );
        assert!(
            !columns
                .sources
                .iter()
                .any(|source| source == "seller_stocks")
        );
        assert_eq!(columns.required, [4, 4, 4, 4, 5, 5, 5, 5, 5]);
        assert_eq!(columns.account_ids[..4], ["wb"; 4]);
        assert_eq!(columns.marketplaces[4..], ["ozon"; 5]);
        assert_eq!(columns.total(), Ok(9));
        assert_eq!(
            RequiredSources::of(&[]).unwrap(),
            RequiredSources::default()
        );
    }
}
