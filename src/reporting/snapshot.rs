use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use thiserror::Error;

const MAX_ACCOUNTS: usize = 64;
const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_POST_CUTOFF_OBSERVATION_DELAY: Duration = Duration::minutes(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Marketplace {
    Ozon,
    Wildberries,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SnapshotSource {
    Sales,
    Advertising,
    Stocks,
    Prices,
}

impl SnapshotSource {
    pub(crate) const ALL: [Self; 4] = [Self::Sales, Self::Advertising, Self::Stocks, Self::Prices];

    fn freshness_sla(self) -> Duration {
        match self {
            Self::Sales => Duration::hours(6),
            Self::Advertising => Duration::hours(2),
            Self::Stocks | Self::Prices => Duration::hours(1),
        }
    }

    fn is_period_source(self) -> bool {
        matches!(self, Self::Sales | Self::Advertising)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStatus {
    Succeeded,
    Partial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SnapshotQuality {
    Complete,
    Partial,
    Stale,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountScope {
    account_id: String,
    marketplace: Marketplace,
}

impl AccountScope {
    pub fn new(account_id: String, marketplace: Marketplace) -> Result<Self, SnapshotError> {
        validate_identifier(&account_id)?;
        Ok(Self {
            account_id,
            marketplace,
        })
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn marketplace(&self) -> Marketplace {
        self.marketplace
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotDescriptor {
    snapshot_id: i64,
    account_id: String,
    marketplace: Marketplace,
    source: SnapshotSource,
    cutoff_at: DateTime<Utc>,
    source_as_of: DateTime<Utc>,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
    row_count: u32,
    pagination_complete: bool,
    status: SnapshotStatus,
}

impl SnapshotDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        snapshot_id: i64,
        account_id: String,
        marketplace: Marketplace,
        source: SnapshotSource,
        cutoff_at: DateTime<Utc>,
        source_as_of: DateTime<Utc>,
        period_start: DateTime<Utc>,
        period_end: DateTime<Utc>,
        row_count: u32,
        pagination_complete: bool,
        status: SnapshotStatus,
    ) -> Result<Self, SnapshotError> {
        if snapshot_id <= 0 {
            return Err(SnapshotError::InvalidSnapshot);
        }
        validate_identifier(&account_id)?;
        if source_as_of > cutoff_at + MAX_POST_CUTOFF_OBSERVATION_DELAY || period_start > period_end
        {
            return Err(SnapshotError::InvalidTimeRange);
        }
        if source.is_period_source() {
            if period_start == period_end || period_end > cutoff_at {
                return Err(SnapshotError::InvalidTimeRange);
            }
        } else if period_start != source_as_of || period_end != source_as_of {
            return Err(SnapshotError::InvalidTimeRange);
        }
        Ok(Self {
            snapshot_id,
            account_id,
            marketplace,
            source,
            cutoff_at,
            source_as_of,
            period_start,
            period_end,
            row_count,
            pagination_complete,
            status,
        })
    }

    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn marketplace(&self) -> Marketplace {
        self.marketplace
    }

    pub fn source(&self) -> SnapshotSource {
        self.source
    }

    pub fn cutoff_at(&self) -> DateTime<Utc> {
        self.cutoff_at
    }

    pub fn source_as_of(&self) -> DateTime<Utc> {
        self.source_as_of
    }

    pub fn period(&self) -> (DateTime<Utc>, DateTime<Utc>) {
        (self.period_start, self.period_end)
    }

    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    pub fn pagination_complete(&self) -> bool {
        self.pagination_complete
    }

    pub fn status(&self) -> SnapshotStatus {
        self.status
    }

    pub fn quality(&self) -> SnapshotQuality {
        let completeness = if self.status == SnapshotStatus::Partial || !self.pagination_complete {
            SnapshotQuality::Partial
        } else {
            SnapshotQuality::Complete
        };
        let age = self.cutoff_at - self.source_as_of;
        let sla = self.source.freshness_sla();
        let freshness = if age > sla * 2 {
            SnapshotQuality::Critical
        } else if age > sla {
            SnapshotQuality::Stale
        } else {
            SnapshotQuality::Complete
        };
        completeness.max(freshness)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenSnapshotManifest {
    cutoff_at: DateTime<Utc>,
    snapshots: Vec<SnapshotDescriptor>,
    quality: SnapshotQuality,
}

impl FrozenSnapshotManifest {
    pub fn new(
        cutoff_at: DateTime<Utc>,
        accounts: Vec<AccountScope>,
        snapshots: Vec<SnapshotDescriptor>,
    ) -> Result<Self, SnapshotError> {
        if accounts.is_empty() || accounts.len() > MAX_ACCOUNTS {
            return Err(SnapshotError::InvalidAccountScope);
        }
        let mut expected = BTreeMap::new();
        for account in accounts {
            if expected
                .insert(account.account_id, account.marketplace)
                .is_some()
            {
                return Err(SnapshotError::DuplicateAccount);
            }
        }
        if snapshots.len() != expected.len() * SnapshotSource::ALL.len() {
            return Err(SnapshotError::IncompleteManifest);
        }

        let mut seen = BTreeSet::new();
        let mut snapshot_ids = BTreeSet::new();
        let mut quality = SnapshotQuality::Complete;
        for snapshot in &snapshots {
            let Some(expected_marketplace) = expected.get(snapshot.account_id()) else {
                return Err(SnapshotError::ForeignAccount);
            };
            if *expected_marketplace != snapshot.marketplace() || snapshot.cutoff_at() != cutoff_at
            {
                return Err(SnapshotError::ScopeMismatch);
            }
            if !seen.insert((snapshot.account_id(), snapshot.source())) {
                return Err(SnapshotError::DuplicateSource);
            }
            if !snapshot_ids.insert(snapshot.snapshot_id()) {
                return Err(SnapshotError::DuplicateSnapshot);
            }
            quality = quality.max(snapshot.quality());
        }
        Ok(Self {
            cutoff_at,
            snapshots,
            quality,
        })
    }

    pub fn cutoff_at(&self) -> DateTime<Utc> {
        self.cutoff_at
    }

    pub fn snapshots(&self) -> &[SnapshotDescriptor] {
        &self.snapshots
    }

    pub fn quality(&self) -> SnapshotQuality {
        self.quality
    }

    pub fn recommendations_allowed(&self) -> bool {
        self.quality == SnapshotQuality::Complete
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotError {
    #[error("snapshot identity or account identifier is invalid")]
    InvalidSnapshot,
    #[error("snapshot time range is invalid")]
    InvalidTimeRange,
    #[error("report account scope is empty or exceeds the supported bound")]
    InvalidAccountScope,
    #[error("report account scope contains a duplicate")]
    DuplicateAccount,
    #[error("snapshot belongs to an account outside the report scope")]
    ForeignAccount,
    #[error("snapshot marketplace or cutoff does not match the report scope")]
    ScopeMismatch,
    #[error("report manifest does not contain every required source")]
    IncompleteManifest,
    #[error("report manifest contains a duplicate account/source pair")]
    DuplicateSource,
    #[error("report manifest reuses a snapshot identity")]
    DuplicateSnapshot,
}

fn validate_identifier(value: &str) -> Result<(), SnapshotError> {
    if value.is_empty()
        || value.len() > MAX_IDENTIFIER_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        Err(SnapshotError::InvalidSnapshot)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};

    use super::{
        AccountScope, FrozenSnapshotManifest, Marketplace, SnapshotDescriptor, SnapshotError,
        SnapshotQuality, SnapshotSource, SnapshotStatus,
    };

    fn cutoff() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 16, 12, 0, 0).unwrap()
    }

    fn account(id: &str, marketplace: Marketplace) -> AccountScope {
        AccountScope::new(id.to_owned(), marketplace).unwrap()
    }

    fn snapshot(
        id: i64,
        account_id: &str,
        marketplace: Marketplace,
        source: SnapshotSource,
    ) -> SnapshotDescriptor {
        let source_as_of = cutoff() - Duration::minutes(30);
        let (start, end) = if source.is_period_source() {
            (cutoff() - Duration::days(1), cutoff())
        } else {
            (source_as_of, source_as_of)
        };
        SnapshotDescriptor::new(
            id,
            account_id.to_owned(),
            marketplace,
            source,
            cutoff(),
            source_as_of,
            start,
            end,
            12,
            true,
            SnapshotStatus::Succeeded,
        )
        .unwrap()
    }

    fn all_snapshots() -> Vec<SnapshotDescriptor> {
        let mut id = 1;
        let mut snapshots = Vec::new();
        for (account_id, marketplace) in [
            ("ozon_store", Marketplace::Ozon),
            ("wb_store", Marketplace::Wildberries),
        ] {
            for source in SnapshotSource::ALL {
                snapshots.push(snapshot(id, account_id, marketplace, source));
                id += 1;
            }
        }
        snapshots
    }

    fn accounts() -> Vec<AccountScope> {
        vec![
            account("ozon_store", Marketplace::Ozon),
            account("wb_store", Marketplace::Wildberries),
        ]
    }

    #[test]
    fn complete_manifest_covers_every_account_and_source_exactly_once() {
        let manifest = FrozenSnapshotManifest::new(cutoff(), accounts(), all_snapshots()).unwrap();
        assert_eq!(manifest.cutoff_at(), cutoff());
        assert_eq!(manifest.snapshots().len(), 8);
        assert_eq!(manifest.quality(), SnapshotQuality::Complete);
        assert!(manifest.recommendations_allowed());

        let scope = account("ozon_store", Marketplace::Ozon);
        assert_eq!(scope.account_id(), "ozon_store");
        assert_eq!(scope.marketplace(), Marketplace::Ozon);

        let source = &manifest.snapshots()[0];
        assert_eq!(source.account_id(), "ozon_store");
        assert_eq!(source.marketplace(), Marketplace::Ozon);
        assert_eq!(source.source(), SnapshotSource::Sales);
        assert_eq!(source.cutoff_at(), cutoff());
        assert_eq!(source.source_as_of(), cutoff() - Duration::minutes(30));
        assert_eq!(source.period(), (cutoff() - Duration::days(1), cutoff()));
        assert_eq!(source.row_count(), 12);
        assert!(source.pagination_complete());
        assert_eq!(source.status(), SnapshotStatus::Succeeded);
    }

    #[test]
    fn quality_is_source_specific_and_suppresses_unsafe_recommendations() {
        let mut stale_ads = snapshot(
            1,
            "ozon_store",
            Marketplace::Ozon,
            SnapshotSource::Advertising,
        );
        stale_ads.source_as_of = cutoff() - Duration::hours(3);
        assert_eq!(stale_ads.quality(), SnapshotQuality::Stale);
        stale_ads.source_as_of = cutoff() - Duration::hours(5);
        assert_eq!(stale_ads.quality(), SnapshotQuality::Critical);

        let mut partial = snapshot(2, "ozon_store", Marketplace::Ozon, SnapshotSource::Stocks);
        partial.pagination_complete = false;
        assert_eq!(partial.quality(), SnapshotQuality::Partial);
        partial.pagination_complete = true;
        partial.status = SnapshotStatus::Partial;
        assert_eq!(partial.quality(), SnapshotQuality::Partial);
        partial.source_as_of = cutoff() - Duration::hours(3);
        assert_eq!(partial.quality(), SnapshotQuality::Critical);

        let mut snapshots = all_snapshots();
        snapshots[0].source_as_of = cutoff() - Duration::hours(7);
        let manifest = FrozenSnapshotManifest::new(cutoff(), accounts(), snapshots).unwrap();
        assert_eq!(manifest.quality(), SnapshotQuality::Stale);
        assert!(!manifest.recommendations_allowed());
    }

    #[test]
    fn snapshot_identity_and_time_ranges_fail_closed() {
        let point = cutoff() - Duration::minutes(1);
        let invalid = [
            SnapshotDescriptor::new(
                0,
                "store".to_owned(),
                Marketplace::Ozon,
                SnapshotSource::Stocks,
                cutoff(),
                point,
                point,
                point,
                0,
                true,
                SnapshotStatus::Succeeded,
            ),
            SnapshotDescriptor::new(
                1,
                "bad store".to_owned(),
                Marketplace::Ozon,
                SnapshotSource::Stocks,
                cutoff(),
                point,
                point,
                point,
                0,
                true,
                SnapshotStatus::Succeeded,
            ),
        ];
        for result in invalid {
            assert_eq!(result, Err(SnapshotError::InvalidSnapshot));
        }

        for (source, source_as_of, start, end) in [
            (
                SnapshotSource::Stocks,
                cutoff() + Duration::minutes(30) + Duration::seconds(1),
                cutoff() + Duration::minutes(30) + Duration::seconds(1),
                cutoff() + Duration::minutes(30) + Duration::seconds(1),
            ),
            (SnapshotSource::Sales, point, cutoff(), point),
            (
                SnapshotSource::Advertising,
                point,
                point,
                cutoff() + Duration::seconds(1),
            ),
            (SnapshotSource::Sales, point, point, point),
            (
                SnapshotSource::Prices,
                point,
                point - Duration::seconds(1),
                point,
            ),
        ] {
            assert_eq!(
                SnapshotDescriptor::new(
                    1,
                    "store".to_owned(),
                    Marketplace::Ozon,
                    source,
                    cutoff(),
                    source_as_of,
                    start,
                    end,
                    0,
                    true,
                    SnapshotStatus::Succeeded,
                ),
                Err(SnapshotError::InvalidTimeRange)
            );
        }

        let delayed_point = cutoff() + Duration::minutes(20);
        assert!(
            SnapshotDescriptor::new(
                1,
                "store".to_owned(),
                Marketplace::Ozon,
                SnapshotSource::Stocks,
                cutoff(),
                delayed_point,
                delayed_point,
                delayed_point,
                0,
                true,
                SnapshotStatus::Succeeded,
            )
            .is_ok()
        );
        assert!(
            SnapshotDescriptor::new(
                1,
                "store".to_owned(),
                Marketplace::Ozon,
                SnapshotSource::Sales,
                cutoff(),
                delayed_point,
                cutoff() - Duration::days(1),
                cutoff(),
                0,
                true,
                SnapshotStatus::Succeeded,
            )
            .is_ok()
        );

        for id in ["", "bad id", &"x".repeat(129)] {
            assert_eq!(
                AccountScope::new(id.to_owned(), Marketplace::Ozon),
                Err(SnapshotError::InvalidSnapshot)
            );
        }
    }

    #[test]
    fn manifest_rejects_incomplete_duplicate_and_cross_scope_data() {
        assert_eq!(
            FrozenSnapshotManifest::new(cutoff(), Vec::new(), Vec::new()),
            Err(SnapshotError::InvalidAccountScope)
        );
        assert_eq!(
            FrozenSnapshotManifest::new(
                cutoff(),
                vec![
                    account("same", Marketplace::Ozon),
                    account("same", Marketplace::Ozon)
                ],
                Vec::new(),
            ),
            Err(SnapshotError::DuplicateAccount)
        );
        assert_eq!(
            FrozenSnapshotManifest::new(
                cutoff(),
                vec![account("only", Marketplace::Ozon)],
                Vec::new(),
            ),
            Err(SnapshotError::IncompleteManifest)
        );

        let mut foreign = all_snapshots();
        foreign[0].account_id = "foreign".to_owned();
        assert_eq!(
            FrozenSnapshotManifest::new(cutoff(), accounts(), foreign),
            Err(SnapshotError::ForeignAccount)
        );

        for mutate in [
            |snapshot: &mut SnapshotDescriptor| snapshot.marketplace = Marketplace::Wildberries,
            |snapshot: &mut SnapshotDescriptor| snapshot.cutoff_at += Duration::seconds(1),
        ] {
            let mut mismatched = all_snapshots();
            mutate(&mut mismatched[0]);
            assert_eq!(
                FrozenSnapshotManifest::new(cutoff(), accounts(), mismatched),
                Err(SnapshotError::ScopeMismatch)
            );
        }

        let mut duplicate_source = all_snapshots();
        duplicate_source[1].source = SnapshotSource::Sales;
        assert_eq!(
            FrozenSnapshotManifest::new(cutoff(), accounts(), duplicate_source),
            Err(SnapshotError::DuplicateSource)
        );

        let mut duplicate_id = all_snapshots();
        duplicate_id[1].snapshot_id = duplicate_id[0].snapshot_id;
        assert_eq!(
            FrozenSnapshotManifest::new(cutoff(), accounts(), duplicate_id),
            Err(SnapshotError::DuplicateSnapshot)
        );

        let too_many = (0..65)
            .map(|index| account(&format!("account_{index}"), Marketplace::Ozon))
            .collect();
        assert_eq!(
            FrozenSnapshotManifest::new(cutoff(), too_many, Vec::new()),
            Err(SnapshotError::InvalidAccountScope)
        );
    }
}
