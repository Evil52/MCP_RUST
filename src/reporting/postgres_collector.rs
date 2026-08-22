use std::collections::BTreeSet;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Config, Transaction, error::SqlState};

use crate::postgres::SupervisedClient;

use super::{
    collector_plan::CollectionTarget,
    snapshot::{Marketplace, SnapshotDescriptor, SnapshotSource, SnapshotStatus},
};

const MAX_FACT_ROWS: usize = 25_000;
const MAX_COLLECTOR_VERSION_BYTES: usize = 64;
const MAX_COLLECTION_TARGETS: usize = 64;
const MAX_ACCOUNT_ID_BYTES: usize = 128;
const COLLECTION_CANARY_MAX_AGE: Duration = Duration::hours(24);

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
    pub basket_additions: u64,
    pub model_attributed_orders: u64,
    pub model_attributed_revenue_minor: u64,
    pub product_price_minor: u64,
    pub average_cpc_minor: Option<u64>,
    pub cpm_minor: Option<u64>,
    pub cpl_minor: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollectedAdvertisingExpenseFact {
    pub business_date: NaiveDate,
    pub campaign_id: u64,
    pub money_spent_minor: u64,
    pub bonus_spent_minor: u64,
    pub prepayment_spent_minor: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FinanceCategory {
    Sale,
    Commission,
    Acquiring,
    Logistics,
    Storage,
    PaidAcceptance,
    Compensation,
    MarketplaceDiscount,
    Advertising,
    Other,
}

impl FinanceCategory {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sale => "sale",
            Self::Commission => "commission",
            Self::Acquiring => "acquiring",
            Self::Logistics => "logistics",
            Self::Storage => "storage",
            Self::PaidAcceptance => "paid_acceptance",
            Self::Compensation => "compensation",
            Self::MarketplaceDiscount => "marketplace_discount",
            Self::Advertising => "advertising",
            Self::Other => "other",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollectedFinanceFact {
    pub business_date: NaiveDate,
    /// `None` is an account-wide accrual which Ozon did not attribute to SKU.
    pub sku: Option<u64>,
    pub category: FinanceCategory,
    /// Signed marketplace amount: credits are positive, deductions negative.
    pub amount_minor: i64,
    pub line_count: u32,
    pub unknown_type_count: u32,
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
    Finance(Vec<CollectedFinanceFact>),
    Stocks(Vec<CollectedStockFact>),
    Prices(Vec<CollectedPriceFact>),
}

impl CollectedFacts {
    fn source(&self) -> SnapshotSource {
        match self {
            Self::Sales(_) => SnapshotSource::Sales,
            Self::Advertising(_) => SnapshotSource::Advertising,
            Self::Finance(_) => SnapshotSource::Finance,
            Self::Stocks(_) => SnapshotSource::Stocks,
            Self::Prices(_) => SnapshotSource::Prices,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Sales(facts) => facts.len(),
            Self::Advertising(facts) => facts.len(),
            Self::Finance(facts) => facts.len(),
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
    advertising_expenses: Vec<CollectedAdvertisingExpenseFact>,
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
            advertising_expenses: Vec::new(),
        })
    }

    pub fn with_advertising_expenses(
        mut self,
        expenses: Vec<CollectedAdvertisingExpenseFact>,
    ) -> Result<Self, PostgresCollectorError> {
        if self.marketplace != Marketplace::Ozon
            || self.facts.source() != SnapshotSource::Advertising
            || expenses.len() > MAX_FACT_ROWS
        {
            return Err(PostgresCollectorError::InvalidInput);
        }
        validate_advertising_expenses(&expenses)?;
        self.advertising_expenses = expenses;
        Ok(self)
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
    #[error("the report collection claim is absent, busy, expired, or superseded")]
    ClaimLost,
    #[error("a recent complete collection canary is unavailable")]
    CanaryMissing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollectionActivationReceipt {
    pub cutoff_at: DateTime<Utc>,
    pub target_count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionClaim {
    id: i64,
    generation: i64,
    account_id: String,
    marketplace: Marketplace,
    cutoff_at: DateTime<Utc>,
    owner_id: String,
    lease_until: DateTime<Utc>,
}

impl CollectionClaim {
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn marketplace(&self) -> Marketplace {
        self.marketplace
    }

    #[must_use]
    pub fn lease_until(&self) -> DateTime<Utc> {
        self.lease_until
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        account_id: &str,
        marketplace: Marketplace,
        lease_until: DateTime<Utc>,
    ) -> Self {
        Self {
            id: 1,
            generation: 1,
            account_id: account_id.to_owned(),
            marketplace,
            cutoff_at: lease_until - chrono::Duration::minutes(15),
            owner_id: "unit-test-owner".to_owned(),
            lease_until,
        }
    }
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

    #[must_use]
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
                        'daily_reporting.advertising_expense_facts', 'INSERT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.finance_facts', 'INSERT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.stock_facts', 'INSERT') \
                    AND has_table_privilege(current_user, \
                        'daily_reporting.price_facts', 'INSERT') \
                    AND has_function_privilege(current_user, \
                        'daily_reporting.claim_report_collection(text,text,timestamptz,text)', \
                        'EXECUTE') \
                    AND has_function_privilege(current_user, \
                        'daily_reporting.release_report_collection_claim(bigint,bigint,text)', \
                        'EXECUTE') \
                    AND has_function_privilege(current_user, \
                        'daily_reporting.complete_report_collection_claim(bigint,bigint,text)', \
                        'EXECUTE') \
                    AND NOT has_table_privilege(current_user, \
                        'daily_reporting.collection_claims', 'SELECT,INSERT,UPDATE,DELETE') \
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

    /// Claims one exact account/marketplace/cutoff before credential lookup or
    /// marketplace I/O. `None` means another live owner already holds the
    /// fifteen-minute lease or the occurrence was completed earlier.
    pub async fn claim_target(
        &self,
        target: &CollectionTarget,
        cutoff_at: DateTime<Utc>,
        owner_id: &str,
    ) -> Result<Option<CollectionClaim>, PostgresCollectorError> {
        validate_coverage_targets(std::slice::from_ref(target))?;
        validate_owner_id(owner_id)?;
        let marketplace = marketplace_name(target.marketplace);
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let row = client
            .query_opt(
                "SELECT claim_id, claim_generation, lease_until \
                 FROM daily_reporting.claim_report_collection($1, $2, $3, $4)",
                &[&target.account_id, &marketplace, &cutoff_at, &owner_id],
            )
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        Ok(row.map(|row| CollectionClaim {
            id: row.get(0),
            generation: row.get(1),
            account_id: target.account_id.clone(),
            marketplace: target.marketplace,
            cutoff_at,
            owner_id: owner_id.to_owned(),
            lease_until: row.get(2),
        }))
    }

    /// Relinquishes a live claim after a failed collection so another bounded
    /// attempt may start without waiting for lease expiry.
    pub async fn release_claim(
        &self,
        claim: &CollectionClaim,
    ) -> Result<bool, PostgresCollectorError> {
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        client
            .query_one(
                "SELECT daily_reporting.release_report_collection_claim($1, $2, $3)",
                &[&claim.id, &claim.generation, &claim.owner_id],
            )
            .await
            .map(|row| row.get(0))
            .map_err(|_| PostgresCollectorError::Unavailable)
    }

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
        let account_ids = targets
            .iter()
            .map(|target| target.account_id.clone())
            .collect::<Vec<_>>();
        let marketplaces = targets
            .iter()
            .map(|target| marketplace_name(target.marketplace).to_owned())
            .collect::<Vec<_>>();
        let required_counts = targets
            .iter()
            .map(|target| i64::try_from(target.sources.len()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| PostgresCollectorError::InvalidInput)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let rows = client
            .query(
                "WITH requested(account_id, marketplace, required_count) AS ( \
                     SELECT * FROM unnest($2::text[], $3::text[], $4::bigint[]) \
                 ) \
                 SELECT snapshot.account_id, snapshot.marketplace \
                 FROM daily_reporting.source_snapshots AS snapshot \
                 JOIN requested \
                   ON requested.account_id = snapshot.account_id::text \
                  AND requested.marketplace = snapshot.marketplace::text \
                 WHERE snapshot.cutoff_at = $1 \
                   AND snapshot.status = 'succeeded' \
                   AND snapshot.pagination_complete \
                 GROUP BY snapshot.account_id, snapshot.marketplace \
                 HAVING count(*) = max(requested.required_count) \
                    AND count(DISTINCT source) = max(requested.required_count) \
                 ORDER BY snapshot.account_id, snapshot.marketplace",
                &[&cutoff_at, &account_ids, &marketplaces, &required_counts],
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
        let account_ids = targets
            .iter()
            .map(|target| target.account_id.clone())
            .collect::<Vec<_>>();
        let marketplaces = targets
            .iter()
            .map(|target| marketplace_name(target.marketplace).to_owned())
            .collect::<Vec<_>>();
        let required_rows = i64::try_from(
            targets
                .iter()
                .map(|target| target.sources.len())
                .sum::<usize>(),
        )
        .map_err(|_| PostgresCollectorError::InvalidInput)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        let row = client
            .query_opt(
                "WITH requested(account_id, marketplace) AS ( \
                     SELECT * FROM unnest($3::text[], $4::text[]) \
                 ) \
                 SELECT snapshot.cutoff_at \
                 FROM daily_reporting.source_snapshots AS snapshot \
                 JOIN requested \
                   ON requested.account_id = snapshot.account_id::text \
                  AND requested.marketplace = snapshot.marketplace::text \
                 WHERE snapshot.cutoff_at BETWEEN $1 AND $2 \
                   AND snapshot.status = 'succeeded' \
                   AND snapshot.pagination_complete \
                 GROUP BY snapshot.cutoff_at \
                 HAVING count(*) = $5 \
                    AND count(DISTINCT (snapshot.account_id, snapshot.marketplace, snapshot.source)) = $5 \
                 ORDER BY snapshot.cutoff_at DESC \
                 LIMIT 1",
                &[
                    &oldest_allowed,
                    &now,
                    &account_ids,
                    &marketplaces,
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

    /// Atomically appends facts and publishes their immutable snapshot.
    ///
    /// Any failure rolls back both the snapshot row and all facts. A duplicate
    /// account/source/cutoff identity fails closed instead of overwriting or
    /// silently reusing data from a previous collection attempt.
    /// Persists a related group of report snapshots as one database unit.
    /// A failed source can therefore never publish only sales, stocks, or
    /// prices for one logical report cutoff. Completion of the fencing claim
    /// is part of the same transaction, so a stale owner cannot publish.
    pub async fn persist_claimed_batch(
        &self,
        claim: &CollectionClaim,
        snapshots: &[CollectedSnapshot],
    ) -> Result<Vec<i64>, PostgresCollectorError> {
        validate_claimed_batch(claim, snapshots)?;
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
            snapshot_ids.push(persist_in_transaction(&transaction, claim, snapshot).await?);
        }
        let completed = transaction
            .query_one(
                "SELECT daily_reporting.complete_report_collection_claim($1, $2, $3)",
                &[&claim.id, &claim.generation, &claim.owner_id],
            )
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?
            .get::<_, bool>(0);
        require_claim_completed(completed)?;
        transaction
            .commit()
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
        Ok(snapshot_ids)
    }
}

fn require_claim_completed(completed: bool) -> Result<(), PostgresCollectorError> {
    completed
        .then_some(())
        .ok_or(PostgresCollectorError::ClaimLost)
}

fn validate_claimed_batch(
    claim: &CollectionClaim,
    snapshots: &[CollectedSnapshot],
) -> Result<(), PostgresCollectorError> {
    let sources = snapshots
        .iter()
        .map(|snapshot| snapshot.facts.source())
        .collect::<BTreeSet<_>>();
    let required_sources = SnapshotSource::required_for(claim.marketplace)
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if snapshots.len() != required_sources.len()
        || sources != required_sources
        || snapshots.iter().any(|snapshot| {
            snapshot.account_id != claim.account_id
                || snapshot.marketplace != claim.marketplace
                || snapshot.cutoff_at != claim.cutoff_at
        })
    {
        Err(PostgresCollectorError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_owner_id(owner_id: &str) -> Result<(), PostgresCollectorError> {
    if owner_id.is_empty()
        || owner_id.len() > 64
        || !owner_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        Err(PostgresCollectorError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_coverage_targets(targets: &[CollectionTarget]) -> Result<(), PostgresCollectorError> {
    let mut identities = BTreeSet::new();
    if targets.len() > MAX_COLLECTION_TARGETS
        || targets.iter().any(|target| {
            let required_sources = SnapshotSource::required_for(target.marketplace)
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            !identities.insert((&target.account_id, target.marketplace))
                || target.account_id.is_empty()
                || target.account_id.len() > MAX_ACCOUNT_ID_BYTES
                || !target
                    .account_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
                || target.sources.iter().copied().collect::<BTreeSet<_>>() != required_sources
                || target.sources.len() != required_sources.len()
        })
    {
        Err(PostgresCollectorError::InvalidInput)
    } else {
        Ok(())
    }
}

fn marketplace_name(marketplace: Marketplace) -> &'static str {
    match marketplace {
        Marketplace::Ozon => "ozon",
        Marketplace::Wildberries => "wildberries",
    }
}

fn parse_marketplace(value: &str) -> Result<Marketplace, PostgresCollectorError> {
    match value {
        "ozon" => Ok(Marketplace::Ozon),
        "wildberries" => Ok(Marketplace::Wildberries),
        _ => Err(PostgresCollectorError::Unavailable),
    }
}

async fn persist_in_transaction(
    transaction: &Transaction<'_>,
    claim: &CollectionClaim,
    snapshot: &CollectedSnapshot,
) -> Result<i64, PostgresCollectorError> {
    let payload = serde_json::to_vec(snapshot).map_err(|_| PostgresCollectorError::InvalidInput)?;
    let payload_sha256 = sha256(&payload);
    let row_count =
        i32::try_from(snapshot.facts.len()).map_err(|_| PostgresCollectorError::InvalidInput)?;
    let snapshot_id = insert_snapshot(transaction, claim, snapshot).await?;
    insert_facts(transaction, snapshot_id, &snapshot.facts).await?;
    insert_advertising_expenses(transaction, snapshot_id, &snapshot.advertising_expenses).await?;
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

async fn insert_advertising_expenses(
    transaction: &Transaction<'_>,
    snapshot_id: i64,
    facts: &[CollectedAdvertisingExpenseFact],
) -> Result<(), PostgresCollectorError> {
    for fact in facts {
        transaction
            .execute(
                "INSERT INTO daily_reporting.advertising_expense_facts \
                 (snapshot_id, business_date, campaign_id, money_spent_minor, \
                  bonus_spent_minor, prepayment_spent_minor) \
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &snapshot_id,
                    &fact.business_date,
                    &as_i64(fact.campaign_id)?,
                    &as_i64(fact.money_spent_minor)?,
                    &as_i64(fact.bonus_spent_minor)?,
                    &as_i64(fact.prepayment_spent_minor)?,
                ],
            )
            .await
            .map_err(|_| PostgresCollectorError::Unavailable)?;
    }
    Ok(())
}

fn map_snapshot_insert_error(error: tokio_postgres::Error) -> PostgresCollectorError {
    classify_snapshot_insert_code(error.code())
}

fn classify_snapshot_insert_code(code: Option<&SqlState>) -> PostgresCollectorError {
    if code == Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE) {
        PostgresCollectorError::ClaimLost
    } else {
        PostgresCollectorError::Unavailable
    }
}

async fn insert_snapshot(
    transaction: &Transaction<'_>,
    claim: &CollectionClaim,
    snapshot: &CollectedSnapshot,
) -> Result<i64, PostgresCollectorError> {
    let marketplace = marketplace_name(snapshot.marketplace);
    let source = match snapshot.facts.source() {
        SnapshotSource::Sales => "sales",
        SnapshotSource::Advertising => "advertising",
        SnapshotSource::Finance => "finance",
        SnapshotSource::Stocks => "stocks",
        SnapshotSource::Prices => "prices",
    };
    let row = transaction
        .query_opt(
            "INSERT INTO daily_reporting.source_snapshots \
                (account_id, marketplace, source, cutoff_at, source_as_of, \
                 period_start, period_end, collector_version, claim_id, claim_generation) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
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
                &claim.id,
                &claim.generation,
            ],
        )
        .await
        .map_err(map_snapshot_insert_error)?;
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
                          spend_minor, attributed_orders, attributed_revenue_minor, \
                          basket_additions, model_attributed_orders, \
                          model_attributed_revenue_minor, product_price_minor, \
                          average_cpc_minor, cpm_minor, cpl_minor) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, \
                                 $13, $14, $15, $16)",
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
                            &as_i32(fact.basket_additions)?,
                            &as_i32(fact.model_attributed_orders)?,
                            &as_i64(fact.model_attributed_revenue_minor)?,
                            &as_i64(fact.product_price_minor)?,
                            &fact.average_cpc_minor.map(as_i64).transpose()?,
                            &fact.cpm_minor.map(as_i64).transpose()?,
                            &fact.cpl_minor.map(as_i64).transpose()?,
                        ],
                    )
                    .await
                    .map_err(|_| PostgresCollectorError::Unavailable)?;
            }
        }
        CollectedFacts::Finance(facts) => {
            for fact in facts {
                let sku = fact.sku.map(as_i64).transpose()?;
                transaction
                    .execute(
                        "INSERT INTO daily_reporting.finance_facts \
                         (snapshot_id, business_date, sku, category, amount_minor, \
                          line_count, unknown_type_count) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7)",
                        &[
                            &snapshot_id,
                            &fact.business_date,
                            &sku,
                            &fact.category.as_str(),
                            &fact.amount_minor,
                            &i32::try_from(fact.line_count)
                                .map_err(|_| PostgresCollectorError::InvalidInput)?,
                            &i32::try_from(fact.unknown_type_count)
                                .map_err(|_| PostgresCollectorError::InvalidInput)?,
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
                    && fits_i64(fact.basket_additions)
                    && fits_i32(fact.model_attributed_orders)
                    && fits_i64(fact.model_attributed_revenue_minor)
                    && fits_i64(fact.product_price_minor)
                    && fact.average_cpc_minor.is_none_or(fits_i64)
                    && fact.cpm_minor.is_none_or(fits_i64)
                    && fact.cpl_minor.is_none_or(fits_i64)
            },
        ),
        CollectedFacts::Finance(facts) => ensure_unique(
            facts,
            |fact| (fact.business_date, fact.sku, fact.category),
            |fact| {
                fact.sku.is_none_or(|sku| sku > 0)
                    && fact.line_count > 0
                    && fact.unknown_type_count <= fact.line_count
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

fn validate_advertising_expenses(
    facts: &[CollectedAdvertisingExpenseFact],
) -> Result<(), PostgresCollectorError> {
    ensure_unique(
        facts,
        |fact| (fact.business_date, fact.campaign_id),
        |fact| {
            fact.campaign_id > 0
                && fits_i64(fact.campaign_id)
                && fits_i64(fact.money_spent_minor)
                && fits_i64(fact.bonus_spent_minor)
                && fits_i64(fact.prepayment_spent_minor)
        },
    )
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
        .fold(String::with_capacity(64), |mut output, byte| {
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
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
            SnapshotSource::Sales | SnapshotSource::Advertising | SnapshotSource::Finance => {
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
            basket_additions: 2,
            model_attributed_orders: 1,
            model_attributed_revenue_minor: 80,
            product_price_minor: 100,
            average_cpc_minor: Some(4),
            cpm_minor: Some(200),
            cpl_minor: Some(10),
        }
    }

    #[test]
    fn every_fact_shape_and_partial_snapshot_validate() {
        assert!(snapshot(CollectedFacts::Sales(vec![sales()])).is_ok());
        assert!(snapshot(CollectedFacts::Advertising(vec![advertising()])).is_ok());
        assert!(
            snapshot(CollectedFacts::Finance(vec![CollectedFinanceFact {
                business_date: cutoff().date_naive(),
                sku: Some(1),
                category: FinanceCategory::Sale,
                amount_minor: 100,
                line_count: 1,
                unknown_type_count: 0,
            }]))
            .is_ok()
        );
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

        let advertising_snapshot = snapshot(CollectedFacts::Advertising(vec![advertising()]))
            .unwrap()
            .with_advertising_expenses(vec![CollectedAdvertisingExpenseFact {
                business_date: cutoff().date_naive(),
                campaign_id: 2,
                money_spent_minor: 20,
                bonus_spent_minor: 5,
                prepayment_spent_minor: 15,
            }]);
        assert!(advertising_snapshot.is_ok());
        assert_eq!(
            snapshot(CollectedFacts::Sales(vec![sales()]))
                .unwrap()
                .with_advertising_expenses(Vec::new()),
            Err(PostgresCollectorError::InvalidInput)
        );

        for category in [
            FinanceCategory::Sale,
            FinanceCategory::Commission,
            FinanceCategory::Acquiring,
            FinanceCategory::Logistics,
            FinanceCategory::Storage,
            FinanceCategory::PaidAcceptance,
            FinanceCategory::Compensation,
            FinanceCategory::MarketplaceDiscount,
            FinanceCategory::Advertising,
            FinanceCategory::Other,
        ] {
            assert!(!category.as_str().is_empty());
        }
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

    #[test]
    fn claimed_batch_requires_the_complete_exact_identity() {
        let claim = CollectionClaim {
            id: 1,
            generation: 1,
            account_id: "pilot".to_owned(),
            marketplace: Marketplace::Ozon,
            cutoff_at: cutoff(),
            owner_id: "test-owner".to_owned(),
            lease_until: cutoff() + Duration::minutes(15),
        };
        assert_eq!(
            validate_claimed_batch(&claim, &[]),
            Err(PostgresCollectorError::InvalidInput)
        );
        assert_eq!(
            validate_owner_id("bad owner"),
            Err(PostgresCollectorError::InvalidInput)
        );
        assert_eq!(
            validate_owner_id(&"a".repeat(65)),
            Err(PostgresCollectorError::InvalidInput)
        );
        assert!(validate_owner_id("collector:1-2").is_ok());
        assert_eq!(
            classify_snapshot_insert_code(Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)),
            PostgresCollectorError::ClaimLost
        );
        assert_eq!(
            classify_snapshot_insert_code(None),
            PostgresCollectorError::Unavailable
        );
        assert_eq!(require_claim_completed(true), Ok(()));
        assert_eq!(
            require_claim_completed(false),
            Err(PostgresCollectorError::ClaimLost)
        );
    }

    #[test]
    fn coverage_query_inputs_are_bounded_and_unique() {
        let target = CollectionTarget {
            account_id: "pilot".to_owned(),
            marketplace: Marketplace::Ozon,
            sources: SnapshotSource::required_for(Marketplace::Ozon).to_vec(),
        };
        assert!(validate_coverage_targets(&[]).is_ok());
        assert!(validate_coverage_targets(std::slice::from_ref(&target)).is_ok());
        assert_eq!(
            validate_coverage_targets(&[target.clone(), target]),
            Err(PostgresCollectorError::InvalidInput)
        );
        let incomplete = CollectionTarget {
            account_id: "incomplete".to_owned(),
            marketplace: Marketplace::Ozon,
            sources: vec![
                SnapshotSource::Sales,
                SnapshotSource::Advertising,
                SnapshotSource::Stocks,
                SnapshotSource::Stocks,
            ],
        };
        assert_eq!(
            validate_coverage_targets(&[incomplete]),
            Err(PostgresCollectorError::InvalidInput)
        );
        for account_id in ["", "unsafe/account", &"a".repeat(MAX_ACCOUNT_ID_BYTES + 1)] {
            assert_eq!(
                validate_coverage_targets(&[CollectionTarget {
                    account_id: account_id.to_owned(),
                    marketplace: Marketplace::Ozon,
                    sources: SnapshotSource::required_for(Marketplace::Ozon).to_vec(),
                }]),
                Err(PostgresCollectorError::InvalidInput)
            );
        }
        let too_many = (0..=MAX_COLLECTION_TARGETS)
            .map(|index| CollectionTarget {
                account_id: format!("account-{index}"),
                marketplace: Marketplace::Ozon,
                sources: SnapshotSource::required_for(Marketplace::Ozon).to_vec(),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            validate_coverage_targets(&too_many),
            Err(PostgresCollectorError::InvalidInput)
        );
        assert_eq!(parse_marketplace("ozon"), Ok(Marketplace::Ozon));
        assert_eq!(
            parse_marketplace("wildberries"),
            Ok(Marketplace::Wildberries)
        );
        assert_eq!(
            parse_marketplace("unknown"),
            Err(PostgresCollectorError::Unavailable)
        );
    }
}
