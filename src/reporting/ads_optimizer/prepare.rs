//! Offline preparation from complete, pinned `ofk_source_snapshot` pages.
//!
//! The source responses remain unmodified JSON in the bundle. Their selected
//! fields and every fact's provenance are checked; extra upstream fields are
//! tolerated. This checks consistency, not authenticity of a local export.
//! Campaign payment models and complete SKU campaign scope require a separate
//! explicit operator confirmation. No page download or marketplace call occurs.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{
    BudgetConstraintEvidence, Currency, MAX_INPUT_BYTES, MAX_PRODUCTS, MAX_WINDOW_DAYS,
    OptimizationObjective, OptimizerError, OptimizerPolicy, OrderEconomics, PricingModel,
    ProductEvidence, ShadowInput, StockEvidence, SupportedMarketplace,
    published::{CpcCampaignScope, aggregate_cpc_days},
};
use crate::reporting::{
    business_date,
    ozon_adapter::stocks::is_sku_fulfillment_dimension,
    postgres_snapshot::PublishedAdvertisingFact,
    snapshot::{Marketplace, SnapshotDescriptor, SnapshotSource, SnapshotStatus},
    yekaterinburg_offset,
};

pub const MAX_PREPARATION_BYTES: usize = 32 * 1024 * 1024;
const MAX_PAGES: usize = 512;
const MAX_ROWS: usize = 25_000;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationBundle {
    pub version: u32,
    pub account_id: String,
    pub as_of: DateTime<Utc>,
    pub window_start: NaiveDate,
    pub window_end: NaiveDate,
    pub objective: OptimizationObjective,
    pub policy: OptimizerPolicy,
    pub products: Vec<PreparationProduct>,
    pub advertising_pages: Vec<SnapshotPage>,
    #[serde(default)]
    pub stock_pages: Vec<SnapshotPage>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationProduct {
    pub sku: u64,
    pub current_daily_budget_minor: u64,
    pub max_daily_budget_minor: u64,
    pub budget_constraint: Option<BudgetConstraintEvidence>,
    pub economics: Option<OrderEconomics>,
    pub cpc_scope: CpcScopeConfirmation,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CpcScopeConfirmation {
    pub campaign_ids: Vec<u64>,
    pub pricing_model: PricingModel,
    pub source_ref: String,
    pub observed_at: DateTime<Utc>,
    /// Confirms that the listed campaigns cover this SKU's full CPC scope.
    /// False preserves the evidence but blocks performance recommendations.
    pub all_campaigns_confirmed: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotPage {
    /// Original request offset. The response itself does not echo this field.
    pub offset: u32,
    pub response: Value,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum PrepareError {
    #[error("preparation bundle exceeds its size or row limit")]
    LimitExceeded,
    #[error("preparation bundle violates the versioned contract")]
    InvalidBundle,
    #[error("published source snapshot or fact provenance is invalid")]
    InvalidSnapshot,
    #[error("published source snapshot pages are missing or inconsistent")]
    IncompleteSnapshot,
    #[error("published advertising snapshots cover overlapping dates")]
    OverlappingSnapshots,
    #[error("current CPC campaign scope is missing, stale, or inconsistent")]
    InvalidCampaignScope,
    #[error("prepared evidence violates the optimizer contract: {0}")]
    Optimizer(#[from] OptimizerError),
}

/// Parse a bounded local export. Unknown fields in our versioned wrapper are
/// rejected; extra fields within the original MCP responses are retained.
pub fn prepare_input(bytes: &[u8]) -> Result<ShadowInput, PrepareError> {
    if bytes.len() > MAX_PREPARATION_BYTES {
        return Err(PrepareError::LimitExceeded);
    }
    let bundle = serde_json::from_slice(bytes).map_err(|_| PrepareError::InvalidBundle)?;
    prepare(bundle)
}

/// Build optimizer evidence without changing observation times or adding
/// zero-activity dates. Old frozen history remains old at recommendation time.
pub fn prepare(bundle: PreparationBundle) -> Result<ShadowInput, PrepareError> {
    let days = (bundle.window_end - bundle.window_start).num_days() + 1;
    if bundle.version != 1 || days <= 0 {
        return Err(PrepareError::InvalidBundle);
    }
    if days > MAX_WINDOW_DAYS || bundle.products.is_empty() || bundle.products.len() > MAX_PRODUCTS
    {
        return Err(PrepareError::LimitExceeded);
    }
    let canonical = serde_json::to_vec(&bundle).map_err(|_| PrepareError::InvalidBundle)?;
    if canonical.len() > MAX_PREPARATION_BYTES {
        return Err(PrepareError::LimitExceeded);
    }
    let digest =
        Sha256::digest(canonical)
            .iter()
            .fold(String::with_capacity(64), |mut output, byte| {
                use std::fmt::Write as _;
                write!(output, "{byte:02x}").expect("writing to String cannot fail");
                output
            });
    let mut refs = BTreeSet::from([format!("prepare-bundle-sha256:{digest}")]);
    let advertising = complete_snapshots(&bundle.advertising_pages, &bundle, "advertising")?;
    if advertising.is_empty() {
        return Err(PrepareError::IncompleteSnapshot);
    }
    let stocks = complete_snapshots(&bundle.stock_pages, &bundle, "stocks")?;
    if stocks.len() > 1 || stocks.keys().any(|id| advertising.contains_key(id)) {
        return Err(PrepareError::InvalidSnapshot);
    }
    let mut observed_dates = BTreeMap::new();
    let mut ad_facts = BTreeMap::<u64, Vec<PublishedAdvertisingFact>>::new();
    let mut global_observed_at = None::<DateTime<Utc>>;
    let selected = bundle
        .products
        .iter()
        .map(|product| product.sku)
        .collect::<BTreeSet<_>>();
    if selected.len() != bundle.products.len() {
        return Err(PrepareError::InvalidBundle);
    }
    for snapshot in advertising.values() {
        let observed = snapshot.envelope.first_observed();
        global_observed_at = Some(global_observed_at.map_or(observed, |time| time.max(observed)));
        refs.insert(snapshot.reference());
        let mut date = snapshot.period_dates()?.0;
        let end = snapshot.period_dates()?.1;
        if date < bundle.window_start || end > bundle.window_end {
            return Err(PrepareError::InvalidSnapshot);
        }
        loop {
            if observed_dates.insert(date, observed).is_some() {
                return Err(PrepareError::OverlappingSnapshots);
            }
            if date == end {
                break;
            }
            date = date.succ_opt().ok_or(PrepareError::InvalidSnapshot)?;
        }
        let mut keys = BTreeSet::new();
        for value in &snapshot.rows {
            let row: AdvertisingRow =
                serde_json::from_value(value.clone()).map_err(|_| PrepareError::InvalidSnapshot)?;
            snapshot.validate_fact(&row.provenance)?;
            if row.currency != "RUB"
                || row.clicks > row.impressions
                || !positive_id(row.campaign_id)
                || row.sku > i64::MAX.cast_unsigned()
                || row.business_date < snapshot.period_dates()?.0
                || row.business_date > end
                || !keys.insert((row.business_date, row.campaign_id, row.sku))
            {
                return Err(PrepareError::InvalidSnapshot);
            }
            if row.sku == 0 {
                return Err(PrepareError::InvalidSnapshot);
            }
            if selected.contains(&row.sku) {
                ad_facts.entry(row.sku).or_default().push(row.into_fact());
            }
        }
    }
    let stock_by_sku = stock_evidence(&stocks, &selected, &mut refs)?;
    for snapshot in stocks.values() {
        refs.insert(snapshot.reference());
    }
    let mut coverage_complete =
        observed_dates.len() == usize::try_from(days).map_err(|_| PrepareError::InvalidBundle)?;
    let mut products = Vec::with_capacity(bundle.products.len());
    for product in &bundle.products {
        let scope = confirmed_scope(product, &bundle)?;
        refs.insert(product.cpc_scope.source_ref.clone());
        coverage_complete &= product.cpc_scope.all_campaigns_confirmed;
        let facts = ad_facts.remove(&product.sku).unwrap_or_default();
        let mut daily = aggregate_cpc_days(&facts, &scope, bundle.window_start, bundle.window_end)?;
        // The adapter has now checked uniqueness and campaign/date scope.
        // Every campaign/date pair needs an explicit row: a complete source
        // page does not turn a missing campaign's activity into zero.
        let expected_pairs = product
            .cpc_scope
            .campaign_ids
            .len()
            .checked_mul(usize::try_from(days).map_err(|_| PrepareError::InvalidBundle)?)
            .ok_or(PrepareError::LimitExceeded)?;
        coverage_complete &= facts.len() == expected_pairs;
        for day in &mut daily {
            day.observed_at = Some(
                *observed_dates
                    .get(&day.date)
                    .ok_or(PrepareError::InvalidSnapshot)?,
            );
        }
        products.push(ProductEvidence {
            sku: product.sku,
            current_daily_budget_minor: product.current_daily_budget_minor,
            max_daily_budget_minor: product.max_daily_budget_minor,
            budget_constraint: product.budget_constraint.clone(),
            economics: product.economics.clone(),
            daily,
            stock: stock_by_sku.get(&product.sku).cloned(),
        });
    }
    let input = ShadowInput {
        version: 1,
        marketplace: SupportedMarketplace::Ozon,
        pricing_model: PricingModel::Cpc,
        currency: Currency::RUB,
        source_refs: refs.into_iter().collect(),
        account_id: bundle.account_id,
        as_of: bundle.as_of,
        observed_at: global_observed_at.ok_or(PrepareError::IncompleteSnapshot)?,
        window_start: bundle.window_start,
        window_end: bundle.window_end,
        coverage_complete,
        objective: bundle.objective,
        policy: bundle.policy,
        products,
    };
    super::validation::validate(&input)?;
    if serde_json::to_vec(&input)
        .map_err(|_| PrepareError::InvalidBundle)?
        .len()
        > MAX_INPUT_BYTES
    {
        return Err(PrepareError::LimitExceeded);
    }
    Ok(input)
}

fn confirmed_scope(
    product: &PreparationProduct,
    bundle: &PreparationBundle,
) -> Result<CpcCampaignScope, PrepareError> {
    let confirmation = &product.cpc_scope;
    let ids = confirmation
        .campaign_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if ids.len() != confirmation.campaign_ids.len()
        || confirmation.source_ref.trim().is_empty()
        || confirmation.source_ref.len() > 256
        || confirmation.observed_at > bundle.as_of
        || bundle.as_of - confirmation.observed_at
            > Duration::hours(i64::from(bundle.policy.max_data_age_hours))
    {
        return Err(PrepareError::InvalidCampaignScope);
    }
    CpcCampaignScope::new(
        bundle.account_id.clone(),
        product.sku,
        ids,
        confirmation.source_ref.clone(),
    )
    .map_err(|_| PrepareError::InvalidCampaignScope)
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
struct SnapshotEnvelope {
    account_id: String,
    marketplace: String,
    source: String,
    storage: String,
    state: String,
    data_state: String,
    quality: Option<String>,
    pagination_complete: Option<bool>,
    snapshot_id: String,
    cutoff_at: DateTime<Utc>,
    source_as_of: DateTime<Utc>,
    observed_from: DateTime<Utc>,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
    total_rows: u64,
    rows: Vec<Value>,
    next_offset: Option<u32>,
}

impl SnapshotEnvelope {
    const fn first_observed(&self) -> DateTime<Utc> {
        self.observed_from
    }
}

struct CompleteSnapshot {
    snapshot_id: i64,
    envelope: SnapshotEnvelope,
    rows: Vec<Value>,
}

impl CompleteSnapshot {
    fn reference(&self) -> String {
        format!("ofk:{}:snapshot:{}", self.envelope.source, self.snapshot_id)
    }

    fn period_dates(&self) -> Result<(NaiveDate, NaiveDate), PrepareError> {
        let envelope = &self.envelope;
        if envelope.period_start >= envelope.period_end
            || envelope
                .period_start
                .with_timezone(&yekaterinburg_offset())
                .time()
                != NaiveTime::MIN
            || envelope
                .period_end
                .with_timezone(&yekaterinburg_offset())
                .time()
                != NaiveTime::MIN
            || envelope.period_end > envelope.first_observed()
        {
            return Err(PrepareError::InvalidSnapshot);
        }
        Ok((
            business_date(envelope.period_start),
            business_date(envelope.period_end - Duration::nanoseconds(1)),
        ))
    }

    fn validate_fact(&self, row: &FactProvenance) -> Result<(), PrepareError> {
        let envelope = &self.envelope;
        if row.snapshot_id != self.snapshot_id
            || row.account_id != envelope.account_id
            || row.marketplace != envelope.marketplace
            || row.source != envelope.source
            || row.cutoff_at != envelope.cutoff_at
            || row.source_as_of != envelope.source_as_of
            || row.snapshot_status != "succeeded"
            || !row.pagination_complete
        {
            return Err(PrepareError::InvalidSnapshot);
        }
        Ok(())
    }
}

fn complete_snapshots(
    pages: &[SnapshotPage],
    bundle: &PreparationBundle,
    source: &str,
) -> Result<BTreeMap<i64, CompleteSnapshot>, PrepareError> {
    if pages.len() > MAX_PAGES {
        return Err(PrepareError::LimitExceeded);
    }
    let mut groups = BTreeMap::<i64, BTreeMap<u32, SnapshotEnvelope>>::new();
    for page in pages {
        let envelope: SnapshotEnvelope = serde_json::from_value(page.response.clone())
            .map_err(|_| PrepareError::InvalidSnapshot)?;
        let id = envelope
            .snapshot_id
            .parse::<i64>()
            .map_err(|_| PrepareError::InvalidSnapshot)?;
        if id <= 0
            || envelope.account_id != bundle.account_id
            || envelope.marketplace != "ozon"
            || envelope.source != source
            || envelope.storage != "published_postgresql_snapshots"
            || !matches!(envelope.state.as_str(), "available" | "stale")
            || !matches!(envelope.quality.as_deref(), Some("complete" | "stale"))
            || envelope.pagination_complete != Some(true)
            || envelope.data_state
                != if envelope.total_rows == 0 {
                    "no_data"
                } else {
                    "available"
                }
            || envelope.source_as_of > bundle.as_of
            || envelope.first_observed() > envelope.source_as_of
            || envelope.cutoff_at > bundle.as_of
            || envelope.rows.len() > 1_000
        {
            return Err(PrepareError::InvalidSnapshot);
        }
        if envelope.total_rows > MAX_ROWS as u64 {
            return Err(PrepareError::LimitExceeded);
        }
        let source_kind = match source {
            "advertising" => SnapshotSource::Advertising,
            "stocks" => SnapshotSource::Stocks,
            _ => return Err(PrepareError::InvalidSnapshot),
        };
        SnapshotDescriptor::new(
            id,
            envelope.account_id.clone(),
            Marketplace::Ozon,
            source_kind,
            envelope.cutoff_at,
            envelope.source_as_of,
            envelope.period_start,
            envelope.period_end,
            u32::try_from(envelope.total_rows).map_err(|_| PrepareError::LimitExceeded)?,
            true,
            SnapshotStatus::Succeeded,
        )
        .map_err(|_| PrepareError::InvalidSnapshot)?;
        if groups
            .entry(id)
            .or_default()
            .insert(page.offset, envelope)
            .is_some()
        {
            return Err(PrepareError::IncompleteSnapshot);
        }
    }
    let mut complete = BTreeMap::new();
    let mut total_rows = 0usize;
    for (id, pages) in groups {
        let mut base = pages
            .get(&0)
            .cloned()
            .ok_or(PrepareError::IncompleteSnapshot)?;
        base.rows.clear();
        base.next_offset = None;
        let mut expected_offset = 0u32;
        let mut rows = Vec::new();
        for (offset, mut page) in pages {
            if offset != expected_offset {
                return Err(PrepareError::IncompleteSnapshot);
            }
            let count = u32::try_from(page.rows.len()).map_err(|_| PrepareError::LimitExceeded)?;
            expected_offset = offset
                .checked_add(count)
                .ok_or(PrepareError::LimitExceeded)?;
            let next = (u64::from(expected_offset) < page.total_rows).then_some(expected_offset);
            if page.next_offset != next || (count == 0 && page.total_rows != 0) {
                return Err(PrepareError::IncompleteSnapshot);
            }
            rows.append(&mut page.rows);
            page.next_offset = None;
            // Freshness state can change while all pinned pages are fetched;
            // it is not part of immutable snapshot identity.
            page.state.clone_from(&base.state);
            page.quality.clone_from(&base.quality);
            if page != base {
                return Err(PrepareError::InvalidSnapshot);
            }
        }
        if rows.len() as u64 != base.total_rows {
            return Err(PrepareError::IncompleteSnapshot);
        }
        total_rows = total_rows
            .checked_add(rows.len())
            .ok_or(PrepareError::LimitExceeded)?;
        if total_rows > MAX_ROWS {
            return Err(PrepareError::LimitExceeded);
        }
        complete.insert(
            id,
            CompleteSnapshot {
                snapshot_id: id,
                envelope: base,
                rows,
            },
        );
    }
    Ok(complete)
}

#[derive(Debug, Deserialize)]
struct FactProvenance {
    account_id: String,
    marketplace: String,
    source: String,
    snapshot_id: i64,
    cutoff_at: DateTime<Utc>,
    source_as_of: DateTime<Utc>,
    snapshot_status: String,
    pagination_complete: bool,
}

#[derive(Debug, Deserialize)]
struct AdvertisingRow {
    #[serde(flatten)]
    provenance: FactProvenance,
    business_date: NaiveDate,
    campaign_id: u64,
    sku: u64,
    currency: String,
    impressions: u64,
    clicks: u64,
    spend_minor: u64,
    attributed_orders: u64,
    attributed_revenue_minor: u64,
    basket_additions: u64,
    model_attributed_orders: u64,
    model_attributed_revenue_minor: u64,
    product_price_minor: u64,
    average_cpc_minor: Option<u64>,
    cpm_minor: Option<u64>,
    cpl_minor: Option<u64>,
}

impl AdvertisingRow {
    fn into_fact(self) -> PublishedAdvertisingFact {
        PublishedAdvertisingFact {
            account_id: self.provenance.account_id,
            business_date: self.business_date,
            campaign_id: self.campaign_id,
            sku: self.sku,
            impressions: self.impressions,
            clicks: self.clicks,
            spend_minor: self.spend_minor,
            attributed_orders: self.attributed_orders,
            attributed_revenue_minor: self.attributed_revenue_minor,
            basket_additions: self.basket_additions,
            model_attributed_orders: self.model_attributed_orders,
            model_attributed_revenue_minor: self.model_attributed_revenue_minor,
            product_price_minor: self.product_price_minor,
            average_cpc_minor: self.average_cpc_minor,
            cpm_minor: self.cpm_minor,
            cpl_minor: self.cpl_minor,
        }
    }
}

#[derive(Debug, Deserialize)]
struct StockRow {
    #[serde(flatten)]
    provenance: FactProvenance,
    sku: u64,
    warehouse_id: String,
    sellable_units: u64,
}

fn stock_evidence(
    snapshots: &BTreeMap<i64, CompleteSnapshot>,
    selected: &BTreeSet<u64>,
    refs: &mut BTreeSet<String>,
) -> Result<BTreeMap<u64, StockEvidence>, PrepareError> {
    let mut stocks = BTreeMap::<u64, StockEvidence>::new();
    for snapshot in snapshots.values() {
        if snapshot.envelope.period_start != snapshot.envelope.source_as_of
            || snapshot.envelope.period_end != snapshot.envelope.source_as_of
        {
            return Err(PrepareError::InvalidSnapshot);
        }
        let mut keys = BTreeSet::new();
        let mut verified_rows = Vec::new();
        let mut fulfillment = false;
        let mut warehouse = false;
        let mut legacy = false;
        for value in &snapshot.rows {
            let row: StockRow =
                serde_json::from_value(value.clone()).map_err(|_| PrepareError::InvalidSnapshot)?;
            snapshot.validate_fact(&row.provenance)?;
            if !positive_id(row.sku) || !keys.insert((row.sku, row.warehouse_id.clone())) {
                return Err(PrepareError::InvalidSnapshot);
            }
            if matches!(row.warehouse_id.as_str(), "FBO" | "FBS" | "RFBS") {
                // The legacy producer stores product_id as sku and present
                // without subtracting reserved. Neither identifier nor quantity
                // can be interpreted as native sellable SKU stock here.
                legacy = true;
            } else if is_sku_fulfillment_dimension(&row.warehouse_id) {
                fulfillment = true;
                verified_rows.push(row);
            } else {
                let warehouse_id = row
                    .warehouse_id
                    .strip_prefix("fbo:")
                    .or_else(|| row.warehouse_id.strip_prefix("fbs:"))
                    .and_then(|id| id.parse::<u64>().ok())
                    .filter(|id| positive_id(*id));
                if warehouse_id.is_none() {
                    return Err(PrepareError::InvalidSnapshot);
                }
                warehouse = true;
                verified_rows.push(row);
            }
        }
        if legacy {
            refs.insert(format!(
                "unsupported-legacy-stock-identity:{}",
                snapshot.snapshot_id
            ));
            continue;
        }
        // These are alternative complete collection modes. Combining their
        // totals would count the same units twice, including zero-valued rows.
        if fulfillment && warehouse {
            return Err(PrepareError::InvalidSnapshot);
        }
        for row in verified_rows {
            if selected.contains(&row.sku) {
                let stock = stocks.entry(row.sku).or_insert_with(|| StockEvidence {
                    sellable_units: 0,
                    observed_at: snapshot.envelope.first_observed(),
                });
                stock.sellable_units = stock
                    .sellable_units
                    .checked_add(row.sellable_units)
                    .ok_or(PrepareError::Optimizer(OptimizerError::Overflow))?;
            }
        }
    }
    Ok(stocks)
}

const fn positive_id(value: u64) -> bool {
    value > 0 && value <= i64::MAX.cast_unsigned()
}

#[cfg(test)]
#[path = "prepare_tests.rs"]
mod tests;
