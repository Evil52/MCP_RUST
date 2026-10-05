//! Offline WB diagnostics. No clients, writes, bid proposals or budget allocation.
//! Captured current composition is independent of historical advertising facts.

mod model;
pub use model::*;

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, NaiveDate, Utc};
use sha2::{Digest as _, Sha256};

use super::{MAX_INPUT_BYTES, OptimizerError};

const MAX_DAYS: i64 = 31;
const MAX_AD_ROWS: usize = 25_000;
const MAX_PRODUCTS: usize = 10_000;
const MAX_CAMPAIGNS: usize = 1_000;
const MAX_CAMPAIGN_SKUS: usize = 25_000;
const FRESHNESS_MINUTES: i64 = 30;

pub fn analyze_wb_export(bytes: &[u8]) -> Result<WbReviewReport, OptimizerError> {
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(OptimizerError::LimitExceeded);
    }
    let input = serde_json::from_slice(bytes).map_err(|_| OptimizerError::InvalidInput)?;
    analyze(input)
}

/// Validates and canonicalizes captured evidence before producing review tasks.
/// Missing dates remain unknown even when the day-level publication is complete.
pub fn analyze(mut input: WbReviewInput) -> Result<WbReviewReport, OptimizerError> {
    validate(&input)?;
    input.coverage.sort_by_key(|row| row.date);
    input
        .advertising
        .sort_by_key(|row| (row.date, row.campaign_id, row.sku));
    input.products.sort_by_key(|row| row.sku);
    input
        .composition
        .campaigns
        .sort_by_key(|row| row.campaign_id);
    for campaign in &mut input.composition.campaigns {
        campaign.sku_ids.sort_unstable();
    }
    let bytes = serde_json::to_vec(&input).map_err(|_| OptimizerError::InvalidInput)?;
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(OptimizerError::LimitExceeded);
    }
    let source_sha256 =
        Sha256::digest(bytes)
            .iter()
            .fold(String::with_capacity(64), |mut text, byte| {
                use std::fmt::Write as _;
                write!(text, "{byte:02x}").expect("writing to String cannot fail");
                text
            });
    let dates = date_range(&input)?;
    let advertising_coverage_complete =
        input.coverage.len() == dates.len() && input.coverage.iter().all(|row| row.complete);
    let composition_complete_and_fresh = input.composition.active_and_paused_complete
        && fresh(input.composition.observed_at, input.as_of);
    let mut current_by_sku = BTreeMap::<u64, Vec<CampaignLink>>::new();
    if composition_complete_and_fresh {
        for campaign in &input.composition.campaigns {
            for &sku in &campaign.sku_ids {
                current_by_sku.entry(sku).or_default().push(CampaignLink {
                    campaign_id: campaign.campaign_id,
                    status: campaign.status,
                    pricing_model: campaign.pricing_model,
                });
            }
        }
    }
    let mut aggregates = BTreeMap::<u64, Aggregate>::new();
    let mut campaign_rows_without_sku = Vec::new();
    for row in &input.advertising {
        let Some(sku) = row.sku else {
            campaign_rows_without_sku.push(row.clone());
            continue;
        };
        let agg = aggregates.entry(sku).or_default();
        agg.dates.insert(row.date);
        agg.campaigns.insert(row.campaign_id);
        agg.spend = add(agg.spend, row.spend_minor)?;
        agg.clicks = add(agg.clicks, row.clicks)?;
        agg.orders = add(agg.orders, row.attributed_orders)?;
        agg.revenue = add(agg.revenue, row.attributed_revenue_minor)?;
    }
    let mut products = Vec::with_capacity(input.products.len());
    for product in &input.products {
        let agg = aggregates.remove(&product.sku).unwrap_or_default();
        let current_campaigns = current_by_sku.remove(&product.sku).unwrap_or_default();
        let promotion_state = promotion_state(composition_complete_and_fresh, &current_campaigns);
        let warehouse_presence = stock_presence(product.fbw.as_ref(), input.as_of);
        let seller_presence = stock_presence(product.fbs.as_ref(), input.as_of);
        let inventory_signal = match (warehouse_presence, seller_presence) {
            (Some(false), Some(false)) => InventorySignal::BothChannelsZero,
            (Some(false), _) => InventorySignal::FbwZeroCheckFbs,
            (Some(true), _) | (_, Some(true)) => InventorySignal::StockPresent,
            _ => InventorySignal::Unknown,
        };
        let spend_without_observed_ad_orders = agg.spend > 0 && agg.orders == 0;
        let action = review_action(
            advertising_coverage_complete,
            spend_without_observed_ad_orders,
            promotion_state,
        );
        products.push(ProductReview {
            sku: product.sku,
            missing_ad_dates: dates
                .iter()
                .filter(|date| !agg.dates.contains(date))
                .copied()
                .collect(),
            observed_ad_dates: agg.dates.into_iter().collect(),
            historical_campaign_ids: agg.campaigns.into_iter().collect(),
            current_campaigns,
            promotion_state,
            spend_minor: agg.spend,
            clicks: agg.clicks,
            attributed_orders: agg.orders,
            attributed_revenue_minor: agg.revenue,
            ordered_units: product.ordered_units,
            spend_without_observed_ad_orders,
            fbw: product.fbw.clone(),
            fbs: product.fbs.clone(),
            fbw_has_stock: warehouse_presence,
            fbs_has_stock: seller_presence,
            inventory_signal,
            action,
        });
    }
    Ok(WbReviewReport {
        version: 1,
        mode: "observation",
        marketplace: "wildberries",
        currency: "RUB",
        account_id: input.account_id,
        as_of: input.as_of,
        date_from: input.date_from,
        date_to: input.date_to,
        source_sha256,
        advertising_coverage_complete,
        composition_complete_and_fresh,
        composition_observed_at: input.composition.observed_at,
        attribution_maturity_verified: false,
        auto_apply_allowed: false,
        campaign_rows_without_sku,
        products,
    })
}

#[derive(Default)]
struct Aggregate {
    dates: BTreeSet<chrono::NaiveDate>,
    campaigns: BTreeSet<u64>,
    spend: u64,
    clicks: u64,
    orders: u64,
    revenue: u64,
}

fn add(left: u64, right: u64) -> Result<u64, OptimizerError> {
    left.checked_add(right).ok_or(OptimizerError::Overflow)
}

fn fresh(observed_at: DateTime<Utc>, as_of: DateTime<Utc>) -> bool {
    observed_at <= as_of && as_of - observed_at <= Duration::minutes(FRESHNESS_MINUTES)
}

fn stock_presence(stock: Option<&StockObservation>, as_of: DateTime<Utc>) -> Option<bool> {
    let stock = stock?;
    if !fresh(stock.observed_at, as_of) {
        return None;
    }
    let units = stock.units?;
    // A partial positive observation proves some stock; partial zero does not
    // prove that the unobserved warehouses/sizes are also empty.
    (units > 0 || stock.quality == StockQuality::Complete).then_some(units > 0)
}

fn date_range(input: &WbReviewInput) -> Result<Vec<chrono::NaiveDate>, OptimizerError> {
    let days = input
        .date_to
        .signed_duration_since(input.date_from)
        .num_days()
        + 1;
    if !(1..=MAX_DAYS).contains(&days) {
        return Err(OptimizerError::InvalidInput);
    }
    (0..days)
        .map(|offset| {
            input
                .date_from
                .checked_add_signed(Duration::days(offset))
                .ok_or(OptimizerError::InvalidInput)
        })
        .collect()
}

fn promotion_state(
    composition_complete_and_fresh: bool,
    current_campaigns: &[CampaignLink],
) -> PromotionState {
    if !composition_complete_and_fresh {
        PromotionState::Unknown
    } else if current_campaigns
        .iter()
        .any(|c| c.status == CampaignStatus::Active)
    {
        PromotionState::Active
    } else if current_campaigns.is_empty() {
        PromotionState::AbsentFromActiveAndPaused
    } else {
        PromotionState::Paused
    }
}

const fn review_action(
    advertising_coverage_complete: bool,
    spend_without_observed_ad_orders: bool,
    promotion_state: PromotionState,
) -> ReviewAction {
    if !advertising_coverage_complete {
        ReviewAction::RestoreAdvertisingCoverage
    } else if !spend_without_observed_ad_orders {
        ReviewAction::Observe
    } else {
        match promotion_state {
            PromotionState::Active => ReviewAction::ReviewActiveProduct,
            PromotionState::Paused => ReviewAction::ReviewBeforeResuming,
            PromotionState::AbsentFromActiveAndPaused => ReviewAction::ReviewHistoricalSpend,
            PromotionState::Unknown => ReviewAction::VerifyCurrentComposition,
        }
    }
}

fn validate(input: &WbReviewInput) -> Result<(), OptimizerError> {
    if input.version != 1
        || input.account_id.is_empty()
        || input.account_id.len() > 128
        || !input
            .account_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        || input.date_to >= crate::reporting::business_date(input.as_of)
        || input.composition.observed_at > input.as_of
    {
        return Err(OptimizerError::InvalidInput);
    }
    let days = date_range(input)?;
    if input.coverage.len() > days.len()
        || input.advertising.len() > MAX_AD_ROWS
        || input.products.len() > MAX_PRODUCTS
        || input.composition.campaigns.len() > MAX_CAMPAIGNS
    {
        return Err(OptimizerError::LimitExceeded);
    }
    let dates = validate_coverage(input, &days)?;
    let skus = validate_products(input)?;
    validate_advertising(input, &dates, &skus)?;
    validate_composition(input)
}

fn validate_coverage(
    input: &WbReviewInput,
    days: &[NaiveDate],
) -> Result<BTreeSet<NaiveDate>, OptimizerError> {
    let mut dates = BTreeSet::new();
    let mut snapshots = BTreeSet::new();
    for row in &input.coverage {
        if !days.contains(&row.date) || row.snapshot_id == 0 || row.observed_at > input.as_of {
            return Err(OptimizerError::InvalidInput);
        }
        if !dates.insert(row.date) || !snapshots.insert(row.snapshot_id) {
            return Err(OptimizerError::DuplicateEvidence);
        }
    }
    Ok(dates)
}

fn validate_products(input: &WbReviewInput) -> Result<BTreeSet<u64>, OptimizerError> {
    let mut skus = BTreeSet::new();
    for product in &input.products {
        if product.sku == 0
            || [&product.fbw, &product.fbs]
                .into_iter()
                .flatten()
                .any(|s| s.observed_at > input.as_of)
        {
            return Err(OptimizerError::InvalidInput);
        }
        if !skus.insert(product.sku) {
            return Err(OptimizerError::DuplicateEvidence);
        }
    }
    Ok(skus)
}

fn validate_advertising(
    input: &WbReviewInput,
    dates: &BTreeSet<NaiveDate>,
    skus: &BTreeSet<u64>,
) -> Result<(), OptimizerError> {
    let mut keys = BTreeSet::new();
    for row in &input.advertising {
        if row.campaign_id == 0
            || row.sku.is_some_and(|sku| !skus.contains(&sku))
            || !dates.contains(&row.date)
        {
            return Err(OptimizerError::InvalidInput);
        }
        if !keys.insert((row.date, row.campaign_id, row.sku)) {
            return Err(OptimizerError::DuplicateEvidence);
        }
    }
    Ok(())
}

fn validate_composition(input: &WbReviewInput) -> Result<(), OptimizerError> {
    let mut campaigns = BTreeSet::new();
    let mut total_skus = 0;
    for campaign in &input.composition.campaigns {
        if campaign.campaign_id == 0 || campaign.sku_ids.contains(&0) {
            return Err(OptimizerError::InvalidInput);
        }
        if !campaigns.insert(campaign.campaign_id)
            || campaign.sku_ids.iter().collect::<BTreeSet<_>>().len() != campaign.sku_ids.len()
        {
            return Err(OptimizerError::DuplicateEvidence);
        }
        total_skus += campaign.sku_ids.len();
        if total_skus > MAX_CAMPAIGN_SKUS {
            return Err(OptimizerError::LimitExceeded);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
