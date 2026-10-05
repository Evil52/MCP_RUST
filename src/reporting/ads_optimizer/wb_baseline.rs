//! Frozen WB measurement baseline. Attribution and causal lift remain unverified.

mod model;
pub use model::*;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use chrono::Datelike as _;
use sha2::{Digest as _, Sha256};

use super::{MAX_INPUT_BYTES, OptimizerError, scaled, wb_review};
use crate::reporting::business_date;

pub fn analyze_wb_baseline_export(bytes: &[u8]) -> Result<WbBaselineReport, OptimizerError> {
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(OptimizerError::LimitExceeded);
    }
    analyze(serde_json::from_slice(bytes).map_err(|_| OptimizerError::InvalidInput)?)
}

pub fn analyze(mut input: WbBaselineInput) -> Result<WbBaselineReport, OptimizerError> {
    validate_sales(&input)?;
    let skus: BTreeSet<_> = input.advertising.iter().filter_map(|r| r.sku).collect();
    // Reuse the observation contract for calendar, duplicates, limits and IDs.
    // Neutral composition/stock fields are internal, never evidence of a read.
    let review = wb_review::analyze(wb_review::WbReviewInput {
        version: input.version,
        account_id: input.account_id.clone(),
        as_of: input.as_of,
        date_from: input.date_from,
        date_to: input.date_to,
        coverage: input.coverage.clone(),
        advertising: input.advertising.clone(),
        products: skus
            .iter()
            .map(|&sku| wb_review::ProductEvidence {
                sku,
                ordered_units: None,
                fbw: None,
                fbs: None,
            })
            .collect(),
        composition: wb_review::CampaignComposition {
            observed_at: input.as_of,
            active_and_paused_complete: false,
            campaigns: Vec::new(),
        },
    })?;
    input.coverage.sort_by_key(|r| r.date);
    input
        .advertising
        .sort_by_key(|r| (r.date, r.campaign_id, r.sku));
    input.sales.sort_by_key(|r| r.date);
    let bytes = serde_json::to_vec(&input).map_err(|_| OptimizerError::InvalidInput)?;
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(OptimizerError::LimitExceeded);
    }
    let mut source_sha256 = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(source_sha256, "{byte:02x}").map_err(|_| OptimizerError::InvalidInput)?;
    }
    let dates: Vec<_> = input
        .date_from
        .iter_days()
        .take_while(|date| *date <= input.date_to)
        .collect();
    let missing_ad_dates = missing_dates(
        &dates,
        input.coverage.iter().filter(|r| r.complete).map(|r| r.date),
    );
    let missing_sales_dates = missing_dates(
        &dates,
        input.sales.iter().filter(|r| r.complete).map(|r| r.date),
    );
    let mut sku_totals = None::<BaselineMetrics>;
    let mut unallocated = None::<BaselineMetrics>;
    let mut by_campaign = BTreeMap::<u64, CampaignAccumulator>::new();
    let mut by_date = BTreeMap::<_, CampaignAccumulator>::new();
    for row in &input.advertising {
        let campaign = by_campaign.entry(row.campaign_id).or_default();
        campaign.dates.insert(row.date);
        let day = by_date.entry(row.date).or_default();
        if row.sku.is_some() {
            sku_totals.get_or_insert_default().add(row)?;
            campaign.sku.get_or_insert_default().add(row)?;
            day.sku.get_or_insert_default().add(row)?;
        } else {
            unallocated.get_or_insert_default().add(row)?;
            campaign.unallocated.get_or_insert_default().add(row)?;
            day.unallocated.get_or_insert_default().add(row)?;
        }
    }
    let products = review
        .products
        .into_iter()
        .map(|p| {
            Ok(ProductBaseline {
                sku: p.sku,
                missing_ad_dates: p.missing_ad_dates,
                metrics: BaselineMetrics {
                    spend_minor: p.spend_minor,
                    clicks: p.clicks,
                    attributed_orders: p.attributed_orders,
                    attributed_revenue_minor: p.attributed_revenue_minor,
                    ..BaselineMetrics::default()
                }
                .finish()?,
            })
        })
        .collect::<Result<Vec<_>, OptimizerError>>()?;
    let campaigns = by_campaign
        .into_iter()
        .map(|(id, c)| {
            Ok(CampaignBaseline {
                campaign_id: id,
                missing_ad_dates: missing_dates(&dates, c.dates.into_iter()),
                sku_metrics: c.sku.map(BaselineMetrics::finish).transpose()?,
                unallocated_campaign_metrics: c
                    .unallocated
                    .map(BaselineMetrics::finish)
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, OptimizerError>>()?;
    let daily = dates
        .iter()
        .map(|&date| {
            let day = input.coverage.iter().find(|r| r.date == date);
            let metrics = by_date.remove(&date);
            let (sku_metrics, unallocated_campaign_metrics) = match metrics {
                Some(day) => (
                    day.sku.map(BaselineMetrics::finish).transpose()?,
                    day.unallocated.map(BaselineMetrics::finish).transpose()?,
                ),
                None => (None, None),
            };
            let sale = input.sales.iter().find(|r| r.date == date).cloned();
            Ok(DayBaseline {
                date,
                snapshot_id: day.map(|r| r.snapshot_id),
                observed_at: day.map(|r| r.observed_at),
                publication_complete: day.is_some_and(|r| r.complete),
                // Empty publication is distinct from explicit zero-activity rows.
                advertising_rows_observed: sku_metrics.is_some()
                    || unallocated_campaign_metrics.is_some(),
                sku_metrics,
                unallocated_campaign_metrics,
                sales: sale,
            })
        })
        .collect::<Result<Vec<_>, OptimizerError>>()?;
    let observed_sales =
        input
            .sales
            .iter()
            .try_fold(StoreSalesTotals::default(), |mut sum, day| {
                sum.ordered_units = add(sum.ordered_units, day.ordered_units)?;
                sum.operational_gmv_minor =
                    add(sum.operational_gmv_minor, day.operational_gmv_minor)?;
                Ok::<_, OptimizerError>(sum)
            })?;
    Ok(WbBaselineReport {
        version: 1,
        mode: "measurement_only",
        marketplace: "wildberries",
        currency: "RUB",
        account_id: input.account_id,
        as_of: input.as_of,
        date_from: input.date_from,
        date_to: input.date_to,
        source_sha256,
        advertising_coverage_complete: missing_ad_dates.is_empty(),
        missing_ad_dates,
        sales_coverage_complete: missing_sales_dates.is_empty(),
        missing_sales_dates,
        observed_store_sales: (!input.sales.is_empty()).then_some(observed_sales),
        observed_sku_metrics: sku_totals.map(BaselineMetrics::finish).transpose()?,
        unallocated_campaign_metrics: unallocated.map(BaselineMetrics::finish).transpose()?,
        daily,
        campaigns,
        products,
        attribution_maturity_verified: false,
        causal_effect_verified: false,
        current_composition_verified: false,
        auto_apply_allowed: false,
    })
}

fn validate_sales(input: &WbBaselineInput) -> Result<(), OptimizerError> {
    if input.sales.len() > 31 {
        return Err(OptimizerError::LimitExceeded);
    }
    if !(2000..=2200).contains(&input.as_of.year())
        || !(2000..=2200).contains(&input.date_from.year())
        || !(2000..=2200).contains(&input.date_to.year())
        || input.advertising.iter().any(|r| {
            r.campaign_id > i64::MAX.cast_unsigned()
                || r.sku.is_some_and(|id| id > i64::MAX.cast_unsigned())
        })
        || input
            .coverage
            .iter()
            .any(|r| business_date(r.observed_at) <= r.date)
    {
        return Err(OptimizerError::InvalidInput);
    }
    let mut dates = BTreeSet::new();
    let mut snapshots: BTreeSet<_> = input.coverage.iter().map(|r| r.snapshot_id).collect();
    for day in &input.sales {
        if day.date < input.date_from
            || day.date > input.date_to
            || day.snapshot_id == 0
            || day.snapshot_id > i64::MAX.cast_unsigned()
            || day.observed_at > input.as_of
            || business_date(day.observed_at) <= day.date
        {
            return Err(OptimizerError::InvalidInput);
        }
        if !dates.insert(day.date) || !snapshots.insert(day.snapshot_id) {
            return Err(OptimizerError::DuplicateEvidence);
        }
    }
    Ok(())
}

fn missing_dates(
    dates: &[chrono::NaiveDate],
    observed: impl Iterator<Item = chrono::NaiveDate>,
) -> Vec<chrono::NaiveDate> {
    let observed: BTreeSet<_> = observed.collect();
    dates
        .iter()
        .filter(|date| !observed.contains(date))
        .copied()
        .collect()
}

#[derive(Default)]
struct CampaignAccumulator {
    dates: BTreeSet<chrono::NaiveDate>,
    sku: Option<BaselineMetrics>,
    unallocated: Option<BaselineMetrics>,
}

fn add(left: u64, right: u64) -> Result<u64, OptimizerError> {
    left.checked_add(right).ok_or(OptimizerError::Overflow)
}

impl BaselineMetrics {
    fn add(&mut self, row: &wb_review::AdvertisingDay) -> Result<(), OptimizerError> {
        self.spend_minor = add(self.spend_minor, row.spend_minor)?;
        self.clicks = add(self.clicks, row.clicks)?;
        self.attributed_orders = add(self.attributed_orders, row.attributed_orders)?;
        self.attributed_revenue_minor =
            add(self.attributed_revenue_minor, row.attributed_revenue_minor)?;
        Ok(())
    }

    fn finish(mut self) -> Result<Self, OptimizerError> {
        self.drr_bps = ratio(self.spend_minor, 10_000, self.attributed_revenue_minor)?;
        self.cpc_minor = ratio(self.spend_minor, 1, self.clicks)?;
        self.cpo_minor = ratio(self.spend_minor, 1, self.attributed_orders)?;
        self.ad_conversion_bps = ratio(self.attributed_orders, 10_000, self.clicks)?;
        Ok(self)
    }
}

fn ratio(value: u64, scale: u64, denominator: u64) -> Result<Option<u64>, OptimizerError> {
    if denominator == 0 {
        Ok(None)
    } else {
        scaled(value, scale, denominator).map(Some)
    }
}

#[cfg(test)]
mod tests;
