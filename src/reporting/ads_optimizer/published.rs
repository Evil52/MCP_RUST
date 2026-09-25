//! Pure adaptation of already selected, published SKU advertising facts.
//!
//! Published statistics do not carry a campaign pricing model. The caller
//! must independently confirm every included campaign uses CPC and retain
//! the evidence reference. This module cannot verify that assertion and does
//! not infer CPC from clicks, spend, or an average-CPC field.
//!
//! Aggregation proves neither complete campaign coverage nor attribution
//! maturity. A caller must separately verify the frozen source manifest,
//! its actual observation time, and the requested history's coverage before
//! constructing `ShadowInput`. Missing rows are never fabricated as zeros.

use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDate;

use super::{AdvertisingDay, MAX_WINDOW_DAYS, OptimizerError};
use crate::reporting::postgres_snapshot::PublishedAdvertisingFact;

const MAX_ADVERTISING_ROWS: usize = 25_000;

/// Explicit caller assertion backed by campaign-settings evidence, not by
/// the advertising rows themselves. Only one account and SKU are accepted.
#[derive(Debug, Clone)]
pub struct CpcCampaignScope {
    account_id: String,
    sku: u64,
    campaign_ids: BTreeSet<u64>,
    confirmation_source_ref: String,
}

impl CpcCampaignScope {
    /// Construct this only after independently checking the current campaign
    /// pricing models. `confirmation_source_ref` identifies that evidence.
    pub fn new(
        account_id: String,
        sku: u64,
        campaign_ids: BTreeSet<u64>,
        confirmation_source_ref: String,
    ) -> Result<Self, OptimizerError> {
        if account_id.is_empty()
            || account_id.len() > 128
            || !account_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            || !valid_id(sku)
            || campaign_ids.is_empty()
            || campaign_ids.len() > MAX_ADVERTISING_ROWS
            || campaign_ids.iter().any(|id| !valid_id(*id))
            || confirmation_source_ref.trim().is_empty()
            || confirmation_source_ref.len() > 512
            || confirmation_source_ref.chars().any(char::is_control)
        {
            return Err(OptimizerError::InvalidInput);
        }
        Ok(Self {
            account_id,
            sku,
            campaign_ids,
            confirmation_source_ref,
        })
    }

    /// Retain this alongside the frozen snapshot references in the input's
    /// provenance; construction does not authenticate the referenced evidence.
    #[must_use]
    pub fn confirmation_source_ref(&self) -> &str {
        &self.confirmation_source_ref
    }
}

/// Sum direct attribution across explicitly confirmed CPC campaigns.
///
/// Every input row must already belong to the requested account, SKU, and
/// inclusive date interval. Foreign rows are rejected rather than silently
/// discarded. Legacy `sku = 0` campaign totals are never product evidence.
/// Model-attributed orders and revenue are deliberately excluded.
///
/// Output is sorted by date and contains only dates present in the input.
/// An empty result or absent campaign/date is unknown activity; callers must
/// not treat this result alone as proof of complete coverage.
pub fn aggregate_cpc_days(
    facts: &[PublishedAdvertisingFact],
    scope: &CpcCampaignScope,
    window_start: NaiveDate,
    window_end: NaiveDate,
) -> Result<Vec<AdvertisingDay>, OptimizerError> {
    if window_start > window_end || (window_end - window_start).num_days() >= MAX_WINDOW_DAYS {
        return Err(OptimizerError::InvalidInput);
    }
    if facts.len() > MAX_ADVERTISING_ROWS {
        return Err(OptimizerError::LimitExceeded);
    }
    let mut keys = BTreeSet::new();
    let mut days = BTreeMap::<NaiveDate, AdvertisingDay>::new();
    for fact in facts {
        if fact.account_id != scope.account_id
            || fact.sku != scope.sku
            || !scope.campaign_ids.contains(&fact.campaign_id)
            || fact.business_date < window_start
            || fact.business_date > window_end
        {
            return Err(OptimizerError::InvalidInput);
        }
        if !keys.insert((fact.campaign_id, fact.sku, fact.business_date)) {
            return Err(OptimizerError::DuplicateEvidence);
        }
        let day = days.entry(fact.business_date).or_insert(AdvertisingDay {
            date: fact.business_date,
            clicks: 0,
            spend_minor: 0,
            direct_orders: 0,
            direct_revenue_minor: 0,
        });
        day.clicks = add(day.clicks, fact.clicks)?;
        day.spend_minor = add(day.spend_minor, fact.spend_minor)?;
        day.direct_orders = add(day.direct_orders, fact.attributed_orders)?;
        day.direct_revenue_minor = add(day.direct_revenue_minor, fact.attributed_revenue_minor)?;
    }
    Ok(days.into_values().collect())
}

fn valid_id(value: u64) -> bool {
    value > 0 && i64::try_from(value).is_ok()
}

fn add(left: u64, right: u64) -> Result<u64, OptimizerError> {
    left.checked_add(right).ok_or(OptimizerError::Overflow)
}

#[cfg(test)]
#[path = "published_tests.rs"]
mod tests;
