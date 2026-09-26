//! Offline trend diagnostic for the Ozon campaign daily report.
//!
//! Campaign-reported orders and revenue are not silently re-labelled as
//! direct SKU attribution. This output cannot be passed to `recommend`.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    MAX_INPUT_BYTES, MAX_WINDOW_DAYS, OptimizerError, reconciliation::ReconciliationCampaignRow,
};

const MAX_ROWS: usize = 25_000;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignHistoryInput {
    pub version: u32,
    pub account_id: String,
    pub source_ref: String,
    pub observed_at: DateTime<Utc>,
    pub as_of: DateTime<Utc>,
    pub window_start: NaiveDate,
    pub window_end: NaiveDate,
    /// Operator assertion about the upstream export, independent of per-campaign gaps.
    pub source_coverage_complete: bool,
    pub rows: Vec<ReconciliationCampaignRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignHistoryMode {
    DiagnosticOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CampaignHistorySummary {
    pub campaign_id: u64,
    pub observed_days: u16,
    pub expected_days: u16,
    /// Missing rows are unknown, never zero-activity days.
    pub missing_dates: Vec<NaiveDate>,
    pub calendar_complete: bool,
    pub clicks: u64,
    pub spend_minor: u64,
    pub orders: u64,
    pub revenue_minor: u64,
    /// Campaign report ratio, floored to basis points; not direct-SKU DRR.
    pub reported_drr_bps: Option<u64>,
    /// Campaign report mean cost per click, floored to kopecks.
    pub average_cpc_minor: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CampaignHistoryReport {
    pub version: u32,
    pub mode: CampaignHistoryMode,
    pub currency: String,
    pub account_id: String,
    pub source_ref: String,
    pub observed_at: DateTime<Utc>,
    pub as_of: DateTime<Utc>,
    pub window_start: NaiveDate,
    pub window_end: NaiveDate,
    pub source_coverage_complete: bool,
    /// The daily campaign report does not establish direct SKU attribution.
    pub direct_sku_attribution_verified: bool,
    pub input_sha256: String,
    pub campaigns: Vec<CampaignHistorySummary>,
}

pub fn parse_campaign_history_input(bytes: &[u8]) -> Result<CampaignHistoryInput, OptimizerError> {
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(OptimizerError::LimitExceeded);
    }
    let input = serde_json::from_slice(bytes).map_err(|_| OptimizerError::InvalidInput)?;
    validate(&input)?;
    Ok(input)
}

/// Aggregates only within the same campaign. Missing days remain explicit.
pub fn analyze_campaign_history(
    mut input: CampaignHistoryInput,
) -> Result<CampaignHistoryReport, OptimizerError> {
    validate(&input)?;
    input.rows.sort_by_key(|row| (row.campaign_id, row.date));
    let canonical = serde_json::to_vec(&input).map_err(|_| OptimizerError::InvalidInput)?;
    if canonical.len() > MAX_INPUT_BYTES {
        return Err(OptimizerError::LimitExceeded);
    }
    let input_sha256 =
        Sha256::digest(canonical)
            .iter()
            .fold(String::with_capacity(64), |mut output, byte| {
                use std::fmt::Write as _;
                write!(output, "{byte:02x}").expect("writing to String cannot fail");
                output
            });
    let mut groups = BTreeMap::<u64, (BTreeSet<NaiveDate>, u64, u64, u64, u64)>::new();
    for row in &input.rows {
        let (dates, clicks, spend, orders, revenue) = groups.entry(row.campaign_id).or_default();
        dates.insert(row.date);
        *clicks = clicks
            .checked_add(row.clicks)
            .ok_or(OptimizerError::Overflow)?;
        *spend = spend
            .checked_add(row.spend_minor)
            .ok_or(OptimizerError::Overflow)?;
        *orders = orders
            .checked_add(row.orders)
            .ok_or(OptimizerError::Overflow)?;
        *revenue = revenue
            .checked_add(row.revenue_minor)
            .ok_or(OptimizerError::Overflow)?;
    }
    let expected_days = u16::try_from((input.window_end - input.window_start).num_days() + 1)
        .map_err(|_| OptimizerError::LimitExceeded)?;
    let mut campaigns = Vec::with_capacity(groups.len());
    for (campaign_id, (dates, clicks, spend_minor, orders, revenue_minor)) in groups {
        let mut missing_dates = Vec::new();
        let mut date = input.window_start;
        while date <= input.window_end {
            if !dates.contains(&date) {
                missing_dates.push(date);
            }
            date = date.succ_opt().ok_or(OptimizerError::InvalidInput)?;
        }
        let reported_drr_bps = if revenue_minor == 0 {
            None
        } else {
            Some(
                u64::try_from(u128::from(spend_minor) * 10_000 / u128::from(revenue_minor))
                    .map_err(|_| OptimizerError::Overflow)?,
            )
        };
        campaigns.push(CampaignHistorySummary {
            campaign_id,
            observed_days: u16::try_from(dates.len()).map_err(|_| OptimizerError::LimitExceeded)?,
            expected_days,
            calendar_complete: missing_dates.is_empty(),
            missing_dates,
            clicks,
            spend_minor,
            orders,
            revenue_minor,
            reported_drr_bps,
            average_cpc_minor: (clicks > 0).then(|| spend_minor / clicks),
        });
    }
    Ok(CampaignHistoryReport {
        version: 1,
        mode: CampaignHistoryMode::DiagnosticOnly,
        currency: "RUB".to_owned(),
        account_id: input.account_id,
        source_ref: input.source_ref,
        observed_at: input.observed_at,
        as_of: input.as_of,
        window_start: input.window_start,
        window_end: input.window_end,
        source_coverage_complete: input.source_coverage_complete,
        direct_sku_attribution_verified: false,
        input_sha256,
        campaigns,
    })
}

fn validate(input: &CampaignHistoryInput) -> Result<(), OptimizerError> {
    let days = (input.window_end - input.window_start).num_days() + 1;
    if input.version != 1
        || input.account_id.is_empty()
        || input.account_id.len() > 128
        || !input
            .account_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
        || input.source_ref.trim().is_empty()
        || input.source_ref.len() > 256
        || input.source_ref.chars().any(char::is_control)
        || !(2000..=2200).contains(&input.observed_at.year())
        || !(2000..=2200).contains(&input.as_of.year())
        || !(2000..=2200).contains(&input.window_start.year())
        || !(2000..=2200).contains(&input.window_end.year())
        || days <= 0
        || input.window_end > input.observed_at.date_naive()
        || input.observed_at > input.as_of
    {
        return Err(OptimizerError::InvalidInput);
    }
    if days > MAX_WINDOW_DAYS || input.rows.len() > MAX_ROWS {
        return Err(OptimizerError::LimitExceeded);
    }
    if input.rows.is_empty() {
        return Err(OptimizerError::InvalidInput);
    }
    let mut seen = BTreeSet::new();
    for row in &input.rows {
        if row.campaign_id == 0
            || i64::try_from(row.campaign_id).is_err()
            || row.date < input.window_start
            || row.date > input.window_end
        {
            return Err(OptimizerError::InvalidInput);
        }
        if !seen.insert((row.campaign_id, row.date)) {
            return Err(OptimizerError::DuplicateEvidence);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "campaign_history_tests.rs"]
mod tests;
