//! Deterministic diagnostics of a live campaign-day response. Never a write plan.

use std::collections::BTreeSet;

use chrono::{DateTime, Datelike as _, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use super::{
    MAX_INPUT_BYTES, OptimizerError,
    campaign_history::{CampaignHistoryInput, CampaignHistorySummary, analyze_campaign_history},
    reconciliation::ReconciliationCampaignRow,
};
use crate::reporting::ozon_adapter::parse_performance_daily_campaigns;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DailyAnalysisScope {
    pub store_id: String,
    pub campaign_ids: Vec<u64>,
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
    pub observed_at: DateTime<Utc>,
    /// Optional comparison threshold, not authorization to change advertising.
    pub target_drr_bps: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DailyAnalysisSignalKind {
    NoRows,
    MissingDates,
    NoObservedSpend,
    SpendWithoutReportedOrders,
    RevenueUnavailable,
    AboveTargetDrr,
    WithinTargetDrr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DailyAnalysisSignal {
    pub campaign_id: u64,
    pub kind: DailyAnalysisSignalKind,
}

#[derive(Debug, Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent evidence and execution guarantees are explicit in the serialized diagnostic contract"
)]
pub struct DailyAnalysisReport {
    pub version: u32,
    pub mode: &'static str,
    pub currency: &'static str,
    pub scope: DailyAnalysisScope,
    pub source_sha256: String,
    pub missing_campaign_ids: Vec<u64>,
    /// Only observed campaigns; wholly missing IDs remain explicitly unknown.
    pub campaigns: Vec<CampaignHistorySummary>,
    pub source_coverage_complete: bool,
    pub direct_sku_attribution_verified: bool,
    pub signals: Vec<DailyAnalysisSignal>,
    pub attribution_maturity_verified: bool,
    pub auto_apply_allowed: bool,
}

/// Replay envelope for an archived, unchanged daily response.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DailyAnalysisEvidence {
    scope: DailyAnalysisScope,
    response: Value,
}

pub fn analyze_daily_export(bytes: &[u8]) -> Result<DailyAnalysisReport, OptimizerError> {
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(OptimizerError::LimitExceeded);
    }
    let input: DailyAnalysisEvidence =
        serde_json::from_slice(bytes).map_err(|_| OptimizerError::InvalidInput)?;
    analyze_daily_response(input.scope, &input.response)
}

/// Upstream omits rows on some days. Neither transport success nor a low DRR
/// proves calendar completeness, mature attribution, profitability or incrementality.
pub fn analyze_daily_response(
    mut scope: DailyAnalysisScope,
    response: &Value,
) -> Result<DailyAnalysisReport, OptimizerError> {
    validate_scope(&scope)?;
    scope.campaign_ids.sort_unstable();
    let bytes = serde_json::to_vec(response).map_err(|_| OptimizerError::InvalidInput)?;
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(OptimizerError::LimitExceeded);
    }
    let source_sha256 =
        Sha256::digest(bytes)
            .iter()
            .fold(String::with_capacity(64), |mut result, byte| {
                use std::fmt::Write as _;
                write!(result, "{byte:02x}").expect("writing to String cannot fail");
                result
            });
    let facts =
        parse_performance_daily_campaigns(response).map_err(|_| OptimizerError::InvalidInput)?;
    if facts.iter().any(|row| {
        !scope.campaign_ids.contains(&row.campaign_id)
            || row.business_date < scope.date_from
            || row.business_date > scope.date_to
    }) {
        return Err(OptimizerError::InvalidInput);
    }
    let present: BTreeSet<_> = facts.iter().map(|row| row.campaign_id).collect();
    let missing_campaign_ids: Vec<_> = scope
        .campaign_ids
        .iter()
        .copied()
        .filter(|id| !present.contains(id))
        .collect();
    let history = if facts.is_empty() {
        None
    } else {
        Some(analyze_campaign_history(CampaignHistoryInput {
            version: 1,
            // The public scope names this legacy selector store_id explicitly.
            account_id: scope.store_id.clone(),
            source_ref: format!("ozon-performance-daily:sha256:{source_sha256}"),
            observed_at: scope.observed_at,
            as_of: scope.observed_at,
            window_start: scope.date_from,
            window_end: scope.date_to,
            source_coverage_complete: false,
            rows: facts
                .into_iter()
                .map(|row| ReconciliationCampaignRow {
                    date: row.business_date,
                    campaign_id: row.campaign_id,
                    clicks: row.clicks,
                    spend_minor: row.spend_minor,
                    orders: row.attributed_orders,
                    revenue_minor: row.attributed_revenue_minor,
                })
                .collect(),
        })?)
    };
    let mut signals: Vec<_> = missing_campaign_ids
        .iter()
        .map(|&campaign_id| DailyAnalysisSignal {
            campaign_id,
            kind: DailyAnalysisSignalKind::NoRows,
        })
        .collect();
    if let Some(history) = &history {
        for row in &history.campaigns {
            let mut push = |kind| {
                signals.push(DailyAnalysisSignal {
                    campaign_id: row.campaign_id,
                    kind,
                });
            };
            if !row.calendar_complete {
                push(DailyAnalysisSignalKind::MissingDates);
            }
            if row.spend_minor == 0 {
                push(DailyAnalysisSignalKind::NoObservedSpend);
            } else if row.orders == 0 {
                push(DailyAnalysisSignalKind::SpendWithoutReportedOrders);
            } else if row.revenue_minor == 0 {
                push(DailyAnalysisSignalKind::RevenueUnavailable);
            } else if let Some(target) = scope.target_drr_bps {
                // Compare exact amounts; a floored displayed DRR can hide a breach.
                push(
                    if u128::from(row.spend_minor) * 10_000
                        > u128::from(target) * u128::from(row.revenue_minor)
                    {
                        DailyAnalysisSignalKind::AboveTargetDrr
                    } else {
                        DailyAnalysisSignalKind::WithinTargetDrr
                    },
                );
            }
        }
    }
    signals.sort_by_key(|signal| signal.campaign_id);
    Ok(DailyAnalysisReport {
        version: 1,
        mode: "diagnostic_only",
        currency: "RUB",
        scope,
        source_sha256,
        missing_campaign_ids,
        campaigns: history.map_or_else(Vec::new, |report| report.campaigns),
        source_coverage_complete: false,
        direct_sku_attribution_verified: false,
        signals,
        attribution_maturity_verified: false,
        auto_apply_allowed: false,
    })
}

pub fn validate_scope(scope: &DailyAnalysisScope) -> Result<(), OptimizerError> {
    let ids: BTreeSet<_> = scope.campaign_ids.iter().copied().collect();
    if scope.store_id.is_empty()
        || scope.store_id.len() > 128
        || !scope
            .store_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
        || scope.campaign_ids.is_empty()
        || scope.campaign_ids.len() > 10
        || ids.len() != scope.campaign_ids.len()
        || ids.contains(&0)
        || ids.last().is_some_and(|id| *id > i64::MAX as u64)
        || !(2000..=2200).contains(&scope.observed_at.year())
        || !(2000..=2200).contains(&scope.date_from.year())
        || !(2000..=2200).contains(&scope.date_to.year())
        || !(0..31).contains(&(scope.date_to - scope.date_from).num_days())
        || scope.date_to > scope.observed_at.date_naive()
        || scope
            .target_drr_bps
            .is_some_and(|target| target == 0 || target > 10_000)
    {
        return Err(OptimizerError::InvalidInput);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
