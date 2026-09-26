//! Offline arithmetic reconciliation of two explicitly identified Ozon exports.
//!
//! A numerical match does not establish that endpoint attribution definitions
//! are equivalent. This module never produces evidence or commands for the
//! optimizer, and never substitutes zero for an absent campaign/day row.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::{Deserialize, Serialize, Serializer};
use sha2::{Digest, Sha256};

use super::{MAX_INPUT_BYTES, MAX_WINDOW_DAYS, OptimizerError};

pub const MAX_RECONCILIATION_ROWS: usize = 25_000;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationInput {
    pub version: u32,
    pub account_id: String,
    pub as_of: DateTime<Utc>,
    pub window_start: NaiveDate,
    pub window_end: NaiveDate,
    pub sku_source: SkuReconciliationSource,
    pub campaign_source: CampaignReconciliationSource,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SkuReconciliationSource {
    pub account_id: String,
    pub source_ref: String,
    pub observed_at: DateTime<Utc>,
    pub coverage_complete: bool,
    pub rows: Vec<ReconciliationSkuRow>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignReconciliationSource {
    pub account_id: String,
    pub source_ref: String,
    pub observed_at: DateTime<Utc>,
    pub coverage_complete: bool,
    pub rows: Vec<ReconciliationCampaignRow>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationSkuRow {
    pub date: NaiveDate,
    pub campaign_id: u64,
    pub sku: u64,
    pub clicks: u64,
    pub spend_minor: u64,
    pub direct_orders: u64,
    pub direct_revenue_minor: u64,
    pub model_orders: u64,
    pub model_revenue_minor: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationCampaignRow {
    pub date: NaiveDate,
    pub campaign_id: u64,
    pub clicks: u64,
    pub spend_minor: u64,
    pub orders: u64,
    pub revenue_minor: u64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationMode {
    DiagnosticOnly,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeltaEncoding {
    SignedDecimalString,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReconciliationSource {
    pub account_id: String,
    pub source_ref: String,
    pub observed_at: DateTime<Utc>,
    pub coverage_complete: bool,
    pub row_count: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationReason {
    SkuCoverageIncomplete,
    CampaignCoverageIncomplete,
    SourceObservationsDiffer,
    SkuObservationBeforeWindowEnd,
    CampaignObservationBeforeWindowEnd,
    MissingSkuRow,
    MissingCampaignRow,
    NoComparableRows,
    ClicksDiffer,
    SpendDiffers,
    DirectOrdersDiffer,
    DirectRevenueDiffers,
    DirectPlusModelOrdersDiffer,
    DirectPlusModelRevenueDiffers,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct SkuReconciliationTotals {
    pub sku_count: u32,
    pub clicks: u64,
    pub spend_minor: u64,
    pub direct_orders: u64,
    pub direct_revenue_minor: u64,
    pub model_orders: u64,
    pub model_revenue_minor: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CampaignReconciliationTotals {
    pub clicks: u64,
    pub spend_minor: u64,
    pub orders: u64,
    pub revenue_minor: u64,
}

/// Signed campaign-day value minus the SKU direct-attribution sum.
/// Strings preserve exact i128 deltas through JSON tools with int64/float limits.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DirectReconciliationDelta {
    #[serde(serialize_with = "serialize_signed_delta")]
    pub clicks: i128,
    #[serde(serialize_with = "serialize_signed_delta")]
    pub spend_minor: i128,
    #[serde(serialize_with = "serialize_signed_delta")]
    pub orders: i128,
    #[serde(serialize_with = "serialize_signed_delta")]
    pub revenue_minor: i128,
}

/// An arithmetic comparison only. Model attribution may overlap with direct
/// attribution; the combined values must never be used as deduplicated revenue.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DirectPlusModelArithmetic {
    pub arithmetic_only: bool,
    pub combined_orders: u64,
    pub combined_revenue_minor: u64,
    #[serde(serialize_with = "serialize_signed_delta")]
    pub daily_minus_combined_orders: i128,
    #[serde(serialize_with = "serialize_signed_delta")]
    pub daily_minus_combined_revenue_minor: i128,
    pub orders_match: bool,
    pub revenue_match: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReconciliationRow {
    pub date: NaiveDate,
    pub campaign_id: u64,
    pub sku_totals: Option<SkuReconciliationTotals>,
    pub campaign_totals: Option<CampaignReconciliationTotals>,
    pub daily_minus_direct: Option<DirectReconciliationDelta>,
    pub direct_plus_model_arithmetic: Option<DirectPlusModelArithmetic>,
    pub reasons: Vec<ReconciliationReason>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReconciliationReport {
    pub version: u32,
    pub mode: ReconciliationMode,
    pub delta_encoding: DeltaEncoding,
    pub currency: String,
    pub account_id: String,
    pub as_of: DateTime<Utc>,
    pub window_start: NaiveDate,
    pub window_end: NaiveDate,
    pub input_sha256: String,
    pub sku_source: ReconciliationSource,
    pub campaign_source: ReconciliationSource,
    /// Always false: arithmetic cannot verify endpoint attribution semantics.
    pub semantic_equivalence_verified: bool,
    pub reasons: Vec<ReconciliationReason>,
    pub rows: Vec<ReconciliationRow>,
}

pub fn parse_reconciliation_input(bytes: &[u8]) -> Result<ReconciliationInput, OptimizerError> {
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(OptimizerError::LimitExceeded);
    }
    let input = serde_json::from_slice(bytes).map_err(|_| OptimizerError::InvalidInput)?;
    validate(&input)?;
    Ok(input)
}

/// Canonical row ordering makes reports and provenance digests reproducible.
/// No source is declared authoritative by an arithmetic match or difference.
pub fn reconcile(mut input: ReconciliationInput) -> Result<ReconciliationReport, OptimizerError> {
    validate(&input)?;
    input
        .sku_source
        .rows
        .sort_by_key(|row| (row.date, row.campaign_id, row.sku));
    input
        .campaign_source
        .rows
        .sort_by_key(|row| (row.date, row.campaign_id));
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
    let mut reasons = source_reasons(&input);
    let sku_totals = aggregate_sku_rows(&input.sku_source.rows)?;
    let mut campaign_totals = input
        .campaign_source
        .rows
        .iter()
        .map(|row| {
            (
                (row.date, row.campaign_id),
                CampaignReconciliationTotals {
                    clicks: row.clicks,
                    spend_minor: row.spend_minor,
                    orders: row.orders,
                    revenue_minor: row.revenue_minor,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut rows = Vec::with_capacity(sku_totals.len() + campaign_totals.len());
    for (key, sku) in sku_totals {
        rows.push(compare_row(
            key,
            Some(sku),
            campaign_totals.remove(&key),
            &reasons,
        )?);
    }
    for (key, campaign) in campaign_totals {
        rows.push(compare_row(key, None, Some(campaign), &reasons)?);
    }
    rows.sort_by_key(|row| (row.date, row.campaign_id));
    if rows.iter().all(|row| row.daily_minus_direct.is_none()) {
        reasons.push(ReconciliationReason::NoComparableRows);
    }
    Ok(ReconciliationReport {
        version: 1,
        mode: ReconciliationMode::DiagnosticOnly,
        delta_encoding: DeltaEncoding::SignedDecimalString,
        currency: "RUB".to_owned(),
        account_id: input.account_id,
        as_of: input.as_of,
        window_start: input.window_start,
        window_end: input.window_end,
        input_sha256,
        sku_source: ReconciliationSource {
            account_id: input.sku_source.account_id,
            source_ref: input.sku_source.source_ref,
            observed_at: input.sku_source.observed_at,
            coverage_complete: input.sku_source.coverage_complete,
            row_count: input.sku_source.rows.len(),
        },
        campaign_source: ReconciliationSource {
            account_id: input.campaign_source.account_id,
            source_ref: input.campaign_source.source_ref,
            observed_at: input.campaign_source.observed_at,
            coverage_complete: input.campaign_source.coverage_complete,
            row_count: input.campaign_source.rows.len(),
        },
        semantic_equivalence_verified: false,
        reasons,
        rows,
    })
}

fn validate(input: &ReconciliationInput) -> Result<(), OptimizerError> {
    let days = (input.window_end - input.window_start).num_days() + 1;
    if input.version != 1
        || !valid_account(&input.account_id)
        || !(2000..=2200).contains(&input.as_of.year())
        || !(2000..=2200).contains(&input.window_start.year())
        || !(2000..=2200).contains(&input.window_end.year())
        || days <= 0
        || input.window_end > input.as_of.date_naive()
    {
        return Err(OptimizerError::InvalidInput);
    }
    let rows = input
        .sku_source
        .rows
        .len()
        .checked_add(input.campaign_source.rows.len())
        .ok_or(OptimizerError::LimitExceeded)?;
    if days > MAX_WINDOW_DAYS || rows > MAX_RECONCILIATION_ROWS {
        return Err(OptimizerError::LimitExceeded);
    }
    for (account, reference, observed_at) in [
        (
            &input.sku_source.account_id,
            &input.sku_source.source_ref,
            input.sku_source.observed_at,
        ),
        (
            &input.campaign_source.account_id,
            &input.campaign_source.source_ref,
            input.campaign_source.observed_at,
        ),
    ] {
        if account != &input.account_id
            || reference.trim().is_empty()
            || reference.len() > 256
            || reference.chars().any(char::is_control)
            || observed_at > input.as_of
            || !(2000..=2200).contains(&observed_at.year())
        {
            return Err(OptimizerError::InvalidInput);
        }
    }
    let valid_scope = |date, campaign_id| {
        date >= input.window_start && date <= input.window_end && valid_id(campaign_id)
    };
    let mut sku_keys = BTreeSet::new();
    for row in &input.sku_source.rows {
        if !valid_scope(row.date, row.campaign_id) || !valid_id(row.sku) {
            return Err(OptimizerError::InvalidInput);
        }
        if !sku_keys.insert((row.date, row.campaign_id, row.sku)) {
            return Err(OptimizerError::DuplicateEvidence);
        }
    }
    let mut campaign_keys = BTreeSet::new();
    for row in &input.campaign_source.rows {
        if !valid_scope(row.date, row.campaign_id) {
            return Err(OptimizerError::InvalidInput);
        }
        if !campaign_keys.insert((row.date, row.campaign_id)) {
            return Err(OptimizerError::DuplicateEvidence);
        }
    }
    Ok(())
}

fn valid_account(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
}

fn valid_id(value: u64) -> bool {
    value > 0 && i64::try_from(value).is_ok()
}

fn source_reasons(input: &ReconciliationInput) -> Vec<ReconciliationReason> {
    let mut reasons = Vec::new();
    for (condition, reason) in [
        (
            !input.sku_source.coverage_complete,
            ReconciliationReason::SkuCoverageIncomplete,
        ),
        (
            !input.campaign_source.coverage_complete,
            ReconciliationReason::CampaignCoverageIncomplete,
        ),
        (
            input.sku_source.observed_at != input.campaign_source.observed_at,
            ReconciliationReason::SourceObservationsDiffer,
        ),
        (
            input.sku_source.observed_at.date_naive() < input.window_end,
            ReconciliationReason::SkuObservationBeforeWindowEnd,
        ),
        (
            input.campaign_source.observed_at.date_naive() < input.window_end,
            ReconciliationReason::CampaignObservationBeforeWindowEnd,
        ),
    ] {
        if condition {
            reasons.push(reason);
        }
    }
    reasons
}

fn aggregate_sku_rows(
    rows: &[ReconciliationSkuRow],
) -> Result<BTreeMap<(NaiveDate, u64), SkuReconciliationTotals>, OptimizerError> {
    let mut groups = BTreeMap::<_, SkuReconciliationTotals>::new();
    for row in rows {
        let group = groups.entry((row.date, row.campaign_id)).or_default();
        group.sku_count = group
            .sku_count
            .checked_add(1)
            .ok_or(OptimizerError::Overflow)?;
        for (total, value) in [
            (&mut group.clicks, row.clicks),
            (&mut group.spend_minor, row.spend_minor),
            (&mut group.direct_orders, row.direct_orders),
            (&mut group.direct_revenue_minor, row.direct_revenue_minor),
            (&mut group.model_orders, row.model_orders),
            (&mut group.model_revenue_minor, row.model_revenue_minor),
        ] {
            *total = total.checked_add(value).ok_or(OptimizerError::Overflow)?;
        }
    }
    Ok(groups)
}

fn compare_row(
    (date, campaign_id): (NaiveDate, u64),
    sku_totals: Option<SkuReconciliationTotals>,
    campaign_totals: Option<CampaignReconciliationTotals>,
    source_reasons: &[ReconciliationReason],
) -> Result<ReconciliationRow, OptimizerError> {
    let mut reasons = source_reasons.to_vec();
    let (daily_minus_direct, direct_plus_model_arithmetic) =
        if let (Some(sku), Some(campaign)) = (&sku_totals, &campaign_totals) {
            let delta = DirectReconciliationDelta {
                clicks: difference(campaign.clicks, sku.clicks),
                spend_minor: difference(campaign.spend_minor, sku.spend_minor),
                orders: difference(campaign.orders, sku.direct_orders),
                revenue_minor: difference(campaign.revenue_minor, sku.direct_revenue_minor),
            };
            let combined_orders = sku
                .direct_orders
                .checked_add(sku.model_orders)
                .ok_or(OptimizerError::Overflow)?;
            let combined_revenue_minor = sku
                .direct_revenue_minor
                .checked_add(sku.model_revenue_minor)
                .ok_or(OptimizerError::Overflow)?;
            let arithmetic = DirectPlusModelArithmetic {
                arithmetic_only: true,
                combined_orders,
                combined_revenue_minor,
                daily_minus_combined_orders: difference(campaign.orders, combined_orders),
                daily_minus_combined_revenue_minor: difference(
                    campaign.revenue_minor,
                    combined_revenue_minor,
                ),
                orders_match: campaign.orders == combined_orders,
                revenue_match: campaign.revenue_minor == combined_revenue_minor,
            };
            for (value, reason) in [
                (delta.clicks, ReconciliationReason::ClicksDiffer),
                (delta.spend_minor, ReconciliationReason::SpendDiffers),
                (delta.orders, ReconciliationReason::DirectOrdersDiffer),
                (
                    delta.revenue_minor,
                    ReconciliationReason::DirectRevenueDiffers,
                ),
                (
                    arithmetic.daily_minus_combined_orders,
                    ReconciliationReason::DirectPlusModelOrdersDiffer,
                ),
                (
                    arithmetic.daily_minus_combined_revenue_minor,
                    ReconciliationReason::DirectPlusModelRevenueDiffers,
                ),
            ] {
                if value != 0 {
                    reasons.push(reason);
                }
            }
            (Some(delta), Some(arithmetic))
        } else {
            if sku_totals.is_none() {
                reasons.push(ReconciliationReason::MissingSkuRow);
            }
            if campaign_totals.is_none() {
                reasons.push(ReconciliationReason::MissingCampaignRow);
            }
            (None, None)
        };
    Ok(ReconciliationRow {
        date,
        campaign_id,
        sku_totals,
        campaign_totals,
        daily_minus_direct,
        direct_plus_model_arithmetic,
        reasons,
    })
}

fn difference(campaign: u64, sku: u64) -> i128 {
    // Every u64 and its negation fit i128, so this subtraction cannot overflow.
    i128::from(campaign) - i128::from(sku)
}

fn serialize_signed_delta<S: Serializer>(value: &i128, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}

#[cfg(test)]
#[path = "reconciliation_tests.rs"]
mod tests;
