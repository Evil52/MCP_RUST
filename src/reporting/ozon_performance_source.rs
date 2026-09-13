//! Bounded Ozon Performance source for daily report advertising facts.
//!
//! The product-level statistics endpoint rejects an empty `campaignIds`
//! array. This source first enumerates campaigns through the fixed read-only
//! campaigns endpoint, then requests statistics in vendor-sized chunks. The
//! complete result remains in memory until the caller atomically publishes the
//! full report snapshot set.

use std::{collections::BTreeSet, future::Future, pin::Pin, sync::Arc};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    config::StoreId,
    ozon_performance::{
        CampaignsQuery, PerformanceClient, PerformanceErrorKind, SkuStatisticsQuery,
        StatisticsQuery,
    },
};

use super::{
    checkpoint::{CheckpointError, Checkpoints, checkpointed},
    ozon_adapter::{
        OzonReportParseError, parse_performance_expenses, parse_performance_sku_advertising,
    },
    postgres_collector::{CollectedAdvertisingExpenseFact, CollectedAdvertisingFact},
};

const CAMPAIGN_PAGE_SIZE: u32 = 100;
const MAX_CAMPAIGN_PAGES: u32 = 100;
const CAMPAIGNS_PER_STATISTICS_REQUEST: usize = 10;
const MAX_ADVERTISING_FACTS: usize = 25_000;

pub trait OzonPerformanceReportTransport: Send + Sync {
    fn campaigns(
        &self,
        page: u32,
        page_size: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonPerformanceReportSourceError>> + Send + '_>>;

    fn sku_statistics(
        &self,
        campaign_ids: Vec<u64>,
        date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonPerformanceReportSourceError>> + Send + '_>>;

    fn expenses(
        &self,
        _campaign_ids: Vec<u64>,
        _date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonPerformanceReportSourceError>> + Send + '_>>
    {
        Box::pin(async { Err(OzonPerformanceReportSourceError::InvalidResponse) })
    }
}

#[derive(Clone)]
pub struct PerformanceClientReportTransport {
    client: PerformanceClient,
    store: StoreId,
}

impl PerformanceClientReportTransport {
    #[must_use]
    pub const fn new(client: PerformanceClient, store: StoreId) -> Self {
        Self { client, store }
    }
}

impl OzonPerformanceReportTransport for PerformanceClientReportTransport {
    fn campaigns(
        &self,
        page: u32,
        page_size: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonPerformanceReportSourceError>> + Send + '_>>
    {
        Box::pin(async move {
            self.client
                .campaigns(
                    &self.store,
                    CampaignsQuery {
                        campaign_ids: Vec::new(),
                        // The downstream endpoint is explicitly scoped to
                        // product/SKU statistics. Enumerating banner, video,
                        // and other campaign kinds only burns quota and can
                        // never yield a valid row for this report.
                        adv_object_type: Some("SKU"),
                        state: None,
                        page,
                        page_size,
                    },
                )
                .await
                .map_err(|error| performance_source_failure(&error))
        })
    }

    fn sku_statistics(
        &self,
        campaign_ids: Vec<u64>,
        date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonPerformanceReportSourceError>> + Send + '_>>
    {
        Box::pin(async move {
            let date = date.format("%Y-%m-%d").to_string();
            self.client
                .sku_statistics(
                    &self.store,
                    SkuStatisticsQuery {
                        campaign_ids,
                        date_from: date.clone(),
                        date_to: date,
                    },
                )
                .await
                .map_err(|error| performance_source_failure(&error))
        })
    }

    fn expenses(
        &self,
        campaign_ids: Vec<u64>,
        date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonPerformanceReportSourceError>> + Send + '_>>
    {
        Box::pin(async move {
            let date = date.format("%Y-%m-%d").to_string();
            self.client
                .expenses(
                    &self.store,
                    StatisticsQuery {
                        campaign_ids,
                        date_from: date.clone(),
                        date_to: date,
                    },
                )
                .await
                .map_err(|error| performance_source_failure(&error))
        })
    }
}

fn performance_source_failure(
    error: &crate::ozon_performance::PerformanceError,
) -> OzonPerformanceReportSourceError {
    match error {
        crate::ozon_performance::PerformanceError::RateLimited {
            retry_after: Some(delay),
            ..
        }
        | crate::ozon_performance::PerformanceError::SharedQuota(
            crate::marketplace_quota::QuotaError::Limited { retry_after: delay },
        ) => OzonPerformanceReportSourceError::RetryAfter {
            seconds: super::checkpoint::delay_seconds(*delay),
        },
        _ => OzonPerformanceReportSourceError::Upstream(error.kind()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OzonPerformanceCollectedFacts {
    pub advertising: Vec<CollectedAdvertisingFact>,
    pub expenses: Vec<CollectedAdvertisingExpenseFact>,
}

pub struct OzonPerformanceReportSource {
    checkpoints: Checkpoints,
    transport: Arc<dyn OzonPerformanceReportTransport>,
}

impl OzonPerformanceReportSource {
    pub fn new(transport: impl OzonPerformanceReportTransport + 'static) -> Self {
        Self {
            transport: Arc::new(transport),
            checkpoints: None,
        }
    }

    #[must_use]
    pub fn with_checkpoints(mut self, checkpoints: Checkpoints) -> Self {
        self.checkpoints = checkpoints;
        self
    }

    pub async fn collect(
        &self,
        date: NaiveDate,
    ) -> Result<Vec<CollectedAdvertisingFact>, OzonPerformanceReportSourceError> {
        let campaign_ids = self.collect_campaign_ids(date).await?;
        let mut facts = Vec::new();
        let mut fact_keys = BTreeSet::new();

        for chunk in campaign_ids.chunks(CAMPAIGNS_PER_STATISTICS_REQUEST) {
            let rows = checkpointed(
                &self.checkpoints,
                json!(["ozon_performance_sku", date, chunk]),
                || async {
                    parse_performance_sku_advertising(
                        &self.transport.sku_statistics(chunk.to_vec(), date).await?,
                    )
                    .map_err(OzonPerformanceReportSourceError::from)
                },
            )
            .await?;
            if facts.len().saturating_add(rows.len()) > MAX_ADVERTISING_FACTS {
                return Err(OzonPerformanceReportSourceError::TooManyFacts);
            }
            for row in rows {
                if row.business_date != date || !chunk.contains(&row.campaign_id) {
                    return Err(OzonPerformanceReportSourceError::InvalidResponse);
                }
                if !fact_keys.insert((row.business_date, row.campaign_id, row.sku)) {
                    return Err(OzonPerformanceReportSourceError::InvalidResponse);
                }
                facts.push(row);
            }
        }
        Ok(facts)
    }

    pub async fn collect_extended(
        &self,
        date: NaiveDate,
    ) -> Result<OzonPerformanceCollectedFacts, OzonPerformanceReportSourceError> {
        let campaign_ids = self.collect_campaign_ids(date).await?;
        let mut advertising = Vec::new();
        let mut expenses = Vec::new();
        let mut advertising_keys = BTreeSet::new();
        let mut expense_keys = BTreeSet::new();
        for chunk in campaign_ids.chunks(CAMPAIGNS_PER_STATISTICS_REQUEST) {
            let rows = checkpointed(
                &self.checkpoints,
                json!(["ozon_performance_sku", date, chunk]),
                || async {
                    let response = self.transport.sku_statistics(chunk.to_vec(), date).await?;
                    parse_performance_sku_advertising(&response).map_err(|error| {
                        tracing::warn!(
                            source = "sku_statistics",
                            parse_error = ?error,
                            "Ozon Performance report response was rejected"
                        );
                        OzonPerformanceReportSourceError::from(error)
                    })
                },
            )
            .await?;
            for row in rows {
                if !valid_advertising_row(&row, date, chunk, &mut advertising_keys) {
                    return Err(OzonPerformanceReportSourceError::InvalidResponse);
                }
                advertising.push(row);
            }
            let rows = checkpointed(
                &self.checkpoints,
                json!(["ozon_performance_expenses", date, chunk]),
                || async {
                    let response = self.transport.expenses(chunk.to_vec(), date).await?;
                    parse_performance_expenses(&response).map_err(|error| {
                        tracing::warn!(
                            source = "expenses",
                            parse_error = ?error,
                            "Ozon Performance report response was rejected"
                        );
                        OzonPerformanceReportSourceError::from(error)
                    })
                },
            )
            .await?;
            for row in rows {
                if !valid_expense_row(&row, date, chunk, &mut expense_keys) {
                    return Err(OzonPerformanceReportSourceError::InvalidResponse);
                }
                expenses.push(row);
            }
            if advertising.len() > MAX_ADVERTISING_FACTS || expenses.len() > MAX_ADVERTISING_FACTS {
                return Err(OzonPerformanceReportSourceError::TooManyFacts);
            }
        }
        Ok(OzonPerformanceCollectedFacts {
            advertising,
            expenses,
        })
    }

    async fn collect_campaign_ids(
        &self,
        date: NaiveDate,
    ) -> Result<Vec<u64>, OzonPerformanceReportSourceError> {
        let mut expected_total = None;
        let mut seen_campaign_ids = BTreeSet::new();
        let mut eligible_campaign_ids = BTreeSet::new();

        // `total` is capped at `PAGE_SIZE * MAX_PAGES`, every non-final page
        // must be full, and campaign IDs must be unique. Those invariants make
        // this loop terminate within `MAX_CAMPAIGN_PAGES` without a separate
        // unreachable exhaustion branch.
        let mut page = 1;
        loop {
            let (campaigns, total) = checkpointed(
                &self.checkpoints,
                json!(["ozon_performance_campaigns", date, page]),
                || async {
                    parse_campaign_page(&self.transport.campaigns(page, CAMPAIGN_PAGE_SIZE).await?)
                },
            )
            .await?;
            if *expected_total.get_or_insert(total) != total {
                return Err(OzonPerformanceReportSourceError::InconsistentPagination);
            }
            absorb_campaign_page(
                &campaigns,
                date,
                &mut seen_campaign_ids,
                &mut eligible_campaign_ids,
            )?;
            if seen_campaign_ids.len() == total {
                let report_date_campaigns = eligible_campaign_ids.len();
                tracing::info!(
                    sku_campaigns = total,
                    report_date_campaigns,
                    "Ozon Performance campaign inventory was bounded to the report date"
                );
                return Ok(eligible_campaign_ids.into_iter().collect());
            }
            if seen_campaign_ids.len() > total || campaigns.len() < CAMPAIGN_PAGE_SIZE as usize {
                return Err(OzonPerformanceReportSourceError::InconsistentPagination);
            }
            page += 1;
        }
    }
}

fn valid_advertising_row(
    row: &CollectedAdvertisingFact,
    date: NaiveDate,
    campaign_ids: &[u64],
    keys: &mut BTreeSet<(NaiveDate, u64, u64)>,
) -> bool {
    row.business_date == date
        && campaign_ids.contains(&row.campaign_id)
        && keys.insert((row.business_date, row.campaign_id, row.sku))
}

fn valid_expense_row(
    row: &CollectedAdvertisingExpenseFact,
    date: NaiveDate,
    campaign_ids: &[u64],
    keys: &mut BTreeSet<(NaiveDate, u64)>,
) -> bool {
    row.business_date == date
        && campaign_ids.contains(&row.campaign_id)
        && keys.insert((row.business_date, row.campaign_id))
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonPerformanceReportSourceError {
    #[error("collection quota requires a pause")]
    RetryAfter { seconds: u64 },
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error("Ozon Performance request failed")]
    Upstream(PerformanceErrorKind),
    #[error("Ozon Performance response has an invalid shape or provenance")]
    InvalidResponse,
    #[error("Ozon Performance campaign pagination changed during collection")]
    InconsistentPagination,
    #[error("Ozon Performance campaign pagination exceeded its bound")]
    PaginationLimit,
    #[error("Ozon Performance advertising facts exceeded their bound")]
    TooManyFacts,
}

impl OzonPerformanceReportSourceError {
    #[must_use]
    pub const fn failure(&self) -> super::source_collection::SourceFailure {
        super::source_collection::SourceFailure {
            code: self.code(),
            retry_after: match self {
                Self::RetryAfter { seconds } => Some(*seconds),
                _ => None,
            },
        }
    }
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::RetryAfter { .. } => "rate_limited",
            Self::Checkpoint(error) => error.code(),
            Self::Upstream(kind) => kind.code(),
            Self::InvalidResponse => "invalid_response",
            Self::InconsistentPagination => "inconsistent_pagination",
            Self::PaginationLimit => "pagination_limit",
            Self::TooManyFacts => "too_many_facts",
        }
    }
}

impl From<OzonReportParseError> for OzonPerformanceReportSourceError {
    fn from(_: OzonReportParseError) -> Self {
        Self::InvalidResponse
    }
}

/// Folds one campaign page into the running inventory.
///
/// Extracted from `collect_campaign_ids` so neither the pagination loop nor
/// the per-campaign filtering has to be read through the other.
fn absorb_campaign_page(
    campaigns: &[CampaignWindow],
    date: NaiveDate,
    seen_campaign_ids: &mut BTreeSet<u64>,
    eligible_campaign_ids: &mut BTreeSet<u64>,
) -> Result<(), OzonPerformanceReportSourceError> {
    for campaign in campaigns {
        if !seen_campaign_ids.insert(campaign.id) {
            return Err(OzonPerformanceReportSourceError::InconsistentPagination);
        }
        if campaign.from_date.is_none_or(|from| from <= date)
            && campaign.to_date.is_none_or(|to| date <= to)
        {
            eligible_campaign_ids.insert(campaign.id);
        }
    }
    Ok(())
}

fn parse_campaign_page(
    response: &Value,
) -> Result<(Vec<CampaignWindow>, usize), OzonPerformanceReportSourceError> {
    let object = response
        .as_object()
        .ok_or(OzonPerformanceReportSourceError::InvalidResponse)?;
    let list = object
        .get("list")
        .and_then(Value::as_array)
        .ok_or(OzonPerformanceReportSourceError::InvalidResponse)?;
    if list.len() > CAMPAIGN_PAGE_SIZE as usize {
        return Err(OzonPerformanceReportSourceError::InvalidResponse);
    }
    let total = parse_positive_or_zero_usize(
        object
            .get("total")
            .ok_or(OzonPerformanceReportSourceError::InvalidResponse)?,
    )?;
    if total > CAMPAIGN_PAGE_SIZE as usize * MAX_CAMPAIGN_PAGES as usize {
        return Err(OzonPerformanceReportSourceError::PaginationLimit);
    }
    let mut campaigns = Vec::with_capacity(list.len());
    for campaign in list {
        let campaign = campaign
            .as_object()
            .ok_or(OzonPerformanceReportSourceError::InvalidResponse)?;
        let id = campaign
            .get("id")
            .and_then(parse_positive_u64)
            .ok_or(OzonPerformanceReportSourceError::InvalidResponse)?;
        let from_date = parse_optional_campaign_date(campaign.get("fromDate"))?;
        let to_date = parse_optional_campaign_date(campaign.get("toDate"))?;
        if from_date.zip(to_date).is_some_and(|(from, to)| from > to) {
            return Err(OzonPerformanceReportSourceError::InvalidResponse);
        }
        campaigns.push(CampaignWindow {
            id,
            from_date,
            to_date,
        });
    }
    Ok((campaigns, total))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct CampaignWindow {
    id: u64,
    from_date: Option<NaiveDate>,
    to_date: Option<NaiveDate>,
}

fn parse_campaign_date(value: &str) -> Option<NaiveDate> {
    let prefix = value.get(..10)?;
    NaiveDate::parse_from_str(prefix, "%Y-%m-%d").ok()
}

fn parse_optional_campaign_date(
    value: Option<&Value>,
) -> Result<Option<NaiveDate>, OzonPerformanceReportSourceError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(Value::String(value)) => parse_campaign_date(value)
            .map(Some)
            .ok_or(OzonPerformanceReportSourceError::InvalidResponse),
        Some(_) => Err(OzonPerformanceReportSourceError::InvalidResponse),
    }
}

fn parse_positive_or_zero_usize(value: &Value) -> Result<usize, OzonPerformanceReportSourceError> {
    let value = match value {
        Value::Number(value) => value.as_u64(),
        Value::String(value) => value.parse::<u64>().ok(),
        _ => None,
    }
    .ok_or(OzonPerformanceReportSourceError::InvalidResponse)?;
    usize::try_from(value).map_err(|_| OzonPerformanceReportSourceError::InvalidResponse)
}

fn parse_positive_u64(value: &Value) -> Option<u64> {
    let value = match value {
        Value::Number(value) => value.as_u64(),
        Value::String(value) if !value.starts_with('0') => value.parse::<u64>().ok(),
        _ => None,
    }?;
    (value != 0).then_some(value)
}

#[cfg(test)]
mod tests;
