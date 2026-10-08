//! Durable WB advertising history, independent of report cutoffs and mail delivery.
//! Only the collector resolves marketplace credentials. MCP schedules local work.

mod normalize;
mod repository;
pub mod worker;

pub use normalize::{normalize_details, normalize_inventory, normalize_statistics};
pub use repository::HistoryRepository;

use anyhow::{Context, Result, ensure};
use chrono::{Duration, NaiveDate, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// WB daily labels use Moscow time. Exclude the open day from historical totals.
#[must_use]
pub fn last_closed_day() -> NaiveDate {
    (Utc::now() + Duration::hours(3)).date_naive() - Duration::days(1)
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistoryGroup {
    #[default]
    Campaign,
    Day,
    Sku,
}

impl HistoryGroup {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Campaign => "campaign",
            Self::Day => "day",
            Self::Sku => "sku",
        }
    }
}

pub fn validate_request(account: &str, from: Option<NaiveDate>, to: NaiveDate) -> Result<()> {
    ensure!(
        crate::identifiers::is_account_id(account),
        "invalid advertising history account"
    );
    let floor = NaiveDate::from_ymd_opt(2010, 1, 1).context("invalid archive floor")?;
    ensure!(
        to >= floor && to <= last_closed_day(),
        "only closed WB days are supported"
    );
    ensure!(
        from.is_none_or(|from| from >= floor && from <= to),
        "invalid history period"
    );
    Ok(())
}
