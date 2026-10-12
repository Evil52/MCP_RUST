//! Explicit contracts checked before credentials, pacing or network access.
use super::{WbError, validate_positive_unique_ids};
use chrono::{DateTime, Months, NaiveDate, Utc};
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OrderFeedQuery {
    #[schemars(
        length(min = 20, max = 40),
        description = "RFC3339 с часовым поясом; последние 31 день, по времени текущего статуса заказа"
    )]
    pub date_from: String,
    #[schemars(length(min = 20, max = 40))]
    pub date_to: String,
    #[serde(default)]
    #[schemars(length(max = 1000), inner(range(min = 1)), extend("uniqueItems" = true))]
    pub nm_ids: Vec<u64>,
    #[serde(default)]
    #[schemars(length(max = 50), inner(range(min = 1)), extend("uniqueItems" = true))]
    pub subject_ids: Vec<u64>,
    #[serde(default)]
    #[schemars(length(max = 50), inner(range(min = 1)), extend("uniqueItems" = true))]
    pub tag_ids: Vec<u64>,
    #[serde(default)]
    #[schemars(length(max = 50), inner(length(min = 1, max = 256)))]
    pub brand_names: Vec<String>,
    #[serde(default)]
    #[schemars(
        length(min = 20, max = 40),
        description = "data.snapshotTime первой страницы; обязателен при offset > 0, период и фильтры сохраняются"
    )]
    pub snapshot_time: Option<String>,
    #[serde(default)]
    #[schemars(range(max = 2_147_483_647_u32))]
    pub offset: u32,
    #[serde(default = "super::read_coverage::default_limit")]
    #[schemars(range(min = 1, max = 10000))]
    pub limit: u32,
}

impl OrderFeedQuery {
    pub(super) fn validate(&self) -> Result<(), WbError> {
        let from = timestamp(&self.date_from, "date_from")?;
        let to = timestamp(&self.date_to, "date_to")?;
        let now = Utc::now();
        if from > to
            || from < now - chrono::Duration::days(31)
            || to.date_naive() > now.date_naive()
            || to - from > chrono::Duration::days(31)
        {
            return Err(invalid("date_from/date_to (last 31 days)"));
        }
        optional_ids(&self.nm_ids, 1000, "nm_ids")?;
        optional_ids(&self.subject_ids, 50, "subject_ids")?;
        optional_ids(&self.tag_ids, 50, "tag_ids")?;
        if self.brand_names.len() > 50
            || self
                .brand_names
                .iter()
                .any(|v| v.trim().is_empty() || v.chars().count() > 256)
        {
            return Err(invalid("brand_names"));
        }
        if self.limit == 0 || self.limit > 10000 || self.offset > i32::MAX as u32 {
            return Err(invalid("limit/offset"));
        }
        if let Some(snapshot) = &self.snapshot_time {
            timestamp(snapshot, "snapshot_time")?;
        }
        if self.offset > 0 && self.snapshot_time.is_none() {
            return Err(invalid("snapshot_time required for offset > 0"));
        }
        Ok(())
    }

    pub(super) fn payload(&self) -> Value {
        let mut pagination = json!({"limit": self.limit, "offset": self.offset});
        if let Some(snapshot) = &self.snapshot_time {
            pagination["snapshotTime"] = json!(snapshot);
        }
        json!({"selectedPeriod":{"start":self.date_from,"end":self.date_to},"nmIds":self.nm_ids,"subjectIds":self.subject_ids,"tagIds":self.tag_ids,"brandNames":self.brand_names,"pagination":pagination})
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ItemReturnStatus {
    Active,
    Archive,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ItemReturnsQuery {
    #[schemars(length(equal = 10))]
    pub date_from: String,
    #[schemars(length(equal = 10))]
    pub date_to: String,
    #[serde(default)]
    pub status: Option<ItemReturnStatus>,
    #[serde(default = "super::read_coverage::default_limit")]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 1_000_000))]
    pub offset: u32,
}

impl ItemReturnsQuery {
    pub(super) fn validate(&self) -> Result<(), WbError> {
        dates(&self.date_from, &self.date_to)?;
        if self.limit == 0 || self.limit > 1000 || self.offset > 1_000_000 {
            return Err(invalid("limit/offset"));
        }
        Ok(())
    }
    pub(super) fn parameters(&self) -> Vec<(&'static str, String)> {
        let mut params = vec![
            ("dateFrom", self.date_from.clone()),
            ("dateTo", self.date_to.clone()),
            ("limit", self.limit.to_string()),
            ("offset", self.offset.to_string()),
        ];
        if let Some(status) = &self.status {
            params.push((
                "status",
                match status {
                    ItemReturnStatus::Active => "active",
                    ItemReturnStatus::Archive => "archive",
                }
                .to_owned(),
            ));
        }
        params
    }
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema)]
pub enum StockType {
    #[default]
    #[serde(rename = "")]
    All,
    #[serde(rename = "wb")]
    Wb,
    #[serde(rename = "mp")]
    Seller,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StockHistoryQuery {
    #[schemars(
        length(equal = 36),
        description = "Уникальный UUID отчёта, сохраняйте для проверки статуса и скачивания; не используйте для другого отчёта"
    )]
    pub download_id: String,
    #[schemars(
        length(equal = 10),
        description = "YYYY-MM-DD, история за последние 3 месяца; доступна без подписки Джем"
    )]
    pub date_from: String,
    #[schemars(length(equal = 10))]
    pub date_to: String,
    #[serde(default)]
    pub stock_type: StockType,
    #[serde(default)]
    pub skip_deleted_nm: bool,
    #[serde(default)]
    #[schemars(length(max = 1000), inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)), extend("uniqueItems" = true))]
    pub nm_ids: Vec<u64>,
}
impl StockHistoryQuery {
    pub(super) fn validate(&self) -> Result<(), WbError> {
        super::recent_reads::validate_download_id(&self.download_id)?;
        let (from, to) = dates(&self.date_from, &self.date_to)?;
        let today = Utc::now().date_naive();
        if from
            < today
                .checked_sub_months(Months::new(3))
                .ok_or_else(|| invalid("date_from"))?
            || to > today
        {
            return Err(invalid("date_from/date_to (last 3 months)"));
        }
        optional_ids(&self.nm_ids, 1000, "nm_ids")
    }
    pub(super) fn payload(&self) -> Value {
        json!({"id":self.download_id,"reportType":"STOCK_HISTORY_DAILY_CSV","params":{"currentPeriod":{"start":self.date_from,"end":self.date_to},"stockType":self.stock_type,"skipDeletedNm":self.skip_deleted_nm,"nmIds":self.nm_ids}})
    }
}

const fn invalid(field: &'static str) -> WbError {
    WbError::InvalidArguments { field }
}
fn timestamp(value: &str, field: &'static str) -> Result<DateTime<Utc>, WbError> {
    if !(20..=40).contains(&value.len()) {
        return Err(invalid(field));
    }
    DateTime::parse_from_rfc3339(value)
        .map(|v| v.with_timezone(&Utc))
        .map_err(|_| invalid(field))
}
fn dates(from: &str, to: &str) -> Result<(NaiveDate, NaiveDate), WbError> {
    let parse = |s: &str| {
        NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .ok()
            .filter(|v| v.format("%Y-%m-%d").to_string() == s)
    };
    parse(from)
        .zip(parse(to))
        .filter(|(from, to)| from <= to)
        .ok_or_else(|| invalid("date_from/date_to"))
}
fn optional_ids(ids: &[u64], max: usize, field: &'static str) -> Result<(), WbError> {
    if ids.is_empty() {
        Ok(())
    } else {
        validate_positive_unique_ids(ids, max, field, Some(i64::MAX as u64))
    }
}
