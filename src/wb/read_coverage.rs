//! Bounded customer, content and FBW supply reads. Credentials stay in `WbClient`.
use super::{WbClient, WbError, coverage_policy as ep};
use chrono::{DateTime, NaiveDate};
use reqwest::Method;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[must_use]
pub const fn default_limit() -> u32 {
    100
}
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DateOrder {
    DateAsc,
    #[default]
    DateDesc,
}
impl DateOrder {
    const fn as_str(self) -> &'static str {
        match self {
            Self::DateAsc => "dateAsc",
            Self::DateDesc => "dateDesc",
        }
    }
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FeedbackQuery {
    pub is_answered: bool,
    #[serde(default)]
    #[schemars(range(min = 1, max = 9_223_372_036_854_775_807_u64))]
    pub nm_id: Option<u64>,
    #[serde(default = "default_limit")]
    #[schemars(range(min = 1, max = 100))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 199_990))]
    pub offset: u32,
    #[serde(default)]
    pub order: DateOrder,
    #[serde(default)]
    #[schemars(range(min = 0))]
    pub date_from: Option<i64>,
    #[serde(default)]
    #[schemars(range(min = 0))]
    pub date_to: Option<i64>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArchiveQuery {
    #[serde(default)]
    pub nm_id: Option<u64>,
    #[serde(default = "default_limit")]
    #[schemars(range(min = 1, max = 100))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 199_990))]
    pub offset: u32,
    #[serde(default)]
    pub order: DateOrder,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClaimQuery {
    pub is_archive: bool,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub nm_id: Option<u64>,
    #[serde(default = "default_limit")]
    #[schemars(range(min = 1, max = 200))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 1_000_000))]
    pub offset: u32,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CardErrorsQuery {
    #[serde(default = "default_limit")]
    #[schemars(range(min = 1, max = 100))]
    pub limit: u32,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub batch_uuid: Option<String>,
    #[serde(default)]
    pub ascending: bool,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TrashQuery {
    #[serde(default = "default_limit")]
    #[schemars(range(min = 1, max = 100))]
    pub limit: u32,
    #[serde(default)]
    pub trashed_at: Option<String>,
    #[serde(default)]
    pub nm_id: Option<u64>,
    #[serde(default)]
    pub ascending: bool,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum SupplyDateType {
    CreateDate,
    SupplyDate,
    FactDate,
    UpdatedDate,
}
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SupplyDates {
    pub from: String,
    pub till: String,
    #[serde(rename = "type")]
    pub kind: SupplyDateType,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuppliesQuery {
    #[serde(default)]
    #[schemars(length(max = 4))]
    pub dates: Vec<SupplyDates>,
    #[serde(default)]
    #[schemars(length(max = 6), inner(range(min = 1, max = 6)))]
    pub status_ids: Vec<u8>,
    #[serde(default = "default_limit")]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 1_000_000))]
    pub offset: u32,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SupplyIdQuery {
    #[schemars(range(min = 1, max = 9_223_372_036_854_775_807_u64))]
    pub id: u64,
    #[serde(default)]
    pub is_preorder_id: bool,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SupplyGoodsQuery {
    pub id: u64,
    #[serde(default)]
    pub is_preorder_id: bool,
    #[serde(default = "default_limit")]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 1_000_000))]
    pub offset: u32,
}
const fn invalid(field: &'static str) -> WbError {
    WbError::InvalidArguments { field }
}
const fn id(value: u64) -> Result<(), WbError> {
    if value == 0 || value > i64::MAX as u64 {
        Err(invalid("id"))
    } else {
        Ok(())
    }
}
const fn page(limit: u32, offset: u32, max_limit: u32, max_offset: u32) -> Result<(), WbError> {
    if limit == 0 || limit > max_limit || offset > max_offset {
        Err(invalid("pagination"))
    } else {
        Ok(())
    }
}
fn opaque(value: &str) -> Result<(), WbError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        Err(invalid("id"))
    } else {
        Ok(())
    }
}
fn cursor_pair(date: Option<&str>, present: bool) -> Result<(), WbError> {
    if date.is_some() != present
        || date.is_some_and(|s| s.len() > 64 || DateTime::parse_from_rfc3339(s).is_err())
    {
        Err(invalid("cursor"))
    } else {
        Ok(())
    }
}
fn uuid(s: &str) -> Result<(), WbError> {
    if s.len() != 36
        || s.bytes().enumerate().any(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b != b'-'
            } else {
                !b.is_ascii_hexdigit()
            }
        })
    {
        Err(invalid("uuid"))
    } else {
        Ok(())
    }
}
fn feedback_params(
    q: &FeedbackQuery,
    questions: bool,
) -> Result<Vec<(&'static str, String)>, WbError> {
    page(
        q.limit,
        q.offset,
        100,
        if questions { 10000 } else { 199_990 },
    )?;
    if questions && u64::from(q.limit) + u64::from(q.offset) > 10000 {
        return Err(invalid("pagination"));
    }
    if q.date_from.is_some_and(|d| d < 0)
        || q.date_to.is_some_and(|d| d < 0)
        || matches!((q.date_from,q.date_to),(Some(a),Some(b)) if a>b)
    {
        return Err(invalid("date_range"));
    }
    let mut p = vec![
        ("isAnswered", q.is_answered.to_string()),
        ("take", q.limit.to_string()),
        ("skip", q.offset.to_string()),
        ("order", q.order.as_str().into()),
    ];
    if let Some(n) = q.nm_id {
        id(n)?;
        p.push(("nmId", n.to_string()));
    }
    if let Some(d) = q.date_from {
        p.push(("dateFrom", d.to_string()));
    }
    if let Some(d) = q.date_to {
        p.push(("dateTo", d.to_string()));
    }
    Ok(p)
}
impl WbClient {
    pub async fn reviews(&self, account: &str, q: &FeedbackQuery) -> Result<Value, WbError> {
        self.request(
            account,
            Method::GET,
            ep::REVIEWS,
            Some(feedback_params(q, false)?),
            None,
        )
        .await
    }
    pub async fn questions(&self, account: &str, q: &FeedbackQuery) -> Result<Value, WbError> {
        self.request(
            account,
            Method::GET,
            ep::QUESTIONS,
            Some(feedback_params(q, true)?),
            None,
        )
        .await
    }
    pub async fn review(&self, account: &str, review_id: &str) -> Result<Value, WbError> {
        opaque(review_id)?;
        self.request(
            account,
            Method::GET,
            ep::REVIEW,
            Some(vec![("id", review_id.into())]),
            None,
        )
        .await
    }
    pub async fn question(&self, account: &str, question_id: &str) -> Result<Value, WbError> {
        opaque(question_id)?;
        self.request(
            account,
            Method::GET,
            ep::QUESTION,
            Some(vec![("id", question_id.into())]),
            None,
        )
        .await
    }
    pub async fn archived_reviews(
        &self,
        account: &str,
        q: &ArchiveQuery,
    ) -> Result<Value, WbError> {
        page(q.limit, q.offset, 100, 199_990)?;
        let mut p = vec![
            ("take", q.limit.to_string()),
            ("skip", q.offset.to_string()),
            ("order", q.order.as_str().into()),
        ];
        if let Some(n) = q.nm_id {
            id(n)?;
            p.push(("nmId", n.to_string()));
        }
        self.request(account, Method::GET, ep::REVIEWS_ARCHIVE, Some(p), None)
            .await
    }
    pub async fn return_claims(&self, account: &str, q: &ClaimQuery) -> Result<Value, WbError> {
        page(q.limit, q.offset, 200, 1_000_000)?;
        let mut p = vec![
            ("is_archive", q.is_archive.to_string()),
            ("limit", q.limit.to_string()),
            ("offset", q.offset.to_string()),
        ];
        if let Some(n) = q.nm_id {
            id(n)?;
            p.push(("nm_id", n.to_string()));
        }
        if let Some(s) = &q.id {
            uuid(s)?;
            p.push(("id", s.clone()));
        }
        self.request(account, Method::GET, ep::CLAIMS, Some(p), None)
            .await
    }
    pub async fn card_errors(&self, account: &str, q: &CardErrorsQuery) -> Result<Value, WbError> {
        page(q.limit, 0, 100, 0)?;
        cursor_pair(q.updated_at.as_deref(), q.batch_uuid.is_some())?;
        let mut c = json!({"limit":q.limit});
        if let (Some(date), Some(batch)) = (&q.updated_at, &q.batch_uuid) {
            uuid(batch)?;
            c["updatedAt"] = json!(date);
            c["batchUUID"] = json!(batch);
        }
        self.request(
            account,
            Method::POST,
            ep::CARD_ERRORS,
            None,
            Some(json!({"cursor":c,"order":{"ascending":q.ascending}})),
        )
        .await
    }
    pub async fn card_limits(&self, account: &str) -> Result<Value, WbError> {
        self.request(account, Method::GET, ep::CARD_LIMITS, None, None)
            .await
    }
    pub async fn subject_characteristics(
        &self,
        account: &str,
        subject_id: u64,
    ) -> Result<Value, WbError> {
        id(subject_id)?;
        self.request(
            account,
            Method::GET,
            &format!("/content/v2/object/charcs/{subject_id}"),
            None,
            None,
        )
        .await
    }
    pub async fn cards_trash(&self, account: &str, q: &TrashQuery) -> Result<Value, WbError> {
        page(q.limit, 0, 100, 0)?;
        cursor_pair(q.trashed_at.as_deref(), q.nm_id.is_some())?;
        let mut c = json!({"limit":q.limit});
        if let (Some(date), Some(n)) = (&q.trashed_at, q.nm_id) {
            id(n)?;
            c["trashedAt"] = json!(date);
            c["nmID"] = json!(n);
        }
        self.request(
            account,
            Method::POST,
            ep::CARDS_TRASH,
            None,
            Some(json!({"settings":{"cursor":c,"sort":{"ascending":q.ascending}}})),
        )
        .await
    }
    pub async fn supplies(&self, account: &str, q: &SuppliesQuery) -> Result<Value, WbError> {
        page(q.limit, q.offset, 1000, 1_000_000)?;
        if q.dates.len() > 4
            || q.status_ids.len() > 6
            || q.status_ids.iter().any(|s| !(1..=6).contains(s))
        {
            return Err(invalid("filters"));
        }
        for d in &q.dates {
            if d.from.len() != 10 || d.till.len() != 10 {
                return Err(invalid("dates"));
            }
            let a = NaiveDate::parse_from_str(&d.from, "%Y-%m-%d").map_err(|_| invalid("dates"))?;
            let b = NaiveDate::parse_from_str(&d.till, "%Y-%m-%d").map_err(|_| invalid("dates"))?;
            if a > b || (b - a).num_days() > 365 {
                return Err(invalid("dates"));
            }
        }
        self.request(
            account,
            Method::POST,
            ep::SUPPLIES,
            Some(vec![
                ("limit", q.limit.to_string()),
                ("offset", q.offset.to_string()),
            ]),
            Some(json!({"dates":q.dates,"statusIDs":q.status_ids})),
        )
        .await
    }
    pub async fn supply(&self, account: &str, q: &SupplyIdQuery) -> Result<Value, WbError> {
        id(q.id)?;
        self.request(
            account,
            Method::GET,
            &format!("/api/v1/supplies/{}", q.id),
            Some(vec![("isPreorderID", q.is_preorder_id.to_string())]),
            None,
        )
        .await
    }
    pub async fn supply_goods(
        &self,
        account: &str,
        q: &SupplyGoodsQuery,
    ) -> Result<Value, WbError> {
        id(q.id)?;
        page(q.limit, q.offset, 1000, 1_000_000)?;
        self.request(
            account,
            Method::GET,
            &format!("/api/v1/supplies/{}/goods", q.id),
            Some(vec![
                ("isPreorderID", q.is_preorder_id.to_string()),
                ("limit", q.limit.to_string()),
                ("offset", q.offset.to_string()),
            ]),
            None,
        )
        .await
    }
    pub async fn supply_packages(&self, account: &str, supply_id: u64) -> Result<Value, WbError> {
        id(supply_id)?;
        self.request(
            account,
            Method::GET,
            &format!("/api/v1/supplies/{supply_id}/package"),
            None,
            None,
        )
        .await
    }
}
