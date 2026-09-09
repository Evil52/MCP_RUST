//! Ozon advertising and customer-feedback input contracts.

use super::{Deserialize, JsonSchema, SortDirection, StoreId, default_page};

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PerformanceAdvObjectType {
    Sku,
    Banner,
    SearchPromo,
    VideoBanner,
}

impl PerformanceAdvObjectType {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Sku => "SKU",
            Self::Banner => "BANNER",
            Self::SearchPromo => "SEARCH_PROMO",
            Self::VideoBanner => "VIDEO_BANNER",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PerformanceCampaignState {
    CampaignStateUnknown,
    CampaignStateRunning,
    CampaignStatePlanned,
    CampaignStateStopped,
    CampaignStateInactive,
    CampaignStateArchived,
    CampaignStateModerationDraft,
    CampaignStateModerationInProgress,
    CampaignStateModerationFailed,
    CampaignStateFinished,
}

impl PerformanceCampaignState {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::CampaignStateUnknown => "CAMPAIGN_STATE_UNKNOWN",
            Self::CampaignStateRunning => "CAMPAIGN_STATE_RUNNING",
            Self::CampaignStatePlanned => "CAMPAIGN_STATE_PLANNED",
            Self::CampaignStateStopped => "CAMPAIGN_STATE_STOPPED",
            Self::CampaignStateInactive => "CAMPAIGN_STATE_INACTIVE",
            Self::CampaignStateArchived => "CAMPAIGN_STATE_ARCHIVED",
            Self::CampaignStateModerationDraft => "CAMPAIGN_STATE_MODERATION_DRAFT",
            Self::CampaignStateModerationInProgress => "CAMPAIGN_STATE_MODERATION_IN_PROGRESS",
            Self::CampaignStateModerationFailed => "CAMPAIGN_STATE_MODERATION_FAILED",
            Self::CampaignStateFinished => "CAMPAIGN_STATE_FINISHED",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PerformanceCampaignsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(length(max = 10), inner(range(min = 1)))]
    pub campaign_ids: Vec<u64>,
    #[serde(default)]
    pub adv_object_type: Option<PerformanceAdvObjectType>,
    #[serde(default)]
    pub state: Option<PerformanceCampaignState>,
    #[serde(default = "default_page")]
    #[schemars(range(min = 1, max = 1_000_000))]
    pub page: u32,
    #[serde(default = "default_performance_page_size")]
    #[schemars(range(min = 1, max = 100))]
    pub page_size: u32,
}

pub(super) const fn default_performance_page_size() -> u32 {
    100
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PerformanceStatisticsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(length(max = 10), inner(range(min = 1)))]
    pub campaign_ids: Vec<u64>,
    #[schemars(
        description = "Начало периода в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD", length(equal = 10))]
    pub date_to: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PerformanceSkuStatisticsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(
        description = "До 10 campaign ID; пустой список означает все кампании",
        length(max = 10),
        inner(range(min = 1))
    )]
    pub campaign_ids: Vec<u64>,
    #[schemars(
        description = "Начало периода YYYY-MM-DD. Ozon требует дату не раньше предыдущего дня, но не публикует timezone для локальной относительной проверки",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(
        description = "Конец периода статистики в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_to: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PerformanceCampaignResourceInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Положительный ID рекламной кампании",
        range(min = 1, max = 9_223_372_036_854_775_807_u64)
    )]
    pub campaign_id: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PerformanceCampaignProductsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Положительный ID рекламной кампании",
        range(min = 1, max = 9_223_372_036_854_775_807_u64)
    )]
    pub campaign_id: u64,
    #[serde(default = "default_page")]
    #[schemars(range(min = 1, max = 1_000_000))]
    pub page: u32,
    #[serde(default = "default_performance_page_size")]
    #[schemars(range(min = 1, max = 100))]
    pub page_size: u32,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StoreOnlyInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RatingHistoryInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Начало периода в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD", length(equal = 10))]
    pub date_to: String,
    #[schemars(
        description = "От одного до 100 кодов из ozon_seller_rating, например rating_shipment_delay_cb",
        length(min = 1, max = 100),
        inner(length(min = 1, max = 128))
    )]
    pub ratings: Vec<String>,
    #[serde(default = "default_true")]
    pub with_premium_scores: bool,
}

pub(super) const fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReviewsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default = "default_reviews_limit")]
    #[schemars(range(min = 20, max = 100))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(length(max = 4_096))]
    pub last_id: String,
    #[serde(default = "default_all_status")]
    #[schemars(
        length(min = 1, max = 128),
        regex(pattern = "^(ALL|NEW|VIEWED|PROCESSED)$")
    )]
    pub status: String,
    #[serde(default)]
    #[schemars(length(max = 100), inner(range(min = 1)))]
    pub skus: Vec<u64>,
    #[serde(default = "default_all_status")]
    #[schemars(
        length(min = 1, max = 128),
        regex(pattern = "^(ALL|DELIVERED|CANCELLED)$")
    )]
    pub order_status: String,
    #[serde(default)]
    #[schemars(length(equal = 10))]
    pub published_from: Option<String>,
    #[serde(default)]
    #[schemars(length(equal = 10))]
    pub published_to: Option<String>,
    #[serde(default)]
    pub direction: SortDirection,
}

pub(super) const fn default_reviews_limit() -> u32 {
    100
}

pub(super) fn default_all_status() -> String {
    "ALL".to_owned()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QuestionsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Начало периода в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD", length(equal = 10))]
    pub date_to: String,
    #[serde(default = "default_all_status")]
    #[schemars(length(min = 1, max = 128))]
    pub status: String,
    #[serde(default)]
    #[schemars(length(max = 4_096))]
    pub last_id: String,
}
