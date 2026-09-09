//! Wildberries tool input contracts.

use super::{Deserialize, JsonSchema, Serialize, default_product_limit, default_true};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbAccountInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
}

macro_rules! wb_funnel_filter_input {
    (
        $name:ident,
        $from_description:literal,
        brand_max = $brand_max:literal,
        ids_max = $ids_max:literal,
        before = { $($before:tt)* },
        after = { $($after:tt)* }
    ) => {
        #[derive(Debug, Deserialize, JsonSchema)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            #[serde(default)]
            #[schemars(
                description = "Канонический account_id Wildberries из wb_stores_status",
                length(min = 1, max = 128)
            )]
            pub account: Option<String>,
            #[schemars(description = $from_description, length(equal = 10))]
            pub date_from: String,
            #[schemars(description = "Конец периода в формате YYYY-MM-DD", length(equal = 10))]
            pub date_to: String,
            $($before)*
            #[serde(default)]
            #[schemars(length(max = $brand_max), inner(length(min = 1, max = 128)))]
            pub brand_names: Vec<String>,
            #[serde(default)]
            #[schemars(length(max = $ids_max))]
            pub subject_ids: Vec<u64>,
            #[serde(default)]
            #[schemars(length(max = $ids_max))]
            pub tag_ids: Vec<u64>,
            $($after)*
        }
    };
}

wb_funnel_filter_input!(
    WbSalesFunnelInput,
    "Начало периода в формате YYYY-MM-DD",
    brand_max = 100,
    ids_max = 1_000,
    before = {
        #[serde(default)]
        #[schemars(length(max = 1_000))]
        pub nm_ids: Vec<u64>,
    },
    after = {
        #[serde(default)]
        pub skip_deleted_nm: bool,
        #[serde(default = "default_product_limit")]
        #[schemars(range(min = 1, max = 1_000))]
        pub limit: u32,
        #[serde(default)]
        #[schemars(range(max = 1_000_000))]
        pub offset: u32,
    }
);

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum WbAggregationLevel {
    #[default]
    Day,
    Week,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbSalesFunnelHistoryInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[schemars(
        description = "Начало периода в формате YYYY-MM-DD; WB хранит историю этого отчёта за последнюю неделю",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD", length(equal = 10))]
    pub date_to: String,
    #[schemars(length(min = 1, max = 20))]
    pub nm_ids: Vec<u64>,
    #[serde(default)]
    pub skip_deleted_nm: bool,
    #[serde(default)]
    pub aggregation_level: WbAggregationLevel,
}

wb_funnel_filter_input!(
    WbSalesFunnelGroupedHistoryInput,
    "Начало периода в формате YYYY-MM-DD; WB хранит историю этого отчёта за последнюю неделю",
    brand_max = 16,
    ids_max = 16,
    before = {},
    after = {
        #[serde(default)]
        pub skip_deleted_nm: bool,
        #[serde(default)]
        pub aggregation_level: WbAggregationLevel,
    }
);

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbWarehouseStocksInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[serde(default)]
    #[schemars(length(max = 1_000))]
    pub nm_ids: Vec<u64>,
    #[serde(default)]
    #[schemars(length(max = 1_000))]
    pub chrt_ids: Vec<u64>,
    #[serde(default = "default_product_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 1_000_000))]
    pub offset: u32,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbSellerWarehouseStocksInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[schemars(
        description = "ID склада продавца из wb_seller_warehouses; для FBS выбирайте deliveryType=1",
        range(min = 1, max = 9_223_372_036_854_775_807_u64)
    )]
    pub warehouse_id: u64,
    #[schemars(
        description = "Уникальные ID размеров sizes[].chrtID из wb_product_cards, НЕ nmID и НЕ баркоды. Разбивайте полный список на пакеты до 1000 ID для каждого склада",
        length(min = 1, max = 1_000),
        inner(range(min = 1, max = 9_223_372_036_854_775_807_u64))
    )]
    pub chrt_ids: Vec<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbStatisticsReportInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[schemars(
        description = "Дата изменения в формате YYYY-MM-DD или RFC3339",
        length(min = 10, max = 64)
    )]
    pub date_from: String,
    #[serde(default)]
    #[schemars(
        description = "0 — данные начиная с date_from; 1 — только данные за указанную дату изменения",
        range(max = 1)
    )]
    pub flag: u8,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WbLocale {
    Ru,
    En,
    Zh,
}

impl WbLocale {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Ru => "ru",
            Self::En => "en",
            Self::Zh => "zh",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbProductCardsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[serde(default)]
    #[schemars(description = "Язык полей ответа: ru, en или zh")]
    pub locale: Option<WbLocale>,
    #[serde(default = "default_true")]
    pub ascending: bool,
    #[serde(default)]
    #[schemars(
        description = "Фильтр фотографий: -1 — без фильтра, 0 — без фото, 1 — с фото",
        range(min = -1, max = 1)
    )]
    pub with_photo: Option<i8>,
    #[serde(default)]
    #[schemars(length(min = 1, max = 256))]
    pub text_search: Option<String>,
    #[serde(default)]
    pub allowed_categories_only: Option<bool>,
    #[serde(default)]
    #[schemars(length(max = 100), inner(range(min = 1)))]
    pub tag_ids: Vec<u64>,
    #[serde(default)]
    #[schemars(length(max = 100), inner(range(min = 1)))]
    pub object_ids: Vec<u64>,
    #[serde(default)]
    #[schemars(length(max = 100), inner(length(min = 1, max = 128)))]
    pub brands: Vec<String>,
    #[serde(default)]
    #[schemars(range(min = 1))]
    pub imt_id: Option<u64>,
    #[serde(default)]
    #[schemars(length(min = 1, max = 64))]
    pub cursor_updated_at: Option<String>,
    #[serde(default)]
    #[schemars(range(min = 1))]
    pub cursor_nm_id: Option<u64>,
    #[serde(default = "default_wb_cards_limit")]
    #[schemars(range(min = 1, max = 100))]
    pub limit: u32,
}

pub(super) const fn default_wb_cards_limit() -> u32 {
    50
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbProductPricesInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[serde(default)]
    #[schemars(range(min = 1))]
    pub nm_id: Option<u64>,
    #[serde(default = "default_wb_prices_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 1_000_000))]
    pub offset: u32,
}

pub(super) const fn default_wb_prices_limit() -> u32 {
    1_000
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbTariffCommissionsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[serde(default)]
    #[schemars(description = "Язык названий категорий: ru, en или zh")]
    pub locale: Option<WbLocale>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbTariffDateInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[schemars(description = "Дата тарифа в формате YYYY-MM-DD", length(equal = 10))]
    pub date: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbAcceptanceCoefficientsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[serde(default)]
    #[schemars(
        description = "До 100 уникальных положительных ID складов; пустой список означает все склады",
        length(max = 100),
        inner(range(min = 1))
    )]
    pub warehouse_ids: Vec<u64>,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WbPromotionPaymentType {
    Cpm,
    Cpc,
}

impl WbPromotionPaymentType {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Cpm => "cpm",
            Self::Cpc => "cpc",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbPromotionCampaignDetailsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[schemars(
        description = "От 1 до 50 уникальных положительных ID кампаний из wb_promotion_campaigns",
        length(min = 1, max = 50),
        inner(range(min = 1)),
        extend("uniqueItems" = true)
    )]
    pub campaign_ids: Vec<u64>,
    #[serde(default)]
    #[schemars(
        description = "Необязательный непустой фильтр официальных статусов WB: -1, 4, 7, 8, 9 или 11",
        length(min = 1, max = 6),
        extend(
            "uniqueItems" = true,
            "items" = {"type": "integer", "enum": [-1, 4, 7, 8, 9, 11]}
        )
    )]
    pub statuses: Option<Vec<i32>>,
    #[serde(default)]
    #[schemars(description = "Необязательный тип оплаты кампании: cpm или cpc")]
    pub payment_type: Option<WbPromotionPaymentType>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbPromotionStatsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[schemars(
        description = "От 1 до 50 уникальных положительных ID кампаний из wb_promotion_campaigns; официальный fullstats поддерживает кампании в статусах 7, 9 и 11",
        length(min = 1, max = 50),
        inner(range(min = 1)),
        extend("uniqueItems" = true)
    )]
    pub campaign_ids: Vec<u64>,
    #[schemars(
        description = "Начало периода в формате YYYY-MM-DD",
        length(equal = 10),
        regex(pattern = r"^\d{4}-\d{2}-\d{2}$")
    )]
    pub begin_date: String,
    #[schemars(
        description = "Конец периода в формате YYYY-MM-DD",
        length(equal = 10),
        regex(pattern = r"^\d{4}-\d{2}-\d{2}$")
    )]
    pub end_date: String,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum WbSearchTopOrderBy {
    OpenCard,
    AddToCart,
    OpenToCart,
    Orders,
    CartToOrder,
}

impl WbSearchTopOrderBy {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::OpenCard => "openCard",
            Self::AddToCart => "addToCart",
            Self::OpenToCart => "openToCart",
            Self::Orders => "orders",
            Self::CartToOrder => "cartToOrder",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbSearchProductQueriesInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[schemars(
        description = "Начало отчётного периода в формате YYYY-MM-DD; период не более 31 дня, данные WB обновляются примерно раз в час",
        length(equal = 10),
        regex(pattern = r"^\d{4}-\d{2}-\d{2}$")
    )]
    pub date_from: String,
    #[schemars(
        description = "Конец отчётного периода в формате YYYY-MM-DD; период не более 31 дня",
        length(equal = 10),
        regex(pattern = r"^\d{4}-\d{2}-\d{2}$")
    )]
    pub date_to: String,
    #[schemars(
        description = "От 1 до 50 уникальных положительных артикулов WB",
        length(min = 1, max = 50),
        inner(range(min = 1)),
        extend("uniqueItems" = true)
    )]
    pub nm_ids: Vec<u64>,
    #[schemars(description = "Метрика для отбора верхних поисковых запросов WB")]
    pub top_order_by: WbSearchTopOrderBy,
    #[serde(default = "default_wb_search_limit")]
    #[schemars(
        description = "Число запросов; безопасный предел стандартного тарифа — 30",
        range(min = 1, max = 30)
    )]
    pub limit: u32,
}

pub(super) const fn default_wb_search_limit() -> u32 {
    30
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbSearchOrdersPositionsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[schemars(
        description = "Начало периода в формате YYYY-MM-DD; максимум 7 дней",
        length(equal = 10),
        regex(pattern = r"^\d{4}-\d{2}-\d{2}$")
    )]
    pub date_from: String,
    #[schemars(
        description = "Конец периода в формате YYYY-MM-DD; максимум 7 дней",
        length(equal = 10),
        regex(pattern = r"^\d{4}-\d{2}-\d{2}$")
    )]
    pub date_to: String,
    #[schemars(description = "Положительный артикул WB", range(min = 1))]
    pub nm_id: u64,
    #[schemars(
        description = "От 1 до 30 уникальных непустых поисковых фраз; каждая не длиннее 256 байт UTF-8 (maxLength также ограничивает число символов)",
        length(min = 1, max = 30),
        inner(length(min = 1, max = 256)),
        extend("uniqueItems" = true)
    )]
    pub search_texts: Vec<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum WbPromotionPlacementType {
    Combined,
    Search,
    Recommendation,
}

impl WbPromotionPlacementType {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Combined => "combined",
            Self::Search => "search",
            Self::Recommendation => "recommendation",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbPromotionMinimumBidsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[schemars(description = "Положительный ID рекламной кампании WB", range(min = 1))]
    pub campaign_id: u64,
    #[schemars(
        description = "От 1 до 100 уникальных положительных артикулов WB",
        length(min = 1, max = 100),
        inner(range(min = 1)),
        extend("uniqueItems" = true)
    )]
    pub nm_ids: Vec<u64>,
    pub payment_type: WbPromotionPaymentType,
    #[schemars(
        description = "От 1 до 3 уникальных мест размещения: combined, search, recommendation",
        length(min = 1, max = 3),
        extend("uniqueItems" = true)
    )]
    pub placement_types: Vec<WbPromotionPlacementType>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbPromotionRecommendedBidsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[schemars(description = "Положительный ID CPM-кампании WB", range(min = 1))]
    pub campaign_id: u64,
    #[schemars(description = "Положительный артикул WB", range(min = 1))]
    pub nm_id: u64,
}

#[derive(Debug, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct WbPromotionSearchClusterPair {
    #[schemars(description = "Положительный ID рекламной кампании WB", range(min = 1))]
    pub campaign_id: u64,
    #[schemars(description = "Положительный артикул WB", range(min = 1))]
    pub nm_id: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbPromotionSearchClusterBidsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[schemars(
        description = "От 1 до 100 уникальных пар кампания + артикул WB",
        length(min = 1, max = 100),
        extend("uniqueItems" = true)
    )]
    pub items: Vec<WbPromotionSearchClusterPair>,
}
