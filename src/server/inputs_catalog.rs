//! Ozon catalog and supply input contracts.

use super::{Deserialize, JsonSchema, Serialize, StoreId};

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ProductSortDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Visibility {
    #[default]
    All,
    Visible,
    Invisible,
    EmptyStock,
    NotModerated,
    Moderated,
    ReadyToSupply,
    StateFailed,
    ValidationStatePending,
    ValidationStateFail,
    ValidationStateSuccess,
    ToSupply,
    InSale,
    RemovedFromSale,
    Banned,
    Overpriced,
    CriticallyOverpriced,
    EmptyBarcode,
    BarcodeExists,
    Quarantine,
    Archived,
    AutoArchived,
    ManualArchived,
    SeasonalAutoArchived,
    VisibleWithFboStock,
    OverpricedWithStock,
    PartialApproved,
    Disabled,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CatalogVisibility {
    #[default]
    All,
    Visible,
    Invisible,
    EmptyStock,
    NotModerated,
    Moderated,
    ReadyToSupply,
    StateFailed,
    ValidationStatePending,
    ValidationStateFail,
    ValidationStateSuccess,
    ToSupply,
    InSale,
    RemovedFromSale,
    Overpriced,
    CriticallyOverpriced,
    EmptyBarcode,
    BarcodeExists,
    Quarantine,
    Archived,
    AutoArchived,
    ManualArchived,
    SeasonalAutoArchived,
    VisibleWithFboStock,
    OverpricedWithStock,
    PartialApproved,
    Disabled,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnalyticsMetric {
    Revenue,
    OrderedUnits,
    HitsView,
    HitsViewSearch,
    HitsViewPdp,
    HitsTocart,
    HitsTocartSearch,
    HitsTocartPdp,
    SessionView,
    SessionViewSearch,
    SessionViewPdp,
    ConvTocart,
    ConvTocartSearch,
    ConvTocartPdp,
    Returns,
    Cancellations,
    DeliveredUnits,
    PositionCategory,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnalyticsDimension {
    Sku,
    Spu,
    Day,
    Week,
    Month,
    Year,
    Brand,
    Category1,
    Category2,
    #[serde(rename = "modelID")]
    ModelId,
    #[serde(rename = "descriptionType")]
    DescriptionType,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AnalyticsInput {
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
    #[schemars(description = "От одной до 14 метрик Ozon", length(min = 1, max = 14))]
    pub metrics: Vec<AnalyticsMetric>,
    #[schemars(
        description = "От одного до двух измерений: например sku и day",
        length(min = 1, max = 2)
    )]
    pub dimensions: Vec<AnalyticsDimension>,
    #[serde(default = "default_analytics_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 1_000_000))]
    pub offset: u32,
    pub sort_by: Option<AnalyticsMetric>,
    #[serde(default)]
    pub sort_direction: SortDirection,
}

pub(super) const fn default_analytics_limit() -> u32 {
    1_000
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProductFilterInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub offer_ids: Vec<String>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub product_ids: Vec<String>,
    #[serde(default)]
    pub visibility: Visibility,
    #[serde(default = "default_product_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[schemars(length(max = 4_096))]
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProductPriceFilterInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub offer_ids: Vec<String>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub product_ids: Vec<String>,
    #[serde(default)]
    pub visibility: CatalogVisibility,
    #[serde(default = "default_product_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[schemars(length(max = 4_096))]
    pub cursor: Option<String>,
}

pub(super) const fn default_product_limit() -> u32 {
    100
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WarehouseStocksInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Положительный идентификатор склада FBS или rFBS",
        range(min = 1, max = 9_223_372_036_854_775_807_u64)
    )]
    pub warehouse_id: u64,
    #[serde(default = "default_product_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(length(max = 4_096))]
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WarehouseStockListInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub offer_ids: Vec<String>,
    #[serde(default)]
    #[schemars(
        length(max = 1_000),
        inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)),
        extend("uniqueItems" = true)
    )]
    pub skus: Vec<u64>,
    #[serde(default = "default_product_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(length(max = 4_096))]
    pub cursor: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WarehouseListInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default = "default_product_limit")]
    #[schemars(
        description = "Локально ограниченный размер страницы; Ozon не публикует границы limit для этого метода",
        range(min = 1, max = 1_000)
    )]
    pub limit: u32,
    #[serde(default)]
    #[schemars(
        description = "Непрозрачный cursor из предыдущего ответа Ozon",
        length(max = 4_096)
    )]
    pub cursor: Option<String>,
    #[serde(default)]
    #[schemars(
        length(max = 1_000),
        inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)),
        extend("uniqueItems" = true)
    )]
    pub warehouse_ids: Vec<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProductCatalogInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub offer_ids: Vec<String>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub product_ids: Vec<String>,
    #[serde(default)]
    #[schemars(
        length(max = 1_000),
        inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)),
        extend("uniqueItems" = true)
    )]
    pub skus: Vec<u64>,
    #[serde(default)]
    pub visibility: CatalogVisibility,
    #[serde(default = "default_product_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(length(max = 4_096))]
    pub last_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProductInfoListInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub offer_ids: Vec<String>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub product_ids: Vec<String>,
    #[serde(default)]
    #[schemars(
        length(max = 1_000),
        inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)),
        extend("uniqueItems" = true)
    )]
    pub skus: Vec<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProductPicturesInfoInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        length(min = 1, max = 1_000),
        inner(length(min = 1, max = 256)),
        extend("uniqueItems" = true)
    )]
    pub product_ids: Vec<String>,
}

pub(super) const fn default_content_diagnostic_visibility() -> CatalogVisibility {
    CatalogVisibility::StateFailed
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProductContentDiagnosticsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(length(max = 100), inner(length(min = 1, max = 256)))]
    pub offer_ids: Vec<String>,
    #[serde(default)]
    #[schemars(length(max = 100), inner(length(min = 1, max = 256)))]
    pub product_ids: Vec<String>,
    #[serde(default)]
    #[schemars(
        length(max = 100),
        inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)),
        extend("uniqueItems" = true)
    )]
    pub skus: Vec<u64>,
    #[serde(default = "default_content_diagnostic_visibility")]
    pub visibility: CatalogVisibility,
    #[serde(default = "default_product_limit")]
    #[schemars(range(min = 1, max = 100))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(length(max = 4_096))]
    pub last_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProductAttributesInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub offer_ids: Vec<String>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub product_ids: Vec<String>,
    #[serde(default)]
    #[schemars(
        length(max = 1_000),
        inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)),
        extend("uniqueItems" = true)
    )]
    pub skus: Vec<u64>,
    #[serde(default)]
    pub visibility: CatalogVisibility,
    #[serde(default = "default_product_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(length(max = 4_096))]
    pub last_id: String,
    #[serde(default)]
    pub sort_direction: ProductSortDirection,
}

#[derive(
    Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SupplyOrderState {
    DataFilling,
    ReadyToSupply,
    AcceptedAtSupplyWarehouse,
    InTransit,
    AcceptanceAtStorageWarehouse,
    ReportsConfirmationAwaiting,
    ReportRejected,
    Completed,
    RejectedAtSupplyWarehouse,
    Cancelled,
    Overdue,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SupplyOrderSortBy {
    #[default]
    OrderCreation,
    OrderStateUpdatedAt,
    TimeslotFromUtc,
    TimeslotFromLocal,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SupplyOrderSortDirection {
    Asc,
    #[default]
    Desc,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SupplyOrderTimeslotFilterType {
    ByLocalTime,
    ByUtcTime,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SupplyOrderTimeslotRangeInput {
    #[serde(default)]
    #[schemars(
        description = "Начало диапазона таймслота в формате RFC3339",
        length(min = 20, max = 64)
    )]
    pub from: Option<String>,
    #[serde(default)]
    #[schemars(
        description = "Конец диапазона таймслота в формате RFC3339",
        length(min = 20, max = 64)
    )]
    pub to: Option<String>,
    #[serde(default)]
    pub timeslot_filter_type: Option<SupplyOrderTimeslotFilterType>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SupplyOrderListInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(
        description = "Уникальные статусы заявок; пустой список означает все статусы",
        length(max = 11),
        extend("uniqueItems" = true)
    )]
    pub states: Vec<SupplyOrderState>,
    #[serde(default)]
    #[schemars(
        description = "До 1000 уникальных положительных ID пунктов отгрузки",
        length(max = 1_000),
        inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)),
        extend("uniqueItems" = true)
    )]
    pub dropoff_warehouse_ids: Vec<u64>,
    #[serde(default)]
    #[schemars(
        description = "Поиск по номеру заявки: от 3 до 256 символов",
        length(min = 3, max = 256)
    )]
    pub order_number_search: Option<String>,
    #[serde(default)]
    pub timeslot_from_range: Option<SupplyOrderTimeslotRangeInput>,
    #[serde(default)]
    #[schemars(length(max = 4_096))]
    pub last_id: Option<String>,
    #[serde(default = "default_supply_order_limit")]
    #[schemars(range(min = 1, max = 100))]
    pub limit: u32,
    #[serde(default)]
    pub sort_by: SupplyOrderSortBy,
    #[serde(default)]
    pub sort_dir: SupplyOrderSortDirection,
}

pub(super) const fn default_supply_order_limit() -> u32 {
    100
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SupplyOrderGetInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "От 1 до 50 уникальных положительных ID заявок на поставку",
        length(min = 1, max = 50),
        inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)),
        extend("uniqueItems" = true)
    )]
    pub order_ids: Vec<u64>,
}
