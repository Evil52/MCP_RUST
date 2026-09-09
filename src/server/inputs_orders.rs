//! Ozon posting, return and finance input contracts.

use super::{
    Deserialize, JsonSchema, Serialize, SortDirection, StoreId, default_product_limit, default_true,
};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TurnoverInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub skus: Vec<String>,
    #[serde(default = "default_product_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 1_000_000))]
    pub offset: u32,
}

macro_rules! period_input {
    ($name:ident, $from_description:literal, $to_description:literal, { $($fields:tt)* }) => {
        #[derive(Debug, Deserialize, JsonSchema)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            #[serde(default)]
            #[schemars(
                description = "Канонический store_id или account_id из marketplace_accounts",
                length(min = 1, max = 128)
            )]
            pub store: Option<StoreId>,
            #[schemars(description = $from_description, length(equal = 10))]
            pub date_from: String,
            #[schemars(description = $to_description, length(equal = 10))]
            pub date_to: String,
            $($fields)*
        }
    };
}

period_input!(
    PostingListInput,
    "Начало периода в формате YYYY-MM-DD",
    "Конец периода в формате YYYY-MM-DD",
    {
    #[serde(default)]
    #[schemars(length(max = 128))]
    pub status: String,
    #[serde(default = "default_posting_limit")]
    #[schemars(range(min = 1, max = 100))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(
        description = "Legacy-поле со значением 0; актуальная пагинация использует cursor",
        range(max = 0)
    )]
    pub offset: u32,
    #[serde(default)]
    #[schemars(
        description = "Непрозрачный cursor из предыдущей страницы",
        length(max = 4_096)
    )]
    pub cursor: Option<String>,
    #[serde(default)]
        pub direction: SortDirection,
    }
);

period_input!(
    PostingSalesFallbackInput,
    "Начало периода отправлений в формате YYYY-MM-DD",
    "Конец периода отправлений в формате YYYY-MM-DD",
    {}
);

pub(super) const fn default_posting_limit() -> u32 {
    100
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostingGetInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(length(min = 1, max = 256))]
    pub posting_number: String,
}

period_input!(
    FbsUnfulfilledInput,
    "Начало периода изменения статуса в формате YYYY-MM-DD",
    "Конец периода изменения статуса в формате YYYY-MM-DD",
    {
    #[serde(default)]
    #[schemars(length(max = 4_096))]
    pub cursor: String,
    #[serde(default = "default_posting_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[serde(default)]
    pub direction: SortDirection,
    #[serde(default)]
    #[schemars(length(max = 100), inner(length(min = 1, max = 128)))]
    pub statuses: Vec<String>,
    #[serde(default)]
    #[schemars(
        length(max = 1_000),
        inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)),
        extend("uniqueItems" = true)
    )]
    pub warehouse_ids: Vec<u64>,
    #[serde(default)]
    #[schemars(
        length(max = 1_000),
        inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)),
        extend("uniqueItems" = true)
    )]
    pub provider_ids: Vec<u64>,
    #[serde(default)]
    #[schemars(
        length(max = 1_000),
        inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)),
        extend("uniqueItems" = true)
    )]
    pub delivery_method_ids: Vec<u64>,
    #[serde(default)]
    #[schemars(length(equal = 10))]
    pub cutoff_from: Option<String>,
    #[serde(default)]
    #[schemars(length(equal = 10))]
    pub cutoff_to: Option<String>,
    #[serde(default)]
    #[schemars(length(equal = 10))]
    pub delivering_date_from: Option<String>,
    #[serde(default)]
    #[schemars(length(equal = 10))]
    pub delivering_date_to: Option<String>,
    }
);

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReturnSchema {
    #[default]
    Fbo,
    Fbs,
}

impl ReturnSchema {
    pub(super) const fn as_ozon_str(self) -> &'static str {
        match self {
            Self::Fbo => "FBO",
            Self::Fbs => "FBS",
        }
    }
}

period_input!(
    ReturnsInput,
    "Начало периода изменения статуса в формате YYYY-MM-DD",
    "Конец периода изменения статуса в формате YYYY-MM-DD",
    {
    #[serde(default)]
    pub return_schema: ReturnSchema,
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub offer_id: String,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub posting_numbers: Vec<String>,
    #[serde(default = "default_returns_limit")]
    #[schemars(range(min = 1, max = 500))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 18_446_744_073_709_551_615_u64))]
        pub last_id: u64,
    }
);

pub(super) const fn default_returns_limit() -> u32 {
    500
}

period_input!(
    RfbsReturnsInput,
    "Начало периода создания возврата в формате YYYY-MM-DD",
    "Конец периода создания возврата в формате YYYY-MM-DD",
    {
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub offer_id: String,
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub posting_number: String,
    #[serde(default)]
    #[schemars(length(max = 100), inner(length(min = 1, max = 128)))]
    pub group_state: Vec<String>,
    #[serde(default)]
    #[schemars(range(max = 18_446_744_073_709_551_615_u64))]
    pub last_id: u64,
    #[serde(default = "default_rfbs_returns_limit")]
    #[schemars(range(min = 1, max = 100))]
        pub limit: u32,
    }
);

pub(super) const fn default_rfbs_returns_limit() -> u32 {
    100
}

period_input!(
    FinanceInput,
    "Начало периода в формате YYYY-MM-DD",
    "Конец периода в формате YYYY-MM-DD",
    {
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub posting_number: String,
    #[serde(default)]
    #[schemars(length(max = 100), inner(length(min = 1, max = 128)))]
    pub operation_types: Vec<String>,
    #[serde(default = "default_transaction_type")]
    #[schemars(length(max = 128))]
    pub transaction_type: String,
    #[serde(default = "default_page")]
    #[schemars(range(min = 1, max = 1_000_000))]
    pub page: u32,
    #[serde(default = "default_finance_page_size")]
    #[schemars(range(min = 1, max = 1_000))]
        pub page_size: u32,
    }
);

pub(super) fn default_transaction_type() -> String {
    "all".to_owned()
}

pub(super) const fn default_page() -> u32 {
    1
}

pub(super) const fn default_finance_page_size() -> u32 {
    1_000
}

period_input!(
    FinanceTotalsInput,
    "Начало периода в формате YYYY-MM-DD",
    "Конец периода в формате YYYY-MM-DD",
    {
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub posting_number: String,
    #[serde(default = "default_transaction_type")]
    #[schemars(length(max = 128))]
        pub transaction_type: String,
    }
);

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FinanceAccrualPostingsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Непустой список номеров отправлений",
        length(min = 1, max = 1_000),
        inner(length(min = 1, max = 256))
    )]
    pub posting_numbers: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FinanceAccrualTypesInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FinanceAccrualByDayInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Дата начислений в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date: String,
    #[serde(default)]
    #[schemars(
        description = "Непрозрачный last_id из предыдущего ответа; действует 15 минут",
        length(max = 4_096)
    )]
    pub last_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FinanceRealizationByDayInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Дата отчёта в формате YYYY-MM-DD; Ozon хранит не более 32 дней",
        length(equal = 10)
    )]
    pub date: String,
}

period_input!(
    FinanceCashFlowInput,
    "Начало расчётного периода Ozon в формате YYYY-MM-DD",
    "Конец расчётного периода Ozon в формате YYYY-MM-DD",
    {
    #[serde(default = "default_page")]
    #[schemars(range(min = 1, max = 1_000_000))]
    pub page: u32,
    #[serde(default = "default_finance_page_size")]
    #[schemars(range(min = 1, max = 1_000))]
    pub page_size: u32,
    #[serde(default = "default_true")]
    pub with_details: bool,
    }
);

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum FinanceLanguage {
    #[default]
    Default,
    Ru,
    En,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FinanceMutualSettlementInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(description = "Месяц отчёта в формате YYYY-MM", length(equal = 7))]
    pub date: String,
    #[serde(default)]
    pub language: FinanceLanguage,
}
