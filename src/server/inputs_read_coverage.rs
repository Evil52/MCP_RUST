//! Explicit query contracts for supplemental marketplace reads.
use super::{Deserialize, JsonSchema, StoreId};
use crate::wb::read_coverage::{
    ArchiveQuery, CardErrorsQuery, ClaimQuery, FeedbackQuery, SuppliesQuery, SupplyGoodsQuery,
    SupplyIdQuery, TrashQuery,
};
macro_rules! query_input {
    ($name:ident,$query:ty) => {
        #[derive(Debug, Deserialize, JsonSchema)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            #[serde(default)]
            #[schemars(length(min = 1, max = 128))]
            pub account: Option<String>,
            pub query: $query,
        }
    };
}
query_input!(WbFeedbackReadInput, FeedbackQuery);
query_input!(WbReviewArchiveInput, ArchiveQuery);
query_input!(WbClaimReadInput, ClaimQuery);
query_input!(WbCardErrorsInput, CardErrorsQuery);
query_input!(WbCardsTrashInput, TrashQuery);
query_input!(WbSuppliesReadInput, SuppliesQuery);
query_input!(WbSupplyReadInput, SupplyIdQuery);
query_input!(WbSupplyGoodsInput, SupplyGoodsQuery);
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbCustomerItemInput {
    #[serde(default)]
    pub account: Option<String>,
    #[schemars(length(min = 1, max = 128))]
    pub id: String,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbSubjectInput {
    #[serde(default)]
    pub account: Option<String>,
    #[schemars(range(min = 1, max = 9_223_372_036_854_775_807_u64))]
    pub subject_id: u64,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbSupplyPackagesInput {
    #[serde(default)]
    pub account: Option<String>,
    #[schemars(range(min = 1, max = 9_223_372_036_854_775_807_u64))]
    pub supply_id: u64,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OzonProductQueriesInput {
    #[serde(default)]
    pub store: Option<StoreId>,
    #[schemars(
        length(min = 20, max = 40),
        description = "Начало периода RFC3339. Для истории старше месяца Ozon использует недельный период без date_to."
    )]
    pub date_from: String,
    #[serde(default)]
    #[schemars(length(min = 20, max = 40))]
    pub date_to: Option<String>,
    #[schemars(length(min = 1, max = 1000), inner(length(min = 1, max = 20)))]
    pub skus: Vec<String>,
    #[serde(default)]
    #[schemars(range(max = 10000))]
    pub page: u32,
    #[serde(default = "crate::wb::read_coverage::default_limit")]
    #[schemars(range(min = 1, max = 1000))]
    pub page_size: u32,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OzonProductQueryDetailsInput {
    #[serde(default)]
    pub store: Option<StoreId>,
    #[schemars(
        length(min = 20, max = 40),
        description = "Начало периода RFC3339. Для истории старше месяца Ozon использует недельный период без date_to."
    )]
    pub date_from: String,
    #[serde(default)]
    #[schemars(length(min = 20, max = 40))]
    pub date_to: Option<String>,
    #[schemars(length(min = 1, max = 1000), inner(length(min = 1, max = 20)))]
    pub skus: Vec<String>,
    #[serde(default)]
    #[schemars(range(max = 10000))]
    pub page: u32,
    #[serde(default = "crate::wb::read_coverage::default_limit")]
    #[schemars(range(min = 1, max = 1000))]
    pub page_size: u32,
    #[schemars(range(min = 1, max = 1000))]
    pub limit_by_sku: u32,
}
