//! Bounded read-only contracts verified with the current Ozon Seller API.
use super::{Deserialize, JsonSchema, StoreId};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClusterListInput {
    #[serde(default)]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(length(max = 100), inner(length(min = 1, max = 20)), extend("uniqueItems" = true))]
    pub cluster_ids: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClusterStockAnalyticsInput {
    #[serde(default)]
    pub store: Option<StoreId>,
    #[schemars(length(min = 1, max = 100), inner(length(min = 1, max = 20)), extend("uniqueItems" = true))]
    pub skus: Vec<String>,
    #[serde(default)]
    #[schemars(length(max = 100), inner(length(min = 1, max = 20)), extend("uniqueItems" = true))]
    pub warehouse_ids: Vec<String>,
    #[serde(default)]
    #[schemars(length(max = 100), inner(length(min = 1, max = 20)), extend("uniqueItems" = true))]
    pub cluster_ids: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SupplyBundleInput {
    #[serde(default)]
    pub store: Option<StoreId>,
    #[schemars(length(min = 1, max = 100), inner(length(min = 1, max = 128)), extend("uniqueItems" = true))]
    pub bundle_ids: Vec<String>,
    #[serde(default = "default_limit")]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub query: String,
    #[serde(default)]
    #[schemars(length(max = 128))]
    pub last_id: Option<String>,
}

const fn default_limit() -> u32 {
    100
}
