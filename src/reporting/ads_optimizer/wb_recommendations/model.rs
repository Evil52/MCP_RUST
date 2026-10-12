use chrono::{DateTime, NaiveDate, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecommendationEvidence {
    pub version: u32,
    pub account_id: String,
    pub as_of: DateTime<Utc>,
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
    pub campaign_ids: Vec<u64>,
    pub target_drr_bps: Option<u32>,
    pub statistics_observed_at: DateTime<Utc>,
    pub composition_observed_at: DateTime<Utc>,
    pub statistics: Value,
    pub composition: Value,
    /// Complete, pinned `ofk_source_snapshot` exports, including all pages.
    pub stocks: Value,
    pub sales: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Hold,
    ReviewDecrease,
    TestIncrease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    MissingHistory,
    UnallocatedSkuHistory,
    StaleHistory,
    StaleComposition,
    NotActive,
    OutsideCurrentComposition,
    UnsupportedPlacementOrPricing,
    UnknownOrStaleFbwStock,
    FbwZeroCheckFbs,
    TargetDrrRequired,
    InsufficientMatureObservations,
    SpendWithoutOrders,
    AboveTargetDrr,
    BelowTargetDrr,
    WithinTargetBand,
    LowFbwBuffer,
    UnknownSalesForIncrease,
    CampaignLevelAttribution,
}

impl Reason {
    #[must_use]
    pub const fn explanation(self) -> &'static str {
        match self {
            Self::CampaignLevelAttribution => {
                "Итог кампании включает несколько товаров или связанные покупки; его нельзя приписать одному SKU для выбора ставки."
            }
            Self::MissingHistory => "За период есть пропуски статистики этой кампании.",
            Self::UnallocatedSkuHistory => "Товарная разбивка не сходится с итогом кампании.",
            Self::StaleHistory => "Историческая статистика давно не перечитывалась.",
            Self::StaleComposition => "Нужны свежие настройки кампании и ставки.",
            Self::NotActive => "Кампания сейчас не активна; рекомендация не разрешает запуск.",
            Self::OutsideCurrentComposition => "Товар отсутствует в текущем составе этой кампании.",
            Self::UnsupportedPlacementOrPricing => {
                "Расчёт изменения ставки поддерживает только ручной CPC в поиске без рекомендаций."
            }
            Self::UnknownOrStaleFbwStock => {
                "Наличие товара на WB не подтверждено свежим полным срезом."
            }
            Self::FbwZeroCheckFbs => {
                "На WB наблюдается нулевой FBW; отдельно проверьте FBS и доступность."
            }
            Self::TargetDrrRequired => "Для предложения ставки нужен явно выбранный целевой ДРР.",
            Self::InsufficientMatureObservations => {
                "Недостаточно перечитанных дней, кликов или заказов для изменения по эффективности."
            }
            Self::SpendWithoutOrders => {
                "После выдержки задержки атрибуции наблюдаются расходы и не менее 100 кликов без рекламных заказов; проверьте воронку и связанные продажи."
            }
            Self::AboveTargetDrr => {
                "Наблюдаемый ДРР выше цели более чем на 10%; проверить причины и возможность снижения расходов."
            }
            Self::BelowTargetDrr => {
                "Наблюдаемый ДРР ниже цели более чем на 10%; кандидат на ограниченный тест роста при подтверждённых условиях."
            }
            Self::WithinTargetBand => "ДРР находится в коридоре цели; сохранить ставку.",
            Self::LowFbwBuffer => {
                "Запас FBW меньше трёх последних суточных объёмов заказов; проверить пополнение перед ростом рекламы."
            }
            Self::UnknownSalesForIncrease => {
                "Для повышения нужен свежий полный срез заказов магазина за последние завершённые сутки."
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, JsonSchema)]
pub struct Metrics {
    #[schemars(with = "Option<u64>")]
    pub spend_minor: u64,
    #[schemars(with = "Option<u64>")]
    pub clicks: u64,
    #[schemars(with = "Option<u64>")]
    pub orders: u64,
    #[schemars(with = "Option<u64>")]
    pub revenue_minor: u64,
    pub observed_days: u32,
    pub no_data_days: u32,
    pub missing_days: u32,
    pub unallocated_days: u32,
    pub excluded_recent_days: u32,
    pub drr_bps: Option<u64>,
    pub cpo_minor: Option<u64>,
}

impl Serialize for Metrics {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let observed = self.observed_days > 0;
        serde_json::json!({
            "spend_minor": observed.then_some(self.spend_minor),
            "clicks": observed.then_some(self.clicks),
            "orders": observed.then_some(self.orders),
            "revenue_minor": observed.then_some(self.revenue_minor),
            "observed_days": self.observed_days, "no_data_days": self.no_data_days,
            "missing_days": self.missing_days, "unallocated_days": self.unallocated_days,
            "excluded_recent_days": self.excluded_recent_days,
            "drr_bps": self.drr_bps, "cpo_minor": self.cpo_minor,
        })
        .serialize(serializer)
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct Window {
    pub days: u32,
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
    pub metrics: Metrics,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ProductRecommendation {
    pub campaign_id: u64,
    pub campaign_name: String,
    pub sku: u64,
    pub current_search_bid_minor: Option<u64>,
    /// A review proposal, never a Control command or an authorized bid.
    pub suggested_step_bps: Option<i32>,
    pub action: Action,
    pub reasons: Vec<Reason>,
    pub explanations: Vec<&'static str>,
    pub fbw_units: Option<u64>,
    /// All-channel units in the separately identified sales snapshot period.
    pub recent_store_ordered_units: Option<u64>,
    pub windows: Vec<Window>,
    pub campaign_windows: Vec<Window>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent attribution, execution, causal and truncation guarantees are explicit"
)]
pub struct RecommendationReport {
    pub version: u32,
    pub mode: &'static str,
    pub account_id: String,
    pub as_of: DateTime<Utc>,
    pub calendar: &'static str,
    pub currency: &'static str,
    pub revenue_basis: &'static str,
    pub decision_basis: &'static str,
    pub target_drr_bps: Option<u32>,
    pub attribution_lag_days: u32,
    pub attribution_finality_verified: bool,
    pub auto_apply_allowed: bool,
    pub causal_effect_verified: bool,
    pub source_sha256: String,
    pub data_classification: &'static str,
    pub total_recommendations: usize,
    pub truncated: bool,
    pub stock_snapshot_id: Option<String>,
    pub sales_snapshot_id: Option<String>,
    pub sales_period_start: Option<String>,
    pub sales_period_end: Option<String>,
    pub recommendations: Vec<ProductRecommendation>,
}
