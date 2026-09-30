//! Live, account-authorized campaign diagnostics using the existing read client.
use super::super::{
    DAILY_STATS_PATH, Json, OzonMcp, OzonResult, Parameters, RequestIdentity, StatisticsQuery,
    StoreId, validate_campaign_ids, validate_date_range,
};
use crate::reporting::ads_optimizer::daily_analysis::{
    DailyAnalysisScope, analyze_daily_response, validate_scope,
};
use chrono::{NaiveDate, Utc};
use rmcp::{schemars::JsonSchema, tool, tool_router};
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(in crate::server) struct AdsAnalysisInput {
    #[serde(default)]
    #[schemars(length(min = 1, max = 128))]
    pub store: Option<StoreId>,
    #[schemars(length(min = 1, max = 10), inner(range(min = 1)))]
    pub campaign_ids: Vec<u64>,
    #[schemars(length(equal = 10))]
    pub date_from: String,
    #[schemars(length(equal = 10))]
    pub date_to: String,
    /// Необязательный ориентир ДРР в базисных пунктах: 1500 = 15%.
    #[serde(default)]
    #[schemars(range(min = 1, max = 10000))]
    pub target_drr_bps: Option<u32>,
}

#[tool_router(router = ads_analysis_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Анализ живой дневной статистики 1–10 кампаний Ozon за период до 31 дня:
    /// расходы, заказы, выручка, CPC, ДРР и пропуски дат. Деньги в копейках.
    /// Сигналы описывают наблюдаемую статистику кампаний; зрелость атрибуции,
    /// прибыль и прямые заказы SKU не подтверждены. Ставки и бюджеты не меняет.
    #[tool(name = "ozon_ads_analysis", annotations(read_only_hint = true))]
    pub(in crate::server) async fn ads_analysis(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<AdsAnalysisInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_campaign_ids(&input.campaign_ids)?;
        validate_date_range(&input.date_from, &input.date_to, 31)?;
        let store = self.performance_context(&identity, input.store.as_ref())?;
        let mut scope = DailyAnalysisScope {
            store_id: store.0.clone(),
            campaign_ids: input.campaign_ids.clone(),
            date_from: NaiveDate::parse_from_str(&input.date_from, "%Y-%m-%d")
                .map_err(|_| "invalid analysis date")?,
            date_to: NaiveDate::parse_from_str(&input.date_to, "%Y-%m-%d")
                .map_err(|_| "invalid analysis date")?,
            observed_at: Utc::now(),
            target_drr_bps: input.target_drr_bps,
        };
        validate_scope(&scope).map_err(|error| error.to_string())?;
        let data = self
            .performance_client
            .daily_statistics(
                &store,
                StatisticsQuery {
                    campaign_ids: input.campaign_ids,
                    date_from: input.date_from,
                    date_to: input.date_to,
                },
            )
            .await
            .map_err(|error| Self::performance_error(&store, DAILY_STATS_PATH, &error))?;
        scope.observed_at = Utc::now();
        let report = analyze_daily_response(scope, &data).map_err(|error| error.to_string())?;
        let data = serde_json::to_value(report).map_err(|_| "analysis serialization failed")?;
        Ok(Self::performance_result(store, DAILY_STATS_PATH, data))
    }
}
