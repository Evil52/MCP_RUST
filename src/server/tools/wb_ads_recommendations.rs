//! Bounded read-only daily WB proposals; every source is authorized server-side.
use chrono::{Duration, Utc};
use rmcp::{schemars::JsonSchema, tool, tool_router};
use serde::Deserialize;
use serde_json::Value;

use super::super::{
    Json, OzonMcp, Parameters, REPORTING_INVALID_REQUEST, RequestIdentity,
    validate_unique_positive_ids,
};
use crate::reporting::{
    ads_optimizer::wb_recommendations::{RecommendationEvidence, RecommendationReport, recommend},
    mcp_read::{SourceSnapshotQuery, SourceSnapshotResult},
    snapshot::{AccountScope, Marketplace, SnapshotSource},
};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(in crate::server) struct WbAdsRecommendationsInput {
    pub account: Option<String>,
    #[schemars(length(min = 1, max = 10), inner(range(min = 1)))]
    pub campaign_ids: Vec<u64>,
    /// 7, 14 or 30 closed Moscow calendar days. Defaults to 30.
    #[serde(default = "default_days")]
    pub days: u32,
    /// Explicit advertising objective, not a break-even or profit estimate.
    #[schemars(range(min = 1, max = 10000))]
    pub target_drr_bps: Option<u32>,
    #[serde(default = "default_limit")]
    #[schemars(range(min = 1, max = 100))]
    pub limit: u16,
}

const fn default_days() -> u32 {
    30
}
const fn default_limit() -> u16 {
    50
}

impl OzonMcp {
    /// Read all pages of one immutable published snapshot, never mix refreshes.
    async fn recommendation_snapshot(
        &self,
        account: &AccountScope,
        source: SnapshotSource,
    ) -> Result<Value, String> {
        let mut snapshot = self
            .reporting_reader
            .source_snapshot(
                account,
                SourceSnapshotQuery {
                    source,
                    snapshot_id: None,
                    limit: 1000,
                    offset: 0,
                },
            )
            .await
            .map_err(Self::reporting_error)?;
        if snapshot.total_rows > 25_000 {
            return Err("WB_RECOMMENDATIONS_LIMIT: снимок превышает предел 25000 строк".into());
        }
        while let Some(offset) = snapshot.next_offset {
            let id = snapshot
                .snapshot_id
                .as_deref()
                .and_then(|id| id.parse::<i64>().ok())
                .ok_or("WB_RECOMMENDATIONS_INVALID_SNAPSHOT")?;
            let page = self
                .reporting_reader
                .source_snapshot(
                    account,
                    SourceSnapshotQuery {
                        source,
                        snapshot_id: Some(id),
                        limit: 1000,
                        offset,
                    },
                )
                .await
                .map_err(Self::reporting_error)?;
            validate_page(&snapshot, &page, offset)?;
            snapshot.next_offset = page.next_offset;
            snapshot.rows.extend(page.rows);
        }
        if snapshot.rows.len() as u64 != snapshot.total_rows {
            return Err("WB_RECOMMENDATIONS_INVALID_SNAPSHOT".into());
        }
        serde_json::to_value(snapshot).map_err(|_| "WB_RECOMMENDATIONS_SERIALIZATION".into())
    }
}

fn validate_page(
    first: &SourceSnapshotResult,
    page: &SourceSnapshotResult,
    offset: u32,
) -> Result<(), String> {
    if first.snapshot_id != page.snapshot_id
        || first.account_id != page.account_id
        || first.source != page.source
        || first.marketplace != page.marketplace
        || first.total_rows != page.total_rows
        || first.source_as_of != page.source_as_of
        || first.period_start != page.period_start
        || first.period_end != page.period_end
        || page.next_offset.is_some_and(|next| next <= offset)
        || page.rows.is_empty()
        || offset as usize != first.rows.len()
        || first.rows.len() + page.rows.len() > 25_000
    {
        return Err("WB_RECOMMENDATIONS_INVALID_SNAPSHOT".into());
    }
    Ok(())
}

#[tool_router(router = wb_ads_recommendations_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Ежедневные рекомендации WB для 1–10 явно выбранных кампаний: история 7/14/30 дней,
    /// свежий состав, FBW и отдельные заказы магазина из PostgreSQL. Перечитывает WB
    /// через общую квоту; исключает 3 дня задержки атрибуции. Только предложения для
    /// ручного CPC в поиске; FBS, прибыль и причинный эффект не подтверждены.
    /// Целевой ДРР задаётся явно, без него выдаются объяснения удержания. Рекламу не меняет.
    #[tool(
        name = "wb_ads_recommendations",
        annotations(
            title = "Ежедневные рекомендации по рекламе WB",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    pub(in crate::server) async fn wb_ads_recommendations(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbAdsRecommendationsInput>,
    ) -> Result<Json<RecommendationReport>, String> {
        // Both reporting finance privileges and the live WB selector precede any I/O.
        let (account, role) =
            self.resolve_reporting_account(&identity, input.account.as_deref())?;
        Self::authorize_reporting_details_for_role(role)?;
        if account.marketplace() != Marketplace::Wildberries {
            return Err(format!(
                "{REPORTING_INVALID_REQUEST}: нужен кабинет Wildberries"
            ));
        }
        let wb_account = self.resolve_wb_account(&identity, Some(account.account_id()))?;
        if input.campaign_ids.is_empty()
            || input.campaign_ids.len() > 10
            || ![7, 14, 30].contains(&input.days)
            || !(1..=100).contains(&input.limit)
            || input
                .target_drr_bps
                .is_some_and(|target| target == 0 || target > 10_000)
        {
            return Err(format!(
                "{REPORTING_INVALID_REQUEST}: недопустимая область рекомендаций"
            ));
        }
        validate_unique_positive_ids("campaign_ids", &input.campaign_ids)?;
        let date_to = crate::reporting::advertising_history::last_closed_day();
        let date_from = date_to - Duration::days(i64::from(input.days) - 1);
        // Read prepared context before consuming marketplace quota.
        let stocks = self
            .recommendation_snapshot(&account, SnapshotSource::Stocks)
            .await?;
        let sales = self
            .recommendation_snapshot(&account, SnapshotSource::Sales)
            .await?;
        let endpoint = "promotion:/api/advert/v2/adverts";
        let composition = self
            .wb_client
            .promotion_campaign_details(&wb_account, input.campaign_ids.clone(), Vec::new(), None)
            .await
            .map_err(|error| self.wb_error(&wb_account, endpoint, &error))?;
        let composition_observed_at = Utc::now();
        let endpoint = "promotion:/adv/v3/fullstats";
        let statistics = self
            .wb_client
            .promotion_stats(
                &wb_account,
                input.campaign_ids.clone(),
                date_from.to_string(),
                date_to.to_string(),
            )
            .await
            .map_err(|error| self.wb_error(&wb_account, endpoint, &error))?;
        let statistics_observed_at = Utc::now();
        let mut report = recommend(RecommendationEvidence {
            version: 1, account_id: wb_account, as_of: statistics_observed_at, date_from, date_to,
            campaign_ids: input.campaign_ids, target_drr_bps: input.target_drr_bps,
            statistics_observed_at, composition_observed_at, statistics, composition, stocks, sales,
        }).map_err(|_| "WB_RECOMMENDATIONS_INVALID_EVIDENCE: данные не прошли проверку; сократите область или проверьте источники".to_owned())?;
        report.truncated = report.recommendations.len() > usize::from(input.limit);
        report.recommendations.truncate(usize::from(input.limit));
        Ok(Json(report))
    }
}
