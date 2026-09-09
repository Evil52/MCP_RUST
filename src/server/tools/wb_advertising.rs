//! Authorized wb advertising MCP tools.

use super::super::OzonMcp;
use super::super::{
    Json, MAX_WB_PROMOTION_CAMPAIGNS, MAX_WB_SIGNED_API_ID, Parameters, RequestIdentity,
    WbAccountInput, WbPromotionCampaignDetailsInput, WbPromotionMinimumBidsInput,
    WbPromotionRecommendedBidsInput, WbPromotionSearchClusterBidsInput, WbPromotionStatsInput,
    WbResult, WbSearchOrdersPositionsInput, WbSearchProductQueriesInput, tool, validate_count,
    validate_unique_positive_ids, validate_wb_promotion_date_range,
    validate_wb_promotion_minimum_bids_input, validate_wb_promotion_search_cluster_pairs,
    validate_wb_promotion_statuses, validate_wb_search_orders_positions_input,
    validate_wb_search_product_queries_input,
};
use rmcp::tool_router;

#[tool_router(router = wb_advertising_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Возвращает read-only сводку рекламных кампаний Wildberries и их ID, не изменяя кампании.
    #[tool(
        name = "wb_promotion_campaigns",
        annotations(
            title = "Рекламные кампании Wildberries",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub(in crate::server) async fn wb_promotion_campaigns(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbAccountInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "promotion:/adv/v1/promotion/count";
        let data = self
            .wb_client
            .promotion_campaigns(&account)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Возвращает read-only настройки выбранных рекламных кампаний Wildberries. Требует явный ограниченный список ID.
    #[tool(
        name = "wb_promotion_campaign_details",
        annotations(
            title = "Настройки рекламных кампаний Wildberries",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub(in crate::server) async fn wb_promotion_campaign_details(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbPromotionCampaignDetailsInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_count(
            "campaign_ids",
            input.campaign_ids.len(),
            1,
            MAX_WB_PROMOTION_CAMPAIGNS,
        )?;
        validate_unique_positive_ids("campaign_ids", &input.campaign_ids)?;
        if let Some(statuses) = input.statuses.as_deref() {
            validate_wb_promotion_statuses(statuses)?;
        }
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "promotion:/api/advert/v2/adverts";
        let data = self
            .wb_client
            .promotion_campaign_details(
                &account,
                input.campaign_ids,
                input.statuses.unwrap_or_default(),
                input
                    .payment_type
                    .map(|payment_type| payment_type.as_str().to_owned()),
            )
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Возвращает read-only статистику кампаний Wildberries в статусах 7, 9 и 11 за период не более 31 дня.
    #[tool(
        name = "wb_promotion_stats",
        annotations(
            title = "Статистика рекламы Wildberries",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    pub(in crate::server) async fn wb_promotion_stats(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbPromotionStatsInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_count(
            "campaign_ids",
            input.campaign_ids.len(),
            1,
            MAX_WB_PROMOTION_CAMPAIGNS,
        )?;
        validate_unique_positive_ids("campaign_ids", &input.campaign_ids)?;
        validate_wb_promotion_date_range(&input.begin_date, &input.end_date)?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "promotion:/adv/v3/fullstats";
        let data = self
            .wb_client
            .promotion_stats(
                &account,
                input.campaign_ids,
                input.begin_date,
                input.end_date,
            )
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Возвращает официальный Search Report WB с топом запросов, средней и медианной позицией: агрегат выбранного периода до 31 дня, обновляемый примерно раз в час. Требует подписку «Джем»; не содержит региона или organic/ad split и не является live-снимком выдачи.
    #[tool(
        name = "wb_search_product_queries",
        annotations(
            title = "Поисковые запросы товаров Wildberries",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    pub(in crate::server) async fn wb_search_product_queries(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbSearchProductQueriesInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_wb_search_product_queries_input(&input)?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "analytics:/api/v2/search-report/product/search-texts";
        let data = self
            .wb_client
            .search_product_queries(
                &account,
                input.date_from,
                input.date_to,
                None,
                input.nm_ids,
                input.top_order_by.as_str().to_owned(),
                input.limit,
            )
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Возвращает официальный Search Report WB с дневными строками заказов и средней позиции за период до 7 дней. Отчёт обновляется примерно раз в час и требует подписку «Джем»; не содержит региона или organic/ad split и не является live-снимком выдачи.
    #[tool(
        name = "wb_search_orders_positions",
        annotations(
            title = "Заказы и позиции по запросам Wildberries",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    pub(in crate::server) async fn wb_search_orders_positions(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbSearchOrdersPositionsInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_wb_search_orders_positions_input(&input)?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "analytics:/api/v2/search-report/product/orders";
        let data = self
            .wb_client
            .search_orders_positions(
                &account,
                input.date_from,
                input.date_to,
                input.nm_id,
                input.search_texts,
            )
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Возвращает минимальные read-only ставки WB в копейках для выбранной кампании, товаров и мест размещения.
    #[tool(
        name = "wb_promotion_minimum_bids",
        annotations(
            title = "Минимальные ставки Wildberries",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    pub(in crate::server) async fn wb_promotion_minimum_bids(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbPromotionMinimumBidsInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_wb_promotion_minimum_bids_input(&input)?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "promotion:/api/advert/v1/bids/min";
        let data = self
            .wb_client
            .promotion_minimum_bids(
                &account,
                input.campaign_id,
                input.nm_ids,
                input.payment_type.as_str().to_owned(),
                input
                    .placement_types
                    .into_iter()
                    .map(|placement| placement.as_str().to_owned())
                    .collect(),
            )
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Возвращает read-only рекомендуемые ставки WB для одного товара в CPM-кампании и её поисковых кластеров.
    #[tool(
        name = "wb_promotion_recommended_bids",
        annotations(
            title = "Рекомендуемые ставки Wildberries",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    pub(in crate::server) async fn wb_promotion_recommended_bids(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbPromotionRecommendedBidsInput>,
    ) -> Result<Json<WbResult>, String> {
        if !(1..=MAX_WB_SIGNED_API_ID).contains(&input.campaign_id) {
            return Err(format!(
                "campaign_id должен быть от 1 до {MAX_WB_SIGNED_API_ID}"
            ));
        }
        if !(1..=MAX_WB_SIGNED_API_ID).contains(&input.nm_id) {
            return Err(format!("nm_id должен быть от 1 до {MAX_WB_SIGNED_API_ID}"));
        }
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "promotion:/api/advert/v0/bids/recommendations";
        let data = self
            .wb_client
            .promotion_recommended_bids(&account, input.campaign_id, input.nm_id)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Возвращает текущие read-only ставки поисковых кластеров WB для ограниченного списка пар «кампания + товар».
    #[tool(
        name = "wb_promotion_search_cluster_bids",
        annotations(
            title = "Ставки поисковых кластеров Wildberries",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    pub(in crate::server) async fn wb_promotion_search_cluster_bids(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbPromotionSearchClusterBidsInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_wb_promotion_search_cluster_pairs(&input.items)?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "promotion:/adv/v0/normquery/get-bids";
        let items = input
            .items
            .into_iter()
            .map(|item| (item.campaign_id, item.nm_id))
            .collect();
        let data = self
            .wb_client
            .promotion_search_cluster_bids(&account, items)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
}
