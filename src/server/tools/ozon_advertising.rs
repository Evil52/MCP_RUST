//! Authorized ozon advertising MCP tools.

use super::super::OzonMcp;
use super::super::{
    CAMPAIGN_OBJECTS_PATH_TEMPLATE, CAMPAIGN_PRODUCTS_PATH_TEMPLATE, CAMPAIGNS_PATH,
    CampaignProductsQuery, CampaignsQuery, DAILY_STATS_PATH, EXPENSES_PATH, Json, LIMITS_PATH,
    MAX_ENUM_VALUE_CHARS, MAX_OPAQUE_TOKEN_CHARS, MAX_PAGE, MAX_PERFORMANCE_PERIOD_DAYS,
    MAX_RATINGS, MIN_REVIEWS_LIMIT, OzonResult, PRODUCT_SKU_STATS_PATH, Parameters,
    PerformanceAdvObjectType, PerformanceCampaignProductsInput, PerformanceCampaignResourceInput,
    PerformanceCampaignState, PerformanceCampaignsInput, PerformanceSkuStatisticsInput,
    PerformanceStatisticsInput, QuestionsInput, RatingHistoryInput, RequestIdentity, ReviewsInput,
    SkuStatisticsQuery, StatisticsQuery, StoreOnlyInput, json, tool, validate_and_expand_dates,
    validate_campaign_ids, validate_count, validate_date_range, validate_limit, validate_max_chars,
    validate_max_u32, validate_non_blank, validate_optional_date_range, validate_ozon_id,
    validate_string_list, validate_unique_ozon_ids,
};
use rmcp::tool_router;

#[tool_router(router = ozon_advertising_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Возвращает настройки и состояния рекламных кампаний Ozon Performance без возможности их изменить.
    #[tool(
        name = "ozon_performance_campaigns",
        annotations(title = "Рекламные кампании Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn performance_campaigns(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PerformanceCampaignsInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_campaign_ids(&input.campaign_ids)?;
        if input.page == 0 {
            return Err("page должен быть не меньше 1".to_owned());
        }
        validate_max_u32("page", input.page, MAX_PAGE)?;
        validate_limit(input.page_size, 100)?;
        let store = self.performance_context(&identity, input.store.as_ref())?;
        let data = self
            .performance_client
            .campaigns(
                &store,
                CampaignsQuery {
                    campaign_ids: input.campaign_ids,
                    adv_object_type: input.adv_object_type.map(PerformanceAdvObjectType::as_str),
                    state: input.state.map(PerformanceCampaignState::as_str),
                    page: input.page,
                    page_size: input.page_size,
                },
            )
            .await
            .map_err(|error| Self::performance_error(&store, CAMPAIGNS_PATH, &error))?;
        Ok(Self::performance_result(store, CAMPAIGNS_PATH, data))
    }

    /// Возвращает текущие минимальные и максимальные ставки Ozon Performance по инструментам и категориям.
    #[tool(
        name = "ozon_performance_limits",
        annotations(title = "Лимиты ставок Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn performance_limits(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<StoreOnlyInput>,
    ) -> Result<Json<OzonResult>, String> {
        let store = self.performance_context(&identity, input.store.as_ref())?;
        let data = self
            .performance_client
            .limits(&store)
            .await
            .map_err(|error| Self::performance_error(&store, LIMITS_PATH, &error))?;
        Ok(Self::performance_result(store, LIMITS_PATH, data))
    }

    /// Возвращает ID объектов, которые продвигает рекламная кампания Ozon.
    #[tool(
        name = "ozon_performance_campaign_objects",
        annotations(title = "Объекты рекламной кампании Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn performance_campaign_objects(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PerformanceCampaignResourceInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_ozon_id("campaign_id", input.campaign_id)?;
        let store = self.performance_context(&identity, input.store.as_ref())?;
        let data = self
            .performance_client
            .campaign_objects(&store, input.campaign_id)
            .await
            .map_err(|error| {
                Self::performance_error(&store, CAMPAIGN_OBJECTS_PATH_TEMPLATE, &error)
            })?;
        Ok(Self::performance_result(
            store,
            CAMPAIGN_OBJECTS_PATH_TEMPLATE,
            data,
        ))
    }

    /// Возвращает товары и ставки в конкретной рекламной кампании Ozon.
    #[tool(
        name = "ozon_performance_campaign_products",
        annotations(title = "Товары рекламной кампании Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn performance_campaign_products(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PerformanceCampaignProductsInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_ozon_id("campaign_id", input.campaign_id)?;
        if input.page == 0 {
            return Err("page должен быть не меньше 1".to_owned());
        }
        validate_max_u32("page", input.page, MAX_PAGE)?;
        validate_limit(input.page_size, 100)?;
        let store = self.performance_context(&identity, input.store.as_ref())?;
        let data = self
            .performance_client
            .campaign_products(
                &store,
                input.campaign_id,
                CampaignProductsQuery {
                    page: u64::from(input.page),
                    page_size: u64::from(input.page_size),
                },
            )
            .await
            .map_err(|error| {
                Self::performance_error(&store, CAMPAIGN_PRODUCTS_PATH_TEMPLATE, &error)
            })?;
        Ok(Self::performance_result(
            store,
            CAMPAIGN_PRODUCTS_PATH_TEMPLATE,
            data,
        ))
    }

    /// Возвращает готовую дневную статистику рекламы: показы, клики, расходы и заказы. Период ограничен 31 днём.
    #[tool(
        name = "ozon_performance_daily",
        annotations(title = "Дневная статистика рекламы Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn performance_daily(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PerformanceStatisticsInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_campaign_ids(&input.campaign_ids)?;
        validate_date_range(
            &input.date_from,
            &input.date_to,
            MAX_PERFORMANCE_PERIOD_DAYS,
        )?;
        let store = self.performance_context(&identity, input.store.as_ref())?;
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
        Ok(Self::performance_result(store, DAILY_STATS_PATH, data))
    }

    /// Возвращает подневную рекламную статистику с разрезом до SKU: показы, клики, корзины, расходы, заказы и выручку.
    #[tool(
        name = "ozon_performance_sku_statistics",
        annotations(title = "Реклама Ozon по SKU", read_only_hint = true)
    )]
    pub(in crate::server) async fn performance_sku_statistics(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PerformanceSkuStatisticsInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_campaign_ids(&input.campaign_ids)?;
        // The vendor documents a relative "not earlier than the previous day"
        // rule for dateFrom but does not publish the timezone. Enforce the
        // deterministic format/order locally and leave that ambiguous relative
        // boundary to Ozon rather than silently choosing a deployment timezone.
        validate_date_range(&input.date_from, &input.date_to, i64::MAX)?;
        let store = self.performance_context(&identity, input.store.as_ref())?;
        let data = self
            .performance_client
            .sku_statistics(
                &store,
                SkuStatisticsQuery {
                    campaign_ids: input.campaign_ids,
                    date_from: input.date_from,
                    date_to: input.date_to,
                },
            )
            .await
            .map_err(|error| Self::performance_error(&store, PRODUCT_SKU_STATS_PATH, &error))?;
        Ok(Self::performance_result(
            store,
            PRODUCT_SKU_STATS_PATH,
            data,
        ))
    }

    /// Возвращает расходы рекламных кампаний Ozon и их разбивку по источникам средств. Период ограничен 31 днём.
    #[tool(
        name = "ozon_performance_expenses",
        annotations(title = "Расходы рекламы Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn performance_expenses(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PerformanceStatisticsInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_campaign_ids(&input.campaign_ids)?;
        validate_date_range(
            &input.date_from,
            &input.date_to,
            MAX_PERFORMANCE_PERIOD_DAYS,
        )?;
        let store = self.performance_context(&identity, input.store.as_ref())?;
        let data = self
            .performance_client
            .expenses(
                &store,
                StatisticsQuery {
                    campaign_ids: input.campaign_ids,
                    date_from: input.date_from,
                    date_to: input.date_to,
                },
            )
            .await
            .map_err(|error| Self::performance_error(&store, EXPENSES_PATH, &error))?;
        Ok(Self::performance_result(store, EXPENSES_PATH, data))
    }

    /// Возвращает текущую сводку рейтингов и показателей качества продавца.
    #[tool(
        name = "ozon_seller_rating",
        annotations(title = "Рейтинг продавца Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn seller_rating(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<StoreOnlyInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.request(&identity, input.store, "/v1/rating/summary", json!({}))
            .await
    }

    /// Возвращает историю выбранных рейтингов продавца за период.
    #[tool(
        name = "ozon_seller_rating_history",
        annotations(title = "История рейтинга Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn seller_rating_history(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<RatingHistoryInput>,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_and_expand_dates(&input.date_from, &input.date_to, 366)?;
        validate_count("ratings", input.ratings.len(), 1, MAX_RATINGS)?;
        validate_string_list("ratings", &input.ratings, MAX_RATINGS, MAX_ENUM_VALUE_CHARS)?;
        self.request(
            &identity,
            input.store,
            "/v1/rating/history",
            json!({
                "date_from": from,
                "date_to": to,
                "ratings": input.ratings,
                "with_premium_scores": input.with_premium_scores,
            }),
        )
        .await
    }

    /// Получает отзывы покупателей для анализа качества товаров; метод Ozon находится в beta.
    #[tool(
        name = "ozon_reviews",
        annotations(title = "Отзывы покупателей Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn reviews(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReviewsInput>,
    ) -> Result<Json<OzonResult>, String> {
        if input.limit < MIN_REVIEWS_LIMIT {
            return Err(format!(
                "limit для отзывов должен быть от {MIN_REVIEWS_LIMIT} до 100"
            ));
        }
        validate_limit(input.limit, 100)?;
        validate_max_chars("last_id", &input.last_id, MAX_OPAQUE_TOKEN_CHARS)?;
        validate_non_blank("status", &input.status)?;
        validate_max_chars("status", &input.status, MAX_ENUM_VALUE_CHARS)?;
        if !matches!(
            input.status.as_str(),
            "ALL" | "NEW" | "VIEWED" | "PROCESSED"
        ) {
            return Err(
                "status для reviews v2 должен быть ALL, NEW, VIEWED или PROCESSED".to_owned(),
            );
        }
        validate_non_blank("order_status", &input.order_status)?;
        validate_max_chars("order_status", &input.order_status, MAX_ENUM_VALUE_CHARS)?;
        if !matches!(
            input.order_status.as_str(),
            "ALL" | "DELIVERED" | "CANCELLED"
        ) {
            return Err(
                "order_status для reviews v2 должен быть ALL, DELIVERED или CANCELLED".to_owned(),
            );
        }
        validate_count("skus", input.skus.len(), 0, 100)?;
        validate_unique_ozon_ids("skus", &input.skus)?;
        let published = validate_optional_date_range(
            "published",
            input.published_from.as_deref(),
            input.published_to.as_deref(),
        )?;
        let mut filters = json!({
            "order_status": input.order_status,
            "skus": input.skus,
            "status": input.status,
        });
        if let Some((from, to)) = published {
            let filters = filters
                .as_object_mut()
                .expect("review filters are an object");
            filters.insert("published_from".to_owned(), json!(from));
            filters.insert("published_to".to_owned(), json!(to));
        }
        self.request(
            &identity,
            input.store,
            "/v2/review/list",
            json!({
                "filters": filters,
                "last_id": input.last_id,
                "limit": input.limit,
                "sort_dir": input.direction,
            }),
        )
        .await
    }

    /// Получает вопросы покупателей за период; метод Ozon находится в beta.
    #[tool(
        name = "ozon_questions",
        annotations(title = "Вопросы покупателей Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn questions(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<QuestionsInput>,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_and_expand_dates(&input.date_from, &input.date_to, 366)?;
        validate_non_blank("status", &input.status)?;
        validate_max_chars("status", &input.status, MAX_ENUM_VALUE_CHARS)?;
        validate_max_chars("last_id", &input.last_id, MAX_OPAQUE_TOKEN_CHARS)?;
        self.request(
            &identity,
            input.store,
            "/v1/question/list",
            json!({
                "filter": { "date_from": from, "date_to": to, "status": input.status },
                "last_id": input.last_id,
            }),
        )
        .await
    }
}
