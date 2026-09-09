//! Authorized orders MCP tools.

use super::super::OzonMcp;
use super::super::{
    FBO_POSTINGS_PATH, FBS_POSTINGS_PATH, FbsUnfulfilledInput, Json, MAX_ANALYTICS_PERIOD_DAYS,
    MAX_ENUM_VALUE_CHARS, MAX_GROUP_STATES, MAX_IDENTIFIER_CHARS, MAX_OPAQUE_TOKEN_CHARS,
    MAX_POSTING_NUMBERS, MAX_PRODUCT_FILTER_ITEMS, OzonPostingSalesFallbackResult, OzonResult,
    Parameters, PostingGetInput, PostingKind, PostingListInput, PostingSalesAccumulator,
    PostingSalesFallbackInput, PostingScheme, RequestIdentity, ReturnsInput, RfbsReturnsInput,
    StoreOnlyInput, UNTRUSTED_DATA_CLASSIFICATION, Utc, json, tool, validate_and_expand_dates,
    validate_count, validate_limit, validate_max_chars, validate_non_blank,
    validate_optional_date_range, validate_string_list, validate_unique_ozon_ids,
};
use rmcp::tool_router;

#[tool_router(router = orders_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Получает список отправлений FBS/rFBS за период и их текущие статусы.
    #[tool(
        name = "ozon_fbs_postings",
        annotations(title = "Отправления FBS Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn fbs_postings(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PostingListInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.posting_list(&identity, input, PostingKind::Fbs).await
    }

    /// Получает список отправлений FBO за период с аналитическими полями; финансовые поля не запрашиваются.
    #[tool(
        name = "ozon_fbo_postings",
        annotations(title = "Отправления FBO Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn fbo_postings(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PostingListInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.posting_list(&identity, input, PostingKind::Fbo).await
    }

    /// Агрегирует количества товаров из всех FBO/FBS-отправлений за период.
    /// Это явный резерв для распределения запасов, а не замена Seller Analytics:
    /// GMV не вычисляется, отменённые единицы возвращаются отдельно.
    #[tool(
        name = "ozon_posting_sales_fallback",
        annotations(
            title = "Резерв продаж Ozon по отправлениям FBO/FBS",
            read_only_hint = true
        )
    )]
    pub(in crate::server) async fn posting_sales_fallback(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PostingSalesFallbackInput>,
    ) -> Result<Json<OzonPostingSalesFallbackResult>, String> {
        let (from, to) =
            validate_and_expand_dates(&input.date_from, &input.date_to, MAX_ANALYTICS_PERIOD_DAYS)?;
        let store = self.posting_sales_context(&identity, input.store.as_ref())?;
        let mut aggregate = PostingSalesAccumulator::default();
        for scheme in [PostingScheme::Fbo, PostingScheme::Fbs] {
            self.collect_posting_sales_scheme(&store, &from, &to, scheme, &mut aggregate)
                .await?;
        }
        let (totals, rows) = aggregate.finish().map_err(|error| {
            let kind = error.code();
            format!(
                "OZON_POSTING_SALES_FALLBACK_FAILED: kind={kind}; store={store}. Итоговая агрегация не прошла fail-closed проверку; частичные данные не возвращены."
            )
        })?;
        Ok(Json(OzonPostingSalesFallbackResult {
            store,
            date_from: input.date_from,
            date_to: input.date_to,
            fetched_at: Utc::now().to_rfc3339(),
            data_classification: UNTRUSTED_DATA_CLASSIFICATION,
            metric: "non_cancelled_posting_units",
            metric_definition: "Количество SKU в FBO/FBS-отправлениях выбранного периода, текущий статус которых не равен cancelled. Это резервная операционная метрика, не Seller Analytics ordered_units и не продажи по факту доставки.",
            gmv_available: false,
            pagination_complete: true,
            source_endpoints: [FBO_POSTINGS_PATH, FBS_POSTINGS_PATH],
            totals,
            rows,
        }))
    }

    /// Возвращает необработанные FBS/rFBS-отправления по актуальному cursor-контракту v4.
    #[tool(
        name = "ozon_fbs_unfulfilled",
        annotations(title = "Неообработанные FBS-отправления Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn fbs_unfulfilled(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FbsUnfulfilledInput>,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_and_expand_dates(&input.date_from, &input.date_to, 366)?;
        let cutoff = validate_optional_date_range(
            "cutoff",
            input.cutoff_from.as_deref(),
            input.cutoff_to.as_deref(),
        )?;
        let delivering = validate_optional_date_range(
            "delivering_date",
            input.delivering_date_from.as_deref(),
            input.delivering_date_to.as_deref(),
        )?;
        if cutoff.is_some() && delivering.is_some() {
            return Err(
                "cutoff и delivering_date нельзя передавать одновременно в FBS unfulfilled"
                    .to_owned(),
            );
        }
        validate_max_chars("cursor", &input.cursor, MAX_OPAQUE_TOKEN_CHARS)?;
        validate_limit(input.limit, 1_000)?;
        validate_string_list(
            "statuses",
            &input.statuses,
            MAX_GROUP_STATES,
            MAX_ENUM_VALUE_CHARS,
        )?;
        for (field, ids) in [
            ("warehouse_ids", input.warehouse_ids.as_slice()),
            ("provider_ids", input.provider_ids.as_slice()),
            ("delivery_method_ids", input.delivery_method_ids.as_slice()),
        ] {
            validate_count(field, ids.len(), 0, MAX_PRODUCT_FILTER_ITEMS)?;
            validate_unique_ozon_ids(field, ids)?;
        }
        let mut filter = json!({
            "delivery_method_ids": input.delivery_method_ids,
            "last_changed_status_date": { "from": from, "to": to },
            "provider_ids": input.provider_ids,
            "statuses": input.statuses,
            "warehouse_ids": input.warehouse_ids,
        });
        if let Some((from, to)) = cutoff {
            let filter = filter
                .as_object_mut()
                .expect("FBS unfulfilled filter is an object");
            filter.insert("cutoff_from".to_owned(), json!(from));
            filter.insert("cutoff_to".to_owned(), json!(to));
        }
        if let Some((from, to)) = delivering {
            let filter = filter
                .as_object_mut()
                .expect("FBS unfulfilled filter is an object");
            filter.insert("delivering_date_from".to_owned(), json!(from));
            filter.insert("delivering_date_to".to_owned(), json!(to));
        }
        self.request(
            &identity,
            input.store,
            "/v4/posting/fbs/unfulfilled/list",
            json!({
                "cursor": input.cursor,
                "filter": filter,
                "limit": input.limit,
                "sort_dir": input.direction,
                "translit": false,
                "with": {
                    "analytics_data": true,
                    "barcodes": true,
                    "financial_data": false,
                    "legal_info": false,
                },
            }),
        )
        .await
    }

    /// Возвращает одно FBO-отправление с товарами и аналитическими полями.
    #[tool(
        name = "ozon_fbo_posting",
        annotations(title = "FBO-отправление Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn fbo_posting(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PostingGetInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_non_blank("posting_number", &input.posting_number)?;
        validate_max_chars(
            "posting_number",
            &input.posting_number,
            MAX_IDENTIFIER_CHARS,
        )?;
        self.request(
            &identity,
            input.store,
            "/v2/posting/fbo/get",
            json!({
                "posting_number": input.posting_number,
                "translit": false,
                "with": {
                    "analytics_data": true,
                    "financial_data": false,
                    "legal_info": false,
                },
            }),
        )
        .await
    }

    /// Возвращает одно FBS/rFBS-отправление с товарами, штрихкодами и связанными отправлениями.
    #[tool(
        name = "ozon_fbs_posting",
        annotations(title = "FBS-отправление Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn fbs_posting(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PostingGetInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_non_blank("posting_number", &input.posting_number)?;
        validate_max_chars(
            "posting_number",
            &input.posting_number,
            MAX_IDENTIFIER_CHARS,
        )?;
        self.request(
            &identity,
            input.store,
            "/v3/posting/fbs/get",
            json!({
                "posting_number": input.posting_number,
                "with": {
                    "analytics_data": true,
                    "barcodes": true,
                    "financial_data": false,
                    "legal_info": false,
                    "product_exemplars": true,
                    "related_postings": true,
                    "translit": false,
                },
            }),
        )
        .await
    }

    /// Возвращает актуальный справочник причин отмены FBO.
    #[tool(
        name = "ozon_fbo_cancel_reasons",
        annotations(title = "Причины отмены FBO Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn fbo_cancel_reasons(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<StoreOnlyInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.request(
            &identity,
            input.store,
            "/v1/posting/fbo/cancel-reason/list",
            json!({}),
        )
        .await
    }

    /// Возвращает актуальный справочник причин отмены FBS/rFBS.
    #[tool(
        name = "ozon_fbs_cancel_reasons",
        annotations(title = "Причины отмены FBS Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn fbs_cancel_reasons(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<StoreOnlyInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.request(
            &identity,
            input.store,
            "/v2/posting/fbs/cancel-reason/list",
            json!({}),
        )
        .await
    }

    /// Получает возвраты FBO/FBS за период изменения статуса.
    #[tool(
        name = "ozon_returns",
        annotations(title = "Возвраты Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn returns(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReturnsInput>,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_and_expand_dates(&input.date_from, &input.date_to, 366)?;
        validate_max_chars("offer_id", &input.offer_id, MAX_IDENTIFIER_CHARS)?;
        validate_string_list(
            "posting_numbers",
            &input.posting_numbers,
            MAX_POSTING_NUMBERS,
            MAX_IDENTIFIER_CHARS,
        )?;
        validate_limit(input.limit, 500)?;
        let schema = input.return_schema.as_ozon_str();
        self.request(
            &identity,
            input.store,
            "/v1/returns/list",
            json!({
                "filter": {
                    "visual_status_change_moment": { "time_from": from, "time_to": to },
                    "posting_numbers": input.posting_numbers,
                    "offer_id": input.offer_id,
                    "return_schema": schema,
                },
                "limit": input.limit,
                "last_id": input.last_id,
            }),
        )
        .await
    }

    /// Получает возвраты rFBS за период создания через отдельный read-only метод Ozon.
    #[tool(
        name = "ozon_rfbs_returns",
        annotations(title = "Возвраты rFBS Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn rfbs_returns(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<RfbsReturnsInput>,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_and_expand_dates(&input.date_from, &input.date_to, 366)?;
        validate_max_chars("offer_id", &input.offer_id, MAX_IDENTIFIER_CHARS)?;
        validate_max_chars(
            "posting_number",
            &input.posting_number,
            MAX_IDENTIFIER_CHARS,
        )?;
        validate_string_list(
            "group_state",
            &input.group_state,
            MAX_GROUP_STATES,
            MAX_ENUM_VALUE_CHARS,
        )?;
        validate_limit(input.limit, 100)?;
        self.request(
            &identity,
            input.store,
            "/v2/returns/rfbs/list",
            json!({
                "filter": {
                    "offer_id": input.offer_id,
                    "posting_number": input.posting_number,
                    "group_state": input.group_state,
                    "created_at": { "from": from, "to": to },
                },
                "last_id": input.last_id,
                "limit": input.limit,
            }),
        )
        .await
    }
}
