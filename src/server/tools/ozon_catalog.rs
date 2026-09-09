//! Authorized ozon catalog MCP tools.

use super::super::OzonMcp;
use super::super::{
    AnalyticsInput, BTreeSet, Json, MAX_ANALYTICS_PERIOD_DAYS, MAX_IDENTIFIER_CHARS, MAX_OFFSET,
    MAX_OPAQUE_TOKEN_CHARS, MAX_PRODUCT_DIAGNOSTIC_ITEMS, MAX_PRODUCT_FILTER_ITEMS, MAX_SKUS,
    MAX_SUPPLY_ORDER_IDS, OzonLivePricesResult, OzonProductContentDiagnosticsResult, OzonResult,
    Parameters, ProductAttributesInput, ProductCatalogInput, ProductContentDiagnosticsInput,
    ProductFilterInput, ProductInfoListInput, ProductPicturesInfoInput, ProductPriceFilterInput,
    RequestIdentity, SupplyOrderGetInput, SupplyOrderListInput, TurnoverInput,
    UNTRUSTED_DATA_CLASSIFICATION, Utc, Value, WarehouseListInput, WarehouseStockListInput,
    WarehouseStocksInput, build_supply_order_filter, diagnostic_text, json, normalize_live_prices,
    normalize_product_content_diagnostics, response_array, tool, validate_count,
    validate_date_range, validate_limit, validate_max_chars, validate_max_u32, validate_ozon_id,
    validate_product_identifiers, validate_string_list, validate_supply_order_list_input,
    validate_unique_ozon_ids,
};
use rmcp::tool_router;

#[tool_router(router = ozon_catalog_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Выполняет редкий административный live-запрос Ozon Analytics.
    /// Для штатных менеджерских запросов используйте `ofk_ozon_sales_analytics` из PostgreSQL-снимков.
    /// После 429 администратор не должен повторять этот инструмент до окончания `local-cooldown`.
    /// Если пользователь явно разрешил резервное распределение по отправлениям,
    /// используйте `ozon_posting_sales_fallback`; его операционная метрика не
    /// эквивалентна Seller Analytics `ordered_units` и не содержит GMV.
    #[tool(
        name = "ozon_analytics",
        annotations(title = "Аналитика продаж Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn analytics(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<AnalyticsInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_date_range(&input.date_from, &input.date_to, MAX_ANALYTICS_PERIOD_DAYS)?;
        validate_count("metrics", input.metrics.len(), 1, 14)?;
        validate_count("dimensions", input.dimensions.len(), 1, 2)?;
        validate_limit(input.limit, 1_000)?;
        validate_max_u32("offset", input.offset, MAX_OFFSET)?;
        let (_, actor) = self.access_context(&identity)?;
        Self::authorize_live_analytics_for_role(actor.role)?;

        let mut sort = Vec::new();
        if let Some(metric) = input.sort_by {
            sort.push(json!({ "key": metric, "order": input.sort_direction }));
        }
        self.request(
            &identity,
            input.store,
            "/v1/analytics/data",
            json!({
                "date_from": input.date_from,
                "date_to": input.date_to,
                "metrics": input.metrics,
                "dimension": input.dimensions,
                "filters": [],
                "sort": sort,
                "limit": input.limit,
                "offset": input.offset,
            }),
        )
        .await
    }

    /// Возвращает текущие остатки товаров Ozon с фильтрацией по `offer_id` или `product_id`.
    #[tool(
        name = "ozon_product_stocks",
        annotations(title = "Остатки товаров Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn product_stocks(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ProductFilterInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.product_list(&identity, input, "/v4/product/info/stocks")
            .await
    }

    /// Возвращает постраничные остатки товаров на конкретном складе FBS или rFBS.
    #[tool(
        name = "ozon_warehouse_stocks",
        annotations(title = "Остатки на складе FBS/rFBS Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn warehouse_stocks(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WarehouseStocksInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_ozon_id("warehouse_id", input.warehouse_id)?;
        validate_limit(input.limit, 1_000)?;
        if let Some(cursor) = input.cursor.as_deref() {
            validate_max_chars("cursor", cursor, MAX_OPAQUE_TOKEN_CHARS)?;
        }
        self.request(
            &identity,
            input.store,
            "/v1/product/info/warehouse/stocks",
            json!({
                "cursor": input.cursor.unwrap_or_default(),
                "limit": input.limit,
                "warehouse_id": input.warehouse_id,
            }),
        )
        .await
    }

    /// Возвращает остатки товаров по каждому складу FBO.
    #[tool(
        name = "ozon_fbo_stocks_by_warehouse",
        annotations(title = "Остатки FBO по складам Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn fbo_stocks_by_warehouse(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WarehouseStockListInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_product_identifiers(&input.offer_ids, &[], &input.skus)?;
        validate_limit(input.limit, 1_000)?;
        validate_max_chars("cursor", &input.cursor, MAX_OPAQUE_TOKEN_CHARS)?;
        self.request(
            &identity,
            input.store,
            "/v1/product/info/stocks-by-warehouse/fbo",
            json!({
                "cursor": input.cursor,
                "limit": input.limit,
                "offer_ids": input.offer_ids,
                "skus": input.skus,
            }),
        )
        .await
    }

    /// Возвращает остатки товаров по каждому складу FBS/rFBS.
    #[tool(
        name = "ozon_fbs_stocks_by_warehouse",
        annotations(title = "Остатки FBS по складам Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn fbs_stocks_by_warehouse(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WarehouseStockListInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_product_identifiers(&input.offer_ids, &[], &input.skus)?;
        validate_limit(input.limit, 1_000)?;
        validate_max_chars("cursor", &input.cursor, MAX_OPAQUE_TOKEN_CHARS)?;
        self.request(
            &identity,
            input.store,
            "/v2/product/info/stocks-by-warehouse/fbs",
            json!({
                "cursor": input.cursor,
                "limit": input.limit,
                "offer_id": input.offer_ids,
                "sku": input.skus,
            }),
        )
        .await
    }

    /// Возвращает список складов продавца и их параметры.
    #[tool(
        name = "ozon_warehouses",
        annotations(title = "Склады продавца Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn warehouses(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WarehouseListInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_limit(input.limit, 1_000)?;
        if let Some(cursor) = input.cursor.as_deref() {
            validate_max_chars("cursor", cursor, MAX_OPAQUE_TOKEN_CHARS)?;
        }
        validate_count(
            "warehouse_ids",
            input.warehouse_ids.len(),
            0,
            MAX_PRODUCT_FILTER_ITEMS,
        )?;
        validate_unique_ozon_ids("warehouse_ids", &input.warehouse_ids)?;
        let mut payload = json!({ "limit": input.limit });
        let fields = payload
            .as_object_mut()
            .expect("warehouse list payload is an object");
        if let Some(cursor) = input.cursor.filter(|cursor| !cursor.is_empty()) {
            fields.insert("cursor".to_owned(), json!(cursor));
        }
        if !input.warehouse_ids.is_empty() {
            fields.insert("warehouse_ids".to_owned(), json!(input.warehouse_ids));
        }
        self.request(&identity, input.store, "/v2/warehouse/list", payload)
            .await
    }

    /// Возвращает текущие цены и скидки товаров Ozon без возможности их изменить.
    #[tool(
        name = "ozon_product_prices",
        annotations(title = "Цены товаров Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn product_prices(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ProductPriceFilterInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_string_list(
            "offer_ids",
            &input.offer_ids,
            MAX_PRODUCT_FILTER_ITEMS,
            MAX_IDENTIFIER_CHARS,
        )?;
        validate_string_list(
            "product_ids",
            &input.product_ids,
            MAX_PRODUCT_FILTER_ITEMS,
            MAX_IDENTIFIER_CHARS,
        )?;
        if input.offer_ids.len() + input.product_ids.len() > MAX_PRODUCT_FILTER_ITEMS {
            return Err(format!(
                "offer_ids и product_ids вместе должны содержать не более {MAX_PRODUCT_FILTER_ITEMS} значений"
            ));
        }
        if let Some(cursor) = input.cursor.as_deref() {
            validate_max_chars("cursor", cursor, MAX_OPAQUE_TOKEN_CHARS)?;
        }
        validate_limit(input.limit, 1_000)?;
        self.request(
            &identity,
            input.store,
            "/v5/product/info/prices",
            json!({
                "cursor": input.cursor.unwrap_or_default(),
                "filter": {
                    "offer_id": input.offer_ids,
                    "product_id": input.product_ids,
                    "visibility": input.visibility,
                },
                "limit": input.limit,
            }),
        )
        .await
    }

    /// Возвращает нормализованный снимок текущих цен Ozon для расчёта по
    /// шаблону O − U. Точная цена с СПП остаётся `null`, если официальный API
    /// не вернул явное поле `price.marketing_price`; цена акции не используется
    /// как подмена.
    #[tool(
        name = "ozon_live_buyer_prices",
        annotations(
            title = "Живые цены Ozon и доступность цены с СПП",
            read_only_hint = true
        )
    )]
    pub(in crate::server) async fn live_buyer_prices(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ProductPriceFilterInput>,
    ) -> Result<Json<OzonLivePricesResult>, String> {
        let Json(result) = self.product_prices(identity, Parameters(input)).await?;
        normalize_live_prices(result).map(Json)
    }

    /// Возвращает постраничный каталог товаров Ozon с их видимостью.
    #[tool(
        name = "ozon_products",
        annotations(title = "Каталог товаров Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn products(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ProductCatalogInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_product_identifiers(&input.offer_ids, &input.product_ids, &input.skus)?;
        validate_limit(input.limit, 1_000)?;
        validate_max_chars("last_id", &input.last_id, MAX_OPAQUE_TOKEN_CHARS)?;
        self.request(
            &identity,
            input.store,
            "/v3/product/list",
            json!({
                "filter": {
                    "offer_id": input.offer_ids,
                    "product_id": input.product_ids,
                    "skus": input.skus,
                    "visibility": input.visibility,
                },
                "last_id": input.last_id,
                "limit": input.limit,
            }),
        )
        .await
    }

    /// Вызывает read-only Ozon `POST /v3/product/info/list` и возвращает
    /// карточки с `images`, `primary_image`, `errors`, `statuses` и
    /// `visibility_details`. Для компактной проверки проблем контента и фото
    /// используйте `ozon_product_content_diagnostics`.
    #[tool(
        name = "ozon_product_info",
        annotations(title = "Карточки, фото и ошибки товаров Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn product_info(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ProductInfoListInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_product_identifiers(&input.offer_ids, &input.product_ids, &input.skus)?;
        if input.offer_ids.is_empty() && input.product_ids.is_empty() && input.skus.is_empty() {
            return Err(
                "offer_ids, product_ids или skus должен содержать хотя бы один идентификатор"
                    .to_owned(),
            );
        }
        self.request(
            &identity,
            input.store,
            "/v3/product/info/list",
            json!({
                "offer_id": input.offer_ids,
                "product_id": input.product_ids,
                "sku": input.skus,
            }),
        )
        .await
    }

    /// Проверяет статус загрузки изображений через read-only Ozon
    /// `POST /v2/product/pictures/info`. Возвращает upstream поля
    /// `primary_photo`, `photo`, `photo_360`, `color_photo` и `errors`.
    #[tool(
        name = "ozon_product_pictures_info",
        annotations(title = "Статусы загрузки изображений Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn product_pictures_info(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ProductPicturesInfoInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_string_list(
            "product_ids",
            &input.product_ids,
            MAX_PRODUCT_FILTER_ITEMS,
            MAX_IDENTIFIER_CHARS,
        )?;
        if input.product_ids.is_empty() {
            return Err("product_ids должен содержать хотя бы один идентификатор".to_owned());
        }
        let unique = input.product_ids.iter().collect::<BTreeSet<_>>();
        if unique.len() != input.product_ids.len() {
            return Err("product_ids не должен содержать повторяющиеся ID".to_owned());
        }
        self.request(
            &identity,
            input.store,
            "/v2/product/pictures/info",
            json!({"product_id": input.product_ids}),
        )
        .await
    }

    /// Диагностирует карточки Ozon одной bounded read-only цепочкой:
    /// `product/list` → `product/info/list` → `product/pictures/info`.
    /// По умолчанию выбирает `STATE_FAILED`; возвращает только идентификаторы,
    /// статусы, счётчики фото и безопасные тексты ошибок без URL изображений.
    #[tool(
        name = "ozon_product_content_diagnostics",
        annotations(title = "Диагностика контента и фото Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn product_content_diagnostics(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ProductContentDiagnosticsInput>,
    ) -> Result<Json<OzonProductContentDiagnosticsResult>, String> {
        validate_product_identifiers(&input.offer_ids, &input.product_ids, &input.skus)?;
        let selected = input.offer_ids.len() + input.product_ids.len() + input.skus.len();
        if selected > MAX_PRODUCT_DIAGNOSTIC_ITEMS {
            return Err(format!(
                "offer_ids, product_ids и skus вместе должны содержать не более {MAX_PRODUCT_DIAGNOSTIC_ITEMS} значений"
            ));
        }
        validate_limit(
            input.limit,
            u32::try_from(MAX_PRODUCT_DIAGNOSTIC_ITEMS)
                .expect("product diagnostic item limit fits u32"),
        )?;
        validate_max_chars("last_id", &input.last_id, MAX_OPAQUE_TOKEN_CHARS)?;

        let catalog = self
            .request(
                &identity,
                input.store,
                "/v3/product/list",
                json!({
                    "filter": {
                        "offer_id": input.offer_ids,
                        "product_id": input.product_ids,
                        "skus": input.skus,
                        "visibility": input.visibility,
                    },
                    "last_id": input.last_id,
                    "limit": input.limit,
                }),
            )
            .await?
            .0;
        let next_last_id = catalog
            .data
            .pointer("/result/last_id")
            .and_then(|value| diagnostic_text(Some(value)))
            .filter(|value| !value.is_empty());
        let product_ids = response_array(&catalog.data, &["/result/items", "/items"])
            .iter()
            .filter_map(Value::as_object)
            .filter_map(|item| diagnostic_text(item.get("product_id")))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let store = catalog.store.clone();

        if product_ids.is_empty() {
            return Ok(Json(OzonProductContentDiagnosticsResult {
                store,
                endpoints: [
                    "/v3/product/list",
                    "/v3/product/info/list",
                    "/v2/product/pictures/info",
                ],
                fetched_at: Utc::now().to_rfc3339(),
                data_classification: UNTRUSTED_DATA_CLASSIFICATION,
                next_last_id,
                items: Vec::new(),
            }));
        }

        let product_info = self
            .request(
                &identity,
                Some(store.clone()),
                "/v3/product/info/list",
                json!({"offer_id": [], "product_id": product_ids.clone(), "sku": []}),
            )
            .await?
            .0;
        let pictures_info = self
            .request(
                &identity,
                Some(store.clone()),
                "/v2/product/pictures/info",
                json!({"product_id": product_ids}),
            )
            .await?
            .0;
        let items = normalize_product_content_diagnostics(
            &catalog.data,
            &product_info.data,
            &pictures_info.data,
        )?;

        Ok(Json(OzonProductContentDiagnosticsResult {
            store,
            endpoints: [
                "/v3/product/list",
                "/v3/product/info/list",
                "/v2/product/pictures/info",
            ],
            fetched_at: Utc::now().to_rfc3339(),
            data_classification: UNTRUSTED_DATA_CLASSIFICATION,
            next_last_id,
            items,
        }))
    }

    /// Возвращает описания и характеристики товаров Ozon.
    #[tool(
        name = "ozon_product_attributes",
        annotations(title = "Характеристики товаров Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn product_attributes(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ProductAttributesInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_product_identifiers(&input.offer_ids, &input.product_ids, &input.skus)?;
        validate_limit(input.limit, 1_000)?;
        validate_max_chars("last_id", &input.last_id, MAX_OPAQUE_TOKEN_CHARS)?;
        self.request(
            &identity,
            input.store,
            "/v4/product/info/attributes",
            json!({
                "filter": {
                    "offer_id": input.offer_ids,
                    "product_id": input.product_ids,
                    "sku": input.skus,
                    "visibility": input.visibility,
                },
                "last_id": input.last_id,
                "limit": input.limit,
                "sort_by": "id",
                "sort_dir": input.sort_direction,
            }),
        )
        .await
    }

    /// Получает показатели оборачиваемости и запасов по SKU.
    #[tool(
        name = "ozon_stock_turnover",
        annotations(title = "Оборачиваемость запасов Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn stock_turnover(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<TurnoverInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_string_list("skus", &input.skus, MAX_SKUS, MAX_IDENTIFIER_CHARS)?;
        validate_limit(input.limit, 1_000)?;
        validate_max_u32("offset", input.offset, MAX_OFFSET)?;
        self.request(
            &identity,
            input.store,
            "/v1/analytics/turnover/stocks",
            json!({ "limit": input.limit, "offset": input.offset, "sku": input.skus }),
        )
        .await
    }

    /// Возвращает список идентификаторов заявок на поставку FBO по фильтрам.
    #[tool(
        name = "ozon_supply_order_list",
        annotations(title = "Список заявок на поставку Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn supply_order_list(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<SupplyOrderListInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_supply_order_list_input(&input)?;
        let filter = build_supply_order_filter(&input);

        self.request(
            &identity,
            input.store,
            "/v3/supply-order/list",
            json!({
                "filter": filter,
                "last_id": input.last_id.unwrap_or_default(),
                "limit": input.limit,
                "sort_by": input.sort_by,
                "sort_dir": input.sort_dir,
            }),
        )
        .await
    }

    /// Возвращает подробную информацию по идентификаторам заявок на поставку FBO.
    #[tool(
        name = "ozon_supply_order_get",
        annotations(title = "Заявки на поставку Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn supply_order_get(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<SupplyOrderGetInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_count("order_ids", input.order_ids.len(), 1, MAX_SUPPLY_ORDER_IDS)?;
        validate_unique_ozon_ids("order_ids", &input.order_ids)?;
        self.request(
            &identity,
            input.store,
            "/v3/supply-order/get",
            json!({ "order_ids": input.order_ids }),
        )
        .await
    }
}
