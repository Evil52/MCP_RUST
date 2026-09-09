//! Authorized wb catalog MCP tools.

use super::super::OzonMcp;
use super::super::{
    Json, MAX_ENUM_VALUE_CHARS, MAX_OFFSET, MAX_PRODUCT_FILTER_ITEMS, Parameters, RequestIdentity,
    WbAcceptanceCoefficientsInput, WbAccountInput, WbProductCardsInput, WbProductPricesInput,
    WbResult, WbSalesFunnelGroupedHistoryInput, WbSalesFunnelHistoryInput, WbSalesFunnelInput,
    WbSellerWarehouseStocksInput, WbSellerWarehouseStocksResult, WbStatisticsReportInput,
    WbTariffCommissionsInput, WbTariffDateInput, WbWarehouseStocksInput, json, parse_date, tool,
    validate_count, validate_date_range, validate_flag, validate_limit, validate_max_u32,
    validate_positive_ids, validate_string_list, validate_unique_positive_ids,
    validate_unique_wb_signed_ids, validate_wb_change_date, wb_missing_stock_ids,
    wb_product_cards_payload,
};
use rmcp::tool_router;

#[tool_router(router = wb_catalog_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Проверяет авторизацию выбранного кабинета через официальный read-only WB /ping.
    #[tool(
        name = "wb_ping",
        annotations(title = "Проверка подключения Wildberries", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_ping(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbAccountInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "analytics:/ping";
        let data = self
            .wb_client
            .ping(&account)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only воронку продаж Wildberries по карточкам за выбранный период.
    #[tool(
        name = "wb_sales_funnel",
        annotations(title = "Воронка продаж Wildberries", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_sales_funnel(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbSalesFunnelInput>,
    ) -> Result<Json<WbResult>, String> {
        // WB sales-funnel accepts at most 365 inclusive calendar days.
        validate_date_range(&input.date_from, &input.date_to, 365)?;
        validate_count("nm_ids", input.nm_ids.len(), 0, MAX_PRODUCT_FILTER_ITEMS)?;
        validate_string_list("brand_names", &input.brand_names, 100, MAX_ENUM_VALUE_CHARS)?;
        validate_count(
            "subject_ids",
            input.subject_ids.len(),
            0,
            MAX_PRODUCT_FILTER_ITEMS,
        )?;
        validate_count("tag_ids", input.tag_ids.len(), 0, MAX_PRODUCT_FILTER_ITEMS)?;
        validate_limit(input.limit, 1_000)?;
        validate_max_u32("offset", input.offset, MAX_OFFSET)?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "analytics:/api/analytics/v3/sales-funnel/products";
        let data = self
            .wb_client
            .sales_funnel(
                &account,
                json!({
                    "selectedPeriod": { "start": input.date_from, "end": input.date_to },
                    "nmIds": input.nm_ids,
                    "brandNames": input.brand_names,
                    "subjectIds": input.subject_ids,
                    "tagIds": input.tag_ids,
                    "skipDeletedNm": input.skip_deleted_nm,
                    "limit": input.limit,
                    "offset": input.offset,
                }),
            )
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only динамику воронки Wildberries по товарам за период до семи дней.
    #[tool(
        name = "wb_sales_funnel_history",
        annotations(title = "История воронки Wildberries", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_sales_funnel_history(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbSalesFunnelHistoryInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_date_range(&input.date_from, &input.date_to, 7)?;
        validate_count("nm_ids", input.nm_ids.len(), 1, 20)?;
        validate_positive_ids("nm_ids", &input.nm_ids)?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "analytics:/api/analytics/v3/sales-funnel/products/history";
        let data = self
            .wb_client
            .sales_funnel_history(
                &account,
                json!({
                    "selectedPeriod": { "start": input.date_from, "end": input.date_to },
                    "nmIds": input.nm_ids,
                    "skipDeletedNm": input.skip_deleted_nm,
                    "aggregationLevel": input.aggregation_level,
                }),
            )
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only динамику воронки Wildberries по брендам, категориям и ярлыкам.
    #[tool(
        name = "wb_sales_funnel_grouped_history",
        annotations(title = "Групповая история воронки Wildberries", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_sales_funnel_grouped_history(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbSalesFunnelGroupedHistoryInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_date_range(&input.date_from, &input.date_to, 7)?;
        validate_string_list("brand_names", &input.brand_names, 16, MAX_ENUM_VALUE_CHARS)?;
        validate_count("subject_ids", input.subject_ids.len(), 0, 16)?;
        validate_count("tag_ids", input.tag_ids.len(), 0, 16)?;
        validate_positive_ids("subject_ids", &input.subject_ids)?;
        validate_positive_ids("tag_ids", &input.tag_ids)?;
        let combinations = input.brand_names.len().max(1)
            * input.subject_ids.len().max(1)
            * input.tag_ids.len().max(1);
        if combinations > 16 {
            return Err(
                "произведение количества brand_names, subject_ids и tag_ids не может превышать 16"
                    .to_owned(),
            );
        }
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "analytics:/api/analytics/v3/sales-funnel/grouped/history";
        let data = self
            .wb_client
            .sales_funnel_grouped_history(
                &account,
                json!({
                    "selectedPeriod": { "start": input.date_from, "end": input.date_to },
                    "brandNames": input.brand_names,
                    "subjectIds": input.subject_ids,
                    "tagIds": input.tag_ids,
                    "skipDeletedNm": input.skip_deleted_nm,
                    "aggregationLevel": input.aggregation_level,
                }),
            )
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only текущие остатки FBW (аналог FBO) на складах Wildberries,
    /// НЕ FBS. Пройдите все страницы limit/offset. Для FBS используйте
    /// `wb_seller_warehouses` и `wb_seller_warehouse_stocks`. Исторической даты нет:
    /// `fetched_at` — время получения текущих данных, не остатки за прошлый день.
    #[tool(
        name = "wb_warehouse_stocks",
        annotations(title = "Остатки FBW на складах Wildberries", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_warehouse_stocks(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbWarehouseStocksInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_count("nm_ids", input.nm_ids.len(), 0, MAX_PRODUCT_FILTER_ITEMS)?;
        validate_count(
            "chrt_ids",
            input.chrt_ids.len(),
            0,
            MAX_PRODUCT_FILTER_ITEMS,
        )?;
        validate_positive_ids("nm_ids", &input.nm_ids)?;
        validate_positive_ids("chrt_ids", &input.chrt_ids)?;
        validate_limit(input.limit, 1_000)?;
        validate_max_u32("offset", input.offset, MAX_OFFSET)?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "analytics:/api/analytics/v1/stocks-report/wb-warehouses";
        let data = self
            .wb_client
            .warehouse_stocks(
                &account,
                json!({
                    "nmIds": input.nm_ids,
                    "chrtIds": input.chrt_ids,
                    "limit": input.limit,
                    "offset": input.offset,
                }),
            )
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only полный список складов продавца WB без пагинации.
    /// Сохраняет deliveryType: для FBS используйте только склады с deliveryType=1;
    /// другие типы доставки не смешивайте с FBS. Остатки каждого склада получайте
    /// через `wb_seller_warehouse_stocks`. Это не список складов WB для FBW.
    #[tool(
        name = "wb_seller_warehouses",
        annotations(
            title = "Склады продавца Wildberries: FBS и другие модели",
            read_only_hint = true
        )
    )]
    pub(in crate::server) async fn wb_seller_warehouses(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbAccountInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "marketplace:/api/v3/warehouses";
        let data = self
            .wb_client
            .seller_warehouses(&account)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only ТЕКУЩИЕ остатки одного склада продавца WB по chrtIds.
    /// Для FBS выберите deliveryType=1 из `wb_seller_warehouses`. Сначала пройдите
    /// все cursor-страницы `wb_product_cards`, соберите sizes[].chrtID и запросите
    /// все пакеты до 1000 ID на каждом складе. Здесь нет offset или `date_from`.
    /// `missing_chrt_ids` — неизвестные остатки, не нули. `complete_for_requested_ids`
    /// относится только к этому пакету. `fetched_at` не подтверждает остатки на
    /// прошлую дату: для неё нужен ранее сохранённый полный снимок именно FBS.
    #[tool(
        name = "wb_seller_warehouse_stocks",
        annotations(
            title = "Текущие остатки склада продавца WB / FBS",
            read_only_hint = true
        )
    )]
    pub(in crate::server) async fn wb_seller_warehouse_stocks(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbSellerWarehouseStocksInput>,
    ) -> Result<Json<WbSellerWarehouseStocksResult>, String> {
        validate_unique_wb_signed_ids("warehouse_id", &[input.warehouse_id])?;
        validate_count("chrt_ids", input.chrt_ids.len(), 1, 1_000)?;
        validate_unique_wb_signed_ids("chrt_ids", &input.chrt_ids)?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "marketplace:/api/v3/stocks/{warehouseId}";
        let data = self
            .wb_client
            .seller_warehouse_stocks(&account, input.warehouse_id, input.chrt_ids.clone())
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        let missing_chrt_ids = wb_missing_stock_ids(&data, &input.chrt_ids)?;
        Ok(Json(WbSellerWarehouseStocksResult {
            source: Self::wb_result(account, endpoint, data).0,
            warehouse_id: input.warehouse_id,
            inventory_scope: "seller_warehouse",
            observation_kind: "current",
            complete_for_requested_ids: missing_chrt_ids.is_empty(),
            requested_chrt_ids: input.chrt_ids,
            missing_chrt_ids,
        }))
    }

    /// Получает read-only список заказов Wildberries, изменённых после `date_from`.
    #[tool(
        name = "wb_orders",
        annotations(title = "Заказы Wildberries", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_orders(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbStatisticsReportInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_wb_change_date(&input.date_from)?;
        validate_flag(input.flag)?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "statistics:/api/v1/supplier/orders";
        let data = self
            .wb_client
            .orders(&account, input.date_from, input.flag)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only список продаж и возвратов Wildberries, изменённых после `date_from`.
    #[tool(
        name = "wb_sales",
        annotations(title = "Продажи и возвраты Wildberries", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_sales(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbStatisticsReportInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_wb_change_date(&input.date_from)?;
        validate_flag(input.flag)?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "statistics:/api/v1/supplier/sales";
        let data = self
            .wb_client
            .sales(&account, input.date_from, input.flag)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only список карточек товаров Wildberries с безопасными фильтрами и курсором.
    #[tool(
        name = "wb_product_cards",
        annotations(title = "Карточки товаров Wildberries", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_product_cards(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbProductCardsInput>,
    ) -> Result<Json<WbResult>, String> {
        let payload = wb_product_cards_payload(&input)?;
        let locale = input.locale.map(|locale| locale.as_str().to_owned());
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "content:/content/v2/get/cards/list";
        let data = self
            .wb_client
            .product_cards(&account, locale, payload)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only текущие цены и скидки Wildberries без возможности их изменения.
    #[tool(
        name = "wb_product_prices",
        annotations(title = "Цены товаров Wildberries", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_product_prices(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbProductPricesInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_limit(input.limit, 1_000)?;
        validate_max_u32("offset", input.offset, MAX_OFFSET)?;
        if input.nm_id == Some(0) {
            return Err("nm_id должен быть положительным ID".to_owned());
        }
        if input.nm_id.is_some() && input.offset != 0 {
            return Err("offset должен быть равен 0 при фильтрации по nm_id".to_owned());
        }
        let limit = if input.nm_id.is_some() {
            1
        } else {
            input.limit
        };
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "prices:/api/v2/list/goods/filter";
        let data = self
            .wb_client
            .product_prices(&account, input.nm_id, limit, input.offset)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only комиссии Wildberries по категориям товаров.
    #[tool(
        name = "wb_tariff_commissions",
        annotations(title = "Комиссии Wildberries", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_tariff_commissions(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbTariffCommissionsInput>,
    ) -> Result<Json<WbResult>, String> {
        let locale = input.locale.map(|locale| locale.as_str().to_owned());
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "common:/api/v1/tariffs/commission";
        let data = self
            .wb_client
            .tariff_commissions(&account, locale)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only тарифы Wildberries для товаров в коробах на выбранную дату.
    #[tool(
        name = "wb_tariff_boxes",
        annotations(title = "Тарифы Wildberries для коробов", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_tariff_boxes(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbTariffDateInput>,
    ) -> Result<Json<WbResult>, String> {
        parse_date(&input.date, "date")?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "common:/api/v1/tariffs/box";
        let data = self
            .wb_client
            .tariff_boxes(&account, input.date)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only тарифы Wildberries для товаров на монопаллетах на выбранную дату.
    #[tool(
        name = "wb_tariff_pallets",
        annotations(title = "Тарифы Wildberries для монопаллет", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_tariff_pallets(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbTariffDateInput>,
    ) -> Result<Json<WbResult>, String> {
        parse_date(&input.date, "date")?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "common:/api/v1/tariffs/pallet";
        let data = self
            .wb_client
            .tariff_pallets(&account, input.date)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only тарифы Wildberries на возврат товаров продавцу на выбранную дату.
    #[tool(
        name = "wb_tariff_returns",
        annotations(title = "Тарифы Wildberries на возврат", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_tariff_returns(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbTariffDateInput>,
    ) -> Result<Json<WbResult>, String> {
        parse_date(&input.date, "date")?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "common:/api/v1/tariffs/return";
        let data = self
            .wb_client
            .tariff_returns(&account, input.date)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Получает read-only коэффициенты приёмки поставок Wildberries на ближайшие 14 дней.
    #[tool(
        name = "wb_acceptance_coefficients",
        annotations(title = "Коэффициенты приёмки Wildberries", read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_acceptance_coefficients(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbAcceptanceCoefficientsInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_count("warehouse_ids", input.warehouse_ids.len(), 0, 100)?;
        validate_unique_positive_ids("warehouse_ids", &input.warehouse_ids)?;
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "common:/api/tariffs/v1/acceptance/coefficients";
        let data = self
            .wb_client
            .acceptance_coefficients(&account, input.warehouse_ids)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
}
