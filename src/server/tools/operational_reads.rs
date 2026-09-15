//! First supplemental package: bounded orders, advertising money and key rights.
use super::super::{
    Json, OzonMcp, OzonResult, Parameters, RequestIdentity, StoreOnlyInput, WbAccountInput,
    WbResult, tool,
};
use rmcp::{schemars::JsonSchema, tool_router};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FbsOrdersInput {
    #[serde(default)]
    #[schemars(length(min = 1, max = 128))]
    pub account: Option<String>,
    #[serde(default = "crate::wb::read_coverage::default_limit")]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 9_223_372_036_854_775_807_u64))]
    pub next: u64,
    /// UTC Unix seconds, fixed for every page of the same collection.
    #[schemars(range(min = 0))]
    pub date_from: i64,
    /// UTC Unix seconds, at most 30 days after `date_from`.
    #[schemars(range(min = 0))]
    pub date_to: i64,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FbsStatusesInput {
    #[serde(default)]
    #[schemars(length(min = 1, max = 128))]
    pub account: Option<String>,
    #[schemars(
        length(min = 1, max = 1000),
        inner(range(min = 1, max = 9_223_372_036_854_775_807_u64))
    )]
    pub orders: Vec<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PromotionBudgetInput {
    #[serde(default)]
    #[schemars(length(min = 1, max = 128))]
    pub account: Option<String>,
    #[schemars(range(min = 1, max = 9_223_372_036_854_775_807_u64))]
    pub advert_id: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PromotionHistoryInput {
    #[serde(default)]
    #[schemars(length(min = 1, max = 128))]
    pub account: Option<String>,
    #[schemars(length(equal = 10))]
    pub from: String,
    #[schemars(length(equal = 10))]
    pub to: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct FbsOrdersPage {
    #[serde(flatten)]
    pub source: WbResult,
    pub next_cursor: Option<u64>,
    pub page_is_last: bool,
    pub date_from: i64,
    pub date_to: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct FbsStatusesResult {
    #[serde(flatten)]
    pub source: WbResult,
    pub missing_order_ids: Vec<u64>,
    pub complete_for_requested_ids: bool,
}

#[tool_router(router = operational_reads_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Новые сборочные задания WB FBS. Сохраняет deliveryType: другие модели не
    /// считать FBS автоматически. Это очередь обработки, не выкупы или реализация.
    #[tool(name = "wb_fbs_new_orders", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_fbs_new_orders(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbAccountInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "marketplace:/api/v3/orders/new";
        let data = self
            .wb_client
            .fbs_new_orders(&account)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Страница сборочных заданий WB за фиксированный UTC-период до 30 дней,
    /// история до 3 месяцев. Начните с next=0, сохраняйте страницы и `next_cursor`,
    /// завершайте на пустой странице. Текущие статусы читайте отдельно. Сохраняйте
    /// deliveryType. `fetched_at` — время чтения; этот вызов не запускает фоновый сбор.
    #[tool(name = "wb_fbs_orders", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_fbs_orders(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FbsOrdersInput>,
    ) -> Result<Json<FbsOrdersPage>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "marketplace:/api/v3/orders";
        let data = self
            .wb_client
            .fbs_orders_page(
                &account,
                input.limit,
                input.next,
                input.date_from,
                input.date_to,
            )
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        let page_is_last = data["orders"]
            .as_array()
            .expect("validated orders")
            .is_empty();
        let next_cursor = if page_is_last {
            None
        } else {
            data["next"].as_u64()
        };
        Ok(Json(FbsOrdersPage {
            source: Self::wb_result(account, endpoint, data).0,
            next_cursor,
            page_is_last,
            date_from: input.date_from,
            date_to: input.date_to,
        }))
    }

    /// Текущие supplierStatus/wbStatus до 1000 сборочных заданий. Отсутствующие
    /// ID перечислены явно: их статус неизвестен, они не считаются отменёнными.
    #[tool(name = "wb_fbs_order_statuses", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_fbs_order_statuses(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FbsStatusesInput>,
    ) -> Result<Json<FbsStatusesResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "marketplace:/api/v3/orders/status";
        let data = self
            .wb_client
            .fbs_order_statuses(&account, &input.orders)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        let returned: std::collections::BTreeSet<_> = data["orders"]
            .as_array()
            .expect("validated orders")
            .iter()
            .filter_map(|row| row["id"].as_u64())
            .collect();
        let missing_order_ids: Vec<_> = input
            .orders
            .into_iter()
            .filter(|id| !returned.contains(id))
            .collect();
        Ok(Json(FbsStatusesResult {
            complete_for_requested_ids: missing_order_ids.is_empty(),
            missing_order_ids,
            source: Self::wb_result(account, endpoint, data).0,
        }))
    }

    /// Источники рекламных средств WB: взаимозачёт net, баланс balance, бонусы.
    /// Это доступные средства, не рекламный расход за период.
    #[tool(name = "wb_promotion_balance", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_promotion_balance(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbAccountInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "promotion:/adv/v1/balance";
        let data = self
            .wb_client
            .promotion_balance(&account)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Остаток бюджета одной рекламной кампании WB. Только чтение.
    #[tool(name = "wb_promotion_budget", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_promotion_budget(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PromotionBudgetInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "promotion:/adv/v1/budget";
        let data = self
            .wb_client
            .promotion_campaign_budget(&account, input.advert_id)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// История рекламных затрат WB, from/to YYYY-MM-DD, до 31 дня включительно.
    /// Сверять со статистикой рекламы, не складывать дважды. Консервативная квота
    /// этого метода — раз в час на продавца; cooldown возвращается без ожидания.
    #[tool(name = "wb_promotion_costs", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_promotion_costs(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PromotionHistoryInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "promotion:/adv/v1/upd";
        let data = self
            .wb_client
            .promotion_costs(&account, &input.from, &input.to)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// История пополнений рекламного счёта WB, до 31 дня включительно. HTTP 204
    /// означает пустую историю. Пополнения не являются расходами. Консервативная
    /// квота — раз в час на продавца отдельно от затрат; без ожидания cooldown.
    #[tool(name = "wb_promotion_payments", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_promotion_payments(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PromotionHistoryInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "promotion:/adv/v1/payments";
        let data = self
            .wb_client
            .promotion_payments(&account, &input.from, &input.to)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, &error))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Роли и методы текущего API-ключа Ozon. Не раскрывает сам ключ, не изменяет
    /// права MCP и не подтверждает доступ к подпискам или полноту данных.
    #[tool(name = "ozon_api_key_roles", annotations(read_only_hint = true))]
    pub(in crate::server) async fn ozon_api_key_roles(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<StoreOnlyInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.request(&identity, input.store, "/v1/roles", serde_json::json!({}))
            .await
    }
}
