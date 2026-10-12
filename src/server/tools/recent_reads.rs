//! Reviewed October 2026 marketplace reads and asynchronous inventory exports.
use super::super::{Json, OzonMcp, Parameters, RequestIdentity, WbResult, tool};
use crate::wb::recent_queries::{ItemReturnsQuery, OrderFeedQuery, StockHistoryQuery};
use rmcp::{schemars::JsonSchema, tool_router};
use serde::Deserialize;

macro_rules! query_input {
    ($name:ident, $query:ty) => {
        #[derive(Debug, Deserialize, JsonSchema)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            #[serde(default)]
            #[schemars(length(min = 1, max = 128))]
            pub account: Option<String>,
            pub query: $query,
        }
    };
}
query_input!(WbOrderFeedInput, OrderFeedQuery);
query_input!(WbItemReturnsInput, ItemReturnsQuery);
query_input!(WbStockHistoryInput, StockHistoryQuery);

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbBudgetsInput {
    #[serde(default)]
    #[schemars(length(min = 1, max = 128))]
    pub account: Option<String>,
    #[schemars(length(min = 1, max = 50), inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)), extend("uniqueItems" = true))]
    pub advert_ids: Vec<u64>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbDailyLimitsInput {
    #[serde(default)]
    #[schemars(length(min = 1, max = 128))]
    pub account: Option<String>,
    #[schemars(length(min = 1, max = 100), inner(range(min = 1, max = 9_223_372_036_854_775_807_u64)), extend("uniqueItems" = true))]
    pub advert_ids: Vec<u64>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbSupplyDiscrepanciesInput {
    #[serde(default)]
    #[schemars(length(min = 1, max = 128))]
    pub account: Option<String>,
    #[schemars(range(min = 1, max = 9_223_372_036_854_775_807_u64))]
    pub supply_id: u64,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbReportDownloadInput {
    #[serde(default)]
    #[schemars(length(min = 1, max = 128))]
    pub account: Option<String>,
    #[schemars(
        length(equal = 36),
        description = "UUID из wb_stock_history_create; отчёт доступен для скачивания 48 часов"
    )]
    pub download_id: String,
}

#[tool_router(router = recent_reads_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Лента заказов WB за последние 31 день, по времени текущего статуса.
    /// Одна единица товара на заказ; created, buyout, cancel, return,
    /// returnDefective и cancelType сохраняются раздельно. Для следующих страниц
    /// сохраняйте data.snapshotTime, период и фильтры, увеличивайте offset.
    /// Данные обновляются в реальном времени; это не финансовый отчёт реализации.
    /// Квота: Персональный токен — 1 минута, нераспознанный — 3 часа.
    #[tool(name = "wb_order_feed", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_order_feed(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbOrderFeedInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "analytics:/api/analytics/v1/order-feed";
        let data = self
            .wb_client
            .order_feed(&account, &input.query)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Расхождения заявленного и принятого количества поставки FBW.
    /// Только ID фактической поставки, не предварительного заказа. Чтение не
    /// подтверждает акт и не изменяет поставку. Категория токена «Поставки».
    #[tool(name = "wb_supply_discrepancies", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_supply_discrepancies(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbSupplyDiscrepanciesInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "supplies:/api/supplies/v1/discrepancies/{supplyId}";
        let data = self
            .wb_client
            .supply_discrepancies(&account, input.supply_id)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Активные и архивные возвраты и перемещения товаров продавцу WB.
    /// Одна страница limit/offset, категория «Аналитика». Это отдельный сценарий
    /// от `wb_return_claims` — заявок покупателей. HTTP 204 сохраняется как `no_data`.
    #[tool(name = "wb_item_returns", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_item_returns(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbItemReturnsInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "analytics:/api/analytics/v1/item-returns";
        let data = self
            .wb_client
            .item_returns(&account, &input.query)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Бюджеты 1–50 рекламных кампаний WB через v2, статусы 4, 9 и 11.
    /// total — основные единицы валюты currency, не копейки. Отсутствующий/null
    /// бюджет не означает ноль. Квота: Персональный — 3 секунды,
    /// нераспознанный — 15 минут. Только чтение.
    #[tool(name = "wb_promotion_budgets", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_promotion_budgets(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbBudgetsInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "promotion:/api/advert/v2/budget";
        let data = self
            .wb_client
            .promotion_campaign_budgets(&account, &input.advert_ids)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Читает настройки дневных лимитов до 100 CPC-кампаний WB: enabled,
    /// dailyLimit, spentToday, currency, carryOverEnabled, valid, requiredLimit.
    /// Нужен Персональный токен «Продвижение». Лимиты не меняются.
    #[tool(name = "wb_promotion_daily_limits", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_promotion_daily_limits(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbDailyLimitsInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "promotion:/api/advert/v0/daily-limits";
        let data = self
            .wb_client
            .promotion_daily_limits(&account, &input.advert_ids)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Заказывает `STOCK_HISTORY_DAILY_CSV` с дневной историей остатков WB за
    /// последние 3 месяца, включая «за вчера», без подписки Джем. Используйте
    /// уникальный `download_id` UUID; затем `wb_analytics_report_status` и
    /// `wb_analytics_report_download`. Данные обновляются раз в 2 часа;
    /// `fetched_at` не является датой остатков.
    /// Генерация аналитического отчёта не меняет товары, заказы или остатки.
    #[tool(name = "wb_stock_history_create", annotations(read_only_hint = true))]
    pub(in crate::server) async fn wb_stock_history_create(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbStockHistoryInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "analytics:/api/v2/nm-report/downloads";
        let data = self
            .wb_client
            .stock_history_create(&account, &input.query)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Проверяет статус аналитического CSV-отчёта WB по UUID. Один запрос без
    /// ожидания или автоматического опроса. При `status=SUCCESS` используйте
    /// `wb_analytics_report_download`; WAITING/PROCESSING ещё не готовы.
    #[tool(
        name = "wb_analytics_report_status",
        annotations(read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_analytics_report_status(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbReportDownloadInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "analytics:/api/v2/nm-report/downloads";
        let data = self
            .wb_client
            .analytics_report_status(&account, &input.download_id)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }

    /// Читает готовый аналитический отчёт WB по UUID, доступный 48 часов.
    /// Возвращает UTF-8 CSV из ZIP в `files[].csv`; до 2 мегабайт распакованных данных,
    /// до 4 файлов. При превышении лимита ошибка, частичные итоги не выдаются.
    #[tool(
        name = "wb_analytics_report_download",
        annotations(read_only_hint = true)
    )]
    pub(in crate::server) async fn wb_analytics_report_download(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbReportDownloadInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = crate::wb::recent_policy::DOWNLOAD_LABEL;
        let data = self
            .wb_client
            .analytics_report_download(&account, &input.download_id)
            .await
            .map_err(|e| self.wb_error(&account, endpoint, &e))?;
        Ok(Self::wb_result(account, endpoint, data))
    }
}
