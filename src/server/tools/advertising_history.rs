//! RBAC is rechecked on every archive request and page, before database access.
use rmcp::{tool, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

use super::super::{
    Json, OzonMcp, Parameters, REPORTING_INVALID_REQUEST, RequestIdentity, parse_date,
};
use crate::reporting::{
    advertising_history::{HistoryGroup, HistoryRepository, last_closed_day, validate_request},
    snapshot::Marketplace,
};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HistoryAccountInput {
    pub account: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HistorySyncInput {
    pub account: Option<String>,
    /// YYYY-MM-DD. Without it, discover the earliest known campaign creation.
    pub date_from: Option<String>,
    /// Inclusive closed day YYYY-MM-DD; defaults to yesterday in WB Moscow time.
    pub date_to: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HistoryStatsInput {
    pub account: Option<String>,
    /// YYYY-MM-DD. Without dates, read the whole available archive.
    pub date_from: Option<String>,
    pub date_to: Option<String>,
    #[serde(default)]
    pub group_by: HistoryGroup,
    #[serde(default = "default_limit")]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: u16,
    #[serde(default)]
    #[schemars(range(max = 100_000))]
    pub offset: u32,
}

const fn default_limit() -> u16 {
    100
}

impl OzonMcp {
    #[must_use]
    pub fn with_advertising_history(mut self, repository: HistoryRepository) -> Self {
        self.advertising_history = repository;
        self
    }

    fn history_account(
        &self,
        identity: &RequestIdentity,
        account: Option<&str>,
    ) -> Result<String, String> {
        let (account, role) = self.resolve_reporting_account(identity, account)?;
        Self::authorize_reporting_details_for_role(role)?;
        if account.marketplace() != Marketplace::Wildberries {
            return Err(format!(
                "{REPORTING_INVALID_REQUEST}: нужен кабинет Wildberries"
            ));
        }
        Ok(account.account_id().to_owned())
    }
}

#[tool_router(router=advertising_history_router,vis="pub(in crate::server)")]
impl OzonMcp {
    /// Ставит сбор исторической рекламы WB в долговечную очередь. Без `date_from`
    /// собирает от создания известных кампаний. Только локальная запись: отдельный
    /// сборщик читает WB с общей квотой. Рекламу, ставки и бюджет не меняет.
    #[tool(
        name = "wb_advertising_history_sync",
        annotations(
            title = "Собрать историю рекламы WB",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn advertising_history_sync(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<HistorySyncInput>,
    ) -> Result<Json<Value>, String> {
        let account = self.history_account(&identity, input.account.as_deref())?;
        let (_, actor) = self.access_context(&identity)?;
        let from = input
            .date_from
            .as_deref()
            .map(|date| parse_date(date, "history_date"))
            .transpose()?;
        let to = input
            .date_to
            .as_deref()
            .map(|date| parse_date(date, "history_date"))
            .transpose()?
            .unwrap_or_else(last_closed_day);
        validate_request(&account, from, to)
            .map_err(|_| format!("{REPORTING_INVALID_REQUEST}: недопустимый период истории"))?;
        self.advertising_history.request(&account,&actor.id,from,to).await.map(Json).map_err(|_| "WB_HISTORY_UNAVAILABLE: очередь истории недоступна; проверьте подключение и миграцию 052".to_owned())
    }

    /// Прогресс последней загрузки истории рекламы WB: выполненные запросы,
    /// ошибки, пропуски и следующая попытка. Внешних API-вызовов нет.
    #[tool(
        name = "wb_advertising_history_status",
        annotations(
            title = "Прогресс истории рекламы WB",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn advertising_history_status(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<HistoryAccountInput>,
    ) -> Result<Json<Value>, String> {
        let account = self.history_account(&identity, input.account.as_deref())?;
        self.advertising_history
            .status(&account)
            .await
            .map(Json)
            .map_err(|_| "WB_HISTORY_UNAVAILABLE: архив истории недоступен".to_owned())
    }

    /// ДРР за весь доступный архив или выбранный период. Расход / сумма
    /// приписанных рекламе заказов (с ассоциированными конверсиями), не выкупы.
    /// Один итог на день/кампанию; повторные наблюдения заменяют предыдущие.
    /// Проверяйте coverage: пропуски не равны нулю; `all_time_verified=false`
    /// означает, что удалённые до создания архива кампании могли отсутствовать.
    #[tool(
        name = "wb_advertising_history_stats",
        annotations(
            title = "Исторический ДРР рекламы WB",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn advertising_history_stats(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<HistoryStatsInput>,
    ) -> Result<Json<Value>, String> {
        let account = self.history_account(&identity, input.account.as_deref())?;
        let from = input
            .date_from
            .as_deref()
            .map(|date| parse_date(date, "history_date"))
            .transpose()?;
        let to = input
            .date_to
            .as_deref()
            .map(|date| parse_date(date, "history_date"))
            .transpose()?;
        validate_request(&account, from, to.unwrap_or_else(last_closed_day))
            .map_err(|_| format!("{REPORTING_INVALID_REQUEST}: недопустимый период истории"))?;
        if !(1..=1000).contains(&input.limit) || input.offset > 100_000 {
            return Err(format!(
                "{REPORTING_INVALID_REQUEST}: недопустимая страница истории"
            ));
        }
        self.advertising_history
            .stats(
                &account,
                from,
                to,
                input.group_by,
                input.limit,
                input.offset,
            )
            .await
            .map(Json)
            .map_err(|_| "WB_HISTORY_UNAVAILABLE: архив истории недоступен".to_owned())
    }
}
