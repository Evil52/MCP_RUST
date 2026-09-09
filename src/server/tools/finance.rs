//! Authorized finance MCP tools.

use super::super::OzonMcp;
use super::super::{
    FinanceAccrualByDayInput, FinanceAccrualPostingsInput, FinanceAccrualTypesInput,
    FinanceCashFlowInput, FinanceInput, FinanceMutualSettlementInput, FinanceRealizationByDayInput,
    FinanceTotalsInput, Json, MAX_ENUM_VALUE_CHARS, MAX_FINANCE_TRANSACTIONS_PERIOD_DAYS,
    MAX_IDENTIFIER_CHARS, MAX_OPAQUE_TOKEN_CHARS, MAX_OPERATION_TYPES, MAX_PAGE,
    MAX_POSTING_NUMBERS, OzonResult, Parameters, RequestIdentity, json, parse_date, tool,
    validate_and_expand_dates, validate_cash_flow_period, validate_limit, validate_max_chars,
    validate_max_u32, validate_string_list, validate_year_month,
};
use chrono::Datelike;
use rmcp::tool_router;

#[tool_router(router = finance_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Устаревающий метод финансовых транзакций Ozon, отключение 2026-09-08. Для новых сценариев используйте методы `ozon_finance_accrual`_*.
    #[tool(
        name = "ozon_finance_transactions",
        annotations(title = "Финансовые транзакции Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn finance_transactions(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FinanceInput>,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_and_expand_dates(
            &input.date_from,
            &input.date_to,
            MAX_FINANCE_TRANSACTIONS_PERIOD_DAYS,
        )?;
        validate_max_chars(
            "posting_number",
            &input.posting_number,
            MAX_IDENTIFIER_CHARS,
        )?;
        validate_string_list(
            "operation_types",
            &input.operation_types,
            MAX_OPERATION_TYPES,
            MAX_ENUM_VALUE_CHARS,
        )?;
        validate_max_chars(
            "transaction_type",
            &input.transaction_type,
            MAX_ENUM_VALUE_CHARS,
        )?;
        validate_limit(input.page_size, 1_000)?;
        if input.page == 0 {
            return Err("page должен быть не меньше 1".to_owned());
        }
        validate_max_u32("page", input.page, MAX_PAGE)?;
        self.request(
            &identity,
            input.store,
            "/v3/finance/transaction/list",
            json!({
                "filter": {
                    "date": { "from": from, "to": to },
                    "operation_type": input.operation_types,
                    "posting_number": input.posting_number,
                    "transaction_type": input.transaction_type,
                },
                "page": input.page,
                "page_size": input.page_size,
            }),
        )
        .await
    }

    /// Устаревающий метод финансовых итогов Ozon, отключение 2026-09-08. Для новых сценариев используйте методы `ozon_finance_accrual`_*.
    #[tool(
        name = "ozon_finance_totals",
        annotations(title = "Финансовые итоги Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn finance_totals(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FinanceTotalsInput>,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_and_expand_dates(&input.date_from, &input.date_to, 366)?;
        validate_max_chars(
            "posting_number",
            &input.posting_number,
            MAX_IDENTIFIER_CHARS,
        )?;
        validate_max_chars(
            "transaction_type",
            &input.transaction_type,
            MAX_ENUM_VALUE_CHARS,
        )?;
        self.request(
            &identity,
            input.store,
            "/v3/finance/transaction/totals",
            json!({
                "date": { "from": from, "to": to },
                "posting_number": input.posting_number,
                "transaction_type": input.transaction_type,
            }),
        )
        .await
    }

    /// Получает начисления Ozon по указанным отправлениям.
    #[tool(
        name = "ozon_finance_accrual_postings",
        annotations(title = "Начисления по отправлениям Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn finance_accrual_postings(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FinanceAccrualPostingsInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_string_list(
            "posting_numbers",
            &input.posting_numbers,
            MAX_POSTING_NUMBERS,
            MAX_IDENTIFIER_CHARS,
        )?;
        if input.posting_numbers.is_empty()
            || input
                .posting_numbers
                .iter()
                .any(|posting_number| posting_number.trim().is_empty())
        {
            return Err(
                "posting_numbers должен быть непустым и не содержать пустых значений".into(),
            );
        }
        self.request(
            &identity,
            input.store,
            "/v1/finance/accrual/postings",
            json!({ "posting_numbers": input.posting_numbers }),
        )
        .await
    }

    /// Получает справочник типов начислений Ozon.
    #[tool(
        name = "ozon_finance_accrual_types",
        annotations(title = "Типы начислений Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn finance_accrual_types(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FinanceAccrualTypesInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.request(
            &identity,
            input.store,
            "/v1/finance/accrual/types",
            json!({}),
        )
        .await
    }

    /// Получает начисления Ozon за один день с пагинацией `last_id`.
    #[tool(
        name = "ozon_finance_accrual_by_day",
        annotations(title = "Начисления Ozon за день", read_only_hint = true)
    )]
    pub(in crate::server) async fn finance_accrual_by_day(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FinanceAccrualByDayInput>,
    ) -> Result<Json<OzonResult>, String> {
        parse_date(&input.date, "date")?;
        validate_max_chars("last_id", &input.last_id, MAX_OPAQUE_TOKEN_CHARS)?;
        self.request(
            &identity,
            input.store,
            "/v1/finance/accrual/by-day",
            json!({ "date": input.date, "last_id": input.last_id }),
        )
        .await
    }

    /// Получает построчный отчёт о реализации товаров за день. Метод требует Premium Plus или Premium Pro в кабинете Ozon.
    #[tool(
        name = "ozon_finance_realization_by_day",
        annotations(title = "Реализация Ozon за день", read_only_hint = true)
    )]
    pub(in crate::server) async fn finance_realization_by_day(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FinanceRealizationByDayInput>,
    ) -> Result<Json<OzonResult>, String> {
        let date = parse_date(&input.date, "date")?;
        self.request(
            &identity,
            input.store,
            "/v1/finance/realization/by-day",
            json!({ "day": date.day(), "month": date.month(), "year": date.year() }),
        )
        .await
    }

    /// Получает детальный отчёт о движении денежных средств за расчётный период Ozon.
    #[tool(
        name = "ozon_finance_cash_flow",
        annotations(title = "Движение денежных средств Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn finance_cash_flow(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FinanceCashFlowInput>,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_cash_flow_period(&input.date_from, &input.date_to)?;
        if input.page == 0 {
            return Err("page должен быть не меньше 1".to_owned());
        }
        validate_max_u32("page", input.page, MAX_PAGE)?;
        validate_limit(input.page_size, 1_000)?;
        self.request(
            &identity,
            input.store,
            "/v1/finance/cash-flow-statement/list",
            json!({
                "date": { "from": from, "to": to },
                "page": input.page,
                "page_size": input.page_size,
                "with_details": input.with_details,
            }),
        )
        .await
    }

    /// Получает месячный отчёт Ozon о взаиморасчётах.
    #[tool(
        name = "ozon_finance_mutual_settlement",
        annotations(title = "Взаиморасчёты Ozon", read_only_hint = true)
    )]
    pub(in crate::server) async fn finance_mutual_settlement(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FinanceMutualSettlementInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_year_month(&input.date)?;
        self.request(
            &identity,
            input.store,
            "/v1/finance/mutual-settlement",
            json!({ "date": input.date, "language": input.language }),
        )
        .await
    }
}
