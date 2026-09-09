//! Authorized reporting MCP tools.

use super::super::OzonMcp;
use super::super::{
    AccountScope, CollectionStatusResult, DataCompletenessResult, Json,
    MAX_REPORTING_HISTORY_POINTS, MAX_REPORTING_REPORTS, MAX_REPORTING_STATUS_ROWS,
    MAX_SALES_ANALYTICS_DAYS, MAX_SALES_ANALYTICS_OFFSET, MAX_SALES_ANALYTICS_ROWS,
    MAX_TOOL_CALL_LOG_ROWS, ManagerActionsResult, Marketplace, MetricsHistoryResult, Parameters,
    REPORT_REFRESH_INVALID_REQUEST, REPORTING_INVALID_REQUEST, ReadyReportsResult,
    ReportingCollectionStatusInput, ReportingCompletenessInput, ReportingManagerActionsInput,
    ReportingMarketplace, ReportingMetricsHistoryInput, ReportingOzonSalesAnalyticsInput,
    ReportingOzonSalesRefreshInput, ReportingReadyReportsInput, ReportingSourceSnapshotInput,
    ReportingWeeklyMarketplaceRankingInput, RequestIdentity, SalesAnalyticsQuery,
    SalesAnalyticsResult, SalesRefreshStatus, TOOL_TELEMETRY_INVALID_REQUEST, ToolCallLogInput,
    ToolCallLogResult, Utc, WeeklyMarketplaceRankingResult, parse_date, parse_reporting_cutoff,
    parse_reporting_date_range, tool, validate_reporting_limit, weekly_ranking_period,
};
use rmcp::tool_router;

#[tool_router(router = reporting_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Показывает последние попытки фонового сбора для одного разрешённого кабинета.
    /// Метод читает только серверную PostgreSQL-проекцию и не обращается к маркетплейсам.
    #[tool(
        name = "ofk_collection_status",
        annotations(
            title = "Статус фонового сбора OFK",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn reporting_collection_status(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReportingCollectionStatusInput>,
    ) -> Result<Json<CollectionStatusResult>, String> {
        validate_reporting_limit(input.limit, MAX_REPORTING_STATUS_ROWS)?;
        let (account, _) = self.resolve_reporting_account(&identity, input.account.as_deref())?;
        self.reporting_reader
            .collection_status(&account, input.limit)
            .await
            .map(Json)
            .map_err(Self::reporting_error)
    }

    /// Проверяет полноту и качество опубликованного набора источников для одного кабинета.
    /// Значение N/D никогда не подменяется нулём; внешние API этим методом не вызываются.
    #[tool(
        name = "ofk_data_completeness",
        annotations(
            title = "Полнота данных OFK",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn reporting_data_completeness(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReportingCompletenessInput>,
    ) -> Result<Json<DataCompletenessResult>, String> {
        let cutoff = parse_reporting_cutoff(input.cutoff_at.as_deref())?;
        let (account, _) = self.resolve_reporting_account(&identity, input.account.as_deref())?;
        self.reporting_reader
            .data_completeness(&account, cutoff)
            .await
            .map(Json)
            .map_err(Self::reporting_error)
    }

    /// Возвращает ограниченную историю канонических KPI по опубликованным снимкам.
    /// Финансово-рекламные показатели доступны только ролям finance/admin.
    #[tool(
        name = "ofk_metrics_history",
        annotations(
            title = "История KPI OFK",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn reporting_metrics_history(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReportingMetricsHistoryInput>,
    ) -> Result<Json<MetricsHistoryResult>, String> {
        validate_reporting_limit(input.limit, MAX_REPORTING_HISTORY_POINTS)?;
        let (date_from, date_to) =
            parse_reporting_date_range(input.date_from.as_deref(), input.date_to.as_deref())?;
        let (account, role) =
            self.resolve_reporting_account(&identity, input.account.as_deref())?;
        Self::authorize_reporting_details_for_role(role)?;
        self.reporting_reader
            .metrics_history(&account, date_from, date_to, input.limit)
            .await
            .map(Json)
            .map_err(Self::reporting_error)
    }

    /// Строит единый рейтинг Ozon и Wildberries только из опубликованных PostgreSQL-снимков.
    /// Лидер и аутсайдер появляются только при полной недельной выборке по всем кабинетам реестра.
    #[tool(
        name = "ofk_weekly_marketplace_ranking",
        annotations(
            title = "Недельный рейтинг маркетплейсов OFK",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn reporting_weekly_marketplace_ranking(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReportingWeeklyMarketplaceRankingInput>,
    ) -> Result<Json<WeeklyMarketplaceRankingResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        Self::authorize_report_catalog_for_role(actor.role)?;
        let (date_from, date_to) = weekly_ranking_period(
            input.date_from.as_deref(),
            input.date_to.as_deref(),
            crate::reporting::business_date(Utc::now()),
        )?;
        let accounts = registry
            .accounts
            .iter()
            .map(|account| {
                let marketplace = match account.marketplace {
                    Marketplace::Ozon => ReportingMarketplace::Ozon,
                    Marketplace::Wildberries => ReportingMarketplace::Wildberries,
                };
                AccountScope::new(account.id.clone(), marketplace).map_err(|_| {
                    format!(
                        "{REPORTING_INVALID_REQUEST}: кабинет реестра имеет недопустимый идентификатор"
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.reporting_reader
            .weekly_marketplace_ranking(&accounts, date_from, date_to)
            .await
            .map(Json)
            .map_err(Self::reporting_error)
    }

    /// Читает последний полный снимок одного источника из PostgreSQL, даже если другие источники не собраны.
    /// Возвращает время наблюдения, свежесть и прогресс обновления. Маркетплейсы не вызываются.
    #[tool(
        name = "ofk_source_snapshot",
        annotations(
            title = "Данные отдельного источника из снимков OFK",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn reporting_source_snapshot(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReportingSourceSnapshotInput>,
    ) -> Result<Json<crate::reporting::mcp_read::SourceSnapshotResult>, String> {
        let (account, role) =
            self.resolve_reporting_account(&identity, input.account.as_deref())?;
        if matches!(
            input.source,
            crate::reporting::snapshot::SnapshotSource::Finance
                | crate::reporting::snapshot::SnapshotSource::Advertising
        ) {
            Self::authorize_reporting_details_for_role(role)?;
        }
        self.reporting_reader
            .source_snapshot(
                &account,
                crate::reporting::mcp_read::SourceSnapshotQuery {
                    source: input.source,
                    snapshot_id: input.snapshot_id,
                    limit: input.limit,
                    offset: input.offset,
                },
            )
            .await
            .map(Json)
            .map_err(Self::reporting_error)
    }

    /// Возвращает стандартную аналитику продаж Ozon из опубликованных PostgreSQL-снимков.
    /// Метод не обращается к Ozon и безопасно обслуживает параллельные запросы менеджеров.
    #[tool(
        name = "ofk_ozon_sales_analytics",
        annotations(
            title = "Аналитика продаж Ozon из снимков OFK",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn reporting_ozon_sales_analytics(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReportingOzonSalesAnalyticsInput>,
    ) -> Result<Json<SalesAnalyticsResult>, String> {
        let date_from = parse_date(&input.date_from, "date_from")?;
        let date_to = parse_date(&input.date_to, "date_to")?;
        let inclusive_days = date_to.signed_duration_since(date_from).num_days() + 1;
        if !(1..=MAX_SALES_ANALYTICS_DAYS).contains(&inclusive_days) {
            return Err(format!(
                "{REPORTING_INVALID_REQUEST}: период аналитики должен содержать от 1 до {MAX_SALES_ANALYTICS_DAYS} дней"
            ));
        }
        if !(1..=MAX_SALES_ANALYTICS_ROWS).contains(&input.limit) {
            return Err(format!(
                "{REPORTING_INVALID_REQUEST}: limit должен быть от 1 до {MAX_SALES_ANALYTICS_ROWS}"
            ));
        }
        if input.offset > MAX_SALES_ANALYTICS_OFFSET {
            return Err(format!(
                "{REPORTING_INVALID_REQUEST}: offset не может превышать {MAX_SALES_ANALYTICS_OFFSET}"
            ));
        }
        let (account, _) = self.resolve_reporting_account(&identity, input.account.as_deref())?;
        if account.marketplace() != ReportingMarketplace::Ozon {
            return Err(format!(
                "{REPORTING_INVALID_REQUEST}: выбранный кабинет должен относиться к Ozon"
            ));
        }
        self.reporting_reader
            .sales_analytics(
                &account,
                SalesAnalyticsQuery {
                    date_from,
                    date_to,
                    group_by: input.group_by,
                    sort_by: input.sort_by,
                    direction: input.direction,
                    limit: input.limit,
                    offset: input.offset,
                },
            )
            .await
            .map(Json)
            .map_err(Self::reporting_error)
    }

    /// Ставит одно фоновое обновление снимка Ozon для разрешённого кабинета.
    /// Параллельные запросы объединяются в одно задание; сам MCP синхронно Ozon не вызывает.
    #[tool(
        name = "ofk_request_ozon_sales_refresh",
        annotations(
            title = "Запросить фоновое обновление Ozon OFK",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn request_ozon_sales_refresh(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReportingOzonSalesRefreshInput>,
    ) -> Result<Json<SalesRefreshStatus>, String> {
        let (_, actor) = self.access_context(&identity)?;
        let (account, _) = self.resolve_reporting_account(&identity, input.account.as_deref())?;
        if account.marketplace() != ReportingMarketplace::Ozon {
            return Err(format!(
                "{REPORT_REFRESH_INVALID_REQUEST}: выбранный кабинет должен относиться к Ozon"
            ));
        }
        self.refresh_requests
            .request(
                account.account_id(),
                account.marketplace(),
                &actor.id,
                crate::reporting::business_date(Utc::now()),
            )
            .await
            .map(Json)
            .map_err(Self::refresh_request_error)
    }

    /// Показывает состояние последнего фонового обновления Ozon из внутренней очереди.
    /// Метод не обращается к Ozon и не создаёт новое задание.
    #[tool(
        name = "ofk_ozon_sales_refresh_status",
        annotations(
            title = "Статус фонового обновления Ozon OFK",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn ozon_sales_refresh_status(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReportingOzonSalesRefreshInput>,
    ) -> Result<Json<SalesRefreshStatus>, String> {
        let (account, _) = self.resolve_reporting_account(&identity, input.account.as_deref())?;
        if account.marketplace() != ReportingMarketplace::Ozon {
            return Err(format!(
                "{REPORT_REFRESH_INVALID_REQUEST}: выбранный кабинет должен относиться к Ozon"
            ));
        }
        self.refresh_requests
            .status(account.account_id(), account.marketplace())
            .await
            .map(Json)
            .map_err(Self::refresh_request_error)
    }

    /// Ставит единое фоновое обновление снимков Ozon или Wildberries для разрешённого кабинета.
    /// Маркетплейс берётся из серверного реестра, а не из недоверенного аргумента модели.
    #[tool(
        name = "ofk_request_marketplace_sales_refresh",
        annotations(
            title = "Запросить фоновое обновление маркетплейса OFK",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn request_marketplace_sales_refresh(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReportingOzonSalesRefreshInput>,
    ) -> Result<Json<SalesRefreshStatus>, String> {
        let (_, actor) = self.access_context(&identity)?;
        let (account, _) = self.resolve_reporting_account(&identity, input.account.as_deref())?;
        self.refresh_requests
            .request(
                account.account_id(),
                account.marketplace(),
                &actor.id,
                crate::reporting::business_date(Utc::now()),
            )
            .await
            .map(Json)
            .map_err(Self::refresh_request_error)
    }

    /// Показывает состояние последнего durable refresh Ozon или Wildberries.
    /// Никаких внешних API-вызовов этот метод не выполняет.
    #[tool(
        name = "ofk_marketplace_sales_refresh_status",
        annotations(
            title = "Статус фонового обновления маркетплейса OFK",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn marketplace_sales_refresh_status(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReportingOzonSalesRefreshInput>,
    ) -> Result<Json<SalesRefreshStatus>, String> {
        let (account, _) = self.resolve_reporting_account(&identity, input.account.as_deref())?;
        self.refresh_requests
            .status(account.account_id(), account.marketplace())
            .await
            .map(Json)
            .map_err(Self::refresh_request_error)
    }

    /// Возвращает до пяти детерминированных рекомендаций по опубликованному снимку.
    /// Пороговые значения задаются серверной политикой и не принимаются от модели.
    #[tool(
        name = "ofk_manager_actions",
        annotations(
            title = "Приоритетные действия менеджера OFK",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn reporting_manager_actions(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReportingManagerActionsInput>,
    ) -> Result<Json<ManagerActionsResult>, String> {
        let cutoff = parse_reporting_cutoff(input.cutoff_at.as_deref())?;
        let (account, role) =
            self.resolve_reporting_account(&identity, input.account.as_deref())?;
        Self::authorize_reporting_details_for_role(role)?;
        self.reporting_reader
            .manager_actions(&account, cutoff)
            .await
            .map(Json)
            .map_err(Self::reporting_error)
    }

    /// Показывает администратору каталог уже сформированных неизменяемых отчётов.
    /// Метод не раскрывает адреса, provider ID, пути, хэши, ошибки доставки или содержимое писем.
    #[tool(
        name = "ofk_reports",
        annotations(
            title = "Готовые отчёты OFK",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn reporting_ready_reports(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReportingReadyReportsInput>,
    ) -> Result<Json<ReadyReportsResult>, String> {
        validate_reporting_limit(input.limit, MAX_REPORTING_REPORTS)?;
        let (_, actor) = self.access_context(&identity)?;
        Self::authorize_report_catalog_for_role(actor.role)?;
        self.reporting_reader
            .ready_reports(input.limit)
            .await
            .map(Json)
            .map_err(Self::reporting_error)
    }

    /// Возвращает очищенную структурированную telemetry вызовов без аргументов,
    /// ответов, credentials и vendor payloads. Доступ разрешён только admin.
    #[tool(
        name = "ofk_tool_call_log",
        annotations(
            title = "Журнал вызовов инструментов OFK",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    pub(in crate::server) async fn tool_call_log(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ToolCallLogInput>,
    ) -> Result<Json<ToolCallLogResult>, String> {
        if !(1..=MAX_TOOL_CALL_LOG_ROWS).contains(&input.limit) {
            return Err(format!(
                "{TOOL_TELEMETRY_INVALID_REQUEST}: limit должен быть от 1 до {MAX_TOOL_CALL_LOG_ROWS}"
            ));
        }
        let (_, actor) = self.access_context(&identity)?;
        Self::authorize_report_catalog_for_role(actor.role)?;
        self.tool_telemetry
            .list(input.limit)
            .await
            .map(Json)
            .map_err(Self::tool_telemetry_error)
    }
}
