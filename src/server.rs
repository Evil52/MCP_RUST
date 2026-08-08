use chrono::{NaiveDate, Utc};
use rmcp::{
    Json, ServerHandler,
    handler::server::{
        common::{AsRequestContext, FromContextPart},
        router::tool::ToolRouter,
        wrapper::Parameters,
    },
    model::{Implementation, ServerCapabilities, ServerInfo},
    schemars::JsonSchema,
    tool, tool_handler, tool_router,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    auth::AuthenticatedActor,
    config::{Actor, Marketplace, RegistrySource, Role, StoreId},
    ozon::OzonClient,
};

const MAX_ANALYTICS_PERIOD_DAYS: i64 = 366;
const OZON_TOOL_FAILURE: &str = "OZON_TOOL_CALL_FAILED";
const ACCESS_DENIED: &str = "ACCESS_DENIED";

fn config_error(error: anyhow::Error) -> String {
    format!("MCP_ACCESS_CONFIG_ERROR: {error}")
}

#[derive(Debug, Clone)]
pub struct OzonMcp {
    client: OzonClient,
    default_actor_id: Option<String>,
    registry: RegistrySource,
    tool_router: ToolRouter<Self>,
}

impl OzonMcp {
    pub fn new(client: OzonClient, actor_id: String, registry: RegistrySource) -> Self {
        Self {
            client,
            default_actor_id: Some(actor_id),
            registry,
            tool_router: Self::tool_router(),
        }
    }

    pub fn new_authenticated(client: OzonClient, registry: RegistrySource) -> Self {
        Self {
            client,
            default_actor_id: None,
            registry,
            tool_router: Self::tool_router(),
        }
    }

    fn access_context(
        &self,
        identity: &RequestIdentity,
    ) -> Result<(crate::config::AccessRegistry, Actor), String> {
        let registry = self.registry.load().map_err(config_error)?;
        let actor_id = identity
            .actor_id
            .as_deref()
            .or(self.default_actor_id.as_deref())
            .ok_or_else(|| "ACCESS_DENIED: отсутствует проверенная идентичность".to_owned())?;
        let actor = registry.actor(actor_id).map_err(config_error)?.clone();
        Ok((registry, actor))
    }

    fn authorize_store(&self, identity: &RequestIdentity, store: &StoreId) -> Result<(), String> {
        let (registry, actor) = self.access_context(identity)?;
        if actor.can_access_store(store, &registry) {
            Ok(())
        } else {
            Err(format!(
                "{ACCESS_DENIED}: {} ({}) не имеет доступа к магазину {}. Не пытайтесь обойти ограничение другим инструментом или идентификатором.",
                actor.name, actor.role, store
            ))
        }
    }

    fn actor_status(actor: &Actor) -> ActorStatus {
        ActorStatus {
            id: actor.id.clone(),
            name: actor.name.clone(),
            role: actor.role,
        }
    }

    async fn request(
        &self,
        identity: &RequestIdentity,
        store: StoreId,
        endpoint: &'static str,
        payload: Value,
    ) -> Result<Json<OzonResult>, String> {
        self.authorize_store(identity, &store)?;
        let data = self
            .client
            .post(&store, endpoint, payload)
            .await
            .map_err(|error| {
                format!(
                    "{OZON_TOOL_FAILURE}: {error}. Остановите текущую операцию: не вызывайте автоматически другие инструменты или магазины Ozon и не заявляйте о прямом доступе к Ozon. Сообщите пользователю об ошибке и дождитесь нового явного запроса с подключённым OzonOFK."
                )
            })?;
        Ok(Json(OzonResult {
            store,
            endpoint,
            fetched_at: Utc::now().to_rfc3339(),
            data,
        }))
    }

    async fn product_list(
        &self,
        identity: &RequestIdentity,
        input: ProductFilterInput,
        endpoint: &'static str,
    ) -> Result<Json<OzonResult>, String> {
        validate_limit(input.limit, 1_000)?;
        self.request(
            identity,
            input.store,
            endpoint,
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

    async fn posting_list(
        &self,
        identity: &RequestIdentity,
        input: PostingListInput,
        kind: PostingKind,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_and_expand_dates(&input.date_from, &input.date_to, 366)?;
        validate_limit(input.limit, 1_000)?;
        let mut payload = json!({
            "dir": input.direction,
            "filter": { "since": from, "to": to, "status": input.status },
            "limit": input.limit,
            "offset": input.offset,
        });
        if kind == PostingKind::Fbo {
            let payload = payload
                .as_object_mut()
                .expect("posting payload is an object");
            payload.insert("translit".to_owned(), json!(false));
            payload.insert(
                "with".to_owned(),
                json!({ "analytics_data": true, "financial_data": true }),
            );
        }
        self.request(identity, input.store, kind.endpoint(), payload)
            .await
    }
}

#[derive(Debug, Clone, Default)]
pub struct RequestIdentity {
    actor_id: Option<String>,
}

impl RequestIdentity {
    #[cfg(test)]
    fn dev() -> Self {
        Self::default()
    }

    #[cfg(test)]
    fn authenticated(actor_id: &str) -> Self {
        Self {
            actor_id: Some(actor_id.to_owned()),
        }
    }
}

impl<C> FromContextPart<C> for RequestIdentity
where
    C: AsRequestContext,
{
    fn from_context_part(context: &mut C) -> Result<Self, rmcp::ErrorData> {
        let actor_id = context
            .as_request_context()
            .extensions
            .get::<axum::http::request::Parts>()
            .and_then(|parts| parts.extensions.get::<AuthenticatedActor>())
            .map(|actor| actor.actor_id.clone());
        Ok(Self { actor_id })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PostingKind {
    Fbs,
    Fbo,
}

impl PostingKind {
    fn endpoint(self) -> &'static str {
        match self {
            Self::Fbs => "/v3/posting/fbs/list",
            Self::Fbo => "/v2/posting/fbo/list",
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct OzonResult {
    pub store: StoreId,
    pub endpoint: &'static str,
    pub fetched_at: String,
    pub data: Value,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct StoresResult {
    pub actor: ActorStatus,
    pub default_store: Option<StoreId>,
    pub access_mode: &'static str,
    pub stores: Vec<StoreStatus>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ActorStatus {
    pub id: String,
    pub name: String,
    pub role: Role,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct StoreStatus {
    pub id: StoreId,
    pub name: String,
    pub seller_client_id: String,
    pub manager: String,
    pub configured: bool,
    pub client_id_env: String,
    pub api_key_env: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct AccountsResult {
    pub actor: ActorStatus,
    pub accounts: Vec<AccountStatus>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MembersResult {
    pub actor: ActorStatus,
    pub members: Vec<MemberStatus>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MemberStatus {
    pub id: String,
    pub name: String,
    pub role: Role,
    pub account_ids: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct AccountStatus {
    pub id: String,
    pub organization: String,
    pub marketplace: Marketplace,
    pub seller_client_id: String,
    pub manager: String,
    pub integration_status: &'static str,
    pub configured: bool,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Visibility {
    #[default]
    All,
    Visible,
    Invisible,
    EmptyStock,
    ReadyToSupply,
    StateFailed,
    Moderate,
    Declined,
    Disabled,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnalyticsMetric {
    Revenue,
    OrderedUnits,
    HitsView,
    HitsViewSearch,
    HitsViewPdp,
    HitsTocart,
    SessionView,
    ConvTocartPercent,
    Returns,
    Cancellations,
    DeliveredUnits,
    AdvSumAll,
    PositionCategory,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnalyticsDimension {
    Sku,
    Spu,
    Day,
    Week,
    Month,
    Year,
    Brand,
    Category1,
    Category2,
    Category3,
    Category4,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AnalyticsInput {
    #[serde(default)]
    pub store: StoreId,
    #[schemars(description = "Начало периода в формате YYYY-MM-DD")]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD")]
    pub date_to: String,
    #[schemars(description = "От одной до 10 метрик Ozon")]
    pub metrics: Vec<AnalyticsMetric>,
    #[schemars(description = "От одного до двух измерений: например sku и day")]
    pub dimensions: Vec<AnalyticsDimension>,
    #[serde(default = "default_analytics_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
    pub sort_by: Option<AnalyticsMetric>,
    #[serde(default)]
    pub sort_direction: SortDirection,
}

fn default_analytics_limit() -> u32 {
    1_000
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ProductFilterInput {
    #[serde(default)]
    pub store: StoreId,
    #[serde(default)]
    pub offer_ids: Vec<String>,
    #[serde(default)]
    pub product_ids: Vec<String>,
    #[serde(default)]
    pub visibility: Visibility,
    #[serde(default = "default_product_limit")]
    pub limit: u32,
    pub cursor: Option<String>,
}

fn default_product_limit() -> u32 {
    100
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct TurnoverInput {
    #[serde(default)]
    pub store: StoreId,
    #[serde(default)]
    pub skus: Vec<String>,
    #[serde(default = "default_product_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct PostingListInput {
    #[serde(default)]
    pub store: StoreId,
    #[schemars(description = "Начало периода в формате YYYY-MM-DD")]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD")]
    pub date_to: String,
    #[serde(default)]
    pub status: String,
    #[serde(default = "default_posting_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
    #[serde(default)]
    pub direction: SortDirection,
}

fn default_posting_limit() -> u32 {
    100
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReturnSchema {
    #[default]
    Fbo,
    Fbs,
    Rfbs,
}

impl ReturnSchema {
    fn as_ozon_str(self) -> &'static str {
        match self {
            Self::Fbo => "FBO",
            Self::Fbs => "FBS",
            Self::Rfbs => "RFBS",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReturnsInput {
    #[serde(default)]
    pub store: StoreId,
    #[schemars(description = "Начало периода изменения статуса в формате YYYY-MM-DD")]
    pub date_from: String,
    #[schemars(description = "Конец периода изменения статуса в формате YYYY-MM-DD")]
    pub date_to: String,
    #[serde(default)]
    pub return_schema: ReturnSchema,
    #[serde(default)]
    pub offer_id: String,
    #[serde(default)]
    pub posting_numbers: Vec<String>,
    #[serde(default = "default_returns_limit")]
    pub limit: u32,
    #[serde(default)]
    pub last_id: u64,
}

fn default_returns_limit() -> u32 {
    500
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FinanceInput {
    #[serde(default)]
    pub store: StoreId,
    #[schemars(description = "Начало периода в формате YYYY-MM-DD")]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD")]
    pub date_to: String,
    #[serde(default)]
    pub posting_number: String,
    #[serde(default)]
    pub operation_types: Vec<String>,
    #[serde(default = "default_transaction_type")]
    pub transaction_type: String,
    #[serde(default = "default_page")]
    pub page: u32,
    #[serde(default = "default_finance_page_size")]
    pub page_size: u32,
}

fn default_transaction_type() -> String {
    "all".to_owned()
}

fn default_page() -> u32 {
    1
}

fn default_finance_page_size() -> u32 {
    1_000
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct StoreOnlyInput {
    #[serde(default)]
    pub store: StoreId,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RatingHistoryInput {
    #[serde(default)]
    pub store: StoreId,
    #[schemars(description = "Начало периода в формате YYYY-MM-DD")]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD")]
    pub date_to: String,
    #[serde(default)]
    pub ratings: Vec<String>,
    #[serde(default = "default_true")]
    pub with_premium_scores: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReviewsInput {
    #[serde(default)]
    pub store: StoreId,
    #[serde(default = "default_reviews_limit")]
    pub limit: u32,
    #[serde(default)]
    pub last_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub direction: SortDirection,
}

fn default_reviews_limit() -> u32 {
    100
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct QuestionsInput {
    #[serde(default)]
    pub store: StoreId,
    #[schemars(description = "Начало периода в формате YYYY-MM-DD")]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD")]
    pub date_to: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub last_id: String,
}

#[tool_router]
impl OzonMcp {
    /// Показывает локально настроенные магазины и наличие ключей, не раскрывая секреты. Не проверяет сеть или авторизацию Ozon API.
    #[tool(
        name = "ozon_stores_status",
        annotations(title = "Статус магазинов Ozon", read_only_hint = true)
    )]
    async fn stores_status(&self, identity: RequestIdentity) -> Result<Json<StoresResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        let accessible_stores: Vec<_> = registry
            .accounts
            .iter()
            .filter_map(|account| account.ozon.as_ref().map(|ozon| (account, ozon)))
            .filter(|(account, _)| actor.can_access_account(account))
            .collect();
        Ok(Json(StoresResult {
            actor: Self::actor_status(&actor),
            default_store: accessible_stores
                .first()
                .map(|(_, ozon)| ozon.store_id.clone()),
            access_mode: "server-side RBAC, read-only allowlist",
            stores: accessible_stores
                .into_iter()
                .map(|(account, ozon)| {
                    let manager = registry
                        .actor(&account.manager_id)
                        .expect("validated manager");
                    StoreStatus {
                        id: ozon.store_id.clone(),
                        name: account.organization.clone(),
                        seller_client_id: account.seller_client_id.clone(),
                        manager: manager.name.clone(),
                        configured: self.client.is_configured(&ozon.store_id),
                        client_id_env: ozon.client_id_env.clone(),
                        api_key_env: ozon.api_key_env.clone(),
                    }
                })
                .collect(),
        }))
    }

    /// Показывает доступные текущему пользователю кабинеты Ozon и реестр кабинетов Wildberries. WB пока зарегистрированы только в справочнике и не подключены к API.
    #[tool(
        name = "marketplace_accounts",
        annotations(title = "Доступные кабинеты маркетплейсов", read_only_hint = true)
    )]
    async fn marketplace_accounts(
        &self,
        identity: RequestIdentity,
    ) -> Result<Json<AccountsResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        Ok(Json(AccountsResult {
            actor: Self::actor_status(&actor),
            accounts: registry
                .accounts
                .iter()
                .filter(|account| actor.can_access_account(account))
                .map(|account| {
                    let (integration_status, configured) = match &account.ozon {
                        Some(ozon) => (
                            "read_only_ozon_api",
                            self.client.is_configured(&ozon.store_id),
                        ),
                        None => ("directory_only", false),
                    };
                    let manager = registry
                        .actor(&account.manager_id)
                        .expect("validated manager");
                    AccountStatus {
                        id: account.id.clone(),
                        organization: account.organization.clone(),
                        marketplace: account.marketplace,
                        seller_client_id: account.seller_client_id.clone(),
                        manager: manager.name.clone(),
                        integration_status,
                        configured,
                    }
                })
                .collect(),
        }))
    }

    /// Показывает сотрудников, их роли и доступные им кабинеты. Администратор видит весь реестр; остальные пользователи видят только собственную запись.
    #[tool(
        name = "list_members",
        annotations(title = "Сотрудники и роли OzonOFK", read_only_hint = true)
    )]
    async fn list_members(&self, identity: RequestIdentity) -> Result<Json<MembersResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        let members = registry
            .actors
            .iter()
            .filter(|member| actor.role == Role::Admin || member.id == actor.id)
            .map(|member| {
                let mut account_ids: Vec<_> = registry
                    .accounts
                    .iter()
                    .filter(|account| member.can_access_account(account))
                    .map(|account| account.id.clone())
                    .collect();
                account_ids.sort();
                MemberStatus {
                    id: member.id.clone(),
                    name: member.name.clone(),
                    role: member.role,
                    account_ids,
                }
            })
            .collect();
        Ok(Json(MembersResult {
            actor: Self::actor_status(&actor),
            members,
        }))
    }

    /// Получает продажи и воронку Ozon по периоду, SKU, бренду, категории или времени.
    #[tool(
        name = "ozon_analytics",
        annotations(title = "Аналитика продаж Ozon", read_only_hint = true)
    )]
    async fn analytics(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<AnalyticsInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_date_range(&input.date_from, &input.date_to, MAX_ANALYTICS_PERIOD_DAYS)?;
        validate_count("metrics", input.metrics.len(), 1, 10)?;
        validate_count("dimensions", input.dimensions.len(), 1, 2)?;
        validate_limit(input.limit, 1_000)?;

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

    /// Возвращает текущие остатки товаров Ozon с фильтрацией по offer_id или product_id.
    #[tool(
        name = "ozon_product_stocks",
        annotations(title = "Остатки товаров Ozon", read_only_hint = true)
    )]
    async fn product_stocks(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ProductFilterInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.product_list(&identity, input, "/v4/product/info/stocks")
            .await
    }

    /// Возвращает текущие цены и скидки товаров Ozon без возможности их изменить.
    #[tool(
        name = "ozon_product_prices",
        annotations(title = "Цены товаров Ozon", read_only_hint = true)
    )]
    async fn product_prices(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ProductFilterInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.product_list(&identity, input, "/v5/product/info/prices")
            .await
    }

    /// Получает показатели оборачиваемости и запасов по SKU.
    #[tool(
        name = "ozon_stock_turnover",
        annotations(title = "Оборачиваемость запасов Ozon", read_only_hint = true)
    )]
    async fn stock_turnover(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<TurnoverInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_limit(input.limit, 1_000)?;
        self.request(
            &identity,
            input.store,
            "/v1/analytics/turnover/stocks",
            json!({ "limit": input.limit, "offset": input.offset, "sku": input.skus }),
        )
        .await
    }

    /// Получает список отправлений FBS/rFBS за период и их текущие статусы.
    #[tool(
        name = "ozon_fbs_postings",
        annotations(title = "Отправления FBS Ozon", read_only_hint = true)
    )]
    async fn fbs_postings(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PostingListInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.posting_list(&identity, input, PostingKind::Fbs).await
    }

    /// Получает список отправлений FBO за период вместе с аналитическими и финансовыми полями.
    #[tool(
        name = "ozon_fbo_postings",
        annotations(title = "Отправления FBO Ozon", read_only_hint = true)
    )]
    async fn fbo_postings(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<PostingListInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.posting_list(&identity, input, PostingKind::Fbo).await
    }

    /// Получает возвраты FBO/FBS за период изменения статуса.
    #[tool(
        name = "ozon_returns",
        annotations(title = "Возвраты Ozon", read_only_hint = true)
    )]
    async fn returns(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReturnsInput>,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_and_expand_dates(&input.date_from, &input.date_to, 366)?;
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

    /// Получает детальные финансовые транзакции: продажи, комиссии, логистику и услуги.
    #[tool(
        name = "ozon_finance_transactions",
        annotations(title = "Финансовые транзакции Ozon", read_only_hint = true)
    )]
    async fn finance_transactions(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FinanceInput>,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_and_expand_dates(&input.date_from, &input.date_to, 366)?;
        validate_limit(input.page_size, 1_000)?;
        if input.page == 0 {
            return Err("page должен быть не меньше 1".to_owned());
        }
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

    /// Получает агрегированные финансовые итоги Ozon за период.
    #[tool(
        name = "ozon_finance_totals",
        annotations(title = "Финансовые итоги Ozon", read_only_hint = true)
    )]
    async fn finance_totals(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FinanceInput>,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_and_expand_dates(&input.date_from, &input.date_to, 366)?;
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

    /// Возвращает текущую сводку рейтингов и показателей качества продавца.
    #[tool(
        name = "ozon_seller_rating",
        annotations(title = "Рейтинг продавца Ozon", read_only_hint = true)
    )]
    async fn seller_rating(
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
    async fn seller_rating_history(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<RatingHistoryInput>,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_and_expand_dates(&input.date_from, &input.date_to, 366)?;
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
    async fn reviews(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<ReviewsInput>,
    ) -> Result<Json<OzonResult>, String> {
        validate_limit(input.limit, 100)?;
        self.request(
            &identity,
            input.store,
            "/v1/review/list",
            json!({
                "last_id": input.last_id,
                "limit": input.limit,
                "sort_dir": input.direction,
                "status": input.status,
            }),
        )
        .await
    }

    /// Получает вопросы покупателей за период; метод Ozon находится в beta.
    #[tool(
        name = "ozon_questions",
        annotations(title = "Вопросы покупателей Ozon", read_only_hint = true)
    )]
    async fn questions(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<QuestionsInput>,
    ) -> Result<Json<OzonResult>, String> {
        let (from, to) = validate_and_expand_dates(&input.date_from, &input.date_to, 366)?;
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

#[tool_handler(router = self.tool_router)]
impl ServerHandler for OzonMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("mcp-ozon", env!("CARGO_PKG_VERSION"))
                    .with_title("Ozon Seller Analytics"),
            )
            .with_instructions(
                "Read-only MCP для аналитики магазинов Ozon. Все инструменты только получают данные. \
                 Сервер не изменяет товары, цены, остатки, заказы, отзывы, вопросы или рекламу. \
                 Доступ к магазинам проверяется сервером по подтверждённой идентичности: JWT/OIDC \
                 в защищённом режиме или MCP_ACTOR_ID в локальном dev-режиме. Менеджер видит только \
                 закреплённый кабинет, администратор — все кабинеты. Не запрашивайте роль или имя \
                 пользователя через аргументы инструмента и не пытайтесь обходить ACCESS_DENIED. \
                 Вызывайте инструменты только когда OzonOFK доступен в текущем чате и пользователь \
                 явно разрешил текущий вызов согласно настройкам ChatGPT. Никогда не заявляйте о \
                 прямом доступе к Ozon без успешного результата инструмента OzonOFK. Если доступ \
                 отклонён, коннектор недоступен или любой инструмент завершился ошибкой, остановитесь: \
                 не вызывайте автоматически другой инструмент или магазин Ozon и дождитесь нового \
                 явного запроса пользователя. ozon_stores_status показывает только локальную \
                 конфигурацию и не подтверждает доступность Ozon API.",
            )
    }
}

fn parse_date(value: &str, field: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| format!("{field} должен иметь формат YYYY-MM-DD"))
}

fn validate_date_range(date_from: &str, date_to: &str, max_days: i64) -> Result<(), String> {
    let from = parse_date(date_from, "date_from")?;
    let to = parse_date(date_to, "date_to")?;
    if to < from {
        return Err("date_to не может быть раньше date_from".to_owned());
    }
    if (to - from).num_days() + 1 > max_days {
        return Err(format!("период не может превышать {max_days} дней"));
    }
    Ok(())
}

fn validate_and_expand_dates(
    date_from: &str,
    date_to: &str,
    max_days: i64,
) -> Result<(String, String), String> {
    validate_date_range(date_from, date_to, max_days)?;
    Ok((
        format!("{date_from}T00:00:00.000Z"),
        format!("{date_to}T23:59:59.999Z"),
    ))
}

fn validate_limit(limit: u32, maximum: u32) -> Result<(), String> {
    if !(1..=maximum).contains(&limit) {
        return Err(format!("limit должен быть от 1 до {maximum}"));
    }
    Ok(())
}

fn validate_count(field: &str, count: usize, minimum: usize, maximum: usize) -> Result<(), String> {
    if !(minimum..=maximum).contains(&count) {
        return Err(format!(
            "{field} должен содержать от {minimum} до {maximum} значений"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
            mpsc,
        },
        time::Duration,
    };

    use super::*;
    use crate::test_support::mock_http;
    use axum::Extension;
    use rmcp::transport::{
        StreamableHttpServerConfig, StreamableHttpService,
        streamable_http_server::session::local::LocalSessionManager,
    };

    static REGISTRY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn registry_source() -> RegistrySource {
        let sequence = REGISTRY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mcp-ozon-access-{}-{sequence}.json",
            std::process::id()
        ));
        fs::write(&path, r#"{
          "version": 1,
          "actors": [
            {"id":"rustam_magasumov","name":"Рустам Магасумов","role":"admin"},
            {"id":"yulia_rogova","name":"Рогова Юлия","role":"manager"}
          ],
          "accounts": [
            {"id":"ofk","organization":"Фурнитура для дома","marketplace":"ozon","seller_client_id":"3165034","manager_id":"rustam_magasumov","ozon":{"store_id":"ofk","client_id_env":"OZON_CLIENT_ID","api_key_env":"OZON_API_KEY"}},
            {"id":"evro","organization":"Евромебелькомплект","marketplace":"ozon","seller_client_id":"881124","manager_id":"yulia_rogova","ozon":{"store_id":"evromebelkomplekt","client_id_env":"EVRO_ID","api_key_env":"EVRO_KEY"}},
            {"id":"wb","organization":"WB directory","marketplace":"wildberries","seller_client_id":"42","manager_id":"rustam_magasumov"}
          ]
        }"#).unwrap();
        RegistrySource::new(path).unwrap()
    }

    fn server() -> OzonMcp {
        OzonMcp::new(
            OzonClient::new(
                "http://127.0.0.1:1".to_owned(),
                Duration::from_secs(1),
                BTreeMap::new(),
            )
            .unwrap(),
            "rustam_magasumov".to_owned(),
            registry_source(),
        )
    }

    fn manager_server(actor: &str) -> OzonMcp {
        OzonMcp::new(
            OzonClient::new(
                "http://127.0.0.1:1".to_owned(),
                Duration::from_secs(1),
                BTreeMap::new(),
            )
            .unwrap(),
            actor.to_owned(),
            registry_source(),
        )
    }

    fn mock_server(expected_requests: usize) -> (OzonMcp, mpsc::Receiver<String>) {
        let responses = vec![(200, r#"{"ok":true}"#.to_owned()); expected_requests];
        let (base_url, receiver) = mock_http(responses);
        let stores = BTreeMap::from([(
            StoreId::from("ofk"),
            crate::config::StoreCredentials {
                client_id: "test-client".to_owned(),
                api_key: "test-key".to_owned(),
            },
        )]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), stores).unwrap();
        (
            OzonMcp::new(client, "rustam_magasumov".to_owned(), registry_source()),
            receiver,
        )
    }

    fn request_path_and_body(request: &str) -> (&str, Value) {
        let path = request
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap();
        let body = request.split_once("\r\n\r\n").unwrap().1;
        (path, serde_json::from_str(body).unwrap())
    }

    #[test]
    fn all_tools_are_read_only_and_described() {
        let tools = server().tool_router.list_all();
        assert!(tools.len() >= 10);
        for tool in tools {
            assert!(!tool.description.as_deref().unwrap_or_default().is_empty());
            assert_eq!(
                tool.annotations
                    .as_ref()
                    .and_then(|annotations| annotations.read_only_hint),
                Some(true),
                "{} must be read-only",
                tool.name
            );
        }
    }

    #[test]
    fn dates_are_validated_and_expanded() {
        assert_eq!(
            validate_and_expand_dates("2026-01-01", "2026-01-31", 366).unwrap(),
            (
                "2026-01-01T00:00:00.000Z".to_owned(),
                "2026-01-31T23:59:59.999Z".to_owned()
            )
        );
        assert!(validate_date_range("2026-02-01", "2026-01-01", 366).is_err());
        assert!(validate_date_range("2024-01-01", "2026-01-01", 366).is_err());
    }

    #[test]
    fn limits_are_bounded() {
        assert!(validate_limit(1, 100).is_ok());
        assert!(validate_limit(100, 100).is_ok());
        assert!(validate_limit(0, 100).is_err());
        assert!(validate_limit(101, 100).is_err());
        assert!(validate_count("items", 1, 1, 2).is_ok());
        assert_eq!(
            validate_count("items", 0, 1, 2).unwrap_err(),
            "items должен содержать от 1 до 2 значений"
        );
    }

    #[test]
    fn ozon_enum_values_are_stable() {
        assert_eq!(ReturnSchema::Fbo.as_ozon_str(), "FBO");
        assert_eq!(ReturnSchema::Fbs.as_ozon_str(), "FBS");
        assert_eq!(ReturnSchema::Rfbs.as_ozon_str(), "RFBS");
        assert_eq!(PostingKind::Fbs.endpoint(), "/v3/posting/fbs/list");
        assert_eq!(PostingKind::Fbo.endpoint(), "/v2/posting/fbo/list");
    }

    #[tokio::test]
    async fn stores_status_and_server_metadata_do_not_expose_secrets() {
        let (server, _) = mock_server(0);
        let status = server
            .stores_status(RequestIdentity::dev())
            .await
            .unwrap()
            .0;
        assert_eq!(status.actor.id, "rustam_magasumov");
        assert_eq!(status.actor.name, "Рустам Магасумов");
        assert_eq!(status.actor.role, Role::Admin);
        assert_eq!(status.default_store, Some(StoreId::from("ofk")));
        assert_eq!(status.access_mode, "server-side RBAC, read-only allowlist");
        assert_eq!(status.stores.len(), 2);
        assert!(status.stores[0].configured);
        assert!(!status.stores[1].configured);
        assert_eq!(status.stores[0].seller_client_id, "3165034");
        assert_eq!(status.stores[0].manager, "Рустам Магасумов");

        let accounts = server
            .marketplace_accounts(RequestIdentity::dev())
            .await
            .unwrap()
            .0;
        assert_eq!(accounts.actor.id, "rustam_magasumov");
        assert_eq!(accounts.accounts.len(), 3);
        assert_eq!(
            accounts.accounts[0].integration_status,
            "read_only_ozon_api"
        );
        assert!(accounts.accounts[0].configured);
        assert_eq!(accounts.accounts[2].integration_status, "directory_only");
        assert!(!accounts.accounts[2].configured);

        let members = server.list_members(RequestIdentity::dev()).await.unwrap().0;
        assert_eq!(members.actor.id, "rustam_magasumov");
        assert_eq!(members.members.len(), 2);
        assert_eq!(members.members[0].role, Role::Admin);
        assert_eq!(members.members[0].account_ids.len(), 3);

        let info = server.get_info();
        assert_eq!(info.server_info.name, "mcp-ozon");
        let instructions = info.instructions.unwrap();
        assert!(instructions.contains("Read-only"));
        assert!(instructions.contains("не вызывайте автоматически другой инструмент"));
        assert!(instructions.contains("без успешного результата инструмента OzonOFK"));
        assert!(instructions.contains("MCP_ACTOR_ID"));
        assert!(instructions.contains("ACCESS_DENIED"));
        assert!(info.capabilities.tools.is_some());
    }

    #[tokio::test]
    async fn managers_only_see_and_access_their_assigned_account() {
        let server = manager_server("yulia_rogova");
        let status = server
            .stores_status(RequestIdentity::dev())
            .await
            .unwrap()
            .0;
        assert_eq!(status.actor.role, Role::Manager);
        assert_eq!(
            status.default_store,
            Some(StoreId::from("evromebelkomplekt"))
        );
        assert_eq!(status.stores.len(), 1);
        assert_eq!(status.stores[0].id, StoreId::from("evromebelkomplekt"));

        let accounts = server
            .marketplace_accounts(RequestIdentity::dev())
            .await
            .unwrap()
            .0;
        assert_eq!(accounts.accounts.len(), 1);
        assert_eq!(accounts.accounts[0].manager, "Рогова Юлия");

        let members = server.list_members(RequestIdentity::dev()).await.unwrap().0;
        assert_eq!(members.members.len(), 1);
        assert_eq!(members.members[0].id, "yulia_rogova");
        assert_eq!(members.members[0].role, Role::Manager);
        assert_eq!(members.members[0].account_ids, vec!["evro".to_owned()]);

        let denied = server
            .seller_rating(
                RequestIdentity::dev(),
                Parameters(StoreOnlyInput {
                    store: StoreId::from("ofk"),
                }),
            )
            .await
            .err()
            .unwrap();
        assert!(denied.starts_with(ACCESS_DENIED));
        assert!(denied.contains("Рогова Юлия"));
        assert!(denied.contains("магазину ofk"));

        let allowed_but_unconfigured = server
            .seller_rating(
                RequestIdentity::dev(),
                Parameters(StoreOnlyInput {
                    store: StoreId::from("evromebelkomplekt"),
                }),
            )
            .await
            .err()
            .unwrap();
        assert!(allowed_but_unconfigured.starts_with(OZON_TOOL_FAILURE));
    }

    #[tokio::test]
    async fn jwt_mode_is_fail_closed_and_uses_the_authenticated_actor() {
        let client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
            BTreeMap::new(),
        )
        .unwrap();
        let server = OzonMcp::new_authenticated(client, registry_source());

        let denied = server
            .marketplace_accounts(RequestIdentity::dev())
            .await
            .err()
            .unwrap();
        assert!(denied.starts_with(ACCESS_DENIED));

        let manager = server
            .marketplace_accounts(RequestIdentity::authenticated("yulia_rogova"))
            .await
            .unwrap()
            .0;
        assert_eq!(manager.actor.id, "yulia_rogova");
        assert_eq!(manager.accounts.len(), 1);
        assert_eq!(manager.accounts[0].id, "evro");
    }

    #[tokio::test]
    async fn streamable_http_propagates_the_verified_actor_to_mcp_tools() {
        let client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
            BTreeMap::new(),
        )
        .unwrap();
        let server = Arc::new(OzonMcp::new_authenticated(client, registry_source()));
        let service: StreamableHttpService<OzonMcp, LocalSessionManager> =
            StreamableHttpService::new(
                move || Ok((*server).clone()),
                Default::default(),
                StreamableHttpServerConfig::default()
                    .with_legacy_session_mode(false)
                    .with_json_response(true),
            );
        let router = axum::Router::new()
            .nest_service("/mcp", service)
            .layer(Extension(AuthenticatedActor {
                actor_id: "rustam_magasumov".to_owned(),
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let response = reqwest::Client::new()
            .post(format!("http://{address}/mcp"))
            .header("accept", "application/json, text/event-stream")
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "marketplace_accounts", "arguments": {}}
            }))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let body = response.text().await.unwrap();
        assert!(body.contains("rustam_magasumov"), "{body}");
        assert!(!body.contains(ACCESS_DENIED), "{body}");
        task.abort();
    }

    #[tokio::test]
    async fn access_changes_are_loaded_without_restarting_the_server() {
        let server = manager_server("yulia_rogova");
        assert_eq!(
            server
                .marketplace_accounts(RequestIdentity::dev())
                .await
                .unwrap()
                .0
                .accounts
                .len(),
            1
        );

        let mut document: Value =
            serde_json::from_str(&fs::read_to_string(server.registry.path()).unwrap()).unwrap();
        document["actors"][1]["role"] = json!("admin");
        fs::write(
            server.registry.path(),
            serde_json::to_vec_pretty(&document).unwrap(),
        )
        .unwrap();

        assert_eq!(
            server
                .marketplace_accounts(RequestIdentity::dev())
                .await
                .unwrap()
                .0
                .accounts
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn invalid_hot_reloaded_registry_returns_a_safe_mcp_error() {
        let server = server();
        fs::write(server.registry.path(), "{").unwrap();
        let result = server.marketplace_accounts(RequestIdentity::dev()).await;
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(error.starts_with("MCP_ACCESS_CONFIG_ERROR:"));
        assert!(error.contains("неверный JSON"));
    }

    #[tokio::test]
    async fn every_read_only_tool_calls_the_expected_ozon_endpoint() {
        let (server, requests) = mock_server(13);
        let mut results = Vec::new();

        results.push(
            server
                .analytics(
                    RequestIdentity::dev(),
                    Parameters(AnalyticsInput {
                        store: StoreId::from("ofk"),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-31".to_owned(),
                        metrics: vec![AnalyticsMetric::Revenue],
                        dimensions: vec![AnalyticsDimension::Sku],
                        limit: 25,
                        offset: 5,
                        sort_by: Some(AnalyticsMetric::Revenue),
                        sort_direction: SortDirection::Desc,
                    }),
                )
                .await
                .unwrap()
                .0,
        );
        for tool_result in [
            server
                .product_stocks(
                    RequestIdentity::dev(),
                    Parameters(ProductFilterInput {
                        store: StoreId::from("ofk"),
                        offer_ids: vec!["offer-1".to_owned()],
                        product_ids: Vec::new(),
                        visibility: Visibility::Visible,
                        limit: 10,
                        cursor: Some("cursor-1".to_owned()),
                    }),
                )
                .await,
            server
                .product_prices(
                    RequestIdentity::dev(),
                    Parameters(ProductFilterInput {
                        store: StoreId::from("ofk"),
                        offer_ids: Vec::new(),
                        product_ids: vec!["123".to_owned()],
                        visibility: Visibility::All,
                        limit: 20,
                        cursor: None,
                    }),
                )
                .await,
        ] {
            results.push(tool_result.unwrap().0);
        }
        results.push(
            server
                .stock_turnover(
                    RequestIdentity::dev(),
                    Parameters(TurnoverInput {
                        store: StoreId::from("ofk"),
                        skus: vec!["sku-1".to_owned()],
                        limit: 30,
                        offset: 2,
                    }),
                )
                .await
                .unwrap()
                .0,
        );
        for result in [
            server
                .fbs_postings(
                    RequestIdentity::dev(),
                    Parameters(PostingListInput {
                        store: StoreId::from("ofk"),
                        date_from: "2026-02-01".to_owned(),
                        date_to: "2026-02-02".to_owned(),
                        status: "awaiting_packaging".to_owned(),
                        limit: 40,
                        offset: 3,
                        direction: SortDirection::Asc,
                    }),
                )
                .await,
            server
                .fbo_postings(
                    RequestIdentity::dev(),
                    Parameters(PostingListInput {
                        store: StoreId::from("ofk"),
                        date_from: "2026-02-01".to_owned(),
                        date_to: "2026-02-02".to_owned(),
                        status: String::new(),
                        limit: 50,
                        offset: 0,
                        direction: SortDirection::Desc,
                    }),
                )
                .await,
        ] {
            results.push(result.unwrap().0);
        }
        results.push(
            server
                .returns(
                    RequestIdentity::dev(),
                    Parameters(ReturnsInput {
                        store: StoreId::from("ofk"),
                        date_from: "2026-03-01".to_owned(),
                        date_to: "2026-03-03".to_owned(),
                        return_schema: ReturnSchema::Rfbs,
                        offer_id: "offer-2".to_owned(),
                        posting_numbers: vec!["posting-1".to_owned()],
                        limit: 60,
                        last_id: 7,
                    }),
                )
                .await
                .unwrap()
                .0,
        );
        for result in [
            server
                .finance_transactions(
                    RequestIdentity::dev(),
                    Parameters(FinanceInput {
                        store: StoreId::from("ofk"),
                        date_from: "2026-04-01".to_owned(),
                        date_to: "2026-04-30".to_owned(),
                        posting_number: "posting-2".to_owned(),
                        operation_types: vec!["OperationAgentDeliveredToCustomer".to_owned()],
                        transaction_type: "orders".to_owned(),
                        page: 2,
                        page_size: 70,
                    }),
                )
                .await,
            server
                .finance_totals(
                    RequestIdentity::dev(),
                    Parameters(FinanceInput {
                        store: StoreId::from("ofk"),
                        date_from: "2026-04-01".to_owned(),
                        date_to: "2026-04-30".to_owned(),
                        posting_number: String::new(),
                        operation_types: Vec::new(),
                        transaction_type: "all".to_owned(),
                        page: 1,
                        page_size: 100,
                    }),
                )
                .await,
        ] {
            results.push(result.unwrap().0);
        }
        results.push(
            server
                .seller_rating(
                    RequestIdentity::dev(),
                    Parameters(StoreOnlyInput {
                        store: StoreId::from("ofk"),
                    }),
                )
                .await
                .unwrap()
                .0,
        );
        results.push(
            server
                .seller_rating_history(
                    RequestIdentity::dev(),
                    Parameters(RatingHistoryInput {
                        store: StoreId::from("ofk"),
                        date_from: "2026-05-01".to_owned(),
                        date_to: "2026-05-10".to_owned(),
                        ratings: vec!["rating_on_time".to_owned()],
                        with_premium_scores: false,
                    }),
                )
                .await
                .unwrap()
                .0,
        );
        results.push(
            server
                .reviews(
                    RequestIdentity::dev(),
                    Parameters(ReviewsInput {
                        store: StoreId::from("ofk"),
                        limit: 80,
                        last_id: "review-cursor".to_owned(),
                        status: "UNPROCESSED".to_owned(),
                        direction: SortDirection::Desc,
                    }),
                )
                .await
                .unwrap()
                .0,
        );
        results.push(
            server
                .questions(
                    RequestIdentity::dev(),
                    Parameters(QuestionsInput {
                        store: StoreId::from("ofk"),
                        date_from: "2026-06-01".to_owned(),
                        date_to: "2026-06-02".to_owned(),
                        status: "NEW".to_owned(),
                        last_id: "question-cursor".to_owned(),
                    }),
                )
                .await
                .unwrap()
                .0,
        );

        let expected_paths = [
            "/v1/analytics/data",
            "/v4/product/info/stocks",
            "/v5/product/info/prices",
            "/v1/analytics/turnover/stocks",
            "/v3/posting/fbs/list",
            "/v2/posting/fbo/list",
            "/v1/returns/list",
            "/v3/finance/transaction/list",
            "/v3/finance/transaction/totals",
            "/v1/rating/summary",
            "/v1/rating/history",
            "/v1/review/list",
            "/v1/question/list",
        ];
        assert_eq!(results.len(), expected_paths.len());
        for (result, expected_path) in results.iter().zip(expected_paths) {
            assert_eq!(result.endpoint, expected_path);
            assert_eq!(result.store, StoreId::from("ofk"));
            assert!(!result.fetched_at.is_empty());
            assert_eq!(result.data, json!({ "ok": true }));
            let request = requests.recv_timeout(Duration::from_secs(3)).unwrap();
            let (actual_path, _) = request_path_and_body(&request);
            assert_eq!(actual_path, expected_path);
        }
    }

    #[tokio::test]
    async fn invalid_tool_inputs_fail_before_calling_ozon() {
        let server = server();
        assert!(
            server
                .analytics(
                    RequestIdentity::dev(),
                    Parameters(AnalyticsInput {
                        store: StoreId::from("ofk"),
                        date_from: "invalid".to_owned(),
                        date_to: "2026-01-01".to_owned(),
                        metrics: vec![AnalyticsMetric::Revenue],
                        dimensions: vec![AnalyticsDimension::Sku],
                        limit: 1,
                        offset: 0,
                        sort_by: None,
                        sort_direction: SortDirection::Asc,
                    })
                )
                .await
                .err()
                .unwrap()
                .contains("YYYY-MM-DD")
        );
        assert!(
            server
                .product_stocks(
                    RequestIdentity::dev(),
                    Parameters(ProductFilterInput {
                        store: StoreId::from("ofk"),
                        offer_ids: Vec::new(),
                        product_ids: Vec::new(),
                        visibility: Visibility::All,
                        limit: 0,
                        cursor: None,
                    })
                )
                .await
                .is_err()
        );
        assert!(
            server
                .finance_transactions(
                    RequestIdentity::dev(),
                    Parameters(FinanceInput {
                        store: StoreId::from("ofk"),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        posting_number: String::new(),
                        operation_types: Vec::new(),
                        transaction_type: "all".to_owned(),
                        page: 0,
                        page_size: 100,
                    })
                )
                .await
                .err()
                .unwrap()
                .contains("page")
        );
    }

    #[tokio::test]
    async fn ozon_errors_are_converted_to_mcp_errors() {
        let result = server()
            .seller_rating(
                RequestIdentity::dev(),
                Parameters(StoreOnlyInput {
                    store: StoreId::from("ofk"),
                }),
            )
            .await;
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(error.starts_with(OZON_TOOL_FAILURE));
        assert!(error.contains("не настроены Client-Id и Api-Key"));
        assert!(error.contains("не вызывайте автоматически другие инструменты"));
        assert!(error.contains("не заявляйте о прямом доступе к Ozon"));
    }
}
