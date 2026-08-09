use std::{sync::Arc, time::Duration};

use chrono::{NaiveDate, Utc};
use rmcp::{
    Json, RoleServer, ServerHandler,
    handler::server::{
        common::{AsRequestContext, FromContextPart},
        router::tool::ToolRouter,
        tool::ToolCallContext,
        wrapper::Parameters,
    },
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation,
        JsonObject, MetaObject, ServerCapabilities, ServerInfo,
    },
    schemars::JsonSchema,
    service::RequestContext,
    tool, tool_handler, tool_router,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    auth::{
        AuthenticatedActor, JwtAuthenticationFailure, JwtAuthenticator, ProtectedResourceMetadata,
    },
    config::{Actor, Marketplace, RegistrySource, Role, StoreId},
    ozon::{OzonClient, is_read_only_endpoint_allowed},
    wb::WbClient,
};

const MAX_ANALYTICS_PERIOD_DAYS: i64 = 366;
const MAX_FINANCE_TRANSACTIONS_PERIOD_DAYS: i64 = 30;
const MAX_STORE_SELECTOR_CHARS: usize = 128;
const MAX_IDENTIFIER_CHARS: usize = 256;
const MAX_ENUM_VALUE_CHARS: usize = 128;
const MAX_OPAQUE_TOKEN_CHARS: usize = 4_096;
const MAX_PRODUCT_FILTER_ITEMS: usize = 1_000;
const MAX_SKUS: usize = 1_000;
const MAX_POSTING_NUMBERS: usize = 1_000;
const MAX_GROUP_STATES: usize = 100;
const MAX_OPERATION_TYPES: usize = 100;
const MAX_RATINGS: usize = 100;
const MIN_REVIEWS_LIMIT: u32 = 20;
const MAX_OFFSET: u32 = 1_000_000;
const MAX_PAGE: u32 = 1_000_000;
const OZON_TOOL_FAILURE: &str = "OZON_TOOL_CALL_FAILED";
const WB_TOOL_FAILURE: &str = "WB_TOOL_CALL_FAILED";
const ACCESS_DENIED: &str = "ACCESS_DENIED";
const UNKNOWN_STORE: &str = "UNKNOWN_STORE";
const STORE_REQUIRED: &str = "STORE_REQUIRED";
const NO_ACCESSIBLE_STORE: &str = "NO_ACCESSIBLE_STORE";
const PREVIEW_CURSOR_REQUIRED: &str = "PREVIEW_CURSOR_REQUIRED";
const PREVIEW_DISABLED: &str = "PREVIEW_DISABLED";
const READ_ONLY_ENDPOINT_DENIED: &str = "READ_ONLY_ENDPOINT_DENIED";
const FINANCE_ACCRUAL_PREVIEW_TOOLS: &[&str] = &[
    "ozon_finance_accrual_postings",
    "ozon_finance_accrual_types",
    "ozon_finance_accrual_by_day",
];

fn config_error(error: anyhow::Error) -> String {
    let message = error.to_string();
    if message.starts_with("MCP_ACCESS_CONFIG_RESTART_REQUIRED:") {
        message
    } else {
        format!("MCP_ACCESS_CONFIG_ERROR: {message}")
    }
}

#[derive(Debug, Clone)]
pub struct OzonMcp {
    client: OzonClient,
    wb_client: WbClient,
    default_actor_id: Option<String>,
    authenticator: Option<JwtAuthenticator>,
    registry: RegistrySource,
    postings_vnext: bool,
    finance_accruals_preview: bool,
    tool_router: ToolRouter<Self>,
}

fn tool_security_schemes(authenticator: Option<&JwtAuthenticator>) -> Vec<JsonObject> {
    let mut scheme = JsonObject::new();
    match authenticator {
        Some(authenticator) => {
            scheme.insert("type".to_owned(), Value::String("oauth2".to_owned()));
            scheme.insert(
                "scopes".to_owned(),
                Value::Array(
                    authenticator
                        .required_scopes()
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            );
        }
        None => {
            scheme.insert("type".to_owned(), Value::String("noauth".to_owned()));
        }
    }
    vec![scheme]
}

impl OzonMcp {
    fn default_tool_router(authenticator: Option<&JwtAuthenticator>) -> ToolRouter<Self> {
        let mut tool_router = Self::tool_router();
        let security_schemes = tool_security_schemes(authenticator);
        let security_schemes_value = Value::Array(
            security_schemes
                .iter()
                .cloned()
                .map(Value::Object)
                .collect(),
        );
        for route in tool_router.map.values_mut() {
            route.attr.security_schemes = Some(security_schemes.clone());
            route
                .attr
                .meta
                .get_or_insert_with(MetaObject::new)
                .0
                .insert("securitySchemes".to_owned(), security_schemes_value.clone());
        }
        for &name in FINANCE_ACCRUAL_PREVIEW_TOOLS {
            tool_router.disable_route(name);
        }
        tool_router
    }

    pub fn new(client: OzonClient, actor_id: String, registry: RegistrySource) -> Self {
        Self {
            client,
            wb_client: WbClient::empty(Duration::from_secs(30)),
            default_actor_id: Some(actor_id),
            authenticator: None,
            registry,
            postings_vnext: false,
            finance_accruals_preview: false,
            tool_router: Self::default_tool_router(None),
        }
    }

    pub fn new_authenticated(
        client: OzonClient,
        registry: RegistrySource,
        authenticator: JwtAuthenticator,
    ) -> Self {
        let tool_router = Self::default_tool_router(Some(&authenticator));
        Self {
            client,
            wb_client: WbClient::empty(Duration::from_secs(30)),
            default_actor_id: None,
            authenticator: Some(authenticator),
            registry,
            postings_vnext: false,
            finance_accruals_preview: false,
            tool_router,
        }
    }

    pub fn with_wildberries_client(mut self, wb_client: WbClient) -> Self {
        self.wb_client = wb_client;
        self
    }

    pub fn protected_resource_metadata(&self) -> Option<ProtectedResourceMetadata> {
        self.authenticator
            .as_ref()
            .map(JwtAuthenticator::protected_resource_metadata)
    }

    pub fn with_preview_features(
        mut self,
        postings_vnext: bool,
        finance_accruals_preview: bool,
    ) -> Self {
        self.postings_vnext = postings_vnext;
        self.finance_accruals_preview = finance_accruals_preview;
        for &name in FINANCE_ACCRUAL_PREVIEW_TOOLS {
            if finance_accruals_preview {
                self.tool_router.enable_route(name);
            } else {
                self.tool_router.disable_route(name);
            }
        }
        self
    }

    fn require_finance_accruals_preview(&self) -> Result<(), String> {
        if self.finance_accruals_preview {
            Ok(())
        } else {
            Err(format!(
                "{PREVIEW_DISABLED}: экспериментальные finance accrual tools выключены; установите OZON_FINANCE_ACCRUALS_PREVIEW=true только в изолированном canary"
            ))
        }
    }

    fn access_context(
        &self,
        identity: &RequestIdentity,
    ) -> Result<(Arc<crate::config::AccessRegistry>, Actor), String> {
        let registry = self.registry.load().map_err(config_error)?;
        let actor_id = identity
            .actor_id
            .as_deref()
            .or(self.default_actor_id.as_deref())
            .ok_or_else(|| "ACCESS_DENIED: отсутствует проверенная идентичность".to_owned())?;
        let actor = registry.actor(actor_id).map_err(config_error)?.clone();
        Ok((registry, actor))
    }

    fn resolve_store(
        &self,
        identity: &RequestIdentity,
        selector: Option<&StoreId>,
    ) -> Result<StoreId, String> {
        let (registry, actor) = self.access_context(identity)?;
        if let Some(selector) = selector {
            let account = registry
                .account_for_store_selector(selector)
                .ok_or_else(|| {
                    format!(
                        "{UNKNOWN_STORE}: выбранный магазин не зарегистрирован. Получите допустимый store через ozon_stores_status или marketplace_accounts."
                    )
                })?;
            if !actor.can_access_account(account) {
                return Err(format!(
                    "{ACCESS_DENIED}: текущий пользователь не имеет доступа к выбранному магазину. Не пытайтесь обходить ограничение другим идентификатором."
                ));
            }
            return Ok(account
                .ozon
                .as_ref()
                .expect("store selector always belongs to an Ozon account")
                .store_id
                .clone());
        }

        let mut accessible = registry
            .accounts
            .iter()
            .filter(|account| account.ozon.is_some() && actor.can_access_account(account));
        let first = accessible.next();
        match (first, accessible.next()) {
            (None, _) => Err(format!(
                "{NO_ACCESSIBLE_STORE}: у текущего пользователя нет доступных магазинов Ozon."
            )),
            (Some(account), None) => Ok(account
                .ozon
                .as_ref()
                .expect("filtered Ozon account")
                .store_id
                .clone()),
            (Some(_), Some(_)) => Err(format!(
                "{STORE_REQUIRED}: доступно несколько магазинов Ozon; явно передайте поле store из ozon_stores_status или marketplace_accounts."
            )),
        }
    }

    fn resolve_wb_account(
        &self,
        identity: &RequestIdentity,
        selector: Option<&str>,
    ) -> Result<String, String> {
        let (registry, actor) = self.access_context(identity)?;
        if let Some(selector) = selector {
            validate_non_blank("account", selector)?;
            validate_max_chars("account", selector, MAX_STORE_SELECTOR_CHARS)?;
            let account = registry
                .accounts
                .iter()
                .find(|account| account.id == selector && account.wildberries.is_some())
                .ok_or_else(|| {
                    "UNKNOWN_WB_ACCOUNT: выбранный кабинет Wildberries не зарегистрирован. Получите допустимый account через wb_stores_status или marketplace_accounts.".to_owned()
                })?;
            if !actor.can_access_account(account) {
                return Err(format!(
                    "{ACCESS_DENIED}: текущий пользователь не имеет доступа к выбранному кабинету Wildberries."
                ));
            }
            return Ok(account.id.clone());
        }

        let mut accessible = registry
            .accounts
            .iter()
            .filter(|account| account.wildberries.is_some() && actor.can_access_account(account));
        match (accessible.next(), accessible.next()) {
            (None, _) => Err("NO_ACCESSIBLE_WB_ACCOUNT: у текущего пользователя нет доступных кабинетов Wildberries.".to_owned()),
            (Some(account), None) => Ok(account.id.clone()),
            (Some(_), Some(_)) => Err("WB_ACCOUNT_REQUIRED: доступно несколько кабинетов Wildberries; явно передайте поле account из wb_stores_status.".to_owned()),
        }
    }

    fn wb_error(&self, account: &str, endpoint: &str, error: crate::wb::WbError) -> String {
        let kind = error.kind().code();
        let request_id = error.request_id().unwrap_or("-");
        format!(
            "{WB_TOOL_FAILURE}: kind={kind}; account={account}; endpoint={endpoint}; request_id={request_id}; message={error}. Остановите текущую операцию и не вызывайте автоматически другие WB-инструменты или кабинеты."
        )
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
        store: Option<StoreId>,
        endpoint: &'static str,
        payload: Value,
    ) -> Result<Json<OzonResult>, String> {
        if !is_read_only_endpoint_allowed(endpoint) {
            return Err(format!(
                "{READ_ONLY_ENDPOINT_DENIED}: endpoint={endpoint} отсутствует в явном read-only allowlist"
            ));
        }
        if let Some(store) = store.as_ref() {
            validate_non_blank("store", &store.0)?;
            validate_max_chars("store", &store.0, MAX_STORE_SELECTOR_CHARS)?;
        }
        let store = self.resolve_store(identity, store.as_ref())?;
        let data = self
            .client
            .post(&store, endpoint, payload)
            .await
            .map_err(|error| {
                let kind = error.kind().code();
                let request_id = error.request_id().unwrap_or("-");
                format!(
                    "{OZON_TOOL_FAILURE}: kind={kind}; store={store}; endpoint={endpoint}; request_id={request_id}; message={error}. Остановите текущую операцию: не вызывайте автоматически другие инструменты или магазины Ozon и не заявляйте о прямом доступе к Ozon. Сообщите пользователю об ошибке и дождитесь нового явного запроса с подключённым OzonOFK."
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
        // Both lists are bounded above before addition, so this cannot overflow usize.
        let selected_products = input.offer_ids.len() + input.product_ids.len();
        if selected_products > MAX_PRODUCT_FILTER_ITEMS {
            return Err(format!(
                "offer_ids и product_ids вместе должны содержать не более {MAX_PRODUCT_FILTER_ITEMS} значений"
            ));
        }
        if let Some(cursor) = input.cursor.as_deref() {
            validate_max_chars("cursor", cursor, MAX_OPAQUE_TOKEN_CHARS)?;
        }
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
        validate_max_chars("status", &input.status, MAX_ENUM_VALUE_CHARS)?;
        if let Some(cursor) = input.cursor.as_deref() {
            validate_max_chars("cursor", cursor, MAX_OPAQUE_TOKEN_CHARS)?;
        }
        validate_max_u32("offset", input.offset, MAX_OFFSET)?;
        let (from, to) = validate_and_expand_dates(&input.date_from, &input.date_to, 366)?;
        if self.postings_vnext {
            if input.offset > 0 {
                return Err(format!(
                    "{PREVIEW_CURSOR_REQUIRED}: preview-методы отправлений используют cursor; offset должен быть равен 0"
                ));
            }
            validate_limit(input.limit, 100)?;
            let statuses = if input.status.is_empty() {
                Vec::new()
            } else {
                vec![input.status]
            };
            let mut payload = json!({
                "filter": { "since": from, "to": to, "statuses": statuses },
                "limit": input.limit,
                "sort_dir": input.direction,
                "translit": false,
                "with": kind.preview_with(),
            });
            if let Some(cursor) = input.cursor.filter(|cursor| !cursor.is_empty()) {
                payload
                    .as_object_mut()
                    .expect("posting preview payload is an object")
                    .insert("cursor".to_owned(), json!(cursor));
            }
            return self
                .request(identity, input.store, kind.preview_endpoint(), payload)
                .await;
        }
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
        let actor_id =
            authenticated_actor(context.as_request_context()).map(|actor| actor.actor_id.clone());
        Ok(Self { actor_id })
    }
}

fn authenticated_actor(context: &RequestContext<RoleServer>) -> Option<&AuthenticatedActor> {
    context.extensions.get::<AuthenticatedActor>().or_else(|| {
        context
            .extensions
            .get::<axum::http::request::Parts>()
            .and_then(|parts| parts.extensions.get::<AuthenticatedActor>())
    })
}

fn request_headers(context: &RequestContext<RoleServer>) -> axum::http::HeaderMap {
    context
        .extensions
        .get::<axum::http::request::Parts>()
        .map(|parts| parts.headers.clone())
        .unwrap_or_default()
}

fn authentication_failure_response(
    authenticator: &JwtAuthenticator,
    failure: JwtAuthenticationFailure,
) -> CallToolResponse {
    let mut result = CallToolResult::error(vec![ContentBlock::text(failure.public_message())]);
    if let Some(challenge) = authenticator.challenge(&failure) {
        let mut meta = MetaObject::new();
        meta.0
            .insert("mcp/www_authenticate".to_owned(), json!([challenge]));
        result = result.with_meta(Some(meta));
    }
    result.into()
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

    fn preview_endpoint(self) -> &'static str {
        match self {
            Self::Fbs => "/v4/posting/fbs/list",
            Self::Fbo => "/v3/posting/fbo/list",
        }
    }

    fn preview_with(self) -> Value {
        match self {
            Self::Fbs => json!({
                "analytics_data": true,
                "barcodes": true,
                "financial_data": true,
                "legal_info": false,
            }),
            Self::Fbo => json!({
                "analytics_data": true,
                "financial_data": true,
                "legal_info": false,
            }),
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
    /// Backward-compatible canonical Ozon store identifier.
    pub id: StoreId,
    pub account_id: String,
    pub store_id: StoreId,
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
    pub accounts: Vec<MemberAccountStatus>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MemberAccountStatus {
    pub account_id: String,
    pub store_id: Option<StoreId>,
    pub organization: String,
    pub marketplace: Marketplace,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct AccountStatus {
    /// Backward-compatible account identifier.
    pub id: String,
    pub account_id: String,
    pub store_id: Option<StoreId>,
    pub organization: String,
    pub marketplace: Marketplace,
    pub seller_client_id: String,
    pub manager: String,
    pub integration_status: &'static str,
    pub configured: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WbResult {
    pub account_id: String,
    pub endpoint: &'static str,
    pub fetched_at: String,
    pub data: Value,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WbStoresResult {
    pub actor: ActorStatus,
    pub default_account: Option<String>,
    pub access_mode: &'static str,
    pub accounts: Vec<WbStoreStatus>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WbStoreStatus {
    pub account_id: String,
    pub organization: String,
    pub seller_client_id: String,
    pub manager: String,
    pub configured: bool,
    pub api_token_env: String,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EmptyInput {}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbAccountInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WbSalesFunnelInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический account_id Wildberries из wb_stores_status",
        length(min = 1, max = 128)
    )]
    pub account: Option<String>,
    #[schemars(
        description = "Начало периода в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD", length(equal = 10))]
    pub date_to: String,
    #[serde(default)]
    #[schemars(length(max = 1_000))]
    pub nm_ids: Vec<u64>,
    #[serde(default)]
    #[schemars(length(max = 100), inner(length(min = 1, max = 128)))]
    pub brand_names: Vec<String>,
    #[serde(default)]
    #[schemars(length(max = 1_000))]
    pub subject_ids: Vec<u64>,
    #[serde(default)]
    #[schemars(length(max = 1_000))]
    pub tag_ids: Vec<u64>,
    #[serde(default)]
    pub skip_deleted_nm: bool,
    #[serde(default = "default_product_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 1_000_000))]
    pub offset: u32,
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
#[serde(deny_unknown_fields)]
pub struct AnalyticsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Начало периода в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD", length(equal = 10))]
    pub date_to: String,
    #[schemars(description = "От одной до 10 метрик Ozon", length(min = 1, max = 10))]
    pub metrics: Vec<AnalyticsMetric>,
    #[schemars(
        description = "От одного до двух измерений: например sku и day",
        length(min = 1, max = 2)
    )]
    pub dimensions: Vec<AnalyticsDimension>,
    #[serde(default = "default_analytics_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 1_000_000))]
    pub offset: u32,
    pub sort_by: Option<AnalyticsMetric>,
    #[serde(default)]
    pub sort_direction: SortDirection,
}

fn default_analytics_limit() -> u32 {
    1_000
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProductFilterInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub offer_ids: Vec<String>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub product_ids: Vec<String>,
    #[serde(default)]
    pub visibility: Visibility,
    #[serde(default = "default_product_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[schemars(length(max = 4_096))]
    pub cursor: Option<String>,
}

fn default_product_limit() -> u32 {
    100
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TurnoverInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub skus: Vec<String>,
    #[serde(default = "default_product_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 1_000_000))]
    pub offset: u32,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostingListInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Начало периода в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD", length(equal = 10))]
    pub date_to: String,
    #[serde(default)]
    #[schemars(length(max = 128))]
    pub status: String,
    #[serde(default = "default_posting_limit")]
    #[schemars(range(min = 1, max = 1_000))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 1_000_000))]
    pub offset: u32,
    #[serde(default)]
    #[schemars(
        description = "Непрозрачный cursor только для экспериментального vNext preview",
        length(max = 4_096)
    )]
    pub cursor: Option<String>,
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
}

impl ReturnSchema {
    fn as_ozon_str(self) -> &'static str {
        match self {
            Self::Fbo => "FBO",
            Self::Fbs => "FBS",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReturnsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Начало периода изменения статуса в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(
        description = "Конец периода изменения статуса в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_to: String,
    #[serde(default)]
    pub return_schema: ReturnSchema,
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub offer_id: String,
    #[serde(default)]
    #[schemars(length(max = 1_000), inner(length(min = 1, max = 256)))]
    pub posting_numbers: Vec<String>,
    #[serde(default = "default_returns_limit")]
    #[schemars(range(min = 1, max = 500))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(range(max = 18_446_744_073_709_551_615_u64))]
    pub last_id: u64,
}

fn default_returns_limit() -> u32 {
    500
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RfbsReturnsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Начало периода создания возврата в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(
        description = "Конец периода создания возврата в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_to: String,
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub offer_id: String,
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub posting_number: String,
    #[serde(default)]
    #[schemars(length(max = 100), inner(length(min = 1, max = 128)))]
    pub group_state: Vec<String>,
    #[serde(default)]
    #[schemars(range(max = 18_446_744_073_709_551_615_u64))]
    pub last_id: u64,
    #[serde(default = "default_rfbs_returns_limit")]
    #[schemars(range(min = 1, max = 100))]
    pub limit: u32,
}

fn default_rfbs_returns_limit() -> u32 {
    100
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FinanceInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Начало периода в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD", length(equal = 10))]
    pub date_to: String,
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub posting_number: String,
    #[serde(default)]
    #[schemars(length(max = 100), inner(length(min = 1, max = 128)))]
    pub operation_types: Vec<String>,
    #[serde(default = "default_transaction_type")]
    #[schemars(length(max = 128))]
    pub transaction_type: String,
    #[serde(default = "default_page")]
    #[schemars(range(min = 1, max = 1_000_000))]
    pub page: u32,
    #[serde(default = "default_finance_page_size")]
    #[schemars(range(min = 1, max = 1_000))]
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
#[serde(deny_unknown_fields)]
pub struct FinanceTotalsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Начало периода в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD", length(equal = 10))]
    pub date_to: String,
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub posting_number: String,
    #[serde(default = "default_transaction_type")]
    #[schemars(length(max = 128))]
    pub transaction_type: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FinanceAccrualPostingsPreviewInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Непустой список номеров отправлений для candidate preview-контракта",
        length(min = 1, max = 1_000),
        inner(length(min = 1, max = 256))
    )]
    pub posting_numbers: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FinanceAccrualTypesPreviewInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FinanceAccrualByDayPreviewInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Дата начислений в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date: String,
    #[serde(default)]
    #[schemars(
        description = "Непрозрачный last_id из предыдущего preview-ответа",
        length(max = 4_096)
    )]
    pub last_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StoreOnlyInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RatingHistoryInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Начало периода в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD", length(equal = 10))]
    pub date_to: String,
    #[schemars(
        description = "От одного до 100 кодов из ozon_seller_rating, например rating_shipment_delay_cb",
        length(min = 1, max = 100),
        inner(length(min = 1, max = 128))
    )]
    pub ratings: Vec<String>,
    #[serde(default = "default_true")]
    pub with_premium_scores: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReviewsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[serde(default = "default_reviews_limit")]
    #[schemars(range(min = 20, max = 100))]
    pub limit: u32,
    #[serde(default)]
    #[schemars(length(max = 4_096))]
    pub last_id: String,
    #[serde(default = "default_all_status")]
    #[schemars(length(min = 1, max = 128))]
    pub status: String,
    #[serde(default)]
    pub direction: SortDirection,
}

fn default_reviews_limit() -> u32 {
    100
}

fn default_all_status() -> String {
    "ALL".to_owned()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QuestionsInput {
    #[serde(default)]
    #[schemars(
        description = "Канонический store_id или account_id из marketplace_accounts",
        length(min = 1, max = 128)
    )]
    pub store: Option<StoreId>,
    #[schemars(
        description = "Начало периода в формате YYYY-MM-DD",
        length(equal = 10)
    )]
    pub date_from: String,
    #[schemars(description = "Конец периода в формате YYYY-MM-DD", length(equal = 10))]
    pub date_to: String,
    #[serde(default = "default_all_status")]
    #[schemars(length(min = 1, max = 128))]
    pub status: String,
    #[serde(default)]
    #[schemars(length(max = 4_096))]
    pub last_id: String,
}

#[tool_router]
impl OzonMcp {
    /// Показывает локально настроенные магазины и наличие ключей, не раскрывая секреты. Не проверяет сеть или авторизацию Ozon API.
    #[tool(
        name = "ozon_stores_status",
        annotations(title = "Статус магазинов Ozon", read_only_hint = true)
    )]
    async fn stores_status(
        &self,
        identity: RequestIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<StoresResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        let accessible_stores: Vec<_> = registry
            .accounts
            .iter()
            .filter_map(|account| account.ozon.as_ref().map(|ozon| (account, ozon)))
            .filter(|(account, _)| actor.can_access_account(account))
            .collect();
        Ok(Json(StoresResult {
            actor: Self::actor_status(&actor),
            default_store: (accessible_stores.len() == 1)
                .then(|| accessible_stores[0].1.store_id.clone()),
            access_mode: "server-side RBAC, read-only allowlist",
            stores: accessible_stores
                .into_iter()
                .map(|(account, ozon)| {
                    let manager = registry
                        .actor(&account.manager_id)
                        .expect("validated manager");
                    StoreStatus {
                        id: ozon.store_id.clone(),
                        account_id: account.id.clone(),
                        store_id: ozon.store_id.clone(),
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

    /// Показывает доступные кабинеты Wildberries и наличие токенов, не раскрывая секреты и не выполняя сетевые запросы.
    #[tool(
        name = "wb_stores_status",
        annotations(title = "Статус кабинетов Wildberries", read_only_hint = true)
    )]
    async fn wb_stores_status(
        &self,
        identity: RequestIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<WbStoresResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        let accessible_accounts: Vec<_> = registry
            .accounts
            .iter()
            .filter(|account| account.wildberries.is_some() && actor.can_access_account(account))
            .collect();
        Ok(Json(WbStoresResult {
            actor: Self::actor_status(&actor),
            default_account: (accessible_accounts.len() == 1)
                .then(|| accessible_accounts[0].id.clone()),
            access_mode: "server-side RBAC, explicit read-only WB methods",
            accounts: accessible_accounts
                .into_iter()
                .map(|account| {
                    let wildberries = account
                        .wildberries
                        .as_ref()
                        .expect("filtered Wildberries account");
                    let manager = registry
                        .actor(&account.manager_id)
                        .expect("validated manager");
                    WbStoreStatus {
                        account_id: account.id.clone(),
                        organization: account.organization.clone(),
                        seller_client_id: account.seller_client_id.clone(),
                        manager: manager.name.clone(),
                        configured: self.wb_client.is_configured(&account.id),
                        api_token_env: wildberries.api_token_env.clone(),
                    }
                })
                .collect(),
        }))
    }

    /// Проверяет авторизацию выбранного кабинета через официальный read-only WB /ping.
    #[tool(
        name = "wb_ping",
        annotations(title = "Проверка подключения Wildberries", read_only_hint = true)
    )]
    async fn wb_ping(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbAccountInput>,
    ) -> Result<Json<WbResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let endpoint = "common:/ping";
        let data = self
            .wb_client
            .ping(&account)
            .await
            .map_err(|error| self.wb_error(&account, endpoint, error))?;
        Ok(Json(WbResult {
            account_id: account,
            endpoint,
            fetched_at: Utc::now().to_rfc3339(),
            data,
        }))
    }

    /// Получает read-only воронку продаж Wildberries по карточкам за выбранный период.
    #[tool(
        name = "wb_sales_funnel",
        annotations(title = "Воронка продаж Wildberries", read_only_hint = true)
    )]
    async fn wb_sales_funnel(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbSalesFunnelInput>,
    ) -> Result<Json<WbResult>, String> {
        validate_date_range(&input.date_from, &input.date_to, 366)?;
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
            .map_err(|error| self.wb_error(&account, endpoint, error))?;
        Ok(Json(WbResult {
            account_id: account,
            endpoint,
            fetched_at: Utc::now().to_rfc3339(),
            data,
        }))
    }

    /// Показывает доступные текущему пользователю кабинеты Ozon и Wildberries и состояние их read-only интеграций.
    #[tool(
        name = "marketplace_accounts",
        annotations(title = "Доступные кабинеты маркетплейсов", read_only_hint = true)
    )]
    async fn marketplace_accounts(
        &self,
        identity: RequestIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<AccountsResult>, String> {
        let (registry, actor) = self.access_context(&identity)?;
        Ok(Json(AccountsResult {
            actor: Self::actor_status(&actor),
            accounts: registry
                .accounts
                .iter()
                .filter(|account| actor.can_access_account(account))
                .map(|account| {
                    let (integration_status, configured) = if let Some(ozon) = &account.ozon {
                        (
                            "read_only_ozon_api",
                            self.client.is_configured(&ozon.store_id),
                        )
                    } else if account.wildberries.is_some() {
                        (
                            "read_only_wildberries_api",
                            self.wb_client.is_configured(&account.id),
                        )
                    } else {
                        ("directory_only", false)
                    };
                    let manager = registry
                        .actor(&account.manager_id)
                        .expect("validated manager");
                    AccountStatus {
                        id: account.id.clone(),
                        account_id: account.id.clone(),
                        store_id: account.ozon.as_ref().map(|ozon| ozon.store_id.clone()),
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
    async fn list_members(
        &self,
        identity: RequestIdentity,
        Parameters(_input): Parameters<EmptyInput>,
    ) -> Result<Json<MembersResult>, String> {
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
                let mut accounts: Vec<_> = registry
                    .accounts
                    .iter()
                    .filter(|account| member.can_access_account(account))
                    .map(|account| MemberAccountStatus {
                        account_id: account.id.clone(),
                        store_id: account.ozon.as_ref().map(|ozon| ozon.store_id.clone()),
                        organization: account.organization.clone(),
                        marketplace: account.marketplace,
                    })
                    .collect();
                accounts.sort_by(|left, right| left.account_id.cmp(&right.account_id));
                MemberStatus {
                    id: member.id.clone(),
                    name: member.name.clone(),
                    role: member.role,
                    account_ids,
                    accounts,
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
        validate_max_u32("offset", input.offset, MAX_OFFSET)?;

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
    async fn rfbs_returns(
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

    /// Получает агрегированные финансовые итоги Ozon за период.
    #[tool(
        name = "ozon_finance_totals",
        annotations(title = "Финансовые итоги Ozon", read_only_hint = true)
    )]
    async fn finance_totals(
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

    /// EXPERIMENTAL PREVIEW: получает read-only начисления по указанным отправлениям через candidate-контракт Ozon. По умолчанию выключен; схема не считается официально подтверждённой.
    #[tool(
        name = "ozon_finance_accrual_postings",
        annotations(
            title = "PREVIEW: начисления по отправлениям Ozon",
            read_only_hint = true
        )
    )]
    async fn finance_accrual_postings_preview(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FinanceAccrualPostingsPreviewInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.require_finance_accruals_preview()?;
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

    /// EXPERIMENTAL PREVIEW: получает read-only candidate-справочник типов начислений Ozon. По умолчанию выключен; схема не считается официально подтверждённой.
    #[tool(
        name = "ozon_finance_accrual_types",
        annotations(title = "PREVIEW: типы начислений Ozon", read_only_hint = true)
    )]
    async fn finance_accrual_types_preview(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FinanceAccrualTypesPreviewInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.require_finance_accruals_preview()?;
        self.request(
            &identity,
            input.store,
            "/v1/finance/accrual/types",
            json!({}),
        )
        .await
    }

    /// EXPERIMENTAL PREVIEW: получает read-only начисления Ozon за один день через candidate-контракт с last_id. По умолчанию выключен; схема не считается официально подтверждённой.
    #[tool(
        name = "ozon_finance_accrual_by_day",
        annotations(title = "PREVIEW: начисления Ozon за день", read_only_hint = true)
    )]
    async fn finance_accrual_by_day_preview(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<FinanceAccrualByDayPreviewInput>,
    ) -> Result<Json<OzonResult>, String> {
        self.require_finance_accruals_preview()?;
        parse_date(&input.date, "date")?;
        validate_max_chars("last_id", &input.last_id, MAX_OPAQUE_TOKEN_CHARS)?;
        let mut payload = json!({ "date": input.date });
        if !input.last_id.is_empty() {
            payload
                .as_object_mut()
                .expect("finance by-day preview payload is an object")
                .insert("last_id".to_owned(), json!(input.last_id));
        }
        self.request(
            &identity,
            input.store,
            "/v1/finance/accrual/by-day",
            payload,
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
    async fn reviews(
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

#[tool_handler(router = self.tool_router)]
impl ServerHandler for OzonMcp {
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        mut context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, rmcp::ErrorData> {
        if let Some(authenticator) = &self.authenticator {
            let actor = match authenticated_actor(&context).cloned() {
                Some(actor) => actor,
                None => {
                    let headers = request_headers(&context);
                    match authenticator.authenticate(&headers).await {
                        Ok(actor) => actor,
                        Err(failure) => {
                            return Ok(authentication_failure_response(authenticator, failure));
                        }
                    }
                }
            };
            context.extensions.insert(actor);
        }

        self.tool_router
            .call(ToolCallContext::new(self, request, context))
            .await
    }

    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("mcp-ozon", env!("CARGO_PKG_VERSION"))
                    .with_title("Ozon and Wildberries Seller Analytics"),
            )
            .with_instructions(
                "Read-only MCP для аналитики кабинетов Ozon и Wildberries. Все инструменты только получают данные. \
                 Сервер не изменяет товары, цены, остатки, заказы, отзывы, вопросы, рекламу или настройки кабинетов. \
                 Доступ к магазинам проверяется сервером по подтверждённой идентичности: JWT/OIDC \
                 в защищённом режиме или MCP_ACTOR_ID в локальном dev-режиме. Менеджер видит только \
                 закреплённый кабинет, администратор — все кабинеты. Не запрашивайте роль или имя \
                 пользователя через аргументы инструмента и не пытайтесь обходить ACCESS_DENIED. \
                 Вызывайте инструменты только когда OzonOFK доступен в текущем чате и пользователь \
                 явно разрешил текущий вызов согласно настройкам ChatGPT. Никогда не заявляйте о \
                 прямом доступе к маркетплейсу без успешного результата инструмента OzonOFK. Если доступ \
                 отклонён, коннектор недоступен или любой инструмент завершился ошибкой, остановитесь: \
                 не вызывайте автоматически другой инструмент или кабинет и дождитесь нового \
                 явного запроса пользователя. ozon_stores_status и wb_stores_status показывают только локальную \
                 конфигурацию и не подтверждают доступность внешнего API.",
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

fn validate_max_chars(field: &str, value: &str, maximum: usize) -> Result<(), String> {
    if value.chars().count() > maximum {
        return Err(format!("{field} не может быть длиннее {maximum} символов"));
    }
    Ok(())
}

fn validate_non_blank(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} не может быть пустым"));
    }
    Ok(())
}

fn validate_string_list(
    field: &str,
    values: &[String],
    maximum_items: usize,
    maximum_chars: usize,
) -> Result<(), String> {
    if values.len() > maximum_items {
        return Err(format!(
            "{field} должен содержать не более {maximum_items} значений"
        ));
    }
    for value in values {
        validate_non_blank(field, value)?;
        validate_max_chars(field, value, maximum_chars)?;
    }
    Ok(())
}

fn validate_max_u32(field: &str, value: u32, maximum: u32) -> Result<(), String> {
    if value > maximum {
        return Err(format!("{field} не может превышать {maximum}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
            mpsc,
        },
        time::Duration,
    };

    use super::*;
    use crate::config::{JwtConfig, MarketplaceAccount};
    use crate::ozon::READ_ONLY_ENDPOINT_ALLOWLIST;
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
            {"id":"admin","name":"Administrator","role":"admin"},
            {"id":"manager","name":"Manager","role":"manager"}
          ],
          "accounts": [
            {"id":"store_a","organization":"Example organization A","marketplace":"ozon","seller_client_id":"client-a","manager_id":"admin","ozon":{"store_id":"store_a","client_id_env":"OZON_CLIENT_ID","api_key_env":"OZON_API_KEY"}},
            {"id":"account_b","organization":"Example organization B","marketplace":"ozon","seller_client_id":"client-b","manager_id":"manager","ozon":{"store_id":"store_b","client_id_env":"EVRO_ID","api_key_env":"EVRO_KEY"}},
            {"id":"account_wb","organization":"WB account","marketplace":"wildberries","seller_client_id":"42","manager_id":"admin","wildberries":{"api_token_env":"WB_TOKEN"}}
          ]
        }"#).unwrap();
        RegistrySource::new(path).unwrap()
    }

    fn jwt_authenticator(registry: &RegistrySource) -> JwtAuthenticator {
        JwtAuthenticator::new(
            JwtConfig {
                issuer: "http://issuer.test/realms/ofk".to_owned(),
                audience: "ozonofk-mcp".to_owned(),
                jwks_url: "http://127.0.0.1:1/jwks".to_owned(),
                resource_url: "http://localhost:8788/mcp".to_owned(),
                resource_metadata_url: "http://localhost:8788/.well-known/oauth-protected-resource"
                    .to_owned(),
                required_scopes: vec!["mcp:tools".to_owned()],
                jwks_cache_ttl: Duration::from_secs(300),
            },
            registry.clone(),
        )
        .unwrap()
    }

    fn server() -> OzonMcp {
        OzonMcp::new(
            OzonClient::new(
                "http://127.0.0.1:1".to_owned(),
                Duration::from_secs(1),
                BTreeMap::new(),
            )
            .unwrap(),
            "admin".to_owned(),
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
            StoreId::from("store_a"),
            crate::config::StoreCredentials {
                client_id: "test-client".to_owned(),
                api_key: "test-key".to_owned(),
            },
        )]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), stores).unwrap();
        (
            OzonMcp::new(client, "admin".to_owned(), registry_source()),
            receiver,
        )
    }

    fn mock_wb_server_for(
        actor: &str,
        expected_requests: usize,
    ) -> (OzonMcp, mpsc::Receiver<String>) {
        let responses =
            vec![(200, r#"{"data":{"products":[]},"Status":"OK"}"#.to_owned()); expected_requests];
        mock_wb_server_with_responses(actor, responses)
    }

    fn mock_wb_server_with_responses(
        actor: &str,
        responses: Vec<(u16, String)>,
    ) -> (OzonMcp, mpsc::Receiver<String>) {
        let (base_url, receiver) = mock_http(responses);
        let wb_client = WbClient::new_for_test(
            Duration::from_secs(3),
            BTreeMap::from([(
                "account_wb".to_owned(),
                crate::wb::WbCredentials {
                    token: "test-wb-token".to_owned(),
                },
            )]),
            &base_url,
            &base_url,
        );
        let ozon_client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
            BTreeMap::new(),
        )
        .unwrap();
        (
            OzonMcp::new(ozon_client, actor.to_owned(), registry_source())
                .with_wildberries_client(wb_client),
            receiver,
        )
    }

    fn selector_registry_source() -> RegistrySource {
        let sequence = REGISTRY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mcp-ozon-selectors-{}-{sequence}.json",
            std::process::id()
        ));
        fs::write(&path, r#"{
          "version": 1,
          "actors": [
            {"id":"admin","name":"Administrator","role":"admin"},
            {"id":"manager_a","name":"Manager A","role":"manager"},
            {"id":"manager_b","name":"Manager B","role":"manager"},
            {"id":"manager_c","name":"Manager C","role":"manager"},
            {"id":"manager_wb","name":"WB Manager","role":"manager"}
          ],
          "accounts": [
            {"id":"account_a","organization":"Example organization A","marketplace":"ozon","seller_client_id":"client-a","manager_id":"manager_a","ozon":{"store_id":"store_a","client_id_env":"OZON_CLIENT_ID","api_key_env":"OZON_API_KEY"}},
            {"id":"account_b","organization":"Example organization B","marketplace":"ozon","seller_client_id":"client-b","manager_id":"manager_b","ozon":{"store_id":"store_b","client_id_env":"OFK_K_ID","api_key_env":"OFK_K_KEY"}},
            {"id":"account_c","organization":"Example organization C","marketplace":"ozon","seller_client_id":"client-c","manager_id":"manager_c","ozon":{"store_id":"store_c","client_id_env":"MEGA_ID","api_key_env":"MEGA_KEY"}},
            {"id":"wb_directory","organization":"WB","marketplace":"wildberries","seller_client_id":"1","manager_id":"manager_wb","wildberries":{"api_token_env":"WB_TOKEN"}}
          ]
        }"#).unwrap();
        RegistrySource::new(path).unwrap()
    }

    fn selector_mock_server(
        actor: &str,
        expected_requests: usize,
    ) -> (OzonMcp, mpsc::Receiver<String>) {
        let responses = vec![(200, r#"{"ok":true}"#.to_owned()); expected_requests];
        let (base_url, receiver) = mock_http(responses);
        let credentials = crate::config::StoreCredentials {
            client_id: "test-client".to_owned(),
            api_key: "test-key".to_owned(),
        };
        let stores = BTreeMap::from([
            (StoreId::from("store_a"), credentials.clone()),
            (StoreId::from("store_b"), credentials.clone()),
            (StoreId::from("store_c"), credentials),
        ]);
        let client = OzonClient::new(base_url, Duration::from_secs(3), stores).unwrap();
        (
            OzonMcp::new(client, actor.to_owned(), selector_registry_source()),
            receiver,
        )
    }

    async fn call_tool_over_http(server: OzonMcp, name: &str, arguments: Value) -> String {
        let server = Arc::new(server);
        let service: StreamableHttpService<OzonMcp, LocalSessionManager> =
            StreamableHttpService::new(
                move || Ok((*server).clone()),
                Default::default(),
                StreamableHttpServerConfig::default()
                    .with_legacy_session_mode(false)
                    .with_json_response(true),
            );
        let router = axum::Router::new().nest_service("/mcp", service);
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
                "params": {"name": name, "arguments": arguments}
            }))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let body = response.text().await.unwrap();
        task.abort();
        body
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

    fn assert_validation_error<T>(result: Result<T, String>, field: &str) {
        let error = result
            .err()
            .expect("expected validation error before an Ozon request");
        assert!(
            error.contains(field),
            "expected error for {field}, got: {error}"
        );
    }

    #[test]
    fn all_tools_are_read_only_and_described() {
        let tools = server()
            .with_preview_features(false, true)
            .tool_router
            .list_all();
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
            assert_eq!(
                tool.input_schema.get("additionalProperties"),
                Some(&Value::Bool(false)),
                "{} must reject unknown input fields",
                tool.name
            );
        }
    }

    #[tokio::test]
    async fn wb_tools_apply_rbac_and_send_only_exact_read_only_contracts() {
        fn result_text(body: &str) -> Value {
            let envelope: Value = serde_json::from_str(body).unwrap();
            let text = envelope
                .pointer("/result/content/0/text")
                .and_then(Value::as_str)
                .expect("tool result must contain text");
            serde_json::from_str(text).unwrap()
        }

        let (server, requests) = mock_wb_server_for("admin", 2);
        let status = call_tool_over_http(server.clone(), "wb_stores_status", json!({})).await;
        let status = result_text(&status);
        assert_eq!(status["default_account"], json!("account_wb"));
        assert_eq!(status["accounts"][0]["configured"], json!(true));
        assert!(status.to_string().find("test-wb-token").is_none());

        let ping = call_tool_over_http(server.clone(), "wb_ping", json!({})).await;
        assert_eq!(result_text(&ping)["account_id"], json!("account_wb"));

        let funnel = call_tool_over_http(
            server,
            "wb_sales_funnel",
            json!({
                "account": "account_wb",
                "date_from": "2026-08-01",
                "date_to": "2026-08-08",
                "nm_ids": [],
                "brand_names": [],
                "subject_ids": [],
                "tag_ids": [],
                "skip_deleted_nm": false,
                "limit": 10,
                "offset": 0
            }),
        )
        .await;
        assert_eq!(
            result_text(&funnel)["endpoint"],
            json!("analytics:/api/analytics/v3/sales-funnel/products")
        );

        let ping_request = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(ping_request.starts_with("GET /ping HTTP/1.1\r\n"));
        assert!(
            ping_request
                .to_ascii_lowercase()
                .contains("authorization: bearer test-wb-token")
        );
        let funnel_request = requests.recv_timeout(Duration::from_secs(1)).unwrap();
        let (path, body) = request_path_and_body(&funnel_request);
        assert_eq!(path, "/api/analytics/v3/sales-funnel/products");
        assert_eq!(
            body,
            json!({
                "selectedPeriod": {"start": "2026-08-01", "end": "2026-08-08"},
                "nmIds": [],
                "brandNames": [],
                "subjectIds": [],
                "tagIds": [],
                "skipDeletedNm": false,
                "limit": 10,
                "offset": 0
            })
        );
        assert!(requests.try_recv().is_err());

        let (manager, denied_requests) = mock_wb_server_for("manager", 0);
        let denied =
            call_tool_over_http(manager, "wb_ping", json!({"account": "account_wb"})).await;
        assert!(denied.contains(ACCESS_DENIED));
        assert!(denied_requests.try_recv().is_err());

        let (admin, unknown_requests) = mock_wb_server_for("admin", 0);
        let unknown = call_tool_over_http(admin, "wb_ping", json!({"account": "unknown-wb"})).await;
        assert!(unknown.contains("UNKNOWN_WB_ACCOUNT"));
        assert!(unknown_requests.try_recv().is_err());

        let (errors, error_requests) = mock_wb_server_with_responses(
            "admin",
            vec![(401, "{}".to_owned()), (403, "{}".to_owned())],
        );
        let ping_error = call_tool_over_http(errors.clone(), "wb_ping", json!({})).await;
        assert!(ping_error.contains(WB_TOOL_FAILURE));
        assert!(ping_error.contains("kind=unauthorized"));
        let funnel_error = call_tool_over_http(
            errors,
            "wb_sales_funnel",
            json!({
                "date_from": "2026-08-01",
                "date_to": "2026-08-08",
                "limit": 10,
                "offset": 0
            }),
        )
        .await;
        assert!(funnel_error.contains(WB_TOOL_FAILURE));
        assert!(funnel_error.contains("kind=forbidden"));
        error_requests.recv_timeout(Duration::from_secs(1)).unwrap();
        error_requests.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(error_requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn wb_resolution_directory_and_error_paths_fail_closed_before_network() {
        let no_wb = manager_server("manager");
        assert!(
            no_wb
                .resolve_wb_account(&RequestIdentity::dev(), None)
                .unwrap_err()
                .starts_with("NO_ACCESSIBLE_WB_ACCOUNT")
        );

        let source = registry_source();
        let mut registry = (*source.load().unwrap()).clone();
        let mut second_wb = registry
            .accounts
            .iter()
            .find(|account| account.id == "account_wb")
            .unwrap()
            .clone();
        second_wb.id = "account_wb_2".to_owned();
        second_wb.seller_client_id = "43".to_owned();
        second_wb.wildberries.as_mut().unwrap().api_token_env = "WB_TOKEN_2".to_owned();
        registry.accounts.push(second_wb);
        registry.accounts.push(MarketplaceAccount {
            id: "wb_directory".to_owned(),
            organization: "WB directory entry".to_owned(),
            marketplace: Marketplace::Wildberries,
            seller_client_id: "44".to_owned(),
            manager_id: "admin".to_owned(),
            ozon: None,
            wildberries: None,
        });
        let path = source.path().to_path_buf();
        fs::write(&path, serde_json::to_vec(&registry).unwrap()).unwrap();
        let source = RegistrySource::new(path).unwrap();

        let server = OzonMcp::new(
            OzonClient::new(
                "http://127.0.0.1:1".to_owned(),
                Duration::from_secs(1),
                BTreeMap::new(),
            )
            .unwrap(),
            "admin".to_owned(),
            source,
        );
        assert!(
            server
                .resolve_wb_account(&RequestIdentity::dev(), None)
                .unwrap_err()
                .starts_with("WB_ACCOUNT_REQUIRED")
        );
        let accounts = server
            .marketplace_accounts(RequestIdentity::dev(), Parameters(EmptyInput::default()))
            .await
            .unwrap()
            .0;
        assert!(accounts.accounts.iter().any(|account| {
            account.id == "wb_directory"
                && account.integration_status == "directory_only"
                && !account.configured
        }));

        let error = server.wb_error(
            "account_wb",
            "common:/ping",
            crate::wb::WbError::Forbidden {
                request_id: Some("safe-id".to_owned()),
            },
        );
        assert!(error.contains(WB_TOOL_FAILURE));
        assert!(error.contains("kind=forbidden"));
        assert!(error.contains("request_id=safe-id"));

        let oversized_subjects = WbSalesFunnelInput {
            account: Some("account_wb".to_owned()),
            date_from: "2026-08-01".to_owned(),
            date_to: "2026-08-08".to_owned(),
            nm_ids: Vec::new(),
            brand_names: Vec::new(),
            subject_ids: vec![1; MAX_PRODUCT_FILTER_ITEMS + 1],
            tag_ids: Vec::new(),
            skip_deleted_nm: false,
            limit: 10,
            offset: 0,
        };
        let error = server
            .wb_sales_funnel(RequestIdentity::dev(), Parameters(oversized_subjects))
            .await
            .err()
            .expect("oversized WB subject filter must be rejected");
        assert!(error.contains("subject_ids"));
    }

    #[test]
    fn every_tool_advertises_exact_security_policy_and_compatibility_mirror() {
        fn assert_policy(tools: Vec<rmcp::model::Tool>, expected: &Value) {
            for tool in tools {
                let serialized = serde_json::to_value(&tool).unwrap();
                assert_eq!(
                    serialized.get("securitySchemes"),
                    Some(expected),
                    "{} canonical security policy differs",
                    tool.name
                );
                assert_eq!(
                    serialized.pointer("/_meta/securitySchemes"),
                    Some(expected),
                    "{} compatibility mirror differs",
                    tool.name
                );
                assert!(serialized.get("security_schemes").is_none());
            }
        }

        let dev_tools = server().tool_router.list_all();
        assert_eq!(dev_tools.len(), 20);
        assert_policy(dev_tools, &json!([{"type": "noauth"}]));

        let seed = server();
        let authenticator = jwt_authenticator(&seed.registry);
        let authenticated = OzonMcp::new_authenticated(seed.client, seed.registry, authenticator);
        let metadata = authenticated.protected_resource_metadata().unwrap();
        assert_eq!(metadata.resource, "http://localhost:8788/mcp");
        assert_eq!(metadata.scopes_supported, vec!["mcp:tools"]);

        let jwt_tools = authenticated.tool_router.list_all();
        assert_eq!(jwt_tools.len(), 20);
        assert_policy(
            jwt_tools,
            &json!([{"type": "oauth2", "scopes": ["mcp:tools"]}]),
        );

        let seed = server();
        let authenticator = jwt_authenticator(&seed.registry);
        let preview_tools = OzonMcp::new_authenticated(seed.client, seed.registry, authenticator)
            .with_preview_features(false, true)
            .tool_router
            .list_all();
        assert_eq!(preview_tools.len(), 23);
        assert_policy(
            preview_tools,
            &json!([{"type": "oauth2", "scopes": ["mcp:tools"]}]),
        );
    }

    #[test]
    fn finance_preview_routes_are_visible_only_when_explicitly_enabled() {
        const STABLE_TOOL_NAMES: &[&str] = &[
            "ozon_stores_status",
            "marketplace_accounts",
            "list_members",
            "wb_stores_status",
            "wb_ping",
            "wb_sales_funnel",
            "ozon_analytics",
            "ozon_product_stocks",
            "ozon_product_prices",
            "ozon_stock_turnover",
            "ozon_fbs_postings",
            "ozon_fbo_postings",
            "ozon_returns",
            "ozon_rfbs_returns",
            "ozon_finance_transactions",
            "ozon_finance_totals",
            "ozon_seller_rating",
            "ozon_seller_rating_history",
            "ozon_reviews",
            "ozon_questions",
        ];

        let names = |server: &OzonMcp| {
            server
                .tool_router
                .list_all()
                .into_iter()
                .map(|tool| tool.name.to_string())
                .collect::<BTreeSet<_>>()
        };
        let preview_names = FINANCE_ACCRUAL_PREVIEW_TOOLS
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<BTreeSet<_>>();

        let default_names = names(&server());
        assert_eq!(default_names.len(), 20);
        assert!(preview_names.is_disjoint(&default_names));
        for name in STABLE_TOOL_NAMES {
            assert!(
                default_names.contains(*name),
                "stable tool {name} disappeared"
            );
        }

        let seed = server();
        let authenticator = jwt_authenticator(&seed.registry);
        let authenticated = OzonMcp::new_authenticated(seed.client, seed.registry, authenticator);
        assert!(preview_names.is_disjoint(&names(&authenticated)));

        let enabled_names = names(&server().with_preview_features(false, true));
        assert_eq!(enabled_names.len(), 23);
        assert!(preview_names.is_subset(&enabled_names));
        assert_eq!(
            enabled_names
                .difference(&default_names)
                .cloned()
                .collect::<BTreeSet<_>>(),
            preview_names
        );

        let disabled_again = server()
            .with_preview_features(false, true)
            .with_preview_features(false, false);
        assert_eq!(names(&disabled_again), default_names);
    }

    #[tokio::test]
    async fn disabled_finance_preview_route_is_rejected_before_network() {
        let (server, requests) = mock_server(1);
        let server = server
            .with_preview_features(false, true)
            .with_preview_features(false, false);
        let body = call_tool_over_http(
            server,
            "ozon_finance_accrual_types",
            json!({"store": "store_a"}),
        )
        .await;

        assert!(body.contains("tool not found"), "{body}");
        assert!(
            requests.recv_timeout(Duration::from_millis(100)).is_err(),
            "disabled route must not reach Ozon"
        );
    }

    #[test]
    fn tool_schemas_match_runtime_bounds_and_keep_store_optional() {
        let tools = server()
            .with_preview_features(false, true)
            .tool_router
            .list_all();
        let schema = |name: &str| {
            tools
                .iter()
                .find(|tool| tool.name == name)
                .expect("tool must be registered")
                .input_schema
                .clone()
        };
        let analytics = schema("ozon_analytics");
        let properties = analytics["properties"].as_object().unwrap();
        assert_eq!(properties["metrics"]["minItems"], json!(1));
        assert_eq!(properties["metrics"]["maxItems"], json!(10));
        assert_eq!(properties["dimensions"]["minItems"], json!(1));
        assert_eq!(properties["dimensions"]["maxItems"], json!(2));
        assert_eq!(properties["limit"]["minimum"], json!(1));
        assert_eq!(properties["limit"]["maximum"], json!(1_000));
        assert!(
            !serde_json::to_string(analytics.as_ref())
                .unwrap()
                .contains("adv_sum_all"),
            "removed Ozon metric must not remain in tool schema"
        );
        assert!(
            !analytics["required"]
                .as_array()
                .unwrap()
                .iter()
                .any(|field| field == "store")
        );

        for (tool, maximum) in [
            ("ozon_product_stocks", 1_000),
            ("ozon_product_prices", 1_000),
            ("ozon_stock_turnover", 1_000),
            ("ozon_fbs_postings", 1_000),
            ("ozon_fbo_postings", 1_000),
            ("ozon_returns", 500),
            ("ozon_rfbs_returns", 100),
            ("ozon_reviews", 100),
        ] {
            let schema = schema(tool);
            let minimum = if tool == "ozon_reviews" {
                MIN_REVIEWS_LIMIT
            } else {
                1
            };
            assert_eq!(
                schema["properties"]["limit"]["minimum"],
                json!(minimum),
                "{tool}"
            );
            assert_eq!(
                schema["properties"]["limit"]["maximum"],
                json!(maximum),
                "{tool}"
            );
        }
        let finance = schema("ozon_finance_transactions");
        assert_eq!(finance["properties"]["page"]["minimum"], json!(1));
        assert_eq!(finance["properties"]["page_size"]["minimum"], json!(1));
        assert_eq!(finance["properties"]["page_size"]["maximum"], json!(1_000));

        let postings = schema("ozon_fbs_postings");
        assert!(postings["properties"].get("cursor").is_some());
        let accrual_postings = schema("ozon_finance_accrual_postings");
        assert_eq!(
            accrual_postings["properties"]["posting_numbers"]["minItems"],
            json!(1)
        );

        let rating_history = schema("ozon_seller_rating_history");
        assert_eq!(
            rating_history["properties"]["ratings"]["minItems"],
            json!(1)
        );
        assert!(
            rating_history["required"]
                .as_array()
                .expect("rating history required fields")
                .iter()
                .any(|field| field == "ratings")
        );

        let returns = schema("ozon_returns");
        assert!(
            !serde_json::to_string(returns.as_ref())
                .unwrap()
                .contains("RFBS")
        );
        let totals = schema("ozon_finance_totals");
        let mut total_fields: Vec<_> = totals["properties"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        total_fields.sort_unstable();
        assert_eq!(
            total_fields,
            [
                "date_from",
                "date_to",
                "posting_number",
                "store",
                "transaction_type"
            ]
        );
    }

    #[test]
    fn tool_schemas_expose_all_input_hardening_bounds() {
        let tools = server()
            .with_preview_features(false, true)
            .tool_router
            .list_all();
        let schema = |name: &str| {
            tools
                .iter()
                .find(|tool| tool.name == name)
                .expect("tool must be registered")
                .input_schema
                .clone()
        };

        for tool in [
            "ozon_analytics",
            "ozon_product_stocks",
            "ozon_product_prices",
            "ozon_stock_turnover",
            "ozon_fbs_postings",
            "ozon_fbo_postings",
            "ozon_returns",
            "ozon_rfbs_returns",
            "ozon_finance_transactions",
            "ozon_finance_totals",
            "ozon_finance_accrual_postings",
            "ozon_finance_accrual_types",
            "ozon_finance_accrual_by_day",
            "ozon_seller_rating",
            "ozon_seller_rating_history",
            "ozon_reviews",
            "ozon_questions",
        ] {
            assert_eq!(
                schema(tool)["properties"]["store"]["minLength"],
                json!(1),
                "{tool}"
            );
            assert_eq!(
                schema(tool)["properties"]["store"]["maxLength"],
                json!(MAX_STORE_SELECTOR_CHARS),
                "{tool}"
            );
        }

        for (tool, fields) in [
            ("ozon_analytics", &["date_from", "date_to"][..]),
            ("ozon_fbs_postings", &["date_from", "date_to"][..]),
            ("ozon_fbo_postings", &["date_from", "date_to"][..]),
            ("ozon_returns", &["date_from", "date_to"][..]),
            ("ozon_rfbs_returns", &["date_from", "date_to"][..]),
            ("ozon_finance_transactions", &["date_from", "date_to"][..]),
            ("ozon_finance_totals", &["date_from", "date_to"][..]),
            ("ozon_seller_rating_history", &["date_from", "date_to"][..]),
            ("ozon_questions", &["date_from", "date_to"][..]),
            ("ozon_finance_accrual_by_day", &["date"][..]),
        ] {
            let schema = schema(tool);
            for field in fields {
                assert_eq!(
                    schema["properties"][field]["minLength"],
                    json!(10),
                    "{tool}.{field}"
                );
                assert_eq!(
                    schema["properties"][field]["maxLength"],
                    json!(10),
                    "{tool}.{field}"
                );
            }
        }

        for tool in ["ozon_product_stocks", "ozon_product_prices"] {
            let schema = schema(tool);
            for field in ["offer_ids", "product_ids"] {
                assert_eq!(
                    schema["properties"][field]["maxItems"],
                    json!(MAX_PRODUCT_FILTER_ITEMS),
                    "{tool}.{field}"
                );
                assert_eq!(
                    schema["properties"][field]["items"]["maxLength"],
                    json!(MAX_IDENTIFIER_CHARS),
                    "{tool}.{field}[]"
                );
            }
            assert_eq!(
                schema["properties"]["cursor"]["maxLength"],
                json!(MAX_OPAQUE_TOKEN_CHARS),
                "{tool}.cursor"
            );
        }

        let turnover = schema("ozon_stock_turnover");
        assert_eq!(turnover["properties"]["skus"]["maxItems"], json!(MAX_SKUS));
        assert_eq!(
            turnover["properties"]["skus"]["items"]["maxLength"],
            json!(MAX_IDENTIFIER_CHARS)
        );

        let postings = schema("ozon_fbs_postings");
        assert_eq!(
            postings["properties"]["status"]["maxLength"],
            json!(MAX_ENUM_VALUE_CHARS)
        );
        assert_eq!(
            postings["properties"]["cursor"]["maxLength"],
            json!(MAX_OPAQUE_TOKEN_CHARS)
        );

        let returns = schema("ozon_returns");
        assert_eq!(
            returns["properties"]["offer_id"]["maxLength"],
            json!(MAX_IDENTIFIER_CHARS)
        );
        assert_eq!(
            returns["properties"]["posting_numbers"]["maxItems"],
            json!(MAX_POSTING_NUMBERS)
        );
        assert_eq!(
            returns["properties"]["posting_numbers"]["items"]["maxLength"],
            json!(MAX_IDENTIFIER_CHARS)
        );

        let rfbs = schema("ozon_rfbs_returns");
        assert_eq!(
            rfbs["properties"]["group_state"]["maxItems"],
            json!(MAX_GROUP_STATES)
        );
        assert_eq!(
            rfbs["properties"]["group_state"]["items"]["maxLength"],
            json!(MAX_ENUM_VALUE_CHARS)
        );

        let finance = schema("ozon_finance_transactions");
        assert_eq!(
            finance["properties"]["operation_types"]["maxItems"],
            json!(MAX_OPERATION_TYPES)
        );
        assert_eq!(
            finance["properties"]["operation_types"]["items"]["maxLength"],
            json!(MAX_ENUM_VALUE_CHARS)
        );
        assert_eq!(finance["properties"]["page"]["maximum"], json!(MAX_PAGE));

        let accrual_postings = schema("ozon_finance_accrual_postings");
        assert_eq!(
            accrual_postings["properties"]["posting_numbers"]["maxItems"],
            json!(MAX_POSTING_NUMBERS)
        );
        assert_eq!(
            accrual_postings["properties"]["posting_numbers"]["items"]["maxLength"],
            json!(MAX_IDENTIFIER_CHARS)
        );

        let rating = schema("ozon_seller_rating_history");
        assert_eq!(
            rating["properties"]["ratings"]["maxItems"],
            json!(MAX_RATINGS)
        );
        assert_eq!(
            rating["properties"]["ratings"]["items"]["maxLength"],
            json!(MAX_ENUM_VALUE_CHARS)
        );

        for tool in ["ozon_reviews", "ozon_questions"] {
            let schema = schema(tool);
            assert_eq!(
                schema["properties"]["status"]["minLength"],
                json!(1),
                "{tool}.status"
            );
            assert_eq!(
                schema["properties"]["status"]["maxLength"],
                json!(MAX_ENUM_VALUE_CHARS),
                "{tool}.status"
            );
            assert_eq!(
                schema["properties"]["last_id"]["maxLength"],
                json!(MAX_OPAQUE_TOKEN_CHARS),
                "{tool}.last_id"
            );
        }

        for (tool, field) in [
            ("ozon_analytics", "offset"),
            ("ozon_stock_turnover", "offset"),
            ("ozon_fbs_postings", "offset"),
            ("ozon_fbo_postings", "offset"),
        ] {
            assert_eq!(
                schema(tool)["properties"][field]["maximum"],
                json!(MAX_OFFSET),
                "{tool}.{field}"
            );
        }

        for (tool, field) in [
            ("ozon_product_stocks", "offer_ids"),
            ("ozon_product_stocks", "product_ids"),
            ("ozon_product_prices", "offer_ids"),
            ("ozon_product_prices", "product_ids"),
            ("ozon_stock_turnover", "skus"),
            ("ozon_returns", "posting_numbers"),
            ("ozon_rfbs_returns", "group_state"),
            ("ozon_finance_transactions", "operation_types"),
            ("ozon_finance_accrual_postings", "posting_numbers"),
            ("ozon_seller_rating_history", "ratings"),
        ] {
            assert_eq!(
                schema(tool)["properties"][field]["items"]["minLength"],
                json!(1),
                "{tool}.{field}[]"
            );
        }

        for tool in ["ozon_returns", "ozon_rfbs_returns"] {
            assert_eq!(
                schema(tool)["properties"]["last_id"]["maximum"],
                json!(u64::MAX),
                "{tool}.last_id"
            );
        }
    }

    #[test]
    fn customer_feedback_inputs_have_ozon_safe_defaults() {
        let reviews: ReviewsInput = serde_json::from_value(json!({})).unwrap();
        assert_eq!(reviews.limit, 100);
        assert_eq!(reviews.status, "ALL");

        let questions: QuestionsInput = serde_json::from_value(json!({
            "date_from": "2026-08-08",
            "date_to": "2026-08-08"
        }))
        .unwrap();
        assert_eq!(questions.status, "ALL");

        assert!(
            serde_json::from_value::<RatingHistoryInput>(json!({
                "date_from": "2026-08-08",
                "date_to": "2026-08-08"
            }))
            .is_err(),
            "rating history must require at least one explicit Ozon rating code"
        );
    }

    #[test]
    fn ozon_network_endpoints_are_confined_to_explicit_read_only_allowlist() {
        const EXPECTED: &[&str] = &[
            "/v1/analytics/data",
            "/v1/analytics/turnover/stocks",
            "/v1/finance/accrual/by-day",
            "/v1/finance/accrual/postings",
            "/v1/finance/accrual/types",
            "/v1/question/list",
            "/v1/rating/history",
            "/v1/rating/summary",
            "/v1/returns/list",
            "/v1/review/list",
            "/v2/posting/fbo/list",
            "/v2/returns/rfbs/list",
            "/v3/finance/transaction/list",
            "/v3/finance/transaction/totals",
            "/v3/posting/fbo/list",
            "/v3/posting/fbs/list",
            "/v4/posting/fbs/list",
            "/v4/product/info/stocks",
            "/v5/product/info/prices",
        ];
        assert_eq!(READ_ONLY_ENDPOINT_ALLOWLIST, EXPECTED);
        for endpoint in READ_ONLY_ENDPOINT_ALLOWLIST {
            for forbidden in [
                "/cancel", "/create", "/delete", "/import", "/set", "/ship", "/update",
            ] {
                assert!(
                    !endpoint.contains(forbidden),
                    "{endpoint} contains {forbidden}"
                );
            }
            assert!(is_read_only_endpoint_allowed(endpoint));
        }
        for endpoint in [
            "/v1/product/update",
            "/v1/order/create",
            "/v2/posting/fbs/ship",
            "/v2/posting/fbs/cancel",
        ] {
            assert!(!is_read_only_endpoint_allowed(endpoint));
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
        assert_eq!(PostingKind::Fbs.endpoint(), "/v3/posting/fbs/list");
        assert_eq!(PostingKind::Fbo.endpoint(), "/v2/posting/fbo/list");
    }

    #[tokio::test]
    async fn stores_status_and_server_metadata_do_not_expose_secrets() {
        let (server, _) = mock_server(0);
        let status = server
            .stores_status(RequestIdentity::dev(), Parameters(EmptyInput {}))
            .await
            .unwrap()
            .0;
        assert_eq!(status.actor.id, "admin");
        assert_eq!(status.actor.name, "Administrator");
        assert_eq!(status.actor.role, Role::Admin);
        assert_eq!(status.default_store, None);
        assert_eq!(status.access_mode, "server-side RBAC, read-only allowlist");
        assert_eq!(status.stores.len(), 2);
        assert!(status.stores[0].configured);
        assert!(!status.stores[1].configured);
        assert_eq!(status.stores[0].seller_client_id, "client-a");
        assert_eq!(status.stores[0].manager, "Administrator");

        let accounts = server
            .marketplace_accounts(RequestIdentity::dev(), Parameters(EmptyInput {}))
            .await
            .unwrap()
            .0;
        assert_eq!(accounts.actor.id, "admin");
        assert_eq!(accounts.accounts.len(), 3);
        assert_eq!(accounts.accounts[0].account_id, "store_a");
        assert_eq!(
            accounts.accounts[0].store_id,
            Some(StoreId::from("store_a"))
        );
        assert_eq!(
            accounts.accounts[0].integration_status,
            "read_only_ozon_api"
        );
        assert!(accounts.accounts[0].configured);
        assert_eq!(
            accounts.accounts[2].integration_status,
            "read_only_wildberries_api"
        );
        assert!(!accounts.accounts[2].configured);

        let members = server
            .list_members(RequestIdentity::dev(), Parameters(EmptyInput {}))
            .await
            .unwrap()
            .0;
        assert_eq!(members.actor.id, "admin");
        assert_eq!(members.members.len(), 2);
        assert_eq!(members.members[0].role, Role::Admin);
        assert_eq!(members.members[0].account_ids.len(), 3);
        assert_eq!(members.members[0].accounts.len(), 3);

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
        let server = manager_server("manager");
        let status = server
            .stores_status(RequestIdentity::dev(), Parameters(EmptyInput {}))
            .await
            .unwrap()
            .0;
        assert_eq!(status.actor.role, Role::Manager);
        assert_eq!(status.default_store, Some(StoreId::from("store_b")));
        assert_eq!(status.stores.len(), 1);
        assert_eq!(status.stores[0].id, StoreId::from("store_b"));

        let accounts = server
            .marketplace_accounts(RequestIdentity::dev(), Parameters(EmptyInput {}))
            .await
            .unwrap()
            .0;
        assert_eq!(accounts.accounts.len(), 1);
        assert_eq!(accounts.accounts[0].manager, "Manager");

        let members = server
            .list_members(RequestIdentity::dev(), Parameters(EmptyInput {}))
            .await
            .unwrap()
            .0;
        assert_eq!(members.members.len(), 1);
        assert_eq!(members.members[0].id, "manager");
        assert_eq!(members.members[0].role, Role::Manager);
        assert_eq!(members.members[0].account_ids, vec!["account_b".to_owned()]);
        assert_eq!(members.members[0].accounts[0].account_id, "account_b");
        assert_eq!(
            members.members[0].accounts[0].store_id,
            Some(StoreId::from("store_b"))
        );

        let denied = server
            .seller_rating(
                RequestIdentity::dev(),
                Parameters(StoreOnlyInput {
                    store: Some(StoreId::from("store_a")),
                }),
            )
            .await
            .err()
            .unwrap();
        assert!(denied.starts_with(ACCESS_DENIED));
        assert!(!denied.contains("Administrator"));

        let allowed_but_unconfigured = server
            .seller_rating(
                RequestIdentity::dev(),
                Parameters(StoreOnlyInput {
                    store: Some(StoreId::from("store_b")),
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
        let registry = registry_source();
        let authenticator = jwt_authenticator(&registry);
        let server = OzonMcp::new_authenticated(client, registry, authenticator);

        let denied = server
            .marketplace_accounts(RequestIdentity::dev(), Parameters(EmptyInput {}))
            .await
            .err()
            .unwrap();
        assert!(denied.starts_with(ACCESS_DENIED));

        let manager = server
            .marketplace_accounts(
                RequestIdentity::authenticated("manager"),
                Parameters(EmptyInput {}),
            )
            .await
            .unwrap()
            .0;
        assert_eq!(manager.actor.id, "manager");
        assert_eq!(manager.accounts.len(), 1);
        assert_eq!(manager.accounts[0].id, "account_b");
    }

    #[tokio::test]
    async fn streamable_http_propagates_the_verified_actor_to_mcp_tools() {
        let client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
            BTreeMap::new(),
        )
        .unwrap();
        let registry = registry_source();
        let authenticator = jwt_authenticator(&registry);
        let server = Arc::new(OzonMcp::new_authenticated(client, registry, authenticator));
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
                actor_id: "admin".to_owned(),
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
        assert!(body.contains("admin"), "{body}");
        assert!(!body.contains(ACCESS_DENIED), "{body}");
        task.abort();
    }

    #[tokio::test]
    async fn mcp_json_boundary_resolves_configured_account_aliases_to_canonical_store_ids() {
        for (selector, canonical) in [
            ("account_a", "store_a"),
            ("account_b", "store_b"),
            ("account_c", "store_c"),
        ] {
            let (server, requests) = selector_mock_server("admin", 1);
            let body =
                call_tool_over_http(server, "ozon_seller_rating", json!({"store": selector})).await;
            assert!(
                body.contains(&format!(r#"\"store\":\"{canonical}\""#)),
                "{body}"
            );
            let request = requests.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(request_path_and_body(&request).0, "/v1/rating/summary");
        }

        let (server, requests) = selector_mock_server("admin", 1);
        let body =
            call_tool_over_http(server, "ozon_seller_rating", json!({"store": "store_a"})).await;
        assert!(body.contains(r#"\"store\":\"store_a\""#), "{body}");
        requests.recv_timeout(Duration::from_secs(3)).unwrap();
    }

    #[tokio::test]
    async fn mcp_json_boundary_is_fail_closed_for_omitted_unknown_and_denied_store() {
        let (admin, _) = selector_mock_server("admin", 0);
        let body = call_tool_over_http(admin, "ozon_seller_rating", json!({})).await;
        assert!(body.contains(STORE_REQUIRED), "{body}");

        let (manager, requests) = selector_mock_server("manager_a", 1);
        let body = call_tool_over_http(manager, "ozon_seller_rating", json!({})).await;
        assert!(body.contains(r#"\"store\":\"store_a\""#), "{body}");
        requests.recv_timeout(Duration::from_secs(3)).unwrap();

        let (no_ozon, _) = selector_mock_server("manager_wb", 0);
        let body = call_tool_over_http(no_ozon, "ozon_seller_rating", json!({})).await;
        assert!(body.contains(NO_ACCESSIBLE_STORE), "{body}");

        let (admin, _) = selector_mock_server("admin", 0);
        let body = call_tool_over_http(
            admin,
            "ozon_seller_rating",
            json!({"store": "not-registered"}),
        )
        .await;
        assert!(body.contains(UNKNOWN_STORE), "{body}");
        assert!(!body.contains(ACCESS_DENIED), "{body}");

        let (manager, _) = selector_mock_server("manager_a", 0);
        let body =
            call_tool_over_http(manager, "ozon_seller_rating", json!({"store": "account_b"})).await;
        assert!(body.contains(ACCESS_DENIED), "{body}");
        assert!(!body.contains("store_b"), "{body}");
        assert!(!body.contains("Example organization B"), "{body}");
    }

    #[tokio::test]
    async fn mcp_json_boundary_rejects_unknown_input_fields() {
        let (server, _) = selector_mock_server("admin", 0);
        let body = call_tool_over_http(
            server,
            "ozon_seller_rating",
            json!({"store": "store_a", "store_id": "store_c"}),
        )
        .await;
        assert!(body.contains("unknown field"), "{body}");
        assert!(body.contains("store_id"), "{body}");

        let (server, _) = selector_mock_server("admin", 0);
        let body =
            call_tool_over_http(server, "marketplace_accounts", json!({"unexpected": true})).await;
        assert!(body.contains("unknown field"), "{body}");
        assert!(body.contains("unexpected"), "{body}");
    }

    #[tokio::test]
    async fn legacy_returns_tool_rejects_rfbs_at_the_mcp_json_boundary() {
        let (server, _) = selector_mock_server("admin", 0);
        let body = call_tool_over_http(
            server,
            "ozon_returns",
            json!({
                "store": "store_a",
                "date_from": "2026-03-01",
                "date_to": "2026-03-02",
                "return_schema": "RFBS"
            }),
        )
        .await;
        assert!(body.contains("unknown variant"), "{body}");
        assert!(body.contains("RFBS"), "{body}");
    }

    #[tokio::test]
    async fn access_changes_are_loaded_without_restarting_the_server() {
        let server = manager_server("manager");
        assert_eq!(
            server
                .marketplace_accounts(RequestIdentity::dev(), Parameters(EmptyInput {}))
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
                .marketplace_accounts(RequestIdentity::dev(), Parameters(EmptyInput {}))
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
        let result = server
            .marketplace_accounts(RequestIdentity::dev(), Parameters(EmptyInput {}))
            .await;
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(error.starts_with("MCP_ACCESS_CONFIG_ERROR:"));
        assert!(error.contains("неверный JSON"));
    }

    #[tokio::test]
    async fn credential_binding_rebind_requires_restart_before_any_network_call() {
        enum CredentialField {
            AccountId,
            StoreId,
            ClientIdEnv,
        }

        for (field, value) in [
            (CredentialField::AccountId, "store-a-renamed"),
            (CredentialField::StoreId, "store-a-renamed"),
            (CredentialField::ClientIdEnv, "OZON_CLIENT_ID_REBOUND"),
        ] {
            let (server, requests) = mock_server(0);
            let mut document: Value =
                serde_json::from_str(&fs::read_to_string(server.registry.path()).unwrap()).unwrap();
            let field_name = match field {
                CredentialField::AccountId => {
                    document["accounts"][0]["id"] = json!(value);
                    "account_id"
                }
                CredentialField::StoreId => {
                    document["accounts"][0]["ozon"]["store_id"] = json!(value);
                    "store_id"
                }
                CredentialField::ClientIdEnv => {
                    document["accounts"][0]["ozon"]["client_id_env"] = json!(value);
                    "client_id_env"
                }
            };
            fs::write(
                server.registry.path(),
                serde_json::to_vec_pretty(&document).unwrap(),
            )
            .unwrap();

            let error = server
                .seller_rating(
                    RequestIdentity::dev(),
                    Parameters(StoreOnlyInput {
                        store: Some(StoreId::from("store_a")),
                    }),
                )
                .await
                .err()
                .unwrap();
            assert!(
                error.starts_with("MCP_ACCESS_CONFIG_RESTART_REQUIRED:"),
                "{field_name}: {error}"
            );
            assert!(
                requests.recv_timeout(Duration::from_millis(50)).is_err(),
                "{field_name} unexpectedly reached Ozon HTTP"
            );
        }
    }

    #[tokio::test]
    async fn every_read_only_tool_sends_the_exact_ozon_contract() {
        let (server, requests) = mock_server(14);
        let mut results = Vec::new();

        results.push(
            server
                .analytics(
                    RequestIdentity::dev(),
                    Parameters(AnalyticsInput {
                        store: Some(StoreId::from("store_a")),
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
                        store: Some(StoreId::from("store_a")),
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
                        store: Some(StoreId::from("store_a")),
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
                        store: Some(StoreId::from("store_a")),
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
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-02-01".to_owned(),
                        date_to: "2026-02-02".to_owned(),
                        status: "awaiting_packaging".to_owned(),
                        limit: 40,
                        offset: 3,
                        cursor: None,
                        direction: SortDirection::Asc,
                    }),
                )
                .await,
            server
                .fbo_postings(
                    RequestIdentity::dev(),
                    Parameters(PostingListInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-02-01".to_owned(),
                        date_to: "2026-02-02".to_owned(),
                        status: String::new(),
                        limit: 50,
                        offset: 0,
                        cursor: None,
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
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-03-01".to_owned(),
                        date_to: "2026-03-03".to_owned(),
                        return_schema: ReturnSchema::Fbs,
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
        results.push(
            server
                .rfbs_returns(
                    RequestIdentity::dev(),
                    Parameters(RfbsReturnsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-03-04".to_owned(),
                        date_to: "2026-03-05".to_owned(),
                        offer_id: "offer-rfbs".to_owned(),
                        posting_number: "posting-rfbs".to_owned(),
                        group_state: vec!["awaiting_return".to_owned()],
                        last_id: 8,
                        limit: 61,
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
                        store: Some(StoreId::from("store_a")),
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
                    Parameters(FinanceTotalsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-04-01".to_owned(),
                        date_to: "2026-04-30".to_owned(),
                        posting_number: String::new(),
                        transaction_type: "all".to_owned(),
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
                        store: Some(StoreId::from("store_a")),
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
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-05-01".to_owned(),
                        date_to: "2026-05-10".to_owned(),
                        ratings: vec!["rating_shipment_delay_cb".to_owned()],
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
                        store: Some(StoreId::from("store_a")),
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
                        store: Some(StoreId::from("store_a")),
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

        let expected_contracts = [
            (
                "/v1/analytics/data",
                json!({
                    "date_from": "2026-01-01",
                    "date_to": "2026-01-31",
                    "metrics": ["revenue"],
                    "dimension": ["sku"],
                    "filters": [],
                    "sort": [{"key": "revenue", "order": "DESC"}],
                    "limit": 25,
                    "offset": 5,
                }),
            ),
            (
                "/v4/product/info/stocks",
                json!({
                    "cursor": "cursor-1",
                    "filter": {
                        "offer_id": ["offer-1"],
                        "product_id": [],
                        "visibility": "VISIBLE",
                    },
                    "limit": 10,
                }),
            ),
            (
                "/v5/product/info/prices",
                json!({
                    "cursor": "",
                    "filter": {
                        "offer_id": [],
                        "product_id": ["123"],
                        "visibility": "ALL",
                    },
                    "limit": 20,
                }),
            ),
            (
                "/v1/analytics/turnover/stocks",
                json!({"limit": 30, "offset": 2, "sku": ["sku-1"]}),
            ),
            (
                "/v3/posting/fbs/list",
                json!({
                    "dir": "ASC",
                    "filter": {
                        "since": "2026-02-01T00:00:00.000Z",
                        "to": "2026-02-02T23:59:59.999Z",
                        "status": "awaiting_packaging",
                    },
                    "limit": 40,
                    "offset": 3,
                }),
            ),
            (
                "/v2/posting/fbo/list",
                json!({
                    "dir": "DESC",
                    "filter": {
                        "since": "2026-02-01T00:00:00.000Z",
                        "to": "2026-02-02T23:59:59.999Z",
                        "status": "",
                    },
                    "limit": 50,
                    "offset": 0,
                    "translit": false,
                    "with": {"analytics_data": true, "financial_data": true},
                }),
            ),
            (
                "/v1/returns/list",
                json!({
                    "filter": {
                        "visual_status_change_moment": {
                            "time_from": "2026-03-01T00:00:00.000Z",
                            "time_to": "2026-03-03T23:59:59.999Z",
                        },
                        "posting_numbers": ["posting-1"],
                        "offer_id": "offer-2",
                        "return_schema": "FBS",
                    },
                    "limit": 60,
                    "last_id": 7,
                }),
            ),
            (
                "/v2/returns/rfbs/list",
                json!({
                    "filter": {
                        "offer_id": "offer-rfbs",
                        "posting_number": "posting-rfbs",
                        "group_state": ["awaiting_return"],
                        "created_at": {
                            "from": "2026-03-04T00:00:00.000Z",
                            "to": "2026-03-05T23:59:59.999Z",
                        },
                    },
                    "last_id": 8,
                    "limit": 61,
                }),
            ),
            (
                "/v3/finance/transaction/list",
                json!({
                    "filter": {
                        "date": {
                            "from": "2026-04-01T00:00:00.000Z",
                            "to": "2026-04-30T23:59:59.999Z",
                        },
                        "operation_type": ["OperationAgentDeliveredToCustomer"],
                        "posting_number": "posting-2",
                        "transaction_type": "orders",
                    },
                    "page": 2,
                    "page_size": 70,
                }),
            ),
            (
                "/v3/finance/transaction/totals",
                json!({
                    "date": {
                        "from": "2026-04-01T00:00:00.000Z",
                        "to": "2026-04-30T23:59:59.999Z",
                    },
                    "posting_number": "",
                    "transaction_type": "all",
                }),
            ),
            ("/v1/rating/summary", json!({})),
            (
                "/v1/rating/history",
                json!({
                    "date_from": "2026-05-01T00:00:00.000Z",
                    "date_to": "2026-05-10T23:59:59.999Z",
                    "ratings": ["rating_shipment_delay_cb"],
                    "with_premium_scores": false,
                }),
            ),
            (
                "/v1/review/list",
                json!({
                    "last_id": "review-cursor",
                    "limit": 80,
                    "sort_dir": "DESC",
                    "status": "UNPROCESSED",
                }),
            ),
            (
                "/v1/question/list",
                json!({
                    "filter": {
                        "date_from": "2026-06-01T00:00:00.000Z",
                        "date_to": "2026-06-02T23:59:59.999Z",
                        "status": "NEW",
                    },
                    "last_id": "question-cursor",
                }),
            ),
        ];
        assert_eq!(results.len(), expected_contracts.len());
        for (result, (expected_path, expected_body)) in results.iter().zip(expected_contracts) {
            assert_eq!(result.endpoint, expected_path);
            assert_eq!(result.store, StoreId::from("store_a"));
            assert!(!result.fetched_at.is_empty());
            assert_eq!(result.data, json!({ "ok": true }));
            let request = requests.recv_timeout(Duration::from_secs(3)).unwrap();
            let (actual_path, actual_body) = request_path_and_body(&request);
            assert_eq!(actual_path, expected_path);
            assert_eq!(actual_body, expected_body, "{expected_path}");
        }
    }

    #[tokio::test]
    async fn preview_tools_send_exact_candidate_contracts_when_explicitly_enabled() {
        let (server, requests) = mock_server(6);
        let server = server.with_preview_features(true, true);

        let results = [
            server
                .fbs_postings(
                    RequestIdentity::dev(),
                    Parameters(PostingListInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-07-01".to_owned(),
                        date_to: "2026-07-02".to_owned(),
                        status: "delivered".to_owned(),
                        limit: 100,
                        offset: 0,
                        cursor: Some("fbs-cursor".to_owned()),
                        direction: SortDirection::Asc,
                    }),
                )
                .await,
            server
                .fbo_postings(
                    RequestIdentity::dev(),
                    Parameters(PostingListInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-07-03".to_owned(),
                        date_to: "2026-07-04".to_owned(),
                        status: String::new(),
                        limit: 50,
                        offset: 0,
                        cursor: None,
                        direction: SortDirection::Desc,
                    }),
                )
                .await,
            server
                .finance_accrual_postings_preview(
                    RequestIdentity::dev(),
                    Parameters(FinanceAccrualPostingsPreviewInput {
                        store: Some(StoreId::from("store_a")),
                        posting_numbers: vec!["posting-1".to_owned(), "posting-2".to_owned()],
                    }),
                )
                .await,
            server
                .finance_accrual_types_preview(
                    RequestIdentity::dev(),
                    Parameters(FinanceAccrualTypesPreviewInput {
                        store: Some(StoreId::from("store_a")),
                    }),
                )
                .await,
            server
                .finance_accrual_by_day_preview(
                    RequestIdentity::dev(),
                    Parameters(FinanceAccrualByDayPreviewInput {
                        store: Some(StoreId::from("store_a")),
                        date: "2026-07-05".to_owned(),
                        last_id: "next-page".to_owned(),
                    }),
                )
                .await,
            server
                .finance_accrual_by_day_preview(
                    RequestIdentity::dev(),
                    Parameters(FinanceAccrualByDayPreviewInput {
                        store: Some(StoreId::from("store_a")),
                        date: "2026-07-06".to_owned(),
                        last_id: String::new(),
                    }),
                )
                .await,
        ];

        let expected = [
            (
                "/v4/posting/fbs/list",
                json!({
                    "cursor": "fbs-cursor",
                    "filter": {
                        "since": "2026-07-01T00:00:00.000Z",
                        "to": "2026-07-02T23:59:59.999Z",
                        "statuses": ["delivered"],
                    },
                    "limit": 100,
                    "sort_dir": "ASC",
                    "translit": false,
                    "with": {
                        "analytics_data": true,
                        "barcodes": true,
                        "financial_data": true,
                        "legal_info": false,
                    },
                }),
            ),
            (
                "/v3/posting/fbo/list",
                json!({
                    "filter": {
                        "since": "2026-07-03T00:00:00.000Z",
                        "to": "2026-07-04T23:59:59.999Z",
                        "statuses": [],
                    },
                    "limit": 50,
                    "sort_dir": "DESC",
                    "translit": false,
                    "with": {
                        "analytics_data": true,
                        "financial_data": true,
                        "legal_info": false,
                    },
                }),
            ),
            (
                "/v1/finance/accrual/postings",
                json!({"posting_numbers": ["posting-1", "posting-2"]}),
            ),
            ("/v1/finance/accrual/types", json!({})),
            (
                "/v1/finance/accrual/by-day",
                json!({"date": "2026-07-05", "last_id": "next-page"}),
            ),
            ("/v1/finance/accrual/by-day", json!({"date": "2026-07-06"})),
        ];

        for (result, (expected_path, expected_body)) in results.into_iter().zip(expected) {
            let result = result.unwrap().0;
            assert_eq!(result.endpoint, expected_path);
            let request = requests.recv_timeout(Duration::from_secs(3)).unwrap();
            let (actual_path, actual_body) = request_path_and_body(&request);
            assert_eq!(actual_path, expected_path);
            assert_eq!(actual_body, expected_body, "{expected_path}");
        }
    }

    #[tokio::test]
    async fn analytics_without_sort_emits_an_empty_sort_list() {
        let (server, requests) = mock_server(1);
        server
            .analytics(
                RequestIdentity::dev(),
                Parameters(AnalyticsInput {
                    store: Some(StoreId::from("store_a")),
                    date_from: "2026-01-01".to_owned(),
                    date_to: "2026-01-02".to_owned(),
                    metrics: vec![AnalyticsMetric::Revenue],
                    dimensions: vec![AnalyticsDimension::Sku],
                    limit: 25,
                    offset: 0,
                    sort_by: None,
                    sort_direction: SortDirection::Asc,
                }),
            )
            .await
            .unwrap();

        let request = requests.recv_timeout(Duration::from_secs(3)).unwrap();
        let (path, body) = request_path_and_body(&request);
        assert_eq!(path, "/v1/analytics/data");
        assert_eq!(body["sort"], json!([]));
    }

    #[tokio::test]
    async fn previews_are_disabled_by_default_and_reject_invalid_inputs_before_network() {
        let disabled_server = server();
        assert!(!disabled_server.postings_vnext);
        assert!(!disabled_server.finance_accruals_preview);
        let error = disabled_server
            .finance_accrual_types_preview(
                RequestIdentity::dev(),
                Parameters(FinanceAccrualTypesPreviewInput {
                    store: Some(StoreId::from("store_a")),
                }),
            )
            .await
            .err()
            .unwrap();
        assert!(error.starts_with(PREVIEW_DISABLED));

        let server = server().with_preview_features(true, true);
        let error = server
            .fbs_postings(
                RequestIdentity::dev(),
                Parameters(PostingListInput {
                    store: Some(StoreId::from("store_a")),
                    date_from: "2026-07-01".to_owned(),
                    date_to: "2026-07-02".to_owned(),
                    status: String::new(),
                    limit: 100,
                    offset: 1,
                    cursor: None,
                    direction: SortDirection::Asc,
                }),
            )
            .await
            .err()
            .unwrap();
        assert!(error.starts_with(PREVIEW_CURSOR_REQUIRED));

        let error = server
            .fbo_postings(
                RequestIdentity::dev(),
                Parameters(PostingListInput {
                    store: Some(StoreId::from("store_a")),
                    date_from: "2026-07-01".to_owned(),
                    date_to: "2026-07-02".to_owned(),
                    status: String::new(),
                    limit: 101,
                    offset: 0,
                    cursor: None,
                    direction: SortDirection::Asc,
                }),
            )
            .await
            .err()
            .unwrap();
        assert!(error.contains("от 1 до 100"));

        let error = server
            .finance_accrual_postings_preview(
                RequestIdentity::dev(),
                Parameters(FinanceAccrualPostingsPreviewInput {
                    store: Some(StoreId::from("store_a")),
                    posting_numbers: Vec::new(),
                }),
            )
            .await
            .err()
            .unwrap();
        assert!(error.contains("posting_numbers"));

        let error = server
            .finance_accrual_by_day_preview(
                RequestIdentity::dev(),
                Parameters(FinanceAccrualByDayPreviewInput {
                    store: Some(StoreId::from("store_a")),
                    date: "07-05-2026".to_owned(),
                    last_id: String::new(),
                }),
            )
            .await
            .err()
            .unwrap();
        assert!(error.contains("YYYY-MM-DD"));
    }

    #[tokio::test]
    async fn oversized_scalar_inputs_fail_before_calling_ozon() {
        let (server, requests) = mock_server(0);
        let server = server.with_preview_features(true, true);
        let oversized_store = "s".repeat(MAX_STORE_SELECTOR_CHARS + 1);
        let oversized_identifier = "i".repeat(MAX_IDENTIFIER_CHARS + 1);
        let oversized_enum = "e".repeat(MAX_ENUM_VALUE_CHARS + 1);
        let oversized_token = "t".repeat(MAX_OPAQUE_TOKEN_CHARS + 1);

        assert_validation_error(
            server
                .request(
                    &RequestIdentity::dev(),
                    Some(StoreId::from("store_a")),
                    "/v1/product/update",
                    json!({}),
                )
                .await,
            READ_ONLY_ENDPOINT_DENIED,
        );
        assert_validation_error(
            server
                .seller_rating(
                    RequestIdentity::dev(),
                    Parameters(StoreOnlyInput {
                        store: Some(StoreId::new(oversized_store)),
                    }),
                )
                .await,
            "store",
        );
        assert_validation_error(
            server
                .seller_rating(
                    RequestIdentity::dev(),
                    Parameters(StoreOnlyInput {
                        store: Some(StoreId::from("  \t")),
                    }),
                )
                .await,
            "store",
        );
        assert_validation_error(
            server
                .product_stocks(
                    RequestIdentity::dev(),
                    Parameters(ProductFilterInput {
                        store: Some(StoreId::from("store_a")),
                        offer_ids: vec!["  \t".to_owned()],
                        product_ids: Vec::new(),
                        visibility: Visibility::All,
                        limit: 100,
                        cursor: None,
                    }),
                )
                .await,
            "offer_ids",
        );
        assert_validation_error(
            server
                .product_stocks(
                    RequestIdentity::dev(),
                    Parameters(ProductFilterInput {
                        store: Some(StoreId::from("store_a")),
                        offer_ids: vec![oversized_identifier.clone()],
                        product_ids: Vec::new(),
                        visibility: Visibility::All,
                        limit: 100,
                        cursor: None,
                    }),
                )
                .await,
            "offer_ids",
        );
        assert_validation_error(
            server
                .product_prices(
                    RequestIdentity::dev(),
                    Parameters(ProductFilterInput {
                        store: Some(StoreId::from("store_a")),
                        offer_ids: Vec::new(),
                        product_ids: vec![oversized_identifier.clone()],
                        visibility: Visibility::All,
                        limit: 100,
                        cursor: None,
                    }),
                )
                .await,
            "product_ids",
        );
        assert_validation_error(
            server
                .product_prices(
                    RequestIdentity::dev(),
                    Parameters(ProductFilterInput {
                        store: Some(StoreId::from("store_a")),
                        offer_ids: Vec::new(),
                        product_ids: Vec::new(),
                        visibility: Visibility::All,
                        limit: 100,
                        cursor: Some(oversized_token.clone()),
                    }),
                )
                .await,
            "cursor",
        );
        assert_validation_error(
            server
                .stock_turnover(
                    RequestIdentity::dev(),
                    Parameters(TurnoverInput {
                        store: Some(StoreId::from("store_a")),
                        skus: vec![oversized_identifier.clone()],
                        limit: 100,
                        offset: 0,
                    }),
                )
                .await,
            "skus",
        );
        assert_validation_error(
            server
                .fbs_postings(
                    RequestIdentity::dev(),
                    Parameters(PostingListInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        status: oversized_enum.clone(),
                        limit: 100,
                        offset: 0,
                        cursor: None,
                        direction: SortDirection::Asc,
                    }),
                )
                .await,
            "status",
        );
        assert_validation_error(
            server
                .fbo_postings(
                    RequestIdentity::dev(),
                    Parameters(PostingListInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        status: String::new(),
                        limit: 100,
                        offset: 0,
                        cursor: Some(oversized_token.clone()),
                        direction: SortDirection::Asc,
                    }),
                )
                .await,
            "cursor",
        );
        assert_validation_error(
            server
                .returns(
                    RequestIdentity::dev(),
                    Parameters(ReturnsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        return_schema: ReturnSchema::Fbo,
                        offer_id: oversized_identifier.clone(),
                        posting_numbers: Vec::new(),
                        limit: 100,
                        last_id: 0,
                    }),
                )
                .await,
            "offer_id",
        );
        assert_validation_error(
            server
                .returns(
                    RequestIdentity::dev(),
                    Parameters(ReturnsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        return_schema: ReturnSchema::Fbo,
                        offer_id: String::new(),
                        posting_numbers: vec![oversized_identifier.clone()],
                        limit: 100,
                        last_id: 0,
                    }),
                )
                .await,
            "posting_numbers",
        );
        assert_validation_error(
            server
                .rfbs_returns(
                    RequestIdentity::dev(),
                    Parameters(RfbsReturnsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        offer_id: oversized_identifier.clone(),
                        posting_number: String::new(),
                        group_state: Vec::new(),
                        last_id: 0,
                        limit: 100,
                    }),
                )
                .await,
            "offer_id",
        );
        assert_validation_error(
            server
                .rfbs_returns(
                    RequestIdentity::dev(),
                    Parameters(RfbsReturnsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        offer_id: String::new(),
                        posting_number: oversized_identifier.clone(),
                        group_state: Vec::new(),
                        last_id: 0,
                        limit: 100,
                    }),
                )
                .await,
            "posting_number",
        );
        assert_validation_error(
            server
                .rfbs_returns(
                    RequestIdentity::dev(),
                    Parameters(RfbsReturnsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        offer_id: String::new(),
                        posting_number: String::new(),
                        group_state: vec![oversized_enum.clone()],
                        last_id: 0,
                        limit: 100,
                    }),
                )
                .await,
            "group_state",
        );
        assert_validation_error(
            server
                .finance_transactions(
                    RequestIdentity::dev(),
                    Parameters(FinanceInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        posting_number: oversized_identifier.clone(),
                        operation_types: Vec::new(),
                        transaction_type: "all".to_owned(),
                        page: 1,
                        page_size: 100,
                    }),
                )
                .await,
            "posting_number",
        );
        assert_validation_error(
            server
                .finance_transactions(
                    RequestIdentity::dev(),
                    Parameters(FinanceInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        posting_number: String::new(),
                        operation_types: vec![oversized_enum.clone()],
                        transaction_type: "all".to_owned(),
                        page: 1,
                        page_size: 100,
                    }),
                )
                .await,
            "operation_types",
        );
        assert_validation_error(
            server
                .finance_transactions(
                    RequestIdentity::dev(),
                    Parameters(FinanceInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        posting_number: String::new(),
                        operation_types: Vec::new(),
                        transaction_type: oversized_enum.clone(),
                        page: 1,
                        page_size: 100,
                    }),
                )
                .await,
            "transaction_type",
        );
        assert_validation_error(
            server
                .finance_totals(
                    RequestIdentity::dev(),
                    Parameters(FinanceTotalsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        posting_number: oversized_identifier.clone(),
                        transaction_type: "all".to_owned(),
                    }),
                )
                .await,
            "posting_number",
        );
        assert_validation_error(
            server
                .finance_totals(
                    RequestIdentity::dev(),
                    Parameters(FinanceTotalsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        posting_number: String::new(),
                        transaction_type: oversized_enum.clone(),
                    }),
                )
                .await,
            "transaction_type",
        );
        assert_validation_error(
            server
                .finance_accrual_postings_preview(
                    RequestIdentity::dev(),
                    Parameters(FinanceAccrualPostingsPreviewInput {
                        store: Some(StoreId::from("store_a")),
                        posting_numbers: vec![oversized_identifier.clone()],
                    }),
                )
                .await,
            "posting_numbers",
        );
        assert_validation_error(
            server
                .finance_accrual_by_day_preview(
                    RequestIdentity::dev(),
                    Parameters(FinanceAccrualByDayPreviewInput {
                        store: Some(StoreId::from("store_a")),
                        date: "2026-01-01".to_owned(),
                        last_id: oversized_token.clone(),
                    }),
                )
                .await,
            "last_id",
        );
        assert_validation_error(
            server
                .seller_rating_history(
                    RequestIdentity::dev(),
                    Parameters(RatingHistoryInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        ratings: vec![oversized_enum.clone()],
                        with_premium_scores: true,
                    }),
                )
                .await,
            "ratings",
        );
        assert_validation_error(
            server
                .reviews(
                    RequestIdentity::dev(),
                    Parameters(ReviewsInput {
                        store: Some(StoreId::from("store_a")),
                        limit: 100,
                        last_id: oversized_token.clone(),
                        status: "ALL".to_owned(),
                        direction: SortDirection::Asc,
                    }),
                )
                .await,
            "last_id",
        );
        assert_validation_error(
            server
                .reviews(
                    RequestIdentity::dev(),
                    Parameters(ReviewsInput {
                        store: Some(StoreId::from("store_a")),
                        limit: 100,
                        last_id: String::new(),
                        status: oversized_enum.clone(),
                        direction: SortDirection::Asc,
                    }),
                )
                .await,
            "status",
        );
        assert_validation_error(
            server
                .questions(
                    RequestIdentity::dev(),
                    Parameters(QuestionsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        status: "ALL".to_owned(),
                        last_id: oversized_token,
                    }),
                )
                .await,
            "last_id",
        );
        assert_validation_error(
            server
                .questions(
                    RequestIdentity::dev(),
                    Parameters(QuestionsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        status: oversized_enum,
                        last_id: String::new(),
                    }),
                )
                .await,
            "status",
        );

        assert!(
            requests.recv_timeout(Duration::from_millis(100)).is_err(),
            "oversized inputs must be rejected before Ozon receives a request"
        );
    }

    #[tokio::test]
    async fn too_many_or_out_of_range_inputs_fail_before_calling_ozon() {
        let (server, requests) = mock_server(0);
        let server = server.with_preview_features(true, true);

        assert_validation_error(
            server
                .analytics(
                    RequestIdentity::dev(),
                    Parameters(AnalyticsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        metrics: vec![AnalyticsMetric::Revenue],
                        dimensions: vec![AnalyticsDimension::Sku],
                        limit: 100,
                        offset: MAX_OFFSET + 1,
                        sort_by: None,
                        sort_direction: SortDirection::Asc,
                    }),
                )
                .await,
            "offset",
        );
        assert_validation_error(
            server
                .product_stocks(
                    RequestIdentity::dev(),
                    Parameters(ProductFilterInput {
                        store: Some(StoreId::from("store_a")),
                        offer_ids: vec!["offer".to_owned(); MAX_PRODUCT_FILTER_ITEMS],
                        product_ids: vec!["product".to_owned()],
                        visibility: Visibility::All,
                        limit: 100,
                        cursor: None,
                    }),
                )
                .await,
            "offer_ids",
        );
        assert_validation_error(
            server
                .stock_turnover(
                    RequestIdentity::dev(),
                    Parameters(TurnoverInput {
                        store: Some(StoreId::from("store_a")),
                        skus: vec!["sku".to_owned(); MAX_SKUS + 1],
                        limit: 100,
                        offset: 0,
                    }),
                )
                .await,
            "skus",
        );
        assert_validation_error(
            server
                .stock_turnover(
                    RequestIdentity::dev(),
                    Parameters(TurnoverInput {
                        store: Some(StoreId::from("store_a")),
                        skus: Vec::new(),
                        limit: 100,
                        offset: MAX_OFFSET + 1,
                    }),
                )
                .await,
            "offset",
        );
        assert_validation_error(
            server
                .fbs_postings(
                    RequestIdentity::dev(),
                    Parameters(PostingListInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        status: String::new(),
                        limit: 100,
                        offset: MAX_OFFSET + 1,
                        cursor: None,
                        direction: SortDirection::Asc,
                    }),
                )
                .await,
            "offset",
        );
        assert_validation_error(
            server
                .returns(
                    RequestIdentity::dev(),
                    Parameters(ReturnsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        return_schema: ReturnSchema::Fbo,
                        offer_id: String::new(),
                        posting_numbers: vec!["posting".to_owned(); MAX_POSTING_NUMBERS + 1],
                        limit: 100,
                        last_id: 0,
                    }),
                )
                .await,
            "posting_numbers",
        );
        assert_validation_error(
            server
                .rfbs_returns(
                    RequestIdentity::dev(),
                    Parameters(RfbsReturnsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        offer_id: String::new(),
                        posting_number: String::new(),
                        group_state: vec!["state".to_owned(); MAX_GROUP_STATES + 1],
                        last_id: 0,
                        limit: 100,
                    }),
                )
                .await,
            "group_state",
        );
        assert_validation_error(
            server
                .finance_transactions(
                    RequestIdentity::dev(),
                    Parameters(FinanceInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        posting_number: String::new(),
                        operation_types: vec!["operation".to_owned(); MAX_OPERATION_TYPES + 1],
                        transaction_type: "all".to_owned(),
                        page: 1,
                        page_size: 100,
                    }),
                )
                .await,
            "operation_types",
        );
        assert_validation_error(
            server
                .finance_transactions(
                    RequestIdentity::dev(),
                    Parameters(FinanceInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        posting_number: String::new(),
                        operation_types: Vec::new(),
                        transaction_type: "all".to_owned(),
                        page: MAX_PAGE + 1,
                        page_size: 100,
                    }),
                )
                .await,
            "page",
        );
        assert_validation_error(
            server
                .finance_accrual_postings_preview(
                    RequestIdentity::dev(),
                    Parameters(FinanceAccrualPostingsPreviewInput {
                        store: Some(StoreId::from("store_a")),
                        posting_numbers: vec!["posting".to_owned(); MAX_POSTING_NUMBERS + 1],
                    }),
                )
                .await,
            "posting_numbers",
        );
        assert_validation_error(
            server
                .seller_rating_history(
                    RequestIdentity::dev(),
                    Parameters(RatingHistoryInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        ratings: vec!["rating".to_owned(); MAX_RATINGS + 1],
                        with_premium_scores: true,
                    }),
                )
                .await,
            "ratings",
        );
        assert_validation_error(
            server
                .seller_rating_history(
                    RequestIdentity::dev(),
                    Parameters(RatingHistoryInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        ratings: Vec::new(),
                        with_premium_scores: true,
                    }),
                )
                .await,
            "ratings",
        );
        assert_validation_error(
            server
                .reviews(
                    RequestIdentity::dev(),
                    Parameters(ReviewsInput {
                        store: Some(StoreId::from("store_a")),
                        limit: MIN_REVIEWS_LIMIT - 1,
                        last_id: String::new(),
                        status: "ALL".to_owned(),
                        direction: SortDirection::Asc,
                    }),
                )
                .await,
            "limit",
        );
        assert_validation_error(
            server
                .reviews(
                    RequestIdentity::dev(),
                    Parameters(ReviewsInput {
                        store: Some(StoreId::from("store_a")),
                        limit: MIN_REVIEWS_LIMIT,
                        last_id: String::new(),
                        status: " ".to_owned(),
                        direction: SortDirection::Asc,
                    }),
                )
                .await,
            "status",
        );
        assert_validation_error(
            server
                .questions(
                    RequestIdentity::dev(),
                    Parameters(QuestionsInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-02".to_owned(),
                        status: " ".to_owned(),
                        last_id: String::new(),
                    }),
                )
                .await,
            "status",
        );

        assert!(
            requests.recv_timeout(Duration::from_millis(100)).is_err(),
            "too-many and out-of-range inputs must be rejected before Ozon receives a request"
        );
    }

    #[tokio::test]
    async fn invalid_tool_inputs_fail_before_calling_ozon() {
        let server = server();
        assert!(
            server
                .analytics(
                    RequestIdentity::dev(),
                    Parameters(AnalyticsInput {
                        store: Some(StoreId::from("store_a")),
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
                        store: Some(StoreId::from("store_a")),
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
                        store: Some(StoreId::from("store_a")),
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
        assert!(
            server
                .finance_transactions(
                    RequestIdentity::dev(),
                    Parameters(FinanceInput {
                        store: Some(StoreId::from("store_a")),
                        date_from: "2026-01-01".to_owned(),
                        date_to: "2026-01-31".to_owned(),
                        posting_number: String::new(),
                        operation_types: Vec::new(),
                        transaction_type: "all".to_owned(),
                        page: 1,
                        page_size: 100,
                    })
                )
                .await
                .err()
                .unwrap()
                .contains("30 дней")
        );
    }

    #[tokio::test]
    async fn ozon_errors_are_converted_to_mcp_errors() {
        let result = server()
            .seller_rating(
                RequestIdentity::dev(),
                Parameters(StoreOnlyInput {
                    store: Some(StoreId::from("store_a")),
                }),
            )
            .await;
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(error.starts_with(OZON_TOOL_FAILURE));
        assert!(error.contains("kind=missing_credentials"));
        assert!(error.contains("store=store_a"));
        assert!(error.contains("endpoint=/v1/rating/summary"));
        assert!(error.contains("request_id=-"));
        assert!(error.contains("не настроены Client-Id и Api-Key"));
        assert!(error.contains("не вызывайте автоматически другие инструменты"));
        assert!(error.contains("не заявляйте о прямом доступе к Ozon"));
    }
}
