use std::{collections::BTreeSet, fmt, future::Future, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use chrono::NaiveDate;
use reqwest::{
    Client, Method, Proxy, Response, StatusCode,
    header::{AUTHORIZATION, HeaderValue},
    redirect::Policy,
};
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::Mutex, time::Instant};

use crate::config::PerformanceCredentials;

const PERFORMANCE_BASE_URL: &str = "https://api-performance.ozon.ru";
const TOKEN_PATH: &str = "/api/client/token";
const CREATE_CPC_CAMPAIGN_PATH: &str = "/api/client/campaign/cpc/v2/product";
const MAX_RESPONSE_BYTES: usize = 1_048_576;
const MAX_TOKEN_BYTES: usize = 64 * 1_024;
const MAX_ACCESS_TOKEN_BYTES: usize = 16 * 1_024;
const MAX_PRODUCTS: usize = 50;
const MAX_TITLE_BYTES: usize = 128;
const MAX_TOKEN_LIFETIME: Duration = Duration::from_hours(24);
const TOKEN_REFRESH_SKEW: Duration = Duration::from_secs(60);
const MIN_WRITE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum OzonPlacement {
    #[serde(rename = "PLACEMENT_SEARCH_AND_CATEGORY")]
    SearchAndCategory,
    #[serde(rename = "PLACEMENT_TOP_PROMOTION")]
    TopPromotion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum OzonCampaignStrategy {
    #[serde(rename = "TARGET_BIDS")]
    TargetBids,
    #[serde(rename = "TARGET_CIR")]
    TargetCir,
    #[serde(rename = "TOP_PROMOTION")]
    TopPromotion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OzonCampaignCreateRequest {
    pub title: String,
    pub from_date: String,
    pub to_date: String,
    pub weekly_budget: u64,
    pub placement: OzonPlacement,
    pub product_autopilot_strategy: OzonCampaignStrategy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OzonCampaignProduct {
    pub sku: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bid: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_cir: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_position: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OzonCampaignProductsRequest {
    pub bids: Vec<OzonCampaignProduct>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OzonWriteErrorKind {
    Definite,
    Ambiguous,
}

#[derive(Debug, Error)]
pub enum OzonWriteError {
    #[error("Ozon Performance write request имеет недопустимые данные")]
    InvalidRequest,
    #[error("Ozon Performance OAuth token response некорректен")]
    InvalidToken,
    #[error("Ozon Performance write endpoint отклонил авторизацию")]
    Unauthorized,
    #[error("Ozon Performance write endpoint запретил операцию")]
    Forbidden,
    #[error("Ozon Performance write endpoint вернул HTTP {status}")]
    Http { status: StatusCode },
    #[error("Ozon Performance write request завершился с неопределённым результатом")]
    AmbiguousTransport,
    #[error("Ozon Performance write response превышает безопасный лимит")]
    ResponseTooLarge,
    #[error("Ozon Performance campaign create response некорректен")]
    InvalidCreateResponse,
}

impl OzonWriteError {
    #[must_use]
    pub fn kind(&self) -> OzonWriteErrorKind {
        match self {
            Self::AmbiguousTransport | Self::InvalidCreateResponse => OzonWriteErrorKind::Ambiguous,
            Self::Http { status } if status.is_server_error() => OzonWriteErrorKind::Ambiguous,
            Self::InvalidRequest
            | Self::InvalidToken
            | Self::Unauthorized
            | Self::Forbidden
            | Self::Http { .. }
            | Self::ResponseTooLarge => OzonWriteErrorKind::Definite,
        }
    }
}

#[derive(Debug, Error)]
pub enum OzonGuardedWriteError<E> {
    #[error("Ozon write permit отклонён")]
    Permit(E),
    #[error(transparent)]
    Write(#[from] OzonWriteError),
}

#[derive(Debug)]
struct CachedToken {
    value: String,
    refresh_at: Instant,
}

#[derive(Debug, Default)]
struct TokenState {
    cached: Option<CachedToken>,
}

#[derive(Clone)]
pub struct OzonAdsWriteClient {
    http: Client,
    base_url: String,
    credentials: PerformanceCredentials,
    token: Arc<Mutex<TokenState>>,
    write_lock: Arc<Mutex<Instant>>,
    minimum_interval: Duration,
}

impl fmt::Debug for OzonAdsWriteClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OzonAdsWriteClient")
            .field("base_url", &self.base_url)
            .field("credentials", &"<redacted>")
            .field("minimum_interval", &self.minimum_interval)
            .finish_non_exhaustive()
    }
}

impl OzonAdsWriteClient {
    pub fn new(
        timeout: Duration,
        credentials: PerformanceCredentials,
        proxy_url: &str,
    ) -> Result<Self> {
        if timeout.is_zero() || timeout > Duration::from_secs(30) {
            bail!("CONTROL_MCP_OZON_TIMEOUT_SECONDS должен задавать 1..=30 секунд");
        }
        validate_credentials(&credentials)?;
        let proxy = Proxy::https(proxy_url).context("неверный CONTROL_MCP_OZON_PROXY")?;
        let http = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .https_only(true)
            .connect_timeout(timeout.min(Duration::from_secs(5)))
            .timeout(timeout)
            .proxy(proxy)
            .build()
            .context("не удалось создать изолированный Ozon Performance write client")?;
        Ok(Self::from_parts(
            http,
            PERFORMANCE_BASE_URL,
            credentials,
            MIN_WRITE_INTERVAL,
        ))
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        base_url: &str,
        credentials: PerformanceCredentials,
        timeout: Duration,
    ) -> Self {
        let http = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(timeout)
            .build()
            .expect("test Ozon write client");
        Self::from_parts(http, base_url, credentials, Duration::ZERO)
    }

    #[cfg(test)]
    pub(crate) fn new_for_test_with_interval(
        base_url: &str,
        credentials: PerformanceCredentials,
        timeout: Duration,
        minimum_interval: Duration,
    ) -> Self {
        let http = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(timeout)
            .build()
            .expect("test Ozon write client");
        Self::from_parts(http, base_url, credentials, minimum_interval)
    }

    fn from_parts(
        http: Client,
        base_url: &str,
        credentials: PerformanceCredentials,
        minimum_interval: Duration,
    ) -> Self {
        Self {
            http,
            base_url: base_url.to_owned(),
            credentials,
            token: Arc::new(Mutex::new(TokenState::default())),
            write_lock: Arc::new(Mutex::new(Instant::now())),
            minimum_interval,
        }
    }

    pub async fn create_campaign_with_permit<E, P, PermitFuture>(
        &self,
        request: &OzonCampaignCreateRequest,
        permit: P,
    ) -> Result<u64, OzonGuardedWriteError<E>>
    where
        P: FnOnce() -> PermitFuture,
        PermitFuture: Future<Output = Result<(), E>>,
    {
        validate_create_request(request).map_err(OzonGuardedWriteError::Write)?;
        let body = serde_json::to_vec(request).map_err(|_| OzonWriteError::InvalidRequest)?;
        let response = self
            .post_guarded(CREATE_CPC_CAMPAIGN_PATH, &body, permit)
            .await?;
        let parsed: CampaignIdResponse =
            serde_json::from_slice(&response).map_err(|_| OzonWriteError::InvalidCreateResponse)?;
        let campaign_id = parsed
            .campaign_id
            .parse::<u64>()
            .map_err(|_| OzonWriteError::InvalidCreateResponse)?;
        if campaign_id == 0 || campaign_id.to_string() != parsed.campaign_id {
            return Err(OzonWriteError::InvalidCreateResponse.into());
        }
        Ok(campaign_id)
    }

    pub async fn add_products_with_permit<E, P, PermitFuture>(
        &self,
        campaign_id: u64,
        strategy: OzonCampaignStrategy,
        request: &OzonCampaignProductsRequest,
        permit: P,
    ) -> Result<(), OzonGuardedWriteError<E>>
    where
        P: FnOnce() -> PermitFuture,
        PermitFuture: Future<Output = Result<(), E>>,
    {
        validate_campaign_id(campaign_id).map_err(OzonGuardedWriteError::Write)?;
        validate_products_request(strategy, request).map_err(OzonGuardedWriteError::Write)?;
        let path = format!("/api/client/campaign/{campaign_id}/products");
        let body = serde_json::to_vec(request).map_err(|_| OzonWriteError::InvalidRequest)?;
        self.post_guarded(&path, &body, permit).await?;
        Ok(())
    }

    pub async fn update_products_with_permit<E, P, PermitFuture>(
        &self,
        campaign_id: u64,
        strategy: OzonCampaignStrategy,
        request: &OzonCampaignProductsRequest,
        permit: P,
    ) -> Result<(), OzonGuardedWriteError<E>>
    where
        P: FnOnce() -> PermitFuture,
        PermitFuture: Future<Output = Result<(), E>>,
    {
        validate_campaign_id(campaign_id).map_err(OzonGuardedWriteError::Write)?;
        validate_products_request(strategy, request).map_err(OzonGuardedWriteError::Write)?;
        let path = format!("/api/client/campaign/{campaign_id}/products");
        let body = serde_json::to_vec(request).map_err(|_| OzonWriteError::InvalidRequest)?;
        self.put_guarded(&path, &body, permit).await?;
        Ok(())
    }

    pub async fn activate_campaign_with_permit<E, P, PermitFuture>(
        &self,
        campaign_id: u64,
        permit: P,
    ) -> Result<(), OzonGuardedWriteError<E>>
    where
        P: FnOnce() -> PermitFuture,
        PermitFuture: Future<Output = Result<(), E>>,
    {
        validate_campaign_id(campaign_id).map_err(OzonGuardedWriteError::Write)?;
        let path = format!("/api/client/campaign/{campaign_id}/activate");
        self.post_guarded(&path, b"{}", permit).await?;
        Ok(())
    }

    pub async fn deactivate_campaign_with_permit<E, P, PermitFuture>(
        &self,
        campaign_id: u64,
        permit: P,
    ) -> Result<(), OzonGuardedWriteError<E>>
    where
        P: FnOnce() -> PermitFuture,
        PermitFuture: Future<Output = Result<(), E>>,
    {
        validate_campaign_id(campaign_id).map_err(OzonGuardedWriteError::Write)?;
        let path = format!("/api/client/campaign/{campaign_id}/deactivate");
        self.post_guarded(&path, b"{}", permit).await?;
        Ok(())
    }

    async fn post_guarded<E, P, PermitFuture>(
        &self,
        path: &str,
        body: &[u8],
        permit: P,
    ) -> Result<Vec<u8>, OzonGuardedWriteError<E>>
    where
        P: FnOnce() -> PermitFuture,
        PermitFuture: Future<Output = Result<(), E>>,
    {
        self.write_guarded(Method::POST, path, body, permit).await
    }

    async fn put_guarded<E, P, PermitFuture>(
        &self,
        path: &str,
        body: &[u8],
        permit: P,
    ) -> Result<Vec<u8>, OzonGuardedWriteError<E>>
    where
        P: FnOnce() -> PermitFuture,
        PermitFuture: Future<Output = Result<(), E>>,
    {
        self.write_guarded(Method::PUT, path, body, permit).await
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "write lock deliberately serializes permit validation and the complete write response"
    )]
    async fn write_guarded<E, P, PermitFuture>(
        &self,
        method: Method,
        path: &str,
        body: &[u8],
        permit: P,
    ) -> Result<Vec<u8>, OzonGuardedWriteError<E>>
    where
        P: FnOnce() -> PermitFuture,
        PermitFuture: Future<Output = Result<(), E>>,
    {
        let token = self.access_token().await?;
        let mut next_start = self.write_lock.lock().await;
        if *next_start > Instant::now() {
            tokio::time::sleep_until(*next_start).await;
        }
        permit().await.map_err(OzonGuardedWriteError::Permit)?;
        *next_start = Instant::now() + self.minimum_interval;

        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| OzonWriteError::InvalidToken)?;
        authorization.set_sensitive(true);
        let response = self
            .http
            .request(method, format!("{}{path}", self.base_url))
            .header(AUTHORIZATION, authorization)
            .header("content-type", "application/json")
            .body(body.to_vec())
            .send()
            .await
            .map_err(|_| OzonWriteError::AmbiguousTransport)?;
        decode_write_response(response).await.map_err(Into::into)
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "token mutex deliberately provides a single-flight OAuth exchange"
    )]
    async fn access_token(&self) -> Result<String, OzonWriteError> {
        let mut state = self.token.lock().await;
        if let Some(cached) = state.cached.as_ref()
            && cached.refresh_at > Instant::now()
        {
            return Ok(cached.value.clone());
        }
        let response = self
            .http
            .post(format!("{}{}", self.base_url, TOKEN_PATH))
            .json(&serde_json::json!({
                "client_id": self.credentials.client_id,
                "client_secret": self.credentials.client_secret,
                "grant_type": "client_credentials",
            }))
            .send()
            .await
            .map_err(|_| OzonWriteError::AmbiguousTransport)?;
        let status = response.status();
        if !status.is_success() {
            return Err(classify_status(status));
        }
        let bytes = read_bounded(response, MAX_TOKEN_BYTES).await?;
        let token: TokenResponse =
            serde_json::from_slice(&bytes).map_err(|_| OzonWriteError::InvalidToken)?;
        if token.access_token.is_empty()
            || token.access_token.len() > MAX_ACCESS_TOKEN_BYTES
            || !token.token_type.eq_ignore_ascii_case("bearer")
            || token.expires_in == 0
            || Duration::from_secs(token.expires_in) > MAX_TOKEN_LIFETIME
        {
            return Err(OzonWriteError::InvalidToken);
        }
        let refresh_at = Instant::now()
            .checked_add(Duration::from_secs(token.expires_in).saturating_sub(TOKEN_REFRESH_SKEW))
            .ok_or(OzonWriteError::InvalidToken)?;
        state.cached = Some(CachedToken {
            value: token.access_token.clone(),
            refresh_at,
        });
        Ok(token.access_token)
    }
}

fn validate_credentials(credentials: &PerformanceCredentials) -> Result<()> {
    for (name, value) in [
        ("client_id", credentials.client_id.as_str()),
        ("client_secret", credentials.client_secret.as_str()),
    ] {
        if value.is_empty()
            || value.len() > MAX_ACCESS_TOKEN_BYTES
            || !value.is_ascii()
            || value
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            bail!("Ozon Performance write {name} имеет недопустимый формат");
        }
    }
    Ok(())
}

pub(super) fn validate_create_request(
    request: &OzonCampaignCreateRequest,
) -> Result<(), OzonWriteError> {
    if request.title.is_empty()
        || request.title.len() > MAX_TITLE_BYTES
        || request.title.trim() != request.title
        || request.title.bytes().any(|byte| byte.is_ascii_control())
        || request.weekly_budget == 0
    {
        return Err(OzonWriteError::InvalidRequest);
    }
    let from = NaiveDate::parse_from_str(&request.from_date, "%Y-%m-%d")
        .map_err(|_| OzonWriteError::InvalidRequest)?;
    let to = NaiveDate::parse_from_str(&request.to_date, "%Y-%m-%d")
        .map_err(|_| OzonWriteError::InvalidRequest)?;
    if to < from || (to - from).num_days() > 31 {
        return Err(OzonWriteError::InvalidRequest);
    }
    match (request.placement, request.product_autopilot_strategy) {
        (
            OzonPlacement::SearchAndCategory,
            OzonCampaignStrategy::TargetBids | OzonCampaignStrategy::TargetCir,
        )
        | (OzonPlacement::TopPromotion, OzonCampaignStrategy::TopPromotion) => Ok(()),
        _ => Err(OzonWriteError::InvalidRequest),
    }
}

pub(super) fn validate_products_request(
    strategy: OzonCampaignStrategy,
    request: &OzonCampaignProductsRequest,
) -> Result<(), OzonWriteError> {
    if request.bids.is_empty() || request.bids.len() > MAX_PRODUCTS {
        return Err(OzonWriteError::InvalidRequest);
    }
    let mut skus = BTreeSet::new();
    for product in &request.bids {
        if product.sku == 0 || !skus.insert(product.sku) {
            return Err(OzonWriteError::InvalidRequest);
        }
        let valid = match strategy {
            OzonCampaignStrategy::TargetBids => {
                product.bid.is_some_and(|value| value > 0)
                    && product.target_cir.is_none()
                    && product.top_position.is_none()
            }
            OzonCampaignStrategy::TargetCir => {
                product.bid.is_none()
                    && product.top_position.is_none()
                    && product
                        .target_cir
                        .is_some_and(|value| (10..=100).contains(&value))
            }
            OzonCampaignStrategy::TopPromotion => {
                product.bid.is_none()
                    && product.target_cir.is_none()
                    && matches!(product.top_position, Some(4 | 12 | 20 | 30))
            }
        };
        if !valid {
            return Err(OzonWriteError::InvalidRequest);
        }
    }
    Ok(())
}

const fn validate_campaign_id(campaign_id: u64) -> Result<(), OzonWriteError> {
    if campaign_id == 0 {
        Err(OzonWriteError::InvalidRequest)
    } else {
        Ok(())
    }
}

async fn decode_write_response(response: Response) -> Result<Vec<u8>, OzonWriteError> {
    let status = response.status();
    if !status.is_success() {
        return Err(classify_status(status));
    }
    read_bounded(response, MAX_RESPONSE_BYTES).await
}

async fn read_bounded(mut response: Response, limit: usize) -> Result<Vec<u8>, OzonWriteError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(OzonWriteError::ResponseTooLarge);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| OzonWriteError::AmbiguousTransport)?
    {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(OzonWriteError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn classify_status(status: StatusCode) -> OzonWriteError {
    match status {
        StatusCode::UNAUTHORIZED => OzonWriteError::Unauthorized,
        StatusCode::FORBIDDEN => OzonWriteError::Forbidden,
        _ => OzonWriteError::Http { status },
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CampaignIdResponse {
    campaign_id: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
}
