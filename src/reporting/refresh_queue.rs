//! Narrow PostgreSQL boundary for manager-requested Ozon sales refreshes.
//!
//! The MCP process can execute only the request/status SECURITY DEFINER
//! functions. It cannot read or mutate the queue table, claim work, access
//! snapshots, or resolve marketplace credentials. The separate report
//! collector owns those capabilities and processes requests sequentially.

use std::{fmt, future::Future, pin::Pin, str::FromStr, sync::Arc};

use anyhow::{Result as AnyResult, anyhow};
use chrono::{DateTime, NaiveDate, Utc};
use schemars::JsonSchema;
use serde::Serialize;
use tokio_postgres::{Config, Row, config::Host};

use crate::postgres::SupervisedClient;

use super::snapshot::Marketplace;

const REQUESTER_COMPONENT: &str = "mcp-ozon-report-refresh-requester";

pub type RefreshRequestFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, RefreshRequestError>> + Send + 'a>>;

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum RefreshRequestError {
    #[error("sales refresh queue is disabled")]
    Disabled,
    #[error("sales refresh request is invalid")]
    InvalidRequest,
    #[error("sales refresh queue is temporarily unavailable")]
    Unavailable,
    #[error("sales refresh queue returned invalid data")]
    InvalidData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SalesRefreshState {
    NeverRequested,
    Queued,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct SalesRefreshStatus {
    pub account_id: String,
    pub marketplace: Marketplace,
    pub request_id: Option<u64>,
    pub state: SalesRefreshState,
    pub business_date: Option<String>,
    pub requested_at: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub snapshot_cutoff_at: Option<String>,
    pub created: Option<bool>,
}

pub trait RefreshRequestRepository: Send + Sync {
    fn enabled(&self) -> bool;

    fn probe(&self) -> RefreshRequestFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn request<'a>(
        &'a self,
        account_id: &'a str,
        marketplace: Marketplace,
        actor_id: &'a str,
        business_date: NaiveDate,
    ) -> RefreshRequestFuture<'a, SalesRefreshStatus>;

    fn status<'a>(
        &'a self,
        account_id: &'a str,
        marketplace: Marketplace,
    ) -> RefreshRequestFuture<'a, SalesRefreshStatus>;
}

#[derive(Clone)]
pub struct RefreshRequestService {
    repository: Arc<dyn RefreshRequestRepository>,
}

impl fmt::Debug for RefreshRequestService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RefreshRequestService")
            .field("enabled", &self.repository.enabled())
            .finish_non_exhaustive()
    }
}

impl RefreshRequestService {
    #[must_use]
    pub fn disabled() -> Self {
        Self::from_repository(Arc::new(DisabledRefreshRequestRepository))
    }

    pub async fn connect_optional(database_url: Option<&str>) -> AnyResult<Self> {
        let Some(database_url) = database_url else {
            return Ok(Self::disabled());
        };
        let mut config = Config::from_str(database_url)
            .map_err(|_| anyhow!("sales refresh database configuration is invalid"))?;
        validate_database_config(&config)
            .map_err(|_| anyhow!("sales refresh database configuration is invalid"))?;
        crate::postgres::harden(&mut config, REQUESTER_COMPONENT);
        let repository = PostgresRefreshRequestRepository::connect(&config)
            .await
            .map_err(|_| anyhow!("sales refresh database contract is unavailable"))?;
        Ok(Self::from_repository(Arc::new(repository)))
    }

    pub fn from_repository(repository: Arc<dyn RefreshRequestRepository>) -> Self {
        Self { repository }
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.repository.enabled()
    }

    pub async fn probe(&self) -> Result<(), RefreshRequestError> {
        self.repository.probe().await
    }

    pub async fn request(
        &self,
        account_id: &str,
        marketplace: Marketplace,
        actor_id: &str,
        business_date: NaiveDate,
    ) -> Result<SalesRefreshStatus, RefreshRequestError> {
        validate_identifier(account_id, 128)?;
        validate_actor(actor_id)?;
        self.repository
            .request(account_id, marketplace, actor_id, business_date)
            .await
    }

    pub async fn status(
        &self,
        account_id: &str,
        marketplace: Marketplace,
    ) -> Result<SalesRefreshStatus, RefreshRequestError> {
        validate_identifier(account_id, 128)?;
        self.repository.status(account_id, marketplace).await
    }
}

struct DisabledRefreshRequestRepository;

impl RefreshRequestRepository for DisabledRefreshRequestRepository {
    fn enabled(&self) -> bool {
        false
    }

    fn request<'a>(
        &'a self,
        _account_id: &'a str,
        _marketplace: Marketplace,
        _actor_id: &'a str,
        _business_date: NaiveDate,
    ) -> RefreshRequestFuture<'a, SalesRefreshStatus> {
        disabled()
    }

    fn status<'a>(
        &'a self,
        _account_id: &'a str,
        _marketplace: Marketplace,
    ) -> RefreshRequestFuture<'a, SalesRefreshStatus> {
        disabled()
    }
}

fn disabled<'a, T>() -> RefreshRequestFuture<'a, T> {
    Box::pin(async { Err(RefreshRequestError::Disabled) })
}

struct PostgresRefreshRequestRepository {
    client: SupervisedClient,
}

impl PostgresRefreshRequestRepository {
    async fn connect(config: &Config) -> Result<Self, RefreshRequestError> {
        let repository = Self {
            client: SupervisedClient::connect(config, REQUESTER_COMPONENT)
                .await
                .map_err(|_| RefreshRequestError::Unavailable)?,
        };
        repository.verify_runtime_contract().await?;
        Ok(repository)
    }

    async fn verify_runtime_contract(&self) -> Result<(), RefreshRequestError> {
        self.client
            .verify_session_bounds()
            .await
            .map_err(|_| RefreshRequestError::Unavailable)?;
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| RefreshRequestError::Unavailable)?;
        let valid: bool = client
            .query_one(
                "SELECT current_user = 'report_refresh_requester' \
                    AND NOT current_setting('transaction_read_only')::boolean \
                    AND has_schema_privilege(current_user, 'daily_reporting', 'USAGE') \
                    AND NOT has_schema_privilege(current_user, 'daily_reporting', 'CREATE') \
                    AND has_function_privilege(current_user, \
                        'daily_reporting.request_marketplace_sales_refresh(text,text,text,date)', 'EXECUTE') \
                    AND has_function_privilege(current_user, \
                        'daily_reporting.marketplace_sales_refresh_status(text,text)', 'EXECUTE') \
                    AND NOT has_table_privilege(current_user, \
                        'daily_reporting.ozon_sales_refresh_requests', \
                        'SELECT,INSERT,UPDATE,DELETE') \
                    AND NOT has_table_privilege(current_user, \
                        'daily_reporting.source_snapshots', 'SELECT,INSERT,UPDATE,DELETE')",
                &[],
            )
            .await
            .map_err(|_| RefreshRequestError::Unavailable)?
            .get(0);
        drop(client);
        valid.then_some(()).ok_or(RefreshRequestError::Unavailable)
    }

    async fn request_impl(
        &self,
        account_id: &str,
        marketplace: Marketplace,
        actor_id: &str,
        business_date: NaiveDate,
    ) -> Result<SalesRefreshStatus, RefreshRequestError> {
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| RefreshRequestError::Unavailable)?;
        let row = client
            .query_one(
                "SELECT request_id, request_status, marketplace, business_date, requested_at, \
                        started_at, finished_at, snapshot_cutoff_at, created \
                 FROM daily_reporting.request_marketplace_sales_refresh($1, $2, $3, $4)",
                &[
                    &account_id,
                    &marketplace_name(marketplace),
                    &actor_id,
                    &business_date,
                ],
            )
            .await
            .map_err(|_| RefreshRequestError::Unavailable)?;
        drop(client);
        refresh_status(account_id, marketplace, &row, true)
    }

    async fn status_impl(
        &self,
        account_id: &str,
        marketplace: Marketplace,
    ) -> Result<SalesRefreshStatus, RefreshRequestError> {
        let client = self
            .client
            .acquire()
            .await
            .map_err(|_| RefreshRequestError::Unavailable)?;
        let row = client
            .query_opt(
                "SELECT request_id, request_status, marketplace, business_date, requested_at, \
                        started_at, finished_at, snapshot_cutoff_at \
                 FROM daily_reporting.marketplace_sales_refresh_status($1, $2)",
                &[&account_id, &marketplace_name(marketplace)],
            )
            .await
            .map_err(|_| RefreshRequestError::Unavailable)?;
        drop(client);
        row.as_ref().map_or_else(
            || {
                Ok(SalesRefreshStatus {
                    account_id: account_id.to_owned(),
                    marketplace,
                    request_id: None,
                    state: SalesRefreshState::NeverRequested,
                    business_date: None,
                    requested_at: None,
                    started_at: None,
                    finished_at: None,
                    snapshot_cutoff_at: None,
                    created: None,
                })
            },
            |row| refresh_status(account_id, marketplace, row, false),
        )
    }
}

impl RefreshRequestRepository for PostgresRefreshRequestRepository {
    fn enabled(&self) -> bool {
        true
    }

    fn probe(&self) -> RefreshRequestFuture<'_, ()> {
        Box::pin(async move { self.verify_runtime_contract().await })
    }

    fn request<'a>(
        &'a self,
        account_id: &'a str,
        marketplace: Marketplace,
        actor_id: &'a str,
        business_date: NaiveDate,
    ) -> RefreshRequestFuture<'a, SalesRefreshStatus> {
        Box::pin(async move {
            self.request_impl(account_id, marketplace, actor_id, business_date)
                .await
        })
    }

    fn status<'a>(
        &'a self,
        account_id: &'a str,
        marketplace: Marketplace,
    ) -> RefreshRequestFuture<'a, SalesRefreshStatus> {
        Box::pin(async move { self.status_impl(account_id, marketplace).await })
    }
}

fn refresh_status(
    account_id: &str,
    expected_marketplace: Marketplace,
    row: &Row,
    with_created: bool,
) -> Result<SalesRefreshStatus, RefreshRequestError> {
    let request_id: i64 = row
        .try_get(0)
        .map_err(|_| RefreshRequestError::InvalidData)?;
    let state: String = row
        .try_get(1)
        .map_err(|_| RefreshRequestError::InvalidData)?;
    let marketplace: String = row
        .try_get(2)
        .map_err(|_| RefreshRequestError::InvalidData)?;
    let marketplace = parse_marketplace(&marketplace)?;
    if marketplace != expected_marketplace {
        return Err(RefreshRequestError::InvalidData);
    }
    let business_date: NaiveDate = row
        .try_get(3)
        .map_err(|_| RefreshRequestError::InvalidData)?;
    let requested_at: DateTime<Utc> = row
        .try_get(4)
        .map_err(|_| RefreshRequestError::InvalidData)?;
    let started_at: Option<DateTime<Utc>> = row
        .try_get(5)
        .map_err(|_| RefreshRequestError::InvalidData)?;
    let finished_at: Option<DateTime<Utc>> = row
        .try_get(6)
        .map_err(|_| RefreshRequestError::InvalidData)?;
    let snapshot_cutoff_at: Option<DateTime<Utc>> = row
        .try_get(7)
        .map_err(|_| RefreshRequestError::InvalidData)?;
    let created = with_created
        .then(|| row.try_get(8).map_err(|_| RefreshRequestError::InvalidData))
        .transpose()?;
    Ok(SalesRefreshStatus {
        account_id: account_id.to_owned(),
        marketplace,
        request_id: Some(u64::try_from(request_id).map_err(|_| RefreshRequestError::InvalidData)?),
        state: parse_state(&state)?,
        business_date: Some(business_date.to_string()),
        requested_at: Some(requested_at.to_rfc3339()),
        started_at: started_at.map(|value| value.to_rfc3339()),
        finished_at: finished_at.map(|value| value.to_rfc3339()),
        snapshot_cutoff_at: snapshot_cutoff_at.map(|value| value.to_rfc3339()),
        created,
    })
}

const fn marketplace_name(marketplace: Marketplace) -> &'static str {
    match marketplace {
        Marketplace::Ozon => "ozon",
        Marketplace::Wildberries => "wildberries",
    }
}

fn parse_marketplace(value: &str) -> Result<Marketplace, RefreshRequestError> {
    match value {
        "ozon" => Ok(Marketplace::Ozon),
        "wildberries" => Ok(Marketplace::Wildberries),
        _ => Err(RefreshRequestError::InvalidData),
    }
}

fn parse_state(value: &str) -> Result<SalesRefreshState, RefreshRequestError> {
    match value {
        "queued" => Ok(SalesRefreshState::Queued),
        "running" => Ok(SalesRefreshState::Running),
        "succeeded" => Ok(SalesRefreshState::Succeeded),
        "failed" => Ok(SalesRefreshState::Failed),
        _ => Err(RefreshRequestError::InvalidData),
    }
}

fn validate_identifier(value: &str, maximum: usize) -> Result<(), RefreshRequestError> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        Err(RefreshRequestError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn validate_actor(value: &str) -> Result<(), RefreshRequestError> {
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'@' | b'-')
        })
    {
        Err(RefreshRequestError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn validate_database_config(config: &Config) -> Result<(), RefreshRequestError> {
    if config.get_user() == Some("report_refresh_requester")
        && config
            .get_password()
            .is_some_and(|password| !password.is_empty())
        && config.get_dbname().is_some_and(|value| !value.is_empty())
        && config.get_hosts().len() == 1
        && matches!(config.get_hosts(), [Host::Tcp(host)] if !host.trim().is_empty())
        && config.get_options().is_none()
    {
        Ok(())
    } else {
        Err(RefreshRequestError::InvalidRequest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DefaultProbeRepository;

    impl RefreshRequestRepository for DefaultProbeRepository {
        fn enabled(&self) -> bool {
            true
        }

        fn request<'a>(
            &'a self,
            _account_id: &'a str,
            _marketplace: Marketplace,
            _actor_id: &'a str,
            _business_date: NaiveDate,
        ) -> RefreshRequestFuture<'a, SalesRefreshStatus> {
            Box::pin(async { Err(RefreshRequestError::InvalidData) })
        }

        fn status<'a>(
            &'a self,
            _account_id: &'a str,
            _marketplace: Marketplace,
        ) -> RefreshRequestFuture<'a, SalesRefreshStatus> {
            Box::pin(async { Err(RefreshRequestError::InvalidData) })
        }
    }

    #[tokio::test]
    async fn disabled_mode_is_explicit_and_fail_closed() {
        let service = RefreshRequestService::connect_optional(None).await.unwrap();
        assert!(!service.is_enabled());
        let date = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        assert_eq!(
            service
                .request("account_a", Marketplace::Ozon, "manager", date)
                .await,
            Err(RefreshRequestError::Disabled)
        );
        assert_eq!(
            service.status("account_a", Marketplace::Ozon).await,
            Err(RefreshRequestError::Disabled)
        );
        assert_eq!(
            format!("{service:?}"),
            "RefreshRequestService { enabled: false, .. }"
        );
    }

    #[tokio::test]
    async fn service_uses_the_default_probe_and_validates_before_dispatch() {
        let service = RefreshRequestService::from_repository(Arc::new(DefaultProbeRepository));
        let date = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        assert!(service.is_enabled());
        assert_eq!(
            format!("{service:?}"),
            "RefreshRequestService { enabled: true, .. }"
        );
        assert_eq!(service.probe().await, Ok(()));
        assert_eq!(
            service
                .request("bad/account", Marketplace::Ozon, "manager", date)
                .await,
            Err(RefreshRequestError::InvalidRequest)
        );
        assert_eq!(
            service
                .request("account_a", Marketplace::Ozon, "bad actor", date)
                .await,
            Err(RefreshRequestError::InvalidRequest)
        );
        assert_eq!(
            service.status("", Marketplace::Ozon).await,
            Err(RefreshRequestError::InvalidRequest)
        );
        assert_eq!(
            service
                .request("account_a", Marketplace::Ozon, "manager", date)
                .await,
            Err(RefreshRequestError::InvalidData)
        );
        assert_eq!(
            service.status("account_a", Marketplace::Ozon).await,
            Err(RefreshRequestError::InvalidData)
        );
    }

    #[tokio::test]
    async fn restricted_postgres_repository_exposes_status_request_and_probe() {
        verify_restricted_postgres_repository(None).await;
        verify_restricted_postgres_repository(
            std::env::var("REPORT_REFRESH_TEST_REQUESTER_URL").ok(),
        )
        .await;
    }

    async fn verify_restricted_postgres_repository(database_url: Option<String>) {
        let Some(database_url) = database_url else {
            return;
        };
        let service = RefreshRequestService::connect_optional(Some(database_url.as_str()))
            .await
            .unwrap();
        assert!(service.is_enabled());
        assert_eq!(service.probe().await, Ok(()));
        let account_id = format!(
            "unit_{}_{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        );
        assert_eq!(
            service
                .status(&account_id, Marketplace::Ozon)
                .await
                .unwrap()
                .state,
            SalesRefreshState::NeverRequested
        );
        let date = crate::reporting::business_date(Utc::now());
        let requested = service
            .request(&account_id, Marketplace::Ozon, "unit_test_manager", date)
            .await
            .unwrap();
        assert_eq!(requested.state, SalesRefreshState::Queued);
        assert_eq!(requested.created, Some(true));
        assert_eq!(
            service
                .status(&account_id, Marketplace::Ozon)
                .await
                .unwrap()
                .request_id,
            requested.request_id
        );

        let collector_url = std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL").unwrap();
        let writer = crate::reporting::postgres_collector::PostgresSnapshotWriter::connect(
            &Config::from_str(&collector_url).unwrap(),
        )
        .await
        .unwrap();
        let claim = writer
            .claim_sales_refresh("refresh-queue-unit-test")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.account_id(), account_id);
        assert!(
            writer
                .fail_sales_refresh(&claim, "unit_test_complete")
                .await
                .unwrap()
        );
        assert_eq!(
            service
                .status(&account_id, Marketplace::Ozon)
                .await
                .unwrap()
                .state,
            SalesRefreshState::Failed
        );
    }

    #[test]
    fn identifiers_states_and_database_role_are_bounded() {
        assert!(validate_identifier("account_a", 128).is_ok());
        assert!(validate_identifier("bad/account", 128).is_err());
        assert!(validate_actor("manager@example.test").is_ok());
        assert!(validate_actor("bad actor").is_err());
        assert_eq!(parse_state("queued"), Ok(SalesRefreshState::Queued));
        assert_eq!(parse_state("running"), Ok(SalesRefreshState::Running));
        assert_eq!(parse_state("succeeded"), Ok(SalesRefreshState::Succeeded));
        assert_eq!(parse_state("failed"), Ok(SalesRefreshState::Failed));
        assert_eq!(
            parse_state("unknown"),
            Err(RefreshRequestError::InvalidData)
        );

        let valid =
            Config::from_str("postgresql://report_refresh_requester:secret@db/ozon_positions")
                .unwrap();
        assert!(validate_database_config(&valid).is_ok());
        let invalid =
            Config::from_str("postgresql://position_reader:secret@db/ozon_positions").unwrap();
        assert!(validate_database_config(&invalid).is_err());
    }

    #[tokio::test]
    async fn configured_mode_rejects_malformed_or_wrong_role_urls_before_network() {
        assert!(
            RefreshRequestService::connect_optional(Some("not a database URL"))
                .await
                .is_err()
        );
        assert!(
            RefreshRequestService::connect_optional(Some(
                "postgresql://position_reader:secret@db/ozon_positions"
            ))
            .await
            .is_err()
        );
        assert!(
            RefreshRequestService::connect_optional(Some(
                "postgresql://report_refresh_requester:secret@127.0.0.1:1/ozon_positions?connect_timeout=1"
            ))
            .await
            .is_err()
        );
    }
}
