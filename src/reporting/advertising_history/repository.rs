use std::{str::FromStr, sync::Arc};

use anyhow::{Context, Result, ensure};
use chrono::NaiveDate;
use serde_json::Value;
use tokio_postgres::{Config, config::Host, types::ToSql};

use crate::postgres::SupervisedClient;

use super::{HistoryGroup, validate_request};

const COMPONENT: &str = "mcp-ozon-wb-advertising-history";

/// Functions are the only database capability granted to the MCP requester.
/// Raw responses, campaign manifests, leases and credentials stay in the worker.
#[derive(Clone, Default)]
pub struct HistoryRepository {
    client: Option<Arc<SupervisedClient>>,
}

impl HistoryRepository {
    pub async fn connect_optional(url: Option<&str>) -> Result<Self> {
        let Some(url) = url else {
            return Ok(Self::default());
        };
        let config = Config::from_str(url)
            .map_err(|_| anyhow::anyhow!("invalid history database configuration"))?;
        Self::connect(&config, "report_refresh_requester").await
    }

    pub async fn connect_collector(config: &Config) -> Result<Self> {
        Self::connect(config, "report_collector").await
    }

    async fn connect(config: &Config, role: &str) -> Result<Self> {
        ensure!(
            config.get_user() == Some(role)
                && config.get_options().is_none()
                && config.get_hosts().len() == 1
                && matches!(config.get_hosts(),[Host::Tcp(h)] if !h.trim().is_empty())
                && config.get_password().is_some_and(|p| !p.is_empty())
                && config.get_dbname().is_some_and(|d| !d.is_empty()),
            "invalid history database configuration"
        );
        let mut config = config.clone();
        crate::postgres::harden(&mut config, COMPONENT);
        let client = SupervisedClient::connect(&config, COMPONENT)
            .await
            .map_err(|_| anyhow::anyhow!("history database unavailable"))?;
        let repository = Self {
            client: Some(Arc::new(client)),
        };
        repository.probe().await?;
        Ok(repository)
    }

    pub async fn probe(&self) -> Result<()> {
        let Some(session) = &self.client else {
            return Ok(());
        };
        session
            .verify_session_bounds()
            .await
            .map_err(|_| anyhow::anyhow!("history database unavailable"))?;
        let client = session
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("history database unavailable"))?;
        let valid: bool = client.query_one(
            "SELECT current_user IN ('report_collector','report_refresh_requester') \
             AND NOT current_setting('transaction_read_only')::boolean \
             AND has_schema_privilege(current_user,'daily_reporting','USAGE') \
             AND NOT has_schema_privilege(current_user,'daily_reporting','CREATE') \
             AND has_function_privilege(current_user,'daily_reporting.wb_history_request(text,text,date,date)','EXECUTE') \
             AND has_function_privilege(current_user,'daily_reporting.wb_history_stats(text,date,date,text,integer,integer)','EXECUTE') \
             AND (has_function_privilege(current_user,'daily_reporting.wb_history_claim(text[],text)','EXECUTE') = (current_user='report_collector')) \
             AND NOT has_table_privilege(current_user,'daily_reporting.wb_history_days','INSERT,UPDATE,DELETE') \
             AND NOT has_table_privilege(current_user,'daily_reporting.wb_history_jobs','SELECT,INSERT,UPDATE,DELETE')",
            &[]).await.context("history database contract unavailable")?.get(0);
        drop(client);
        ensure!(valid, "history database contract unavailable");
        Ok(())
    }

    pub async fn request(
        &self,
        account: &str,
        actor: &str,
        from: Option<NaiveDate>,
        to: NaiveDate,
    ) -> Result<Value> {
        validate_request(account, from, to)?;
        ensure!(
            crate::identifiers::is_actor_id(actor),
            "invalid history actor"
        );
        self.json_query(
            "SELECT daily_reporting.wb_history_request($1,$2,$3,$4)::text",
            &[&account, &actor, &from, &to],
        )
        .await
    }

    pub async fn status(&self, account: &str) -> Result<Value> {
        validate_request(account, None, super::last_closed_day())?;
        self.json_query(
            "SELECT daily_reporting.wb_history_status($1)::text",
            &[&account],
        )
        .await
    }

    pub async fn stats(
        &self,
        account: &str,
        from: Option<NaiveDate>,
        to: Option<NaiveDate>,
        group: HistoryGroup,
        limit: u16,
        offset: u32,
    ) -> Result<Value> {
        validate_request(account, from, to.unwrap_or_else(super::last_closed_day))?;
        ensure!(
            (1..=1000).contains(&limit) && offset <= 100_000,
            "invalid history page"
        );
        self.json_query(
            "SELECT daily_reporting.wb_history_stats($1,$2,$3,$4,$5,$6)::text",
            &[
                &account,
                &from,
                &to,
                &group.name(),
                &i32::from(limit),
                &i32::try_from(offset)?,
            ],
        )
        .await
    }

    pub(super) async fn json_query(
        &self,
        sql: &str,
        args: &[&(dyn ToSql + Sync)],
    ) -> Result<Value> {
        let session = self
            .client
            .as_ref()
            .context("advertising history is disabled")?;
        let client = session
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("history database unavailable"))?;
        let text: Option<String> = client
            .query_one(sql, args)
            .await
            .context("history database operation unavailable")?
            .get(0);
        drop(client);
        text.map_or_else(
            || Ok(Value::Null),
            |text| serde_json::from_str(&text).context("invalid history database result"),
        )
    }

    pub(super) async fn execute(&self, sql: &str, args: &[&(dyn ToSql + Sync)]) -> Result<()> {
        let session = self
            .client
            .as_ref()
            .context("advertising history is disabled")?;
        let client = session
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("history database unavailable"))?;
        client
            .execute(sql, args)
            .await
            .context("history database operation unavailable")?;
        drop(client);
        Ok(())
    }
}

impl std::fmt::Debug for HistoryRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryRepository")
            .field("enabled", &self.client.is_some())
            .finish_non_exhaustive()
    }
}
