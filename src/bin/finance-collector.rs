#![forbid(unsafe_code)]

//! Explicit local operator only; no schedule is enabled by installing this
//! binary. Marketplace keys stay in a deployment-owned private directory.

#[path = "finance-collector/access.rs"]
mod access;
#[path = "finance-collector/checkpoint.rs"]
mod checkpoint;

use std::{collections::BTreeMap, future::Future, path::PathBuf, pin::Pin, sync::Arc};

use anyhow::{Context, Result, ensure};
use chrono::{NaiveDate, Utc};
use mcp_ozon::{
    reporting::{
        checkpoint::{CheckpointError, Checkpoints},
        finance_ledger::{FinanceLedgerError, PostgresFinanceLedger, WbFinanceBatch},
        wb_finance_source::{WbClientFinanceTransport, WbFinanceTransport},
        wb_source::WbReportSourceError,
    },
    runtime::print_runtime_version_if_requested,
    wb::{WbError, WbErrorKind},
};
use serde_json::{Value, json};

use checkpoint::LocalJournal;

#[derive(Clone, Copy)]
enum Egress {
    Direct,
    CollectorProxy,
}

#[derive(Clone, Copy)]
enum Command {
    ProbeWb,
    CollectWb,
}

struct Arguments {
    command: Command,
    registry: PathBuf,
    actor: String,
    account: String,
    credentials_dir: PathBuf,
    state_dir: PathBuf,
    from: NaiveDate,
    to: NaiveDate,
    egress: Egress,
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if print_runtime_version_if_requested("finance-collector", &arguments)? {
        return Ok(());
    }
    if arguments == ["--help"] {
        println!(
            "finance-collector probe-wb|collect-wb --registry PATH --actor ID --account ID --credentials-dir PATH --state-dir PATH --from YYYY-MM-DD --to YYYY-MM-DD [--egress collector-proxy|direct]"
        );
        println!(
            "collect-wb reads REPORT_COLLECTOR_DATABASE_URL. Reuse one deployment-owned private state directory for all invocations. Default egress is collector-proxy. A probe reserves the same 12-hour seller quota as collection."
        );
        return Ok(());
    }
    let arguments = parse_arguments(&arguments)?;
    let result = execute(&arguments).await?;
    println!(
        "{}",
        serde_json::to_string(&result).context("operator result cannot be encoded")?
    );
    Ok(())
}

fn parse_arguments(raw: &[String]) -> Result<Arguments> {
    let command = match raw.first().map(String::as_str) {
        Some("probe-wb") => Command::ProbeWb,
        Some("collect-wb") => Command::CollectWb,
        _ => anyhow::bail!("an explicit probe-wb or collect-wb command is required; see --help"),
    };
    ensure!(
        raw.len() % 2 == 1,
        "every option requires exactly one value"
    );
    let mut values = BTreeMap::new();
    for pair in raw[1..].as_chunks::<2>().0 {
        ensure!(
            matches!(
                pair[0].as_str(),
                "--registry"
                    | "--actor"
                    | "--account"
                    | "--credentials-dir"
                    | "--state-dir"
                    | "--from"
                    | "--to"
                    | "--egress"
            ),
            "unknown finance collector option"
        );
        ensure!(
            !pair[1].is_empty() && values.insert(pair[0].as_str(), pair[1].as_str()).is_none(),
            "empty or duplicate finance collector option"
        );
    }
    let required = |name| {
        values
            .get(name)
            .copied()
            .context("a required finance collector option is missing")
    };
    let from = date(required("--from")?)?;
    let to = date(required("--to")?)?;
    ensure!(
        from >= NaiveDate::from_ymd_opt(2024, 1, 29).expect("constant date")
            && to >= from
            && (to - from).num_days() < 31
            && to < Utc::now().date_naive(),
        "financial collection requires a closed date interval of at most 31 days since 2024-01-29"
    );
    let egress = match values.get("--egress").copied().unwrap_or("collector-proxy") {
        "collector-proxy" => Egress::CollectorProxy,
        "direct" => Egress::Direct,
        _ => anyhow::bail!("egress must be collector-proxy or direct"),
    };
    Ok(Arguments {
        command,
        registry: required("--registry")?.into(),
        actor: required("--actor")?.to_owned(),
        account: required("--account")?.to_owned(),
        credentials_dir: required("--credentials-dir")?.into(),
        state_dir: required("--state-dir")?.into(),
        from,
        to,
        egress,
    })
}

fn date(raw: &str) -> Result<NaiveDate> {
    let value = NaiveDate::parse_from_str(raw, "%Y-%m-%d").context("date must use YYYY-MM-DD")?;
    ensure!(
        raw.len() == 10 && value.to_string() == raw,
        "date must use YYYY-MM-DD"
    );
    Ok(value)
}

async fn execute(arguments: &Arguments) -> Result<Value> {
    let scoped = access::resolve(arguments)?;
    let identity = format!(
        "wb-detailed-v1:{}:{}:{}",
        arguments.account, arguments.from, arguments.to
    );
    let journal = Arc::new(LocalJournal::open(
        &arguments.state_dir,
        &scoped.identity.seller_scope,
        &identity,
    )?);
    match arguments.command {
        Command::ProbeWb => {
            match journal.reserve(Utc::now()) {
                Ok(()) => {}
                Err(CheckpointError::Deferred) => return deferred(arguments, &journal),
                Err(_) => anyhow::bail!("financial quota reservation failed; no request was sent"),
            }
            let fresh = access::resolve(arguments)?;
            ensure!(
                fresh.identity == scoped.identity,
                "financial credential binding changed before request"
            );
            let result = fresh
                .client
                .financial_report_page(&arguments.account, arguments.from, arguments.to, 1, 0)
                .await;
            match result {
                Ok(response) => Ok(json!({
                    "account_id": arguments.account, "actor_id": arguments.actor,
                    "status": "method_read_succeeded", "http_status": if response.is_some() { 200 } else { 204 },
                    "endpoint": "POST https://finance-api.wildberries.ru/api/finance/v1/sales-reports/detailed",
                    "checked_at": Utc::now(),
                    "local_claims_only": { "personal": true, "read_only": true, "finance_category": true },
                    "pagination_complete": response.is_none(),
                    "next_request_at": journal.next_allowed_at()?,
                    "financial_rows_emitted": 0,
                })),
                Err(error) => {
                    match &error {
                        WbError::RateLimited {
                            retry_after: Some(delay),
                            ..
                        }
                        | WbError::LocalRateLimited { retry_after: delay } => {
                            journal
                                .postpone(mcp_ozon::reporting::checkpoint::delay_seconds(*delay))?;
                        }
                        _ => {}
                    }
                    Ok(
                        json!({"account_id": arguments.account, "actor_id": arguments.actor,
                        "status": "method_read_failed", "error_class": error.kind().code(),
                        "http_status": error_http_status(&error), "checked_at": Utc::now(),
                        "endpoint": "POST https://finance-api.wildberries.ru/api/finance/v1/sales-reports/detailed",
                        "next_request_at": journal.next_allowed_at()?, "financial_rows_emitted": 0}),
                    )
                }
            }
        }
        Command::CollectWb => {
            let database_url = std::env::var("REPORT_COLLECTOR_DATABASE_URL")
                .context("REPORT_COLLECTOR_DATABASE_URL is required only for collect-wb")?;
            let config = database_url
                .parse::<tokio_postgres::Config>()
                .map_err(|_| {
                    anyhow::anyhow!("financial collector database configuration is invalid")
                })?;
            ensure!(
                config.get_user() == Some("report_collector")
                    && config.get_password().is_some()
                    && config.get_options().is_none(),
                "financial collector database must use the restricted report_collector identity"
            );
            let writer = PostgresFinanceLedger::connect(&config).await?;
            let transport = AuthorizedTransport {
                arguments,
                identity: &scoped.identity,
            };
            let checkpoints: Checkpoints = Some(journal.clone());
            let batch = match WbFinanceBatch::collect(
                &transport,
                arguments.account.clone(),
                arguments.from,
                arguments.to,
                &checkpoints,
            )
            .await
            {
                Ok(batch) => batch,
                Err(FinanceLedgerError::Source(WbReportSourceError::Checkpoint(
                    CheckpointError::Deferred,
                ))) => {
                    return deferred(arguments, &journal);
                }
                Err(FinanceLedgerError::Source(WbReportSourceError::RetryAfter { seconds })) => {
                    journal.postpone(seconds)?;
                    return deferred(arguments, &journal);
                }
                Err(error) => return Err(error.into()),
            };
            let fresh = access::resolve(arguments)?;
            ensure!(
                fresh.identity == scoped.identity,
                "financial credential binding changed before publication"
            );
            let publication = writer.publish_wb(&batch).await?;
            Ok(
                json!({"account_id": arguments.account, "actor_id": arguments.actor,
                "status": "published", "batch_id": publication.batch_id,
                "already_present": publication.already_present, "row_count": batch.rows().len(),
                "date_from": batch.date_from(), "date_to": batch.date_to(),
                "pagination_complete": true, "terminal_http_status": 204,
                "financial_reconciliation": "unavailable", "profit": null,
                "next_request_at": journal.next_allowed_at()?}),
            )
        }
    }
}

fn deferred(arguments: &Arguments, journal: &LocalJournal) -> Result<Value> {
    Ok(
        json!({"account_id": arguments.account, "actor_id": arguments.actor,
        "status": "deferred", "next_request_at": journal.next_allowed_at()?,
        "pagination_complete": false, "published": false}),
    )
}

struct AuthorizedTransport<'a> {
    arguments: &'a Arguments,
    identity: &'a access::CredentialIdentity,
}

impl WbFinanceTransport for AuthorizedTransport<'_> {
    fn finance_page<'a>(
        &'a self,
        start: NaiveDate,
        end: NaiveDate,
        limit: u32,
        rrd_id: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, WbReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            let fresh = access::resolve(self.arguments)
                .map_err(|_| WbReportSourceError::Upstream(WbErrorKind::Forbidden))?;
            if &fresh.identity != self.identity {
                return Err(WbReportSourceError::Upstream(WbErrorKind::Forbidden));
            }
            WbClientFinanceTransport::new(fresh.client, self.arguments.account.clone())
                .finance_page(start, end, limit, rrd_id)
                .await
        })
    }
}

const fn error_http_status(error: &WbError) -> Option<u16> {
    match error {
        WbError::Unauthorized { .. } => Some(401),
        WbError::Forbidden { .. } => Some(403),
        WbError::SubscriptionRequired { .. } => Some(402),
        WbError::RateLimited { .. } => Some(429),
        WbError::Api { status, .. } => Some(status.as_u16()),
        _ => None,
    }
}
