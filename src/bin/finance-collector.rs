#![forbid(unsafe_code)]

//! Explicit local operator only; no schedule is enabled by installing this
//! binary. Marketplace keys stay in a deployment-owned private directory.

#[path = "finance-collector/access.rs"]
mod access;
#[path = "finance-collector/arguments.rs"]
mod arguments;
#[path = "finance-collector/checkpoint.rs"]
mod checkpoint;
#[path = "finance-collector/reports.rs"]
mod reports;

use std::{future::Future, pin::Pin, sync::Arc};

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

use arguments::{Arguments, Command, Egress, parse_arguments};

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if print_runtime_version_if_requested("finance-collector", &arguments)? {
        return Ok(());
    }
    if arguments == ["--help"] {
        println!(
            "finance-collector probe-wb|collect-wb|list-reports-wb|reconcile-report-wb|publish-report-wb|migrate-personal-quota --registry PATH --actor ID --account ID --credentials-dir PATH --state-dir PATH --from YYYY-MM-DD --to YYYY-MM-DD [--egress collector-proxy|direct]"
        );
        println!(
            "Official report commands require --observation ID and accept --period weekly|daily (default weekly), --currency RUB. reconcile-report-wb and publish-report-wb also require --report-id INT64. Reuse the same observation to resume pages; use a new ID to observe revisions."
        );
        println!(
            "collect-wb and publish-report-wb read REPORT_COLLECTOR_DATABASE_URL. Reuse one deployment-owned private state directory for all invocations. Default egress is collector-proxy. Seller quota becomes 60 seconds only after an observed successful Personal read. migrate-personal-quota requires a reviewed local legacy success receipt and sends no HTTP request."
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

async fn execute(arguments: &Arguments) -> Result<Value> {
    let scoped = access::resolve(arguments)?;
    let identity = reports::collection_identity(arguments)?;
    let journal = Arc::new(LocalJournal::open_personal(
        &arguments.state_dir,
        &scoped.identity.seller_scope,
        &identity,
        scoped.identity.fingerprint(),
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
                Ok(response) => {
                    journal.confirm_personal_read()?;
                    Ok(json!({
                        "account_id": arguments.account, "actor_id": arguments.actor,
                        "status": "method_read_succeeded", "http_status": if response.is_some() { 200 } else { 204 },
                        "endpoint": "POST https://finance-api.wildberries.ru/api/finance/v1/sales-reports/detailed",
                        "checked_at": Utc::now(),
                        "local_claims_only": { "personal": true, "read_only": true, "finance_category": true },
                        "pagination_complete": response.is_none(),
                        "next_request_at": journal.next_allowed_at()?,
                        "financial_rows_emitted": 0,
                    }))
                }
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
                journal: &journal,
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
        Command::ListReportsWb | Command::ReconcileReportWb | Command::PublishReportWb => {
            reports::execute(arguments, &scoped.identity, &journal).await
        }
        Command::MigratePersonalQuota => {
            journal.migrate_personal_legacy(&arguments.actor, &scoped.identity.seller_scope)?;
            Ok(
                json!({"account_id": arguments.account, "actor_id": arguments.actor,
                "status": "personal_quota_migrated", "http_requests": 0,
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
    journal: &'a LocalJournal,
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
            let response =
                WbClientFinanceTransport::new(fresh.client, self.arguments.account.clone())
                    .finance_page(start, end, limit, rrd_id)
                    .await?;
            self.journal
                .confirm_personal_read()
                .map_err(|_| WbReportSourceError::Checkpoint(CheckpointError::Unavailable))?;
            Ok(response)
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
