use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use chrono::NaiveDate;
use mcp_ozon::{
    reporting::{
        checkpoint::{CheckpointError, Checkpoints},
        finance_reconciliation::WbFinanceReportPeriod,
        wb_finance_source::diagnose_finance_page,
        wb_official_reconciliation::reconcile_wb_official_report,
        wb_report_repository::PostgresWbReportRepository,
        wb_report_source::{
            WbClientOfficialReportTransport, WbCompleteReportDetails, WbOfficialReportFuture,
            WbOfficialReportTransport, WbSelectedReport, collect_report_list_checkpointed,
        },
        wb_source::WbReportSourceError,
    },
    wb::WbErrorKind,
};
use serde_json::{Value, json};

use super::{
    Arguments, Command, access,
    arguments::{moscow_today, period_name},
    checkpoint::LocalJournal,
    deferred,
};

pub fn collection_identity(arguments: &Arguments) -> Result<String> {
    if matches!(
        arguments.command,
        Command::ListReportsWb
            | Command::ReconcileReportWb
            | Command::PublishReportWb
            | Command::SyncReportsWb
    ) {
        let observation = arguments
            .observation
            .as_deref()
            .context("observation is required")?;
        Ok(format!(
            "wb-official-v1:{}:{}:{}:{}:{}",
            arguments.account,
            arguments.from,
            arguments.to,
            period_name(arguments.period),
            observation
        ))
    } else {
        Ok(format!(
            "wb-detailed-v1:{}:{}:{}",
            arguments.account, arguments.from, arguments.to
        ))
    }
}

pub async fn execute(
    arguments: &Arguments,
    identity: &access::CredentialIdentity,
    journal: &Arc<LocalJournal>,
) -> Result<Value> {
    let writer = if matches!(
        arguments.command,
        Command::PublishReportWb | Command::SyncReportsWb
    ) {
        let database_url = std::env::var("REPORT_COLLECTOR_DATABASE_URL")
            .context("REPORT_COLLECTOR_DATABASE_URL is required for publish-report-wb")?;
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
        Some(PostgresWbReportRepository::connect(&config).await?)
    } else {
        None
    };
    let checkpoints: Checkpoints = Some(journal.clone());
    let transport = AuthorizedOfficialTransport {
        arguments,
        identity,
        journal,
    };
    let list = match collect_report_list_checkpointed(
        &transport,
        &arguments.account,
        arguments.from,
        arguments.to,
        arguments.period,
        &checkpoints,
    )
    .await
    {
        Ok(list) => list,
        Err(error) => return source_failure(error, arguments, journal),
    };
    recheck(arguments, identity)?;
    if matches!(arguments.command, Command::ListReportsWb) {
        let path = journal.save_evidence(
            "report-list.json",
            &json!({
                "version": 1, "account_id": arguments.account,
                "observation": arguments.observation, "list": list,
                "terminal_http_status": 204,
            }),
        )?;
        let candidates: Vec<_> = list
            .reports()
            .iter()
            .map(|report| {
                json!({
                    "scope": report.scope, "created_date": report.created_date,
                    "report_type": report.report_type,
                    "closed": report.scope.date_to < moscow_today()
                        && report.created_date <= moscow_today()
                        && report.created_date >= report.scope.date_to,
                })
            })
            .collect();
        return Ok(json!({
            "account_id": arguments.account, "actor_id": arguments.actor,
            "status": "report_list_complete", "report_count": candidates.len(),
            "reports": candidates, "observation": arguments.observation,
            "pagination_complete": true, "terminal_http_status": 204,
            "evidence_file": path, "next_request_at": journal.next_allowed_at()?,
        }));
    }
    if arguments.command == Command::SyncReportsWb {
        let mut result = super::batch::sync_reports(&list, moscow_today(), |selected| {
            let transport = &transport;
            let writer = writer.as_ref();
            async move {
                finish_report(arguments, identity, journal, transport, writer, &selected).await
            }
        })
        .await?;
        recheck(arguments, identity)?;
        result["account_id"] = json!(arguments.account);
        result["actor_id"] = json!(arguments.actor);
        result["observation"] = json!(arguments.observation);
        result["date_from"] = json!(arguments.from);
        result["date_to"] = json!(arguments.to);
        return Ok(result);
    }
    ensure!(
        matches!(
            arguments.command,
            Command::ReconcileReportWb | Command::PublishReportWb
        ),
        "unsupported report command"
    );
    let report_id = arguments.report_id.context("report ID is required")?;
    let selected = list.select_closed(report_id, &arguments.currency, moscow_today())?;
    finish_report(
        arguments,
        identity,
        journal,
        &transport,
        writer.as_ref(),
        &selected,
    )
    .await
}

async fn finish_report(
    arguments: &Arguments,
    identity: &access::CredentialIdentity,
    journal: &Arc<LocalJournal>,
    transport: &AuthorizedOfficialTransport<'_>,
    writer: Option<&PostgresWbReportRepository>,
    selected: &WbSelectedReport,
) -> Result<Value> {
    let checkpoints: Checkpoints = Some(journal.clone());
    let report_id = selected.summary().scope.report_id;
    let details = match WbCompleteReportDetails::collect(transport, selected, &checkpoints).await {
        Ok(details) => details,
        Err(error) => return source_failure(error, arguments, journal),
    };
    let comparison = reconcile_wb_official_report(
        details.rows(),
        details.evidence(),
        Some(&selected.baseline()),
    )?;
    recheck(arguments, identity)?;
    // Rows already exist in private normalized page checkpoints. Keep this
    // immutable manifest small while binding both sources and the mapping.
    let path = journal.save_evidence(
        &format!("official-report-{report_id}.json"),
        &json!({
            "version": 1, "account_id": arguments.account,
            "observation": arguments.observation, "summary": selected,
            "details_evidence": details.evidence(), "row_count": details.rows().len(),
            "comparison": comparison,
        }),
    )?;
    let publication = if let Some(writer) = writer {
        recheck(arguments, identity)?;
        Some(writer.publish(selected, &details, &arguments.actor).await?)
    } else {
        None
    };
    Ok(json!({
        "account_id": arguments.account, "actor_id": arguments.actor,
        "status": if publication.is_some() { "report_published" } else { "report_comparison_complete" },
        "publication": publication, "report_id": report_id,
        "observation": arguments.observation, "scope": selected.summary().scope,
        "row_count": details.rows().len(), "pagination_complete": true,
        "terminal_http_status": 204, "comparison": comparison,
        "summary_sha256": selected.evidence().source_sha256,
        "details_sha256": details.evidence().source_sha256,
        "evidence_file": path, "next_request_at": journal.next_allowed_at()?,
        "profit": null,
    }))
}

fn source_failure(
    error: WbReportSourceError,
    arguments: &Arguments,
    journal: &LocalJournal,
) -> Result<Value> {
    match error {
        WbReportSourceError::Checkpoint(CheckpointError::Deferred) => deferred(arguments, journal),
        WbReportSourceError::RetryAfter { seconds } => {
            journal.postpone(seconds)?;
            deferred(arguments, journal)
        }
        _ => Err(error.into()),
    }
}

fn recheck(
    arguments: &Arguments,
    identity: &access::CredentialIdentity,
) -> Result<access::ScopedClient> {
    let fresh = access::resolve(arguments)?;
    ensure!(
        &fresh.identity == identity,
        "financial credential binding changed before operation"
    );
    Ok(fresh)
}

struct AuthorizedOfficialTransport<'a> {
    arguments: &'a Arguments,
    identity: &'a access::CredentialIdentity,
    journal: &'a LocalJournal,
}

impl AuthorizedOfficialTransport<'_> {
    fn authorized(&self) -> Result<WbClientOfficialReportTransport, WbReportSourceError> {
        let fresh = recheck(self.arguments, self.identity)
            .map_err(|_| WbReportSourceError::Upstream(WbErrorKind::Forbidden))?;
        Ok(WbClientOfficialReportTransport::new(
            fresh.client,
            self.arguments.account.clone(),
        ))
    }

    fn confirmed(&self, response: Option<Value>) -> Result<Option<Value>, WbReportSourceError> {
        self.journal
            .confirm_personal_read()
            .map_err(|_| WbReportSourceError::Checkpoint(CheckpointError::Unavailable))?;
        Ok(response)
    }
}

impl WbOfficialReportTransport for AuthorizedOfficialTransport<'_> {
    fn account_id(&self) -> &str {
        &self.arguments.account
    }

    fn list_reports(
        &self,
        from: NaiveDate,
        to: NaiveDate,
        period: WbFinanceReportPeriod,
        limit: u32,
        offset: u32,
    ) -> WbOfficialReportFuture<'_> {
        Box::pin(async move {
            let response = self
                .authorized()?
                .list_reports(from, to, period, limit, offset)
                .await?;
            self.confirmed(response)
        })
    }

    fn report_details(
        &self,
        report_id: u64,
        limit: u32,
        rrd_id: u64,
    ) -> WbOfficialReportFuture<'_> {
        Box::pin(async move {
            let response = self
                .authorized()?
                .report_details(report_id, limit, rrd_id)
                .await?;
            if let Some(diagnostics) = response.as_ref().and_then(|page| {
                diagnose_finance_page(page, report_id, &self.arguments.currency, rrd_id)
            }) && let Ok(encoded) = serde_json::to_string(&diagnostics)
            {
                eprintln!("WB financial protocol diagnostics: {encoded}");
            }
            self.confirmed(response)
        })
    }
}
