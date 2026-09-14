use std::{future::Future, pin::Pin};

use chrono::NaiveDate;
use serde_json::Value;

use crate::wb::{WbClient, WbError, WbFinancePeriod};

use super::{WbFinanceReportPeriod, WbReportSourceError};

pub type WbOfficialReportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<Value>, WbReportSourceError>> + Send + 'a>>;

pub trait WbOfficialReportTransport: Send + Sync {
    /// The trusted account bound to this transport's credentials and quota.
    fn account_id(&self) -> &str;

    /// `None` means observed HTTP 204; HTTP 200 JSON null must remain Some.
    fn list_reports(
        &self,
        from: NaiveDate,
        to: NaiveDate,
        period: WbFinanceReportPeriod,
        limit: u32,
        offset: u32,
    ) -> WbOfficialReportFuture<'_>;

    /// Fixed by-ID endpoint and projection, with no user-selected URL/fields.
    fn report_details(&self, report_id: u64, limit: u32, rrd_id: u64)
    -> WbOfficialReportFuture<'_>;
}

#[derive(Clone)]
pub struct WbClientOfficialReportTransport {
    client: WbClient,
    account_id: String,
}

impl WbClientOfficialReportTransport {
    #[must_use]
    pub const fn new(client: WbClient, account_id: String) -> Self {
        Self { client, account_id }
    }
}

impl WbOfficialReportTransport for WbClientOfficialReportTransport {
    fn account_id(&self) -> &str {
        &self.account_id
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
            self.client
                .financial_reports_list_page(
                    &self.account_id,
                    from,
                    to,
                    match period {
                        WbFinanceReportPeriod::Daily => WbFinancePeriod::Daily,
                        WbFinanceReportPeriod::Weekly => WbFinancePeriod::Weekly,
                    },
                    limit,
                    offset,
                )
                .await
                .map_err(|error| source_error(&error))
        })
    }

    fn report_details(
        &self,
        report_id: u64,
        limit: u32,
        rrd_id: u64,
    ) -> WbOfficialReportFuture<'_> {
        Box::pin(async move {
            self.client
                .financial_report_by_id_page(&self.account_id, report_id, limit, rrd_id)
                .await
                .map_err(|error| source_error(&error))
        })
    }
}

fn source_error(error: &WbError) -> WbReportSourceError {
    match error {
        WbError::RateLimited {
            retry_after: Some(delay),
            ..
        }
        | WbError::LocalRateLimited { retry_after: delay } => WbReportSourceError::RetryAfter {
            seconds: crate::reporting::checkpoint::delay_seconds(*delay),
        },
        _ => WbReportSourceError::Upstream(error.kind()),
    }
}

#[cfg(test)]
mod tests;
