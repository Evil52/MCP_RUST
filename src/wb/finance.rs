//! Collector-only financial report read. No caller-selected host, fields or path.

use chrono::NaiveDate;
use serde_json::{Value, json};

use super::{FINANCE_DETAILS_PATH, MAX_WB_SIGNED_ID, Method, WbClient, WbError};
use crate::reporting::wb_finance_source::WB_FINANCE_FIELDS;

const FIRST_SUPPORTED: NaiveDate = NaiveDate::from_ymd_opt(2024, 1, 29).expect("static date");

impl WbClient {
    /// Fetch one daily-report page. Only HTTP 204 returns `None`.
    ///
    /// This is deliberately absent from the public MCP router. An unknown
    /// token tier uses the conservative documented 12-hour departure interval;
    /// neither rate-limit nor authorization failures cause a credential fallback.
    pub async fn financial_report_page(
        &self,
        account: &str,
        start: NaiveDate,
        end: NaiveDate,
        limit: u32,
        rrd_id: u64,
    ) -> Result<Option<Value>, WbError> {
        if start < FIRST_SUPPORTED || end < start || (end - start).num_days() >= 31 {
            return Err(WbError::InvalidArguments { field: "period" });
        }
        if !(1..=1_000).contains(&limit) {
            return Err(WbError::InvalidArguments { field: "limit" });
        }
        if rrd_id > MAX_WB_SIGNED_ID {
            return Err(WbError::InvalidArguments { field: "rrd_id" });
        }
        self.request_document(
            account,
            Method::POST,
            FINANCE_DETAILS_PATH,
            None,
            Some(json!({
                "dateFrom": start.to_string(),
                "dateTo": end.to_string(),
                "limit": limit,
                "rrdId": rrd_id,
                "period": "daily",
                "fields": WB_FINANCE_FIELDS,
            })),
        )
        .await
    }
}

#[cfg(test)]
mod tests;
