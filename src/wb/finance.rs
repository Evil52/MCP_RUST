//! Collector-only financial report read. No caller-selected host, fields or path.

use chrono::NaiveDate;
use serde_json::{Value, json};

use super::{FINANCE_DETAILS_PATH, MAX_WB_SIGNED_ID, Method, WbClient, WbError};
use crate::reporting::wb_finance_source::WB_FINANCE_FIELDS;

const FIRST_SUPPORTED: NaiveDate = NaiveDate::from_ymd_opt(2024, 1, 29).expect("static date");
const FIRST_REPORT_LIST: NaiveDate = NaiveDate::from_ymd_opt(2025, 1, 1).expect("static date");
pub(super) const FINANCE_LIST_PATH: &str = "/api/finance/v1/sales-reports/list";
pub(super) const FINANCE_REPORT_ID_PATH: &str = "/api/finance/v1/sales-reports/detailed/{reportId}";

/// The two report periodicities accepted by the official finance API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WbFinancePeriod {
    Daily,
    Weekly,
}

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
        self.finance_document(
            account,
            FINANCE_DETAILS_PATH,
            json!({
                "dateFrom": start.to_string(),
                "dateTo": end.to_string(),
                "limit": limit,
                "rrdId": rrd_id,
                "period": "daily",
                "fields": WB_FINANCE_FIELDS,
            }),
        )
        .await
    }

    /// Fetch one bounded list page. The upstream list has no fields projection;
    /// the collector must normalize it before persisting or returning reports.
    pub async fn financial_reports_list_page(
        &self,
        account: &str,
        start: NaiveDate,
        end: NaiveDate,
        period: WbFinancePeriod,
        limit: u32,
        offset: u32,
    ) -> Result<Option<Value>, WbError> {
        if start < FIRST_REPORT_LIST || end < start || (end - start).num_days() >= 31 {
            return Err(WbError::InvalidArguments { field: "period" });
        }
        if !(1..=1_000).contains(&limit) {
            return Err(WbError::InvalidArguments { field: "limit" });
        }
        // The boundary offset is necessary to observe terminal HTTP 204 after
        // exactly 25,000 reports. The collector rejects any overflowing rows.
        if offset > 25_000 {
            return Err(WbError::InvalidArguments { field: "offset" });
        }
        self.finance_document(
            account,
            FINANCE_LIST_PATH,
            json!({
                "dateFrom": start.to_string(), "dateTo": end.to_string(),
                "period": period, "limit": limit, "offset": offset,
            }),
        )
        .await
    }

    /// Fetch one report ID's details with the same privacy projection as the
    /// period endpoint. A short HTTP 200 page is never terminal proof.
    pub async fn financial_report_by_id_page(
        &self,
        account: &str,
        report_id: u64,
        limit: u32,
        rrd_id: u64,
    ) -> Result<Option<Value>, WbError> {
        if report_id == 0 || report_id > MAX_WB_SIGNED_ID {
            return Err(WbError::InvalidArguments { field: "report_id" });
        }
        if !(1..=1_000).contains(&limit) {
            return Err(WbError::InvalidArguments { field: "limit" });
        }
        if rrd_id > MAX_WB_SIGNED_ID {
            return Err(WbError::InvalidArguments { field: "rrd_id" });
        }
        self.finance_document(
            account,
            &format!("{FINANCE_DETAILS_PATH}/{report_id}"),
            json!({"limit":limit, "rrdId":rrd_id, "fields":WB_FINANCE_FIELDS}),
        )
        .await
    }

    async fn finance_document(
        &self,
        account: &str,
        path: &str,
        payload: Value,
    ) -> Result<Option<Value>, WbError> {
        let result = self
            .request_document(account, Method::POST, path, None, Some(payload))
            .await?;
        let credentials = self
            .accounts
            .get(account)
            .expect("request resolved credentials");
        let limiter = self
            .limiters
            .get(account)
            .expect("request resolved limiter");
        limiter
            .finance_access
            .confirm_read(
                &credentials.token,
                &limiter.finance_reports,
                self.policy.finance_interval,
            )
            .await;
        Ok(result)
    }
}

#[cfg(test)]
mod tests;
