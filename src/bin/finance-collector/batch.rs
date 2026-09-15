use anyhow::{Result, ensure};
use chrono::NaiveDate;
use mcp_ozon::reporting::wb_report_source::{WbReportList, WbSelectedReport};
use serde_json::{Value, json};
use std::future::Future;

/// Every item is published idempotently; replay never skips an unverified
/// local 'done' flag or silently filters a currency or an unclosed report.
pub async fn sync_reports<F, Fut>(
    list: &WbReportList,
    today: NaiveDate,
    mut publish: F,
) -> Result<Value>
where
    F: FnMut(WbSelectedReport) -> Fut,
    Fut: Future<Output = Result<Value>>,
{
    ensure!(
        list.reports().len() <= 256,
        "report batch exceeds its fixed 256-report bound"
    );
    let mut reports = Vec::new();
    for summary in list.reports() {
        let selected =
            list.select_closed(summary.scope.report_id, &summary.scope.currency, today)?;
        let result = publish(selected).await?;
        if result["status"] == "deferred" {
            return Ok(
                json!({"status":"deferred","next_request_at":result["next_request_at"],
                "published_reports":reports.len(),"total_reports":list.reports().len(),
                "collection_complete":false,"profit":null}),
            );
        }
        ensure!(
            result["status"] == "report_published",
            "batch item was not published"
        );
        reports.push(json!({"report_id":result["report_id"],"scope":result["scope"],
            "publication":result["publication"],"comparison":result["comparison"],"row_count":result["row_count"]}));
    }
    Ok(
        json!({"status":if reports.is_empty() {"no_reports"} else {"reports_published_complete"},
        "collection_complete":true,"total_reports":reports.len(),"reports":reports,"profit":null}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcp_ozon::reporting::{
        finance_reconciliation::WbFinanceReportPeriod,
        wb_report_source::{
            WbOfficialReportFuture, WbOfficialReportTransport, collect_report_list_checkpointed,
        },
    };
    struct Source(Vec<Value>);
    impl WbOfficialReportTransport for Source {
        fn account_id(&self) -> &'static str {
            "test_account"
        }
        fn list_reports(
            &self,
            _: NaiveDate,
            _: NaiveDate,
            _: WbFinanceReportPeriod,
            _: u32,
            offset: u32,
        ) -> WbOfficialReportFuture<'_> {
            Box::pin(async move {
                Ok(if offset == 0 && !self.0.is_empty() {
                    Some(json!(self.0))
                } else {
                    None
                })
            })
        }
        fn report_details(&self, _: u64, _: u32, _: u64) -> WbOfficialReportFuture<'_> {
            unreachable!()
        }
    }
    fn date(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 9, day).unwrap()
    }
    async fn list(count: u64) -> WbReportList {
        let rows = (1..=count)
            .map(|id| {
                json!({"reportId":id,"dateFrom":"2026-09-01","dateTo":"2026-09-07",
            "createDate":"2026-09-08","currency":if id==1 {"RUB"} else {"CNY"},"reportType":1,
            "retailAmountSum":"12.01","forPaySum":"10.01"})
            })
            .collect();
        collect_report_list_checkpointed(
            &Source(rows),
            "test_account",
            date(1),
            date(8),
            WbFinanceReportPeriod::Weekly,
            &None,
        )
        .await
        .unwrap()
    }
    #[tokio::test]
    async fn interruption_retains_partial_progress_and_resume_preserves_currency_and_mismatch() {
        let list = list(2).await;
        let mut seen = Vec::new();
        let deferred = sync_reports(&list, date(13), |report| {
            let id = report.summary().scope.report_id;
            seen.push(id);
            std::future::ready(Ok(if id == 2 {
                json!({"status":"deferred","next_request_at":"2026-09-13T10:00:00Z"})
            } else {
                json!({"status":"report_published","report_id":id})
            }))
        })
        .await
        .unwrap();
        assert_eq!(seen, vec![1, 2]);
        assert_eq!(deferred["published_reports"], 1);
        assert_eq!(deferred["collection_complete"], false);
        let completed=sync_reports(&list,date(13),|report| std::future::ready(Ok(json!({
            "status":"report_published","report_id":report.summary().scope.report_id,"scope":report.summary().scope,
            "comparison":{"status":"mismatch"},"publication":{"already_present":report.summary().scope.report_id==1}
        })))).await.unwrap();
        assert_eq!(completed["total_reports"], 2);
        assert_eq!(
            completed["reports"][0]["publication"]["already_present"],
            true
        );
        assert_eq!(completed["reports"][1]["scope"]["currency"], "CNY");
        assert_eq!(completed["reports"][1]["comparison"]["status"], "mismatch");
        assert!(completed["profit"].is_null());
    }
    #[tokio::test]
    async fn no_report_is_not_zero_finance_and_unclosed_or_unpublished_items_cannot_complete() {
        let empty = list(0).await;
        assert_eq!(
            sync_reports(&empty, date(13), |_| std::future::ready(Ok(Value::Null)))
                .await
                .unwrap()["status"],
            "no_reports"
        );
        let reports = list(1).await;
        assert!(
            sync_reports(&reports, date(7), |_| std::future::ready(Ok(Value::Null)))
                .await
                .is_err()
        );
        assert!(
            sync_reports(&reports, date(13), |_| std::future::ready(Ok(
                json!({"status":"report_comparison_complete"})
            )))
            .await
            .is_err()
        );
        let excessive = list(257).await;
        assert!(
            sync_reports(&excessive, date(13), |_| std::future::ready(Ok(
                Value::Null
            )))
            .await
            .is_err()
        );
    }
}
