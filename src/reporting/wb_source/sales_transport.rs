use super::{
    NaiveDate, Value, WbClientReportTransport, WbReportSourceError, json, wb_source_failure,
};

impl WbClientReportTransport {
    pub(super) async fn fetch_sales_page(
        &self,
        start: NaiveDate,
        end: NaiveDate,
        limit: u32,
        offset: u32,
        closing: bool,
    ) -> Result<Value, WbReportSourceError> {
        let mut payload = json!({
            "selectedPeriod":{"start":start,"end":end},"nmIds":[],
            "brandNames":[],"subjectIds":[],"tagIds":[],"skipDeletedNm":false,
            "limit":limit,"offset":offset
        });
        if closing {
            payload["orderBy"] = json!({"field":"orderCount","mode":"desc"});
        }
        self.client
            .sales_funnel(&self.account_id, payload)
            .await
            .map_err(|error| wb_source_failure(&error))
    }

    pub(super) async fn fetch_sales_control_totals(
        &self,
        date: NaiveDate,
    ) -> Result<Value, WbReportSourceError> {
        self.client
            .sales_funnel_grouped_history(
                &self.account_id,
                json!({
                    "selectedPeriod":{"start":date,"end":date},"brandNames":[],"subjectIds":[],
                    "tagIds":[],"skipDeletedNm":false,"aggregationLevel":"day"
                }),
            )
            .await
            .map_err(|error| wb_source_failure(&error))
    }
}
