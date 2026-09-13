use super::{
    CollectedAdvertisingFact, Context, NaiveDate, Result, WbAutomationObserver,
    parse_promotion_stats,
};

impl WbAutomationObserver {
    pub(super) async fn advertising(
        &self,
        status: i32,
        previous: NaiveDate,
        current: NaiveDate,
    ) -> Result<Vec<CollectedAdvertisingFact>> {
        // WB fullstats supports 7/9/11 only. Absence here is NOT zero-spend
        // evidence: build_observation retains both completeness flags false.
        if status == 4 {
            return Ok(Vec::new());
        }
        let response = self
            .client
            .promotion_stats(
                &self.policy.account_id,
                vec![self.policy.campaign_id],
                previous.format("%Y-%m-%d").to_string(),
                current.format("%Y-%m-%d").to_string(),
            )
            .await
            .context("WB automation campaign stats недоступны")?;
        parse_promotion_stats(&response)
            .map_err(|_| anyhow::anyhow!("WB automation campaign stats имеют неверную форму"))
    }
}
