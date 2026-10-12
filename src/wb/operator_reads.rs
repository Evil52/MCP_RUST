use super::{
    MAX_WB_SIGNED_ID, PROMOTION_BALANCE_PATH, WbClient, WbError, validate_positive_unique_ids,
};
use reqwest::Method;
use serde_json::Value;

impl WbClient {
    /// Returns funding sources without transferring money. `net` is WB's
    /// type=1 mutual settlement; `balance` is the prepaid type=0 account.
    pub async fn promotion_balance(&self, account: &str) -> Result<Value, WbError> {
        self.request(account, Method::GET, PROMOTION_BALANCE_PATH, None, None)
            .await
    }

    /// Returns the remaining budget for exactly one promotion campaign.
    pub async fn promotion_campaign_budget(
        &self,
        account: &str,
        advert_id: u64,
    ) -> Result<Value, WbError> {
        validate_positive_unique_ids(&[advert_id], 1, "advert_id", Some(MAX_WB_SIGNED_ID))?;
        let data = self
            .promotion_campaign_budgets(account, &[advert_id])
            .await?;
        let invalid = || WbError::InvalidJson {
            request_id: None,
            source: serde::de::Error::custom(
                "WB budget response must contain exactly the requested campaign and a nonnegative integer total",
            ),
        };
        let rows = data
            .get("adverts")
            .and_then(Value::as_array)
            .ok_or_else(invalid)?;
        // Missing/null budgets are unavailable, never a confirmed zero. Preserve
        // the single-campaign `total` contract used by guarded automation.
        if rows.len() != 1 {
            return Err(invalid());
        }
        let row = &rows[0];
        if row.get("advertId").and_then(Value::as_u64) != Some(advert_id)
            || row.get("total").and_then(Value::as_u64).is_none()
            || row.get("currency").and_then(Value::as_str) != Some("RUB")
        {
            return Err(invalid());
        }
        Ok(row.clone())
    }
}
