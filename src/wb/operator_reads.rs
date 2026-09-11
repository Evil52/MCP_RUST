use super::{
    MAX_WB_SIGNED_ID, PROMOTION_BALANCE_PATH, PROMOTION_BUDGET_PATH, WbClient, WbError,
    validate_positive_unique_ids,
};
use reqwest::Method;
use serde_json::Value;

impl WbClient {
    /// Returns account funding sources without transferring money. `balance`
    /// is WB's type=1 mutual-settlement source, not bonuses or an external card.
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
        self.request(
            account,
            Method::GET,
            PROMOTION_BUDGET_PATH,
            Some(vec![("id", advert_id.to_string())]),
            None,
        )
        .await
    }
}
