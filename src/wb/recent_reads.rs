//! Bounded order feed, seller returns, supplies and promotion reads.
use super::{
    MAX_WB_SIGNED_ID, WbClient, WbError, comma_separated, recent_policy as ep,
    validate_positive_unique_ids,
};
use reqwest::Method;
use serde_json::{Value, json};

impl WbClient {
    pub async fn promotion_campaign_budgets(
        &self,
        account: &str,
        advert_ids: &[u64],
    ) -> Result<Value, WbError> {
        validate_positive_unique_ids(advert_ids, 50, "advert_ids", Some(MAX_WB_SIGNED_ID))?;
        self.request(
            account,
            Method::POST,
            ep::BUDGETS,
            None,
            Some(json!({"advertIds": advert_ids})),
        )
        .await
    }

    pub async fn promotion_daily_limits(
        &self,
        account: &str,
        advert_ids: &[u64],
    ) -> Result<Value, WbError> {
        validate_positive_unique_ids(advert_ids, 100, "advert_ids", Some(MAX_WB_SIGNED_ID))?;
        self.request(
            account,
            Method::GET,
            ep::DAILY_LIMITS,
            Some(vec![("advertIds", comma_separated(advert_ids))]),
            None,
        )
        .await
    }

    pub async fn supply_discrepancies(
        &self,
        account: &str,
        supply_id: u64,
    ) -> Result<Value, WbError> {
        validate_positive_unique_ids(&[supply_id], 1, "supply_id", Some(MAX_WB_SIGNED_ID))?;
        self.request(
            account,
            Method::GET,
            &format!("/api/supplies/v1/discrepancies/{supply_id}"),
            None,
            None,
        )
        .await
    }

    pub async fn order_feed(
        &self,
        account: &str,
        query: &super::recent_queries::OrderFeedQuery,
    ) -> Result<Value, WbError> {
        query.validate()?;
        self.request(
            account,
            Method::POST,
            ep::ORDER_FEED,
            None,
            Some(query.payload()),
        )
        .await
    }

    pub async fn item_returns(
        &self,
        account: &str,
        query: &super::recent_queries::ItemReturnsQuery,
    ) -> Result<Value, WbError> {
        query.validate()?;
        self.request(
            account,
            Method::GET,
            ep::ITEM_RETURNS,
            Some(query.parameters()),
            None,
        )
        .await
    }

    pub async fn stock_history_create(
        &self,
        account: &str,
        query: &super::recent_queries::StockHistoryQuery,
    ) -> Result<Value, WbError> {
        query.validate()?;
        self.request(
            account,
            Method::POST,
            ep::DOWNLOADS,
            None,
            Some(query.payload()),
        )
        .await
    }

    pub async fn analytics_report_status(
        &self,
        account: &str,
        download_id: &str,
    ) -> Result<Value, WbError> {
        validate_download_id(download_id)?;
        self.request(
            account,
            Method::GET,
            ep::DOWNLOADS,
            Some(vec![("filter[downloadIds]", download_id.to_owned())]),
            None,
        )
        .await
    }

    pub async fn analytics_report_download(
        &self,
        account: &str,
        download_id: &str,
    ) -> Result<Value, WbError> {
        validate_download_id(download_id)?;
        self.request(
            account,
            Method::GET,
            &format!("/api/v2/nm-report/downloads/file/{download_id}"),
            None,
            None,
        )
        .await
    }
}

pub(super) fn validate_download_id(value: &str) -> Result<(), WbError> {
    if !ep::valid_uuid(value) {
        return Err(WbError::InvalidArguments {
            field: "download_id (UUID)",
        });
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/recent_reads.rs"]
mod tests;
