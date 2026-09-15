//! Current inventory at WB-owned warehouses.
use super::{Method, Value, WAREHOUSE_STOCKS_PATH, WbClient, WbError};

impl WbClient {
    pub async fn warehouse_stocks(&self, account: &str, payload: Value) -> Result<Value, WbError> {
        self.request(
            account,
            Method::POST,
            WAREHOUSE_STOCKS_PATH,
            None,
            Some(payload),
        )
        .await
    }
}
