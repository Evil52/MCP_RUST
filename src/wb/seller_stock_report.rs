//! Paginated Analytics inventory, independent of the active Content catalogue.
use super::{
    BTreeSet, MAX_WB_SIGNED_ID, Method, Value, WbClient, WbError, validate_positive_unique_ids,
};
use serde::Deserialize;
use serde_json::json;

pub const SELLER_STOCK_REPORT_PATH: &str = "/api/analytics/v1/stocks-report/seller-warehouses";
pub const SELLER_STOCK_REPORT_LABEL: &str =
    "analytics:/api/analytics/v1/stocks-report/seller-warehouses";
pub const SELLER_STOCK_REPORT_PAGE_LIMIT: u32 = 1_000;

#[derive(Debug)]
pub struct SellerStockReportPage {
    pub data: Value,
    pub returned_rows: u32,
    pub next_offset: Option<u32>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StockRow {
    nm_id: u64,
    chrt_id: u64,
    warehouse_id: u64,
    quantity: u64,
}

fn invalid_page() -> WbError {
    WbError::InvalidJson {
        request_id: None,
        source: serde::de::Error::custom("invalid seller stock report page"),
    }
}

impl WbClient {
    /// One bounded page. A terminal page says nothing about omitted pairs being zero.
    pub async fn seller_warehouses_stock_report(
        &self,
        account: &str,
        nm_ids: &[u64],
        chrt_ids: &[u64],
        limit: u32,
        offset: u32,
    ) -> Result<SellerStockReportPage, WbError> {
        for (field, values) in [("nm_ids", nm_ids), ("chrt_ids", chrt_ids)] {
            if !values.is_empty() {
                validate_positive_unique_ids(values, 1_000, field, Some(MAX_WB_SIGNED_ID))?;
            }
        }
        if !chrt_ids.is_empty() && nm_ids.is_empty() {
            return Err(WbError::InvalidArguments { field: "nm_ids" });
        }
        if !(1..=SELLER_STOCK_REPORT_PAGE_LIMIT).contains(&limit) {
            return Err(WbError::InvalidArguments { field: "limit" });
        }
        if offset > 1_000_000 {
            return Err(WbError::InvalidArguments { field: "offset" });
        }
        let data = self
            .request(
                account,
                Method::POST,
                SELLER_STOCK_REPORT_PATH,
                None,
                Some(
                    json!({"nmIds": nm_ids, "chrtIds": chrt_ids, "limit": limit, "offset": offset}),
                ),
            )
            .await?;
        let items = data
            .pointer("/data/items")
            .and_then(Value::as_array)
            .ok_or_else(invalid_page)?;
        let count = u32::try_from(items.len()).map_err(|_| invalid_page())?;
        if count > limit {
            return Err(invalid_page());
        }
        let mut seen = BTreeSet::new();
        for item in items {
            let row: StockRow = serde_json::from_value(item.clone()).map_err(|_| invalid_page())?;
            if [row.nm_id, row.chrt_id, row.warehouse_id]
                .iter()
                .any(|id| *id == 0 || *id > MAX_WB_SIGNED_ID)
                || row.quantity > MAX_WB_SIGNED_ID
                || !seen.insert((row.chrt_id, row.warehouse_id))
                || (!nm_ids.is_empty() && !nm_ids.contains(&row.nm_id))
                || (!chrt_ids.is_empty() && !chrt_ids.contains(&row.chrt_id))
            {
                return Err(invalid_page());
            }
        }
        Ok(SellerStockReportPage {
            data,
            returned_rows: count,
            next_offset: (count == limit).then_some(offset + count),
        })
    }
}
