//! Operational assembly orders, kept separate from Statistics sales/orders.
use super::{MAX_WB_SIGNED_ID, WbClient, WbError, validate_positive_unique_ids};
use chrono::{DateTime, Utc};
use reqwest::Method;
use serde_json::{Value, json};
use std::collections::BTreeSet;

pub const NEW_PATH: &str = "/api/v3/orders/new";
pub const LIST_PATH: &str = "/api/v3/orders";
pub const STATUS_PATH: &str = "/api/v3/orders/status";

impl WbClient {
    pub async fn fbs_new_orders(&self, account: &str) -> Result<Value, WbError> {
        let data = self
            .request(account, Method::GET, NEW_PATH, None, None)
            .await?;
        validate_rows(&data, None)?;
        Ok(data)
    }

    /// One page only. Callers persist the cursor and the fixed UTC period.
    pub async fn fbs_orders_page(
        &self,
        account: &str,
        limit: u32,
        next: u64,
        date_from: i64,
        date_to: i64,
    ) -> Result<Value, WbError> {
        if !(1..=1_000).contains(&limit)
            || next > MAX_WB_SIGNED_ID
            || date_from < 0
            || date_to < date_from
            || date_to.saturating_sub(date_from) > 30 * 86_400
            || DateTime::<Utc>::from_timestamp(date_from, 0).is_none()
            || DateTime::<Utc>::from_timestamp(date_to, 0).is_none()
        {
            return Err(WbError::InvalidArguments {
                field: "limit/next/date_from/date_to",
            });
        }
        let data = self
            .request(
                account,
                Method::GET,
                LIST_PATH,
                Some(vec![
                    ("limit", limit.to_string()),
                    ("next", next.to_string()),
                    ("dateFrom", date_from.to_string()),
                    ("dateTo", date_to.to_string()),
                ]),
                None,
            )
            .await?;
        let count = validate_rows(&data, Some(limit as usize))?;
        let cursor = data
            .get("next")
            .and_then(Value::as_u64)
            .filter(|id| *id <= MAX_WB_SIGNED_ID)
            .ok_or_else(|| invalid("WB orders response has no valid next cursor"))?;
        // WB owns the cursor. Do not assume numeric ordering; only reject
        // a restart or a repeated cursor on a nonempty page.
        if count > 0 && (cursor == 0 || cursor == next) {
            return Err(invalid(
                "WB orders cursor did not advance; completeness is unknown",
            ));
        }
        Ok(data)
    }

    pub async fn fbs_order_statuses(
        &self,
        account: &str,
        orders: &[u64],
    ) -> Result<Value, WbError> {
        validate_positive_unique_ids(orders, 1_000, "orders", Some(MAX_WB_SIGNED_ID))?;
        let data = self
            .request(
                account,
                Method::POST,
                STATUS_PATH,
                None,
                Some(json!({"orders":orders})),
            )
            .await?;
        validate_rows(&data, Some(orders.len()))?;
        let requested: BTreeSet<_> = orders.iter().copied().collect();
        for row in data["orders"]
            .as_array()
            .ok_or_else(|| invalid("missing orders array"))?
        {
            if !row["id"].as_u64().is_some_and(|id| requested.contains(&id))
                || row
                    .get("supplierStatus")
                    .and_then(Value::as_str)
                    .is_none_or(|status| status.trim().is_empty())
                || row
                    .get("wbStatus")
                    .and_then(Value::as_str)
                    .is_none_or(|status| status.trim().is_empty())
            {
                return Err(invalid(
                    "WB returned an unrequested order or malformed status",
                ));
            }
        }
        Ok(data)
    }
}

fn validate_rows(data: &Value, limit: Option<usize>) -> Result<usize, WbError> {
    let rows = data
        .get("orders")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("WB response must contain an orders array"))?;
    if limit.is_some_and(|limit| rows.len() > limit) {
        return Err(invalid("WB returned more orders than requested"));
    }
    let mut seen = BTreeSet::new();
    for row in rows {
        let id = row
            .get("id")
            .and_then(Value::as_u64)
            .filter(|id| *id > 0 && *id <= MAX_WB_SIGNED_ID)
            .ok_or_else(|| invalid("WB order has no valid id"))?;
        if !seen.insert(id) {
            return Err(invalid("WB returned duplicate order ids"));
        }
    }
    Ok(rows.len())
}

fn invalid(message: &str) -> WbError {
    WbError::InvalidJson {
        request_id: None,
        source: serde::de::Error::custom(message),
    }
}

#[cfg(test)]
#[path = "tests/fbs_orders.rs"]
mod tests;
