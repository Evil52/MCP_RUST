//! Bounded histories; payments are funding, not advertising expenditure.
use super::{WbClient, WbError};
use chrono::NaiveDate;
use reqwest::Method;
use serde_json::Value;

pub const COSTS_PATH: &str = "/adv/v1/upd";
pub const PAYMENTS_PATH: &str = "/adv/v1/payments";
pub const PAYMENTS_LABEL: &str = "promotion:/adv/v1/payments";

impl WbClient {
    pub async fn promotion_costs(
        &self,
        account: &str,
        from: &str,
        to: &str,
    ) -> Result<Value, WbError> {
        self.promotion_money_history(account, COSTS_PATH, from, to)
            .await
    }

    pub async fn promotion_payments(
        &self,
        account: &str,
        from: &str,
        to: &str,
    ) -> Result<Value, WbError> {
        self.promotion_money_history(account, PAYMENTS_PATH, from, to)
            .await
    }

    async fn promotion_money_history(
        &self,
        account: &str,
        path: &str,
        from: &str,
        to: &str,
    ) -> Result<Value, WbError> {
        validate_period(from, to)?;
        let data = self
            .request(
                account,
                Method::GET,
                path,
                Some(vec![("from", from.to_owned()), ("to", to.to_owned())]),
                None,
            )
            .await?;
        if !data.is_array() {
            return Err(WbError::InvalidJson {
                request_id: None,
                source: serde::de::Error::custom("WB money history must be an array"),
            });
        }
        Ok(data)
    }
}

fn validate_period(from: &str, to: &str) -> Result<(), WbError> {
    let parse = |value: &str| {
        if value.len() != 10 {
            return None;
        }
        NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .ok()
            .filter(|date| date.format("%Y-%m-%d").to_string() == value)
    };
    let valid = parse(from)
        .zip(parse(to))
        .is_some_and(|(from, to)| (0..31).contains(&to.signed_duration_since(from).num_days()));
    if !valid {
        return Err(WbError::InvalidArguments {
            field: "from/to (YYYY-MM-DD, <=31 inclusive days)",
        });
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/promotion_money.rs"]
mod tests;
