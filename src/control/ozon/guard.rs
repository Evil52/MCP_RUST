use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OzonGuardStopReason {
    SpendCapReached,
    DrrCapExceeded,
}

impl OzonGuardStopReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SpendCapReached => "spend_cap_reached",
            Self::DrrCapExceeded => "drr_cap_exceeded",
        }
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonGuardEvaluationError {
    #[error("Ozon guard limits use invalid units or values")]
    InvalidLimit,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum OzonProductGuardError {
    #[error("Ozon product guard bid limits are invalid")]
    InvalidLimit,
    #[error("Ozon campaign product snapshot is invalid")]
    InvalidSnapshot,
    #[error("Ozon campaign product SKU differs from the guarded SKU")]
    SkuMismatch,
    #[error("Ozon campaign product bid is outside the guarded corridor")]
    BidOutOfRange,
}

pub fn validate_ozon_campaign_product_guard(
    snapshot: &Value,
    expected_sku: u64,
    min_bid_microrubles: u64,
    max_bid_microrubles: u64,
) -> Result<u64, OzonProductGuardError> {
    if expected_sku == 0
        || min_bid_microrubles == 0
        || min_bid_microrubles > max_bid_microrubles
        || !min_bid_microrubles.is_multiple_of(1_000_000)
        || !max_bid_microrubles.is_multiple_of(1_000_000)
    {
        return Err(OzonProductGuardError::InvalidLimit);
    }
    let products = snapshot
        .get("products")
        .and_then(Value::as_array)
        .filter(|products| products.len() == 1)
        .ok_or(OzonProductGuardError::InvalidSnapshot)?;
    let product = &products[0];
    let sku = canonical_u64(product.get("sku")).ok_or(OzonProductGuardError::InvalidSnapshot)?;
    if sku != expected_sku {
        return Err(OzonProductGuardError::SkuMismatch);
    }
    let bid = canonical_u64(product.get("bid")).ok_or(OzonProductGuardError::InvalidSnapshot)?;
    if !(min_bid_microrubles..=max_bid_microrubles).contains(&bid) {
        return Err(OzonProductGuardError::BidOutOfRange);
    }
    Ok(bid)
}

fn canonical_u64(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Number(value) => value.as_u64(),
        Value::String(value) => value
            .parse::<u64>()
            .ok()
            .filter(|parsed| parsed.to_string() == *value),
        _ => None,
    }
}

pub fn evaluate_ozon_campaign_guard(
    spend_minor: u64,
    attributed_revenue_minor: u64,
    spend_cap_microrubles: u64,
    target_drr_percent: u8,
) -> Result<Option<OzonGuardStopReason>, OzonGuardEvaluationError> {
    if spend_cap_microrubles == 0
        || !spend_cap_microrubles.is_multiple_of(10_000)
        || !(10..=100).contains(&target_drr_percent)
    {
        return Err(OzonGuardEvaluationError::InvalidLimit);
    }
    let spend_cap_minor = spend_cap_microrubles / 10_000;
    if spend_minor >= spend_cap_minor {
        return Ok(Some(OzonGuardStopReason::SpendCapReached));
    }
    let drr_exceeded = spend_minor > 0
        && u128::from(spend_minor) * 100
            > u128::from(attributed_revenue_minor) * u128::from(target_drr_percent);
    Ok(drr_exceeded.then_some(OzonGuardStopReason::DrrCapExceeded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spend_cap_has_priority_and_uses_exact_currency_conversion() {
        assert_eq!(
            evaluate_ozon_campaign_guard(200_000, 10_000_000, 2_000_000_000, 15),
            Ok(Some(OzonGuardStopReason::SpendCapReached))
        );
        assert_eq!(
            evaluate_ozon_campaign_guard(199_999, 10_000_000, 2_000_000_000, 15),
            Ok(None)
        );
    }

    #[test]
    fn drr_boundary_is_inclusive_and_zero_revenue_stops_after_spend() {
        assert_eq!(
            evaluate_ozon_campaign_guard(15_000, 100_000, 2_000_000_000, 15),
            Ok(None)
        );
        assert_eq!(
            evaluate_ozon_campaign_guard(15_001, 100_000, 2_000_000_000, 15),
            Ok(Some(OzonGuardStopReason::DrrCapExceeded))
        );
        assert_eq!(
            evaluate_ozon_campaign_guard(1, 0, 2_000_000_000, 15),
            Ok(Some(OzonGuardStopReason::DrrCapExceeded))
        );
        assert_eq!(
            evaluate_ozon_campaign_guard(0, 0, 2_000_000_000, 15),
            Ok(None)
        );
    }

    #[test]
    fn invalid_limits_fail_closed() {
        assert_eq!(
            evaluate_ozon_campaign_guard(0, 0, 1, 15),
            Err(OzonGuardEvaluationError::InvalidLimit)
        );
        assert_eq!(
            evaluate_ozon_campaign_guard(0, 0, 2_000_000_000, 9),
            Err(OzonGuardEvaluationError::InvalidLimit)
        );
    }

    #[test]
    fn product_guard_requires_one_exact_sku_inside_the_bid_corridor() {
        let snapshot = serde_json::json!({
            "products": [{"sku": "3588576015", "bid": "7000000"}]
        });
        assert_eq!(
            validate_ozon_campaign_product_guard(&snapshot, 3_588_576_015, 7_000_000, 12_000_000),
            Ok(7_000_000)
        );

        for (snapshot, expected) in [
            (
                serde_json::json!({"products": []}),
                OzonProductGuardError::InvalidSnapshot,
            ),
            (
                serde_json::json!({"products": [
                    {"sku": "3588576015", "bid": "7000000"},
                    {"sku": "1", "bid": "7000000"}
                ]}),
                OzonProductGuardError::InvalidSnapshot,
            ),
            (
                serde_json::json!({"products": [{"sku": "03588576015", "bid": "7000000"}]}),
                OzonProductGuardError::InvalidSnapshot,
            ),
            (
                serde_json::json!({"products": [{"sku": "1", "bid": "7000000"}]}),
                OzonProductGuardError::SkuMismatch,
            ),
            (
                serde_json::json!({"products": [{"sku": "3588576015", "bid": "6000000"}]}),
                OzonProductGuardError::BidOutOfRange,
            ),
            (
                serde_json::json!({"products": [{"sku": "3588576015", "bid": 13000000}]}),
                OzonProductGuardError::BidOutOfRange,
            ),
        ] {
            assert_eq!(
                validate_ozon_campaign_product_guard(
                    &snapshot,
                    3_588_576_015,
                    7_000_000,
                    12_000_000
                ),
                Err(expected)
            );
        }
        assert_eq!(
            validate_ozon_campaign_product_guard(&snapshot, 3_588_576_015, 12_000_000, 7_000_000),
            Err(OzonProductGuardError::InvalidLimit)
        );
    }
}
