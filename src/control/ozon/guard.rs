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
}
