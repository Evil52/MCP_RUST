//! Deterministic marketplace unit-economics calculations.
//!
//! Marketplace amounts enter this module only after source-specific parsing
//! and provenance checks. Missing inputs stay `None`; they are never replaced
//! with zero because that would turn an incomplete marketplace statement into
//! a plausible-looking profit figure.

use super::kpi::BasisPoints;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitEconomicsInput {
    pub realized_revenue_minor: u64,
    pub marketplace_discount_minor: u64,
    pub commission_minor: u64,
    pub acquiring_minor: u64,
    pub logistics_minor: u64,
    pub storage_minor: u64,
    pub paid_acceptance_minor: u64,
    pub other_deductions_minor: u64,
    pub advertising_minor: u64,
    pub cost_of_goods_minor: u64,
    pub operating_expenses_minor: u64,
    pub taxes_minor: u64,
    pub compensation_minor: u64,
    pub sold_units: u64,
    pub returned_units: u64,
    pub cancelled_units: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitEconomicsSummary {
    pub net_profit_minor: i64,
    pub margin: Option<BasisPoints>,
    pub roi: Option<i64>,
    pub buyout_rate: Option<BasisPoints>,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum UnitEconomicsError {
    #[error("unit-economics aggregation overflowed")]
    Overflow,
}

pub fn calculate_unit_economics(
    input: UnitEconomicsInput,
) -> Result<UnitEconomicsSummary, UnitEconomicsError> {
    let income = u128::from(input.realized_revenue_minor)
        .checked_add(u128::from(input.marketplace_discount_minor))
        .and_then(|value| value.checked_add(u128::from(input.compensation_minor)))
        .ok_or(UnitEconomicsError::Overflow)?;
    let expenses = [
        input.commission_minor,
        input.acquiring_minor,
        input.logistics_minor,
        input.storage_minor,
        input.paid_acceptance_minor,
        input.other_deductions_minor,
        input.advertising_minor,
        input.cost_of_goods_minor,
        input.operating_expenses_minor,
        input.taxes_minor,
    ]
    .into_iter()
    .try_fold(0u128, add_expense)?;
    let net_profit = i128::try_from(income)
        .ok()
        .and_then(|income| {
            i128::try_from(expenses)
                .ok()
                .map(|expenses| income - expenses)
        })
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(UnitEconomicsError::Overflow)?;
    let margin = signed_percentage(net_profit, input.realized_revenue_minor)?;
    let roi = signed_percentage_i64(net_profit, input.cost_of_goods_minor)?;
    let decided_units = input
        .sold_units
        .checked_add(input.returned_units)
        .and_then(|value| value.checked_add(input.cancelled_units))
        .ok_or(UnitEconomicsError::Overflow)?;
    let buyout_rate = unsigned_percentage(input.sold_units, decided_units)?;

    Ok(UnitEconomicsSummary {
        net_profit_minor: net_profit,
        margin,
        roi,
        buyout_rate,
    })
}

fn add_expense(total: u128, value: u64) -> Result<u128, UnitEconomicsError> {
    total
        .checked_add(u128::from(value))
        .ok_or(UnitEconomicsError::Overflow)
}

fn signed_percentage(
    numerator: i64,
    denominator: u64,
) -> Result<Option<BasisPoints>, UnitEconomicsError> {
    let value = signed_percentage_i64(numerator, denominator)?;
    match value {
        Some(value) if value >= 0 => Ok(Some(BasisPoints(
            u64::try_from(value).map_err(|_| UnitEconomicsError::Overflow)?,
        ))),
        // `BasisPoints` is unsigned. Negative margin remains available through
        // `net_profit_minor`; callers must not relabel it as a positive ratio.
        Some(_) | None => Ok(None),
    }
}

fn signed_percentage_i64(
    numerator: i64,
    denominator: u64,
) -> Result<Option<i64>, UnitEconomicsError> {
    if denominator == 0 {
        return Ok(None);
    }
    let scaled = i128::from(numerator)
        .checked_mul(10_000)
        .ok_or(UnitEconomicsError::Overflow)?;
    let denominator = i128::from(denominator);
    let rounded = if scaled >= 0 {
        (scaled + denominator / 2) / denominator
    } else {
        (scaled - denominator / 2) / denominator
    };
    i64::try_from(rounded)
        .map(Some)
        .map_err(|_| UnitEconomicsError::Overflow)
}

fn unsigned_percentage(
    numerator: u64,
    denominator: u64,
) -> Result<Option<BasisPoints>, UnitEconomicsError> {
    if denominator == 0 {
        return Ok(None);
    }
    let scaled = u128::from(numerator) * 10_000;
    let rounded = (scaled + u128::from(denominator / 2)) / u128::from(denominator);
    Ok(Some(BasisPoints(
        u64::try_from(rounded).map_err(|_| UnitEconomicsError::Overflow)?,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> UnitEconomicsInput {
        UnitEconomicsInput {
            realized_revenue_minor: 100_000,
            marketplace_discount_minor: 5_000,
            commission_minor: 10_000,
            acquiring_minor: 2_000,
            logistics_minor: 8_000,
            storage_minor: 1_000,
            paid_acceptance_minor: 500,
            other_deductions_minor: 1_500,
            advertising_minor: 10_000,
            cost_of_goods_minor: 40_000,
            operating_expenses_minor: 4_000,
            taxes_minor: 6_000,
            compensation_minor: 1_000,
            sold_units: 8,
            returned_units: 1,
            cancelled_units: 1,
        }
    }

    #[test]
    fn calculates_profit_margin_roi_and_buyout_without_floats() {
        let result = calculate_unit_economics(input()).unwrap();
        assert_eq!(result.net_profit_minor, 23_000);
        assert_eq!(result.margin, Some(BasisPoints(2_300)));
        assert_eq!(result.roi, Some(5_750));
        assert_eq!(result.buyout_rate, Some(BasisPoints(8_000)));
    }

    #[test]
    fn zero_denominators_and_losses_are_not_fabricated_as_positive_ratios() {
        let mut value = input();
        value.realized_revenue_minor = 0;
        value.cost_of_goods_minor = 0;
        value.sold_units = 0;
        value.returned_units = 0;
        value.cancelled_units = 0;
        let result = calculate_unit_economics(value).unwrap();
        assert!(result.net_profit_minor < 0);
        assert_eq!(result.margin, None);
        assert_eq!(result.roi, None);
        assert_eq!(result.buyout_rate, None);

        let mut loss = input();
        loss.cost_of_goods_minor = 200_000;
        let result = calculate_unit_economics(loss).unwrap();
        assert_eq!(result.margin, None);
        assert!(result.roi.unwrap() < 0);
    }

    #[test]
    fn aggregate_overflow_fails_closed() {
        let mut value = input();
        value.sold_units = u64::MAX;
        value.returned_units = 1;
        assert_eq!(
            calculate_unit_economics(value),
            Err(UnitEconomicsError::Overflow)
        );

        let mut money = input();
        money.realized_revenue_minor = u64::MAX;
        money.marketplace_discount_minor = 0;
        money.compensation_minor = 0;
        assert_eq!(
            calculate_unit_economics(money),
            Err(UnitEconomicsError::Overflow)
        );

        assert_eq!(
            signed_percentage_i64(i64::MAX, 1),
            Err(UnitEconomicsError::Overflow)
        );
        assert_eq!(
            signed_percentage(i64::MAX, 1),
            Err(UnitEconomicsError::Overflow)
        );
        assert_eq!(
            unsigned_percentage(u64::MAX, 1),
            Err(UnitEconomicsError::Overflow)
        );
    }
}
