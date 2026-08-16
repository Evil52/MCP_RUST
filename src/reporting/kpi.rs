#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SalesMetricInput {
    pub ordered_units: u64,
    pub operational_gmv_minor: u64,
    pub cancelled_units: u64,
    pub returned_units: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdvertisingMetricInput {
    pub impressions: u64,
    pub clicks: u64,
    pub spend_minor: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
}

/// A percentage expressed in hundredths of one percent.
///
/// `1_000` means 10.00%; integer representation keeps server calculations
/// deterministic across HTML, XLSX and MCP projections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BasisPoints(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KpiSummary {
    pub ordered_units: u64,
    pub operational_gmv_minor: u64,
    pub cancelled_units: u64,
    pub returned_units: u64,
    pub ad_impressions: u64,
    pub ad_clicks: u64,
    pub ad_spend_minor: u64,
    pub attributed_orders: u64,
    pub attributed_revenue_minor: u64,
    pub ctr: Option<BasisPoints>,
    pub cpc_minor: Option<u64>,
    pub ad_conversion: Option<BasisPoints>,
    pub cpo_minor: Option<u64>,
    pub drr: Option<BasisPoints>,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum KpiError {
    #[error("daily KPI input contains an impossible advertising counter")]
    InvalidAdvertisingCounters,
    #[error("daily KPI aggregation exceeds the supported integer range")]
    Overflow,
}

pub fn calculate_kpis(
    sales: &[SalesMetricInput],
    advertising: &[AdvertisingMetricInput],
) -> Result<KpiSummary, KpiError> {
    let mut summary = KpiSummary {
        ordered_units: 0,
        operational_gmv_minor: 0,
        cancelled_units: 0,
        returned_units: 0,
        ad_impressions: 0,
        ad_clicks: 0,
        ad_spend_minor: 0,
        attributed_orders: 0,
        attributed_revenue_minor: 0,
        ctr: None,
        cpc_minor: None,
        ad_conversion: None,
        cpo_minor: None,
        drr: None,
    };

    for input in sales {
        summary.ordered_units = checked_sum(summary.ordered_units, input.ordered_units)?;
        summary.operational_gmv_minor =
            checked_sum(summary.operational_gmv_minor, input.operational_gmv_minor)?;
        summary.cancelled_units = checked_sum(summary.cancelled_units, input.cancelled_units)?;
        summary.returned_units = checked_sum(summary.returned_units, input.returned_units)?;
    }
    for input in advertising {
        if input.clicks > input.impressions || input.attributed_orders > input.clicks {
            return Err(KpiError::InvalidAdvertisingCounters);
        }
        summary.ad_impressions = checked_sum(summary.ad_impressions, input.impressions)?;
        summary.ad_clicks = checked_sum(summary.ad_clicks, input.clicks)?;
        summary.ad_spend_minor = checked_sum(summary.ad_spend_minor, input.spend_minor)?;
        summary.attributed_orders =
            checked_sum(summary.attributed_orders, input.attributed_orders)?;
        summary.attributed_revenue_minor = checked_sum(
            summary.attributed_revenue_minor,
            input.attributed_revenue_minor,
        )?;
    }

    summary.ctr = percentage(summary.ad_clicks, summary.ad_impressions)?;
    summary.cpc_minor = per_event(summary.ad_spend_minor, summary.ad_clicks);
    summary.ad_conversion = percentage(summary.attributed_orders, summary.ad_clicks)?;
    summary.cpo_minor = per_event(summary.ad_spend_minor, summary.attributed_orders);
    summary.drr = percentage(summary.ad_spend_minor, summary.attributed_revenue_minor)?;
    Ok(summary)
}

fn checked_sum(total: u64, value: u64) -> Result<u64, KpiError> {
    total.checked_add(value).ok_or(KpiError::Overflow)
}

fn percentage(numerator: u64, denominator: u64) -> Result<Option<BasisPoints>, KpiError> {
    if denominator == 0 {
        return Ok(None);
    }
    let scaled = u128::from(numerator) * 10_000;
    let rounded = (scaled + u128::from(denominator / 2)) / u128::from(denominator);
    Ok(Some(BasisPoints(
        u64::try_from(rounded).map_err(|_| KpiError::Overflow)?,
    )))
}

fn per_event(amount_minor: u64, events: u64) -> Option<u64> {
    (events != 0).then(|| {
        let rounded = (u128::from(amount_minor) + u128::from(events / 2)) / u128::from(events);
        u64::try_from(rounded).expect("an average of u64 values fits u64")
    })
}

#[cfg(test)]
mod tests {
    use super::{AdvertisingMetricInput, BasisPoints, KpiError, SalesMetricInput, calculate_kpis};

    #[test]
    fn kpis_use_exact_documented_denominators_and_rounding() {
        let summary = calculate_kpis(
            &[
                SalesMetricInput {
                    ordered_units: 5,
                    operational_gmv_minor: 100_000,
                    cancelled_units: 1,
                    returned_units: 0,
                },
                SalesMetricInput {
                    ordered_units: 2,
                    operational_gmv_minor: 45_000,
                    cancelled_units: 0,
                    returned_units: 1,
                },
            ],
            &[AdvertisingMetricInput {
                impressions: 3,
                clicks: 2,
                spend_minor: 101,
                attributed_orders: 1,
                attributed_revenue_minor: 505,
            }],
        )
        .unwrap();
        assert_eq!(summary.ordered_units, 7);
        assert_eq!(summary.operational_gmv_minor, 145_000);
        assert_eq!(summary.cancelled_units, 1);
        assert_eq!(summary.returned_units, 1);
        assert_eq!(summary.ad_impressions, 3);
        assert_eq!(summary.ad_clicks, 2);
        assert_eq!(summary.ad_spend_minor, 101);
        assert_eq!(summary.attributed_orders, 1);
        assert_eq!(summary.attributed_revenue_minor, 505);
        assert_eq!(summary.ctr, Some(BasisPoints(6_667)));
        assert_eq!(summary.cpc_minor, Some(51));
        assert_eq!(summary.ad_conversion, Some(BasisPoints(5_000)));
        assert_eq!(summary.cpo_minor, Some(101));
        assert_eq!(summary.drr, Some(BasisPoints(2_000)));
    }

    #[test]
    fn zero_denominators_are_not_misreported_as_zero_performance() {
        let summary = calculate_kpis(
            &[],
            &[AdvertisingMetricInput {
                impressions: 0,
                clicks: 0,
                spend_minor: 25,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            }],
        )
        .unwrap();
        assert_eq!(summary.ctr, None);
        assert_eq!(summary.cpc_minor, None);
        assert_eq!(summary.ad_conversion, None);
        assert_eq!(summary.cpo_minor, None);
        assert_eq!(summary.drr, None);
    }

    #[test]
    fn impossible_counters_and_every_aggregate_overflow_fail_closed() {
        for input in [
            AdvertisingMetricInput {
                impressions: 1,
                clicks: 2,
                spend_minor: 0,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            },
            AdvertisingMetricInput {
                impressions: 1,
                clicks: 1,
                spend_minor: 0,
                attributed_orders: 2,
                attributed_revenue_minor: 0,
            },
        ] {
            assert_eq!(
                calculate_kpis(&[], &[input]),
                Err(KpiError::InvalidAdvertisingCounters)
            );
        }

        let maximal_sale = SalesMetricInput {
            ordered_units: u64::MAX,
            operational_gmv_minor: u64::MAX,
            cancelled_units: u64::MAX,
            returned_units: u64::MAX,
        };
        for second in [
            SalesMetricInput {
                ordered_units: 1,
                operational_gmv_minor: 0,
                cancelled_units: 0,
                returned_units: 0,
            },
            SalesMetricInput {
                ordered_units: 0,
                operational_gmv_minor: 1,
                cancelled_units: 0,
                returned_units: 0,
            },
            SalesMetricInput {
                ordered_units: 0,
                operational_gmv_minor: 0,
                cancelled_units: 1,
                returned_units: 0,
            },
            SalesMetricInput {
                ordered_units: 0,
                operational_gmv_minor: 0,
                cancelled_units: 0,
                returned_units: 1,
            },
        ] {
            assert_eq!(
                calculate_kpis(&[maximal_sale, second], &[]),
                Err(KpiError::Overflow)
            );
        }

        let maximal_ad = AdvertisingMetricInput {
            impressions: u64::MAX,
            clicks: 0,
            spend_minor: u64::MAX,
            attributed_orders: 0,
            attributed_revenue_minor: u64::MAX,
        };
        for second in [
            AdvertisingMetricInput {
                impressions: 1,
                clicks: 0,
                spend_minor: 0,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            },
            AdvertisingMetricInput {
                impressions: 0,
                clicks: 0,
                spend_minor: 1,
                attributed_orders: 0,
                attributed_revenue_minor: 0,
            },
            AdvertisingMetricInput {
                impressions: 0,
                clicks: 0,
                spend_minor: 0,
                attributed_orders: 0,
                attributed_revenue_minor: 1,
            },
        ] {
            assert_eq!(
                calculate_kpis(&[], &[maximal_ad, second]),
                Err(KpiError::Overflow)
            );
        }
        assert_eq!(
            calculate_kpis(
                &[],
                &[AdvertisingMetricInput {
                    impressions: 1,
                    clicks: 0,
                    spend_minor: u64::MAX,
                    attributed_orders: 0,
                    attributed_revenue_minor: 1,
                }]
            ),
            Err(KpiError::Overflow)
        );
    }
}
