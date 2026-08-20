use std::time::Duration;

use chrono::NaiveDate;
use mcp_ozon::{
    config::StoreId,
    ozon_performance::{PerformanceClient, PerformanceErrorKind},
    reporting::{
        ozon_performance_source::{
            OzonPerformanceReportSourceError, OzonPerformanceReportTransport,
            PerformanceClientReportTransport,
        },
        unit_economics::{UnitEconomicsInput, calculate_unit_economics},
    },
};

#[tokio::test]
async fn production_library_extensions_are_executable_without_network_access() {
    let transport = PerformanceClientReportTransport::new(
        PerformanceClient::empty(Duration::from_secs(1)),
        StoreId::from("missing"),
    );
    assert_eq!(
        transport
            .expenses(vec![1], NaiveDate::from_ymd_opt(2026, 8, 19).unwrap(),)
            .await,
        Err(OzonPerformanceReportSourceError::Upstream(
            PerformanceErrorKind::MissingCredentials,
        ))
    );

    let input = UnitEconomicsInput {
        realized_revenue_minor: 1_000,
        marketplace_discount_minor: 0,
        commission_minor: 100,
        acquiring_minor: 10,
        logistics_minor: 100,
        storage_minor: 0,
        paid_acceptance_minor: 0,
        other_deductions_minor: 0,
        advertising_minor: 100,
        cost_of_goods_minor: 300,
        operating_expenses_minor: 0,
        taxes_minor: 50,
        compensation_minor: 0,
        sold_units: 1,
        returned_units: 0,
        cancelled_units: 0,
    };
    let summary = calculate_unit_economics(input).unwrap();
    assert_eq!(summary.net_profit_minor, 340);

    let loss = calculate_unit_economics(UnitEconomicsInput {
        cost_of_goods_minor: 2_000,
        ..input
    })
    .unwrap();
    assert!(loss.net_profit_minor < 0);
    assert_eq!(loss.margin, None);
    assert!(loss.roi.unwrap() < 0);
}
