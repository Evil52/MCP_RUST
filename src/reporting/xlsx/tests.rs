mod inventory;
use chrono::{Duration, TimeZone};

use super::*;

fn generated_at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 16, 12, 1, 0).unwrap()
}

fn summary<'a>(kpis: &'a KpiSummary, problems: &'a [PriorityProblem]) -> HtmlReport<'a> {
    HtmlReport {
        manager_name: "Диана",
        account_ids: &["ozon_store"],
        generated_at: generated_at(),
        interval_start: generated_at() - Duration::days(1),
        interval_end: generated_at() - Duration::minutes(1),
        preliminary: false,
        quality: SnapshotQuality::Complete,
        kpis,
        problems,
    }
}

fn kpis() -> KpiSummary {
    KpiSummary {
        ordered_units: 10,
        realized_units: Some(7),
        operational_gmv_minor: 100_000,
        cancelled_units: Some(1),
        returned_units: Some(2),
        ad_impressions: 1_000,
        ad_clicks: 20,
        ad_spend_minor: 10_000,
        attributed_orders: 2,
        attributed_revenue_minor: 50_000,
        ctr: Some(BasisPoints(200)),
        cpc_minor: Some(500),
        ad_conversion: Some(BasisPoints(1_000)),
        cpo_minor: Some(5_000),
        drr: Some(BasisPoints(2_000)),
        buyout_rate: Some(BasisPoints(7_000)),
    }
}

fn problem(sku: u64, kind: ProblemKind, severity: Severity) -> PriorityProblem {
    PriorityProblem {
        account_id: "ozon_store".to_owned(),
        sku,
        kind,
        severity,
        observed: 1,
        threshold: 2,
        impact_minor: 10_000,
    }
}

#[test]
fn workbook_contains_six_bounded_sheets_and_all_supported_rows() {
    let kpis = kpis();
    let problems = [
        problem(1, ProblemKind::AdvertisedWithoutStock, Severity::Red),
        problem(2, ProblemKind::Stockout, Severity::Yellow),
        problem(3, ProblemKind::LowStockCover, Severity::Red),
        problem(4, ProblemKind::SpendWithoutOrders, Severity::Yellow),
        problem(5, ProblemKind::HighDrr, Severity::Red),
    ];
    let sales = [SalesDetail {
        account_id: "ozon_store",
        sku: "=external",
        ordered_units: 10,
        operational_gmv_minor: 100_000,
        cancelled_units: Some(1),
        returned_units: Some(2),
    }];
    let advertising = [AdvertisingDetail {
        account_id: "ozon_store",
        campaign_id: "+campaign",
        sku: "@sku",
        impressions: 1_000,
        clicks: 20,
        spend_minor: 10_000,
        attributed_orders: 2,
        attributed_revenue_minor: 50_000,
    }];
    let inventory = [InventoryDetail {
        account_id: "ozon_store",
        sku: "-sku",
        sellable_stock: 7,
        stock_observed: true,
        price_minor: Some(12_345),
        observed_at: generated_at() - Duration::minutes(5),
    }];
    let quality = [
        SourceQualityDetail {
            account_id: "ozon_store",
            source: SnapshotSource::Sales,
            quality: SnapshotQuality::Complete,
            source_as_of: generated_at() - Duration::minutes(10),
            row_count: 1,
        },
        SourceQualityDetail {
            account_id: "ozon_store",
            source: SnapshotSource::Advertising,
            quality: SnapshotQuality::Partial,
            source_as_of: generated_at() - Duration::minutes(20),
            row_count: 2,
        },
        SourceQualityDetail {
            account_id: "ozon_store",
            source: SnapshotSource::Stocks,
            quality: SnapshotQuality::Stale,
            source_as_of: generated_at() - Duration::minutes(30),
            row_count: 3,
        },
        SourceQualityDetail {
            account_id: "ozon_store",
            source: SnapshotSource::Prices,
            quality: SnapshotQuality::Critical,
            source_as_of: generated_at() - Duration::minutes(40),
            row_count: 4,
        },
    ];
    let mut report_summary = summary(&kpis, &problems);
    report_summary.preliminary = true;
    let bytes = render_xlsx(XlsxReport {
        summary: report_summary,
        sales: &sales,
        advertising: &advertising,
        inventory: &inventory,
        source_quality: &quality,
    })
    .unwrap();
    assert!(bytes.starts_with(b"PK"));
    assert!(bytes.len() < MAX_OUTPUT_BYTES);
    assert_eq!(safe_text("=SUM(A1:A2)"), "'=SUM(A1:A2)");
    assert_eq!(safe_text("normal"), "normal");
}

#[test]
fn malformed_rows_limits_and_internal_errors_fail_closed() {
    let kpis = kpis();
    let base_sales = SalesDetail {
        account_id: "ozon_store",
        sku: "sku",
        ordered_units: 1,
        operational_gmv_minor: 1,
        cancelled_units: Some(0),
        returned_units: Some(0),
    };
    let excessive = vec![base_sales; MAX_ROWS + 1];
    assert_eq!(
        render_xlsx(XlsxReport {
            summary: summary(&kpis, &[]),
            sales: &excessive,
            advertising: &[],
            inventory: &[],
            source_quality: &[],
        }),
        Err(XlsxReportError::InvalidInput)
    );
    let invalid_optional = [SalesDetail {
        cancelled_units: Some(MAX_EXACT_EXCEL_INTEGER + 1),
        ..base_sales
    }];
    assert_eq!(
        render_xlsx(XlsxReport {
            summary: summary(&kpis, &[]),
            sales: &invalid_optional,
            advertising: &[],
            inventory: &[],
            source_quality: &[],
        }),
        Err(XlsxReportError::InvalidInput)
    );

    for invalid in [
        SalesDetail {
            account_id: "bad/account",
            ..base_sales
        },
        SalesDetail {
            sku: " ",
            ..base_sales
        },
        SalesDetail {
            ordered_units: MAX_EXACT_EXCEL_INTEGER + 1,
            ..base_sales
        },
    ] {
        assert_eq!(
            render_xlsx(XlsxReport {
                summary: summary(&kpis, &[]),
                sales: &[invalid],
                advertising: &[],
                inventory: &[],
                source_quality: &[],
            }),
            Err(XlsxReportError::InvalidInput)
        );
    }

    for invalid in [
        AdvertisingDetail {
            account_id: "ozon_store",
            campaign_id: " ",
            sku: "sku",
            impressions: 1,
            clicks: 0,
            spend_minor: 0,
            attributed_orders: 0,
            attributed_revenue_minor: 0,
        },
        AdvertisingDetail {
            account_id: "ozon_store",
            campaign_id: "campaign",
            sku: "sku",
            impressions: 1,
            clicks: 2,
            spend_minor: 0,
            attributed_orders: 0,
            attributed_revenue_minor: 0,
        },
    ] {
        assert_eq!(
            render_xlsx(XlsxReport {
                summary: summary(&kpis, &[]),
                sales: &[],
                advertising: &[invalid],
                inventory: &[],
                source_quality: &[],
            }),
            Err(XlsxReportError::InvalidInput)
        );
    }

    let multiple_units = [AdvertisingDetail {
        account_id: "ozon_store",
        campaign_id: "campaign",
        sku: "sku",
        impressions: 1,
        clicks: 1,
        spend_minor: 100,
        attributed_orders: 2,
        attributed_revenue_minor: 1_000,
    }];
    assert!(
        render_xlsx(XlsxReport {
            summary: summary(&kpis, &[]),
            sales: &[],
            advertising: &multiple_units,
            inventory: &[],
            source_quality: &[],
        })
        .is_ok()
    );

    let future_inventory = InventoryDetail {
        account_id: "ozon_store",
        sku: "sku",
        sellable_stock: 0,
        stock_observed: true,
        price_minor: None,
        observed_at: generated_at() + Duration::seconds(1),
    };
    assert_eq!(
        render_xlsx(XlsxReport {
            summary: summary(&kpis, &[]),
            sales: &[],
            advertising: &[],
            inventory: &[future_inventory],
            source_quality: &[],
        }),
        Err(XlsxReportError::InvalidInput)
    );

    let quality = SourceQualityDetail {
        account_id: "ozon_store",
        source: SnapshotSource::Sales,
        quality: SnapshotQuality::Complete,
        source_as_of: generated_at(),
        row_count: 0,
    };
    for invalid_quality in [
        vec![quality, quality],
        vec![SourceQualityDetail {
            source_as_of: generated_at() + Duration::seconds(1),
            ..quality
        }],
    ] {
        assert_eq!(
            render_xlsx(XlsxReport {
                summary: summary(&kpis, &[]),
                sales: &[],
                advertising: &[],
                inventory: &[],
                source_quality: &invalid_quality,
            }),
            Err(XlsxReportError::InvalidInput)
        );
    }

    assert_eq!(
        validate_output_size(MAX_OUTPUT_BYTES + 1),
        Err(XlsxReportError::OutputTooLarge)
    );
    assert_eq!(validate_output_size(MAX_OUTPUT_BYTES), Ok(()));
    assert_eq!(
        map_xlsx(XlsxError::RowColumnLimitError),
        XlsxReportError::Generation
    );

    let formats = Formats::new();
    let mut workbook = Workbook::new();
    assert_eq!(
        write_sales(&mut workbook, &formats, &[], u32::MAX),
        Err(XlsxReportError::Generation)
    );
    let mut workbook = Workbook::new();
    assert_eq!(
        write_advertising(&mut workbook, &formats, &[], u32::MAX),
        Err(XlsxReportError::Generation)
    );
    let mut workbook = Workbook::new();
    assert_eq!(
        write_inventory(&mut workbook, &formats, &[], u32::MAX),
        Err(XlsxReportError::Generation)
    );
    let mut workbook = Workbook::new();
    assert_eq!(
        write_recommendations(&mut workbook, &formats, &[], u32::MAX),
        Err(XlsxReportError::Generation)
    );
    let mut workbook = Workbook::new();
    assert_eq!(
        write_quality(&mut workbook, &formats, &[], u32::MAX),
        Err(XlsxReportError::Generation)
    );
}
