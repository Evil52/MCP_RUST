use super::*;

fn page(rows: &[(u64, &str)]) -> Value {
    json!({"result":{"data":rows.iter().map(|(sku, date)| json!({
        "dimensions":[{"id":sku.to_string()},{"id":date}],"metrics":["1.25",2]
    })).collect::<Vec<_>>()}})
}

#[tokio::test]
async fn sales_rejects_dates_outside_the_requested_business_period() {
    let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
    for wrong in ["2026-08-16", "2026-08-18"] {
        let source =
            OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([Ok(page(&[
                (1, "2026-08-17"),
                (2, wrong),
            ]))]))));
        assert_eq!(
            source.collect_sales_pages(date, date).await,
            Err(OzonReportSourceError::InvalidSalesResponse {
                shape: "date_outside_requested_period".to_owned()
            })
        );
    }
}

#[tokio::test]
async fn sales_identity_distinguishes_days_but_never_merges_duplicates() {
    let start = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
    let end = start.succ_opt().unwrap();
    let source =
        OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([Ok(page(&[
            (1, "2026-08-16"),
            (1, "2026-08-17"),
        ]))]))));
    assert_eq!(
        source.collect_sales_pages(start, end).await.unwrap().len(),
        2
    );
    let source =
        OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([Ok(page(&[
            (1, "2026-08-16"),
            (1, "2026-08-16"),
        ]))]))));
    assert_eq!(
        source.collect_sales_pages(start, end).await,
        Err(OzonReportSourceError::SalesPageOverlap)
    );
}
