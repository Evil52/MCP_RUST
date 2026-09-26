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

#[tokio::test]
async fn zero_overlap_requires_matching_independent_day_totals() {
    let day = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
    let mut first = page(
        &(1..=1000)
            .map(|sku| (sku, "2026-08-17"))
            .collect::<Vec<_>>(),
    );
    for row in first["result"]["data"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .skip(1)
    {
        row["metrics"] = json!([0, 0]);
    }
    let mut second = page(&[(2, "2026-08-17"), (1001, "2026-08-17")]);
    second["result"]["data"][0]["metrics"] = json!([0, 0]);
    for (units, expected_ok) in [(4, true), (5, false)] {
        let control = json!({"result":{"data":[{"dimensions":[{"id":"2026-08-17"}],"metrics":["2.50",units]}]}});
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Ok(first.clone()),
            Ok(second.clone()),
            Ok(control),
        ]))));
        let result = source.collect_sales_pages(day, day).await;
        if expected_ok {
            assert_eq!(result.unwrap().len(), 1001);
        } else {
            assert_eq!(result, Err(OzonReportSourceError::SalesPageOverlap));
        }
    }
}

#[test]
fn independent_ozon_totals_reject_wrong_dimensions_dates_and_counts() {
    let day = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
    for (dimensions, metrics) in [
        (json!([{"id":"2026-08-16"}]), json!([1, 1])),
        (json!([{"id":"2026-08-17"},{"id":"1"}]), json!([1, 1])),
        (json!([{"id":"2026-08-17"}]), json!([1, -1])),
    ] {
        assert!(
            parse_sales_control_totals(
                &json!({"result":{"data":[{"dimensions":dimensions,"metrics":metrics}]}}),
                day,
                day
            )
            .is_err()
        );
    }
}
