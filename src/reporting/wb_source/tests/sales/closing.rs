use super::*;

fn zeroed(mut page: Value) -> Value {
    for row in page["data"]["products"].as_array_mut().unwrap() {
        row["statistic"]["selected"]["orderCount"] = json!(0);
        row["statistic"]["selected"]["orderSum"] = json!(0);
    }
    page
}

fn initial_chain() -> Vec<Value> {
    let mut first = zeroed(sales_page(DATE, 0, 250, 0));
    first["data"]["products"][0]["statistic"]["selected"]["orderCount"] = json!(1);
    first["data"]["products"][0]["statistic"]["selected"]["orderSum"] = json!(90);
    let mut second = sales_page(DATE, 250, 1, 0);
    second["data"]["products"]
        .as_array_mut()
        .unwrap()
        .push(first["data"]["products"][1].clone());
    vec![first, second, control_total(4, 360)]
}

fn closing_page() -> Value {
    let mut page = zeroed(sales_page(DATE, 0, 250, 0));
    let rows = page["data"]["products"].as_array_mut().unwrap();
    rows[0]["statistic"]["selected"]["orderCount"] = json!(3);
    rows[0]["statistic"]["selected"]["orderSum"] = json!(270);
    rows[1]["product"]["nmId"] = json!(999);
    rows[1]["statistic"]["selected"]["orderCount"] = json!(1);
    rows[1]["statistic"]["selected"]["orderSum"] = json!(90);
    page
}

#[tokio::test]
async fn closing_pass_resumes_each_read_and_only_publishes_fresh_matching_totals() {
    let date = NaiveDate::parse_from_str(DATE, "%Y-%m-%d").unwrap();
    for final_units in [4, 5] {
        let mut inputs = initial_chain();
        inputs.push(control_total(final_units, 360));
        let fixture = SalesFixtureTransport::new(inputs).with_closing(vec![closing_page()]);
        let pages = MemoryPages::default();
        for _ in 0..4 {
            let source = WbReportSource::new(fixture.clone()).with_checkpoints(journal(&pages));
            assert_eq!(
                source.collect_sales_pages(date).await,
                Err(WbReportSourceError::Checkpoint(CheckpointError::Deferred))
            );
        }
        let source = WbReportSource::new(fixture.clone()).with_checkpoints(journal(&pages));
        let result = source.collect_sales_pages(date).await;
        if final_units == 4 {
            let facts = result.unwrap();
            assert_eq!(facts.iter().map(|r| r.ordered_units).sum::<u64>(), 4);
            assert_eq!(
                facts.iter().map(|r| r.operational_gmv_minor).sum::<u64>(),
                36_000
            );
            assert_eq!(facts.iter().find(|r| r.sku == 1).unwrap().ordered_units, 3);
            assert_eq!(
                facts.iter().find(|r| r.sku == 999).unwrap().ordered_units,
                1
            );
            assert!(facts.iter().all(|r| r.sku != 251));
            assert!(
                facts
                    .iter()
                    .all(|r| r.cancelled_units.is_none() && r.returned_units.is_none())
            );
            let replay = WbReportSource::new(fixture.clone()).with_checkpoints(journal(&pages));
            assert_eq!(replay.collect_sales_pages(date).await.unwrap(), facts);
        } else {
            assert_eq!(result, Err(WbReportSourceError::SalesPageOverlap));
        }
        assert_eq!(
            *fixture.requested.lock().unwrap(),
            vec![(250, 0), (250, 250), (0, 0), (u32::MAX, 0), (0, 0)]
        );
    }
}

#[tokio::test]
async fn closing_pages_validate_sort_scope_overlap_and_positive_prefix_bound() {
    let date = NaiveDate::parse_from_str(DATE, "%Y-%m-%d").unwrap();
    let mut unsorted = closing_page();
    unsorted["data"]["products"]
        .as_array_mut()
        .unwrap()
        .swap(0, 1);
    for invalid in [
        unsorted,
        sales_page("2026-08-16", 0, 1, 0),
        sales_page(DATE, 0, 251, 0),
    ] {
        let source =
            WbReportSource::new(SalesFixtureTransport::new(vec![]).with_closing(vec![invalid]));
        assert_eq!(
            source.collect_closing_sales_pages(date, 1).await,
            Err(WbReportSourceError::InvalidSalesResponse)
        );
    }
    let full = sales_page(DATE, 0, 250, 0);
    let source =
        WbReportSource::new(SalesFixtureTransport::new(vec![]).with_closing(vec![full.clone()]));
    assert_eq!(
        source.collect_closing_sales_pages(date, 1).await,
        Err(WbReportSourceError::PaginationLimit)
    );
    let source = WbReportSource::new(
        SalesFixtureTransport::new(vec![]).with_closing(vec![full, sales_page(DATE, 249, 2, 0)]),
    );
    assert_eq!(
        source.collect_closing_sales_pages(date, 2).await,
        Err(WbReportSourceError::SalesPageOverlap)
    );
    let source = WbReportSource::new(
        SalesFixtureTransport::new(vec![]).with_closing(vec![sales_page(DATE, 0, 0, 0)]),
    );
    assert!(
        source
            .collect_closing_sales_pages(date, 1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn closing_transport_requests_documented_order_count_sort_without_filters() {
    let (url, requests) = mock_http(vec![(200, closing_page().to_string())]);
    let client = WbClient::new_for_test(
        Duration::from_secs(2),
        BTreeMap::from([(
            "account".to_owned(),
            WbCredentials {
                token: "test-token".to_owned(),
            },
        )]),
        &url,
        &url,
    );
    let transport = WbClientReportTransport::new(client, "account".to_owned());
    let date = NaiveDate::parse_from_str(DATE, "%Y-%m-%d").unwrap();
    transport.sales_closing_page(date, 250, 0).await.unwrap();
    let request = requests.recv().unwrap();
    assert!(request.starts_with("POST /api/analytics/v3/sales-funnel/products HTTP/1.1"));
    let (_, body) = request.split_once("\r\n\r\n").unwrap();
    let payload: Value = serde_json::from_str(body).unwrap();
    assert_eq!(
        payload,
        json!({"selectedPeriod":{"start":DATE,"end":DATE},"nmIds":[],"brandNames":[],"subjectIds":[],"tagIds":[],"skipDeletedNm":false,"limit":250,"offset":0,"orderBy":{"field":"orderCount","mode":"desc"}})
    );
}
