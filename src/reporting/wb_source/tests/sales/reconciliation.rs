use super::*;

fn prefix() -> Value {
    let mut page = sales_page(DATE, 0, 2, 0);
    page["data"]["products"][0]["statistic"]["selected"]["orderSum"] = json!(100);
    page
}
fn history() -> Value {
    json!([{"product":{"nmId":1},"currency":"RUB","history":[{"date":DATE,"orderCount":1,"orderSum":100}]},
        {"product":{"nmId":2},"currency":"RUB","history":[{"date":DATE,"orderCount":1,"orderSum":90}]}])
}
fn fixture(group: u64, last: u64, history: Value) -> SalesFixtureTransport {
    let f = SalesFixtureTransport::new(vec![
        sales_page(DATE, 0, 250, 0),
        sales_page(DATE, 249, 2, 0),
        control_total(2, group),
        control_total(2, last),
    ])
    .with_closing(vec![prefix()]);
    f.history.lock().unwrap().push_back(history);
    f
}

#[tokio::test]
async fn positive_overlap_discards_old_rows_and_independently_certifies_the_fresh_prefix() {
    let date = NaiveDate::parse_from_str(DATE, "%Y-%m-%d").unwrap();
    let f = fixture(190, 190, history());
    let rows = WbReportSource::new(f.clone())
        .collect_sales_pages(date)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.iter().map(|r| r.ordered_units).sum::<u64>(), 2);
    assert!(
        rows.iter()
            .all(|r| r.cancelled_units.is_none() && r.returned_units.is_none())
    );
    assert_eq!(
        f.history.lock().unwrap().len(),
        1,
        "exact totals need no tolerance"
    );
}

#[tokio::test]
async fn one_ruble_both_directions_needs_exact_sku_history_and_unchanged_group_control() {
    let date = NaiveDate::parse_from_str(DATE, "%Y-%m-%d").unwrap();
    for amount in [189, 191] {
        let f = fixture(amount, amount, history());
        let pages = MemoryPages::default();
        for _ in 0..5 {
            let source = WbReportSource::new(f.clone()).with_checkpoints(journal(&pages));
            assert_eq!(
                source.collect_sales_pages(date).await,
                Err(WbReportSourceError::Checkpoint(CheckpointError::Deferred))
            );
        }
        let source = WbReportSource::new(f.clone()).with_checkpoints(journal(&pages));
        let facts = source.collect_sales_pages(date).await.unwrap();
        assert_eq!(
            facts.iter().map(|r| r.operational_gmv_minor).sum::<u64>(),
            19000
        );
        assert!(
            pages
                .lock()
                .unwrap()
                .values()
                .any(|v| v["kind"] == "wb_sales_whole_ruble_v1"
                    && v["sku_gmv_minor"] == 19000
                    && v["group_gmv_minor"] == amount * 100
                    && v["verified_skus"] == 2)
        );
        let replay = WbReportSource::new(f.clone()).with_checkpoints(journal(&pages));
        assert_eq!(replay.collect_sales_pages(date).await.unwrap(), facts);
        assert_eq!(f.requested.lock().unwrap().len(), 6);
    }
}

#[tokio::test]
async fn reconciliation_rejects_larger_gaps_bad_history_or_changing_control() {
    let date = NaiveDate::parse_from_str(DATE, "%Y-%m-%d").unwrap();
    for amount in [188, 192] {
        let f = fixture(amount, amount, history());
        assert_eq!(
            WbReportSource::new(f.clone())
                .collect_sales_pages(date)
                .await,
            Err(WbReportSourceError::SalesPageOverlap)
        );
        assert_eq!(f.history.lock().unwrap().len(), 1);
    }
    for final_amount in [190, 192] {
        assert_eq!(
            WbReportSource::new(fixture(191, final_amount, history()))
                .collect_sales_pages(date)
                .await,
            Err(WbReportSourceError::SalesPageOverlap)
        );
    }
    let mut variants = vec![json!([]), json!([history()[0]])];
    for (pointer, value) in [
        ("/0/product/nmId", json!(999)),
        ("/0/currency", json!("USD")),
        ("/0/history/0/date", json!("2026-08-16")),
        ("/0/history/0/orderCount", json!(2)),
        ("/0/history/0/orderSum", json!(101)),
        ("/1/product/nmId", json!(1)),
    ] {
        let mut bad = history();
        *bad.pointer_mut(pointer).unwrap() = value;
        variants.push(bad);
    }
    for bad in variants {
        assert_eq!(
            WbReportSource::new(fixture(191, 191, bad))
                .collect_sales_pages(date)
                .await,
            Err(WbReportSourceError::SalesPageOverlap)
        );
    }
}

#[tokio::test]
async fn history_transport_uses_requested_date_ids_and_day_aggregation() {
    let (url, requests) = mock_http(vec![(200, history().to_string())]);
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
    transport
        .sales_history(
            NaiveDate::parse_from_str(DATE, "%Y-%m-%d").unwrap(),
            vec![1, 2],
        )
        .await
        .unwrap();
    let request = requests.recv().unwrap();
    assert!(request.starts_with("POST /api/analytics/v3/sales-funnel/products/history HTTP/1.1"));
    let payload: Value = serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(
        payload,
        json!({"selectedPeriod":{"start":DATE,"end":DATE},"nmIds":[1,2],"skipDeletedNm":false,"aggregationLevel":"day"})
    );
}
