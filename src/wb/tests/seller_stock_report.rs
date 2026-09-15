use super::*;
use crate::wb::seller_stock_report::SELLER_STOCK_REPORT_PATH;

#[tokio::test]
async fn seller_report_pages_preserve_zero_and_original_rows() {
    let body = br#"{"data":{"items":[{"nmId":1,"chrtId":2,"warehouseId":3,"quantity":0}]}}"#;
    let (base_url, requests, task) = raw_http(vec![
        raw_response(200, "", body),
        raw_response(204, "x-request-id: empty-seller-page\r\n", b""),
    ]);
    let client = client(&base_url);
    let page = client
        .seller_warehouses_stock_report("account", &[], &[], 1, 0)
        .await
        .unwrap();
    assert_eq!(page.returned_rows, 1);
    assert_eq!(page.next_offset, Some(1));
    assert_eq!(page.data["data"]["items"][0]["quantity"], 0);
    let request = String::from_utf8(requests.recv().unwrap()).unwrap();
    assert!(request.starts_with(&format!("POST {SELLER_STOCK_REPORT_PATH} HTTP/1.1\r\n")));
    let payload: Value = serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(
        payload,
        json!({"nmIds":[],"chrtIds":[],"limit":1,"offset":0})
    );
    let terminal = client
        .seller_warehouses_stock_report("account", &[1], &[2], 1, 1)
        .await
        .unwrap();
    assert_eq!(terminal.next_offset, None);
    assert_eq!(
        terminal.data["meta"]["source_endpoint"],
        SELLER_STOCK_REPORT_PATH
    );
    assert_eq!(terminal.data["meta"]["request_id"], "empty-seller-page");
    assert_eq!(terminal.data["meta"]["upstream_status"], 204);
    requests.recv().unwrap();
    task.join().unwrap();
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn seller_report_rejects_bad_filters_before_network_and_disallows_writes() {
    let client = client("http://127.0.0.1:1");
    for (nm, chrt, limit, offset) in [
        (vec![0], vec![], 10, 0),
        (vec![1, 1], vec![], 10, 0),
        (vec![u64::MAX], vec![], 10, 0),
        (vec![], vec![1], 10, 0),
        (vec![1], vec![2, 2], 10, 0),
        (vec![1], vec![0], 10, 0),
        (vec![], vec![], 0, 0),
        (vec![], vec![], 1_001, 0),
        (vec![], vec![], 10, 1_000_001),
        (vec![1; 1_001], vec![], 10, 0),
    ] {
        assert_eq!(
            client
                .seller_warehouses_stock_report("account", &nm, &chrt, limit, offset)
                .await
                .unwrap_err()
                .kind(),
            WbErrorKind::InvalidArguments
        );
    }
    for method in [Method::GET, Method::PUT, Method::DELETE] {
        assert_eq!(
            client
                .request_for_test("account", method, SELLER_STOCK_REPORT_PATH)
                .await
                .unwrap_err()
                .kind(),
            WbErrorKind::EndpointNotAllowed
        );
    }
    let policy = EndpointPolicy::for_request(&Method::POST, SELLER_STOCK_REPORT_PATH).unwrap();
    assert_eq!(policy.request_class, RequestClass::AnalyticsReport);
    assert_eq!(
        ClientPolicy::production(Duration::from_secs(30)).interval(policy.request_class),
        Duration::from_secs(20)
    );
}

#[tokio::test]
async fn seller_report_rejects_missing_quantities_duplicates_and_foreign_rows() {
    let good = json!({"nmId":1,"chrtId":2,"warehouseId":3,"quantity":7});
    let mut bodies = vec![json!({}), json!({"data":{"items":null}})];
    for quantity in [Value::Null, json!(-1), json!(1.5), json!("7")] {
        let mut row = good.clone();
        row["quantity"] = quantity;
        bodies.push(json!({"data":{"items":[row]}}));
    }
    bodies.push(json!({"data":{"items":[good.clone(),good.clone()]}}));
    let mut foreign = good.clone();
    foreign["nmId"] = json!(9);
    bodies.push(json!({"data":{"items":[foreign]}}));
    let (base_url, requests, task) = raw_http(
        bodies
            .iter()
            .map(|body| raw_response(200, "", body.to_string().as_bytes()))
            .collect(),
    );
    let client = client(&base_url);
    for _ in bodies {
        assert_eq!(
            client
                .seller_warehouses_stock_report("account", &[1], &[2], 10, 0)
                .await
                .unwrap_err()
                .kind(),
            WbErrorKind::InvalidJson
        );
        requests.recv().unwrap();
    }
    task.join().unwrap();
}

#[tokio::test]
async fn seller_report_does_not_accept_empty_200_or_retry_access_denial() {
    let (base_url, requests, task) = raw_http(vec![
        raw_response(200, "", b""),
        raw_response(403, "x-request-id: denied-report\r\n", b"{}"),
    ]);
    let client = client(&base_url);
    for kind in [WbErrorKind::InvalidJson, WbErrorKind::Forbidden] {
        assert_eq!(
            client
                .seller_warehouses_stock_report("account", &[], &[], 10, 0)
                .await
                .unwrap_err()
                .kind(),
            kind
        );
        requests.recv().unwrap();
    }
    task.join().unwrap();
    assert!(requests.try_recv().is_err());
}
