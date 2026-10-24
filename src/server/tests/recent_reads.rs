use super::*;
use crate::server::tools::recent_reads::*;

fn new_tools() -> Vec<(&'static str, Value)> {
    let today = Utc::now().date_naive().to_string();
    let now = Utc::now().to_rfc3339();
    vec![
        (
            "wb_order_feed",
            json!({"query":{"date_from":now,"date_to":now}}),
        ),
        ("wb_supply_discrepancies", json!({"supply_id":1})),
        (
            "wb_item_returns",
            json!({"query":{"date_from":today,"date_to":today}}),
        ),
        ("wb_promotion_budgets", json!({"advert_ids":[1]})),
        ("wb_promotion_daily_limits", json!({"advert_ids":[1]})),
        (
            "wb_stock_history_create",
            json!({"query":{"download_id":"11111111-1111-4111-8111-111111111111","date_from":today,"date_to":today}}),
        ),
        (
            "wb_analytics_report_status",
            json!({"download_id":"11111111-1111-4111-8111-111111111111"}),
        ),
        (
            "wb_analytics_report_download",
            json!({"download_id":"11111111-1111-4111-8111-111111111111"}),
        ),
    ]
}

#[tokio::test]
async fn recent_tools_authorize_account_and_reject_arbitrary_arguments() {
    let (server, requests) = mock_wb_server_for("manager", 0);
    for (name, mut args) in new_tools() {
        args["account"] = json!("account_wb");
        let result = call_tool_over_http(server.clone(), name, args.clone()).await;
        assert!(result.contains(ACCESS_DENIED), "{name}: {result}");
        args["url"] = json!("http://127.0.0.1/private");
        let result = call_tool_over_http(server.clone(), name, args).await;
        let envelope: Value = serde_json::from_str(&result).unwrap();
        assert!(
            envelope.get("error").is_some() || envelope["result"]["isError"] == true,
            "{name}: {result}"
        );
    }
    assert!(requests.try_recv().is_err());
}

#[test]
fn recent_tools_are_excluded_from_prepared_reporting_mode() {
    let tools = server()
        .into_reporting_only()
        .unwrap()
        .tool_router
        .list_all();
    for (name, _) in new_tools() {
        assert!(!tools.iter().any(|t| t.name == name), "{name}");
    }
}

#[tokio::test]
async fn new_returns_route_preserves_204_and_archive_filter() {
    let (server, requests) = mock_wb_server_with_responses("admin", vec![(204, String::new())]);
    let input = serde_json::from_value(json!({"account":"account_wb","query":{"date_from":"2025-01-01","date_to":"2026-10-01","status":"archive","limit":10,"offset":20}})).unwrap();
    let result = server
        .wb_item_returns(RequestIdentity::dev(), Parameters(input))
        .await
        .unwrap();
    assert_eq!(result.0.data["meta"]["upstream_status"], 204);
    assert_eq!(result.0.data["meta"]["data_state"], "no_data");
    let request = requests.recv().unwrap();
    assert!(request.starts_with("GET /api/analytics/v1/item-returns?"));
    assert!(request.contains("status=archive"));
    assert!(request.contains("offset=20"));
}

#[tokio::test]
async fn order_feed_preserves_snapshot_and_distinct_current_statuses() {
    let now = Utc::now().to_rfc3339();
    let upstream = json!({"data":{"snapshotTime":now,"currency":"RUB","orders":[{"srid":"o1","status":"buyout"},{"srid":"o2","status":"cancel","cancelType":"expire"},{"srid":"o3","status":"returnDefective"}]}});
    let (server, requests) =
        mock_wb_server_with_responses("admin", vec![(200, upstream.to_string())]);
    let input: WbOrderFeedInput = serde_json::from_value(json!({"account":"account_wb","query":{"date_from":now,"date_to":now,"snapshot_time":now,"offset":10,"limit":20,"nm_ids":[123]}})).unwrap();
    let result = server
        .wb_order_feed(RequestIdentity::dev(), Parameters(input))
        .await
        .unwrap();
    let mut expected = upstream;
    for row in expected["data"]["orders"].as_array_mut().unwrap() {
        row["srid"] = json!(REDACTED_VALUE);
    }
    assert_eq!(result.0.data, expected);
    let request = requests.recv().unwrap();
    assert!(request.starts_with("POST /api/analytics/v1/order-feed "));
    let body: Value = serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(
        body["pagination"],
        json!({"snapshotTime":now,"offset":10,"limit":20})
    );
    assert_eq!(body["nmIds"], json!([123]));
    assert_eq!(body["selectedPeriod"], json!({"start":now,"end":now}));
}

#[tokio::test]
async fn stock_history_uses_daily_csv_and_explicit_history_date() {
    let yesterday = Utc::now().date_naive().pred_opt().unwrap().to_string();
    let (server, requests) = mock_wb_server_for("admin", 1);
    let input: WbStockHistoryInput = serde_json::from_value(json!({"account":"account_wb","query":{"download_id":"11111111-1111-4111-8111-111111111111","date_from":yesterday,"date_to":yesterday,"stock_type":"wb","nm_ids":[123]}})).unwrap();
    server
        .wb_stock_history_create(RequestIdentity::dev(), Parameters(input))
        .await
        .unwrap();
    let request = requests.recv().unwrap();
    let body: Value = serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(body["reportType"], "STOCK_HISTORY_DAILY_CSV");
    assert_eq!(
        body["params"]["currentPeriod"],
        json!({"start":yesterday,"end":yesterday})
    );
    assert_eq!(body["params"]["stockType"], "wb");
    assert!(body.get("account").is_none());
}

#[tokio::test]
async fn fbo_limit_one_uses_current_cursor_contract_without_financial_fields() {
    let (server, requests) = mock_server(1);
    let input: PostingListInput = serde_json::from_value(
        json!({"store":"store_a","date_from":"2026-10-11","date_to":"2026-10-11","limit":1}),
    )
    .unwrap();
    server
        .fbo_postings(RequestIdentity::dev(), Parameters(input))
        .await
        .unwrap();
    let request = requests.recv().unwrap();
    assert!(request.starts_with("POST /v3/posting/fbo/list "));
    let body: Value = serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(body["limit"], 1);
    assert_eq!(body["filter"]["statuses"], json!([]));
    assert_eq!(body["with"]["financial_data"], false);
    assert!(body.get("offset").is_none());
    assert!(body.get("cursor").is_none());
}

#[test]
fn search_descriptions_and_schema_state_two_hour_refresh() {
    let tools = server().tool_router.list_all();
    for name in ["wb_search_product_queries", "wb_search_orders_positions"] {
        let tool = tools.iter().find(|t| t.name == name).unwrap();
        assert!(
            tool.description
                .as_deref()
                .unwrap()
                .contains("раз в два часа")
        );
    }
}

const DOWNLOAD_ID: &str = "11111111-1111-4111-8111-111111111111";

/// Arguments that pass local validation, so each call reaches WB.
fn wired_tools() -> Vec<(&'static str, Value, &'static str)> {
    let now = Utc::now().to_rfc3339();
    let yesterday = Utc::now().date_naive().pred_opt().unwrap().to_string();
    vec![
        (
            "wb_order_feed",
            json!({"query":{"date_from":now,"date_to":now}}),
            "analytics:/api/analytics/v1/order-feed",
        ),
        (
            "wb_supply_discrepancies",
            json!({"supply_id":1}),
            "supplies:/api/supplies/v1/discrepancies/{supplyId}",
        ),
        (
            "wb_item_returns",
            json!({"query":{"date_from":"2025-01-01","date_to":"2026-10-01"}}),
            "analytics:/api/analytics/v1/item-returns",
        ),
        (
            "wb_promotion_budgets",
            json!({"advert_ids":[1]}),
            "promotion:/api/advert/v2/budget",
        ),
        (
            "wb_promotion_daily_limits",
            json!({"advert_ids":[1]}),
            "promotion:/api/advert/v0/daily-limits",
        ),
        (
            "wb_stock_history_create",
            json!({"query":{"download_id":DOWNLOAD_ID,"date_from":yesterday,"date_to":yesterday}}),
            "analytics:/api/v2/nm-report/downloads",
        ),
        (
            "wb_analytics_report_status",
            json!({"download_id":DOWNLOAD_ID}),
            "analytics:/api/v2/nm-report/downloads",
        ),
        (
            "wb_analytics_report_download",
            json!({"download_id":DOWNLOAD_ID}),
            crate::wb::recent_policy::DOWNLOAD_LABEL,
        ),
    ]
}

#[tokio::test]
async fn recent_upstream_failures_name_the_wb_endpoint_that_failed() {
    for (name, mut args, label) in wired_tools() {
        let (server, requests) =
            mock_wb_server_with_responses("admin", vec![(500, "{}".to_owned())]);
        args["account"] = json!("account_wb");
        let text = call_tool_over_http(server, name, args).await;
        let envelope: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(envelope["result"]["isError"], true, "{name}: {text}");
        assert!(text.contains(WB_TOOL_FAILURE), "{name}: {text}");
        assert!(
            text.contains(&format!("endpoint={label}")),
            "{name}: {text}"
        );
        requests.recv_timeout(Duration::from_secs(2)).unwrap();
    }
}

#[tokio::test]
async fn supply_discrepancies_read_the_requested_supply_only() {
    let body = json!({"discrepancies":[{"nmId":3,"planQuantity":2,"factQuantity":1}]});
    let (server, requests) = mock_wb_server_with_responses("admin", vec![(200, body.to_string())]);
    let result = server
        .wb_supply_discrepancies(
            RequestIdentity::dev(),
            Parameters(
                serde_json::from_value(json!({"account":"account_wb","supply_id":7})).unwrap(),
            ),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(
        result.endpoint,
        "supplies:/api/supplies/v1/discrepancies/{supplyId}"
    );
    assert_eq!(result.data, body);
    assert!(
        requests
            .recv()
            .unwrap()
            .starts_with("GET /api/supplies/v1/discrepancies/7 ")
    );
}

#[tokio::test]
async fn promotion_budgets_and_daily_limits_send_every_requested_campaign() {
    let budgets = json!({"adverts":[{"advertId":3,"currency":"RUB","total":10}]});
    let (server, requests) =
        mock_wb_server_with_responses("admin", vec![(200, budgets.to_string())]);
    let result = server
        .wb_promotion_budgets(
            RequestIdentity::dev(),
            Parameters(
                serde_json::from_value(json!({"account":"account_wb","advert_ids":[3,4]})).unwrap(),
            ),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(result.data, budgets);
    let request = requests.recv().unwrap();
    assert!(request.starts_with("POST /api/advert/v2/budget "));
    let body: Value = serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(body, json!({"advertIds":[3,4]}));

    let limits = json!({"limits":[{"advertId":3,"dailyLimit":500}]});
    let (server, requests) =
        mock_wb_server_with_responses("admin", vec![(200, limits.to_string())]);
    let result = server
        .wb_promotion_daily_limits(
            RequestIdentity::dev(),
            Parameters(
                serde_json::from_value(json!({"account":"account_wb","advert_ids":[3,4]})).unwrap(),
            ),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(result.endpoint, "promotion:/api/advert/v0/daily-limits");
    assert_eq!(result.data, limits);
    let request = requests.recv().unwrap();
    assert!(
        request.starts_with("GET /api/advert/v0/daily-limits?advertIds=3%2C4 "),
        "{request}"
    );
}

#[tokio::test]
async fn report_status_filters_by_the_requested_download_id() {
    let status = json!({"data":[{"id":DOWNLOAD_ID,"status":"SUCCESS"}]});
    let (server, requests) =
        mock_wb_server_with_responses("admin", vec![(200, status.to_string())]);
    let result = server
        .wb_analytics_report_status(
            RequestIdentity::dev(),
            Parameters(
                serde_json::from_value(json!({"account":"account_wb","download_id":DOWNLOAD_ID}))
                    .unwrap(),
            ),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(result.endpoint, "analytics:/api/v2/nm-report/downloads");
    assert_eq!(result.data, status);
    let request = requests.recv().unwrap();
    assert!(
        request.starts_with("GET /api/v2/nm-report/downloads?"),
        "{request}"
    );
    assert!(request.contains(DOWNLOAD_ID), "{request}");
}

/// Serves one binary ZIP response, which the JSON mock cannot express.
fn zip_wb_server(csv: &str) -> (OzonMcp, std::thread::JoinHandle<String>) {
    use std::io::Write as _;
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file("stocks.csv", zip::write::SimpleFileOptions::default())
        .unwrap();
    writer.write_all(csv.as_bytes()).unwrap();
    let bytes = writer.finish().unwrap().into_inner();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let upstream = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = crate::test_support::read_request(&stream);
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/zip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            bytes.len()
        )
        .unwrap();
        stream.write_all(&bytes).unwrap();
        request
    });
    let wb_client = WbClient::new_for_test(
        Duration::from_secs(3),
        BTreeMap::from([(
            "account_wb".to_owned(),
            crate::wb::WbCredentials {
                token: "test-wb-token".to_owned(),
            },
        )]),
        &base_url,
        &base_url,
    );
    let ozon_client = OzonClient::new(
        "http://127.0.0.1:1".to_owned(),
        Duration::from_secs(1),
        BTreeMap::new(),
    )
    .unwrap();
    let server = OzonMcp::new(ozon_client, "admin".to_owned(), registry_source())
        .with_wildberries_client(wb_client);
    (server, upstream)
}

#[tokio::test]
async fn report_download_tool_returns_decoded_csv_files() {
    let csv = "date,nmId,stock\n2026-10-11,123,7\n";
    let (server, upstream) = zip_wb_server(csv);
    let result = server
        .wb_analytics_report_download(
            RequestIdentity::dev(),
            Parameters(
                serde_json::from_value(json!({"account":"account_wb","download_id":DOWNLOAD_ID}))
                    .unwrap(),
            ),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(result.endpoint, crate::wb::recent_policy::DOWNLOAD_LABEL);
    assert_eq!(result.data["files"][0]["csv"], csv);
    assert_eq!(result.data["truncated"], false);
    let request = upstream.join().unwrap();
    assert!(
        request.starts_with(&format!(
            "GET /api/v2/nm-report/downloads/file/{DOWNLOAD_ID} "
        )),
        "{request}"
    );
}
