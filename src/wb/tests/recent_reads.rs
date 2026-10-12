use super::super::*;
use crate::test_support::mock_http;
use serde_json::json;
use std::io::{Cursor, Write};

fn client(base: &str) -> WbClient {
    WbClient::new_for_test(
        Duration::from_secs(2),
        BTreeMap::from([(
            "account".into(),
            WbCredentials {
                token: "test-token".into(),
            },
        )]),
        base,
        base,
    )
}

#[tokio::test]
async fn budget_v2_single_adapter_checks_identity_currency_and_unknown_balances() {
    for data in [
        json!({"adverts":null}),
        json!({"adverts":[]}),
        json!({"adverts":[null]}),
        json!({"adverts":[{"advertId":8,"currency":"RUB","total":5}]}),
        json!({"adverts":[{"advertId":7,"currency":"RUB","total":null}]}),
        json!({"adverts":[{"advertId":7,"currency":"USD","total":5}]}),
    ] {
        let (url, requests) = mock_http(vec![(200, data.to_string())]);
        assert!(matches!(
            client(&url)
                .promotion_campaign_budget("account", 7)
                .await
                .unwrap_err(),
            WbError::InvalidJson { .. }
        ));
        let request = requests.recv().unwrap();
        assert!(request.starts_with("POST /api/advert/v2/budget "));
        let body: Value = serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(body, json!({"advertIds":[7]}));
    }
    let (url, _) = mock_http(vec![(
        200,
        json!({"adverts":[{"advertId":7,"currency":"RUB","total":0}]}).to_string(),
    )]);
    assert_eq!(
        client(&url)
            .promotion_campaign_budget("account", 7)
            .await
            .unwrap()["total"],
        0
    );
}

#[tokio::test]
async fn recent_read_routes_and_terminal_errors_do_not_replay() {
    let (url, requests) = mock_http(vec![
        (200, "{}".into()),
        (200, "{}".into()),
        (200, "{}".into()),
    ]);
    let client = client(&url);
    client
        .promotion_campaign_budgets("account", &[4, 9])
        .await
        .unwrap();
    client
        .promotion_daily_limits("account", &[4, 9])
        .await
        .unwrap();
    client.supply_discrepancies("account", 42).await.unwrap();
    assert!(
        requests
            .recv()
            .unwrap()
            .starts_with("POST /api/advert/v2/budget ")
    );
    assert!(
        requests
            .recv()
            .unwrap()
            .starts_with("GET /api/advert/v0/daily-limits?advertIds=4%2C9 ")
    );
    assert!(
        requests
            .recv()
            .unwrap()
            .starts_with("GET /api/supplies/v1/discrepancies/42 ")
    );
    for status in [401, 403, 429, 503] {
        let (url, requests) = mock_http(vec![(status, "{}".into())]);
        assert!(
            self::client(&url)
                .promotion_daily_limits("account", &[4])
                .await
                .is_err()
        );
        requests.recv().unwrap();
        assert!(requests.try_recv().is_err());
    }
}

#[test]
fn recent_read_policy_rejects_mutations_and_path_injection() {
    for (method, path) in [
        (Method::GET, "/adv/v1/budget"),
        (Method::GET, recent_policy::BUDGETS),
        (Method::PUT, recent_policy::DAILY_LIMITS),
        (Method::POST, "/api/supplies/v1/discrepancies/42"),
        (Method::GET, "/api/supplies/v1/discrepancies/042"),
        (Method::GET, "/api/supplies/v1/discrepancies/42/../1"),
        (Method::GET, "/api/v2/nm-report/downloads/file/../x"),
    ] {
        assert!(
            EndpointPolicy::for_request(&method, path).is_none(),
            "{method} {path}"
        );
    }
}

fn quota_token(claims: &Value) -> String {
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        claims,
        &jsonwebtoken::EncodingKey::from_secret(b"synthetic-test-secret"),
    )
    .unwrap()
}

#[test]
fn recent_quotas_classify_token_type_category_and_expiration() {
    let policy = ClientPolicy::production(Duration::from_secs(2));
    let claims =
        json!({"acc":3,"for":"self","t":false,"s":(1<<2)|(1<<6),"exp":Utc::now().timestamp()+3600});
    for (class, expected) in [
        (RequestClass::PromotionBudgets, Duration::from_secs(3)),
        (RequestClass::OrderFeed, Duration::from_mins(1)),
        (RequestClass::AnalyticsDownloads, Duration::from_secs(20)),
        (
            RequestClass::AnalyticsDownloadStatus,
            Duration::from_secs(20),
        ),
        (RequestClass::AnalyticsDownloadFile, Duration::from_secs(20)),
    ] {
        assert_eq!(
            policy.recent_interval(class, &quota_token(&claims)),
            expected
        );
        for patch in [
            json!({"acc":1}),
            json!({"acc":2,"t":true}),
            json!({"t":true}),
            json!({"s":0}),
            json!({"exp":Utc::now().timestamp()-1}),
            json!({"for":"unknown"}),
            json!({"acc":4,"for":"asid:unknown"}),
            json!({"acc":4,"for":"asid:11111111-1111-4111-8111-111111111111"}),
        ] {
            let mut invalid = claims.clone();
            for (key, value) in patch.as_object().unwrap() {
                invalid[key] = value.clone();
            }
            assert_eq!(
                policy.recent_interval(class, &quota_token(&invalid)),
                policy.interval(class)
            );
        }
        assert_eq!(
            policy.recent_interval(class, "invalid-token"),
            policy.interval(class)
        );
    }
}

#[tokio::test]
async fn personal_budget_quota_waits_for_a_short_slot_without_blocking_launch() {
    let (url, requests) = mock_http(vec![(200, "{}".into()); 2]);
    let token = quota_token(
        &json!({"acc":3,"for":"self","t":false,"s":1<<6,"exp":Utc::now().timestamp()+3600}),
    );
    let client = WbClient::build(
        Duration::from_secs(2),
        BTreeMap::from([("account".into(), WbCredentials { token })]),
        BaseUrls::for_test(&url, &url),
        ClientPolicy::production(Duration::from_secs(2)),
    );
    client
        .promotion_campaign_budgets("account", &[7])
        .await
        .unwrap();
    let gate = client.limiters["account"].gate(RequestClass::PromotionBudgets);
    let ready_in = gate.ready_in().await;
    assert!(ready_in > Duration::from_secs(2) && ready_in <= Duration::from_secs(3));
    // Simulate the end of the real slot, then exercise sequential launch reads.
    *gate.next_allowed.lock().await = Instant::now() + Duration::from_millis(20);
    client
        .promotion_campaign_budgets("account", &[7])
        .await
        .unwrap();
    requests.recv().unwrap();
    *gate.next_allowed.lock().await = Instant::now() + Duration::from_mins(1);
    let blocked = tokio::time::timeout(
        Duration::from_millis(200),
        client.promotion_campaign_budgets("account", &[7]),
    )
    .await
    .unwrap();
    assert!(
        matches!(blocked, Err(WbError::LocalRateLimited { retry_after }) if retry_after > Duration::from_secs(59))
    );
    requests.recv().unwrap();
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn long_quotas_fail_fast_and_do_not_block_distinct_report_methods() {
    let (url, requests) = mock_http(vec![(200, "{}".into()); 3]);
    let mut client = client(&url);
    client.policy.analytics_downloads_interval = Duration::from_hours(1);
    let today = Utc::now().date_naive().to_string();
    let query: recent_queries::StockHistoryQuery=serde_json::from_value(json!({"download_id":"11111111-1111-4111-8111-111111111111","date_from":today,"date_to":today})).unwrap();
    client
        .stock_history_create("account", &query)
        .await
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_millis(200),
        client.stock_history_create("account", &query),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(WbError::LocalRateLimited { .. })));
    client
        .analytics_report_status("account", &query.download_id)
        .await
        .unwrap();
    client
        .promotion_daily_limits("account", &[4])
        .await
        .unwrap();
    for _ in 0..3 {
        requests.recv().unwrap();
    }
    assert!(requests.try_recv().is_err());
}

fn archive(name: &str, body: &[u8]) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    writer
        .start_file(name, zip::write::SimpleFileOptions::default())
        .unwrap();
    writer.write_all(body).unwrap();
    writer.finish().unwrap().into_inner()
}

#[tokio::test]
async fn report_download_decodes_binary_zip_from_the_http_response() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let csv = "date,nmId,stock\n2026-10-11,123,7\n";
    let bytes = archive("stocks.csv", csv.as_bytes());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = crate::test_support::read_request(&stream);
        assert!(request.starts_with(
            "GET /api/v2/nm-report/downloads/file/11111111-1111-4111-8111-111111111111 "
        ));
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/zip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            bytes.len()
        )
        .unwrap();
        stream.write_all(&bytes).unwrap();
    });
    let result = client(&url)
        .analytics_report_download("account", "11111111-1111-4111-8111-111111111111")
        .await
        .unwrap();
    assert_eq!(result["files"][0]["csv"], csv);
    assert_eq!(result["truncated"], false);
    server.join().unwrap();
}

#[test]
fn csv_zip_preserves_history_dates_and_rejects_oversize_or_invalid_files() {
    let csv = "date,nmId,stock\n2026-10-11,123,7\n";
    let result = report_archive::decode(archive("stocks.csv", csv.as_bytes()), None).unwrap();
    assert_eq!(result["files"][0]["csv"], csv);
    assert_eq!(result["truncated"], false);
    for bytes in [
        b"not a zip".to_vec(),
        archive("../stocks.csv", b"x"),
        archive("x.exe", b"x"),
        archive("stocks.csv", &[0xff]),
        archive("stocks.csv", &vec![b'x'; 2 * 1024 * 1024 + 1]),
    ] {
        assert!(report_archive::decode(bytes, None).is_err());
    }
}

#[tokio::test]
async fn invalid_recent_arguments_never_reach_network() {
    let client = client("http://127.0.0.1:1");
    for ids in [vec![], vec![0], vec![1, 1], (1..=51).collect()] {
        assert!(matches!(
            client.promotion_campaign_budgets("account", &ids).await,
            Err(WbError::InvalidArguments { .. })
        ));
    }
    assert!(
        client
            .analytics_report_download("account", "../x")
            .await
            .is_err()
    );
    let now = Utc::now().to_rfc3339();
    for patch in [
        json!({"offset":1}),
        json!({"limit":10001}),
        json!({"nm_ids":[0]}),
        json!({"subject_ids":[1,1]}),
        json!({"date_from":(Utc::now()-chrono::Duration::days(32)).to_rfc3339()}),
        json!({"date_from":"2026-10-11"}),
    ] {
        let mut data = json!({"date_from":now,"date_to":now});
        for (k, v) in patch.as_object().unwrap() {
            data[k] = v.clone();
        }
        let q: recent_queries::OrderFeedQuery = serde_json::from_value(data).unwrap();
        assert!(matches!(
            client.order_feed("account", &q).await,
            Err(WbError::InvalidArguments { .. })
        ));
    }
}
