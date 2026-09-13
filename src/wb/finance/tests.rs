use super::*;
use crate::{
    test_support::mock_http,
    wb::{BaseUrls, ClientPolicy, RequestClass, WbCredentials, WbErrorKind},
};
use std::{collections::BTreeMap, time::Duration};

fn date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, 10).unwrap()
}

fn client(base: &str) -> WbClient {
    let mut urls = BaseUrls::for_test("http://127.0.0.1:1", "http://127.0.0.1:1");
    urls.finance = base.to_owned();
    WbClient::build(
        Duration::from_secs(2),
        BTreeMap::from([(
            "account".to_owned(),
            WbCredentials {
                token: "test-finance-token".to_owned(),
            },
        )]),
        urls,
        ClientPolicy::immediate_single_attempt(Duration::from_secs(2)),
    )
}

#[tokio::test]
async fn finance_preserves_http_terminal_proof_and_exact_decimal_strings() {
    let (base, requests) = mock_http(vec![
        (
            200,
            "[{\"rrdId\":7,\"forPay\":\"123456789012345.678\"}]".to_owned(),
        ),
        (200, "null".to_owned()),
        (200, "[]".to_owned()),
        (204, String::new()),
    ]);
    let client = client(&base);
    let first = client
        .financial_report_page("account", date(), date(), 250, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first[0]["forPay"].as_str().unwrap(), "123456789012345.678");
    assert_eq!(
        client
            .financial_report_page("account", date(), date(), 250, 7)
            .await
            .unwrap(),
        Some(Value::Null)
    );
    assert_eq!(
        client
            .financial_report_page("account", date(), date(), 250, 7)
            .await
            .unwrap(),
        Some(json!([]))
    );
    assert_eq!(
        client
            .financial_report_page("account", date(), date(), 250, 7)
            .await
            .unwrap(),
        None
    );
    for cursor in [0, 7, 7, 7] {
        let request = requests.recv().unwrap();
        assert!(request.starts_with("POST /api/finance/v1/sales-reports/detailed HTTP/1.1\r\n"));
        let body: Value = serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(
            body,
            json!({"dateFrom":"2026-09-10", "dateTo":"2026-09-10", "limit":250, "rrdId":cursor, "period":"daily", "fields":WB_FINANCE_FIELDS})
        );
    }
}

#[tokio::test]
async fn finance_rejects_invalid_scope_before_credentials_or_network() {
    let client = client("http://127.0.0.1:1");
    for (start, end, limit, cursor) in [
        (date(), date(), 0, 0),
        (date(), date(), 1_001, 0),
        (date(), date(), 1, u64::MAX),
        (date(), date().pred_opt().unwrap(), 1, 0),
        (NaiveDate::from_ymd_opt(2024, 1, 28).unwrap(), date(), 1, 0),
        (NaiveDate::from_ymd_opt(2026, 8, 1).unwrap(), date(), 1, 0),
    ] {
        assert!(matches!(
            client
                .financial_report_page("missing", start, end, limit, cursor)
                .await,
            Err(WbError::InvalidArguments { .. })
        ));
    }
    for (method, path) in [
        (Method::GET, FINANCE_DETAILS_PATH),
        (Method::PUT, FINANCE_DETAILS_PATH),
        (Method::DELETE, FINANCE_DETAILS_PATH),
        (Method::POST, "/api/finance/v1/sales-reports/detailed/"),
        (
            Method::POST,
            "/api/finance/v1/sales-reports/detailed?url=http://127.0.0.1",
        ),
        (Method::GET, "/api/finance/v1/sales-reports/list"),
        (Method::POST, "/adv/v1/budget/deposit"),
    ] {
        assert!(matches!(
            client
                .request_document("missing", method, path, None, None)
                .await,
            Err(WbError::EndpointNotAllowed { .. })
        ));
    }
}

#[tokio::test]
async fn finance_never_falls_back_or_retries_vendor_failures() {
    for (status, expected) in [
        (401, WbErrorKind::Unauthorized),
        (403, WbErrorKind::Forbidden),
        (402, WbErrorKind::SubscriptionRequired),
        (429, WbErrorKind::RateLimited),
        (503, WbErrorKind::Http),
    ] {
        let (base, requests) = mock_http(vec![(
            status,
            "{\"error\":\"private upstream diagnostic\"}".to_owned(),
        )]);
        let client = client(&base);
        let error = client
            .financial_report_page("account", date(), date(), 1, 0)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), expected);
        assert!(!format!("{error:?}").contains("private upstream diagnostic"));
        assert!(!format!("{error:?}").contains("test-finance-token"));
        assert!(
            requests
                .recv()
                .unwrap()
                .starts_with("POST /api/finance/v1/sales-reports/detailed ")
        );
        assert!(requests.try_recv().is_err());
    }
    let policy = ClientPolicy::production(Duration::from_secs(10));
    assert_eq!(
        policy.interval(RequestClass::FinanceReport),
        Duration::from_hours(12)
    );
    assert!(!RequestClass::FinanceReport.allows_automatic_retry());
    assert_eq!(
        BaseUrls::production().finance,
        "https://finance-api.wildberries.ru"
    );
}

#[tokio::test]
async fn finance_shares_long_vendor_cooldown_across_token_aliases() {
    use std::io::Write;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _request = crate::test_support::read_request(&stream);
        stream.write_all(b"HTTP/1.1 429 Error\r\nRetry-After: 86400\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").unwrap();
    });
    let credential = WbCredentials {
        token: "shared-finance-token".to_owned(),
    };
    let client = WbClient::build(
        Duration::from_secs(2),
        BTreeMap::from([
            ("account".to_owned(), credential.clone()),
            ("alias".to_owned(), credential),
        ]),
        BaseUrls::for_test(&base, &base),
        ClientPolicy::immediate_single_attempt(Duration::from_secs(2)),
    );
    assert!(std::sync::Arc::ptr_eq(
        &client.limiters["account"],
        &client.limiters["alias"]
    ));
    assert!(
        matches!(client.financial_report_page("account", date(), date(), 1, 0).await,
        Err(WbError::RateLimited { retry_after: Some(delay), .. }) if delay == Duration::from_hours(24))
    );
    assert!(
        matches!(client.financial_report_page("alias", date(), date(), 1, 0).await,
        Err(WbError::LocalRateLimited { retry_after }) if retry_after > Duration::from_secs(86_390))
    );
    thread.join().unwrap();
}

#[tokio::test]
async fn finance_lists_and_id_details_use_fixed_read_contracts() {
    let (base, requests) = mock_http(vec![
        (
            200,
            "[{\"reportId\":9223372036854775807,\"forPaySum\":\"1.001\"}]".to_owned(),
        ),
        (200, "[]".to_owned()),
        (204, String::new()),
    ]);
    let client = client(&base);
    let summary = client
        .financial_reports_list_page(
            "account",
            date(),
            date(),
            WbFinancePeriod::Weekly,
            1000,
            2000,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary[0]["reportId"].as_u64(), Some(MAX_WB_SIGNED_ID));
    assert_eq!(summary[0]["forPaySum"], "1.001");
    assert_eq!(
        client
            .financial_report_by_id_page("account", MAX_WB_SIGNED_ID, 1000, 5)
            .await
            .unwrap(),
        Some(json!([]))
    );
    assert_eq!(
        client
            .financial_report_by_id_page("account", 123, 1000, 5)
            .await
            .unwrap(),
        None
    );
    let request = requests.recv().unwrap();
    assert!(request.starts_with("POST /api/finance/v1/sales-reports/list HTTP/1.1\r\n"));
    assert_eq!(
        serde_json::from_str::<Value>(request.split_once("\r\n\r\n").unwrap().1).unwrap(),
        json!({
            "dateFrom":"2026-09-10", "dateTo":"2026-09-10", "period":"weekly", "limit":1000, "offset":2000,
        })
    );
    for id in [MAX_WB_SIGNED_ID, 123] {
        let request = requests.recv().unwrap();
        assert!(request.starts_with(&format!(
            "POST /api/finance/v1/sales-reports/detailed/{id} HTTP/1.1\r\n"
        )));
        assert_eq!(
            serde_json::from_str::<Value>(request.split_once("\r\n\r\n").unwrap().1).unwrap(),
            json!({
                "limit":1000, "rrdId":5, "fields":WB_FINANCE_FIELDS,
            })
        );
    }
}

#[tokio::test]
async fn finance_id_and_list_scopes_reject_invalid_inputs_before_credentials() {
    let client = client("http://127.0.0.1:1");
    for (report, limit, cursor) in [
        (0, 1, 0),
        (u64::MAX, 1, 0),
        (1, 0, 0),
        (1, 1001, 0),
        (1, 1, u64::MAX),
    ] {
        assert!(matches!(
            client
                .financial_report_by_id_page("missing", report, limit, cursor)
                .await,
            Err(WbError::InvalidArguments { .. })
        ));
    }
    for (start, end, limit, offset) in [
        (NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(), date(), 1, 0),
        (date(), date().pred_opt().unwrap(), 1, 0),
        (
            date(),
            date().checked_add_days(chrono::Days::new(31)).unwrap(),
            1,
            0,
        ),
        (date(), date(), 0, 0),
        (date(), date(), 1001, 0),
        (date(), date(), 1, 25001),
    ] {
        assert!(matches!(
            client
                .financial_reports_list_page(
                    "missing",
                    start,
                    end,
                    WbFinancePeriod::Daily,
                    limit,
                    offset
                )
                .await,
            Err(WbError::InvalidArguments { .. })
        ));
    }
    for suffix in [
        "",
        "0",
        "01",
        "+1",
        "-1",
        "1/",
        "1?x=2",
        "1#x",
        "%31",
        "9223372036854775808",
        "1/delete",
    ] {
        let path = format!("/api/finance/v1/sales-reports/detailed/{suffix}");
        assert!(
            matches!(
                client
                    .request_document("missing", Method::POST, &path, None, None)
                    .await,
                Err(WbError::EndpointNotAllowed { .. })
            ),
            "{path}"
        );
    }
    for method in [Method::GET, Method::PUT, Method::DELETE, Method::PATCH] {
        for path in [
            FINANCE_LIST_PATH,
            "/api/finance/v1/sales-reports/detailed/1",
        ] {
            assert!(matches!(
                client
                    .request_document("missing", method.clone(), path, None, None)
                    .await,
                Err(WbError::EndpointNotAllowed { .. })
            ));
        }
    }
}

#[tokio::test]
async fn finance_list_allows_terminal_probe_at_exact_collection_bound() {
    let (base, requests) = mock_http(vec![(204, String::new())]);
    let client = client(&base);
    assert_eq!(
        client
            .financial_reports_list_page(
                "account",
                date(),
                date(),
                WbFinancePeriod::Weekly,
                1000,
                25000
            )
            .await
            .unwrap(),
        None
    );
    let request = requests.recv().unwrap();
    let body: Value = serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(body["offset"], 25000);
    assert_eq!(body["limit"], 1000);
}
