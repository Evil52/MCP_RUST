use super::*;

#[test]
fn posting_sales_context_accepts_an_explicit_authorized_store() {
    let store = StoreId::from("store_a");
    assert_eq!(
        server().posting_sales_context(&RequestIdentity::dev(), Some(&store)),
        Ok(store)
    );
}

#[test]
fn posting_sales_context_rejects_blank_and_oversized_store_selectors() {
    let server = server();
    for (store, expected) in [
        (" ".to_owned(), "store не может быть пустым".to_owned()),
        (
            "x".repeat(MAX_STORE_SELECTOR_CHARS + 1),
            format!("store не может быть длиннее {MAX_STORE_SELECTOR_CHARS} символов"),
        ),
    ] {
        let error = server
            .posting_sales_context(
                &RequestIdentity::dev(),
                Some(&StoreId::from(store.as_str())),
            )
            .expect_err("an invalid explicit selector must be rejected");
        assert_eq!(error, expected);
    }
}

#[tokio::test]
async fn weekly_ranking_rejects_registry_identifiers_outside_reporting_scope() {
    let repository = Arc::new(FakeReportingRepository::succeeding());
    let server = reporting_edge_test_server("admin", repository.clone());
    let error = reporting_tool_error(
        server
            .reporting_weekly_marketplace_ranking(
                RequestIdentity::dev(),
                Parameters(ReportingWeeklyMarketplaceRankingInput {
                    date_from: Some("2026-08-24".to_owned()),
                    date_to: Some("2026-08-30".to_owned()),
                }),
            )
            .await,
    );
    assert!(error.starts_with(REPORTING_INVALID_REQUEST));
    assert_eq!(repository.calls(), 0);
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn completed_tool_result_survives_terminal_telemetry_permission_loss() {
    use tracing::instrument::WithSubscriber as _;
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").unwrap();
    let writer_url = std::env::var("REPORT_REFRESH_TEST_REQUESTER_URL").unwrap();
    let (admin, connection) = tokio_postgres::connect(&admin_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let telemetry = ToolTelemetryService::connect_optional(Some(&writer_url))
        .await
        .unwrap();
    let server = server().with_tool_telemetry(telemetry);
    let (transport, _remote) = tokio::io::duplex(1024);
    let running =
        rmcp::service::serve_directly::<RoleServer, _, _, _, _>(server.clone(), transport, None);
    let logs = Arc::new(Mutex::new(Vec::new()));
    let writer = ToolNameLogWriter(Arc::clone(&logs));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    admin.batch_execute("REVOKE EXECUTE ON FUNCTION daily_reporting.finish_mcp_tool_call(bigint,text,integer,text) FROM report_refresh_requester").await.unwrap();
    let request =
        serde_json::from_value(json!({"name":"marketplace_accounts","arguments":{}})).unwrap();
    let context = RequestContext::new(rmcp::model::RequestId::Number(1), running.peer().clone());
    let result = server
        .call_tool(request, context)
        .with_subscriber(subscriber)
        .await;
    admin.batch_execute("GRANT EXECUTE ON FUNCTION daily_reporting.finish_mcp_tool_call(bigint,text,integer,text) TO report_refresh_requester").await.unwrap();
    assert!(
        matches!(result, Ok(CallToolResponse::Complete(result)) if result.is_error != Some(true))
    );
    let captured = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
    assert!(captured.contains("tool call finished but terminal telemetry could not be recorded"));
    assert!(captured.contains("marketplace_accounts"));
    assert!(!captured.contains(&writer_url));
    let _ = running.cancel().await;
    drop(server);
    drop(admin);
    driver.await.unwrap().unwrap();
}

#[tokio::test]
async fn readiness_reports_configured_telemetry_connection_loss() {
    use tracing::instrument::WithSubscriber as _;
    let service = ToolTelemetryService::from_test_client(closed_telemetry_client().await);
    let server = server().with_tool_telemetry(service);
    let logs = Arc::new(Mutex::new(Vec::new()));
    let writer = ToolNameLogWriter(Arc::clone(&logs));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    assert_eq!(
        server.readiness().with_subscriber(subscriber).await,
        Err(())
    );
    let captured = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
    assert!(captured.contains("tool telemetry is unavailable"));
}

#[test]
fn terminal_telemetry_distinguishes_protocol_cancellation_and_overload() {
    assert_eq!(
        classify_tool_call_result(&Err(rmcp::ErrorData::invalid_params("bad request", None))),
        (ToolCallOutcome::Failed, Some("MCP_PROTOCOL_ERROR"))
    );
    let input = rmcp::model::InputRequiredResult::from_request_state("test").into();
    assert_eq!(
        classify_tool_call_result(&Ok(input)),
        (ToolCallOutcome::Succeeded, None)
    );
    assert_eq!(
        classify_tool_call_result(&Ok(tool_call_cancelled_response())),
        (ToolCallOutcome::Cancelled, Some("MCP_CANCELLED"))
    );
    assert_eq!(
        classify_tool_call_result(&Ok(tool_call_control_failure("local_overloaded", "full"))),
        (ToolCallOutcome::Overloaded, Some("MCP_LOCAL_OVERLOADED"))
    );
    for (error, expected) in [
        (
            ToolTelemetryError::InvalidRequest,
            TOOL_TELEMETRY_INVALID_REQUEST,
        ),
        (ToolTelemetryError::Unavailable, TOOL_TELEMETRY_UNAVAILABLE),
        (ToolTelemetryError::InvalidData, TOOL_TELEMETRY_UNAVAILABLE),
    ] {
        assert!(OzonMcp::tool_telemetry_error(error).starts_with(expected));
    }
}

#[test]
fn weekly_defaults_cross_year_and_partial_dates_are_rejected() {
    use super::super::validation::weekly_ranking_period;
    let current = NaiveDate::from_ymd_opt(2027, 1, 4).unwrap();
    assert_eq!(
        weekly_ranking_period(None, None, current).unwrap(),
        (
            NaiveDate::from_ymd_opt(2026, 12, 28).unwrap(),
            NaiveDate::from_ymd_opt(2027, 1, 3).unwrap(),
        )
    );
    for (from, to) in [(Some("2026-12-28"), None), (None, Some("2027-01-03"))] {
        assert!(
            weekly_ranking_period(from, to, current)
                .expect_err("request must fail")
                .contains("вместе")
        );
    }
}

#[tokio::test]
async fn marketplace_refresh_uses_registry_marketplace_and_checks_scope() {
    let repository = Arc::new(FakeRefreshRequestRepository::default());
    let admin =
        server().with_refresh_requests(RefreshRequestService::from_repository(repository.clone()));
    for (account, marketplace) in [
        ("store_a", ReportingMarketplace::Ozon),
        ("account_wb", ReportingMarketplace::Wildberries),
    ] {
        let result = admin
            .request_marketplace_sales_refresh(
                RequestIdentity::dev(),
                Parameters(ReportingOzonSalesRefreshInput {
                    account: Some(account.to_owned()),
                }),
            )
            .await
            .unwrap()
            .0;
        assert_eq!(result.account_id, account);
        assert_eq!(result.marketplace, marketplace);
        assert_eq!(result.created, Some(true));
        let status = admin
            .marketplace_sales_refresh_status(
                RequestIdentity::dev(),
                Parameters(ReportingOzonSalesRefreshInput {
                    account: Some(account.to_owned()),
                }),
            )
            .await
            .unwrap()
            .0;
        assert_eq!(status.created, None);
        let recorded = repository.last_request.lock().unwrap().clone().unwrap();
        assert_eq!(
            (recorded.0.as_str(), recorded.1, recorded.2.as_str()),
            (account, marketplace, "admin")
        );
    }
    let manager = manager_server("manager")
        .with_refresh_requests(RefreshRequestService::from_repository(repository.clone()));
    let before = repository.calls();
    assert!(
        manager
            .request_marketplace_sales_refresh(
                RequestIdentity::dev(),
                Parameters(ReportingOzonSalesRefreshInput {
                    account: Some("account_wb".to_owned())
                })
            )
            .await
            .err()
            .expect("request must fail")
            .starts_with(ACCESS_DENIED)
    );
    assert_eq!(repository.calls(), before);
}

#[tokio::test]
async fn invalid_page_limits_fail_before_disabled_services() {
    let server = server().with_tool_telemetry(ToolTelemetryService::disabled());
    for limit in [0, MAX_TOOL_CALL_LOG_ROWS + 1] {
        assert!(
            server
                .tool_call_log(
                    RequestIdentity::dev(),
                    Parameters(ToolCallLogInput { limit })
                )
                .await
                .err()
                .expect("request must fail")
                .starts_with(TOOL_TELEMETRY_INVALID_REQUEST)
        );
    }
    for limit in [0, MAX_SALES_ANALYTICS_ROWS + 1] {
        let input = serde_json::from_value(
            json!({"date_from":"2026-09-01", "date_to":"2026-09-01", "limit":limit}),
        )
        .unwrap();
        assert!(
            server
                .reporting_ozon_sales_analytics(RequestIdentity::dev(), Parameters(input))
                .await
                .err()
                .expect("request must fail")
                .contains("limit")
        );
    }
}

#[tokio::test]
async fn posting_sales_fallback_rejects_repeated_cursors_and_page_exhaustion() {
    for repeated in [true, false] {
        let count = if repeated { 2 } else { MAX_POSTING_SALES_PAGES };
        let responses = (0..count).map(|index| (200, json!({
            "postings":[{"posting_number":format!("p-{index}"),"status":"delivered","products":[{"sku":7,"quantity":1}]}],
            "has_next":true,"cursor":if repeated { "repeat".to_owned() } else { format!("cursor-{index}") }
        }).to_string())).collect();
        let (server, requests) = mock_server_with_responses(responses);
        let error = server
            .posting_sales_fallback(
                RequestIdentity::dev(),
                Parameters(PostingSalesFallbackInput {
                    store: Some(StoreId::from("store_a")),
                    date_from: "2026-07-01".to_owned(),
                    date_to: "2026-07-02".to_owned(),
                }),
            )
            .await
            .err()
            .expect("request must fail");
        assert!(error.contains(if repeated {
            "repeated_cursor"
        } else {
            "pagination_limit"
        }));
        assert_eq!(requests.try_iter().count(), count);
    }
}
