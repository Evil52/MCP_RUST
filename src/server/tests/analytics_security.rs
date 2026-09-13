use super::*;

#[test]
fn reporting_only_registry_contains_only_prepared_data_and_directory_tools() {
    let server = server().into_reporting_only().unwrap();
    let tools = server.tool_router.list_all();
    let actual = tools
        .iter()
        .map(|tool| tool.name.as_ref())
        .collect::<BTreeSet<_>>();
    let expected = BTreeSet::from([
        "marketplace_accounts",
        "list_members",
        "ofk_collection_status",
        "ofk_data_completeness",
        "ofk_metrics_history",
        "ofk_weekly_marketplace_ranking",
        "ofk_source_snapshot",
        "ofk_ozon_sales_analytics",
        "ofk_request_ozon_sales_refresh",
        "ofk_ozon_sales_refresh_status",
        "ofk_request_marketplace_sales_refresh",
        "ofk_marketplace_sales_refresh_status",
        "ofk_manager_actions",
        "ofk_reports",
        "ofk_tool_call_log",
    ]);
    assert_eq!(actual, expected);
    for tool in tools {
        let annotations = tool.annotations.as_ref().unwrap();
        assert_eq!(annotations.open_world_hint, Some(false), "{}", tool.name);
        assert_eq!(
            annotations.read_only_hint,
            Some(!REPORT_REFRESH_WRITE_TOOLS.contains(&tool.name.as_ref()))
        );
        assert_eq!(tool.input_schema["additionalProperties"], json!(false));
    }
    assert!(
        server
            .get_info()
            .instructions
            .unwrap()
            .contains("reporting_only")
    );
}

#[tokio::test]
async fn reporting_only_drops_keys_and_rejects_live_dispatch() {
    let (server, requests) = mock_server(0);
    let store = StoreId::from("store_a");
    let wb = WbClient::new(
        Duration::from_secs(1),
        BTreeMap::from([(
            "account_wb".to_owned(),
            crate::wb::WbCredentials {
                token: "synthetic-token".into(),
            },
        )]),
    );
    let performance = PerformanceClient::new(
        Duration::from_secs(1),
        BTreeMap::from([(
            store.clone(),
            PerformanceCredentials {
                client_id: "synthetic-client".into(),
                client_secret: "synthetic-secret".into(),
            },
        )]),
    )
    .unwrap();
    assert!(server.client.is_configured(&store));
    assert!(wb.is_configured("account_wb"));
    assert!(performance.is_configured(&store));
    let server = server
        .with_wildberries_client(wb.clone())
        .with_performance_client(performance.clone())
        .into_reporting_only()
        .unwrap()
        .with_wildberries_client(wb)
        .with_performance_client(performance);
    assert!(!server.client.is_configured(&store));
    assert!(!server.wb_client.is_configured("account_wb"));
    assert!(!server.performance_client.is_configured(&store));
    for name in [
        "ozon_analytics",
        "ozon_finance_accrual_postings",
        "wb_sales",
        "wb_promotion_stats",
        "http_request",
    ] {
        let body = call_tool_over_http(
            server.clone(),
            name,
            json!({"account":"account_wb", "store":"store_a"}),
        )
        .await;
        assert!(body.contains("tool not found"), "{name}: {body}");
    }
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn reporting_only_rechecks_scope_on_each_read_and_refresh() {
    let reads = Arc::new(FakeReportingRepository::succeeding());
    let refreshes = Arc::new(FakeRefreshRequestRepository::default());
    let server = manager_server("manager")
        .with_reporting_reader(ReportingReader::from_repository(reads.clone()))
        .with_refresh_requests(RefreshRequestService::from_repository(refreshes.clone()))
        .into_reporting_only()
        .unwrap();
    let read_args = json!({"account":"account_b","source":"stocks"});
    let refresh_args = json!({"account":"account_b"});
    let read = call_tool_over_http(server.clone(), "ofk_source_snapshot", read_args.clone()).await;
    assert!(read.contains("published_postgresql_snapshots"), "{read}");
    let refresh = call_tool_over_http(
        server.clone(),
        "ofk_request_marketplace_sales_refresh",
        refresh_args.clone(),
    )
    .await;
    assert!(refresh.contains("queued"), "{refresh}");
    assert_eq!((reads.calls(), refreshes.calls()), (1, 1));

    let mut registry: Value =
        serde_json::from_slice(&fs::read(server.registry.path()).unwrap()).unwrap();
    registry["accounts"][1]["manager_id"] = json!("admin");
    fs::write(
        server.registry.path(),
        serde_json::to_vec(&registry).unwrap(),
    )
    .unwrap();
    for (tool, arguments) in [
        ("ofk_source_snapshot", read_args),
        (
            "ofk_request_marketplace_sales_refresh",
            refresh_args.clone(),
        ),
        ("ofk_marketplace_sales_refresh_status", refresh_args),
    ] {
        let body = call_tool_over_http(server.clone(), tool, arguments).await;
        assert!(body.contains(ACCESS_DENIED), "{tool}: {body}");
    }
    assert_eq!((reads.calls(), refreshes.calls()), (1, 1));
}

#[tokio::test]
async fn reporting_only_accounts_describe_reader_without_claiming_upstream_access() {
    let reads = Arc::new(FakeReportingRepository::succeeding());
    let server = manager_server("manager").into_reporting_only().unwrap();
    for enabled in [false, true] {
        let server = if enabled {
            server
                .clone()
                .with_reporting_reader(ReportingReader::from_repository(reads.clone()))
        } else {
            server.clone()
        };
        let result = server
            .marketplace_accounts(RequestIdentity::dev(), Parameters(EmptyInput {}))
            .await
            .unwrap()
            .0;
        assert_eq!(result.accounts.len(), 1);
        assert_eq!(
            result.accounts[0].integration_status,
            "published_postgresql_snapshots"
        );
        assert_eq!(result.accounts[0].configured, enabled);
    }
    assert_eq!(reads.calls(), 0);
}

#[tokio::test]
async fn independent_source_tool_respects_account_and_finance_access_before_reading() {
    use crate::reporting::snapshot::SnapshotSource;
    let repository = Arc::new(FakeReportingRepository::succeeding());
    let manager = reporting_test_server("manager", repository.clone());
    let input = ReportingSourceSnapshotInput {
        account: None,
        source: SnapshotSource::Prices,
        snapshot_id: None,
        limit: 100,
        offset: 0,
    };
    assert_eq!(
        manager
            .reporting_source_snapshot(RequestIdentity::dev(), Parameters(input.clone()))
            .await
            .unwrap()
            .0
            .storage,
        "published_postgresql_snapshots"
    );
    for source in [SnapshotSource::Advertising, SnapshotSource::Finance] {
        let denied = reporting_tool_error(
            manager
                .reporting_source_snapshot(
                    RequestIdentity::dev(),
                    Parameters(ReportingSourceSnapshotInput {
                        source,
                        ..input.clone()
                    }),
                )
                .await,
        );
        assert!(denied.starts_with(ROLE_ACCESS_DENIED));
    }
    assert!(
        manager
            .reporting_source_snapshot(
                RequestIdentity::dev(),
                Parameters(ReportingSourceSnapshotInput {
                    account: Some("unknown_account".to_owned()),
                    ..input.clone()
                })
            )
            .await
            .is_err()
    );
    assert!(
        manager
            .reporting_source_snapshot(
                RequestIdentity::dev(),
                Parameters(ReportingSourceSnapshotInput {
                    offset: 1,
                    ..input.clone()
                })
            )
            .await
            .is_err()
    );
    assert_eq!(repository.calls(), 1);
    let finance = reporting_test_server("finance", repository.clone());
    finance
        .reporting_source_snapshot(
            RequestIdentity::dev(),
            Parameters(ReportingSourceSnapshotInput {
                source: SnapshotSource::Finance,
                ..input
            }),
        )
        .await
        .unwrap();
    assert_eq!(repository.calls(), 2);
}
