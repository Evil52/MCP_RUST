use super::*;
use crate::server::inputs_reporting::ReportingWbFinancialLedgerInput;

fn input() -> ReportingWbFinancialLedgerInput {
    ReportingWbFinancialLedgerInput {
        account: Some("account_a".into()),
        batch_id: None,
        after_rrd_id: None,
        limit: 100,
    }
}

fn ledger_server(actor: &str, repository: Arc<dyn ReportingReadRepository>) -> OzonMcp {
    let mut server = reporting_test_server(actor, repository);
    let mut registry: Value =
        serde_json::from_slice(&fs::read(server.registry.path()).unwrap()).unwrap();
    let account = registry["accounts"][0].as_object_mut().unwrap();
    account.insert("marketplace".into(), json!("wildberries"));
    account.insert("wildberries".into(), json!({"api_token_env":"WB_TOKEN"}));
    account.remove("ozon");
    fs::write(
        server.registry.path(),
        serde_json::to_vec(&registry).unwrap(),
    )
    .unwrap();
    server.registry = RegistrySource::new(server.registry.path().to_path_buf()).unwrap();
    server.into_reporting_only().unwrap()
}

#[tokio::test]
async fn ledger_requires_finance_role_and_account_ownership_before_repository_access() {
    let repository = Arc::new(FakeReportingRepository::succeeding());
    for actor in ["manager", "analyst"] {
        let server = ledger_server(actor, repository.clone());
        let error = reporting_tool_error(
            server
                .reporting_wb_financial_ledger(RequestIdentity::dev(), Parameters(input()))
                .await,
        );
        assert!(error.starts_with(ROLE_ACCESS_DENIED), "{error}");
    }
    let denied = ledger_server("finance_denied", repository.clone());
    let error = reporting_tool_error(
        denied
            .reporting_wb_financial_ledger(RequestIdentity::dev(), Parameters(input()))
            .await,
    );
    assert!(error.starts_with(ACCESS_DENIED), "{error}");
    assert_eq!(repository.calls(), 0);
    for actor in ["finance", "admin"] {
        let result = ledger_server(actor, repository.clone())
            .reporting_wb_financial_ledger(RequestIdentity::dev(), Parameters(input()))
            .await
            .unwrap()
            .0;
        assert_eq!(result.account_id, "account_a");
        assert_eq!(result.state, DataState::Unavailable);
        assert_eq!(result.reconciliation_state, DataState::Unavailable);
        assert!(result.batch.is_none());
        assert!(result.rows.is_empty());
    }
    assert_eq!(repository.calls(), 2);
}

#[tokio::test]
async fn ledger_rechecks_employee_scope_on_each_page_after_revocation() {
    let repository = Arc::new(FakeReportingRepository::succeeding());
    let server = ledger_server("finance", repository.clone());
    server
        .reporting_wb_financial_ledger(RequestIdentity::dev(), Parameters(input()))
        .await
        .unwrap();
    let mut registry: Value =
        serde_json::from_slice(&fs::read(server.registry.path()).unwrap()).unwrap();
    registry["actors"][1]["account_ids"] = json!(["account_b"]);
    fs::write(
        server.registry.path(),
        serde_json::to_vec(&registry).unwrap(),
    )
    .unwrap();
    let next = ReportingWbFinancialLedgerInput {
        batch_id: Some(7),
        after_rrd_id: Some("9007199254740993".into()),
        ..input()
    };
    let error = reporting_tool_error(
        server
            .reporting_wb_financial_ledger(RequestIdentity::dev(), Parameters(next))
            .await,
    );
    assert!(error.starts_with(ACCESS_DENIED), "{error}");
    assert_eq!(repository.calls(), 1);
}

#[tokio::test]
async fn ledger_rejects_unbounded_and_unpinned_cursor_requests_before_database_access() {
    let repository = Arc::new(FakeReportingRepository::succeeding());
    let server = ledger_server("finance", repository.clone());
    let invalid = [
        ReportingWbFinancialLedgerInput {
            limit: 0,
            ..input()
        },
        ReportingWbFinancialLedgerInput {
            limit: 1001,
            ..input()
        },
        ReportingWbFinancialLedgerInput {
            batch_id: Some(0),
            ..input()
        },
        ReportingWbFinancialLedgerInput {
            batch_id: Some(-1),
            ..input()
        },
        ReportingWbFinancialLedgerInput {
            after_rrd_id: Some("1".into()),
            ..input()
        },
        ReportingWbFinancialLedgerInput {
            batch_id: Some(7),
            after_rrd_id: Some("9223372036854775808".into()),
            ..input()
        },
        ReportingWbFinancialLedgerInput {
            batch_id: Some(7),
            after_rrd_id: Some("01".into()),
            ..input()
        },
        ReportingWbFinancialLedgerInput {
            batch_id: Some(7),
            after_rrd_id: Some("+1".into()),
            ..input()
        },
    ];
    for input in invalid {
        let error = reporting_tool_error(
            server
                .reporting_wb_financial_ledger(RequestIdentity::dev(), Parameters(input))
                .await,
        );
        assert!(error.starts_with(REPORTING_INVALID_REQUEST), "{error}");
    }
    assert_eq!(repository.calls(), 0);
    let other_market = reporting_test_server("finance", repository.clone());
    let error = reporting_tool_error(
        other_market
            .reporting_wb_financial_ledger(RequestIdentity::dev(), Parameters(input()))
            .await,
    );
    assert!(error.starts_with(REPORTING_INVALID_REQUEST), "{error}");
    assert_eq!(repository.calls(), 0);
}

#[test]
fn ledger_input_schema_rejects_arbitrary_requests_and_numeric_cursors() {
    for extra in [
        json!({"url":"http://internal"}),
        json!({"fields":["buyerPhone"]}),
        json!({"after_rrd_id":9_007_199_254_740_993_u64}),
    ] {
        assert!(serde_json::from_value::<ReportingWbFinancialLedgerInput>(extra).is_err());
    }
    let tool = ledger_server("finance", Arc::new(FakeReportingRepository::succeeding()))
        .tool_router
        .list_all()
        .into_iter()
        .find(|tool| tool.name == "ofk_wb_financial_ledger")
        .unwrap();
    assert_eq!(tool.input_schema["additionalProperties"], json!(false));
    assert_eq!(
        tool.annotations.as_ref().unwrap().read_only_hint,
        Some(true)
    );
    assert_eq!(
        tool.annotations.as_ref().unwrap().open_world_hint,
        Some(false)
    );
}

#[tokio::test]
async fn ledger_disabled_and_unavailable_storage_fail_closed_with_stable_errors() {
    let seed = ledger_server("finance", Arc::new(FakeReportingRepository::succeeding()));
    let disabled = seed
        .clone()
        .with_reporting_reader(ReportingReader::disabled());
    let error = reporting_tool_error(
        disabled
            .reporting_wb_financial_ledger(RequestIdentity::dev(), Parameters(input()))
            .await,
    );
    assert!(error.starts_with(REPORTING_UNAVAILABLE), "{error}");
    let unavailable = seed.with_reporting_reader(ReportingReader::from_repository(Arc::new(
        FakeReportingRepository::unavailable(),
    )));
    let error = reporting_tool_error(
        unavailable
            .reporting_wb_financial_ledger(RequestIdentity::dev(), Parameters(input()))
            .await,
    );
    assert!(
        error.starts_with(REPORTING_TEMPORARILY_UNAVAILABLE),
        "{error}"
    );
}
