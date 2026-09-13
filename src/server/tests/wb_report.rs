use super::financial_ledger::ledger_server;
use super::*;
use crate::{
    reporting::mcp_read::{WbReportReconciliationQuery, WbReportReconciliationResult},
    server::inputs_reporting::ReportingWbReportReconciliationInput,
};

pub(super) fn missing_result(
    account: &AccountScope,
    query: WbReportReconciliationQuery,
) -> WbReportReconciliationResult {
    WbReportReconciliationResult {
        account_id: account.account_id().to_owned(),
        marketplace: ReadMarketplace::Wildberries,
        report_id: query.report_id.to_string(),
        storage: "published_postgresql_wb_official_reports".into(),
        state: DataState::Unavailable,
        report: None,
        comparison: None,
        rows: Vec::new(),
        next_after_rrd_id: None,
    }
}

fn input() -> ReportingWbReportReconciliationInput {
    ReportingWbReportReconciliationInput {
        account_id: "account_a".into(),
        report_id: "9007199254740993".into(),
        after_rrd_id: None,
        limit: 100,
    }
}

#[tokio::test]
async fn official_report_requires_finance_or_admin_and_owned_account_before_database() {
    let repository = Arc::new(FakeReportingRepository::succeeding());
    for actor in ["manager", "analyst", "finance_denied"] {
        let error = reporting_tool_error(
            ledger_server(actor, repository.clone())
                .reporting_wb_report_reconciliation(RequestIdentity::dev(), Parameters(input()))
                .await,
        );
        assert!(
            error.starts_with(ROLE_ACCESS_DENIED) || error.starts_with(ACCESS_DENIED),
            "{error}"
        );
    }
    assert_eq!(repository.calls(), 0);
    for actor in ["finance", "admin"] {
        let result = ledger_server(actor, repository.clone())
            .reporting_wb_report_reconciliation(RequestIdentity::dev(), Parameters(input()))
            .await
            .unwrap()
            .0;
        assert_eq!(result.account_id, "account_a");
        assert_eq!(result.report_id, "9007199254740993");
        assert_eq!(result.state, DataState::Unavailable);
        assert!(result.report.is_none());
        assert!(result.comparison.is_none());
        assert!(result.rows.is_empty());
    }
    assert_eq!(repository.calls(), 2);
}

#[tokio::test]
async fn official_report_revalidates_employee_access_on_every_cursor_request() {
    let repository = Arc::new(FakeReportingRepository::succeeding());
    let server = ledger_server("finance", repository.clone());
    server
        .reporting_wb_report_reconciliation(RequestIdentity::dev(), Parameters(input()))
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
    let next = ReportingWbReportReconciliationInput {
        after_rrd_id: Some("9007199254740994".into()),
        ..input()
    };
    let error = reporting_tool_error(
        server
            .reporting_wb_report_reconciliation(RequestIdentity::dev(), Parameters(next))
            .await,
    );
    assert!(error.starts_with(ACCESS_DENIED), "{error}");
    assert_eq!(repository.calls(), 1);
}

#[tokio::test]
async fn official_report_rejects_bad_ids_cursors_limits_and_other_market_without_database() {
    let repository = Arc::new(FakeReportingRepository::succeeding());
    let server = ledger_server("finance", repository.clone());
    let mut invalid = vec![
        ReportingWbReportReconciliationInput {
            limit: 0,
            ..input()
        },
        ReportingWbReportReconciliationInput {
            limit: 1001,
            ..input()
        },
    ];
    for id in ["0", "-1", "+1", "01", "9223372036854775808", ""] {
        invalid.push(ReportingWbReportReconciliationInput {
            report_id: id.into(),
            ..input()
        });
    }
    for cursor in ["-1", "+1", "01", "9223372036854775808", ""] {
        invalid.push(ReportingWbReportReconciliationInput {
            after_rrd_id: Some(cursor.into()),
            ..input()
        });
    }
    for input in invalid {
        let error = reporting_tool_error(
            server
                .reporting_wb_report_reconciliation(RequestIdentity::dev(), Parameters(input))
                .await,
        );
        assert!(error.starts_with(REPORTING_INVALID_REQUEST), "{error}");
    }
    let other = reporting_test_server("finance", repository.clone());
    let error = reporting_tool_error(
        other
            .reporting_wb_report_reconciliation(RequestIdentity::dev(), Parameters(input()))
            .await,
    );
    assert!(error.starts_with(REPORTING_INVALID_REQUEST), "{error}");
    assert_eq!(repository.calls(), 0);
}

#[test]
fn official_report_schema_rejects_arbitrary_api_fields_and_preserves_read_only_annotations() {
    let seed = json!({"account_id":"account_a","report_id":"9007199254740993"});
    for (name, value) in [
        ("url", json!("http://internal")),
        ("fields", json!(["buyerPhone"])),
        ("report_id", json!(9_007_199_254_740_993_u64)),
        ("after_rrd_id", json!(1)),
        ("actor_id", json!("admin")),
    ] {
        let mut input = seed.clone();
        input[name] = value;
        assert!(serde_json::from_value::<ReportingWbReportReconciliationInput>(input).is_err());
    }
    let tool = ledger_server("finance", Arc::new(FakeReportingRepository::succeeding()))
        .tool_router
        .list_all()
        .into_iter()
        .find(|tool| tool.name == "ofk_wb_report_reconciliation")
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
async fn official_report_unavailable_storage_uses_stable_sanitized_errors() {
    let seed = ledger_server("finance", Arc::new(FakeReportingRepository::succeeding()));
    let disabled = seed
        .clone()
        .with_reporting_reader(ReportingReader::disabled());
    let error = reporting_tool_error(
        disabled
            .reporting_wb_report_reconciliation(RequestIdentity::dev(), Parameters(input()))
            .await,
    );
    assert!(error.starts_with(REPORTING_UNAVAILABLE), "{error}");
    let unavailable = seed.with_reporting_reader(ReportingReader::from_repository(Arc::new(
        FakeReportingRepository::unavailable(),
    )));
    let error = reporting_tool_error(
        unavailable
            .reporting_wb_report_reconciliation(RequestIdentity::dev(), Parameters(input()))
            .await,
    );
    assert!(
        error.starts_with(REPORTING_TEMPORARILY_UNAVAILABLE),
        "{error}"
    );
}
