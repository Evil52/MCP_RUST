use crate::reporting::advertising_history::HistoryGroup;
use crate::server::tests::*;
use crate::server::tools::advertising_history::{
    HistoryAccountInput, HistoryStatsInput, HistorySyncInput,
};

#[tokio::test]
async fn history_requires_finance_and_account_scope_before_database_access() {
    let repository = Arc::new(FakeReportingRepository::succeeding());
    for (actor, expected) in [
        ("manager", ROLE_ACCESS_DENIED),
        ("analyst", ROLE_ACCESS_DENIED),
        ("finance_denied", ACCESS_DENIED),
        ("finance", "WB_HISTORY_UNAVAILABLE"),
        ("admin", "WB_HISTORY_UNAVAILABLE"),
    ] {
        let server = financial_ledger::ledger_server(actor, repository.clone());
        let sync = server
            .advertising_history_sync(
                RequestIdentity::dev(),
                Parameters(HistorySyncInput {
                    account: Some("account_a".into()),
                    date_from: None,
                    date_to: None,
                }),
            )
            .await;
        assert!(reporting_tool_error(sync).starts_with(expected));
        let status = server
            .advertising_history_status(
                RequestIdentity::dev(),
                Parameters(HistoryAccountInput {
                    account: Some("account_a".into()),
                }),
            )
            .await;
        assert!(reporting_tool_error(status).starts_with(expected));
        let projection = server
            .advertising_history_stats(
                RequestIdentity::dev(),
                Parameters(HistoryStatsInput {
                    account: Some("account_a".into()),
                    date_from: None,
                    date_to: None,
                    group_by: HistoryGroup::Campaign,
                    limit: 100,
                    offset: 0,
                }),
            )
            .await;
        assert!(reporting_tool_error(projection).starts_with(expected));
    }
    assert_eq!(repository.calls(), 0);
}

#[tokio::test]
async fn history_rejects_open_days_and_non_wb_accounts() {
    let repository = Arc::new(FakeReportingRepository::succeeding());
    let server = financial_ledger::ledger_server("finance", repository.clone());
    let result = server
        .advertising_history_sync(
            RequestIdentity::dev(),
            Parameters(HistorySyncInput {
                account: Some("account_a".into()),
                date_from: None,
                date_to: Some("2100-01-01".into()),
            }),
        )
        .await;
    assert!(reporting_tool_error(result).starts_with(REPORTING_INVALID_REQUEST));
    let ozon = reporting_test_server("finance", repository);
    let result = ozon
        .advertising_history_status(
            RequestIdentity::dev(),
            Parameters(HistoryAccountInput {
                account: Some("account_a".into()),
            }),
        )
        .await;
    assert!(reporting_tool_error(result).starts_with(REPORTING_INVALID_REQUEST));
}
