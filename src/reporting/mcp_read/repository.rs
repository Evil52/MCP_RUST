//! Repository dispatch for the fixed reporting read operations.

use super::{
    AccountScope, CollectionStatusResult, DataCompletenessResult, DateTime, ManagerActionsResult,
    MetricsHistoryResult, NaiveDate, PostgresReportingRepository, ReadyReportsResult,
    ReportingReadError, ReportingReadFuture, SalesAnalyticsQuery, SalesAnalyticsResult,
    SourceSnapshotQuery, SourceSnapshotResult, Utc, WbFinancialLedgerQuery,
    WbFinancialLedgerResult, WbReportReconciliationQuery, WbReportReconciliationResult,
};

/// Injectable repository boundary used by the MCP router's RBAC tests.
pub trait ReportingReadRepository: Send + Sync {
    fn enabled(&self) -> bool;

    fn probe(&self) -> ReportingReadFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn source_snapshot<'a>(
        &'a self,
        _account: &'a AccountScope,
        _query: SourceSnapshotQuery,
    ) -> ReportingReadFuture<'a, SourceSnapshotResult> {
        Box::pin(async { Err(ReportingReadError::Disabled) })
    }

    fn wb_report_reconciliation<'a>(
        &'a self,
        _account: &'a AccountScope,
        _query: WbReportReconciliationQuery,
    ) -> ReportingReadFuture<'a, WbReportReconciliationResult> {
        Box::pin(async { Err(ReportingReadError::Disabled) })
    }

    fn wb_financial_ledger<'a>(
        &'a self,
        _account: &'a AccountScope,
        _query: WbFinancialLedgerQuery,
    ) -> ReportingReadFuture<'a, WbFinancialLedgerResult> {
        Box::pin(async { Err(ReportingReadError::Disabled) })
    }

    fn collection_status<'a>(
        &'a self,
        account: &'a AccountScope,
        limit: u16,
    ) -> ReportingReadFuture<'a, CollectionStatusResult>;

    fn data_completeness<'a>(
        &'a self,
        account: &'a AccountScope,
        cutoff: Option<DateTime<Utc>>,
    ) -> ReportingReadFuture<'a, DataCompletenessResult>;

    fn metrics_history<'a>(
        &'a self,
        account: &'a AccountScope,
        from: Option<NaiveDate>,
        to: Option<NaiveDate>,
        limit: u16,
    ) -> ReportingReadFuture<'a, MetricsHistoryResult>;

    fn sales_analytics<'a>(
        &'a self,
        account: &'a AccountScope,
        query: SalesAnalyticsQuery,
    ) -> ReportingReadFuture<'a, SalesAnalyticsResult>;

    fn manager_actions<'a>(
        &'a self,
        account: &'a AccountScope,
        cutoff: Option<DateTime<Utc>>,
    ) -> ReportingReadFuture<'a, ManagerActionsResult>;

    fn ready_reports(&self, limit: u16) -> ReportingReadFuture<'_, ReadyReportsResult>;
}

impl ReportingReadRepository for PostgresReportingRepository {
    fn wb_report_reconciliation<'a>(
        &'a self,
        account: &'a AccountScope,
        query: WbReportReconciliationQuery,
    ) -> ReportingReadFuture<'a, WbReportReconciliationResult> {
        Box::pin(self.wb_report_reconciliation_impl(account, query))
    }

    fn enabled(&self) -> bool {
        true
    }

    fn probe(&self) -> ReportingReadFuture<'_, ()> {
        Box::pin(self.verify_runtime_contract())
    }

    fn source_snapshot<'a>(
        &'a self,
        account: &'a AccountScope,
        query: SourceSnapshotQuery,
    ) -> ReportingReadFuture<'a, SourceSnapshotResult> {
        Box::pin(self.source_snapshot_impl(account, query))
    }

    fn wb_financial_ledger<'a>(
        &'a self,
        account: &'a AccountScope,
        query: WbFinancialLedgerQuery,
    ) -> ReportingReadFuture<'a, WbFinancialLedgerResult> {
        Box::pin(self.wb_financial_ledger_impl(account, query))
    }

    fn collection_status<'a>(
        &'a self,
        account: &'a AccountScope,
        limit: u16,
    ) -> ReportingReadFuture<'a, CollectionStatusResult> {
        Box::pin(async move { self.collection_status_impl(account, limit).await })
    }

    fn data_completeness<'a>(
        &'a self,
        account: &'a AccountScope,
        cutoff: Option<DateTime<Utc>>,
    ) -> ReportingReadFuture<'a, DataCompletenessResult> {
        Box::pin(async move { self.data_completeness_impl(account, cutoff).await })
    }

    fn metrics_history<'a>(
        &'a self,
        account: &'a AccountScope,
        from: Option<NaiveDate>,
        to: Option<NaiveDate>,
        limit: u16,
    ) -> ReportingReadFuture<'a, MetricsHistoryResult> {
        Box::pin(async move { self.metrics_history_impl(account, from, to, limit).await })
    }

    fn sales_analytics<'a>(
        &'a self,
        account: &'a AccountScope,
        query: SalesAnalyticsQuery,
    ) -> ReportingReadFuture<'a, SalesAnalyticsResult> {
        Box::pin(async move { self.sales_analytics_impl(account, query).await })
    }

    fn manager_actions<'a>(
        &'a self,
        account: &'a AccountScope,
        cutoff: Option<DateTime<Utc>>,
    ) -> ReportingReadFuture<'a, ManagerActionsResult> {
        Box::pin(async move { self.manager_actions_impl(account, cutoff).await })
    }

    fn ready_reports(&self, limit: u16) -> ReportingReadFuture<'_, ReadyReportsResult> {
        Box::pin(async move { self.ready_reports_impl(limit).await })
    }
}
