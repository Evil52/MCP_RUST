//! Composition of the fixed domain-specific MCP tool registries.

mod directory;
mod finance;
mod operational_reads;
mod orders;
mod ozon_advertising;
mod ozon_catalog;
mod ozon_search;
mod reporting;
mod wb_advertising;
mod wb_catalog;
mod wb_read_coverage;
mod wb_report;
mod wb_stock_report;

use super::{OzonMcp, ToolRouter};

impl OzonMcp {
    pub(super) fn build_tool_router() -> ToolRouter<Self> {
        Self::reporting_router()
            + Self::operational_reads_router()
            + Self::wb_stock_report_router()
            + Self::wb_report_router()
            + Self::wb_read_coverage_router()
            + Self::ozon_search_router()
            + Self::directory_router()
            + Self::wb_catalog_router()
            + Self::wb_advertising_router()
            + Self::ozon_catalog_router()
            + Self::orders_router()
            + Self::finance_router()
            + Self::ozon_advertising_router()
    }
}
