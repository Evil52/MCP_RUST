//! Composition of the fixed domain-specific MCP tool registries.

mod directory;
mod finance;
mod orders;
mod ozon_advertising;
mod ozon_catalog;
mod reporting;
mod wb_advertising;
mod wb_catalog;

use super::{OzonMcp, ToolRouter};

impl OzonMcp {
    pub(super) fn build_tool_router() -> ToolRouter<Self> {
        Self::reporting_router()
            + Self::directory_router()
            + Self::wb_catalog_router()
            + Self::wb_advertising_router()
            + Self::ozon_catalog_router()
            + Self::orders_router()
            + Self::finance_router()
            + Self::ozon_advertising_router()
    }
}
