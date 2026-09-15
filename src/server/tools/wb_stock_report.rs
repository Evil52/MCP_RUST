//! Current seller inventory report; does not reinterpret missing pairs as zeros.
use super::super::{
    Json, OzonMcp, Parameters, RequestIdentity, WbResult, WbWarehouseStocksInput, tool,
};
use crate::wb::seller_stock_report::SELLER_STOCK_REPORT_LABEL;
use rmcp::{schemars::JsonSchema, tool_router};
use serde::Serialize;

#[derive(Debug, Serialize, JsonSchema)]
pub struct WbSellerStockReportResult {
    #[serde(flatten)]
    pub source: WbResult,
    pub inventory_scope: &'static str,
    pub observation_kind: &'static str,
    pub upstream_refresh_interval_seconds: u32,
    pub limit: u32,
    pub offset: u32,
    pub returned_rows: u32,
    pub next_offset: Option<u32>,
    /// Describes this page only; all preceding pages must also be collected.
    pub page_is_last: bool,
    pub missing_rows_mean_zero: bool,
}

#[tool_router(router = wb_stock_report_router, vis = "pub(in crate::server)")]
impl OzonMcp {
    /// Текущие остатки ВСЕХ складов продавца WB через Analytics, без обхода каталога.
    /// Для полного отчёта оставьте `nm_ids`/`chrt_ids` пустыми и пройдите все `next_offset`,
    /// начиная с `offset=0`. `chrt_ids` действует только вместе с `nm_ids`. Лимит MCP — 1000
    /// строк на страницу. Сверяйте уникальность `chrtId`/`warehouseId` между страницами.
    /// Для FBS сопоставьте `warehouseId` с `wb_seller_warehouses` и `deliveryType=1`;
    /// остальные модели не смешивайте с FBS. WB обновляет данные раз в 30 минут;
    /// `fetched_at` — время чтения, не дата остатков. Нужен Personal/Service токен
    /// Analytics. HTTP 204 — нет данных. Отсутствие строки не подтверждает ноль.
    #[tool(
        name = "wb_seller_warehouses_stock_report",
        annotations(
            title = "Отчёт остатков складов продавца WB / FBS",
            read_only_hint = true
        )
    )]
    pub(in crate::server) async fn wb_seller_warehouses_stock_report(
        &self,
        identity: RequestIdentity,
        Parameters(input): Parameters<WbWarehouseStocksInput>,
    ) -> Result<Json<WbSellerStockReportResult>, String> {
        let account = self.resolve_wb_account(&identity, input.account.as_deref())?;
        let page = self
            .wb_client
            .seller_warehouses_stock_report(
                &account,
                &input.nm_ids,
                &input.chrt_ids,
                input.limit,
                input.offset,
            )
            .await
            .map_err(|error| self.wb_error(&account, SELLER_STOCK_REPORT_LABEL, &error))?;
        Ok(Json(WbSellerStockReportResult {
            source: Self::wb_result(account, SELLER_STOCK_REPORT_LABEL, page.data).0,
            inventory_scope: "seller",
            observation_kind: "current",
            upstream_refresh_interval_seconds: 1_800,
            limit: input.limit,
            offset: input.offset,
            returned_rows: page.returned_rows,
            next_offset: page.next_offset,
            page_is_last: page.next_offset.is_none(),
            missing_rows_mean_zero: false,
        }))
    }
}
