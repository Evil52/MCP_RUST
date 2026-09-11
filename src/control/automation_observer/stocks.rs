//! A missing SKU means zero stock only after a terminal short source page.

use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};

use crate::{
    reporting::{postgres_collector::CollectedStockFact, wb_adapter::parse_stock_page},
    wb::WbClient,
};

use super::WbAutomationPolicy;

const PAGE_SIZE: u32 = 100;
pub(super) const MAX_STOCK_PAGES: u32 = 10;
// Pagination must not extend the stock read beyond the client's existing
// maximum logical request duration, even when each individual page is fast.
const COLLECTION_TIMEOUT: Duration = Duration::from_secs(60);

pub(super) async fn collect(
    client: &WbClient,
    policy: &WbAutomationPolicy,
) -> Result<Vec<CollectedStockFact>> {
    tokio::time::timeout(COLLECTION_TIMEOUT, collect_pages(client, policy))
        .await
        .context("WB automation stock snapshot превысил общий deadline")?
}

async fn collect_pages(
    client: &WbClient,
    policy: &WbAutomationPolicy,
) -> Result<Vec<CollectedStockFact>> {
    let mut stocks = Vec::new();
    for page in 0..MAX_STOCK_PAGES {
        let offset = page
            .checked_mul(PAGE_SIZE)
            .context("WB automation stock offset overflow")?;
        // The existing client owns per-account pacing, retries and timeouts.
        let response = client
            .warehouse_stocks(
                &policy.account_id,
                serde_json::json!({
                    "nmIds": policy.nm_ids,
                    "chrtIds": [],
                    "limit": PAGE_SIZE,
                    "offset": offset
                }),
            )
            .await
            .context("WB automation stock snapshot недоступен")?;
        let (rows, source_rows) = parse_stock_page(&response)
            .map_err(|_| anyhow::anyhow!("WB automation stock snapshot имеет неверную форму"))?;
        let page_size = usize::try_from(PAGE_SIZE).expect("stock page size fits usize");
        ensure!(
            source_rows <= page_size,
            "WB automation stock response превысил размер страницы"
        );
        stocks.extend(rows);
        // Sizes/warehouses can normalize into fewer facts than source rows.
        // A full page therefore always requires a subsequent request.
        if source_rows < page_size {
            return Ok(stocks);
        }
    }
    bail!("WB automation stock snapshot неполон: исчерпан лимит страниц")
}
