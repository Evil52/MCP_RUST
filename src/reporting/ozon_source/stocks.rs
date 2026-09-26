//! Native warehouse collection and the versioned SKU fulfillment fallback.

use super::{
    BTreeSet, CollectedStockFact, MAX_PRODUCT_PAGES, OzonErrorKind, OzonReportSource,
    OzonReportSourceError, OzonReportTransport, checkpointed, json, rejected_stock_response,
};
use crate::reporting::ozon_adapter::{
    next_warehouse_stock_cursor, parse_stock_page, parse_warehouse_stock_page,
    warehouse_stock_page_request,
};

impl<T: OzonReportTransport> OzonReportSource<T> {
    /// Collects real warehouse-granular FBO and FBS stock pages when the
    /// legacy warehouse endpoints remain available.
    ///
    /// Ozon has retired the legacy FBO route for some accounts. Only an HTTP
    /// rejection or an explicit not-found response from that first route may
    /// fall back to the already allowlisted `/v4/product/info/stocks` source.
    /// The normalized fallback exposes fulfillment-level, not physical-
    /// warehouse-level, inventory. Versioned `sku-fulfillment-v2:*` identifiers
    /// mark real SKU identities and quantities after subtracting reserves. Authentication, quota, server, and transport failures
    /// remain fail-closed and are never hidden by the fallback.
    pub async fn collect_stock_pages(
        &self,
    ) -> Result<Vec<CollectedStockFact>, OzonReportSourceError> {
        let fbo = self
            .collect_warehouse_stock_pages("/v1/product/info/stocks-by-warehouse/fbo", "fbo")
            .await;
        let mut facts = match fbo {
            Ok(facts) => facts,
            Err(OzonReportSourceError::Upstream(OzonErrorKind::Http | OzonErrorKind::NotFound)) => {
                tracing::warn!(
                    endpoint = "/v1/product/info/stocks-by-warehouse/fbo",
                    fallback = "/v4/product/info/stocks",
                    "legacy Ozon stock endpoint was rejected; using fulfillment-level fallback"
                );
                return self
                    .collect_product_pages(
                        "/v4/product/info/stocks",
                        parse_stock_page,
                        OzonReportSourceError::InvalidStocksResponse,
                    )
                    .await;
            }
            Err(error) => return Err(error),
        };
        facts.extend(
            self.collect_warehouse_stock_pages("/v2/product/info/stocks-by-warehouse/fbs", "fbs")
                .await?,
        );
        Ok(facts)
    }

    async fn collect_warehouse_stock_pages(
        &self,
        path: &'static str,
        scheme: &'static str,
    ) -> Result<Vec<CollectedStockFact>, OzonReportSourceError> {
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut facts = Vec::new();
        for _ in 0..MAX_PRODUCT_PAGES {
            let request = warehouse_stock_page_request(path, cursor.as_deref())
                .map_err(|_| OzonReportSourceError::InvalidResponse)?;
            let page: Option<(Vec<CollectedStockFact>, Option<String>)> = checkpointed(
                &self.checkpoints,
                json!([request.path, request.payload]),
                || async {
                    let response = match self.transport.post(request).await {
                        Ok(value) => value,
                        Err(OzonReportSourceError::Upstream(
                            OzonErrorKind::Http | OzonErrorKind::NotFound,
                        )) if scheme == "fbo" => return Ok(None),
                        Err(error) => return Err(error),
                    };
                    let rows = parse_warehouse_stock_page(&response, scheme).map_err(|error| {
                        rejected_stock_response(path, "facts", error, &response)
                    })?;
                    let next = next_warehouse_stock_cursor(&response).map_err(|error| {
                        rejected_stock_response(path, "cursor", error, &response)
                    })?;
                    Ok::<_, OzonReportSourceError>(Some((rows, next)))
                },
            )
            .await?;
            let (rows, next) =
                page.ok_or(OzonReportSourceError::Upstream(OzonErrorKind::NotFound))?;
            facts.extend(rows);
            cursor = next;
            if cursor
                .as_ref()
                .is_some_and(|cursor| !seen_cursors.insert(cursor.clone()))
            {
                return Err(OzonReportSourceError::InvalidStocksResponse);
            }
            if cursor.is_none() {
                return Ok(facts);
            }
        }
        Err(OzonReportSourceError::PaginationLimit)
    }
}
