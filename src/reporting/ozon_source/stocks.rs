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
    /// Enumerate actual fulfillment SKUs before warehouse reads: Ozon rejects
    /// an unfiltered FBO warehouse request for otherwise fully authorized keys.
    /// Every page is checkpointed and the requested IDs are part of its key.
    pub async fn collect_detailed_stock_pages(
        &self,
    ) -> Result<Vec<CollectedStockFact>, OzonReportSourceError> {
        let catalog = self
            .collect_product_pages(
                "/v4/product/info/stocks",
                parse_stock_page,
                OzonReportSourceError::InvalidStocksResponse,
            )
            .await?;
        let mut catalog_keys = BTreeSet::new();
        if catalog.len() > 25_000
            || catalog
                .iter()
                .any(|r| !catalog_keys.insert((r.sku, r.warehouse_id.clone())))
        {
            return Err(OzonReportSourceError::InvalidStocksResponse);
        }
        let mut facts = Vec::new();
        for (scheme, path) in [
            ("fbo", "/v1/product/info/stocks-by-warehouse/fbo"),
            ("fbs", "/v2/product/info/stocks-by-warehouse/fbs"),
        ] {
            let dimension = format!("sku-fulfillment-v2:{scheme}");
            let selected = catalog
                .iter()
                .filter(|r| r.warehouse_id == dimension)
                .collect::<Vec<_>>();
            let skus = selected.iter().map(|r| r.sku).collect::<Vec<_>>();
            let mut warehouse = Vec::new();
            let mut unsupported = false;
            for ids in skus.chunks(100) {
                match self
                    .collect_filtered_warehouse_pages(path, scheme, ids)
                    .await
                {
                    Ok(rows) => warehouse.extend(rows),
                    Err(OzonReportSourceError::Upstream(
                        OzonErrorKind::Http | OzonErrorKind::NotFound,
                    )) => {
                        unsupported = true;
                        break;
                    }
                    Err(error) => return Err(error),
                }
            }
            if unsupported {
                tracing::warn!(
                    endpoint = path,
                    "filtered warehouse stock source rejected; preserving explicitly scoped fulfillment totals"
                );
                facts.extend(selected.into_iter().cloned());
            } else {
                // Native FBS may expose free_stock without reserved. Do not
                // discard the catalog's explicit scheme reserve or infer a
                // reserve by subtraction. Physical availability is published
                // separately by cluster_stocks.
                let preserve_totals = selected
                    .iter()
                    .filter(|r| r.reserved_units.is_some())
                    .filter(|r| {
                        warehouse
                            .iter()
                            .any(|w| w.sku == r.sku && w.reserved_units.is_none())
                    })
                    .map(|r| r.sku)
                    .collect::<BTreeSet<_>>();
                warehouse.retain(|r| !preserve_totals.contains(&r.sku));
                let returned = warehouse.iter().map(|r| r.sku).collect::<BTreeSet<_>>();
                // A missing warehouse row is not a synthetic local zero. Retain
                // its known scheme total under the fulfillment-only dimension.
                facts.extend(
                    selected
                        .into_iter()
                        .filter(|r| !returned.contains(&r.sku))
                        .cloned(),
                );
                facts.extend(warehouse);
            }
        }
        facts.extend(
            catalog
                .into_iter()
                .filter(|r| r.warehouse_id == "sku-fulfillment-v2:rfbs"),
        );
        let mut keys = BTreeSet::new();
        if facts.len() > 25_000
            || facts
                .iter()
                .any(|r| !keys.insert((r.sku, r.warehouse_id.clone())))
        {
            return Err(OzonReportSourceError::InvalidStocksResponse);
        }
        Ok(facts)
    }

    async fn collect_filtered_warehouse_pages(
        &self,
        path: &'static str,
        scheme: &'static str,
        ids: &[u64],
    ) -> Result<Vec<CollectedStockFact>, OzonReportSourceError> {
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut facts = Vec::new();
        for _ in 0..MAX_PRODUCT_PAGES {
            let mut request = warehouse_stock_page_request(path, cursor.as_deref())
                .map_err(|_| OzonReportSourceError::InvalidResponse)?;
            request.payload[if scheme == "fbo" { "skus" } else { "sku" }] = json!(ids);
            let page: Option<(Vec<CollectedStockFact>, Option<String>)> = checkpointed(
                &self.checkpoints,
                json!(["ozon-warehouse-reserves-v3", request.path, request.payload]),
                || async {
                    let response = match self.transport.post(request).await {
                        Ok(response) => response,
                        Err(OzonReportSourceError::Upstream(
                            OzonErrorKind::Http | OzonErrorKind::NotFound,
                        )) => return Ok(None),
                        Err(error) => return Err(error),
                    };
                    let rows = parse_warehouse_stock_page(&response, scheme).map_err(|error| {
                        rejected_stock_response(path, "facts", error, &response)
                    })?;
                    if rows.iter().any(|r| !ids.contains(&r.sku)) {
                        return Err(OzonReportSourceError::InvalidStocksResponse);
                    }
                    let next = next_warehouse_stock_cursor(&response).map_err(|error| {
                        rejected_stock_response(path, "cursor", error, &response)
                    })?;
                    Ok(Some((rows, next)))
                },
            )
            .await?;
            let (rows, next) =
                page.ok_or(OzonReportSourceError::Upstream(OzonErrorKind::NotFound))?;
            facts.extend(rows);
            if facts.len() > 25_000 {
                return Err(OzonReportSourceError::PaginationLimit);
            }
            cursor = next;
            if cursor.as_ref().is_some_and(|c| !cursors.insert(c.clone())) {
                return Err(OzonReportSourceError::InvalidStocksResponse);
            }
            if cursor.is_none() {
                return Ok(facts);
            }
        }
        Err(OzonReportSourceError::PaginationLimit)
    }

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
