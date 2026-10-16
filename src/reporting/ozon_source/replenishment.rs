//! Independent durable sources: cluster statistics and operational postings.
use super::{
    BTreeSet, DateTime, Duration, OzonReportRequest, OzonReportSource, OzonReportSourceError,
    OzonReportTransport, Utc, Value, checkpointed, json,
};
use crate::ozon_posting_sales::{
    MAX_POSTING_SALES_PAGES, PostingSalesAccumulator, PostingSalesRow, PostingScheme,
    posting_page_request,
};
use crate::reporting::postgres_collector::{
    CollectedClusterStockFact, replenishment::parse_cluster_rows,
};

impl<T: OzonReportTransport> OzonReportSource<T> {
    pub async fn collect_cluster_stock_pages(
        &self,
    ) -> Result<Vec<CollectedClusterStockFact>, OzonReportSourceError> {
        let catalog = self
            .collect_product_pages(
                "/v4/product/info/stocks",
                crate::reporting::ozon_adapter::parse_stock_page,
                OzonReportSourceError::InvalidStocksResponse,
            )
            .await?;
        let skus = catalog
            .iter()
            .map(|r| r.sku)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut facts = Vec::new();
        let mut keys = BTreeSet::new();
        for ids in skus.chunks(25) {
            let request = OzonReportRequest {
                path: "/v1/analytics/stocks",
                payload: json!({"skus":ids.iter().map(ToString::to_string).collect::<Vec<_>>(),"cluster_ids":[],"warehouse_ids":[]}),
            };
            let rows = checkpointed(
                &self.checkpoints,
                json!(["ozon-cluster-stocks-v1", request.path, request.payload]),
                || async {
                    let response = self.transport.post(request).await?;
                    parse_cluster_rows(&response, ids)
                        .map_err(|_| OzonReportSourceError::InvalidStocksResponse)
                },
            )
            .await?;
            for row in rows {
                if !keys.insert((
                    row.sku,
                    row.warehouse_id,
                    row.cluster_id,
                    row.macrolocal_cluster_id,
                )) {
                    return Err(OzonReportSourceError::InvalidStocksResponse);
                }
                facts.push(row);
            }
            if facts.len() > 25_000 {
                return Err(OzonReportSourceError::PaginationLimit);
            }
        }
        Ok(facts)
    }

    pub async fn collect_posting_movement(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<PostingSalesRow>, OzonReportSourceError> {
        if from >= to || to - from > Duration::days(31) {
            return Err(OzonReportSourceError::InvalidResponse);
        }
        let from = from.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let to =
            (to - Duration::milliseconds(1)).to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let mut aggregate = PostingSalesAccumulator::default();
        for scheme in [PostingScheme::Fbo, PostingScheme::Fbs] {
            let mut cursor = None;
            let mut seen = BTreeSet::new();
            let mut complete = false;
            for _ in 0..MAX_POSTING_SALES_PAGES {
                let request = OzonReportRequest {
                    path: scheme.endpoint(),
                    payload: posting_page_request(scheme, &from, &to, cursor.as_deref()),
                };
                let page: Value = checkpointed(
                    &self.checkpoints,
                    json!(["ozon-posting-movement-v1", request.path, request.payload]),
                    || async {
                        let response = self.transport.post(request).await?;
                        sanitize_posting_page(scheme, &response)
                    },
                )
                .await?;
                cursor = aggregate
                    .absorb_page(scheme, &page)
                    .map_err(|_| OzonReportSourceError::InvalidResponse)?;
                if cursor.as_ref().is_some_and(|c| !seen.insert(c.clone())) {
                    return Err(OzonReportSourceError::InvalidResponse);
                }
                if cursor.is_none() {
                    complete = true;
                    break;
                }
            }
            if !complete {
                return Err(OzonReportSourceError::PaginationLimit);
            }
        }
        aggregate
            .finish()
            .map(|(_, rows)| rows)
            .map_err(|_| OzonReportSourceError::InvalidResponse)
    }
}

// Retain only bounded identifiers and quantities after validating the entire
// upstream page. Checkpoint replay repeats the full global deduplication.
fn sanitize_posting_page(
    scheme: PostingScheme,
    response: &Value,
) -> Result<Value, OzonReportSourceError> {
    PostingSalesAccumulator::default()
        .absorb_page(scheme, response)
        .map_err(|_| OzonReportSourceError::InvalidResponse)?;
    let postings = response["postings"]
        .as_array()
        .ok_or(OzonReportSourceError::InvalidResponse)?;
    let mut safe = Vec::with_capacity(postings.len());
    for posting in postings {
        let products = posting["products"]
            .as_array()
            .ok_or(OzonReportSourceError::InvalidResponse)?;
        safe.push(json!({"posting_number":posting["posting_number"],"status":posting["status"],"products":products.iter().map(|r|json!({"sku":r["sku"],"quantity":r["quantity"]})).collect::<Vec<_>>()}));
    }
    Ok(json!({"postings":safe,"has_next":response["has_next"],"cursor":response["cursor"]}))
}
