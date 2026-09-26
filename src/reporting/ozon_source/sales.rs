use super::{
    CollectedSalesFact, MAX_SALES_PAGES, NaiveDate, OzonReportRequest, OzonReportSource,
    OzonReportSourceError, OzonReportTransport, checkpointed, json, parse_sales_control_totals,
    parse_sales_page, sales_request, sales_response_shape,
};

impl<T: OzonReportTransport> OzonReportSource<T> {
    pub async fn sales_page(
        &self,
        date_from: NaiveDate,
        date_to: NaiveDate,
        offset: u32,
    ) -> Result<Vec<CollectedSalesFact>, OzonReportSourceError> {
        let request = sales_request(date_from, date_to, offset)
            .map_err(|_| OzonReportSourceError::InvalidResponse)?;
        checkpointed(
            &self.checkpoints,
            json!([request.path, request.payload]),
            || async {
                let response = self.transport.post(request).await?;
                parse_sales_page(&response).map_err(|_| {
                    let shape = sales_response_shape(&response);
                    tracing::warn!(shape, "Ozon Seller analytics response shape was rejected");
                    OzonReportSourceError::InvalidSalesResponse { shape }
                })
            },
        )
        .await
    }

    /// Collects offset-paginated sales rows under the client's one-request-per-
    /// minute Analytics gate. A full-size final page is not accepted as
    /// complete because the upstream response has no trustworthy total-row
    /// contract.
    pub async fn collect_sales_pages(
        &self,
        date_from: NaiveDate,
        date_to: NaiveDate,
    ) -> Result<Vec<CollectedSalesFact>, OzonReportSourceError> {
        let mut pages = crate::reporting::sales_integrity::SalesPages::default();
        for page in 0..MAX_SALES_PAGES {
            let offset = u32::try_from(page)
                .ok()
                .and_then(|page| page.checked_mul(1_000))
                .ok_or(OzonReportSourceError::PaginationLimit)?;
            let rows = self.sales_page(date_from, date_to, offset).await?;
            if rows
                .iter()
                .any(|row| row.business_date < date_from || row.business_date > date_to)
            {
                return Err(OzonReportSourceError::InvalidSalesResponse {
                    shape: "date_outside_requested_period".to_owned(),
                });
            }
            let complete = rows.len() < 1_000;
            if !pages.add(rows) {
                return Err(OzonReportSourceError::SalesPageOverlap);
            }
            if complete {
                if pages.zero_overlap {
                    let request = OzonReportRequest {
                        path: "/v1/analytics/data",
                        payload: json!({
                            "date_from":date_from,"date_to":date_to,"metrics":["revenue","ordered_units"],
                            "dimension":["day"],"filters":[],"sort":[{"key":"day","order":"ASC"}],"limit":1000,"offset":0
                        }),
                    };
                    let totals = checkpointed(
                        &self.checkpoints,
                        json!(["sales-overlap-control-v1", request.payload]),
                        || async {
                            let response = self.transport.post(request).await?;
                            parse_sales_control_totals(&response, date_from, date_to)
                                .map_err(|_| OzonReportSourceError::SalesPageOverlap)
                        },
                    )
                    .await?;
                    if !pages.matches(&totals) {
                        return Err(OzonReportSourceError::SalesPageOverlap);
                    }
                    tracing::info!(
                        source = "sales",
                        "zero-only page overlap verified against independent daily totals"
                    );
                }
                return Ok(pages.into_facts());
            }
        }
        Err(OzonReportSourceError::PaginationLimit)
    }
}
