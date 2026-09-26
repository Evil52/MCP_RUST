use super::{
    CollectedSalesFact, MAX_SALES_PAGES, NaiveDate, SALES_PAGE_SIZE, SALES_PAGE_SIZE_U32,
    WbReportSource, WbReportSourceError, checkpointed, json, page_offset,
    parse_sales_control_totals, parse_sales_page,
};

impl WbReportSource {
    pub async fn collect_sales_pages(
        &self,
        date: NaiveDate,
    ) -> Result<Vec<CollectedSalesFact>, WbReportSourceError> {
        self.collect_sales_pages_with_limit(date, MAX_SALES_PAGES)
            .await
    }

    pub(super) async fn collect_sales_pages_with_limit(
        &self,
        date: NaiveDate,
        max_pages: usize,
    ) -> Result<Vec<CollectedSalesFact>, WbReportSourceError> {
        let mut pages = crate::reporting::sales_integrity::SalesPages::default();
        for page in 0..max_pages {
            let offset = page_offset(page, SALES_PAGE_SIZE_U32)?;
            let (rows, source_rows) = checkpointed(
                &self.checkpoints,
                // Old 1,000-row checkpoints have different page boundaries.
                // Never replay them as a short 250-row page after an upgrade.
                json!(["wb_sales_v2", date, SALES_PAGE_SIZE_U32, offset]),
                || async {
                    parse_sales_page(
                        &self
                            .transport
                            .sales_page(date, date, SALES_PAGE_SIZE_U32, offset)
                            .await?,
                    )
                    .map_err(|_| WbReportSourceError::InvalidSalesResponse)
                },
            )
            .await?;
            if source_rows > SALES_PAGE_SIZE || rows.iter().any(|row| row.business_date != date) {
                return Err(WbReportSourceError::InvalidSalesResponse);
            }
            if !pages.add(rows) {
                return Err(WbReportSourceError::SalesPageOverlap);
            }
            if source_rows < SALES_PAGE_SIZE {
                if pages.zero_overlap {
                    let totals = self.overlap_control(date, false).await?;
                    if !pages.matches(&totals) {
                        let fresh = self
                            .collect_closing_sales_pages(date, MAX_SALES_PAGES)
                            .await?;
                        if !pages.refresh_sales(fresh)
                            || !pages.matches(&self.overlap_control(date, true).await?)
                        {
                            return Err(WbReportSourceError::SalesPageOverlap);
                        }
                        tracing::info!(
                            source = "sales",
                            "live sales closing pass verified against fresh daily totals"
                        );
                    }
                    tracing::info!(
                        source = "sales",
                        "zero-only page overlap verified against independent daily totals"
                    );
                }
                return Ok(pages.into_facts());
            }
        }
        Err(WbReportSourceError::PaginationLimit)
    }

    async fn overlap_control(
        &self,
        date: NaiveDate,
        closing: bool,
    ) -> Result<crate::reporting::sales_integrity::SalesTotals, WbReportSourceError> {
        let key = if closing {
            "wb-sales-overlap-closing-control-v1"
        } else {
            "wb-sales-overlap-control-v1"
        };
        checkpointed(&self.checkpoints, json!([key, date]), || async {
            let response = self.transport.sales_control_totals(date).await?;
            parse_sales_control_totals(&response, date)
                .map_err(|_| WbReportSourceError::SalesPageOverlap)
        })
        .await
    }

    /// A closing observation, after the complete catalogue walk. Descending
    /// order counts bring every current positive SKU into the bounded prefix.
    /// Only a subsequent, separately checkpointed account total may certify it.
    pub(super) async fn collect_closing_sales_pages(
        &self,
        date: NaiveDate,
        max_pages: usize,
    ) -> Result<Vec<CollectedSalesFact>, WbReportSourceError> {
        let mut pages = crate::reporting::sales_integrity::SalesPages::default();
        let mut previous_units = u64::MAX;
        for page in 0..max_pages {
            let offset = page_offset(page, SALES_PAGE_SIZE_U32)?;
            let (rows, source_rows) = checkpointed(
                &self.checkpoints,
                json!([
                    "wb-sales-overlap-closing-v1",
                    date,
                    SALES_PAGE_SIZE_U32,
                    offset
                ]),
                || async {
                    parse_sales_page(
                        &self
                            .transport
                            .sales_closing_page(date, SALES_PAGE_SIZE_U32, offset)
                            .await?,
                    )
                    .map_err(|_| WbReportSourceError::InvalidSalesResponse)
                },
            )
            .await?;
            if source_rows > SALES_PAGE_SIZE || rows.iter().any(|row| row.business_date != date) {
                return Err(WbReportSourceError::InvalidSalesResponse);
            }
            for row in &rows {
                if row.ordered_units > previous_units {
                    return Err(WbReportSourceError::InvalidSalesResponse);
                }
                previous_units = row.ordered_units;
            }
            let complete =
                source_rows < SALES_PAGE_SIZE || rows.iter().any(|row| row.ordered_units == 0);
            if !pages.add(rows) {
                return Err(WbReportSourceError::SalesPageOverlap);
            }
            if complete {
                return Ok(pages.into_facts());
            }
        }
        Err(WbReportSourceError::PaginationLimit)
    }
}
