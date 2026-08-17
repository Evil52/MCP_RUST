//! Bounded read-only Ozon report source.
//!
//! This module is deliberately transport-agnostic. A future runtime adapter
//! may delegate to `OzonClient`, but every request is first built by the exact
//! request contract in `ozon_adapter` and every response is normalized before
//! it can reach report persistence.

use std::{future::Future, pin::Pin};

use chrono::NaiveDate;
use serde_json::Value;
use thiserror::Error;

use crate::{config::StoreId, ozon::OzonClient};

use super::{
    ozon_adapter::{
        OzonReportParseError, OzonReportRequest, parse_price_page, parse_sales_page,
        parse_stock_page, product_page_request, sales_request,
    },
    postgres_collector::{CollectedPriceFact, CollectedSalesFact, CollectedStockFact},
};

pub trait OzonReportTransport: Send + Sync {
    fn post<'a>(
        &'a self,
        request: OzonReportRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>>;
}

/// Production transport adapter. It deliberately owns no credential material:
/// `OzonClient` owns the policy-scoped store map and enforces the central
/// Seller API read-only egress allowlist at request time.
#[derive(Clone)]
pub struct OzonClientReportTransport {
    client: OzonClient,
    store: StoreId,
}

impl OzonClientReportTransport {
    pub fn new(client: OzonClient, store: StoreId) -> Self {
        Self { client, store }
    }
}

impl OzonReportTransport for OzonClientReportTransport {
    fn post<'a>(
        &'a self,
        request: OzonReportRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .post(&self.store, request.path, request.payload)
                .await
                .map_err(|_| OzonReportSourceError::Transport)
        })
    }
}

pub struct OzonReportSource<T> {
    transport: T,
}

impl<T> OzonReportSource<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OzonReportSourceError {
    #[error("Ozon daily-report source request failed")]
    Transport,
    #[error("Ozon daily-report source response is invalid")]
    InvalidResponse,
}

impl<T: OzonReportTransport> OzonReportSource<T> {
    pub async fn sales_page(
        &self,
        date_from: NaiveDate,
        date_to: NaiveDate,
        offset: u32,
    ) -> Result<Vec<CollectedSalesFact>, OzonReportSourceError> {
        let request = sales_request(date_from, date_to, offset)
            .map_err(|_| OzonReportSourceError::InvalidResponse)?;
        let response = self.transport.post(request).await?;
        parse_sales_page(&response).map_err(parse_error)
    }

    pub async fn stock_page(
        &self,
        cursor: Option<&str>,
    ) -> Result<Vec<CollectedStockFact>, OzonReportSourceError> {
        self.product_page("/v4/product/info/stocks", cursor, parse_stock_page)
            .await
    }

    pub async fn price_page(
        &self,
        cursor: Option<&str>,
    ) -> Result<Vec<CollectedPriceFact>, OzonReportSourceError> {
        self.product_page("/v5/product/info/prices", cursor, parse_price_page)
            .await
    }

    async fn product_page<F, Fact>(
        &self,
        path: &'static str,
        cursor: Option<&str>,
        parse: F,
    ) -> Result<Vec<Fact>, OzonReportSourceError>
    where
        F: Fn(&Value) -> Result<Vec<Fact>, OzonReportParseError>,
    {
        let request = product_page_request(path, cursor)
            .map_err(|_| OzonReportSourceError::InvalidResponse)?;
        let response = self.transport.post(request).await?;
        parse(&response).map_err(parse_error)
    }
}

fn parse_error(_: OzonReportParseError) -> OzonReportSourceError {
    OzonReportSourceError::InvalidResponse
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex};

    use chrono::NaiveDate;
    use serde_json::json;

    use super::*;

    struct FixtureTransport(Mutex<VecDeque<Result<Value, OzonReportSourceError>>>);

    impl OzonReportTransport for FixtureTransport {
        fn post<'a>(
            &'a self,
            _request: OzonReportRequest,
        ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>>
        {
            Box::pin(async move { self.0.lock().unwrap().pop_front().unwrap() })
        }
    }

    #[tokio::test]
    async fn source_normalizes_only_valid_read_only_pages() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Ok(json!({"result":{"data":[{
                "dimensions":[{"id":"1"},{"id":"2026-08-16"}],
                "metrics":["1.00", 2, 0, 0]
            }]}})),
            Ok(json!({"items":[{"product_id":1,"stocks":[{"type":"FBO","present":3}]}]})),
            Ok(
                json!({"items":[{"product_id":1,"price":{"currency_code":"RUB","price":"2","old_price":"0"}}]}),
            ),
        ]))));
        let day = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        assert_eq!(
            source.sales_page(day, day, 0).await.unwrap()[0].ordered_units,
            2
        );
        assert_eq!(source.stock_page(None).await.unwrap()[0].sellable_units, 3);
        assert_eq!(source.price_page(None).await.unwrap()[0].price_minor, 200);
    }

    #[tokio::test]
    async fn source_hides_transport_and_parse_details() {
        let source = OzonReportSource::new(FixtureTransport(Mutex::new(VecDeque::from([
            Err(OzonReportSourceError::Transport),
            Ok(json!({"items":"invalid"})),
        ]))));
        let day = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        assert_eq!(
            source.sales_page(day, day, 0).await,
            Err(OzonReportSourceError::Transport)
        );
        assert_eq!(
            source.stock_page(None).await,
            Err(OzonReportSourceError::InvalidResponse)
        );
    }

    #[tokio::test]
    async fn client_transport_uses_the_hardened_client_and_hides_its_error() {
        let client = OzonClient::new(
            "http://127.0.0.1:1".to_owned(),
            std::time::Duration::from_millis(1),
            Default::default(),
        )
        .unwrap();
        let transport = OzonClientReportTransport::new(client, StoreId::from("missing"));
        assert_eq!(
            transport
                .post(product_page_request("/v4/product/info/stocks", None).unwrap())
                .await,
            Err(OzonReportSourceError::Transport)
        );
    }
}
