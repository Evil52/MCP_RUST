use super::*;
use crate::reporting::{
    checkpoint::{
        CheckpointError,
        tests::{MemoryPages, journal},
    },
    wb_source::WbReportTransport,
};
use chrono::NaiveDate;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

type Response<'a> = Pin<Box<dyn Future<Output = Result<Value, WbReportSourceError>> + Send + 'a>>;

#[derive(Clone)]
struct Fixture(Arc<Mutex<VecDeque<(&'static str, Value)>>>);
impl Fixture {
    fn new(responses: Vec<(&'static str, Value)>) -> Self {
        Self(Arc::new(Mutex::new(responses.into())))
    }
}
impl WbReportTransport for Fixture {
    fn seller_inventory(&self, request: SellerStockRequest) -> Response<'_> {
        Box::pin(async move {
            let (expected, response) = self
                .0
                .lock()
                .unwrap()
                .pop_front()
                .expect("no extra request");
            let actual = match request {
                SellerStockRequest::Warehouses => "warehouses",
                SellerStockRequest::Cards { .. } => "cards",
                SellerStockRequest::Stocks {
                    warehouse_id,
                    chrt_ids,
                } => {
                    assert!([10, 20].contains(&warehouse_id));
                    assert!(chrt_ids.len() <= STOCK_BATCH);
                    "stocks"
                }
            };
            assert_eq!(expected, actual);
            Ok(response)
        })
    }
    fn stock_page(&self, _: u32, _: u32) -> Response<'_> {
        Box::pin(async { Ok(json!({"data":{"items":[{"nmId":7,"warehouseId":10,"quantity":3}]}})) })
    }
    fn sales_page(&self, _: NaiveDate, _: NaiveDate, _: u32, _: u32) -> Response<'_> {
        unreachable!()
    }
    fn price_page(&self, _: u32, _: u32) -> Response<'_> {
        unreachable!()
    }
    fn campaigns(&self) -> Response<'_> {
        unreachable!()
    }
    fn promotion_stats(&self, _: Vec<u64>, _: NaiveDate, _: NaiveDate) -> Response<'_> {
        unreachable!()
    }
}

fn cards(sizes: &[u64]) -> Value {
    json!({"cards":[{"nmID":7,"title":"untrusted discarded text","sizes":sizes.iter().map(|id| json!({"chrtID":id})).collect::<Vec<_>>()}],"cursor":{"total":1}})
}

#[tokio::test]
async fn every_warehouse_and_size_is_resumed_without_pii_or_double_counting() {
    let fixture = Fixture::new(vec![
        (
            "warehouses",
            json!([{"id":10,"deliveryType":1,"name":"private"},{"id":20,"deliveryType":2}]),
        ),
        ("cards", cards(&[100, 101])),
        (
            "stocks",
            json!({"stocks":[{"chrtId":100,"amount":2},{"chrtId":101,"amount":5}]}),
        ),
        (
            "stocks",
            json!({"stocks":[{"chrtId":100,"amount":0},{"chrtId":101,"amount":1}]}),
        ),
    ]);
    let pages = MemoryPages::default();
    for _ in 0..4 {
        assert_eq!(
            WbReportSource::new(fixture.clone())
                .with_checkpoints(journal(&pages))
                .collect_complete_stock_pages()
                .await,
            Err(WbReportSourceError::Checkpoint(CheckpointError::Deferred))
        );
    }
    let facts = WbReportSource::new(fixture.clone())
        .with_checkpoints(journal(&pages))
        .collect_complete_stock_pages()
        .await
        .unwrap();
    assert_eq!(
        facts,
        vec![
            CollectedStockFact {
                sku: 7,
                warehouse_id: "wb:10".into(),
                sellable_units: 3
            },
            CollectedStockFact {
                sku: 7,
                warehouse_id: "wb:seller:1:10".into(),
                sellable_units: 7
            },
            CollectedStockFact {
                sku: 7,
                warehouse_id: "wb:seller:2:20".into(),
                sellable_units: 1
            },
        ]
    );
    assert!(fixture.0.lock().unwrap().is_empty());
    let saved = serde_json::to_string(&*pages.lock().unwrap()).unwrap();
    assert!(!saved.contains("private") && !saved.contains("untrusted") && !saved.contains("title"));
    assert_eq!(
        WbReportSource::new(fixture)
            .with_checkpoints(journal(&pages))
            .collect_complete_stock_pages()
            .await
            .unwrap(),
        facts
    );
}

#[test]
fn omitted_sizes_are_unknown_and_foreign_or_duplicate_rows_are_rejected() {
    assert_eq!(
        parse_amounts(&json!({"stocks":[]}), &[1]),
        Err(WbReportSourceError::SellerStockCoverageIncomplete)
    );
    assert_eq!(
        parse_amounts(&json!({"stocks":[{"chrtId":1,"amount":0}]}), &[1]).unwrap(),
        vec![(1, 0)]
    );
    for body in [
        json!({}),
        json!({"stocks":[{"chrtId":1}]}),
        json!({"stocks":[{"chrtId":2,"amount":3}]}),
        json!({"stocks":[{"chrtId":1,"amount":-1}]}),
        json!({"stocks":[{"chrtId":1,"amount":1},{"chrtId":1,"amount":1}]}),
    ] {
        assert!(parse_amounts(&body, &[1]).is_err());
    }
    assert!(
        parse_warehouses(&json!([{"id":1,"deliveryType":1},{"id":1,"deliveryType":1}])).is_err()
    );
    assert!(parse_warehouses(&json!([{"id":1}])).is_err());
    assert!(parse_cards(&cards(&[])).is_err());
    assert!(parse_cards(&json!({"cards":[],"cursor":{"total":10}})).is_err());
}

#[tokio::test]
async fn duplicate_catalog_id_and_incomplete_inventory_never_publish_fbw_as_complete() {
    for (catalog, stocks) in [
        (cards(&[100, 100]), vec![]),
        (cards(&[100]), vec![("stocks", json!({"stocks":[]}))]),
    ] {
        let mut responses = vec![
            ("warehouses", json!([{"id":10,"deliveryType":1}])),
            ("cards", catalog),
        ];
        responses.extend(stocks);
        assert!(
            WbReportSource::new(Fixture::new(responses))
                .collect_complete_stock_pages()
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn content_cursor_is_traversed_to_a_short_page_and_duplicate_products_fail_closed() {
    let page = json!({"cards":(1..=100).map(|id| json!({"nmID":id,"sizes":[{"chrtID":id}]})).collect::<Vec<_>>(),
        "cursor":{"total":100,"nmID":100,"updatedAt":"2026-09-13T00:00:00Z"}});
    let fixture = Fixture::new(vec![
        ("cards", page.clone()),
        ("cards", json!({"cards":[],"cursor":{"total":0}})),
    ]);
    assert_eq!(
        WbReportSource::new(fixture)
            .seller_size_catalog()
            .await
            .unwrap()
            .len(),
        100
    );
    let fixture = Fixture::new(vec![("cards", page.clone()), ("cards", page)]);
    assert!(
        WbReportSource::new(fixture)
            .seller_size_catalog()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn seller_transport_uses_fixed_reads_and_the_documented_content_cursor() {
    use crate::{
        test_support::mock_http,
        wb::{WbClient, WbCredentials},
    };
    let (base, requests) = mock_http(vec![
        (200, "[]".into()),
        (200, "{}".into()),
        (200, "{}".into()),
    ]);
    let client = WbClient::new_for_test(
        std::time::Duration::from_secs(2),
        BTreeMap::from([(
            "account".into(),
            WbCredentials {
                token: "test-token".into(),
            },
        )]),
        &base,
        &base,
    );
    let transport = WbClientReportTransport::new(client, "account".into());
    transport
        .seller_inventory(SellerStockRequest::Warehouses)
        .await
        .unwrap();
    transport
        .seller_inventory(SellerStockRequest::Cards {
            cursor: Some(("2026-09-13T00:00:00Z".into(), 100)),
        })
        .await
        .unwrap();
    transport
        .seller_inventory(SellerStockRequest::Stocks {
            warehouse_id: 10,
            chrt_ids: vec![100, 101],
        })
        .await
        .unwrap();
    assert!(
        requests
            .recv()
            .unwrap()
            .starts_with("GET /api/v3/warehouses HTTP/1.1")
    );
    let cards = requests.recv().unwrap();
    assert!(cards.starts_with("POST /content/v2/get/cards/list HTTP/1.1"));
    let body: Value = serde_json::from_str(cards.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(
        body,
        json!({"settings":{"sort":{"ascending":true},"cursor":{"limit":100,"updatedAt":"2026-09-13T00:00:00Z","nmID":100},"filter":{"withPhoto":-1}}})
    );
    let stocks = requests.recv().unwrap();
    assert!(stocks.starts_with("POST /api/v3/stocks/10 HTTP/1.1"));
    assert_eq!(
        serde_json::from_str::<Value>(stocks.split_once("\r\n\r\n").unwrap().1).unwrap(),
        json!({"chrtIds":[100,101]})
    );
}
