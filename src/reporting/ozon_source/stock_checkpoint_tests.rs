use std::{collections::VecDeque, sync::Mutex};

use super::*;
use crate::reporting::checkpoint::{
    CheckpointError,
    tests::{MemoryPages, journal},
};

struct RecordingTransport {
    responses: Mutex<VecDeque<Result<Value, OzonReportSourceError>>>,
    paths: Mutex<Vec<&'static str>>,
}

impl OzonReportTransport for RecordingTransport {
    fn post<'a>(
        &'a self,
        request: OzonReportRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.paths.lock().unwrap().push(request.path);
            self.responses.lock().unwrap().pop_front().unwrap()
        })
    }
}

#[tokio::test]
async fn retired_stock_route_is_checkpointed_before_resuming_fallback() {
    let transport = RecordingTransport {
        responses: Mutex::new(VecDeque::from([
            Err(OzonReportSourceError::Upstream(OzonErrorKind::NotFound)),
            Ok(json!({"items":[],"cursor":""})),
        ])),
        paths: Mutex::new(vec![]),
    };
    let pages = MemoryPages::default();
    assert_eq!(
        OzonReportSource::new(&transport)
            .with_checkpoints(journal(&pages))
            .collect_stock_pages()
            .await,
        Err(OzonReportSourceError::Checkpoint(CheckpointError::Deferred))
    );
    assert!(
        OzonReportSource::new(&transport)
            .with_checkpoints(journal(&pages))
            .collect_stock_pages()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(transport.paths.lock().unwrap().len(), 2);
    assert!(transport.responses.lock().unwrap().is_empty());
}

#[tokio::test]
async fn rfbs_fallback_resumes_normalized_pages_without_repeating_requests() {
    let transport = RecordingTransport {
        responses: Mutex::new(VecDeque::from([
            Err(OzonReportSourceError::Upstream(OzonErrorKind::NotFound)),
            Ok(json!({"items": [{
                "product_id": 123,
                "stocks": [
                    {"type": "fbs", "sku": 1123, "present": 2, "reserved": 1},
                    {"type": "rfbs", "sku": 1123, "present": 7, "reserved": 2}
                ]
            }], "cursor": "next-rfbs-page"})),
            Ok(json!({"items": [{
                "product_id": 456,
                "stocks": [{"type": "rfbs", "sku": 1456, "present": 0, "reserved": 0}]
            }], "cursor": ""})),
        ])),
        paths: Mutex::new(vec![]),
    };
    let pages = MemoryPages::default();
    // The first quantum persists the unavailable legacy route; the next
    // persists the first fallback page. Both must survive reconstruction.
    for _ in 0..2 {
        assert_eq!(
            OzonReportSource::new(&transport)
                .with_checkpoints(journal(&pages))
                .collect_stock_pages()
                .await,
            Err(OzonReportSourceError::Checkpoint(CheckpointError::Deferred))
        );
    }
    let facts = OzonReportSource::new(&transport)
        .with_checkpoints(journal(&pages))
        .collect_stock_pages()
        .await
        .unwrap();
    assert_eq!(
        facts,
        vec![
            CollectedStockFact {
                sku: 1123,
                warehouse_id: "sku-fulfillment-v2:fbs".to_owned(),
                sellable_units: 1
            },
            CollectedStockFact {
                sku: 1123,
                warehouse_id: "sku-fulfillment-v2:rfbs".to_owned(),
                sellable_units: 5
            },
            CollectedStockFact {
                sku: 1456,
                warehouse_id: "sku-fulfillment-v2:rfbs".to_owned(),
                sellable_units: 0
            },
        ]
    );
    assert_eq!(
        transport.paths.lock().unwrap().as_slice(),
        [
            "/v1/product/info/stocks-by-warehouse/fbo",
            "/v4/product/info/stocks",
            "/v4/product/info/stocks",
        ]
    );
    assert!(transport.responses.lock().unwrap().is_empty());
    assert_eq!(pages.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn legacy_stock_checkpoint_is_not_replayed_as_corrected_sku_inventory() {
    let pages = MemoryPages::default();
    let request = product_page_request("/v4/product/info/stocks", None).unwrap();
    let legacy_rows = vec![CollectedStockFact {
        sku: 123,
        warehouse_id: "FBS".to_owned(),
        sellable_units: 7,
    }];
    checkpointed(
        &journal(&pages),
        json!([request.path, request.payload]),
        || async { Ok::<_, CheckpointError>((legacy_rows, None::<String>)) },
    )
    .await
    .unwrap();
    let transport = RecordingTransport {
        responses: Mutex::new(VecDeque::from([
            Err(OzonReportSourceError::Upstream(OzonErrorKind::NotFound)),
            Ok(json!({"items": [{"product_id": 123, "stocks": [
                {"sku": 456, "type": "fbs", "present": 7, "reserved": 1}
            ]}], "cursor": ""})),
        ])),
        paths: Mutex::new(vec![]),
    };
    assert_eq!(
        OzonReportSource::new(&transport)
            .with_checkpoints(journal(&pages))
            .collect_stock_pages()
            .await,
        Err(OzonReportSourceError::Checkpoint(CheckpointError::Deferred))
    );
    let fresh = OzonReportSource::new(&transport)
        .with_checkpoints(journal(&pages))
        .collect_stock_pages()
        .await
        .unwrap();
    assert_eq!(
        fresh,
        vec![CollectedStockFact {
            sku: 456,
            warehouse_id: "sku-fulfillment-v2:fbs".to_owned(),
            sellable_units: 6,
        }]
    );
    // The corrected page is now resumable without another upstream request.
    assert_eq!(
        OzonReportSource::new(&transport)
            .with_checkpoints(journal(&pages))
            .collect_stock_pages()
            .await
            .unwrap(),
        fresh
    );
    assert_eq!(transport.paths.lock().unwrap().len(), 2);
    assert!(transport.responses.lock().unwrap().is_empty());
    assert_eq!(pages.lock().unwrap().len(), 3);
}
