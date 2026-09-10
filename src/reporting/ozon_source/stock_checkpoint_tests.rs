use std::{collections::VecDeque, sync::Mutex};

use super::*;
use crate::reporting::checkpoint::tests::{MemoryPages, journal};

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
                    {"type": "fbs", "present": 2, "reserved": 0},
                    {"type": "rfbs", "present": 7, "reserved": 0}
                ]
            }], "cursor": "next-rfbs-page"})),
            Ok(json!({"items": [{
                "product_id": 456,
                "stocks": [{"type": "rfbs", "present": 0, "reserved": 0}]
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
                sku: 123,
                warehouse_id: "FBS".to_owned(),
                sellable_units: 2
            },
            CollectedStockFact {
                sku: 123,
                warehouse_id: "RFBS".to_owned(),
                sellable_units: 7
            },
            CollectedStockFact {
                sku: 456,
                warehouse_id: "RFBS".to_owned(),
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
