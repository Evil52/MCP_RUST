use super::*;
use std::{collections::VecDeque, sync::Mutex};

struct Transport {
    responses: Mutex<VecDeque<Result<Value, OzonReportSourceError>>>,
    requests: Mutex<Vec<OzonReportRequest>>,
}
impl OzonReportTransport for &Transport {
    fn post<'a>(
        &'a self,
        request: OzonReportRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(request);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("no extra upstream requests")
        })
    }
}
fn catalog() -> Value {
    json!({"items":[{"product_id":123,"stocks":[{"sku":11,"type":"fbo","present":7,"reserved":2},{"sku":12,"type":"fbs","present":0,"reserved":0}]}],"cursor":""})
}
fn transport(responses: Vec<Result<Value, OzonReportSourceError>>) -> Transport {
    Transport {
        responses: Mutex::new(responses.into()),
        requests: Mutex::new(vec![]),
    }
}
#[tokio::test]
async fn detailed_stocks_enumerate_skus_and_keep_reserves_and_missing_warehouse_scope() {
    let t = transport(vec![
        Ok(catalog()),
        Ok(
            json!({"products":[{"sku":11,"warehouse_id":99,"present":7,"reserved":2}],"has_next":false,"cursor":""}),
        ),
        Ok(json!({"products":[],"has_next":false,"cursor":""})),
    ]);
    let rows = OzonReportSource::new(&t)
        .collect_detailed_stock_pages()
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].warehouse_id, "fbo:99");
    assert_eq!(
        (
            rows[0].present_units,
            rows[0].reserved_units,
            rows[0].sellable_units
        ),
        (Some(7), Some(2), 5)
    );
    assert_eq!(rows[1].warehouse_id, "sku-fulfillment-v2:fbs");
    assert_eq!(rows[1].reserved_units, Some(0));
    let requests = t.requests.lock().unwrap();
    assert_eq!(requests[0].path, "/v4/product/info/stocks");
    assert_eq!(requests[1].payload["skus"], json!([11]));
    assert_eq!(requests[2].payload["sku"], json!([12]));
}
#[tokio::test]
async fn warehouse_replies_cannot_escape_the_requested_catalogue_scope() {
    let t = transport(vec![
        Ok(catalog()),
        Ok(
            json!({"products":[{"sku":999,"warehouse_id":99,"present":7,"reserved":2}],"has_next":false,"cursor":""}),
        ),
    ]);
    assert_eq!(
        OzonReportSource::new(&t)
            .collect_detailed_stock_pages()
            .await,
        Err(OzonReportSourceError::InvalidStocksResponse)
    );
    assert_eq!(t.requests.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn warehouse_timeout_does_not_publish_fulfillment_fallback_or_continue() {
    let t = transport(vec![
        Ok(catalog()),
        Err(OzonReportSourceError::Upstream(OzonErrorKind::Timeout)),
    ]);
    assert_eq!(
        OzonReportSource::new(&t)
            .collect_detailed_stock_pages()
            .await,
        Err(OzonReportSourceError::Upstream(OzonErrorKind::Timeout))
    );
    assert_eq!(t.requests.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn old_reserve_free_checkpoints_are_not_replayed_by_the_new_collector() {
    use crate::reporting::checkpoint::CheckpointError;
    use crate::reporting::checkpoint::tests::{MemoryPages, journal};
    let pages = MemoryPages::default();
    let request = product_page_request("/v4/product/info/stocks", None).unwrap();
    checkpointed(
        &journal(&pages),
        json!(["ozon-sku-fulfillment-v2", request.path, request.payload]),
        || async { Ok::<_, CheckpointError>((Vec::<CollectedStockFact>::new(), None::<String>)) },
    )
    .await
    .unwrap();
    let t = transport(vec![
        Ok(catalog()),
        Ok(json!({"products":[],"has_next":false,"cursor":""})),
        Ok(json!({"products":[],"has_next":false,"cursor":""})),
    ]);
    for _ in 0..2 {
        assert_eq!(
            OzonReportSource::new(&t)
                .with_checkpoints(journal(&pages))
                .collect_detailed_stock_pages()
                .await,
            Err(OzonReportSourceError::Checkpoint(CheckpointError::Deferred))
        );
    }
    let rows = OzonReportSource::new(&t)
        .with_checkpoints(journal(&pages))
        .collect_detailed_stock_pages()
        .await
        .unwrap();
    assert_eq!(rows[0].reserved_units, Some(2));
    assert_eq!(
        OzonReportSource::new(&t)
            .with_checkpoints(journal(&pages))
            .collect_detailed_stock_pages()
            .await
            .unwrap(),
        rows
    );
    assert_eq!(t.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn native_free_stock_does_not_discard_the_explicit_scheme_reserve() {
    let t = transport(vec![
        Ok(
            json!({"items":[{"product_id":123,"stocks":[{"sku":12,"type":"fbs","present":7,"reserved":2}]}],"cursor":""}),
        ),
        Ok(
            json!({"products":[{"sku":12,"warehouse_id":99,"present":7,"free_stock":4}],"has_next":false,"cursor":""}),
        ),
    ]);
    let rows = OzonReportSource::new(&t)
        .collect_detailed_stock_pages()
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].warehouse_id, "sku-fulfillment-v2:fbs");
    assert_eq!(
        (
            rows[0].present_units,
            rows[0].reserved_units,
            rows[0].sellable_units
        ),
        (Some(7), Some(2), 5)
    );
}
