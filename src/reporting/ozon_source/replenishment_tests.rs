use super::*;
use crate::reporting::checkpoint::{
    CheckpointError,
    tests::{MemoryPages, journal},
};
use std::{collections::VecDeque, sync::Mutex};
struct Transport {
    responses: Mutex<VecDeque<Value>>,
    requests: Mutex<Vec<OzonReportRequest>>,
}
impl OzonReportTransport for &Transport {
    fn post<'a>(
        &'a self,
        request: OzonReportRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Value, OzonReportSourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(request);
            Ok(self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected upstream request"))
        })
    }
}
fn transport(pages: Vec<Value>) -> Transport {
    Transport {
        responses: Mutex::new(pages.into()),
        requests: Mutex::new(vec![]),
    }
}
fn page(number: &str, status: &str, quantity: u64, next: bool) -> Value {
    json!({"postings":[{"posting_number":number,"status":status,"customer":{"phone":"secret"},"products":[{"sku":11,"quantity":quantity,"name":"untrusted text"}]}],"has_next":next,"cursor":if next {"next"} else {""}})
}
#[tokio::test]
async fn posting_resume_uses_exact_interval_and_never_persists_customer_data() {
    let t = transport(vec![
        page("a", "delivered", 3, true),
        page("b", "cancelled", 2, false),
        page("c", "awaiting_packaging", 1, false),
    ]);
    let pages = MemoryPages::default();
    let from: DateTime<Utc> = "2026-10-13T19:00:00Z".parse().unwrap();
    let to = from + Duration::days(1);
    for _ in 0..2 {
        assert_eq!(
            OzonReportSource::new(&t)
                .with_checkpoints(journal(&pages))
                .collect_posting_movement(from, to)
                .await,
            Err(OzonReportSourceError::Checkpoint(CheckpointError::Deferred))
        );
    }
    let rows = OzonReportSource::new(&t)
        .with_checkpoints(journal(&pages))
        .collect_posting_movement(from, to)
        .await
        .unwrap();
    assert_eq!(
        (
            rows[0].fbo_non_cancelled_units,
            rows[0].fbo_cancelled_units,
            rows[0].fbs_non_cancelled_units
        ),
        (3, 2, 1)
    );
    assert_eq!(
        OzonReportSource::new(&t)
            .with_checkpoints(journal(&pages))
            .collect_posting_movement(from, to)
            .await
            .unwrap(),
        rows
    );
    let requests = t.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for r in requests.iter() {
        assert_eq!(r.payload["filter"]["since"], "2026-10-13T19:00:00.000Z");
        assert_eq!(r.payload["filter"]["to"], "2026-10-14T18:59:59.999Z");
        assert_eq!(r.payload["with"]["analytics_data"], false);
    }
    drop(requests);
    let cached = serde_json::to_string(&*pages.lock().unwrap()).unwrap();
    for excluded in ["customer", "phone", "secret", "untrusted text"] {
        assert!(!cached.contains(excluded));
    }
}
#[tokio::test]
async fn posting_duplicates_across_pages_and_incomplete_cursors_fail_closed() {
    let from: DateTime<Utc> = "2026-10-13T19:00:00Z".parse().unwrap();
    let t = transport(vec![
        page("a", "delivered", 3, true),
        page("a", "delivered", 3, false),
    ]);
    assert!(
        OzonReportSource::new(&t)
            .collect_posting_movement(from, from + Duration::days(1))
            .await
            .is_err()
    );
    assert_eq!(t.requests.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn cluster_resume_keeps_missing_skus_unknown_and_drops_untrusted_contacts() {
    let t = transport(vec![
        json!({"items":[{"product_id":1,"stocks":[{"sku":11,"type":"fbo","present":7,"reserved":2},{"sku":12,"type":"fbs","present":3,"reserved":0}]}],"cursor":""}),
        json!({"items":[{"sku":11,"warehouse_id":99,"cluster_id":154,"macrolocal_cluster_id":4039,"available_stock_count":5,"ads_cluster":0.5,"customer":{"phone":"secret"}}]}),
    ]);
    let pages = MemoryPages::default();
    assert_eq!(
        OzonReportSource::new(&t)
            .with_checkpoints(journal(&pages))
            .collect_cluster_stock_pages()
            .await,
        Err(OzonReportSourceError::Checkpoint(CheckpointError::Deferred))
    );
    let rows = OzonReportSource::new(&t)
        .with_checkpoints(journal(&pages))
        .collect_cluster_stock_pages()
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].warehouse_id, None);
    assert!(rows[1].data.get("available_stock_count").is_none());
    assert!(
        !serde_json::to_string(&*pages.lock().unwrap())
            .unwrap()
            .contains("secret")
    );
    assert_eq!(
        t.requests.lock().unwrap()[1].payload["skus"],
        json!(["11", "12"])
    );
}
