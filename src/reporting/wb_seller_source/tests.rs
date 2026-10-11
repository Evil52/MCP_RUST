//! Seller inventory coverage and restart regression tests.
use super::*;
use crate::reporting::checkpoint::tests::{MemoryPages, journal};
use std::{collections::VecDeque, sync::Mutex};

#[derive(Default, Clone)]
struct Calls {
    warehouses: usize,
    cards: usize,
    card_cursors: Vec<Value>,
    stocks: Vec<(u64, Vec<u64>)>,
}
#[derive(Clone)]
struct Fixture {
    warehouses: Value,
    catalogue: Value,
    card_pages: Arc<Mutex<VecDeque<Result<Value, SourceFailure>>>>,
    replies: Arc<Mutex<VecDeque<Result<Value, SourceFailure>>>>,
    calls: Arc<Mutex<Calls>>,
}
impl Fixture {
    fn new(size_count: u64, warehouses: Value) -> Self {
        Self {
            warehouses,
            catalogue: json!({"cards":[{"nmID":7,"sizes":(1..=size_count).map(|id|json!({"chrtID":id})).collect::<Vec<_>>()}],"cursor":{"total":1}}),
            card_pages: Arc::default(),
            replies: Arc::default(),
            calls: Arc::default(),
        }
    }
    fn source(&self, checkpoints: Checkpoints) -> WbSellerSource {
        WbSellerSource {
            transport: Arc::new(self.clone()),
            checkpoints,
        }
    }
}
impl SellerTransport for Fixture {
    fn warehouses(&self) -> Fetch<'_> {
        Box::pin(async {
            self.calls.lock().unwrap().warehouses += 1;
            Ok(self.warehouses.clone())
        })
    }
    fn cards(&self, cursor: Value) -> Fetch<'_> {
        Box::pin(async move {
            {
                let mut calls = self.calls.lock().unwrap();
                calls.cards += 1;
                calls.card_cursors.push(cursor);
            }
            let page = self.card_pages.lock().unwrap().pop_front();
            page.unwrap_or_else(|| Ok(self.catalogue.clone()))
        })
    }
    fn stocks(&self, warehouse: u64, ids: Vec<u64>) -> Fetch<'_> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .stocks
                .push((warehouse, ids.clone()));
            let reply = self.replies.lock().unwrap().pop_front();
            if let Some(reply) = reply {
                return reply;
            }
            Ok(
                json!({"stocks":ids.into_iter().filter(|id| *id != 2).map(|id|json!({"chrtId":id,"amount":u64::from(id != 1)})).collect::<Vec<_>>()}),
            )
        })
    }
}

#[tokio::test]
async fn full_two_warehouse_batches_preserve_explicit_zero_and_unknown() {
    let fixture = Fixture::new(
        3_589,
        json!([{"id":20,"deliveryType":1},{"id":10,"deliveryType":1}]),
    );
    let facts = fixture.source(None).collect().await.unwrap();
    assert_eq!(facts.len(), 7_178);
    assert_eq!(
        facts
            .iter()
            .map(|f| (f.warehouse_id, f.chrt_id))
            .collect::<BTreeSet<_>>()
            .len(),
        7_178
    );
    for warehouse in [10, 20] {
        assert_eq!(
            facts
                .iter()
                .find(|f| f.warehouse_id == warehouse && f.chrt_id == 1)
                .unwrap()
                .sellable_units,
            Some(0)
        );
        assert_eq!(
            facts
                .iter()
                .find(|f| f.warehouse_id == warehouse && f.chrt_id == 2)
                .unwrap()
                .sellable_units,
            None
        );
    }
    let calls = fixture.calls.lock().unwrap().clone();
    assert_eq!(
        (calls.warehouses, calls.cards, calls.stocks.len()),
        (1, 1, 8)
    );
    for warehouse in [10, 20] {
        let batches = calls
            .stocks
            .iter()
            .filter(|(w, _)| *w == warehouse)
            .map(|(_, ids)| ids)
            .collect::<Vec<_>>();
        assert_eq!(
            batches.iter().map(|ids| ids.len()).collect::<Vec<_>>(),
            vec![1_000, 1_000, 1_000, 589]
        );
        assert_eq!(
            batches.into_iter().flatten().copied().collect::<Vec<_>>(),
            (1..=3_589).collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn failed_second_batch_resumes_without_refetching_saved_pages() {
    let fixture = Fixture::new(1_001, json!([{"id":10,"deliveryType":1}]));
    fixture.replies.lock().unwrap().extend([
        Ok(json!({"stocks":(1..=1_000).map(|id|json!({"chrtId":id,"amount":2})).collect::<Vec<_>>()})),
        Err("timeout".into()),
    ]);
    let pages = MemoryPages::default();
    for count in 1..=3 {
        assert_eq!(
            fixture
                .source(journal(&pages))
                .collect()
                .await
                .unwrap_err()
                .code,
            "checkpoint_deferred"
        );
        assert_eq!(pages.lock().unwrap().len(), count);
    }
    assert_eq!(
        fixture
            .source(journal(&pages))
            .collect()
            .await
            .unwrap_err()
            .code,
        "timeout"
    );
    assert_eq!(pages.lock().unwrap().len(), 3);
    let facts = fixture.source(journal(&pages)).collect().await.unwrap();
    assert_eq!(facts.len(), 1_001);
    assert_eq!(facts[0].sellable_units, Some(2));
    assert_eq!(facts[1_000].sellable_units, Some(1));
    assert_eq!(
        fixture.source(journal(&pages)).collect().await.unwrap(),
        facts
    );
    let calls = fixture.calls.lock().unwrap().clone();
    assert_eq!((calls.warehouses, calls.cards), (1, 1));
    assert_eq!(
        calls
            .stocks
            .iter()
            .map(|(_, ids)| ids.len())
            .collect::<Vec<_>>(),
        vec![1_000, 1, 1]
    );
}

#[tokio::test]
async fn invalid_foreign_or_duplicate_rows_never_enter_stock_checkpoint() {
    for response in [
        json!({}),
        json!({"stocks":null}),
        json!({"stocks":[{"chrtId":2,"amount":1}]}),
        json!({"stocks":[{"chrtId":1,"amount":1},{"chrtId":1,"amount":2}]}),
        json!({"stocks":[{"chrtId":1,"amount":-1}]}),
        json!({"stocks":[{"chrtId":1,"amount":2_147_483_648_u64}]}),
        json!({"stocks":[{"chrtId":1}]}),
    ] {
        let fixture = Fixture::new(1, json!([{"id":10,"deliveryType":1}]));
        fixture.replies.lock().unwrap().push_back(Ok(response));
        let pages = MemoryPages::default();
        for _ in 0..2 {
            assert_eq!(
                fixture
                    .source(journal(&pages))
                    .collect()
                    .await
                    .unwrap_err()
                    .code,
                "checkpoint_deferred"
            );
        }
        assert_eq!(
            fixture
                .source(journal(&pages))
                .collect()
                .await
                .unwrap_err()
                .code,
            "seller_invalid_response"
        );
        assert_eq!(pages.lock().unwrap().len(), 2);
    }
}

#[test]
fn catalogue_cursor_and_http_failure_contracts_are_bounded() {
    let mut page = json!({"cards":(1..=100).map(|id| json!({"nmID":id,"sizes":[{"chrtID":id}]})).collect::<Vec<_>>(), "cursor":{"total":100,"nmID":100,"updatedAt":"2026-09-14T01:00:00Z"}});
    let parsed = parse_cards(&page, CARD_LIMIT).unwrap();
    assert_eq!(parsed.items.len(), 100);
    assert_eq!(parsed.next.unwrap()["nmID"], 100);
    assert!(
        parse_cards(&json!({"cards":[],"cursor":{"total":0}}), CARD_LIMIT)
            .unwrap()
            .next
            .is_none()
    );
    page["cursor"]["total"] = json!(99);
    assert_eq!(
        parse_cards(&page, CARD_LIMIT).err().unwrap().code,
        "seller_invalid_response"
    );
    for (status, code) in [
        (400, "seller_upstream_rejected"),
        (503, "upstream_server_error"),
    ] {
        let error = WbError::Api {
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            request_id: None,
            diagnostic: String::new(),
        };
        assert_eq!(failure(&error).code, code);
    }
}

#[tokio::test]
async fn all_positive_delivery_types_keep_their_identity_and_sparse_values() {
    let fixture = Fixture::new(
        2,
        json!([
            {"id":10,"deliveryType":1}, {"id":20,"deliveryType":2},
            {"id":30,"deliveryType":7}, {"id":40,"deliveryType":2_147_483_647}
        ]),
    );
    let facts = fixture.source(None).collect().await.unwrap();
    assert_eq!(facts.len(), 8);
    for (warehouse, delivery_type) in [(10, 1), (20, 2), (30, 7), (40, 2_147_483_647)] {
        let rows = facts
            .iter()
            .filter(|fact| fact.warehouse_id == warehouse)
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|fact| fact.delivery_type == delivery_type));
        assert_eq!(rows[0].sellable_units, Some(0));
        assert_eq!(rows[1].sellable_units, None);
    }
    assert_eq!(fixture.calls.lock().unwrap().stocks.len(), 4);
}

#[tokio::test]
async fn more_than_25000_size_pairs_and_up_to_100_warehouses_are_supported() {
    let fixture = Fixture::new(
        15_000,
        json!([{"id":10,"deliveryType":1},{"id":20,"deliveryType":2}]),
    );
    let facts = fixture.source(None).collect().await.unwrap();
    assert_eq!(facts.len(), 30_000);
    assert_eq!(fixture.calls.lock().unwrap().stocks.len(), 30);
    let warehouses = json!(
        (1..=100)
            .map(|id| json!({"id":id,"deliveryType":1}))
            .collect::<Vec<_>>()
    );
    assert_eq!(parse_warehouses(&warehouses).unwrap().len(), 100);
    let too_many = json!(
        (1..=101)
            .map(|id| json!({"id":id,"deliveryType":1}))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        parse_warehouses(&too_many).err().unwrap().code,
        "seller_warehouse_limit"
    );
    let too_many_sizes = Fixture::new(25_001, json!([{"id":10,"deliveryType":1}]));
    assert_eq!(
        too_many_sizes
            .source(None)
            .collect()
            .await
            .unwrap_err()
            .code,
        "seller_invalid_response"
    );
    for value in [0_u64, 2_147_483_648, u64::MAX] {
        assert_eq!(
            parse_warehouses(&json!([{"id":10,"deliveryType":value}]))
                .err()
                .unwrap()
                .code,
            "seller_invalid_response"
        );
    }
}

#[tokio::test]
async fn sku_warehouse_aggregate_limit_is_checked_before_inventory_calls() {
    let fixture = Fixture::new(
        1,
        json!(
            (1..=100)
                .map(|id| json!({"id":id,"deliveryType":1}))
                .collect::<Vec<_>>()
        ),
    );
    for (first, count) in [(1, 100), (101, 100), (201, 51)] {
        fixture.card_pages.lock().unwrap().push_back(Ok(json!({
            "cards":(first..first+count).map(|id|json!({"nmID":id,"sizes":[{"chrtID":id}]})).collect::<Vec<_>>(),
            "cursor":{"total":count,"nmID":first+count,"updatedAt":"2026-09-14T01:00:00Z"}
        })));
    }
    assert_eq!(
        fixture.source(None).collect().await.unwrap_err().code,
        "seller_pair_limit"
    );
    assert!(fixture.calls.lock().unwrap().stocks.is_empty());
}

#[tokio::test]
async fn compact_checkpoints_retain_amount_order_without_repeated_fact_fields() {
    let fixture = Fixture::new(1_001, json!([{"id":10,"deliveryType":7}]));
    let pages = MemoryPages::default();
    for _ in 0..3 {
        assert_eq!(
            fixture
                .source(journal(&pages))
                .collect()
                .await
                .unwrap_err()
                .code,
            "checkpoint_deferred"
        );
    }
    let facts = fixture.source(journal(&pages)).collect().await.unwrap();
    assert_eq!(facts.len(), 1_001);
    assert!(facts.iter().all(|fact| fact.delivery_type == 7));
    let stored = pages.lock().unwrap();
    let amounts = stored
        .values()
        .find_map(|page| page.as_array().filter(|rows| rows.len() == 1_000))
        .unwrap();
    assert_eq!(amounts[0], json!(0));
    assert_eq!(amounts[1], Value::Null);
    assert!(
        amounts
            .iter()
            .all(|amount| amount.is_null() || amount.is_u64())
    );
    let payload_bytes = stored
        .values()
        .map(|page| page.to_string().len())
        .sum::<usize>();
    drop(stored);
    assert!(payload_bytes < 20_000);
}

struct ScopedJournal {
    pages: MemoryPages,
    scopes: Arc<Mutex<Vec<&'static str>>>,
}
impl crate::reporting::checkpoint::PageJournal for ScopedJournal {
    fn load<'a>(
        &'a self,
        key: &'a str,
    ) -> crate::reporting::checkpoint::JournalFuture<'a, Option<Value>> {
        Box::pin(async move { Ok(self.pages.lock().unwrap().get(key).cloned()) })
    }
    fn admit(&self) -> crate::reporting::checkpoint::JournalFuture<'_, ()> {
        // A seller job must never borrow the default Analytics gate.
        Box::pin(async { Err(CheckpointError::Invalid) })
    }
    fn admit_stock_page(
        &self,
        scope: StockPageScope,
    ) -> crate::reporting::checkpoint::JournalFuture<'_, ()> {
        Box::pin(async move {
            self.scopes.lock().unwrap().push(scope.quota_name());
            Ok(())
        })
    }
    fn save<'a>(
        &'a self,
        key: &'a str,
        page: Value,
    ) -> crate::reporting::checkpoint::JournalFuture<'a, ()> {
        Box::pin(async move {
            self.pages.lock().unwrap().insert(key.to_owned(), page);
            Ok(())
        })
    }
}

#[tokio::test]
async fn seller_pages_reserve_content_and_inventory_quotas_independently() {
    let fixture = Fixture::new(3, json!([{"id":10,"deliveryType":1}]));
    let scopes = Arc::new(Mutex::new(Vec::new()));
    let checkpoints = Some(Arc::new(ScopedJournal {
        pages: MemoryPages::default(),
        scopes: Arc::clone(&scopes),
    }) as Arc<dyn crate::reporting::checkpoint::PageJournal>);
    assert_eq!(
        fixture.source(checkpoints).collect().await.unwrap().len(),
        3
    );
    assert_eq!(
        *scopes.lock().unwrap(),
        vec![
            "wb_seller_inventory",
            "wb_stock_content",
            "wb_seller_inventory"
        ]
    );
}

fn card_page(first: u64, count: u64) -> Value {
    json!({
        "cards": (first..first + count)
            .map(|id| json!({"nmID":id,"sizes":[{"chrtID":id}]}))
            .collect::<Vec<_>>(),
        "cursor": {"total":count,"nmID":first + count - u64::from(count > 0),
            "updatedAt":"2026-09-14T01:00:00Z"}
    })
}

#[tokio::test]
async fn oversized_content_page_resumes_same_cursor_and_keeps_saved_pages() {
    let fixture = Fixture::new(1, json!([{"id":10,"deliveryType":1}]));
    fixture.card_pages.lock().unwrap().extend([
        Err("response_too_large".into()),
        Ok(card_page(101, 50)),
        Ok(card_page(151, 1)),
    ]);
    let pages = MemoryPages::default();
    // Simulate full-size pages saved by the production version before upgrade.
    pages.lock().unwrap().insert(
        // Stable v1 key from the existing production journal.
        "567d38b9199d8556aa01b1eb6924b4bf4dfc6dcc15aef3b0c247e1ac36cacb7f".to_owned(),
        json!({
            "items": (1..=100).map(|id| json!([id,[id]])).collect::<Vec<_>>(),
            "next": {"limit":100,"nmID":100,"updatedAt":"2026-09-14T01:00:00Z"}
        }),
    );
    let mut completed = None;
    // Every invocation has a fresh one-page quantum, like a restarted worker.
    for _ in 0..8 {
        match fixture.source(journal(&pages)).collect().await {
            Ok(facts) => {
                completed = Some(facts);
                break;
            }
            Err(error) => assert_eq!(error.code, "checkpoint_deferred"),
        }
    }
    let facts = completed.expect("smaller full page must continue to the final page");
    assert_eq!(facts.len(), 151);
    assert_eq!(facts[0].sellable_units, Some(0));
    assert_eq!(facts[1].sellable_units, None);
    assert_eq!(
        facts
            .iter()
            .map(|fact| fact.chrt_id)
            .collect::<BTreeSet<_>>(),
        (1..=151).collect()
    );
    assert_eq!(
        fixture.source(journal(&pages)).collect().await.unwrap(),
        facts
    );
    let calls = fixture.calls.lock().unwrap().clone();
    assert_eq!(
        (calls.warehouses, calls.cards, calls.stocks.len()),
        (1, 3, 1)
    );
    assert_eq!(calls.card_cursors[0]["nmID"], json!(100));
    assert_eq!(calls.card_cursors[1]["nmID"], json!(100));
    assert_eq!(calls.card_cursors[1]["limit"], json!(50));
    assert_eq!(calls.card_cursors[2]["nmID"], json!(150));
    assert_eq!(calls.card_cursors[2]["limit"], json!(50));
    let old_page: Option<CardPage> = load_checkpoint(
        &journal(&pages),
        json!(["wb_seller_cards_v1", calls.card_cursors[0]]),
    )
    .await
    .unwrap();
    assert!(
        old_page.is_none(),
        "rollback must not see adaptive continuations"
    );
}

#[tokio::test]
async fn oversized_single_card_remains_an_error_without_inventory_publication() {
    let fixture = Fixture::new(1, json!([{"id":10,"deliveryType":1}]));
    fixture
        .card_pages
        .lock()
        .unwrap()
        .extend((0..7).map(|_| Err(SourceFailure::from("response_too_large"))));
    let error = fixture.source(None).collect().await.unwrap_err();
    assert_eq!(error.code, "response_too_large");
    let calls = fixture.calls.lock().unwrap();
    assert_eq!(
        calls
            .card_cursors
            .iter()
            .map(|cursor| cursor["limit"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![100, 50, 25, 12, 6, 3, 1]
    );
    assert!(calls.stocks.is_empty());
}

#[tokio::test]
async fn shrinking_pages_preserves_the_catalogue_bound_beyond_251_pages() {
    let fixture = Fixture::new(1, json!([{"id":10,"deliveryType":1}]));
    {
        let mut pages = fixture.card_pages.lock().unwrap();
        pages.extend((0..6).map(|_| Err(SourceFailure::from("response_too_large"))));
        pages.extend((1..=260).map(|id| Ok(card_page(id, 1))));
        pages.push_back(Ok(card_page(261, 0)));
    }
    let facts = fixture.source(None).collect().await.unwrap();
    assert_eq!(facts.len(), 260);
    assert_eq!(fixture.calls.lock().unwrap().cards, 267);
    assert_eq!(facts.last().unwrap().chrt_id, 260);
}

#[tokio::test]
async fn content_errors_other_than_response_size_do_not_shrink_or_cache_a_page() {
    for failure in [
        SourceFailure {
            code: "rate_limited",
            retry_after: Some(120),
        },
        "forbidden".into(),
        "timeout".into(),
    ] {
        let fixture = Fixture::new(1, json!([{"id":10,"deliveryType":1}]));
        fixture.card_pages.lock().unwrap().push_back(Err(failure));
        let pages = MemoryPages::default();
        assert_eq!(
            fixture
                .source(journal(&pages))
                .collect()
                .await
                .unwrap_err()
                .code,
            "checkpoint_deferred"
        );
        let error = fixture.source(journal(&pages)).collect().await.unwrap_err();
        assert_eq!(
            (error.code, error.retry_after),
            (failure.code, failure.retry_after)
        );
        assert_eq!(pages.lock().unwrap().len(), 1);
        assert_eq!(fixture.calls.lock().unwrap().cards, 1);
    }
}

#[test]
fn reduced_page_limit_is_validated_and_full_reduced_pages_keep_the_cursor() {
    let page = parse_cards(&card_page(1, 50), 50).unwrap();
    assert_eq!(page.items.len(), 50);
    assert_eq!(page.next.unwrap()["limit"], json!(50));
    assert!(parse_cards(&card_page(1, 49), 50).unwrap().next.is_none());
    assert_eq!(
        parse_cards(&card_page(1, 51), 50).err().unwrap().code,
        "seller_invalid_response"
    );
}
