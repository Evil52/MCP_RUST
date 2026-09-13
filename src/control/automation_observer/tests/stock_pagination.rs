use std::sync::mpsc;

use super::*;
use crate::control::automation::WbAutomationDisableReason;

fn full_stock_page() -> Value {
    let rows = (0..100)
        .map(|index| {
            serde_json::json!({
                "nmId": if index < 50 { 449_627_598_u64 } else { 449_627_015_u64 },
                "warehouseId": 1,
                "chrtId": index + 1,
                "quantity": 1
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({"data": {"items": rows}})
}

async fn observe_with_pages(
    pages: Vec<(u16, String)>,
) -> (Result<WbAutomationSnapshot>, mpsc::Receiver<String>) {
    let fixture = Fixture::new();
    let mut observer = fixture.observer(None);
    let mut campaign = campaign_response();
    campaign["adverts"][0]["nm_settings"][2]["bids_kopecks"]["search"] = serde_json::json!(110);
    let mut responses = vec![
        (200, campaign.to_string()),
        (200, minimum_bids_response().to_string()),
        (200, serde_json::json!({"total": 1_000}).to_string()),
        (200, stats_response().to_string()),
    ];
    responses.extend(pages);
    let (url, requests) = mock_http(responses);
    install_test_client(&mut observer, &url);
    let snapshot = observer
        .observe(
            Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, 0).unwrap(),
            WbAutomationStateView::default(),
        )
        .await;
    (snapshot, requests)
}

fn stock_requests(requests: &mpsc::Receiver<String>) -> Vec<Value> {
    requests
        .try_iter()
        .skip(4)
        .map(|request| {
            assert!(request.starts_with("POST /api/analytics/v1/stocks-report/wb-warehouses "));
            serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
        })
        .collect()
}

#[tokio::test]
async fn sku_on_second_page_does_not_become_zero_stock() {
    let second_page = serde_json::json!({"data": {"items": [
        {"nmId": 449_627_598_u64, "warehouseId": 1, "quantity": 3},
        {"nmId": 497_424_314_u64, "warehouseId": 1, "quantity": 8}
    ]}});
    let (snapshot, requests) = observe_with_pages(vec![
        (200, full_stock_page().to_string()),
        (200, second_page.to_string()),
    ])
    .await;
    let snapshot = snapshot.unwrap();
    let stocks = snapshot
        .observation
        .skus
        .iter()
        .map(|sku| (sku.nm_id, sku.sellable_stock))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(stocks[&449_627_598], 53);
    assert_eq!(stocks[&449_627_015], 50);
    assert_eq!(stocks[&497_424_314], 8);
    assert!(!matches!(
        snapshot.decision.action,
        WbAutomationAction::DisableSku {
            reason: WbAutomationDisableReason::LowStock,
            ..
        }
    ));
    let requests = stock_requests(&requests);
    assert_eq!(requests.len(), 2);
    for (request, offset) in requests.iter().zip([0, 100]) {
        assert_eq!(request["offset"], offset);
        assert_eq!(request["limit"], 100);
        assert_eq!(request["nmIds"], serde_json::json!(policy_fixture().nm_ids));
        assert_eq!(request["chrtIds"], serde_json::json!([]));
    }
}

#[tokio::test]
async fn terminal_empty_page_allows_confirmed_zero_stock() {
    let (snapshot, requests) = observe_with_pages(vec![
        (200, full_stock_page().to_string()),
        (200, serde_json::json!({"data": {"items": []}}).to_string()),
    ])
    .await;
    assert!(matches!(
        snapshot.unwrap().decision.action,
        WbAutomationAction::DisableSku {
            nm_id: 497_424_314,
            reason: WbAutomationDisableReason::LowStock,
        }
    ));
    assert_eq!(stock_requests(&requests).len(), 2);
}

#[tokio::test]
async fn page_limit_prevents_decisions_from_incomplete_stocks() {
    let page_limit = super::super::stocks::MAX_STOCK_PAGES;
    let pages = (0..page_limit)
        .map(|_| (200, full_stock_page().to_string()))
        .collect();
    let (snapshot, requests) = observe_with_pages(pages).await;
    assert!(snapshot.unwrap_err().to_string().contains("лимит страниц"));
    let requests = stock_requests(&requests);
    assert_eq!(requests.len(), usize::try_from(page_limit).unwrap());
    assert_eq!(requests.last().unwrap()["offset"], (page_limit - 1) * 100);
}

#[tokio::test]
async fn failed_or_invalid_later_page_prevents_stock_decisions() {
    let oversized = serde_json::json!({"data": {"items": vec![
        serde_json::json!({"nmId": 497_424_314_u64, "warehouseId": 1, "quantity": 8}); 101
    ]}});
    for (status, body, expected_error) in [
        (403, "{}".to_owned(), "недоступен"),
        (200, "{}".to_owned(), "неверную форму"),
        (200, oversized.to_string(), "размер страницы"),
    ] {
        let (snapshot, requests) =
            observe_with_pages(vec![(200, full_stock_page().to_string()), (status, body)]).await;
        assert!(snapshot.unwrap_err().to_string().contains(expected_error));
        assert_eq!(stock_requests(&requests).len(), 2);
    }
}
