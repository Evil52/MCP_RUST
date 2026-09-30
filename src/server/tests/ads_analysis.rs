use super::*;
use crate::server::tools::ads_analysis::AdsAnalysisInput;

fn input() -> AdsAnalysisInput {
    AdsAnalysisInput {
        store: Some(StoreId::from("store_a")),
        campaign_ids: vec![11, 22],
        date_from: "2026-09-01".to_owned(),
        date_to: "2026-09-02".to_owned(),
        target_drr_bps: Some(1500),
    }
}

#[tokio::test]
async fn live_ads_analysis_uses_authorized_daily_read_and_preserves_missing_data() {
    let (server, requests) = performance_mock_server("admin", vec![
        (200, performance_token_response()),
        (200, json!({"rows":[{"id":"11","date":"2026-09-01","title":"ignored", "views":"100","clicks":"10","moneySpent":"20,00","orders":"2","ordersMoney":"100,00"}]}).to_string()),
    ]);
    let result = server
        .ads_analysis(RequestIdentity::dev(), Parameters(input()))
        .await
        .unwrap()
        .0;
    assert_eq!(result.store, StoreId::from("store_a"));
    assert_eq!(result.endpoint, DAILY_STATS_PATH);
    assert_eq!(result.data["campaigns"][0]["spend_minor"], 2000);
    assert_eq!(result.data["missing_campaign_ids"], json!([22]));
    assert_eq!(result.data["auto_apply_allowed"], false);
    assert_eq!(result.data["mode"], "diagnostic_only");
    assert_eq!(result.data_classification, UNTRUSTED_DATA_CLASSIFICATION);
    assert!(
        requests
            .recv_timeout(Duration::from_secs(3))
            .unwrap()
            .starts_with("POST /api/client/token ")
    );
    let request = requests.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(
        request.lines().next().unwrap(),
        "GET /api/client/statistics/daily/json?campaignIds=11&campaignIds=22&dateFrom=2026-09-01&dateTo=2026-09-02 HTTP/1.1"
    );
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn live_ads_analysis_rejects_revoked_identity_and_foreign_store_before_io() {
    let (server, requests) = performance_mock_server("admin", Vec::new());
    assert!(
        server
            .ads_analysis(RequestIdentity::authenticated("ghost"), Parameters(input()))
            .await
            .is_err()
    );
    let (manager, manager_requests) = performance_mock_server("manager", Vec::new());
    assert!(
        manager
            .ads_analysis(RequestIdentity::dev(), Parameters(input()))
            .await
            .is_err()
    );
    assert!(requests.try_recv().is_err());
    assert!(manager_requests.try_recv().is_err());
    let (restricted, restricted_requests) = performance_mock_server("finance_denied", Vec::new());
    assert!(
        restricted
            .ads_analysis(RequestIdentity::dev(), Parameters(input()))
            .await
            .is_err()
    );
    assert!(restricted_requests.try_recv().is_err());
}

#[tokio::test]
async fn live_ads_analysis_validates_bounds_before_io() {
    let (server, requests) = performance_mock_server("admin", Vec::new());
    let mut empty = input();
    empty.campaign_ids.clear();
    let mut duplicate = input();
    duplicate.campaign_ids = vec![11, 11];
    let mut invalid_target = input();
    invalid_target.target_drr_bps = Some(10001);
    let mut long_window = input();
    long_window.date_to = "2026-10-02".to_owned();
    for invalid in [empty, duplicate, invalid_target, long_window] {
        assert!(
            server
                .ads_analysis(RequestIdentity::dev(), Parameters(invalid))
                .await
                .is_err()
        );
    }
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn live_ads_analysis_is_registered_and_returns_diagnostics_over_mcp() {
    let (server, requests) = performance_mock_server(
        "admin",
        vec![
            (200, performance_token_response()),
            (200, json!({"rows":[]}).to_string()),
        ],
    );
    let response = call_tool_over_http(server, "ozon_ads_analysis", json!({"store":"store_a", "campaign_ids":[11,22], "date_from":"2026-09-01", "date_to":"2026-09-02"})).await;
    assert!(response.contains("diagnostic_only"), "{response}");
    assert!(response.contains("missing_campaign_ids"), "{response}");
    assert!(requests.recv_timeout(Duration::from_secs(3)).is_ok());
    assert!(requests.recv_timeout(Duration::from_secs(3)).is_ok());
}
