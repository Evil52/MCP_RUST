use super::*;
use crate::server::tools::wb_ads_recommendations::WbAdsRecommendationsInput;

fn input() -> WbAdsRecommendationsInput {
    WbAdsRecommendationsInput {
        account: Some("account_wb".into()),
        campaign_ids: vec![11],
        days: 14,
        target_drr_bps: Some(1500),
        limit: 50,
    }
}

#[tokio::test]
async fn recommendations_deny_foreign_accounts_and_revoked_identity_before_io() {
    let reads = Arc::new(FakeReportingRepository::succeeding());
    let (server, requests) = mock_wb_server_for("manager", 0);
    let server = server.with_reporting_reader(ReportingReader::from_repository(reads.clone()));
    assert!(
        server
            .wb_ads_recommendations(RequestIdentity::dev(), Parameters(input()))
            .await
            .is_err()
    );
    assert!(
        server
            .wb_ads_recommendations(RequestIdentity::authenticated("ghost"), Parameters(input()))
            .await
            .is_err()
    );
    assert_eq!(reads.calls(), 0);
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn recommendations_validate_scope_before_database_or_marketplace_io() {
    let reads = Arc::new(FakeReportingRepository::succeeding());
    let (server, requests) = mock_wb_server_for("admin", 0);
    let server = server.with_reporting_reader(ReportingReader::from_repository(reads.clone()));
    let mut bad_window = input();
    bad_window.days = 31;
    let mut bad_target = input();
    bad_target.target_drr_bps = Some(0);
    let mut duplicate = input();
    duplicate.campaign_ids = vec![11, 11];
    let mut too_many = input();
    too_many.campaign_ids = (1..=11).collect();
    let mut bad_page = input();
    bad_page.limit = 101;
    for invalid in [bad_window, bad_target, duplicate, too_many, bad_page] {
        assert!(
            server
                .wb_ads_recommendations(RequestIdentity::dev(), Parameters(invalid))
                .await
                .is_err()
        );
    }
    assert_eq!(reads.calls(), 0);
    assert!(requests.try_recv().is_err());
}

#[tokio::test]
async fn recommendations_read_live_scope_once_and_keep_missing_fbw_unknown() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../config/wb-ads-recommendations.example.json"
    ))
    .unwrap();
    let mut statistics = fixture["statistics"].clone();
    let closed = crate::reporting::advertising_history::last_closed_day();
    for (index, day) in statistics[0]["days"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .enumerate()
    {
        day["date"] = json!(format!(
            "{}T00:00:00Z",
            closed - chrono::Duration::days(i64::try_from(index).unwrap() + 4)
        ));
    }
    let (server, requests) = mock_wb_server_with_responses(
        "admin",
        vec![
            (200, fixture["composition"].to_string()),
            (200, statistics.to_string()),
        ],
    );
    let reads = Arc::new(FakeReportingRepository::succeeding());
    let server = server.with_reporting_reader(ReportingReader::from_repository(reads.clone()));
    let report = server
        .wb_ads_recommendations(RequestIdentity::dev(), Parameters(input()))
        .await
        .unwrap()
        .0;
    assert_eq!(report.account_id, "account_wb");
    assert_eq!(report.recommendations[0].fbw_units, None);
    assert_eq!(
        report.recommendations[0].windows[1].metrics.spend_minor,
        15000
    );
    assert_eq!(
        report.recommendations[0].action,
        crate::reporting::ads_optimizer::wb_recommendations::Action::Hold
    );
    assert!(!report.auto_apply_allowed);
    assert_eq!(reads.calls(), 2);
    let details = requests.recv_timeout(Duration::from_secs(3)).unwrap();
    let history = requests.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(details.starts_with("GET /api/advert/v2/adverts?"));
    assert!(history.starts_with("GET /adv/v3/fullstats?"));
    assert!(requests.try_recv().is_err());
}

#[test]
fn recommendations_are_live_read_only_and_absent_in_credentialless_reporting_mode() {
    let tool = server()
        .tool_router
        .list_all()
        .into_iter()
        .find(|tool| tool.name == "wb_ads_recommendations")
        .unwrap();
    assert_eq!(tool.annotations.unwrap().read_only_hint, Some(true));
    assert_eq!(tool.input_schema["additionalProperties"], json!(false));
    assert!(
        !server()
            .into_reporting_only()
            .unwrap()
            .tool_router
            .list_all()
            .iter()
            .any(|tool| tool.name == "wb_ads_recommendations")
    );
}
