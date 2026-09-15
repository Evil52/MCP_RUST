use super::*;

#[test]
fn foreign_campaign_or_date_still_fail_closed() {
    let current_date = NaiveDate::from_ymd_opt(2026, 8, 25).unwrap();
    let previous_date = current_date.pred_opt().unwrap();
    for (campaign_id, date) in [
        (39_682_634_u64, "2026-08-25"),
        (39_682_633_u64, "2026-08-23"),
    ] {
        let facts = parse_promotion_stats(&serde_json::json!([{
            "advertId": campaign_id,
            "stats": [{"date": date, "nm_id": 777_u64, "views": 1, "clicks": 0,
                "sum": 0, "orders": 0, "sumPrice": 0}]
        }]))
        .unwrap();
        assert!(
            validate_advertising_scope(&facts, 39_682_633, current_date, previous_date).is_err()
        );
    }
}

#[tokio::test]
async fn inactive_fullstats_skus_are_excluded_from_bids_but_count_towards_daily_spend() {
    let fixture = Fixture::new();
    let mut observer = fixture.observer(None);
    let inactive_nm_id = 777_777_777_u64;
    let stats_with_inactive_sku = serde_json::json!([{
        "advertId": 39_682_633,
        "stats": [
            {"date": "2026-08-24", "nm_id": 449_627_598_u64, "views": 100, "clicks": 10, "sum": 2, "orders": 1, "sumPrice": 100},
            {"date": "2026-08-24", "nm_id": inactive_nm_id, "views": 50, "clicks": 5, "sum": 3, "orders": 1, "sumPrice": 90},
            {"date": "2026-08-25", "nm_id": 449_627_598_u64, "views": 5, "clicks": 1, "sum": 1.5, "orders": 0, "sumPrice": 0},
            {"date": "2026-08-25", "nm_id": inactive_nm_id, "views": 8, "clicks": 2, "sum": 2.5, "orders": 0, "sumPrice": 0}
        ]
    }]);
    let (base_url, _) = mock_http(vec![
        (200, campaign_response().to_string()),
        (200, minimum_bids_response().to_string()),
        (200, serde_json::json!({"total": 1_000}).to_string()),
        (200, stats_with_inactive_sku.to_string()),
        (200, stocks_response().to_string()),
    ]);
    install_test_client(&mut observer, &base_url);

    let snapshot = observer
        .observe(
            Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, 0).unwrap(),
            WbAutomationStateView::default(),
        )
        .await
        .unwrap();

    assert_eq!(snapshot.observation.daily_spend_minor, 400);
    assert!(snapshot.observation.daily_spend_complete);
    assert!(snapshot.observation.attribution_complete);
    assert_eq!(snapshot.observation.skus.len(), 3);
    assert!(
        snapshot
            .observation
            .skus
            .iter()
            .all(|sku| sku.nm_id != inactive_nm_id)
    );
    assert_eq!(snapshot.observation.skus[0].impressions, 100);
    assert_eq!(snapshot.observation.skus[0].spend_minor, 200);
}
