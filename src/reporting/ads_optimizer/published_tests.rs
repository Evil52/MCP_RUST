use super::*;

fn date(day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, day).unwrap()
}

fn scope() -> CpcCampaignScope {
    CpcCampaignScope::new(
        "store_1".to_owned(),
        123,
        BTreeSet::from([10, 20]),
        "campaign-settings-snapshot-7".to_owned(),
    )
    .unwrap()
}

fn fact(day: u32, campaign_id: u64) -> PublishedAdvertisingFact {
    PublishedAdvertisingFact {
        account_id: "store_1".to_owned(),
        business_date: date(day),
        campaign_id,
        sku: 123,
        impressions: 100,
        clicks: 10,
        spend_minor: 200,
        attributed_orders: 2,
        attributed_revenue_minor: 3_000,
        basket_additions: 5,
        model_attributed_orders: 4,
        model_attributed_revenue_minor: 6_000,
        product_price_minor: 1_500,
        average_cpc_minor: Some(20),
        cpm_minor: Some(2_000),
        cpl_minor: Some(40),
    }
}

#[test]
fn sums_two_campaigns_by_day_in_sorted_order() {
    let facts = [fact(3, 10), fact(1, 20), fact(1, 10)];
    let days = aggregate_cpc_days(&facts, &scope(), date(1), date(3)).unwrap();
    assert_eq!(days.len(), 2);
    assert_eq!(days[0].date, date(1));
    assert_eq!(days[0].clicks, 20);
    assert_eq!(days[0].spend_minor, 400);
    assert_eq!(days[0].direct_orders, 4);
    assert_eq!(days[0].direct_revenue_minor, 6_000);
    assert_eq!(days[1].date, date(3));
    assert_eq!(days[1].clicks, 10);
}

#[test]
fn rejects_duplicate_campaign_sku_date_even_when_counters_match() {
    assert!(matches!(
        aggregate_cpc_days(&[fact(1, 10), fact(1, 10)], &scope(), date(1), date(1)),
        Err(OptimizerError::DuplicateEvidence)
    ));
}

#[test]
fn rejects_foreign_account_sku_and_campaign_wide_sentinel() {
    let mut foreign_account = fact(1, 10);
    foreign_account.account_id = "store_2".to_owned();
    let mut foreign_sku = fact(1, 10);
    foreign_sku.sku = 456;
    let mut campaign_total = fact(1, 10);
    campaign_total.sku = 0;
    for row in [foreign_account, foreign_sku, campaign_total] {
        assert!(matches!(
            aggregate_cpc_days(&[row], &scope(), date(1), date(1)),
            Err(OptimizerError::InvalidInput)
        ));
    }
}

#[test]
fn validates_dates_inclusive_interval_and_bounded_window() {
    assert!(aggregate_cpc_days(&[fact(1, 10)], &scope(), date(1), date(1)).is_ok());
    for (start, end) in [(date(2), date(3)), (date(3), date(2))] {
        assert!(matches!(
            aggregate_cpc_days(&[fact(1, 10)], &scope(), start, end),
            Err(OptimizerError::InvalidInput)
        ));
    }
    assert!(matches!(
        aggregate_cpc_days(&[fact(3, 10)], &scope(), date(1), date(2)),
        Err(OptimizerError::InvalidInput)
    ));
    let end = date(1) + chrono::Duration::days(MAX_WINDOW_DAYS);
    assert!(matches!(
        aggregate_cpc_days(&[], &scope(), date(1), end),
        Err(OptimizerError::InvalidInput)
    ));
}

#[test]
fn checked_sums_reject_overflow_in_every_output_counter() {
    for counter in 0..4 {
        let mut first = fact(1, 10);
        match counter {
            0 => {
                first.impressions = u64::MAX;
                first.clicks = u64::MAX;
            }
            1 => first.spend_minor = u64::MAX,
            2 => first.attributed_orders = u64::MAX,
            _ => first.attributed_revenue_minor = u64::MAX,
        }
        assert!(matches!(
            aggregate_cpc_days(&[first, fact(1, 20)], &scope(), date(1), date(1)),
            Err(OptimizerError::Overflow)
        ));
    }
}

#[test]
fn ignores_model_attribution_and_upstream_average_cpc() {
    let mut row = fact(1, 10);
    row.model_attributed_orders = u64::MAX;
    row.model_attributed_revenue_minor = u64::MAX;
    row.average_cpc_minor = Some(u64::MAX);
    let days = aggregate_cpc_days(&[row], &scope(), date(1), date(1)).unwrap();
    assert_eq!(days[0].direct_orders, 2);
    assert_eq!(days[0].direct_revenue_minor, 3_000);
    assert_eq!(days[0].spend_minor, 200);
}

#[test]
fn preserves_explicit_zero_activity_without_inventing_missing_dates() {
    let mut zero = fact(1, 10);
    zero.impressions = 0;
    zero.clicks = 0;
    zero.spend_minor = 0;
    zero.attributed_orders = 0;
    zero.attributed_revenue_minor = 0;
    let days = aggregate_cpc_days(&[zero, fact(3, 10)], &scope(), date(1), date(4)).unwrap();
    assert_eq!(days.len(), 2);
    assert_eq!(days[0].date, date(1));
    assert_eq!(days[0].clicks, 0);
    assert_eq!(days[0].direct_orders, 0);
    assert_eq!(days[1].date, date(3));
    assert!(
        aggregate_cpc_days(&[], &scope(), date(1), date(4))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn requires_explicit_confirmation_for_every_included_campaign() {
    assert_eq!(
        scope().confirmation_source_ref(),
        "campaign-settings-snapshot-7"
    );
    assert!(matches!(
        aggregate_cpc_days(&[fact(1, 30)], &scope(), date(1), date(1)),
        Err(OptimizerError::InvalidInput)
    ));
    for (campaign_ids, source_ref) in [
        (BTreeSet::new(), "snapshot"),
        (BTreeSet::from([0]), "snapshot"),
        (BTreeSet::from([10]), "   "),
        (BTreeSet::from([10]), "snapshot\ninvalid"),
    ] {
        assert!(
            CpcCampaignScope::new(
                "store_1".to_owned(),
                123,
                campaign_ids,
                source_ref.to_owned()
            )
            .is_err()
        );
    }
}

#[test]
fn rejects_invalid_scope_identifiers() {
    for (account, sku) in [("", 123), ("store/1", 123), ("store_1", 0)] {
        assert!(
            CpcCampaignScope::new(
                account.to_owned(),
                sku,
                BTreeSet::from([10]),
                "snapshot".to_owned()
            )
            .is_err()
        );
    }
}

#[test]
fn limits_unbounded_history_before_processing_rows() {
    let facts = vec![fact(1, 10); MAX_ADVERTISING_ROWS + 1];
    assert!(matches!(
        aggregate_cpc_days(&facts, &scope(), date(1), date(1)),
        Err(OptimizerError::LimitExceeded)
    ));
}
