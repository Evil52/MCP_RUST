use chrono::{Duration, TimeZone};

use super::*;

fn cutoff() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 17, 3, 0, 0).unwrap()
}

fn snapshot(facts: CollectedFacts) -> Result<CollectedSnapshot, PostgresCollectorError> {
    let source_as_of = cutoff() - Duration::minutes(10);
    let (period_start, period_end) = match facts.source() {
        SnapshotSource::Sales
        | SnapshotSource::Advertising
        | SnapshotSource::Finance
        | SnapshotSource::PostingMovement => (cutoff() - Duration::days(1), cutoff()),
        SnapshotSource::Stocks
        | SnapshotSource::SellerStocks
        | SnapshotSource::ClusterStocks
        | SnapshotSource::Prices => (source_as_of, source_as_of),
    };
    CollectedSnapshot::new(
        "pilot".to_owned(),
        Marketplace::Ozon,
        cutoff(),
        source_as_of,
        period_start,
        period_end,
        SnapshotStatus::Succeeded,
        true,
        "test-1.0".to_owned(),
        facts,
    )
}

fn sales() -> CollectedSalesFact {
    CollectedSalesFact {
        business_date: cutoff().date_naive(),
        sku: 1,
        ordered_units: 2,
        operational_gmv_minor: 300,
        cancelled_units: Some(0),
        returned_units: Some(0),
    }
}

fn advertising() -> CollectedAdvertisingFact {
    CollectedAdvertisingFact {
        business_date: cutoff().date_naive(),
        campaign_id: 2,
        sku: 1,
        impressions: 100,
        clicks: 5,
        spend_minor: 20,
        attributed_orders: 1,
        attributed_revenue_minor: 80,
        basket_additions: 2,
        model_attributed_orders: 1,
        model_attributed_revenue_minor: 80,
        product_price_minor: 100,
        average_cpc_minor: Some(4),
        cpm_minor: Some(200),
        cpl_minor: Some(10),
    }
}

#[test]
fn every_fact_shape_and_partial_snapshot_validate() {
    assert!(snapshot(CollectedFacts::Sales(vec![sales()])).is_ok());
    assert!(snapshot(CollectedFacts::Advertising(vec![advertising()])).is_ok());
    assert!(
        snapshot(CollectedFacts::Finance(vec![CollectedFinanceFact {
            business_date: cutoff().date_naive(),
            sku: Some(1),
            category: FinanceCategory::Sale,
            amount_minor: 100,
            line_count: 1,
            unknown_type_count: 0,
        }]))
        .is_ok()
    );
    assert!(
        snapshot(CollectedFacts::Stocks(vec![CollectedStockFact {
            sku: 1,
            warehouse_id: "fbo-msk:1".to_owned(),
            sellable_units: 3,
            present_units: None,
            reserved_units: None,
        }]))
        .is_ok()
    );
    assert!(
        snapshot(CollectedFacts::Prices(vec![CollectedPriceFact {
            sku: 1,
            price_minor: 100,
            old_price_minor: Some(120),
        }]))
        .is_ok()
    );
    let source_as_of = cutoff() - Duration::minutes(10);
    assert!(
        CollectedSnapshot::new(
            "wb-pilot".to_owned(),
            Marketplace::Wildberries,
            cutoff(),
            source_as_of,
            source_as_of,
            source_as_of,
            SnapshotStatus::Partial,
            false,
            "test".to_owned(),
            CollectedFacts::Stocks(Vec::new()),
        )
        .is_ok()
    );

    let advertising_snapshot = snapshot(CollectedFacts::Advertising(vec![advertising()]))
        .unwrap()
        .with_advertising_expenses(vec![CollectedAdvertisingExpenseFact {
            business_date: cutoff().date_naive(),
            campaign_id: 2,
            money_spent_minor: 20,
            bonus_spent_minor: 5,
            prepayment_spent_minor: 15,
        }]);
    assert!(advertising_snapshot.is_ok());
    assert_eq!(
        snapshot(CollectedFacts::Sales(vec![sales()]))
            .unwrap()
            .with_advertising_expenses(Vec::new()),
        Err(PostgresCollectorError::InvalidInput)
    );

    for category in [
        FinanceCategory::Sale,
        FinanceCategory::Commission,
        FinanceCategory::Acquiring,
        FinanceCategory::Logistics,
        FinanceCategory::Storage,
        FinanceCategory::PaidAcceptance,
        FinanceCategory::Compensation,
        FinanceCategory::MarketplaceDiscount,
        FinanceCategory::Advertising,
        FinanceCategory::Other,
    ] {
        assert!(!category.as_str().is_empty());
    }
}

#[test]
fn snapshot_metadata_and_fact_bounds_fail_closed() {
    let mut duplicate = sales();
    duplicate.ordered_units = 3;
    assert_eq!(
        snapshot(CollectedFacts::Sales(vec![sales(), duplicate])),
        Err(PostgresCollectorError::InvalidInput)
    );

    let mut bad_ad = advertising();
    bad_ad.clicks = bad_ad.impressions + 1;
    assert!(snapshot(CollectedFacts::Advertising(vec![bad_ad])).is_err());

    for warehouse_id in ["", "bad warehouse"] {
        assert!(
            snapshot(CollectedFacts::Stocks(vec![CollectedStockFact {
                sku: 1,
                warehouse_id: warehouse_id.to_owned(),
                sellable_units: 1,
                present_units: None,
                reserved_units: None,
            }]))
            .is_err()
        );
    }

    assert!(
        snapshot(CollectedFacts::Prices(vec![CollectedPriceFact {
            sku: 1,
            price_minor: 200,
            old_price_minor: Some(100),
        }]))
        .is_err()
    );

    let source_as_of = cutoff() - Duration::minutes(10);
    for (account, version, complete) in [
        ("bad account", "test", true),
        ("pilot", "bad version!", true),
        ("pilot", "test", false),
    ] {
        assert!(
            CollectedSnapshot::new(
                account.to_owned(),
                Marketplace::Ozon,
                cutoff(),
                source_as_of,
                source_as_of,
                source_as_of,
                SnapshotStatus::Succeeded,
                complete,
                version.to_owned(),
                CollectedFacts::Stocks(Vec::new()),
            )
            .is_err()
        );
    }
}

#[test]
fn integer_conversion_helpers_reject_overflow() {
    assert_eq!(as_i32(i32::MAX as u64).unwrap(), i32::MAX);
    assert_eq!(as_i64(i64::MAX as u64).unwrap(), i64::MAX);
    assert_eq!(
        as_i32(i32::MAX as u64 + 1),
        Err(PostgresCollectorError::InvalidInput)
    );
    assert_eq!(
        as_i64(i64::MAX as u64 + 1),
        Err(PostgresCollectorError::InvalidInput)
    );
}

#[test]
fn claimed_batch_requires_the_complete_exact_identity() {
    let claim = CollectionClaim {
        id: 1,
        generation: 1,
        account_id: "pilot".to_owned(),
        marketplace: Marketplace::Ozon,
        cutoff_at: cutoff(),
        owner_id: "test-owner".to_owned(),
        lease_until: cutoff() + Duration::minutes(15),
    };
    assert_eq!(
        validate_claimed_batch(&claim, &[]),
        Err(PostgresCollectorError::InvalidInput)
    );
    assert_eq!(
        validate_owner_id("bad owner"),
        Err(PostgresCollectorError::InvalidInput)
    );
    assert_eq!(
        validate_owner_id(&"a".repeat(65)),
        Err(PostgresCollectorError::InvalidInput)
    );
    assert!(validate_owner_id("collector:1-2").is_ok());
    assert!(validate_error_class("seller_collection_failed").is_ok());
    for error_class in ["", "BadClass", "bad-class"] {
        assert_eq!(
            validate_error_class(error_class),
            Err(PostgresCollectorError::InvalidInput)
        );
    }
    assert_eq!(
        validate_error_class(&"a".repeat(65)),
        Err(PostgresCollectorError::InvalidInput)
    );
    let refresh_claim = SalesRefreshClaim {
        id: 2,
        generation: 1,
        account_id: "pilot".to_owned(),
        marketplace: Marketplace::Ozon,
        business_date: cutoff().date_naive(),
        cutoff_at: cutoff(),
        owner_id: "test-owner".to_owned(),
        lease_until: cutoff() + Duration::minutes(15),
    };
    assert_eq!(refresh_claim.account_id(), "pilot");
    assert_eq!(refresh_claim.marketplace(), Marketplace::Ozon);
    assert_eq!(refresh_claim.business_date(), cutoff().date_naive());
    assert_eq!(refresh_claim.cutoff_at(), cutoff());
    assert_eq!(
        refresh_claim.lease_until(),
        cutoff() + Duration::minutes(15)
    );
    assert_eq!(
        classify_snapshot_insert_code(Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)),
        PostgresCollectorError::ClaimLost
    );
    assert_eq!(
        classify_snapshot_insert_code(None),
        PostgresCollectorError::Unavailable
    );
    assert_eq!(require_claim_completed(true), Ok(()));
    assert_eq!(
        require_claim_completed(false),
        Err(PostgresCollectorError::ClaimLost)
    );
}

#[test]
fn coverage_query_inputs_are_bounded_and_unique() {
    let target = CollectionTarget {
        account_id: "pilot".to_owned(),
        marketplace: Marketplace::Ozon,
        sources: SnapshotSource::required_for(Marketplace::Ozon).to_vec(),
    };
    assert!(validate_coverage_targets(&[]).is_ok());
    assert!(validate_coverage_targets(std::slice::from_ref(&target)).is_ok());
    assert_eq!(
        validate_coverage_targets(&[target.clone(), target]),
        Err(PostgresCollectorError::InvalidInput)
    );
    let incomplete = CollectionTarget {
        account_id: "incomplete".to_owned(),
        marketplace: Marketplace::Ozon,
        sources: vec![
            SnapshotSource::Sales,
            SnapshotSource::Advertising,
            SnapshotSource::Stocks,
            SnapshotSource::Stocks,
        ],
    };
    assert_eq!(
        validate_coverage_targets(&[incomplete]),
        Err(PostgresCollectorError::InvalidInput)
    );
    for account_id in ["", "unsafe/account", &"a".repeat(MAX_ACCOUNT_ID_BYTES + 1)] {
        assert_eq!(
            validate_coverage_targets(&[CollectionTarget {
                account_id: account_id.to_owned(),
                marketplace: Marketplace::Ozon,
                sources: SnapshotSource::required_for(Marketplace::Ozon).to_vec(),
            }]),
            Err(PostgresCollectorError::InvalidInput)
        );
    }
    let too_many = (0..=MAX_COLLECTION_TARGETS)
        .map(|index| CollectionTarget {
            account_id: format!("account-{index}"),
            marketplace: Marketplace::Ozon,
            sources: SnapshotSource::required_for(Marketplace::Ozon).to_vec(),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        validate_coverage_targets(&too_many),
        Err(PostgresCollectorError::InvalidInput)
    );
    assert_eq!(parse_marketplace("ozon"), Ok(Marketplace::Ozon));
    assert_eq!(
        parse_marketplace("wildberries"),
        Ok(Marketplace::Wildberries)
    );
    assert_eq!(
        parse_marketplace("unknown"),
        Err(PostgresCollectorError::Unavailable)
    );
}
