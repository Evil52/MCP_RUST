use super::*;

fn input() -> WbReviewInput {
    serde_json::from_str(include_str!(
        "../../../../config/wb-ads-review.example.json"
    ))
    .unwrap()
}

#[test]
fn active_paused_and_removed_products_produce_distinct_review_tasks() {
    let report = analyze(input()).unwrap();
    assert!(report.advertising_coverage_complete && report.composition_complete_and_fresh);
    assert!(!report.auto_apply_allowed && !report.attribution_maturity_verified);
    assert_eq!(report.products[0].action, ReviewAction::ReviewActiveProduct);
    assert_eq!(
        report.products[1].action,
        ReviewAction::ReviewBeforeResuming
    );
    assert_eq!(
        report.products[2].action,
        ReviewAction::ReviewHistoricalSpend
    );
    assert_eq!(report.products[2].historical_campaign_ids, vec![10]);
    assert!(report.products[2].current_campaigns.is_empty());
    assert_eq!(
        report.products[0].current_campaigns[0].pricing_model,
        PricingModel::Cpc
    );
    assert_eq!(
        report.products[1].current_campaigns[0].pricing_model,
        PricingModel::Cpm
    );
    assert_eq!(report.products[1].ordered_units, Some(1));
    assert!(report.products[1].spend_without_observed_ad_orders);
}

#[test]
fn partial_positive_fbs_prevents_a_total_stockout_claim() {
    let report = analyze(input()).unwrap();
    let row = &report.products[0];
    assert_eq!(row.fbw_has_stock, Some(false));
    assert_eq!(row.fbs_has_stock, Some(true));
    assert_eq!(row.inventory_signal, InventorySignal::FbwZeroCheckFbs);
}

#[test]
fn missing_partial_zero_and_stale_fbs_never_prove_total_stockout() {
    for mode in 0..4 {
        let mut evidence = input();
        let fbs = &mut evidence.products[0].fbs;
        match mode {
            0 => *fbs = None,
            1 => fbs.as_mut().unwrap().units = Some(0),
            2 => fbs.as_mut().unwrap().units = None,
            _ => fbs.as_mut().unwrap().observed_at -= Duration::minutes(31),
        }
        let row = analyze(evidence).unwrap().products.remove(0);
        assert_eq!(row.fbs_has_stock, None);
        assert_eq!(row.inventory_signal, InventorySignal::FbwZeroCheckFbs);
    }
    let mut evidence = input();
    let stock = evidence.products[0].fbs.as_mut().unwrap();
    stock.quality = StockQuality::Complete;
    stock.units = Some(0);
    assert_eq!(
        analyze(evidence).unwrap().products[0].inventory_signal,
        InventorySignal::BothChannelsZero
    );
}

#[test]
fn absent_ad_dates_and_absent_publications_remain_distinct() {
    let mut evidence = input();
    let report = analyze(evidence.clone()).unwrap();
    assert_eq!(
        report.products[0].missing_ad_dates,
        vec![evidence.date_from]
    );
    assert_eq!(report.products[0].spend_minor, 90000);
    evidence.coverage.remove(0);
    let report = analyze(evidence).unwrap();
    assert!(!report.advertising_coverage_complete);
    assert!(
        report
            .products
            .iter()
            .all(|p| p.action == ReviewAction::RestoreAdvertisingCoverage)
    );
}

#[test]
fn partial_or_stale_composition_requires_verification_even_if_campaign_is_listed() {
    for stale in [false, true] {
        let mut evidence = input();
        if stale {
            evidence.composition.observed_at -= Duration::minutes(31);
        } else {
            evidence.composition.active_and_paused_complete = false;
        }
        let report = analyze(evidence).unwrap();
        assert!(!report.composition_complete_and_fresh);
        assert!(
            report
                .products
                .iter()
                .all(|p| p.promotion_state == PromotionState::Unknown
                    && p.action == ReviewAction::VerifyCurrentComposition)
        );
    }
}

#[test]
fn permutations_have_identical_digest_and_output() {
    let evidence = input();
    let mut shuffled = evidence.clone();
    shuffled.products.reverse();
    shuffled.advertising.reverse();
    shuffled.coverage.reverse();
    shuffled.composition.campaigns.reverse();
    assert_eq!(analyze(evidence).unwrap(), analyze(shuffled).unwrap());
}

#[test]
fn duplicate_foreign_and_future_evidence_is_rejected() {
    let mut evidence = input();
    evidence.advertising.push(evidence.advertising[0].clone());
    assert_eq!(
        analyze(evidence).unwrap_err(),
        OptimizerError::DuplicateEvidence
    );
    let mut evidence = input();
    evidence.advertising[0].sku = Some(999);
    assert_eq!(analyze(evidence).unwrap_err(), OptimizerError::InvalidInput);
    let mut evidence = input();
    evidence.products[0].fbw.as_mut().unwrap().observed_at = evidence.as_of + Duration::seconds(1);
    assert_eq!(analyze(evidence).unwrap_err(), OptimizerError::InvalidInput);
    let mut evidence = input();
    evidence.composition.campaigns[0].sku_ids.push(1);
    assert_eq!(
        analyze(evidence).unwrap_err(),
        OptimizerError::DuplicateEvidence
    );
}

#[test]
fn campaign_expense_without_sku_is_preserved_without_fabricated_product_attribution() {
    let mut evidence = input();
    evidence.advertising[0].sku = None;
    let report = analyze(evidence).unwrap();
    assert_eq!(report.campaign_rows_without_sku.len(), 1);
    assert_eq!(report.campaign_rows_without_sku[0].spend_minor, 90000);
    assert_eq!(report.products[0].spend_minor, 0);
    assert_eq!(report.products[0].action, ReviewAction::Observe);
    assert_eq!(report.products.len(), 3);
}

#[test]
fn overflow_and_oversized_or_unknown_json_fail_without_payload_disclosure() {
    let mut evidence = input();
    evidence.advertising[0].spend_minor = u64::MAX;
    let mut extra = evidence.advertising[0].clone();
    extra.date = evidence.date_from;
    extra.spend_minor = 1;
    evidence.advertising.push(extra);
    assert_eq!(analyze(evidence).unwrap_err(), OptimizerError::Overflow);
    assert_eq!(
        analyze_wb_export(&vec![b' '; MAX_INPUT_BYTES + 1]).unwrap_err(),
        OptimizerError::LimitExceeded
    );
    let mut raw = serde_json::to_value(input()).unwrap();
    raw["execute"] = serde_json::json!("PRIVATE_UNTRUSTED_DATA");
    let err = analyze_wb_export(&serde_json::to_vec(&raw).unwrap()).unwrap_err();
    assert_eq!(err, OptimizerError::InvalidInput);
    assert!(!err.to_string().contains("PRIVATE_UNTRUSTED_DATA"));
}
