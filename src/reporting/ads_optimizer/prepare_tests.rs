use chrono::TimeZone;
use serde_json::json;

use super::*;
use crate::reporting::ads_optimizer::{RecommendationAction, RecommendationReason, recommend};

fn time(day: u32, hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, day, hour, 0, 0).unwrap()
}

fn date(day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, day).unwrap()
}

fn advertisement(day: u32, id: i64, campaign: u64) -> Value {
    json!({
        "account_id":"store_1", "marketplace":"ozon", "source":"advertising",
        "snapshot_id":id, "cutoff_at":time(day + 1, 3),
        "source_as_of":time(day + 1, 4), "snapshot_status":"succeeded",
        "pagination_complete":true, "business_date":date(day), "campaign_id":campaign,
        "sku":1, "currency":"RUB", "impressions":100, "clicks":10,
        "spend_minor":100, "attributed_orders":2, "attributed_revenue_minor":1_000,
        "basket_additions":3, "model_attributed_orders":20,
        "model_attributed_revenue_minor":10_000, "product_price_minor":500,
        "average_cpc_minor":10, "cpm_minor":1_000, "cpl_minor":33
    })
}

fn advertising_page(day: u32, id: i64, rows: Vec<Value>) -> SnapshotPage {
    let total_rows = rows.len();
    SnapshotPage {
        offset: 0,
        response: json!({
            "account_id":"store_1", "marketplace":"ozon", "source":"advertising",
            "storage":"published_postgresql_snapshots", "state":"available", "data_state":"available",
            "quality":"complete", "pagination_complete":true,
            "snapshot_id":id.to_string(), "cutoff_at":time(day + 1, 3),
            "source_as_of":time(day + 1, 4), "observed_from":time(day + 1, 3),
            "period_start":format!("2026-09-{day:02}T00:00:00+05:00"),
            "period_end":format!("2026-09-{:02}T00:00:00+05:00", day + 1),
            "total_rows":total_rows, "rows":Value::Array(rows), "next_offset":null,
            "latest_collection":{"status":"running"}, "coverage":null
        }),
    }
}

fn stock_row(sku: u64, warehouse: &str, units: u64) -> Value {
    json!({
        "account_id":"store_1", "marketplace":"ozon", "source":"stocks",
        "snapshot_id":200, "cutoff_at":time(26, 12), "source_as_of":time(26, 12),
        "snapshot_status":"succeeded", "pagination_complete":true,
        "sku":sku, "warehouse_id":warehouse, "sellable_units":units
    })
}

fn stock_page(rows: Vec<Value>) -> SnapshotPage {
    let total_rows = rows.len();
    SnapshotPage {
        offset: 0,
        response: json!({
            "account_id":"store_1", "marketplace":"ozon", "source":"stocks",
            "storage":"published_postgresql_snapshots", "state":"available", "data_state":"available",
            "quality":"complete", "pagination_complete":true,
            "snapshot_id":"200", "cutoff_at":time(26, 12), "source_as_of":time(26, 12),
            "observed_from":time(26, 11), "period_start":time(26, 12), "period_end":time(26, 12),
            "total_rows":total_rows, "rows":Value::Array(rows), "next_offset":null
        }),
    }
}

fn fixture() -> PreparationBundle {
    PreparationBundle {
        version: 1,
        account_id: "store_1".to_owned(),
        as_of: time(26, 12),
        window_start: date(24),
        window_end: date(25),
        objective: OptimizationObjective::TargetAdvertisingDrr { max_drr_bps: 1_000 },
        policy: OptimizerPolicy {
            total_daily_budget_minor: 10_000,
            attribution_lag_days: 7,
            min_mature_days: 1,
            min_clicks: 1,
            min_orders: 1,
            max_data_age_hours: 168,
            max_window_end_age_days: 30,
            max_stock_age_hours: 24,
            safety_discount_bps: 2_000,
            max_budget_increase_bps: 1_000,
            budget_decrease_bps: 2_000,
            zero_order_spend_allowances: 2,
        },
        products: vec![PreparationProduct {
            sku: 1,
            current_daily_budget_minor: 1_000,
            max_daily_budget_minor: 2_000,
            budget_constraint: Some(BudgetConstraintEvidence {
                limited: true,
                observed_at: time(26, 12),
            }),
            economics: None,
            cpc_scope: CpcScopeConfirmation {
                campaign_ids: vec![10, 20],
                pricing_model: PricingModel::Cpc,
                source_ref: "reviewed-cpc-campaign-scope".to_owned(),
                observed_at: time(26, 12),
                all_campaigns_confirmed: true,
            },
        }],
        advertising_pages: vec![
            advertising_page(
                24,
                100,
                vec![advertisement(24, 100, 10), advertisement(24, 100, 20)],
            ),
            advertising_page(
                25,
                101,
                vec![advertisement(25, 101, 10), advertisement(25, 101, 20)],
            ),
        ],
        stock_pages: vec![stock_page(vec![
            stock_row(1, "fbo:1", 5),
            stock_row(1, "fbs:2", 7),
        ])],
    }
}

#[test]
fn prepares_actual_wire_dates_provenance_counters_and_unique_stock_warehouses() {
    let input = prepare(fixture()).unwrap();
    assert!(input.coverage_complete);
    assert_eq!(input.observed_at, time(26, 3));
    let product = &input.products[0];
    assert_eq!(product.daily.len(), 2);
    assert_eq!(product.daily[0].date, date(24));
    assert_eq!(product.daily[0].observed_at, Some(time(25, 3)));
    assert_eq!(product.daily[0].clicks, 20);
    assert_eq!(product.daily[0].direct_orders, 4);
    assert_eq!(product.daily[0].direct_revenue_minor, 2_000);
    assert_eq!(product.stock.as_ref().unwrap().sellable_units, 12);
    assert_eq!(product.stock.as_ref().unwrap().observed_at, time(26, 11));
    assert!(
        input
            .source_refs
            .iter()
            .any(|value| value == "ofk:advertising:snapshot:100")
    );
    assert!(
        input
            .source_refs
            .iter()
            .any(|value| value.starts_with("prepare-bundle-sha256:"))
    );
}

#[test]
fn frozen_daily_history_does_not_mature_when_latest_snapshot_is_newer() {
    let report = recommend(prepare(fixture()).unwrap()).unwrap();
    let product = &report.recommendations[0];
    assert_eq!(product.action, RecommendationAction::Hold);
    assert_eq!(product.metrics.mature_days, 0);
    assert_eq!(product.metrics.excluded_recent_days, 2);
    assert!(
        product
            .reasons
            .contains(&RecommendationReason::InsufficientMatureDays)
    );
}

#[test]
fn pinned_pages_join_by_offsets_even_when_bundle_order_is_reversed() {
    let mut bundle = fixture();
    let second_row = bundle.advertising_pages[0].response["rows"]
        .as_array_mut()
        .unwrap()
        .pop()
        .unwrap();
    bundle.advertising_pages[0].response["next_offset"] = json!(1);
    let mut second_page = bundle.advertising_pages[0].clone();
    second_page.offset = 1;
    second_page.response["rows"] = json!([second_row]);
    second_page.response["next_offset"] = Value::Null;
    // These fields can change as source freshness expires during pagination.
    second_page.response["state"] = json!("stale");
    second_page.response["quality"] = json!("stale");
    bundle.advertising_pages.push(second_page);
    bundle.advertising_pages.reverse();
    assert_eq!(prepare(bundle).unwrap().products[0].daily[0].clicks, 20);
}

#[test]
fn rejects_missing_duplicate_and_inconsistent_pagination() {
    for case in 0..4 {
        let mut bundle = fixture();
        match case {
            0 => bundle.advertising_pages[0].offset = 1,
            1 => bundle
                .advertising_pages
                .push(bundle.advertising_pages[0].clone()),
            2 => bundle.advertising_pages[0].response["next_offset"] = json!(2),
            _ => bundle.advertising_pages[0].response["total_rows"] = json!(3),
        }
        assert!(
            matches!(prepare(bundle), Err(PrepareError::IncompleteSnapshot)),
            "case {case}"
        );
    }
}

#[test]
fn rejects_forged_fact_identity_currency_status_and_missing_observation_start() {
    for (field, wrong) in [
        ("account_id", json!("foreign")),
        ("snapshot_id", json!(999)),
        ("source_as_of", json!(time(26, 4))),
        ("currency", json!("USD")),
        ("snapshot_status", json!("partial")),
        ("pagination_complete", json!(false)),
        ("business_date", json!(date(23))),
    ] {
        let mut bundle = fixture();
        bundle.advertising_pages[0].response["rows"][0][field] = wrong;
        assert!(
            matches!(prepare(bundle), Err(PrepareError::InvalidSnapshot)),
            "{field}"
        );
    }
    let mut missing = fixture();
    missing.advertising_pages[0].response["observed_from"] = Value::Null;
    assert!(matches!(
        prepare(missing),
        Err(PrepareError::InvalidSnapshot)
    ));
}

#[test]
fn missing_period_or_missing_product_date_is_never_filled_with_zero() {
    let mut missing_period = fixture();
    missing_period.advertising_pages.remove(0);
    let input = prepare(missing_period).unwrap();
    assert!(!input.coverage_complete);
    assert_eq!(input.products[0].daily.len(), 1);
    assert_eq!(input.products[0].daily[0].date, date(25));

    let mut missing_sku = fixture();
    for row in missing_sku.advertising_pages[0].response["rows"]
        .as_array_mut()
        .unwrap()
    {
        row["sku"] = json!(999);
    }
    let input = prepare(missing_sku).unwrap();
    assert_eq!(input.products[0].daily.len(), 1);
    let report = recommend(input).unwrap();
    assert!(
        report.recommendations[0]
            .reasons
            .contains(&RecommendationReason::MissingDates)
    );
}

#[test]
fn no_data_snapshot_preserves_unknown_product_activity() {
    let mut bundle = fixture();
    bundle.advertising_pages[0].response["rows"] = json!([]);
    bundle.advertising_pages[0].response["total_rows"] = json!(0);
    bundle.advertising_pages[0].response["data_state"] = json!("no_data");
    let input = prepare(bundle).unwrap();
    assert_eq!(input.products[0].daily.len(), 1);
    assert_eq!(input.products[0].daily[0].date, date(25));
}

#[test]
fn overlapping_snapshots_and_preliminary_evening_periods_are_rejected() {
    let mut overlap = fixture();
    overlap
        .advertising_pages
        .push(advertising_page(24, 102, vec![advertisement(24, 102, 10)]));
    assert!(matches!(
        prepare(overlap),
        Err(PrepareError::OverlappingSnapshots)
    ));
    let mut evening = fixture();
    evening.advertising_pages[0].response["period_end"] = json!("2026-09-24T17:00:00+05:00");
    assert!(matches!(
        prepare(evening),
        Err(PrepareError::InvalidSnapshot)
    ));
}

#[test]
fn cpc_scope_must_be_current_explicit_and_include_selected_sku_campaigns() {
    let mut unconfirmed = fixture();
    unconfirmed.products[0].cpc_scope.all_campaigns_confirmed = false;
    assert!(!prepare(unconfirmed).unwrap().coverage_complete);
    for case in 0..5 {
        let mut bundle = fixture();
        match case {
            0 => bundle.products[0].cpc_scope.observed_at = time(1, 0),
            1 => bundle.products[0].cpc_scope.campaign_ids.clear(),
            2 => bundle.products[0].cpc_scope.campaign_ids.push(10),
            3 => bundle.products[0].cpc_scope.source_ref.clear(),
            _ => bundle.products[0].cpc_scope.observed_at = time(27, 0),
        }
        assert!(
            matches!(prepare(bundle), Err(PrepareError::InvalidCampaignScope)),
            "case {case}"
        );
    }
    let mut unknown_campaign = fixture();
    unknown_campaign.products[0].cpc_scope.campaign_ids = vec![10];
    assert!(prepare(unknown_campaign).is_err());
    let mut cpo = serde_json::to_value(fixture()).unwrap();
    cpo["products"][0]["cpc_scope"]["pricing_model"] = json!("cpo");
    assert!(matches!(
        prepare_input(&serde_json::to_vec(&cpo).unwrap()),
        Err(PrepareError::InvalidBundle)
    ));
}

#[test]
fn missing_stock_is_none_and_duplicate_warehouses_are_not_double_counted() {
    let mut bundle = fixture();
    bundle.stock_pages.clear();
    assert!(prepare(bundle).unwrap().products[0].stock.is_none());
    let mut other_sku = fixture();
    other_sku.stock_pages = vec![stock_page(vec![stock_row(999, "fbo:1", 7)])];
    assert!(prepare(other_sku).unwrap().products[0].stock.is_none());
    let mut duplicate = fixture();
    duplicate.stock_pages = vec![stock_page(vec![
        stock_row(1, "fbo:1", 7),
        stock_row(1, "fbo:1", 7),
    ])];
    assert!(matches!(
        prepare(duplicate),
        Err(PrepareError::InvalidSnapshot)
    ));
}

#[test]
fn upstream_extensions_are_preserved_but_cannot_override_selected_fields() {
    let mut bundle = fixture();
    bundle.advertising_pages[0].response["future_metadata"] = json!({"value":"unused"});
    bundle.advertising_pages[0].response["rows"][0]["future_counter"] = json!(999);
    let expected = prepare(bundle.clone()).unwrap();
    let actual = prepare_input(&serde_json::to_vec(&bundle).unwrap()).unwrap();
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    let mut unknown_wrapper = serde_json::to_value(bundle).unwrap();
    unknown_wrapper["override_observed_at"] = json!(time(26, 12));
    assert!(matches!(
        prepare_input(&serde_json::to_vec(&unknown_wrapper).unwrap()),
        Err(PrepareError::InvalidBundle)
    ));
}

#[test]
fn incompatible_source_and_snapshot_identifiers_are_rejected() {
    for (field, wrong) in [
        ("marketplace", json!("wildberries")),
        ("account_id", json!("other_store")),
        ("source", json!("finance")),
        ("pagination_complete", json!(false)),
        ("state", json!("partial")),
        ("quality", json!("partial")),
        ("source_as_of", json!(time(27, 0))),
    ] {
        let mut bundle = fixture();
        bundle.advertising_pages[0].response[field] = wrong;
        assert!(
            matches!(prepare(bundle), Err(PrepareError::InvalidSnapshot)),
            "{field}"
        );
    }
}

#[test]
fn checked_aggregation_rejects_stock_and_advertising_overflow() {
    let mut stock = fixture();
    stock.stock_pages[0].response["rows"][0]["sellable_units"] = json!(u64::MAX);
    assert!(matches!(
        prepare(stock),
        Err(PrepareError::Optimizer(OptimizerError::Overflow))
    ));
    let mut advertising = fixture();
    advertising.advertising_pages[0].response["rows"][0]["spend_minor"] = json!(u64::MAX);
    assert!(matches!(
        prepare(advertising),
        Err(PrepareError::Optimizer(OptimizerError::Overflow))
    ));
}

#[test]
fn bounded_export_rejects_excessive_rows_and_unknown_campaign_totals() {
    let mut excessive = fixture();
    excessive.advertising_pages[0].response["total_rows"] = json!(25_001);
    assert!(matches!(
        prepare(excessive),
        Err(PrepareError::LimitExceeded)
    ));
    let mut sentinel = fixture();
    sentinel.advertising_pages[0].response["rows"][0]["sku"] = json!(0);
    assert!(matches!(
        prepare(sentinel),
        Err(PrepareError::InvalidSnapshot)
    ));
    assert!(matches!(
        prepare_input(&vec![b' '; MAX_PREPARATION_BYTES + 1]),
        Err(PrepareError::LimitExceeded)
    ));
}

#[test]
fn legacy_stock_namespace_collision_never_authorizes_growth_or_pause() {
    for units in [0, 777] {
        for warehouse in ["FBO", "FBS", "RFBS"] {
            let mut bundle = fixture();
            bundle.stock_pages = vec![stock_page(vec![stock_row(1, warehouse, units)])];
            let input = prepare(bundle).unwrap();
            assert_eq!(input.products[0].sku, 1);
            assert!(input.products[0].stock.is_none());
            assert!(
                input
                    .source_refs
                    .iter()
                    .any(|value| value == "unsupported-legacy-stock-identity:200")
            );
            let report = recommend(input).unwrap();
            assert_eq!(report.recommendations[0].action, RecommendationAction::Hold);
            assert!(
                report.recommendations[0]
                    .reasons
                    .contains(&RecommendationReason::MissingStock)
            );
            assert!(
                !report.recommendations[0]
                    .reasons
                    .contains(&RecommendationReason::OutOfStock)
            );
        }
    }
}

#[test]
fn mixed_legacy_stock_blocks_native_totals_but_still_validates_all_rows() {
    let mut mixed = fixture();
    mixed.stock_pages[0].response["rows"]
        .as_array_mut()
        .unwrap()
        .push(stock_row(999, "FBO", 99));
    mixed.stock_pages[0].response["total_rows"] = json!(3);
    let input = prepare(mixed.clone()).unwrap();
    assert!(input.products[0].stock.is_none());
    mixed.stock_pages[0].response["rows"][2]["snapshot_id"] = json!(999);
    assert!(matches!(prepare(mixed), Err(PrepareError::InvalidSnapshot)));
    let mut duplicate = fixture();
    duplicate.stock_pages = vec![stock_page(vec![
        stock_row(1, "FBO", 7),
        stock_row(1, "FBO", 8),
    ])];
    assert!(matches!(
        prepare(duplicate),
        Err(PrepareError::InvalidSnapshot)
    ));
}

#[test]
fn missing_campaign_date_is_incomplete_even_when_the_product_date_exists() {
    let mut bundle = fixture();
    bundle.advertising_pages[1].response["rows"]
        .as_array_mut()
        .unwrap()
        .pop();
    bundle.advertising_pages[1].response["total_rows"] = json!(1);
    let input = prepare(bundle).unwrap();
    assert!(!input.coverage_complete);
    assert_eq!(input.products[0].daily.len(), 2);
    assert_eq!(input.products[0].daily[1].date, date(25));
    assert_eq!(input.products[0].daily[1].clicks, 10);
    assert_eq!(input.products[0].daily[1].spend_minor, 100);
    let report = recommend(input).unwrap();
    assert_eq!(report.recommendations[0].action, RecommendationAction::Hold);
    assert!(
        report.recommendations[0]
            .reasons
            .contains(&RecommendationReason::IncompleteCoverage)
    );
    assert!(
        !report.recommendations[0]
            .reasons
            .contains(&RecommendationReason::MissingDates)
    );
}

#[test]
fn explicit_zero_campaign_date_is_evidence_for_complete_coverage() {
    let mut bundle = fixture();
    let row = &mut bundle.advertising_pages[1].response["rows"][1];
    for field in [
        "clicks",
        "spend_minor",
        "attributed_orders",
        "attributed_revenue_minor",
    ] {
        row[field] = json!(0);
    }
    let input = prepare(bundle).unwrap();
    assert!(input.coverage_complete);
    assert_eq!(input.products[0].daily[1].clicks, 10);
    assert_eq!(input.products[0].daily[1].spend_minor, 100);
}

#[test]
fn rejects_impossible_clicks_using_the_existing_published_writer_invariant() {
    let mut bundle = fixture();
    bundle.advertising_pages[0].response["rows"][0]["clicks"] = json!(101);
    assert!(matches!(
        prepare(bundle),
        Err(PrepareError::InvalidSnapshot)
    ));
}

#[test]
fn source_descriptor_rejects_bad_cutoff_even_when_all_fact_provenance_matches() {
    for cutoff in [time(1, 3), time(24, 18)] {
        let mut bundle = fixture();
        bundle.advertising_pages[0].response["cutoff_at"] = json!(cutoff);
        for row in bundle.advertising_pages[0].response["rows"]
            .as_array_mut()
            .unwrap()
        {
            row["cutoff_at"] = json!(cutoff);
        }
        assert!(matches!(
            prepare(bundle),
            Err(PrepareError::InvalidSnapshot)
        ));
    }
    let mut late = fixture();
    // Valid period cutoff, but completion more than 24 hours after cutoff.
    let cutoff = time(24, 19);
    let source_as_of = time(26, 10);
    late.advertising_pages[0].response["cutoff_at"] = json!(cutoff);
    late.advertising_pages[0].response["source_as_of"] = json!(source_as_of);
    for row in late.advertising_pages[0].response["rows"]
        .as_array_mut()
        .unwrap()
    {
        row["cutoff_at"] = json!(cutoff);
        row["source_as_of"] = json!(source_as_of);
    }
    assert!(matches!(prepare(late), Err(PrepareError::InvalidSnapshot)));
}
