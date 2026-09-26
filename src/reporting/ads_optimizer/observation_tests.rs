use chrono::{Duration, TimeZone as _, Utc};

use super::*;

fn fixture() -> ShadowInput {
    parse_input(include_bytes!("../../../config/ads-optimizer.example.json")).unwrap()
}

#[test]
fn legacy_input_keeps_its_observation_and_json_contract() {
    let input = fixture();
    let encoded = serde_json::to_value(&input).unwrap();
    assert!(
        encoded["products"][0]["daily"][0]
            .get("observed_at")
            .is_none()
    );
    let report = recommend(input).unwrap();
    assert_eq!(report.recommendations[0].metrics.mature_days, 14);
    assert_eq!(
        report.recommendations[0].action,
        RecommendationAction::TestBudgetIncrease
    );
}

#[test]
fn frozen_daily_snapshots_do_not_mature_when_the_bundle_is_exported_later() {
    let mut input = fixture();
    for day in &mut input.products[0].daily {
        day.observed_at = Some(
            Utc.from_utc_datetime(&(day.date + Duration::days(1)).and_hms_opt(8, 0, 0).unwrap()),
        );
    }
    let report = recommend(input.clone()).unwrap();
    let row = &report.recommendations[0];
    assert_eq!(row.metrics.mature_days, 0);
    assert_eq!(row.metrics.excluded_recent_days, 14);
    assert_eq!(row.metrics.all_period_spend_minor, 140_000);
    assert_eq!(row.action, RecommendationAction::Hold);
    assert!(
        row.reasons
            .contains(&RecommendationReason::StaleAdvertising)
    );
    input.as_of += Duration::days(2);
    assert_eq!(
        recommend(input).unwrap().recommendations[0]
            .metrics
            .mature_days,
        0
    );
}

#[test]
fn only_actually_reobserved_days_gain_mature_attribution() {
    let mut input = fixture();
    for day in &mut input.products[0].daily {
        day.observed_at = Some(input.observed_at);
    }
    input.products[0].daily[0].observed_at =
        Some(Utc.with_ymd_and_hms(2026, 9, 2, 8, 0, 0).unwrap());
    let row = recommend(input.clone()).unwrap().recommendations.remove(0);
    assert_eq!(row.metrics.mature_days, 13);
    assert_eq!(row.metrics.mature_spend_minor, 130_000);
    assert_eq!(row.action, RecommendationAction::Hold);
    assert!(
        row.reasons
            .contains(&RecommendationReason::StaleAdvertising)
    );
    input.products[0].daily[0].observed_at = Some(input.observed_at);
    let row = recommend(input).unwrap().recommendations.remove(0);
    assert_eq!(row.metrics.mature_days, 14);
    assert_eq!(row.action, RecommendationAction::TestBudgetIncrease);
}

#[test]
fn per_day_observation_cannot_precede_activity_or_exceed_manifest_observation() {
    for observed_at in [
        Utc.with_ymd_and_hms(2026, 9, 1, 23, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2026, 9, 26, 0, 0, 0).unwrap(),
    ] {
        let mut input = fixture();
        input.products[0].daily[0].observed_at = Some(observed_at);
        assert_eq!(recommend(input).unwrap_err(), OptimizerError::InvalidInput);
    }
}

#[test]
fn per_day_observations_participate_in_the_reproducible_digest() {
    let input = fixture();
    let old = recommend(input.clone()).unwrap();
    let mut observed = input;
    observed.products[0].daily[0].observed_at = Some(observed.observed_at);
    let explicit = recommend(observed.clone()).unwrap();
    assert_ne!(old.input_sha256, explicit.input_sha256);
    assert_eq!(old.recommendations, explicit.recommendations);
    observed.products[0].daily.reverse();
    assert_eq!(explicit, recommend(observed).unwrap());
}
