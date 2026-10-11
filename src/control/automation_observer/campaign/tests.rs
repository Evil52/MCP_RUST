use super::*;

fn policy() -> WbAutomationPolicy {
    serde_json::from_str(include_str!(
        "../../../../config/wb-automation-oduvanchik.v4.json"
    ))
    .unwrap()
}

fn campaign(policy: &WbAutomationPolicy, recommendations: bool, bid: u64) -> Value {
    serde_json::json!({"adverts": [{
        "id": policy.campaign_id, "status": 9, "bid_type": "manual",
        "settings": {"name": policy.campaign_name, "payment_type": "cpc",
            "placements": {"search": true, "recommendations": recommendations}},
        "nm_settings": policy.nm_ids.iter().map(|nm| serde_json::json!({
            "nm_id": nm, "bids_kopecks": {"search": 1042, "recommendations": bid}
        })).collect::<Vec<_>>()
    }]})
}

#[test]
fn legacy_contract_still_requires_disabled_recommendations_and_zero_bids() {
    let policy = policy();
    assert!(parse_campaign(&campaign(&policy, false, 0), &policy).is_ok());
    assert!(parse_campaign(&campaign(&policy, true, 153), &policy).is_err());
    assert!(parse_campaign(&campaign(&policy, false, 153), &policy).is_err());
    assert!(parse_campaign(&campaign(&policy, true, 0), &policy).is_err());
}

#[test]
fn authorized_recommendations_leave_search_bids_as_the_observed_control_variable() {
    let mut policy = policy();
    policy.expected_recommendation_bid_kopecks = Some(153);
    let parsed = parse_campaign(&campaign(&policy, true, 153), &policy).unwrap();
    assert_eq!(parsed.status, 9);
    assert_eq!(parsed.bids.len(), policy.nm_ids.len());
    assert!(parsed.bids.values().all(|bid| *bid == 1042));
    for (enabled, bid) in [(false, 153), (true, 0), (true, 154), (false, 0)] {
        assert!(parse_campaign(&campaign(&policy, enabled, bid), &policy).is_err());
    }
}

#[test]
fn recommendations_opt_in_preserves_every_campaign_identity_and_sku_guard() {
    let mut policy = policy();
    policy.expected_recommendation_bid_kopecks = Some(153);
    let original = campaign(&policy, true, 153);
    for (pointer, value) in [
        ("/adverts/0/id", serde_json::json!(1)),
        ("/adverts/0/settings/name", serde_json::json!("other")),
        ("/adverts/0/bid_type", serde_json::json!("unified")),
        ("/adverts/0/settings/payment_type", serde_json::json!("cpm")),
        (
            "/adverts/0/settings/placements/search",
            serde_json::json!(false),
        ),
        ("/adverts/0/nm_settings/0/nm_id", serde_json::json!(999)),
        (
            "/adverts/0/nm_settings/0/bids_kopecks/recommendations",
            Value::Null,
        ),
        (
            "/adverts/0/nm_settings/4/bids_kopecks/recommendations",
            serde_json::json!(154),
        ),
    ] {
        let mut changed = original.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        assert!(parse_campaign(&changed, &policy).is_err(), "{pointer}");
    }
    let mut missing = original.clone();
    missing["adverts"][0]["nm_settings"]
        .as_array_mut()
        .unwrap()
        .pop();
    assert!(parse_campaign(&missing, &policy).is_err());
    let mut duplicate = original.clone();
    duplicate["adverts"][0]["nm_settings"]
        .as_array_mut()
        .unwrap()
        .push(original["adverts"][0]["nm_settings"][0].clone());
    assert!(parse_campaign(&duplicate, &policy).is_err());
    let mut duplicate_campaign = original.clone();
    duplicate_campaign["adverts"]
        .as_array_mut()
        .unwrap()
        .push(original["adverts"][0].clone());
    assert!(parse_campaign(&duplicate_campaign, &policy).is_err());
}
