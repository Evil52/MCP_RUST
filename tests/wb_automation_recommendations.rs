use chrono::{Duration, TimeZone, Utc};
use mcp_ozon::control::{
    WbAutomationPolicy, validate_wb_automation_corridor_update, validate_wb_automation_policy,
    validate_wb_automation_recommendations_update,
};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

fn source() -> WbAutomationPolicy {
    serde_json::from_str(include_str!("../config/wb-automation-oduvanchik.v4.json")).unwrap()
}

fn now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 11, 15, 0, 0).unwrap()
}

fn target() -> WbAutomationPolicy {
    let mut target = source();
    target.expected_recommendation_bid_kopecks = Some(153);
    target.authorization_reference = "test/recommendations-authorized".into();
    target.authorized_at = now() - Duration::minutes(2);
    target.observe_until = now() - Duration::minutes(1);
    target.authorization_expires_at = now() + Duration::days(18);
    target
}

#[test]
fn recommendation_context_is_explicit_hashed_and_cannot_expand_write_placement() {
    let source = source();
    let mut target = target();
    validate_wb_automation_policy(&target).unwrap();
    validate_wb_automation_recommendations_update(&source, &target, now()).unwrap();
    assert!(validate_wb_automation_corridor_update(&source, &target, now()).is_err());
    let legacy = serde_json::to_vec(&source).unwrap();
    // Digest of the pre-extension policy, pinned before this implementation.
    let mut legacy_sha256 = String::with_capacity(64);
    for byte in Sha256::digest(&legacy) {
        write!(&mut legacy_sha256, "{byte:02x}").unwrap();
    }
    assert_eq!(
        legacy_sha256,
        "fba3e80523bc8d340e66fadf479c2d12115338631a30363b0b94cfeb48e1b29f"
    );
    assert!(
        !serde_json::to_value(&source)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("expected_recommendation_bid_kopecks")
    );
    let explicit = serde_json::to_vec(&target).unwrap();
    assert_ne!(Sha256::digest(&legacy), Sha256::digest(&explicit));
    // Opting in without a new authorization cannot be adopted into durable state.
    let mut reused_authorization = source.clone();
    reused_authorization.expected_recommendation_bid_kopecks = Some(153);
    assert!(
        validate_wb_automation_recommendations_update(&source, &reused_authorization, now())
            .is_err()
    );
    target.placement = "recommendations".into();
    assert!(validate_wb_automation_policy(&target).is_err());
    for bid in [0, u64::MAX] {
        let mut target = source.clone();
        target.expected_recommendation_bid_kopecks = Some(bid);
        assert!(validate_wb_automation_policy(&target).is_err());
    }
}

#[test]
fn authorization_cannot_change_financial_or_campaign_scope() {
    let source = source();
    let target = target();
    let base = serde_json::to_value(&target).unwrap();
    for (key, value) in base.as_object().unwrap() {
        if [
            "authorization_reference",
            "authorized_at",
            "observe_until",
            "authorization_expires_at",
            "expected_recommendation_bid_kopecks",
        ]
        .contains(&key.as_str())
        {
            continue;
        }
        let mut changed = base.clone();
        changed[key] = match value {
            serde_json::Value::Bool(value) => (!value).into(),
            serde_json::Value::Number(value) => (value.as_u64().unwrap() + 1).into(),
            serde_json::Value::String(value) => format!("{value}_changed").into(),
            serde_json::Value::Array(value) => {
                let mut value = value.clone();
                value.push(999.into());
                value.into()
            }
            _ => panic!("unexpected policy field {key}"),
        };
        if let Ok(changed) = serde_json::from_value::<WbAutomationPolicy>(changed) {
            assert!(
                validate_wb_automation_recommendations_update(&source, &changed, now()).is_err(),
                "{key}"
            );
        }
    }
    for field in ["authorized_at", "observe_until", "authorization_expires_at"] {
        let mut changed = base.clone();
        changed[field] = if field == "authorization_expires_at" {
            now().to_rfc3339().into()
        } else {
            (now() + Duration::minutes(1)).to_rfc3339().into()
        };
        let changed = serde_json::from_value(changed).unwrap();
        assert!(
            validate_wb_automation_recommendations_update(&source, &changed, now()).is_err(),
            "{field}"
        );
    }
    let mut excessive = target.clone();
    excessive.authorization_expires_at = now() + Duration::days(32);
    assert!(validate_wb_automation_recommendations_update(&source, &excessive, now()).is_err());
    let mut unchanged = target;
    unchanged.authorization_reference = "test/repeated-authorization".into();
    assert!(validate_wb_automation_recommendations_update(&unchanged, &unchanged, now()).is_err());
}
