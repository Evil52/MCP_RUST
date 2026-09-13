use serde_json::{Value, json};

use super::*;

fn started() -> DateTime<Utc> {
    "2026-09-13T11:07:00Z".parse().unwrap()
}

fn key() -> String {
    "a".repeat(64)
}

fn seller() -> String {
    "b".repeat(64)
}

fn receipt_hash() -> String {
    "c".repeat(64)
}

fn legacy() -> Quota {
    serde_json::from_value(json!({"next_allowed_at":started() + CONSERVATIVE})).unwrap()
}

fn receipt_value() -> Value {
    json!({
        "version":1, "actor_id":"finance-operator", "key_fingerprint":key(),
        "seller_scope":seller(),
        "endpoint":"POST https://finance-api.wildberries.ru/api/finance/v1/sales-reports/detailed",
        "http_status":200, "request_started_at":started(),
        "expected_next_allowed_at":started() + CONSERVATIVE,
        "operator_verified_at":started() + Duration::minutes(5),
        "evidence_ref":"synthetic-access-receipt",
    })
}

fn receipt() -> LegacySuccessReceipt {
    serde_json::from_value(receipt_value()).unwrap()
}

fn migrate(quota: &mut Quota, receipt: &LegacySuccessReceipt) -> Result<()> {
    quota.migrate(
        receipt,
        "finance-operator",
        &key(),
        &seller(),
        started() + Duration::minutes(10),
        receipt_hash(),
    )
}

fn snapshot(quota: &Quota) -> Value {
    serde_json::to_value(quota).unwrap()
}

#[test]
fn first_reservation_is_conservative_and_only_matching_success_confirms_personal() {
    let mut quota = Quota::default();
    assert!(quota.confirm(&key(), started()).is_err());
    assert!(quota.reserve(started(), Some(&key())).unwrap());
    assert_eq!(quota.next_allowed_at, Some(started() + CONSERVATIVE));
    assert!(!quota.reserve(started() + PERSONAL, Some(&key())).unwrap());
    let before = snapshot(&quota);
    assert!(
        quota
            .confirm(&key(), started() + Duration::seconds(1))
            .is_err()
    );
    assert_eq!(snapshot(&quota), before);
    assert!(quota.confirm("unbound-key", started()).is_err());
    assert_eq!(snapshot(&quota), before);
    quota.confirm(&key(), started()).unwrap();
    quota.validate().unwrap();
    assert_eq!(quota.next_allowed_at, Some(started() + PERSONAL));
}

#[test]
fn persisted_personal_proof_survives_restart_but_rotated_key_reserves_twelve_hours() {
    let mut quota = Quota::default();
    quota.reserve(started(), Some(&key())).unwrap();
    quota.confirm(&key(), started()).unwrap();
    let encoded = serde_json::to_vec(&quota).unwrap();
    let mut resumed: Quota = serde_json::from_slice(&encoded).unwrap();
    resumed.validate().unwrap();
    assert!(resumed.reserve(started() + PERSONAL, Some(&key())).unwrap());
    assert_eq!(resumed.next_allowed_at, Some(started() + PERSONAL * 2));
    let rotated = "d".repeat(64);
    assert!(
        resumed
            .reserve(started() + PERSONAL * 2, Some(&rotated))
            .unwrap()
    );
    assert_eq!(
        resumed.next_allowed_at,
        Some(started() + PERSONAL * 2 + CONSERVATIVE)
    );
    resumed.confirm(&rotated, started() + PERSONAL * 2).unwrap();
    assert_eq!(resumed.next_allowed_at, Some(started() + PERSONAL * 3));
    let mut unknown = Quota::default();
    unknown.reserve(started(), None).unwrap();
    assert_eq!(unknown.next_allowed_at, Some(started() + CONSERVATIVE));
}

#[test]
fn server_retry_after_is_monotonic_and_survives_personal_confirmation_and_restart() {
    let mut quota = Quota::default();
    quota.reserve(started(), Some(&key())).unwrap();
    quota.postpone(started(), 7200).unwrap();
    quota.confirm(&key(), started()).unwrap();
    let due = started() + Duration::hours(2);
    assert_eq!(quota.next_allowed_at, Some(due));
    quota.postpone(started() + PERSONAL, 1).unwrap();
    quota.confirm(&key(), started()).unwrap();
    assert_eq!(quota.next_allowed_at, Some(due));
    let mut resumed: Quota = serde_json::from_value(snapshot(&quota)).unwrap();
    resumed.validate().unwrap();
    assert!(
        !resumed
            .reserve(due - Duration::seconds(1), Some(&key()))
            .unwrap()
    );
    assert!(resumed.reserve(due, Some(&key())).unwrap());
    assert_eq!(resumed.next_allowed_at, Some(due + PERSONAL));
    resumed.postpone(due, 86400).unwrap();
    resumed.confirm(&key(), due).unwrap();
    assert_eq!(resumed.next_allowed_at, Some(due + Duration::hours(24)));
}

#[test]
fn future_legacy_quota_cannot_be_bypassed_by_a_personal_hint_or_confirmation() {
    let mut quota = legacy();
    quota.validate().unwrap();
    let before = snapshot(&quota);
    assert!(
        !quota
            .reserve(started() + Duration::hours(1), Some(&key()))
            .unwrap()
    );
    assert!(quota.confirm(&key(), started()).is_err());
    assert_eq!(snapshot(&quota), before);
    assert!(
        quota
            .reserve(started() + CONSERVATIVE, Some(&key()))
            .unwrap()
    );
    assert_eq!(quota.next_allowed_at, Some(started() + CONSERVATIVE * 2));
}

#[test]
fn exact_reviewed_legacy_success_migrates_once_and_remembers_its_receipt_hash() {
    for status in [200, 204] {
        let mut quota = legacy();
        let mut value = receipt_value();
        value["http_status"] = json!(status);
        let receipt = serde_json::from_value(value).unwrap();
        migrate(&mut quota, &receipt).unwrap();
        quota.validate().unwrap();
        assert_eq!(quota.next_allowed_at, Some(started() + PERSONAL));
        assert_eq!(snapshot(&quota)["legacy_receipt_sha256"], receipt_hash());
        let before = snapshot(&quota);
        assert!(migrate(&mut quota, &receipt).is_err());
        assert_eq!(snapshot(&quota), before);
        let resumed: Quota = serde_json::from_value(snapshot(&quota)).unwrap();
        resumed.validate().unwrap();
    }
}

#[test]
fn legacy_receipt_requires_exact_actor_key_seller_reservation_status_and_fixed_endpoint() {
    let cases = [
        ("version", json!(2)),
        ("actor_id", json!("another-operator")),
        ("key_fingerprint", json!("d".repeat(64))),
        ("seller_scope", json!("e".repeat(64))),
        (
            "endpoint",
            json!("POST https://finance-api.wildberries.ru/api/finance/v1/sales-reports/list"),
        ),
        (
            "endpoint",
            json!("POST https://localhost/api/finance/v1/sales-reports/detailed"),
        ),
        ("http_status", json!(201)),
        ("http_status", json!(401)),
        ("http_status", json!(429)),
        (
            "request_started_at",
            json!(started() + Duration::seconds(1)),
        ),
        (
            "expected_next_allowed_at",
            json!(started() + CONSERVATIVE + Duration::seconds(1)),
        ),
        (
            "operator_verified_at",
            json!(started() - Duration::seconds(1)),
        ),
        (
            "operator_verified_at",
            json!(started() + Duration::hours(1)),
        ),
        ("evidence_ref", json!("")),
        ("evidence_ref", json!("x".repeat(257))),
        ("evidence_ref", json!("evidence\nforged")),
    ];
    for (field, value) in cases {
        let mut modified = receipt_value();
        modified[field] = value;
        let receipt = serde_json::from_value(modified).unwrap();
        let mut quota = legacy();
        let before = snapshot(&quota);
        assert!(
            migrate(&mut quota, &receipt).is_err(),
            "accepted mismatch: {field}"
        );
        assert_eq!(snapshot(&quota), before, "mutated on mismatch: {field}");
    }
    for (actor, key, seller, hash) in [
        ("other".to_owned(), key(), seller(), receipt_hash()),
        (
            "finance-operator".to_owned(),
            "f".repeat(64),
            seller(),
            receipt_hash(),
        ),
        (
            "finance-operator".to_owned(),
            key(),
            "f".repeat(64),
            receipt_hash(),
        ),
        (
            "finance-operator".to_owned(),
            key(),
            seller(),
            "invalid-hash".to_owned(),
        ),
    ] {
        let mut quota = legacy();
        let before = snapshot(&quota);
        assert!(
            quota
                .migrate(
                    &receipt(),
                    &actor,
                    &key,
                    &seller,
                    started() + Duration::minutes(10),
                    hash
                )
                .is_err()
        );
        assert_eq!(snapshot(&quota), before);
    }
}

#[test]
fn observed_legacy_vendor_delay_can_never_be_reclassified_as_a_successful_probe() {
    let mut quota = legacy();
    quota.postpone(started(), 86400).unwrap();
    let before = snapshot(&quota);
    assert!(migrate(&mut quota, &receipt()).is_err());
    assert_eq!(snapshot(&quota), before);
    assert_eq!(quota.next_allowed_at, Some(started() + Duration::hours(24)));
}

#[test]
fn corrupt_or_unknown_quota_journals_are_rejected() {
    let at = started();
    for value in [
        json!({}),
        json!({"next_allowed_at":null}),
        json!({"version":1,"next_allowed_at":at}),
        json!({"version":2,"next_allowed_at":at}),
        json!({"version":0,"next_allowed_at":at,"last_attempt_at":at}),
        json!({"version":0,"next_allowed_at":at,"personal_key_fingerprint":key()}),
        json!({"version":0,"next_allowed_at":at,"legacy_receipt_sha256":receipt_hash()}),
        json!({"version":2,"next_allowed_at":at,"last_attempt_at":at}),
        json!({"version":2,"next_allowed_at":at+PERSONAL,"last_attempt_at":at}),
        json!({"version":2,"next_allowed_at":at+CONSERVATIVE,"last_attempt_at":at,"retry_after_until":at+CONSERVATIVE*2}),
        json!({"version":2,"next_allowed_at":at+CONSERVATIVE,"last_attempt_at":at,"personal_key_fingerprint":"bad"}),
        json!({"version":2,"next_allowed_at":at+CONSERVATIVE,"last_attempt_at":at,"legacy_receipt_sha256":"bad"}),
        json!({"next_allowed_at":at,"unexpected":"field"}),
    ] {
        if let Ok(quota) = serde_json::from_value::<Quota>(value.clone()) {
            assert!(
                quota.validate().is_err(),
                "accepted corrupt journal: {value}"
            );
        }
    }
}

#[test]
fn extreme_timestamps_and_delays_return_errors_without_panics_or_partial_mutation() {
    let mut quota = Quota::default();
    let before = snapshot(&quota);
    assert!(
        quota
            .reserve(DateTime::<Utc>::MAX_UTC, Some(&key()))
            .is_err()
    );
    assert_eq!(snapshot(&quota), before);
    quota.reserve(started(), Some(&key())).unwrap();
    for (at, seconds) in [
        (started(), u64::MAX),
        (started(), i64::MAX.unsigned_abs()),
        (DateTime::<Utc>::MAX_UTC, 1),
    ] {
        let before = snapshot(&quota);
        assert!(quota.postpone(at, seconds).is_err());
        assert_eq!(snapshot(&quota), before);
    }
    let extreme: Quota = serde_json::from_value(json!({
        "version":2, "last_attempt_at":DateTime::<Utc>::MAX_UTC,
        "next_allowed_at":DateTime::<Utc>::MAX_UTC,
    }))
    .unwrap();
    assert!(extreme.validate().is_err());
    let mut extreme = extreme;
    let before = snapshot(&extreme);
    assert!(extreme.confirm(&key(), DateTime::<Utc>::MAX_UTC).is_err());
    assert_eq!(snapshot(&extreme), before);
    let mut modified = receipt_value();
    modified["request_started_at"] = json!(DateTime::<Utc>::MAX_UTC);
    let bad_receipt = serde_json::from_value(modified).unwrap();
    let mut quota = legacy();
    let before = snapshot(&quota);
    assert!(migrate(&mut quota, &bad_receipt).is_err());
    assert_eq!(snapshot(&quota), before);
}
