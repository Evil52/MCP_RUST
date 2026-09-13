use super::*;

fn wb_token(sid: &str, signature: &str) -> String {
    let payload =
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&serde_json::json!({"sid": sid})).unwrap());
    format!("header.{payload}.{signature}")
}

#[test]
fn vendor_bucket_and_identity_are_separate_and_opaque() {
    let seller = QuotaKey::ozon_seller("123", "api").unwrap();
    assert_eq!(seller, QuotaKey::ozon_seller("123", "api").unwrap());
    assert_ne!(seller, QuotaKey::ozon_performance("123", "api").unwrap());
    assert_ne!(seller, QuotaKey::ozon_seller("124", "api").unwrap());
    assert_ne!(seller, QuotaKey::ozon_seller("123", "analytics").unwrap());
    assert_eq!(seller.0.len(), 64);
    assert_eq!(format!("{seller:?}"), "QuotaKey(<opaque>)");
    for (identity, bucket) in [
        ("", "api"),
        ("123", ""),
        ("123", "bad/path"),
        (" 123", "api"),
    ] {
        assert_eq!(
            QuotaKey::ozon_seller(identity, bucket),
            Err(QuotaError::InvalidIdentity)
        );
    }
}

#[test]
fn token_rotation_and_different_tokens_share_seller_quota() {
    let sid = "11111111-1111-4111-8111-111111111111";
    let first = QuotaKey::wb(&wb_token(sid, "signature1"), "analytics").unwrap();
    let rotated = QuotaKey::wb(&wb_token(sid, "signature2"), "analytics").unwrap();
    assert_eq!(first, rotated);
    assert_ne!(
        first,
        QuotaKey::wb(
            &wb_token("22222222-2222-4222-8222-222222222222", "signature"),
            "analytics"
        )
        .unwrap()
    );
    for token in [
        "opaque".to_owned(),
        "a.b.c.d".to_owned(),
        "a.invalid!.c".to_owned(),
        wb_token("not-a-sid", "sig"),
        wb_token("00000000-0000-0000-0000-000000000000", "sig"),
        wb_token(sid, ""),
    ] {
        assert_eq!(
            QuotaKey::wb(&token, "analytics"),
            Err(QuotaError::InvalidIdentity)
        );
    }
}

#[tokio::test]
async fn only_absent_optional_configuration_can_disable_coordination() {
    use std::env::VarError::NotPresent;
    let disabled = SharedQuota::from_settings(Err(NotPresent), &Err(NotPresent));
    assert!(!disabled.is_enabled());
    assert!(disabled.preflight().await.is_ok());
    let key = QuotaKey::ozon_seller("123", "api").unwrap();
    assert!(disabled.admit(&key, Duration::ZERO).await.is_ok());
    assert!(disabled.defer(&key, Duration::ZERO).await.is_ok());
    for gate in [
        SharedQuota::from_settings(Err(NotPresent), &Ok("true".into())),
        SharedQuota::from_settings(Err(NotPresent), &Ok("invalid".into())),
        SharedQuota::from_database_url(""),
        SharedQuota::from_database_url("postgresql://position_admin:secret@localhost/db"),
        SharedQuota::from_database_url("postgresql://position_reader:secret@localhost/db"),
    ] {
        assert!(gate.is_enabled());
        assert_eq!(gate.preflight().await, Err(QuotaError::Unavailable));
        assert_eq!(
            gate.admit(&key, Duration::from_secs(1)).await,
            Err(QuotaError::Unavailable)
        );
        assert_eq!(
            gate.defer(&key, Duration::from_secs(1)).await,
            Err(QuotaError::Unavailable)
        );
        assert!(!format!("{gate:?}").contains("secret"));
    }
}

#[test]
fn delays_round_up_and_reject_unbounded_or_zero_values() {
    assert_eq!(bounded_millis(Duration::from_nanos(1)), Ok(1));
    assert_eq!(bounded_millis(Duration::from_micros(1001)), Ok(2));
    assert_eq!(bounded_millis(Duration::from_hours(24)), Ok(86_400_000));
    assert_eq!(bounded_millis(Duration::ZERO), Err(QuotaError::Unavailable));
    assert_eq!(bounded_millis(Duration::MAX), Err(QuotaError::Unavailable));
}

#[test]
fn cooldowns_never_shorten_long_vendor_delays() {
    assert_eq!(cooldown_millis(Duration::ZERO), 1);
    assert_eq!(cooldown_millis(Duration::from_hours(48)), 172_800_000);
    assert_eq!(cooldown_millis(Duration::MAX), i64::MAX);
    assert!(QuotaKey::ozon_performance("123-456@advertising.performance.ozon.ru", "api").is_ok());
}
