//! An explicit authorization for existing, unmanaged recommendation delivery.
use super::{DateTime, Duration, Utc, WbAutomationPolicy, validate_policy};
use anyhow::{Result, ensure};

/// Only the expected recommendation bid and authorization may change. Search
/// bidding, financial limits, SKU scope and durable protective state stay exact.
pub fn validate_wb_automation_recommendations_update(
    source: &WbAutomationPolicy,
    target: &WbAutomationPolicy,
    now: DateTime<Utc>,
) -> Result<()> {
    validate_policy(source)?;
    validate_policy(target)?;
    ensure!(
        source.write_enabled && source.bid_writes_enabled,
        "recommendations authorization requires an existing bid-live policy"
    );
    ensure!(
        target.expected_recommendation_bid_kopecks.is_some()
            && target.expected_recommendation_bid_kopecks
                != source.expected_recommendation_bid_kopecks,
        "recommendations authorization requires a changed, positive expected bid"
    );
    ensure!(
        target.authorization_reference != source.authorization_reference
            && target.authorized_at >= source.authorized_at
            && target.authorized_at <= now
            && target.observe_until <= now
            && now < target.authorization_expires_at
            && target.authorization_expires_at - target.authorized_at <= Duration::days(31),
        "recommendations authorization requires a new active authorization of at most 31 days"
    );
    let mut expected = source.clone();
    expected.expected_recommendation_bid_kopecks = target.expected_recommendation_bid_kopecks;
    expected
        .authorization_reference
        .clone_from(&target.authorization_reference);
    expected.authorized_at = target.authorized_at;
    expected.observe_until = target.observe_until;
    expected.authorization_expires_at = target.authorization_expires_at;
    ensure!(
        target == &expected,
        "recommendations authorization changes an unrelated policy field"
    );
    Ok(())
}
