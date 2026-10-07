//! Explicit policy renewal and recovery of positive bids below its lower bound.
use super::{
    DateTime, Duration, Utc, WbAutomationAction, WbAutomationBidChange, WbAutomationBidReason,
    WbAutomationPolicy, WbAutomationSkuObservation, validate_policy,
};
use anyhow::{Result, ensure};

/// Validate renewal and optional bid bounds. All other policy fields stay exact.
pub fn validate_wb_automation_corridor_update(
    source: &WbAutomationPolicy,
    target: &WbAutomationPolicy,
    now: DateTime<Utc>,
) -> Result<()> {
    validate_policy(source)?;
    validate_policy(target)?;
    ensure!(
        source.write_enabled && source.bid_writes_enabled,
        "corridor renewal requires an existing bid-live policy"
    );
    ensure!(
        target.authorization_reference != source.authorization_reference
            && target.authorized_at >= source.authorized_at
            && target.authorized_at <= now
            && target.observe_until <= now
            && now < target.authorization_expires_at
            && target.authorization_expires_at - target.authorized_at <= Duration::days(31),
        "corridor renewal requires a new active authorization of at most 31 days"
    );
    let mut expected = source.clone();
    expected
        .authorization_reference
        .clone_from(&target.authorization_reference);
    expected.authorized_at = target.authorized_at;
    expected.observe_until = target.observe_until;
    expected.authorization_expires_at = target.authorization_expires_at;
    expected.min_bid_kopecks = target.min_bid_kopecks;
    expected.max_bid_kopecks = target.max_bid_kopecks;
    ensure!(
        target == &expected,
        "corridor renewal changes an unrelated policy field"
    );
    Ok(())
}

/// Campaign guards and hard stops precede this repair. One eligible bid is
/// restored before ordinary exploration, preserving cooldown and action quota.
/// A stopped SKU may remain below the floor without freezing healthy peers.
///
/// The floor is the effective one: when WB raises its own minimum above the
/// current bid, the SKU is repaired here under its true reason instead of a
/// later "decrease" quietly raising the bid of a SKU the policy was cutting.
pub(super) fn recover_minimum<'a>(
    policy: &WbAutomationPolicy,
    skus: impl Iterator<Item = &'a WbAutomationSkuObservation>,
) -> Option<WbAutomationAction> {
    let sku = skus
        .filter(|sku| {
            sku.sellable_stock > policy.min_sellable_stock
                && sku.current_bid_kopecks < super::effective_minimum_bid(policy, sku)
        })
        .min_by_key(|sku| (sku.current_bid_kopecks, sku.nm_id))?;
    Some(WbAutomationAction::ChangeBids {
        changes: vec![WbAutomationBidChange {
            nm_id: sku.nm_id,
            from_bid_kopecks: sku.current_bid_kopecks,
            to_bid_kopecks: super::effective_minimum_bid(policy, sku),
            reason: WbAutomationBidReason::PolicyMinimumNotMet,
        }],
    })
}

#[cfg(test)]
mod tests;
