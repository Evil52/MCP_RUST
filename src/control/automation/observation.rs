use super::{
    BTreeMap, BTreeSet, WbAutomationDecisionError, WbAutomationObservation, WbAutomationPolicy,
    WbAutomationSkuObservation,
};

pub(super) fn validate_observation<'a>(
    policy: &WbAutomationPolicy,
    observation: &'a WbAutomationObservation,
) -> Result<BTreeMap<u64, &'a WbAutomationSkuObservation>, WbAutomationDecisionError> {
    if observation
        .last_action_at
        .is_some_and(|last_action| last_action > observation.observed_at)
        || observation.attribution_complete && observation.campaign_level_metrics.is_some()
        || [
            observation.campaign_level_metrics.as_ref(),
            observation.current_campaign_metrics.as_ref(),
        ]
        .into_iter()
        .flatten()
        .any(|metrics| metrics.clicks > metrics.impressions)
    {
        return Err(WbAutomationDecisionError::InvalidObservation);
    }
    let mut observations = BTreeMap::new();
    for sku in &observation.skus {
        let vendor_minimum_valid = (1..=policy.max_bid_kopecks).contains(&sku.minimum_bid_kopecks);
        if sku.nm_id == 0
            || !vendor_minimum_valid
            || sku.current_bid_kopecks == 0
            || sku.clicks > sku.impressions
            || observations.insert(sku.nm_id, sku).is_some()
        {
            return Err(WbAutomationDecisionError::InvalidObservation);
        }
    }
    let expected = policy.nm_ids.iter().copied().collect::<BTreeSet<_>>();
    let actual = observations.keys().copied().collect::<BTreeSet<_>>();
    if expected != actual {
        return Err(WbAutomationDecisionError::InvalidObservation);
    }
    Ok(observations)
}
