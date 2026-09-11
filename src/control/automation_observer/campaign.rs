use super::WbAutomationPolicy;
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug)]
pub(in crate::control) struct CampaignObservation {
    pub(in crate::control) status: i32,
    pub(in crate::control) bids: BTreeMap<u64, u64>,
}

pub(in crate::control) fn parse_campaign(
    response: &Value,
    policy: &WbAutomationPolicy,
) -> Result<CampaignObservation> {
    let adverts = response
        .get("adverts")
        .and_then(Value::as_array)
        .context("WB campaign details не содержит adverts")?;
    let matching = adverts
        .iter()
        .filter(|advert| advert.get("id").and_then(Value::as_u64) == Some(policy.campaign_id))
        .collect::<Vec<_>>();
    ensure!(
        matching.len() == 1,
        "WB automation не нашёл ровно одну разрешённую campaign"
    );
    let advert = matching[0];
    ensure!(
        advert.pointer("/settings/name").and_then(Value::as_str)
            == Some(policy.campaign_name.as_str())
            && advert.get("bid_type").and_then(Value::as_str) == Some("manual")
            && advert
                .pointer("/settings/payment_type")
                .and_then(Value::as_str)
                == Some("cpc")
            && advert
                .pointer("/settings/placements/search")
                .and_then(Value::as_bool)
                == Some(true)
            && advert
                .pointer("/settings/placements/recommendations")
                .and_then(Value::as_bool)
                == Some(false),
        "WB automation campaign contract изменился"
    );
    let status = advert
        .get("status")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .context("WB automation campaign status неверен")?;
    let nm_settings = advert
        .get("nm_settings")
        .and_then(Value::as_array)
        .context("WB automation campaign не содержит nm_settings")?;
    let mut bids = BTreeMap::new();
    for setting in nm_settings {
        let nm_id = setting
            .get("nm_id")
            .and_then(Value::as_u64)
            .context("WB automation campaign nm_id неверен")?;
        let bid = setting
            .pointer("/bids_kopecks/search")
            .and_then(Value::as_u64)
            .context("WB automation campaign search bid отсутствует")?;
        let recommendations = setting
            .pointer("/bids_kopecks/recommendations")
            .and_then(Value::as_u64)
            .context("WB automation campaign recommendations bid отсутствует")?;
        ensure!(
            recommendations == 0 && bids.insert(nm_id, bid).is_none(),
            "WB automation campaign содержит неожиданную ставку или duplicate SKU"
        );
    }
    let expected = policy.nm_ids.iter().copied().collect::<BTreeSet<_>>();
    ensure!(
        bids.keys().copied().collect::<BTreeSet<_>>() == expected,
        "WB automation campaign SKU scope изменился"
    );
    Ok(CampaignObservation { status, bids })
}
