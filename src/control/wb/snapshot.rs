use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::config::is_canonical_uuid;

use super::{MAX_CHANGES, WbBidChange, WbCampaignBidSnapshot, WbPreparedBidChange, WbSnapshotBid};
use crate::control::policy::{BidLimits, WbBidPlacement, WbPromotionBidTargetPolicy};

fn require_exact_advert(response: &Value, advert_id: u64) -> Result<&Value> {
    let adverts = response
        .get("adverts")
        .and_then(Value::as_array)
        .context("WB campaign details не содержит adverts")?;
    let mut matching_adverts = adverts
        .iter()
        .filter(|advert| advert.get("id").and_then(Value::as_u64) == Some(advert_id));
    let advert = matching_adverts
        .next()
        .context("WB campaign details не содержит запрошенную кампанию")?;
    if matching_adverts.next().is_some() {
        bail!("WB campaign details содержит повтор запрошенной кампании");
    }
    Ok(advert)
}

fn require_exact_nm_setting(nm_settings: &[Value], nm_id: u64) -> Result<&Value> {
    let mut matching_nm = nm_settings
        .iter()
        .filter(|item| item.get("nm_id").and_then(Value::as_u64) == Some(nm_id));
    let nm = matching_nm
        .next()
        .with_context(|| format!("nm_id {nm_id} отсутствует в WB campaign"))?;
    if matching_nm.next().is_some() {
        bail!("nm_id {nm_id} повторяется в WB campaign");
    }
    Ok(nm)
}

fn placement_bid(nm: &Value, bid_type: &str, placement: WbBidPlacement) -> Result<u64> {
    match placement {
        WbBidPlacement::Combined => {
            if bid_type != "unified" {
                bail!("placement combined разрешён только для unified bid_type");
            }
            let search = nm
                .pointer("/bids_kopecks/search")
                .and_then(Value::as_u64)
                .context("WB campaign не содержит search bid")?;
            let recommendations = nm
                .pointer("/bids_kopecks/recommendations")
                .and_then(Value::as_u64)
                .context("WB campaign не содержит recommendations bid")?;
            if search != recommendations {
                bail!("unified WB campaign вернула разные ставки размещений");
            }
            Ok(search)
        }
        WbBidPlacement::Search => {
            if bid_type != "manual" {
                bail!("placement search разрешён только для manual bid_type");
            }
            nm.pointer("/bids_kopecks/search")
                .and_then(Value::as_u64)
                .context("WB campaign не содержит search bid")
        }
        WbBidPlacement::Recommendations => {
            if bid_type != "manual" {
                bail!("placement recommendations разрешён только для manual bid_type");
            }
            nm.pointer("/bids_kopecks/recommendations")
                .and_then(Value::as_u64)
                .context("WB campaign не содержит recommendations bid")
        }
    }
}

pub(in crate::control) fn campaign_snapshot(
    response: &Value,
    seller_sid: &str,
    advert_id: u64,
    requested: &[WbBidChange],
) -> Result<WbCampaignBidSnapshot> {
    if !is_canonical_uuid(seller_sid) {
        bail!("WB campaign snapshot имеет неверный seller_sid");
    }
    if requested.is_empty() || requested.len() > MAX_CHANGES {
        bail!("WB campaign snapshot request имеет неверный размер");
    }
    let advert = require_exact_advert(response, advert_id)?;
    let status = advert
        .get("status")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .context("WB campaign details содержит неверный status")?;
    if !matches!(status, 4 | 9 | 11) {
        bail!("WB campaign status не допускает изменение ставок");
    }
    let bid_type = advert
        .get("bid_type")
        .and_then(Value::as_str)
        .context("WB campaign details не содержит bid_type")?
        .to_owned();
    let payment_type = advert
        .pointer("/settings/payment_type")
        .and_then(Value::as_str)
        .filter(|value| matches!(*value, "cpm" | "cpc"))
        .context("WB campaign details не содержит поддерживаемый payment_type")?
        .to_owned();
    let nm_settings = advert
        .get("nm_settings")
        .and_then(Value::as_array)
        .context("WB campaign details не содержит nm_settings")?;

    let unique = requested
        .iter()
        .map(|change| (change.nm_id, change.placement))
        .collect::<BTreeSet<_>>();
    if unique.len() != requested.len() {
        bail!("WB bid changes содержат повтор nm_id/placement");
    }
    let mut bids = Vec::with_capacity(requested.len());
    for change in requested {
        let nm = require_exact_nm_setting(nm_settings, change.nm_id)?;
        let before = placement_bid(nm, &bid_type, change.placement)?;
        bids.push(WbSnapshotBid {
            nm_id: change.nm_id,
            placement: change.placement,
            bid_kopecks: before,
        });
    }
    bids.sort_by_key(|bid| (bid.nm_id, bid.placement));
    Ok(WbCampaignBidSnapshot {
        seller_sid: seller_sid.to_owned(),
        advert_id,
        status,
        bid_type,
        payment_type,
        bids,
    })
}

pub(in crate::control) fn prepare_changes(
    target: &WbPromotionBidTargetPolicy,
    requested: &[WbBidChange],
    snapshot: &WbCampaignBidSnapshot,
) -> Result<Vec<WbPreparedBidChange>> {
    if target.seller_sid != snapshot.seller_sid
        || target.advert_id != snapshot.advert_id
        || requested.is_empty()
        || requested.len() > MAX_CHANGES
    {
        bail!("WB bid changes находятся вне policy scope");
    }
    let allowed_nm_ids = target.nm_ids.iter().copied().collect::<BTreeSet<_>>();
    let allowed_placements = target.placements.iter().copied().collect::<BTreeSet<_>>();
    let unique = requested
        .iter()
        .map(|change| (change.nm_id, change.placement))
        .collect::<BTreeSet<_>>();
    if unique.len() != requested.len()
        || requested.iter().any(|change| {
            !allowed_nm_ids.contains(&change.nm_id)
                || !allowed_placements.contains(&change.placement)
        })
    {
        bail!("WB bid changes находятся вне policy scope");
    }

    let mut prepared = Vec::with_capacity(requested.len());
    for change in requested {
        let before = snapshot
            .bids
            .iter()
            .find(|bid| bid.nm_id == change.nm_id && bid.placement == change.placement)
            .context("WB snapshot не содержит текущую ставку")?
            .bid_kopecks;
        validate_bid_delta(&target.bid_limits_kopecks, before, change.bid_kopecks)?;
        prepared.push(WbPreparedBidChange {
            nm_id: change.nm_id,
            placement: change.placement,
            before_bid_kopecks: before,
            bid_kopecks: change.bid_kopecks,
        });
    }
    prepared.sort_by_key(|change| (change.nm_id, change.placement));
    Ok(prepared)
}

pub(super) fn validate_bid_delta(limits: &BidLimits, before: u64, after: u64) -> Result<()> {
    if before > i64::MAX as u64 || after > i64::MAX as u64 {
        bail!("WB bid должен помещаться в int64");
    }
    if after < limits.min_minor || after > limits.max_minor {
        bail!("WB bid выходит за серверные min/max policy limits");
    }
    if before == 0 {
        bail!("нулевую текущую WB ставку нельзя менять без ручной проверки");
    }
    if before == after {
        bail!("WB bid change не должен быть no-op");
    }
    let delta = u128::from(before.abs_diff(after));
    if delta * 100 > u128::from(before) * u128::from(limits.max_delta_percent) {
        bail!("изменение WB bid превышает max_delta_percent");
    }
    Ok(())
}

pub(super) fn snapshot_matches_expected(
    snapshot: &WbCampaignBidSnapshot,
    changes: &[WbPreparedBidChange],
    expected_after: bool,
) -> bool {
    changes.iter().all(|change| {
        snapshot
            .bids
            .iter()
            .find(|bid| bid.nm_id == change.nm_id && bid.placement == change.placement)
            .is_some_and(|bid| {
                bid.bid_kopecks
                    == if expected_after {
                        change.bid_kopecks
                    } else {
                        change.before_bid_kopecks
                    }
            })
    })
}

/// Compares all control-relevant campaign metadata bound into the plan plus
/// the exact requested bid pairs. A queued write must not cross a campaign
/// status, bid-type or payment-type transition unnoticed.
pub(in crate::control) fn snapshot_matches_plan_state(
    snapshot: &WbCampaignBidSnapshot,
    before: &WbCampaignBidSnapshot,
    changes: &[WbPreparedBidChange],
    expected_after: bool,
) -> bool {
    snapshot.seller_sid == before.seller_sid
        && snapshot.advert_id == before.advert_id
        && snapshot.status == before.status
        && snapshot.bid_type == before.bid_type
        && snapshot.payment_type == before.payment_type
        && snapshot.bids.len() == before.bids.len()
        && before.bids.len() == changes.len()
        && snapshot_matches_expected(snapshot, changes, expected_after)
}
