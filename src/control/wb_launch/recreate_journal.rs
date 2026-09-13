//! Exactly one explicitly reauthorized create revision. Original files survive.
use super::{digest, read_private_json};
use crate::control::wb_launch::{LaunchScope, Manifest, NMS};
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::path::Path;

pub(super) fn validate(directory: &Path, new: &Value) -> Result<()> {
    let manifest: Manifest = serde_json::from_value(new.clone())?;
    let approval = manifest
        .recreate
        .as_ref()
        .context("replacement approval missing")?;
    ensure!(
        manifest.scope == LaunchScope::CreateOnly
            && manifest.budget_rubles == 0
            && manifest.funding_type == 1
            && manifest.account_id == "ofk_region_wb"
            && manifest.campaign_name == "Nexus"
            && manifest.bids_kopecks.keys().copied().eq(NMS),
        "replacement scope invalid"
    );
    let old: Value = read_private_json(&directory.join("manifest.json"))?;
    let attempt: Value = read_private_json(&directory.join("create-attempted.json"))?;
    ensure!(
        digest(&serde_json::to_vec(&old)?) == approval.previous_manifest_sha256
            && digest(&serde_json::to_vec(&attempt)?) == approval.previous_attempt_sha256,
        "previous immutable evidence hash mismatch"
    );
    let old: Manifest = serde_json::from_value(old)?;
    let attempted_at: chrono::DateTime<chrono::Utc> =
        serde_json::from_value(attempt["attempted_at"].clone())?;
    ensure!(
        attempt.get("evidence").is_some_and(Value::is_object),
        "previous attempt evidence missing"
    );
    ensure!(
        old.recreate.is_none()
            && old.scope == LaunchScope::CreateOnly
            && old.budget_rubles == 0
            && old.funding_type == 1
            && old.account_id == manifest.account_id
            && old.campaign_name == manifest.campaign_name
            && old.bids_kopecks.keys().copied().eq([
                146_312_604,
                207_418_966,
                455_101_276,
                461_126_890,
                529_996_417
            ])
            && old.authorization_reference != manifest.authorization_reference
            && manifest.authorized_at > old.expires_at
            && manifest.authorized_at > attempted_at,
        "replacement requires a new authorization for the original create-only attempt"
    );
    for name in [
        "create-receipt",
        "bids-attempted",
        "bids-receipt",
        "policy-receipt",
        "fund-attempted",
        "fund-receipt",
        "start-attempted",
        "start-receipt",
    ] {
        ensure!(
            !directory.join(format!("{name}.json")).try_exists()?,
            "prior operation progressed beyond unconfirmed create; reconcile only"
        );
    }
    Ok(())
}
