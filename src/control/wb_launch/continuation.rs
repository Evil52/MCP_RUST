//! Append-only approval for the actual Nexus create. No reset, fabricated
//! marketplace receipt, new creation or broadened funding source is allowed.
use super::{
    ACCOUNT, DateTime, Deserialize, Journal, LaunchScope, Manifest, NAME, NMS, Operator, Path,
    Result, Serialize, Utc, Value, ensure,
    journal::{digest, entry_exists, read_private_json},
    json,
};
use anyhow::Context;

pub(super) const CAMPAIGN_ID: u64 = 40_141_836;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Approval {
    pub campaign_id: u64,
    pub previous_manifest_sha256: String,
    pub previous_attempt_sha256: String,
    pub previous_receipt_sha256: String,
    pub previous_policy_sha256: String,
}

impl Approval {
    pub(super) fn validate_scope(&self, manifest: &Manifest) -> Result<()> {
        ensure!(
            self.campaign_id == CAMPAIGN_ID
                && manifest.account_id == ACCOUNT
                && manifest.campaign_name == NAME
                && manifest.scope == LaunchScope::FundAndStart
                && manifest.recreate.is_none()
                && manifest.budget_rubles == 1000
                && manifest.funding_type == 1
                && manifest.bids_kopecks.keys().copied().eq(NMS)
                && manifest.bids_kopecks.values().all(|bid| *bid == 922),
            "continuation requires confirmed Nexus, five bids of 922 and 1000 RUB type=1"
        );
        Ok(())
    }
}

pub(super) fn validate(directory: &Path, value: &Value) -> Result<()> {
    let manifest: Manifest = serde_json::from_value(value.clone())?;
    let approval = manifest
        .continue_created
        .as_ref()
        .context("continuation approval missing")?;
    approval.validate_scope(&manifest)?;
    let mut records = Vec::new();
    for (name, expected) in [
        ("manifest", &approval.previous_manifest_sha256),
        ("create-attempted", &approval.previous_attempt_sha256),
        ("create-receipt", &approval.previous_receipt_sha256),
        ("policy-receipt", &approval.previous_policy_sha256),
    ] {
        let record: Value = read_private_json(&directory.join(format!("recreate-{name}.json")))?;
        ensure!(
            digest(&serde_json::to_vec(&record)?) == *expected,
            "confirmed create evidence changed"
        );
        records.push(record);
    }
    // Keep the original failed attempt linked too, not just the successful one.
    super::journal::validate_recreate(directory, &records[0])?;
    let previous: Manifest = serde_json::from_value(records[0].clone())?;
    let attempted_at: DateTime<Utc> = serde_json::from_value(records[1]["attempted_at"].clone())?;
    ensure!(
        previous.continue_created.is_none()
            && previous.scope == LaunchScope::CreateOnly
            && previous.account_id == manifest.account_id
            && previous.campaign_name == manifest.campaign_name
            && previous.bids_kopecks == manifest.bids_kopecks
            && previous.authorization_reference != manifest.authorization_reference
            && manifest.authorized_at > previous.authorized_at
            && manifest.authorized_at > attempted_at
            && records[1]["evidence"].is_object()
            && records[2]["campaign_id"] == CAMPAIGN_ID
            && records[2]["wb_http"] == 200
            && records[3]["campaign_id"] == CAMPAIGN_ID,
        "continuation requires new approval for a confirmed create"
    );
    // Failed/partial prior writes cannot be migrated to a fresh retry key.
    for stage in [
        "bids-attempted",
        "bids-receipt",
        "fund-attempted",
        "fund-response-receipt",
        "fund-receipt",
        "start-attempted",
        "start-receipt",
    ] {
        for prefix in ["", "recreate-"] {
            ensure!(
                !entry_exists(&directory.join(format!("{prefix}{stage}.json")))?,
                "earlier trading attempt exists; reconcile only"
            );
        }
    }
    let existing = directory.join("continue-manifest.json");
    if entry_exists(&existing)? {
        ensure!(
            read_private_json::<Value>(&existing)? == *value,
            "continuation approval is immutable"
        );
    }
    Ok(())
}

impl Operator {
    pub(super) fn continuation_preflight_id(&self, supplied: Option<u64>) -> Result<Option<u64>> {
        let Some(approval) = &self.manifest.continue_created else {
            return Ok(supplied);
        };
        validate(
            &self.manifest.journal_directory.join("ofk_region_wb-Nexus"),
            &serde_json::to_value(&self.manifest)?,
        )?;
        ensure!(
            supplied.is_none_or(|id| id == approval.campaign_id),
            "continuation campaign mismatch"
        );
        Ok(Some(approval.campaign_id))
    }

    /// Reads WB and appends the reviewed policy. No marketplace write or
    /// activation/import of robot state. Re-running does not replace a receipt.
    pub(super) async fn prepare_continuation(&self, journal: &Journal) -> Result<Value> {
        ensure!(
            self.manifest.continue_created.is_some(),
            "prepare requires continuation approval"
        );
        journal.assert_not_attempted("bids")?;
        journal.assert_not_attempted("fund")?;
        journal.assert_not_attempted("start")?;
        let id = journal.campaign_id()?;
        let preflight = self.preflight(Some(id)).await?;
        ensure!(
            self.inactive(id, false).await?.status == 4,
            "continuation requires unstarted Nexus"
        );
        ensure!(
            self.budget(id).await? == 0,
            "Nexus already funded; reconcile only"
        );
        self.minimums(id).await?;
        self.fresh_authorization()?;
        let policy = serde_json::to_value(self.target_policy(id))?;
        if journal.has_receipt("policy") {
            ensure!(
                journal.require_receipt("policy")? == policy,
                "prepared policy changed"
            );
        } else {
            journal.receipt("policy", &policy)?;
        }
        Ok(json!({"outcome":"continuation_prepared", "campaign_id":id,
            "preflight":preflight,"marketplace_write_sent":false,"robot_installed":false}))
    }

    pub(super) fn validate_prepared_continuation(&self, journal: &Journal, id: u64) -> Result<()> {
        ensure!(
            journal.require_receipt("policy")? == serde_json::to_value(self.target_policy(id))?,
            "prepare continuation before initial bids"
        );
        Ok(())
    }
}
