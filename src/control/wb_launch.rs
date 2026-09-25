//! Explicit local operator workflow. Not exposed through analytics MCP.
//! Each stage has an immutable, fsynced attempt record before HTTP. An
//! uncertain write permanently fences that stage; reconcile never writes WB.
//! The operator provisions an inactive campaign. Start is deliberately
//! separate from funding and must use the installed protective robot.

use super::{
    WbAutomationPolicy, WbBidPlacement, WbCampaignBidType, WbCampaignPaymentType,
    WbCreateCampaignRequest, WbPreparedBidChange,
    automation_observer::{CampaignObservation, parse_campaign},
    config::{read_control_token, validate_wb_reader_token, validate_wb_writer_token},
    wb::{WbBidWriteClient, WbGuardedWriteError},
};
use crate::{
    config::{Marketplace, RegistrySource, Role},
    wb::{WbClient, WbCredentials},
};
use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::Duration,
};

mod categories;
mod manifest;
mod setup;

pub use setup::{enroll_wb_campaign, export_wb_campaign, prepare_wb_campaign};
pub use setup::{prepare_wb_campaign_from_request, wb_campaign_profile_runtime};

/// Opaque reusable-campaign handle for an account/name identity. The legacy
/// Nexus journal is reserved and cannot become a new MCP campaign bundle.
pub fn wb_campaign_handle_for_name(account_id: &str, campaign_name: &str) -> Result<String> {
    let directory = journal::directory_name(&json!({
        "version": 2,
        "account_id": account_id,
        "campaign_name": campaign_name,
    }))?;
    Ok(directory
        .strip_prefix("campaign-")
        .context("legacy Nexus campaign identity is reserved")?
        .to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WbCampaignManifestScope {
    pub account_id: String,
    pub actor_id: String,
    pub campaign_name: String,
}

/// Bounded, read-only identity check for MCP handles resolved under a fixed root.
pub fn wb_campaign_manifest_scope(path: &Path) -> Result<WbCampaignManifestScope> {
    let manifest: Manifest = read_private_json(path)?;
    ensure_mcp_bundle_identity(path, &manifest)?;
    Ok(WbCampaignManifestScope {
        account_id: manifest.account_id,
        actor_id: manifest.actor_id,
        campaign_name: manifest.campaign_name,
    })
}

fn ensure_mcp_bundle_identity(path: &Path, manifest: &Manifest) -> Result<()> {
    ensure!(
        manifest.version == 2,
        "MCP requires a reusable campaign manifest"
    );
    let expected = format!(
        "campaign-{}",
        wb_campaign_handle_for_name(&manifest.account_id, &manifest.campaign_name,)?
    );
    ensure!(
        path.file_name().and_then(|name| name.to_str()) == Some("manifest.json")
            && path
                .parent()
                .and_then(|parent| parent.file_name())
                .and_then(|name| name.to_str())
                == Some(expected.as_str()),
        "MCP campaign handle and manifest identity differ"
    );
    Ok(())
}
mod continuation;
mod journal;
mod start;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod workflow_tests;
use journal::{Journal, read_policy_json, read_private_json};

const ACCOUNT: &str = "ofk_region_wb";
const NAME: &str = "Nexus";
const SOURCE: u64 = 39_807_762;
const NMS: [u64; 5] = [
    190_904_855,
    207_418_966,
    218_972_074,
    455_101_276,
    529_996_417,
];

/// Versioned, reviewed one-time launch authorization. Legacy documents keep
/// their exact Nexus scope; reusable documents bind an explicit cabinet and products.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    /// Version 1 preserves the original Nexus authorization and journal bytes.
    #[serde(
        default = "manifest::legacy_version",
        skip_serializing_if = "manifest::is_legacy"
    )]
    version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    manual_control: Option<setup::ManualControl>,
    scope: LaunchScope,
    account_id: String,
    campaign_name: String,
    source_policy: PathBuf,
    source_policy_sha256: String,
    bids_kopecks: BTreeMap<u64, u64>,
    budget_rubles: u64,
    funding_type: u8,
    actor_id: String,
    authorization_reference: String,
    authorized_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    registry: PathBuf,
    reader_token: PathBuf,
    writer_token: PathBuf,
    reader_proxy: String,
    writer_proxy: String,
    allow_broad_reader: bool,
    /// One stable private runtime directory. Never delete to retry a write.
    journal_directory: PathBuf,
    robot_policy: PathBuf,
    /// Explicit reviewed replacement of the original failed create, never funding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recreate: Option<RecreateApproval>,
    /// Fresh approval to fund/start the confirmed create-only Nexus, not recreate it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    continue_created: Option<continuation::Approval>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecreateApproval {
    previous_manifest_sha256: String,
    previous_attempt_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LaunchScope {
    CreateOnly,
    FundAndStart,
}

struct Operator {
    manifest_path: PathBuf,
    manifest: Manifest,
    seller_sid: String,
    policy: WbAutomationPolicy,
    reader: WbClient,
    writer: WbBidWriteClient,
}

impl Operator {
    fn load(path: &Path, allow_expired: bool) -> Result<Self> {
        let manifest: Manifest = read_private_json(path)?;
        let policy: WbAutomationPolicy = read_policy_json(&manifest.source_policy)?;
        manifest.validate(&policy, Utc::now(), allow_expired)?;
        let sid = validate_registry(&manifest)?;
        let reader_token = read_control_token(&manifest.reader_token, "WB_LAUNCH_READER")?;
        validate_wb_reader_token(&reader_token, &sid, manifest.allow_broad_reader)?;
        let writer_token = read_control_token(&manifest.writer_token, "WB_LAUNCH_WRITER")?;
        validate_wb_writer_token(&writer_token, &sid)?;
        let reader = WbClient::new_with_https_proxy(
            Duration::from_secs(30),
            BTreeMap::from([(
                manifest.account_id.clone(),
                WbCredentials {
                    token: reader_token,
                },
            )]),
            &manifest.reader_proxy,
        )?;
        let writer = WbBidWriteClient::new(
            Duration::from_secs(30),
            &writer_token,
            &manifest.writer_proxy,
        )?;
        Ok(Self {
            manifest_path: path.to_owned(),
            manifest,
            seller_sid: sid,
            policy,
            reader,
            writer,
        })
    }

    fn fresh_authorization(&self) -> Result<()> {
        let current: Manifest = read_private_json(&self.manifest_path)?;
        ensure!(
            serde_json::to_value(&current)? == serde_json::to_value(&self.manifest)?,
            "launch authorization changed or was revoked"
        );
        ensure!(
            validate_registry(&current)? == self.seller_sid,
            "seller binding changed while waiting for write slot"
        );
        let policy = read_policy_json(&self.manifest.source_policy)?;
        self.manifest.validate(&policy, Utc::now(), false)
    }

    fn target_policy(&self, id: u64) -> WbAutomationPolicy {
        self.manifest.target_policy(&self.policy, id)
    }

    async fn details(&self, id: u64) -> Result<CampaignObservation> {
        let details = self
            .reader
            .promotion_campaign_details(&self.manifest.account_id, vec![id], vec![], None)
            .await?;
        parse_campaign(&details, &self.target_policy(id))
    }

    async fn budget(&self, id: u64) -> Result<u64> {
        let value = self
            .reader
            .promotion_campaign_budget(&self.manifest.account_id, id)
            .await?;
        value
            .get("total")
            .and_then(Value::as_u64)
            .context("campaign budget unavailable")
    }

    async fn balance(&self) -> Result<u64> {
        let value = self
            .reader
            .promotion_balance(&self.manifest.account_id)
            .await?;
        value
            .get("net")
            .and_then(Value::as_u64)
            .context("WB type=1 netting balance (net) unavailable")
    }

    async fn preflight(&self, own_id: Option<u64>) -> Result<Value> {
        self.fresh_authorization()?;
        let own_id = self.continuation_preflight_id(own_id)?;
        if self.manifest.recreate.is_some() && own_id.is_none() {
            Journal::inspect_recreate(
                &self.manifest.journal_directory,
                &serde_json::to_value(&self.manifest)?,
            )?;
        }
        let subject_id = self.verify_category().await?;
        if self.manifest.version == 1 {
            let source = self
                .reader
                .promotion_campaign_details(&self.manifest.account_id, vec![SOURCE], vec![], None)
                .await?;
            parse_campaign(&source, &self.policy)?;
        }
        let groups = self
            .reader
            .promotion_campaigns(&self.manifest.account_id)
            .await?;
        let ids = if self.manifest.version == 2 || self.manifest.recreate.is_some() {
            recovery_campaign_ids(&groups, own_id)?
        } else {
            nonfinished_campaign_ids(&groups, own_id)?
        };
        let checked_campaign_count = ids.len();
        self.verify_no_campaign_overlap(ids).await?;
        let totals = self.verified_stock_totals().await?;
        let balance = if self.manifest.scope == LaunchScope::FundAndStart {
            let balance = self.balance().await?;
            ensure!(
                balance >= self.manifest.budget_rubles,
                "WB type=1 balance is {balance} RUB; {} RUB required",
                self.manifest.budget_rubles
            );
            Some(balance)
        } else {
            // Creating an inactive, unfunded campaign is not a financial
            // operation. Do not read or require a wallet balance in this scope.
            None
        };
        self.fresh_authorization()?;
        Ok(
            json!({"checked_at":Utc::now(),"account_id":self.manifest.account_id,"campaign_name":self.manifest.campaign_name,
            "netting_balance_rubles":balance,"scope":self.manifest.scope,
            "authorized_funding_rubles":self.manifest.budget_rubles,
            "subject_id":subject_id,
            "campaign_scan":{"listed_total":groups.get("all"),"checked_details":checked_campaign_count,
                "scope":if self.manifest.version == 2 || self.manifest.recreate.is_some(){"all_modern_including_terminal; obsolete_terminal_types_excluded"}else{"nonfinished"}},
            "bids_kopecks":self.manifest.bids_kopecks,"wb_stock":totals,
            "source_policy_sha256":self.manifest.source_policy_sha256,
            "daily_cap_kopecks":self.policy.daily_spend_cap_minor,"pause_threshold_kopecks":self.policy.daily_pause_threshold_minor,"target_drr_basis_points":self.policy.target_drr_basis_points,
            "auto_top_up":false,"credential_role":"seller-bound promotion-only dedicated writer"}),
        )
    }

    /// Rejects a launch while another campaign with this name exists or a launch SKU
    /// already belongs to a non-finished campaign. Every selected campaign
    /// must come back from the details endpoint.
    async fn verify_no_campaign_overlap(&self, ids: BTreeSet<u64>) -> Result<()> {
        for chunk in ids.into_iter().collect::<Vec<_>>().chunks(50) {
            let response = self
                .reader
                .promotion_campaign_details(&self.manifest.account_id, chunk.to_vec(), vec![], None)
                .await?;
            let adverts = response
                .get("adverts")
                .and_then(Value::as_array)
                .context("incomplete campaign details")?;
            let returned = adverts
                .iter()
                .filter_map(|ad| ad.get("id").and_then(Value::as_u64))
                .collect::<BTreeSet<_>>();
            ensure!(
                returned == chunk.iter().copied().collect() && adverts.len() == chunk.len(),
                "campaign overlap check did not return every selected campaign"
            );
            for ad in adverts {
                self.verify_campaign_does_not_overlap(ad)?;
            }
        }
        Ok(())
    }

    fn verify_campaign_does_not_overlap(&self, ad: &Value) -> Result<()> {
        ensure!(
            ad.pointer("/settings/name")
                .and_then(Value::as_str)
                .context("campaign name absent")?
                != self.manifest.campaign_name,
            "another campaign with this name exists; reconcile instead of creating a duplicate"
        );
        if (self.manifest.version == 2 || self.manifest.recreate.is_some())
            && matches!(ad.get("status").and_then(Value::as_i64), Some(-1 | 7 | 8))
        {
            return Ok(());
        }
        let nms = ad
            .get("nm_settings")
            .and_then(Value::as_array)
            .context("campaign SKU list absent")?;
        for nm in nms {
            let id = nm
                .get("nm_id")
                .and_then(Value::as_u64)
                .context("campaign SKU invalid")?;
            ensure!(
                !self.manifest.bids_kopecks.contains_key(&id),
                "SKU {id} already belongs to another non-finished campaign"
            );
        }
        Ok(())
    }

    /// Reads one complete stock page and requires the configured sellable stock
    /// for every launch SKU (the legacy Nexus floor remains 20).
    async fn verified_stock_totals(&self) -> Result<BTreeMap<u64, u64>> {
        let stocks = self
            .reader
            .warehouse_stocks(
                &self.manifest.account_id,
                json!({"nmIds":self.manifest.nm_ids(),"chrtIds":[],"limit":100,"offset":0}),
            )
            .await?;
        let (stocks, count) = crate::reporting::wb_adapter::parse_stock_page(&stocks)?;
        ensure!(count < 100, "stock page may be truncated");
        let mut totals = BTreeMap::<u64, u64>::new();
        for row in stocks {
            let total = totals.entry(row.sku).or_default();
            *total = total
                .checked_add(row.sellable_units)
                .context("stock overflow")?;
        }
        for nm in self.manifest.nm_ids() {
            ensure!(
                totals.get(&nm).copied().unwrap_or(0)
                    >= self.manifest.minimum_launch_stock(&self.policy),
                "SKU {nm} has insufficient verified WB stock"
            );
        }
        Ok(totals)
    }

    async fn minimums(&self, id: u64) -> Result<()> {
        let response = self
            .reader
            .promotion_minimum_bids(
                &self.manifest.account_id,
                id,
                self.manifest.nm_ids(),
                "cpc".to_owned(),
                vec!["search".to_owned()],
            )
            .await?;
        let rows = response
            .get("bids")
            .and_then(Value::as_array)
            .context("minimum bids missing")?;
        let mut seen = BTreeSet::new();
        for row in rows {
            let nm = row
                .get("nm_id")
                .and_then(Value::as_u64)
                .context("minimum nm_id invalid")?;
            ensure!(seen.insert(nm), "duplicate minimum bid");
            let floors = row
                .get("bids")
                .and_then(Value::as_array)
                .context("minimum bids invalid")?;
            let floors = floors
                .iter()
                .filter(|bid| bid.get("type").and_then(Value::as_str) == Some("search"))
                .collect::<Vec<_>>();
            ensure!(floors.len() == 1, "minimum search bid ambiguous");
            let floor = floors[0]
                .get("value")
                .and_then(Value::as_u64)
                .context("minimum bid invalid")?;
            ensure!(
                floor > 0
                    && self
                        .manifest
                        .bids_kopecks
                        .get(&nm)
                        .is_some_and(|bid| *bid >= floor),
                "SKU {nm}: approved bid below WB minimum {floor} kopecks"
            );
        }
        ensure!(
            seen == self.manifest.nm_ids().into_iter().collect(),
            "minimum bids incomplete"
        );
        Ok(())
    }

    async fn inactive(&self, id: u64, exact_bids: bool) -> Result<CampaignObservation> {
        let state = self.details(id).await?;
        ensure!(
            matches!(state.status, 4 | 11),
            "campaign must be ready or paused, not active"
        );
        if exact_bids {
            ensure!(
                state.bids == self.manifest.bids_kopecks,
                "campaign bids drifted; no write permitted"
            );
        }
        Ok(state)
    }

    async fn create(&self, journal: &Journal) -> Result<Value> {
        self.manifest.authorize_stage("create")?;
        journal.assert_not_attempted("create")?;
        let preflight = self.preflight(None).await?;
        let request = WbCreateCampaignRequest {
            name: self.manifest.campaign_name.clone(),
            nm_ids: self.manifest.nm_ids(),
            bid_type: WbCampaignBidType::Manual,
            payment_type: WbCampaignPaymentType::Cpc,
            placement_types: vec![WbBidPlacement::Search],
        };
        let id = self
            .writer
            .create_campaign_with_permit(&request, || async {
                let evidence = if self.manifest.version == 2 {
                    self.preflight(None).await?
                } else {
                    preflight
                };
                self.fresh_authorization()?;
                journal.attempt("create", &evidence)
            })
            .await
            .map_err(write_error)?;
        journal.receipt("create", &json!({"campaign_id":id,"wb_http":200}))?;
        self.inactive(id, false).await?;
        ensure!(
            self.budget(id).await? == 0,
            "new campaign has unexpected budget"
        );
        journal.receipt("policy", &serde_json::to_value(self.target_policy(id))?)?;
        self.reconcile(journal).await
    }

    async fn bids(&self, journal: &Journal) -> Result<Value> {
        let id = journal.campaign_id()?;
        journal.assert_not_attempted("bids")?;
        if self.manifest.continue_created.is_some() {
            self.validate_prepared_continuation(journal, id)?;
            self.preflight(Some(id)).await?;
        }
        let before = self.inactive(id, false).await?;
        ensure!(
            self.budget(id).await? == 0,
            "set initial bids before funding"
        );
        self.minimums(id).await?;
        let changes = self
            .manifest
            .bids_kopecks
            .iter()
            .map(|(&nm_id, &bid_kopecks)| WbPreparedBidChange {
                nm_id,
                placement: WbBidPlacement::Search,
                before_bid_kopecks: before.bids[&nm_id],
                bid_kopecks,
            })
            .collect::<Vec<_>>();
        self.writer
            .change_bids_with_permit(id, &changes, || async {
                self.fresh_authorization()?;
                let current = self.inactive(id, false).await?;
                ensure!(
                    current.bids == before.bids,
                    "bids changed while waiting; no write attempted"
                );
                ensure!(
                    self.budget(id).await? == 0,
                    "campaign funded while waiting; no initial bid write permitted"
                );
                self.fresh_authorization()?;
                journal.attempt("bids", &json!({"campaign_id":id,"changes":changes}))
            })
            .await
            .map_err(write_error)?;
        self.inactive(id, true).await?;
        journal.receipt(
            "bids",
            &json!({"campaign_id":id,"bids_kopecks":self.manifest.bids_kopecks}),
        )?;
        self.reconcile(journal).await
    }

    async fn fund(&self, journal: &Journal) -> Result<Value> {
        self.manifest.authorize_stage("fund")?;
        let id = journal.campaign_id()?;
        journal.assert_not_attempted("fund")?;
        journal.require_receipt("bids")?;
        let initial_status = self.inactive(id, true).await?.status;
        start::validate_start_status(initial_status)?;
        self.installed_protection(id)?;
        let preflight = self.preflight(Some(id)).await?;
        self.writer
            .deposit_budget_with_permit(id, self.manifest.budget_rubles, || async {
                self.fresh_authorization()?;
                self.minimums(id).await?;
                ensure!(
                    self.inactive(id, true).await?.status == initial_status,
                    "campaign status drifted before funding"
                );
                ensure!(
                    self.budget(id).await? == 0,
                    "new campaign budget no longer zero"
                );
                let balance = self.balance().await?;
                ensure!(
                    balance >= self.manifest.budget_rubles,
                    "WB type=1 balance {balance} RUB is insufficient"
                );
                self.fresh_authorization()?;
                self.installed_protection(id)?;
                journal.attempt(
                    "fund",
                    &json!({"campaign_id":id,"sum":self.manifest.budget_rubles,"type":1,
                "budget_before":0,"balance_before":balance,"preflight":preflight}),
                )
            })
            .await
            .map_err(write_error)
            .and_then(|total| {
                journal.receipt("fund-response", &json!({"wb_http":200,"total":total}))?;
                ensure!(
                    total == self.manifest.budget_rubles,
                    "WB deposit response total differs; reconcile, never repeat POST"
                );
                Ok(())
            })?;
        ensure!(
            self.budget(id).await? == self.manifest.budget_rubles,
            "budget readback differs; do not start or repeat deposit"
        );
        self.inactive(id, true).await?;
        journal.receipt(
            "fund",
            &json!({"campaign_id":id,"transferred_rubles":self.manifest.budget_rubles,
            "type":1,"budget_after":self.manifest.budget_rubles,"checked_at":Utc::now()}),
        )?;
        self.reconcile(journal).await
    }

    async fn reconcile(&self, journal: &Journal) -> Result<Value> {
        let Some(id) = journal.maybe_campaign_id()? else {
            return Ok(json!({"outcome":"no_confirmed_campaign_id",
                "create_attempted":journal.attempted("create"),
                "instruction":"If create was attempted, inspect WB campaign listing; never retry create."}));
        };
        let state = self.details(id).await?;
        Ok(
            json!({"checked_at":Utc::now(),"campaign_id":id,"account_id":self.manifest.account_id,
            "campaign_name":self.manifest.campaign_name,"status":state.status,"bids_kopecks":state.bids,
            "budget_rubles":self.budget(id).await?,"fund_attempted":journal.attempted("fund"),
            "fund_confirmed":journal.has_receipt("fund"),
            "instruction":"Funding does not start ads. Install and verify the campaign protective robot before guarded start."}),
        )
    }
}

fn nonfinished_campaign_ids(groups: &Value, own_id: Option<u64>) -> Result<BTreeSet<u64>> {
    let groups = groups
        .get("adverts")
        .and_then(Value::as_array)
        .context("campaign listing incomplete")?;
    let mut ids = BTreeSet::new();
    for group in groups {
        let status = group
            .get("status")
            .and_then(Value::as_i64)
            .context("missing campaign status")?;
        if matches!(status, -1 | 7 | 8) {
            continue;
        }
        let list = group
            .get("advert_list")
            .and_then(Value::as_array)
            .context("missing advert_list")?;
        for item in list {
            let id = item
                .get("advertId")
                .and_then(Value::as_u64)
                .context("invalid advertId")?;
            if Some(id) != own_id {
                ids.insert(id);
            }
        }
    }
    ensure!(
        ids.len() <= 500,
        "campaign overlap check exceeds bounded scan"
    );
    Ok(ids)
}

// Include every modern campaign, even completed: the earlier create may have
// succeeded then stopped. Retired types 4..7 cannot be produced by seacat CPC
// create and are absent from v2 details (WB API announcement /forum/1659).
// Only terminal retired campaigns are excluded; all other unknown data blocks.
fn recovery_campaign_ids(groups: &Value, own_id: Option<u64>) -> Result<BTreeSet<u64>> {
    let rows = groups
        .get("adverts")
        .and_then(Value::as_array)
        .context("campaign listing incomplete")?;
    let mut count = 0usize;
    let mut all_ids = BTreeSet::new();
    let mut selected = BTreeSet::new();
    for group in rows {
        let kind = group
            .get("type")
            .and_then(Value::as_u64)
            .context("campaign type missing")?;
        let status = group
            .get("status")
            .and_then(Value::as_i64)
            .context("campaign status missing")?;
        ensure!(
            matches!(status, -1 | 4 | 7 | 8 | 9 | 11),
            "unknown campaign status"
        );
        let modern = matches!(kind, 8 | 9);
        ensure!(
            modern || (matches!(kind, 4..=7) && matches!(status, -1 | 7 | 8)),
            "unknown or nonterminal legacy campaign type; cannot safely exclude"
        );
        let list = group
            .get("advert_list")
            .and_then(Value::as_array)
            .context("campaign listing missing IDs")?;
        ensure!(
            group.get("count").and_then(Value::as_u64) == Some(list.len() as u64),
            "campaign group truncated"
        );
        count += list.len();
        ensure!(count <= 500, "campaign overlap check exceeds bounded scan");
        for item in list {
            let id = item
                .get("advertId")
                .and_then(Value::as_u64)
                .filter(|id| *id > 0)
                .context("invalid advertId")?;
            ensure!(all_ids.insert(id), "campaign listing duplicate ID");
            if modern && Some(id) != own_id {
                selected.insert(id);
            }
        }
    }
    ensure!(
        groups.get("all").and_then(Value::as_u64) == Some(count as u64),
        "campaign listing total incomplete"
    );
    Ok(selected)
}

fn validate_registry(manifest: &Manifest) -> Result<String> {
    let registry = RegistrySource::new(&manifest.registry)?.load()?;
    let account = registry
        .accounts
        .iter()
        .find(|account| {
            account.id == manifest.account_id && account.marketplace == Marketplace::Wildberries
        })
        .context("WB account not bound")?;
    ensure!(
        registry
            .actors
            .iter()
            .any(|actor| actor.id == manifest.actor_id
                && actor.role == Role::Admin
                && actor.can_access_account(account)),
        "launch requires an account-authorized admin operator"
    );
    account
        .wildberries
        .as_ref()
        .and_then(|wb| wb.seller_sid.clone())
        .context("reviewed seller binding is missing")
}

fn write_error(error: WbGuardedWriteError<anyhow::Error>) -> anyhow::Error {
    match error {
        WbGuardedWriteError::Permit(error) => error,
        WbGuardedWriteError::Write(error) => {
            anyhow::anyhow!("{error}; reconcile only, automatic retry forbidden")
        }
    }
}

enum WriteStage {
    Prepare,
    Create,
    Bids,
    Fund,
    Start,
}

/// Explicit local CLI entry point with fixed WB routes and journaled stages.
pub async fn run_wb_campaign_launch(mode: &str, manifest_path: &Path) -> Result<Value> {
    run_wb_campaign_launch_inner(mode, manifest_path, None).await
}

/// MCP entry point: the process-bound account and authenticated actor are
/// checked against the manifest before any marketplace write can be attempted.
pub async fn run_wb_campaign_launch_scoped(
    mode: &str,
    manifest_path: &Path,
    account_id: &str,
    actor_id: &str,
) -> Result<Value> {
    run_wb_campaign_launch_inner(mode, manifest_path, Some((account_id, actor_id))).await
}

async fn run_wb_campaign_launch_inner(
    mode: &str,
    manifest_path: &Path,
    expected_scope: Option<(&str, &str)>,
) -> Result<Value> {
    // Reject an out-of-scope manifest before opening any credential or client.
    // Operator::load checks the same identity again after loading its snapshot.
    if let Some((account_id, actor_id)) = expected_scope {
        let scope = wb_campaign_manifest_scope(manifest_path)?;
        ensure!(
            scope.account_id == account_id && scope.actor_id == actor_id,
            "MCP campaign is outside account/actor scope"
        );
    }
    // A revoked/expired writer or changed source robot must never prevent
    // reading the outcome of an already attempted money operation.
    if mode == "reconcile" {
        return reconcile_read_only(manifest_path).await;
    }
    let stage = match mode {
        "preflight" => None,
        "prepare" => Some(WriteStage::Prepare),
        "create" => Some(WriteStage::Create),
        "bids" => Some(WriteStage::Bids),
        "fund" => Some(WriteStage::Fund),
        "start" => Some(WriteStage::Start),
        _ => bail!("unknown launch stage"),
    };
    let operator = Operator::load(manifest_path, false)?;
    if let Some((account_id, actor_id)) = expected_scope {
        ensure_mcp_bundle_identity(manifest_path, &operator.manifest)?;
        ensure!(
            operator.manifest.account_id == account_id && operator.manifest.actor_id == actor_id,
            "MCP campaign is outside account/actor scope"
        );
    }
    let Some(stage) = stage else {
        return operator.preflight(None).await;
    };
    operator.manifest.authorize_stage(mode)?;
    let journal = Journal::open(
        &operator.manifest.journal_directory,
        &serde_json::to_value(&operator.manifest)?,
        true,
    )?;
    match stage {
        WriteStage::Prepare => operator.prepare_continuation(&journal).await,
        WriteStage::Create => operator.create(&journal).await,
        WriteStage::Bids => operator.bids(&journal).await,
        WriteStage::Fund => operator.fund(&journal).await,
        WriteStage::Start => operator.start(&journal).await,
    }
}

async fn reconcile_read_only(manifest_path: &Path) -> Result<Value> {
    let manifest: Manifest = read_private_json(manifest_path)?;
    manifest.validate_identity()?;
    let sid = validate_registry(&manifest)?;
    let token = read_control_token(&manifest.reader_token, "WB_LAUNCH_READER")?;
    validate_wb_reader_token(&token, &sid, manifest.allow_broad_reader)?;
    let journal = Journal::open(
        &manifest.journal_directory,
        &serde_json::to_value(&manifest)?,
        false,
    )?;
    let Some(id) = journal.maybe_campaign_id()? else {
        return Ok(json!({"outcome":"no_confirmed_campaign_id",
            "create_attempted":journal.attempted("create"),"automatic_retry_allowed":false}));
    };
    let reader = WbClient::new_with_https_proxy(
        Duration::from_secs(30),
        BTreeMap::from([(manifest.account_id.clone(), WbCredentials { token })]),
        &manifest.reader_proxy,
    )?;
    reconcile_campaign(&manifest, &reader, &journal, id).await
}

async fn reconcile_campaign(
    manifest: &Manifest,
    reader: &WbClient,
    journal: &Journal,
    id: u64,
) -> Result<Value> {
    manifest.validate_identity()?;
    let response = reader
        .promotion_campaign_details(&manifest.account_id, vec![id], vec![], None)
        .await?;
    let adverts = response
        .get("adverts")
        .and_then(Value::as_array)
        .context("details unavailable")?;
    ensure!(
        adverts.len() == 1 && adverts[0].get("id").and_then(Value::as_u64) == Some(id),
        "campaign readback identity mismatch"
    );
    let ad = &adverts[0];
    let budget = reader
        .promotion_campaign_budget(&manifest.account_id, id)
        .await?;
    Ok(
        json!({"checked_at":Utc::now(),"account_id":manifest.account_id,"campaign_id":id,
        "campaign_name":ad.pointer("/settings/name"),"status":ad.get("status"),
        "payment_type":ad.pointer("/settings/payment_type"),"bid_type":ad.get("bid_type"),
        "placements":ad.pointer("/settings/placements"),"nm_settings":ad.get("nm_settings"),
        "budget":budget,"fund_attempted":journal.attempted("fund"),
        "fund_confirmed_in_journal":journal.has_receipt("fund"),
        "start_confirmed_in_journal":journal.has_receipt("start"),"automatic_retry_allowed":false}),
    )
}
