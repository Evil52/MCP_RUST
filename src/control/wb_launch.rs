//! Explicit local operator workflow. Not exposed through analytics MCP.
//! Each stage has an immutable, fsynced attempt record before HTTP. An
//! uncertain write permanently fences that stage; reconcile never writes WB.
//! This first rollout provisions a paused campaign. Start is deliberately
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
    146_312_604,
    207_418_966,
    455_101_276,
    461_126_890,
    529_996_417,
];

/// Reviewed one-time authorization, not a generic payment service. Fixed
/// account/name/products/amount/source keep this rollout within Nexus scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LaunchScope {
    CreateOnly,
    FundAndStart,
}

impl Manifest {
    const fn financial_scope_matches(&self) -> bool {
        self.funding_type == 1
            && match self.scope {
                LaunchScope::CreateOnly => self.budget_rubles == 0,
                LaunchScope::FundAndStart => self.budget_rubles == 1000,
            }
    }

    fn authorize_stage(&self, mode: &str) -> Result<()> {
        ensure!(
            matches!(mode, "preflight" | "create" | "bids" | "reconcile")
                || (self.scope == LaunchScope::FundAndStart && matches!(mode, "fund" | "start")),
            "create-only authorization forbids funding and campaign start"
        );
        Ok(())
    }

    fn target_policy(&self, source: &WbAutomationPolicy, id: u64) -> WbAutomationPolicy {
        let mut policy = source.clone();
        policy.campaign_id = id;
        NAME.clone_into(&mut policy.campaign_name);
        policy.nm_ids = NMS.to_vec();
        policy.authorized_by_actor_id.clone_from(&self.actor_id);
        policy
            .authorization_reference
            .clone_from(&self.authorization_reference);
        policy
    }

    fn validate(
        &self,
        policy: &WbAutomationPolicy,
        now: DateTime<Utc>,
        allow_expired: bool,
    ) -> Result<()> {
        ensure!(
            self.account_id == ACCOUNT
                && self.campaign_name == NAME
                && self.bids_kopecks.keys().copied().eq(NMS),
            "Nexus account/name/SKU scope mismatch"
        );
        ensure!(
            self.financial_scope_matches(),
            "create-only requires zero funding; fund-and-start permits exactly 1000 RUB type=1"
        );
        ensure!(
            !self.authorization_reference.trim().is_empty()
                && (allow_expired || (self.authorized_at <= now && now < self.expires_at))
                && (1..=86400).contains(&(self.expires_at - self.authorized_at).num_seconds()),
            "launch authorization is absent or expired (maximum 24 hours)"
        );
        super::validate_wb_automation_policy(policy)?;
        ensure!(
            policy.account_id == ACCOUNT
                && policy.campaign_id == SOURCE
                && policy.campaign_name == "Одуванчик"
                && policy.payment_type == "cpc"
                && policy.placement == "search"
                && !policy.allow_budget_top_up
                && policy.daily_spend_cap_minor == 50_000
                && policy.daily_pause_threshold_minor == 45_000
                && policy.target_drr_basis_points == 1500
                && policy.write_enabled
                && policy.bid_writes_enabled
                && (allow_expired
                    || (policy.authorized_at <= now && now < policy.authorization_expires_at)),
            "source robot protection policy is incompatible or inactive"
        );
        ensure!(
            self.bids_kopecks
                .values()
                .all(|bid| (policy.min_bid_kopecks..=policy.max_bid_kopecks).contains(bid)),
            "initial bids exceed the source policy corridor"
        );
        ensure!(
            journal::digest(&serde_json::to_vec(policy)?) == self.source_policy_sha256,
            "source policy changed since review"
        );
        // The campaign ID is not known before create. Validate every derived
        // field with an already valid ID before allowing the first WB write.
        super::validate_wb_automation_policy(&self.target_policy(policy, SOURCE))
            .context("derived target robot policy is invalid")?;
        ensure!(
            !self.reader_proxy.is_empty() && !self.writer_proxy.is_empty(),
            "dedicated egress proxies are mandatory"
        );
        Ok(())
    }
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
                ACCOUNT.to_owned(),
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
            .promotion_campaign_details(ACCOUNT, vec![id], vec![], None)
            .await?;
        parse_campaign(&details, &self.target_policy(id))
    }

    async fn budget(&self, id: u64) -> Result<u64> {
        let value = self.reader.promotion_campaign_budget(ACCOUNT, id).await?;
        value
            .get("total")
            .and_then(Value::as_u64)
            .context("campaign budget unavailable")
    }

    async fn balance(&self) -> Result<u64> {
        let value = self.reader.promotion_balance(ACCOUNT).await?;
        value
            .get("balance")
            .and_then(Value::as_u64)
            .context("WB type=1 balance unavailable")
    }

    async fn preflight(&self, own_id: Option<u64>) -> Result<Value> {
        self.fresh_authorization()?;
        let subject_id = self.verify_category().await?;
        let source = self
            .reader
            .promotion_campaign_details(ACCOUNT, vec![SOURCE], vec![], None)
            .await?;
        parse_campaign(&source, &self.policy)?;
        let groups = self.reader.promotion_campaigns(ACCOUNT).await?;
        let ids = nonfinished_campaign_ids(&groups, own_id)?;
        for chunk in ids.into_iter().collect::<Vec<_>>().chunks(50) {
            let response = self
                .reader
                .promotion_campaign_details(ACCOUNT, chunk.to_vec(), vec![], None)
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
                ensure!(
                    ad.pointer("/settings/name")
                        .and_then(Value::as_str)
                        .context("campaign name absent")?
                        != NAME,
                    "another Nexus campaign exists; reconcile instead of creating a duplicate"
                );
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
                        !NMS.contains(&id),
                        "SKU {id} already belongs to another non-finished campaign"
                    );
                }
            }
        }
        let stocks = self
            .reader
            .warehouse_stocks(
                ACCOUNT,
                json!({"nmIds":NMS,"chrtIds":[],"limit":100,"offset":0}),
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
        for nm in NMS {
            ensure!(
                totals.get(&nm).copied().unwrap_or(0) >= 20,
                "SKU {nm} has insufficient verified WB stock (minimum 20)"
            );
        }
        let balance = if self.manifest.scope == LaunchScope::FundAndStart {
            let balance = self.balance().await?;
            ensure!(
                balance >= 1000,
                "WB type=1 balance is {balance} RUB; 1000 RUB required"
            );
            Some(balance)
        } else {
            // Creating an inactive, unfunded campaign is not a financial
            // operation. Do not read or require a wallet balance in this scope.
            None
        };
        self.fresh_authorization()?;
        Ok(
            json!({"checked_at":Utc::now(),"account_id":ACCOUNT,"campaign_name":NAME,
            "cash_balance_rubles":balance,"scope":self.manifest.scope,
            "authorized_funding_rubles":self.manifest.budget_rubles,
            "subject_id":subject_id,
            "bids_kopecks":self.manifest.bids_kopecks,"wb_stock":totals,
            "source_policy_sha256":self.manifest.source_policy_sha256,
            "daily_cap_rubles":500,"pause_threshold_rubles":450,"target_drr_percent":15,
            "auto_top_up":false,"credential_role":"seller-bound promotion-only dedicated writer"}),
        )
    }

    async fn minimums(&self, id: u64) -> Result<()> {
        let response = self
            .reader
            .promotion_minimum_bids(
                ACCOUNT,
                id,
                NMS.to_vec(),
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
        ensure!(seen == NMS.into_iter().collect(), "minimum bids incomplete");
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
        journal.assert_not_attempted("create")?;
        let preflight = self.preflight(None).await?;
        let request = WbCreateCampaignRequest {
            name: NAME.to_owned(),
            nm_ids: NMS.to_vec(),
            bid_type: WbCampaignBidType::Manual,
            payment_type: WbCampaignPaymentType::Cpc,
            placement_types: vec![WbBidPlacement::Search],
        };
        let id = self
            .writer
            .create_campaign_with_permit(&request, || async {
                self.fresh_authorization()?;
                journal.attempt("create", &preflight)
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
        let preflight = self.preflight(Some(id)).await?;
        self.writer
            .deposit_once_with_permit(id, || async {
                self.fresh_authorization()?;
                self.minimums(id).await?;
                self.inactive(id, true).await?;
                ensure!(
                    self.budget(id).await? == 0,
                    "new campaign budget no longer zero"
                );
                let balance = self.balance().await?;
                ensure!(
                    balance >= 1000,
                    "WB type=1 balance {balance} RUB is insufficient"
                );
                self.fresh_authorization()?;
                journal.attempt(
                    "fund",
                    &json!({"campaign_id":id,"sum":1000,"type":1,
                "budget_before":0,"balance_before":balance,"preflight":preflight}),
                )
            })
            .await
            .map_err(write_error)
            .and_then(|total| {
                journal.receipt("fund-response", &json!({"wb_http":200,"total":total}))?;
                ensure!(
                    total == 1000,
                    "WB deposit response total differs; reconcile, never repeat POST"
                );
                Ok(())
            })?;
        ensure!(
            self.budget(id).await? == 1000,
            "budget readback differs; do not start or repeat deposit"
        );
        self.inactive(id, true).await?;
        journal.receipt(
            "fund",
            &json!({"campaign_id":id,"transferred_rubles":1000,
            "type":1,"budget_after":1000,"checked_at":Utc::now()}),
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
            json!({"checked_at":Utc::now(),"campaign_id":id,"account_id":ACCOUNT,
            "campaign_name":NAME,"status":state.status,"bids_kopecks":state.bids,
            "budget_rubles":self.budget(id).await?,"fund_attempted":journal.attempted("fund"),
            "fund_confirmed":journal.has_receipt("fund"),
            "instruction":"Funding does not start ads. Install and verify Nexus protective robot before guarded start."}),
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

fn validate_registry(manifest: &Manifest) -> Result<String> {
    let registry = RegistrySource::new(&manifest.registry)?.load()?;
    let account = registry
        .accounts
        .iter()
        .find(|account| account.id == ACCOUNT && account.marketplace == Marketplace::Wildberries)
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
    Create,
    Bids,
    Fund,
    Start,
}

/// Explicit local CLI entry point; no generic HTTP paths or payment amounts.
pub async fn run_wb_campaign_launch(mode: &str, manifest_path: &Path) -> Result<Value> {
    // A revoked/expired writer or changed source robot must never prevent
    // reading the outcome of an already attempted money operation.
    let stage = match mode {
        "reconcile" => return reconcile_read_only(manifest_path).await,
        "preflight" => return Operator::load(manifest_path, false)?.preflight(None).await,
        "create" => WriteStage::Create,
        "bids" => WriteStage::Bids,
        "fund" => WriteStage::Fund,
        "start" => WriteStage::Start,
        _ => bail!("unknown launch stage"),
    };
    let operator = Operator::load(manifest_path, false)?;
    operator.manifest.authorize_stage(mode)?;
    let journal = Journal::open(
        &operator.manifest.journal_directory,
        &serde_json::to_value(&operator.manifest)?,
        true,
    )?;
    match stage {
        WriteStage::Create => operator.create(&journal).await,
        WriteStage::Bids => operator.bids(&journal).await,
        WriteStage::Fund => operator.fund(&journal).await,
        WriteStage::Start => operator.start(&journal).await,
    }
}

async fn reconcile_read_only(manifest_path: &Path) -> Result<Value> {
    let manifest: Manifest = read_private_json(manifest_path)?;
    ensure!(
        manifest.account_id == ACCOUNT
            && manifest.campaign_name == NAME
            && manifest.bids_kopecks.keys().copied().eq(NMS)
            && manifest.financial_scope_matches(),
        "reconcile manifest is outside Nexus scope"
    );
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
        BTreeMap::from([(ACCOUNT.to_owned(), WbCredentials { token })]),
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
    ensure!(
        manifest.account_id == ACCOUNT && manifest.campaign_name == NAME,
        "readback scope mismatch"
    );
    let response = reader
        .promotion_campaign_details(ACCOUNT, vec![id], vec![], None)
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
    let budget = reader.promotion_campaign_budget(ACCOUNT, id).await?;
    Ok(
        json!({"checked_at":Utc::now(),"account_id":ACCOUNT,"campaign_id":id,
        "campaign_name":ad.pointer("/settings/name"),"status":ad.get("status"),
        "payment_type":ad.pointer("/settings/payment_type"),"bid_type":ad.get("bid_type"),
        "placements":ad.pointer("/settings/placements"),"nm_settings":ad.get("nm_settings"),
        "budget":budget,"fund_attempted":journal.attempted("fund"),
        "fund_confirmed_in_journal":journal.has_receipt("fund"),
        "start_confirmed_in_journal":journal.has_receipt("start"),"automatic_retry_allowed":false}),
    )
}
