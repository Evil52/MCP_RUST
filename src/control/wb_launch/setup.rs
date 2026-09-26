//! Reusable account profile -> immutable per-campaign launch inputs.
//! Preparation/export only write new local files, never a marketplace request.
use super::{
    BTreeMap, Context, DateTime, Deserialize, Journal, LaunchScope, Manifest, Path, PathBuf,
    RegistrySource, Result, Serialize, Utc, Value, WbAutomationPolicy, ensure, journal, json,
    read_policy_json, read_private_json, validate_registry,
};
use crate::control::{ControlPolicy, WbActionLimits};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManualControl {
    pub approver_actor_ids: Vec<String>,
    pub max_delta_percent: u8,
    pub action_limits: WbActionLimits,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    version: u32,
    account_id: String,
    actor_id: String,
    registry: PathBuf,
    reader_token: PathBuf,
    writer_token: PathBuf,
    reader_proxy: String,
    writer_proxy: String,
    allow_broad_reader: bool,
    robot_template: PathBuf,
    journal_directory: PathBuf,
    campaigns_directory: PathBuf,
    max_initial_budget_rubles: u64,
    manual_control: ManualControl,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    campaign_name: String,
    bids_kopecks: BTreeMap<u64, u64>,
    /// Zero prepares a create-only authorization. Positive values also permit
    /// a separately invoked, once-only funding and guarded first start.
    budget_rubles: u64,
    authorization_reference: String,
    authorized_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    robot_authorization_expires_at: DateTime<Utc>,
}

/// Fixed scope and credential paths of a private reusable campaign profile.
#[derive(Debug, Clone)]
pub struct WbCampaignProfileRuntime {
    pub account_id: String,
    pub actor_id: String,
    pub registry: PathBuf,
    pub reader_token: PathBuf,
    pub writer_token: PathBuf,
    pub allow_broad_reader: bool,
    pub robot_template: PathBuf,
    pub journal_directory: PathBuf,
    pub campaigns_directory: PathBuf,
}

pub fn wb_campaign_profile_runtime(path: &Path) -> Result<WbCampaignProfileRuntime> {
    let profile: Profile = read_private_json(path)?;
    ensure!(
        profile.version == 1,
        "unsupported WB campaign profile version"
    );
    Ok(WbCampaignProfileRuntime {
        account_id: profile.account_id,
        actor_id: profile.actor_id,
        registry: profile.registry,
        reader_token: profile.reader_token,
        writer_token: profile.writer_token,
        allow_broad_reader: profile.allow_broad_reader,
        robot_template: profile.robot_template,
        journal_directory: profile.journal_directory,
        campaigns_directory: profile.campaigns_directory,
    })
}

fn private_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && metadata.permissions().mode().trailing_zeros() >= 6,
        "campaign root must be an existing private non-symlink directory"
    );
    Ok(())
}

fn write_new_json(path: &Path, value: &Value) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    File::open(path.parent().context("file requires a parent directory")?)?.sync_all()?;
    Ok(())
}

/// Builds one campaign from a reusable local account profile.
///
/// Existing bundles
/// and journals are never overwritten. No credentials are opened and no WB
/// request is made. Paths in the profile must be absolute (container paths when
/// the operator runs in the production container).
pub fn prepare_wb_campaign(profile_path: &Path, request_path: &Path) -> Result<Value> {
    let request: Request = read_private_json(request_path)?;
    prepare_wb_campaign_inner(profile_path, request)
}

/// Prepare from a structured MCP request without accepting an arbitrary file path.
pub fn prepare_wb_campaign_from_request(profile_path: &Path, request: &Value) -> Result<Value> {
    let request: Request = serde_json::from_value(request.clone())?;
    prepare_wb_campaign_inner(profile_path, request)
}

fn prepare_wb_campaign_inner(profile_path: &Path, request: Request) -> Result<Value> {
    let profile: Profile = read_private_json(profile_path)?;
    ensure!(
        profile.version == 1,
        "unsupported WB campaign profile version"
    );
    ensure!(
        (1..=1_000_000).contains(&profile.max_initial_budget_rubles)
            && request.budget_rubles <= profile.max_initial_budget_rubles,
        "initial budget exceeds the account profile limit"
    );
    for path in [
        &profile.registry,
        &profile.reader_token,
        &profile.writer_token,
        &profile.robot_template,
        &profile.journal_directory,
        &profile.campaigns_directory,
    ] {
        ensure!(path.is_absolute(), "profile paths must be absolute");
    }
    private_directory(&profile.campaigns_directory)?;
    private_directory(&profile.journal_directory)?;
    let mut policy: WbAutomationPolicy = read_policy_json(&profile.robot_template)?;
    ensure!(
        policy.account_id == profile.account_id,
        "robot template account differs from profile"
    );
    policy.campaign_id = 1; // A template identity only; never used for a WB call.
    policy.campaign_name.clone_from(&request.campaign_name);
    policy.nm_ids = request.bids_kopecks.keys().copied().collect();
    policy.authorized_by_actor_id.clone_from(&profile.actor_id);
    policy
        .authorization_reference
        .clone_from(&request.authorization_reference);
    policy.authorized_at = request.authorized_at;
    policy.observe_until = request
        .authorized_at
        .checked_add_signed(chrono::Duration::seconds(1))
        .context("authorization timestamp overflow")?;
    policy.authorization_expires_at = request.robot_authorization_expires_at;
    ensure!(
        request.expires_at <= request.robot_authorization_expires_at,
        "robot authorization must cover the launch authorization"
    );
    let mut manifest = Manifest {
        version: 2,
        manual_control: Some(profile.manual_control),
        scope: if request.budget_rubles == 0 {
            LaunchScope::CreateOnly
        } else {
            LaunchScope::FundAndStart
        },
        account_id: profile.account_id,
        campaign_name: request.campaign_name,
        source_policy: PathBuf::new(),
        source_policy_sha256: journal::digest(&serde_json::to_vec(&policy)?),
        bids_kopecks: request.bids_kopecks,
        budget_rubles: request.budget_rubles,
        funding_type: 1,
        actor_id: profile.actor_id,
        authorization_reference: request.authorization_reference,
        authorized_at: request.authorized_at,
        expires_at: request.expires_at,
        registry: profile.registry,
        reader_token: profile.reader_token,
        writer_token: profile.writer_token,
        reader_proxy: profile.reader_proxy,
        writer_proxy: profile.writer_proxy,
        allow_broad_reader: profile.allow_broad_reader,
        journal_directory: profile.journal_directory,
        robot_policy: PathBuf::new(),
        recreate: None,
        continue_created: None,
    };
    manifest.validate(&policy, Utc::now(), false)?;
    let sid = validate_registry(&manifest)?;
    let _ = control_document(&manifest, &policy, &sid, 1)?;
    let directory = profile
        .campaigns_directory
        .join(journal::directory_name(&serde_json::to_value(&manifest)?)?);
    manifest.source_policy = directory.join("template.json");
    manifest.robot_policy = directory.join("robot-policy.json");
    // A new approval or bid edit cannot silently replace an earlier attempt.
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .context("campaign bundle already exists or cannot be created; inspect its history")?;
    File::open(&profile.campaigns_directory)?.sync_all()?;
    write_new_json(&manifest.source_policy, &serde_json::to_value(&policy)?)?;
    let path = directory.join("manifest.json");
    write_new_json(&path, &serde_json::to_value(&manifest)?)?;
    Ok(
        json!({"outcome":"prepared", "manifest":path, "account_id":manifest.account_id,
        "campaign_name":manifest.campaign_name,"bids_kopecks":manifest.bids_kopecks,
        "budget_rubles":manifest.budget_rubles,"marketplace_write_sent":false}),
    )
}

fn control_document(
    manifest: &Manifest,
    policy: &WbAutomationPolicy,
    sid: &str,
    id: u64,
) -> Result<Value> {
    let settings = manifest
        .manual_control
        .as_ref()
        .context("manual Control settings are absent")?;
    let document = json!({"version":1,"revision":1,"mode":"plan_only","actors":[{
    "actor_id":manifest.actor_id,"targets":[],"ozon_campaign_launch_targets":[],
    "wb_promotion_bid_targets":[{
        "account_id":manifest.account_id,"seller_sid":sid,"advert_id":id,
        "nm_ids":manifest.nm_ids(),"placements":["search"],
        "bid_limits_kopecks":{"min_minor":policy.min_bid_kopecks,"max_minor":policy.max_bid_kopecks,
            "max_delta_percent":settings.max_delta_percent},
        "approver_actor_ids":settings.approver_actor_ids,"action_limits":settings.action_limits
    }]}]});
    let registry = RegistrySource::new(&manifest.registry)?.load()?;
    ControlPolicy::from_slice(
        &serde_json::to_vec(&document)?,
        Path::new("generated-control-policy"),
        &registry,
    )?;
    Ok(document)
}

/// Exports only a campaign with confirmed create and initial-bid receipts.
///
/// The real campaign ID comes from the journal, never from user input.
/// Repeated export checks the existing bytes and does not reset robot state.
pub fn export_wb_campaign(manifest_path: &Path) -> Result<Value> {
    let manifest: Manifest = read_private_json(manifest_path)?;
    manifest.validate_identity()?;
    ensure!(
        manifest.version == 2,
        "export requires a reusable campaign manifest"
    );
    let sid = validate_registry(&manifest)?;
    let journal = Journal::open(
        &manifest.journal_directory,
        &serde_json::to_value(&manifest)?,
        false,
    )?;
    let id = journal.campaign_id()?;
    let policy: WbAutomationPolicy = read_policy_json(&manifest.source_policy)?;
    // Export is local and may inspect an expired launch, but cannot alter the
    // reviewed template, limits, or durable approval.
    manifest.validate(&policy, Utc::now(), true)?;
    let target = manifest.target_policy(&policy, id);
    ensure!(
        journal.require_receipt("policy")? == serde_json::to_value(&target)?,
        "created robot policy differs from the reviewed template"
    );
    let bids = journal.require_receipt("bids")?;
    ensure!(
        bids["campaign_id"] == id
            && bids["bids_kopecks"] == serde_json::to_value(&manifest.bids_kopecks)?,
        "initial bids are not confirmed; reconcile first"
    );
    let control = control_document(&manifest, &target, &sid, id)?;
    let mut shadow = target.clone();
    shadow.write_enabled = false;
    shadow.bid_writes_enabled = false;
    let directory = manifest_path
        .parent()
        .context("manifest requires a parent directory")?;
    let outputs = [
        (
            manifest.robot_policy.clone(),
            serde_json::to_value(&target)?,
        ),
        (
            directory.join("shadow-policy.json"),
            serde_json::to_value(shadow)?,
        ),
        (directory.join("control-policy.json"), control),
        (
            directory.join("initial-state.json"),
            json!({
                "schema_version":1,"policy_sha256":journal::digest(&serde_json::to_vec(&target)?),
                "account_id":target.account_id,"campaign_id":id,
                "business_date":crate::control::wb_automation_business_date(manifest.authorized_at),
                "actions_today":0,"last_action_at":null,"paused_for_daily_cap_on":null,
                "pending":null,"incident_class":null
            }),
        ),
    ];
    for (path, value) in &outputs {
        if journal::entry_exists(path)? {
            ensure!(
                read_private_json::<Value>(path)? == *value,
                "existing export differs; refusing to overwrite {}",
                path.display()
            );
        } else {
            write_new_json(path, value)?;
        }
    }
    Ok(
        json!({"outcome":"exported","account_id":manifest.account_id,"campaign_id":id,
        "files":outputs.iter().map(|(path,_)| path).collect::<Vec<_>>(),
        "control_mode":"plan_only","runtime_installed":false,"marketplace_write_sent":false}),
    )
}

/// Adds a confirmed campaign to an existing policy as a new candidate file.
///
/// Existing targets, mode and actors are preserved; no server is reloaded.
pub fn enroll_wb_campaign(
    manifest_path: &Path,
    current_policy: &Path,
    output: &Path,
) -> Result<Value> {
    let manifest: Manifest = read_private_json(manifest_path)?;
    manifest.validate_identity()?;
    ensure!(
        manifest.version == 2,
        "enrollment requires a reusable campaign manifest"
    );
    let sid = validate_registry(&manifest)?;
    let journal = Journal::open(
        &manifest.journal_directory,
        &serde_json::to_value(&manifest)?,
        false,
    )?;
    let id = journal.campaign_id()?;
    let source: WbAutomationPolicy = read_policy_json(&manifest.source_policy)?;
    manifest.validate(&source, Utc::now(), true)?;
    ensure!(
        journal.require_receipt("policy")?
            == serde_json::to_value(manifest.target_policy(&source, id))?,
        "launch policy receipt does not match the template"
    );
    let bids = journal.require_receipt("bids")?;
    ensure!(
        bids["campaign_id"] == id
            && bids["bids_kopecks"] == serde_json::to_value(&manifest.bids_kopecks)?,
        "initial campaign bids must be confirmed before enrollment"
    );
    let exported = control_document(&manifest, &source, &sid, id)?;
    let target: crate::control::policy::WbPromotionBidTargetPolicy =
        serde_json::from_value(exported["actors"][0]["wb_promotion_bid_targets"][0].clone())?;
    let registry = RegistrySource::new(&manifest.registry)?.load()?;
    let mut policy = ControlPolicy::load(current_policy, &registry)?;
    let actor = policy
        .actors
        .iter_mut()
        .find(|actor| actor.actor_id == manifest.actor_id)
        .context("the campaign operator needs an existing Control actor binding")?;
    ensure!(
        !actor
            .wb_promotion_bid_targets
            .iter()
            .any(|existing| existing.account_id == manifest.account_id && existing.advert_id == id),
        "campaign is already registered; use existing Control bid tools"
    );
    actor.wb_promotion_bid_targets.push(target);
    policy.revision = policy
        .revision
        .checked_add(1)
        .context("Control revision overflow")?;
    let document = serde_json::to_value(&policy)?;
    ControlPolicy::from_slice(&serde_json::to_vec(&document)?, output, &registry)?;
    write_new_json(output, &document)?;
    Ok(
        json!({"outcome":"control_policy_prepared","account_id":manifest.account_id,
        "campaign_id":id,"policy_revision":policy.revision,"mode":policy.mode,
        "candidate_policy":output,"runtime_installed":false,"marketplace_write_sent":false}),
    )
}
