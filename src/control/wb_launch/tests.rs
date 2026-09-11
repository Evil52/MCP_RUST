use super::*;

pub(super) fn fixture() -> (Manifest, WbAutomationPolicy) {
    let now = Utc::now();
    let mut policy: WbAutomationPolicy = serde_json::from_str(include_str!(
        "../../../config/wb-automation-oduvanchik.live.json"
    ))
    .unwrap();
    policy.bid_writes_enabled = true;
    policy.authorized_at = now - chrono::Duration::hours(1);
    policy.observe_until = now;
    policy.authorization_expires_at = now + chrono::Duration::days(10);
    let manifest = Manifest {
        scope: LaunchScope::FundAndStart,
        account_id: ACCOUNT.to_owned(),
        campaign_name: NAME.to_owned(),
        source_policy: PathBuf::from("source.json"),
        source_policy_sha256: journal::digest(&serde_json::to_vec(&policy).unwrap()),
        bids_kopecks: NMS.into_iter().map(|nm| (nm, 922)).collect(),
        budget_rubles: 1000,
        funding_type: 1,
        actor_id: "admin".to_owned(),
        authorization_reference: "test/exact-one-time-approval".to_owned(),
        authorized_at: now - chrono::Duration::minutes(1),
        expires_at: now + chrono::Duration::hours(1),
        registry: PathBuf::from("registry.json"),
        reader_token: PathBuf::from("reader.token"),
        writer_token: PathBuf::from("writer.token"),
        reader_proxy: "http://reader:3128".to_owned(),
        writer_proxy: "http://writer:3130".to_owned(),
        allow_broad_reader: false,
        journal_directory: PathBuf::from("journal"),
        robot_policy: PathBuf::from("robot.json"),
    };
    (manifest, policy)
}

#[test]
fn create_only_authorizes_zero_budget_and_denies_funding_and_start() {
    let (mut manifest, policy) = fixture();
    manifest.scope = LaunchScope::CreateOnly;
    assert!(manifest.validate(&policy, Utc::now(), false).is_err());
    manifest.budget_rubles = 0;
    manifest.validate(&policy, Utc::now(), false).unwrap();
    for mode in ["preflight", "create", "bids", "reconcile"] {
        manifest.authorize_stage(mode).unwrap();
    }
    for mode in ["fund", "start", "unknown"] {
        assert!(manifest.authorize_stage(mode).is_err());
    }
    manifest.scope = LaunchScope::FundAndStart;
    assert!(manifest.validate(&policy, Utc::now(), false).is_err());
}

#[test]
fn launch_manifest_accepts_exact_scope() {
    let (manifest, policy) = fixture();
    manifest.validate(&policy, Utc::now(), false).unwrap();
}

#[test]
fn launch_manifest_rejects_source_amount_and_campaign_expansion() {
    let (manifest, policy) = fixture();
    for amount in [0, 999, 1001, 10_000] {
        let mut altered = manifest.clone();
        altered.budget_rubles = amount;
        assert!(altered.validate(&policy, Utc::now(), false).is_err());
    }
    for source in [0, 2, 3] {
        let mut altered = manifest.clone();
        altered.funding_type = source;
        assert!(altered.validate(&policy, Utc::now(), false).is_err());
    }
    let mut altered = manifest.clone();
    altered.bids_kopecks.insert(44_081_434, 922);
    assert!(altered.validate(&policy, Utc::now(), false).is_err());
    altered = manifest.clone();
    altered.account_id = "diana".to_owned();
    assert!(altered.validate(&policy, Utc::now(), false).is_err());
    altered = manifest;
    altered.campaign_name = "Одуванчик".to_owned();
    assert!(altered.validate(&policy, Utc::now(), false).is_err());
}

#[test]
fn launch_manifest_rejects_policy_drift_and_auto_topup() {
    let (manifest, policy) = fixture();
    let mut changed = policy.clone();
    changed.allow_budget_top_up = true;
    assert!(manifest.validate(&changed, Utc::now(), false).is_err());
    changed = policy.clone();
    changed.daily_spend_cap_minor += 100;
    assert!(manifest.validate(&changed, Utc::now(), false).is_err());
    changed = policy;
    changed.no_order_reduce_clicks += 1;
    assert!(manifest.validate(&changed, Utc::now(), false).is_err());
}

#[test]
fn expired_authorization_allows_reconcile_but_never_mutations() {
    let (manifest, policy) = fixture();
    let later = manifest.expires_at + chrono::Duration::seconds(1);
    assert!(manifest.validate(&policy, later, false).is_err());
    assert!(manifest.validate(&policy, later, true).is_ok());
}

#[test]
fn initial_bids_cannot_escape_source_corridor() {
    let (manifest, policy) = fixture();
    for bid in [0, 499, 1051, u64::MAX] {
        let mut altered = manifest.clone();
        altered.bids_kopecks.insert(NMS[0], bid);
        assert!(altered.validate(&policy, Utc::now(), false).is_err());
    }
}
