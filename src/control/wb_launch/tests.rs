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
        version: 1,
        manual_control: None,
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
        recreate: None,
        continue_created: None,
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
fn launch_manifest_validates_derived_robot_authorization_before_creation() {
    let (mut manifest, policy) = fixture();
    for reference in [
        "user approved Nexus".to_owned(),
        "разрешено".to_owned(),
        "a".repeat(129),
    ] {
        manifest.authorization_reference = reference;
        assert!(manifest.validate(&policy, Utc::now(), false).is_err());
    }
    for reference in ["a".repeat(128), "approval/2026-09-11.admin_1".to_owned()] {
        manifest.authorization_reference = reference;
        manifest.validate(&policy, Utc::now(), false).unwrap();
        super::super::validate_wb_automation_policy(&manifest.target_policy(&policy, 42)).unwrap();
    }
    manifest.actor_id = "invalid actor".to_owned();
    assert!(manifest.validate(&policy, Utc::now(), false).is_err());
}

#[test]
fn replacement_never_authorizes_money_and_scan_requires_complete_history() {
    let (mut manifest, policy) = fixture();
    manifest.recreate = Some(RecreateApproval {
        previous_manifest_sha256: "a".repeat(64),
        previous_attempt_sha256: "b".repeat(64),
    });
    assert!(manifest.validate(&policy, Utc::now(), false).is_err());
    manifest.scope = LaunchScope::CreateOnly;
    manifest.budget_rubles = 0;
    manifest.validate(&policy, Utc::now(), false).unwrap();
    assert!(manifest.authorize_stage("fund").is_err());
    assert!(manifest.authorize_stage("start").is_err());
    let groups = json!({"all":2,"adverts":[
        {"type":9,"status":7,"count":1,"advert_list":[{"advertId":1}]},
        {"type":9,"status":9,"count":1,"advert_list":[{"advertId":2}]}]});
    assert_eq!(
        recovery_campaign_ids(&groups, None).unwrap(),
        BTreeSet::from([1, 2])
    );
    let mut invalid = groups.clone();
    invalid["all"] = json!(3);
    assert!(recovery_campaign_ids(&invalid, None).is_err());
    invalid = groups.clone();
    invalid["adverts"][0]["count"] = json!(2);
    assert!(recovery_campaign_ids(&invalid, None).is_err());
    invalid = groups;
    invalid["adverts"][0]["advert_list"][0]["advertId"] = json!(2);
    assert!(recovery_campaign_ids(&invalid, None).is_err());
}

#[test]
fn recovery_excludes_only_terminal_obsolete_types_and_keeps_total_integrity() {
    let group = |kind, status, id| {
        json!({"type":kind,"status":status,"count":1,
        "advert_list":[{"advertId":id}]})
    };
    let groups = json!({"all":4,"adverts":[group(5,7,1),group(6,7,2),group(9,7,3),group(9,9,4)]});
    assert_eq!(
        recovery_campaign_ids(&groups, None).unwrap(),
        BTreeSet::from([3, 4])
    );
    assert_eq!(
        recovery_campaign_ids(&groups, Some(4)).unwrap(),
        BTreeSet::from([3])
    );
    for kind in [4, 5, 6, 7, 8, 9] {
        for status in [-1, 4, 7, 8, 9, 11] {
            let value = json!({"all":1,"adverts":[group(kind,status,1)]});
            let modern = kind >= 8;
            let terminal = matches!(status, -1 | 7 | 8);
            assert_eq!(
                recovery_campaign_ids(&value, None).is_ok(),
                modern || terminal
            );
            if modern {
                assert_eq!(
                    recovery_campaign_ids(&value, None).unwrap(),
                    BTreeSet::from([1])
                );
            }
        }
    }
    for (kind, status, id) in [(0, 7, 1), (10, 7, 1), (9, 10, 1), (5, 7, 0)] {
        assert!(
            recovery_campaign_ids(&json!({"all":1,"adverts":[group(kind,status,id)]}), None)
                .is_err()
        );
    }
    let mut broken = groups.clone();
    broken["adverts"][0]["advert_list"][0]["advertId"] = json!(3);
    assert!(recovery_campaign_ids(&broken, None).is_err());
    broken = groups.clone();
    broken["adverts"][0]["count"] = json!(2);
    assert!(recovery_campaign_ids(&broken, None).is_err());
    broken = groups;
    broken["adverts"][0].as_object_mut().unwrap().remove("type");
    assert!(recovery_campaign_ids(&broken, None).is_err());
}

#[test]
fn only_reviewed_handle_selection_is_authorized() {
    let (mut manifest, policy) = fixture();
    assert_eq!(
        NMS,
        [
            190_904_855,
            207_418_966,
            218_972_074,
            455_101_276,
            529_996_417
        ]
    );
    manifest.validate(&policy, Utc::now(), false).unwrap();
    manifest.bids_kopecks = [
        146_312_604,
        207_418_966,
        455_101_276,
        461_126_890,
        529_996_417,
    ]
    .into_iter()
    .map(|nm| (nm, 922))
    .collect();
    assert!(manifest.validate(&policy, Utc::now(), false).is_err());
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
fn authorization_duration_is_bounded_without_fractional_second_truncation() {
    let (mut manifest, policy) = fixture();
    let now = manifest.authorized_at;
    for duration in [chrono::Duration::seconds(1), chrono::Duration::hours(24)] {
        manifest.expires_at = now + duration;
        manifest.validate(&policy, now, false).unwrap();
    }
    for duration in [
        chrono::Duration::seconds(1) - chrono::Duration::nanoseconds(1),
        chrono::Duration::hours(24) + chrono::Duration::nanoseconds(1),
    ] {
        manifest.expires_at = now + duration;
        assert!(manifest.validate(&policy, now, false).is_err());
        assert!(manifest.validate(&policy, now, true).is_err());
    }
    manifest.expires_at = now + chrono::Duration::seconds(1);
    assert!(
        manifest
            .validate(&policy, manifest.expires_at, false)
            .is_err()
    );
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
