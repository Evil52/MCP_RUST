use super::*;
use std::os::unix::fs::DirBuilderExt;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "nexus-journal-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn replacement_fixture() -> (Fixture, Value, Value) {
    let root = Fixture::new();
    let (mut old, _) = crate::control::wb_launch::tests::fixture();
    old.scope = crate::control::wb_launch::LaunchScope::CreateOnly;
    old.budget_rubles = 0;
    old.bids_kopecks = [
        146_312_604,
        207_418_966,
        455_101_276,
        461_126_890,
        529_996_417,
    ]
    .into_iter()
    .map(|nm| (nm, 922))
    .collect();
    old.expires_at = chrono::Utc::now() - chrono::Duration::minutes(10);
    let old = serde_json::to_value(old).unwrap();
    let journal = Journal::open(&root.0, &old, true).unwrap();
    journal
        .attempt("create", &json!({"previous_preflight":true}))
        .unwrap();
    let attempt: Value = read_private_json(&journal.path("create-attempted")).unwrap();
    let (mut new, _) = crate::control::wb_launch::tests::fixture();
    new.scope = crate::control::wb_launch::LaunchScope::CreateOnly;
    new.budget_rubles = 0;
    new.authorized_at = chrono::Utc::now() + chrono::Duration::seconds(1);
    new.authorization_reference = "test/new-reviewed-create".into();
    new.recreate = Some(crate::control::wb_launch::RecreateApproval {
        previous_manifest_sha256: digest(&serde_json::to_vec(&old).unwrap()),
        previous_attempt_sha256: digest(&serde_json::to_vec(&attempt).unwrap()),
    });
    drop(journal);
    (root, old, serde_json::to_value(new).unwrap())
}

#[test]
fn replacement_preserves_history_and_allows_exactly_one_new_create() {
    let (root, old, new) = replacement_fixture();
    Journal::inspect_recreate(&root.0, &new).unwrap();
    assert!(
        !root
            .0
            .join("ofk_region_wb-Nexus/recreate-manifest.json")
            .exists()
    );
    let journal = Journal::open(&root.0, &new, true).unwrap();
    assert_eq!(
        read_private_json::<Value>(&journal.directory.join("manifest.json")).unwrap(),
        old
    );
    assert!(journal.directory.join("create-attempted.json").exists());
    journal
        .attempt("create", &json!({"new_preflight":true}))
        .unwrap();
    assert!(Journal::inspect_recreate(&root.0, &new).is_err());
    assert!(journal.attempt("create", &json!({})).is_err());
    assert!(journal.attempt("fund", &json!({})).is_err());
    assert!(journal.attempt("start", &json!({})).is_err());
    assert!(Journal::open(&root.0, &new, true).is_err());
    drop(journal);
    assert!(Journal::open(&root.0, &old, true).is_err());
    let journal = Journal::open(&root.0, &new, true).unwrap();
    assert!(journal.assert_not_attempted("create").is_err());
    journal
        .receipt("create", &json!({"campaign_id":42}))
        .unwrap();
    drop(journal);
    assert_eq!(
        Journal::open(&root.0, &new, false)
            .unwrap()
            .campaign_id()
            .unwrap(),
        42
    );
    let mut changed = new;
    changed["authorization_reference"] = json!("third/attempt");
    assert!(Journal::open(&root.0, &changed, true).is_err());
}

#[test]
fn replacement_rejects_changed_evidence_and_prior_progress() {
    for field in ["previous_manifest_sha256", "previous_attempt_sha256"] {
        let (root, _, mut new) = replacement_fixture();
        new["recreate"][field] = json!("wrong");
        assert!(Journal::open(&root.0, &new, true).is_err());
    }
    for stage in ["create", "bids", "fund", "start", "policy"] {
        let (root, old, new) = replacement_fixture();
        let journal = Journal::open(&root.0, &old, true).unwrap();
        journal.receipt(stage, &json!({})).unwrap();
        drop(journal);
        assert!(Journal::open(&root.0, &new, true).is_err());
    }
}

#[test]
fn attempted_transfer_cannot_be_repeated_after_reopen() {
    let root = Fixture::new();
    let manifest = json!({"approval":"one"});
    let journal = Journal::open(&root.0, &manifest, true).unwrap();
    journal.attempt("fund", &json!({"sum":1000})).unwrap();
    assert!(journal.attempt("fund", &json!({"sum":1000})).is_err());
    assert!(Journal::open(&root.0, &manifest, true).is_err());
    drop(journal);
    let journal = Journal::open(&root.0, &manifest, true).unwrap();
    assert!(journal.assert_not_attempted("fund").is_err());
    assert!(!journal.has_receipt("fund"));
    drop(journal);
    assert!(Journal::open(&root.0, &json!({"approval":"two"}), true).is_err());
    let read_only = Journal::open(&root.0, &manifest, false).unwrap();
    assert!(read_only.receipt("fund", &json!({"total":1000})).is_err());
}

#[test]
fn partial_attempt_record_fails_closed() {
    let root = Fixture::new();
    let manifest = json!({"approval":"one"});
    let journal = Journal::open(&root.0, &manifest, true).unwrap();
    File::create(journal.directory.join("fund-attempted.json")).unwrap();
    assert!(journal.assert_not_attempted("fund").is_err());
    assert!(journal.campaign_id().is_err());
    assert!(journal.maybe_campaign_id().unwrap().is_none());
}

#[test]
fn symlink_and_world_readable_roots_are_rejected() {
    let root = Fixture::new();
    let link = root.0.join("link");
    std::os::unix::fs::symlink(&root.0, &link).unwrap();
    assert!(Journal::open(&link, &json!({}), true).is_err());
    fs::set_permissions(&root.0, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(Journal::open(&root.0, &json!({}), true).is_err());
}

#[test]
fn public_robot_policy_is_allowed_but_not_public_approval_or_writable_policy() {
    let root = Fixture::new();
    let path = root.0.join("policy.json");
    let mut file = File::create(&path).unwrap();
    file.write_all(b"{}").unwrap();
    drop(file);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(read_policy_json::<Value>(&path).is_ok());
    assert!(read_private_json::<Value>(&path).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
    assert!(read_policy_json::<Value>(&path).is_err());
}
