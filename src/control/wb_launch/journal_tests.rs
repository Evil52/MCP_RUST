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
