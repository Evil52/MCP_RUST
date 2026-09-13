#[path = "../src/bin/finance-collector/checkpoint.rs"]
mod checkpoint;

use chrono::{Duration, Utc};
use mcp_ozon::reporting::checkpoint::{CheckpointError, PageJournal};
use serde_json::json;
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

use checkpoint::{LocalJournal, sha256};

struct Directory(PathBuf);

impl Directory {
    fn new(label: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
            "finance-journal-{label}-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        )))
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn quota_survives_restarts_and_page_replay_does_not_spend_another_departure() {
    let directory = Directory::new("restart");
    let scope = sha256(b"seller-one");
    let journal = LocalJournal::open(&directory.0, &scope, "pilot:2026-09-10").unwrap();
    let now = Utc::now();
    journal.reserve(now).unwrap();
    let due = journal.next_allowed_at().unwrap().unwrap();
    assert_eq!(due, now + Duration::hours(12));
    assert_eq!(
        journal.reserve(now + Duration::minutes(1)),
        Err(CheckpointError::Deferred)
    );
    let key = sha256(b"page-one");
    let normalized = json!([{"rrd_id": 11, "for_pay": {"units": 123, "scale": 3}}]);
    journal.save(&key, normalized.clone()).await.unwrap();
    journal.save(&key, normalized.clone()).await.unwrap();
    assert_eq!(
        journal.save(&key, json!([])).await,
        Err(CheckpointError::Invalid)
    );
    drop(journal);
    let resumed = LocalJournal::open(&directory.0, &scope, "pilot:2026-09-10").unwrap();
    assert_eq!(resumed.load(&key).await.unwrap(), Some(normalized));
    assert_eq!(resumed.admit().await, Err(CheckpointError::Deferred));
    assert_eq!(resumed.next_allowed_at().unwrap(), Some(due));
    resumed.reserve(due).unwrap();
    let terminal_key = sha256(b"page-terminal");
    resumed.save(&terminal_key, json!(null)).await.unwrap();
    assert_eq!(
        resumed.load(&terminal_key).await.unwrap(),
        Some(json!(null))
    );
    assert!(resumed.postpone(u64::MAX).is_err());
    assert!(resumed.postpone(i64::MAX.unsigned_abs()).is_err());
    resumed.postpone(3 * 24 * 60 * 60).unwrap();
    assert!(resumed.next_allowed_at().unwrap().unwrap() > due + Duration::hours(12));
}

#[test]
fn seller_lease_prevents_parallel_invocations_and_quota_is_shared_between_periods() {
    let directory = Directory::new("lease");
    let scope = sha256(b"seller-two");
    let first = LocalJournal::open(&directory.0, &scope, "period-one").unwrap();
    assert!(LocalJournal::open(&directory.0, &scope, "period-two").is_err());
    first.reserve(Utc::now()).unwrap();
    drop(first);
    let second = LocalJournal::open(&directory.0, &scope, "period-two").unwrap();
    assert_eq!(second.reserve(Utc::now()), Err(CheckpointError::Deferred));
}

#[tokio::test]
async fn untrusted_paths_corruption_and_unbounded_pages_fail_closed() {
    let directory = Directory::new("invalid");
    let scope = sha256(b"seller-three");
    let journal = LocalJournal::open(&directory.0, &scope, "collection").unwrap();
    assert_eq!(
        journal.load("../quota").await,
        Err(CheckpointError::Invalid)
    );
    let key = sha256(b"too-large");
    assert_eq!(
        journal.save(&key, json!("x".repeat(8 * 1024 * 1024))).await,
        Err(CheckpointError::Invalid)
    );
    drop(journal);
    let quota = directory.0.join(format!("wb-{scope}/quota.json"));
    fs::write(&quota, b"malformed").unwrap();
    fs::set_permissions(&quota, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(LocalJournal::open(&directory.0, &scope, "collection").is_err());
    fs::write(&quota, b"{}").unwrap();
    assert!(LocalJournal::open(&directory.0, &scope, "collection").is_err());
    fs::remove_file(&quota).unwrap();
    std::os::unix::fs::symlink("/dev/null", &quota).unwrap();
    assert!(LocalJournal::open(&directory.0, &scope, "collection").is_err());
}
