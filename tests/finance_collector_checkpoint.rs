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

#[test]
fn personal_success_is_persisted_and_a_rotated_key_gets_conservative_admission() {
    let directory = Directory::new("personal");
    let scope = sha256(b"synthetic-seller-personal");
    let key = sha256(b"synthetic-key-personal");
    let now = Utc::now();
    let journal = LocalJournal::open_personal(&directory.0, &scope, "period", &key).unwrap();
    assert!(journal.confirm_personal_read().is_err());
    journal.reserve(now).unwrap();
    assert_eq!(
        journal.next_allowed_at().unwrap(),
        Some(now + Duration::hours(12))
    );
    journal.confirm_personal_read().unwrap();
    assert_eq!(
        journal.next_allowed_at().unwrap(),
        Some(now + Duration::seconds(60))
    );
    drop(journal);
    let resumed =
        LocalJournal::open_personal(&directory.0, &scope, "another-period", &key).unwrap();
    assert!(resumed.confirm_personal_read().is_err());
    assert_eq!(resumed.reserve(now), Err(CheckpointError::Deferred));
    resumed.reserve(now + Duration::seconds(60)).unwrap();
    assert_eq!(
        resumed.next_allowed_at().unwrap(),
        Some(now + Duration::seconds(120))
    );
    drop(resumed);
    let rotated = LocalJournal::open_personal(
        &directory.0,
        &scope,
        "third-period",
        &sha256(b"rotated-key"),
    )
    .unwrap();
    rotated.reserve(now + Duration::seconds(120)).unwrap();
    assert_eq!(
        rotated.next_allowed_at().unwrap(),
        Some(now + Duration::seconds(120) + Duration::hours(12))
    );
}

#[test]
fn official_evidence_is_immutable_bounded_and_restricted_to_canonical_file_names() {
    let directory = Directory::new("evidence");
    let scope = sha256(b"synthetic-seller-evidence");
    let journal = LocalJournal::open(&directory.0, &scope, "closed-report").unwrap();
    let report = json!({"report_id":123,"state":"unavailable","source":"synthetic"});
    let path = journal
        .save_evidence("official-report-123.json", &report)
        .unwrap();
    assert_eq!(
        journal
            .save_evidence("official-report-123.json", &report)
            .unwrap(),
        path
    );
    assert!(
        journal
            .save_evidence("official-report-123.json", &json!({"state":"changed"}))
            .is_err()
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&fs::read(&path).unwrap()).unwrap(),
        report
    );
    assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o077, 0);
    journal
        .save_evidence("report-list.json", &json!([]))
        .unwrap();
    for name in [
        "official-report.json",
        "official-report-0.json",
        "official-report-01.json",
        "official-report-+1.json",
        "official-report-9223372036854775808.json",
        "../official-report-123.json",
        "https://internal/report",
        "official-report-1.json/extra",
    ] {
        assert!(
            journal.save_evidence(name, &report).is_err(),
            "accepted filename: {name}"
        );
    }
    drop(journal);
    let resumed = LocalJournal::open(&directory.0, &scope, "closed-report").unwrap();
    assert_eq!(
        resumed
            .save_evidence("official-report-123.json", &report)
            .unwrap(),
        path
    );
    assert!(
        resumed
            .save_evidence("official-report-123.json", &json!(null))
            .is_err()
    );
}

#[test]
fn reviewed_legacy_migration_preserves_reservation_and_receipt_and_cannot_be_replayed() {
    let directory = Directory::new("legacy-receipt");
    let scope = sha256(b"synthetic-seller-legacy");
    let key = sha256(b"synthetic-key-legacy");
    let now = Utc::now();
    let started = now - Duration::hours(1);
    let due = started + Duration::hours(12);
    let journal = LocalJournal::open_personal(&directory.0, &scope, "closed-report", &key).unwrap();
    drop(journal);
    let seller_root = directory.0.join(format!("wb-{scope}"));
    let quota_path = seller_root.join("quota.json");
    fs::write(
        &quota_path,
        serde_json::to_vec(&json!({"next_allowed_at":due})).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&quota_path, fs::Permissions::from_mode(0o600)).unwrap();
    let journal = LocalJournal::open_personal(&directory.0, &scope, "closed-report", &key).unwrap();
    assert_eq!(journal.reserve(now), Err(CheckpointError::Deferred));
    assert!(
        journal
            .migrate_personal_legacy("finance-operator", &scope)
            .is_err()
    );
    assert_eq!(journal.next_allowed_at().unwrap(), Some(due));
    let receipt = serde_json::to_vec(&json!({
        "version":1,"actor_id":"finance-operator","key_fingerprint":key,
        "seller_scope":scope,
        "endpoint":"POST https://finance-api.wildberries.ru/api/finance/v1/sales-reports/detailed",
        "http_status":200,"request_started_at":started,"expected_next_allowed_at":due,
        "operator_verified_at":now,"evidence_ref":"synthetic-reviewed-probe",
    }))
    .unwrap();
    let receipt_path = seller_root.join("legacy-success-receipt.json");
    fs::write(&receipt_path, &receipt).unwrap();
    fs::set_permissions(&receipt_path, fs::Permissions::from_mode(0o600)).unwrap();
    journal
        .migrate_personal_legacy("finance-operator", &scope)
        .unwrap();
    assert_eq!(
        journal.next_allowed_at().unwrap(),
        Some(started + Duration::seconds(60))
    );
    assert_eq!(fs::read(&receipt_path).unwrap(), receipt);
    assert!(
        journal
            .migrate_personal_legacy("finance-operator", &scope)
            .is_err()
    );
    let backups = fs::read_dir(&seller_root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("legacy-quota-")
        })
        .collect::<Vec<_>>();
    assert_eq!(backups.len(), 1);
    let original: serde_json::Value =
        serde_json::from_slice(&fs::read(&backups[0]).unwrap()).unwrap();
    assert_eq!(original["next_allowed_at"], json!(due));
    drop(journal);
    let resumed = LocalJournal::open_personal(&directory.0, &scope, "closed-report", &key).unwrap();
    assert!(
        resumed
            .migrate_personal_legacy("finance-operator", &scope)
            .is_err()
    );
    resumed.reserve(now).unwrap();
    assert_eq!(
        resumed.next_allowed_at().unwrap(),
        Some(now + Duration::seconds(60))
    );
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
