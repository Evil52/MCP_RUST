use anyhow::{Context, Result, ensure};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

pub(super) fn digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut result, byte| {
            write!(result, "{byte:02x}").expect("string write");
            result
        })
}

#[cfg(test)]
#[path = "journal_tests.rs"]
mod tests;

pub(super) fn read_private_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    read_json(path, 0o077)
}

/// Existing robot policies are intentionally installed 0644: they contain
/// no credentials. Disallow group/other writes, but not policy visibility.
pub(super) fn read_policy_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    read_json(path, 0o022)
}

fn read_json<T: DeserializeOwned>(path: &Path, forbidden_permissions: u32) -> Result<T> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file()
            && metadata.permissions().mode() & forbidden_permissions == 0
            && metadata.len() <= 256 * 1024,
        "expected bounded regular JSON file with safe permissions"
    );
    let mut bytes = Vec::new();
    File::open(path)?
        .take(256 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 256 * 1024, "JSON file grew beyond limit");
    serde_json::from_slice(&bytes).context("invalid operator JSON")
}

/// One directory per account/name (not per attempt), on durable storage.
/// The exclusive OS lock survives until drop; crash recovery never deletes
/// an attempt record. A partial record is still an attempted operation.
pub(super) struct Journal {
    directory: PathBuf,
    lock: Option<File>,
}

impl Journal {
    pub(super) fn open(root: &Path, manifest: &Value, writable: bool) -> Result<Self> {
        let metadata = fs::symlink_metadata(root)?;
        ensure!(
            metadata.is_dir() && metadata.permissions().mode().trailing_zeros() >= 6,
            "journal root must be an existing private non-symlink directory"
        );
        // Stable across authorization references and changed bids: changing
        // the manifest cannot silently unlock a second Nexus budget transfer.
        let directory = root.join("ofk_region_wb-Nexus");
        if writable && !directory.try_exists()? {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new().mode(0o700).create(&directory)?;
            File::open(root)?.sync_all()?;
        }
        let metadata = fs::symlink_metadata(&directory)?;
        ensure!(
            metadata.is_dir() && metadata.permissions().mode().trailing_zeros() >= 6,
            "unsafe launch journal directory"
        );
        let lock = if writable {
            let path = directory.join("operator.lock");
            if path.try_exists()? {
                ensure!(
                    fs::symlink_metadata(&path)?.is_file(),
                    "unsafe operator lock"
                );
            }
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .open(path)?;
            lock.try_lock()
                .context("another launch operator owns the journal")?;
            Some(lock)
        } else {
            None
        };
        let journal = Self { directory, lock };
        let path = journal.directory.join("manifest.json");
        if writable && !path.try_exists()? {
            journal.record("manifest", manifest)?;
        }
        ensure!(
            read_private_json::<Value>(&path)? == *manifest,
            "manifest differs from immutable launch authorization; no writes allowed"
        );
        Ok(journal)
    }

    fn record(&self, name: &str, value: &Value) -> Result<()> {
        ensure!(
            self.lock.is_some(),
            "read-only reconcile cannot write journal"
        );
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.directory.join(format!("{name}.json")))?;
        file.write_all(&serde_json::to_vec(value)?)?;
        file.sync_all()?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    pub(super) fn attempted(&self, stage: &str) -> bool {
        self.directory
            .join(format!("{stage}-attempted.json"))
            .exists()
    }

    pub(super) fn assert_not_attempted(&self, stage: &str) -> Result<()> {
        ensure!(
            !self.attempted(stage),
            "{stage} already attempted; reconcile only, do not repeat"
        );
        Ok(())
    }

    pub(super) fn attempt(&self, stage: &str, evidence: &Value) -> Result<()> {
        self.record(
            &format!("{stage}-attempted"),
            &json!({"attempted_at":chrono::Utc::now(),"evidence":evidence}),
        )
    }

    pub(super) fn receipt(&self, stage: &str, value: &Value) -> Result<()> {
        self.record(&format!("{stage}-receipt"), value)
    }

    pub(super) fn has_receipt(&self, stage: &str) -> bool {
        self.directory
            .join(format!("{stage}-receipt.json"))
            .exists()
    }

    pub(super) fn require_receipt(&self, stage: &str) -> Result<Value> {
        read_private_json(&self.directory.join(format!("{stage}-receipt.json")))
    }

    pub(super) fn maybe_campaign_id(&self) -> Result<Option<u64>> {
        if !self.has_receipt("create") {
            return Ok(None);
        }
        Ok(Some(self.campaign_id()?))
    }

    pub(super) fn campaign_id(&self) -> Result<u64> {
        self.require_receipt("create")?
            .get("campaign_id")
            .and_then(Value::as_u64)
            .filter(|id| *id > 0)
            .context("confirmed create response is missing")
    }
}
