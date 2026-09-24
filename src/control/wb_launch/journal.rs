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

#[path = "recreate_journal.rs"]
mod recreate;
pub(super) fn validate_recreate(directory: &Path, manifest: &Value) -> Result<()> {
    recreate::validate(directory, manifest)
}

/// A dangling symlink/partial file also fences a previously attempted stage.
pub(super) fn entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

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
    _account_lock: Option<File>,
    recovery: bool,
    continuation: bool,
}

impl Journal {
    /// Advisory read-only check; create still locks and journals before POST.
    pub(super) fn inspect_recreate(root: &Path, manifest: &Value) -> Result<()> {
        let old = read_private_json(&root.join("ofk_region_wb-Nexus/manifest.json"))?;
        let original = Self::open(root, &old, false)?;
        recreate::validate(&original.directory, manifest)?;
        let revised_path = original.directory.join("recreate-manifest.json");
        if revised_path.try_exists()? {
            ensure!(
                read_private_json::<Value>(&revised_path)? == *manifest,
                "replacement manifest differs from immutable authorization"
            );
        }
        ensure!(
            !original
                .directory
                .join("recreate-create-attempted.json")
                .try_exists()?,
            "replacement create already attempted; reconcile only"
        );
        ensure!(
            !original
                .directory
                .join("recreate-create-receipt.json")
                .try_exists()?,
            "replacement create already confirmed; reconcile only"
        );
        Ok(())
    }

    pub(super) fn open(root: &Path, manifest: &Value, writable: bool) -> Result<Self> {
        let metadata = fs::symlink_metadata(root)?;
        ensure!(
            metadata.is_dir() && metadata.permissions().mode().trailing_zeros() >= 6,
            "journal root must be an existing private non-symlink directory"
        );
        // Serialize v2 creation/funding across one account, including different
        // campaign names whose product selections could overlap.
        let account_lock = if writable && manifest.get("version").and_then(Value::as_u64) == Some(2)
        {
            let account = manifest
                .get("account_id")
                .and_then(Value::as_str)
                .context("missing launch account")?;
            let path = root.join(format!("account-{}.lock", digest(account.as_bytes())));
            if entry_exists(&path)? {
                let metadata = fs::symlink_metadata(&path)?;
                ensure!(
                    metadata.is_file() && metadata.permissions().mode().trailing_zeros() >= 6,
                    "unsafe account lock"
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
                .context("another campaign operation owns this account journal")?;
            Some(lock)
        } else {
            None
        };
        // Stable across authorization references and changed bids: changing
        // the manifest cannot silently unlock a second Nexus budget transfer.
        let directory = root.join(directory_name(manifest)?);
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
        let recovery = manifest.get("recreate").is_some_and(|v| !v.is_null());
        let continuation = manifest
            .get("continue_created")
            .is_some_and(|v| !v.is_null());
        let journal = Self {
            directory,
            lock,
            _account_lock: account_lock,
            recovery,
            continuation,
        };
        if continuation {
            super::continuation::validate(&journal.directory, manifest)?;
        }
        if writable && !continuation {
            ensure!(
                !entry_exists(&journal.directory.join("continue-manifest.json"))?,
                "continuation authorization recorded; earlier writes forbidden"
            );
        }
        if recovery {
            recreate::validate(&journal.directory, manifest)?;
        } else if writable && !continuation {
            ensure!(
                !journal
                    .directory
                    .join("recreate-manifest.json")
                    .try_exists()?,
                "replacement authorization already recorded; legacy writes forbidden"
            );
        }
        let path = journal.path("manifest");
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
        if self.continuation {
            ensure!(!name.starts_with("create"), "continuation cannot create");
            let manifest = if name == "manifest" {
                value.clone()
            } else {
                read_private_json(&self.path("manifest"))?
            };
            super::continuation::validate(&self.directory, &manifest)?;
        }
        ensure!(
            !self.recovery || !name.starts_with("fund") && !name.starts_with("start"),
            "replacement create cannot fund or start"
        );
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.path(name))?;
        file.write_all(&serde_json::to_vec(value)?)?;
        file.sync_all()?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    fn path(&self, name: &str) -> PathBuf {
        let prefix = if self.continuation && !name.starts_with("create-") {
            "continue-"
        } else if self.recovery || self.continuation {
            "recreate-"
        } else {
            ""
        };
        self.directory.join(format!("{prefix}{name}.json"))
    }

    pub(super) fn attempted(&self, stage: &str) -> bool {
        entry_exists(&self.path(&format!("{stage}-attempted"))).unwrap_or(true)
    }

    pub(super) fn assert_not_attempted(&self, stage: &str) -> Result<()> {
        ensure!(
            !self.attempted(stage)
                && !self.has_receipt(stage)
                && !self.has_receipt(&format!("{stage}-response")),
            "{stage} already attempted; reconcile only, do not repeat"
        );
        Ok(())
    }

    pub(super) fn attempt(&self, stage: &str, evidence: &Value) -> Result<()> {
        self.assert_not_attempted(stage)?;
        if self.recovery {
            let manifest = read_private_json(&self.path("manifest"))?;
            recreate::validate(&self.directory, &manifest)?;
        }
        self.record(
            &format!("{stage}-attempted"),
            &json!({"attempted_at":chrono::Utc::now(),"evidence":evidence}),
        )
    }

    pub(super) fn receipt(&self, stage: &str, value: &Value) -> Result<()> {
        self.record(&format!("{stage}-receipt"), value)
    }

    pub(super) fn has_receipt(&self, stage: &str) -> bool {
        entry_exists(&self.path(&format!("{stage}-receipt"))).unwrap_or(true)
    }

    pub(super) fn require_receipt(&self, stage: &str) -> Result<Value> {
        read_private_json(&self.path(&format!("{stage}-receipt")))
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

/// Stable account/name identity, independent of authorization, amount and SKU edits.
/// The legacy Nexus directory is also used by v2 so changing schema cannot reset its history.
pub(super) fn directory_name(manifest: &Value) -> Result<String> {
    let version = manifest.get("version").and_then(Value::as_u64).unwrap_or(1);
    if version == 1 {
        return Ok("ofk_region_wb-Nexus".to_owned());
    }
    ensure!(version == 2, "unsupported launch journal version");
    let account = manifest
        .get("account_id")
        .and_then(Value::as_str)
        .context("missing journal account")?;
    let name = manifest
        .get("campaign_name")
        .and_then(Value::as_str)
        .context("missing journal campaign name")?;
    if account == super::ACCOUNT && name == super::NAME {
        return Ok("ofk_region_wb-Nexus".to_owned());
    }
    Ok(format!(
        "campaign-{}",
        digest(&serde_json::to_vec(&(account, name))?)
    ))
}
