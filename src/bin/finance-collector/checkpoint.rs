#![expect(
    clippy::verbose_bit_mask,
    reason = "explicit octal Unix permission masks make the privacy boundary auditable"
)]

//! A deployment-owned, single-host quota and normalized page journal. The file
//! lease spans the entire invocation; the quota is reserved durably before HTTP.
//! This is not a distributed lease across independent hosts/state directories.

use std::{
    fmt::Write as _,
    fs::{self, DirBuilder, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Duration, Utc};
use mcp_ozon::reporting::checkpoint::{CheckpointError, JournalFuture, PageJournal};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

const QUOTA_INTERVAL: Duration = Duration::hours(12);
const MAX_CHECKPOINT_BYTES: u64 = 8 * 1024 * 1024;
const MAX_QUOTA_BYTES: u64 = 4096;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Quota {
    next_allowed_at: Option<DateTime<Utc>>,
}

pub struct LocalJournal {
    seller_root: PathBuf,
    pages: PathBuf,
    quota: Mutex<Quota>,
    _lease: File,
}

impl LocalJournal {
    pub fn open(root: &Path, seller_scope: &str, collection: &str) -> Result<Self> {
        ensure!(valid_digest(seller_scope), "seller quota scope is invalid");
        let root = private_directory(root)?;
        let seller_root = private_directory(&root.join(format!("wb-{seller_scope}")))?;
        let lock_path = seller_root.join("exclusive.lock");
        if lock_path.exists() {
            validate_file(&lock_path, MAX_QUOTA_BYTES)?;
        }
        let lease = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&lock_path)
            .context("financial journal lease is unavailable")?;
        lease
            .try_lock()
            .context("another financial operator holds this seller lease")?;
        validate_file(&lock_path, MAX_QUOTA_BYTES)?;
        let quota_path = seller_root.join("quota.json");
        let quota = match read_private(&quota_path, MAX_QUOTA_BYTES)? {
            Some(bytes) => {
                let quota: Quota =
                    serde_json::from_slice(&bytes).context("financial quota journal is invalid")?;
                ensure!(
                    quota.next_allowed_at.is_some(),
                    "financial quota journal is incomplete"
                );
                quota
            }
            None => Quota {
                next_allowed_at: None,
            },
        };
        let pages = private_directory(
            &seller_root.join(format!("pages-{}", sha256(collection.as_bytes()))),
        )?;
        Ok(Self {
            seller_root,
            pages,
            quota: Mutex::new(quota),
            _lease: lease,
        })
    }

    pub fn reserve(&self, now: DateTime<Utc>) -> Result<(), CheckpointError> {
        let mut quota = self
            .quota
            .lock()
            .map_err(|_| CheckpointError::Unavailable)?;
        if quota.next_allowed_at.is_some_and(|next| now < next) {
            return Err(CheckpointError::Deferred);
        }
        quota.next_allowed_at = Some(
            now.checked_add_signed(QUOTA_INTERVAL)
                .ok_or(CheckpointError::Invalid)?,
        );
        persist_json(&self.seller_root, "quota.json", &*quota)
            .map_err(|_| CheckpointError::Unavailable)
    }

    pub fn postpone(&self, seconds: u64) -> Result<()> {
        let delay = Duration::try_seconds(
            i64::try_from(seconds).context("upstream pause exceeds the supported bound")?,
        )
        .context("upstream pause exceeds the supported bound")?;
        let next = Utc::now()
            .checked_add_signed(delay)
            .context("upstream pause exceeds the supported bound")?;
        let mut quota = self
            .quota
            .lock()
            .map_err(|_| anyhow::anyhow!("financial quota lock is unavailable"))?;
        quota.next_allowed_at = Some(
            quota
                .next_allowed_at
                .map_or(next, |previous| previous.max(next)),
        );
        persist_json(&self.seller_root, "quota.json", &*quota)
    }

    pub fn next_allowed_at(&self) -> Result<Option<DateTime<Utc>>> {
        Ok(self
            .quota
            .lock()
            .map_err(|_| anyhow::anyhow!("financial quota lock is unavailable"))?
            .next_allowed_at)
    }
}

impl PageJournal for LocalJournal {
    fn load<'a>(&'a self, key: &'a str) -> JournalFuture<'a, Option<Value>> {
        Box::pin(async move {
            if !valid_digest(key) {
                return Err(CheckpointError::Invalid);
            }
            read_private(
                &self.pages.join(format!("{key}.json")),
                MAX_CHECKPOINT_BYTES,
            )
            .map_err(|_| CheckpointError::Unavailable)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(|_| CheckpointError::Invalid))
            .transpose()
        })
    }

    fn admit(&self) -> JournalFuture<'_, ()> {
        Box::pin(async move { self.reserve(Utc::now()) })
    }

    fn save<'a>(&'a self, key: &'a str, page: Value) -> JournalFuture<'a, ()> {
        Box::pin(async move {
            if !valid_digest(key) {
                return Err(CheckpointError::Invalid);
            }
            let path = self.pages.join(format!("{key}.json"));
            if let Some(previous) = read_private(&path, MAX_CHECKPOINT_BYTES)
                .map_err(|_| CheckpointError::Unavailable)?
            {
                let previous: Value =
                    serde_json::from_slice(&previous).map_err(|_| CheckpointError::Invalid)?;
                return if previous == page {
                    Ok(())
                } else {
                    Err(CheckpointError::Invalid)
                };
            }
            let bytes = serde_json::to_vec(&page).map_err(|_| CheckpointError::Invalid)?;
            if bytes.len() as u64 > MAX_CHECKPOINT_BYTES {
                return Err(CheckpointError::Invalid);
            }
            persist_bytes(&self.pages, &format!("{key}.json"), &bytes)
                .map_err(|_| CheckpointError::Unavailable)
        })
    }
}

fn private_directory(path: &Path) -> Result<PathBuf> {
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            DirBuilder::new()
                .mode(0o700)
                .create(path)
                .context("private journal directory cannot be created")?;
        }
        Err(_) => anyhow::bail!("private journal directory is unavailable"),
    }
    let metadata = fs::symlink_metadata(path).context("journal directory is unavailable")?;
    ensure!(
        metadata.is_dir()
            && !metadata.file_type().is_symlink()
            && metadata.permissions().mode() & 0o077 == 0,
        "journal directory must be a real private directory (0700)"
    );
    // Also cover a previous initialization interrupted after mkdir but before
    // parent fsync. Syncing a leaf alone cannot persist its parent entry.
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .context("journal parent directory is unavailable")?
        .sync_all()
        .context("journal parent directory cannot be synchronized")?;
    path.canonicalize()
        .context("journal directory is unavailable")
}

fn validate_file(path: &Path, bound: u64) -> Result<()> {
    let metadata = fs::symlink_metadata(path).context("journal file is unavailable")?;
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.nlink() == 1
            && metadata.permissions().mode() & 0o077 == 0
            && metadata.len() <= bound,
        "journal file must be a bounded private regular file"
    );
    Ok(())
}

fn read_private(path: &Path, bound: u64) -> Result<Option<Vec<u8>>> {
    match fs::symlink_metadata(path) {
        Ok(_) => validate_file(path, bound)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => anyhow::bail!("journal file is unavailable"),
    }
    let mut bytes = Vec::new();
    File::open(path)
        .context("journal file cannot be read")?
        .take(bound + 1)
        .read_to_end(&mut bytes)
        .context("journal file cannot be read")?;
    ensure!(
        bytes.len() as u64 <= bound,
        "journal file exceeded its bound"
    );
    Ok(Some(bytes))
}

fn persist_json(root: &Path, name: &str, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value).context("journal cannot be serialized")?;
    persist_bytes(root, name, &bytes)
}

fn persist_bytes(root: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let temporary = root.join(format!(
        ".pending-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .context("journal transaction cannot be created")?;
    file.write_all(bytes)
        .context("journal transaction cannot be written")?;
    file.sync_all()
        .context("journal transaction cannot be synchronized")?;
    drop(file);
    fs::rename(&temporary, root.join(name)).context("journal transaction cannot be published")?;
    File::open(root)
        .context("journal directory is unavailable")?
        .sync_all()
        .context("journal directory cannot be synchronized")?;
    Ok(())
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub fn sha256(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        let _ = write!(result, "{byte:02x}");
    }
    result
}
