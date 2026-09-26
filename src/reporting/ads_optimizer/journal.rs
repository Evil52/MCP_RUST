//! Explicit, append-only local run archives for the offline optimizer.
//!
//! The operator controls the existing parent directory and its ancestors for
//! the duration of the write. This is not a hostile shared-filesystem boundary.
//! A valid manifest and matching artifact hashes identify a completed run;
//! directory or manifest-file existence alone does not. Digests establish
//! integrity relative to the manifest, not authenticity or execution approval.

use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io::Write as _,
    os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{MAX_INPUT_BYTES, OptimizationObjective, ShadowReport, parse_input, recommend};

pub const MAX_REPORT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunArtifact {
    pub file_name: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunManifest {
    pub version: u32,
    pub optimizer_version: String,
    /// Wall-clock archive time, separate from the deterministic evidence clock.
    pub recorded_at: DateTime<Utc>,
    pub account_id: String,
    pub as_of: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub window_start: NaiveDate,
    pub window_end: NaiveDate,
    pub objective: OptimizationObjective,
    /// Canonical optimizer input identity, not the hash of the raw JSON file.
    pub input_sha256: String,
    pub evidence: RunArtifact,
    pub report: RunArtifact,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum JournalError {
    #[error("journal input exceeds its size limit")]
    LimitExceeded,
    #[error("journal evidence violates the optimizer contract")]
    InvalidEvidence,
    #[error("journal report does not match its evidence")]
    InvalidReport,
    #[error("journal destination path is invalid")]
    InvalidPath,
    #[error("journal parent must be an existing operator-controlled directory")]
    UnsafeParent,
    #[error("journal destination already exists")]
    DestinationExists,
    #[error("journal storage is unavailable")]
    Unavailable,
}

/// Archives exact evidence and the same pretty-printed JSON plus final newline
/// emitted by the CLI. Both report representations must match a fresh, pure
/// calculation before any filesystem mutation occurs.
///
/// The destination must not exist, including as an empty directory, file or
/// dangling symlink. Its parent must already exist, must not itself be a symlink,
/// and must not be writable by group or others. Relative destinations are
/// resolved beneath the current directory. Explicit `.`/`..` segments are
/// rejected. Failed writes leave their partial run in place; retries need a new
/// destination and cannot silently repair or overwrite an attempted archive.
pub fn record_run(
    directory: &Path,
    evidence_bytes: &[u8],
    report: &ShadowReport,
    report_bytes: &[u8],
) -> Result<RunManifest, JournalError> {
    if evidence_bytes.len() > MAX_INPUT_BYTES || report_bytes.len() > MAX_REPORT_BYTES {
        return Err(JournalError::LimitExceeded);
    }
    let input = parse_input(evidence_bytes).map_err(|_| JournalError::InvalidEvidence)?;
    let expected = recommend(input.clone()).map_err(|_| JournalError::InvalidEvidence)?;
    if expected != *report || json_bytes(&expected)? != report_bytes {
        return Err(JournalError::InvalidReport);
    }
    let (parent, directory) = destination(directory)?;
    let manifest = RunManifest {
        version: 1,
        optimizer_version: env!("CARGO_PKG_VERSION").to_owned(),
        recorded_at: Utc::now(),
        account_id: input.account_id,
        as_of: input.as_of,
        observed_at: input.observed_at,
        window_start: input.window_start,
        window_end: input.window_end,
        objective: input.objective,
        input_sha256: report.input_sha256.clone(),
        evidence: artifact("evidence.json", evidence_bytes),
        report: artifact("report.json", report_bytes),
    };
    let manifest_bytes = json_bytes(&manifest)?;
    DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                JournalError::DestinationExists
            } else {
                JournalError::Unavailable
            }
        })?;
    sync_directory(&parent)?;
    persist_artifacts(
        &directory,
        evidence_bytes,
        report_bytes,
        &manifest_bytes,
        write_new_file,
    )?;
    Ok(manifest)
}

fn destination(directory: &Path) -> Result<(PathBuf, PathBuf), JournalError> {
    if directory
        .as_os_str()
        .as_encoded_bytes()
        .split(|byte| *byte == b'/')
        .any(|segment| matches!(segment, b"." | b".."))
    {
        return Err(JournalError::InvalidPath);
    }
    let name = directory.file_name().ok_or(JournalError::InvalidPath)?;
    let parent = directory
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let metadata = fs::symlink_metadata(parent).map_err(|_| JournalError::UnsafeParent)?;
    if !metadata.is_dir() || metadata.permissions().mode() & 0o022 != 0 {
        return Err(JournalError::UnsafeParent);
    }
    let parent = parent
        .canonicalize()
        .map_err(|_| JournalError::UnsafeParent)?;
    let directory = parent.join(name);
    Ok((parent, directory))
}

fn json_bytes(value: &impl Serialize) -> Result<Vec<u8>, JournalError> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|_| JournalError::InvalidReport)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn artifact(file_name: &str, bytes: &[u8]) -> RunArtifact {
    use std::fmt::Write as _;

    let sha256 =
        Sha256::digest(bytes)
            .iter()
            .fold(String::with_capacity(64), |mut output, byte| {
                write!(output, "{byte:02x}").expect("writing to String cannot fail");
                output
            });
    RunArtifact {
        file_name: file_name.to_owned(),
        size_bytes: bytes.len() as u64,
        sha256,
    }
}

fn persist_artifacts(
    directory: &Path,
    evidence_bytes: &[u8],
    report_bytes: &[u8],
    manifest_bytes: &[u8],
    write: fn(&Path, &[u8]) -> Result<(), JournalError>,
) -> Result<(), JournalError> {
    write(&directory.join("evidence.json"), evidence_bytes)?;
    write(&directory.join("report.json"), report_bytes)?;
    sync_directory(directory)?;
    // Publish completion only after both preceding artifacts are durable.
    write(&directory.join("manifest.json"), manifest_bytes)?;
    sync_directory(directory)
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<(), JournalError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| JournalError::Unavailable)?;
    file.write_all(bytes)
        .map_err(|_| JournalError::Unavailable)?;
    file.sync_all().map_err(|_| JournalError::Unavailable)
}

fn sync_directory(path: &Path) -> Result<(), JournalError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| JournalError::Unavailable)
}

#[cfg(test)]
#[path = "journal_tests.rs"]
mod tests;
