use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use sha2::{Digest, Sha256};

use super::{
    bundle::ReportBundle,
    outbox::ArtifactIdentity,
    postgres_outbox::{PostgresOutboxError, PostgresOutboxRepository},
};

const MAX_XLSX_BYTES: u64 = 16 * 1024 * 1024;
const MAX_HTML_BYTES: u64 = 2 * 1024 * 1024;
static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct LocalArtifactStore {
    root: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistDisposition {
    Created,
    Reused,
    Recovered,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedArtifactReceipt {
    pub artifact: ArtifactIdentity,
    pub html_object_key: String,
    pub xlsx_size_bytes: u64,
    pub html_size_bytes: u64,
    pub disposition: PersistDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredReportBundle {
    pub html: String,
    pub xlsx: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum ArtifactStoreError {
    #[error("artifact store root is invalid")]
    InvalidRoot,
    #[error("artifact object key is invalid")]
    InvalidObjectKey,
    #[error("artifact bundle failed its integrity check")]
    Integrity,
    #[error("artifact store is unavailable")]
    Unavailable,
}

#[derive(Debug, thiserror::Error)]
pub enum ArtifactPublicationError {
    #[error("daily report artifact could not be persisted")]
    Storage,
    #[error("daily report artifact could not be linked to the outbox")]
    Outbox,
}

/// Persists a deterministic report and links its XLSX identity to an existing
/// generating outbox row.
///
/// Files are committed before the database transition. If the database reply
/// is lost, retrying verifies and reuses the same immutable files and the same
/// ready row. A storage failure leaves the database in `generating`; no ready
/// delivery can point at missing bytes.
pub async fn persist_and_mark_ready(
    store: &LocalArtifactStore,
    outbox: &PostgresOutboxRepository,
    batch_id: i64,
    bundle: &ReportBundle,
) -> Result<PersistedArtifactReceipt, ArtifactPublicationError> {
    outbox
        .verify_generation_artifact(batch_id, &bundle.artifact)
        .await
        .map_err(map_outbox)?;
    let store = store.clone();
    let bundle = bundle.clone();
    let receipt = tokio::task::spawn_blocking(move || store.persist(&bundle))
        .await
        .map_err(|_| ArtifactPublicationError::Storage)?
        .map_err(|_| ArtifactPublicationError::Storage)?;
    outbox
        .mark_ready(batch_id, &receipt.artifact)
        .await
        .map_err(map_outbox)?;
    Ok(receipt)
}

fn map_outbox(_: PostgresOutboxError) -> ArtifactPublicationError {
    ArtifactPublicationError::Outbox
}

impl LocalArtifactStore {
    /// Opens an existing operator-controlled artifact root.
    ///
    /// The root is never created implicitly and may not itself be a symbolic
    /// link. This prevents a configuration typo from writing reports into an
    /// unexpected filesystem location.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, ArtifactStoreError> {
        let root = root.as_ref();
        let metadata = fs::symlink_metadata(root).map_err(|_| ArtifactStoreError::InvalidRoot)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(ArtifactStoreError::InvalidRoot);
        }
        let root = root
            .canonicalize()
            .map_err(|_| ArtifactStoreError::InvalidRoot)?;
        Ok(Self { root })
    }

    /// Proves that the configured root is writable without leaving an
    /// artifact behind. Used by the container health check before any report
    /// generation is enabled.
    pub fn verify_writable(&self) -> Result<(), ArtifactStoreError> {
        let probe = self.root.join(format!(
            ".artifact-store-health-{}-{}",
            std::process::id(),
            NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe)
            .map_err(|_| ArtifactStoreError::Unavailable)?;
        let result = file.sync_all().map_err(|_| ArtifactStoreError::Unavailable);
        drop(file);
        let removed = fs::remove_file(&probe).map_err(|_| ArtifactStoreError::Unavailable);
        result.and(removed)
    }

    /// Persists one rendered report without overwriting an existing artifact.
    ///
    /// A crash may leave either sibling present. Retrying completes the pair
    /// only when every existing byte matches the deterministic rendered
    /// content. Any mismatch fails closed.
    pub fn persist(
        &self,
        bundle: &ReportBundle,
    ) -> Result<PersistedArtifactReceipt, ArtifactStoreError> {
        super::bundle::inspect_dry_run(bundle).map_err(|_| ArtifactStoreError::Integrity)?;
        let relative_xlsx = validate_object_key(&bundle.artifact.object_key)?;
        let relative_html = relative_xlsx.with_extension("html");
        let html_object_key = path_to_object_key(&relative_html)?;
        let html_bytes = bundle.html.as_bytes();
        if html_bytes.is_empty() || html_bytes.len() as u64 > MAX_HTML_BYTES {
            return Err(ArtifactStoreError::Integrity);
        }
        let html_sha256 = &bundle.artifact.html_sha256;
        let parent = relative_xlsx
            .parent()
            .ok_or(ArtifactStoreError::InvalidObjectKey)?;
        let directory = self.ensure_directory(parent)?;
        let xlsx_path = directory.join(
            relative_xlsx
                .file_name()
                .ok_or(ArtifactStoreError::InvalidObjectKey)?,
        );
        let html_path = directory.join(
            relative_html
                .file_name()
                .ok_or(ArtifactStoreError::InvalidObjectKey)?,
        );
        let xlsx_created = persist_one(
            &xlsx_path,
            &bundle.xlsx,
            &bundle.artifact.sha256,
            MAX_XLSX_BYTES,
        )?;
        let html_created = persist_one(&html_path, html_bytes, html_sha256, MAX_HTML_BYTES)?;
        let disposition = match (xlsx_created, html_created) {
            (true, true) => PersistDisposition::Created,
            (false, false) => PersistDisposition::Reused,
            _ => PersistDisposition::Recovered,
        };
        Ok(PersistedArtifactReceipt {
            artifact: bundle.artifact.clone(),
            html_object_key,
            xlsx_size_bytes: bundle.xlsx.len() as u64,
            html_size_bytes: html_bytes.len() as u64,
            disposition,
        })
    }

    /// Loads and verifies a stored report before a future delivery provider
    /// receives it. The database identity authenticates the XLSX bytes; the
    /// deterministic HTML sibling remains bounded and valid UTF-8.
    pub fn load(
        &self,
        artifact: &ArtifactIdentity,
    ) -> Result<StoredReportBundle, ArtifactStoreError> {
        let relative_xlsx = validate_object_key(&artifact.object_key)?;
        let relative_html = relative_xlsx.with_extension("html");
        let xlsx_path = self.resolve_existing(&relative_xlsx)?;
        let html_path = self.resolve_existing(&relative_html)?;
        let xlsx = read_bounded(&xlsx_path, MAX_XLSX_BYTES)?;
        if sha256(&xlsx) != artifact.sha256 {
            return Err(ArtifactStoreError::Integrity);
        }
        let html_bytes = read_bounded(&html_path, MAX_HTML_BYTES)?;
        if sha256(&html_bytes) != artifact.html_sha256 {
            return Err(ArtifactStoreError::Integrity);
        }
        let html = String::from_utf8(html_bytes).map_err(|_| ArtifactStoreError::Integrity)?;
        if html.trim().is_empty() {
            return Err(ArtifactStoreError::Integrity);
        }
        Ok(StoredReportBundle { html, xlsx })
    }

    fn ensure_directory(&self, relative: &Path) -> Result<PathBuf, ArtifactStoreError> {
        let mut current = self.root.clone();
        for segment in relative {
            current.push(segment);
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => return Err(ArtifactStoreError::Unavailable),
                Err(_) => {
                    fs::create_dir(&current).map_err(|_| ArtifactStoreError::Unavailable)?;
                }
            }
        }
        let canonical = current
            .canonicalize()
            .map_err(|_| ArtifactStoreError::Unavailable)?;
        if !canonical.starts_with(&self.root) {
            return Err(ArtifactStoreError::Unavailable);
        }
        Ok(canonical)
    }

    fn resolve_existing(&self, relative: &Path) -> Result<PathBuf, ArtifactStoreError> {
        let path = self.root.join(relative);
        let metadata = fs::symlink_metadata(&path).map_err(|_| ArtifactStoreError::Unavailable)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(ArtifactStoreError::Unavailable);
        }
        let canonical = path
            .canonicalize()
            .map_err(|_| ArtifactStoreError::Unavailable)?;
        if !canonical.starts_with(&self.root) {
            return Err(ArtifactStoreError::Unavailable);
        }
        Ok(canonical)
    }
}

fn validate_object_key(value: &str) -> Result<PathBuf, ArtifactStoreError> {
    if value.is_empty() || value.len() > 512 {
        return Err(ArtifactStoreError::InvalidObjectKey);
    }
    let path = Path::new(value);
    if path.extension().and_then(|value| value.to_str()) != Some("xlsx") {
        return Err(ArtifactStoreError::InvalidObjectKey);
    }
    let mut components = 0usize;
    for component in path.components() {
        let Component::Normal(segment) = component else {
            return Err(ArtifactStoreError::InvalidObjectKey);
        };
        let segment = segment
            .to_str()
            .ok_or(ArtifactStoreError::InvalidObjectKey)?;
        if segment.is_empty()
            || segment.len() > 128
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(ArtifactStoreError::InvalidObjectKey);
        }
        components += 1;
    }
    if !(2..=16).contains(&components) {
        return Err(ArtifactStoreError::InvalidObjectKey);
    }
    Ok(path.to_path_buf())
}

fn path_to_object_key(path: &Path) -> Result<String, ArtifactStoreError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or(ArtifactStoreError::InvalidObjectKey)
}

fn persist_one(
    path: &Path,
    bytes: &[u8],
    expected_sha256: &str,
    maximum: u64,
) -> Result<bool, ArtifactStoreError> {
    if bytes.is_empty() || bytes.len() as u64 > maximum || sha256(bytes) != expected_sha256 {
        return Err(ArtifactStoreError::Integrity);
    }
    if fs::symlink_metadata(path).is_ok() {
        verify_existing(path, expected_sha256, maximum)?;
        return Ok(false);
    }
    let parent = path.parent().ok_or(ArtifactStoreError::InvalidObjectKey)?;
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(ArtifactStoreError::InvalidObjectKey)?;
    let temp_path = parent.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed)
    ));
    let mut temp = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .map_err(|_| ArtifactStoreError::Unavailable)?;
    let result = (|| {
        temp.write_all(bytes)
            .map_err(|_| ArtifactStoreError::Unavailable)?;
        temp.sync_all()
            .map_err(|_| ArtifactStoreError::Unavailable)?;
        link_temp(&temp_path, path, expected_sha256, maximum)
    })();
    drop(temp);
    fs::remove_file(&temp_path).map_err(|_| ArtifactStoreError::Unavailable)?;
    result
}

fn link_temp(
    temp_path: &Path,
    path: &Path,
    expected_sha256: &str,
    maximum: u64,
) -> Result<bool, ArtifactStoreError> {
    match fs::hard_link(temp_path, path) {
        Ok(()) => {
            let parent = path.parent().ok_or(ArtifactStoreError::InvalidObjectKey)?;
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|_| ArtifactStoreError::Unavailable)?;
            Ok(true)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            verify_existing(path, expected_sha256, maximum)?;
            Ok(false)
        }
        Err(_) => Err(ArtifactStoreError::Unavailable),
    }
}

fn verify_existing(
    path: &Path,
    expected_sha256: &str,
    maximum: u64,
) -> Result<(), ArtifactStoreError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| ArtifactStoreError::Unavailable)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > maximum {
        return Err(ArtifactStoreError::Integrity);
    }
    let bytes = read_bounded(path, maximum)?;
    if sha256(&bytes) != expected_sha256 {
        return Err(ArtifactStoreError::Integrity);
    }
    Ok(())
}

fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>, ArtifactStoreError> {
    let metadata = fs::metadata(path).map_err(|_| ArtifactStoreError::Unavailable)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > maximum {
        return Err(ArtifactStoreError::Integrity);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)
        .and_then(|file| file.take(maximum + 1).read_to_end(&mut bytes))
        .map_err(|_| ArtifactStoreError::Unavailable)?;
    validate_loaded_bytes(&bytes, maximum)?;
    Ok(bytes)
}

fn validate_loaded_bytes(bytes: &[u8], maximum: u64) -> Result<(), ArtifactStoreError> {
    if bytes.is_empty() || bytes.len() as u64 > maximum {
        Err(ArtifactStoreError::Integrity)
    } else {
        Ok(())
    }
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use chrono::{NaiveDate, TimeZone, Utc};

    use super::*;
    use crate::reporting::{
        ReportKey, ReportKind,
        bundle::{ReportBundleRequest, render_bundle},
        dataset::{InventoryReportRow, ReportDataset, SourceQualityRow},
        kpi::calculate_kpis,
        snapshot::{SnapshotQuality, SnapshotSource},
    };

    static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

    fn root(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "mcp-ozon-artifact-{label}-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    fn bundle() -> ReportBundle {
        let key = ReportKey {
            local_date: NaiveDate::from_ymd_opt(2026, 8, 18).unwrap(),
            kind: ReportKind::Morning,
            recipient_id: "diana".to_owned(),
            report_version: 1,
        };
        let observed_at = Utc.with_ymd_and_hms(2026, 8, 18, 2, 0, 0).unwrap();
        let dataset = ReportDataset {
            kpis: calculate_kpis(&[], &[]).unwrap(),
            sales: vec![],
            advertising: vec![],
            advertising_expenses: vec![],
            finance: vec![],
            inventory: vec![InventoryReportRow {
                account_id: "store".to_owned(),
                sku: "10".to_owned(),
                sellable_stock: 3,
                stock_observed: true,
                price_minor: Some(10_000),
                observed_at,
            }],
            source_quality: vec![SourceQualityRow {
                account_id: "store".to_owned(),
                source: SnapshotSource::Stocks,
                quality: SnapshotQuality::Complete,
                source_as_of: observed_at,
                row_count: 1,
            }],
        };
        render_bundle(ReportBundleRequest {
            key: &key,
            manager_name: "Диана",
            generated_at: Utc.with_ymd_and_hms(2026, 8, 18, 3, 0, 0).unwrap(),
            dataset: &dataset,
            problems: &[],
        })
        .unwrap()
    }

    #[test]
    fn deterministic_bundle_is_created_reused_and_verified() {
        let root = root("roundtrip");
        let store = LocalArtifactStore::open(&root).unwrap();
        let bundle = bundle();
        let first = store.persist(&bundle).unwrap();
        assert_eq!(first.disposition, PersistDisposition::Created);
        assert_eq!(first.artifact, bundle.artifact);
        assert!(first.html_object_key.ends_with("/morning.html"));
        assert_eq!(first.xlsx_size_bytes, bundle.xlsx.len() as u64);
        assert_eq!(first.html_size_bytes, bundle.html.len() as u64);
        assert_eq!(first.artifact.html_sha256, sha256(bundle.html.as_bytes()));
        let second = store.persist(&bundle).unwrap();
        assert_eq!(second.disposition, PersistDisposition::Reused);
        let loaded = store.load(&bundle.artifact).unwrap();
        assert_eq!(loaded.html, bundle.html);
        assert_eq!(loaded.xlsx, bundle.xlsx);
    }

    #[test]
    fn writable_probe_is_ephemeral_and_fails_for_an_unusable_root() {
        let root = root("writable-probe");
        let store = LocalArtifactStore::open(&root).unwrap();
        store.verify_writable().unwrap();
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);

        fs::remove_dir(&root).unwrap();
        assert!(matches!(
            store.verify_writable(),
            Err(ArtifactStoreError::Unavailable)
        ));
    }

    #[test]
    fn interrupted_pair_is_recovered_but_tampering_fails_closed() {
        let root = root("recovery");
        let store = LocalArtifactStore::open(&root).unwrap();
        let bundle = bundle();
        let relative = validate_object_key(&bundle.artifact.object_key).unwrap();
        let directory = store.ensure_directory(relative.parent().unwrap()).unwrap();
        fs::write(directory.join("morning.xlsx"), &bundle.xlsx).unwrap();
        let receipt = store.persist(&bundle).unwrap();
        assert_eq!(receipt.disposition, PersistDisposition::Recovered);
        fs::write(directory.join("morning.xlsx"), b"tampered").unwrap();
        assert!(matches!(
            store.persist(&bundle),
            Err(ArtifactStoreError::Integrity)
        ));
        assert!(matches!(
            store.load(&bundle.artifact),
            Err(ArtifactStoreError::Integrity)
        ));
    }

    #[test]
    fn invalid_roots_keys_and_bundle_metadata_are_rejected() {
        let missing = std::env::temp_dir().join("mcp-ozon-artifact-missing-root");
        assert!(matches!(
            LocalArtifactStore::open(&missing),
            Err(ArtifactStoreError::InvalidRoot)
        ));
        let file = root("file").join("not-a-directory");
        fs::write(&file, b"x").unwrap();
        assert!(matches!(
            LocalArtifactStore::open(&file),
            Err(ArtifactStoreError::InvalidRoot)
        ));
        let invalid_keys = vec![
            String::new(),
            "../report.xlsx".to_owned(),
            "/absolute/report.xlsx".to_owned(),
            "one.xlsx".to_owned(),
            "daily/report.pdf".to_owned(),
            "daily/bad segment/report.xlsx".to_owned(),
            format!("daily/{}/report.xlsx", "x".repeat(129)),
            "x".repeat(513),
        ];
        for key in invalid_keys {
            assert!(validate_object_key(&key).is_err(), "{key}");
        }
        let root = root("invalid-bundle");
        let store = LocalArtifactStore::open(&root).unwrap();
        let mut invalid_bundle = bundle();
        invalid_bundle.artifact.object_key = "../escape.xlsx".to_owned();
        assert!(matches!(
            store.persist(&invalid_bundle),
            Err(ArtifactStoreError::InvalidObjectKey | ArtifactStoreError::Integrity)
        ));
        let mut invalid_bundle = bundle();
        invalid_bundle.html.clear();
        assert!(matches!(
            store.persist(&invalid_bundle),
            Err(ArtifactStoreError::Integrity)
        ));
        let mut invalid_bundle = bundle();
        invalid_bundle.html = "x".repeat(MAX_HTML_BYTES as usize + 1);
        invalid_bundle.artifact.html_sha256 = sha256(invalid_bundle.html.as_bytes());
        assert!(matches!(
            store.persist(&invalid_bundle),
            Err(ArtifactStoreError::Integrity)
        ));
        assert!(matches!(
            store.ensure_directory(Path::new("..")),
            Err(ArtifactStoreError::Unavailable)
        ));
        assert!(matches!(
            store.resolve_existing(Path::new(".")),
            Err(ArtifactStoreError::Unavailable)
        ));
    }

    #[test]
    fn low_level_commit_and_read_guards_cover_races_and_invalid_files() {
        let root = root("low-level");
        let existing = root.join("existing.xlsx");
        let temp = root.join("temp.xlsx");
        fs::write(&existing, b"same").unwrap();
        fs::write(&temp, b"same").unwrap();
        let digest = sha256(b"same");
        assert!(!link_temp(&temp, &existing, &digest, 16).unwrap());
        assert!(matches!(
            link_temp(&temp, &root.join("missing/report.xlsx"), &digest, 16),
            Err(ArtifactStoreError::Unavailable)
        ));
        assert!(matches!(
            persist_one(&root.join("empty.xlsx"), b"", &sha256(b""), 16),
            Err(ArtifactStoreError::Integrity)
        ));
        assert!(matches!(
            persist_one(&root.join("wrong.xlsx"), b"data", &sha256(b"other"), 16),
            Err(ArtifactStoreError::Integrity)
        ));
        assert!(matches!(
            verify_existing(&root, &digest, 16),
            Err(ArtifactStoreError::Integrity)
        ));
        let empty = root.join("zero");
        fs::write(&empty, b"").unwrap();
        assert!(matches!(
            read_bounded(&empty, 16),
            Err(ArtifactStoreError::Integrity)
        ));
        assert!(matches!(
            validate_loaded_bytes(b"too long", 3),
            Err(ArtifactStoreError::Integrity)
        ));
        assert!(validate_loaded_bytes(b"ok", 2).is_ok());
    }

    #[test]
    fn blank_or_non_utf8_stored_html_is_rejected_on_load() {
        let root = root("invalid-html");
        let store = LocalArtifactStore::open(&root).unwrap();
        let bundle = bundle();
        store.persist(&bundle).unwrap();
        let html_path = root
            .join(validate_object_key(&bundle.artifact.object_key).unwrap())
            .with_extension("html");
        fs::write(&html_path, b" ").unwrap();
        assert!(matches!(
            store.load(&bundle.artifact),
            Err(ArtifactStoreError::Integrity)
        ));
        let mut blank_artifact = bundle.artifact.clone();
        blank_artifact.html_sha256 = sha256(b" ");
        assert!(matches!(
            store.load(&blank_artifact),
            Err(ArtifactStoreError::Integrity)
        ));
        fs::write(&html_path, [0xff]).unwrap();
        let mut non_utf8_artifact = bundle.artifact.clone();
        non_utf8_artifact.html_sha256 = sha256(&[0xff]);
        assert!(matches!(
            store.load(&non_utf8_artifact),
            Err(ArtifactStoreError::Integrity)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_links_are_never_followed() {
        use std::os::unix::fs::symlink;

        let artifact_root = root("symlink");
        let outside = root("outside");
        symlink(&outside, artifact_root.join("daily-reports")).unwrap();
        let store = LocalArtifactStore::open(&artifact_root).unwrap();
        assert!(matches!(
            store.persist(&bundle()),
            Err(ArtifactStoreError::Unavailable)
        ));
        let linked_root = root("linked-root");
        let link = linked_root.with_extension("link");
        symlink(&linked_root, &link).unwrap();
        assert!(matches!(
            LocalArtifactStore::open(&link),
            Err(ArtifactStoreError::InvalidRoot)
        ));

        let load_root = root("load-symlink-parent");
        let load_outside = root("load-outside");
        fs::write(load_outside.join("report.xlsx"), b"outside").unwrap();
        symlink(&load_outside, load_root.join("linked")).unwrap();
        let store = LocalArtifactStore::open(&load_root).unwrap();
        assert!(matches!(
            store.resolve_existing(Path::new("linked/report.xlsx")),
            Err(ArtifactStoreError::Unavailable)
        ));
    }
}
