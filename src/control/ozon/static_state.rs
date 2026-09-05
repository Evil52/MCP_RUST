//! Durable local state for the explicitly configured static Ozon guard.
//!
//! Static campaign state is intentionally independent from PostgreSQL, but it
//! must still serialize local state writers and survive a crash between an
//! intended mutation and its readback. Cross-process ownership of the shared
//! marketplace executor identity is handled separately by a database session
//! lease; this module owns the local durability boundary.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read as _, Write as _},
    os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::static_guard::MAX_OZON_STATIC_GUARDS;

/// Maximum serialized state accepted from disk.
pub const MAX_OZON_STATIC_GUARD_STATE_BYTES: u64 = 256 * 1024;

static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// A bid mutation persisted before the marketplace write and cleared only
/// after an exact readback.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OzonStaticPendingBidChange {
    /// Legacy intents did not persist their authorization binding. They load
    /// as `None` so startup can lock them for explicit reconciliation instead
    /// of either trusting them or making the whole state file unrecoverable.
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub sku: Option<u64>,
    #[serde(default)]
    pub min_cpc_bid_microrubles: Option<u64>,
    #[serde(default)]
    pub max_cpc_bid_microrubles: Option<u64>,
    #[serde(default)]
    pub date_from: Option<String>,
    #[serde(default)]
    pub spend_cap_microrubles: Option<u64>,
    #[serde(default)]
    pub target_drr_percent: Option<u8>,
    pub from_microrubles: u64,
    pub to_microrubles: u64,
    pub started_at: DateTime<Utc>,
}

/// A durable static campaign-state mutation boundary.
///
/// Once present, ordinary recovery is readback-only: it may confirm the
/// requested state, but must never repeat the mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OzonStaticCampaignMutationKind {
    Deactivate,
    Activate,
}

/// Fully scoped durable boundary for a static campaign state mutation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OzonStaticPendingCampaignMutation {
    pub account_id: String,
    pub sku: u64,
    pub min_cpc_bid_microrubles: u64,
    pub max_cpc_bid_microrubles: u64,
    pub date_from: String,
    pub spend_cap_microrubles: u64,
    pub target_drr_percent: u8,
    pub kind: OzonStaticCampaignMutationKind,
    #[serde(default)]
    pub stop_reason: Option<String>,
    #[serde(default)]
    pub spend_minor: Option<u64>,
    #[serde(default)]
    pub revenue_minor: Option<u64>,
    pub started_at: DateTime<Utc>,
}

/// Persisted evidence explaining why a campaign remains locked.
///
/// Optional binding fields are used only for legacy `incident_campaign_ids`
/// entries. New incidents always carry the complete reviewed scope.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OzonStaticGuardIncident {
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub sku: Option<u64>,
    #[serde(default)]
    pub min_cpc_bid_microrubles: Option<u64>,
    #[serde(default)]
    pub max_cpc_bid_microrubles: Option<u64>,
    #[serde(default)]
    pub date_from: Option<String>,
    #[serde(default)]
    pub spend_cap_microrubles: Option<u64>,
    #[serde(default)]
    pub target_drr_percent: Option<u8>,
    #[serde(default)]
    pub stop_reason: Option<String>,
    pub error_class: String,
    #[serde(default)]
    pub spend_minor: Option<u64>,
    #[serde(default)]
    pub revenue_minor: Option<u64>,
    pub occurred_at: DateTime<Utc>,
}

/// Crash-recoverable state for static campaign guards.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OzonStaticGuardState {
    /// Highest append-only PostgreSQL authorization event incorporated into
    /// this exact local state snapshot. A missing or older local volume must
    /// never silently re-arm mutations after the database has observed one.
    #[serde(default)]
    pub last_static_audit_event_id: Option<u64>,
    #[serde(default)]
    pub incident_campaign_ids: BTreeSet<u64>,
    #[serde(default)]
    pub incidents: BTreeMap<u64, OzonStaticGuardIncident>,
    #[serde(default)]
    pub last_bid_change_at: BTreeMap<u64, DateTime<Utc>>,
    #[serde(default)]
    pub pending_bid_changes: BTreeMap<u64, OzonStaticPendingBidChange>,
    #[serde(default)]
    pub pending_campaign_mutations: BTreeMap<u64, OzonStaticPendingCampaignMutation>,
}

/// Static state persistence or ownership failure.
#[derive(Debug, Error)]
pub enum OzonStaticGuardStateError {
    #[error("static Ozon guard state path must have a dedicated parent directory")]
    InvalidPath,
    #[error("static Ozon guard state directory is not private and regular")]
    UnsafeDirectory,
    #[error("static Ozon guard state file is not private and regular")]
    UnsafeFile,
    #[error("static Ozon guard state exceeds its size bound")]
    TooLarge,
    #[error("static Ozon guard state is invalid")]
    InvalidState,
    #[error("another static Ozon guard owns the state lease")]
    LeaseBusy,
    #[error("static Ozon guard state I/O failed")]
    Io(#[source] std::io::Error),
    #[error("static Ozon guard state serialization failed")]
    Serialization(#[source] serde_json::Error),
}

/// An exclusive process lease for one static state file.
///
/// The lock is attached to the open file description, so a crash releases it
/// automatically while the private lease inode remains available for health
/// inspection. This avoids both stale create-only locks and unlink races.
#[derive(Debug)]
pub struct OzonStaticGuardStateLease {
    _file: File,
}

impl OzonStaticGuardStateLease {
    /// Acquires an exclusive lease next to `state_path`.
    pub fn acquire(state_path: &Path) -> Result<Self, OzonStaticGuardStateError> {
        let parent = ensure_private_parent(state_path)?;
        let file_name = state_path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .ok_or(OzonStaticGuardStateError::InvalidPath)?;
        let lease_path = parent.join(format!(".{file_name}.lease"));
        match fs::symlink_metadata(&lease_path) {
            Ok(metadata) => validate_private_file(&metadata)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(OzonStaticGuardStateError::Io(error)),
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // Truncation must happen only after the advisory lock is ours;
            // otherwise a losing process could corrupt the owner's heartbeat.
            .truncate(false)
            .mode(0o600)
            .open(&lease_path)
            .map_err(OzonStaticGuardStateError::Io)?;
        validate_private_file(&file.metadata().map_err(OzonStaticGuardStateError::Io)?)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(OzonStaticGuardStateError::LeaseBusy);
            }
            Err(fs::TryLockError::Error(error)) => {
                return Err(OzonStaticGuardStateError::Io(error));
            }
        }
        file.set_len(0).map_err(OzonStaticGuardStateError::Io)?;
        writeln!(file, "pid={}", std::process::id()).map_err(OzonStaticGuardStateError::Io)?;
        file.sync_all().map_err(OzonStaticGuardStateError::Io)?;
        sync_directory(parent)?;
        Ok(Self { _file: file })
    }
}

/// Loads a bounded, private state file.
///
/// A missing file becomes an uninitialized empty state. Runtime serve and
/// health paths reject it until the explicit PostgreSQL genesis command.
pub fn load_ozon_static_guard_state(
    path: &Path,
) -> Result<OzonStaticGuardState, OzonStaticGuardStateError> {
    let _ = ensure_private_parent(path)?;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(OzonStaticGuardState::default());
        }
        Err(error) => return Err(OzonStaticGuardStateError::Io(error)),
    };
    validate_private_file(&metadata)?;
    if metadata.len() > MAX_OZON_STATIC_GUARD_STATE_BYTES {
        return Err(OzonStaticGuardStateError::TooLarge);
    }
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(OzonStaticGuardStateError::Io)?
        .take(MAX_OZON_STATIC_GUARD_STATE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(OzonStaticGuardStateError::Io)?;
    if bytes.len() as u64 > MAX_OZON_STATIC_GUARD_STATE_BYTES {
        return Err(OzonStaticGuardStateError::TooLarge);
    }
    let mut state: OzonStaticGuardState =
        serde_json::from_slice(&bytes).map_err(OzonStaticGuardStateError::Serialization)?;
    normalize_legacy_incidents(&mut state);
    validate_state(&state)?;
    Ok(state)
}

/// Atomically persists state through a private unique temporary file and
/// synchronizes both the file and its directory.
pub fn persist_ozon_static_guard_state(
    path: &Path,
    state: &OzonStaticGuardState,
) -> Result<(), OzonStaticGuardStateError> {
    validate_state(state)?;
    let parent = ensure_private_parent(path)?;
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_private_file(&metadata)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(OzonStaticGuardStateError::Io(error)),
    }
    let bytes = serde_json::to_vec(state).map_err(OzonStaticGuardStateError::Serialization)?;
    if bytes.len() as u64 > MAX_OZON_STATIC_GUARD_STATE_BYTES {
        return Err(OzonStaticGuardStateError::TooLarge);
    }
    let (temporary_path, mut temporary) = create_unique_temporary(parent, path)?;
    let result = (|| {
        temporary
            .write_all(&bytes)
            .map_err(OzonStaticGuardStateError::Io)?;
        temporary
            .sync_all()
            .map_err(OzonStaticGuardStateError::Io)?;
        fs::rename(&temporary_path, path).map_err(OzonStaticGuardStateError::Io)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary_path);
    }
    result
}

/// Verifies that durable entries cannot outlive or expand the reviewed static
/// campaign scope.
pub fn validate_ozon_static_guard_state_scope(
    state: &OzonStaticGuardState,
    allowed_campaign_ids: &BTreeSet<u64>,
) -> Result<(), OzonStaticGuardStateError> {
    validate_state(state)?;
    if state
        .incident_campaign_ids
        .iter()
        .chain(state.incidents.keys())
        .chain(state.last_bid_change_at.keys())
        .chain(state.pending_bid_changes.keys())
        .chain(state.pending_campaign_mutations.keys())
        .any(|campaign_id| !allowed_campaign_ids.contains(campaign_id))
    {
        return Err(OzonStaticGuardStateError::InvalidState);
    }
    Ok(())
}

fn validate_state(state: &OzonStaticGuardState) -> Result<(), OzonStaticGuardStateError> {
    if state.last_static_audit_event_id == Some(0) {
        return Err(OzonStaticGuardStateError::InvalidState);
    }
    let mut campaign_ids = state.incident_campaign_ids.clone();
    campaign_ids.extend(state.incidents.keys().copied());
    campaign_ids.extend(state.last_bid_change_at.keys().copied());
    campaign_ids.extend(state.pending_bid_changes.keys().copied());
    campaign_ids.extend(state.pending_campaign_mutations.keys().copied());
    if campaign_ids.len() > MAX_OZON_STATIC_GUARDS
        || campaign_ids.contains(&0)
        || state
            .incidents
            .keys()
            .any(|campaign_id| !state.incident_campaign_ids.contains(campaign_id))
        || state
            .incidents
            .values()
            .any(|incident| !valid_incident(incident))
        || state.pending_bid_changes.values().any(|pending| {
            let binding_is_legacy = pending.account_id.is_none()
                && pending.sku.is_none()
                && pending.min_cpc_bid_microrubles.is_none()
                && pending.max_cpc_bid_microrubles.is_none()
                && pending.date_from.is_none()
                && pending.spend_cap_microrubles.is_none()
                && pending.target_drr_percent.is_none();
            let binding_is_valid = match (
                pending.account_id.as_deref(),
                pending.sku,
                pending.min_cpc_bid_microrubles,
                pending.max_cpc_bid_microrubles,
                pending.date_from.as_deref(),
                pending.spend_cap_microrubles,
                pending.target_drr_percent,
            ) {
                (
                    Some(account_id),
                    Some(sku),
                    Some(min_bid),
                    Some(max_bid),
                    Some(date_from),
                    Some(spend_cap),
                    Some(target_drr),
                ) => {
                    valid_complete_binding(
                        account_id, sku, min_bid, max_bid, date_from, spend_cap, target_drr,
                    ) && (min_bid..=max_bid).contains(&pending.from_microrubles)
                        && (min_bid..=max_bid).contains(&pending.to_microrubles)
                }
                _ => false,
            };
            (!binding_is_legacy && !binding_is_valid)
                || pending.from_microrubles == 0
                || pending.to_microrubles == 0
                || pending.from_microrubles == pending.to_microrubles
                || !pending.from_microrubles.is_multiple_of(1_000_000)
                || !pending.to_microrubles.is_multiple_of(1_000_000)
        })
        || state
            .pending_campaign_mutations
            .iter()
            .any(|(campaign_id, pending)| {
                state.pending_bid_changes.contains_key(campaign_id)
                    || !valid_pending_campaign_mutation(pending)
            })
    {
        return Err(OzonStaticGuardStateError::InvalidState);
    }
    Ok(())
}

fn normalize_legacy_incidents(state: &mut OzonStaticGuardState) {
    for campaign_id in &state.incident_campaign_ids {
        state
            .incidents
            .entry(*campaign_id)
            .or_insert_with(|| OzonStaticGuardIncident {
                account_id: None,
                sku: None,
                min_cpc_bid_microrubles: None,
                max_cpc_bid_microrubles: None,
                date_from: None,
                spend_cap_microrubles: None,
                target_drr_percent: None,
                stop_reason: None,
                error_class: "legacy_incident_unclassified".to_owned(),
                spend_minor: None,
                revenue_minor: None,
                occurred_at: DateTime::UNIX_EPOCH,
            });
    }
}

fn valid_pending_campaign_mutation(pending: &OzonStaticPendingCampaignMutation) -> bool {
    valid_complete_binding(
        &pending.account_id,
        pending.sku,
        pending.min_cpc_bid_microrubles,
        pending.max_cpc_bid_microrubles,
        &pending.date_from,
        pending.spend_cap_microrubles,
        pending.target_drr_percent,
    ) && valid_evidence_pair(pending.spend_minor, pending.revenue_minor)
        && match pending.kind {
            OzonStaticCampaignMutationKind::Deactivate => pending
                .stop_reason
                .as_deref()
                .is_some_and(valid_evidence_label),
            OzonStaticCampaignMutationKind::Activate => {
                pending.stop_reason.is_none()
                    && pending.spend_minor.is_none()
                    && pending.revenue_minor.is_none()
            }
        }
}

fn valid_incident(incident: &OzonStaticGuardIncident) -> bool {
    let legacy_binding = incident.account_id.is_none()
        && incident.sku.is_none()
        && incident.min_cpc_bid_microrubles.is_none()
        && incident.max_cpc_bid_microrubles.is_none()
        && incident.date_from.is_none()
        && incident.spend_cap_microrubles.is_none()
        && incident.target_drr_percent.is_none();
    let complete_binding = match (
        incident.account_id.as_deref(),
        incident.sku,
        incident.min_cpc_bid_microrubles,
        incident.max_cpc_bid_microrubles,
        incident.date_from.as_deref(),
        incident.spend_cap_microrubles,
        incident.target_drr_percent,
    ) {
        (
            Some(account_id),
            Some(sku),
            Some(min_bid),
            Some(max_bid),
            Some(date_from),
            Some(spend_cap),
            Some(target_drr),
        ) => valid_complete_binding(
            account_id, sku, min_bid, max_bid, date_from, spend_cap, target_drr,
        ),
        _ => false,
    };
    (legacy_binding || complete_binding)
        && valid_evidence_label(&incident.error_class)
        && incident
            .stop_reason
            .as_deref()
            .is_none_or(valid_evidence_label)
        && valid_evidence_pair(incident.spend_minor, incident.revenue_minor)
}

fn valid_complete_binding(
    account_id: &str,
    sku: u64,
    min_bid: u64,
    max_bid: u64,
    date_from: &str,
    spend_cap: u64,
    target_drr: u8,
) -> bool {
    !account_id.is_empty()
        && account_id.trim() == account_id
        && account_id.len() <= 128
        && !account_id.chars().any(char::is_control)
        && sku != 0
        && min_bid != 0
        && min_bid <= max_bid
        && min_bid.is_multiple_of(1_000_000)
        && max_bid.is_multiple_of(1_000_000)
        && chrono::NaiveDate::parse_from_str(date_from, "%Y-%m-%d").is_ok()
        && spend_cap != 0
        && spend_cap.is_multiple_of(10_000)
        && (10..=100).contains(&target_drr)
}

const fn valid_evidence_pair(spend_minor: Option<u64>, revenue_minor: Option<u64>) -> bool {
    spend_minor.is_some() == revenue_minor.is_some()
}

fn valid_evidence_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 64
        && label
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn state_parent(path: &Path) -> Result<&Path, OzonStaticGuardStateError> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or(OzonStaticGuardStateError::InvalidPath)
}

fn ensure_private_parent(path: &Path) -> Result<&Path, OzonStaticGuardStateError> {
    let parent = state_parent(path)?;
    match fs::symlink_metadata(parent) {
        Ok(metadata) => validate_private_directory(&metadata)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(parent).map_err(OzonStaticGuardStateError::Io)?;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
                .map_err(OzonStaticGuardStateError::Io)?;
            validate_private_directory(
                &fs::symlink_metadata(parent).map_err(OzonStaticGuardStateError::Io)?,
            )?;
        }
        Err(error) => return Err(OzonStaticGuardStateError::Io(error)),
    }
    Ok(parent)
}

fn validate_private_directory(metadata: &fs::Metadata) -> Result<(), OzonStaticGuardStateError> {
    if !metadata.file_type().is_dir()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(OzonStaticGuardStateError::UnsafeDirectory);
    }
    Ok(())
}

fn validate_private_file(metadata: &fs::Metadata) -> Result<(), OzonStaticGuardStateError> {
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(OzonStaticGuardStateError::UnsafeFile);
    }
    Ok(())
}

fn create_unique_temporary(
    parent: &Path,
    state_path: &Path,
) -> Result<(PathBuf, File), OzonStaticGuardStateError> {
    let file_name = state_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(OzonStaticGuardStateError::InvalidPath)?;
    for _ in 0..16 {
        let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary_path = parent.join(format!(
            ".{file_name}.{}.{}.tmp",
            std::process::id(),
            sequence
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary_path)
        {
            Ok(file) => return Ok((temporary_path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(OzonStaticGuardStateError::Io(error)),
        }
    }
    Err(OzonStaticGuardStateError::Io(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate a unique state temporary file",
    )))
}

fn sync_directory(path: &Path) -> Result<(), OzonStaticGuardStateError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(OzonStaticGuardStateError::Io)
}

#[cfg(test)]
mod tests {
    use std::{os::unix::fs::symlink, sync::atomic::AtomicU64};

    use super::*;

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "mcp-ozon-static-state-{}-{}",
                std::process::id(),
                TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
            Self(path)
        }

        fn state(&self) -> PathBuf {
            self.0.join("state.json")
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn populated_state() -> OzonStaticGuardState {
        OzonStaticGuardState {
            last_static_audit_event_id: Some(7),
            incident_campaign_ids: BTreeSet::from([11]),
            incidents: BTreeMap::from([(
                11,
                OzonStaticGuardIncident {
                    account_id: Some("account".to_owned()),
                    sku: Some(111),
                    min_cpc_bid_microrubles: Some(7_000_000),
                    max_cpc_bid_microrubles: Some(10_000_000),
                    date_from: Some("2026-09-01".to_owned()),
                    spend_cap_microrubles: Some(2_000_000_000),
                    target_drr_percent: Some(15),
                    stop_reason: Some("telemetry_unavailable".to_owned()),
                    error_class: "readback_unavailable".to_owned(),
                    spend_minor: None,
                    revenue_minor: None,
                    occurred_at: DateTime::UNIX_EPOCH,
                },
            )]),
            last_bid_change_at: BTreeMap::from([(12, DateTime::UNIX_EPOCH)]),
            pending_bid_changes: BTreeMap::from([(
                13,
                OzonStaticPendingBidChange {
                    account_id: Some("account".to_owned()),
                    sku: Some(113),
                    min_cpc_bid_microrubles: Some(7_000_000),
                    max_cpc_bid_microrubles: Some(10_000_000),
                    date_from: Some("2026-09-01".to_owned()),
                    spend_cap_microrubles: Some(2_000_000_000),
                    target_drr_percent: Some(15),
                    from_microrubles: 7_000_000,
                    to_microrubles: 8_000_000,
                    started_at: DateTime::UNIX_EPOCH,
                },
            )]),
            pending_campaign_mutations: BTreeMap::new(),
        }
    }

    #[test]
    fn state_round_trips_atomically_with_private_permissions() {
        let directory = TestDirectory::new();
        let path = directory.state();
        assert_eq!(
            load_ozon_static_guard_state(&path).unwrap(),
            OzonStaticGuardState::default()
        );
        let state = populated_state();
        persist_ozon_static_guard_state(&path, &state).unwrap();
        assert_eq!(load_ozon_static_guard_state(&path).unwrap(), state);
        assert_eq!(fs::metadata(path).unwrap().permissions().mode() & 0o077, 0);
    }

    #[test]
    fn legacy_incident_ids_load_as_explicit_fail_closed_evidence() {
        let directory = TestDirectory::new();
        let path = directory.state();
        fs::write(&path, br#"{"incident_campaign_ids":[42]}"#).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

        let state = load_ozon_static_guard_state(&path).unwrap();
        let incident = state.incidents.get(&42).unwrap();
        assert_eq!(incident.error_class, "legacy_incident_unclassified");
        assert_eq!(incident.account_id, None);
        assert_eq!(incident.occurred_at, DateTime::UNIX_EPOCH);
        assert!(state.incident_campaign_ids.contains(&42));
    }

    #[test]
    fn exclusive_lease_survives_until_the_owner_drops_it() {
        let directory = TestDirectory::new();
        let path = directory.state();
        let lease = OzonStaticGuardStateLease::acquire(&path).unwrap();
        assert!(matches!(
            OzonStaticGuardStateLease::acquire(&path),
            Err(OzonStaticGuardStateError::LeaseBusy)
        ));
        drop(lease);
        assert!(directory.0.join(".state.json.lease").is_file());
        drop(OzonStaticGuardStateLease::acquire(&path).unwrap());
    }

    #[test]
    fn state_rejects_invalid_paths_types_permissions_sizes_and_payloads() {
        assert!(matches!(
            OzonStaticGuardStateLease::acquire(Path::new("state.json")),
            Err(OzonStaticGuardStateError::InvalidPath)
        ));

        let broad = TestDirectory::new();
        fs::set_permissions(&broad.0, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(
            OzonStaticGuardStateLease::acquire(&broad.state()),
            Err(OzonStaticGuardStateError::UnsafeDirectory)
        ));

        let directory = TestDirectory::new();
        let path = directory.state();
        fs::write(&path, b"{}").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            load_ozon_static_guard_state(&path),
            Err(OzonStaticGuardStateError::UnsafeFile)
        ));
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(
            &path,
            vec![
                b' ';
                usize::try_from(MAX_OZON_STATIC_GUARD_STATE_BYTES)
                    .expect("state size bound fits usize")
                    + 1
            ],
        )
        .unwrap();
        assert!(matches!(
            load_ozon_static_guard_state(&path),
            Err(OzonStaticGuardStateError::TooLarge)
        ));
        fs::write(&path, b"not-json").unwrap();
        assert!(matches!(
            load_ozon_static_guard_state(&path),
            Err(OzonStaticGuardStateError::Serialization(_))
        ));

        fs::remove_file(&path).unwrap();
        symlink(&directory.0, &path).unwrap();
        assert!(matches!(
            load_ozon_static_guard_state(&path),
            Err(OzonStaticGuardStateError::UnsafeFile)
        ));

        let lease_directory = TestDirectory::new();
        let lease_target = lease_directory.0.join("lease-target");
        fs::write(&lease_target, b"pid=1\n").unwrap();
        fs::set_permissions(&lease_target, fs::Permissions::from_mode(0o600)).unwrap();
        symlink(&lease_target, lease_directory.0.join(".state.json.lease")).unwrap();
        assert!(matches!(
            OzonStaticGuardStateLease::acquire(&lease_directory.state()),
            Err(OzonStaticGuardStateError::UnsafeFile)
        ));

        let parent_root = TestDirectory::new();
        let real_parent = parent_root.0.join("real-parent");
        fs::create_dir(&real_parent).unwrap();
        fs::set_permissions(&real_parent, fs::Permissions::from_mode(0o700)).unwrap();
        let linked_parent = parent_root.0.join("linked-parent");
        symlink(&real_parent, &linked_parent).unwrap();
        assert!(matches!(
            OzonStaticGuardStateLease::acquire(&linked_parent.join("state.json")),
            Err(OzonStaticGuardStateError::UnsafeDirectory)
        ));
    }

    #[test]
    fn invalid_or_unbounded_state_never_reaches_disk() {
        let directory = TestDirectory::new();
        let path = directory.state();
        let mut invalid = populated_state();
        invalid.last_static_audit_event_id = Some(0);
        assert!(matches!(
            persist_ozon_static_guard_state(&path, &invalid),
            Err(OzonStaticGuardStateError::InvalidState)
        ));

        let mut invalid = populated_state();
        invalid.incident_campaign_ids.insert(0);
        assert!(matches!(
            persist_ozon_static_guard_state(&path, &invalid),
            Err(OzonStaticGuardStateError::InvalidState)
        ));

        let mut too_many = OzonStaticGuardState::default();
        too_many
            .incident_campaign_ids
            .extend(1..=u64::try_from(MAX_OZON_STATIC_GUARDS + 1).unwrap());
        assert!(matches!(
            persist_ozon_static_guard_state(&path, &too_many),
            Err(OzonStaticGuardStateError::InvalidState)
        ));

        for pending in [
            (0, 8_000_000),
            (7_000_000, 0),
            (7_000_000, 7_000_000),
            (7_000_001, 8_000_000),
            (7_000_000, 8_000_001),
        ] {
            let mut state = OzonStaticGuardState::default();
            state.pending_bid_changes.insert(
                1,
                OzonStaticPendingBidChange {
                    account_id: Some("account".to_owned()),
                    sku: Some(101),
                    min_cpc_bid_microrubles: Some(7_000_000),
                    max_cpc_bid_microrubles: Some(10_000_000),
                    date_from: Some("2026-09-01".to_owned()),
                    spend_cap_microrubles: Some(2_000_000_000),
                    target_drr_percent: Some(15),
                    from_microrubles: pending.0,
                    to_microrubles: pending.1,
                    started_at: DateTime::UNIX_EPOCH,
                },
            );
            assert!(matches!(
                persist_ozon_static_guard_state(&path, &state),
                Err(OzonStaticGuardStateError::InvalidState)
            ));
        }

        let mut partially_bound = populated_state();
        partially_bound
            .pending_bid_changes
            .get_mut(&13)
            .unwrap()
            .sku = None;
        assert!(matches!(
            persist_ozon_static_guard_state(&path, &partially_bound),
            Err(OzonStaticGuardStateError::InvalidState)
        ));

        let valid_mutation = OzonStaticPendingCampaignMutation {
            account_id: "account".to_owned(),
            sku: 101,
            min_cpc_bid_microrubles: 7_000_000,
            max_cpc_bid_microrubles: 10_000_000,
            date_from: "2026-09-01".to_owned(),
            spend_cap_microrubles: 2_000_000_000,
            target_drr_percent: 15,
            kind: OzonStaticCampaignMutationKind::Deactivate,
            stop_reason: Some("telemetry_unavailable".to_owned()),
            spend_minor: None,
            revenue_minor: None,
            started_at: DateTime::UNIX_EPOCH,
        };
        let mut mismatched_evidence = OzonStaticGuardState::default();
        mismatched_evidence.pending_campaign_mutations.insert(
            1,
            OzonStaticPendingCampaignMutation {
                spend_minor: Some(1),
                ..valid_mutation.clone()
            },
        );
        assert!(matches!(
            persist_ozon_static_guard_state(&path, &mismatched_evidence),
            Err(OzonStaticGuardStateError::InvalidState)
        ));

        let mut invalid_activation = OzonStaticGuardState::default();
        invalid_activation.pending_campaign_mutations.insert(
            1,
            OzonStaticPendingCampaignMutation {
                kind: OzonStaticCampaignMutationKind::Activate,
                ..valid_mutation.clone()
            },
        );
        assert!(matches!(
            persist_ozon_static_guard_state(&path, &invalid_activation),
            Err(OzonStaticGuardStateError::InvalidState)
        ));

        let mut overlapping_mutations = OzonStaticGuardState::default();
        overlapping_mutations
            .pending_campaign_mutations
            .insert(1, valid_mutation);
        overlapping_mutations.pending_bid_changes.insert(
            1,
            OzonStaticPendingBidChange {
                account_id: Some("account".to_owned()),
                sku: Some(101),
                min_cpc_bid_microrubles: Some(7_000_000),
                max_cpc_bid_microrubles: Some(10_000_000),
                date_from: Some("2026-09-01".to_owned()),
                spend_cap_microrubles: Some(2_000_000_000),
                target_drr_percent: Some(15),
                from_microrubles: 7_000_000,
                to_microrubles: 8_000_000,
                started_at: DateTime::UNIX_EPOCH,
            },
        );
        assert!(matches!(
            persist_ozon_static_guard_state(&path, &overlapping_mutations),
            Err(OzonStaticGuardStateError::InvalidState)
        ));

        let mut orphan_incident = populated_state();
        orphan_incident.incident_campaign_ids.remove(&11);
        assert!(matches!(
            persist_ozon_static_guard_state(&path, &orphan_incident),
            Err(OzonStaticGuardStateError::InvalidState)
        ));
    }

    #[test]
    fn legacy_unbound_pending_intent_loads_for_fail_closed_reconciliation() {
        let directory = TestDirectory::new();
        let path = directory.state();
        fs::write(
            &path,
            br#"{"pending_bid_changes":{"13":{"from_microrubles":7000000,"to_microrubles":8000000,"started_at":"1970-01-01T00:00:00Z"}}}"#,
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

        let state = load_ozon_static_guard_state(&path).unwrap();
        let pending = state.pending_bid_changes.get(&13).unwrap();
        assert_eq!(pending.account_id, None);
        assert_eq!(pending.sku, None);
        assert_eq!(pending.min_cpc_bid_microrubles, None);
        assert_eq!(pending.max_cpc_bid_microrubles, None);
    }

    #[test]
    fn durable_state_cannot_expand_the_reviewed_campaign_scope() {
        let state = populated_state();
        assert!(
            validate_ozon_static_guard_state_scope(&state, &BTreeSet::from([11, 12, 13])).is_ok()
        );
        assert!(matches!(
            validate_ozon_static_guard_state_scope(&state, &BTreeSet::from([11, 12])),
            Err(OzonStaticGuardStateError::InvalidState)
        ));
    }

    #[test]
    fn missing_private_parent_is_created_and_non_files_are_rejected() {
        let root = TestDirectory::new();
        let nested = root.0.join("nested");
        let state_path = nested.join("state.json");
        persist_ozon_static_guard_state(&state_path, &OzonStaticGuardState::default()).unwrap();
        assert_eq!(
            fs::metadata(&nested).unwrap().permissions().mode() & 0o077,
            0
        );

        let directory_path = root.0.join("directory-state");
        fs::create_dir(&directory_path).unwrap();
        assert!(matches!(
            persist_ozon_static_guard_state(&directory_path, &OzonStaticGuardState::default()),
            Err(OzonStaticGuardStateError::UnsafeFile)
        ));
    }
}
