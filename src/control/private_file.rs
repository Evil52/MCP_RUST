//! Crash-safe owner-only state files: atomic replacement and process leases.

use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, ErrorKind},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};

static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const TEMPORARY_ATTEMPTS: usize = 16;

/// Atomically replaces `directory/file_name` with `bytes`, written by `write`
/// into a fresh owner-only temporary, and then syncs the directory.
///
/// A crash between creating the temporary and renaming it leaves that file
/// behind. Temporary names therefore combine the PID with a wall-clock stamp
/// and a process-wide sequence, and an occupied name is skipped: a leftover
/// never blocks a later write, even from a restarted container whose process
/// is PID 1 again. Every failure after creation removes this call's own
/// temporary, so failed publications do not accumulate files either.
pub(super) fn replace_private_file(
    directory: &Path,
    file_name: &str,
    bytes: &[u8],
    write: fn(&mut File, &[u8]) -> Result<()>,
    label: &str,
) -> Result<()> {
    let (temporary, mut file) = create_temporary(directory, file_name)
        .with_context(|| format!("{label}: временный файл недоступен"))?;
    let published = write(&mut file, bytes).and_then(|()| {
        fs::rename(&temporary, directory.join(file_name))
            .with_context(|| format!("{label} нельзя опубликовать"))
    });
    if let Err(error) = published {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("{label}: каталог нельзя синхронизировать"))
}

/// Takes the exclusive advisory lock on the owner-only lease file
/// `directory/file_name`, creating it on first use. `None` means another
/// live process holds the lease.
///
/// The lock belongs to the open file description, so a crashed holder
/// releases it automatically: unlike a create-only lock file, no leftover
/// can wedge a later run. Dropping the returned file releases the lease.
pub(super) fn try_acquire_private_lease(
    directory: &Path,
    file_name: &str,
    label: &str,
) -> Result<Option<File>> {
    let path = directory.join(file_name);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => ensure!(metadata.is_file(), "{label}: lease небезопасен"),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error).with_context(|| format!("{label}: lease недоступен")),
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .with_context(|| format!("{label}: lease недоступен"))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("{label}: lease недоступен"))?;
    ensure!(
        metadata.is_file() && metadata.permissions().mode().is_multiple_of(0o100),
        "{label}: lease небезопасен"
    );
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(error)) => {
            Err(error).with_context(|| format!("{label}: lease нельзя заблокировать"))
        }
    }
}

fn create_temporary(directory: &Path, file_name: &str) -> io::Result<(PathBuf, File)> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for _ in 0..TEMPORARY_ATTEMPTS {
        let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = directory.join(format!(
            ".{file_name}.{}.{stamp}.{sequence}.tmp",
            std::process::id()
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
        {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        ErrorKind::AlreadyExists,
        "no unique temporary file name is available",
    ))
}

#[cfg(test)]
mod tests {
    use std::{io::Write as _, os::unix::fs::symlink};

    use super::*;

    static DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "mcp-ozon-private-file-{}-{}",
                std::process::id(),
                DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
            Self(path)
        }

        fn names(&self) -> Vec<String> {
            let mut names = fs::read_dir(&self.0)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .collect::<Vec<_>>();
            names.sort();
            names
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_all(file: &mut File, bytes: &[u8]) -> Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(())
    }

    #[test]
    fn leftovers_of_a_crashed_run_never_block_the_next_publication() {
        let directory = TestDirectory::new();
        // A run killed between creating its temporary and renaming it, with
        // the same PID as the next run: PID 1 in a restarted container. The
        // former `.name-{pid}.tmp` scheme made every later write fail here.
        let (crashed, mut partial) = create_temporary(&directory.0, "state.json").unwrap();
        partial.write_all(b"partial").unwrap();
        drop(partial);

        for bytes in [&b"first"[..], b"second"] {
            replace_private_file(&directory.0, "state.json", bytes, write_all, "test state")
                .unwrap();
        }

        let published = directory.0.join("state.json");
        assert_eq!(fs::read(&published).unwrap(), b"second");
        assert_eq!(
            fs::metadata(&published).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let crashed = crashed.file_name().unwrap().to_str().unwrap().to_owned();
        assert_eq!(
            directory.names(),
            [crashed, "state.json".to_owned()],
            "only the foreign leftover remains beside the published file"
        );
    }

    #[test]
    fn temporary_names_are_unique_and_owner_only() {
        let directory = TestDirectory::new();
        let (first, _) = create_temporary(&directory.0, "state.json").unwrap();
        let (second, _) = create_temporary(&directory.0, "state.json").unwrap();
        assert_ne!(first, second);
        for path in [first, second] {
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn failed_write_or_publication_removes_its_own_temporary() {
        let directory = TestDirectory::new();
        fs::write(directory.0.join("state.json"), b"previous").unwrap();
        let error = replace_private_file(
            &directory.0,
            "state.json",
            b"next",
            |_, _| Err(anyhow::anyhow!("injected write failure")),
            "test state",
        )
        .unwrap_err();
        assert!(error.to_string().contains("injected write failure"));
        assert_eq!(
            fs::read(directory.0.join("state.json")).unwrap(),
            b"previous"
        );
        assert_eq!(directory.names(), ["state.json"]);

        // A directory at the target makes the rename itself fail.
        fs::create_dir(directory.0.join("blocked.json")).unwrap();
        let error = replace_private_file(
            &directory.0,
            "blocked.json",
            b"next",
            write_all,
            "test state",
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("test state нельзя опубликовать"));
        assert_eq!(directory.names(), ["blocked.json", "state.json"]);
    }

    #[test]
    fn a_lease_is_exclusive_until_its_holder_releases_it() {
        let directory = TestDirectory::new();
        let held = try_acquire_private_lease(&directory.0, ".state.lease", "test state")
            .unwrap()
            .expect("an unowned lease is acquired");
        // A second open file description is what another process would hold.
        assert!(
            try_acquire_private_lease(&directory.0, ".state.lease", "test state")
                .unwrap()
                .is_none()
        );
        let path = directory.0.join(".state.lease");
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );

        drop(held);
        assert!(
            try_acquire_private_lease(&directory.0, ".state.lease", "test state")
                .unwrap()
                .is_some(),
            "a released or crashed holder never wedges the next run"
        );
    }

    #[test]
    fn an_unsafe_lease_file_fails_closed() {
        let directory = TestDirectory::new();
        fs::write(directory.0.join("target"), b"").unwrap();
        symlink(directory.0.join("target"), directory.0.join(".link.lease")).unwrap();
        let error =
            try_acquire_private_lease(&directory.0, ".link.lease", "test state").unwrap_err();
        assert!(error.to_string().contains("test state: lease небезопасен"));

        let shared = directory.0.join(".shared.lease");
        fs::write(&shared, b"").unwrap();
        fs::set_permissions(&shared, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(try_acquire_private_lease(&directory.0, ".shared.lease", "test state").is_err());

        fs::create_dir(directory.0.join(".directory.lease")).unwrap();
        assert!(try_acquire_private_lease(&directory.0, ".directory.lease", "test state").is_err());
        assert!(
            try_acquire_private_lease(&directory.0.join("missing"), ".state.lease", "test state")
                .is_err()
        );
    }

    #[test]
    fn a_missing_directory_is_reported_before_any_write() {
        let directory = TestDirectory::new();
        let missing = directory.0.join("missing");
        let error = replace_private_file(&missing, "state.json", b"next", write_all, "test state")
            .unwrap_err();
        assert!(format!("{error:#}").contains("test state: временный файл недоступен"));
    }
}
