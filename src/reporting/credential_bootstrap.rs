//! Operator-only bootstrap for the scheduled report credential directory.
//!
//! The importer deliberately accepts only a strict `NAME=value` dotenv
//! subset. It never mutates the process environment, performs substitutions,
//! follows symlinks, copies unrelated values, or overwrites an existing
//! destination.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, DirBuilder, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};

use crate::config::{AccessRegistry, Marketplace, RegistrySource};

use super::policy::DailyReportPolicy;

const MAX_INPUT_BYTES: u64 = 1024 * 1024;
const MAX_SECRET_BYTES: usize = 16_384;
const MAX_ENV_ENTRIES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialBootstrapSummary {
    pub account_count: usize,
    pub credential_count: usize,
}

/// Creates a new least-privilege credential directory for an enabled policy.
pub fn bootstrap_report_credentials(
    registry_path: &Path,
    policy_path: &Path,
    dotenv_path: &Path,
    output_path: &Path,
) -> Result<CredentialBootstrapSummary> {
    ensure_regular_input(registry_path, "access registry")?;
    ensure_regular_input(policy_path, "daily report policy")?;
    ensure_regular_input(dotenv_path, "dotenv source")?;

    let registry = RegistrySource::new(registry_path)
        .context("access registry is invalid")?
        .load()
        .context("access registry cannot be loaded")?;
    let policy_bytes = read_bounded(policy_path)?;
    let policy = DailyReportPolicy::from_slice(&policy_bytes, &registry)
        .context("daily report policy is invalid")?;
    ensure!(
        policy.enabled,
        "credential bootstrap requires an enabled daily report policy"
    );
    let (account_count, required_names) = required_credential_names(&registry, &policy)?;
    let dotenv = read_bounded(dotenv_path)?;
    let values = parse_required_values(&dotenv, &required_names)?;
    create_credential_directory(output_path, &values)?;
    Ok(CredentialBootstrapSummary {
        account_count,
        credential_count: values.len(),
    })
}

fn ensure_regular_input(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).with_context(|| format!("{label} is unavailable"))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "{label} must be a regular non-symlink file"
    );
    ensure!(
        metadata.len() <= MAX_INPUT_BYTES,
        "{label} exceeds the 1 MiB limit"
    );
    Ok(())
}

fn read_bounded(path: &Path) -> Result<Vec<u8>> {
    let bytes = fs::read(path).context("bounded configuration file cannot be read")?;
    ensure!(
        bytes.len() as u64 <= MAX_INPUT_BYTES,
        "configuration file exceeds the 1 MiB limit"
    );
    Ok(bytes)
}

fn required_credential_names(
    registry: &AccessRegistry,
    policy: &DailyReportPolicy,
) -> Result<(usize, BTreeSet<String>)> {
    let account_ids = policy
        .audiences
        .iter()
        .flat_map(|audience| &audience.managers)
        .flat_map(|manager| &manager.account_ids)
        .collect::<BTreeSet<_>>();
    let mut names = BTreeSet::new();
    for account_id in &account_ids {
        let account = registry
            .accounts
            .iter()
            .find(|account| account.id == ***account_id)
            .context("policy account disappeared from the access registry")?;
        match account.marketplace {
            Marketplace::Ozon => {
                let binding = account
                    .ozon
                    .as_ref()
                    .context("policy Ozon account has no credential binding")?;
                names.insert(binding.client_id_env.clone());
                names.insert(binding.api_key_env.clone());
                let performance = binding
                    .performance
                    .as_ref()
                    .context("policy Ozon account has no Performance credential binding")?;
                names.insert(performance.client_id_env.clone());
                names.insert(performance.client_secret_env.clone());
            }
            Marketplace::Wildberries => {
                let binding = account
                    .wildberries
                    .as_ref()
                    .context("policy WB account has no credential binding")?;
                names.insert(binding.api_token_env.clone());
            }
        }
    }
    Ok((account_ids.len(), names))
}

fn parse_required_values(
    bytes: &[u8],
    required_names: &BTreeSet<String>,
) -> Result<BTreeMap<String, String>> {
    let text = std::str::from_utf8(bytes).context("dotenv source must be UTF-8")?;
    let mut seen = BTreeSet::new();
    let mut values = BTreeMap::new();
    for raw_line in text.lines() {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, value) = line
            .split_once('=')
            .context("dotenv source must use strict NAME=value lines")?;
        ensure!(valid_name(name), "dotenv source contains an invalid name");
        ensure!(
            seen.insert(name),
            "dotenv source contains a duplicate name: {name}"
        );
        ensure!(
            seen.len() <= MAX_ENV_ENTRIES,
            "dotenv source has too many entries"
        );
        if required_names.contains(name) {
            ensure!(!value.is_empty(), "required credential {name} is empty");
            ensure!(
                value.len() <= MAX_SECRET_BYTES,
                "required credential {name} exceeds the safe limit"
            );
            values.insert(name.to_owned(), value.to_owned());
        }
    }
    let missing = required_names
        .iter()
        .filter(|name| !values.contains_key(name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        missing.is_empty(),
        "dotenv source is missing required credentials: {}",
        missing.join(", ")
    );
    Ok(values)
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value.as_bytes()[0].is_ascii_digit()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn create_credential_directory(path: &Path, values: &BTreeMap<String, String>) -> Result<()> {
    let file_name = path
        .file_name()
        .context("credential output directory name is invalid")?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .canonicalize()
        .context("credential output parent is unavailable")?;
    let path = parent.join(file_name);
    ensure!(
        fs::symlink_metadata(&path).is_err(),
        "refusing to overwrite the credential output directory"
    );
    let mut builder = DirBuilder::new();
    builder.mode(0o700);
    builder
        .create(&path)
        .context("credential output directory cannot be created")?;
    let mut cleanup = IncompleteDirectory::new(path.clone());
    for (name, value) in values {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path.join(name))
            .context("credential file cannot be created")?;
        file.write_all(value.as_bytes())
            .context("credential file cannot be written")?;
        file.write_all(b"\n")
            .context("credential file cannot be finalized")?;
        file.sync_all()
            .context("credential file cannot be synchronized")?;
    }
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
        .context("credential directory permissions cannot be finalized")?;
    cleanup.disarm();
    Ok(())
}

struct IncompleteDirectory {
    path: PathBuf,
    armed: bool,
}

impl IncompleteDirectory {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for IncompleteDirectory {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        os::unix::fs::PermissionsExt,
        path::Path,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::{MAX_INPUT_BYTES, bootstrap_report_credentials, create_credential_directory};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

    fn directory(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "mcp-ozon-credential-bootstrap-{name}-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn write_fixture(root: &Path, enabled: bool, dotenv: &str) {
        fs::create_dir_all(root).unwrap();
        fs::write(
            root.join("access.json"),
            r#"{
              "version":1,
              "actors":[
                {"id":"diana","name":"Diana","role":"manager"},
                {"id":"anna","name":"Anna","role":"manager"}
              ],
              "accounts":[
                {"id":"ozon","organization":"Ozon","marketplace":"ozon","seller_client_id":"1","manager_id":"diana","ozon":{"store_id":"1","client_id_env":"OZON_ID","api_key_env":"OZON_KEY","performance":{"client_id_env":"PERF_ID","client_secret_env":"PERF_SECRET"}}},
                {"id":"wb","organization":"WB","marketplace":"wildberries","seller_client_id":"2","manager_id":"anna","wildberries":{"api_token_env":"WB_TOKEN"}}
              ]
            }"#,
        )
        .unwrap();
        fs::write(
            root.join("policy.json"),
            format!(
                r#"{{"version":1,"enabled":{enabled},"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{{"id":"pilot","email_env":"RECIPIENT","managers":[{{"actor_id":"diana","account_ids":["ozon"]}}]}}]}}"#
            ),
        )
        .unwrap();
        fs::write(root.join("source.env"), dotenv).unwrap();
    }

    fn bootstrap(root: &Path) -> anyhow::Result<super::CredentialBootstrapSummary> {
        bootstrap_report_credentials(
            &root.join("access.json"),
            &root.join("policy.json"),
            &root.join("source.env"),
            &root.join("credentials"),
        )
    }

    fn select_both_accounts(root: &Path) {
        fs::write(
            root.join("policy.json"),
            r#"{"version":1,"enabled":true,"timezone":"Asia/Yekaterinburg","sender_email_env":"SENDER","audiences":[{"id":"pilot","email_env":"RECIPIENT","managers":[{"actor_id":"diana","account_ids":["ozon"]},{"actor_id":"anna","account_ids":["wb"]}]}]}"#,
        )
        .unwrap();
    }

    #[test]
    fn writes_only_policy_scoped_credentials_with_restrictive_permissions() {
        let root = directory("happy");
        write_fixture(
            &root,
            true,
            "# local secrets\nOZON_ID=123\nOZON_KEY=key=value\nPERF_ID=456\nPERF_SECRET=secret\nWB_TOKEN=not-selected\nUNRELATED=ignored\n",
        );
        select_both_accounts(&root);
        let summary = bootstrap(&root).unwrap();
        assert_eq!(summary.account_count, 2);
        assert_eq!(summary.credential_count, 5);
        let output = root.join("credentials");
        assert_eq!(
            fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(fs::read_dir(&output).unwrap().count(), 5);
        assert_eq!(
            fs::read_to_string(output.join("OZON_KEY")).unwrap(),
            "key=value\n"
        );
        assert_eq!(
            fs::read_to_string(output.join("WB_TOKEN")).unwrap(),
            "not-selected\n"
        );
        assert!(!output.join("UNRELATED").exists());
        assert_eq!(
            fs::metadata(output.join("OZON_ID"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn refuses_disabled_policy_missing_duplicate_or_invalid_dotenv() {
        for (name, enabled, dotenv, expected) in [
            (
                "disabled",
                false,
                "OZON_ID=1\nOZON_KEY=2\nPERF_ID=3\nPERF_SECRET=4\n",
                "enabled",
            ),
            (
                "missing",
                true,
                "OZON_ID=1\nOZON_KEY=2\nPERF_ID=3\n",
                "missing required",
            ),
            (
                "duplicate",
                true,
                "OZON_ID=1\nOZON_ID=2\nOZON_KEY=2\nPERF_ID=3\nPERF_SECRET=4\n",
                "duplicate",
            ),
            (
                "invalid",
                true,
                "export OZON_ID=1\nOZON_KEY=2\nPERF_ID=3\nPERF_SECRET=4\n",
                "invalid name",
            ),
            (
                "empty",
                true,
                "OZON_ID=\nOZON_KEY=2\nPERF_ID=3\nPERF_SECRET=4\n",
                "is empty",
            ),
        ] {
            let root = directory(name);
            write_fixture(&root, enabled, dotenv);
            let error = format!("{:#}", bootstrap(&root).unwrap_err());
            assert!(error.contains(expected), "{error}");
            assert!(!root.join("credentials").exists());
        }
    }

    #[test]
    fn refuses_overwrite_symlink_and_oversized_inputs() {
        let missing = directory("missing-input");
        assert!(
            bootstrap_report_credentials(
                &missing.join("access.json"),
                &missing.join("policy.json"),
                &missing.join("source.env"),
                &missing.join("credentials"),
            )
            .unwrap_err()
            .to_string()
            .contains("unavailable")
        );

        let existing = directory("existing");
        write_fixture(
            &existing,
            true,
            "OZON_ID=1\nOZON_KEY=2\nPERF_ID=3\nPERF_SECRET=4\n",
        );
        fs::create_dir(existing.join("credentials")).unwrap();
        assert!(
            bootstrap(&existing)
                .unwrap_err()
                .to_string()
                .contains("overwrite")
        );

        let symlink = directory("symlink");
        write_fixture(
            &symlink,
            true,
            "OZON_ID=1\nOZON_KEY=2\nPERF_ID=3\nPERF_SECRET=4\n",
        );
        let real = symlink.join("real.env");
        fs::rename(symlink.join("source.env"), &real).unwrap();
        std::os::unix::fs::symlink(&real, symlink.join("source.env")).unwrap();
        assert!(
            bootstrap(&symlink)
                .unwrap_err()
                .to_string()
                .contains("non-symlink")
        );

        let oversized = directory("oversized");
        write_fixture(&oversized, true, "unused=1\n");
        fs::write(
            oversized.join("source.env"),
            vec![b'X'; MAX_INPUT_BYTES as usize + 1],
        )
        .unwrap();
        assert!(
            bootstrap(&oversized)
                .unwrap_err()
                .to_string()
                .contains("1 MiB")
        );

        let interrupted = directory("interrupted");
        fs::create_dir(&interrupted).unwrap();
        let mut values = BTreeMap::new();
        values.insert("A".to_owned(), "first".to_owned());
        values.insert("A/B".to_owned(), "cannot-open".to_owned());
        let output = interrupted.join("credentials");
        assert!(create_credential_directory(&output, &values).is_err());
        assert!(!output.exists());
    }
}
