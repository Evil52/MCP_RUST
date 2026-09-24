//! One scheduled process for any explicitly registered WB campaigns.
use super::super::{ExecuteOptions, PostgresExecuteOptions, execute_postgres_once};
use anyhow::{Result, ensure};
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fleet {
    version: u32,
    account_id: String,
    registry: PathBuf,
    reader_token: PathBuf,
    writer_token: PathBuf,
    reader_proxy: String,
    writer_proxy: String,
    allow_broad_reader: bool,
    campaigns: Vec<Campaign>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Campaign {
    policy: PathBuf,
    initial_state: PathBuf,
}

fn options(path: &Path) -> Result<Vec<PostgresExecuteOptions>> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file()
            && metadata.permissions().mode().trailing_zeros() >= 6
            && metadata.len() <= 256 * 1024,
        "fleet configuration must be a bounded private regular file"
    );
    let fleet: Fleet = serde_json::from_slice(&fs::read(path)?)?;
    ensure!(
        fleet.version == 1 && (1..=50).contains(&fleet.campaigns.len()),
        "fleet requires version 1 and 1..=50 campaigns"
    );
    ensure!(
        !fleet.reader_proxy.is_empty() && !fleet.writer_proxy.is_empty(),
        "fleet requires dedicated read/write proxies"
    );
    let mut identities = BTreeSet::new();
    let mut result = Vec::new();
    for campaign in fleet.campaigns {
        let metadata = fs::symlink_metadata(&campaign.policy)?;
        ensure!(
            metadata.is_file()
                && metadata.permissions().mode() & 0o022 == 0
                && metadata.len() <= 256 * 1024,
            "unsafe fleet campaign policy"
        );
        let policy: mcp_ozon::control::WbAutomationPolicy =
            serde_json::from_slice(&fs::read(&campaign.policy)?)?;
        mcp_ozon::control::validate_wb_automation_policy(&policy)?;
        ensure!(
            policy.account_id == fleet.account_id && identities.insert(policy.campaign_id),
            "fleet contains another account or duplicate campaign"
        );
        result.push(PostgresExecuteOptions {
            execute: ExecuteOptions {
                policy: campaign.policy,
                registry: fleet.registry.clone(),
                reader_token: fleet.reader_token.clone(),
                writer_token: fleet.writer_token.clone(),
                state_directory: PathBuf::new(),
                allow_broad_reader: fleet.allow_broad_reader,
                writer_proxy_url: fleet.writer_proxy.clone(),
                reader_proxy_url: Some(fleet.reader_proxy.clone()),
            },
            legacy_state: campaign.initial_state,
        });
    }
    Ok(result)
}

pub async fn run(path: &Path) -> Result<()> {
    let campaigns = options(path)?;
    let mut failures = 0;
    for campaign in campaigns {
        let policy = campaign.execute.policy.clone();
        // The existing executor owns campaign locks, incidents, state import,
        // bid corridor, quota and readback. One campaign failure cannot skip the
        // protection cycle of the remaining registered campaigns.
        if let Err(error) = execute_postgres_once(campaign).await {
            failures += 1;
            eprintln!(
                "{}",
                serde_json::json!({"policy":policy,"outcome":"cycle_failed","error":error.to_string()})
            );
        }
    }
    ensure!(
        failures == 0,
        "{failures} WB campaign cycles failed; all remaining campaigns were checked"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::DirBuilderExt;

    #[test]
    fn fleet_rejects_duplicates_and_cross_account_before_any_execution() {
        let root = std::env::temp_dir().join(format!(
            "wb-fleet-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let policy_path = root.join("policy.json");
        let mut policy: serde_json::Value = serde_json::from_str(include_str!(
            "../../../config/wb-automation-oduvanchik.v4.json"
        ))
        .unwrap();
        fs::write(&policy_path, serde_json::to_vec(&policy).unwrap()).unwrap();
        let path = root.join("fleet.json");
        let entry =
            serde_json::json!({"policy":policy_path,"initial_state":root.join("initial.json")});
        let mut config = serde_json::json!({"version":1,"account_id":"ofk_region_wb","registry":"unused",
            "reader_token":"unused","writer_token":"unused","reader_proxy":"http://reader:3128",
            "writer_proxy":"http://writer:3130","allow_broad_reader":false,"campaigns":[entry]});
        let save = |value: &serde_json::Value| {
            fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        };
        save(&config);
        assert_eq!(options(&path).unwrap().len(), 1);
        config["campaigns"] = serde_json::json!([entry, entry]);
        save(&config);
        assert!(options(&path).is_err());
        config["campaigns"] = serde_json::json!([entry]);
        save(&config);
        policy["account_id"] = serde_json::json!("another_account");
        fs::write(&policy_path, serde_json::to_vec(&policy).unwrap()).unwrap();
        assert!(options(&path).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
