//! Operator-only file import. It neither starts a listener nor loads marketplace keys.

use std::{
    fs::{File, Metadata},
    io::Read,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
    str::FromStr,
};

use anyhow::{Result, anyhow, bail, ensure};
use mcp_ozon::{
    config::AccessRegistry,
    reporting::{
        cost_import::{MAX_COST_IMPORT_BYTES, PostgresCostRepository, ValidatedCostBatch},
        cost_import_service::CostImportPolicy,
    },
    runtime::print_runtime_version_if_requested,
};
use serde_json::json;
use tokio_postgres::{Config, config::Host};

fn variable(name: &str) -> Result<String> {
    std::env::var(name).map_err(|_| anyhow!("required operator configuration is missing: {name}"))
}

fn validate_file(metadata: &Metadata, maximum: usize, trusted: bool) -> Result<()> {
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink() && metadata.nlink() == 1,
        "input must be a regular file with one link"
    );
    ensure!(
        metadata.len() <= maximum as u64,
        "input exceeds its size limit"
    );
    ensure!(
        !trusted || metadata.permissions().mode() & 0o022 == 0,
        "operator policy and registry must not be writable by group or others"
    );
    Ok(())
}

fn read_file(path: &Path, maximum: usize, trusted: bool) -> Result<Vec<u8>> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|_| anyhow!("input file is unavailable"))?;
    validate_file(&metadata, maximum, trusted)?;
    let file = File::open(path).map_err(|_| anyhow!("input file is unavailable"))?;
    let opened = file
        .metadata()
        .map_err(|_| anyhow!("input file is unavailable"))?;
    validate_file(&opened, maximum, trusted)?;
    ensure!(
        metadata.dev() == opened.dev() && metadata.ino() == opened.ino(),
        "input file changed while opening"
    );
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| anyhow!("input file is unavailable"))?;
    let after =
        std::fs::symlink_metadata(path).map_err(|_| anyhow!("input file is unavailable"))?;
    validate_file(&after, maximum, trusted)?;
    ensure!(
        opened.dev() == after.dev()
            && opened.ino() == after.ino()
            && opened.len() == after.len()
            && opened.mtime() == after.mtime()
            && opened.mtime_nsec() == after.mtime_nsec(),
        "input file changed while reading"
    );
    ensure!(bytes.len() <= maximum, "input exceeds its size limit");
    Ok(bytes)
}

fn registry(path: &Path) -> Result<AccessRegistry> {
    let registry: AccessRegistry = serde_json::from_slice(&read_file(path, 1024 * 1024, true)?)
        .map_err(|_| anyhow!("access registry is invalid"))?;
    registry
        .validate()
        .map_err(|_| anyhow!("access registry is invalid"))?;
    Ok(registry)
}

fn database() -> Result<Config> {
    let mut database = Config::from_str(&variable("COST_IMPORT_DATABASE_URL")?)
        .map_err(|_| anyhow!("invalid cost import database configuration"))?;
    ensure!(
        database.get_user() == Some("report_cost_importer")
            && database
                .get_password()
                .is_some_and(|password| !password.is_empty())
            && database.get_dbname().is_some()
            && matches!(database.get_hosts(), [Host::Tcp(host)] if !host.is_empty()),
        "cost import requires its dedicated authenticated TCP database role"
    );
    mcp_ozon::postgres::harden(&mut database, "mcp-ozon-cost-import");
    Ok(database)
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if print_runtime_version_if_requested("cost-import", &arguments)? {
        return Ok(());
    }
    let (writing, input) = match arguments.as_slice() {
        [command, input] if command == "validate" => (false, input),
        [command, input] if command == "import" => (true, input),
        _ => bail!("usage: cost-import <validate|import> <prepared-json-file>"),
    };
    // No dotenv: only explicitly mounted service configuration is consulted.
    let policy_path = variable("COST_IMPORT_POLICY")?;
    let policy =
        CostImportPolicy::from_json(&read_file(Path::new(&policy_path), 1024 * 1024, true)?)?;
    let registry_path = variable("MCP_ACCESS_CONFIG")?;
    let actor = variable("COST_IMPORT_ACTOR_ID")?;
    let scope = policy.authorize(&registry(Path::new(&registry_path))?, &actor, writing)?;
    let bytes = read_file(Path::new(input), MAX_COST_IMPORT_BYTES, false)?;
    let batch = ValidatedCostBatch::parse_json(&bytes, &scope)?;
    if !writing {
        println!(
            "{}",
            json!({"status":"validated", "export_id":batch.export_id(),
            "sha256":batch.sha256(), "row_count":batch.row_count(), "imported":false})
        );
        return Ok(());
    }
    let repository = PostgresCostRepository::connect(&database()?).await?;
    // Apply current rights again after I/O; the file itself cannot select an actor.
    let policy =
        CostImportPolicy::from_json(&read_file(Path::new(&policy_path), 1024 * 1024, true)?)?;
    let scope = policy.authorize(&registry(Path::new(&registry_path))?, &actor, true)?;
    let batch = ValidatedCostBatch::parse_json(&bytes, &scope)?;
    let receipt = repository.import(&batch).await?;
    println!("{}", serde_json::to_string(&receipt)?);
    Ok(())
}
