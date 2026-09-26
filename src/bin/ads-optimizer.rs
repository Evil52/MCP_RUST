//! Offline operator recommendations from an explicitly supplied evidence file.

use std::{
    fs::{File, Metadata},
    io::{Read as _, Write as _},
    os::unix::fs::MetadataExt as _,
    path::Path,
};

use anyhow::{Result, anyhow, bail, ensure};
use mcp_ozon::{
    reporting::ads_optimizer::{
        MAX_INPUT_BYTES,
        campaign_history::{analyze_campaign_history, parse_campaign_history_input},
        journal::record_run,
        parse_input,
        prepare::{MAX_PREPARATION_BYTES, prepare_input},
        recommend,
        reconciliation::{parse_reconciliation_input, reconcile},
    },
    runtime::print_runtime_version_if_requested,
};

const USAGE: &str = "usage: ads-optimizer recommend <evidence.json> [--journal-dir <new-run-directory>]\n       ads-optimizer prepare <bundle.json>\n       ads-optimizer reconcile <exports.json>\n       ads-optimizer campaign-history <campaign-days.json>\n       ads-optimizer --help\n       ads-optimizer --version";

fn validate_file(metadata: &Metadata, max_bytes: usize) -> Result<()> {
    ensure!(metadata.is_file(), "input must be a regular file");
    ensure!(
        metadata.len() <= max_bytes as u64,
        "input exceeds its size limit"
    );
    Ok(())
}

fn read_evidence(path: &Path, max_bytes: usize) -> Result<Vec<u8>> {
    let before =
        std::fs::symlink_metadata(path).map_err(|_| anyhow!("input file is unavailable"))?;
    validate_file(&before, max_bytes)?;
    let file = File::open(path).map_err(|_| anyhow!("input file is unavailable"))?;
    let opened = file
        .metadata()
        .map_err(|_| anyhow!("input file is unavailable"))?;
    validate_file(&opened, max_bytes)?;
    ensure!(
        before.dev() == opened.dev() && before.ino() == opened.ino(),
        "input file changed while opening"
    );
    let mut bytes = Vec::new();
    file.take(max_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| anyhow!("input file is unavailable"))?;
    ensure!(bytes.len() <= max_bytes, "input exceeds its size limit");
    let after =
        std::fs::symlink_metadata(path).map_err(|_| anyhow!("input file is unavailable"))?;
    validate_file(&after, max_bytes)?;
    ensure!(
        opened.dev() == after.dev()
            && opened.ino() == after.ino()
            && opened.len() == after.len()
            && opened.mtime() == after.mtime()
            && opened.mtime_nsec() == after.mtime_nsec(),
        "input file changed while reading"
    );
    Ok(bytes)
}

fn main() -> Result<()> {
    let arguments = std::env::args_os()
        .skip(1)
        .map(|argument| {
            argument
                .into_string()
                .map_err(|_| anyhow!("arguments must be valid Unicode"))
        })
        .collect::<Result<Vec<_>>>()?;
    if print_runtime_version_if_requested("ads-optimizer", &arguments)
        .map_err(|_| anyhow!("output is unavailable"))?
    {
        return Ok(());
    }
    if matches!(arguments.as_slice(), [argument] if argument == "--help" || argument == "-h") {
        writeln!(std::io::stdout().lock(), "{USAGE}")
            .map_err(|_| anyhow!("output is unavailable"))?;
        return Ok(());
    }
    // No environment configuration, dotenv, database or marketplace client.
    let output = match arguments.as_slice() {
        [command, input] if command == "recommend" => recommend_file(input, None)?,
        [command, input, flag, directory] if command == "recommend" && flag == "--journal-dir" => {
            recommend_file(input, Some(directory))?
        }
        [command, input] if command == "prepare" => {
            let bytes = read_evidence(Path::new(input), MAX_PREPARATION_BYTES)?;
            let output = json_bytes(&prepare_input(&bytes)?)?;
            ensure!(
                output.len() <= MAX_INPUT_BYTES,
                "prepared evidence exceeds its size limit"
            );
            output
        }
        [command, input] if command == "reconcile" => {
            let bytes = read_evidence(Path::new(input), MAX_INPUT_BYTES)?;
            json_bytes(&reconcile(parse_reconciliation_input(&bytes)?)?)?
        }
        [command, input] if command == "campaign-history" => {
            let bytes = read_evidence(Path::new(input), MAX_INPUT_BYTES)?;
            json_bytes(&analyze_campaign_history(parse_campaign_history_input(
                &bytes,
            )?)?)?
        }
        _ => bail!("{USAGE}"),
    };
    std::io::stdout()
        .lock()
        .write_all(&output)
        .map_err(|_| anyhow!("output is unavailable"))?;
    Ok(())
}

fn recommend_file(input: &str, directory: Option<&str>) -> Result<Vec<u8>> {
    let bytes = read_evidence(Path::new(input), MAX_INPUT_BYTES)?;
    let report = recommend(parse_input(&bytes)?)?;
    let output = json_bytes(&report)?;
    if let Some(directory) = directory {
        record_run(Path::new(directory), &bytes, &report, &output)?;
    }
    Ok(output)
}

fn json_bytes(value: &impl serde::Serialize) -> Result<Vec<u8>> {
    let mut output =
        serde_json::to_vec_pretty(value).map_err(|_| anyhow!("report serialization failed"))?;
    output.push(b'\n');
    Ok(output)
}
