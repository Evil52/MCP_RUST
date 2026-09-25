//! Offline operator recommendations from an explicitly supplied evidence file.

use std::{
    fs::{File, Metadata},
    io::{Read as _, Write as _},
    os::unix::fs::MetadataExt as _,
    path::Path,
};

use anyhow::{Result, anyhow, bail, ensure};
use mcp_ozon::{
    reporting::ads_optimizer::{MAX_INPUT_BYTES, parse_input, recommend},
    runtime::print_runtime_version_if_requested,
};

const USAGE: &str = "usage: ads-optimizer recommend <evidence.json>\n       ads-optimizer --help\n       ads-optimizer --version";

fn validate_file(metadata: &Metadata) -> Result<()> {
    ensure!(metadata.is_file(), "input must be a regular file");
    ensure!(
        metadata.len() <= MAX_INPUT_BYTES as u64,
        "input exceeds its size limit"
    );
    Ok(())
}

fn read_evidence(path: &Path) -> Result<Vec<u8>> {
    let before =
        std::fs::symlink_metadata(path).map_err(|_| anyhow!("input file is unavailable"))?;
    validate_file(&before)?;
    let file = File::open(path).map_err(|_| anyhow!("input file is unavailable"))?;
    let opened = file
        .metadata()
        .map_err(|_| anyhow!("input file is unavailable"))?;
    validate_file(&opened)?;
    ensure!(
        before.dev() == opened.dev() && before.ino() == opened.ino(),
        "input file changed while opening"
    );
    let mut bytes = Vec::new();
    file.take(MAX_INPUT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| anyhow!("input file is unavailable"))?;
    ensure!(
        bytes.len() <= MAX_INPUT_BYTES,
        "input exceeds its size limit"
    );
    let after =
        std::fs::symlink_metadata(path).map_err(|_| anyhow!("input file is unavailable"))?;
    validate_file(&after)?;
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
    let input = match arguments.as_slice() {
        [command, input] if command == "recommend" => input,
        _ => bail!("{USAGE}"),
    };
    // No environment configuration, dotenv, database or marketplace client.
    let bytes = read_evidence(Path::new(input))?;
    let report = recommend(parse_input(&bytes)?)?;
    let mut output =
        serde_json::to_vec_pretty(&report).map_err(|_| anyhow!("report serialization failed"))?;
    output.push(b'\n');
    std::io::stdout()
        .lock()
        .write_all(&output)
        .map_err(|_| anyhow!("output is unavailable"))?;
    Ok(())
}
