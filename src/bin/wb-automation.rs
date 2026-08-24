#![forbid(unsafe_code)]

use std::{path::PathBuf, time::Duration};

use anyhow::{Result, bail};
use chrono::Utc;
use mcp_ozon::control::{
    WbAutomationExecutor, WbAutomationObserver, WbAutomationStateView,
    persist_wb_automation_snapshot,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> Result<()> {
    match parse_command(&std::env::args().skip(1).collect::<Vec<_>>())? {
        Command::Observe(options) => observe_once(options).await,
        Command::Execute(options) => execute_once(options).await,
        Command::Auto(options) => auto_once(options).await,
    }
}

async fn observe_once(options: ObserveOptions) -> Result<()> {
    let observer = build_observer(&options)?;
    persist_observation(&observer, &options.state_directory, Utc::now()).await
}

async fn auto_once(options: ExecuteOptions) -> Result<()> {
    let now = Utc::now();
    let observer_options = options.observer_options();
    let observer = build_observer(&observer_options)?;
    if now < observer.policy().observe_until {
        return persist_observation(&observer, &options.state_directory, now).await;
    }
    execute_once_at(options, now).await
}

async fn execute_once(options: ExecuteOptions) -> Result<()> {
    execute_once_at(options, Utc::now()).await
}

async fn execute_once_at(options: ExecuteOptions, now: chrono::DateTime<Utc>) -> Result<()> {
    let executor = WbAutomationExecutor::from_files(
        &options.policy,
        &options.registry,
        &options.reader_token,
        &options.writer_token,
        &options.state_directory,
        options.allow_broad_reader,
        REQUEST_TIMEOUT,
        options.reader_proxy_url.as_deref(),
        &options.writer_proxy_url,
    )?;
    println!("{}", serde_json::to_string(&executor.run_once(now).await?)?);
    Ok(())
}

fn build_observer(options: &ObserveOptions) -> Result<WbAutomationObserver> {
    WbAutomationObserver::from_files(
        &options.policy,
        &options.registry,
        &options.reader_token,
        options.allow_broad_reader,
        REQUEST_TIMEOUT,
        options.reader_proxy_url.as_deref(),
    )
}

async fn persist_observation(
    observer: &WbAutomationObserver,
    state_directory: &std::path::Path,
    now: chrono::DateTime<Utc>,
) -> Result<()> {
    let snapshot = observer
        .observe(now, WbAutomationStateView::default())
        .await?;
    let history = persist_wb_automation_snapshot(state_directory, &snapshot)?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "account_id": &snapshot.decision.account_id,
            "campaign_id": snapshot.decision.campaign_id,
            "observed_at": snapshot.observation.observed_at,
            "decision": &snapshot.decision.action,
            "outcome": "observed",
            "snapshot": history,
        }))?
    );
    Ok(())
}

enum Command {
    Observe(ObserveOptions),
    Execute(ExecuteOptions),
    Auto(ExecuteOptions),
}

struct ObserveOptions {
    policy: PathBuf,
    registry: PathBuf,
    reader_token: PathBuf,
    state_directory: PathBuf,
    allow_broad_reader: bool,
    reader_proxy_url: Option<String>,
}

struct ExecuteOptions {
    policy: PathBuf,
    registry: PathBuf,
    reader_token: PathBuf,
    writer_token: PathBuf,
    state_directory: PathBuf,
    allow_broad_reader: bool,
    writer_proxy_url: String,
    reader_proxy_url: Option<String>,
}

impl ExecuteOptions {
    fn observer_options(&self) -> ObserveOptions {
        ObserveOptions {
            policy: self.policy.clone(),
            registry: self.registry.clone(),
            reader_token: self.reader_token.clone(),
            state_directory: self.state_directory.clone(),
            allow_broad_reader: self.allow_broad_reader,
            reader_proxy_url: self.reader_proxy_url.clone(),
        }
    }
}

fn parse_command(arguments: &[String]) -> Result<Command> {
    if let [
        command,
        policy,
        registry,
        reader_token,
        writer_token,
        state_directory,
        broad_reader,
        writer_proxy_url,
        tail @ ..,
    ] = arguments
        && matches!(command.as_str(), "execute-once" | "auto-once")
    {
        let options = ExecuteOptions {
            policy: policy.into(),
            registry: registry.into(),
            reader_token: reader_token.into(),
            writer_token: writer_token.into(),
            state_directory: state_directory.into(),
            allow_broad_reader: parse_bool(broad_reader)?,
            writer_proxy_url: nonempty(writer_proxy_url)?,
            reader_proxy_url: optional_proxy(tail)?,
        };
        return Ok(if command == "execute-once" {
            Command::Execute(options)
        } else {
            Command::Auto(options)
        });
    }
    let [
        command,
        policy,
        registry,
        reader_token,
        state_directory,
        broad_reader,
        tail @ ..,
    ] = arguments
    else {
        return usage();
    };
    if command != "observe-once" {
        return usage();
    }
    Ok(Command::Observe(ObserveOptions {
        policy: policy.into(),
        registry: registry.into(),
        reader_token: reader_token.into(),
        state_directory: state_directory.into(),
        allow_broad_reader: parse_bool(broad_reader)?,
        reader_proxy_url: optional_proxy(tail)?,
    }))
}

fn optional_proxy(values: &[String]) -> Result<Option<String>> {
    match values {
        [] => Ok(None),
        [proxy_url] => nonempty(proxy_url).map(Some),
        _ => usage(),
    }
}

fn nonempty(value: &str) -> Result<String> {
    if value.is_empty() {
        usage()
    } else {
        Ok(value.to_owned())
    }
}

fn parse_bool(value: &str) -> Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => usage(),
    }
}

fn usage<T>() -> Result<T> {
    bail!(
        "usage: wb-automation observe-once <policy.json> <access.json> <read-token-file> <private-state-directory> <allow-broad-reader:true|false> [reader-proxy-url] | wb-automation <execute-once|auto-once> <policy.json> <access.json> <read-token-file> <write-token-file> <private-state-directory> <allow-broad-reader:true|false> <writer-proxy-url> [reader-proxy-url]"
    )
}
