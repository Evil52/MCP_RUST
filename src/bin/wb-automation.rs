#![forbid(unsafe_code)]

use std::{
    fmt::Write as _,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, NaiveDate, Utc};
use mcp_ozon::control::{
    WbAutomationExecutor, WbAutomationLegacyStateSeed, WbAutomationObserver,
    WbAutomationPostgresStore, WbAutomationStateView, persist_wb_automation_snapshot,
    wb_automation_business_date,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio_postgres::Config;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_LEGACY_STATE_BYTES: u64 = 256 * 1024;
const DATABASE_URL_ENV: &str = "WB_AUTOMATION_DATABASE_URL";

#[tokio::main]
async fn main() -> Result<()> {
    match parse_command(&std::env::args().skip(1).collect::<Vec<_>>())? {
        Command::Observe(options) => observe_once(options).await,
        Command::ShadowPostgres(options) => shadow_postgres_once(options).await,
        Command::ActivateProtectiveLivePostgres(options) => {
            activate_protective_live_postgres(options).await
        }
        Command::ActivateBidWritesPostgres(options) => activate_bid_writes_postgres(options).await,
        Command::ExecutePostgres(options) => execute_postgres_once(options).await,
        Command::ExplicitExposureIncreasePostgres(options) => {
            explicit_exposure_increase_postgres_once(options).await
        }
        Command::Execute(options) => execute_once(options).await,
        Command::Auto(options) => auto_once(options).await,
    }
}

async fn activate_protective_live_postgres(options: ActivatePolicyOptions) -> Result<()> {
    let shadow = build_observer(&options.source)?;
    let live = build_observer(&ObserveOptions {
        policy: options.target_policy,
        registry: options.source.registry.clone(),
        reader_token: options.source.reader_token.clone(),
        state_directory: PathBuf::new(),
        allow_broad_reader: options.source.allow_broad_reader,
        reader_proxy_url: options.source.reader_proxy_url.clone(),
    })?;
    ensure!(
        !shadow.policy().write_enabled
            && !shadow.policy().bid_writes_enabled
            && live.policy().write_enabled
            && !live.policy().bid_writes_enabled,
        "WB automation protective live cutover policy modes are invalid"
    );
    let mut expected_shadow = live.policy().clone();
    expected_shadow.write_enabled = false;
    ensure!(
        shadow.policy() == &expected_shadow,
        "WB automation protective live policy expands the reviewed shadow scope"
    );
    let now = Utc::now();
    ensure!(
        now >= live.policy().authorized_at && now < live.policy().authorization_expires_at,
        "WB automation protective live authorization is not active"
    );
    let database_url =
        std::env::var(DATABASE_URL_ENV).context("WB automation PostgreSQL URL is unavailable")?;
    let database_config =
        Config::from_str(&database_url).context("WB automation PostgreSQL URL is invalid")?;
    let store = WbAutomationPostgresStore::connect(&database_config).await?;
    store.verify_runtime_contract().await?;
    let Some(mut lease) = store
        .try_acquire_campaign(live.policy().account_id.as_str(), live.policy().campaign_id)
        .await?
    else {
        println!("{}", serde_json::json!({"outcome": "lock_contended"}));
        return Ok(());
    };
    let receipt = lease
        .activate_protective_live_policy(shadow.policy_sha256(), live.policy_sha256())
        .await?;
    lease.release().await?;
    println!(
        "{}",
        serde_json::json!({
            "account_id": live.policy().account_id,
            "campaign_id": live.policy().campaign_id,
            "outcome": if receipt.changed {
                "protective_live_activated"
            } else {
                "protective_live_already_active"
            },
            "state_revision": receipt.state_revision,
            "bid_writes_enabled": false,
        })
    );
    Ok(())
}

async fn activate_bid_writes_postgres(options: ActivatePolicyOptions) -> Result<()> {
    let protective = build_observer(&options.source)?;
    let bid_live = build_observer(&ObserveOptions {
        policy: options.target_policy,
        registry: options.source.registry.clone(),
        reader_token: options.source.reader_token.clone(),
        state_directory: PathBuf::new(),
        allow_broad_reader: options.source.allow_broad_reader,
        reader_proxy_url: options.source.reader_proxy_url.clone(),
    })?;
    ensure!(
        protective.policy().write_enabled
            && !protective.policy().bid_writes_enabled
            && bid_live.policy().write_enabled
            && bid_live.policy().bid_writes_enabled,
        "WB automation bid-live cutover policy modes are invalid"
    );
    let mut expected_protective = bid_live.policy().clone();
    expected_protective.bid_writes_enabled = false;
    ensure!(
        protective.policy() == &expected_protective,
        "WB automation bid-live policy expands the reviewed protective scope"
    );
    let now = Utc::now();
    ensure!(
        now >= bid_live.policy().authorized_at && now < bid_live.policy().authorization_expires_at,
        "WB automation bid-live authorization is not active"
    );
    let database_url =
        std::env::var(DATABASE_URL_ENV).context("WB automation PostgreSQL URL is unavailable")?;
    let database_config =
        Config::from_str(&database_url).context("WB automation PostgreSQL URL is invalid")?;
    let store = WbAutomationPostgresStore::connect(&database_config).await?;
    store.verify_runtime_contract().await?;
    let Some(mut lease) = store
        .try_acquire_campaign(
            bid_live.policy().account_id.as_str(),
            bid_live.policy().campaign_id,
        )
        .await?
    else {
        bail!("WB automation bid-live campaign lock is contended");
    };
    let receipt = lease
        .activate_bid_writes_policy(protective.policy_sha256(), bid_live.policy_sha256())
        .await?;
    lease.release().await?;
    println!(
        "{}",
        serde_json::json!({
            "account_id": bid_live.policy().account_id,
            "campaign_id": bid_live.policy().campaign_id,
            "outcome": if receipt.changed {
                "bid_writes_activated"
            } else {
                "bid_writes_already_active"
            },
            "state_revision": receipt.state_revision,
            "bid_writes_enabled": true,
        })
    );
    Ok(())
}

async fn shadow_postgres_once(options: ShadowPostgresOptions) -> Result<()> {
    let observer = build_observer(&options.observer)?;
    ensure!(
        !observer.policy().write_enabled,
        "PostgreSQL shadow runtime refuses a write-enabled policy"
    );
    let legacy = load_legacy_state(
        &options.legacy_state,
        observer.policy().account_id.as_str(),
        observer.policy().campaign_id,
        observer.policy_sha256(),
    )?;
    let database_url =
        std::env::var(DATABASE_URL_ENV).context("WB automation PostgreSQL URL is unavailable")?;
    let database_config =
        Config::from_str(&database_url).context("WB automation PostgreSQL URL is invalid")?;
    let store = WbAutomationPostgresStore::connect(&database_config).await?;
    store.verify_runtime_contract().await?;
    let Some(mut lease) = store
        .try_acquire_campaign(
            observer.policy().account_id.as_str(),
            observer.policy().campaign_id,
        )
        .await?
    else {
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "account_id": observer.policy().account_id,
                "campaign_id": observer.policy().campaign_id,
                "outcome": "lock_contended",
            }))?
        );
        return Ok(());
    };
    let imported = lease.initialize_from_legacy(&legacy).await?;
    let state = lease
        .load_state()
        .await?
        .context("WB automation PostgreSQL state is unavailable")?;
    let now = Utc::now();
    let business_date = wb_automation_business_date(now);
    let state_view = WbAutomationStateView {
        paused_by_automation: state
            .paused_for_daily_cap_on
            .is_some_and(|paused_on| paused_on < business_date),
        actions_today: if state.business_date == business_date {
            state.actions_today
        } else {
            0
        },
        last_action_at: state.last_action_at,
    };
    let snapshot = observer.observe(now, state_view).await?;
    let snapshot_json = serde_json::to_string(&snapshot)?;
    let decision_json = serde_json::to_string(&snapshot.decision)?;
    let cycle_id = sha256_domain("wb-automation-cycle-v1", snapshot_json.as_bytes());
    let inserted = lease
        .persist_shadow_cycle(
            &cycle_id,
            observer.policy_sha256(),
            snapshot.observation.observed_at,
            business_date,
            state.revision,
            &snapshot_json,
            &decision_json,
        )
        .await?;
    lease.release().await?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "account_id": &snapshot.decision.account_id,
            "campaign_id": snapshot.decision.campaign_id,
            "observed_at": snapshot.observation.observed_at,
            "decision": &snapshot.decision.action,
            "outcome": "shadow_persisted",
            "cycle_id": cycle_id,
            "cycle_inserted": inserted,
            "legacy_imported": imported,
        }))?
    );
    Ok(())
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

async fn execute_postgres_once(options: PostgresExecuteOptions) -> Result<()> {
    execute_postgres_with_target(options, None).await
}

async fn explicit_exposure_increase_postgres_once(
    options: ExplicitExposureIncreaseOptions,
) -> Result<()> {
    ensure!(
        options.confirmation == "--confirm-explicit-exposure-increase",
        "WB explicit exposure increase confirmation is invalid"
    );
    execute_postgres_with_target(options.execute, Some(options.target_impressions)).await
}

async fn execute_postgres_with_target(
    options: PostgresExecuteOptions,
    target_impressions: Option<u64>,
) -> Result<()> {
    let state_directory = options
        .legacy_state
        .parent()
        .context("WB automation legacy state parent is unavailable")?;
    let executor = WbAutomationExecutor::from_files(
        &options.execute.policy,
        &options.execute.registry,
        &options.execute.reader_token,
        &options.execute.writer_token,
        state_directory,
        options.execute.allow_broad_reader,
        REQUEST_TIMEOUT,
        options.execute.reader_proxy_url.as_deref(),
        &options.execute.writer_proxy_url,
    )?;
    let legacy = load_legacy_state(
        &options.legacy_state,
        &executor.policy().account_id,
        executor.policy().campaign_id,
        executor.policy_sha256(),
    )?;
    let database_url =
        std::env::var(DATABASE_URL_ENV).context("WB automation PostgreSQL URL is unavailable")?;
    let database_config =
        Config::from_str(&database_url).context("WB automation PostgreSQL URL is invalid")?;
    let store = WbAutomationPostgresStore::connect(&database_config).await?;
    store.verify_runtime_contract().await?;
    let receipt = match target_impressions {
        Some(target) => {
            executor
                .run_explicit_exposure_increase_once_postgres(&store, &legacy, Utc::now(), target)
                .await?
        }
        None => {
            executor
                .run_once_postgres(&store, &legacy, Utc::now())
                .await?
        }
    };
    match receipt {
        Some(receipt) => println!("{}", serde_json::to_string(&receipt)?),
        None => println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "account_id": executor.policy().account_id,
                "campaign_id": executor.policy().campaign_id,
                "outcome": "lock_contended",
            }))?
        ),
    }
    Ok(())
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
    ShadowPostgres(ShadowPostgresOptions),
    ActivateProtectiveLivePostgres(ActivatePolicyOptions),
    ActivateBidWritesPostgres(ActivatePolicyOptions),
    ExecutePostgres(PostgresExecuteOptions),
    ExplicitExposureIncreasePostgres(ExplicitExposureIncreaseOptions),
    Execute(ExecuteOptions),
    Auto(ExecuteOptions),
}

struct ActivatePolicyOptions {
    source: ObserveOptions,
    target_policy: PathBuf,
}

struct ShadowPostgresOptions {
    observer: ObserveOptions,
    legacy_state: PathBuf,
}

struct PostgresExecuteOptions {
    execute: ExecuteOptions,
    legacy_state: PathBuf,
}

struct ExplicitExposureIncreaseOptions {
    execute: PostgresExecuteOptions,
    target_impressions: u64,
    confirmation: String,
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
        legacy_state,
        broad_reader,
        writer_proxy_url,
        target_impressions,
        confirmation,
        tail @ ..,
    ] = arguments
        && command == "explicit-exposure-increase-once-pg"
    {
        return Ok(Command::ExplicitExposureIncreasePostgres(
            ExplicitExposureIncreaseOptions {
                execute: PostgresExecuteOptions {
                    execute: ExecuteOptions {
                        policy: policy.into(),
                        registry: registry.into(),
                        reader_token: reader_token.into(),
                        writer_token: writer_token.into(),
                        state_directory: PathBuf::new(),
                        allow_broad_reader: parse_bool(broad_reader)?,
                        writer_proxy_url: nonempty(writer_proxy_url)?,
                        reader_proxy_url: optional_proxy(tail)?,
                    },
                    legacy_state: legacy_state.into(),
                },
                target_impressions: target_impressions
                    .parse()
                    .context("WB explicit exposure target is invalid")?,
                confirmation: confirmation.clone(),
            },
        ));
    }
    if let [
        command,
        shadow_policy,
        live_policy,
        registry,
        reader_token,
        broad_reader,
        tail @ ..,
    ] = arguments
        && command == "activate-protective-live-pg"
    {
        return Ok(Command::ActivateProtectiveLivePostgres(
            ActivatePolicyOptions {
                source: ObserveOptions {
                    policy: shadow_policy.into(),
                    registry: registry.into(),
                    reader_token: reader_token.into(),
                    state_directory: PathBuf::new(),
                    allow_broad_reader: parse_bool(broad_reader)?,
                    reader_proxy_url: optional_proxy(tail)?,
                },
                target_policy: live_policy.into(),
            },
        ));
    }
    if let [
        command,
        protective_policy,
        bid_policy,
        registry,
        reader_token,
        broad_reader,
        tail @ ..,
    ] = arguments
        && command == "activate-bid-writes-pg"
    {
        return Ok(Command::ActivateBidWritesPostgres(ActivatePolicyOptions {
            source: ObserveOptions {
                policy: protective_policy.into(),
                registry: registry.into(),
                reader_token: reader_token.into(),
                state_directory: PathBuf::new(),
                allow_broad_reader: parse_bool(broad_reader)?,
                reader_proxy_url: optional_proxy(tail)?,
            },
            target_policy: bid_policy.into(),
        }));
    }
    if let [
        command,
        policy,
        registry,
        reader_token,
        legacy_state,
        broad_reader,
        tail @ ..,
    ] = arguments
        && command == "shadow-once-pg"
    {
        return Ok(Command::ShadowPostgres(ShadowPostgresOptions {
            observer: ObserveOptions {
                policy: policy.into(),
                registry: registry.into(),
                reader_token: reader_token.into(),
                state_directory: PathBuf::new(),
                allow_broad_reader: parse_bool(broad_reader)?,
                reader_proxy_url: optional_proxy(tail)?,
            },
            legacy_state: legacy_state.into(),
        }));
    }
    if let [
        command,
        policy,
        registry,
        reader_token,
        writer_token,
        legacy_state,
        broad_reader,
        writer_proxy_url,
        tail @ ..,
    ] = arguments
        && command == "execute-once-pg"
    {
        return Ok(Command::ExecutePostgres(PostgresExecuteOptions {
            execute: ExecuteOptions {
                policy: policy.into(),
                registry: registry.into(),
                reader_token: reader_token.into(),
                writer_token: writer_token.into(),
                state_directory: PathBuf::new(),
                allow_broad_reader: parse_bool(broad_reader)?,
                writer_proxy_url: nonempty(writer_proxy_url)?,
                reader_proxy_url: optional_proxy(tail)?,
            },
            legacy_state: legacy_state.into(),
        }));
    }
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
        "usage: wb-automation observe-once <policy.json> <access.json> <read-token-file> <private-state-directory> <allow-broad-reader:true|false> [reader-proxy-url] | wb-automation shadow-once-pg <policy.json> <access.json> <read-token-file> <legacy-execution-state.json> <allow-broad-reader:true|false> [reader-proxy-url] | wb-automation activate-protective-live-pg <shadow-policy.json> <live-policy.json> <access.json> <read-token-file> <allow-broad-reader:true|false> [reader-proxy-url] | wb-automation activate-bid-writes-pg <protective-policy.json> <bid-policy.json> <access.json> <read-token-file> <allow-broad-reader:true|false> [reader-proxy-url] | wb-automation execute-once-pg <policy.json> <access.json> <read-token-file> <write-token-file> <legacy-execution-state.json> <allow-broad-reader:true|false> <writer-proxy-url> [reader-proxy-url] | wb-automation explicit-exposure-increase-once-pg <policy.json> <access.json> <read-token-file> <write-token-file> <legacy-execution-state.json> <allow-broad-reader:true|false> <writer-proxy-url> <target-impressions> --confirm-explicit-exposure-increase [reader-proxy-url] | wb-automation <execute-once|auto-once> <policy.json> <access.json> <read-token-file> <write-token-file> <private-state-directory> <allow-broad-reader:true|false> <writer-proxy-url> [reader-proxy-url]"
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyExecutionState {
    schema_version: u32,
    policy_sha256: String,
    account_id: String,
    campaign_id: u64,
    business_date: NaiveDate,
    actions_today: u32,
    last_action_at: Option<DateTime<Utc>>,
    paused_for_daily_cap_on: Option<NaiveDate>,
    pending: Option<serde_json::Value>,
    incident_class: Option<String>,
}

fn load_legacy_state(
    path: &Path,
    account_id: &str,
    campaign_id: u64,
    current_policy_digest: &str,
) -> Result<WbAutomationLegacyStateSeed> {
    let metadata = fs::symlink_metadata(path)
        .context("WB automation legacy execution state is unavailable")?;
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.len() <= MAX_LEGACY_STATE_BYTES
            && metadata.permissions().mode().is_multiple_of(0o100),
        "WB automation legacy execution state is unsafe"
    );
    let bytes = fs::read(path).context("WB automation legacy execution state cannot be read")?;
    let state = serde_json::from_slice::<LegacyExecutionState>(&bytes)
        .context("WB automation legacy execution state is invalid")?;
    ensure!(
        state.schema_version == 1
            && state.account_id == account_id
            && state.campaign_id == campaign_id
            && is_lower_sha256(&state.policy_sha256),
        "WB automation legacy execution state does not match policy"
    );
    ensure!(
        state.pending.is_none(),
        "WB automation refuses to import an unresolved legacy write"
    );
    Ok(WbAutomationLegacyStateSeed {
        policy_digest: current_policy_digest.to_owned(),
        business_date: state.business_date,
        actions_today: state.actions_today,
        last_action_at: state.last_action_at,
        paused_for_daily_cap_on: state.paused_for_daily_cap_on,
        incident_class: state.incident_class,
        legacy_digest: sha256_domain("wb-automation-legacy-state-v1", &bytes),
    })
}

fn sha256_domain(domain: &str, bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain.as_bytes());
    digest.update([0]);
    digest.update(bytes);
    digest
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
            output
        })
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}
