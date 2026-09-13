use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{Context, Result, ensure};
use chrono::{FixedOffset, NaiveDate, Utc};
use mcp_ozon::reporting::finance_reconciliation::WbFinanceReportPeriod;

#[derive(Clone, Copy)]
pub enum Egress {
    Direct,
    CollectorProxy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Command {
    ProbeWb,
    CollectWb,
    ListReportsWb,
    ReconcileReportWb,
    PublishReportWb,
    MigratePersonalQuota,
}

pub struct Arguments {
    pub command: Command,
    pub registry: PathBuf,
    pub actor: String,
    pub account: String,
    pub credentials_dir: PathBuf,
    pub state_dir: PathBuf,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub egress: Egress,
    pub period: WbFinanceReportPeriod,
    pub report_id: Option<u64>,
    pub currency: String,
    pub observation: Option<String>,
}

pub fn parse_arguments(raw: &[String]) -> Result<Arguments> {
    let command = match raw.first().map(String::as_str) {
        Some("probe-wb") => Command::ProbeWb,
        Some("collect-wb") => Command::CollectWb,
        Some("list-reports-wb") => Command::ListReportsWb,
        Some("reconcile-report-wb") => Command::ReconcileReportWb,
        Some("publish-report-wb") => Command::PublishReportWb,
        Some("migrate-personal-quota") => Command::MigratePersonalQuota,
        _ => anyhow::bail!("an explicit finance collector command is required; see --help"),
    };
    ensure!(
        raw.len() % 2 == 1,
        "every option requires exactly one value"
    );
    let mut values = BTreeMap::new();
    for pair in raw[1..].as_chunks::<2>().0 {
        ensure!(
            matches!(
                pair[0].as_str(),
                "--registry"
                    | "--actor"
                    | "--account"
                    | "--credentials-dir"
                    | "--state-dir"
                    | "--from"
                    | "--to"
                    | "--egress"
                    | "--period"
                    | "--report-id"
                    | "--currency"
                    | "--observation"
            ),
            "unknown finance collector option"
        );
        ensure!(
            !pair[1].is_empty() && values.insert(pair[0].as_str(), pair[1].as_str()).is_none(),
            "empty or duplicate finance collector option"
        );
    }
    let required = |name| {
        values
            .get(name)
            .copied()
            .context("a required finance collector option is missing")
    };
    let from = date(required("--from")?)?;
    let to = date(required("--to")?)?;
    let official = matches!(
        command,
        Command::ListReportsWb | Command::ReconcileReportWb | Command::PublishReportWb
    );
    let earliest = if official {
        NaiveDate::from_ymd_opt(2025, 1, 1).expect("constant date")
    } else {
        NaiveDate::from_ymd_opt(2024, 1, 29).expect("constant date")
    };
    ensure!(
        from >= earliest && to >= from && (to - from).num_days() < 31 && to < moscow_today(),
        "financial collection requires a supported closed date interval of at most 31 days"
    );
    let egress = match values.get("--egress").copied().unwrap_or("collector-proxy") {
        "collector-proxy" => Egress::CollectorProxy,
        "direct" => Egress::Direct,
        _ => anyhow::bail!("egress must be collector-proxy or direct"),
    };
    ensure!(
        official
            || !["--period", "--report-id", "--currency", "--observation"]
                .iter()
                .any(|key| values.contains_key(key)),
        "report options require an official report command"
    );
    let period = match values.get("--period").copied().unwrap_or("weekly") {
        "weekly" => WbFinanceReportPeriod::Weekly,
        "daily" => WbFinanceReportPeriod::Daily,
        _ => anyhow::bail!("report period must be weekly or daily"),
    };
    let currency = values.get("--currency").copied().unwrap_or("RUB");
    ensure!(
        currency.len() == 3 && currency.bytes().all(|byte| byte.is_ascii_uppercase()),
        "currency must be three uppercase ASCII letters"
    );
    let report_id = values
        .get("--report-id")
        .map(|raw| {
            ensure!(
                !raw.is_empty()
                    && raw.bytes().all(|byte| byte.is_ascii_digit())
                    && !(raw.len() > 1 && raw.starts_with('0')),
                "report ID must be a positive int64 literal"
            );
            let id = raw
                .parse::<u64>()
                .context("report ID must be a positive int64 literal")?;
            ensure!(
                id > 0 && id <= (u64::MAX >> 1),
                "report ID must be a positive int64 literal"
            );
            Ok::<_, anyhow::Error>(id)
        })
        .transpose()?;
    ensure!(
        matches!(
            command,
            Command::ReconcileReportWb | Command::PublishReportWb
        ) == report_id.is_some(),
        "--report-id is required exactly for reconcile-report-wb or publish-report-wb"
    );
    let observation = values.get("--observation").map(|raw| (*raw).to_owned());
    if official {
        let value = observation
            .as_deref()
            .context("--observation is required for official reports")?;
        ensure!(
            value.len() <= 128
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte)),
            "observation must be a bounded ASCII identifier"
        );
    }
    Ok(Arguments {
        command,
        registry: required("--registry")?.into(),
        actor: required("--actor")?.to_owned(),
        account: required("--account")?.to_owned(),
        credentials_dir: required("--credentials-dir")?.into(),
        state_dir: required("--state-dir")?.into(),
        from,
        to,
        egress,
        period,
        report_id,
        currency: currency.to_owned(),
        observation,
    })
}

fn date(raw: &str) -> Result<NaiveDate> {
    let value = NaiveDate::parse_from_str(raw, "%Y-%m-%d").context("date must use YYYY-MM-DD")?;
    ensure!(
        raw.len() == 10 && value.to_string() == raw,
        "date must use YYYY-MM-DD"
    );
    Ok(value)
}

pub fn moscow_today() -> NaiveDate {
    Utc::now()
        .with_timezone(&FixedOffset::east_opt(3 * 3_600).expect("Moscow offset"))
        .date_naive()
}

pub const fn period_name(period: WbFinanceReportPeriod) -> &'static str {
    match period {
        WbFinanceReportPeriod::Daily => "daily",
        WbFinanceReportPeriod::Weekly => "weekly",
    }
}
