#[path = "../src/bin/finance-collector/arguments.rs"]
mod arguments;

use arguments::{Command, Egress, parse_arguments, period_name};
use mcp_ozon::reporting::finance_reconciliation::WbFinanceReportPeriod;

fn raw(command: &str) -> Vec<String> {
    [
        command,
        "--registry",
        "/private/registry.json",
        "--actor",
        "finance",
        "--account",
        "ofk_region_wb",
        "--credentials-dir",
        "/private/credentials",
        "--state-dir",
        "/private/state",
        "--from",
        "2026-08-31",
        "--to",
        "2026-09-06",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[test]
fn old_operator_commands_keep_common_arguments_and_default_egress() {
    for (name, command) in [
        ("probe-wb", Command::ProbeWb),
        ("collect-wb", Command::CollectWb),
        ("migrate-personal-quota", Command::MigratePersonalQuota),
    ] {
        let parsed = parse_arguments(&raw(name)).unwrap();
        assert_eq!(parsed.command, command);
        assert!(matches!(parsed.egress, Egress::CollectorProxy));
        assert_eq!(parsed.actor, "finance");
        assert_eq!(parsed.account, "ofk_region_wb");
        assert_eq!(parsed.registry.to_str(), Some("/private/registry.json"));
        assert_eq!(
            parsed.credentials_dir.to_str(),
            Some("/private/credentials")
        );
        assert_eq!(parsed.state_dir.to_str(), Some("/private/state"));
        assert_eq!(parsed.from.to_string(), "2026-08-31");
        assert_eq!(parsed.to.to_string(), "2026-09-06");
        assert!(parsed.observation.is_none());
    }
}

#[test]
fn official_commands_require_observation_and_exact_report_identifier() {
    assert!(parse_arguments(&raw("list-reports-wb")).is_err());
    let mut list = raw("list-reports-wb");
    list.extend(["--observation", "pilot_20260913"].map(str::to_owned));
    let parsed = parse_arguments(&list).unwrap();
    assert_eq!(parsed.command, Command::ListReportsWb);
    assert_eq!(parsed.period, WbFinanceReportPeriod::Weekly);
    assert_eq!(period_name(parsed.period), "weekly");
    assert_eq!(parsed.currency, "RUB");
    assert_eq!(parsed.observation.as_deref(), Some("pilot_20260913"));
    list[0] = "reconcile-report-wb".to_owned();
    assert!(parse_arguments(&list).is_err());
    list.extend(["--report-id", "9007199254740993"].map(str::to_owned));
    assert_eq!(
        parse_arguments(&list).unwrap().report_id,
        Some(9_007_199_254_740_993)
    );
    list[0] = "publish-report-wb".to_owned();
    assert_eq!(
        parse_arguments(&list).unwrap().command,
        Command::PublishReportWb
    );
    for malformed in ["0", "01", "1e3", "+1", "-1", "9223372036854775808"] {
        *list.last_mut().unwrap() = malformed.to_owned();
        assert!(parse_arguments(&list).is_err());
    }
}

#[test]
fn unknown_unused_duplicate_and_unbounded_options_are_rejected() {
    for option in [
        ("--period", "weekly"),
        ("--observation", "pilot"),
        ("--arbitrary-url", "http://localhost"),
        ("--actor", "duplicate"),
    ] {
        let mut options = raw("probe-wb");
        options.extend(<[&str; 2]>::from(option).map(str::to_owned));
        assert!(parse_arguments(&options).is_err());
    }
    for observation in ["../escape", "with space", "", &"x".repeat(129)] {
        let mut options = raw("list-reports-wb");
        options.extend(["--observation", observation].map(str::to_owned));
        assert!(parse_arguments(&options).is_err());
    }
    let mut options = raw("list-reports-wb");
    options.extend(
        [
            "--observation",
            "pilot",
            "--period",
            "daily",
            "--egress",
            "direct",
        ]
        .map(str::to_owned),
    );
    let parsed = parse_arguments(&options).unwrap();
    assert_eq!(period_name(parsed.period), "daily");
    assert!(matches!(parsed.egress, Egress::Direct));
}

#[test]
fn report_sync_requires_durable_observation_and_bounded_follow_options() {
    let mut options = raw("sync-reports-wb");
    assert!(parse_arguments(&options).is_err());
    options.extend(
        [
            "--observation",
            "period_202608",
            "--follow",
            "true",
            "--max-run-seconds",
            "60",
        ]
        .map(str::to_owned),
    );
    let parsed = parse_arguments(&options).unwrap();
    assert_eq!(parsed.command, Command::SyncReportsWb);
    assert!(parsed.follow);
    assert_eq!(parsed.max_run_seconds, 60);
    for invalid in ["0", "86401", "-1", "bad"] {
        *options.last_mut().unwrap() = invalid.into();
        assert!(parse_arguments(&options).is_err());
    }
    *options.last_mut().unwrap() = "60".into();
    for option in [("--report-id", "7"), ("--currency", "RUB")] {
        let mut invalid = options.clone();
        invalid.extend(<[&str; 2]>::from(option).map(str::to_owned));
        assert!(parse_arguments(&invalid).is_err());
    }
    options[0] = "list-reports-wb".into();
    assert!(parse_arguments(&options).is_err());
}
