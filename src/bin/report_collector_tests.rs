use chrono::{TimeZone, Timelike};

use super::*;

fn arguments(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

#[test]
fn dry_run_cli_defaults_to_morning_and_accepts_only_explicit_report_kinds() {
    assert_eq!(
        parse_command(&arguments(&["collection-preflight"])).unwrap(),
        Command::CollectionPreflight
    );
    assert_eq!(
        parse_command(&arguments(&["refresh-once"])).unwrap(),
        Command::RefreshOnce
    );
    assert!(matches!(
        parse_command(&arguments(&["ozon-dry-run", "ozon", "2026-08-18"])).unwrap(),
        Command::OzonDryRun {
            kind: ReportKind::Morning,
            ..
        }
    ));
    assert!(matches!(
        parse_command(&arguments(&["wb-dry-run", "wb", "2026-08-19", "evening"])).unwrap(),
        Command::WbDryRun {
            kind: ReportKind::Evening,
            ..
        }
    ));
    for invalid in [
        arguments(&["ozon-dry-run", "bad/account", "2026-08-18"]),
        arguments(&["ozon-dry-run", "ozon", "18-08-2026"]),
        arguments(&["ozon-dry-run", "ozon", "2026-08-18", "night"]),
    ] {
        assert!(parse_command(&invalid).is_err());
    }
}

#[test]
fn manager_refresh_reserves_scheduled_windows_and_api_pacing_tail() {
    // UTC 03:00 and 12:00 are 08:00 and 17:00 in Yekaterinburg.
    assert!(refresh_window_is_open(utc(2, 47, 59)));
    assert!(!refresh_window_is_open(utc(2, 48, 0)));
    assert!(!refresh_window_is_open(utc(3, 31, 5)));
    assert!(refresh_window_is_open(utc(3, 31, 6)));
    assert!(refresh_window_is_open(utc(11, 47, 59)));
    assert!(!refresh_window_is_open(utc(11, 48, 0)));
    assert!(!refresh_window_is_open(utc(12, 31, 5)));
    assert!(refresh_window_is_open(utc(12, 31, 6)));
}

fn utc(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 18, hour, minute, second)
        .unwrap()
}

#[test]
fn dry_run_windows_match_the_exact_morning_and_evening_cutoffs() {
    let requested_date = NaiveDate::from_ymd_opt(2026, 8, 18).unwrap();
    let morning_now = Utc.with_ymd_and_hms(2026, 8, 19, 3, 10, 0).unwrap();
    let (morning_start, morning_end, morning_cutoff) =
        dry_run_report_window(requested_date, ReportKind::Morning, morning_now).unwrap();
    assert_eq!(business_date(morning_start), requested_date);
    assert_eq!(
        business_date(morning_end),
        NaiveDate::from_ymd_opt(2026, 8, 19).unwrap()
    );
    assert_eq!(morning_cutoff.hour(), 3);

    let evening_now = Utc.with_ymd_and_hms(2026, 8, 18, 12, 20, 0).unwrap();
    let (evening_start, evening_end, evening_cutoff) =
        dry_run_report_window(requested_date, ReportKind::Evening, evening_now).unwrap();
    assert_eq!(business_date(evening_start), requested_date);
    assert_eq!(evening_end, evening_cutoff);
    assert_eq!(evening_cutoff.hour(), 12);

    assert!(
        dry_run_report_window(
            requested_date,
            ReportKind::Evening,
            Utc.with_ymd_and_hms(2026, 8, 18, 11, 59, 59).unwrap(),
        )
        .is_err()
    );
    assert!(
        dry_run_report_window(
            requested_date,
            ReportKind::Evening,
            Utc.with_ymd_and_hms(2026, 8, 19, 11, 59, 59).unwrap(),
        )
        .is_ok()
    );
    assert!(
        dry_run_report_window(
            requested_date,
            ReportKind::Evening,
            Utc.with_ymd_and_hms(2026, 8, 19, 12, 0, 1).unwrap(),
        )
        .is_err()
    );
}
