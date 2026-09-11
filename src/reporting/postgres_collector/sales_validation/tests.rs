use super::*;
use chrono::NaiveDate;

fn fact(sku: u64) -> CollectedSalesFact {
    CollectedSalesFact {
        business_date: NaiveDate::from_ymd_opt(2026, 8, 17).unwrap(),
        sku,
        ordered_units: 3,
        operational_gmv_minor: 123_456,
        cancelled_units: None,
        returned_units: None,
    }
}

#[test]
fn duplicate_counts_do_not_merge_conflicting_sales_values() {
    let first = fact(731_892_456);
    let mut conflict = first.clone();
    conflict.ordered_units = 8;
    let facts = vec![first.clone(), fact(731_892_457), first, conflict];
    let fingerprint = Fingerprint::inspect(&facts);
    assert_eq!(fingerprint.reason(), Some("duplicate_identity"));
    assert_eq!(fingerprint.checked_rows, 4);
    assert_eq!(fingerprint.distinct_identities, 2);
    assert_eq!(fingerprint.duplicate_rows, 2);
    assert_eq!(validate(&facts), Err(PostgresCollectorError::InvalidInput));
    assert_eq!(facts.len(), 4);
    assert_eq!(facts[3].ordered_units, 8);
}

#[test]
fn empty_valid_and_same_sku_on_different_days_remain_valid() {
    assert_eq!(validate(&[]), Ok(()));
    let first = fact(1);
    let mut second = first.clone();
    second.business_date = first.business_date.succ_opt().unwrap();
    assert_eq!(validate(&[first, second]), Ok(()));
}

#[test]
fn numeric_failures_are_separate_from_duplicate_identity() {
    let mut count = fact(1);
    count.ordered_units = i32::MAX as u64 + 1;
    let mut cancelled = fact(2);
    cancelled.cancelled_units = Some(i32::MAX as u64 + 1);
    let mut returned = fact(3);
    returned.returned_units = Some(i32::MAX as u64 + 1);
    let mut money = fact(4);
    money.operational_gmv_minor = i64::MAX as u64 + 1;
    let facts = [count, cancelled, returned, money, fact(0), fact(u64::MAX)];
    let fingerprint = Fingerprint::inspect(&facts);
    assert_eq!(fingerprint.reason(), Some("invalid_numeric_range"));
    assert_eq!(fingerprint.duplicate_rows, 0);
    assert_eq!(fingerprint.invalid_sku_rows, 2);
    assert_eq!(fingerprint.invalid_count_rows, 3);
    assert_eq!(fingerprint.invalid_money_rows, 1);
    assert_eq!(validate(&facts), Err(PostgresCollectorError::InvalidInput));
}

#[test]
fn fingerprint_work_and_output_are_bounded() {
    let facts = vec![fact(1); MAX_FACT_ROWS + 1];
    let fingerprint = Fingerprint::inspect(&facts);
    assert_eq!(fingerprint.checked_rows, MAX_FACT_ROWS);
    assert_eq!(fingerprint.duplicate_rows, MAX_FACT_ROWS - 1);
    assert_eq!(fingerprint.reason(), Some("row_limit"));
    assert!(fingerprint.truncated);
    assert!(format!("{fingerprint:?}").len() < 256);
}

fn capture_logs(action: impl FnOnce()) -> String {
    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };

    #[derive(Clone)]
    struct Log(Arc<Mutex<Vec<u8>>>);
    impl Write for Log {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let log = Log(Arc::clone(&bytes));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_writer(move || log.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, action);
    String::from_utf8(bytes.lock().unwrap().clone()).unwrap()
}

#[test]
fn emitted_diagnostic_contains_no_identity_date_or_metric_values() {
    let output = capture_logs(|| {
        let first = fact(731_892_456);
        assert_eq!(
            validate(&[first.clone(), first]),
            Err(PostgresCollectorError::InvalidInput)
        );
    });
    assert!(output.contains("reason=\"duplicate_identity\""));
    assert!(output.contains("duplicate_rows=1"));
    assert!(output.contains("checked_rows=2"));
    for forbidden in [
        "731892456",
        "123456",
        "2026-08-17",
        "ordered_units",
        "business_date",
    ] {
        assert!(
            !output.contains(forbidden),
            "{forbidden} leaked into diagnostic"
        );
    }
    assert!(output.len() < 512);
}

#[test]
fn metadata_rejection_is_identified_before_sales_facts_and_redacted() {
    use crate::reporting::postgres_collector::CollectedSnapshot;
    use crate::reporting::snapshot::Marketplace;
    use chrono::{Duration, TimeZone, Utc};

    let cutoff = Utc.with_ymd_and_hms(2026, 8, 17, 12, 0, 0).unwrap();
    for (account, version, complete, end, expected) in [
        (
            "sensitive account",
            "test",
            true,
            cutoff,
            "invalid_descriptor",
        ),
        (
            "fixture",
            "secret version!",
            true,
            cutoff,
            "invalid_collector_version",
        ),
        ("fixture", "test", false, cutoff, "incomplete_pagination"),
        (
            "fixture",
            "test",
            true,
            cutoff + Duration::hours(1),
            "invalid_time_range",
        ),
    ] {
        let output = capture_logs(|| {
            let duplicate = fact(731_892_456);
            assert_eq!(
                CollectedSnapshot::new(
                    account.to_owned(),
                    Marketplace::Ozon,
                    cutoff,
                    cutoff,
                    cutoff - Duration::hours(1),
                    end,
                    SnapshotStatus::Succeeded,
                    complete,
                    version.to_owned(),
                    CollectedFacts::Sales(vec![duplicate.clone(), duplicate]),
                ),
                Err(PostgresCollectorError::InvalidInput)
            );
        });
        assert!(output.contains(expected), "{output}");
        assert!(
            !output.contains("duplicate_identity"),
            "metadata failed before checking facts"
        );
        for forbidden in [
            "sensitive account",
            "secret version!",
            "731892456",
            "2026-08-17",
        ] {
            assert!(
                !output.contains(forbidden),
                "{forbidden} leaked into diagnostic"
            );
        }
    }
    let output = capture_logs(|| {
        assert_eq!(
            validate_metadata(
                &CollectedFacts::Sales(vec![fact(1); MAX_FACT_ROWS + 1]),
                "test",
                SnapshotStatus::Succeeded,
                true
            ),
            Err(PostgresCollectorError::InvalidInput)
        );
    });
    assert!(output.contains("row_limit"));
    assert!(!output.contains("duplicate_identity"));
}
