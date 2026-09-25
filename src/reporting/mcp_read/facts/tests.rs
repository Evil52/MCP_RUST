use chrono::{Duration, TimeZone};

use super::*;

fn descriptor(id: i64, day: u32, source: SnapshotSource, rows: u32) -> SnapshotDescriptor {
    let cutoff = Utc.with_ymd_and_hms(2026, 9, day, 3, 0, 0).unwrap();
    let start = if source == SnapshotSource::Stocks {
        cutoff
    } else {
        cutoff - Duration::days(1)
    };
    SnapshotDescriptor::new(
        id,
        "store_1".to_owned(),
        Marketplace::Ozon,
        source,
        cutoff,
        cutoff,
        start,
        cutoff,
        rows,
        true,
        SnapshotStatus::Succeeded,
    )
    .unwrap()
}

fn expected(descriptors: Vec<SnapshotDescriptor>) -> BTreeMap<i64, SnapshotDescriptor> {
    descriptors
        .into_iter()
        .map(|descriptor| (descriptor.snapshot_id(), descriptor))
        .collect()
}

fn row_count(batch: &BTreeMap<i64, SnapshotDescriptor>) -> usize {
    batch
        .values()
        .map(|descriptor| descriptor.row_count() as usize)
        .sum()
}

#[test]
fn multi_day_history_exceeding_single_query_cap_retains_every_cutoff_and_descriptor() {
    // Snapshot IDs intentionally run opposite to cutoff order.
    let descriptors = expected(
        (18..=24)
            .flat_map(|day| {
                let id = i64::from(25 - day) * 2;
                [
                    descriptor(id, day, SnapshotSource::Sales, 1_200),
                    descriptor(id + 1, day, SnapshotSource::Advertising, 3_000),
                ]
            })
            .collect(),
    );
    assert_eq!(row_count(&descriptors), 29_400);
    let batches = history_kpi_batches(&descriptors).unwrap();
    assert_eq!(
        batches.iter().map(row_count).collect::<Vec<_>>(),
        [21_000, 8_400]
    );
    let mut seen_cutoffs = std::collections::BTreeSet::new();
    for batch in &batches {
        let mut sources = MetricSourceIds::new();
        for descriptor in batch.values() {
            insert_metric_source_id(&mut sources, descriptor).unwrap();
        }
        for (cutoff, pair) in sources {
            assert!(
                seen_cutoffs.insert(cutoff),
                "a cutoff was split between batches"
            );
            assert!(complete_metric_pair(pair).is_some());
        }
    }
    assert_eq!(seen_cutoffs.len(), 7);
    // Equality covers provenance, time range, status and exact declared count.
    assert_eq!(
        batches.into_iter().flatten().collect::<BTreeMap<_, _>>(),
        descriptors
    );
}

#[test]
fn exact_limit_and_zero_row_cutoffs_remain_valid_without_empty_batches() {
    let descriptors = expected(vec![
        descriptor(1, 18, SnapshotSource::Sales, 10_000),
        descriptor(2, 18, SnapshotSource::Advertising, 15_000),
        descriptor(3, 19, SnapshotSource::Sales, 0),
        descriptor(4, 19, SnapshotSource::Advertising, 0),
        descriptor(5, 20, SnapshotSource::Sales, 1),
    ]);
    let batches = history_kpi_batches(&descriptors).unwrap();
    assert_eq!(
        batches.iter().map(row_count).collect::<Vec<_>>(),
        [25_000, 1]
    );
    assert_eq!(batches[0].len(), 4);
    assert_eq!(batches[1].len(), 1);
    assert_eq!(
        batches.into_iter().flatten().collect::<BTreeMap<_, _>>(),
        descriptors
    );
}

#[test]
fn oversized_single_cutoff_still_fails_without_truncating_or_splitting_its_pair() {
    let descriptors = expected(vec![
        descriptor(1, 18, SnapshotSource::Sales, 12_500),
        descriptor(2, 18, SnapshotSource::Advertising, 12_501),
    ]);
    assert_eq!(
        history_kpi_batches(&descriptors),
        Err(ReportingReadError::InvalidRequest)
    );
}

#[test]
fn irrelevant_sources_do_not_consume_the_history_fact_budget() {
    let sales = descriptor(1, 18, SnapshotSource::Sales, 1);
    let stock = descriptor(2, 18, SnapshotSource::Stocks, 1_000_000);
    let descriptors = expected(vec![sales.clone(), stock.clone()]);
    assert_eq!(
        history_kpi_batches(&descriptors).unwrap(),
        vec![expected(vec![sales])]
    );
    assert!(
        history_kpi_batches(&expected(vec![stock]))
            .unwrap()
            .is_empty()
    );
    assert!(history_kpi_batches(&BTreeMap::new()).unwrap().is_empty());
}

#[test]
fn duplicate_metric_sources_are_preserved_for_existing_validation_to_reject() {
    let descriptors = expected(vec![
        descriptor(1, 18, SnapshotSource::Sales, 1),
        descriptor(2, 18, SnapshotSource::Sales, 1),
    ]);
    let batches = history_kpi_batches(&descriptors).unwrap();
    assert_eq!(batches, vec![descriptors]);
    let mut sources = MetricSourceIds::new();
    let mut descriptors = batches[0].values();
    insert_metric_source_id(&mut sources, descriptors.next().unwrap()).unwrap();
    assert_eq!(
        insert_metric_source_id(&mut sources, descriptors.next().unwrap()),
        Err(ReportingReadError::InvalidPublishedData)
    );
}
