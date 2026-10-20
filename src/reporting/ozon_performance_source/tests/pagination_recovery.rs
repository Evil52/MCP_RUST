use super::*;
use crate::reporting::checkpoint::tests::{MemoryPages, journal};

#[tokio::test]
async fn retry_on_second_campaign_page_resumes_without_partial_results_or_lost_batches() {
    let first = (1..=100).collect::<Vec<_>>();
    let transport = FixtureTransport::new(
        vec![campaign_page(&first, 101), campaign_page(&[101], 101)],
        (0..11)
            .map(|index| statistics(index * 10 + 1, index + 1, "2026-08-18"))
            .collect(),
    );
    transport.campaign_pages.lock().unwrap().insert(
        1,
        Err(OzonPerformanceReportSourceError::RetryAfter { seconds: 81 }),
    );
    let pages = MemoryPages::default();
    let mut source = OzonPerformanceReportSource::new(transport);
    let date = NaiveDate::from_ymd_opt(2026, 8, 18).unwrap();
    source.checkpoints = journal(&pages);
    assert_eq!(
        source.collect(date).await,
        Err(OzonPerformanceReportSourceError::Checkpoint(
            CheckpointError::Deferred
        ))
    );
    assert_eq!(pages.lock().unwrap().len(), 1);

    source.checkpoints = journal(&pages);
    let error = source.collect(date).await.unwrap_err();
    assert_eq!(
        error,
        OzonPerformanceReportSourceError::RetryAfter { seconds: 81 }
    );
    assert_eq!(error.failure().retry_after, Some(81));
    assert_eq!(
        pages.lock().unwrap().len(),
        1,
        "a failed page must not be checkpointed"
    );

    // The same page is retried. Each quantum saves one successful upstream
    // response, and all earlier pages/batches are replayed from checkpoints.
    for _ in 0..11 {
        source.checkpoints = journal(&pages);
        assert_eq!(
            source.collect(date).await,
            Err(OzonPerformanceReportSourceError::Checkpoint(
                CheckpointError::Deferred
            ))
        );
    }
    source.checkpoints = journal(&pages);
    let facts = source.collect(date).await.unwrap();
    assert_eq!(facts.len(), 11);
    assert_eq!(
        facts.iter().map(|row| row.campaign_id).collect::<Vec<_>>(),
        (0..11).map(|index| index * 10 + 1).collect::<Vec<_>>()
    );
    assert_eq!(pages.lock().unwrap().len(), 13);

    // Exhausted transport fixtures would fail if even one completed page or
    // statistics batch were fetched again.
    source.checkpoints = journal(&pages);
    assert_eq!(source.collect(date).await.unwrap(), facts);
}
