use super::*;
use crate::ozon_posting_sales::{PostingScheme, posting_page_request};

#[tokio::test]
async fn optional_source_dispatch_replays_pages_and_respects_departure_limits() {
    let (Ok(admin_url), Ok(collector_url)) = (
        std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL"),
        std::env::var("REPORT_SNAPSHOT_TEST_COLLECTOR_URL"),
    ) else {
        return;
    };
    let _database = DATABASE.lock().await;
    let mut fixture = Fixture::new(&admin_url, &collector_url).await;
    fixture.config =
        Fixture::config_with_replenishment(&fixture.root, &collector_url, "scheduled", true);
    let target = fixture.config.collection_plan()[0].clone();
    assert_eq!(target.marketplace, Marketplace::Ozon);
    let cutoff = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    let start = cutoff - Duration::days(1);
    fixture
        .writer
        .enqueue_replenishment_jobs(std::slice::from_ref(&target), cutoff, start, cutoff)
        .await
        .unwrap();
    for source in [
        SnapshotSource::ClusterStocks,
        SnapshotSource::PostingMovement,
    ] {
        fixture
            .select(&target.account_id, source_name(source), cutoff)
            .await;
        let claim = fixture
            .writer
            .claim_source_job_with_replenishment(
                std::slice::from_ref(&target),
                "optional-dispatch",
                true,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.source, source);
        fixture
            .seed(&claim, json!(["departure-probe"]), json!([]))
            .await;
        fixture.forbid_departures(&target.account_id).await;
        let failure = collect(&fixture.config, &fixture.writer, &claim)
            .await
            .err()
            .unwrap();
        assert_eq!(failure.code, "checkpoint_deferred");
        if source == SnapshotSource::ClusterStocks {
            let request = product_page_request("/v4/product/info/stocks", None).unwrap();
            fixture
                .seed(
                    &claim,
                    json!(["ozon-sku-reserves-v3", request.path, request.payload]),
                    json!([[], null]),
                )
                .await;
        } else {
            let from = start.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            let to = (cutoff - Duration::milliseconds(1))
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            for scheme in [PostingScheme::Fbo, PostingScheme::Fbs] {
                let payload = posting_page_request(scheme, &from, &to, None);
                fixture
                    .seed(
                        &claim,
                        json!(["ozon-posting-movement-v1", scheme.endpoint(), payload]),
                        json!({"postings":[],"has_next":false,"cursor":""}),
                    )
                    .await;
            }
        }
        fixture.forbid_departures(&target.account_id).await;
        let (facts, expenses) = collect(&fixture.config, &fixture.writer, &claim)
            .await
            .unwrap();
        match (&facts, source) {
            (CollectedFacts::ClusterStocks(rows), SnapshotSource::ClusterStocks) => {
                assert!(rows.is_empty());
            }
            (CollectedFacts::PostingMovement(rows), SnapshotSource::PostingMovement) => {
                assert!(rows.is_empty());
            }
            _ => panic!("optional source dispatched to a different fact type"),
        }
        assert!(expenses.is_empty());
        complete_quantum(&fixture.writer, &claim, Ok((facts, expenses)))
            .await
            .unwrap();
        assert_eq!(fixture.state(&claim).await.0, "published");
    }
}
