use super::*;

#[tokio::test]
async fn campaign_inventory_and_stats_scope_are_validated_before_publication() {
    let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
    let mut invalid = FixtureTransport::complete();
    invalid.campaign_ids = json!({"adverts":"invalid"});
    assert_eq!(
        WbReportSource::new(invalid).collect_advertising(date).await,
        Err(WbReportSourceError::InvalidCampaignResponse)
    );
    for (campaign, business_date) in [(5, "2026-08-17"), (4, "2026-08-18")] {
        let fixture = FixtureTransport::complete();
        *fixture.stats.lock().unwrap() = VecDeque::from([Ok(
            json!([{"advertId":campaign,"stats":[{
                "date":business_date,"nm_id":1,"views":10,"clicks":1,"sum":2,"orders":1,"sum_price":90
            }]}]),
        )]);
        assert_eq!(
            WbReportSource::new(fixture).collect_advertising(date).await,
            Err(WbReportSourceError::InvalidPromotionResponse)
        );
    }
}

#[tokio::test]
async fn full_advertising_snapshot_has_a_bound_across_individually_valid_chunks() {
    let mut fixture = FixtureTransport::complete();
    fixture.campaign_ids = json!({"adverts":[{"status":9,"advert_list":(1..=51).map(|id|json!({"advertId":id})).collect::<Vec<_>>()}]});
    *fixture.stats.lock().unwrap() = [1, 51]
        .into_iter()
        .map(|campaign| {
            Ok(
                json!([{"advertId":campaign,"stats":(1..=12501).map(|sku|json!({
            "date":"2026-08-17","nm_id":sku,"views":10,"clicks":1,"sum":2,"orders":1,"sum_price":90
        })).collect::<Vec<_>>()}]),
            )
        })
        .collect();
    assert_eq!(
        WbReportSource::new(fixture)
            .collect_advertising(NaiveDate::from_ymd_opt(2026, 8, 17).unwrap())
            .await,
        Err(WbReportSourceError::PaginationLimit)
    );
}

#[test]
fn rate_limited_source_failure_keeps_the_rounded_vendor_delay() {
    let error = wb_source_failure(&WbError::RateLimited {
        request_id: None,
        retry_after: Some(Duration::from_millis(1500)),
    });
    assert_eq!(error.code(), "rate_limited");
    assert_eq!(error.failure().retry_after, Some(2));
}

#[tokio::test(start_paused = true)]
async fn local_admission_uses_one_deadline_without_retrying_zero_or_expired_waits() {
    let started = Instant::now();
    let mut attempts = 0;
    let error = wait_for_local_admission(started + PROMOTION_STATS_ADMISSION_BUDGET, || {
        attempts += 1;
        std::future::ready(Err(WbError::LocalRateLimited {
            retry_after: Duration::from_secs(20),
        }))
    })
    .await
    .unwrap_err();
    assert!(matches!(error, WbError::LocalRateLimited { .. }));
    assert_eq!(attempts, 3);
    assert_eq!(started.elapsed(), Duration::from_secs(40));

    let mut attempts = 0;
    let error = wait_for_local_admission(Instant::now() + Duration::from_secs(60), || {
        attempts += 1;
        std::future::ready(Err(WbError::LocalRateLimited {
            retry_after: Duration::ZERO,
        }))
    })
    .await
    .unwrap_err();
    assert!(matches!(error, WbError::LocalRateLimited { .. }));
    assert_eq!(attempts, 1, "a zero wait must not spin or repeat a request");

    let error = wait_for_local_admission(Instant::now(), || {
        attempts += 1;
        std::future::ready(Ok(json!([])))
    })
    .await
    .unwrap_err();
    assert!(matches!(error, WbError::DeadlineExceeded));
    assert_eq!(attempts, 1, "an expired budget must not start a request");
}

#[tokio::test(start_paused = true)]
async fn cancelled_or_late_local_wait_never_starts_another_request() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    for cancel in [true, false] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let request_attempts = Arc::clone(&attempts);
        let task = tokio::spawn(wait_for_local_admission(
            Instant::now() + Duration::from_secs(60),
            move || {
                request_attempts.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err(WbError::LocalRateLimited {
                    retry_after: Duration::from_secs(20),
                }))
            },
        ));
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            tokio::time::advance(Duration::from_secs(60)).await;
            assert!(matches!(
                task.await.unwrap().unwrap_err(),
                WbError::LocalRateLimited { .. }
            ));
        }
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn local_admission_never_retries_or_overwrites_an_admitted_vendor_failure() {
    let started = Instant::now();
    let mut attempts = 0;
    let error = wait_for_local_admission(started + Duration::from_secs(60), || {
        attempts += 1;
        let local = attempts == 1;
        async move {
            if local {
                Err(WbError::LocalRateLimited {
                    retry_after: Duration::from_secs(20),
                })
            } else {
                // An admitted WbClient call has its own logical deadline.
                // Its causal vendor result survives the admission budget.
                sleep(Duration::from_secs(45)).await;
                Err(WbError::RateLimited {
                    request_id: Some("fixture-request".to_owned()),
                    retry_after: Some(Duration::from_secs(120)),
                })
            }
        }
    })
    .await
    .unwrap_err();
    assert_eq!(attempts, 2);
    assert_eq!(started.elapsed(), Duration::from_secs(65));
    assert!(matches!(
        error,
        WbError::RateLimited { request_id, retry_after }
            if request_id.as_deref() == Some("fixture-request")
                && retry_after == Some(Duration::from_secs(120))
    ));
}
