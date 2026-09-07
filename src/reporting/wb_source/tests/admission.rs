use super::*;

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
