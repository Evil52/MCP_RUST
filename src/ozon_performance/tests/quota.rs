use super::*;

#[tokio::test]
async fn unavailable_shared_quota_blocks_oauth_and_cached_token_api_transport() {
    for cached_token in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let client = PerformanceClient::new_for_test(
            format!("http://{}", listener.local_addr().unwrap()),
            Duration::from_secs(1),
            credentials(),
        )
        .with_shared_quota(SharedQuota::from_database_url("invalid-quota-url"));
        if cached_token {
            client.accounts[&StoreId::from("shop")]
                .token
                .lock()
                .await
                .cached = Some(CachedToken {
                value: "test-token".to_owned(),
                refresh_at: Instant::now() + Duration::from_secs(60),
            });
        }
        let error = client
            .get(&StoreId::from("shop"), CAMPAIGNS_PATH, Vec::new())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            PerformanceError::SharedQuota(QuotaError::Unavailable)
        ));
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}

#[tokio::test(start_paused = true)]
async fn shared_pacer_orders_reads_around_an_exclusive_write_boundary() {
    let pacer = PerformanceRequestPacer::new();
    let reader = PerformanceClient::new_with_https_proxy_and_pacer(
        Duration::from_secs(2),
        credentials(),
        "http://127.0.0.1:3128",
        &pacer,
    )
    .unwrap();
    assert!(Arc::ptr_eq(
        &reader.accounts[&StoreId::from("shop")].pacing.next_allowed,
        &pacer.next_allowed
    ));
    assert!(
        PerformanceClient::new_with_https_proxy_and_pacer(
            Duration::from_secs(2),
            credentials(),
            "http://[invalid",
            &pacer
        )
        .is_err()
    );
    assert!(pacer.try_claim_request_slot(Duration::from_secs(10)).await);

    let writer_pacer = pacer.clone();
    let mut writer = tokio::spawn(async move { writer_pacer.reserve_write().await });
    tokio::task::yield_now().await;
    assert!(
        !writer.is_finished(),
        "a write must wait for the preceding read start interval"
    );
    tokio::time::advance(Duration::from_secs(10)).await;
    let mut write_guard = (&mut writer).await.unwrap();
    drop(writer);
    write_guard.mark_request_started(Duration::from_secs(5));

    let reader_pacer = pacer.clone();
    let reader = tokio::spawn(async move {
        reader_pacer.wait_until_ready().await;
        reader_pacer
            .try_claim_request_slot(Duration::from_secs(1))
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        !reader.is_finished(),
        "a read must not cross an active write marker-to-result boundary"
    );

    drop(write_guard);
    tokio::task::yield_now().await;
    assert!(
        !reader.is_finished(),
        "the write request-start interval remains active after its result"
    );
    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(reader.await.unwrap());
}
